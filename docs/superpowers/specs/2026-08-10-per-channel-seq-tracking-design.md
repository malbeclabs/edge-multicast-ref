# Per-publisher sequence tracking and reset counting

**Status:** designed
**Applies to:** `topofbook-parser`, `marketbyorder-parser`, `marketbyprice-parser`, `marketbyorder-bot`
**Related:** `marketbyprice-bot` reset fix, PR #38 (open)

## The root cause

A single multicast group and port pair carries **two redundant publishers**,
interleaved packet by packet and distinguished by their source IP and by
`Channel ID` in the frame header. Their content is equivalent, but each
maintains its own **independent per-publisher counters**: frame sequence numbers
and Reset Count.

Any such counter held as a single global value is wrong, and wrong silently. The
wire is well formed, no error is logged, and the derived metric or state simply
stops meaning what its name says.

Two instances exist in this repo. One is fixed:

| Component | State | Symptom | Status |
|---|---|---|---|
| `marketbyprice-bot` | `Coordinator.resetCount uint8` | 1.35M spurious reset barriers wiped refdata; book read-outs persisted with an empty symbol | fixed in PR #38, awaiting merge |
| all three parsers | `receive()` holds one `seqTracker` per port | two interleaved sequence spaces conflate into phantom gaps | this design |
| `marketbyorder-bot` | `Coordinator.resetCount uint8` | same as `marketbyprice-bot`, currently unexposed | this design |

`topofbook-bot` has no reset-count or shard logic and needs no change.

## Observed evidence

On the market-by-price snapshot port, one tracker spanning two sequence spaces
reported ~409k `frames_missing_total` and ~38k gaps against ~5.9M frames received,
while the host reported zero UDP `RcvbufErrors` and zero `InErrors`.

The market-by-price mktdata port and the top-of-book group both carry two
publishers as well, yet report almost no gaps. Their publisher pairs happen to
stay frame-synchronised, so the conflated tracker sees a near-monotonic sequence.
That is luck, not design: the top-of-book parser holds the identical latent bug
and will report the same phantom loss the moment its pair drifts.

**This design makes the metric correct. It does not by itself establish how much
of the residual snapshot loss is real** — that is answered by reading the
per-publisher counters once the change is running against the live feed.

## The publisher identity tuple

A sequence space belongs to **`(port, source_ip, channel_id)`**, not to the port
alone.

Including `source_ip` is defence in depth rather than a fix for an observed
break, and the spec should be honest about that. Measured over a 25-second
capture of both groups:

- 67 distinct `(source_ip, channel_id)` pairs against 64 distinct `channel_id`
  values, so some channel id is genuinely reused across sources.
- The only case of one `(group, port, channel_id)` arriving from more than one
  source was on a port no parser binds. On the three ports the parsers do bind,
  the publishers follow an `N` / `N+100` channel-id convention, so `channel_id`
  alone currently separates them.

`source_ip` earns its place because that convention is a convention, not a
guarantee, and because the failure it prevents is the silent kind this whole
design exists to eliminate. A publisher renumbered onto an id already in use
would conflate two sequence spaces and reproduce the original bug. The cost is
near zero: the address is already in hand at receive time.

The safe-direction property matters too. If a publisher is rehomed to a new IP,
the tracker sees a new key and initialises silently — under-reporting briefly,
never inventing a phantom gap.

## Parser change

`seqTracker` gains per-publisher state and takes the full identity:

```go
type pubKey struct {
    src netip.Addr // source IP
    ch  uint8      // channel_id from the frame header
}

type seqTracker struct {
    last map[pubKey]uint64
}

func (s *seqTracker) observe(src netip.Addr, ch uint8, seq uint64) (gaps, missing uint64)
```

`netip.Addr` rather than a string: it is comparable, usable directly as a map
key, and costs no allocation per datagram the way `.String()` would.

The map is lazily initialised inside `observe`, so the existing
`var tracker seqTracker` zero-value usage in `receive()` keeps working rather
than forcing a constructor.

The keying lives in the tracker, not in the receive loop, so a caller cannot
omit it. That matters here specifically: this code is triplicated across three
parsers, and putting the invariant in the type is what stops the third copy from
drifting back.

The caller reads the channel from the frame header:

```go
const frameHeaderChannelOffset = 3
```

All three parsers share the header prefix `Magic[2], SchemaVersion, ChannelID,
SequenceNumber`, so the channel is byte 3 and the sequence is bytes 4:12 in each.
Byte 3 falls inside the existing `n >= frameHeaderMinLen` (12) guard, so no new
bounds check is needed.

### Plumbing the source address

`readDatagram` currently discards it. Both variants must return it:

| File | Current | Change |
|---|---|---|
| `timestamp_linux.go` | `conn.ReadMsgUDP(buf, oob)` drops the returned addr | return it |
| `timestamp_other.go` | `conn.ReadFromUDP(buf)` drops the returned addr | return it |

That is two build-tagged files per parser, six in total. The signature becomes
`(int, netip.Addr, time.Time, string, error)`. This is the only part of the
change that touches anything outside the receive loop and the tracker.

**refdata stays excluded.** Per-channel keying fixes the "shared seq space" half
of the existing exclusion, but refdata is a periodic-retransmit stream, so
sequence gaps there remain meaningless by design.

## Metric surface

`frame_seq_gaps_total` and `frames_missing_total` gain **`source_ip`** and
**`channel_id`** labels, giving `(port, source_ip, channel_id)` — the same tuple
the tracker keys on, so the metric can answer "which publisher is losing
frames" directly.

The channel label is named `channel_id`, not `channel`, because
`topofbook-parser` already uses `channel` to mean the port. Prometheus cannot
carry two labels of the same name, so `channel` is unavailable there. Renaming
that existing misnomer is a separate, dashboard-breaking cleanup and is out of
scope.

Cardinality grows to roughly two series per port, since each bound port carries
two publishers. The only dashboard query over these metrics is a
`sum(rate(...))`, which aggregates the new labels away and keeps working
unchanged.

A rehomed publisher leaves its old series behind as a stale counter. That is
normal Prometheus behaviour and needs no handling.

## Edge cases

- The map is keyed by `(source_ip, channel_id)`, so its size is bounded by the
  number of distinct senders actually observed on a bound port — two in
  practice. Unlike a `channel_id`-only key it is not bounded at 256 by the type,
  but only a sender reaching this socket can add an entry, and the socket is
  joined to one multicast group. No eviction required.
- A publisher first seen mid-stream initialises silently, instead of emitting
  one enormous phantom gap. This is an improvement on current behaviour, and it
  is what makes a rehomed source safe.
- Reorders and duplicates (`seq <= last`) stay ignored, now per publisher.
- Frames shorter than 12 bytes are skipped as today; byte 3 is only read inside
  that guard.

**Known limitation, out of scope.** If a publisher restarts and its sequence
drops to a low value, `last` for that channel stays high and loss is
under-reported until the sequence climbs back. This is pre-existing behaviour,
unchanged by this design. Reset Count is available in the same header and could
drive a tracker reset, but that is its own change.

## marketbyorder-bot reset fix

A direct port of the `marketbyprice-bot` fix in PR #38. That PR should land
first, so this port follows a reviewed shape rather than a proposed one:

- `Coordinator.resetCount` becomes `map[uint8]uint8`, keyed by `Channel ID`;
  `resetSeen` is dropped, since map presence carries it.
- `Shard.reset()` becomes `resetChannel(ch)`, deleting only keys whose
  `instKey.ch` matches. It must **also** clear `snapCtx`, which is keyed by
  `snapKey{channel, snapshot_id}` — this is the one thing that differs from the
  market-by-price fix, whose shard has no equivalent map.
- Every shard is still drained on a barrier: ordering the wipe after all
  in-flight records is the barrier's job, and any shard may hold records for the
  resetting channel.

## Testing

Extend `seqtracker_test.go` in each parser with a table covering:

- two interleaved publishers with independent sequences report **zero** gaps —
  the regression test for this bug
- a genuine gap within one publisher is still counted, and attributed to that
  publisher
- the first frame seen from a publisher is silent
- reorders and duplicates are handled per publisher, without disturbing the other
- **two sources sharing one `channel_id` stay separate** — the case `source_ip`
  exists to cover, and the one not reachable through a channel-only key
- the same source on two channel ids stays separate

For `marketbyorder-bot`, mirror the two tests from PR #38: interleaved
channels with distinct steady Reset Counts run no barrier, and a real Reset Count
change runs exactly one barrier that spares the other channel's instruments,
refdata, and snapshot contexts.

Each test must be confirmed failing before the corresponding fix.

## Delivery

Two pull requests off one spec:

1. **Parsers** — per-publisher `seqTracker` keyed by
   `(source_ip, channel_id)`, the source-address plumbing, and the new labels,
   across all three parsers.
2. **marketbyorder-bot** — the reset-count port described above.

Verification for PR 1 is against the live feed: the market-by-price snapshot
port's `frames_missing_total` should resolve into per-publisher series, and
whatever remains is the real loss.

## Success criteria

- Loss is attributable to a specific publisher rather than an aggregate.
- The snapshot-port loss figure is trustworthy enough to alert on.
- No per-publisher counter in this repo is tracked as a single global value.
