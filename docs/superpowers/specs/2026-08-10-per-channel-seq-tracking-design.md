# Per-channel sequence tracking and reset counting

**Status:** designed
**Applies to:** `topofbook-parser`, `marketbyorder-parser`, `marketbyprice-parser`, `marketbyorder-bot`
**Related:** `marketbyprice-bot` reset fix, PR #38 (open)

## The root cause

A single multicast group and port pair carries **two redundant publishers**,
interleaved packet by packet and distinguished only by `Channel ID` in the frame
header. Their content is equivalent, but each maintains its own **independent
per-publisher counters**: frame sequence numbers and Reset Count.

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
per-channel counters once the change is running against the live feed.

## Parser change

`seqTracker` gains per-channel state and takes the channel:

```go
type seqTracker struct {
    last map[uint8]uint64 // channel_id -> last seq seen
}

func (s *seqTracker) observe(ch uint8, seq uint64) (gaps, missing uint64)
```

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

**refdata stays excluded.** Per-channel keying fixes the "shared seq space" half
of the existing exclusion, but refdata is a periodic-retransmit stream, so
sequence gaps there remain meaningless by design.

## Metric surface

`frame_seq_gaps_total` and `frames_missing_total` gain a **`channel_id`** label,
giving `(port, channel_id)`.

The label is named `channel_id`, not `channel`, because `topofbook-parser`
already uses `channel` to mean the port. Prometheus cannot carry two labels of
the same name, so `channel` is unavailable there. Renaming that existing misnomer
is a separate, dashboard-breaking cleanup and is out of scope.

Cardinality grows to roughly two series per port. The only dashboard query over
these metrics is a `sum(rate(...))`, which aggregates the new label away and
keeps working unchanged.

## Edge cases

- `channel_id` is a `uint8`, so the map is bounded at 256 entries and typically
  holds 2. No eviction required.
- A channel first seen mid-stream initialises silently, instead of emitting one
  enormous phantom gap. This is an improvement on current behaviour.
- Reorders and duplicates (`seq <= last`) stay ignored, now per channel.
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

- two interleaved channels with independent sequences report **zero** gaps — the
  regression test for this bug
- a genuine gap within one channel is still counted, and attributed to that
  channel
- the first frame seen on a channel is silent
- reorders and duplicates are handled per channel, without disturbing the other

For `marketbyorder-bot`, mirror the two tests from PR #38: interleaved
channels with distinct steady Reset Counts run no barrier, and a real Reset Count
change runs exactly one barrier that spares the other channel's instruments,
refdata, and snapshot contexts.

Each test must be confirmed failing before the corresponding fix.

## Delivery

Two pull requests off one spec:

1. **Parsers** — per-channel `seqTracker` and the `channel_id` label across all
   three parsers.
2. **marketbyorder-bot** — the reset-count port described above.

Verification for PR 1 is against the live feed: the market-by-price snapshot
port's `frames_missing_total` should resolve into per-publisher series, and
whatever remains is the real loss.

## Success criteria

- Loss is attributable to a specific publisher rather than an aggregate.
- The snapshot-port loss figure is trustworthy enough to alert on.
- No per-publisher counter in this repo is tracked as a single global value.
