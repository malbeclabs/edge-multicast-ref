# Shared publisher crates for DoubleZero Edge

**Status:** draft, pending review
**Applies to:** the venue publisher repositories and this repository
**Authority:** [`edge-feed-spec`](https://github.com/malbeclabs/edge-feed-spec), its [`GLOSSARY.md`](https://github.com/malbeclabs/edge-feed-spec/blob/main/GLOSSARY.md) and [`VERSIONING.md`](https://github.com/malbeclabs/edge-feed-spec/blob/main/VERSIONING.md); Feed Publisher Playbook Phases 5, 6 and 6.5

---

## Naming

This repository is public. This document names no venue, venue repository,
venue crate, config key, metric prefix or issue tracker, and gives no count of
publishers. Findings refer to "an existing publisher" or "another publisher",
identified where needed by the property under discussion. The same rule binds
every later document here.

`GLOSSARY.md` governs all vocabulary and overrides any local definition.

---

## Purpose

Existing publishers each solved the same problems separately. These crates hold
the solutions once, so the next one inherits them.

A publisher should have almost no semantics of its own. What differs between
venues is the upstream source and the mapping onto our messages. The specs
decide everything else, and every place a publisher decides it again is a place
one of them has decided it wrong.

---

## What the audit found

### Vocabulary

The glossary was published 2026-08-20. The feed specs were conformed to it; no
publisher was. Counting matching lines in first-party code, one publisher
carries over 800 uses of `frame` for our own traffic and another over 1,300;
one carries over 1,100 uses of `lane`; `epoch` appears in two. This repository
uses `bot` 92 times for components the glossary calls book-builders, and names
three binaries `*-bot`. Its remaining `frame` uses are largely correct, since
the XDP receiver and GRE decapsulator handle real layer-2 frames.

One publisher names an enumeration `Channel` whose variants are market data,
reference data and snapshot. Those are port roles, and the same crate separately
handles `Channel ID`.

**One publisher is at zero across every banned term.** Where this design chooses
a name it takes that one's. Where it chooses a behavior it usually takes the
publisher that has met production.

### Wire

**The codec was forked twice by copy-paste.** One codec records its origin as a
port from another publisher's protocol module, *"which is still on schema 1."*
It then moved to Schema Version 3 alone. A second publisher moved to 3 alone,
separately. The original still publishes 1.

**One publisher exceeds the mandated datagram size.** Every feed spec mandates
1,232 bytes for GRE headroom. One publisher defaults to 1448, ships that to
production, and does not clamp, so its top-of-book feed can emit 216 bytes over
the cap; its other two feeds are correct, because the constant lives in three
places and two were fixed. Another holds a single constant at 1232. A third
inherited the 1448 default but clamps to `min(mtu, MAX_DATAGRAM_SIZE)` in the
builder, with a test asserting it, and is safe because the limit is in the
builder rather than in configuration.

**Two publishers reached opposite conclusions on egress.** One transport module
defers an improvement: *"Hosts that must pin multicast egress to a specific
interface ... need `socket2`'s `set_multicast_if_v4`."* Another records why that
is wrong here: `IP_MULTICAST_IF` *"stays unset: the kernel resolves it to an
interface index at `setsockopt` time and `doublezerod` recreates [the tunnel
interface] with a new index on every re-provision, which left the socket
returning `ENODEV` forever."* One publisher's roadmap is another's outage.

That second publisher also survived a tunnel address moving without notice: the
configured address stopped existing and the service crash-looped 31,108 times
over two days. It now derives its source IP address from the route. The others
read it from config.

**One publisher bursts the definition cycle.** `reference-data/spec.md` rule 2:
*"Publishers MUST NOT emit the entire published set as a single burst."* That
publisher's refdata module: *"the emission is a synchronized burst."* Another
paces at 80% of the cycle period, because the period is a maximum on the
interval between retransmissions of any single definition, not a lap target.

**Publishers transmit at `0x05`, and three feeds answer for it three different
ways.** Two publishers define and transmit a `ChannelReset` there, one as a
startup handshake on both ports. What that means depends on which feed it lands
on:

- **Top-of-book** neither reserves nor defines `0x05`: its message table steps
  from `0x04` straight to `0x06`, leaving the ID unlisted. The reference
  top-of-book parser fills that silence, decoding `0x05` as `ChannelReset`
  (12 bytes, either port role, "publisher startup, drop cached state"). There
  the emission agrees with our own decoder rather than intruding on reserved
  space, and what is missing is spec coverage.
- **Perp-stats** names the ID and excludes it: type IDs `0x03`, `0x04`, `0x05`
  and `0x08` are *"intentionally not carried on this feed"*, so that a datagram
  misrouted from a sibling feed cannot cross-decode. An emission there is
  documented as excluded rather than undocumented, and remains a live finding.
  It is constructed conditionally, so whether it reaches the wire today is a
  deployment question rather than a code one.
- **Market-by-price** marks `0x05` *(reserved)*, as market-by-order and
  order-intent also do, and its parser records the slot as intentionally unused
  for that same cross-decoding reason. Its spec carries reset in the header via
  `Reset Count` instead. That is the resolved shape: what a startup handshake
  would announce is already carried normatively, so no message type is needed
  for it.

Three feeds resolving three ways is the argument for per-feed type ID space
rather than against it, and a crate-wide rule would be wrong: `dz-edge-core`
constrains nothing at `0x05` and defines no message at it. Whether
`ChannelReset` earns a documented identifier on top-of-book, and what becomes of
the perp-stats emission, are upstream questions.

### Instrumentation

Playbook Phase 6.5 declares the `dz_publisher_*` names normative and requires a
shared `dz-publisher-metrics` library. No publisher emits a `dz_publisher_*`
series. Two use their own venue prefixes by two different mechanisms; another
runs its own registry. One fleet dashboard is not currently possible.

### Subscriber side

Across the three parsers here, `timestamp_linux.go`, `sink_socket.go` and
`sink_json.go` are byte-identical copies; `sink.go` differs by two lines;
`runner.go`, `metrics.go` and `seqtracker.go` are drifted near-copies. Roughly
360 cloned lines per feed. The most recent work here, *"parsers: decode refdata
at schema v1 and v3 (#37)"*, is this side paying for the schema drift above.

---

## Architecture

Three layers, one repository, two languages. The codec layer serves both
directions: a publisher encodes with it, a subscriber decodes with it. The input
layer serves both too, since a venue handing off over its own multicast needs
the receiver a parser needs.

### Codec layer

No I/O, no async, no dependency above `thiserror`. One crate per feed spec,
because `VERSIONING.md` versions the specs independently.

| Crate | Holds |
|---|---|
| `dz-edge-core` | datagram header, message header, `DatagramBuilder` with the 1,232 clamp, shared enumerations, `Heartbeat`, `EndOfSession`, `BatchBoundary`, `InstrumentReset`, `SnapshotEnd`, `DecodeError` |
| `dz-edge-refdata` | `InstrumentDefinition`, `ManifestSummary`. Encodes schema 3, decodes 1 and 3 |
| `dz-edge-tob` | `Quote`, `Trade` |
| `dz-edge-mbp` | `LevelUpdate`, `BookClear`, `SnapshotLevel`, 40-byte `SnapshotBegin` |
| `dz-edge-mbo` | `OrderAdd`, `OrderCancel`, `OrderExecute`, `SnapshotOrder`, 36-byte `SnapshotBegin` |
| `dz-edge-perp-stats` | `PerpStats` |

`BatchBoundary` (16B), `InstrumentReset` (28B) and `SnapshotEnd` (20B) are
byte-identical across the depth feeds, so they sit in core. `SnapshotBegin` is
not: market-by-price appends `Depth Bound` and grows to 40, so each depth crate
carries its own. `dz-edge-order-intent` and `dz-edge-midpoint` follow when
needed; midpoint stays at schema 1 with its 64-byte definition.

Two rules, both from what went wrong:

**Encode one generation, decode several.** A publisher speaks one generation.
`dz-edge-refdata` encodes 3 and decodes 1 and 3. It does not decode 2: that
128-byte layout was superseded before any publisher emitted it, as this
repository's own `2026-08-08-refdata-v3-dual-version-design.md` already
concluded.

**Put invariants where configuration cannot reach them.** `DatagramBuilder::new`
clamps to `min(mtu, MAX_DATAGRAM_SIZE)`, so adopting the crate fixes the overrun
without anyone editing a deployment default.

Names follow the conformant publisher: `DatagramBuilder`, datagram header,
`MAX_DATAGRAM_SIZE`. Port roles are `PortRole { Mktdata, Refdata, Snapshot }`.
`Channel` means the `Channel ID` shard and nothing else.

### Publisher layer

| Crate | Holds |
|---|---|
| `dz-publisher-metrics` | the normative `dz_publisher_*` set, standard histogram buckets, the `/metrics` server |
| `dz-publisher-egress` | `MulticastTransmitter`, route-derived egress policy, transmitter discipline, per-channel-instance sequencer, `Reset Count` persistence, `DatagramSink` |
| `dz-publisher-refdata` | ID minting and persistence, single-writer guard, selection policy, paced definition cycle, `Manifest Seq`, `Valid` flag |
| `dz-publisher-runtime` | config composition, guards, shutdown and `EndOfSession`, the skeleton wiring the rest |

**Enforcement, not convention.** The playbook has asked for common metrics since
Phase 6.5 and has not got them, because asking is not a mechanism.

- A venue never constructs a metric. `dz-publisher-metrics` exposes typed
  handles, not names, and the crates owning the hot paths record internally. A
  publisher transmitting through `dz-publisher-egress` emits
  `dz_publisher_egress_*` whether or not anyone thought about it.
- The registry constructor applies `venue` and `source_id`. There is no path to
  a series without them.
- Venue-specific metrics go to a second registry that rejects any name starting
  `dz_publisher_`.
- Histogram buckets are defined once, so two venues' percentiles are comparable.

The same device carries the spec obligations: the clamp is in the builder, the
pacer owns the cycle, the sequencer owns `Sequence Number` and `Reset Count`.
Every defect above is a publisher re-deciding something a spec already decided.

**Egress** takes the implementation that has met production. It derives its
source IP address from the route rather than config, because the address is a
pool lease and not a host identity. It leaves `IP_MULTICAST_IF` unset for the
`ENODEV` reason above, and the crate must say so, or the deferred improvement
gets carried forward into it. It distinguishes a transmitter whose failure ends
the process from one that darkens only its own channel. Sequencing keys on the
channel instance. `Reset Count` persists per feed, so a newly enabled feed
advertises 1 rather than inheriting another feed's history. The sink boundary is
a `DatagramSink` trait, which is what makes the engine testable without a
socket.

**Reference data** takes the paced implementation and its registry. The pacer
laps at 80% of the cycle, caps datagrams per tick so a stall degrades into a
denser lap, and derives definitions-per-datagram from the datagram and message
sizes. The registry writes with atomic rename and refuses a directory it already
holds live, since two writers means the last flush wins and published IDs
resolve to nothing after a restart. Selection is the playbook default: seed top
N, cap at 2N, evict on natural end of life, warn above N, sticky admission.

**Runtime** owns the loop, config composition, signals, `EndOfSession` and the
guards. Upstream liveness is a property of the input connection and alone
justifies a restart; feed silence is a property of one channel's published set,
and a channel whose instruments are dormant is silent and healthy. Conflating
them lets one quiet channel restart every other.

### Input layer

`Input` yields payloads, receive timestamps and connection lifecycle. It is
venue-agnostic and is where every `dz_publisher_ingress_*` series is recorded.
`Adapter` maps a payload onto our messages; it is product-line-specific and
small.

One publisher already has an input trait with three implementations and
another's WebSocket client hands back a receiver of decoded events, so two
reached this boundary alone. What they do not share is the half above it, so
each rebuilt reconnection, backoff, rate limits and error classification.

`dz-ingress-core` holds the trait, reconnection and backoff, gap detection
against the upstream source's sequencing, the playbook's parse-error taxonomy
(`schema`, `unknown_field`, `malformed`, `truncated`), the connection-state
gauge, reconnect counters by trigger, and venue-timestamp handling with
`timestamp_kind`. Transports: `dz-ingress-websocket`, `-fix`, `-multicast`,
`-rest`, `-filetail`, `-uds`. The family keeps the `ingress` name to match the
normative metric family; the trait inside is `Input`, per the glossary.

---

## Configuration

Six values appear in every existing publisher. One uses the same key in all of
them: `refdata_port`. The others are spelled two or three ways each — the
matching engine identity, the multicast group, the egress interface, the market
data port and the metrics endpoint. Heartbeat interval, definition cycle and
manifest cadence appear in some and are hardcoded in others. One publisher
suffixes durations `_seconds` and takes integers; others parse duration strings.

Two are more than inconsistency. **Separate groups for market data and reference
data**: the supplement specifies *"one multicast group with two destination
ports"* and rejects a second group by name. **An operator-settable datagram
size**: spec-mandated, and the key already set wrong in production.

Each shared crate parses its own section, so keys, types and defaults cannot
drift between venues.

```toml
venue = "..."                  # the label on every dz_publisher_* series

[egress]
expected_prefix = "..."        # optional invariant on the discovered address
pin             = "..."        # optional override of route discovery
ttl             = 1

[[feed]]                       # one per feed emitted
spec            = "top-of-book"
enabled         = true
channel_id      = 0
source_id       = 0
multicast_group = "..."        # one group
mktdata_port    = 0
refdata_port    = 0
snapshot_port   = 0            # depth feeds only
heartbeat_interval = "1s"
definition_cycle   = "30s"
manifest_cadence   = "1s"
idle_guard         = "60s"

[refdata]
state_dir = "..."
[refdata.selection]
bootstrap_top_n      = 0
max_published        = 0
warn_published_above = 0

[metrics]
enabled     = true
listen_addr = "127.0.0.1:9100"

[ingress]
kind                      = "websocket"
connect_timeout           = "5s"
reconnect_backoff_initial = "500ms"
reconnect_backoff_max     = "30s"
rate_limit_per_second     = 0
```

There is no `mtu` key. `[[feed]]` is an array because a publisher may emit
several, which one already expresses as repeated blocks and another as four
differently-named sections.

### Adapter skeleton

Reconnection, backoff, timeouts, rate limits and poll intervals are transport
properties and move to `[ingress]`. What remains is the venue's.

```toml
[adapter]
kind = "..."                   # required; names the adapter implementation

[adapter.upstream]             # endpoints; keys defined by the adapter
[adapter.credentials]          # optional; paths only, never inline secrets
[adapter.replay]               # optional; fixture directory for offline runs
enabled = false
path    = "..."
```

Four rules: `kind` is required; everything venue-specific lives under
`[adapter.*]` and a top-level venue key is a load error; credentials are paths;
`[adapter.replay]` is uniform, since publishers already carry a
live-versus-fixture switch under different spellings.

Below that, `[adapter.upstream]` is free. An adapter reading a local directory,
one holding two credentialed APIs, and one reading a chain RPC plus a local
socket have nothing useful in common, and forcing a shape would move the sprawl
up a level.

`deny_unknown_fields` applies everywhere including `[adapter]`. One publisher
had a misspelled section parse cleanly, fall back to a default, and run the
wrong transport while the operator believed otherwise.

---

## Golden vectors and conformance

Hand-written codecs in two languages need a binding contract: one canonical byte
vector per message type per schema version, in `edge-feed-spec`, reproduced in
CI by Rust encode, Rust decode, Go decode, the conformance tool and the
dissectors.

This catches drift without coupling implementations. One publisher's conformance
crate transcribes layout tables by hand and refuses to depend on the encoder, so
a failure means the encoder is wrong rather than that both agree. Code
generation would kill that independence, which is the main argument against it.

Two fleet-wide assets move here, where the playbook says they belong: the
Wireshark dissectors, currently inside one venue's publisher, which the playbook
calls *"the wrong home"*, and that conformance crate, with its hand-transcription
discipline and pinned spec revision.

---

## Go parity

The codec mirrors as Go modules under `go/edge/`: `core`, `refdata`, `tob`,
`mbp`, `mbo`. The parsers drop their private decoders and cloned support files
for `go/internal/feed`. The `*-bot` binaries become `*-book-builder`.

---

## Repository layout

```
edge-multicast-ref/
  rust/
    codec/       dz-edge-core, -refdata, -tob, -mbp, -mbo, -perp-stats
    publisher/   dz-publisher-metrics, -egress, -refdata, -runtime
    ingress/     dz-ingress-core, -websocket, -fix, -multicast, -rest,
                 -filetail, -uds
    conformance/ the hand-transcribed suite, moved in from a venue repository
    receivers/   kernel-receiver, xdp-receiver (existing)
  go/
    edge/        core, refdata, tob, mbp, mbo
    internal/feed/
    *-parser/, *-book-builder/
  dissectors/    the Lua dissectors, moved in from a venue repository
```

Crates version and tag independently, matching how `edge-feed-spec` versions the
specs they implement.

---

## Migration

The one risky step is isolated. Everything before it is invisible on the wire.

| Step | Work | Wire effect |
|---|---|---|
| 1 | Codec crates and golden vectors | none |
| 2 | A publisher already on schema 3 adopts the codec | none: output byte-identical. This is the proof |
| 3 | The other schema-3 publisher adopts it | none. Exercises market-by-price and perp-stats |
| 4 | **The schema-1 publisher adopts it** | **visible: schema 1 to 3.** Needs a subscriber comms plan and a dual-publish window |
| 5 | `dz-publisher-metrics`, everywhere | none. Series renamed, dashboards re-template on `venue` |
| 6 | `dz-publisher-egress` | none. The config-bound publishers inherit route discovery |
| 7 | `dz-publisher-refdata` | none. Fixes the definition-cycle burst |
| 8 | `dz-ingress-*`, per venue as touched | none |
| 9 | `dz-publisher-runtime`; the next venue built on it | none |

Step 2 is first because a byte-diff of captured output is its acceptance test.
Step 4 is the only subscriber-visible change, scheduled after the crate is
already proven twice. Step 5 renames every series at once; doing it per venue
leaves the dashboards split for the duration.

---

## Decisions

**Publishing the egress logic here: yes.** The venue-facing page for venues
standing up their own publishers is why this is right rather than tolerable.
**The playbook needs updating when this lands:** Phase 6 should stop telling a
new venue to implement egress and reference data itself, and Phase 6.5's
instruction to factor out a shared metrics library becomes a statement that it
exists and must be used.

**Crates are consumed as tagged releases.** Each tags independently; a venue
pins a tag. Needs a release discipline before step 2.

**Venue repositories keep their own Cargo workspaces**, holding the adapter,
`main` and config, and depending on the crates by tag. The Ansible role,
Terraform and operational history already live there.

---

## Non-goals

No feed spec changes: where a venue needs a missing field, propose an additive
change upstream first, per the playbook.

No convergence of book state machines. Each follows its venue's microstructure,
and one publisher already runs two that are deliberately not converged.

The Go conformance tool stays Go.
