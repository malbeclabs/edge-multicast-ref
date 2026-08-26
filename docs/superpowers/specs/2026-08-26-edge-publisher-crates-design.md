# Shared publisher crates for DoubleZero Edge

**Status:** draft, pending review
**Applies to:** the venue publisher repositories and this repository
**Upstream authority:** [`edge-feed-spec`](https://github.com/malbeclabs/edge-feed-spec), its [`GLOSSARY.md`](https://github.com/malbeclabs/edge-feed-spec/blob/main/GLOSSARY.md) and [`VERSIONING.md`](https://github.com/malbeclabs/edge-feed-spec/blob/main/VERSIONING.md)
**Process authority:** Feed Publisher Playbook, Phases 5, 6 and 6.5

---

## A note on naming

This repository is public, and not every venue with a publisher has announced
its DoubleZero feed. This document therefore names no venue, no venue
repository, no venue-specific crate, module or configuration key, and no venue
issue tracker.

Three publishers exist today. They are called **A**, **B** and **C** here. The
mapping is recorded outside this repository, and nothing below depends on
knowing it. A fourth venue is in early phases and is called **the next venue**.

Where evidence comes from source comments, the quotation is trimmed to the part
that carries the argument, and identifying detail is removed rather than
paraphrased into vagueness. Where a metric prefix, path or configuration key
would identify a venue, it is described by its role instead.

The same rule applies to the implementation plan that follows this document, and
to anything else committed to this repository on this subject.

---

## Purpose

Three publishers now exist. Each was written on its own, and each solved the
same problems in its own way. This document defines a set of crates that hold
those solutions once, so a fourth publisher inherits them instead of
rediscovering them.

The goal is not code reuse for its own sake. It is that a publisher should have
almost no semantics of its own. What differs between venues is the upstream
source and the mapping onto our messages. Everything else the specs already
decide, and every place a publisher gets to decide it again is a place one of
them has already decided it wrong.

---

## Vocabulary

`GLOSSARY.md` in `edge-feed-spec` governs this document and every crate it
defines. Its precedence rule is absolute: a definition there overrides any local
one. This section does not restate the glossary. It records the terms this
design pins down, and the conflicts an audit of the three publishers found.

### Terms this design adds

The glossary does not name these, and they are used consistently below.

| Term | Meaning |
|---|---|
| **Codec crate** | A crate holding one feed spec's wire layout. Encoders and decoders, no I/O, no async. |
| **Input** | The transport a publisher reads its upstream source through: WebSocket, FIX, multicast, REST poll, file tail, Unix socket. Venue-agnostic. |
| **Adapter** | The product-line-specific code mapping an upstream payload onto our messages. The only part a new venue writes from scratch. |

`Input` and `Adapter` are chosen against the glossary rather than by taste.
Bare `source` is banned, and the glossary says an input is an `input`. `Adapter`
is the glossary's own replacement for a Rust trait spanning product lines.

### Conflicts the audit found

An audit of first-party code across the three publishers and this repository
found the following. The glossary was published on 2026-08-20 and the feed specs
were conformed to it. No publisher was.

Counts are matching lines in first-party code, excluding build artifacts and
vendored dependencies.

| Banned term | A | B | C | this repo |
|---|---|---|---|---|
| `frame`, for our own traffic | 827 | 1339 | 0 | 489 |
| `lane` | 0 | 1131 | 0 | 0 |
| `epoch` | 97 | 18 | 0 | 7 |
| `bot` | 0 | 0 | 0 | 92 |

Some of this repository's `frame` uses are correct. The XDP receiver and the GRE
decapsulator handle real layer-2 Ethernet frames, which is the word's proper
sense. Every use describing our own UDP payload is a violation.

Three conflicts matter enough to name:

**One publisher declares an enumeration named `Channel` whose variants are
market data, reference data and snapshot.** Those are port roles. The same crate
separately handles `Channel ID`, which is the shard. One word, two meanings, in
one crate. The glossary bans this directly.

**This repository names three binaries `*-bot`.** The glossary reserves `bot`
for a real automated trading client and says we ship none. They are
book-builders.

**One publisher's codec decodes a Schema Version 2 that never existed on the
wire.** It carries constants for a 128-byte `InstrumentDefinition` and a
three-element table of accepted versions. This repository's own
`2026-08-08-refdata-v3-dual-version-design.md` reached the opposite conclusion
and stated it plainly: the 128-byte layout was superseded before any publisher
emitted it, and the accepted-version check was *"deliberately built so it cannot
be mistaken for one"*. This repository is right. The shared codec accepts 1 and
3.

**Publisher C is at zero across every banned term.** It is the only
implementation already conformant. Where this design must choose a name, it
takes C's. Where it must choose a behavior, it usually takes B's. Those are
different questions and this document answers them separately.

---

## What is wrong today

Every item below was found by reading the three publishers against each other.
Each is a case of one publisher having learned something the others have not.

### The wire codec has been forked twice by copy-paste

C's codec crate records its own origin in a header comment: it was ported from
A's protocol module, *"which is still on schema 1."* C then moved to Schema
Version 3 on its own. B moved to 3 on its own, separately. Neither reused the
other's work, and A still publishes 1.

Two independent migrations of one 130-byte message is the whole problem in a
sentence.

### One publisher exceeds the mandated datagram size

Every feed spec mandates 1,232 bytes, sized to leave room for the GRE
encapsulation DoubleZero uses for last-mile delivery.

| | Schema Version | Datagram size handling |
|---|---|---|
| A | 1 | a default of 1448, shipped as an Ansible default to production; the builder does not clamp |
| C | 3 | inherited the same 1448 default and A's incorrect comment, but its builder clamps capacity to `min(mtu, MAX_DATAGRAM_SIZE)`, with a test asserting it |
| B | 3 | a single constant at 1232 |

A's top-of-book feed can emit datagrams 216 bytes over the cap. Its
market-by-order and order-intent feeds are correct at 1,232, because the
constant lives in three places in one repository and only two of them were
fixed.

C is safe for a reason worth copying. It put the limit in the builder rather
than in configuration, so no operator and no Ansible default can overrun it.

### Two publishers reached opposite conclusions on multicast egress

C's transport module records a deferred improvement: *"Hosts that must pin
multicast egress to a specific interface independent of source IP address need
`socket2`'s `set_multicast_if_v4`; that is deliberately deferred to keep deps
minimal."*

B's transport module records why that call is wrong here: `IP_MULTICAST_IF`
*"stays unset: the kernel resolves it to an interface index at `setsockopt` time
and `doublezerod` recreates [the tunnel interface] with a new index on every
re-provision, which left the socket returning `ENODEV` forever."*

C's roadmap is B's outage. Neither repository can see the other.

B also survived something A and C have not met. DoubleZero moved a host's tunnel
address without notice, the configured address stopped existing, and the service
crash-looped 31,108 times over two days. B now derives its source IP address
from the route `doublezerod` installs for the group. The other two take it from
configuration and will meet the same failure.

### One publisher violates a reference-data MUST NOT, and says so

`reference-data/spec.md` publisher rule 2: *"Definitions SHOULD be paced evenly
over the cycle period. Publishers MUST NOT emit the entire published set as a
single burst."*

C's refdata module: *"the emission is a synchronized burst (deliberately simple
— the universe is tiny)."*

B's refdata module exists to satisfy that rule and works the arithmetic out: a
lap at 80% of the cycle period, because the period is a maximum on the interval
between retransmissions of any single definition rather than a lap target, and a
lap sized at exactly the period violates rule 1 under ordinary timer jitter.

### The mandated metrics library does not exist

Playbook Phase 6.5 declares the `dz_publisher_*` names normative and says to
*"factor the required set into a shared `dz-publisher-metrics` library so a new
publisher inherits all of it. A per-venue reimplementation is how the names
drift apart, and the names are the only reason a shared dashboard works."*

No publisher emits a `dz_publisher_*` series. Two emit families under their own
venue prefixes, by two different mechanisms, and the third runs a registry of
its own. One dashboard across the fleet is not currently possible.

### The subscriber side has the same disease

Across the three parsers in this repository, `timestamp_linux.go`,
`sink_socket.go` and `sink_json.go` are byte-identical copies. `sink.go` differs
by two lines. `runner.go`, `metrics.go` and `seqtracker.go` are near-copies that
have drifted apart. That is roughly 360 cloned lines per feed, and a fourth feed
clones them again.

This repository's most recent work, *"parsers: decode refdata at schema v1 and
v3 (#37)"*, is the subscriber side paying for A's schema drift.

---

## Architecture

Three layers, in one repository, in two languages.

```
                    ┌──────────────────────────────┐
                    │       venue repository       │
                    │   adapter + main + config    │
                    └───────────────┬──────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        │                           │                           │
┌───────▼────────┐        ┌─────────▼─────────┐       ┌─────────▼────────┐
│  input layer   │        │  publisher layer  │       │   codec layer    │
│  dz-ingress-*  │───────▶│  dz-publisher-*   │──────▶│   dz-edge-*      │
│  transports    │        │  runtime skeleton │       │  wire layouts    │
└────────────────┘        └───────────────────┘       └──────────────────┘
        │                                                       │
        └───────────────────────┬───────────────────────────────┘
                                │
                    ┌───────────▼────────────┐
                    │  subscriber side       │
                    │  parsers, book-builders│
                    │  Go and Rust           │
                    └────────────────────────┘
```

The codec layer serves both directions. A publisher encodes with it and a
subscriber decodes with it. The input layer serves both too: a venue that hands
off over its own multicast needs the same receiver a parser needs.

### Crate inventory

**Codec layer.** No I/O, no async, no dependency above `thiserror`. One crate
per feed spec, because `VERSIONING.md` versions the specs independently and the
crates must be able to follow.

| Crate | Holds |
|---|---|
| `dz-edge-core` | 24-byte datagram header, 4-byte message header, `DatagramBuilder` with the 1,232 clamp, shared enumerations, `Heartbeat`, `ChannelReset`, `EndOfSession`, `BatchBoundary`, `InstrumentReset`, `SnapshotEnd`, `DecodeError` |
| `dz-edge-refdata` | `InstrumentDefinition`, `ManifestSummary`. Encodes Schema Version 3 only. Decodes 1 and 3 |
| `dz-edge-tob` | `Quote`, `Trade` |
| `dz-edge-mbp` | `LevelUpdate`, `BookClear`, `SnapshotLevel`, the 40-byte `SnapshotBegin` |
| `dz-edge-mbo` | `OrderAdd`, `OrderCancel`, `OrderExecute`, `SnapshotOrder`, the 36-byte `SnapshotBegin` |
| `dz-edge-perp-stats` | `PerpStats` |

`BatchBoundary` at 16 bytes and `InstrumentReset` at 28 are byte-identical
across market-by-order and market-by-price, so they belong in core. `SnapshotEnd`
at 20 bytes likewise. `SnapshotBegin` is not identical. Market-by-price appends
`Depth Bound` at offset 36 and the message grows to 40, so each depth feed
carries its own.

`dz-edge-order-intent` and `dz-edge-midpoint` follow the same rule when a
publisher needs them. Midpoint stays at Schema Version 1 with its 64-byte
definition, as upstream specifies.

**Publisher layer.** Four crates, each with one job.

| Crate | Holds |
|---|---|
| `dz-publisher-metrics` | The normative `dz_publisher_*` set, the standard histogram buckets, the `/metrics` server |
| `dz-publisher-egress` | `MulticastTransmitter`, route-derived egress policy, transmitter discipline, the per-channel-instance sequencer, `Reset Count` persistence, the `DatagramSink` trait |
| `dz-publisher-refdata` | Instrument ID minting and persistence, the single-writer guard, the selection policy, the paced definition cycle, `Manifest Seq`, the `Valid` flag |
| `dz-publisher-runtime` | Configuration composition, the guards, shutdown and `EndOfSession`, and the skeleton wiring the rest |

Configuration is not a crate of its own. Each crate defines its own
`serde` section and `dz-publisher-runtime` composes them, so a crate's
configuration cannot drift from the crate that reads it.

**Input layer.** `dz-ingress-core` holds the `Input` trait, reconnection and
backoff, gap detection against the upstream source's own sequencing, and the
parse-error taxonomy the playbook fixes at `schema`, `unknown_field`,
`malformed` and `truncated`.

The transports are `dz-ingress-websocket`, `-fix`, `-multicast`, `-rest`,
`-filetail` and `-uds`. Each is a transport the existing publishers already
speak, and each is written once here rather than once per venue.

The crate family keeps the `ingress` name to match the normative
`dz_publisher_ingress_*` metric family, which is already a published dashboard
contract. The trait inside is `Input`, per the glossary.

---

## The codec layer

Two rules govern it, and both come from what went wrong.

**Encode one generation, decode several.** A publisher speaks one generation.
There is no reader asking it to downgrade, and emitting a mixture would make the
version byte meaningless. So `dz-edge-refdata` encodes Schema Version 3 and
nothing else. It decodes 1 and 3, because a subscriber meets both while one
publisher is still on 1, and because a staged rollout is the one moment a feed
is guaranteed to be mixed.

It does not decode 2. No publisher ever emitted the 128-byte layout.

**Put the invariant where configuration cannot reach it.** `DatagramBuilder::new`
clamps capacity to `min(mtu, MAX_DATAGRAM_SIZE)`. C already does this and holds
a test asserting it. Adopting the crate therefore fixes A's overrun without
anyone editing an Ansible default, which is the point: the fix survives the next
operator who does not know about it.

### Naming

C's vocabulary wins throughout: `DatagramBuilder`, not `FrameBuilder`. The
datagram header, not the frame header. `MAX_DATAGRAM_SIZE`, not
`MAX_FRAME_SIZE`. B's crate is the better implementation and C's is the better
naming, so the shared crate takes B's byte handling under C's names.

Port roles are `PortRole { Mktdata, Refdata, Snapshot }`, using the glossary's
three tokens verbatim. `Channel` names the `Channel ID` shard and nothing else.

---

## The publisher layer

### Enforcement, not convention

This is the part that does the work. The playbook has asked for common metrics
since Phase 6.5 was written and has not got them, because asking is not a
mechanism. These crates make the contract structural.

**A venue never constructs a metric.** `dz-publisher-metrics` exposes typed
handles rather than names. `IngressMetrics::message(kind)`,
`EgressMetrics::datagram(port_role)`. The crates owning the hot paths take those
handles at construction and record inside. A publisher that transmits through
`dz-publisher-egress` emits `dz_publisher_egress_*` whether or not anyone
thought about it.

**Required labels cannot be omitted.** The registry constructor applies `venue`
and `source_id`. There is no path to a series without them.

**Venue-specific metrics are quarantined.** They go to a second registry that
rejects any name beginning with `dz_publisher_`. A venue may add anything it
likes under its own prefix and may not shadow the shared contract.

**Histogram buckets are defined once.** The playbook requires this and gives the
reason: buckets chosen per publisher make two venues' percentiles incomparable
even when both are correct.

The same device carries the spec obligations. The clamp lives in the builder, so
no configuration can overrun the datagram size. The pacer owns the definition
cycle, so no publisher can burst it. The sequencer owns `Sequence Number` and
`Reset Count`, so no publisher can forget to advance the era across a restart.

Every defect in this document is a publisher re-deciding something a spec had
already decided. The design's job is to remove the opportunity.

### Egress

`dz-publisher-egress` takes B's implementation, which is the only one that has
met production.

It derives its source IP address from the route `doublezerod` installs for the
group rather than reading it from configuration, because the address is a pool
lease and not a host identity. It leaves `IP_MULTICAST_IF` unset, for the
`ENODEV` reason recorded above, and this must be stated in the crate so the
deferred improvement in C's transport module is not carried forward into it. It
distinguishes a transmitter whose failure should end the process from one whose
failure should darken only its own channel, because a publisher serving many
channels must not restart them all for one.

Sequencing keys on the channel instance, `(source IP address, Channel ID,
destination port)`, as the glossary requires. `Reset Count` persists per feed
rather than per process. A feed enabled for the first time on a host that has
published another feed for months must advertise 1, not inherit another feed's
history.

The sink boundary is C's `DatagramSink` trait, which is what makes the engine
testable without a socket.

### Reference data

`dz-publisher-refdata` takes B's pacer and its instrument registry.

The pacer laps at 80% of the configured cycle period, caps the datagrams one
tick may emit so that a stall degrades into a denser lap rather than a burst,
and derives definitions-per-datagram from the datagram size and the message size
so that changing either moves it. Adopting the crate fixes C's rule 2 violation.

The registry mints and persists instrument IDs with atomic rename, and refuses a
directory the process already holds a live registry for. Two writers to one file
means the last flush wins, every ID the loser minted disappears, and IDs already
published on the wire resolve to nothing after a restart.

The selection policy is the playbook's default: seed the top N at start, cap
growth at 2N, evict only on natural end of life, warn when the published set
exceeds N. Sticky admission matters because a published set withdrawn on a
refresh is a subscriber-visible fault.

### Runtime

`dz-publisher-runtime` owns the loop and the wiring: configuration composition,
signal handling, `EndOfSession` on shutdown, the `/metrics` server, and the
guards.

The guards take B's distinction, which it reached the hard way. Upstream
liveness is a property of the input connection and is the only thing that
justifies restarting the process. Feed silence is a property of one channel's
published set, and a channel whose instruments are dormant is silent and
healthy. Conflating them means any one quiet channel restarts every other
channel in the process.

---

## The input layer

`Input` yields payloads, receive timestamps and connection lifecycle events. It
is venue-agnostic, and it is where every `dz_publisher_ingress_*` series is
recorded.

`Adapter` maps a payload onto our messages. It is product-line-specific, and it
is small.

This split is not invented here. C already has an input trait with three
implementations, and B's WebSocket client already hands back a receiver of
decoded events. Two of the three arrived at the same boundary on their own. What
they do not share is the half above the boundary, so each rebuilt reconnection,
backoff, rate-limit handling and error classification.

`dz-ingress-core` also carries what the playbook requires of every ingest path:
the connection-state gauge, reconnection counters tagged by trigger, the
rate-limit counter, and venue-timestamp handling with `timestamp_kind`
distinguishing `exchange_recv`, `matching_engine`, `gateway_send` and
`block_time`. A venue exposing no timestamps sets
`dz_publisher_venue_timestamps_available` to 0 and fabricates nothing.

---

## Configuration

### What the three configs share today, and under how many names

The three publishers configure the same publisher. They do not spell it the same
way. Six values appear in all three; only one of the six uses the same key in all
three.

| Concept | A | B | C |
|---|---|---|---|
| Matching engine identity | `tob_source_id`, `source_id` | `source_id` | `source_id` |
| Multicast group | `group_addr` | `multicast_group` | `mktdata_group` **and** `refdata_group` |
| Egress interface | `bind_addr` | `multicast_interface_ip` | `interface` |
| Market data port | `port`, `mktdata_port` | `mktdata_port` | `mktdata_port` |
| Reference data port | `refdata_port` | `refdata_port` | `refdata_port` |
| Metrics endpoint | `metrics_address` + `metrics_port` | `listen_addr` | `metrics_addr` |

Three more appear in two of the three and are hardcoded in the third: the
heartbeat interval, the definition cycle and the manifest cadence. Two of the
three make the datagram size an operator-settable key.

The units diverge too. One publisher suffixes durations with `_seconds` and takes
integers; the other two parse duration strings. So the same concept is a
different key with a different type depending on which host an operator is
looking at.

Two of these are not merely inconsistent:

**One publisher takes separate groups for market data and reference data.** The
reference-data supplement specifies *"one multicast group with two destination
ports"* and rejects the alternative explicitly: splitting into a separate group
*"provides no NIC-filter benefit worth the operational cost of provisioning,
IGMP-joining, and managing a second group per channel."* A configuration surface
that accepts two groups permits a deviation the supplement argued against.

**Two publishers let an operator set the datagram size.** It is spec-mandated at
1,232 and is the key that is already set wrong in production. It stops being
configuration.

### The common sections

Each section below is parsed by the shared crate that reads it, so the keys, the
types and the defaults are the same at every venue by construction. A venue
cannot rename a key, change a default, or add one.

```toml
venue = "..."                  # the label on every dz_publisher_* series

[egress]                       # dz-publisher-egress
expected_prefix = "..."        # optional invariant the discovered address must satisfy
pin             = "..."        # optional override of route discovery
ttl             = 1

[[feed]]                       # one per feed this publisher emits
spec            = "top-of-book"   # top-of-book | market-by-price | market-by-order | perp-stats
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

[refdata]                      # dz-publisher-refdata
state_dir = "..."
[refdata.selection]
bootstrap_top_n      = 0
max_published        = 0
warn_published_above = 0

[metrics]                      # dz-publisher-metrics
enabled     = true
listen_addr = "127.0.0.1:9100"

[ingress]                      # dz-ingress-core
kind                      = "websocket"
connect_timeout           = "5s"
reconnect_backoff_initial = "500ms"
reconnect_backoff_max     = "30s"
rate_limit_per_second     = 0
```

There is no `mtu` key. The datagram size is mandated by the specs and clamped in
the builder, so there is nothing for an operator to set and no way to set it
wrong.

`[[feed]]` is an array because a publisher may emit several feeds, which is what
one publisher's repeated per-channel blocks already express and what another
expresses as four differently-named sections. The `spec` key names the feed spec
and selects the codec crate.

### The adapter skeleton

Most of what sits in a venue block today is not venue-specific. Reconnection,
backoff, connect timeouts, rate limits and poll intervals are properties of the
transport, and they move to `[ingress]`. What is left is genuinely the venue's,
and the skeleton constrains its shape without constraining its content.

```toml
[adapter]
kind = "..."                   # required; names the adapter implementation

[adapter.upstream]             # endpoints; keys defined by the adapter
[adapter.credentials]          # optional; paths only, never inline secrets
[adapter.replay]               # optional; fixture directory for offline runs
enabled = false
path    = "..."
```

Four rules, and nothing else:

1. `kind` is required and names the adapter, so the runtime can select it and the
   `venue` metric label can be cross-checked against it.
2. Everything venue-specific lives under `[adapter.*]`. A venue key at the top
   level is a load error.
3. Credentials are paths. A secret never appears inline in a rendered config.
4. `[adapter.replay]` is uniform. Two of the three publishers already carry a
   live-versus-fixture switch under three different spellings, and a common one
   is what lets an offline conformance run be described the same way everywhere.

Beyond those, `[adapter.upstream]` is free. An adapter reading a local node's
directory, one holding two REST and WebSocket credentials, and one reading a
chain RPC plus a local socket have nothing useful in common below that level,
and inventing a shape they must share would be the config sprawl this is meant
to prevent, moved up a level.

`deny_unknown_fields` applies at every level, including inside `[adapter]`. One
publisher had a misspelled section parse cleanly, fall back to a default, and run
the wrong transport while the operator believed otherwise. A typo in a transport
selection must fail at load rather than publish from the wrong one.

---

## Golden vectors and conformance

Hand-written codecs in two languages need something binding them together. A
canonical set of byte vectors, one per message type per schema version, lives in
`edge-feed-spec` and every implementation must reproduce it in CI: Rust encode,
Rust decode, Go decode, the Go conformance tool, and the Wireshark dissectors.

This catches drift without coupling implementations, which preserves a property
worth keeping. One publisher's conformance crate transcribes the layout tables
by hand from the specification and refuses to depend on the encoder, so a
conformance failure means the encoder is wrong rather than that both agree. That
independence dies if everything is generated from one table, which is the main
argument against code generation here.

Two fleet-wide assets move into this repository, where the playbook already says
they belong. The Wireshark dissectors currently live inside one venue's
publisher; the playbook calls that *"the wrong home"* because a fleet-wide
verification asset inside one venue's repository is discoverable only by someone
who already knows it exists. That conformance crate joins them, and its
hand-transcription discipline and its pinned specification revision come with
it.

---

## Go parity

The codec layer mirrors as Go modules under `go/edge/`: `core`, `refdata`,
`tob`, `mbp`, `mbo`. The three parsers drop their private wire decoders and
their cloned support files in favor of `go/internal/feed`, which absorbs the
sink implementations, the sequence tracker, the runner and the timestamping.

The `*-bot` binaries are renamed to `*-book-builder`. They are book-builders,
the glossary is explicit that we ship no bots, and the rename is cheap now and
expensive after the crates take a dependency on the names.

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
specifications they implement.

---

## Migration

Ordered so that the one risky step is isolated rather than spread across the
program. Every step before it is invisible on the wire.

| Step | Work | Wire effect |
|---|---|---|
| 1 | Land the codec crates and the golden vectors | none |
| 2 | C adopts the codec | none: it is already Schema Version 3, so output should be byte-identical, which makes it the proof |
| 3 | B adopts the codec | none: also 3. Exercises market-by-price and perp-stats |
| 4 | **A adopts the codec** | **visible: Schema Version 1 to 3.** Needs a subscriber communication plan and a dual-publish window |
| 5 | `dz-publisher-metrics`, all three publishers | none on the feed. Series are renamed and dashboards re-template on `venue` |
| 6 | `dz-publisher-egress` | none. A and C inherit route-derived egress |
| 7 | `dz-publisher-refdata` | none. Fixes C's rule 2 violation |
| 8 | `dz-ingress-*`, per venue as it is touched | none |
| 9 | `dz-publisher-runtime`; the next venue is built on it | none |

Step 2 is deliberately first among the adoptions. C should produce identical
bytes before and after, so a byte-diff of captured output is the acceptance
test, and the crate is proven before anything harder depends on it.

Step 4 is the only subscriber-visible change in the program. It is scheduled
after two publishers have proven the crate and before the runtime work, so it
does not compete with anything else for attention.

Step 5 renames every series in the fleet at once. That is the point. Doing it
per venue leaves the dashboards split for the duration.

---

## Decisions

**Publishing the egress logic: yes.** The publisher layer lives here, and the
DoubleZero egress design is published with it: the route-derived source IP
address, the `IP_MULTICAST_IF` finding, the transmitter discipline. The
venue-facing page for venues standing up their own publishers is the reason this
is right rather than merely tolerable, since those venues need exactly these
crates.

The playbook must be updated when this work lands. Phase 6 currently tells a new
venue to implement egress, reference data and the definition cycle itself, and
after this it should tell them to take the crates. Phase 6.5's instruction to
"factor the required set into a shared `dz-publisher-metrics` library" becomes a
statement that the library exists and must be used.

**Crate consumption: tagged releases.** Path dependencies do not cross
repositories. Each crate tags independently, matching how `edge-feed-spec`
versions the specifications they implement, and a venue pins a tag. This needs a
release discipline defined before step 2, because step 2 is the first
cross-repository consumption.

**The publisher labels map to venues in each venue's own private repository.**
Not here, and not in one shared index. A venue repository records which label it
is; this repository records nothing.

---

## Open questions

**Whether the venue repositories keep their own Cargo workspaces.** This design
leaves each venue with a workspace holding its adapter, its `main` and its
configuration, depending on the shared crates by tag. The alternative moves the
binaries here. The venue repositories hold the Ansible roles, the Terraform and
the operational history, so splitting a publisher across two repositories has a
cost that has not been weighed.

---

## Non-goals

Nothing here changes a feed spec. Where a venue needs a field the specification
lacks, the playbook's rule still applies: propose an additive change upstream,
get it accepted, then implement.

Nothing here converges the book state machines. Each venue's book follows its
venue's microstructure, and one publisher already runs two that are deliberately
not converged. That is a separate question with a separate answer.

Nothing here changes the conformance tool's language. The Go tool stays Go.
