# The venue adapter interface

**Status:** draft, pending review
**Applies to:** venue publisher repositories, `rust/publisher/`, `rust/ingress/`, `rust/recorder/`
**Authority:** [`edge-feed-spec`](https://github.com/malbeclabs/edge-feed-spec), its [`GLOSSARY.md`](https://github.com/malbeclabs/edge-feed-spec/blob/main/GLOSSARY.md) and [`VERSIONING.md`](https://github.com/malbeclabs/edge-feed-spec/blob/main/VERSIONING.md)
**Builds on:** [2026-08-26-edge-publisher-crates-design.md](2026-08-26-edge-publisher-crates-design.md), [2026-08-28-edge-recorder-crates-design.md](2026-08-28-edge-recorder-crates-design.md)

---

## Naming

This repository is public. This document names no venue, venue repository,
venue crate, config key, metric prefix or issue tracker, and gives no count of
publishers. `GLOSSARY.md` governs all vocabulary.

---

## Purpose

The publisher crates design named the boundary in one sentence — *"`Adapter`
maps a payload onto our messages; it is product-line-specific and small"* — and
left it undefined. This defines it: the exact trait a venue repository
implements, what it may and may not decide, how a binary is composed from it,
and why the same trait makes the recorder able to answer *did the publisher
publish what the venue said?*

The design constraint is one sentence long. **A venue implements its upstream
transport and its own microstructure, and decides nothing that a specification
already decided.** Every defect the publisher audit found was a publisher
re-deciding something: the datagram cap, the source address, the definition
cycle, the schema version. An interface that lets a venue supply an
`Instrument ID`, a scaled integer, a flags byte or an `Action` is an interface
that will be supplied a wrong one.

---

## What the existing publishers already built

Both were read before this was written, and neither is a blank page. Each has
already built a version of every piece below — separately, in its own
vocabulary, and to a different depth. That is the evidence for the shape here,
and in two places it overrules what a first-principles design would have chosen.

### Both have a normalized event type, and both put it in the same place

One publisher has an explicit one: *"Normalized, pre-encoding publishable
events. Produced by a `FeedTransport` (WS or FIX) and consumed by [the engine],
which does registry/scale lookup and protocol encoding. Prices and sizes stay as
the venue's decimal strings here."* That is this design's `Event`, its
`Scalar::Text`, and its division of labour, arrived at independently. The other
publisher's equivalent is its normalization module over node data, reached from
a different upstream shape entirely.

### One already has the venue seam, with associated types

The same publisher carries two traits — one per feed — that separate an engine
from a venue: associated types for the venue's event, book, reference-data
record and per-instrument scale, and methods for classification, reference data
and encoding. Its own module header states the rule this design is built on:
**"the engine owns everything a subscriber's state machine depends on, the venue
owns only what the wire cannot tell apart."** The reason it gives is the reason
to generalise it: both bugs of its first depth implementation *"landed in the
engine core"*, and with two copies the next such bug is fixed once and shipped
once. That argument does not stop at the repository boundary.

Where this design names a thing that seam already names, it takes that name. It
diverges in exactly one place, and the divergence is the next two findings.

### Finding: a venue that encodes writes a flags byte, and two of them read it differently

That seam's venue methods return *wire messages* — the venue is handed
`instrument_id`, `source_id` and its scale, and hands back an encoded `Quote`.
The engine has already taken the sequence number, the `Action`, and the id
minting, so almost nothing is left. Almost: one of those encoders sets

```
update_flags: QUOTE_UPDATE_BID | QUOTE_UPDATE_ASK
```

unconditionally, on every quote, on a live feed.

**That constant is not currently producing a wrong byte, and the reason it is
not is the finding.** Its book's top accessor returns `None` unless both sides
exist, and its caller turns that into a missing-field error, so a one-sided book
never reaches the encoder. What makes the constant safe is therefore a property
of a different function, two layers away, that nothing states and nothing tests.
The next feed to reuse that encoder over a book that can be one-sided ships the
wrong byte, and no round-trip test sees it because the encoder and the decoder
agree.

**And the wire convention itself is settled only by convention.** Both
publishers derive the byte the same way, independently: the *updated* bit and
the *gone* bit of a side are mutually exclusive, so the bit says the side is
**present**, not that it moved. One states that as a normative table in its own
encoder and pins the absent side's zeros byte for byte; the other reaches the
same four values from whether each side of a truncated book is empty. The feed
spec fixes the four bit positions and stops. Two agreeing implementations and a
silent specification is exactly the state in which a third implementation
invents a third convention.

`Update Flags` is derivable and nothing else is derived from it: the publisher
knows which sides its event carried. So the last step — normalized event to wire
message — moves out of the venue too, and `SideUpdate` above is what makes the
derivation the only way to reach the byte. Since the byte carries presence,
`SideUpdate` has two cases and not three: there is no way to say *unchanged*,
and a quote that would restate the top unchanged is not an event — both
publishers already suppress it, in the layer that holds the book.

### Finding: two scaling paths in one publisher, and the exact one is unused

The other publisher holds both conversions from a venue decimal string to
fixed-point:

| | Path | Status |
|---|---|---|
| `f64` parse, multiply, `.round()` | reference-data and market-data helpers | on the live path |
| exact-or-drop, string arithmetic, no `f64`, refuses what it cannot represent exactly | a separate normalization module | carries `#![allow(dead_code)]` — *"until that wiring exists they have no non-test caller within the crate"* |

The correct implementation was written, reasoned about at length, and is not the
one running. `dz_edge_core::fixed_point::parse_signed` is a third
implementation of the same function, in this repository, already exact.

**What that costs on the wire, precisely.** The live path takes the rounding
conversion's failure as `.unwrap_or(0)`, and the same publisher's quote sets a
side's *updated* bit whenever it has a level for that side. So a value it cannot
convert is published as a price of zero **with the side flagged as present** — a
real-looking quote at nothing, in range, indistinguishable at a subscriber from
a genuine bid at zero. This is the strongest single argument in this document
for the boundary being where it is.

Scaling belongs behind the interface for the same reason the datagram cap
belongs in the builder: not because a venue would choose wrongly, but because
the choice keeps being available.

### Finding: pre-scaled integers must stay expressible

Against the above, the same seam carries a second quote shape deliberately, and
its reasoning is right: a top folded out of the venue's own book is already
integers, *those integers are the book's own keys*, and rendering them back to
decimal strings for the interface to re-parse would be **"a second scaling that
could drift"** — which would break the hash join it runs against a sibling
capture. A `Scalar` accepting only text would force exactly that round-trip.

Hence two variants and not one. `Scalar::Fixed { mantissa, exponent }` takes the
venue's integers *with the exponent they are at* and rescales exactly. Its
publisher's problem was the round-trip through a string, and its solution — a
second event variant carrying pre-scaled integers all the way to a second
encoder — is what this collapses: one variant, one lowering, both inputs.

### Both already run comparators and fixture replays, and neither can leave the process

Four pieces of the recorder section below already exist, twice, as test
infrastructure:

| Built | By | Reaches |
|---|---|---|
| a transport comparator matching the same market event across two upstreams, windowed, *"diagnostic only … never touches the publish path"* | one publisher | its own two upstreams, live, in-process, into logs |
| committed capture fixtures plus a two-binary replay diff, guarded by a rule that a change touching the engine and the fixtures together *"has deleted its own control"* | one publisher | a refactor, before merge |
| fixture replay-and-capture shared between golden parity tests and a conformance driver | the other publisher | its own test suite |
| a reference subscriber decoding the publisher's own emitted datagrams off loopback and rebuilding a book, asserted against the publisher's other feed | the other publisher | one process, synthetic events |

Everything the comparison modes below need has been built. What none of them can
do is run against production traffic, at a subscriber, over an archive — because
each is welded to one publisher's test harness. Modes A to C are those four
things pointed at the recorder instead.

Two smaller notes, both load-bearing:

- **The two publishers' upstreams have nothing in common.** One connects out to
  a websocket and a FIX session; the other tails a local node's output
  directory and a mempool. A trait assuming a connection, a subscription, or a
  reconnect would fit one and not the other. This is why `Input` and `Adapter`
  are separate, and why `Adapter` names no transport.
- **`Trade` must be byte-identical across a venue's two feeds**, per the wire
  spec's cross-spec policy for `0x04`. Today that is a doc comment on two
  separate encoder implementations in one publisher, holding them to each other
  by hand. One lowering makes it structural.

---

## Where the boundary goes

Three places it could sit, and only one is defensible.

| Boundary | The venue hands back | Verdict |
|---|---|---|
| **Datagrams** | encoded bytes | **No.** This is what exists today, and it is how the codec was forked twice by copy-paste, how one publisher shipped a 1448-byte cap to production, and how one publisher is still on schema 1. A byte-level boundary re-opens every one of them. |
| **Wire messages** | `Quote`, `LevelUpdate`, … | **No, and this is the one an existing publisher chose.** Its engine hands the venue the id, the source and the scale so that almost nothing is left to get wrong — and the venue still writes `Update Flags` as a constant, on a live feed. A boundary at the wire type keeps a byte the venue does not need to author. |
| **Normalized venue events** | *"this instrument's bid is now 1.23 for 400, the venue stamped it at T"* | **Yes.** Everything the venue actually knows, and nothing it does not. |

The wire types stay the *target* of the boundary rather than the boundary
itself: the runtime lowers a normalized event onto `dz-edge-tob` and
`dz-edge-mbp`/`-mbo` types, and the lowering is one implementation for every
venue.

### What that moves out of the venue

Each row is a defect class, not a convenience.

| Concern | Owner | Why not the venue |
|---|---|---|
| `Instrument ID` minting, persistence, `Manifest Seq` | `dz-publisher-refdata` | IDs must survive a restart and resolve to a published definition; two writers means published IDs resolve to nothing |
| Decimal → fixed-point at the instrument's exponent | runtime, via `dz_edge_core::fixed_point` | the conversion has three distinct failure modes and each is a different operator action; a venue doing it inline reports none of them |
| `Update Flags` on `Quote` | runtime | derived from which sides the event carries. Not hypothetical: a venue encoder writes both bits unconditionally today |
| `Action` on `LevelUpdate` | runtime | the known-shipped bug: a publisher numbering the table from `New` emits every removal as `Change` carrying zero. Self-consistent, invisible to round-trip tests, wrong for every consumer reading `Action` |
| `Sequence Number`, `Reset Count`, `Channel ID` | `dz-publisher-egress` | per channel instance, persisted per feed across restarts |
| `Per-Instrument Seq` | runtime | it is a publisher-side counter, and it is the join key the recorder needs (below) |
| datagram framing, the 1,232 cap, port roles | `dz-edge-core` | mandated; already enforced in `DatagramBuilder::new` |
| heartbeats, `EndOfSession`, definition cycle pacing, manifest cadence | `dz-publisher-runtime`, `-refdata` | spec-timed, and one publisher already bursts the cycle a rule forbids |
| every `dz_publisher_*` series | the crates owning the paths | asking for common metrics has not produced them; the venue must not be able to omit one |

### What stays in the venue

The upstream protocol, and the book state machine. The publisher crates design
already ruled the second a non-goal — *"No convergence of book state machines.
Each follows its venue's microstructure"* — and that decides the event grain:
the adapter emits **resolved** messages, not raw venue deltas. An adapter for a
venue quoting absolute depth emits levels; one quoting deltas keeps its own book
and emits the resulting absolute level. `LevelUpdate` carries the aggregate
resting quantity *after* the change, and only the adapter knows how its upstream
gets there.

---

## The traits

Two traits, because two things vary independently and one of them is already
half-solved. Transport is `Input` — async, in `dz-ingress-*`, already scoped by
the publisher crates design. Semantics is `Adapter` — synchronous, no I/O, and
the subject of this document.

### `dz-adapter-core`, and why it is its own crate

This is the one crate every venue repository compiles against, so its dependency
tree is inherited by every venue. It depends on **`dz-edge-core` and `thiserror`
and nothing else**: no async runtime, no TLS stack, no `prometheus`, no HTTP
client. A venue pinned to our tokio minor version because the trait crate named
one is a version conflict we caused.

`Input` therefore does *not* live here. It is inherently async and inherently
transport, and putting the two traits in one crate hands every adapter the
websocket crate's dependency tree whether it uses it or not.

```rust
// dz-adapter-core

/// An instrument the runtime has admitted and minted an `Instrument ID` for.
///
/// A dense index, not the wire `Instrument ID`: an adapter carries it in its
/// own per-symbol state and cannot mint one. The runtime resolves it to the
/// published ID at lowering, so an event for an instrument that was never
/// admitted is unrepresentable rather than droppable.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct InstrumentRef(u32);

/// A price or quantity as the venue stated it, before the instrument's
/// exponent is applied.
///
/// There is deliberately no variant carrying an integer already at the
/// instrument's own exponent. That is the one shape that would let a venue
/// scale for itself, and scaling is where exactness is lost.
pub enum Scalar<'a> {
    /// The venue's own decimal text. Converted by `dz_edge_core::fixed_point`,
    /// which refuses anything it cannot represent exactly.
    Text(&'a str),
    /// Already an integer, at an exponent the venue states. Rescaled to the
    /// instrument's, exactly or not at all.
    Fixed { mantissa: i64, exponent: i8 },
}

/// One side of a two-sided quote.
///
/// Two cases, because the wire has two states per side: the *updated* and
/// *gone* bits are mutually exclusive, so the bit says a side is present. The
/// runtime derives `Update Flags` from the pair; an adapter never writes one.
pub enum SideUpdate<'a> {
    Gone,
    Present { px: Scalar<'a>, qty: Scalar<'a>, source_count: Option<u16> },
}

#[non_exhaustive]
pub enum Event<'a> {
    Quote { instrument: InstrumentRef, source_ts_ns: u64,
            bid: SideUpdate<'a>, ask: SideUpdate<'a> },

    Trade { instrument: InstrumentRef, source_ts_ns: u64,
            px: Scalar<'a>, qty: Scalar<'a>,
            aggressor: Aggressor, trade_id: Option<u64>,
            cumulative_volume: Option<u64>, flags: TradeFlags },

    /// Market-by-price. `qty` is the absolute aggregate resting quantity at
    /// `px` after the change; zero removes the level.
    Level { instrument: InstrumentRef, source_ts_ns: u64, side: Side,
            px: Scalar<'a>, qty: Scalar<'a>,
            order_count: Option<u16>, presence: Presence },

    Clear { instrument: InstrumentRef, source_ts_ns: u64, scope: ClearScope },

    /// Market-by-order.
    OrderAdd { .. }, OrderCancel { .. }, OrderExecute { .. },
}

/// What the venue knows about whether a level existed before.
///
/// The runtime derives `Action`: `qty == 0` is `Delete` and nothing else can
/// be, a non-zero quantity takes this hint, and `Unknown` is conformant when
/// the upstream does not distinguish an insertion from a change. The illegal
/// pairings the specification names — a deletion carrying another `Action`, a
/// `Delete` carrying quantity — are then unrepresentable rather than merely
/// forbidden.
pub enum Presence { Unknown, New, Change }
```

The trait itself:

```rust
pub trait Adapter: Send {
    /// The upstream message-type names this adapter counts individually on
    /// `dz_publisher_ingress_messages_total`. Anything else falls to `other`.
    fn message_types(&self) -> &[&'static str];

    /// Instruments this adapter wants published, and the definition fields
    /// only the venue knows: symbol, asset class, exponents, tick and lot
    /// size, expiry. Drained by the runtime, which applies the selection
    /// policy, mints IDs, and hands back an `InstrumentRef` per admission.
    fn poll_listings(&mut self, out: &mut dyn ListingSink);

    /// Called after every successful connect, including reconnects. The venue
    /// writes its auth and subscription frames here; the runtime owns when.
    fn on_connected(&mut self, conn: ConnectionId, out: &mut dyn UpstreamSink)
        -> Result<(), AdapterError>;

    fn on_disconnected(&mut self, conn: ConnectionId, reason: DisconnectReason);

    /// One upstream payload in, zero or more events out.
    ///
    /// Synchronous, allocation-free, and free of I/O. This is not an
    /// ergonomic preference: it is what makes the adapter a pure function of
    /// its input bytes, which is what lets the recorder re-run it offline over
    /// an archive (below), and what lets a venue's mapping be tested in CI
    /// against a fixture with no network.
    fn on_payload(&mut self, payload: &Payload<'_>, out: &mut dyn EventSink)
        -> Result<(), ParseError>;

    /// Emit the current book for one instrument, on demand.
    ///
    /// Pulled by the runtime on the snapshot cadence rather than pushed,
    /// because the snapshot port's pacing is the runtime's and the book is the
    /// adapter's. A top-of-book adapter leaves this defaulted.
    fn snapshot(&self, instrument: InstrumentRef, out: &mut dyn SnapshotSink)
        -> Result<(), AdapterError> { Ok(()) }
}

/// The playbook's parse-error taxonomy, and nothing else.
///
/// The variants are exactly `ParseErrorReason` in `dz-publisher-metrics`. The
/// adapter's error type *is* the metric label, so an adapter cannot fail to
/// parse without the series moving.
pub enum ParseError { Schema, UnknownField, Malformed, Truncated }
```

### Five properties this shape buys, each from a specific failure

**Sink-passing, not `-> Vec<Event>`.** Zero allocation on the highest-frequency
path, and the trait stays object-safe so the composition below can hold a
`Box<dyn Adapter>`.

**Synchronous and I/O-free.** An `async fn` in the trait pins every venue to one
runtime version and makes deterministic replay impossible. Both costs are paid
for nothing: the async work is the transport's, and the transport is `Input`.

**Borrowed payloads and borrowed `Scalar::Text`.** The adapter reads out of the
receive buffer and writes into the encode buffer; nothing is owned in between.

**Nothing spec-decided is expressible.** No `Instrument ID`, no `Source ID`, no
`Channel ID`, no sequence number, no scaled integer, no flags byte, no `Action`,
no datagram. Not *"a venue should not"* — there is no parameter to pass one
through.

**Additive by construction.** `#[non_exhaustive]` on the event enums, defaulted
methods, and sink-passing rather than a return type mean a new feed's messages
and a new optional field are minor versions. A trait whose every extension is a
breaking change would strand venues on old tags, which is the failure mode of
consuming crates by tag.

---

## Composition: how configuration picks the source

Configuration selects the adapter, but Rust has no runtime library loading, so
*how* it selects matters and the design already half-states it. The publisher
crates design gives `[adapter] kind = "..."` — *"required; names the adapter
implementation"* — and gives `[ingress] kind = "websocket"` the same word for a
different mechanism. They are not the same mechanism and the document should not
have spelled them alike.

| | `[ingress] kind` | `[adapter] kind` |
|---|---|---|
| Chooses among | transports in this family: `websocket`, `fix`, `multicast`, `rest`, `filetail`, `uds` | adapters the binary was linked with |
| Resolved by | a closed match in `dz-ingress-core`, gated by cargo features | a registry the venue's `main` populates |
| An unknown value is | a config error naming the built-in set | a config error naming what this binary registered |

Three ways the second could work:

**1. Compile-time link, runtime registry — chosen.** The generic publisher is a
*library*, not a service. The venue repository owns `main`, which is short:

```rust
fn main() -> ExitCode {
    dz_publisher_runtime::run(
        AdapterRegistry::new().with("<venue-adapter>", |cfg| Ok(Box::new(VenueAdapter::new(cfg)?)))
    )
}
```

The runtime owns config loading, guards, signals, metrics, egress and refdata;
the registry is the only thing it cannot know. Static dispatch where it matters,
`cargo` resolving versions, no ABI, and a binary that cannot be pointed at an
adapter it does not contain. A `kind` naming an unregistered adapter is a
startup error listing what *is* registered — the audit's own lesson, where a
misspelled section parsed cleanly, fell back to a default, and ran the wrong
transport while the operator believed otherwise.

**2. Dynamic library (`libloading`).** Rust has no stable ABI, so this needs a
`repr(C)` FFI or `abi_stable`, permanently: every event across the boundary
becomes a C-shaped struct, monomorphization stops at the edge, and a mismatched
build is a segfault rather than a link error. It buys the ability to ship an
adapter without rebuilding the publisher, which nobody has asked for. **Rejected
unless a third party must ship a closed-source adapter**, and then as an
addition rather than the default.

**3. Out-of-process source — adopted as a second transport, not the default.**
`[adapter] kind = "uds"` selects a built-in adapter that reads a framed
normalized-event stream from a Unix socket. The "library implementing the
source" is then another process, in any language. This is worth having for two
independent reasons: it is the only path for a venue whose integration is not
Rust, and its framing is the same framing the recorder comparison below needs.
It costs a serialization format and a copy per event, which is why it is not
what a Rust venue should use.

### Configuration

Unchanged from the publisher crates design, which already put every
venue-specific key under `[adapter.*]` with `deny_unknown_fields`. Two
clarifications this design adds:

```toml
[adapter]
kind = "..."         # must name a registered adapter; error lists the registry

[adapter.tee]        # optional; the reference stream of the comparison below
enabled = false
path    = "..."      # Unix socket the publisher fans encoded datagrams out to
```

`[adapter.tee]` sits under `[adapter]` rather than `[egress]` deliberately: it
is not a transmitter, it darkens nothing when it fails, and it must never be
able to end a send.

---

## The same interface in the recorder

The recorder today archives bytes off the wire and decodes nothing in the record
path. Its analysis tier replays an archive, decodes it, rebuilds books, and runs
conformance. What it cannot currently answer is the question that matters most
when a subscriber complains: **was the message never sent, or sent and lost?**
Its cross-site join compares two receivers and infers; it has no reference for
what the publisher actually emitted.

The adapter interface supplies that reference three ways, of increasing strength
and cost. None of the three is new machinery: each is one of the four in-process
test harnesses above, pointed at an archive instead of a test.

### Mode A — the egress tee

`dz-publisher-egress` already boundaries on `DatagramSink`. Fan out: one sink is
the multicast transmitter, the second writes the identical encoded datagrams to
a local Unix socket a recorder on the publisher host archives.

Every subscriber-site archive then diffs against a reference archive, datagram
for datagram, keyed on `(source, Channel ID, destination port, Sequence
Number)` — the channel instance the recorder already keys on. Network loss,
reordering, MTU drops and one-way latency become measured rather than inferred.

Two rules, both non-negotiable, and both the reason this is a *tee* and not a
second transmitter: the tee never blocks the send path, and a tee failure is
counted and dropped, never propagated. A reference stream that can stall the
feed it measures is worse than no reference stream.

What it does not catch: anything upstream of the tee. The tee sees what the
publisher decided to send, so a mapping bug is faithfully reproduced on both
sides.

### Mode B — an independent adapter run at the recorder

The recorder host links the same adapter, opens its own upstream connection, and
lowers to messages, which are compared against the decoded multicast.

**This comparison must be state-based, not event-based.** Two upstream
connections to the same venue do not deliver identical streams: conflation
differs, per-connection sequencing differs, and snapshots are taken at different
instants. An event-for-event diff here would report a finding per second and
mean nothing. What is comparable is state at aligned instants — the book
fingerprint the recorder's analysis tier already builds — plus event-rate and
latency distributions.

Catches publisher-side conflation, staleness, silently dropped instruments and
whole channels that stopped. Blind to mapping bugs, because both sides run the
same mapping code.

### Mode C — offline re-lowering, the deterministic one

Archive the raw upstream payloads as `Input` yields them. Offline, re-run
`Adapter::on_payload` over exactly those bytes, lower the events with the same
runtime lowering, and compare the resulting messages against the messages
decoded from the multicast archive — **event for event, because it is the same
input.**

This is the one that answers the original question, and it is the argument for
every constraint on the trait above. It works only because `on_payload` is
synchronous, does no I/O, and is a pure function of its payload and the
adapter's own state. Had the trait been `async fn next_event()`, none of it
would be possible.

Three things make the comparison well-defined, and each is a design requirement
rather than an observation:

- **Compare at message grain, not datagram grain.** Datagram batching is
  time-dependent; the messages inside it are not. The comparison strips framing
  on both sides.
- **`Per-Instrument Seq` is the join key, and it must be deterministic.** The
  runtime — not the adapter — stamps it, from a counter keyed on the instrument
  and reset with the channel. Both the wire copy and the re-lowered copy then
  carry the same value for the same upstream event, and the diff is a join
  rather than a heuristic alignment.
- **Reference data comes from the archive.** `InstrumentDefinition` and
  `ManifestSummary` are on the wire, so the archive already carries the
  `Instrument ID` and exponent state the re-lowering needs. It is reconstructed
  from the capture, never from live registry state, or the re-run reproduces
  today's mapping rather than the one that was live.

Given those, a finding is one of exactly four things, and the mode says which:

| Finding | Meaning |
|---|---|
| In the re-lowered stream, not on the wire | the publisher dropped it: a full queue, a guard, a crash window |
| On the wire, not in the re-lowered stream | the publisher invented it, or refdata state diverged |
| Both, fields differ | a lowering or scaling defect, named by field |
| Both, identical, different timing | framing and pacing only — the healthy case |

### What this does not do

None of the three validates the adapter against the venue. Both sides of Mode C
run the same mapping, so an adapter reading the wrong field is invisible to it.
That is what golden fixtures of upstream payloads are for, and they belong in
the venue repository beside the adapter — one recorded payload, one expected
event list. The interface makes them cheap: a fixture test needs no network, no
socket, and no runtime. Both publishers already commit such fixtures, and one
guards them with a rule worth copying wholesale: a change that edits the engine
and regenerates its own control in the same commit has deleted the control.

---

## Consequences for the crate layout

Added to the layout in the publisher crates design:

```
rust/
  adapter/     dz-adapter-core          the trait, the events, nothing else
  publisher/   dz-publisher-lowering    normalized events -> AppMessage values
  ingress/     dz-ingress-core, -websocket, ...
  recorder/    dz-recorder-relower      Mode C, in the analysis tier
```

`dz-publisher-lowering` is a separate crate from `-runtime` for one reason: the
recorder must link the lowering without linking the runtime, its egress socket
or its signal handling.

---

## Prerequisites this design depends on and does not have

Stated plainly, because two of them gate half the scope.

- **`dz-edge-mbo` does not exist.** `codec/` holds `-core`, `-tob`, `-refdata`
  and `-mbp`. The market-by-order half of the interface cannot be built, tested
  or lowered until that crate does. One publisher has a working market-by-order
  implementation — messages, per-instrument sequencing, an apply-step tap, and
  a reset-and-recover path for a dropped delta — so the crate is a port with a
  reference rather than new work, and its runtime behaviour (a drop pauses the
  instrument, emits `InstrumentReset` and schedules a recovery snapshot, instead
  of leaving subscribers applying to a diverged book) belongs in the shared
  runtime, where it is also what makes a Mode C finding attributable.
- **`dz-publisher-egress` and `-refdata` do not exist.** They are the owners of
  everything the table above moves out of the venue. Until they land, the
  interface has no runtime to be lowered by.
- **`dz-ingress-core` does not exist.** The adapter is usable without it — a
  venue can drive `on_payload` from its own reader — but then the venue keeps
  reconnection and backoff, which is one of the things this exists to end.
- **New message types need `EgressMessageType` entries** in
  `dz-publisher-metrics`; a unit test there fails until they do.

---

## Non-goals

No feed spec changes. No convergence of book state machines. No code generation
from the specs — the hand-transcription discipline in the conformance suite is
what makes a golden vector evidence rather than a tautology, and the same
argument applies here. No Go implementation of the adapter trait: the Go side of
this repository is a subscriber, and a non-Rust venue integration uses the UDS
transport rather than a second trait to keep in step.
