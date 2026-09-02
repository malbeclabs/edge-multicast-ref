# The venue adapter interface — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** One trait a venue repository implements to turn its own data source
into top-of-book and depth messages, one lowering that turns those into the
wire, and one path that lets the recorder answer *did the publisher publish what
the venue said?*

**Spec:** `docs/superpowers/specs/2026-09-02-venue-adapter-interface-design.md`

**Tech stack:** Rust 2021, workspace MSRV. `dz-adapter-core` depends on
`thiserror` and nothing else — stricter than this plan first asked for, see
task 1's note. No async runtime below `dz-ingress-*`.

---

## Scope, and what it is not

This is plan 1 of two. It lands the interface, the lowering, the composition and
the offline comparison — everything that can be built, tested and merged with no
network, no socket and no privileges. It does **not** land the transports, the
egress crate or the refdata crate; those are the publisher-crates design's own
steps and this plan consumes them where they exist and stubs them where they do
not.

> **Plan 2's crates landed early, out of order.** `dz-publisher-egress`,
> `dz-publisher-refdata`, `dz-ingress-core` and `dz-ingress-websocket` are in
> the workspace. What is still missing before a venue can *run* is
> `dz-publisher-runtime`: the config composition, the adapter registry of task
> 7, the guards, and the shutdown that sends `EndOfSession`.

| Plan | Lands | Delivers |
|---|---|---|
| **1 — the interface** (this one) | `dz-adapter-core`, `dz-publisher-lowering`, the registry and config binding, `dz-recorder-relower` | a venue can write an adapter and prove its mapping in CI; an archive can be re-lowered and diffed |
| **2 — the runtime and the tee** | `dz-ingress-core`/`-websocket`, the `DatagramSink` fan-out, `dz-publisher-runtime` wiring, the reference recorder | a venue can *run*, and Modes A and B become live |

Plan 1 is sized so that every task is a merge into this repository with a test
that runs in CI. No task in it requires a venue repository to change.

---

## Global constraints

- **Vocabulary:** `GLOSSARY.md` governs every identifier, comment, test name and
  commit message. `datagram` never `frame`; `era` never `epoch`; `port role`
  with the tokens `mktdata`/`refdata`/`snapshot`; `channel` only for the
  `Channel ID` shard. The seam this generalises calls its layers *product line*
  and *feed*; neither term survives into these crates.
- **No venue names.** This repository is public. No commit message, comment,
  test name, fixture, config example or crate name in this plan names a venue, a
  venue repository, a venue crate, an issue tracker, or gives a count of
  publishers.
- **`dz-adapter-core` takes no new dependency.** `thiserror`, and nothing
  else. A task that needs a second stops and asks. This is the crate every
  venue inherits and it is the whole point of the crate being separate.
- **Nothing spec-decided is expressible through the interface.** No
  `Instrument ID`, `Source ID`, `Channel ID`, sequence number, scaled integer,
  flags byte, `Action`, or datagram in any parameter or return type an adapter
  touches. A task that adds one has misread the spec.
- **Lints:** `#![forbid(unsafe_code)]` and the workspace clippy set on every new
  crate. `cargo clippy --all-targets --all-features -- -D warnings` and
  `cargo fmt --all --check` pass at every task boundary.
- **Tests run in both profiles.** `debug_assert!` differs and CI runs both.

---

## The pieces where the obvious implementation is the wrong one

Stated up front, because each was found by reading production code rather than
by reasoning, and each is a task below that would otherwise be written wrong.

| Piece | Why it is not obvious |
|---|---|
| `Scalar` has two variants and no third | text-only forces a venue whose book already holds integers to round-trip through a string, which is a second scaling that can drift; a variant carrying an integer already at the *instrument's* exponent would hand scaling back to the venue |
| `Update Flags` is derived, never passed | the bit says a side is *present*, not that it moved, and the two bits of a side are mutually exclusive — which both publishers derive independently and neither specification states. See task 3's note |
| `Action` is derived from `qty` plus a hint | the shipped bug is a table numbered from `New`; deriving `Delete` from zero makes both illegal pairings unrepresentable while leaving New-vs-Change with the only layer that knows |
| `on_payload` is sync and I/O-free | it is what makes Mode C a pure function; `async fn` in the trait would also pin every venue to one runtime version |
| the snapshot is pulled, not pushed | the cadence is the runtime's and the book is the adapter's |
| `Per-Instrument Seq` is stamped by the runtime | it is the join key the re-lowering diff needs, and it must be identical on both sides for the same upstream event |
| refdata state for a re-lowering comes from the archive | reconstructing it from live registry state re-runs today's mapping over yesterday's bytes and reports nothing |
| `InstrumentRef` is a dense index, not the wire id | a venue that can name an `Instrument ID` can name one that was never published |

---

## Tasks

### 1. `dz-adapter-core`: the value types

- [x] New crate `rust/adapter/dz-adapter-core`, added to workspace `members`.
- [x] `InstrumentRef` — opaque, `Copy`, constructible only inside the workspace.
- [x] `Scalar<'a>` — `Text(&'a str)` and `Fixed { mantissa: i64, exponent: i8 }`.
- [x] `SideUpdate<'a>` — `Gone` / `Present { px, qty, source_count }`; three
      cases as first written, corrected to two in task 3.
- [x] `Presence`, `Side`, `Aggressor`, `TradeFlags`, `ClearScope`.
- [x] `ParseError` with exactly the four variants of `ParseErrorReason`, and a
      test that fails if the two enums ever differ in arity or token.
- [x] `AdapterError`, `Payload<'a>`, `ConnectionId`, `DisconnectReason`.

**Test:** the crate's dependency list is asserted from `cargo metadata` — a test
that fails the moment a third dependency appears.

### 2. `dz-adapter-core`: the sinks and the trait

- [x] `EventSink`, `ListingSink`, `SnapshotSink`, `UpstreamSink` — all
      `dyn`-safe, all sink-passing, none allocating per event.
- [x] `Event<'a>`, `#[non_exhaustive]`, top-of-book and depth variants.
- [x] `Adapter`, object-safe, with `snapshot` defaulted.
- [x] Rustdoc on every method stating what the implementor must *not* decide.

**Test:** a compile-time assertion that `Box<dyn Adapter>` exists; a doc test of
a ten-line adapter that ignores its payloads, proving the trait can be
implemented without importing anything else.

> **Tasks 1 and 2: landed.** 28 tests in the crate, 6 more in
> `dz-publisher-metrics`, `clippy --all-targets -D warnings` and `fmt --check`
> clean, `cargo test --all` green in both profiles. Four things came out
> differently from the text above, and each is a decision rather than a slip.
>
> **The crate depends on `thiserror` and nothing else** — not on `dz-edge-core`
> either. Nothing in the boundary needed it: the codec's own types appear only
> in the lowering, and the tables this crate mirrors are held to the codec
> through dev-dependencies, which a venue does not inherit. Stricter than the
> spec asked for, in the same direction.
>
> **The dependency test reads the manifest rather than `cargo metadata`.**
> Walking the resolved graph needs a JSON parser, which would be a
> dev-dependency added in order to check that dependencies are not added. The
> manifest reader covers direct dependencies exactly, the transitive closure is
> pinned by the allowed set being `thiserror` alone, and the test says so — plus
> a second test proving the reader finds a dependency it is shown, because a
> reader that silently found nothing would pass no matter what was added.
>
> **The taxonomy cross-check lives in `dz-publisher-metrics`.** It can only live
> there: the dependency runs that way round, and `ParseErrorReason::ALL` is
> `pub(crate)` besides. Both directions are exhaustive matches rather than
> lists, so a variant added on either side fails to compile — including the
> direction that is easy to forget, a label with a panel that no adapter can
> ever report.
>
> **The market-by-order variants of `Event` are absent, not guessed.**
> `#[non_exhaustive]` makes *adding a variant* a minor version but *adding a
> field to one* a breaking change. `dz-edge-mbo` does not exist, so specifying
> their fields now against nothing would buy exactly the breaking change the
> attribute exists to avoid, for consumers pinned to a tag. They land with the
> codec crate.
>
> Two things were added that the tasks did not name and the work required.
> `EventSink::upstream_message` — an adapter names the upstream message type it
> recognised, which is what `dz_publisher_ingress_messages_total` counts, so the
> series comes from implementing the trait rather than from remembering to
> record it. And `rust/adapter` was added to the scan roots in
> `scripts/check-public-repo-rules.sh`: that script fails loudly for a root that
> has gone missing and cannot know about one that should have been added, so a
> new directory under `rust/` is outside every rule it enforces until someone
> puts it in.

### 3. `dz-publisher-lowering`: top-of-book

- [x] New crate `rust/publisher/dz-publisher-lowering`.
- [x] `InstrumentTable`: `InstrumentRef` → `(Instrument ID, price exponent, qty
      exponent)`, populated by the refdata owner, read on the hot path.
- [x] `lower_quote`: `Event::Quote` → `dz_edge_tob::Quote`, deriving
      `Update Flags` from the `SideUpdate` pair and scaling both sides through
      `dz_edge_core::fixed_point`.
- [x] `lower_trade`: `Event::Trade` → `dz_edge_tob::Trade`.
- [x] `Scalar::Fixed` rescaling: exact or refused, never rounded, sharing the
      three `ScaleError` cases so each reaches its own metric reason.

**Tests:** every `SideUpdate` pair against its expected flags byte, as a table —
including the two an unconditional encoder gets wrong. `Scalar::Text` and
`Scalar::Fixed` carrying the same value lower to identical bytes. A `Fixed`
whose exponent cannot be rescaled exactly is refused rather than rounded.

> **Task 3: landed**, and it moved the boundary first. 22 tests in the new
> crate, 581 in the workspace, `clippy --all-targets --all-features -D
> warnings`, `fmt --check` and `check-public-repo-rules.sh` clean, `cargo test
> --all` green in both profiles.
>
> **`Update Flags` states presence, not change, and the byte was read off the
> two publishers rather than reasoned about.** Both derive it the same way,
> independently, on live feeds: a side's *updated* and *gone* bits are mutually
> exclusive, so the bit says the side is present. One publisher states that
> table in its own encoder as normative for its feed and pins the absent side's
> zeros byte for byte; the other reaches the same four values from whether each
> side of a truncated book is empty. The feed spec fixes the four bit positions
> and settles nothing else.
>
> So `SideUpdate` lost `Unchanged` and became `Gone` or `Present`, which is a
> correction to task 2. A venue cannot say *this side did not move* because the
> wire cannot carry it, and a quote that would restate the top unchanged is
> therefore not an event at all: suppressing it stays with the adapter, which is
> the layer holding the book. Both publishers already do that, one counting the
> suppressions as its expected bulk of traffic and one deduplicating on the
> encoded top.
>
> **One of the two defects this design cites was miscounted.** The encoder that
> writes both update bits unconditionally cannot reach the wire with an absent
> side: its book's top accessor returns `None` unless both sides exist and the
> caller turns that into a missing-field error, so under presence semantics the
> constant is the correct byte for every quote that feed emits. The live hazard
> on the quote path is the other publisher's, and it is worse — its market-data
> scaling goes through `f64` and `.round()` and takes failure as
> `.unwrap_or(0)`, so a value it cannot convert is published as a price of zero
> with the side's *updated* bit set. An exact, string-only conversion sits in
> the same repository, uncalled on that path. That is now the finding the
> scaling argument rests on, and it is the stronger one. The `Action` defect
> holds up unchanged: it landed, and it is fixed there by the same
> transcribe-the-spec-tables technique as `tests/wire_vocabularies.rs`.
>
> **A withdrawn instrument leaves a hole.** `InstrumentTable` slots never shift
> and are never reused. An `InstrumentRef` is a handle rather than a capability
> — the runtime that mints one is in a different crate from the boundary that
> carries it, so a forged or stale handle is reachable — and this is where
> either is refused, once, countably, instead of resolving to whichever
> `Instrument ID` moved into that slot.
>
> **The specification's own conformance subscriber was read afterwards, and it
> settles one of these decisions and leaves the other free.** Its rule catalog
> carries two rules on the flags byte. `TOB.QUOTE.UPDATE_FLAGS_COHERENCE`
> states, in the rule's own implementation, that the specification defines the
> four bits independently and does **not** couple *gone* to *updated*, and that
> asserting either pairing would false-positive on conformant publishers — so
> following the two live publishers is a free choice rather than a risk. What
> the same rule *does* grade a violation is a flags byte with bits 0-3 all
> zero, "a quote that claims nothing changed": which is exactly what the
> three-case `SideUpdate` produced for a quote with both sides unchanged.
> Dropping `Unchanged` removed the one way this lowering could have emitted a
> non-conformant quote, and that is now pinned by
> `no_pair_of_sides_can_produce_an_empty_flags_byte`.
> `TOB.QUOTE.GONE_VS_ZERO_PRICE` grades the price half of a gone side a
> **must**, which the lowering satisfies; the quantity half is mandated by
> nothing and written anyway.
>
> **`Source ID` is a checked type, because three quarters of a `u16` is a
> conformance violation.** `TOB.QUOTE.SOURCE_ID_REGISTRY` refuses `0`
> unconditionally — the registry reserves it, and it is what an unset
> configuration key hands you — and the registry also reserves `1024`–`32767`
> for future assignment. So `Lowering::new` takes a `SourceId` whose
> constructor admits only the assigned and the private ranges, and a publisher
> with no valid identity fails at startup instead of failing conformance on
> every message it ever sends. Which *assigned* id belongs to a given publisher
> needs the registry itself, and is deferred exactly as the conformance
> subscriber defers it.
>
> **`Trade ID` has no conformance rule.** `TOB.TRADE.FIELDS` checks the
> aggressor enum, the unused qualifier bits and the source id, and nothing
> checks the identifier. Worth knowing where the tool's silence is: a venue
> that publishes a wrong `Trade ID` fails nothing.
>
> **The lowering refusal has no normative metric family yet.**
> `LoweringError::reason()` keeps `unknown_instrument` and the three
> `ScaleError` cases distinguishable, but the normative set in
> `dz-publisher-metrics` has no series for a lowering refusal, and the set is
> closed by a governing playbook. Which family these land on is a decision for
> the runtime task, and it needs either a playbook addition or a deliberate
> mapping onto an existing `reason`. Flagged rather than invented.

### 4. `dz-publisher-lowering`: depth

- [x] `lower_level`: `Event::Level` → `dz_edge_mbp::LevelUpdate`, deriving
      `Action` — `qty_raw == 0` is `ACTION_DELETE` unconditionally, non-zero
      takes `Presence`, `Unknown` is conformant.
- [x] `lower_clear`, the snapshot framing (`SnapshotBegin`, the levels,
      `SnapshotEnd`) of a pulled snapshot.
- [x] `PerInstrumentSeq`: the runtime's counter, keyed on the instrument,
      reset with the era, stamped here and nowhere else.

**Tests:** the two illegal pairings the specification names are unreachable —
asserted by exhausting `Presence` against zero and non-zero quantity. A snapshot
pulled from a fake adapter frames as begin, N levels, end, with the level count
and the last sequence the begin declared.

> **This task has no independent control, and that is the reason to be careful
> with it.** The specification's conformance subscriber has 32 rules for
> market-by-price against 68 for market-by-order, and the gap is not an artifact
> of counting: the enum ranges on `LevelUpdate`'s `Side` and `Action` are
> registered market-by-order-only because their emit paths read market-by-order
> messages, and `testdata/` has no conformant market-by-price capture at all.
> So the very defect class this whole boundary was shaped around — an `Action`
> table numbered from the wrong value — is the one that feed's conformance does
> not yet check. Until it does, the derivation table in this task is the only
> control there is, which is why it is written as a table over exhausted
> `Presence` values rather than as a few examples.

> **Task 4: landed.** 18 tests, 605 in the workspace, both profiles green,
> clippy and fmt clean. Four things are worth stating because a later reading
> would otherwise take them for slips.
>
> **`DepthLowering` is a second type rather than a method on `Lowering`.** The
> per-instrument sequence is a counter, top-of-book has no such field, and
> folding the two together would make the stateless path carry state for
> nothing and stop it being `Copy`.
>
> **The sequence number is taken last, after every refusal.** A number spent on
> a message that never reached the wire is a phantom gap every subscriber reads
> as packet loss, and the refusal is reachable rather than theoretical — a
> price the instrument's exponent cannot state exactly is refused rather than
> rounded. Asserted directly.
>
> **`Order Count` absent is `0xFFFF`, and that is the opposite of the
> top-of-book sentinel.** Two specifications answer the identical question with
> opposite values: this feed treats `0` as a real count, while top-of-book's
> `Source Count` says "unavailable" with `0`. The two are written out
> separately rather than shared through a helper that would have to pick a
> side, and a test pins both directions.
>
> **The snapshot framer records a refusal instead of returning it.**
> `SnapshotSink::level` cannot fail — it is called from inside the adapter's
> own loop over its book — so the first refusal is kept and surfaced by
> `finish`, which is where the runtime can count it and skip the instrument.
> Nothing partial is ever returned: an incomplete snapshot is worse than none,
> because a subscriber cannot tell a level that was refused from a level that
> was lost.
>
> `Level Index`, `Update Reason`, `Level Flags` and `Clear Reason` are all
> informational and none is expressible at the boundary; each carries the value
> the specification defines for absent. `Level Index` in particular is a
> property of the publisher's own book *as it emits*, not of the venue's event,
> so it is `0xFFFF` rather than guessed.

### 5. Lowering is one implementation, and `0x04` proves it

- [x] `Event::Trade` lowers through one function regardless of which feed the
      channel carries.
- [x] Test: the same `Event::Trade` lowered for a top-of-book channel and for a
      depth channel produces byte-identical `Trade` messages.

This is today a doc comment on two encoders held to each other by hand. The
test is the whole task.

> **Task 5: landed**, and made structural rather than asserted. The body moved
> to `src/trade.rs`, and both `Lowering::lower_trade` and
> `DepthLowering::lower_trade` are calls to it with no body of their own — so
> the two channels cannot drift, and `tests/trade_is_one_implementation.rs`
> keeps it that way. The specification's own words are what make it worth the
> file: *"Trade and Liquidation are byte-for-byte identical between the
> top-of-book feed, the market-by-order feed, and this feed."*
>
> Three things beyond the checklist, each because agreement on bytes is only
> half of one implementation: the two channels are asserted to **refuse** the
> same trade for the same reason, to agree for a venue that quotes a contract
> (the newest thing in the path and the likeliest place for two callers to
> diverge), and a trade is asserted to spend **no** `Per-Instrument Seq` — that
> series belongs to the messages that mutate the book, and spending a number on
> a trade would put a gap in a series every subscriber reads for loss.

### 6. Golden vectors, both directions

- [x] For each lowered message type, a vector in `testdata/golden/`: the
      normalized event, the instrument's exponents, and the expected bytes.
- [x] Asserted from `dz-publisher-lowering` (encode) and from the existing
      decoders (decode), so the vector binds the interface the way
      `testdata/golden/` already binds the codec across languages.
- [x] `EgressMessageType` entries in `dz-publisher-metrics` for anything new;
      its unit test fails until they exist.

> **Task 6: landed, and half of it needed no new vector.** `Quote` and `Trade`
> are reachable from a normalized event at the exponents the existing vectors
> imply, so the lowering reproduces `quote-v3.bin` and `trade-v3.bin` exactly.
> That is a stronger statement than a vector of this crate's own would be: a
> vector generated here would say the lowering agrees with itself, while
> reproducing the vector another language already reproduces says it agrees
> with the wire. Both `Scalar` shapes reach those bytes.
>
> The depth messages needed vectors of their own, and the reason is a finding
> rather than an inconvenience. The codec's vectors set every field to a
> distinct value so a transposed pair cannot pass — including three the
> boundary cannot state at all: a level's `Level Index` is a rank in the
> publisher's own book at emission rather than a property of the venue's event,
> and `Update Reason` and `Clear Reason` have nowhere in a normalized event to
> come from. No event can reproduce them, so reproducing them would have meant
> inventing a way for a venue to author a field it does not know. The five new
> vectors carry `-from-event-` in their names, `manifest.json` records the
> event beside the bytes in a `lowered_from` block, and the generator is an
> `#[ignore]`d test because regenerating one is a wire change.
>
> `EgressMessageType` gained the five depth types, and its cross-check against
> the codec's own `PORT_ROLES` failed until they had port roles — which is
> exactly what this task said it would do. The snapshot three are
> snapshot-port-only: a snapshot message on the market-data port is a series
> that can never be written to.

### 7. The registry and the config binding

- [x] `AdapterRegistry`: `&'static str` → constructor, in `dz-publisher-runtime`
      (or a `dz-publisher-compose` crate if the runtime has not landed).
- [x] `[adapter] kind` resolves against it. An unregistered `kind` is a startup
      error **naming every registered kind**, never a fallback and never a
      default.
- [x] `deny_unknown_fields` on `[adapter]` and every section under it.
- [x] `[adapter.tee]` parsed, defaulted off, plumbed nowhere yet.

**Tests:** an unknown `kind` fails and the message lists the registry; a
misspelled `[adapter.upstrem]` fails to load rather than silently defaulting —
the audit's own failure, as a test.

> **Task 7: landed**, inside `dz-publisher-runtime`, which is the crate a venue
> links and calls from a short `main`. 70 tests.
>
> Both tests this task named exist by name, and the misspelling one is paired
> with a control — a test that only proves a misspelling fails would pass
> against a parser that refused everything. Three more guard the shape of the
> refusal rather than its existence: an unknown kind never falls back to the
> only registered adapter (the most tempting fallback there is), an empty
> registry says so in words rather than printing an empty list, and a kind
> registered twice panics rather than shadowing one adapter with another.
>
> **The registry constructor returns the transport as well as the adapter**, and
> that is one thing the design's `main` sketch was short of. `[adapter.upstream]`
> is the venue's own keys, so only the adapter's code knows its endpoint key —
> and a runtime that built the transport itself would have to depend on every
> transport crate, which would make the linked-transport check always true.
> `[ingress] kind` is still resolved against the closed set and handed to the
> constructor. What the runtime cannot check is that the venue built the
> matching transport, and that is documented rather than hidden.
>
> **Teardown order**, transcribed as a list a test asserts against: ingress
> stopped, admissions closed, the final manifest with `Valid = 0` on refdata,
> `EndOfSession` on mktdata, both roles flushed, the exit reason recorded.
> The manifest goes before `EndOfSession` because a subscriber that stops at the
> terminal statement would otherwise never see it. Sockets and the state claim
> are released by `Drop` rather than by a step — a teardown whose correctness
> depended on a close call only works on the polite path.
>
> Three things it needed and could not have, none invented: there is still no
> metric family for a lowering refusal, so the five reasons are exposed and
> reported rather than mapped onto a series that would then mean two things;
> `recv_to_send` and `venue_to_recv` latency cannot be observed at all, because
> `EventSink` does not carry the payload's receive stamp and nothing at the
> boundary declares a timestamp kind; and the duration parser is now its third
> copy, which the second copy's own doc named as the point at which it needs one
> home.

> **Depth followed, once the labels existed.** `EgressMessageType` gained its
> five depth types in task 6, which removed the only thing keeping
> `spec = "market-by-price"` a startup error. The runtime now lowers
> `Event::Level` and `Event::Clear`, carries three port roles, and sends a
> framed snapshot. 94 tests.
>
> **A publisher can emit both feeds from one process, and that is where the
> `0x04` obligation stops being a doc comment.** The trade is lowered **once**
> and the same value is handed to both send paths — which is stronger than task
> 5's "both channels delegate to one function", because there is no second call
> site at all. One registry serves both feeds, and the three things that makes
> true were checked rather than assumed: `Instrument ID` identity is the one
> thing there can only be one of, `Manifest Seq` describes the published set
> rather than a channel, and the datagram builder stamps the `Channel ID` at
> push, so one composed `ManifestSummary` is truthful on both refdata ports.
>
> **A codec gap it had to work around, and the workaround is not the fix.**
> `DatagramBuilder<F: Feed>` is generic over the feed, so the magic is right,
> but `push<M: AppMessage>` carries no bound tying the message to the feed —
> nothing stops a `Quote` going into a market-by-price datagram, and `0x03` is
> not in that feed's message table. The codec validates `PORT_ROLES` and stops
> there. The runtime closed it for itself with a type-level `EmittedFeed: Feed`
> carrying a `const SPEC`, so a pipeline cannot disagree with its own
> specification; the codec is still permissive, and the proper fix is a feed
> declaration on `AppMessage` that `push` can check. Same class as the `Action`
> table: a byte the codec permits that a specification forbids.
>
> A snapshot has no cadence key to pace against, so `Publisher::snapshot` frames
> one on demand and the pacing is explicitly the caller's. No key was invented.

### 8. The UDS adapter

- [x] A length-delimited normalized-event **record** encoding, one event per
      record, versioned in a header byte.
- [x] `UdsAdapter`, a built-in `Adapter` reading that encoding, so a non-Rust
      integration can be the source.
- [x] The same encoding, written: a `RecordWriter` plan 2 uses for
      `[adapter.tee]`.

**Test:** round-trip every `Event` variant through the encoding and lower both
copies — the encoding is lossless with respect to the lowering, which is the
only property that matters.

> **Task 8: landed** as `dz-adapter-uds`, 8 tests. Two notes on what it is
> called and what it does not do.
>
> **"Record", not "frame".** This plan's own vocabulary rule is `datagram`
> never `frame`, and a length-delimited unit here is neither — calling it a
> frame would put a third meaning on a word the glossary already narrowed to
> one.
>
> **It opens no socket.** Reading bytes off a Unix socket is a transport's job
> and belongs to `dz-ingress-*`; the adapter is handed payloads and asked what
> they mean, exactly as every other adapter is. That is why it is testable with
> no socket at all, and it is the same split the boundary rests on everywhere
> else.
>
> **A record names the instrument by symbol.** An `InstrumentRef` is a dense
> index into one runtime's admitted table, so a record carrying one would mean
> something only inside the process that minted it — and the whole point is that
> the writer is somewhere else. The symbol is the only identity two sides can
> both state, which is the same conclusion task 9 reached independently.
>
> Length first, version second, and that ordering is the design: a reader that
> does not know the version can still find the next record. So an unknown
> version or an unknown event kind is **skipped** and the stream continues,
> while a body carrying more bytes than its shape is malformed rather than
> tolerated — a writer that added a field and a reader that ignored it is
> exactly what the version exists to catch.

### 9. `dz-recorder-relower`: Mode C

- [x] New crate `rust/recorder/dz-recorder-relower`, linking
      `dz-publisher-lowering` and **not** `dz-publisher-runtime`.
- [x] `InstrumentTable` reconstructed from `InstrumentDefinition` and
      `ManifestSummary` messages found in an archive, never from live state.
- [x] Re-run an adapter over an archived upstream-payload stream, lower, and
      join against the messages decoded from a multicast archive, keyed on
      `(Instrument ID, Per-Instrument Seq)` for depth and
      `(Instrument ID, source timestamp, Update Flags)` for top-of-book.
- [x] Report the four findings the spec names: missing on the wire, absent from
      the re-lowering, present in both with fields differing (named by field),
      and identical but differently timed.

**Tests:** a synthetic adapter and a synthetic archive built with
`dz-recorder-replay`'s `SyntheticPublisher`, with each of the four findings
injected deliberately and asserted — including that the fourth, framing and
pacing differences only, produces **no** finding. That last one is the test that
keeps the tool usable.

> **Task 9: landed.** 41 tests. The four findings are each injected and
> asserted, and the fourth — framing and pacing only — produces no finding over
> a window batched five-per-datagram against one, with the sequence offset by
> 9,000, a different era, and a send clock a hundred seconds out. That test also
> asserts four *identical* pairings, because a clean report over an empty join
> would otherwise pass it.
>
> **Reference data comes from the archive, and the API is what enforces it.**
> There is no entry point that accepts an `InstrumentTable` or a `SourceId`, so
> the requirement is the shape of the crate rather than a doc comment. The table
> is keyed on `Symbol`, because that is the only key both sides can state — an
> adapter cannot name an `Instrument ID` and the wire does not carry a handle.
> An instrument with no definition in the archive is **declined**, not invented:
> minting an id here would turn our own gap into a stream of "the publisher
> dropped it".
>
> **Six things make the comparison less deterministic than the design claims,
> and each is detected and reported rather than papered over.** The two worth
> naming: `Per-Instrument Seq` is only deterministic if the window opens on an
> era boundary — a window that opened mid-era carries a numbering offset and
> then every depth key is wrong in both directions, which the tool detects from
> the lowest sequence on the wire. And **the contract factor is not on the wire
> and is not derivable**: `Contract Value` is what a contract is worth, while
> `quoted_per_contract` is how much of the underlying a contract *is*, so for a
> per-contract venue the re-lowering applies no factor and every price differs.
> That is the one place this task's own text — "where the definition carries
> what is needed" — is not satisfiable, and it is the same gap the boundary work
> found from the other side.
>
> A duplicated datagram reports as absent from the re-lowering, because the
> comparison holds no datagram identity and cannot tell a network duplicate from
> a publisher sending twice. Stated, and asserted.

### 10. Documentation

- [x] `rust/adapter/README.md`: what a venue implements, in one page, with the
      ten-line adapter from task 2 as the example.
- [x] `rust/publisher/README.md`: the planned-crates table gains the two that
      landed; `dz-ingress-*` stays planned.
- [x] `docs/README.md`: this spec and plan in the index.
- [x] `rust/README.md`: the workspace table gains `adapter/`.

> **Task 10: landed**, and it needed one more page than the checklist named:
> `rust/ingress/README.md`, because that directory now exists and a reader
> arriving at it would otherwise have to infer why the transport is not in
> `adapter/`.
>
> `dz-ingress-*` did **not** stay planned — the websocket transport landed, so
> the publisher's planned table now names what actually remains: the other
> transports, market-by-order behind its missing codec crate, and the egress
> tee, which was waiting on a framing that task 8 has now written.
>
> One correction rather than an addition: `rust/README.md` claimed CI ran on
> changes under a list of paths. There is no path filter — CI runs on every pull
> request, which is what keeps a codec change from breaking a publisher
> silently. The claim was stale before this task and is now what the workflow
> does.

---

## Order, and why it is this one

Tasks 1–2 first because everything else is typed in terms of them, and because
the dependency-count test in task 1 is the constraint most easily lost later.

Tasks 3–6 before 7 because the lowering is the part with a byte-level right
answer, and golden vectors are cheaper to write while it is the only thing that
exists.

Task 9 last of the code because it is the only one that needs three other crates
to be real, and because its value depends on the lowering already being trusted:
a re-lowering diff over an untested lowering reports the lowering's own bugs as
publisher findings.

Nothing here waits on `dz-edge-mbo`, and the market-by-order variants of
`Event` are specified in task 2 and lowered in a follow-up when that crate
lands. Blocking the whole interface on a codec crate that does not exist would
leave the top-of-book path — the one both existing publishers already run —
waiting on a feed only one of them publishes.

---

## It ran

Everything above is tested against fakes — sockets behind traits, injected
clocks, recording sinks — which is what makes the suite run unprivileged with no
network, and which means all of it was a hypothesis until a byte left a socket.

`rust/publisher/dz-publisher-runtime/examples/loopback.sh` is that byte. It
composes the real runtime over real transmitters on a multicast group, publishes
a known set of events, and reads the other end with **this repository's Go
subscriber** — which makes the check cross-language rather than our encoder
agreeing with our decoder.

The subscriber decoded a heartbeat, the manifest cadence, two instrument
definitions, both quotes, the trade and the `EndOfSession`, and every value it
read is the value the publisher sent: the golden vector's raw integers on the
two-sided quote, `update_flags = 6` with zeroed bid on the one-sided one, and
the venue's own exponents on the definition.

**Two things only the real run could find.**

Unpinned, the transmitter resolves its source off the route to the group — which
is the discipline, and on a host whose route to that group leaves by the default
interface, a subscriber joined on loopback hears nothing. The publisher reports
a clean teardown, because nothing in the send path is wrong: the two ends chose
different interfaces. That is what `[egress] pin` is for, and it is invisible to
every test that holds a fake socket.

And `check-public-repo-rules.sh` refused the first group the example used. A
multicast address outside MCAST-TEST-NET in a public repository is exactly what
that gate is for, and it caught it before the commit rather than after.

**`run()` ran too**, once `[adapter.replay]` had an implementation behind it —
it parsed and nothing read it. `examples/replay.sh` writes a config document,
records a directory of normalized-event payloads with the built-in encoding,
and runs `run()` over them: the real config, the real registry, the real
adapter, the real lowering, real sockets, the real teardown. The Go subscriber
decoded both quotes, the trade, the definitions, the manifest cadence, the
heartbeats and the `EndOfSession`.

`[adapter.replay]` substitutes for the **transport**, not for the adapter: a
replay *is* a transport, so it is an `Input`, and the adapter cannot tell the
difference. That is the property that makes the exercise worth anything.

**Two more things only running it could find.**

Listings were polled on the tick loop, which starts *alongside* the driver — so
the first payloads of every restart reached an adapter holding no handles and
were dropped as events for instruments nobody had admitted. Continuous traffic
hides that completely; a finite replay does not. `run()` now admits once before
the driver starts.

And a replay has no handshake. A live connection spends time — a socket, a
negotiation, a subscription answered — and the publisher spends it publishing
reference data. A replay yields its first payload in microseconds, so the first
market-data messages went out before any `InstrumentDefinition` had, leaving a
subscriber holding a quote for an `Instrument ID` it could not resolve. That is
the ordering a live feed gets for free and an offline run has to buy, so the
replay settles before its first payload.

## Acceptance

The plan is done when a venue repository can, against tagged crates and with no
code from this repository copied into it:

1. implement `Adapter` for its own upstream, with a fixture test of its mapping
   that needs no network;
2. get a wire-correct `Quote`, `Trade`, `LevelUpdate` and snapshot without
   naming an `Instrument ID`, a sequence number, a flags byte, an `Action` or a
   scaled integer anywhere in its own code;
3. be pointed at an unregistered adapter and fail at startup with a message
   naming what it does have;

and when, in this repository, a synthetic archive with a deliberately dropped
message produces exactly one Mode C finding that names it.
