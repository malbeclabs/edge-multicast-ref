# A key a venue side can compute, and a seam a recorder can link

**Status:** design.

Four changes, and the first is a correction to a claim this repository makes about its own code.

## `state_key` cannot be joined on across two observers, and a design here says it can

[The venue-observation design](2026-09-09-recorder-venue-observation-design.md) decides that the race between a venue's own upstream and a publisher's channel is a view on `(feed, symbol, state_key, occurrence)`, and rests that on the key being "already transport-independent by construction".

**It is not.** The column's doc comment says "a hash over the instrument and both sides, and over nothing else". The function is:

```rust
pub fn state_key(channel_id: u8, instrument_id: u32, top: &Top) -> u64
```

It eats the `channel_id` and the `instrument_id` before it eats a single price, and both are values a venue may not know. The channel is the operator's mapping from a shard to a channel — the one thing the adapter boundary exists to keep a venue from naming. The `Instrument ID` is minted by the publisher's reference-data registry and is unique only within an era.

So the key is transport-independent and **observer-dependent**. Two recorders of one multicast feed pair on it because both read the same identifiers off the same datagrams. A venue-side observation has neither input and cannot compute it at all.

The failure is the one that function's own doc comment names, in the paragraph arguing for FNV-1a over `DefaultHasher`: a key that stops matching "looks like a quiet feed". A race keyed on it across the two sides returns **zero pairs**, and every occurrence reads as the other side having missed a state — a total outage reported as a clean feed on both paths.

## `book_key`, and `state_key` folded onto it

```rust
pub fn book_key(top: &Top) -> u64;
pub fn state_key(channel_id: u8, instrument_id: u32, top: &Top) -> u64;
```

The book-only key hashes the two sides and nothing else. `state_key` keeps its signature, keeps its value, keeps every caller, and is the channel and the instrument eaten ahead of that same fold over the two sides.

**The two keys are different values, and that is what they are for.** `state_key` answers *is this the same state of this channel's instrument*. `book_key` answers *is this the same book*. The first is what two recorders of one feed pair on; the second is what two observers of one market pair on. A view joining across the two sides uses `book_key` and carries the symbol beside it, because the symbol is the only instrument identity both sides hold.

**The two keys owe different things, and one field is where that becomes visible.** The top-of-book specification states `Bid Source Count` as *"Orders/sources at best bid. 0 if unavailable"*, so the multicast side reads a zero off the wire exactly where an observer of the venue's own upstream holds nothing at all. `book_key` has to read those alike or one book has two keys — and it can, because it is new, no row is keyed on it, and all it owes is that two observers of one book agree. `state_key` cannot: rows are written under it, so its value over given wire bytes must not move, and a `Quote` whose venue states no count is the commonest shape there is.

So the reading belongs to `book_key`'s subject and not to the derivation that feeds both keys. The fold stays byte-for-byte the fold `state_key` has always been, the derivation keeps the wire's number, and the zero is read as the absence in the one place where no stored value depends on the answer. Normalising upstream of that — in the `Quote` derivation — moves `state_key` for every quote a venue states no count on.

The venue-observation design's sentence is corrected with this change. The equivalence key was not designed for a cross-observer join, the column's doc comment overstates what the function does, and the race view that design proposes keys on `book_key`.

## `Side::is_absent` stays private

An absent side is a distinguished tag in the hash rather than zeros: "an empty side and a side priced at zero are different books", and the top-of-book convention of stating *unavailable* with a zero is exactly what would collapse them.

Anything computing this key outside the crate therefore has to carry its own copy of what absent means, and a copy that drifts is a race that stops pairing with no symptom. That is an argument for the key being computable **inside** the crate, which `book_key` makes it — so the field predicate stays private and nothing outside needs it.

## `EventSink::upstream_identity`, defaulted

An adapter can name the *kind* of message a top came from — `upstream_message(message_type)` — and never *which* message.

For a publisher that is right and stays right: a venue's own session numbering is a transport's number rather than a book's, which is why the row tables refuse it along with the rest of a venue's provenance.

For a recorder it is evidence. It separates a venue resending a state from the venue producing that state again, and without it an unpaired occurrence has one fewer explanation available to it. The only other way to obtain it is to decode the payload a second time beside the adapter, and two decoders of one venue is a race measuring its own decoders.

```rust
fn upstream_identity(&mut self, sid: Option<u64>, seq: Option<u64>) { let _ = (sid, seq); }
```

Defaulted, so no adapter and no sink changes. A publisher's sink ignores it and nothing reaches the wire. Both parameters are `Option` because a venue that publishes one and not the other is ordinary, and **neither is ever a key** — the rule `event.upstream_ts` already carries.

## The composition seam leaves the publisher's crate

`AdapterRegistry`, `Venue` and `AdapterContext` live in `dz-publisher-runtime`. Anything else that composes a venue through them links the egress, the transmitters, the era store and the reference-data registry in order to publish nothing — the dependency shape this repository has already removed from its own tree once.

`Venue` holds an `Adapter` and a set of `Input`s: one type from `dz-adapter-core`, one from `dz-ingress-core`. Neither is the home, because a boundary crate that gained a registry would be a boundary crate with a composition in it. The home is a crate over both, and `dz-publisher-runtime` re-exports what it exports today so that no venue's `main` changes.

### What the three types actually drag, measured against the tree

The paragraph above says "a small crate over the two boundary crates". Measured, it is not small, and the difference is not incidental — **it is the seam**. Each of the following is a thing some venue's constructor reads, and what a venue reads is where the boundary is:

| The type | What it holds that is not in the two boundary crates |
|---|---|
| `Venue` | `collectors: Vec<Box<dyn Collector>>`, a Prometheus type reached today through `dz-publisher-metrics` |
| `AdapterContext` | `&ReplayConfig`, `&[Source]` and `&[FeedSpec]`, all three in `dz-publisher-runtime::config`; `FeedSpec` reaches `dz-edge-core`, `dz-edge-tob` and `dz-edge-mbp` |
| `AdapterRegistry` | `open` returns `StartupError`, which names `dz-publisher-egress`, `dz-publisher-refdata` and `dz-publisher-metrics` in its own variants; and it resolves `crate::builtin`, which is `dz-adapter-uds` |

The context's accessors are its dependency list, so narrowing the crate means narrowing the accessors, and narrowing the accessors changes a venue's `main`. That is refused. The crate is therefore as wide as the context is, and the four decisions below are about where each of those costs is paid rather than about avoiding them.

**The registry's error is a type parameter with a default.** `open`'s two failures — *no adapter answers this `kind`* and *the adapter this `kind` names refused* — are not about the egress or the era store, and `StartupError` is the publisher's whole document-loading enumeration. So the moved registry is `AdapterRegistry<E>`, over a trait `AdapterResolution` with one constructor per failure, and `dz-publisher-runtime` re-exports `type AdapterRegistry = AdapterRegistry<StartupError>`. Three properties fall out and all three are load-bearing:

- **A venue's `main` is unchanged**, because it names `AdapterRegistry` through the alias and never spells the parameter.
- **A venue's constructor is unchanged**, because the parameter is on the registry rather than on the closure: a constructor still returns `Result<Venue, AdapterInitError>`, whichever runtime is going to report its refusal.
- **A venue's registration function can be generic**, `fn register<E>(AdapterRegistry<E>) -> AdapterRegistry<E>`, and serve two runtimes from one line. A newtype in `dz-publisher-runtime` would have given the first two and not the third, and the third is the reason a recorder wants the seam at all.

The *default* on that parameter is a smaller thing than it looks and is worth stating so nobody removes the alias believing it redundant: Rust applies a struct parameter's default in type position only. `let registry: AdapterRegistry = AdapterRegistry::new();` takes it; `AdapterRegistry::new().open(&cx)` with nothing naming the error is `E0282`, and what makes that spelling compile for a publisher is the alias. The default's user is a caller with no startup enumeration of its own — a process composing a venue in order to record — and the crate's own suite is where that caller and the two messages its fallback formats are exercised, because nothing that publishes reaches either.

**The built-in moves with the registry**, because it is resolved inside `open` and because it is the one adapter that is nobody's venue code: `dz-adapter-uds` depends on `dz-adapter-core` and `thiserror` and on nothing else. A recorder driving records is the same composition a publisher driving records is.

**Prometheus is the price of `Venue::collectors`, and it is paid directly.** The field cannot be dropped: it exists precisely because the metrics registry cannot travel down into a constructor, so a venue hands its collectors up inside the value it returns, and splitting them out would put one composition in two places. It cannot be a trait of ours either — the runtime has to hand the same objects to a Prometheus registry, and a trait object of our own cannot be turned back into one. So the crate depends on `prometheus` rather than on `dz-publisher-metrics`, and re-exports it for the reason that crate does. What that avoids is the exposition server: `dz-publisher-metrics` carries `tiny_http`, and a crate that composes a venue in order to record has no port to serve.

**The `[adapter]` section arrives through a trait, so `AdapterConfig` stays.** `AdapterContext::new` reads four values out of it — the `kind`, the two free tables, and the replay block — and never the rest; the rest includes the fan-out key whose one method returns `StartupError` and takes a `PortRole`, which is the publisher's business and would drag the publisher's error back in. `AdapterSection` names those four, `AdapterConfig` implements it, and `AdapterContext::new(&document.adapter, …)` is the call it was. The document that carried the section is not one of the four, which is the whole content of the claim that a recorder can build the same context.

`ReplayConfig`, `Source`, `SourceRole` and `FeedSpec` move with the context that exposes them. Two of them carry a `resolve` that reported a `StartupError`; each keeps its message by returning a small error of the new crate's own that `dz-publisher-runtime` maps into the variant it always produced, so the operator-facing text is byte-identical and the two variants keep their fields.

### The crate is `dz-venue-composition`

Argued against the glossary's banned-word table the way `shard` was:

- **`venue`** is a defined term — *the external exchange or market operator* — and is banned in exactly one sense, "a Rust trait over product lines", with `product line` or `adapter` as the replacement. The `Venue` this crate holds is neither a trait nor a product line: it is the composed integration for one venue, an adapter and the transports it reads from, and it has carried that name in this tree since the registry existed.
- **`composition`** is undefined and unbanned, is what the operation is called in `dz-publisher-runtime::run` already, and does not collide with `Channel`, `Feed`, `Era` or any other defined term.
- `lane`, `stream`, `frame` and bare `source` are banned outright, which removes `dz-venue-lane`, `dz-adapter-stream` and every spelling built on `source`.
- `dz-venue-core` is refused on the tree's own convention rather than on the glossary: `-core` here means a boundary crate a venue implements against — `dz-adapter-core`, `dz-ingress-core`, `dz-edge-core` — and this crate implements nothing and is implemented by nobody.
- `dz-venue-runtime` is refused for the same kind of reason: a `-runtime` in this tree owns a process, and this crate opens no file, binds no socket and starts no runtime.
- `dz-adapter-registry` names one of the three types it holds and would sit in `adapter/` beside the boundary crate, which is the confusion the design's own sentence about "a boundary crate with a composition in it" is trying to prevent. It also depends on `dz-ingress-core`, which nothing in `adapter/` does.

It lives in a directory of its own, `rust/venue/`, which the public-repository check has to be told about: that script fails loudly for a scan root that has gone missing and says nothing at all about one that was never added.

**One field needs a decision and no code.** `AdapterContext::new` takes `&[FeedSpec]`, which reads as *what this publisher publishes*. For a process that records rather than publishes it is *what this process is recording*: the same question with a different verb, the same value, and the same refusal — an adapter that cannot answer a depth feed's snapshot must fail at startup either way. The field keeps its name and its doc gains the second reading.

### What the crate must not link, asserted rather than assumed

`dz-publisher-egress` and `dz-publisher-refdata` — the transmitters and the era store are in the first, the reference-data registry is the second — plus `dz-publisher-runtime` itself, read out of `cargo metadata`'s resolved graph with no network. A dependency shape is not observable from behaviour, which is why the assertion is on the graph: the tempting non-move, leaving the three types where they are and re-exporting them from the new crate, passes every behavioural test in the workspace and fails this one line.

## What this does not do

- **No recorder runtime.** Capturing the wire, driving a registry's adapter over a venue's transport, deriving both sides and writing the rows is a runtime and its own design. Moving the seam unblocks it and is worth landing alone, because a refactor with no behaviour in it is reviewable in a way a runtime is not.
- **No change to `state_key`'s value.** Every row already written and every pairing already computed stays byte-identical.
- **No new metric.** `upstream_identity` records nothing; it hands a value to a sink that may keep it.
- **No claim that a venue's symbol and a published symbol are the same string.** That is a reference-data fact about one venue and belongs in a column a query can see rather than in code that assumes it.

## Decisions

| Decision | Why |
|---|---|
| `book_key(top)`, with `state_key` folding the channel and the instrument into it | A key joined on across two observers has to be computable from a book; the publisher-side value must not move |
| A zero `source_count` is read as the absence in `book_key`'s subject alone | The two observers must agree on the field, and `state_key`'s value over given wire bytes may not move — so the fold and the derivation are both left as they are |
| The venue-observation design is corrected in the same change | It states the opposite of what the function does, and a design that cannot be implemented is worse than one that is missing |
| `Side::is_absent` stays private | `book_key` removes the reason to copy it out, and a copy that drifts is a race that stops pairing with no symptom |
| `upstream_identity` is defaulted and both fields are `Option` | A publisher ignores it, a recorder keeps it, and a venue that publishes one identifier and not the other is ordinary |
| The seam moves to `dz-venue-composition`, over the two boundary crates and the codecs | Composing a venue must not require linking the egress and the era store, and a boundary crate must not gain a composition |
| The registry is generic over the error it reports, defaulted, with `dz-publisher-runtime` aliasing it to `StartupError` | `open`'s two failures are not about the egress; a venue's `main`, its constructor and a generic registration function all stay writable once |
| The `[adapter]` section reaches the context through `AdapterSection` rather than by moving `AdapterConfig` | The constructor reads four values out of it, and the rest of the section is the publisher's — including the one method that returns `StartupError` |
| `Venue` keeps `collectors`, and the crate depends on `prometheus` rather than on `dz-publisher-metrics` | The field exists because the registry cannot travel down into a constructor; a metrics client is the cost, and the exposition server is what is avoided |
| The built-in record adapter moves with the registry | `open` resolves it, and `dz-adapter-uds` is `dz-adapter-core` and `thiserror` — the one adapter that is nobody's venue code |
| The dependency shape is asserted from `cargo metadata`, not assumed | The non-move that re-exports from the new crate passes every behavioural test in the workspace and fails exactly this one |
| `AdapterContext`'s feed set keeps its name | *What this process is recording* and *what this publisher publishes* are one question with two verbs and one value |

## Non-goals

A recorder runtime. Market-by-order, which needs a per-order identity no transport supplies. A second decoder of any venue.
