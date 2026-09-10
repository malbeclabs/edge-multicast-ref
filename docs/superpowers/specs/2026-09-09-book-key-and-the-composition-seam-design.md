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

The book-only key hashes the two sides and nothing else. `state_key` keeps its signature, keeps its value, keeps every caller, and is the channel and the instrument folded into `book_key`'s subject.

**The two keys are different values, and that is what they are for.** `state_key` answers *is this the same state of this channel's instrument*. `book_key` answers *is this the same book*. The first is what two recorders of one feed pair on; the second is what two observers of one market pair on. A view joining across the two sides uses `book_key` and carries the symbol beside it, because the symbol is the only instrument identity both sides hold.

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

`Venue` holds an `Adapter` and a set of `Input`s: one type from `dz-adapter-core`, one from `dz-ingress-core`. Neither is the home, because a boundary crate that gained a registry would be a boundary crate with a composition in it. The home is a small crate over both, and `dz-publisher-runtime` re-exports what it exports today so that no venue's `main` changes.

**One field needs a decision and no code.** `AdapterContext::new` takes `&[FeedSpec]`, which reads as *what this publisher publishes*. For a process that records rather than publishes it is *what this process is recording*: the same question with a different verb, the same value, and the same refusal — an adapter that cannot answer a depth feed's snapshot must fail at startup either way. The field keeps its name and its doc gains the second reading.

## What this does not do

- **No recorder runtime.** Capturing the wire, driving a registry's adapter over a venue's transport, deriving both sides and writing the rows is a runtime and its own design. Moving the seam unblocks it and is worth landing alone, because a refactor with no behaviour in it is reviewable in a way a runtime is not.
- **No change to `state_key`'s value.** Every row already written and every pairing already computed stays byte-identical.
- **No new metric.** `upstream_identity` records nothing; it hands a value to a sink that may keep it.
- **No claim that a venue's symbol and a published symbol are the same string.** That is a reference-data fact about one venue and belongs in a column a query can see rather than in code that assumes it.

## Decisions

| Decision | Why |
|---|---|
| `book_key(top)`, with `state_key` folding the channel and the instrument into it | A key joined on across two observers has to be computable from a book; the publisher-side value must not move |
| The venue-observation design is corrected in the same change | It states the opposite of what the function does, and a design that cannot be implemented is worse than one that is missing |
| `Side::is_absent` stays private | `book_key` removes the reason to copy it out, and a copy that drifts is a race that stops pairing with no symptom |
| `upstream_identity` is defaulted and both fields are `Option` | A publisher ignores it, a recorder keeps it, and a venue that publishes one identifier and not the other is ordinary |
| The seam moves to a crate over the two boundary crates | Composing a venue must not require linking the egress and the era store, and a boundary crate must not gain a composition |
| `AdapterContext`'s feed set keeps its name | *What this process is recording* and *what this publisher publishes* are one question with two verbs and one value |

## Non-goals

A recorder runtime. Market-by-order, which needs a per-order identity no transport supplies. A second decoder of any venue.
