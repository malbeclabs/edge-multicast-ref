# A key a venue side can compute, and a seam a recorder can link — plan

Turns [the design](../specs/2026-09-09-book-key-and-the-composition-seam-design.md) into ordered tasks.

**Base:** this plan and its design ship in their own branch, with the code stacked on it. The venue-observation design task 1 corrects ships alongside. Independent of the shards work.

## The ordering constraint

Task 1 is a correction to a document that is wrong now, and it lands first because the rest of this plan is what makes that document implementable. Tasks 2 and 3 are additive and independent of each other. Task 4 is a refactor with no behaviour in it and touches the most files, so it lands last and alone.

## Global constraints

- **Vocabulary:** `GLOSSARY.md` governs every identifier, comment, test name and commit message. A venue's own session numbering is not a `sequence` in this repository's sense; `source` never appears bare.
- **Every change shown to kill its mutant**, and two of these have an obvious one: a `book_key` that still eats an identifier, and a `state_key` whose value moved.
- **`state_key`'s value may not change.** It is written into rows that exist and compared against rows loaded before this change.

---

## Tasks

### 1. The venue-observation design says what the function does

- [x] The sentence claiming `state_key` "is already transport-independent by construction" is replaced by what is true: it is transport-independent and observer-dependent, it eats the `channel_id` and the `instrument_id` before any price, and a venue side can compute neither.
- [x] The race view that design proposes keys on `book_key` and carries the symbol, since the symbol is the only instrument identity both sides have.
- [x] The column's own doc comment — "a hash over the instrument and both sides, and over nothing else" — is corrected where it overstates the function, and the correction says which of the two keys each sentence is about. **This bullet names a Rust file, not a document**, and it was missed when the rest of task 1 landed on the documents branch: `dz-recorder-rows/src/rows.rs`'s `BookTop::state_key` kept the overstatement while `dz-recorder-events` gained a correct one, so the tree briefly held both. It is corrected here, where the code is.

**Test:** none, and the plan says so. This is a document stating what a function does, and a test asserting a sentence against itself documents the sentence.

---

### 2. `book_key`, and `state_key` folded onto it

- [x] `pub fn book_key(top: &Top) -> u64` over the two sides, with the absent-side tag and the FNV-1a constants exactly as they are today.
- [x] `state_key` keeps its signature and its value, defined as the channel and the instrument folded into `book_key`'s subject.
- [x] Both doc comments say which question each answers: *the same state of this channel's instrument* against *the same book*.

**Test** (`dz-recorder-events`):
- **`state_key`'s value is unchanged**, asserted against literals computed before the split — the property everything already written depends on;
- two books that differ only in a side's price get different `book_key`s, and the same book twice gets the same one;
- an absent side and a side priced at zero get different `book_key`s, which is the tag the hash carries;
- **the same book under two different channels gets one `book_key` and two `state_key`s** — which is the whole point, stated as one assertion.

**The revert:** make `book_key` eat the channel. The last test fails. Before this task there is no key that can pass it.

**Run.** `book_key(channel_id, top)`, folding the channel ahead of the two sides, kills `one_book_under_two_channels_is_one_book_key_and_two_state_keys` and nothing else: one book on two channels came back as two keys. The second mutant the plan names — `state_key` eating the instrument before the channel — kills `state_keys_value_did_not_move` and nothing else. Neither mutant touches the other's test, which is the two questions being separable.

---

### 3. `EventSink::upstream_identity`, defaulted

- [x] `fn upstream_identity(&mut self, sid: Option<u64>, seq: Option<u64>)`, defaulted to ignoring both.
- [x] The doc says what a publisher does with it — nothing — and what a recorder does, and that neither value is ever a key, for the reason `event.upstream_ts` already carries.

**Test** (`dz-adapter-core`): a sink implementing nothing new compiles and inherits the default; a recording sink sees what an adapter passed. The first is the whole of "no adapter changes", stated as a compile.

**The revert:** remove the default. Every sink in the workspace fails to compile, which is a stronger signal than a test and is why the default is not optional.

**Run.** No named test dies, because nothing gets as far as running. `cargo check --workspace --all-targets --keep-going` reports `E0046 missing: upstream_identity` at six `impl EventSink` sites across four crates: `dz-ingress-core/src/driver.rs`, `dz-recorder-relower/src/relower.rs`, both sinks in `dz-adapter-uds`'s lowering test, and both sinks in `dz-adapter-core`'s own tests. The two remaining sinks in the workspace are test targets of crates whose library failed first, so the check never reached them.

---

### 4. The composition seam leaves `dz-publisher-runtime`

- [ ] A crate over `dz-adapter-core` and `dz-ingress-core` holding `AdapterRegistry`, `Venue` and `AdapterContext`. Not either boundary crate: one that gained a registry would be a boundary crate with a composition in it.
- [ ] `dz-publisher-runtime` re-exports all three, so no venue's `main` and no in-tree caller changes.
- [ ] `AdapterContext`'s feed set keeps its name and gains the second reading in its doc: *what this process is recording* is the same question as *what this publisher publishes*, with the same value and the same refusal.
- [ ] The new crate's own documentation states what it is for, which is that two runtimes compose a venue the same way.

**Test:** the existing `adapter_registry.rs` suite moves with the types and passes unchanged — this task is a refactor and that suite is its gate. Plus one new assertion that the new crate's dependency graph contains neither the egress nor the reference-data registry, which is the whole reason it exists; `cargo metadata` is where that is readable without a network.

**The revert:** leave the types in `dz-publisher-runtime` and re-export from the new crate instead. The dependency assertion fails, and nothing else does — which is the point of asserting the graph rather than the behaviour.

---

## Acceptance

The plan is done when:

1. a key computable from a book alone exists, and `state_key`'s value has not moved;
2. the venue-observation design describes the function that exists;
3. an adapter can state a venue's own message identity and a publisher's sink ignores it;
4. a crate can compose a venue without linking the egress, the transmitters, the era store or the reference-data registry;

and when reverting task 2's split makes a named test fail.

## What this plan does not do

It does not build the recorder runtime that would use all four. That is its own design: capture the wire in socket mode, drive the registry's adapter over the venue's transport, derive both sides, key them, write the rows. Moving the seam is what unblocks it and is worth landing alone, because a refactor with no behaviour in it is reviewable in a way that a runtime is not.
