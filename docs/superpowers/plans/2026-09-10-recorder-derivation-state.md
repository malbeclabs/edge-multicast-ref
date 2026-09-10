# Edge Recorder: derivation state that outlives one call — Implementation Plan

**Date:** 2026-09-10

**Goal:** Add an entry point to `dz-recorder-events` that takes the derivation's state by reference, so a caller cutting arrivals into windows keeps the reference data, the book and the snapshot-id attribution map across the cut instead of starting empty once per window.

**Architecture:** The fold is unchanged. What changes is where its state is constructed: `derive_events` builds it, folds, and ends the object; `derive_events_into` is handed it and folds. `derive_events` becomes `DerivationState::new()`, one call into the new entry point, and `close_object()` — so the archive path is unchanged by construction rather than by review.

**Tech Stack:** Rust 2021, no new dependency. `dz-recorder-core` (`Source`), `dz-recorder-relower` (the archive walk and `WireCapture::datagrams()`), `dz-recorder-rows` (the row types, untouched).

**Spec:** `docs/superpowers/specs/2026-09-10-recorder-derivation-state-design.md`. The three semantics — the per-call `book_refused` delta, `close_object` at the end of a derivation only, and a `datagram_index` that continues across calls — are decided there and are not re-litigated here.

**Scope:** The entry point. Decision 4 of `2026-09-06-recorder-market-data-rows.md` deferred four things together; this is the first, and the loader holding state between objects, the ledger column with its migration, and the out-of-order guard stay deferred.

---

## Global constraints

- **Nothing here touches the record path**, and nothing here touches `dz-recorder-load`. A change that appears to need one is a signal that state is being put in the wrong process.
- **No existing test is changed or removed.** `derive_events` keeps its signature and its behaviour, so the crate's suite passing unmodified is the check that this is an addition. A task that wants to edit an existing assertion has changed semantics and must stop.
- **No row type, column, refusal or refusal name changes.** Two doc comments are corrected because persisting the state makes them false.
- **Every task is verifiable with no server, no socket and no privileges**, over the synthetic publisher the crate's tests already use.
- **Each of task 5's cases must fail against today's `derive_events` when the bytes are split**, and the plan records which assertion failed and how. A case that passes both ways documented the tree instead of changing it.

---

## Tasks

### 1. `DerivationState`, and `derive_events` rebuilt on top of it

`DerivationState` holds the three pieces that cross a call — the `InstrumentTable`, the `Book`, and the `(ChannelInstance, snapshot_id)` attribution map `instrument_of_state` maintains — plus the two counters tasks 2 and 3 add. `Default` and `new()`.

`derive_events_into(state, source, input)` is today's fold body with the three constructions removed and the trailing `close_object()` removed. `derive_events` becomes exactly: construct, call, `close_object()`, return.

`seen` and `at_datagram` stay local to the call, for the reasons the spec gives: the first is the instrument grain for definitions observed in this call, and the second must reset so that the first datagram of a window is tested against the previous window's high-water mark.

**Verification:** `cargo test -p dz-recorder-events` green with no existing test touched. One new test asserts that `derive_events` over an input and `derive_events_into` over the same input with a fresh `DerivationState` plus a `close_object()` produce equal `event`, `book_top` and `instrument` rows and equal counters — the two paths are one path, asserted rather than assumed.

### 2. The datagram base, so `datagram_index` continues

`DerivationState` carries `datagrams: u64`. `derive_events_into` adds it to `provenance.datagram_index` where rows are built, and advances it by `WireCapture::datagrams()` after the fold — that being the count of what the index indexes into, foreign and undecodable datagrams included.

**Verification:** a test splits an input at a datagram boundary and asserts the second half's rows carry the indices they carried in the whole. Under the revert — the base dropped, or advanced by the message count instead of `datagrams()` — the second half's indices restart at 0 or skip the datagrams that yielded no message, and the test names which.

### 3. `book_refused` as the per-call delta

`DerivationState` remembers the previous `Book::refused`. `derive_events_into` reports the difference. `Book::refused` stays cumulative and public, unchanged.

**Verification:** a test derives two windows into one `DerivationState` where the first strands a cycle and asserts the second window's `book_refused` does not re-report the first's. A second asserts the deltas sum to the cumulative total on the book. Under the revert both windows report the running total and the sum double-counts, which is the caller bug the spec names.

### 4. `close_object` reports what it counted, and two doc comments stop being false

**Corrected while the task was being written.** An earlier draft of this plan had `Book::close_object` return the `BookRefused` it closed over. What landed is `DerivationState::close_object` returning the refusals the derivation is responsible for since its previous call, and `Book::close_object` keeping its signature — a live caller still has the number it needs, and the per-call currency stays in the one place that owns it. Returning a just-closed figure from the book as well would put two currencies on one counter, which is the confusion decision 1 exists to remove. The subtraction is a free function, `refused_since`, so the fold and the close share it rather than stating it twice.

`BookRefused::unclosed_cycle`'s comment argues that non-zero and persistent means the anchoring is losing a race against object rotation. That holds for an archive object and is wrong for a window, where the counter rises once per boundary per open cycle as a matter of course. The comment states both, and says that a live caller calls `close_object` at the end of the derivation rather than per window. `Book`'s own comment says it is "for every channel instance in one object", which persisting it makes false.

**Verification:** `cargo test -p dz-recorder-events`. The doc change is checked by `cargo test --doc` compiling its example, and by review against the spec's semantics 2.

### 5. Splitting is a no-op: the harness and the four cases

`tests/common/mod.rs`'s `DatagramLog` takes a `Vec<OwnedDatagram>`, so the harness is: build one log, derive it whole, then split the vector and derive the halves into one `DerivationState`.

A helper asserts the criterion — `event` and `book_top` equal and in order, counters summing, and `instrument` equal after the `ReplacingMergeTree(last_seen_ts)` reduction the spec states, because `seen` is per call by design and the store collapses on that version column.

The four cases: a definition in the first half with its quotes in the second; a snapshot cycle straddling the cut; a `mktdata` sequence gap straddling the cut; and a restatement in the second half applying only to the prices after it.

**Verification:** each case asserted through the new entry point, and each shown failing under today's `derive_events` over the same split bytes, with the failure recorded here per case.

---

## What died under which revert

The gates were run against each of these with everything else in place, and the
test named is the one that failed. A revert that killed nothing would mean the
task had documented the tree.

| Revert | What failed |
|---|---|
| The state does not cross the call — `derive_events_into` resets it on entry | all four split cases, and `the_datagram_index_continues_across_a_call` |
| The datagram base is not applied to provenance | `the_datagram_index_continues_across_a_call`, and all four split cases through row equality |
| The base advances by the message count instead of `WireCapture::datagrams()` | the same five: a datagram carrying no message stops being counted, so the halves disagree |
| `book_refused` reports `Book::refused` raw instead of the delta | `book_refused_does_not_re_report_an_earlier_windows_refusal` (1 where 0 is owed), `the_archive_path_is_the_new_entry_point_over_fresh_state` (2 where 1 is owed), and the existing `an_incomplete_cycle_is_refused_rather_than_applied` and `a_snapshot_in_flight_when_a_reset_was_published_is_refused` |
| `derive_events` reports `close_object()` alone rather than composing it with the fold's delta | `the_archive_path_is_the_new_entry_point_over_fresh_state`, and the same two existing book tests — this one was a real bug during implementation, caught by the existing suite before the new tests existed |

### The review's own reverts

Four more, from reviewing the landed code rather than from writing it.

| Revert | What failed |
|---|---|
| The attribution map is never pruned | `a_level_after_its_cycle_ended_in_an_earlier_window_is_an_orphan` |
| The map is pruned on the `SnapshotEnd` itself rather than at the call's end | the **existing** `a_complete_cycle_states_what_it_promised_and_what_it_carried` — `state_row` reads `levels` off that entry for the end row's `levels_seen`, so marking rather than removing is required and not a preference |
| `close_object` leaves the attribution map alone | `ending_the_derivation_clears_the_cycles_still_in_flight` |
| `table()` reports an empty table rather than the derivation's | `the_reference_data_is_readable_per_window` and `a_source_that_tears_folds_nothing_and_leaves_the_state_usable` |

The second row is the one worth keeping: the obvious fix for an unpruned map
breaks a column the archive path already writes, and the existing suite says so.

The last row is the reason task 1's verification compares the two entry points
over a fixture that strands a cycle: with the delta subtracted in one place and
the close added in another, a figure reported twice or not at all is invisible
to a fixture whose counters are all zero.

## What is not in this plan

- **The loader holding state between objects, the ledger column and its migration, and the out-of-order guard.** The rest of the deferred decision. Each wants its own review, and none is reachable without this entry point.
- **Any caller, and any window length.** Nothing here cuts a window. The spec's phase hypothesis says a recommended length would be a coincidence until `InstrumentTable::defined_count` per window confirms or kills it, and that measurement is not in this plan either.
- **Inline mode's own period.** The same shape at a longer period, named in the spec so it is not discovered later, and changed by nothing here.
