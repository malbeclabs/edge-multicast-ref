# Edge Recorder: market data as rows — Implementation Plan

**Date:** 2026-09-06

**Goal:** Turn an archive into market data rows — `event`, `instrument`, `book_top` — so that a feed in the family's price-level message set becomes rows by being recorded, with no per-venue capture process, no venue client, and no change to the record path.

**Architecture:** The shape the loader already has. Derivation is pure and sink-agnostic, exercised in CI against the synthetic publisher with no socket, no privileges and no server; the column store is one `RowSink` and the file sink is the other. What is new is a *stateful* derivation — a book — where everything before it was a fold over one object.

**Tech Stack:** Rust 2021. `dz-recorder-replay` (reading), `dz-recorder-core` (`Source`, `PortRole`, `RecordedDatagram`), `dz-recorder-relower` (the archive walk, widened), the `dz-edge-*` codec crates, `dz-recorder-rows` and `dz-recorder-clickhouse` (row model and sink, already landed). No async runtime, no ORM, no migration framework.

**Spec:** `docs/superpowers/specs/2026-09-05-recorder-market-data-rows-design.md`. The three tables, the equivalence key, the two anchoring rules and the retention split are decided there and are not re-litigated here.

**Scope:** That spec's *What this needs that does not exist yet*, in full. The conformance runner over decoded messages is explicitly out — it is a non-goal there and stays one here.

---

## Four decisions to settle before task 1

The spec leaves each of these open, and each one changes the shape of more than one task.

### 1. The deriver reuses `WireCapture`, widened. It does not get its own walk.

`MessageBody`'s four variants are a statement about what can be **compared**, not about what can be **decoded**: a re-lowering joins only the messages a venue event produces, and `Heartbeat`, the reference data and the snapshot triple are excluded for reasons that are correct and specific. Widening that enum would make the exclusion implicit rather than stated.

So `WireCapture` keeps `messages()` and its join semantics **exactly as they are**, and gains a second accessor for the messages it currently only counts. The alternative — a second walk in a new crate — duplicates the framing, the `Magic` check and the three skip classes, and those are precisely the parts that must never diverge between two readers of one archive.

### 2. The era-scoped accumulator is new, and it does not live in `dz-recorder-relower`.

`ArchivedRefdata` keys `by_symbol` and pins the first definition, raising `ScaleRestated`. That is right for a re-lowering, which holds two archives whose clocks belong to a subscriber and a publisher with no key ordering one against the other. A deriver holds one archive in which every definition arrives at a sequence number, so it can place a restatement exactly — and it must, because the symbol key is the thing the spec argues against.

The new accumulator is keyed `(channel_id, instrument_id)`, scoped to an era, and takes each definition **with its provenance** so the restatement has a position. `ArchivedRefdata` is untouched.

### 3. `SnapshotLevel` feeds the book always, and is persisted only when asked.

A snapshot cycle is `total_levels` messages per instrument per cycle. Persisting every one of them puts the largest row count in the system on the port role with the least analytical value per row, and it does it on the publisher's cadence rather than on the market's.

**The book consumes every level; `event` persists them behind a per-feed switch that is off by default.** `SnapshotBegin` and `SnapshotEnd` are always persisted, so a cycle is always visible as a row even when its levels are not.

This costs one column that the spec does not have: `SnapshotEnd` rows carry **`levels_seen`**, the count the deriver actually observed, so that `total_levels` on the begin row and `levels_seen` on the end row answer "was the snapshot complete" from rows alone — which is the question persisting the levels would otherwise have been the only way to ask. **If this decision is accepted, that column is a one-line addendum to the spec's mapping table and DDL**, and task 4 makes it.

### 4. The book is stateful across objects, and the ledger says as of when.

Everything derived so far folds over one object. A book does not: an anchor in object *n* is what makes object *n+1* certain, and objects arrive one at a time.

So the deriver holds book state **per `(feed, channel instance, instrument)`**, advanced in object order, and the ledger records the object the state is valid as of. Three consequences, all of which are cheaper to accept now than to discover in task 6:

- **An object that arrives out of order does not rewind the book.** It is loaded for `event` rows and the book is marked uncertain from it, because a book rebuilt backwards is a book nobody can reason about.
- **A restart re-anchors rather than resumes.** Book state is not persisted between processes; the first cycle after a restart is what makes it certain again, and until then `book_certain = 0` with `no_anchor`.
- **`Quote` needs none of this.** A quote-only feed is stateless and is unaffected by every line above, which is the second reason decision 1 of the spec matters.

---

## Global constraints

- **Nothing here touches the record path.** No change to `dz-recorder`, `dz-recorder-capture` or `dz-recorder-archive` is in scope. A change that appears to need one is a signal that the derivation is being put in the wrong process.
- **`WireCapture::messages()` is unchanged, and a test asserts it.** The relower's existing suite passing is the check that widening did not alter the comparison.
- **Every task is verifiable with no server and no network** except where a task says otherwise, and the ones that need a column store are feature-gated behind the container the demo already provisions.
- **Derivation is per feed and off by default**, at every stage. No task turns it on for anything.

---

## Tasks

### 1. Widen `WireProvenance` to carry the identity block

`RecordedDatagram` carries `src`, `dst` and `recv_ts_kind`; the walk drops all three. Every one is in the spec's identity block, so nothing can be written until they survive it.

Add `src`, `dst` and `recv_ts_kind` to `WireProvenance`, populate them in `absorb_datagram`, and correct the type's doc comment: it says every field is one a batching or pacing decision moves, which was true and stops being true here. These three are identity, not timing, and they are still never compared.

**Verification:** `cargo test -p dz-recorder-relower` green with no test changed — the walk's output is a superset. One new test asserts that a datagram recorded from a known address and port produces provenance carrying them, and that two datagrams differing only in source address produce provenance that differs.

### 2. Surface the messages the walk only counts

`InstrumentReset` does not appear in `absorb_message` at all and falls to `unknown_type`; the snapshot triple is counted as `skipped.snapshot`. Both are needed and both are already decodable by the codec.

Add a second output to `WireCapture` — a `Vec<StateMessage>` beside `messages` — carrying `InstrumentReset`, `SnapshotBegin`, `SnapshotLevel` and `SnapshotEnd` with their provenance. The `Skipped` counters keep counting them, because a re-lowering's report is about what it did not compare and that has not changed.

**Verification:** `cargo test -p dz-recorder-relower`. New tests: a reset is no longer `unknown_type`; a complete cycle appears in order with one begin, `total_levels` levels and one end; an incomplete cycle appears as what it is rather than being repaired.

### 3. `dz-recorder-events`: the era-scoped reference data

A new crate. `InstrumentTable` keyed on the **channel** — `(source address, Channel ID)` — within an era, taking definitions with provenance, restating exponents forward, and resetting at an era boundary. Resolves `source_id`, `price_exp` and `qty_exp` for the messages that do not carry them.

**Corrected while task 5 was being written**, and the correction is in the spec: the key is the channel and not the channel instance, and a statement is positioned by *arrival time* rather than by sequence number. Definitions arrive on `refdata` and prices on `mktdata` — two instances, two sequence spaces — so a sequence-number position orders them against a ruler they do not share, and a key holding the port files the definitions where the prices can never find them.

**Verification:** unit tests with no archive. A restatement mid-window applies the old exponents before its sequence number and the new ones after — the case `ArchivedRefdata` deliberately cannot answer and this one must. A symbol reused across eras resolves to two instruments, not one.

### 4. The row types, and migration `005`

`Event`, `Instrument` and `BookTop` in `dz-recorder-rows`, with the spec's DDL as `005_recorder_market_data.sql` in `dz-recorder-clickhouse/db/clickhouse/`, embedded and added to `migrations()` the way the existing five are. Includes the `levels_seen` column from decision 3 and the spec addendum that goes with it.

The sentinel translation lives here and nowhere else: `U16_UNAVAILABLE` on `order_count` and `level_index` serialises as `NULL`.

**Verification:** `cargo test -p dz-recorder-rows` for the serialisation, including that the sentinel round-trips as null and that a `0` order count does not. `cargo test -p dz-recorder-clickhouse` for the DDL splitting, as the existing migrations are tested.

### 5. Event derivation

The fold: walk an object, join each message to the `InstrumentTable`, emit `Event` and `Instrument` rows. Pure, sink-agnostic, no book.

**Verification:** golden tests over the synthetic publisher in `dz-recorder-replay`. Every message type in the mapping table gets a fixture and an asserted row, including the two that carry no `Source ID` and the level that carries no instrument. A message whose `snapshot_id` matches no open begin is refused rather than attributed.

### 6. The book, and certainty

Two derivations, as the spec has them: `Quote` self-anchoring, and a delta book anchored only on a complete cycle whose `anchor_seq` satisfies any preceding `InstrumentReset`. `book_certain` falls to 0 on a sequence gap or a reset and is restored only as the spec allows for each derivation. A certainty transition emits a row.

**Verification:** golden tests over the `faults` fixtures that already exist — a gap, a reset, a backward run — each with an asserted `book_certain` sequence. One fixture asserts the case the whole design exists for: a gap, then no price movement, then a query, and the answer is uncertain rather than stale. One asserts that a snapshot in flight when a reset was published is refused.

### 7. `state_key`, and the pairing view

The hash over the spec's tuple, and the occurrence-ordinal pairing as a view over `book_top`.

**Verification:** the three stability tests the spec names — unchanged across a schema version bump, across a change in batching, and across two observation points decoding one state. One test asserts a repeating state pairs one-to-one and leaves an unpaired occurrence visible rather than double-counting.

### 8. Wiring, and the switch

The per-feed derivation switch in `dz-recorder-load`'s configuration, the per-feed level-persistence switch from decision 3, and a derivation lag metric distinct from the datagram load's.

**Verification:** an end-to-end test in `dz-recorder-e2e` — encode with the real encoder, record with the real writer, derive with the real deriver into `FileSink`, assert the rows against what was encoded. Then the same against the container, feature-gated. A test asserts the switch off produces no market data rows and does not change the datagram rows.

### 9. The sizing measurement

Messages walked over datagrams walked, per feed, over a window including a burst and a snapshot cycle — the number the spec says must exist before a feed's derivation is enabled. A subcommand or a test-only harness, not a service.

**Verification:** run against a recorded archive and report. This task produces a number, not a behaviour, and the number is what a deployment decision is made against.

---

## What is not in this plan

- **The conformance runner over decoded messages.** A non-goal in the spec.
- **Order-level reconstruction.** A different message set and a different document, as the spec's last non-goal says.
- **Any deployment.** No Ansible, no release, no account. The loader's own deployment does not exist yet either, and that is a prerequisite this plan inherits rather than solves.
