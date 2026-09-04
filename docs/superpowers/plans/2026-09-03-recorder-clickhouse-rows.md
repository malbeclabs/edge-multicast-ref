# Edge Recorder: the rows, and the loader that writes them — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Turn an archive into rows a dashboard can ask, in a column store, without the record path learning what a column store is. One pure crate that derives the rows, one crate that writes them, one binary that walks a directory of completed objects and is idempotent on `(object key, sha256)`.

**Architecture:** The loader is a *separate process from the recorder* and reads the archive through the same `Source` the live capture presents, so the derivation is exercised in CI against a synthetic publisher and needs no socket, no privileges and no server. Row derivation is pure and sink-agnostic; the column store is one implementation of a `RowSink` behind a trait, and a file sink is the other, which is what makes the golden tests possible.

**Tech Stack:** Rust 2021. `dz-recorder-replay` (reading), `dz-recorder-loss` (gap detection, already landed), `dz-recorder-core` (`Source`, `PortRole`, identity), `serde_json` (JSONEachRow), `ureq` or `reqwest` blocking (HTTP), `thiserror`. No async runtime. No ORM, no schema migration framework — the DDL is checked-in SQL applied by hand or by the deploy, as the demo's schema already is.

**Spec:** `docs/superpowers/specs/2026-08-31-sequence-loss-and-conformance-rows-design.md` — the four grains, the era numbering, the verdicts and the retention split are decided there and are not re-litigated here.

**Scope:** The spec's *What this needs that does not exist yet*, minus the conformance runner. This is the loader half of plan 3 of the recorder design; the state machine, book, fingerprint and the conformance replay are the other half and land beside it, not inside it.

---

## The table-shape question, settled

**One generic set of tables, with the feed as a column. Never a table per feed.**
The spec's DDL already commits to this and the reasons are worth restating,
because "a table set per feed" is the intuition a reader arrives with:

- **The order key is the channel instance**, `(source_addr, channel_id, dst_port, …)`.
  A per-feed table moves the discriminator into the table *name*, where no query
  can range over it: every cross-feed question becomes a `UNION` written by hand,
  and the join that isolates publisher loss — the same datagram seen from
  somewhere else — becomes a fan-out over the product of feeds and sites instead
  of a scan along one key.
- **`feed` is `LowCardinality(String)`**, which is dictionary-encoded per part.
  The column costs approximately nothing, and pruning happens on the partition
  date and the order key, not on a table name.
- **The grain, not the venue, is what earns a table.** The four grains exist
  because each answers what the one above it cannot. A feed is a *value* at
  every one of those grains.
- **Label parity with the health tier is a stated requirement**: the metrics are
  labelled `site`, `recorder`, `feed`, `channel`, `role`, `source`, and the
  columns carry those same names for those same things. Splitting by feed in one
  half and not the other is how the live panel and the historical panel start
  disagreeing about what a channel is.
- **The count of lanes is the argument's other end.** A recorder in a live
  deployment already carries a dozen feeds and the capture beside it reads
  dozens more; per-feed tables multiply every future materialised view and every
  TTL by that number, and many small parts is the access pattern this engine is
  worst at.

**Where a per-shape table *is* right, and it is not per feed:** the decoded
message grain. A top-of-book quote row and a level row have genuinely different
columns, so those are separate tables *per feed flavour* — the same split the
existing per-event tables already made. That grain is out of scope here; nothing
in this plan decodes a payload.

---

## Global Constraints

- **Vocabulary:** `GLOSSARY.md` in `edge-feed-spec` governs every identifier, column, test name and commit message. `datagram` never `frame`; `era` never `epoch`; `port role` with the tokens `mktdata`/`refdata`/`snapshot`; `channel` only for the `Channel ID` shard.
- **No venue names.** This repository is public. No commit message, comment, column, test name, fixture, dashboard or config example in this plan names a venue, a venue repository, a venue crate, an issue tracker, a metro, or gives a count of publishers or of recorder sites.
- **The record path is not touched.** No new key in `RecorderConfig`, and in particular no endpoint, credential or database key: the configuration crate documents the absence of exactly those keys as an invariant, because the recorder does not upload. The loader has its own configuration file, its own service user and its own metrics port.
- **Nothing in the loader may block the recorder.** They share only a directory, which the loader opens read-only, and a load ledger the loader owns. A column store that is down, slow or full must cost loading progress and nothing else.
- **The expensive table carries only what its own object states.** Derived numbers live in the derived tables. A column in `datagram` that depends on another object is a column a backfill silently invalidates.
- **The derived tables are written by the loader, never by a refreshable materialized view.** A refresh executes wholly on one replica under a Keeper lock, is never distributed, and replica placement is skewed rather than randomised, so a cohort of refreshes concentrates on one replica — measured at roughly 1.7x its peers' load on the destination cluster, and the reason that service sat at its maximum size for six days. The upstream fix is open and unmerged, and does not help a view without `RANDOMIZE FOR` anyway. Writing rows from a loader sidesteps all of it.
- **Idempotence is a property, not a procedure.** Loading the same object twice produces the same rows, and `ReplacingMergeTree` keyed on `(object_key, object_sha256)` is what makes a re-run after an analyser fix a replace rather than a duplication.
- **Lints:** `#![forbid(unsafe_code)]` and the workspace clippy set.

---

## Two decisions the spec leaves contradictory, to settle before task 1

Both are one paragraph of the spec disagreeing with another, and both change the
schema, so they are decided here and reviewed with the plan rather than
discovered in task 4.

**1. `era_index` in `datagram`, or a range join to `era`?** The DDL lists
`era_index UInt32 -- the era, assigned by the loader` and puts it in the sort
key; *Numbering the eras* then says the index is a `dense_rank` over the `era`
table and that "a `datagram` row joins to its era by range … the rows themselves
carry only what the object states". These cannot both hold: a stored rank is
renumbered by any later-arriving *earlier* object, which is exactly what a
backfill or a recovered segment is, and renumbering a column inside the sort key
of the largest table is a rewrite.

**Recommendation:** `datagram` carries `reset_count` and `segment_seq` and no
`era_index`; the sort key becomes
`(source_addr, channel_id, dst_port, sequence_number, site)`; the era is
resolved by range join at query time, and `sequence_gap` — small, derived,
re-derivable — carries the resolved `era_index` and `anchor_certain` so the
panels pay nothing. This follows the spec's own stated principle over its own
DDL.

**2. What the loader does with an object whose predecessor is missing.** The
adjacency check needs the immediately preceding segment's `reset_counts_seen`;
under a staging budget that evicts, the predecessor is routinely gone. The spec
settles the *reporting* (`anchor_certain = false`, gaps touching it are
`unverifiable`) but not the *ordering*: a loader that must see segment *n−1*
before it can anchor segment *n* is a loader that stalls on the first eviction.

**Recommendation:** the loader never waits. It writes the `era` row with
`anchor_certain = false` immediately, and a later load of the missing
predecessor rewrites that one row — `ReplacingMergeTree` on the `era` table,
keyed on the anchor. Evidence arriving late upgrades a verdict; its absence never
blocks one.

---

## The pieces where the obvious implementation is the wrong one

| Piece | Why it is not obvious |
|---|---|
| Span minus count, at the datagram grain | It is valid *here* and invalid one grain up. One row per datagram means `max(sequence) − min(sequence) + 1 − count()` is loss; against a decoded per-message table it is not, because messages that carry no quote still consume sequence numbers, and that subtraction then reports a fixed fraction of every feed as missing at every site at once. The golden test asserts the datagram grain includes heartbeat-shaped datagrams for exactly this reason. |
| `drop_scope` | The recorder's own loss is counted per port role in one mode and per capture handle in the other, and the archive declares which. Subtracting a handle-scoped count from one role's gaps is arithmetic nobody can see is wrong. |
| `recv_ts_kind` | A latency from an application-fallback stamp measures our own scheduler. The column exists so a panel can exclude it; averaging the two kinds measures nothing. |
| Reordering | A gap counter is an upper bound until reordering is subtracted. The row set must let a query recover set-truth, which the live counters cannot. |
| The manifest hash | A finding drawn from an object whose sha256 was never checked is a finding about a file, not about a feed. Verification is part of loading, not an operator's habit. |
| Wrapped `Reset Count` | Two eras sharing a wire value hide every gap between them. Measured: partitioning by the wire value detects zero gaps where a monotonic index detects five missing datagrams. |
| The loader's lag against eviction | Objects are deleted under the staging budget, so a loader slower than the write rate loses history permanently and silently. Lag is a first-class metric with an alert, not a log line. |

---

## The write pattern, decided here rather than during task 3

Row volume is not the constraint: on the order of 100 million rows a day from one
host is under one percent of the busiest table on the destination cluster.
**Merge pressure is the constraint, and it is set by rows per part, not rows per
day** — and merge work is invisible in the query log, showing up only as the
difference between the provider's CPU graph and query-attributed CPU, so a chatty
inserter raises it silently.

So the batch is a number in the sink configuration, not a sentiment:

| Key | Default | Why |
|---|---|---|
| `insert_max_rows` | 1,000,000 | An object's rows land in one or two parts. The busiest lane measured on a live recorder is 224,000 datagrams a minute, which is about 1.1 million rows in a time-rotated object. |
| `insert_min_rows` | 50,000 | Rows from several objects coalesce into one insert rather than each becoming a part. The quietest lanes measured run 130-150 datagrams a minute — about 700 rows an object — and one part per object per lane is the pathological profile the cluster already has two examples of. |
| `insert_max_delay` | `15m` | The bound on coalescing, so a quiet lane is late rather than absent. At the rates above the worst case is roughly 2,000 rows a part. |
| `insert_max_bytes` | `256MiB` | Not a merge-pressure bound and not in the reasoning above: a row count says nothing about a row's width, and the widest grain carries an object key and two digests. This is what keeps one request reasonable whatever the row count says. |

Never per datagram, and never per row. One insert per grain: a batch spanning
grains is refused, while a batch spanning objects is ordinary, because
`ReplacingMergeTree` dedups on `(object_key, object_sha256)` and the load ledger
marks an object loaded only once every grain carrying its rows has landed.

Steady state per host is then on the order of a couple of thousand inserts a day
across every grain and lane, with rows per part between about 2,000 on the
quietest lane and about a million on the busiest — better on both axes than the
best-behaved high-volume table on the destination cluster, which sustains 166,000
inserts a day at 78,700 rows a part.

---

## Tasks

### 1. `dz-recorder-rows`: the row types, pure

- [x] New crate `recorder/dz-recorder-rows`, added to the workspace members.
- [x] `Datagram`, `Era`, `SegmentCoverage`, `SequenceGap`, `ConformanceFinding` as plain structs with `serde` derives, field names exactly the column names.
- [x] A `RowSink` trait, with one method per grain refused — a batch spanning grains is one unit of idempotence. **As built the signature is `write_batch(&mut self, rows: RowBatch, now_ns: u64) -> Result<Accepted, RowSinkError>`, plus `post_if_due` and `flush`, and the reason is the write pattern below:** once a sink coalesces across objects, `Ok` no longer means the rows are in the store, so it returns which objects *landed* and the loader records those. The clock is a parameter for the same reason the archive writer's `rotate_at` takes one — an age bound that read a clock inside could not be tested without sleeping.
- [x] `FileSink`: newline-delimited JSON per grain into a directory. This is the CI sink and the `--dry-run` sink.
- [x] Unit tests: every struct round-trips through JSON with the column names the DDL uses, asserted against a literal, so a rename cannot pass.

**Verification:** `cargo test -p dz-recorder-rows` with no server and no network.

### 2. Derivation from an archive, over `Source`

- [x] `derive(source, manifest) -> RowBatch`: one `Datagram` row per archived datagram, reading the 24-byte header only. No message walk.
- [x] `SegmentCoverage` from the manifest, including `reset_counts_seen` and the per-instance first/last sequence, so a coverage question opens no object.
- [x] Manifest sha256 verified before any row is derived; a mismatch is a refusal naming the object, never a partial load.
- [x] `Era` rows from reset transitions plus the adjacency check against the previous segment's coverage, setting `anchor_certain`.
- [x] `SequenceGap` rows from `dz-recorder-loss`, which already decides which sequence values nobody delivered — this task wires it, it does not reimplement it.
- [x] Verdicts assigned per the spec, with `seen_elsewhere` left absent rather than guessed when the cross-site window is incomplete.

**Verification:** golden tests over the synthetic publisher in `dz-recorder-replay`. The existing `faults` fixtures — a gap, backward motion, a reset, a new source, a duplicate, a reordered pair, an oversized declared length, an unknown schema version, a silent channel — each get an asserted row set. Add one fixture that is a clean segment carrying heartbeat-shaped datagrams and assert `span − count == 0` for it.

### 3. `dz-recorder-clickhouse`: the sink

- [x] New crate `recorder/dz-recorder-clickhouse` implementing `RowSink` over HTTP with `JSONEachRow`.
- [x] Batching by `insert_max_rows` / `insert_min_rows` / `insert_max_delay` from the table above, with the batch as the retry unit; retries bounded, with the last error readable.
- [x] **A bounded database user, checked in beside the DDL and created with it.** `INSERT` on the five tables, `SELECT` on `segment_coverage` and `era` only — the adjacency check needs those two and nothing else reads — plus a settings profile with an explicit read-bytes ceiling and thread cap, a quota, and a workload thread share where the cluster supports one. A writer that arrives with a ceiling already set costs far less than one given a ceiling after an incident: every workload added to the destination cluster in the last month was discovered weeks later from a graph, one of them at a quarter of total cluster CPU.
- [x] **Document that `ReplacingMergeTree` dedup is merge-time, not insert-time**, in the DDL comment and in the crate docs. A re-run after an analyser fix leaves duplicate rows *visible* until a merge runs, which is correct for idempotence and surprising for consumers: a data-quality check that counts rows reads a reload as a doubling, and an exact count needs `FINAL` or an explicit dedup in the query. This has already produced one false "row count doubled" finding on the destination cluster that had to be retracted.
- [x] Credentials from the environment only, never from the configuration file, and never logged. The configuration carries the endpoint, database and user.
- [x] A failed batch is retained and counted, and the loader treats the object as unloaded: partial credit is how a gap becomes invisible.
- [x] The checked-in DDL: `db/clickhouse/*.sql`, one file per migration, numbered as the existing schema files are, with the decisions above applied.

**Verification:** `cargo test -p dz-recorder-clickhouse` covers batching, retry and the JSON body against literals with no server. A feature-gated `--features clickhouse-tests` suite runs the DDL against the container the demo already provisions and asserts a load, a re-load and the arithmetic — and asserts **rows per part** from `system.parts`: loading a set of objects whose rows exceed `insert_min_rows` produces parts at or above it, and no configuration produces a part of single-digit rows. The re-load assertion covers both readings: duplicate rows are visible before a merge, and `FINAL` returns the single row.

### 4. `dz-recorder-load`: the binary

- [x] `--config`, `--check`, `--once`, `--watch`, `--dry-run`, `--version`, parsed by hand as the recorder's is.
- [x] Walks a completed-objects directory, oldest first, and keeps a load ledger keyed on `(object_key, object_sha256)` so a restart resumes rather than re-loads.
- [x] `--check` validates configuration and reachability and loads nothing.
- [x] `dz_loader_*` metrics: objects loaded, rows written per grain, batches failed, bytes read, the last error, and **lag** as both the age of the oldest unloaded object and the count of unloaded objects.
- [x] Runs as its own user with the objects directory read-only; a systemd unit and its `ExecStartPre=--check`, mirroring the recorder's.

**Verification:** an end-to-end test in `dz-recorder-e2e` — encode with the real encoder, record with the real writer, load with the real loader into `FileSink`, and assert the rows against the datagrams that were encoded. Then the same against the container, feature-gated.

### 5. Retention and the sizing it implies

- [x] TTL on `datagram` matched to the retention of the objects themselves; none on `sequence_gap`, `segment_coverage`, `conformance_finding`, `era`.
- [x] The sizing stated in the DDL comment from a measurement, not an estimate: a busy recorder in a live deployment sustains roughly 80,000 datagrams a minute across its feeds, which is on the order of 100 million rows a day from one host. The derived grains are three to four orders of magnitude smaller, which is what makes keeping them indefinitely reasonable and keeping the base rows not.
- [x] A documented answer to what happens when the TTL is shorter than the question: the derived rows survive, and `segment_coverage` says whether the window was ever covered — which is the difference between "no loss" and "nothing kept".
- [x] **State the steady-state part count the TTL implies, not only the row count.** A short TTL on the largest table is a continuous delete-and-merge treadmill that a row count does not predict: partitions are days, and the expected parts per partition follow from the write pattern above. Write the number down, then check it — two separate retention incidents on the destination cluster came from TTL behaviour rather than volume.
- [x] **The TTL lives in the checked-in DDL and is never applied by hand.** One of those incidents was a hand-applied change that auto-sync silently reverted nightly for six days, which no row count would have shown.

**Verification:** the TTL is asserted in the feature-gated suite by inserting a row dated past the window and observing it leave after a merge; the derived tables' rows survive the same merge.

### 6. Where it runs, and the prerequisite that is missing

- [x] Document that the loader runs **on the recorder host**, reading that host's own completed directory. Nothing ships objects off a recorder host today, and objects are evicted under the staging budget in about a day and a half on a busy one: the rows are tens of bytes against a datagram's twelve hundred, so the small thing travels and the bytes stay local.
- [x] State the consequence plainly: this is what makes the cross-site join available *before* a shipper exists, because the join is over rows and not over objects.
- [x] The lag alert from task 4 is the gate on that arrangement — a loader that falls behind eviction loses history that no re-run can recover.

**Verification:** documentation only, reviewed with this plan.

---

## Out of scope

The conformance runner over replay and the decoded per-message rows. Both are the
other half of the spec's plan 3, and `conformance_finding` is written here as a
table the runner fills, not as a runner.

Any repoint of an existing dashboard. The spec's rule stands: rows must be proven
equivalent to what a panel already shows before anything is switched over.

Any shipper. The loader is deliberately arranged so that not having one costs
retention and not the join.
