# Inline mode — Implementation Plan

**Goal:** One `dz-recorder` process that captures a feed, derives its rows
through the same derivation archive mode uses, spools them to disk and loads
them into the column store — keeping no datagrams, and saying so on every row.

**Spec:** `docs/superpowers/specs/2026-09-08-recorder-inline-mode-design.md`

**Tech stack:** Rust 2021, workspace MSRV. The new crate takes no dependency
that is not already in the workspace. Everything the mode adds to the recorder
binary is behind a build feature, so the default build gains no HTTP client, no
column-store crate and no row crates.

---

## Scope, and what it is not

This lands inline mode end to end: the ring, the window, the spool, the ledger,
the pipeline, the configuration, the refusals, the metrics and the tests.

It does **not** change archive mode. Not its configuration, not its objects, not
its manifest, not its metrics, and not `dz-recorder-load`'s behaviour. The one
change outside inline mode's own code is the `derivation` column, which defaults
to `archive` so that every row already written keeps its meaning.

| Task group | Lands | Runs in CI with |
|---|---|---|
| 1 | the provenance column, in the rows and in the DDL | nothing |
| 2 | `dz-recorder-load` as a library as well as a binary | nothing |
| 3–6 | `dz-recorder-inline`, the whole mode as a library | nothing |
| 7 | the equivalence gate | nothing |
| 8–9 | the recorder binary's inline mode | nothing; a server behind `clickhouse-tests` |
| 10 | documentation | — |

Tasks 1 and 2 are independent of each other and of everything after them. Task 7
is the gate the design rests on and is written against tasks 3–6 only, not
against the binary.

---

## Global constraints

- **Vocabulary:** `GLOSSARY.md` governs every identifier, comment, test name,
  metric name, config key and commit message. `datagram` never `frame`; `era`
  never `epoch`; `feed` or `path` never `lane`; `fan-out` never `tee`; `arm` is
  banned outright in every sense. `source` is never bare in an identifier, a
  config key, a metric name or a log field — `WindowSource` is admissible only
  because it is the qualified form the workspace already uses in
  `ArchiveSource` and `SocketSource`.
- **`RecorderConfig` gains no key.** Not the destination, not a credential, not
  the window bound, not the spool. A task that needs one has misread the design:
  every existing `config_hash` in the fleet must stay byte-for-byte what it is,
  because that hash is written into archives as provenance.
- **The capture path never blocks and never parses.** No task may make the
  capture loop wait on the ring, on the spool, on the derivation or on the
  destination. No task may decode a message anywhere in the process.
- **Derivation is called, never reimplemented.** `dz-recorder-rows` is not
  modified by any task after task 1. A task that wants to change `derive` has
  found a bug in archive mode and should say so instead.
- **Lints:** `#![forbid(unsafe_code)]` and the workspace clippy set on the new
  crate. `cargo clippy --all-targets --all-features -- -D warnings` and
  `cargo fmt --all --check` pass at every task boundary. CI's toolchain is newer
  than local stable, so lint against CI's version before pushing.
- **Every test in tasks 1–7 needs no socket, no privileges and no server.**

---

## The pieces where the obvious implementation is the wrong one

Stated up front, because each was found by reading the existing code and each is
a task below that would otherwise be written wrong.

| Piece | Why it is not obvious |
|---|---|
| the ring charges its drops as loss | a datagram the deriver never saw is a sequence value with nothing admitted behind it, and the row that describes it gets a `publisher` verdict — the recorder's own loss reported as the publisher's. `PendingLoss` is the fix and it already exists |
| the ring pools its slots | `OwnedDatagram::from_recorded` allocates a `Vec` per datagram, and an allocation per datagram on the capture thread is a regression against a path that today costs a copy and a buffered write |
| a fresh `LossDeriver` per window is correct | it looks like state that must span windows; archive mode already creates one per object and carries continuity in the trailer, so a window behaves identically |
| the window's manifest digest is empty, not synthesised | a digest over rows would be a different claim wearing the field name of a claim about datagrams |
| the spool is written on every window | a disk path used only during an outage is first exercised during an outage. It is also what bounds a crash to one window, and what brings the ledger back |
| the spool never applies backpressure | blocking derivation stalls the ring, overflows the receive queue, and turns a column-store outage into feed loss plus false publisher findings in every window written during it |
| the ledger entry is written when rows land, not when accepted | a sink that coalesces has taken rows it has not sent; an entry on acceptance marks a window loaded whose rows a crash then loses |
| three stages, not two | `write_batch` posts synchronously and retries, so posting on the derivation thread lets a slow destination reach the capture |
| the provenance column is in no `ORDER BY` | putting it in the sort key makes two modes' views of one datagram two rows instead of one |
| the mode is chosen on the command line | a key in `RecorderConfig` changes the archive's provenance hash, and a password rotation would change what an archive says produced it |

---

## Tasks

### 1. The provenance column

- [ ] `dz-recorder-rows/src/rows.rs`: a `derivation` field on all five grains —
      `Datagram`, `SegmentCoverage`, `SequenceGap`, `Era`, `ConformanceFinding`.
      A two-token type, not a bare `String`, so a third value cannot be written
      by accident; `archive` and `live` are its tokens.
- [ ] `dz-recorder-rows/src/derive.rs`: `derive` writes `archive`. Inline mode
      overrides it on the batch it gets back, which keeps `derive` a function of
      the window and nothing else.
- [ ] `dz-recorder-clickhouse/db/clickhouse/005_recorder_derivation.sql`:
      `derivation LowCardinality(String) DEFAULT 'archive'` on the five tables.
      In no `ORDER BY`. The file states why the default exists — rows written
      before this migration were all derived from archived objects.
- [ ] `dz-recorder-rows/tests/column_names.rs`: the literal, so a rename cannot
      pass.
- [ ] Golden tests updated.

**Test:** a golden row set carries `archive` on every grain, and the DDL's
`ORDER BY` clauses are asserted unchanged — the deduplication key is what this
task must not touch.

### 2. `dz-recorder-load` gains a library

Mechanical, and **no behaviour changes**. The existing tests are the net.

- [ ] `src/lib.rs` exposing `ledger` (`Ledger`, `Entry`, `LedgerError`),
      `metrics` (`LoaderMetrics` and its label discipline), and the pieces of
      `loader` inline mode reuses: `Pending`, `record_landed`, `now_unix_nanos`.
- [ ] `main.rs` keeps the binary's own concerns — CLI, config, the directory
      walk, the pass loop — and reaches the rest through the library.
- [ ] The crate's description says it is both.

**Test:** the whole existing suite, unchanged, plus the binary test. A diff that
touches a behaviour is a diff that has exceeded this task.

### 3. `dz-recorder-inline`: the ring, and the debt

New crate `rust/recorder/dz-recorder-inline`, added to workspace `members`.

- [ ] `ring.rs`: a bounded ring of pooled slots between the capture thread and
      the derivation thread. Each slot owns buffers sized to the datagram cap
      and is returned to the pool after derivation reads it, so steady state
      allocates nothing.
- [ ] A push that does not fit drops the datagram, calls `PendingLoss::owe(1)`
      and returns without waiting. **Never blocks, never waits, never grows.**
- [ ] The next datagram accepted declares everything owed in its `drop_delta`,
      on top of what the capture already charged it, and the debt is cleared
      only once that datagram is in the ring.
- [ ] Saturating arithmetic on the debt, because a delta that wrapped would
      report an outage as a clean stretch.

**Tests, and the first is the one whose mutant must die:**

- A full ring drops and counts rather than waiting; the next accepted datagram
  carries the drop. Revert the `owe` and this test must fail.
- The debt survives several consecutive drops and is charged once, in full.
- A drop of the datagram that was already carrying a delta charges both.
- Steady-state operation returns every slot to the pool: a run of N datagrams
  through a ring of capacity K allocates K slots, not N.

### 4. `dz-recorder-inline`: the window and its manifest

- [ ] `window.rs`: `WindowSource`, a `Source` over the ring that hands datagrams
      through and returns `Ok(None)` at the window bound — bytes or age,
      whichever comes first. A quiet feed's window closes on age, which is what
      the age bound is for.
- [ ] The window counts what the archive writer counts: datagram and payload
      totals, per-instance coverage, short datagrams, the capture's cumulative
      drop totals and their declared scope, the roles joined, and whether link
      headers were captured or synthesised.
- [ ] `manifest.rs`: the synthesised `SegmentManifest`. Observed fields from the
      window and the recorder's identity; `object_key` a window key carrying the
      window's start in wall-clock nanoseconds; `sha256` empty and `byte_count`
      zero, with the rustdoc stating that an invented digest is a claim that
      something was verified.
- [ ] The trailer of window *n* is the `preceding` of window *n+1*, and comes
      from the ledger after a restart.

**Tests:**

- A window closes on its byte bound and on its age bound, and a datagram
  arriving after the close belongs to the next window.
- `derive` over a `WindowSource` yields the grains, with a fresh `LossDeriver`
  per window, and the second window's era anchor is certain.
- The anchor is still certain when the second window is derived after a restart,
  through the trailer in the ledger.
- The manifest's synthesised fields are empty or zero rather than plausible.

### 5. `dz-recorder-inline`: the spool and the ledger

- [ ] `spool.rs`: one window is one directory — a newline-delimited JSON file
      per grain, written through `dz-recorder-rows::FileSink` so the spool holds
      exactly the bytes the column-store sink will send, plus a digest over
      them. `fsync` at close, once per window, not once per batch.
- [ ] Windows are consumed oldest first, by the start stamp in the window key,
      which orders across runs where a per-run sequence cannot.
- [ ] A byte budget. When it is full the oldest window is evicted and counted,
      and **derivation is never blocked.**
- [ ] Replay at start: windows the previous run left are loaded before any new
      window is derived, so a crash costs the open window and nothing else.
- [ ] A window whose digest does not match is discarded, named in the error and
      counted — never loaded in part.
- [ ] A window's rows land, then `Ledger::record`, then the directory is
      deleted. In that order.

**Tests:**

- A destination that refuses leaves the window on disk and the ledger empty.
- A destination that recovers lands the windows oldest first, and each ledger
  entry follows its window's rows rather than preceding them.
- A full budget evicts the oldest window and counts it, and the spool's own
  reported age comes from the oldest window that is left.
- A spool written and then abandoned is replayed on the next start and lands.
- A truncated grain file is discarded by name; the windows around it still load.

### 6. `dz-recorder-inline`: the pipeline and its metrics

- [ ] `pipeline.rs`: the derivation stage and the posting stage, each on its own
      thread, with the ring between capture and derivation and the spool between
      derivation and posting.
- [ ] A panic on either stage is caught, counted, and the stage restarted. The
      capture thread is never one of them, and cannot be brought down by either.
- [ ] The `dz_recorder_inline_*` family: ring drops, windows derived, rows
      written per grain, spool bytes, spool evictions, the age of the oldest
      unposted window, posts and their failures, stage restarts, and the last
      error as a string an operator can read.
- [ ] Shutdown in order: drain the ring, close the open window, derive it, spool
      it, flush the sink, record what landed.

**Tests:**

- A stage that panics is restarted and the counter says so; the capture keeps
  going.
- Shutdown leaves nothing in the ring, nothing underived, and nothing held by
  the sink that the ledger does not account for.
- The age gauge is the oldest unposted window's, and is zero when the spool is
  empty rather than absent.

### 7. The equivalence gate

In `dz-recorder-e2e`, which already holds `archive_to_rows.rs`.

- [ ] `tests/inline_vs_archive.rs`: one synthetic feed, from the real encoder,
      through both paths — captured to an archive and derived with
      `derive_object`, and derived through inline mode — and the row sets are
      asserted equal but for `derivation`, `object_key`, `object_sha256` and
      `byte_count`.
- [ ] The same over the fault cases `dz-recorder-replay`'s `faults` test already
      injects: a sequence gap, backward motion, a reset, a new source IP
      address, a duplicate, a reordered pair, an oversized declared length, an
      unknown schema version and a silent channel.
- [ ] A ring drop is asserted at the row altitude: the gap it causes is *not*
      given a `publisher` verdict.

**This test is the gate on the design.** If the two paths agree, inline mode is
the same analysis with a different provenance.

### 8. `dz-recorder`: the second configuration, and the refusals

- [ ] `Cargo.toml`: feature `inline`, bringing `dz-recorder-inline`,
      `dz-recorder-rows`, `dz-recorder-clickhouse` and `dz-recorder-load`'s
      library. Off by default.
- [ ] `cli.rs`: `--inline-config <path>`, with `USAGE` saying what the mode
      keeps and what it does not. Asking for it in a build without the feature
      fails at startup naming the feature.
- [ ] `inline_config.rs`: the second file. `[inline]` — window bound, ring
      capacity, spool directory and budget, ledger path — and `[clickhouse]`,
      reusing `ClickHouseConfig` verbatim, credential included.
      `deny_unknown_fields` on every struct.
- [ ] `site` and `recorder` are **not** in this file; they come from
      `RecorderConfig`, so the two halves cannot name the host differently.
- [ ] `startup.rs`: the four refusals, each naming its key — an archive
      directory configured in inline mode, an unwritable spool directory, a
      ledger inside the spool directory, and the mode asked for by a build
      without it.
- [ ] `--check` in inline mode validates both files and probes the destination
      with `SELECT 1`, as the loader's does. Nothing is bound, created or
      joined.
- [ ] The startup summary says which mode is running, and in inline mode says
      that no datagram is kept.

**Tests:** each refusal, by key, in `startup.rs`'s existing table-driven style.
A configuration valid for archive mode is still valid. `--check` in inline mode
touches nothing.

### 9. `dz-recorder`: the wiring

- [ ] `runner.rs`: in inline mode the ring replaces the archive writer as what
      the capture loop delivers into. `pump`, `Capturing` and `drain_and_stop`
      are reused unchanged — the shutdown ordering they encode is the part most
      worth not rewriting.
- [ ] `endpoint.rs`: `serve` takes a render closure instead of the health
      metrics directly, and inline mode renders both registries into one
      exposition on one port. The two families are disjoint.
- [ ] The health tier is unchanged and runs in both modes.
- [ ] Signals run the whole sequence, and a second signal exits at once, exactly
      as archive mode documents.

**Tests:** a binary-altitude run under `--run-for` derives, spools and posts to
a `FileSink` destination with no server; the same against a real server behind
`clickhouse-tests`; the kill-and-restart durability case.

### 10. Documentation

- [ ] `rust/recorder/README.md`: a *two modes* section beside the existing *two
      capture modes* section, which already establishes the shape. It must say
      plainly that inline mode keeps no datagrams and what that costs.
- [ ] `dz-recorder-load/README.md`: a sentence placing it as archive mode's half.
- [ ] `BRINGING-UP-A-FEED.md`: how a feed is pointed at each mode, and the spool
      age alert.
- [ ] An example `inline.toml` and a systemd unit, with the credential coming
      from where the loader's comes from.

---

## Order, and why it is this one

1 and 2 first because they are independent, mechanical and reviewable on their
own — and because 2 is what stops task 5 from reimplementing a ledger. Then 3
before 4 because the window reads the ring, 4 before 5 because the spool stores
what the window derived, and 5 before 6 because the pipeline is the three of
them wired together. 7 comes as soon as 6 exists and before any binary work,
because it is the gate: if inline derivation and archive derivation disagree,
nothing in 8 or 9 is worth writing. 8 and 9 last, because a binary is the
easiest thing here to get right and the hardest thing to test.

---

## Acceptance

Inline mode is done when, on a host with no staging disk sized for datagrams and
no second service:

1. `dz-recorder --config recorder.toml --inline-config inline.toml --check`
   validates both files, reaches the destination, and touches nothing;
2. the same command without `--check` records a live feed and rows appear in the
   column store within one window bound, every one of them marked `live`;
3. the destination can be stopped for longer than the sink's age bound and the
   recorder keeps capturing, with the windows on disk and the oldest-window age
   gauge climbing — and when it comes back, the windows land oldest first with a
   ledger entry each;
4. the process can be killed with `SIGKILL` mid-window and, on restart, the
   spooled windows land;
5. a configuration naming an archive directory is refused at startup by key;

and when, in this repository, one synthetic feed derived through both paths
produces row sets that differ in nothing but their provenance, and a gap caused
by a full ring is not attributed to the publisher.
