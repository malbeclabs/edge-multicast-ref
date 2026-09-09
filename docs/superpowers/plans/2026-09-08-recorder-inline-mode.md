# Inline mode — Implementation Plan

**Goal:** One `dz-recorder` process that captures a feed, derives its rows
through the same derivation archive mode uses, spools them to disk and loads
them into the column store — keeping no datagrams, and saying so on every row.
And it is **the default**: what a command line naming no mode is read as.
Archive mode is asked for by `--archive` and is refused-by-key rather than
reinterpreted if a host that wants it says nothing.

**Progress marks:** a ticked box was verified against the tree — the identifier,
error variant, config key, DDL literal or test function was grepped for and
found at the path recorded beside the task. An unticked box is outstanding, and
each task carrying one says what is missing and where the gap is. Where a
bullet's text turned out not to describe what the tree does or should do, the
text is amended and the reason given before the box is ticked.

**Spec:** `docs/superpowers/specs/2026-09-08-recorder-inline-mode-design.md`

**Tech stack:** Rust 2021, workspace MSRV. The new crate takes no dependency
that is not already in the workspace. Everything the mode adds to the recorder
binary is behind a build feature, and that feature is in the default set because
the mode is the default mode — so a recorder that only records asks for the
smaller tree by name, with `--no-default-features`, and gets a binary with no
HTTP client, no column-store crate and no row crates that can only ever be in
archive mode.

---

## Scope, and what it is not

This lands inline mode end to end: the ring, the window, the spool, the ledger,
the pipeline, the configuration, the refusals, the metrics and the tests.

It does **not** change what archive mode does. Not its configuration, not its
objects, not its manifest, not its metrics, and not `dz-recorder-load`'s
behaviour. Two things outside inline mode's own code do change:

- the `derivation` column, which defaults to `archive` so that every row already
  written keeps its meaning; and
- the one word that selects archive mode. It used to be silence and is now
  `--archive`, which is task 11 and is a breaking change to a default that has
  an answer today. The design names its cost rather than this plan: see
  *[What it costs](../specs/2026-09-08-recorder-inline-mode-design.md#what-it-costs)*.

| Task group | Lands | Runs in CI with |
|---|---|---|
| 1 | the provenance column, in the rows and in the DDL | nothing |
| 2 | `dz-recorder-load` as a library as well as a binary | nothing |
| 3–6 | `dz-recorder-inline`, the whole mode as a library | nothing |
| 7 | the equivalence gate | nothing |
| 8–9 | the recorder binary's inline mode | nothing; a server behind `clickhouse-tests` |
| 10 | documentation | — |
| 11 | the default: inline mode is the reading, archive mode is asked for | nothing |
| 12 | what a review of the whole of the above found | nothing |

Tasks 1 and 2 are independent of each other and of everything after them. Task 7
is the gate the design rests on and is written against tasks 3–6 only, not
against the binary. Task 11 is last because it can only be written once both
modes exist to choose between, and because it is the only task that changes what
an existing host's command line means.

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
- **The default is a reading, not a fallback and not an invention.** No task may
  make a command line naming no mode *fall back* to archive mode, and none may
  invent a spool directory, a ledger path or a destination so that inline mode
  can start without its own file. Both are refusals that name the flag. A
  recorder that starts on a guess is the first property the binary documents,
  and a default is the easiest place to break it.
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
| the window is walked twice | `derive` stamps the manifest onto rows as it reads, and the manifest describes what the window saw — so one pass hands it a tally of nothing. It does not fail: it writes rows stamped at the Unix epoch under one key per window sequence number, which is one key for every run the recorder makes. **This row was added by a review, after the tree got it wrong** |
| an empty window spends no window sequence number | it looks like a counter of windows opened. It is the thing that tells a reader the derivation was down, so spending one on a window a quiet feed produced puts that claim in front of somebody whose feed was merely silent — and leaves the next window's era anchor uncertain into the bargain |
| the spool is written on every window | a disk path used only during an outage is first exercised during an outage. It is also what bounds a crash to one window, and what brings the ledger back |
| the spool never applies backpressure | blocking derivation stalls the ring, overflows the receive queue, and turns a column-store outage into feed loss plus false publisher findings in every window written during it |
| the ledger entry is written when rows land, not when accepted | a sink that coalesces has taken rows it has not sent; an entry on acceptance marks a window loaded whose rows a crash then loses |
| three stages, not two | `write_batch` posts synchronously and retries, so posting on the derivation thread lets a slow destination reach the capture |
| the provenance column is in no `ORDER BY` | putting it in the sort key makes two modes' views of one datagram two rows instead of one |
| the mode is chosen on the command line | a key in `RecorderConfig` changes the archive's provenance hash, and a password rotation would change what an archive says produced it |
| the two modes' refusals are what make the default safe, not the flag | archive mode requires two directories inline mode refuses a value for, so no host can be moved between the modes silently. Weaken either refusal and the default starts losing evidence quietly, which is the failure a default is worst at showing |
| the default is a *reading*, not a manufactured configuration | inline mode needs a spool and a destination and neither has a defensible value to invent, so a command line naming no mode and giving no second file is a refusal naming both flags. A default that invented a path would be the recorder guessing, which is the one thing it documents that it does not do |
| the build feature moves into the default set | a binary whose own default mode its feature set excludes refuses every command line that names no mode. The feature still exists, and `--no-default-features` is still the record-only build; what changes is which way round the default runs |

---

## Tasks

### 1. The provenance column

- [x] `dz-recorder-rows/src/rows.rs`: a `derivation` field on all eight grains —
      `Datagram`, `SegmentCoverage`, `SequenceGap`, `Era`, `ConformanceFinding`,
      and the market data three, `Event`, `Instrument` and `BookTop`.
      A two-token type, not a bare `String`, so a third value cannot be written
      by accident; `archive` and `live` are its tokens.
- [x] `dz-recorder-rows/src/derive.rs`: `DeriveInput` carries the provenance and
      has no `Default`, so a derivation states it or does not compile;
      `derive_object` says `archive` because it verified the digest itself.
      An input rather than a patch applied to the batch afterwards: it is the
      same kind of fact as `drop_scope` — one the caller knows and the
      derivation cannot observe — and stamping it costs nothing where walking
      a hundred thousand rows to overwrite a field would.
      `dz-recorder-events` takes the same field on `EventInput`, for the market
      data grains.
- [x] `dz-recorder-clickhouse/db/clickhouse/008_recorder_derivation.sql`:
      `derivation LowCardinality(String) DEFAULT 'archive'` on the eight tables.
      `005` through `007` are the market data migrations, so this is `008`; the
      column is declared in `001` and `005` beside the tables themselves, and
      this file is what reaches a deployment whose tables already exist.
      In no `ORDER BY`. The file states why the default exists — rows written
      before this migration were all derived from archived objects.
- [x] `dz-recorder-rows/tests/column_names.rs`: the literal, so a rename cannot
      pass.
- [x] Golden tests updated.

**Test:** a golden row set carries `archive` on every grain, and the DDL's
`ORDER BY` clauses are asserted unchanged — the deduplication key is what this
task must not touch.

**Done.** `Derivation` at `rows.rs:124`, the field on all eight grains
(`rows.rs:301`, `:359`, `:403`, `:493`, `:524`, `:690`, `:740`, `:798`), the
migration on all eight tables and in no `ORDER BY`, and
`dz-recorder-clickhouse/tests/ddl.rs:413`
`provenance_is_on_every_grain_and_in_no_sort_key` holding both halves.

### 2. `dz-recorder-load` gains a library

Mechanical, and **no behaviour changes**. The existing tests are the net.

- [x] `src/lib.rs` exposing `ledger` (`Ledger`, `Entry`, `LedgerError`),
      `metrics` (`LoaderMetrics` and its label discipline), the pieces of
      `loader` inline mode reuses (`Pending`, `record_landed`, `now_unix_nanos`),
      and — less tidily than this plan first assumed — `config` and
      `market_data`. The pass reaches into both for `MarketDataFeed` and for the
      market data derivation, and the pass cannot be a library while half of
      what it calls is not.
- [x] `main.rs` keeps the binary's own concerns — the command line, the metrics
      endpoint, the build identity, the directory walk and the pass loop — and
      reaches the rest through the library.
- [x] The crate's description says it is both.

**Test:** the whole existing suite, unchanged, plus the binary test. A diff that
touches a behaviour is a diff that has exceeded this task.

**Done.** `dz-recorder-load/src/lib.rs:53-62` exports all seven names, and
`main.rs` declares only `cli`, `endpoint` and `identity` of its own.

### 3. `dz-recorder-inline`: the ring, and the debt

New crate `rust/recorder/dz-recorder-inline`, added to workspace `members`.

- [x] `ring.rs`: a bounded ring of pooled slots between the capture thread and
      the derivation thread. Each slot owns buffers sized to the datagram cap
      and is returned to the pool after derivation reads it, so steady state
      allocates nothing.
- [x] A push that does not fit drops the datagram, calls `PendingLoss::owe(1)`
      and returns without waiting. **Never blocks, never waits, never grows.**
      Reached through `PendingLoss::undelivered`, which *is* `owe(1)`
      (`dz-recorder-capture/src/socket.rs:108`) — the debt's own vocabulary
      rather than a second spelling of it.
- [x] The next datagram accepted declares everything owed in its `drop_delta`,
      on top of what the capture already charged it, and the debt is cleared
      only once that datagram is in the ring. `ring.rs:225` charges,
      `ring.rs:229` settles inside the `Ok` branch of the send, and a send that
      fills re-owes at `ring.rs:237`.
- [x] Saturating arithmetic on the debt, because a delta that wrapped would
      report an outage as a clean stretch. `socket.rs:93`.

**Tests, and the first is the one whose mutant must die:**

- A full ring drops and counts rather than waiting; the next accepted datagram
  carries the drop. Revert the `owe` and this test must fail.
- The debt survives several consecutive drops and is charged once, in full.
- A drop of the datagram that was already carrying a delta charges both.
- Steady-state operation returns every slot to the pool: a run of N datagrams
  through a ring of capacity K allocates K slots, not N.

### 4. `dz-recorder-inline`: the window and its manifest

- [x] `window.rs`: `WindowSource`, a `Source` over the ring that hands datagrams
      through and returns `Ok(None)` at the window bound — bytes or age,
      whichever comes first. A quiet feed's window closes on age, which is what
      the age bound is for.
- [x] The window counts most of what the archive writer counts: datagram and
      payload totals (`window.rs:87`), per-instance coverage, short datagrams
      and instances dropped through the archive writer's own `CoverageTracker`
      (`manifest.rs:112`), the declared drop scope, the roles joined, and
      whether link headers were captured or synthesised (`manifest.rs:40-55`).
- [x] **The capture's cumulative drop totals.** `window_manifest` took
      `capture_drop_total` and `interface_drop_total` as parameters and its only
      caller passed zeros —
      `pipeline.rs:345`, `window_manifest(&identity, window.tally(), *window_seq, 0, 0)`
      — so every inline `segment_coverage` row reported a capture that dropped
      nothing. Row-level loss attribution was unaffected, because that travels
      on `drop_delta` through the ring's debt; what was wrong is the cumulative
      counter a reader uses to ask *did this host keep up*. The equivalence gate
      could not catch it: the synthetic feed has no kernel drops, so both paths
      reported zero and agreed. **Settled in task 12** — both parameters are
      gone, `capture_drop_total` is the window's own sum of the `drop_delta` it
      walked, and `interface_drop_total` is a zero the manifest builder writes
      with the reason on it.
- [x] `manifest.rs`: the synthesised `SegmentManifest`. Observed fields from the
      window and the recorder's identity; `object_key` a window key carrying the
      window's start in wall-clock nanoseconds; `sha256` empty and `byte_count`
      zero, with the rustdoc stating that an invented digest is a claim that
      something was verified.
- [x] **The manifest is built from a window that has been walked.** This bullet
      is the one this task never wrote down, and the tree got it wrong for
      exactly that reason. `pipeline.rs:344-361` built the manifest from
      `window.tally()` immediately after `WindowSource::open`, which is
      `WindowTally::default()` — so `derive` was handed `start_ns = 0`,
      `end_ns = 0` and no `instances`, and that value *was* the manifest the
      rows were stamped from rather than a discarded first pass. A local named
      `probe` and a comment describing two passes are the whole of the two-pass
      shape that was there. **Settled in task 12**, which is also where the
      design gained the section saying why one pass cannot work.
- [x] The trailer of window *n* is the `preceding` of window *n+1*, within a
      run: `pipeline.rs:302` holds it, `:351` reads it into the next `derive`,
      `:390` replaces it.
- [ ] **Outstanding: the trailer is not read back from the ledger after a
      restart.** `pipeline.rs:162` starts a run at `preceding: None`, and
      nothing under `dz-recorder-inline/src/` or `dz-recorder/src/` calls
      `Ledger::trailer()`. The trailer *is* persisted (`spool.rs:231`, `:529`),
      so the data is on disk and only the read is missing — but until it is
      wired the design's claim that the era anchor *"stays certain across a
      restart"* is a claim about the intended behaviour and not about this
      tree. The first window after every restart writes an uncertain anchor.

**Tests:**

- A window closes on its byte bound and on its age bound, and a datagram
  arriving after the close belongs to the next window —
  `tests/window.rs:81`, `:109`, `:146`, `:170`.
- The manifest's synthesised fields are empty or zero rather than plausible —
  `tests/window.rs:197`, `:256`.
- **Not written:** `derive` over a `WindowSource` yielding the grains with a
  fresh `LossDeriver` per window and a certain second-window anchor. Neither
  `derive` nor `anchor_certain` appears in `tests/window.rs`; the equivalence
  gate covers the derivation over a window but asserts nothing about the
  anchor.
- **Not written, and it is the one that matters:** the anchor still certain
  after a restart. It cannot pass against this tree, because of the outstanding
  bullet above — which is how the gap was found.

### 5. `dz-recorder-inline`: the spool and the ledger

- [x] `spool.rs`: one window is one directory — a newline-delimited JSON file
      per grain, written through `dz-recorder-rows::FileSink` so the spool holds
      exactly the bytes the column-store sink will send, plus a digest over
      them. `fsync` at close, once per window, not once per batch.
- [x] Windows are consumed oldest first, by the start stamp in the window key,
      which orders across runs where a per-run sequence cannot.
- [x] A byte budget. When it is full the oldest window is evicted and counted,
      and **derivation is never blocked.**
- [x] Replay at start: windows the previous run left are loaded before any new
      window is derived, so a crash costs the open window and nothing else.
- [x] A window whose digest does not match is discarded, named in the error and
      counted — never loaded in part.
- [x] A window's rows land, then `Ledger::record`, then the directory is
      deleted. In that order.

**Tests:**

- A destination that refuses leaves the window on disk and the ledger empty.
- A destination that recovers lands the windows oldest first, and each ledger
  entry follows its window's rows rather than preceding them.
- A full budget evicts the oldest window and counts it, and the spool's own
  reported age comes from the oldest window that is left.
- A spool written and then abandoned is replayed on the next start and lands.
- A truncated grain file is discarded by name; the windows around it still load.

**Done, and over-delivered.** All five tests exist (`tests/spool.rs:245`,
`:283`, `:391`, `:444`, `:483`) and seven more with them, including a window the
ledger already records being dropped rather than posted twice (`:705`) and a
ledger entry that will not write owing an entry rather than a second insert
(`:750`).

### 6. `dz-recorder-inline`: the pipeline and its metrics

- [x] `pipeline.rs`: the derivation stage and the posting stage, each on its own
      thread, with the ring between capture and derivation and the spool between
      derivation and posting.
- [x] A panic on either stage is caught, counted, and the stage restarted. The
      capture thread is never one of them, and cannot be brought down by either.
      `pipeline.rs:268` catches, `:273` counts, `:263` restarts, and the capture
      holds only a `RingSender`.
- [x] The `dz_recorder_inline_*` family: ring drops, windows derived and windows
      empty, rows derived, windows landed and posts failed, stage restarts,
      windows evicted and discarded, spool bytes and windows, and the age of the
      oldest unposted window. Twelve series, `metrics.rs:98-154`, each labelled
      `feed` with `site` and `recorder` as constants.
- [ ] **Outstanding: rows written per grain, and the last error as a string.**
      `dz_recorder_inline_rows_derived_total` is across every grain
      (`metrics.rs:113`) with no `grain` label, so the reason the design gave
      for wanting it per grain — the grains are orders of magnitude apart in
      volume, so one total hides a grain that stopped — is not served. And
      there is no `last_error` gauge anywhere in the crate, though archive mode
      publishes one (`dz-recorder/src/runner.rs:294`), which is the asymmetry
      that makes it worth keeping on the list rather than dropping.
- [x] Shutdown in order: drain the ring, close the open window, derive it, spool
      it, flush the sink, record what landed. `pipeline.rs:208`, and `stop`
      takes the capture end so the ordering is the signature's rather than the
      caller's.

**Tests:**

- A stage that panics is restarted and the counter says so; the capture keeps
  going.
- Shutdown leaves nothing in the ring, nothing underived, and nothing held by
  the sink that the ledger does not account for.
- The age gauge is the oldest unposted window's, and is zero when the spool is
  empty rather than absent.

### 7. The equivalence gate

In `dz-recorder-e2e`, which already holds `archive_to_rows.rs`.

- [x] `tests/inline_vs_archive.rs`: one synthetic feed, from the real encoder,
      through both paths — captured to an archive and derived with
      `derive_object`, and derived through inline mode — and the row sets are
      asserted equal but for `derivation`, `object_key` and `object_sha256`.
      **Three fields, not four.** This plan asked for `byte_count` as well and
      no grain carries it: it is a manifest field, not a row field
      (`dz-recorder-rows/src/rows.rs` has no `byte_count` at all), so a fourth
      erasure would have been unimplementable rather than merely redundant. The
      test erases the three by clearing them rather than skipping them
      (`inline_vs_archive.rs:130`), so a provenance field added later is
      compared without anyone remembering to add it.
- [x] **And it derives the way the derivation stage derives.** As first written
      this gate built the inline side itself: it walked the window, built the
      manifest from the completed tally, then fed a *second* ring to a second
      window for `derive` (`inline_vs_archive.rs:89-112`). Its own comment gave
      the requirement — *"the manifest describes what the window saw, and the
      window has not seen anything until the derivation has walked it"* — and
      the derivation stage did not meet it, so the gate was asserting an
      equivalence between archive mode and a shape nothing ran. That is the one
      failure this gate cannot report: a fixture supplying the correctness under
      test looks exactly like a pass. **Settled in task 12**: the two passes
      live in one place and the gate calls it, so the shape under test is the
      shape that runs.
- [x] The same over the fault cases `dz-recorder-replay`'s `faults` test
      injects: a sequence gap, backward motion, a reset, a new source IP
      address, a source IP address that disappears, a duplicate, a reordered pair,
      an oversized declared length and an unknown schema version.
      `inline_vs_archive.rs:256-266`.
- [ ] **Outstanding: `Fault::SilentChannel`.** It exists
      (`dz-recorder-replay/src/synthetic.rs:104`) and the replay crate's own
      faults test exercises it (`dz-recorder-replay/tests/faults.rs:37`), but it
      is the one of the nine this gate does not run. It is also the fault whose
      inline behaviour is least like archive mode's: a channel that stops
      publishing is found by a window closing on *age* with nothing in it, and
      the age bound is inline mode's own key. Absent from the gate, the one
      grain shape only a quiet feed produces is compared by nothing.
- [x] A ring drop is asserted at the row altitude: the gap it causes is *not*
      given a `publisher` verdict. `inline_vs_archive.rs:303`
      `a_gap_the_ring_caused_is_not_attributed_to_the_publisher`, asserting
      `assert_ne!(.., Verdict::Publisher)` at `:369`.

**This test is the gate on the design.** If the two paths agree, inline mode is
the same analysis with a different provenance.

### 8. `dz-recorder`: the second configuration, and the refusals

- [x] `Cargo.toml`: feature `inline`, bringing `dz-recorder-inline`,
      `dz-recorder-rows`, `dz-recorder-clickhouse` and `dz-recorder-load`'s
      library. **In the default set**, which is task 11's doing and not this
      task's: this task wrote it off by default, and the default set is where a
      feature gating the default mode has to be. `--no-default-features` is the
      record-only build.
- [x] `cli.rs`: `--inline-config <path>`, with `USAGE` saying what the mode
      keeps and what it does not. It is the file the mode needs and no longer
      the switch that selects it — task 11 — and a build without the feature
      fails at startup naming the feature.
- [x] `inline_config.rs`: the second file. `[inline]` — window bound, ring
      capacity, spool directory and budget, ledger path — and `[clickhouse]`,
      reusing `ClickHouseConfig` verbatim, credential included.
      `deny_unknown_fields` on every struct.
- [x] `site` and `recorder` are **not** in this file; they come from
      `RecorderConfig`, so the two halves cannot name the host differently.
      Held by `inline_config.rs:740` `site_and_recorder_are_not_keys_in_this_file`
      and `:757` `there_is_no_password_key_anywhere_in_this_file`.
- [x] The refusals, each naming its key — an archive directory configured in
      inline mode, a spool directory that is missing or unopenable, a ledger
      inside the spool directory, and the mode asked for by a build without it.
      **In `inline_config.rs`, not `startup.rs`**, which this plan named
      wrongly: they are refusals about inline mode's own file, and putting them
      in the archive plan's module would have made a build without the feature
      carry checks over keys it has no type for. `ArchiveDirectoryConfigured`
      `:90`, `SpoolDirUnusable` `:123` and `NoSpoolDir` `:114`,
      `LedgerInsideSpool` `:170` and `NoLedger` `:160`, `NotCompiledIn` `:53` —
      with four more the task did not ask for: `WindowBoundIsZero` `:98`,
      `RingHoldsNothing` `:106`, `SpoolBudgetIsZero` `:130` and
      `SpoolBudgetTooSmall` `:153`.
- [x] `--check` in inline mode validates both files and probes the destination
      with `SELECT 1`, as the loader's does. Nothing is bound, created or
      joined. `inline_config.rs:468`, held by
      `tests/inline_mode.rs:200`.
- [x] The startup summary says which mode is running, and in inline mode says
      that no datagram is kept. `INLINE_MODE` at `inline_config.rs:406`.

**Tests:** each refusal, by key, in `startup.rs`'s existing table-driven style.
A configuration valid for archive mode is still valid. `--check` in inline mode
touches nothing.

### 9. `dz-recorder`: the wiring

- [x] The ring replaces the archive writer as what the capture loop delivers
      into, and `pump`, `Capturing` and `drain_and_stop` are reused unchanged —
      the shutdown ordering they encode is the part most worth not rewriting.
      **In a new `inline_runner.rs` rather than inside `runner.rs`**, which is
      the better shape and the reason to record it: `runner.rs` stays the
      archive record path and exports the three reused pieces
      (`inline_runner.rs:50`, used at `:302` and `:325`), so a
      `--no-default-features` build compiles none of the inline wiring.
- [x] `endpoint.rs`: a render closure, and inline mode renders both registries
      into one exposition on one port. The two families are disjoint.
      **Added beside `serve` rather than changing its signature**:
      `serve_rendering` at `endpoint.rs:63`, with `serve` at `:51` delegating to
      it. Archive mode's call site is therefore untouched, which is what *no
      change to archive mode* asked for.
- [x] The health tier is unchanged and runs in both modes. `inline_runner.rs:92`,
      and the observer sees a datagram before the ring does (`:293`) so a ring
      drop cannot hide from the health tier.
- [x] Signals run the whole sequence, and a second signal exits at once, exactly
      as archive mode documents — by reusing the handler,
      `inline_runner.rs:162`.

**Tests: none of the three are written, and this is the largest gap on the
branch.**

- **Not written:** a binary-altitude run under `--run-for` that derives, spools
  and posts to a destination with no server. The plan asked for a `FileSink`
  destination and the binary has no such destination to point at — `FileSink`
  appears nowhere under `dz-recorder/src/` — so the test needs either that
  destination or a stub HTTP server, and neither exists. The pipeline is
  covered at library altitude (`dz-recorder-inline/tests/pipeline.rs:125`)
  against a fake sink, which is why the gap is a wiring gap rather than a
  derivation gap: nothing proves the binary's two files reach that pipeline.
- **Not written:** the same against a real server behind `clickhouse-tests`.
  `dz-recorder` names that feature nowhere.
- **Not written:** the kill-and-restart durability case. Acceptance criterion 4
  is exactly this, and it is unproven; `tests/inline_mode.rs` is refusals and
  `--check` only.

**And the reach of the default suite is part of the gap.** `cargo test
--workspace` does not enable `socket-e2e`, `afpacket`, `clickhouse-tests` or
`conformance`, and a suite behind one of those reports *0 tests* rather than
*skipped* — so a green workspace run says nothing about it. That is how task 11
shipped with `tests/shutdown.rs` unfixed. Anything asserting the binary's
behaviour has to be run by naming the feature, and the features CI enables are
the list to run before pushing:

```bash
cargo test --workspace
cargo test -p dz-recorder --no-default-features
cargo test -p dz-recorder --features socket-e2e
cargo test -p dz-recorder-e2e --features socket-e2e -- --test-threads=1
cargo test -p dz-recorder-e2e --features conformance
cargo test -p dz-recorder-capture --features loopback-tests
# these two need libpcap-dev, and a column store, respectively
cargo test -p dz-recorder --features afpacket
cargo test -p dz-recorder-e2e --features clickhouse-tests -- --test-threads=1
```

### 10. Documentation

- [x] `rust/recorder/README.md`: a *two modes* section beside the existing *two
      capture modes* section, which already establishes the shape. It must say
      plainly that inline mode keeps no datagrams and what that costs.
      `README.md:118`.
- [x] `dz-recorder-load/README.md`: a sentence placing it as archive mode's half.
      `:7`.
- [x] `BRINGING-UP-A-FEED.md`: how a feed is pointed at each mode, and the spool
      age alert. `:397` and the checklist line at `:547`.
- [x] An example `inline.toml` and a systemd unit, with the credential coming
      from where the loader's comes from. `dz-recorder/inline.example.toml`,
      asserted parseable by `inline_config.rs:1021`, and
      `systemd/dz-recorder-inline.service:75`.

Each of these states the default in its own words, which is why task 11 has to
come back through all four of them.

### 11. The default: inline mode is the reading, archive mode is asked for

Designed in
*[Why inline mode is the default](../specs/2026-09-08-recorder-inline-mode-design.md#why-inline-mode-is-the-default)*,
costed in
*[What it costs](../specs/2026-09-08-recorder-inline-mode-design.md#what-it-costs)*,
and decided as inline *only* in
*[Whether the default is inline only](../specs/2026-09-08-recorder-inline-mode-design.md#whether-the-default-is-inline-only-or-inline-and-an-archive-together)*.

This task is a list of the places the old default was written down. **A default
expressed in ten places is a default that gets inverted in nine**, and the
tenth is the one an operator reads.

- [x] `cli.rs`: `--archive`, and `Args` carries it beside `inline_config`. Both
      flags together is `CliError::ArchiveAndInline`, refused on the command
      line for the reason `--check` with `--run-for` is: two arrangements that
      keep different things were asked for at once, and there is no reading of
      that which is what somebody meant.
- [x] `cli.rs`: `USAGE` inverted. `--archive` documents the arrangement and what
      omitting it now means; `--inline-config` stops describing itself as the
      whole of the opting in and becomes the file the default mode needs.
- [x] `main.rs`: the dispatch. `--archive` takes `Plan::from_config` and the
      archive runner; anything else takes inline mode — including a command line
      that named no second file, which is a refusal and never a fallback.
- [x] `inline_config.rs`: `InlineConfigError::NotStated`, naming
      `--inline-config` and `--archive`. Named after
      `StartupError::DirectoryNotStated` because it is the same failure in the
      other arrangement: a required path with no defensible value to invent.
      Spelled `NotStated` and not `InlineConfigNotStated` as this plan first
      had it — the type it sits in is already `InlineConfigError`, so the longer
      name stuttered at every call site.
- [x] `inline_config.rs`: `ArchiveDirectoryConfigured` names `--archive` instead
      of telling an operator to drop a flag they did not pass. This message is
      what every existing archive-mode host meets on its first restart after the
      default changed, so it is the migration instruction as much as the
      refusal, and it is the only place that instruction is guaranteed to be
      read.
- [x] `inline_config.rs`: `NotCompiledIn` says the default mode is one this
      build cannot run, and names `--archive` as what it can.
- [x] `startup.rs`: `Arrangement::Inline` documented as the default and
      `Arrangement::Archive` as the one asked for by name. `writes_an_archive`
      is unchanged: a build without the feature is still only ever in archive
      mode, and returning `true` there is still right.
- [x] `Cargo.toml`: `default = ["inline"]`. A binary that refuses the
      arrangement its own command line asks for when told nothing is a binary
      whose default it cannot honour.
- [x] `.github/workflows/rust-codec.yml`: a `--no-default-features` clippy and
      test step for `dz-recorder`. The feature moving into the default set makes
      the record-only build the untested one, and it is the build that has to
      make the `NotCompiledIn` refusal — the same argument the workflow already
      makes beside the `afpacket` steps, for the same reason.
- [x] `tests/shutdown.rs`: `--archive`. **The eleventh place, and the one this
      task's own list missed** — it spawns the real binary against an
      archive-mode fixture and waits for a segment, which inline mode never
      opens, so the inversion turned a 0.7-second test into a fifteen-second
      timeout. Found by CI and not by `cargo test --workspace`, because the
      suite is `#![cfg(feature = "socket-e2e")]` and the workspace run reports
      it as *0 tests* rather than as skipped. The same class as task 9's
      missing binary-altitude tests: a test at binary altitude that the default
      suite does not reach. `rust/recorder/dz-recorder-core/tests/fixtures/recorder_example.toml`
      gained the same statement in prose, being the archive-mode configuration
      an operator copies.
- [x] The prose, all of it: `rust/recorder/README.md`'s two-modes table and its
      *what bounds it* and *which mode a host runs*,
      `dz-recorder-load/README.md`, `BRINGING-UP-A-FEED.md` and its bring-up
      checklist, `inline.example.toml`, and the systemd unit's header. A default
      left stated correctly in nine places and wrongly in the tenth is worse
      than one stated nowhere, because the wrong one will be the one somebody
      quotes.

Two things the task needed that the bullets above did not predict:

- **The archive-directory refusal has to run before the missing-file one.** The
  command line most likely to arrive here by mistake is an archive-mode host's,
  unchanged: it carries the two directories and no second file. Told about the
  missing file first, that operator is being answered about a file they never
  wanted; told about `archive.staging_dir` first, they are told the key they
  wrote and the flag they are missing. `run` therefore calls
  `check_archive_is_not_configured` before it unwraps the path.
- **`tests/check_mode.rs` was archive mode's binary-altitude suite and every
  check in it needed the flag.** That is the shape of this cost across the
  fleet, in miniature, and it is why the refusal test lives in that file rather
  than beside inline mode's: the file that had to change is the file where the
  proof belongs.

**Tests, and the revert that killed each. Every one was run — reverted, watched
fail, restored:**

| Revert | Test that died |
|---|---|
| `main.rs`'s dispatch back to `if args.inline_config.is_some()` | `check_mode.rs`'s `an_archive_configuration_with_no_mode_named_is_refused_and_names_the_flag` **and** `inline_mode.rs`'s `a_command_line_naming_no_mode_and_no_second_file_is_refused_by_name` — both, because that one line is the default |
| a defaulted `InlineConfig` where `NotStated` is returned | `a_command_line_naming_no_mode_and_no_second_file_is_refused_by_name` |
| the `ArchiveAndInline` branch dropped from `cli::parse` | `cli::tests::naming_both_modes_is_refused` |
| `default = ["inline"]` removed from `Cargo.toml` | `inline_mode_is_a_default_feature_because_it_is_the_default_mode` |
| `ArchiveDirectoryConfigured` back to *"drop `--inline-config`"* | `an_archive_configuration_with_no_mode_named_is_refused_and_names_the_flag` |
| `--archive` dropped from `tests/shutdown.rs`'s spawn | `sigterm_publishes_the_open_segment_and_exits_zero`, after a fifteen-second wait for a segment inline mode never opens. Needs `--features socket-e2e` to run at all |

Two of those are worth reading past the table.

**The defaulted-`InlineConfig` revert fails in the way that makes the case for
the refusal.** Under it the recorder does not start either — it gets as far as
`inline.spool_dir` being empty and refuses there. So the mutant is not *silent*;
it is *unhelpful*, and the test is asserting the difference. An operator whose
command line forgot `--archive` would be told about a key they never wrote, in
a file they never made, on a host they thought was keeping bytes.

**The default-feature revert is caught by nothing except the manifest
assertion.** With `inline` out of the default set, the whole
`#[cfg(feature = "inline")]` suite is compiled out rather than failed, and
`an_archive_configuration_with_no_mode_named_is_refused_and_names_the_flag`
still passes through its `cfg!(not(feature))` branch. That is exactly why the
assertion reads the manifest text: a `cfg!` test disappears in the build it
exists to catch, and a suite that shrinks is a suite that reports success.

Everything task 8 refuses is unchanged, and the archive-mode configuration is
still valid — with `--archive`.

---

### 12. The review: the manifest, the sequence number, the trailer and the ring

Four things a review of the whole branch found, and one of them writes rows a
reader cannot detect are wrong. Each is a defect in something tasks 4, 6 and 7
claimed, which is why they are answered here rather than by a new design: the
design said what to build in every case, and this is the tree being made to say
it too.

**The manifest was built from a window nothing had walked.** The blocker. Its
mechanism is task 4's new bullet and its consequence is the design's
*[The window is walked twice](../specs/2026-09-08-recorder-inline-mode-design.md#the-window-is-walked-twice-and-the-second-walk-is-not-an-optimisation)*.
The corruption is not a wrong number: with `start_ns` at zero, every window of
every run carries the sort key `recorder.segment_coverage` orders by, so a
second run of the recorder replaces the first run's coverage rather than
standing beside it — and no coverage row was written at all, because
`manifest.instances` came from the same empty tally.

- [ ] `window.rs`: a held window. One walk drains the ring into a buffer and
      completes the tally; the second reads the buffer back as a `Source`. The
      buffer is owned by the derivation stage and reused window after window,
      refilled slot by slot through the ring's own `refill`, so the second pass
      costs a copy and not an allocation per datagram.
- [ ] `pipeline.rs`: drain, then build the manifest, then derive from the
      buffer. The local named `probe` goes with the shape it was named for.
- [ ] `inline_vs_archive.rs`: the gate calls the same held window, so its inline
      side is the derivation stage's own two passes rather than a second
      arrangement of them.

**The capture drop totals.** Decided rather than merely wired, because the two
halves of it have different answers:

- [ ] `capture_drop_total` is **wired**, and from the archive writer's own
      arithmetic: `WindowTally` sums every `drop_delta` the window walked,
      whatever port role carried it, which is what `SegmentWriter` sums into the
      same field (`dz-recorder-archive/src/writer.rs:359`). The two modes'
      coverage rows are then subtractable against each other, which is the whole
      point of the column.
- [ ] `interface_drop_total` **stays zero**, and the zero moves from a literal
      at a call site into the manifest builder with the reason on it. It is not
      a column nobody wired: **archive mode leaves it at zero too**, and
      deliberately — `dz-recorder/src/runner.rs:459-467` reads the interface
      total and hands it to the health tier, never to the writer, because the
      manifest's accounting for it is per port role and afpacket mode declares
      its drops at capture-handle scope, where there is no role to charge them
      to. Inline mode writing a number there would be one mode claiming a
      measurement the other declines to make, in a column a reader subtracts
      across both.
- [ ] `window_manifest` loses both parameters. A builder with no parameter to
      pass a zero to is a builder no caller can get this wrong in again, which
      is what made the defect survive review once already.

**An empty window spent a window sequence number.** `window.rs`'s `is_empty`
said *"An empty window spends no window sequence number"* and `pipeline.rs:397`
incremented for every window. **The doc is right and the code changes**, for
three reasons and the third is the one that decides it:

- a hole in `segment_seq` is defined here and in `manifest.rs:79-81` as a hole
  in the derivation, which is what tells a reader the recorder was down rather
  than the feed quiet. A quiet feed closes windows on age and `window.rs:71`
  already calls that ordinary, so the code was writing *the derivation was down*
  once per window bound for as long as a feed stayed silent;
- the alternative reading — that a window sequence counts windows opened — has
  no reader. Nothing joins on it, and `segment_coverage` is the only table that
  carries it;
- it takes the era anchor with it. `precedes` is a `segment_seq + 1` test, so an
  empty window that spends a number leaves the next window's predecessor two
  behind and every window after a silence writes an uncertain anchor —
  contradicting the design's claim that the anchor is certain from the second
  window onward.

- [ ] `pipeline.rs`: the increment moves inside the non-empty branch.

**A window the spool refused still became the next window's trailer.**
`pipeline.rs:386` printed the error and `:390` set `*preceding = Some(trailer)`
regardless, so the next window anchored certain on a window whose rows are not
in the store. The fix is `None` — *unknown*, and never *there was none* — and
the design's
*[The era anchor gets better, not worse](../specs/2026-09-08-recorder-inline-mode-design.md#the-era-anchor-gets-better-not-worse)*
now says so. Stale would give the same verdict by accident, one off-by-one away
from giving the wrong one.

- [ ] `pipeline.rs`: the trailer is handed on from the `Ok` branch, and the
      `Err` branch clears it.

**The ring could not report a derivation that had gone.** `ring.rs:222`'s
`TryRecvError::Disconnected` was unreachable: `RingSender` held `free_return`,
its own sending end of the free list, so the free list never disconnected while
the sender existed. A derivation thread that really had gone took its slots with
it, the free list stayed empty, and every offer after that returned `Dropped` —
for ever, and indistinguishable on every counter from a ring that was merely
overrun. That branch also skipped `pending.undelivered()`.

- [ ] `ring.rs`: `free_return` becomes a `spare` slot held in the sender itself.
      It does the one job `free_return` had — the unreachable
      `TrySendError::Full` branch puts its slot somewhere rather than shrinking the
      pool for the life of the process — without holding a sending end that
      masks the disconnection, and it is one handle fewer rather than one more.
- [ ] Both `Disconnected` branches charge the datagram through
      `pending.undelivered()` and count it. A drop nobody can carry the
      admission for is still a drop, and `RingCounters::dropped`'s rustdoc says
      which of the two it is.

**Tests:**

- Two runs of one recorder produce two window keys and two sets of coverage
  rows, at the row altitude the corruption appears at.
- A window's `capture_drop_total` is the sum of the `drop_delta` it walked.
- A quiet window leaves no hole in the sequence, and the window after it carries
  a certain era anchor.
- A spool that refuses a window leaves the next window's anchor uncertain.
- A ring whose deriver is gone reports `Disconnected` rather than `Dropped`,
  from both branches, and owes the datagram either way.

---

## Order, and why it is this one

1 and 2 first because they are independent, mechanical and reviewable on their
own — and because 2 is what stops task 5 from reimplementing a ledger. Then 3
before 4 because the window reads the ring, 4 before 5 because the spool stores
what the window derived, and 5 before 6 because the pipeline is the three of
them wired together. 7 comes as soon as 6 exists and before any binary work,
because it is the gate: if inline derivation and archive derivation disagree,
nothing in 8 or 9 is worth writing. 8 and 9 before 10, because a binary is the
easiest thing here to get right and the hardest thing to test. 11 last of all,
because a default can only be chosen once both readings exist, and because it is
the only task whose diff changes what a command line already deployed means —
so it wants the whole of the rest of this plan behind it, green, before it is
written.

12 is after all of them because it is a review of all of them, and its own
internal order is the one thing about it that is not free: the design's
corrections land before the code, and within the code the held window comes
before the gate that has to call it. The sequence number, the trailer and the
ring are independent of the blocker and of each other.

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
6. the same configuration with no mode named is refused at startup by key too,
   naming `--archive`, and a configuration naming neither shape is refused
   naming both flags;

and when, in this repository, one synthetic feed derived through both paths
produces row sets that differ in nothing but their provenance, and a gap caused
by a full ring is not attributed to the publisher.

### Where that stands

| | Met | By, or what is missing |
|---|---|---|
| 1 | yes, in this repository | `tests/inline_mode.rs:200`, against a documentation address nothing answers on. Against a real destination: not run |
| 2 | **no** | no live feed and no live column store has been run. The path is covered against a fake sink at `dz-recorder-inline/tests/pipeline.rs:125`, and the derivation against an archive at `inline_vs_archive.rs:243`, but nothing has recorded real traffic |
| 3 | partly | the library behaviour is asserted (`tests/spool.rs:245`, `:283`, `:391`) including the age gauge; the binary-altitude outage run is task 9's missing test |
| 4 | **no** | the spool's own replay is asserted (`tests/spool.rs:444`); the `SIGKILL` of the process is task 9's missing test |
| 5 | yes | `tests/inline_mode.rs:241`, both keys |
| 6 | yes | task 11's two refusal tests |
| the equivalence gate | yes, for eight of nine faults, and over the shape that runs | `inline_vs_archive.rs:256`; `Fault::SilentChannel` is task 7's outstanding bullet. Until task 12 the gate's inline side was its own two-pass arrangement and the derivation stage's was one pass, so the gate was green over a shape nothing ran |
| a ring gap is not the publisher's | yes | `inline_vs_archive.rs:303` |

Two criteria therefore remain open — 2 and 4 — and both need a host rather than
a test: they are the ones the design cannot close in this repository, and they
should not be read as closed because everything runnable here is green.
