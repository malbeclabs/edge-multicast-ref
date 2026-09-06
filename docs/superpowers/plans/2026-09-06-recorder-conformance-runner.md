# Edge Recorder: the conformance runner — Implementation Plan

**Date:** 2026-09-06

**Goal:** Fill `recorder.conformance_finding` with verdicts the specification's own rule set stated, over replayed archive objects, each stamped with the rule set version that produced it — and write no verdict that rule set did not state.

**Architecture:** The shape the loader already has. A pure derivation over one object, its manifest and that object's own loss runs; the column store is one `RowSink` and the file sink is the other. What is new is that the judgement comes from **a process this repository does not build** — the rule set lives in `edge-feed-spec` and is pinned by commit — so the plan puts a declared report format between the runner and that process and tests everything above it against fixtures.

**Tech Stack:** Rust 2021. `dz-recorder-replay` (reading, `OwnedDatagram`), `dz-recorder-archive` (`SegmentManifest`, `JoinedRole`, `InstanceCoverage`), `dz-recorder-loss` (`SequenceRun`, for the absence downgrade), `dz-recorder-rows` (`ConformanceFinding`, `FindingVerdict`, `Grain`, `RowSink`), `dz-recorder-load` (the pass, the walk, the metrics). No async runtime, no ORM, no migration framework — and **no codec crate at all**: the runner never decodes a payload.

**Spec:** `docs/superpowers/specs/2026-09-06-recorder-conformance-runner-design.md`. The verdict vocabulary, the meaning of `rule_set_version`, the four conditions on a `pass` row, the absence downgrade and the refusals are decided there and are not re-litigated here.

**Scope:** That spec's *What this needs that does not exist yet*, minus the two items it names as asks on `edge-feed-spec`. Those are prerequisites, not tasks; decision 1 says what the plan does about them.

---

## What this depends on that is not on `main`

**`origin/jo/recorder-market-data-rows` is in review and this plan builds on it.**
The dependency is on the *loader's* shape and not on the archive walk's:

- **`LoaderConfig` gains a `[[market_data]]` section** on that branch, with
  `derivation_for`, a per-feed switch that is off by default and off by absence,
  a `--check` that prints what was read, and refusals for a feed named twice or
  a `Magic` nobody filled in. Task 7 adds a `[[conformance]]` section in exactly
  that shape. Landing the two independently produces two conflicting edits to one
  file and two different answers to *what does a per-feed switch look like here*.
- **The loader gains a second derivation over the same object**, in
  `market_data.rs`, which establishes the rules this one follows: the object is
  opened again rather than widening the first walk, the identity comes from the
  manifest and never from configuration, a walk that ends anywhere but the end of
  the archive derives nothing, and a failure leaves the *whole* object unloaded
  rather than half of it.
- **The loader gains a per-derivation lag pair** —
  `dz_loader_market_data_unloaded_objects` and
  `dz_loader_market_data_oldest_unloaded_age_seconds` — separate from the load's
  own because they are different numbers. Task 7 adds the third pair on the same
  argument.

**What this plan does *not* need from that branch** is worth stating, because the
branch's headline is a widening of `dz-recorder-relower` — `WireProvenance` now
carries `src`, `dst` and `recv_ts_kind`, and `WireCapture` gained
`state_messages()` and `reference_messages()` beside `messages()`. None of it is
used here. The runner works at datagram grain and hands payload bytes to a rule
set that does its own decoding; the crate that knows what a `Quote` is stays out
of this one entirely.

---

## Four decisions to settle before task 1

Each of these changes the shape of more than one task.

### 1. The report format is the seam, and everything above it is built against fixtures

The spec names two things `edge-feed-spec` must supply and this repository must
not invent: a per-rule outcome that is **placed and ranged**, and a build that
stamps the commit it was built from. Both are smaller than they read, because
both interfaces already exist — the tool writes a declared `-json-report` whose
rule entries carry `rule_id`, `severity` and counts, and `--version` prints
`version+commit`. What is missing is a channel instance and an evidence range on
each entry, and a build that sets those two fields rather than leaving them
`dev+none`.

The gate's interface — an exit code plus violations on standard error, which
`dz-recorder-e2e` matches by substring — stays what it is, and **parsing that
text is refused**: a format nobody declared changes with a log line, and a
`rule_id` recovered by a regular expression becomes an empty string on the day
somebody improves the wording.

So the plan puts a trait at that boundary in task 2 and builds tasks 3 through 7
against recorded fixtures of the report the spec specifies. Those tasks need no
Go toolchain, no network and no binary, and they are where all the judgement
lives. **Only tasks 1 and 8 touch the real tool**, and only task 8 is blocked on
the upstream ask landing. That ordering is deliberate: the expensive, arguable
work — what makes a `pass` honest, what makes a violation ours — is not held
hostage to another repository's release, and the thing that is held hostage is a
wiring test.

### 2. The window is one object, and the runner is pure over it

Everything in this tier rests on a derivation being a function of the object it
was given, so that re-running one replaces its own rows and nothing else. The
runner keeps that: one object, its manifest, and that object's own loss runs.

Two consequences are cheaper to accept now than to discover in task 4.

- **A rule whose state must be carried across a segment boundary is
  `unverifiable` at the head of an object.** A per-instrument state machine
  needs a snapshot anchor that may have been in the previous segment; a cadence
  rule needs the datagram before the window. This is the same answer, for the
  same reason, that the era boundary gets from `anchor_certain` — a reading the
  archive cannot distinguish is not a finding.
- **The runner never consults another object, another site, or a table.** The
  absence downgrade in task 4 uses this object's own `SequenceRun`s, computed in
  the same pass, and not a query against `sequence_gap`. A derivation that reads
  the table it writes beside is a derivation whose output depends on load order.

### 3. The runner keeps its own ledger, and the load ledger is untouched

`Ledger::is_loaded(object_key, object_sha256)` is a boolean over two fields and
is correct for the rows it guards, because those rows are a pure function of
those two fields. A finding is a function of the object *and* a rule set version
that moves independently of this repository's binary, so that boolean answers
*have I judged this* with a confident yes on exactly the day a version bump means
no.

A second ledger keyed `(object key, sha256, rule set version)`, in its own file,
following the existing one's habits: written after the rows land, a torn last
line treated as absent, compaction against the objects still present.

Widening `Entry` was considered and rejected — it carries a `SegmentTrailer` for
the era adjacency check, which has nothing to do with rule sets, and the boolean
that guards datagram rows must not start depending on whether a Go binary was
rebuilt.

### 4. The e2e conformance helper is promoted, not copied

`dz-recorder-e2e/tests/common/conformance.rs` already replays an archive, writes
a classic pcap and runs the tool. Four tests depend on it — one per feed
flavour, plus two negative controls that exist to prove an exit code of 0 means
*validated and clean* rather than *saw nothing*.

It moves into the runner's crate and is widened for the three cases the spec
names — the two pcap lengths written separately, captured link headers preferred
over synthesised ones, one file per group — and **those four tests become its
first consumer rather than a second copy of it**. A bridge with two
implementations is a bridge where the gate and the runner can disagree about what
the tool was shown, and the gate is the one nobody would think to re-check.

---

## Global constraints

- **Nothing here touches the record path.** No change to `dz-recorder`,
  `dz-recorder-capture` or `dz-recorder-archive` is in scope. A change that
  appears to need one is a signal that the judgement is being put in the wrong
  process.
- **No rule is written, encoded, enumerated or allow-listed in this repository.**
  `rule_id` travels through as an opaque string. A runner that knew the names of
  rules would refuse the next one added.
- **The runner writes only `conformance_finding`.** It decides no attribution and
  touches no gap row.
- **Off by default and per feed**, at every stage. No task turns it on for
  anything.
- **Every task is verifiable with no server, no network and no Go toolchain**
  except tasks 1 and 8, which are feature-gated behind the tool the CI job
  already builds from a pinned checkout.

---

## Tasks

### 1. The pcap bridge, promoted out of the test module and widened

Move the replay-to-pcap conversion into `dz-recorder-conformance` and correct the
three things that make the e2e version specific to its fixtures:

- **The two record lengths are written separately.** The helper writes one value
  into both, which asserts *not truncated*; a datagram whose `wire_payload_len`
  exceeds its payload length is one the capture cut short, and declaring it
  complete hands the rule set a body shorter than its declared length — a
  violation the recorder caused.
- **Captured link headers are used where the archive has them.**
  `RecordedDatagram::link_headers` is `Some` when the capture mode read the
  Ethernet, IPv4 and UDP bytes off the interface, and `None` when they were
  synthesised. Rebuilding over the captured case discards the identification
  field, the fragmentation flags and the checksums the archive kept on purpose.
- **One file per group.** The tool takes one `-group`, so an archive holding
  several needs one invocation each.

**Verification:** `cargo test -p dz-recorder-e2e --features conformance`, with the
four existing tests unchanged in what they assert and reaching the tool through
the new crate — the negative controls are what prove the bridge still shows the
rule set a stream it can fail. Beside them, unit tests needing no tool: a
truncated datagram writes an included length below its original length and an
untruncated one writes them equal; a datagram with captured headers reproduces
those bytes rather than synthesised ones; two groups in one archive produce two
files, each holding only its own datagrams.

### 2. The tool boundary, as a type, and the version behind it

A trait with one method — given a pcap, a group, the three ports and a feed,
return a report — and one real implementation that runs the tool. The report is
the shape the spec asks upstream for: one entry per rule evaluated, naming the
rule, the outcome, the channel instance and the sequence range its evidence lies
in.

The three exit codes map to three categorically different results and never
collapse: 0 and 1 both yield a report, and 2 yields an **error**. An exit the
runner cannot interpret, or a report it cannot parse, is likewise an error.

Version resolution lives here. The runner asks the tool which rule set it is; a
tool that cannot say, or that says something other than the configured value, is
a refusal that names both.

**Verification:** unit tests against a stub implementation and against a recorded
report fixture. Exit 2 produces an error and never a verdict of any kind — the
test asserts zero rows, because a table full of `unverifiable` on a missing
binary would move the panel that measures how often the archive opens the gate.
An unparseable report produces an error and never an empty set of passes. A
version that cannot be resolved produces no rows, and the error names the tool.

### 3. `na`, and where a finding is placed

Two rules, both read from the manifest and neither from configuration:

- A rule needing a port role that `roles_joined` never claimed is `na`. So is one
  needing a role that was joined but contributed no entry to the manifest's
  `instances` map — a port that carried nothing looks exactly like a clean feed,
  and the manifest is what tells them apart.
- A finding is placed on the channel instance the report names. A report entry
  naming none is **refused and counted**, never filed under a guess: a finding on
  the wrong instance sends a reader to a sequence space where the evidence is not.

**Verification:** unit tests over manifest fixtures. A manifest whose
`roles_joined` omits the snapshot role yields `na` for a rule that needs it and
leaves the mktdata rules alone. A manifest that claims the role but whose
`instances` map has no entry on that port yields `na` too, and the test asserts
the two cases produce the same verdict for different reasons — because the code
paths differ and only one of them is obvious. A report entry with no instance
raises the refusal counter and produces no row.

### 4. The absence downgrade

A `violation` whose evidence range overlaps a `SequenceRun` this object's loss
derivation produced for the same instance and era becomes `unverifiable`, keeping
the rule's own message and the missing range in `detail`.

**Verification:** golden tests over the synthetic publisher in
`dz-recorder-replay`. A structural violation with no hole near it stays a
`violation`. The same violation with a gap injected across its evidence range
becomes `unverifiable`. And the test this task exists for: an archive recorded
through a `StarvationWindow`, so that the hole is demonstrably the recorder's
own — **revert the downgrade and this fixture reports a publisher-facing
violation manufactured out of our own drop**, which is the mutant the test has to
kill and the failure mode the whole design is arranged against.

### 5. The finding rows

`ConformanceFinding` from the report, the manifest and the run: `window_start`
and `window_end` from the manifest's `start_ns` and `end_ns`, `first_seq` and
`last_seq` from that instance's `InstanceCoverage`, `object_key` from the
manifest, `run_ts` from the run, `rule_set_version` from task 2. Identity — `site`,
`recorder`, `env`, `feed` — from the manifest, never from the loader's own
configuration.

`pass` is written under the four conditions the spec states, and under no others.

**Verification:** golden rows through `FileSink`. A clean object produces one row
per rule per instance, all `pass`. The sort key behaves as the design needs:
two rule set versions over one window produce two rows that both stand, a re-run
of one version over one window produces one row, and a re-run of an *older*
version after a newer one does not displace the newer. A test asserts that no row
is produced for an object whose digest did not match, so that a refusal is an
absence rather than a window of passes.

### 6. The second ledger

Keyed `(object key, sha256, rule set version)`, in its own file on the runner's
own writable path, with the load ledger untouched.

**Verification:** unit tests. An object judged under one version reads as
unjudged under the next. A torn last line is treated as absent and the object is
re-judged, which the tables make a replace. Compaction drops entries for objects
no longer present. And one test asserts the thing decision 3 exists for: the load
ledger's `is_loaded` still answers only about rows, and nothing in this file
changes what it says.

### 7. Configuration, `--check`, and the metrics

A `[[conformance]]` section in `LoaderConfig`, off by default and off by absence,
in the shape `[[market_data]]` establishes: named per feed, `deny_unknown_fields`,
a duplicate feed refused, and a `--check` that prints back what was read.

The tool's path is **required with no default**. A runner that found the binary on
`PATH` would stamp its verdicts with whatever rule set happened to be installed,
which is the same failure `magic` is required to prevent one layer down.
`--check` resolves the version and prints it.

Its own lag pair, and counters for each refusal the spec names: a tool that could
not run, a verdict naming no instance, an unresolvable version, and a
configuration that disagrees with the tool.

**Verification:** unit tests in the loader. A configuration that says nothing
judges nothing, prints no conformance line, and — asserted against the datagram
rows — changes nothing else. A missing tool path is a `--check` failure and not a
default. A misspelled key in the new section is refused rather than defaulted, as
every other section's is. A duplicate feed is refused.

### 8. End to end, behind the `conformance` feature

Encode with the real encoder, record with the real writer, judge with the real
tool, write through `FileSink`, and assert the rows. The two negative controls
from `dz-recorder-e2e` become row assertions rather than exit-code assertions:
the stream carrying a `Change` with a zero quantity produces a `violation` row
whose `rule_id` is the one that stream breaks, and the malformed snapshot cycle
produces its own.

Then the same streams recorded through a starvation window, asserting
`unverifiable` where the object's own holes cover the evidence.

**Blocked on the upstream ask.** Until the rule set emits a declared per-rule
report, this task cannot assert a `rule_id` on a row — only that the tool ran.
That is the whole of what the ask costs, and it is one task rather than seven
because of decision 1.

**Verification:** the tests themselves, in `dz-recorder-e2e`, feature-gated as the
existing four are and failing rather than skipping when the tool is absent.

### 9. The sizing measurement

Three numbers, measured against a recorded archive before any feed is judged, for
the same reason the market data plan measures its multiplier: they are properties
of the archive and the rule set rather than of this code, and they cannot be
assumed from another feed.

- **Temporary bytes per object**, against the manifest's `payload_byte_count` and
  `datagram_count` — the prediction the runner uses to refuse an object it has no
  room for, checked against what the bridge actually writes.
- **Wall time to judge one object**, beside the wall time to load it, so the third
  walk's cost is a number rather than an adjective.
- **Invocations per object**, which is groups per object and is a property of what
  the recorder was asked to join.

**Verification:** run against a recorded archive and report. This task produces
numbers, not behaviour, and the numbers are what a decision to judge a feed is
made against.

---

## What is not in this plan

- **The two upstream asks.** A placed and ranged per-rule outcome, and a build
  that stamps its commit — see decision 1 for what already exists of each
  its version are changes to `edge-feed-spec`, proposed there. This plan is
  arranged so that everything except task 8 lands without them.
- **Any conformance rule.** Not written, not encoded, not enumerated, not
  allow-listed. The rule set is the specification's, and this repository's only
  decision is which commit of it ran.
- **Attribution.** The runner declines to accuse a publisher over a hole and does
  nothing else about it. Whose the hole is stays `sequence_gap`'s question.
- **Cross-site conformance.** A rule set reads one feed as delivered to one
  observation point. What two sites answer together is loss, and that is a query.
- **A bulk re-judgement of the retained archive.** The spec states why a version
  bump must be a deliberate, bounded act rather than a consequence of a deploy;
  the mechanism for choosing a range of objects to re-judge is a separate piece of
  work and this plan deliberately ships no automatic path to it.
- **Any deployment.** No release, no account, no schedule. The loader's own
  deployment does not exist yet either, and that is a prerequisite this plan
  inherits rather than solves.
