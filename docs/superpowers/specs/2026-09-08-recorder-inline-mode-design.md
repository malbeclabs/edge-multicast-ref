# Inline mode: one recorder process, from capture to rows

**Status:** draft, pending review
**Applies to:** `rust/recorder/`
**Authority:** [`edge-feed-spec`](https://github.com/malbeclabs/edge-feed-spec), its [`GLOSSARY.md`](https://github.com/malbeclabs/edge-feed-spec/blob/main/GLOSSARY.md) and [`VERSIONING.md`](https://github.com/malbeclabs/edge-feed-spec/blob/main/VERSIONING.md)
**Builds on:** [2026-08-28-edge-recorder-crates-design.md](2026-08-28-edge-recorder-crates-design.md), [2026-08-31-sequence-loss-and-conformance-rows-design.md](2026-08-31-sequence-loss-and-conformance-rows-design.md)

---

## Naming

This repository is public. This document names no venue, venue repository,
venue crate, config key, metric prefix or issue tracker, and gives no count of
recorder hosts. `GLOSSARY.md` governs all vocabulary.

---

## Purpose

A recorder host today runs two processes. One captures a feed and leaves hashed,
manifested objects in a directory; the other walks that directory, derives rows
and loads them into a column store. That arrangement is the one the recorder
crates design settled and it is not in question here.

This defines a **second arrangement**: one process that captures a feed and
derives its rows directly, keeping no datagrams. Neither arrangement is a
default — each is selected by the configuration that only it can run on, and
*[The configuration states the arrangement, and no flag names it](#the-configuration-states-the-arrangement-and-no-flag-names-it)*
is how. It exists for three cases.

**Bringing up a feed.** The question during bring-up is *are the rows right*,
and the loop that answers it today is minutes long: rotate an object, wait for a
pass, query. Deriving inline makes it seconds.

**Hosts where a datagram archive is not the point.** The staging budget is sized
as retention × bytes per second and it is the number that decides host sizing —
the recorder crates design says so plainly. A host that wants the rows and was
never going to keep the bytes is paying for a disk it has no use for.

**One deploy unit.** One binary to pin, one configuration bundle, one metrics
port, one service to stop and start.

The two arrangements are named throughout as follows, and the words are the ones
this document introduces:

| | What it is | How it is entered | Where it is defined |
|---|---|---|---|
| **archive mode** | `dz-recorder` writes objects; `dz-recorder-load` derives rows from them. Unchanged, including how it is entered. | `archive.staging_dir` and `archive.completed_dir`, which it has always required | the recorder crates design |
| **inline mode** | one process captures, derives and loads. Keeps no datagrams. | `--inline-config`, the file carrying the spool, the ledger and the destination it cannot run without | here |

---

## The decision this contradicts, and how far

The recorder crates design decides: **the archive is bytes, not rows.** *"Rows
are derived and re-derivable; bytes are not. A recorder that stores only its own
interpretation has thrown away the ability to be wrong about it."* The single
strongest argument for keeping the bytes is stated there too: a conformance rule
set is a growing thing, and an archive is what lets it grow backwards.

Inline mode keeps no bytes. That is a real loss and this document does not
soften it:

- **A rule written next month cannot be run against last month's traffic.** There
  is nothing to run it against.
- **A row cannot be re-derived.** A derivation bug found later is a bug in rows
  that cannot be corrected in place, only stopped.
- **Nothing verifies what the derivation read.** In archive mode a digest
  mismatch means no row is derived; inline there are no stored bytes to disagree
  with.

Three things bound that loss, and they are why the mode is defensible rather
than merely cheaper.

**Neither arrangement can be entered without being stated, because each is
named by what it cannot run without.** Archive mode requires
`archive.staging_dir` and `archive.completed_dir`; inline mode requires a spool
directory, a ledger and a destination, which live in the file `--inline-config`
names. Not one of those five has a defensible value to invent, so a
configuration cannot be ambiguous about the arrangement without stating **both**
— refused — and cannot be silent about it without stating **neither** — refused
too. There is no third case and there is no silence left over for a default to
be placed on.
*[The configuration states the arrangement, and no flag names it](#the-configuration-states-the-arrangement-and-no-flag-names-it)*
argues the selection;
*[What it costs](#what-it-costs)* names what it takes.

Archive mode is also still what a host recording a production feed for evidence
should run, and it says so by carrying the two directories that arrangement
writes into.

**The rows say so.** A `derivation` column carries `archive` or `live` on every
row of every grain. No query can mistake a row derived from verified bytes for
one derived in flight, and a dashboard that must not mix them can filter.
Without that column the two modes' rows are indistinguishable in one table,
which is the failure this design would otherwise introduce and never detect.

**The derivation is the same function.** Inline mode does not reimplement
derivation. It calls `dz-recorder-rows::derive` — the same function archive mode
calls, over the same `Source` trait — and the gate on this whole design is a
test that feeds one synthetic feed through both paths and asserts the rows are
equal but for their provenance. What inline mode changes is where the datagrams
came from and how long they are kept, never what a row means.

---

## Why the arrangement is stated and never defaulted

The recorder crates design's *"the archive is bytes, not rows"* is still the
stronger argument for a host recording a production feed for evidence, and
nothing here softens it. This section does not answer which argument is
stronger. It answers a different question: **what does a configuration that
says nothing mean** — and the answer is that it means nothing, and is refused.

**A default belongs on the arrangement whose wrong choice is loud, and neither
of these is.** Put the default on archive mode and a host that meant inline mode
and said nothing gets a running recorder: it joins the feed, writes objects,
publishes them and reports itself healthy. Nothing derives them, because the
second process was never deployed. The symptom is an empty table, and an empty
table is indistinguishable from a feed nobody published on — which is the one
diagnosis this whole tier exists to make. Put the default on inline mode and a
host that meant archive mode and said nothing needs a spool directory, a ledger
and a destination it has not got, so it is refused — loudly, but for the wrong
reason, telling an operator about a file they never wanted rather than about the
arrangement they meant.

So the loud reading is not one of the two arrangements. It is the **refusal**,
and once the refusal is what silence means, there is nothing for a default to
decide. That is the whole of this section's argument, and the four cases below
are it in a table.

### Four cases, and two of them run

| `[archive]` directories | `--inline-config` | What runs |
|---|---|---|
| set | not given | **archive mode** |
| not set | given | **inline mode** |
| set | given | **refused**, naming `archive.staging_dir` and the file: two arrangements that keep different things |
| not set | not given | **refused**, naming both: nothing states an arrangement, and neither has a value worth inventing |

**It is total because each arrangement requires what the other has no use for.**
Archive mode has required `archive.staging_dir` and `archive.completed_dir`
since it existed, and refuses to start without them — a host recorded nothing
before this document and records nothing after it if they are absent. Inline
mode requires a spool directory, a ledger and a destination, none of which the
recorder's own configuration carries or should: the second file exists precisely
because those are what inline mode needs and archive mode does not. So the two
sets of keys are disjoint, each set is required by exactly one arrangement, and
neither set has a defaulted member. The four rows above are therefore every
case, and no fifth one is reachable.

**Every mode change is an edit that states the new mode, and every half-finished
edit is a non-zero exit code.** This is the property a default cannot have. An
operator who deletes an `[archive]` section to stop keeping bytes for an
afternoon has reached the fourth row, not inline mode: the recorder refuses,
names both, and nothing has been recorded wrongly because nothing has been
recorded. An operator who adds `--inline-config` to a host that still carries
the directories has reached the third row and is told which two statements
disagree. Neither host can drift into an arrangement nobody chose, and neither
finding arrives weeks later as a year of retention that was never kept.

### Why this is not the inference the `derivation` column exists to forbid

An earlier draft of this document rejected selecting the arrangement from the
configuration, by analogy: the `derivation` column is a column rather than a
test for an empty digest, *because a reader should not have to know that one
value means nothing was ever verified*. Selecting a mode by whether a key has a
value, the argument went, is that trap one layer up.

**The analogy does not carry, and the difference is the whole of it.** What the
`derivation` column replaces is a reading of an **absence** — an empty `sha256`,
which is a hole a writer left and which a reader can only interpret by knowing
how that writer behaved. Nobody stated it. Two directories in a configuration
file are the opposite kind of thing: a **positive statement, written by the
person who took the decision, in the file that is that decision's record.**
Reading them is not inferring provenance from a hole; it is reading what was
written, in the only place it was written.

The analogy also fails on the failure it predicted. It predicted an operator who
deletes an `[archive]` section and silently changes what a month of rows means.
That requires a silence to be readable as an arrangement, and the fourth row is
why there is none: deleting the directories and stating nothing else is refused.
The prediction was correct about a *two*-case rule and wrong about a total one.

And a flag would not have removed the reading, only moved it. `--archive` on a
command line with no directories cannot record an archive — archive mode refuses
by key — so the flag never was the thing that selected the arrangement. The
directories always were. What the flag added was a second place to say it, which
is the next section.

---

## Whether a flag survives as an explicit override

**Decided: no. There is no `--archive`, and no flag names the arrangement.**

The reading above needs no flag, and this section is why adding one back would
make the design worse rather than more explicit. Four reasons, and the second is
the one that decides it.

**A flag has no case of its own left.** Every command line on which `--archive`
would be legal is one whose configuration already selects archive mode, and
every command line on which the configuration says otherwise is one of the two
refusals. So the flag can only ever agree with the configuration or be refused
by it. A word that decides nothing is a word an operator has to maintain, get
right, and reconcile against the file that does decide.

**What is left of it is the one thing it must not do.** The only power an
override can add is the power to *resolve* a refusal. Resolving the third row
starts a recorder whose configuration names an archive it will never write —
which is a host somebody believes is keeping bytes for a year that it never kept
for a second, the exact failure this arrangement's refusals exist for. Resolving
the fourth starts one on paths nobody stated. An override is therefore a way to
start a recorder in an arrangement the operator's own configuration contradicts,
which is the thing this whole section is trying to make impossible; and it would
arrive as a flag rather than as a default, which changes nothing about the
outcome.

**It would state in a second place a thing the configuration already states in a
load-bearing one.** The arrangement is not a key in the recorder's own file
because `config_hash` is provenance and any new key rewrites the hash of every
configuration in the fleet. It is not a key in inline mode's own file because
that file is read by only one of the two arrangements. The same objection
retires the flag, and more sharply: a token that names the arrangement and does
nothing else can **disagree** with the paths that do the work, and then somebody
has to decide which wins. Each arrangement is now named by the resource it
consumes — archive mode by the two directories it writes into, inline mode by
the file carrying its spool, ledger and destination — and a name that is also
the thing cannot disagree with itself.

**What an override would have bought is bought better, and totally, by
`--check`.** The real value in a flag is a deployment pipeline pinning the
arrangement it believes it is deploying. `--check` does exactly that and does it
for every host rather than for the ones that remembered the flag: it refuses
unless exactly one arrangement is stated, prints the arrangement as the **first
line** of its output, and exits non-zero — before anything is restarted, which
is why that subcommand is an `ExecStartPre` rather than a convenience. An
assertion that only protects the hosts carrying it has the same defect a default
has.

`--archive` therefore does not exist, and a command line carrying it gets the
usage error any unknown flag gets: the message, `USAGE`, and exit code 2. That
is the loudest answer available for a word the binary does not have, and it is
the right one for a word that has never had a meaning in a released build.

**What this depends on staying true.** Stated here so that a later change cannot
undo it quietly: `archive.staging_dir` and `archive.completed_dir` must stay
required with no default in archive mode, and `inline.spool_dir`,
`inline.ledger` and the destination must stay required with no default in inline
mode's own file. A defaulted `staging_dir` would make every configuration in the
fleet state archive mode. A defaulted spool directory would make the second file
optional, and an optional file cannot select an arrangement. Both are held by
tests rather than by this paragraph.

---

## What it costs

One cost, and it is a build-time one. The two costs an earlier draft of this
document carried — a word on every archive-mode command line, and a fleet-wide
edit to add it — are what selecting from the configuration removes, and they are
recorded below as the reason it was chosen.

### Nothing outside this repository changes

This is the argument that decided the selection against a flag, and it is worth
stating as a cost table with nothing in it.

Archive mode's configuration has always carried `archive.staging_dir` and
`archive.completed_dir`, because archive mode has always refused to start
without them. So every unit, pipeline and runbook that starts `dz-recorder`
today keeps working unchanged: the host selects archive mode by saying what it
already said, in the file it already said it in. Inline mode's command line
carries `--inline-config` because the mode needs the file, so it too states its
arrangement by carrying what it cannot run without. There is no `ExecStart` to
edit, no `ExecStartPre` to edit, no restart-triggered failure attributed to
whatever else was in flight, and no infrastructure repository this one does not
contain to land a change in.

The code is smaller as well. The condition that selects the arrangement is the
condition that already existed: the refusal an inline-mode host got for a
configured archive directory read those two keys in order to name them, so the
same read chooses the arrangement instead, and the separate check disappears
into it.

### The mode is a default build feature, for a new reason

Inline mode is behind a build feature so that a recorder that only records
carries no column-store client, no HTTP client and no row crates. That argument
is untouched and the feature stays. `inline` is nonetheless in the **default**
feature set, and the reason has changed with the selection: it is no longer that
a binary must be able to honour its own default mode, because there is no
default mode.

The reason now is that **the arrangement is a property of a host's configuration
and the binary is not.** Configuration is rendered per host; the released
recorder asset is one asset for the fleet. A default build that carried only one
arrangement would have to be matched to configurations at deploy time — a second
thing to get right, whose wrong answer is a startup refusal on a host whose
configuration was correct. So the default build carries both, and the record-only
build is asked for by name with `--no-default-features`.

What that costs is real and small: every default build of the recorder compiles
and links the column-store client, an HTTP client and the row crates. The
property those crates were kept out for — **nothing in the record path reaches
the destination** — was never enforced by the feature and is not weakened here:
it is enforced by the capture path's own rule that it never blocks and never
parses, and by the derivation and posting stages living off that path entirely.
The feature buys a smaller build, not a safer one.

A `--no-default-features` build can only ever be in archive mode, so a
configuration that selects inline mode — a second file given, no archive
directories — is refused there by the feature's name, rather than falling back to
archive mode without being asked. That refusal exists only in the builds that
need it, and it is reached by a configuration that positively asked for the
arrangement this binary has not got.

---
## Whether inline mode could write an archive beside its rows

Decided: **it could not, and there is no third arrangement.** Inline mode keeps
no datagrams, and the code implements exactly that — one `Arrangement` per run,
the two mutually exclusive by construction, and no configuration able to state
both without being refused.

The alternative has to be answered rather than waved at, because it is the
obvious way to make the third row of
*[Four cases, and two of them run](#four-cases-and-two-of-them-run)* run instead
of refuse. If a host stating the archive directories *and* a second file kept its
bytes and got its rows, nothing would be refused and no evidence would be at
risk. Four reasons reject it, and the third is the one that decides it here.

**It would be the most expensive arrangement to be in by accident.** A host in
it is sized for retention × bytes per second — the exact cost this mode exists to
avoid — and needs a spool, a budget and a destination on top. It is the only
arrangement carrying two disk budgets and two sizing questions, and the case that
motivated the mode at all is served by neither it nor by archive mode.

**It is the one arrangement no test covers.** The gate this design rests on
compares two paths and asserts their rows are equal but for provenance. A third
path that runs both at once has its own interleaving, its own backpressure and
its own shutdown ordering, and an equivalence test between the other two
exercises none of them.

**It would make the selection non-total, which is the property everything above
rests on.** The four cases work because the two key sets are disjoint and each is
required by exactly one arrangement — so *both stated* has no reading and is
refused. An arrangement that writes both is precisely a reading for that row:
`archive.staging_dir` accepted alongside a second file. Grant it and the third
row stops being a refusal, and a configuration that states both is no longer a
contradiction somebody has to resolve — it is a running recorder in the
arrangement nobody sized a host for. The refusal is not an inconvenience the
selection tolerates; it is what makes the selection a reading rather than a
guess.

**Writing both is not free at the capture.** Every datagram would go to the
archive writer *and* into the ring: a second consumer on the record path and a
second place backpressure can appear. The capture path never blocks, and holding
that against two sinks with unrelated failure characteristics — a full disk and
a slow destination — is a harder property than holding it against one.

A host that wants both runs archive mode and derives from the objects, which is
what archive mode is. What it gives up against a hypothetical both-arrangement is
latency to the first row, and that latency is the trade the two modes were drawn
around.

---

## What this builds on and does not rebuild

**`Source` is the same trait in both halves.** That is the load-bearing detail
of the recorder crates design, and it is what makes inline mode small: a live
capture and a replayed archive present identically, so derivation runs over
either. Inline mode adds a third implementation of the same trait and touches
`dz-recorder-rows` not at all.

**Derivation is already a pure function of one window.** `derive` reads a
`Source` to exhaustion and returns a `RowBatch` plus a trailer; the only thing
it consults outside the window is the preceding window's trailer, and that
decides exactly one bit. Nothing in it knows what an object is. The object was
never the unit of derivation — it was the unit of *storage* — which is why a
window with no object behind it derives correctly.

**The loss debt already exists.** `PendingLoss` in `dz-recorder-capture` is the
rule that a datagram the record path could not take is charged to the next one
that gets through. Inline mode adds one more place a datagram can be lost, and
reuses that type rather than restating its arithmetic.

**The ledger already exists.** `dz-recorder-load` owns a ledger keyed on the
loaded unit, so a restart posts nothing it has already posted. Inline mode's
spool needs exactly that and reuses it. What it does not inherit is the ledger's
other half: the trailer that lets a loader resume with the certainty a
continuous run had settles nothing across a *recorder* run boundary, for the
reason in *[The era anchor gets better, not worse — and stops at the run
boundary](#the-era-anchor-gets-better-not-worse--and-stops-at-the-run-boundary)*.

**Deduplication does not depend on the object.** No `ORDER BY` in the checked-in
DDL includes the object key or its digest; the eight tables collapse on the
channel instance and the row's own identity. A retried insert is a replace
whether or not an object ever existed, so inline mode needs no synthetic
idempotence key and no schema change to the deduplication.

---

## Architecture

```
        ┌───────────────────── recorder host, one process ─────────────────────┐
        │                                                                     │
 feed ─►│  capture: membership, kernel receive timestamps, drop accounting    │
        │     │                                                               │
        │     ├──► Observer (health, header-only) ──────────────────────────► │──► /metrics
        │     │                                                               │
        │     ▼                                                               │
        │  bounded ring of pooled datagram slots                              │
        │     a push that does not fit: charge the loss, never block          │
        │     │                                                               │
        │     ▼                                                               │
        │  derivation: WindowSource ──► derive() ──► RowBatch                 │
        │     │                                                               │
        │     ▼                                                               │
        │  spool on disk: one window, one directory of rows, fsync at close   │
        │     │        a byte budget, evicting the oldest, never blocking     │
        │     ▼                                                               │
        │  posting: oldest window first ──► RowSink ──► ledger ──► delete     │
        └─────────────────────────────────────┼───────────────────────────────┘
                                              ▼
                                        column store
```

Three stages and not two. `RowSink::write_batch` posts synchronously and retries
what the destination refuses, so a destination that is slow would, on the
derivation stage, fill the ring backwards until it reached the capture.

### The capture path still never blocks, and now there are two places it could

The recorder crates design gives one rule above the others: **the capture path
never blocks and never parses**, because both are how a recorder manufactures
the loss it was built to measure. Inline mode introduces two new places that
rule can be broken, and answers both the way the archive already answers its
own.

**The ring, between capture and derivation.** A push that does not fit drops the
datagram and charges its loss to the next datagram that is accepted, through
`PendingLoss`. This is not a nicety. A datagram the deriver never sees is a
sequence value nobody delivered, and a sequence value nobody delivered with
nothing admitted behind it is a **publisher** verdict on a gap the recorder
caused. The debt is what makes the gap ours in the row.

**The spool, between derivation and posting.** When the byte budget is full the
oldest window is evicted and counted, and derivation is never blocked. A spool
that applied backpressure would stall derivation, fill the ring, overflow the
receive queue, and convert a column-store outage into feed loss — the same
inversion the staging budget exists to prevent. Losing bounded history is
recoverable; contaminating live data is not.

Both are the archive's rules, restated for a different unit. Neither is new
policy.

---

## The window: what replaces the object

Archive mode's unit is an object: a rotation bound's worth of datagrams,
compressed, hashed and described by a manifest. Inline mode's unit is a
**window**: the same bound, in memory, derived and then discarded.

A window closes on bytes or on age, whichever comes first, for the reasons
rotation does: a size bound keeps windows uniform for the analysis tier, and an
age bound keeps a quiet feed's rows moving. The bound lives in inline mode's own
configuration, not in `[archive]`, because it bounds a derivation and not a
file.

`derive` requires a manifest, and a window has no object to describe. It
synthesises one, under the discipline socket-mode capture already applies to the
IP headers it synthesises: **a field the recorder did not observe is never
written as though it had been.**

| Manifest field | In inline mode |
|---|---|
| `site`, `recorder`, `env`, `feed`, `build_version`, `build_commit`, `config_hash` | Observed. The recorder's own identity, exactly as archive mode writes it. |
| `segment_seq`, `start_ns`, `end_ns`, `datagram_count`, `payload_byte_count`, `instances`, `short_datagrams`, `instances_dropped`, `capture_drop_scope`, `roles_joined`, `link_headers`, `link_header_exceptions` | Observed. The window counts what the writer would have counted, from the same datagrams. |
| `capture_drop_total` | Observed, **cumulative and never reset** — see *[The one column whose zero is an accusation](#the-one-column-whose-zero-is-an-accusation)*. Every `drop_delta` the capture has declared this run, plus every datagram this recorder's own ring refused, read from the ring's counters when the window closes. |
| `interface_drop_total` | **Zero, and the same zero archive mode writes.** Loss upstream of the capture point is read per capture handle, and the manifest's own accounting for it is per port role, so the record path routes it to the health tier and never into a segment. No admissibility gate reads it — it is not in `segment_overflow`, and the only verdict it can reach is `upstream`, which is an exculpation and not an accusation — so a zero there withholds an explanation rather than manufacturing one. A number in one mode only would make the two modes reach different verdicts on the same traffic, which is the equivalence the whole design rests on. |
| `object_key` | The window's key. It carries the window's start in wall-clock nanoseconds, so it is unique and orders windows across runs — but it names no object anyone can fetch. |
| `sha256`, `byte_count` | **Empty and zero.** No datagrams were kept, so nothing was hashed. An invented digest is worse than an absent one: it is a claim that something was verified. |

The rows' `derivation` column is what a reader is meant to consult, so that an
empty digest is corroboration rather than the only signal.

### The window is walked twice, and the second walk is not an optimisation

`derive` stamps the manifest's key, its window sequence and its start and end
onto every row as it reads, so the manifest has to exist before the first
datagram is taken. A window's manifest describes what the window saw, and a
window has seen nothing until it has been walked. Those two facts do not fit in
one pass — and an object never had to make them fit: the writer counted as it
wrote the object, and the loader walks the finished file again to derive.

A window is the object's replacement, so the window is what has to be readable
twice. The derivation stage therefore **drains the window into memory, builds
the manifest from the completed tally, and derives from the buffer.** The buffer
is reused window after window, slot by slot, each keeping the payload capacity
it was allocated with — the ring's own pooling discipline, for the ring's own
reason.

What a single pass costs is not a slower derivation. It is a manifest built from
an empty tally, which writes:

| Field | What a manifest built before the walk carries |
|---|---|
| `start_ns`, `end_ns` | `0`, so every `segment_coverage` row is stamped at the Unix epoch |
| `instances` | empty, so the window writes **no** `segment_coverage` row at all |
| `object_key` | `live/…/0-<window_seq>` — one key for window *k* of every run this recorder ever makes |

The third is data loss rather than a wrong number.
`recorder.segment_coverage` is a `ReplacingMergeTree` whose sort key ends in
`start_ts`, and `window_seq` restarts at zero on every run, so with the stamp at
zero the second run's window *k* carries the first run's sort key and replaces
it. The window key carries a wall-clock start precisely so that cannot happen,
and a manifest built before the walk is how the guard is lost.

### The one column whose zero is an accusation

`capture_drop_total` is not a diagnostic. It is read by
`recorder.segment_overflow`, which subtracts consecutive windows of it and calls
a zero difference `overflow_free = 1` — and `overflow_free` is one of the four
conditions deciding whether **a site's absence may be used as evidence about
the publisher**. The cross-site view states the rule it enforces: a site that
dropped datagrams itself cannot contribute an absence, because its gap may be
its own ring, and counting it as evidence about the publisher is the
subtraction the drop scope exists to forbid.

So a wrong zero here is not a missing number. It is this path: kernel
receive-queue overflow on this host, datagrams the derivation never sees, an
absence in the sequence, `overflow_free = 1`, and the cross-site machinery
admitting that absence as evidence *against the publisher*. That is the finding
this recorder exists to make correctly, made against the wrong party, by a
column nobody looked at. Row-level attribution is unaffected — that travels on
`drop_delta` through the ring's debt — and it is the only half that is.

Two consequences for what inline mode writes.

**It is cumulative, never per-window.** The view computes
`c.capture_drop_total - least(p.capture_drop_total, c.capture_drop_total)`,
which is a delta over a running total. Given per-window figures, a window that
dropped less than its predecessor subtracts to zero and is certified clean, so a
per-window number is the same defect at lower frequency rather than a fix.

**It includes what the ring refused.** The ring is inline mode's own place to
lose a datagram and it does not exist in archive mode, so a datagram it dropped
is exactly the kind of loss `overflow_free` must refuse to certify away. The two
summands are what the capture declared and what the ring refused; both are
cumulative counters on the capture thread, which is where the facts are, and the
derivation stage reads them through the ring's own counters when a window
closes.

### An empty window spends no window sequence number

A hole in `segment_seq` is how a reader learns the derivation had one, and it is
the whole of what distinguishes a recorder that was down from a feed that was
quiet. A quiet feed closes windows on age, and that is ordinary — so a window
that saw nothing leaves the sequence where it found it. Spending a number on it
would put the hole that means *the derivation was down* in front of a reader
whose feed was merely silent, once per window bound, for as long as the silence
lasted.

It is also what keeps the era anchor certain across the silence. The
predecessor test is `segment_seq + 1`, so an empty window that spent a number
would leave the next window's trailer two behind it, and every window following
a quiet stretch would write an uncertain anchor.

### The era anchor gets better, not worse — and stops at the run boundary

`Era::anchor_certain` is written uncertain when the preceding window's trailer
is unknown, and in archive mode that is routine: the staging budget evicts, and
the predecessor of the oldest surviving object is regularly gone. Inline
windows are strictly sequential and none is evicted before it is derived, so the
anchor is certain from the second window of a run onward. That is the one
analytical result inline mode improves, and it stops where the run does. **The
first window of every run anchors on nothing, and that is the decision rather
than the outstanding half of it.** The rest of this section is why.

**What an uncertain anchor costs, traced rather than assumed.** The trailer is
written to the ledger and is not read back, so the first window of every run
derives with no predecessor. Following that through: the derivation's own
verdict function cannot reach `publisher` at all — it is not among its outcomes,
by design, because `publisher` needs a datagram absent from every site and one
vantage has neither half of that — so the first window of a run answers
`recorder` where its residue is fully admitted and `unverifiable` otherwise. The
cross-site pass that turns `unverifiable` into `publisher` requires, per gap
occurrence, that the vantage's era boundary be settled: `anchor_certain = 1`. An
uncertain anchor therefore makes that window's absences **inadmissible**.

So the cost is a window's worth of evidence, once per restart, and never an
accusation drawn from ignorance. That is the right direction for the failure to
lean, and the three questions below are whether the read that would buy the
evidence back can be made without leaning the other way. It cannot.

**A first run has no trailer legitimately, and an unreadable ledger is a
different thing — the distinction is already made.** `None` has to stay a valid
state whatever else is decided here: it is what a genuinely first window has,
and it is what a window the spool refused hands on. What must not be collapsed
into it is a ledger that exists and cannot be read. `Ledger::open` already keeps
the two apart at the file's own front door: a path that is not there opens an
empty ledger with no trailer, and a path that exists and cannot be read is
`LedgerError::Io`, which the runner turns into a refusal of the whole feed
before a socket is bound. A ledger that cannot be read is not one that is not
there — starting on the second is resuming from nothing, and starting on the
first is deriving beside rows that may already be in the store with nothing
recording it. Between the two sits the torn last line a crash mid-append leaves,
which is skipped: the trailer then falls back to the highest surviving entry,
which is a trailer that precedes less rather than a trailer that precedes wrong.
Every one of those degradations leans toward *uncertain*. No new refusal is
owed, and this answer holds whether or not anything reads the trailer back.

**Which trailer the ledger holds is not the one the read would want.**
`Ledger::trailer()` is the trailer of the highest `segment_seq` the file knows,
which is not the same statement as *the last window whose rows landed*. Within
one run the two coincide, because the numbers only go up. Across a restart they
come apart, and in the direction that matters: `window_seq` restarts at zero on
every run, so a previous run's numbering outranks every entry the new run
writes, and the ledger goes on answering with a window from the run before for
as long as the new run takes to climb past it. The persisted trailer is
therefore not exactly the wanted one, and it is not merely stale for the first
window. The same shape reaches the archive loader, whose objects carry a
`segment_seq` that restarts on every *recorder* run; that is the loader's to
decide and is recorded here because it was found here.

**An anchor that turns out wrong is worse than an uncertain one, and wrong is
what this read would produce.** An uncertain anchor withholds a window's
evidence; an anchor read from a previous run's trailer asserts a continuation
nobody observed. The check that makes the second unreachable already exists, and
it is `SegmentTrailer::precedes`: the predecessor test is `segment_seq + 1`, and
a run that begins at zero has no predecessor a previous run could supply. So
wiring the read on its own changes no answer anywhere — the trailer arrives,
`boundary` filters it out, and the anchor is uncertain exactly as before.

The only wiring that changes an answer is one that also continues the window
sequence across the restart, and that is the claim the sequence exists to
refuse. A hole in `segment_seq` is how a reader is told the derivation was down.
A restart is a run boundary; the capture stopped over it, and the datagrams that
passed in the interval were never offered to anything. A contiguous number
across that interval says *the derivation was not down* over precisely the
stretch in which it was.

The reader that pays for it is `segment_overflow` in
`007_recorder_cross_site.sql`. It takes the nearest earlier segment by
`start_ts`, checks `p.segment_seq + 1 = c.segment_seq`, and clamps a counter
that went backwards to zero — the clamp being right for the reboot it was
written for, and reachable only because no two adjacent segments today come from
two runs. The capture-drop counter belongs to the capture handle, and a new run
has a new handle, so the first window of a continued sequence would report
`capture_drop_delta = 0`: `overflow_free = 1`, a clean statement about this
host's capture over a window that may have admitted drops. A clean statement
there is one of the two things that make an absence usable against a publisher.

**What certainty would have bought, measured before it was paid for.** Less than
it looks. The first window of a run cannot be an absence witness for another
site's gap whatever its anchor says, because `absence_admissible` also requires
that vantage's `overflow_free = 1`, and a run's first segment has no predecessor
to subtract from. The whole gain is that the window's *own* gaps become
promotable past `unverifiable`, `sequence_gap_cross_site` testing
`g.anchor_certain = 1`. One window's own gaps, once per restart, bought with a
statement about that same window's capture health that the recorder is not in a
position to make.

**Decided: the trailer is not read back.** A run starts at `window_seq: 0` and
`preceding: None`, both of them, and the pair is the decision. The refusal on a
missing `inline.ledger` no longer promises an era anchor that survives a
restart, because nothing does: the ledger is required because a restart without
one re-posts every window the spool still holds, which is a replace paid for
rows already in the store. What the trailer in an inline ledger is *for*, then,
is that the entry is the loader's own entry — one type for both arrangements —
and it is what a window spooled by one run and posted by the next writes.

Two alternatives were weighed and both are worse. **Resuming the sequence with a
deliberate hole** — `trailer.segment_seq + 2`, so the run boundary is a hole
rather than a return to zero — leaves every answer where it already is, because
the adjacency test fails either way, and buys a durable sequence whose loss with
the ledger would be silent. **Continuing the sequence and the capture-drop total
together**, so that the delta over the boundary is the new run's own drops,
repairs the false zero but at the price of a column that means the kernel's
counter in one arrangement and a synthetic cross-run sum in the other, read by
views that cannot tell which wrote the row — and it still erases the hole that
says the derivation was down.

**What would change it.** One thing would make certainty across a restart honest
rather than assumed: a per-instance last sequence value in the trailer, and a
boundary that settles an era from the sequence itself rather than from segment
adjacency. The first window of a run could then say *this instance resumed at
the value after the one the last landed window ended on*, which is evidence, and
the capture-drop delta would stay unknown, which is correct. It changes
`SegmentTrailer`, which both arrangements write and the ledger serialises; it
changes what `anchor_certain` means for the archive too, where `precedes`
guards an eviction hole rather than a run boundary. That is its own design and
not a line in this one.

One case gives the anchor away inside a run, and gives it away deliberately. **A
window the spool could not take does not hand its trailer to the next window.**
Its rows are not in the store, so the next window's predecessor is *unknown* —
which is what `None` means there, and never *there was none*. Carrying the
trailer across would let a reader join the two eras as one continuous sequence
space over a hole nothing in the rows can explain, which is the merge an
uncertain anchor exists to prevent.

---

## The spool: rows reach disk before they reach the column store

Rows are written to disk on the way to the destination, on every window, not
only when the destination is unreachable. Four reasons, and the fourth is the
one that decides it.

**A recovery path that only runs during an incident is a recovery path nobody
has tested.** If disk were the exception, that code would first be exercised on
the day it is most needed.

**A crash otherwise loses what no archive can return.** A row sink coalesces
across windows deliberately, to keep merge pressure a function of rows per part
— which means it holds rows in memory for as long as its age bound allows. In
archive mode a crash costs nothing: the objects are on disk and the next pass
re-derives them. Inline, that memory is the only copy, and an out-of-memory
kill, an uncaught panic or a host reboot takes it with nothing recording that it
did. With rows on disk first, a crash costs the open window.

**The recording process's memory stops depending on the destination's health.**
Held rows are bounded by the sink's own coalescing rather than by how long a
column store has been down.

**It restores the ledger, and with it idempotence.** `Accepted` and `Landed` are
already distinct in the row-sink trait precisely because a sink that has taken
rows has not necessarily sent them. With windows on disk there is something to
retry and something to record: a window whose insert was never acknowledged is
replayed on the next pass, `ReplacingMergeTree` makes the replay a replace, and
the ledger entry is written when the rows land and never when they are accepted.
That is the loader's arrangement, over rows instead of objects, and it is reused
rather than restated.

The cost is small and worth naming as small: rows are about two orders of
magnitude smaller than the datagrams they describe, and archive mode already
writes every datagram to disk on every host. What inline mode gives up is not
I/O budget but the claim to need no disk at all — it needs a writable directory
with a budget, just a much smaller one.

**A window whose rows are on disk is not yet loaded.** The spool's oldest
unposted window is the lag that matters, and it is alerted on by **age**, never
by the eviction counter: a full budget evicts on every pass at steady state by
design, so the counter rises whether or not anything is wrong, while one window
older than the eviction window is history already gone. That is the same mistake
the loader's own documentation warns about, and the same answer.

### Eviction order, and the one window that goes last

Eviction is oldest-first and a window the sink is holding is evicted like any
other: it is the oldest, which is the rule, and the cost is one ledger entry
that will not be written for rows that may yet land — which leaves the next
window's era anchor uncertain and says so, rather than claiming a continuity
nothing on this host can still evidence.

**One window is the exception, and it is the one whose rows have already
landed.** A window whose insert succeeded and whose ledger entry could not be
written stays on disk to hold that entry: its rows are in the store, and its
directory is the only thing left that can record that they are. Evicting it is
not giving up rows that may yet land — the rows *have* landed — it is giving up
the only remaining evidence of a load that happened, and with it the trailer the
next era anchor is checked against. So a window owing a ledger entry is the last
thing the budget takes, and it is taken only when nothing else is left to take,
because a budget that stopped bounding the disk in order to protect an entry
would trade a bounded backlog for an unbounded one.

**A budget that cannot delete stops bounding at one window, not at every
window.** An eviction whose directory will not delete stops that pass, for the
reason it always did: retrying the same undeletable directory would walk the
whole spool into the same failure and leave the disk no emptier. What it must not
do is leave that window counted, because then the budget is over its bound for
ever, the same directory is chosen on every later pass, and the disk stops being
bounded from the first failure on. So the window stops being a window and its
bytes move to the unreclaimable count, which is where bytes this module can no
longer reach belong — and the next pass makes one more attempt, on the next
window.

### Every byte the spool put on disk is a byte its budget can see

A window is written in one place and it either becomes a window in the map or
leaves nothing behind. A store that failed after creating the directory used to
leave that directory on disk and out of the map, so its bytes sat outside the
byte count, outside eviction and outside the unreclaimable count alike: the
spool reported itself empty while orphans accumulated, one per failed window,
until a restart adopted or discarded them. The failure is silent in the one
number an operator would look at, which is why it belongs in this document
rather than only in a fix: **the spool's byte count is a claim about the disk,
and a claim with an exception is not one.** A store that fails removes what it
created, and a removal that itself fails moves those bytes to the unreclaimable
count instead of forgetting them.

---

## Why no market data rows, and why that is refused rather than left empty

Eight grains carry the `derivation` column and inline mode derives **five** of
them: `datagram`, `era`, `segment_coverage`, `sequence_gap` and
`conformance_finding`. The other three — `event`, `instrument` and `book_top` —
are the market data grains, and inline mode derives none of them.

**The gap as a reviewer found it, stated exactly.** In archive mode those three
tables are also empty unless somebody asked: derivation is per feed and off
everywhere, selected by a `[[market_data]]` entry in the loader's own
configuration, and a host with no entry loads what it always loaded. So the
emptiness is not what inline mode introduces. What inline mode introduced is
that **there was no way to ask and nothing said so** — a feed pointed at this
arrangement had three permanently empty tables, indistinguishable from a feed
nobody published on, and no key an operator could have written to find out
otherwise. The equivalence gate cannot see it, because its synthetic feed has no
`[[market_data]]` entry on either side and both paths therefore agree on zero.

### Why they are not derived

Two reasons, and the first is a decision this design has already taken.

**Nothing in the record path decodes a datagram.** That is one of this
document's own decisions, and it is not about cost. Derivation reads the 24-byte
datagram header through a peek that judges nothing but the buffer's length, and
no message is decoded — so a message a decoder would reject still carries the
sequence number whose absence is the finding, which is the whole reason the
transport grains are trustworthy at all. Market data derivation is a codec walk.
In archive mode it runs in the loader, off the host, over bytes already stored
and already hashed, and a decoder that refuses or panics costs a loader pass. On
the derivation stage of a recording process it costs **the window** — and with
it that window's transport rows, which are the rows this whole tier exists for.
Putting a codec there trades the grains that work for the grains that might.

**An instrument table and a book are state that spans the unit the spool
bounds.** `derive_events` builds its instrument table per call, and a definition
is in force from the instant it was received until the next statement — so
resolving a price message needs the definitions seen before it. An object is a
rotation interval; a window is a window bound, orders of magnitude smaller.
Derived per window, nearly every message after the first window would be refused
for an unresolved instrument. Making them resolve means carrying the instrument
table, the book and every open snapshot cycle across windows *and* across a
restart — which is durable state spanning the unit the spool exists to bound, so
a crash would stop costing one window. That is a design, with its own
persistence and its own failure modes, and this branch does not have it.

### Why the ask exists in order to be refused

The alternative to deriving them was **announcing the gap** — a `--check` line,
and nothing else. That is not enough on its own, and this repository's pattern
is to refuse rather than to be quietly empty. So both:

- **Inline mode's own file accepts a `[[market_data]]` section**, the loader's
  `MarketDataFeed` type reused verbatim the way the destination's configuration
  type is, and **refuses any entry at startup** — naming the feed, naming the
  three tables that would stay empty, and naming archive mode as the arrangement
  that derives them. An operator who asks is answered at `--check`, before a
  socket is bound, instead of being handed three empty tables.
- **`--check` and the startup summary state which feeds derive market data
  rows**, in both arrangements, beside the mode line they already lead with. In
  inline mode that line says none, and says which arrangement can.

**Why the key is accepted-and-refused rather than required.** A required key
whose one legal value is *none* was considered, and rejected: it would make
inline mode stricter than archive mode about a decision the two arrangements
make identically. A feed with no entry derives no market data rows in *either*
arrangement, which is the equivalence the gate on this design asserts everywhere
else, and it is defended in the loader's own configuration for the reason a
global switch was refused there. If the un-asked case should be loud, that is a
change to the loader as much as to this mode, and it belongs in one place rather
than in the arrangement that happened to be reviewed. What inline mode owes is
that the *ask* is possible and answered, and that is what this gives.

---

## Provenance: the `derivation` column

One column on all eight grains, `archive` or `live`, defaulting to `archive` so
that rows already written keep their meaning.

It is a column rather than an inference for the reason the recorder synthesises
nothing silently. The alternative considered was to let the empty digest carry
it: inline rows have no `object_sha256`, so a reader could test for that. It is
rejected because it is a trap — a future query that treats the empty string as
*verified* is wrong in the direction that matters, and nothing about the column
name warns anybody. A reader should not have to know that one string means *no
bytes were ever hashed*.

It is deliberately in no `ORDER BY`. Deduplication must not change: a row is
the same row whichever mode produced it, and putting provenance in the sort key
would make two modes' views of the same datagram two rows instead of one.

---

## Configuration: two files, and why not one

Inline mode reads a second configuration file. `RecorderConfig` gains no key at
all — not the destination, not a credential, not the window bound.

**The record path's configuration hash is provenance, and a database endpoint is
not part of what a recorder does.** `config_hash` is written into every pcapng
object and into every coverage row so that a finding stays attributable to the
configuration that produced it. It is taken over the *parsed* configuration
precisely so that a reformatting or a reordered key does not change it. Put a
column-store endpoint in that file and rotating a password changes the
provenance of an archive, though nothing about what the recorder captured,
joined or wrote has changed. Worse, adding any key to that struct changes the
hash of every existing configuration in the fleet.

**The invariant stays literally true.** `RecorderConfig` documents the absence of
an endpoint, a credential and a database key. Inline mode does not weaken that
sentence; it puts those things in a different file, as the loader already does,
with the credential coming from where the loader's already comes from and not
from a second mechanism invented here.

**One identity, so the two halves cannot disagree.** `site` and `recorder` are
*not* in the second file. They come from the recorder's own configuration, which
is the one thing today's two-process arrangement cannot guarantee: two files can
name the same host differently, and then the live panel and the historical panel
of one dashboard describe two recorders that do not exist.

The second file carries the window bound, the ring capacity, the spool
directory and its budget, the ledger path, and the destination — reusing the
column-store configuration type verbatim.

### The configuration states the arrangement, and no flag names it

The arrangement cannot be a key in the recorder's own file for the reason
nothing else is: `config_hash` is provenance, and adding any key changes the
hash of every configuration in the fleet. It could have been a key in inline
mode's own file, except that this file's whole reason to exist is what inline
mode needs *from* it — a spool and a destination — so a mode key inside it would
be an arrangement chosen by a file only one of the two arrangements reads.

**A mode token is therefore homeless, and it turns out not to be needed.** Each
arrangement is already named, in the only place it could be, by the resource it
cannot run without: archive mode by `archive.staging_dir` and
`archive.completed_dir`, inline mode by the file `--inline-config` names. Both
sets are required with no default, the sets are disjoint, and the four
combinations are total — so the selection is a read of what an operator wrote
rather than a second statement of it.
*[Why the arrangement is stated and never defaulted](#why-the-arrangement-is-stated-and-never-defaulted)*
argues the reading and answers the `derivation`-column analogy that an earlier
draft rejected it by;
*[Whether a flag survives as an explicit override](#whether-a-flag-survives-as-an-explicit-override)*
is why no flag comes back beside it.

**Nothing here mints vocabulary either.** The keys are the ones archive mode has
always required, and `--inline-config` names the file inline mode has to read.
`GLOSSARY.md` bans no word used above, and no token that names an arrangement
has been added to a key, a field or a metric label — which is the cost the
publisher's feed-routes design argued `route` out of existence to avoid paying.
The arrangement is a reading, and a reading spends no vocabulary at all.

### What it refuses at startup

The recorder refuses rather than invents, and this design adds six refusals.
Each names the key or the file.

- **A configuration stating both arrangements.** `archive.staging_dir` or
  `archive.completed_dir` carrying a value *and* a second file given. Nothing
  writes objects in inline mode, so a directory with a value is an operator
  expecting an archive they will not get — and ignoring it silently is how a
  host is believed to be keeping bytes for a year that it never kept for a
  second. Refused naming the key and the file, so the two statements that
  disagree are both on the screen.
- **A configuration stating neither.** No archive directory and no second file.
  Archive mode needs the two directories, inline mode needs a spool directory, a
  ledger and a destination, and not one of the five has a defensible value to
  invent — a recorder that invented them would keep bytes on a disk nobody sized
  or load rows into a database nobody chose. Refused naming both ways of stating
  an arrangement, because the operator reading it has stated neither.
- **A spool directory that does not exist or cannot be written.** The spool is
  the durability of this mode; a recorder that could not write it would be
  holding every row in memory and calling itself healthy.
- **A ledger inside the spool directory.** For the reason the loader's ledger
  may not live inside the objects directory: a file the budget cannot classify
  is a file eviction cannot reach.
- **A feed whose market data rows were asked for.** Inline mode derives the
  transport grains and not the market data ones, and
  *[Why no market data rows, and why that is refused rather than left empty](#why-no-market-data-rows-and-why-that-is-refused-rather-than-left-empty)*
  argues it. The ask exists so that it can be refused by name, naming the feed
  and the three tables that would otherwise be empty, rather than being a
  question an operator has no way to put.
- **Inline mode from a build that does not carry it.** The mode is behind a build
  feature. A `--no-default-features` build can only be in archive mode, so a
  configuration that selects inline mode — a second file given, no archive
  directories — fails at startup naming the feature, rather than falling back to
  archive mode without being asked, which would leave a host keeping bytes where
  rows were asked for.

---

## Metrics

Inline mode publishes its own family alongside the health tier's, on one port,
in one exposition. The families are disjoint, and the health tier's series are
unchanged and mean the same things they mean in archive mode.

What the family has to cover is the three places this mode can be wrong that
archive mode cannot:

| | |
|---|---|
| ring drops | datagrams the derivation never saw, and therefore the loss this mode admits into its own rows |
| spool age | how old the oldest unposted window is — the number to alert on |
| spool evictions and bytes | history given up under the budget, and how close the budget is |
| windows derived, rows written per grain | that derivation is keeping up, per grain because the grains are orders of magnitude apart in volume |
| stage restarts | a derivation or posting stage that panicked and was restarted, which must never be silent |

---

## Testing

Everything below runs with no socket, no privileges and no server, which is what
makes it a gate rather than a habit.

**The equivalence test is the gate on the whole design.** One synthetic feed,
fed through both paths — captured to an archive and derived with
`derive_object`, and derived inline — and the row sets must be equal but for
`derivation`, `object_key` and `object_sha256`. Those three are the whole of
what a row says about where it came from; there is no fourth, because
`byte_count` is a manifest field and no grain carries it. If they are equal,
inline mode is the same analysis with a different provenance. If they are not,
the difference is a bug and the failing grain says where.

**And the gate has to derive the way the derivation stage derives.** The gate
builds its own window rather than starting a pipeline, which is what lets it run
with no spool and no destination — and it is therefore also free to build a
window the pipeline never has. It must call the same two-pass derivation from
the same place, or it is asserting an equivalence between archive mode and a
shape nothing runs, and the shape that does run is unasserted. A gate whose own
fixture supplies the correctness under test is the one failure this gate cannot
report, because it looks exactly like a pass.

**Two runs of one recorder are two windows, asserted at the row.** `window_seq`
restarts at zero on every run, so the row that proves the manifest was built
from a walked window is a `segment_coverage` row: it exists at all, it is
stamped with the window's own first and last receive timestamps, and the window
key under it differs between two runs whose datagrams differ. All three fail
together against a manifest built before the walk, and the first of them is what
a reader would never think to check — a table with no rows in it looks like a
feed nobody joined.

**An empty window leaves no hole.** A quiet stretch is derived as windows that
saw nothing, and the sequence numbers either side of it are consecutive. The
window after the silence carries a certain era anchor, which is the same
assertion from the other end.

**The capture loss total is cumulative, and a test says so at the second
window.** A per-window figure passes every assertion a single window can make,
so the gate is two windows on one ring: the second reports the first's drops as
well as its own. Nothing else in this repository can catch that — the view which
reads the column lives in SQL, and the equivalence gate's synthetic feed has no
kernel drops, so both paths report zero and agree.

**The fault list is all nine, `SilentChannel` included.** A channel that stops
publishing is an ordinary overnight occurrence rather than an injected
condition, and it is the fault whose *production* behaviour differs most between
the modes: archive mode finds it when a segment rotates on its interval, inline
mode when a window closes on age. What the gate can assert is the derivation
half — one quiet channel among several, the same rows both ways — and it is
worth being explicit that the timing half is not asserted by it, because a
window and a segment holding the same datagrams is the fixture's whole premise.

**The debt test is the one whose mutant must die.** Force the ring to drop, and
assert the next accepted datagram declares the loss — and, at the row altitude,
that no sequence gap caused by the recorder is given a `publisher` verdict.
Revert the charge and the test must fail.

**And the ring distinguishes a deriver that is behind from one that is gone.** A
full ring is a drop and a counter, and it is the ordinary case. A derivation
that is not there is not: it is a run of drops with no end and no window ever
derived, which reads on every counter exactly like a ring that is merely
overrun. So the outcome that says so is a tested outcome and not a branch left
for a reader to reason about, and the datagram is charged either way — a drop
nobody can carry the admission for is still a drop.

**The spool tests are about the failures, not the happy path.** A destination
that is down leaves windows on disk; one that recovers lands them oldest first
and records them; a full budget evicts the oldest and counts it; a process
killed mid-run replays its spool on the next start and the window lands; a
window whose own digest does not match is discarded by name rather than loaded
in part.

**The window tests cover the seam derivation cannot see.** A window closes on
its bound, the next one opens with the previous trailer, and the era anchor is
certain from the second window of a run. Across a restart it is not, and a test
says so by name rather than leaving the boundary untested: the ledger holds a
trailer, the run begins at zero, and the first window of the new run anchors on
nothing.

**The selection is held by a test per case, and there are four cases.** A
selection is the kind of decision that leaves no trace when it is wrong, so each
row of *[Four cases, and two of them run](#four-cases-and-two-of-them-run)* is a
test rather than a paragraph: an archive-shaped configuration with no second
file runs archive mode and says so on the first line of `--check`; an
inline-shaped one with a second file runs inline mode and says so; a
configuration stating both is refused naming `archive.staging_dir` and the file;
a configuration stating neither is refused naming both ways of stating an
arrangement. **A fifth test asserts the four are total** — that the two
predicates the selection reads are exactly the two the refusals name — because
four cases enumerated by hand are four cases somebody can add a fifth to. And
the build feature is asserted to be in the default feature set, because the
released asset is one asset for a fleet whose arrangements differ per host.
Reverting any one of them fails a named test, and the plan records which.

**A word that names an arrangement is asserted not to exist.** `--archive` was
on this branch and is gone, so a command line carrying it is a usage error. That
is asserted rather than left to the absence of code: a flag is cheap to add back
and the argument against it is four paragraphs long, so the test is what carries
the decision to whoever reaches for it next.

---

## Decisions

**Neither arrangement is a default; each is stated by the configuration only it
can run on.** Archive mode is selected by the two directories it has always
required, inline mode by the file carrying the spool, ledger and destination it
cannot run without. Both stated is refused; neither stated is refused. The four
cases are total, so nothing is left for a default to decide and no host can drift
into an arrangement nobody chose. A host recording a production feed for evidence
should still run archive mode, and states it by writing the directories that
arrangement fills; inline mode is for bring-up, for hosts that were never keeping
the bytes, and for one deploy unit.

**No flag names the arrangement, and `--archive` does not exist.** A flag could
only agree with the configuration or be refused by it, and the one power it would
add is the power to resolve one of the two refusals — which is starting a
recorder in an arrangement its own configuration contradicts. Selecting from the
configuration also costs no edit in any infrastructure repository, because every
archive host's configuration already carries the keys that select it.

**Inline mode never writes an archive beside its rows.** An arrangement that
wrote both would be the only one carrying two disk budgets, the one no test
covers, and — decisively — the reading that makes *both stated* run instead of
refuse, which is what the totality of the selection rests on.

**Inline mode derives the five transport grains and none of the three market
data ones.** Not because the emptiness is acceptable — it is the same emptiness
archive mode has for an unasked feed — but because deriving them would put a
codec on the record path, against this design's own decision that nothing there
decodes a datagram, and would need an instrument table and a book carried across
windows and across a restart. So the ask exists in inline mode's own file in
order to be **refused by name**, and the summary states in both arrangements
which feeds derive them.

**Derivation is not reimplemented.** Inline mode supplies a third `Source` and a
synthesised manifest, and calls the same `derive`. A second derivation would be
a second definition of what a row means, and the two would drift in exactly the
cases that matter.

**Rows reach disk before the column store, always.** Not only under outage. The
recovery path is the ordinary path, a crash costs one window, and the ledger and
its idempotence come back with it.

**Provenance is a column, not an inference.** An empty digest is not allowed to
be the only thing distinguishing a row derived from verified bytes from a row
derived in flight.

**The record path's configuration gains no key.** Two files, so that the archive's
provenance hash stays a function of what the recorder does, and so that the
documented absence of a credential in it stays true.

**The capture path never blocks, in either new place.** The ring charges its
drops as the recorder's own loss; the spool evicts under its budget. Both are
the archive's existing rules applied to a new unit.

**Nothing in the record path decodes a datagram.** Unchanged, and worth
restating because inline mode brings derivation into the recording process:
derivation reads the 24-byte datagram header through a peek that judges nothing
but the buffer's length, and no message is decoded. A message a decoder would
reject still carries the sequence number whose absence is the finding.

---

## Non-goals

**No shipping of rows off the host.** The spool is durability against a crash and
an outage, not an archive. Its budget is a bounded backlog and its oldest
window is evicted like any other.

**No datagram archive in inline mode, optional or otherwise.** A third
arrangement would carry its own sizing, its own failure modes and its own tests,
and it would have to accept the very keys whose refusal makes the selection
total;
*[Whether inline mode could write an archive beside its rows](#whether-inline-mode-could-write-an-archive-beside-its-rows)*
decides it at length. A host that wants bytes runs archive mode.

**No change to what archive mode does, and none to how it is entered.** Not to
its configuration, not to its objects, not to its manifest, not to its metrics,
not to the loader, and not to a single `ExecStart` or `ExecStartPre` in any
infrastructure repository. An archive-mode host selects archive mode by carrying
`archive.staging_dir` and `archive.completed_dir`, which archive mode has
required since it existed — so the configuration that ran yesterday runs
unchanged, and *[What it costs](#what-it-costs)* is a cost table with nothing
external in it.

**No market data rows in inline mode.** Deriving them would put a codec on the
record path and need reference-data state carried across windows and across a
restart;
*[Why no market data rows, and why that is refused rather than left empty](#why-no-market-data-rows-and-why-that-is-refused-rather-than-left-empty)*
decides it. A feed whose `event`, `instrument` and `book_top` rows are wanted
runs archive mode, and asking for them in inline mode is a refusal at `--check`
rather than three empty tables.

**No change to the deduplication keys.** The provenance column is in no sort key,
and no grain's identity changes.

**No conformance rule set here.** Inline mode derives the same grains archive
mode derives, and the conformance tier is where rules live in both.
