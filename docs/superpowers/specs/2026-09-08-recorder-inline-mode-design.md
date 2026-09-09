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

This defines a **second arrangement**, and makes it the one a configuration
that says nothing is read as: one process that captures a feed and derives its
rows directly, keeping no datagrams. It exists for three cases, and
*[Why inline mode is the default](#why-inline-mode-is-the-default)* is why those
are the cases to point a default at.

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
| **archive mode** | `dz-recorder` writes objects; `dz-recorder-load` derives rows from them. Unchanged in everything but how it is asked for. | `--archive` | the recorder crates design |
| **inline mode** | one process captures, derives and loads. Keeps no datagrams. | nothing: it is what a command line naming no mode is read as | here |

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

**The two configurations refuse each other, so the default cannot be silently
wrong.** Inline mode is what a command line naming no mode is read as, and
archive mode is asked for by `--archive`. That is a default placed over a
decision that already had an answer, and what makes it safe is not the flag: it
is that each mode refuses the keys the other requires. Archive mode requires
`archive.staging_dir` and `archive.completed_dir`; inline mode refuses either of
them carrying a value. So an archive-mode host whose command line names no mode
is **refused at startup, by key**, and told to pass `--archive` — it is never
read as a recorder that quietly stopped keeping bytes. A configuration stating
neither shape is refused too, naming both flags. There is no third case, and
that is the whole of what makes the inversion defensible.

Archive mode is also still what a host recording a production feed for evidence
should run, and it now says so on its own command line rather than by silence.
*[Why inline mode is the default](#why-inline-mode-is-the-default)* argues the
reading; *[What it costs](#what-it-costs)* names what it takes.

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

## Why inline mode is the default

The recorder crates design's *"the archive is bytes, not rows"* is still the
stronger argument for a host recording a production feed for evidence, and
nothing here softens it. What a default decides is not which argument is
stronger. It decides how a configuration that says nothing is **read**, and that
is a different question with a different answer.

**A default belongs on the arrangement whose wrong choice is loud.** Put the
default on archive mode and a host that meant inline mode and said nothing gets
a running recorder: it joins the feed, writes objects, publishes them and
reports itself healthy. Nothing derives them, because the second process was
never deployed. The symptom is an empty table, and an empty table is
indistinguishable from a feed nobody published on — which is the one diagnosis
this whole tier exists to make. Put the default on inline mode and a host that
meant archive mode and said nothing gets a **startup refusal naming
`archive.staging_dir`**, because it carries that key and inline mode refuses it.
One reading fails as silence in a dashboard; the other fails as a non-zero exit
code in front of the pipeline that caused it. The default belongs on the second.

**Bring-up is the first thing every feed does, and it is inline mode's own
case.** *[Purpose](#purpose)* gives three cases for the mode and the first is
bringing a feed up: the question is *are the rows right*, and the loop that
answers it in archive mode is a rotation, a pass and a query. Every feed goes
through that loop before it is a production feed at all. A default that is wrong
for the first thing every feed does is a default every feed overrides once, and
a default nobody keeps has only ever cost a line of configuration.

**An archive-mode default makes a disk sizing the price of a first row.** The
staging budget is retention × bytes per second and it is the number that decides
host sizing — the recorder crates design says so plainly. Under an archive-mode
default that number stands between every new host and its first queryable row,
including the hosts that were never going to keep the bytes. Under an
inline-mode default it stands only in front of the hosts that asked to keep
them, which are the hosts the number is about.

**A row is the product; a byte is the evidence for it.** Archive mode's output
is not rows: it is objects, plus a second process that has to be deployed
somewhere else before anything can be asked a question. A recorder in archive
mode with no loader behind it has produced nothing queryable at all. Inline
mode's output is rows in the column store from one process, which is the shape
of a thing that is finished when it starts.

None of that changes which mode a host recording for evidence should run. It
changes only what silence means, and silence now means the reading that gets
caught.

---

## What it costs

Three costs. The first is the one to read, and it is the one the inversion turns
on.

### An evidence host states one word, and is refused if it does not

A host recording a production feed for evidence needs `--archive` on its command
line. What it loses if nobody sets it is **nothing quietly**: it does not start.

| Configuration shape | Mode named | What happens |
|---|---|---|
| `[archive]` directories set | `--archive` | archive mode, exactly as before |
| `[archive]` directories set | nothing | **refused**, naming `archive.staging_dir` and `--archive` |
| no `[archive]`, an inline file given | nothing | inline mode |
| no `[archive]`, no inline file | nothing | **refused**, naming `--inline-config` and `--archive` |
| both flags | both | **refused** on the command line: two arrangements that keep different things |

There is no row in which a host that wanted an archive gets a running recorder
without one, and that is a property of the refusals rather than of the default:
**archive mode requires two directories that inline mode refuses a value for.**

A log line saying `mode=inline` would not have been enough. The restart that
changes a host's mode is the moment nobody is reading its log, and the finding
would arrive weeks later as a year of retention that was never kept. So the
loudness is a startup refusal with a non-zero exit code, made before a socket is
bound; and `--check` makes it before anything is restarted at all, which is why
that subcommand is an `ExecStartPre` rather than a convenience.

**What the inversion therefore depends on is that both refusals stay refusals.**
If archive mode ever gains a defaulted `staging_dir`, or inline mode ever
downgrades its archive-directory refusal to a warning, this default becomes
silent in the one direction that loses evidence. Both are held by tests rather
than by this paragraph.

### Every archive-mode command line stops working, once, at its next restart

This is a breaking change to a default that has an answer today, and it is worth
classing the way the publisher's feed-routes design classes its own costs:

| Cost | Class |
|---|---|
| Every unit, pipeline and runbook that starts `dz-recorder` for archive mode needs `--archive` added — on `ExecStart` and on the `ExecStartPre` that runs `--check`. Those live in infrastructure repositories this one does not contain. | External, and not ours to land |
| The failure surfaces on a restart, which is when something else was already being changed, so it arrives attributed to whatever else was in flight. | Restart-triggered |
| No archive is lost and nothing is corrupted. The refusal is made before a socket is bound, so a host that refuses has not recorded the wrong thing — it has recorded nothing. | Recoverable |
| The recovery is one word on one line, and `--check` proves it without restarting anything. | Cheap, and discoverable ahead of time |

Nothing in that table is irreversible, which is what separates this cost from
the class the feed-routes design had to reject an option over: an identity space
spent cannot be spent back, and a command line can. The honest summary is a
fleet-wide edit a pipeline can make and a `--check` can prove, in exchange for a
default that fails loudly in the direction that matters.

### The mode becomes a default build feature

Inline mode is behind a build feature so that a recorder that only records
carries no column-store client, no HTTP client and no row crates. That argument
is untouched and the feature stays. What cannot stand alongside the inversion is
the feature being **off** by default while the mode it gates is the default
mode: a binary that refuses the arrangement its own command line asks for when
told nothing is a binary whose default it cannot honour. So `inline` joins the
default feature set, and the record-only build becomes
`--no-default-features`.

What that costs is real and small: every default build of the recorder now
compiles and links the column-store client, an HTTP client and the row crates.
The property those crates were kept out for — **nothing in the record path
reaches the destination** — was never enforced by the feature and is not
weakened here: it is enforced by the capture path's own rule that it never
blocks and never parses, and by the derivation and posting stages living off
that path entirely. The feature buys a smaller build, not a safer one, and a
host that wants the smaller build asks for it by name.

A build made with `--no-default-features` can only ever be in archive mode, so
a command line naming no mode is refused there by the feature's name and by
`--archive`, rather than by the second file it would otherwise have needed.

One consequence is worth stating because it settles a deployment question this
design would otherwise leave open: the released recorder asset is built with the
default feature set plus `afpacket`, and features are additive, so the asset
carries inline mode without the release pipeline naming it. A released binary
that could not run its own default mode would be the alternative.

---

## Whether the default is inline only, or inline and an archive together

Decided: **inline only.** The default keeps no datagrams, and the code
implements exactly that — one `Arrangement` per run, the two mutually exclusive
by construction, and no configuration or command line able to ask for both.

The alternative has to be answered rather than waved at, because the inversion
is what gives it force. If the default derived rows inline *and* wrote the
archive, a host that said nothing would keep its bytes and get its rows, no
evidence would be lost to a silent default, and everything in
*[What it costs](#what-it-costs)* would be unnecessary. Four reasons reject it.

**A default has to be the arrangement that is cheapest to be wrong about, and
that one is the most expensive.** A host that stated nothing would be sized for
retention × bytes per second — the exact cost the mode exists to avoid — and
would need a spool, a budget and a destination on top. The default would be the
only arrangement carrying two disk budgets and two sizing questions, and the
case that motivated the mode at all would be served by neither the default nor
the explicit ask.

**It is the one arrangement no test covers.** The gate this design rests on
compares two paths and asserts their rows are equal but for provenance. A third
path that runs both at once has its own interleaving, its own backpressure and
its own shutdown ordering, and an equivalence test between the other two
exercises none of them. A default nothing tests is worse than a non-default that
is tested.

**It would delete the refusal that makes the inversion safe.** An arrangement
writing both has to *accept* `archive.staging_dir` in the mode that also derives
inline. That refusal is what makes an archive-mode host's silent command line
loud, and it is the whole argument of the first cost above. A default that had to
accept those keys would be a default that could be silently wrong again, having
gone to this trouble to stop being.

**Writing both is not free at the capture.** Every datagram would go to the
archive writer *and* into the ring: a second consumer on the record path and a
second place backpressure can appear. The capture path never blocks, and holding
that against two sinks with unrelated failure characteristics — a full disk and
a slow destination — is a harder property than holding it against one.

A host that wants both runs archive mode and derives from the objects, which is
what archive mode is. What it gives up against a hypothetical both-mode is
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
loaded unit, carrying the trailer so that a restart resumes with the certainty a
continuous run had. Inline mode's spool needs exactly that and reuses it.

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

### The era anchor gets better, not worse

`Era::anchor_certain` is written uncertain when the preceding window's trailer
is unknown, and in archive mode that is routine: the staging budget evicts, and
the predecessor of the oldest surviving object is regularly gone. Inline
windows are strictly sequential, are never evicted before derivation, and carry
their trailer into the ledger — so the anchor is certain from the second window
onward, and stays certain across a restart. This is the one analytical result
inline mode improves.

**What an uncertain anchor costs, traced rather than assumed.** The trailer is
written to the ledger and is not yet read back, so today the first window after
every restart derives with no predecessor. Following that through: the
derivation's own verdict function cannot reach `publisher` at all — it is not
among its outcomes, by design, because `publisher` needs a datagram absent from
every site and one vantage has neither half of that — so the first window after
a restart answers `recorder` where its residue is fully admitted and
`unverifiable` otherwise. The cross-site pass that turns `unverifiable` into
`publisher` requires, per gap occurrence, that the vantage's era boundary be
settled: `anchor_certain = 1`. An uncertain anchor therefore makes that
window's absences **inadmissible**.

So the cost is a window's worth of evidence, once per restart, and never an
accusation drawn from ignorance. That is the right direction for the failure to
lean, and it is why reading the trailer back is an improvement to make rather
than a correctness hole to stop the mode for.

One case gives it back, and gives it back deliberately. **A window the spool
could not take does not hand its trailer to the next window.** Its rows are not
in the store, so the next window's predecessor is *unknown* — which is what
`None` means there, and never *there was none*. Carrying the trailer across
would let a reader join the two eras as one continuous sequence space over a
hole nothing in the rows can explain, which is the merge an uncertain anchor
exists to prevent.

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

### The mode is stated on the command line, and `--archive` is not a new word

The mode cannot be a key in the recorder's own file for the reason nothing else
is: `config_hash` is provenance, and adding any key changes the hash of every
configuration in the fleet. It could have been a key in inline mode's own file,
except that this file's whole reason to exist is what inline mode needs *from*
it — a spool and a destination — so a mode key inside it would be a mode chosen
by a file only one of the two modes reads.

So the mode is stated on the command line, as `--archive`, and the flag's
absence is inline mode.

**`--archive` mints no vocabulary.** `GLOSSARY.md` bans neither the word nor any
sense of it, and the token is already this repository's four times over: the
`[archive]` section of the recorder's configuration, the `archive` value of the
rows' `derivation` column, `dz-recorder-archive`'s crate name, and the name this
document has given the arrangement since its first paragraph. A genuinely new
mode token would have had to be argued against the glossary the way the
publisher's feed-routes design argued `route` out of existence before it became
a key, a field and a metric label. This is that argument's opposite case: the
word an operator has already read in three places, used for the fourth.

The rejected alternative is inferring the mode from whether the `[archive]`
directories carry a value, which needs no flag at all and reads *archive mode is
what an operator asks for explicitly* literally. It is rejected because this
design has already rejected an inference of exactly this shape one layer down:
the `derivation` column is a column rather than a test for an empty digest,
because a reader should not have to know that one value means nothing was ever
verified. Choosing the arrangement by whether a key has a value is that trap
above the rows instead of inside them, and its version of the failure is an
operator who deletes an `[archive]` section to stop archiving for an afternoon
and finds they have changed what a month of rows means.

### What it refuses at startup

The recorder refuses rather than invents, and inline mode adds five refusals.
Each names the key or the flag.

- **An archive directory configured in inline mode.** Nothing writes objects, so
  a `staging_dir` or `completed_dir` with a value is an operator expecting an
  archive they will not get. Ignoring it silently is how a host is believed to
  be keeping bytes for a year that it never kept for a second.
- **A spool directory that does not exist or cannot be written.** The spool is
  the durability of this mode; a recorder that could not write it would be
  holding every row in memory and calling itself healthy.
- **A ledger inside the spool directory.** For the reason the loader's ledger
  may not live inside the objects directory: a file the budget cannot classify
  is a file eviction cannot reach.
- **Inline mode with no file to run it from.** The mode is what a command line
  naming no mode is read as, and it needs a spool directory and a destination,
  neither of which has a defensible value to invent — a recorder that invented
  one would load rows into a database nobody chose. Refused naming
  `--inline-config`, and naming `--archive` as well, because the operator whose
  host wanted the other arrangement is the one most likely to be reading it.
- **The default mode asked for from a build that does not carry it.** The mode is
  behind a build feature, and that feature is in the default set precisely
  because the mode is the default mode. A `--no-default-features` build can only
  be in archive mode, so a command line naming no mode fails at startup naming
  the feature and naming `--archive` — rather than falling back to archive mode
  without being asked, which would leave a host keeping bytes where rows were
  asked for.

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
certain from the second window — including across a restart, through the ledger.

**The default is held by two refusals and a manifest assertion.** A default is
the kind of decision that leaves no trace when it is wrong, so each half of it
is a test rather than a paragraph: an archive-shaped configuration whose command
line names no mode is refused naming `archive.staging_dir` and `--archive`; a
configuration naming neither shape is refused naming `--inline-config` and
`--archive`; and the build feature the default mode needs is asserted to be in
the default feature set, because a mode entered by silence from a binary that
cannot run it is a default that refuses itself. Reverting any one of the three
fails a named test, and the plan records which.

---

## Decisions

**Inline mode is the default; archive mode is asked for by name.** A default
decides how a configuration that says nothing is read, and it is put here
because this reading fails loudly — an archive-mode host that names no mode is
refused by key — while the other fails as an empty table nobody can distinguish
from a silent feed. A host recording a production feed for evidence should still
run archive mode, and now says so with `--archive`; inline mode is for bring-up,
for hosts that were never keeping the bytes, and for one deploy unit. Neither
mode can be entered by accident, because each refuses the keys the other
requires.

**The default is inline only, and never inline with an archive beside it.** A
default that wrote both would be the only arrangement carrying two disk budgets,
the one arrangement no test covers, and the one that would have to accept the
archive keys whose refusal is what keeps this default from being silently
wrong.

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
and it would have to accept the very keys whose refusal makes the default safe;
*[Whether the default is inline only](#whether-the-default-is-inline-only-or-inline-and-an-archive-together)*
decides it at length. A host that wants bytes runs archive mode.

**No change to what archive mode does.** Not to its configuration, not to its
objects, not to its manifest, not to its metrics, and not to the loader. What
changes is the one word that selects it: a command line that used to enter
archive mode by saying nothing now says `--archive`, and is refused by key
rather than reinterpreted if it does not. That refusal, and the cost of the edit
it forces on every existing command line, is
*[What it costs](#what-it-costs)*.

**No change to the deduplication keys.** The provenance column is in no sort key,
and no grain's identity changes.

**No conformance rule set here.** Inline mode derives the same grains archive
mode derives, and the conformance tier is where rules live in both.
