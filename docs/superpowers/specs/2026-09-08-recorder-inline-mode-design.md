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

This defines a **second arrangement** for hosts the first one does not suit: one
process that captures a feed and derives its rows directly, keeping no
datagrams. It exists for three cases.

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

| | What it is | Where it is defined |
|---|---|---|
| **archive mode** | `dz-recorder` writes objects; `dz-recorder-load` derives rows from them. The default, and unchanged. | the recorder crates design |
| **inline mode** | one process captures, derives and loads. Keeps no datagrams. | here |

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

**It is a second arrangement, not a replacement.** Archive mode is the default,
is unchanged, and is what a host recording a production feed for evidence
should run. Inline mode is opt-in, and asking for it takes a flag the default
configuration does not carry.

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
| `segment_seq`, `start_ns`, `end_ns`, `datagram_count`, `payload_byte_count`, `instances`, `short_datagrams`, `instances_dropped`, `capture_drop_total`, `capture_drop_scope`, `interface_drop_total`, `roles_joined`, `link_headers`, `link_header_exceptions` | Observed. The window counts what the writer would have counted, from the same datagrams. |
| `object_key` | The window's key. It carries the window's start in wall-clock nanoseconds, so it is unique and orders windows across runs — but it names no object anyone can fetch. |
| `sha256`, `byte_count` | **Empty and zero.** No datagrams were kept, so nothing was hashed. An invented digest is worse than an absent one: it is a claim that something was verified. |

The rows' `derivation` column is what a reader is meant to consult, so that an
empty digest is corroboration rather than the only signal.

### The era anchor gets better, not worse

`Era::anchor_certain` is written uncertain when the preceding window's trailer
is unknown, and in archive mode that is routine: the staging budget evicts, and
the predecessor of the oldest surviving object is regularly gone. Inline
windows are strictly sequential, are never evicted before derivation, and carry
their trailer into the ledger — so the anchor is certain from the second window
onward, and stays certain across a restart. This is the one analytical result
inline mode improves.

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

### What it refuses at startup

The recorder refuses rather than invents, and inline mode adds four refusals.
Each names the key.

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
- **Inline mode asked for by a build that does not carry it.** The mode is behind
  a build feature, so that the default build of the recorder gains no
  column-store dependency, no HTTP client and no row crates. A configuration
  asking for what the binary cannot do fails at startup rather than falling back
  to archive mode, which would leave a host silently in the arrangement nobody
  chose.

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
`derivation`, the object key, the digest and the byte count. If they are equal,
inline mode is the same analysis with a different provenance. If they are not,
the difference is a bug and the failing grain says where.

**The debt test is the one whose mutant must die.** Force the ring to drop, and
assert the next accepted datagram declares the loss — and, at the row altitude,
that no sequence gap caused by the recorder is given a `publisher` verdict.
Revert the charge and the test must fail.

**The spool tests are about the failures, not the happy path.** A destination
that is down leaves windows on disk; one that recovers lands them oldest first
and records them; a full budget evicts the oldest and counts it; a process
killed mid-run replays its spool on the next start and the window lands; a
window whose own digest does not match is discarded by name rather than loaded
in part.

**The window tests cover the seam derivation cannot see.** A window closes on
its bound, the next one opens with the previous trailer, and the era anchor is
certain from the second window — including across a restart, through the ledger.

---

## Decisions

**Inline mode is a second arrangement, not a replacement.** Archive mode is the
default and is unchanged. A host recording a production feed for evidence
should run archive mode; inline mode is for bring-up, for hosts that were never
keeping the bytes, and for one deploy unit.

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

**No datagram archive in inline mode, optional or otherwise.** A mode that could
write both would be a third arrangement with its own sizing, its own failure
modes and its own tests, and the two modes it sits between already cover the
cases. A host that wants bytes runs archive mode.

**No change to archive mode.** Not to its configuration, not to its objects, not
to its manifest, not to its metrics, and not to the loader.

**No change to the deduplication keys.** The provenance column is in no sort key,
and no grain's identity changes.

**No conformance rule set here.** Inline mode derives the same grains archive
mode derives, and the conformance tier is where rules live in both.
