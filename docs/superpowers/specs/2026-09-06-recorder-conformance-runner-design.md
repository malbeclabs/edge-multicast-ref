# Conformance findings, from an archive the rule set can be re-run over

**Status:** draft, pending review
**Date:** 2026-09-06
**Applies to:** the recorder's analysis tier, and the column store a dashboard reads
**Authority:** [`edge-feed-spec`](https://github.com/malbeclabs/edge-feed-spec) and its [`GLOSSARY.md`](https://github.com/malbeclabs/edge-feed-spec/blob/main/GLOSSARY.md), which owns the rule set this runs and every rule in it; `2026-08-31-sequence-loss-and-conformance-rows-design.md`, whose `conformance_finding` table this fills; `2026-08-28-edge-recorder-crates-design.md`, whose analysis tier names this as the missing half of step 7; `2026-09-05-recorder-market-data-rows-design.md` (on a branch in review at the time of writing), whose *No conformance rules over decoded messages* non-goal this document is the other side of

---

## Naming

This repository is public. This document names no venue, venue repository,
product line, host, bucket, dashboard or issue tracker, and gives no count of
publishers or of recorder sites. It states only what is to be built.

`GLOSSARY.md` governs the vocabulary: `datagram` never `frame`, **`era` never
`epoch`** for the sequence space a `Reset Count` opens, `channel` only for the
`Channel ID` shard, `port role` for `mktdata`/`refdata`/`snapshot`, `feed` never
`lane` or `stream`, and **`source` never bare** — every use below is `source
address`, `Source ID`, or `upstream`.

One further distinction is load-bearing here rather than decorative, because two
tables in this schema have a column called `verdict` and they do not share a
vocabulary. `finding` below always means a row in `conformance_finding`, and
`gap` always means a row in `sequence_gap`. Where this document needs the
relower's own word it says `re-lowering finding`, spelled out.

---

## The question

`recorder.conformance_finding` exists. It is in `001_recorder_rows.sql`, it has a
row type in `dz-recorder-rows`, it is a member of `Grain`, the sink knows how to
insert it and `002_recorder_retention.sql` states in as many words that it has no
TTL because *a verdict's whole value is that it was recorded when the rule ran*.
Nothing anywhere writes a row into it.

**That emptiness is the honest state and not an oversight**, and it is worth
saying why before proposing to end it. A `pass` row asserts that a rule was
evaluated, over evidence, and was satisfied. A table filled with `pass` rows a
runner inferred — from an exit code, from the absence of a complaint, from a port
that was never joined — asserts the same thing while none of it happened, and it
does so in the one table whose entire value is that somebody can trust what it
says about last month. An empty table says *nothing judged this*. A table of
manufactured passes says *this was judged and it was fine*, which is a different
statement and, when it is wrong, an expensive one.

So the question this document answers is not *how do we fill the table*. It is
**what has to be true before a row in it is worth more than the empty table it
replaces**, and then what a runner that only writes such rows looks like.

---

## The rule set is not ours, and this is not a hedge

The specification's rule set lives in `edge-feed-spec` as the `dz-conformance`
tool, and this repository already treats it as a third party rather than as a
dependency to be absorbed. That arrangement is checked in and can be read:

- `dz-recorder-e2e` runs it behind a `conformance` feature, over a replayed
  archive, and its own module comment states the reason — every other test in
  that crate compares the chain against itself, which cannot catch the two halves
  agreeing on something the specification forbids. The tool is the third party,
  and the comment puts the size of what it knows at 88 rules this repository has
  never encoded.
- The CI job that builds it checks out `edge-feed-spec` **pinned to a commit**,
  with the pin's own comment saying why: an unpinned checkout means the rule set
  changes under this repository without a commit here, so a rule-set update should
  be a deliberate bump rather than a build that broke overnight.
- Two design documents in this repository state the same non-goal in the same
  words — no new conformance rule set; rules are proposed upstream, not forked
  here — and the market data design states it a third time from the other side:
  if a decoded row makes a new rule expressible, that rule is a change to
  `edge-feed-spec`.

**So this document settles no rule.** It settles what a runner does with the
verdicts a rule set states, and every question of the form *should this be a
violation* is answered here in exactly one way: it is a change to
`edge-feed-spec`, proposed there, reviewed by the people who own the
specification, and reaching this repository as a pin bump. That is not a
deferral for want of an opinion. A rule set forked into a recorder is a rule set
with two authorities, and the first time they disagree the archive's verdicts
stop meaning what the specification means — which destroys the one property that
makes keeping the bytes worth anything.

What this document does owe, and pays below, is the precise statement of what the
runner needs from that rule set that it does not today expose, so that the ask is
a specification of an interface rather than a complaint.

---

## What a conformance rule is, over an archive

A rule is a predicate the specification states over a feed as delivered, and the
runner evaluates it over **one archived segment, for one channel instance, within
one era**. Every part of that grain is forced. The instance because a sequence
number means nothing under a coarser key. The era because a `Reset Count`
transition opens a new sequence space and a predicate carried across one is
comparing two rulers. The segment because of the purity argument below.

Rules divide into two classes, and the division is not by subject matter but by
**what makes a violation provable**.

**A structural rule is decidable from one archive alone.** Its evidence is bytes
that are present: a header field out of range, a declared length that does not
match the body, a schema version this generation does not define, a message
ordering the specification forbids within a datagram, a delta carrying an
`Action` the specification reserves for something else, a snapshot cycle whose
levels claim a `snapshot_id` no open `SnapshotBegin` ever opened. The recorder
kept those bytes; replaying them puts the rule in front of exactly what the
publisher sent. Nothing about the recorder's own losses can make such a violation
appear where there was none, because the violating bytes are the evidence and we
have them.

Both negative controls in `dz-recorder-e2e` are of this class, which is why they
work: a `LevelUpdate` carrying `Action = Change` with a zero quantity trips
`MBP.DELTA.ABSOLUTE_APPLY`, and a `SnapshotLevel` whose `snapshot_id` matches no
open begin trips `MBP.SNAP.GROUP_STRUCTURE`. Each is self-consistent — every
round-trip test in this repository passes the same stream — and each is a
violation the bytes themselves carry.

**An absence rule is not decidable from one archive alone**, and this is the
whole difficulty. Its evidence is that something is *missing*: a sequence number
nobody delivered, a heartbeat that did not arrive within its cadence, a snapshot
cycle with fewer levels than it declared, an instrument that stopped quoting.
Every one of those readings has a second explanation that is not the publisher's:
the recorder dropped it, the capture ring dropped it before it could tell the
port roles apart, the interface dropped it upstream of the capture point, or the
segment simply ends there.

The predecessor design already fixed the arithmetic that separates those, and it
is not available to a tool reading one capture file: `drop_delta` is what we lost,
`drop_scope` says at what scope it may be subtracted, and at `capture-handle`
scope it may not be subtracted per instance at all because the ring counts frames
dropped before demultiplexing. A rule set handed a replayed archive sees a hole
and has no way to learn whose it was.

**So the runner never writes a `violation` whose evidence range overlaps a hole
this same object's loss derivation found.** It writes `unverifiable`, with the
rule's own message and the missing range in `detail`, and the attribution lives
where it already lives — in `sequence_gap`, decided by the deriver that has the
admitted drops and the scope they are valid at.

This is computable in the same pass and needs nothing outside the object.
`dz-recorder-loss` already produces, per instance, a `Vec<SequenceRun>` carrying
`instance`, `era_ordinal`, `reset_count`, `missing_from` and `missing_to`; the
downgrade is an overlap test between a rule's evidence range and those runs. It
can only turn a `violation` into an `unverifiable` and never the reverse, which
is the direction this whole tier is conservative in, and the failure mode it
prevents is exactly the one `drop_scope` exists for: **a publisher violation
manufactured out of the recorder's own drop.**

It also states the limit of what an archive buys, which is worth being exact
about because the crates design oversells it slightly. Lossless replay does not
make an absence attributable — an absence in an archive is an absence in an
archive. What replay makes judgeable is *structure*: a rule that had to be
`unverifiable` live because the validator could not be sure it had seen the whole
datagram gets a complete, ordered, re-readable stream and can decide. The gate
opens far more often for structural rules and not at all for absence rules, and a
panel that reports the `unverifiable` share without that split will read as
though the archive stopped helping.

### The boundary against re-lowering, which is a different question

`dz-recorder-relower` also reads an archive and also produces something called a
finding, and the two must not be confused, because a reader who thinks one
subsumes the other will delete the wrong one.

**Re-lowering asks whether the publisher published what the venue said.** Its own
crate documentation is explicit about the shape: it takes **two** archives — the
multicast wire and a second archive of the raw upstream payloads — re-runs
`Adapter::on_payload` over the upstream bytes, lowers them with the publisher's
own lowering, and joins the two streams at message grain on `Per-Instrument Seq`.
Its four classes are *in the re-lowering and not on the wire*, *on the wire and
not in the re-lowering*, *both, fields differ*, and *both, identical, different
timing* — the last producing no finding at all.

**Conformance asks whether what was published is legal.** One archive, no
upstream, no adapter, no venue involvement of any kind.

Neither implies the other, and both directions occur. A publisher can lower an
upstream event with perfect fidelity into a message the specification forbids:
re-lowering is clean, conformance fails. A publisher can emit a flawlessly legal
message the venue never sent: conformance passes, re-lowering reports
`OnWireNotReLowered`. The crate says as much about itself — both sides run the
same mapping, so an adapter reading the wrong upstream field is invisible to it.

The practical rule is short. **The runner writes only `conformance_finding`, and
the relower writes none of it.** They share the archive, the walk's
`WireProvenance` and nothing else. Two tables that could each hold the other's
findings are two places for one stream to disagree with itself, which is the same
argument the market data design makes about refusing a fourth table for a window
in which the book is unknown.

---

## What a finding says

The table's columns are settled and this document does not move them. What was
not settled is what each value means and when it may be written.

### Four verdicts, and the five next door

`verdict` is one of `pass`, `violation`, `unverifiable`, `na` — the four values
`FindingVerdict` already carries in `dz-recorder-rows`, and the four the DDL's
own comment lists.

The neighbouring `sequence_gap.verdict` has five — `recorder`, `upstream`,
`path`, `unverifiable`, `publisher` — and they are **not the same vocabulary**.
That table answers *whose loss is this*; this one answers *did this rule hold*.
The one word they share is `unverifiable`, and it means the same thing in both:
the evidence to decide was not available, and saying so is the answer rather than
a failure to produce one. A reader joining the two tables on the instance is
joining two questions about one window, not two halves of one column.

| verdict | when the runner writes it |
|---|---|
| `pass` | the rule was evaluated, over a window whose coverage the runner could vouch for, on a port role that was joined, and it held |
| `violation` | the rule set stated a violation and its evidence range overlaps no hole in this object |
| `unverifiable` | the rule set could not decide, or it stated a violation whose evidence range overlaps a hole this object's loss derivation found |
| `na` | the rule needs a port role `roles_joined` never claimed, or one that was joined and carried nothing — so it did not run |

### `rule_set_version` is a commit, not a number we mint

The column is `LowCardinality(String)` and it is in the sort key — last, after
`site` and `recorder` — which is what lets one window legally hold two verdicts
from two versions instead of the later one replacing the earlier. That is the
column's purpose, stated in `001_recorder_rows.sql`: a rule added next month runs
against last month's traffic, and a dashboard that cannot say which version
produced a verdict cannot show that the rule set improved.

**The value is the upstream repository's commit, as CI already pins it, and never
a version this repository chooses.** A semantic version we assign is a number we
can be wrong about — two builds of one tag, a local patch, a tag moved — and
being wrong about it corrupts the exact comparison the column exists to make. A
commit resolves to bytes or it does not resolve at all.

It follows that **the runner must be able to ask the tool which rule set it is**,
and must refuse to run when it cannot get an answer. A verdict stamped with a
version somebody typed into a configuration file is a verdict whose provenance is
a claim, and the whole point of this table is that its provenance is a fact. If
the tool is configured against a stated version and the two disagree, that is a
refusal and not a warning, for the same reason the row deriver refuses an object
whose digest does not match its manifest: a finding drawn from bytes nobody
checked is a finding about a file rather than about a feed.

### The `pass` row is written, and here is what makes it honest

Four conditions, all checkable before the tool is invoked or from what it
returns, and all four required:

1. **The object was opened and vouched for.** Its sha256 matches the manifest and
   the replay terminated at the end of the archive rather than on a tear. This is
   the same pair of refusals `derive_object` already makes, for the same reason: a
   short window read as a complete one is a verdict about traffic that was never
   there.
2. **The port role the rule needs was joined**, as `roles_joined` in the manifest
   states. Otherwise the verdict is `na`.
3. **The rule set states a per-rule outcome**, rather than the runner inferring
   one from silence. This is the interface that does not exist yet; see below.
4. **For a rule of the absence class, the evidence window carries no hole this
   object's loss derivation found.** Otherwise `unverifiable`.

Given those, `pass` is written — and it is written for every rule, on every
instance, in every object judged.

**The argument for writing it at all is a panel that already exists.** The
predecessor's dashboard lists *`unverifiable` share, over time*, against
`conformance_finding`, answering *is the archive making the gate open*. A share
has a denominator. A table holding only violations cannot compute one, cannot
distinguish a clean feed from a rule set that evaluated nothing, and cannot show
the improvement that is the strongest single argument for keeping the bytes. The
second argument is the one this document opened with, inverted: an empty table
and a table of violations say the same ambiguous thing, and only the presence of
passes tells a reader that something was actually judged.

**The argument that it is affordable is arithmetic, not optimism.** The rows are
per rule per instance per segment, not per datagram. `002_recorder_retention.sql`
puts a busy recorder at roughly 80,000 datagrams a minute, on the order of 100
million `datagram` rows a day. Against that, 88 rules over the instances in a
segment, once per rotation, is thousands of rows a day — three to four orders
below the base rows, which is the band the retention split was drawn around. The
cost that is *not* negligible is stated under **Cost** below, and it is not the
row count.

### `na`, and the vacuity a clean exit can hide

A port role nobody joined produces no data, and no data looks exactly like a
clean feed. The predecessor says this about `segment_coverage` and it is the
reason `roles_joined` is in the manifest at all: the recorder records what it was
*asked* to join, so that a silent port reports `na` rather than `pass`.

The same trap exists one layer out, and this repository has already been bitten by
it well enough to have written a test about it. `dz-recorder-e2e`'s snapshot
negative control exists because a `-snapshot-port` that is *set but wrong*
evaluates zero snapshot datagrams and still exits 0 — the tool warns about a
starved rule only when the flag is unset. An exit code of 0 therefore means
*found no violations*, which includes *looked at nothing*.

Two consequences, both binding on the runner:

- **The ports and groups handed to the tool come from the manifest's
  `roles_joined`, never from the runner's own configuration.** The manifest
  describes the recorder that observed the bytes; a configuration describes what
  somebody believes. This is the same rule the market data derivation already
  follows for row identity, and for the same reason.
- **A role in `roles_joined` that contributed no datagram to this segment yields
  `na` for the rules that need it, not `pass`.** The manifest's `instances` map is
  what says whether anything arrived.

### The grain, and where the window comes from

One row is `(rule_id, channel instance, window)`, and the window is **the
segment**: `window_start` and `window_end` are the manifest's `start_ns` and
`end_ns`, and `first_seq` and `last_seq` are that instance's `first_seq` and
`last_seq` from the manifest's `InstanceCoverage`. `object_key` names where the
evidence is. All of them are read from the manifest rather than reconstructed
from what the runner happened to decode, which is what keeps a row placeable
independently of how far into the object the rule set got: a window assembled
from the first and last messages a rule evaluated would silently shrink whenever
a rule stopped early, and two rules over one segment would then disagree about
what segment they were talking about.

The sort key is `(rule_id, source_addr, channel_id, dst_port, window_start, site,
recorder, rule_set_version)`. `object_key` is deliberately not in it and does not
need to be: `window_start` is one recorder's own segment start, and two segments
from one recorder do not begin in the same nanosecond. The residual case — a
recorder restarting and a new run's first segment opening within the same
nanosecond as an old one's — is the same residual `datagram`'s key carries for a
duplicate whose receive stamp matches to the nanosecond, and nothing here closes
it either.

**A finding attaches to the instance the rule set names, and to no other.** Some
rules span port roles — a snapshot anchors on one role and the deltas it anchors
arrive on another, and those are two channel instances because the destination
port is part of the key. Where the rule set names which instance carried the
violating evidence, that is the row's instance. Where it does not, the runner
**refuses the verdict and counts it** rather than filing it under a guess: a
finding placed on the wrong instance is worse than one nobody wrote, because it
sends a reader to a sequence space where the evidence is not.

---

## A rule set the runner does not recognise

Three different things go under that heading and they have three different
answers.

**An unknown `rule_id`: write it.** The runner holds no enumeration of rules, no
allow-list and no mapping from rule to meaning. `rule_id` is an opaque
`LowCardinality(String)` and travels through untouched. A runner that refused an
identifier it did not know would refuse precisely the rule that was added to
catch the thing nobody had thought of — which is the case the whole
run-last-month's-traffic property exists to serve. The only structure the runner
reads out of a rule's report is the instance it names and the evidence range it
cites, and both of those are interface, not taxonomy.

**An unresolvable rule-set version: refuse to run, and write nothing.** A verdict
that cannot say which rule set produced it is unattributable, and an
unattributable verdict is worse than an absent one: it will sit in the same
column as the attributable ones and quietly break every comparison across
versions. This is the same shape as the loader's refusal of a `Magic` nobody
filled in and of a capture-drop scope the archive does not state — a default here
would license a claim nothing made.

**A tool that could not run at all: count it, and leave the object unjudged.**
`dz-conformance` exits 2 for this, and the e2e helper's `assert_clean` checks that
code first and separately, because it means something categorically different
from a violation. It must **not** become a table full of `unverifiable` rows.
`unverifiable` is a statement the rule set made about the *traffic*; a tool that
did not start is a statement about *us*, and writing the first where the second
happened would move the `unverifiable` share panel — the panel that is supposed
to measure how often the archive opens the gate — every time a binary went
missing. The object stays unjudged, a counter rises, and the absence of rows is
the honest record.

The same reasoning forbids the tempting shortcut of inferring `pass` from exit 0.
Exit 0 means *found no violations*, and the snapshot negative control above is a
checked-in demonstration that it can also mean *evaluated nothing*. **The runner
writes no verdict a rule set did not state.**

---

## Idempotency, re-runs, and the third axis

`ReplacingMergeTree(run_ts)` on the key above gives the re-run behaviour this
tier needs, and it is worth spelling out which re-runs it makes safe, because
they are not all the same re-run.

**The same rule set over the same object, again.** Same key, later `run_ts`, and
the engine keeps the greater `run_ts` — so the second run replaces the first
rather than duplicating it. Note that this is *replacement*, not byte identity:
`run_ts` is when the rule set ran and it legitimately differs. Everything else in
the row is a pure function of the object, the manifest and the rule set version,
which is what makes the replacement a no-op in every column anybody queries.

**A newer rule set over an already-judged object.** Different key, because the
version is in it. Both verdicts stand, which is the whole design: one window
legally holds two verdicts from two versions.

**An older rule set after a newer one.** Different key again, so the newer
verdict is not clobbered by a re-run of history. This is the case the version
column earns its place in the sort key for; without it the last runner to finish
would win.

**A runner that runs late.** Objects are evicted under the recorder's staging
budget, and an object that is gone is never judged. There is no verdict for it and
there must not be: `segment_coverage` already says the window existed, and the
absence of findings against a window that has coverage is the honest reading. The
runner never writes a verdict for an object it did not open.

**Two runners over one directory.** The identity on every row comes from the
manifest — as the market data derivation already establishes, because a process
re-processing another recorder's object must not sign it — so two runners produce
identical keys and the writes collapse. They will duplicate work and neither will
corrupt the other.

### The load ledger's key is not sufficient here, and that is the new part

The loader's `Ledger` answers `is_loaded(object_key, object_sha256)`: a boolean
over two fields, and correct for what it guards, because the rows it guards are a
pure function of those two fields. **A finding is not.** It is a function of the
object *and* of a rule set version that moves independently of this repository's
binary. An object judged under one version and not yet under the next is neither
loaded nor unloaded in that ledger's vocabulary, and asking it produces the wrong
answer in the expensive direction: the version bump lands, the runner asks *have I
loaded this object*, hears yes, and never re-judges anything.

**So the runner keeps its own ledger, keyed `(object key, sha256, rule set
version)`, in its own file.** Widening the load ledger's `Entry` was considered
and rejected: it would push a rule-set version into a record whose other consumer
— the era adjacency check, which carries a `SegmentTrailer` there — has nothing to
do with rule sets, and it would make the boolean that guards datagram rows depend
on whether a Go binary was rebuilt. Two derivations with two different notions of
*done* get two ledgers.

The consequences follow the loader's existing habits. The entry is written after
the rows are in, so a failure leaves the object unjudged rather than falsely
complete. Compaction drops entries for objects that are no longer present, as the
load ledger's already does. And the lag metric this ledger feeds is the runner's
own and is not the loader's, for the same reason the market data derivation has
its own: they are different numbers, and one that folded them would be a number
about neither.

---

## The bridge, and the three things it must not assert

The tool reads **classic pcap**; the archive is **pcapng**. `dz-recorder-e2e`
already crosses that gap by replaying the archive and writing a pcap by hand —
twenty-four bytes of file header, sixteen per record — and notes that the
conversion is the only thing the test adds to the chain, adding nothing to the
payloads.

That helper is correct for the fixtures it serves and is **not** a general bridge,
in three specific ways. Each one is a place where a careless conversion would
manufacture a finding, which makes them design constraints rather than
implementation notes.

**It must not assert that a truncated datagram was complete.** `RecordedDatagram`
carries `wire_payload_len` beside the payload precisely so that a datagram over
the mandated cap survives as the violation it is — the type's own documentation
says archiving its first 1232 bytes as though that were the whole thing turns the
violation into a clean datagram, and discarding it turns the violation into a
sequence gap the publisher is then blamed for. A pcap record has both an included
length and an original length; the e2e helper writes the same value into both,
which asserts *not truncated*. A bridge that did that for a genuinely truncated
datagram would hand the rule set a body shorter than its declared length — one of
the three ways a walk goes wrong — and collect a violation the recorder caused.
The two lengths are written separately, and a violation whose evidence is a body
we ourselves cut short is `unverifiable`, by the same rule as any other absence.

**It must not synthesise link headers over captured ones.** The e2e helper always
synthesises, exactly as socket mode's archive does. But `RecordedDatagram`
carries `link_headers: Option<&[u8]>` — the Ethernet, IPv4 and UDP bytes as they
arrived when the capture mode read them off the interface, and `None` meaning
they were synthesised and are therefore not evidence about the wire. The manifest
says which, in `link_headers`. A bridge that rebuilt headers over captured ones
would discard the identification field, the fragmentation flags and the checksums
that the archive kept on purpose, and would present a fiction to any rule that
reads below UDP.

**It must not hand the tool more than one group at a time.** The tool takes one
`-group` and one port per role. An archive may hold several groups, and it may
hold redundant publishers — two source addresses on one channel and port. The
second case is the rule set's own business, since it keys its state on the
channel instance; the first is not, so the runner invokes the tool once per group
present in `roles_joined`, and the per-object process count follows from that
rather than from anything the runner chose.

One limit falls out of that interface and is named here rather than left to be
discovered as a mysterious verdict. Where a feed's three port roles sit on three
different groups, no single invocation can see all three, so a rule that needs
two of them cannot be evaluated at all and its verdict is `unverifiable`. That is
a property of the flags the tool takes and not of the archive, which holds every
role's datagrams either way. It is an ask on `edge-feed-spec` should such a rule
matter, and deliberately not one this document makes now: the two asks below are
what the table cannot be filled without, and adding a third that nothing is yet
blocked on would dilute them.

---

## Cost, stated before it is discovered

The row count is the cheap part and is not what this tier costs.

**Every judged object is read a third time, decompressed, and written out
uncompressed.** The loader already walks an object once for the transport rows,
and the market data derivation walks it a second time for a feed that asked. This
is a third walk, and unlike the other two it produces a temporary file: the pcap
the tool reads is the payload bytes plus 42 bytes of link header and 16 bytes of
record header per datagram, with no compression. The manifest states
`payload_byte_count` and `datagram_count` before the object is opened, so the size
is known in advance — which means the runner can refuse an object it has no room
for rather than filling a disk, and **the temporary file lives on the runner's own
writable path and never in the recorder's directories**, for the same reason the
ledger does: a file the staging budget cannot classify, sitting beside the objects
eviction has to reach, is a way to lose the archive.

**It is a process per group per object, not a library call.** A fork, an exec, a
Go process's own walk of the file, and its exit. Bounded per pass by the same
`max_objects_per_pass` the loader already has, and slower per object than either
derivation beside it.

**It depends on a binary this repository does not build.** The tool is written in
Go, lives in another repository and is pinned by commit. A runner that found it on
`PATH` would stamp its verdicts with whatever rule set happened to be installed,
so the path is required configuration with no default — the same stance the
loader takes on `objects_dir` and on `magic`. Configured and absent is a `--check`
failure; configured and disappearing at run time leaves objects unjudged and
raises a counter. It never silently skips, which is the position `dz-recorder-e2e`
already takes for its own feature and for the same stated reason: a conformance
gate that passes when it could not run reports a clean feed for a stream nobody
validated.

**A version bump re-judges history, and that is both the point and the largest
bill this tier can present.** Running a new rule set over everything still
retained is the property that justifies keeping bytes at all. It is also, on the
day it happens, a full re-read of the retained archive and a second copy of every
row. It must be a deliberate, bounded act — a range of objects, chosen — and never
a consequence of a deploy that happened to pick up a new pin. A runner that
re-judged the archive automatically on restart would turn a routine binary roll
into the most expensive thing the host does.

**And the table has no TTL.** `002_recorder_retention.sql` says so and gives the
reason, and the reason is right. The consequence is that this table's growth rate
is the row arithmetic above *multiplied by the number of rule set versions ever
run over a given window*, and it never sheds any of it. Thousands of rows a day
per version is affordable for a long time and is not affordable forever; the
decision to be taken later is which old versions' verdicts to drop, and it is
noted here so that it is taken deliberately rather than discovered.

---

## What this needs that does not exist yet

**Two of these are asks on the specification's repository, and they are the
reason this document cannot end with an implementation note.**

- **A per-rule outcome placed and ranged.** The tool already writes a declared
  report — `-json-report` emits `{version, commit, strict, read_error, rules}`,
  and each rule entry carries `rule_id`, `severity` and a map of `counts`. So the
  ask is a **widening of a file that exists**, not a new interface, and it is
  smaller than it first appeared. What the counts cannot supply is where a
  finding goes and what it is about: an entry needs **the channel instance it
  applies to** — source address, `Channel ID` and destination port — and **the
  sequence range its evidence lies in**. The first is what places the row, since
  a sequence number means nothing under a coarser key and one capture can hold
  several instances. The second is what the absence downgrade tests against the
  object's own holes; without it every absence rule is unusable here, because a
  finding we cannot range is a finding we cannot decline to stand behind.

  What is **not** asked for is a parse of standard error. The exit code and the
  named violations there are enough for a gate — `dz-recorder-e2e` matches
  `MBP.DELTA.ABSOLUTE_APPLY` and `MBP.SNAP.GROUP_STRUCTURE` by substring — and
  they are not enough for a table whose grain is one row per rule. Parsing them
  is refused on the grounds this document gives elsewhere: a format nobody
  declared changes with a log line, and a `rule_id` recovered by a regular
  expression becomes an empty string silently.
- **A build that stamps the commit it was built from.** The version query also
  exists: `--version` prints `version+commit`, one line, and the report carries
  the same two fields. Both are set at build time through
  `-ldflags -X main.version -X main.commit`, and a build that omits them answers
  `dev+none` — which resolves to a value rather than to a commit, and a value is
  what this table must not store. So what is needed upstream is not a query but
  that the build producing the consumed binary stamps its pin, and this
  repository's own CI job must do the same for the binary it builds from a
  checkout. A verdict stamped `dev+none` says its provenance is unknown while
  looking exactly like a verdict whose provenance is known.

The rest is work in this repository:

- **A pcap bridge**, promoted out of `dz-recorder-e2e`'s test module into
  something the runner can use, widened for the three cases above: the two
  lengths written separately, captured link headers preferred over synthesised
  ones, and one invocation per group.
- **The runner itself**: a pure function of one object, its manifest, that
  object's own loss runs, and the rule set's report — producing
  `ConformanceFinding` rows and nothing else.
- **The `na` derivation** from `roles_joined` and the manifest's `instances`, so
  that a port nobody joined and a port that carried nothing are both distinguished
  from a clean one.
- **The absence downgrade**, as an overlap test between a rule's evidence range
  and `dz-recorder-loss`'s `SequenceRun`s for the same instance and era.
- **A second ledger**, keyed on the object and the rule set version, with its own
  lag metric.
- **A per-feed switch in the loader's configuration**, off by default, in the
  shape `[[market_data]]` already establishes, plus the tool's path as required
  configuration and a `--check` that says back which rule set version was found.
- **Counters** for the refusals this document names: a tool that could not run, a
  verdict naming no instance, a version that could not be resolved, and a
  disagreement between the configured version and the tool's own.

---

## Non-goals

**No new conformance rule, and no rule forked into this repository.** The rule set
is `edge-feed-spec`'s, its maintainers decide what a violation is, and this
repository's only decision is which commit of it ran. A rule this archive makes
newly expressible is a change proposed there.

**No rules over decoded messages, written here.** The market data design's last
non-goal is the same statement from the other side, and it stands: if a market
data row makes a new rule expressible, that rule is a change to `edge-feed-spec`,
not a query in this schema.

**No attribution.** The runner decides no loss and writes into no gap row. Whose a
hole is stays the gap deriver's question, decided with the admitted drops and the
scope they are valid at, and the runner's only interaction with it is to decline
to accuse a publisher over a hole.

**No cross-site conformance.** A rule set reads one feed as delivered to one
observation point. The question two sites answer together is loss, and the
predecessor already establishes that it is a query over `datagram` rather than a
new table or a new tool.

**No change to the record path.** The recorder still decodes nothing while
recording. All of this happens in a separate process reading completed objects
read-only — the property that lets it be turned off, run late, or re-run over the
same objects without touching a live capture.

**No venue clients, credentials, or comparison against a venue's own service.**
Every row here comes from bytes an archive already holds.
