# `dz-recorder-load` — where the rows come from, and where it runs

Turns a directory of completed objects into rows a dashboard can ask, and does
nothing else. It is a **separate process from the recorder**, sharing one
directory with it and nothing more.

```bash
dz-recorder-load --config /etc/dz-recorder-load/loader.toml --check
dz-recorder-load --config /etc/dz-recorder-load/loader.toml --watch
dz-recorder-load --config loader.toml --once --dry-run /var/tmp/rows
```

## It runs on the recorder host

Against that host's own completed directory, opened read-only.

**Nothing ships objects off a recorder host today**, and objects are evicted
under the staging budget in about a day and a half on a busy one. The rows are
tens of bytes against a datagram's twelve hundred, so the small thing travels
and the bytes stay local. That is the whole argument: a shipper moves 1232-byte
datagrams across a network to a place where a loader would read them once and
reduce them by two orders of magnitude, and the reduction can just as well
happen where the bytes already are.

**State the consequence plainly, because it is the reason the arrangement is
worth having: this is what makes the cross-site join available before a shipper
exists.** `(channel instance, sequence number)` identifies a datagram
independently of who received it, so two sites' loaders writing into one column
store can be joined on that key — and the join is over rows, not over objects.
Publisher-attributable loss, per-site arrival latency from one publisher's send
stamp, which site saw a datagram first and by how much: all of it is answerable
with no shipper, no credentials and no venue involvement. Not having a shipper
costs *retention*, and not the join.

### The gate on that arrangement

**`dz_loader_oldest_unloaded_age_seconds`, alerted against the eviction window.**

Objects are deleted under the recorder's staging budget, so a loader slower than
the write rate loses history permanently and silently — there is no re-run that
recovers an object that is gone. That makes lag a first-class metric with an
alert and not a log line, and it is published as two numbers because either
alone misleads:

| | |
|---|---|
| `dz_loader_unloaded_objects` | how much is waiting |
| `dz_loader_oldest_unloaded_age_seconds` | how close the oldest of it is to being evicted |

A backlog of two hundred young objects is a busy loader. One object an hour
older than the eviction window is history already gone. **Alert on the age.**

## What it cannot do

**It cannot touch the recorder.** The objects directory is opened read-only —
the unit mounts it that way as well — and the ledger lives on the loader's own
path, deliberately not inside the archive directory: a file the recorder's
staging budget cannot classify is a file eviction cannot reach. A column store
that is down, slow or full costs loading progress and nothing else.

**It adds no key to the record path.** `RecorderConfig` documents the absence of
an endpoint, a credential and a database key as an invariant, because the
recorder does not upload. This process has its own configuration file, service
user and metrics port. There is no password key in either file: the column
store's password comes from a systemd credential
(`DZ_LOADER_CLICKHOUSE_PASSWORD_FILE`) or, second best, from
`DZ_LOADER_CLICKHOUSE_PASSWORD`.

## Two things to do at provisioning, neither of them in this crate

**Build with `--features tls` if the destination is `https://`.** TLS is off by
default, so the default build carries no TLS stack at all — which keeps `rustls`
and its provider out of every crate in this workspace. An `https://` endpoint is
then refused at configuration load rather than silently downgraded, because a
loader that quietly spoke plain HTTP to an endpoint an operator wrote as `https`
would put the password on the wire in the clear having been told not to. The
failure mode is a startup failure, which is the good kind — but it is a startup
failure, so the build and deploy pipeline has to enable the feature *before* the
unit is enabled, and `--check` is where it is caught. The released asset is that
pipeline's answer and carries the feature already, so this is a thing to do only
for a binary built by hand.

**Apply `004_recorder_loader_user.sql`, with the password as a parameter.** The
account is checked in beside the schema and bounded at creation: `INSERT` on the
five tables and nothing else — the adjacency check reads the preceding trailer
from the loader's own ledger, not from the destination, so the account needs no
`SELECT` on anything — no DDL at all, a settings profile with a read-bytes
ceiling and a single thread, and a quota. It is applied by an administrator and
not by the loader, because a loader that could grant itself privileges is the
thing the file exists to prevent, and the password is a query parameter so that
no credential lives in this repository.

That leaves one thing the file cannot do, and it is the workload thread share: set it where the cluster supports one. A loader is a
write-path workload and should be cheap; what makes an unbounded account
dangerous is not this process misbehaving but the queries somebody later points
at the rows it wrote. A workload added without limits is discovered weeks later
by someone reading a graph, and by then it is a table too big to fix cheaply.

## Releases, and what the version number is

`dz-recorder-load release` in `.github/workflows/` publishes
`dz-recorder-load_<version>_linux_amd64.tar.gz` under the tag
`dz-recorder-load/<version>`, built on Ubuntu 24.04 with `--features tls` so
that one asset serves both an `http://` destination and an `https://` one. The
unit and `loader.example.toml` travel in the tarball beside the binary, because
a unit fetched from a branch is the unpinned copy the release exists to replace.

**The version is the loader's own, and does not track `dz-recorder`'s.** The
crate states it in its own `Cargo.toml` rather than taking
`version.workspace = true`, and the reason is what a pin is for: a number that
advanced because the recorder was bumped tells an operator comparing two pins
nothing about whether the loader changed. So a loader and a recorder carrying
different numbers are not a mismatched pair, and equal numbers are not a matched
one. What the two must agree about is the archive format, and every object
states that in its Section Header block and the manifest beside it — where this
process checks it, and refuses.

**Neither the unit nor `loader.example.toml` names a version**, deliberately.
Both would then be a second number somebody keeps in step with the installed
binary by hand, and nothing would enforce it — the failure the paragraph above
is arranged to avoid, reintroduced one directory down. The installed binary
answers the question itself: `dz-recorder-load --version` reports the version
and the commit it was built from, and `ExecStartPre` already runs that binary at
every start.

## What the sink sends, and why it holds

**One insert is one part, so merge pressure is set by rows per part rather than
rows per day** — and merge work never appears in a query log, only as the gap
between a provider's CPU graph and query-attributed CPU. A sink that posted once
per object would write one part per object per lane, and the quietest lanes
measured produce about 700 rows in a time-rotated object.

So the sink holds rows across objects:

| Key | Default | |
|---|---|---|
| `insert_max_rows` | 1,000,000 | an object's rows land in one or two parts |
| `insert_min_rows` | 50,000 | the floor that stops one part per object per lane |
| `insert_max_delay` | 900s | the bound on holding, so a quiet lane is late rather than absent |

**Which means accepted is not loaded.** `dz_loader_held_objects` is the part of
the backlog that is the sink coalescing as designed; `dz_loader_unloaded_objects`
includes it, deliberately, because rows in memory are not in the store. A ledger
entry is written when the insert carrying an object's rows is acknowledged and
never when the sink takes them — an entry written on acceptance would mark an
object loaded whose rows a crash then loses, with nothing recording that it did.

A `--once` pass and a shutdown both flush, so no run leaves rows in memory the
ledger will never account for.

The one query shape worth knowing before it is pointed at a dashboard is
`recorder.era_ranked`: `era_index` is a dense rank, and a dense rank is defined
over all history, so no predicate on time can be pushed through it. `era` is
kept indefinitely and carries one row per channel instance per segment. A
time-ranged panel joins `recorder.datagram_in_era` or `recorder.sequence_gap`
instead — both key on the era's anchor, and both prune. `003_recorder_era_rank.sql`
states which is which and why.

## Idempotence is a property, not a procedure

Loading the same object twice produces the same rows, because the derivation is
a pure function of the object and the manifest beside it. The ledger is keyed on
`(object key, sha256)` — the digest is part of the key because an object key
alone would make a *re-derived* object look loaded when its rows are not there.
The tables are `ReplacingMergeTree` on the same pair, so a re-run after an
analyser fix is a replace rather than a duplication.

Two consequences follow, and both look wrong until the above is in mind:

- **The ledger entry is written after the rows are in.** An entry written first
  would make a failed load look complete for ever.
- **A failure anywhere in an object's batch leaves the whole object unloaded,
  even though rows landed.** Reporting what got through would leave an object
  whose datagram rows are present and whose gap rows are not — and that object
  reads as a clean feed for ever. Partial credit is how a gap becomes invisible.

## Oldest object first

Not a preference. Two reasons, pointing the same way:

- The oldest object is the one closest to eviction, so it is the one whose loss
  is permanent.
- The adjacency check that settles an era boundary needs the *preceding*
  segment. In order, every boundary after the first is certain; out of order,
  the first object of every run writes an uncertain anchor and every gap inside
  it is reported `unverifiable`.

The loader never *waits* for a predecessor, though. Under a staging budget that
evicts, the predecessor is routinely gone, and a loader that must see segment
*n−1* before it can anchor segment *n* stalls on the first eviction. It writes
the era row uncertain and moves on; a later load with the predecessor in hand
rewrites that one row. **Evidence arriving late upgrades a verdict; its absence
never blocks one.**

## What it refuses

Each refusal is the alternative to a finding drawn from something we did to the
evidence ourselves.

| Refusal | Because |
|---|---|
| A digest that does not match the manifest | A finding drawn from an object nobody verified is a finding about a file, not about a feed. Verification is part of loading, not an operator's habit. |
| A replay that did not end on a block boundary | A short window read as a complete one is a sequence gap with nothing admitted behind it — a publisher finding manufactured out of our own truncation. |
| An archive that will not state its capture-drop scope, or states two | Every subtraction rests on a declared scope, and a default would license one the archive never claimed. |

A refused object is counted, named on stderr, and left unloaded. The pass carries
on: one damaged file must not stop an archive from being loaded, and the object
it would stop at is the one closest to eviction.

## Not here yet

The conformance runner over replay. `conformance_finding` is created as the
table a runner fills, and this loader writes no row into it — an empty table is
the honest statement that nothing judged the object, where a `pass` row would be
a pass over a rule that never ran.

The cross-site pass that turns `unverifiable` into `publisher`. This loader
never writes `publisher`: it needs a datagram absent from *every* site with no
recorder overflow anywhere, and one vantage cannot say that. The row carries
`seen_elsewhere` as `NULL` and the verdict as `unverifiable` until the join has
run.
