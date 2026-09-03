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
