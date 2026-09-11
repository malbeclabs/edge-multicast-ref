# The DoubleZero Edge recorder

Keeps the bytes of an edge feed, with the recorder's own losses recorded inside
the archive, so that *did the publisher send what the spec says it must, and did
it arrive?* can be answered after the fact — hours later, or against a rule that
did not exist when the traffic passed.

The recorder is agnostic to the feed it records, to the venue behind it, and to
whether it is reading a socket or a file. It ships as a library so that several
recorder hosts can be built on it without any of them re-deciding what to keep.

Design: [`2026-08-28-edge-recorder-crates-design.md`](../../docs/superpowers/specs/2026-08-28-edge-recorder-crates-design.md).
Plan: [`2026-08-30-edge-recorder-record-path.md`](../../docs/superpowers/plans/2026-08-30-edge-recorder-record-path.md).

## What runs, on one host

Two processes, and the directory between them is the whole interface. Nothing
else connects them: no socket, no queue, no shared memory. That is **archive
mode**, selected by the two `[archive]` directories it has always required, and
the arrangement everything below describes unless it says otherwise;
[the two modes](#the-two-modes) is where the other one is.

```
  the wire                    RECORD PATH  (dz-recorder)                        the disk
┌────────────┐   UDP    ┌───────────────┐         ┌───────────────┐      ┌────────────────────┐
│ publishers │─────────▶│ -capture      │────────▶│ -archive      │─────▶│ staging/  →        │
│ (2 paths,  │ multicast│ joins, stamps,│ datagram│ rotates, zstd,│ .zst │ completed/         │
│  n feeds)  │          │ counts drops  │         │ hashes, mani- │ +json│  <feed>/<seg>.zst  │
└────────────┘          │               │         │ fests, evicts │      │  <feed>/<seg>.json │
                        └───────────────┘         └───────────────┘      └─────────┬──────────┘
                         decodes NOTHING                                           │ read-only
                         ─────────────────                                         │
                         A datagram a decoder                                      ▼
                         would reject is a          ANALYSIS PATH  (dz-recorder-load)
                         datagram the archive     ┌──────────────────────────────────────────┐
                         never holds — and the    │ -replay   reads objects back as a Source │
                         evidence needed to       │ -loss     which sequence values are gone │
                         diagnose that bug is     │ -relower  bytes → messages + state msgs  │
                         what the bug destroyed.  │ -events   era-scoped reference data      │
                                                  │ -rows     the row model, sink-agnostic   │
                                                  └────────────────────┬─────────────────────┘
                                                                       │ JSONEachRow
                                                                       ▼
                                                            ┌────────────────────┐
                                                            │ -clickhouse (sink) │
                                                            └────────────────────┘
```

**The separation is the design, not an implementation detail.** The record path
must never block, so it never decodes, never joins and never waits on a server.
The analysis path can be turned off, run late, or run twice over the same object,
because it only reads objects that are already written and its writes are
idempotent on `(object key, sha256)`.

## What is analysed, and where the sites meet

Each host loads its own objects. Nothing ships an object anywhere — the rows are
tens of bytes against a datagram's twelve hundred, so the small thing travels and
the bytes stay local. That is also what makes a cross-site question answerable
**before** a shipper exists: the join is over rows.

```
   site A                        site B                        site C
┌────────────┐               ┌────────────┐               ┌────────────┐
│ recorder   │               │ recorder   │               │ recorder   │
│  objects ──┼──┐            │  objects ──┼──┐            │  objects ──┼──┐
│  (local,   │  │ loader     │  (local,   │  │ loader     │  (local,   │  │ loader
│   evicted) │  │            │   evicted) │  │            │   evicted) │  │
└────────────┘  │            └────────────┘  │            └────────────┘  │
                └──────────────────┬─────────┴───────────────────┬────────┘
                                   ▼                             ▼
                        ┌──────────────────────────────────────────────┐
                        │                 column store                 │
                        ├──────────────────────────────────────────────┤
                        │ TRANSPORT   datagram · era · segment_coverage│
                        │            sequence_gap · conformance_finding│
                        │ MARKET DATA event · instrument · book_top    │
                        └──────────────────────────────────────────────┘
                                   │
                                   ├─ was a datagram lost, and whose was it?
                                   │    absent at ONE site  → that site's path
                                   │    absent at EVERY site → before they diverge
                                   ├─ what was the book, and can it be believed?
                                   │    book_certain = 0 says the honest answer
                                   └─ who saw this state first?
                                        one state_key at two `observation` values
```

**A gap at one site is a path; a gap at every site is upstream of all of them.**
One vantage point cannot tell those apart, which is why a `sequence_gap` row lands
`unverifiable` until the join has run — and why the second recorder exists at all.

## The crates

| Crate | What it is for |
|---|---|
| `dz-recorder-core` | The types every other crate speaks: `RecordedDatagram`, `ChannelInstance`, the `Source`/`Sink`/`Observer` traits, `RecorderIdentity`, and the configuration |
| `dz-recorder-capture` | Live capture as a `Source`: membership, kernel receive timestamps, drop accounting, rejoin, source admission |
| `dz-recorder-archive` | Two archive shapes: the pcapng writer for datagrams, the [upstream-message object format](dz-recorder-archive/UPSTREAM-OBJECT-FORMAT.md) for a venue's own bytes, and the rotation, compression, hashing, manifest and staging watermark both share |
| `dz-recorder-replay` | An archive read back as a `Source`, plus the synthetic publisher the tests are built on |
| `dz-recorder-loss` | Which sequence values nobody delivered, per channel instance and per era, and whose they are |
| `dz-recorder-relower` | An archive read back as decoded messages, and re-run against a venue's own mapping: *did the publisher publish what the venue said?* |
| `dz-recorder-health` | Whether a recorder is recording, as the process itself can tell |
| `dz-recorder-rows` | The rows an archive derives into, and the derivation: pure, sink-agnostic, and exercised with no server |
| `dz-recorder-events` | Market data rows: reference data scoped to an era, the fold that joins the messages to it, and the book that says when its top cannot be believed |
| `dz-recorder-venue` | The venue half of a feed race: a derivation that drives a venue's own `Adapter` over archived upstream messages, and the rows it produces |
| `dz-recorder-clickhouse` | The column store as one `RowSink`, plus the checked-in DDL |
| `dz-recorder-load` | The loader binary ([README](dz-recorder-load/README.md)) |
| `dz-recorder-inline` | Inline mode as a library: the ring, the window and its synthesised manifest, and the spool that holds rows until the column store has taken them |
| `dz-recorder-e2e` | The tests that use the real encoder, the real writer and the real reader end to end |

Take what you need. A publisher wanting a byte-exact record of its own egress
takes `-archive` alone; a test harness takes `-replay` alone; a host that only
needs alerting takes `-capture` and writes nothing. Nothing above
`dz-edge-core` is required in order to record, and **nothing in the record path
decodes a datagram** — a message a decoder rejects is a message the archive
never holds, and the evidence needed to diagnose that bug is what the bug
destroyed.

## The two modes

Two arrangements of the same crates, and what separates them is what the host
keeps. Designed in
[`2026-09-08-recorder-inline-mode-design.md`](../../docs/superpowers/specs/2026-09-08-recorder-inline-mode-design.md).

| | **archive mode** | **inline mode** |
|---|---|---|
| Processes | two: `dz-recorder` writes objects, `dz-recorder-load` derives rows from them | one: capture, derivation and load in the same process |
| Unit | an object — a rotation bound's worth of datagrams, compressed, hashed, manifested | a window — the same bound, in memory, derived and then discarded |
| What is on disk afterwards | the datagrams, and then the rows | the rows, and only until the destination has taken them |
| Every row says | `derivation = archive` | `derivation = live` |
| Selected by | `archive.staging_dir` and `archive.completed_dir`, which it has always required | `--inline-config`, the file carrying the spool, the ledger and the destination |
| A configuration stating both | **refused**, naming the key and the file | |
| A configuration stating neither | **refused**, naming both ways of stating one | |
| Disk sized as | retention × bytes per second | a bounded backlog of rows |

**Neither is a default, and no flag names either.** Each arrangement is selected
by the resource only it can run on: archive mode by the two directories it writes
into, inline mode by the file carrying its spool, ledger and destination. Archive
mode is still what a host recording a production feed for evidence should run —
rows are derived and re-derivable, bytes are not, and a recorder that stored only
its own interpretation would have thrown away the ability to be wrong about it.

**What makes the selection safe is that it is total.** Both key sets are required
with no default and they are disjoint, so the four combinations are every
combination: an archive stated and no file is archive mode, a file and no archive
is inline mode, both is refused naming the key and the file, neither is refused
naming both. There is no silence left over for a default to be placed on, so no
host can be moved between the two arrangements quietly — which matters because
the restart that would move it is the moment nobody is reading its log. The
reading is
[argued in the design](../../docs/superpowers/specs/2026-09-08-recorder-inline-mode-design.md#why-the-arrangement-is-stated-and-never-defaulted),
the flag is
[decided against there](../../docs/superpowers/specs/2026-09-08-recorder-inline-mode-design.md#whether-a-flag-survives-as-an-explicit-override),
and what it costs — nothing outside this repository, because an archive host
selects its arrangement by saying what it already said — is
[named there](../../docs/superpowers/specs/2026-09-08-recorder-inline-mode-design.md#what-it-costs).

**Inline mode keeps no datagrams.** One process joins the feed, derives its rows
through the same derivation archive mode uses, spools them and loads them. There
is no object, no manifest to fetch and no digest: the window's synthesised
manifest carries an empty `sha256` and a zero `byte_count`, because an invented
digest is a claim that something was verified.

### What that costs

Three losses, and none of them is recoverable later:

- **A rule written next month cannot be run against last month's traffic.** A
  conformance rule set is a growing thing and an archive is what lets it grow
  backwards. There is nothing to run a new rule against.
- **A row cannot be re-derived.** A derivation defect found later is a defect in
  rows that can be stopped and not corrected: the objects that would be re-read
  do not exist.
- **Nothing verified the bytes the rows came from.** In archive mode a digest
  that disagrees with the manifest means no row is derived at all. Inline there
  are no stored bytes for anything to disagree with, so no verification stands
  behind a row.

### What bounds it

**Neither arrangement can be entered by accident.** Archive mode is unchanged —
its configuration, its objects, its manifest, its metrics, the loader, and the
command line it runs on are all what they were — and its two directories are what
select it. Inline mode is selected by the file it cannot run without, and a
configuration stating both is refused rather than resolved. Those refusals are
what the selection rests on: weaken either and a host can stop keeping bytes
without saying so.

**Inline mode derives the five transport grains and no market data rows.**
`event`, `instrument` and `book_top` come from a codec walk, which archive mode
runs in the loader and which this arrangement will not run on the record path. A
`[[market_data]]` entry in inline mode's file is **refused by name** rather than
ignored, so asking is answered at `--check` instead of leaving three tables
empty for a feed somebody expected them from.
[The design argues it](../../docs/superpowers/specs/2026-09-08-recorder-inline-mode-design.md#why-no-market-data-rows-and-why-that-is-refused-rather-than-left-empty).

**Every row says which it is.** A `derivation` column carries `archive` or
`live` on all eight grains, so no query can mistake a row derived from verified
bytes for one derived in flight, and a panel that must not mix them can filter.
It defaults to `archive`, so rows written before the column existed keep their
meaning, and it is in no `ORDER BY`: a row is the same row whichever mode
produced it, and provenance in the sort key would make two modes' views of one
datagram two rows instead of one.

**The derivation is the same function.** Inline mode supplies a third `Source`
and calls `dz-recorder-rows::derive`, the one archive mode calls. What holds it
there is a test:
[`dz-recorder-e2e/tests/inline_vs_archive.rs`](dz-recorder-e2e/tests/inline_vs_archive.rs)
feeds one synthetic feed through both paths — recorded to a real archive and
derived with `derive_object`, and pushed through the ring and a window and
derived with `derive` — and asserts the row sets are equal but for `derivation`,
`object_key` and `object_sha256`. A ring drop is asserted at the row altitude
too: a gap the recorder itself caused is not given a `publisher` verdict.

```bash
cargo test -p dz-recorder-inline
cargo test -p dz-recorder-e2e --test inline_vs_archive
# the record-only build, which is the one that has to refuse a configuration
# selecting inline mode
cargo test -p dz-recorder --no-default-features
```

### Rows reach disk before the column store, on every window

Not only when the destination is unreachable, and the reasons are why the spool
is a directory with a byte budget rather than a buffer:

- **A recovery path that only runs during an incident is a recovery path nobody
  has tested.** If the disk were the exception, that code would first be
  exercised on the day it is most needed.
- **A crash otherwise loses what no archive can return.** The sink holds rows
  across windows deliberately, to keep merge pressure a function of rows per
  part. In archive mode that costs nothing — the objects are on disk and the
  next pass re-derives them. Inline, that memory is the only copy, and an
  out-of-memory kill, an uncaught panic or a host reboot takes it with nothing
  recording that it did. With rows on disk first, a crash costs the open window.
- **Windows on disk are what bring the ledger back, and idempotence with it.** A
  window whose insert was never acknowledged is replayed on the next pass,
  `ReplacingMergeTree` makes the replay a replace, and the ledger entry is
  written when the rows land and never when the sink accepts them. That is the
  loader's arrangement over rows instead of objects, reused rather than
  restated.

When the budget is full the oldest window is evicted and counted, and the
derivation is never blocked. A spool that applied backpressure would stall the
derivation, fill the ring behind it, overflow the receive queue and convert a
column-store outage into feed loss — the same inversion the staging budget
exists to prevent. Losing bounded history is recoverable; contaminating live
data is not.

### The one number to alert on

**The age of the oldest unposted window, and never the eviction counter.** A
full budget evicts on every window at steady state by design, so that counter
rises whether or not anything is wrong, while one window older than the eviction
horizon is history already gone. The gauge is `0` when the spool is empty rather
than absent, so a rule written over it does not silence itself on the healthy
case.

The loader's README makes this argument about object lag, and this is the same
rule over a different unit: see
[the gate on that arrangement](dz-recorder-load/README.md#the-gate-on-that-arrangement).
Inline mode publishes its own `dz_recorder_inline_*` family beside the health
tier's, in one exposition on the recorder's own metrics port; the two families
are disjoint and the health tier's series mean what they mean in archive mode.

### Which mode a host runs

Inline mode is what a host gets by saying nothing, and it exists for three
cases:

- **Bringing up a feed.** The question during bring-up is *are the rows right*,
  and answering it in archive mode takes a rotation, a pass and a query. Inline
  it takes a window.
- **A host that was never going to keep the bytes.** The staging budget is
  retention × bytes per second and it is the number that decides host sizing. A
  host that wants the rows is otherwise paying for a disk it has no use for.
- **One deploy unit.** One binary to pin, one configuration bundle, one metrics
  port, one service to stop and start.

**Archive mode is the answer for a host recording a production feed for
evidence, and that host states it by writing the two directories that
arrangement fills.** Which it has always done, so nothing about that host's
command line or unit file changes. What changed is that saying nothing at all no
longer gets an arrangement: it gets a startup refusal, rather than an empty table
nobody can tell apart from a feed nobody published on.

Configuration is two files, because the record path's own file gains no key:
`config_hash` is provenance written into every object and every coverage row, so
a column-store endpoint there would make rotating a password change what an
archive says produced it. `site` and `recorder` stay in the recorder's file and
are not repeated in inline mode's, so one host cannot appear in a dashboard as
two recorders that do not exist. See
[`dz-recorder/inline.example.toml`](dz-recorder/inline.example.toml) for the
keys and what each one bounds,
[`dz-recorder/systemd/dz-recorder-inline.service`](dz-recorder/systemd/dz-recorder-inline.service)
for the unit, and
[BRINGING-UP-A-FEED.md](../../BRINGING-UP-A-FEED.md#pointing-a-feed-at-a-mode)
for pointing a feed at either arrangement.

The mode needs a build carrying `inline`, which is a **default feature** because
the arrangement is a property of a host's configuration and the released asset is
one asset for the fleet: a default build carrying one arrangement would have to be
matched to configurations at deploy time, and its wrong answer is a startup
refusal on a host whose configuration was correct. `--no-default-features` is the
record-only build — no column-store client, no HTTP client, no row crates — and
it can only ever be in archive mode, so it refuses a configuration that selects
inline mode by the feature's name.

## The two capture modes

Both sit behind one `Source`, and both write the same archive format. The choice
is orthogonal to the two modes above: inline mode derives from the same
captures, and records the same distinction between a link header that was read
off the interface and one that was synthesised.

**`AF_PACKET` on the arrival interface is the default.** It records what the
network delivered, so the source address, destination, TTL and payload are
*captured*
rather than synthesised, and a datagram the recorder's own socket would have
lost to receive-queue overflow is still in the archive, correctly attributed.
The multicast socket is still opened and joined — the network has no reason to
deliver the traffic otherwise — and its receive path is drained and discarded.
Needs `CAP_NET_RAW`, and an Ethernet capture device: the parse reads a 14-byte
Ethernet header, so a handle on any other datalink is refused at open, naming
the datalink and the mode that does record on it. A device with no link layer of
its own is what the refusal exists for — an `ipip` tunnel and a `tun` device
both open on `DLT_RAW`, bare IP with no warning of any kind, and a cooked-mode
device opens on `LINUX_SLL` (`tcpdump -ni <device>` prints its `link-type`).
Without the refusal every frame fails the parse, nothing is archived, and the
recorder reports itself healthy against a live feed.

**Socket mode is the fallback**, for where `CAP_NET_RAW` is unavailable or the
capture device carries no Ethernet header the parse can read — a tunnel on bare
IP as much as a cooked-mode device, which is the case the refusal above exists
for — and it is the right mode when the question is about a consumer's own stack
rather than about the publisher. It synthesises the Ethernet, IPv4 and UDP
headers and records that fact in the archive, so no reader mistakes a
synthesised field for a captured one. A field the kernel did not report is
written as zero in the synthesised header, because an IPv4 header has no way to
express *absent*; the recorder's own knowledge of it stays unobserved rather
than becoming a zero somebody will later average.

## Build

Default features need no system package:

```bash
cargo test -p dz-recorder-core -p dz-recorder-archive -p dz-recorder-replay -p dz-recorder-capture
```

`AF_PACKET` mode is behind the `afpacket` feature and needs `libpcap-dev` at
build time (verified against 1.10.6):

```bash
sudo apt-get install -y libpcap-dev
cargo test -p dz-recorder-capture --features afpacket
```

That split is deliberate. Socket mode was built first precisely so that the
gate needs no extra package, and CI keeps the two apart for the same reason:
the default job installs nothing, and a second job covers the feature.

## A local run, with no network and no credentials

The synthetic publisher writes straight into the `Sink`, so the whole path —
publisher, pcapng writer, rotation, compression, manifest, replay — is
exercisable on one host with no socket at all. That is the round-trip contract,
and it is a test rather than a ritual:

```bash
cargo test -p dz-recorder-replay --test round_trip
cargo test -p dz-recorder-replay --test faults
```

`round_trip` records a thousand datagrams through the real writer and asserts
that replay yields the identical payloads, addresses, port roles, receive
timestamps to the nanosecond, stamp kinds and drop deltas. `faults` injects a
sequence gap, backward motion, a reset, a new source address, a duplicate, a
reordered pair, an oversized declared length, an unknown schema version and a
silent channel, and asserts each survives the round trip verbatim.

For the live capture path:

```bash
cargo test -p dz-recorder-capture --features loopback-tests --test socket_loopback
```

## Operating notes

**Alert on the delta, never on the total.** Overflow and interface-drop
counters are cumulative and are never reset, so a host carries the sum of every
burst it has ever had. A large total that has not moved in a day says nothing
about capture health now; a small one that is climbing says everything.

**Ring drops and interface drops are separate categories.** "Gap, no capture
drops, interface drops rising" is loss upstream of the capture point, and
folding it into publisher loss is how a switch problem becomes a publisher
finding.

**When staging fills, the oldest object is evicted and counted, and the capture
path is never blocked.** A writer that blocks on a full disk stalls the drain
thread, overflows the receive queue, and converts a storage outage into a
feed-loss incident plus false publisher-loss findings in every archive written
during it. Losing bounded history is recoverable; contaminating live data is
not. Size a recorder host for the archive, not for the receive path.

**A datagram we could not hand to the writer is admitted on the next one that
gets through.** Loss is carried as a debt: when the internal queue is full the
datagram is dropped, and its own loss plus the drops it was already declaring
ride on the next datagram that is accepted. Interface drops are never owed —
loss upstream of the capture point is not ours to admit.

**An over-cap datagram is archived truncated and declared honestly, never
discarded.** It is a publisher violation, and a violation recorded as a sequence
gap becomes publisher *loss* attributed to somebody else. The archive states the
on-wire length beside what it actually holds, so a reader sees both.

**A publication that cannot land retains its segment inside the staging budget
and says so.** The object is not lost silently: the partial is renamed under an
accounted name, the failure reaches `last_error()` and a counter, and eviction
can reach it — so an unwritable destination costs bounded history rather than
turning into feed loss.

**Nanosecond precision is verified at open, not assumed.** A handle that came up
at microsecond precision refuses to record, because a microsecond archive is
indistinguishable from a nanosecond one that happens to end in three zeros.

**Verify the manifest's sha256 before drawing a finding from an archive.**
Compressed objects carry a zstd frame checksum, which catches damage that would
otherwise decode to a *different* buffer with no error at all; the manifest hash
is what covers the rest.

## A live run is not a CI test

The live capture tests need `CAP_NET_RAW` and a host that delivers multicast to
itself. CI compiles them and runs them never — a test that can only run by hand
must not be able to fail the build. To run them, build the test binary
unprivileged and run only the binary as root, so no build artifact ends up
root-owned:

```bash
cargo test -p dz-recorder-capture --features afpacket-live-tests --no-run
sudo ./target/debug/deps/afpacket_mode-<hash> live:: --test-threads=1
```

## The rows, and where the loader runs

The analysis tier turns an archive into rows a dashboard can ask, without the
record path learning what a column store is. Two families, in one database and
joined on one identity block:

| | Tables | Grain | Migration |
|---|---|---|---|
| **Transport** | `datagram`, `era`, `segment_coverage`, `sequence_gap`, `conformance_finding` | the channel instance — what arrived, and what did not | `001` |
| **Market data** | `event`, `instrument`, `book_top` | the instrument — what the messages said | `005` |

The split is a key, not a category. A sequence number is meaningful only under
`(source address, Channel ID, destination port)` and a price is meaningful only
under an instrument within an era, and one sort key cannot be both — which is
why these are tables beside each other rather than columns added to the first
five. The derivation reads a `Source`, so it is exercised in CI against the
synthetic publisher with no socket, no privileges and no server, and the column
store is one implementation of a `RowSink` behind a trait.

**The loader runs on the recorder host**, against that host's own completed
directory, opened read-only. Nothing ships objects off a recorder host today,
and objects are evicted under the staging budget: the rows are tens of bytes
against a datagram's twelve hundred, so the small thing travels and the bytes
stay local. That is also what makes the cross-site join available *before* a
shipper exists, because the join is over rows and not over objects — not having
a shipper costs retention, and not the join.

**The gate on that arrangement is
`dz_loader_oldest_unloaded_age_seconds` against the eviction window.** A loader
slower than the write rate loses history permanently and silently, and no re-run
recovers an object that is gone. Alert on the age and not on the backlog count:
two hundred young objects is a busy loader, and one object older than the window
is history already gone. See
[`dz-recorder-load/README.md`](dz-recorder-load/README.md).

```bash
cargo test -p dz-recorder-rows            # the derivation, no server
cargo test -p dz-recorder-clickhouse      # batching, retry and the DDL, no server
cargo test -p dz-recorder-e2e --test archive_to_rows
```

## Decoding an archive, which is not the record path decoding one

`dz-recorder-relower` is where an archive becomes messages again. Nothing in the
record path decodes, and that stays true: this reads objects that are already
written, in a process that can be turned off, run late, or run twice.

`WireCapture` has two outputs and the distinction between them is the crate's
whole contract:

- **`messages()`** is what a comparison compares — `Quote`, `Trade`,
  `LevelUpdate`, `BookClear`. Four types, because those are the ones a venue
  event produces and therefore the ones a re-lowering can produce a counterpart
  for.
- **`state_messages()`** is what a *book* needs — `InstrumentReset` and the
  snapshot triple. Each is the publisher's own statement about its own book,
  lowered from no upstream payload, so a re-lowering has nothing to compare them
  against and excludes every one. A consumer building a book cannot do without
  them: a complete cycle is the only anchor a delta book has, and a reset is the
  only statement that what precedes it is not to be trusted.

There is a third, `reference_messages()`, carrying `InstrumentDefinition` and
`ManifestSummary` **with their positions**. `ArchivedRefdata` consumes the same
two and keeps a set rather than a history, which is right for a comparison that
holds two archives with no key ordering them; a consumer holding one archive can
place a restatement exactly, and needs the position in order to.

`Skipped` still counts the second and third groups, because that report is about
what the comparison did not compare and that has not changed. Provenance carries the
channel instance — source address, `Channel ID`, destination port — because a
sequence number is meaningless without it and two redundant publishers serving
one channel are told apart by nothing else.

## Market data as rows

The transport rows say how many datagrams were missing and whose they were.
None of them can say what the top of book was for an instrument at an instant,
because `datagram` records how large a message was and never what it said —
a deliberate property of the record path that had been allowed to become a
property of the rows. `dz-recorder-events` derives the answer from objects that
are already written, so that a feed becomes rows by being recorded rather than by
someone writing a capture for it. Designed in
[`2026-09-05-recorder-market-data-rows-design.md`](../../docs/superpowers/specs/2026-09-05-recorder-market-data-rows-design.md)
and planned in
[`2026-09-06-recorder-market-data-rows.md`](../../docs/superpowers/plans/2026-09-06-recorder-market-data-rows.md).

Three tables, declared by `005`. `instrument` is the archived reference data kept
as a history rather than a set, so a restatement of an exponent has a position and
the prices either side of it decode at different scales. `event` is one row per
decoded message, joined to the definition in force when it arrived. `book_top` is
one row per change in top of book — and per change in whether that top can be
believed.

**`book_certain` is the point of the book.** A live book that missed datagrams
applies the deltas that arrived and keeps quoting a top that has diverged from the
publisher's, and it cannot notice, because noticing needs the datagram it did not
receive. A derived book can: the gap is observable in the archive, so certainty
falls on a gap or an `InstrumentReset` and is restored only by each derivation's
own rule — a `Quote` is self-anchoring, a delta book anchors only on a complete
snapshot cycle. A certainty transition emits its own row, so a gap that moves no
price is still visible as one.

Two observation points recognise the same book state by `state_key`, a hash over
a stated tuple that excludes every timestamp — a timestamp is what the race
measures, so it cannot also be what identifies the state. `006` numbers the
occurrences of a repeating state and pairs them ordinal to ordinal, because a
state repeats and an `ASOF` join does not care: without the ordinal, several
occurrences at one point all pair with one at the other and the lead times that
come out are not measurements of anything. An occurrence with no counterpart stays
visible rather than being dropped.

**Derivation is per feed and off by the absence of a section**, not by a flag
whose default could be flipped, and it has its own backlog and lag gauges: the
datagram tier loads every object, so a shared series would page about book rows
for objects that hold none. Before a feed is turned on, `dz-recorder-events`'
sizing measurement states its messages-per-datagram multiplier over a window that
held a burst and a snapshot cycle — and says so plainly when the window held
neither, rather than reporting a true number about the wrong window.

```bash
cargo test -p dz-recorder-events          # the fold, the book, the key
cargo test -p dz-recorder-e2e --test archive_to_market_data
cargo run -p dz-recorder-events --example sizing -- \
  --feed market-by-price <object>.pcapng.zst   # the multiplier, before turning one on
```

## The venue half of a feed race

A feed race compares what a venue said with what a publisher sent. Everything
above is the publisher half of it. `dz-recorder-venue` is the other half:
archived upstream messages become rows by driving that venue's own `Adapter`
over them. Designed in
[`2026-09-09-recorder-venue-observation-design.md`](../../docs/superpowers/specs/2026-09-09-recorder-venue-observation-design.md)
and planned in
[`2026-09-09-recorder-venue-observation.md`](../../docs/superpowers/plans/2026-09-09-recorder-venue-observation.md).

**A venue's recorder is a binary the venue assembles, as its publisher is.**
`derive_venue_object` takes `&mut dyn Adapter`; no crate here links a venue and
none may.

**Three tiers, and none of them is a capture.** A capture is a receive path over
a socket that observes datagrams, counts what the handle dropped and records
link headers. A venue-side recording is none of the three, so it has its own
[archive shape](dz-recorder-archive/UPSTREAM-OBJECT-FORMAT.md) — length-delimited
upstream messages, each with the connection that delivered it and a receive
stamp — compressed, digested and keyed by the archive tier's own code rather
than a second copy of it, and rotated under the archive tier's own policy: the
segment writer accounts for the bytes it has written and states the window it
covers, and the venue's binary reads that count against the one policy. That document also states why this is not the
record encoding the offline re-lowering uses: that one carries normalized
events, which sit downstream of the venue's decode, and the evidence has to be
what the venue sent.

**A published object is opened under its own key.** `publish` compresses by
default and the suffix goes on the key, so `ArchivedVenueObject::open_published`
is what a caller reaches for and it decides from the name — one place decides
what `.zst` means, in the archive tier, for both archive shapes.
`ArchivedVenueObject::open` is the one that takes bytes already an upstream
object, for a caller that has decoded and for a test over a segment it wrote
itself.

**The derivation reads objects, not a socket**, and that is what restores every
guarantee a live input would have given up: `(object key, sha256)` idempotence,
the object as the batch boundary, and the bytes still being there to re-derive
with a corrected adapter. It also means this tier waits on neither of the two
transports a venue's receive path needs. An adapter that refuses a message costs
**that message** and is counted by the reason it gave: a derivation that stopped
at the first message a venue's own adapter could not parse would report the
venue's feed as having ended there, and those rows read exactly like a venue
that went quiet.

**The rows are their own two grains**, declared by `009`. `venue_book_top` is
one row per change in the top of book as an observer of the venue's own upstream
states it; `venue_object` is one row per object, carrying what was read, what was
refused and by which reason. Neither carries `channel_id`, `instrument_id`,
`sequence_number`, `reset_count`, `segment_seq`, `drop_delta` or an era — each is
a statement about a datagram on a channel instance, and a venue's upstream
message is not one. The absence is held against column-name literals in two
places, the row types and the DDL, because a column that exists reads as a
column somebody may fill.

**What identifies a venue-side row is the object, the record and the change
within it.** `message_index` says which archived record moved the top;
`change_index` says
which change in the top that record produced. Both, because a record is one
payload the adapter is handed and a payload may carry a batch — the sink contract
has `upstream_message` called once per member — while one member may move a top
more than once. Every such row carries the record's own receive stamp, because
that is the only stamp the transport took, so a key ending at the record is one
key for all of them and `ReplacingMergeTree` would keep whichever merged last.
The pair is in the sort key and in the occurrence window's ordering, which is
what makes the ordinal reproducible rather than the engine's choice among rows
that arrived at one stamp.

And `object_key` ahead of both, because the record index restarts at zero in
every object and so separates nothing across two of them. A rotation closes one
object and opens the next, and a clock coarser than the gap stamps records
either side of the boundary alike — so an object whose whole window fits inside
one tick puts its only record and the next object's first at one stamp under one
index, and the two book states collapse into one. The object is the only column
a venue-side row carries that tells them apart. It costs no idempotence: a
re-derivation reads the same object and so produces the same key, which is why
`object_sha256` stays out — two digests under one key are one window the archive
re-published, and the rows of the object that is there now replace the rows of
the one that was.

**Tells them apart, and not which of them came first.** A key ends in the name
the archive mints, `<start_ns>-<end_ns>-<segment_seq>`, and the sequence is
written without padding — so two objects that share a window, which is what a
rotation inside one clock tick produces, collate segment 10 ahead of segment 9.
The occurrence window asks the key for uniqueness and not for order:
`(object_key, message_index, change_index)` is unique for every row, so the
numbering is total and answers the same on two runs and either side of a merge,
and the tie-break only ever separates rows that already agree on `recv_ts` —
the quantity `lead_ms` is measured from. The race therefore pairs the same way
and reports the same lead whichever of the two is numbered first, which is what
the container suite asserts and why the objects' own order is not worth a column
this table would have to be given.

**The race is a view keyed on `book_key`**, the hash over the two sides of a top
and nothing else, computed by `dz_recorder_events::book_key` and never by a copy
of it. Not `state_key`: that one folds the `Channel ID` and the `Instrument ID`
in before it folds a price, and a venue side can compute neither — the channel
is the operator's mapping and the identifier is minted by the publisher's
registry. Keyed on it the race would return zero pairs and read as each side
missing every state the other saw. `009` numbers the occurrences per observation
point and pairs ordinal to ordinal, exactly as `006` does, and carries
`symbols_agree` and `exponents_agree` as columns rather than assumptions.

**What a venue-side observation cannot say.** It cannot report loss: it has no
sequence space of its own that this repository defines, so a state the venue
produced and nobody recorded is invisible on that side. It has no
`book_certain`, because certainty on the publisher side means a gap in the
publisher's own sequence and on the venue side would mean the venue's own
resynchronisation — one column, two meanings, and a `min()` over a pair mixes
them. And it claims nothing about attribution: whose fault a missing state is
stays the loss derivation's question and the cross-site views'.

```bash
cargo test -p dz-recorder-venue                      # the derivation and the grains
cargo test -p dz-recorder-archive --test upstream_format
```

## Not here yet

The conformance runner over replay. `conformance_finding` exists as the table a
runner fills, and nothing writes a row into it — an empty table is the honest
statement that nothing judged the object, where a `pass` row would be a pass over
a rule that never ran.

The cross-site pass that turns `unverifiable` into `publisher`. That verdict
needs a datagram absent from *every* site with no recorder overflow anywhere,
and one vantage cannot say it: a gap row lands with `seen_elsewhere` as `NULL`
and `unverifiable` as the verdict until the join has run.

Any repoint of an existing dashboard. Rows have to be proven equivalent to what
a panel already shows before anything is switched over.

Any shipper. The loader is deliberately arranged so that not having one costs
retention and not the join.

A venue-side receive path. A venue-side observation needs an `Input`, and the
two transports a venue would use for one — a session transport and a polled one
— are each declared and unbuilt. `dz-recorder-venue` takes archived objects as
its starting point precisely so that it does not depend on either, and a venue
that wants to produce those objects today writes them with the archive's own
upstream segment writer.

The publisher side as a contributor to the venue race. `book_top` carries `state_key`
and no `book_key`, and the two ways to bridge that are a new column on
`book_top` and a second fold written in SQL — the first is a change to a
publisher-side grain and the second is the copy `book_key` exists to remove.
`009`'s pairing names no observation point, so the publisher side enters it by
contributing rows rather than by that view learning its name.

One check is deliberately deferred: the archive is an interface between two
languages, and the golden-vector check that a pcapng segment written here reads
identically from Go belongs with the Go reader that will consume it. That reader
is not in this repository yet. Until it lands, the format is checked against
independent C implementations instead — `capinfos` for the section metadata and
nanosecond resolution, `tshark` for the datagrams and their drop counts.
