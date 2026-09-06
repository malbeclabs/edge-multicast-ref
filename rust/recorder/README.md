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
else connects them: no socket, no queue, no shared memory.

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
| `dz-recorder-archive` | The pcapng writer: rotation, compression, hashing, the manifest, and the staging watermark |
| `dz-recorder-replay` | An archive read back as a `Source`, plus the synthetic publisher the tests are built on |
| `dz-recorder-loss` | Which sequence values nobody delivered, per channel instance and per era, and whose they are |
| `dz-recorder-relower` | An archive read back as decoded messages, and re-run against a venue's own mapping: *did the publisher publish what the venue said?* |
| `dz-recorder-health` | Whether a recorder is recording, as the process itself can tell |
| `dz-recorder-rows` | The rows an archive derives into, and the derivation: pure, sink-agnostic, and exercised with no server |
| `dz-recorder-events` | Market data rows: reference data scoped to an era, and the fold that joins the messages to it |
| `dz-recorder-clickhouse` | The column store as one `RowSink`, plus the checked-in DDL |
| `dz-recorder-load` | The loader binary ([README](dz-recorder-load/README.md)) |
| `dz-recorder-e2e` | The tests that use the real encoder, the real writer and the real reader end to end |

Take what you need. A publisher wanting a byte-exact record of its own egress
takes `-archive` alone; a test harness takes `-replay` alone; a host that only
needs alerting takes `-capture` and writes nothing. Nothing above
`dz-edge-core` is required in order to record, and **nothing in the record path
decodes a datagram** — a message a decoder rejects is a message the archive
never holds, and the evidence needed to diagnose that bug is what the bug
destroyed.

## The two capture modes

Both sit behind one `Source`, and both write the same archive format.

**`AF_PACKET` on the arrival interface is the default.** It records what the
network delivered, so the source, destination, TTL and payload are *captured*
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

## Not here yet

The conformance runner over replay. `conformance_finding` exists as the table a
runner fills, and nothing writes a row into it — an empty table is the honest
statement that nothing judged the object, where a `pass` row would be a pass over
a rule that never ran.

**The market data derivation, though its tables now exist.** Designed in
[`2026-09-05-recorder-market-data-rows-design.md`](../../docs/superpowers/specs/2026-09-05-recorder-market-data-rows-design.md)
and planned in
[`2026-09-06-recorder-market-data-rows.md`](../../docs/superpowers/plans/2026-09-06-recorder-market-data-rows.md).
Four of the nine tasks are in: provenance carries the channel instance, the walk
surfaces the four state messages, `dz-recorder-events` holds era-scoped reference
data, and `005` declares `event`, `instrument` and `book_top` with the row types
that fill them.

Five of the nine tasks are in, and `event` and `instrument` are now written:
`dz-recorder-events`' fold walks an object, merges the walk's three outputs into
archive order, joins each message to the reference data in force at its arrival,
and refuses what it cannot attribute rather than filling it in.

**`book_top` is still empty**, and will be until task 6. It needs state that spans
objects — a book — and an empty table here means what it means for
`conformance_finding`: that nothing derived it, where an invented row would be a
book state nothing observed.

The cross-site pass that turns `unverifiable` into `publisher`. That verdict
needs a datagram absent from *every* site with no recorder overflow anywhere,
and one vantage cannot say it: a gap row lands with `seen_elsewhere` as `NULL`
and `unverifiable` as the verdict until the join has run.

Any repoint of an existing dashboard. Rows have to be proven equivalent to what
a panel already shows before anything is switched over.

Any shipper. The loader is deliberately arranged so that not having one costs
retention and not the join.

One check is deliberately deferred: the archive is an interface between two
languages, and the golden-vector check that a pcapng segment written here reads
identically from Go belongs with the Go reader that will consume it. That reader
is not in this repository yet. Until it lands, the format is checked against
independent C implementations instead — `capinfos` for the section metadata and
nanosecond resolution, `tshark` for the datagrams and their drop counts.
