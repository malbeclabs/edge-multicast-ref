# A generic recorder for the DoubleZero Edge feed family

**Status:** draft, pending review
**Date:** 2026-08-28
**Applies to:** this repository, and any host that receives an edge feed
**Authority:** [`edge-feed-spec`](https://github.com/malbeclabs/edge-feed-spec), its [`GLOSSARY.md`](https://github.com/malbeclabs/edge-feed-spec/blob/main/GLOSSARY.md) and [`VERSIONING.md`](https://github.com/malbeclabs/edge-feed-spec/blob/main/VERSIONING.md); `2026-08-26-edge-publisher-crates-design.md`, whose layer diagram this extends downward

---

## Naming

This repository is public. This document names no venue, venue repository,
venue crate, config key, metric prefix or issue tracker, and gives no count of
publishers or of recorder sites.

It also states only what is to be built. Where a decision below came from
operating something, the decision and its reason are given and the system is
not: a design is read for the thing it specifies, and a description of what
happens to be running would age into a false one.

`GLOSSARY.md` governs all vocabulary and overrides any local definition:
`datagram` never `frame`, `port role` with the tokens
`mktdata`/`refdata`/`snapshot`, `channel` only for the `Channel ID` shard,
`era` never `epoch`.

---

## Purpose

We want to answer, for any edge feed, at any site, at any time in the past:
**did the publisher send what the spec says it must, and did it arrive?**

Nothing answers that question after the fact unless something kept the bytes.
A live validator can say the last few minutes were clean and then forget them.
A decoder that writes rows can only answer the questions its author thought of,
and only in the interpretation it happened to hold at the time. But the
question is asked in the past tense — usually hours after an incident,
sometimes about a rule that did not exist when the traffic passed — and the
only artefact that answers it in that tense is the traffic itself.

So this design specifies a **recorder**: agnostic to the feed it records,
agnostic to the venue behind it, and agnostic to whether it is reading a socket
or a file. It ships as a library, so that several recorder hosts, in several
places, can be built on it without any of them re-deciding what to keep.

It is deliberately not a venue comparison. The recorder has no venue client and
no credentials, and never compares a multicast feed against a venue's own
service. What a venue-agnostic recorder *can* compare is a feed against
*itself as seen from somewhere else* — and for the question above, that
comparison, specified below, is the stronger one.

---

## The first question, settled first

> Should the recorder be a process that parses, or should it write a pcap to
> object storage and let a second process parse and analyse?

**Both, split by responsibility — but not along the line the question draws.**
The split is not "parse now" versus "parse later". It is:

| Tier | Runs | Knows about the feed | Failure of this tier costs |
|---|---|---|---|
| **Record** | on the recorder host, in the receive path | nothing — bytes only | the data, permanently |
| **Health** | on the recorder host, beside the receive path | the 24-byte datagram header only | an alert |
| **Analysis** | wherever, later, repeatedly | everything, per feed flavour | a re-run |

Three findings drive that shape.

**A decoder in the record path is a correctness risk, not a feature.** Every
message the decoder rejects is a message the archive never holds. A schema the
recorder was built before is a schema it silently drops. The bug class this
creates is the worst one available to a recorder: the evidence needed to
diagnose the bug is what the bug destroyed. A recorder that writes bytes has no
such class, and gains a property no parsing recorder can have — it records
feeds it does not understand, including the ones defined after it ships.

**A decoder in the receive path is also a loss risk.** Parsing costs CPU on the
thread draining the socket. When it falls behind, the kernel's receive queue
overflows and datagrams are dropped *by us*. Those losses are indistinguishable,
downstream, from a publisher that skipped a sequence number — so the tool built
to measure publisher loss becomes a generator of fake publisher loss under
exactly the load where the measurement matters. Section *Attributing loss*
below turns this from a hazard into a recorded fact, but the first defence is
to keep the receive path doing almost nothing.

**A pure archive-and-forget recorder is undeployable.** If the only output is
objects in a bucket, then a recorder whose socket died, whose interface flapped,
whose disk filled or whose group membership lapsed looks exactly like a quiet
feed until somebody reads the bucket. Feed health is a minutes-scale question
and an archive is an hours-scale answer. So a thin online tier is not a
convenience, it is what makes the thing operable.

The health tier is affordable precisely because it is feed-agnostic. Every feed
in the family shares the 24-byte datagram header, which is where the sequence
number, the channel, the reset count and the send timestamp live. Continuity,
reordering, duplication, reset accounting, send→receive latency, heartbeat
cadence and the size cap are all decidable from that header alone, with no
per-feed crate linked in and no per-instrument state. Everything that needs a
message walk, an instrument's own sequence, a book, or reference data is
offline work.

So: **`pcap` to object storage is the right answer for the archive, and a
parsing process is the right answer for health — and they are the same
process, sharing one socket, not two competing designs.** The heavy parse
belongs in a third place that can be re-run, and being re-runnable is the whole
point: the analysis is the part that will be wrong first and improved most.

---

## What this builds on, and what it must not become

**The codec crates in this repository** give the health tier the header it
needs — `DatagramHeader::decode`, `PortRole`, `MAX_DATAGRAM_SIZE`, the schema
version set — with no I/O, no async and no per-feed dependency. The health tier
links `dz-edge-core` and nothing else, and nothing above `dz-edge-core` is
required in order to record.

**The conformance rule set is the analysis tier**, not something the recorder
reimplements. It validates a feed against the spec in two tiers, with
verifiability gating so packet loss reports as `unverifiable` rather than as a
violation, and it keys its state on the **channel instance** — `(source
address, Channel ID, destination port)` — which is the only correct key when an
operator runs redundant publishers. The recorder does not become it and does
not modify it: the recorder *feeds* it. This design takes its `Source`
boundary and its instance key, in Rust, and adds the two things a validator
cannot supply for itself — an archive it can be re-run over, and a record of
the recorder's own losses, so that a gap in that archive is attributable rather
than charged to the publisher by default.

So what has to be written is the archive's *format and metadata* — pcapng,
rotation, backpressure, object layout, and the manifest that makes the archive
queryable without opening it — plus the health tier that makes a recorder
operable at all, and the analysis tier that turns replayed bytes into rows.

A classic `pcap` rotated to a bucket is the thing this must not be mistaken
for. Such an archive reports no loss of its own, so a gap in it cannot be
attributed; it has no slot for the recorder's identity, its build, its
configuration or its drop counts, so an object separated from its context
cannot say what wrote it; and with no manifest, coverage cannot be answered
without opening objects, while a port that was never joined reads exactly like
a clean feed. Keeping bytes is the easy half. The metadata is what makes the
bytes evidence.

---

## Architecture

```
                    ┌──────────────── recorder host ────────────────┐
                    │                                               │
 multicast group ──►│  socket: IGMP membership only, no data path   │
   (mktdata,        │                                               │
    refdata,        │  AF_PACKET on the arrival interface           │
    snapshot)       │       │  kernel ts, PACKET_STATISTICS         │
                    │       ▼                                       │
                    │    Source ──┬──► Observer (health,            │──► /metrics
                    │             │    header-only)                 │   dz_recorder_*
                    │             │                                 │
                    │             └──► Sink (pcapng writer)         │
                    │                       │                       │
                    │            staging → rotate, hash, manifest   │
                    │                       │                       │
                    └───────────────────────┼───────────────────────┘
                                            ▼
                                     completed dir
                                            │  external shipper
                                            ▼
                                     object storage
                                   (pcapng.zst + manifest)
                                            │
                    ┌───────────────────────┼───────────────────────┐
                    │  analysis             ▼                       │
                    │   replay ──► Source ──► conformance rules     │──► findings
                    │                     └─► per-datagram rows     │──► column store
                    │                     └─► per-message rows      │──► cross-site join
                    └───────────────────────────────────────────────┘
```

The load-bearing detail is that **`Source` is the same trait in both halves.**
A live capture and a replayed archive present identically, so the analysis tier
runs unchanged over live traffic during development, the health tier runs
unchanged over an archive in a test, and a recorder is testable end-to-end with
no network. The same boundary is drawn on the Go side, and the archive format
is chosen so that both languages sit on it.

### Crates

```
rust/recorder/
  dz-recorder-core     Source and Sink traits, RecordedDatagram, the run loop,
                       recorder identity, config
  dz-recorder-capture  live Source: membership, AF_PACKET or socket, kernel
                       receive timestamps, drop accounting, rejoin, admission
  dz-recorder-archive  pcapng writer, rotation, staging, compression, manifest,
                       watermark eviction
  dz-recorder-replay   archive Source: read an archive back, local or remote
  dz-recorder-health   the header-only Observer and the dz_recorder_* metrics
  dz-recorder          the binary that wires them, for hosts that want one
```

Six small crates rather than one, for the reason the publisher design gives:
a consumer takes what it needs. A publisher wanting a byte-exact record of its
own egress takes `-archive` alone. A test harness takes `-replay` alone. A host
that only needs alerting takes `-capture` and `-health` and writes nothing.
Nothing above `dz-edge-core` is required to record.

**There is no upload crate.** The recorder rotates, hashes, writes the manifest
and evicts under its watermark — all of which it must do anyway, because it
owns the staging directory — and then leaves the finished segment in a
completed directory. Moving a directory of immutable, hashed files into object
storage is a solved problem with correct implementations already available,
and a crate here would be a worse one. If a shipper
turns out not to honour the manifest, that is when to write it.

### The core types

```rust
/// One received datagram and everything known about its arrival.
pub struct RecordedDatagram<'a> {
    pub payload:      &'a [u8],
    pub src:          SocketAddrV4,   // channel-instance identity
    pub dst:          SocketAddrV4,   // group and port
    pub role:         PortRole,       // dz-edge-core
    pub recv_ts_ns:   u64,
    pub recv_ts_kind: RecvTsKind,     // kernel vs application fallback
    pub drop_delta:   u32,            // datagrams the kernel dropped before this one
}

pub trait Source {
    /// Blocking for a live source, EOF-terminated for an archive.
    fn next(&mut self) -> Result<Option<RecordedDatagram<'_>>, SourceError>;
}

pub trait Sink {
    fn write(&mut self, dg: &RecordedDatagram<'_>) -> Result<(), SinkError>;
    fn rotate(&mut self) -> Result<Option<CompletedSegment>, SinkError>;
    fn flush(&mut self) -> Result<(), SinkError>;
}

pub trait Observer {
    fn on_datagram(&mut self, dg: &RecordedDatagram<'_>);
}
```

`recv_ts_kind` is carried rather than assumed because a stamp the kernel did
not produce must not be mistaken for one it did — a latency number computed
from an application-level fallback is measuring the recorder's scheduler, and
an archive that cannot say which it holds cannot be trusted for latency at all.

`drop_delta` is the subject of the next section.

---

## Attributing loss

A sequence gap in an archive has three possible authors: the publisher, the
network, or the recorder. Only the third can be measured directly, and if it is
not measured the other two inherit the blame.

**Both capture modes report their own losses**, and the recorder carries the
delta since the previous datagram into `RecordedDatagram::drop_delta` and, as
described below, into the archive itself. In `AF_PACKET` mode the source is
`PACKET_STATISTICS`, whose `tp_drops` counts frames the kernel could not fit in
the ring buffer. In socket mode it is `SO_RXQ_OVFL`, reported in a control
message on each `recvmsg` as a running count of datagrams dropped because the
receive queue was full. Offline, a gap in a channel instance's sequence is
subtracted against the recorder's own admitted losses before it is reported as
network or publisher loss.

Three consequences worth stating:

- **A gap covered by our own overflow is not a finding.** It is recorded, and
  it is counted, and it does not page anyone.
- **A gap not covered by our own overflow is a real finding**, and now a much
  stronger one, because the obvious alternative explanation has been excluded
  by evidence rather than by assumption.
- **A recorder that is dropping is itself an alert**, on the health tier,
  independent of any feed conclusion. `dz_recorder_capture_drops_total` rising
  means the archive is becoming less trustworthy, which is a fact you want
  before you rely on it, not after.

Both counters are per capture handle and both wrap, so the delta arithmetic
must be wrapping, and the first datagram on a handle establishes the baseline
rather than reporting the whole counter as a loss.

There is a fourth author of gaps that this cannot see: loss upstream of the
capture point, in the switch or on the link. `dz_recorder_*` therefore also scrapes
the interface's own counters where available, and the analysis tier treats
"gap, no capture drops, interface drops rising" as its own category rather
than folding it into publisher loss.

---

## The archive format: pcapng

The archive is **pcapng**, one Enhanced Packet Block per datagram, compressed
per segment.

It is tempting to invent a framing here, because the recorder is capturing at
the socket and therefore already holds the fields a custom record would carry.
Three arguments defeat that.

**The tooling exists and is the tooling people already use.** Wireshark opens
the archive, and the dissectors that decode the feeds inside it live in this
repository. `tcpdump`, `tshark`, `editcap`, `mergecap` and `capinfos` all work
on it, and a capture file is an input the conformance tool already takes. A
custom format means writing every one of those again.

**pcapng has a slot for exactly the metadata a recorder must not lose.** This
is the argument that actually decides it:

| pcapng element | What the recorder puts there |
|---|---|
| Section Header options (`shb_hardware`, `shb_os`, `shb_userappl`, `opt_comment`) | recorder identity, site, build version and commit, config hash |
| Interface Description block, one per port role | group, port, port role, the interface joined, the source address at join time |
| Enhanced Packet Block option `epb_dropcount` | the capture handle's drop delta before this datagram |
| Interface Statistics blocks, written at each rotation | received, dropped by interface, dropped by OS, for the segment |

`epb_dropcount` is defined as the number of packets lost between this packet
and the preceding one — it is the same quantity both capture modes report, so the
attribution described above travels *inside* the archive rather than beside it.
An archive that has been copied, renamed, or pulled out of the bucket by hand
still knows what the recorder dropped while writing it. A sidecar file does
not survive any of those.

**A self-describing archive cannot be separated from its provenance.** The
alternative — classic `pcap` plus a JSON manifest — is the cheaper thing to
reach for, and it is rejected on that separation: two files that must travel
together will one day not, and the one that goes missing is the small one that
says which recorder, which config and how many drops. The Go reader moves to
pcapng in the same library; it is a contained change and it buys that side the
same block metadata.

### Where the bytes are captured

The recorder joins the group with a socket — it must, or the network has no
reason to deliver the traffic — and then reads the data from **`AF_PACKET` on
the interface the feed arrives on**, not from that socket. The socket exists
for the IGMP membership and for nothing else; its receive path is drained and
discarded.

That is a reversal of the obvious design, and the reason is what the two
choices actually record.

A **socket** capture records *what one subscriber's socket saw*. It is faithful,
but it requires synthesising the IPv4 and UDP headers back around the payload,
and it silently omits every datagram the socket itself dropped — which is
exactly the population an archive most needs to be honest about.

An **`AF_PACKET`** capture records *what the network delivered*. The feed
arrives on a GRE tunnel interface already de-encapsulated, so the frames on that
interface are the clean inner IP/UDP a subscriber would receive: nothing is
synthesised and nothing is guessed. The TTL, the fragmentation and the
duplicate-delivery evidence a socket discards are all present, and the ring
buffer's own drop counter fills `epb_dropcount` exactly as the socket counter
would.

For validating a **publisher** — this design's stated goal — the network-level
view is the correct one, and it is strictly more informative: a datagram the
recorder's own socket would have lost to receive-queue overflow is still in the
archive, correctly attributed.

**Socket mode remains, as the fallback**, behind the same `Source` trait and
writing the same archive format. It needs no `CAP_NET_RAW`, which is the case
where `AF_PACKET` is simply unavailable, and it is the right mode when the
question is about a consumer's own stack rather than about the publisher. It
synthesises the headers as described above, taking what it can from
`IP_RECVTTL` and `IP_PKTINFO`, and recording the fact in the archive so that no
reader mistakes a synthesised field for a captured one. A synthesised header
field the kernel did not report is written as zero, because an IPv4 header has
no way to express *absent* — which is a statement about the bytes in the
archive and not about the recorder's own knowledge, where an unobserved value
stays unobserved rather than becoming a zero somebody will later average.

### Capture frameworks considered

An accelerated capture framework — PF_RING and its zero-copy variant, or any
comparable kernel-bypass stack — is deliberately not adopted for
`dz-recorder-capture`. The measured load is what decides that, so the numbers
are recorded here rather than left as a preference.

The load a recorder is built to absorb, measured on a live edge feed for two
host classes:

| | cloud instance | bare metal |
|---|---|---|
| sustained | 51,500 datagrams/s | 26,200 datagrams/s |
| peak over 24h | 71,000 datagrams/s | 40,800 datagrams/s |
| mean datagram | 195 bytes | 555 bytes |
| on the arrival interface | ~80 Mbit/s | ~116 Mbit/s |
| CPU | 43% of 8 vCPU | 8.8% of 32 cores |

Four things follow, in descending order of how much they settle the question.

**The rate is two orders of magnitude below where the framework starts to
matter.** Zero-copy capture is built for 10–100 Gbit/s, which is 1–14 million
packets per second. The busiest class above peaks at 71,000. Even the slowest
receive path available — one `recvmsg` per datagram, no ring at all — has some
14 microseconds of budget per datagram at that peak, and the record path spends
it on a copy and a buffered write, with no decode at all. `AF_PACKET` with a
`TPACKET_v3` ring absorbs the same load with a much larger margin, which is why
it is the default mode rather than the accelerated one.

**A drop counter is evidence only as a rate.** Overflow and interface-drop
counters are cumulative and are never reset, so a host carries the sum of every
burst it has ever had. A large total that has not moved in a day says nothing
about capture health now; a small one that is climbing says everything. So the
health tier alerts on the delta and never on the total, and a decision to buy
capture headroom has to be justified by a counter rising under load rather than
by a number that looks big. This is the misattribution *Attributing loss* above
exists to prevent, pointed at ourselves.

**Zero copy is inapplicable at the capture point this design chose, and bare
metal does not fix it.** Its drivers cover specific physical adapter families;
they do not bind to a GRE tunnel interface, and the feed arrives on one. On a
cloud instance the adapter is not a supported family either, so the blocker is
doubled — but removing that half by moving to bare metal leaves the structural
half untouched, because bare metal changes the host, not the capture point.
Bare metal also *lowers* the pressure rather than raising it: fewer datagrams
per second and roughly ten times the CPU headroom.

**The option costs nothing to defer.** The framework ships a `libpcap` shim,
and `AF_PACKET` mode goes through `libpcap` already. Adopting it later is a
link-time and deployment change; the `Source` implementation is untouched. Set
against that, a kernel module maintained out of tree is a real cost landing on
exactly the failure this design exists to prevent — a rebuild that fails after
a kernel upgrade is a recorder that stops capturing and looks like a quiet
feed.

Three conditions must hold together before revisiting: sustained load above
500,000 datagrams/s or 1 Gbit/s on one capture point; a non-zero
`dz_recorder_capture_drops_total` at that load *after* the ring has been
enlarged; and a capture point on a physical adapter rather than on a tunnel
interface. The third is the one that does not arrive on its own — it means
capturing the encapsulated outer frames on the physical adapter and
de-encapsulating offline, which is a different design and would need its own.
Bare metal is necessary for that and not sufficient.

Cheaper levers come first in every case: the socket receive buffer and the
ring size, the drain thread's placement, and the instance's core count.

The standalone commercial recorders in the same product family were also
considered. They solve the easy half — bytes to disk at line rate — and none
of the half this design is for: they cannot be linked as a library, they emit
either classic `pcap` or a proprietary compressed variant that forfeits the
"tooling people already use" argument that chose the format, and they carry
neither the header-only health tier nor a manifest keyed on the channel
instance.

### Compression

Segments are compressed after rotation, on a separate thread, never in the
receive path. `zstd` is the default: the payloads are dense fixed-size binary
structures with high inter-record redundancy and compress several-fold. Recent
Wireshark reads `zstd` directly; `gzip` is available for environments that need
the older guarantee, at a worse ratio.

---

## Rotation, staging, and the rule that the capture path never blocks

The writer holds one open segment per recorder, rotating on **size or age,
whichever comes first** — a size bound keeps objects uniform for the analysis
tier, and an age bound keeps a low-volume feed's data from sitting on a local
disk for hours. On rotation the segment is fsynced, closed, compressed, hashed,
and moved to the completed directory the shipper watches; the manifest is
computed from state the writer already holds, not by re-reading the file.

The staging directory is the buffer for an object-storage outage, and its size
is a deliberate decision: `retention_minutes × measured_bytes_per_second`.

**When staging fills, the oldest segment is deleted and counted. The capture
path is never blocked.** This is the single most important operational rule in
the design, and it is the opposite of what a naive implementation does. A
writer that blocks on a full disk stalls the drain thread, which overflows the
receive queue, which loses live data — so an object-storage outage, or a
credential expiry, or a slow disk, is converted into a feed-loss incident and
into false publisher-loss findings in every archive written during it. Deleting
the oldest segment loses *history*, which is recoverable in every sense that
matters: it is bounded, it is counted in
`dz_recorder_segments_evicted_total`, it is alertable, and it does not
contaminate the data still being written.

The same rule governs the health tier: an `Observer` that falls behind is not
allowed to slow the loop. Observers run on the drain thread and must be
allocation-free per datagram — which the header-only tier is — or they run
behind a bounded channel that drops and counts.

### Host sizing: the staging buffer decides it, not the capture

`staging_max` is the outage buffer, so a recorder host is chosen by how long
that buffer lasts. At the rates above it is the only dimension where the choice
is not comfortable on its own.

Taking those rates, adding the 42 bytes of Ethernet, IPv4 and UDP headers and
the 32-byte Enhanced Packet Block per datagram, and assuming the 4× ratio the
*Cost* section uses — an assumption the recorder replaces with a measurement on
day one:

| | archive per day | local capacity for an 8-hour outage | for a 5-day outage |
|---|---|---|---|
| at the cloud-instance rates | ~300 GB | ~100 GB | ~1.5 TB |
| at the bare-metal rates | ~332 GB | ~111 GB | ~1.7 TB |

The two classes differ by a few percent in daily volume, and that is the whole
point: the higher-rate class is not the larger archive, the compute on both is
idle relative to the load, and nothing on the capture side distinguishes them.
What distinguishes a host is whether it has one to two terabytes to give
`staging_dir`.

Eight hours of buffer means the eviction rule fires during any half-day
incident in object storage, in a credential expiry, or in a shipper that stops
draining — and eviction is permanent loss of history, counted and alertable but
not recoverable. Five days means it fires almost never. Since evicting is
correct behaviour and blocking is not, the size of the buffer is the only lever
that decides whether the correct behaviour is ever needed.

So: **size a recorder host for the archive, not for the receive path.** Local
capacity for `staging_dir` and `completed_dir` is the binding constraint;
cores and adapter are not, and a decision that optimises them is optimising
the dimension that already had margin.

---

## Object layout and the manifest

Keys are Hive-partitioned so an object store can be queried as a table without
a separate catalogue:

```
s3://<bucket>/feed=<spec>/env=<env>/site=<site>/recorder=<id>/
    date=<YYYY-MM-DD>/hour=<HH>/<start_ns>-<end_ns>-<segment_seq>.pcapng.zst
```

`feed` is the spec name, not a venue. `site` and `recorder` are what make the
cross-site comparison below a partition prune rather than a full scan.
`segment_seq` is monotonic per recorder run and is what makes a *missing*
segment detectable: a gap in the sequence of objects is a gap in the archive,
and without it a recorder that was down for an hour is indistinguishable from a
feed that was quiet for an hour.

Each object carries a manifest, both as object metadata and as a row in an
index table:

| Field | Why |
|---|---|
| recorder id, site, env, build version and commit, config hash | provenance; a finding is attributable to a build |
| segment sequence, start and end receive timestamp | ordering and gap detection across objects |
| datagram count, byte count, sha256 | integrity, and idempotent reprocessing |
| per channel instance: first and last sequence number, count, reset count seen | lets the analysis tier plan, and lets a coverage question be answered without opening a single object |
| capture drop total, interface drop total | archive trustworthiness, visible before use |
| port roles and groups joined | tells a reader what the archive is *supposed* to contain, which is how an unbound port is detected |

That last row deserves emphasis, and the lesson behind it is worth stating
plainly: **a port that was never joined produces no data, and no data looks
exactly like a clean feed.** An archive that does not record which
ports the recorder was configured to join cannot distinguish "the snapshot port
was silent" from "nobody asked for the snapshot port", and the analysis tier
will report a pass over rules it never ran. The manifest states the intent, so
the analysis tier can report `na` rather than silence.

Reprocessing is keyed on `(object key, sha256)`, so the analysis tier is
idempotent and a re-run after an analyser fix replaces rather than duplicates.

---

## The health tier

Runs in the recorder process. Links `dz-edge-core` only. Keys everything on the
**channel instance** — `(source address, Channel ID, destination port)` — for
the reason that governs every instance-keyed tracker: an operator may run two
publishers
serving the same channel to the same group and port, each advancing its own
sequence space and its own `Reset Count`, and a tracker keyed any less finely
reads every alternation as backward motion in one direction and lets one
publisher's heartbeats cover the other's total outage in the other.

A source address not seen before opens a new series **silently**: no gap, no
loss, no alert. A tunnel address is a lease, it can be reassigned under a live
host, and a reassignment must not page. The instance map is bounded with
least-recently-seen eviction, because an any-source join accepts datagrams from
any sender and the key space is therefore not ours to trust.

What it checks, all from the 24-byte header:

- sequence continuity per channel instance: gaps, reordering, duplicates
- `Reset Count` transitions, and backward sequence motion that is not a reset
- `send_timestamp_ns` → `recv_ts_ns`, as a histogram, per port role
- heartbeat cadence and channel silence, per instance — with one inference
  stated rather than assumed: the header does not identify a message type, so a
  heartbeat is recognised by the only shape the header decides, a single message
  in a datagram of exactly the heartbeat's length. No other message in the
  family is that size today, so it is exact; if one is ever defined, the failure
  is a wider cadence histogram rather than a silence that goes unseen. The exact
  answer belongs to the opt-in message-walk mode below, which is where a message
  type can actually be read
- declared datagram length against the 1232-byte cap, and against the received length
- magic and schema version, counted by value rather than judged
- its own losses: capture drops, interface drops, rejoins, segments evicted

Metrics are `dz_recorder_*`, mirroring the `dz_publisher_*` decision in the
`2026-08-26` design and for the same reason: the names are normative, the
crates that own the hot paths record them internally, and a recorder emits them
whether or not anyone thought about it. The identity labels are `site`,
`recorder`, `feed`, `channel`, `port_role`, `source` — never a venue — and a
family may carry a taxonomy dimension beside them (`kind`, `reason`, and the
by-value `magic` and `schema_version`), the way the publisher set does.

`port_role` and not `role`, because that is the glossary's term and because
`dz_publisher_*` already spells it that way: a dashboard joining a publisher
series to a recorder series over the same feed cannot join on a dimension the
two sides name differently.

**What the health tier deliberately does not do:** walk the messages inside a
datagram, track a per-instrument sequence, build a book, or resolve reference
data. An opt-in message-walk mode exists for the structural checks that are
decidable from one intact datagram — message count against the walk, per-type
lengths, port placement — because those are loss-immune and cheap. It is off by
default. Anything stateful per instrument is offline, permanently.

---

## Why no venue comparison, and what takes its place

The recorder joins a multicast feed and nothing else. No venue client, no
credentials, no second transport to compare against. That constraint is chosen
rather than inherited, and it costs something specific, so both halves are
stated here instead of being left as an omission.

**What a source-agnostic tool cannot answer.** Whether the edge feed is *ahead
of* a venue's own transport, and by how much. That needs the same book seen over
two transports on one clock, and the second transport is an authenticated venue
service. Admitting one would reintroduce exactly the coupling that stops a tool
from running anywhere the venue comparison does not exist — which is most places
a recorder is wanted. No adapter fixes it: a venue-facing claim needs a
venue-facing measurement. If that claim must be made, it belongs in something
separate and much smaller — a client that records one venue's own transport into
the same column store, with no multicast join, no decoder, no book and no
archive. One input to a comparison, not half a capture pipeline; and separable
from this design either way.

**What takes its place: cross-site.** `(channel instance, sequence number)`
identifies a datagram independently of who received it, so the same datagram
recorded at two sites joins on that key, with no credentials anywhere in the
picture. For the question this design exists to answer — *did the publisher send
what the spec says it must, and did it arrive* — that is the stronger
comparison, because it isolates publisher-attributable loss directly: a
sequence number absent from *every* site, with no recorder overflow anywhere, is
the publisher's. *The analysis tier* below states the full set of answers the
join yields.

**What the analysis tier must therefore contain**, per feed flavour, and none of
it venue-specific: a subscriber state machine that tracks per-instrument
sequence, discards a stale snapshot anchor, counts a snapshot whose level count
does not match its body, and re-bootstraps every ready instrument after a
datagram gap; a book that folds absolute level updates rather than signed
deltas; and a fingerprint that makes one book state comparable to another
observation of the same book. That is the bulk of the per-flavour work. All of
it is offline, and none of it belongs anywhere near the receive path.

---

## The analysis tier

Reads the archive back through `Source` and produces three things.

**Conformance findings.** The rule set, unmodified, over replayed archives.
Two properties a live validator cannot have: lossless replay means the
verifiability gate opens far more often, so `unverifiable` collapses toward
`pass` or `violation` instead of being the majority outcome; and a rule added
next month can be run against last month's traffic. That second property is the
strongest single argument for keeping the bytes — a rule set is a growing
thing, and an archive is what lets it grow backwards.

**Rows.** Per datagram: channel instance, sequence, reset count, send and
receive timestamps, size, port role, recorder, site. Per message, per feed
flavour: the decoded fields, using the codec crates. Both idempotent on
`(object key, sha256)`. This is what turns "did the publisher send correctly"
into a query rather than a run.

**The cross-site comparison.** Because
`(channel instance, sequence number)` identifies a datagram independently of
who received it, the same datagram recorded at two sites joins on that key. The
join answers, with no venue involvement and no credentials:

- per-site loss on the same feed over the same window: a datagram present at
  one site and absent at another was not a publisher gap
- per-site arrival latency from one publisher send timestamp, so the sites are
  compared on a single clock rather than on their own
- which site saw it first, and by how much
- on redundant channel instances, per-path loss and the fill rate one path
  contributes over the other — the number that says whether the redundancy is
  earning its cost
- publisher-attributable loss, isolated at last: a sequence number absent from
  *every* site, with no recorder overflow anywhere, is the publisher's

Every one of those is a query over rows, and every one of them is available
only because the bytes were kept and the archives were brought together.

---

## Configuration

Each crate parses its own section, so keys, types and defaults cannot drift
between recorder hosts. `deny_unknown_fields` everywhere: a misspelled section
that parses cleanly and falls back to a default is how a host runs the wrong
transport while the operator believes otherwise.

```toml
site     = "..."            # label on every dz_recorder_* series and object key
recorder = "..."            # unique within site
env      = "..."

[[feed]]                    # one per feed recorded
spec            = "top-of-book"
multicast_group = "..."     # one group, per the reference-data supplement
interface       = "..."     # optional; route discovery when unset
mktdata_port    = 0
refdata_port    = 0
snapshot_port   = 0         # depth feeds only
expected_sources = []       # optional; empty means no expectation stated

[capture]
mode            = "afpacket"  # or "socket", where CAP_NET_RAW is unavailable
buffer          = "64MiB"     # AF_PACKET ring, or socket rcvbuf in socket mode

[archive]
staging_dir     = "..."       # the segment currently being written
completed_dir   = "..."       # rotated, hashed, manifested; the shipper's input
rotate_bytes    = "256MiB"
rotate_interval = "60s"
compression     = "zstd"
staging_max     = "64GiB"     # the outage buffer; oldest evicted, never blocking

[health]
walk_messages   = false     # structural, loss-immune, off by default

[metrics]
listen_addr = "127.0.0.1:9100"
```

There is no key that can raise the datagram size cap, no key that can select a
second multicast group for reference data, and no key that can disable drop
accounting. Each is an invariant a spec or this design already decided, and
each is placed where configuration cannot reach it — the same discipline the
publisher design applies to `MAX_DATAGRAM_SIZE`, for the same reason.

**An unexpected source is recorded, not dropped.** `expected_sources` gates
counting and alerting, never the archive. A wrongly recorded datagram is
filterable afterwards on the source address; a wrongly dropped one is gone.

There are no bucket, credential or endpoint keys, because the recorder does not
upload. `completed_dir` is the whole interface to the shipper, and `staging_max`
governs what happens when the shipper stops draining it.

---

## Cost, and the retention decision this forces

The archive is the expensive part and the sizing must be stated rather than
discovered.

```
bytes/day ≈ datagrams/sec × mean datagram bytes × 86400 × (1 / compression ratio)
```

Illustrative only — **the recorder measures the real rate on day one, and that
measurement, not this arithmetic, sets the policy.** A depth feed sustaining
10,000 datagrams/sec at a 1232-byte mean is roughly 1 TB/day raw and, at a
4× ratio, roughly 250 GB/day per recorder per feed. Multiply by feeds and by
sites. This is affordable for a window and not affordable forever.

So retention is tiered, and the tiers are chosen by what each answers:

- **`refdata` and `snapshot` port roles: keep raw, long.** They are a small
  fraction of the volume and they are what makes reconstruction possible at
  all. Discarding them to save storage saves almost nothing and costs the
  ability to rebuild a book.
- **`mktdata` raw: keep hot for a window** measured in days, sized to how long
  after an incident someone actually asks. Then expire.
- **Derived rows: keep indefinitely.** The per-datagram row is tens of bytes
  against a 1232-byte datagram, and it is what every sequence, latency,
  loss and cross-site question is actually asked against. The raw bytes are
  needed to *produce* it and to answer questions nobody has thought of yet.
- **A header-only thin archive is the middle tier if one is needed:** the
  24-byte datagram header and the receive metadata, without payloads, at a few
  percent of the cost, keeping every continuity and latency question
  answerable after the raw bytes expire.

---

## Testing

The `Source` symmetry is what makes this testable, and the tests are named here
because a recorder whose failure modes are untested is a recorder that will
report a clean archive of nothing.

- **Round-trip.** A synthetic publisher emits a known datagram stream; the
  recorder archives it; replay yields the identical bytes, timestamps, source
  addresses and port roles. This is the whole contract in one test.
- **Injected faults, and their counters.** Sequence gap, backward motion,
  reset, a new source address appearing, a source address disappearing, a
  duplicate, a reordered pair, an oversized datagram, an unknown schema
  version, a silent channel. Each maps to exactly one counter, asserted, so a
  fault that moves no counter fails CI rather than waiting to be discovered in
  production. A fault-to-counter table with no test behind it is a table that
  drifts.
- **Loss attribution.** A recorder starved deliberately — an undersized ring,
  a paused drain — must produce an archive whose `epb_dropcount` accounts for the
  gap, and an analysis run over it must report the gap as recorder loss and not
  as a violation.
- **Backpressure.** A staging directory forced full must evict, count, and
  leave the receive path's drop counter at zero. This is the rule that matters
  most and the one most easily broken by a later change.
- **Cross-language.** A pcapng segment written by the Rust writer is read by
  the Go conformance tool and yields the same datagram sequence. The archive is
  an interface between two languages and needs the same golden-vector
  discipline the codec has.

---

## Order of work

| Step | Work | Status | Risk |
|---|---|---|---|
| 1 | `dz-recorder-core`, `-capture`, `-archive`, `-replay`; round-trip test | **done** (#60) | none; nothing runs on a host yet, and every test is a CI test needing no privileges and no network |
| 2 | The Go capture reader moves to pcapng | not started | none; classic-`pcap` fixtures still read |
| 3 | Run the recorder on one host and compare its archive, datagram for datagram, against a capture taken at the same point by independent tooling | not started | one host, and the comparison *is* the acceptance test — a byte-level control, not an inspection |
| 4 | `-health` and `dz_recorder_*` | **done** (#61) | the first step that makes a claim rather than matching one; alerts and dashboards land, and capture loss becomes visible for the first time |
| 5 | Object layout, manifest and index table | object key and manifest **done** (#60, #61); the index table and the shipper contract are not | the first storage cost; the retention policy is decided here |
| 6 | Roll out one host at a time, each proven before the next | not started | bounded to one host per change, with a rollback that does not depend on the new path being healthy |
| 7 | Analysis tier: replay into conformance plus the row loaders | sequence loss **done** (#63); `dz-edge-mbp` **done**; the state machine, book, fingerprint, conformance runner and loaders are not | none; re-runnable by construction, and it needs only an archive |
| 8 | Cross-site join | not started | the payoff |
| 9 | Point the dashboards at the analysis tier's rows | not started | the rows must be proven equivalent to what a dashboard already shows before anything is switched over |

Steps 4 and 7 ran ahead of 2, 3 and 6, which the paragraph below anticipated for
7 and did not for 4: the health tier landed before any host ran the recorder,
because everything in it is testable without one. What has *not* moved is the
part that needs a host — the acceptance comparison at step 3 and the rollout at
step 6 — and no claim in this document about running recorders should be read as
describing something that has happened.

Three properties of this order are deliberate. **Nothing runs on a host before
step 3**, and step 3 is one host. **The health tier comes before the object
layout**, because the health tier and the archive format are the two things
this design adds that nothing else supplies, and the health tier is the cheaper
of the two to be wrong about — a metric is renamed, an object layout is
rewritten. And **the analysis tier does not wait for the rollout**: it needs an
archive and nothing else, so steps 7 and 8 run in parallel with 5 and 6 rather
than behind them.

---

## Decisions

**The archive is bytes, not rows.** Rows are derived and re-derivable; bytes
are not. A recorder that stores only its own interpretation has thrown away the
ability to be wrong about it.

**The recorder does not need to know the feed.** Feed knowledge is confined to
the analysis tier and to an opt-in structural mode. This is what makes it
generic across the family's flavours — and across the flavours not yet written.

**Rust, with the archive as the language seam.** The codec crates this sits on
are Rust, and the receive path wants kernel timestamps, no allocation per
datagram, and no garbage collector between the socket and the disk. The Go
analysis tier keeps its own language, and the seam between them is a file
format that Wireshark also reads, so neither side owns the other. The
alternative — a Go recorder embedding the conformance engine — is not
unreasonable; it is rejected because it would duplicate the codec crates and
put a decoder back in the recording process, which is the single thing this
design is most careful to keep out.

**The capture point is the interface, not the socket.** The socket provides
membership; `AF_PACKET` provides the bytes. What the network delivered is the
right subject for a design whose goal is validating a publisher, it needs no
header synthesis, and it keeps in the archive the datagrams a socket capture
would have quietly lost.

**The recorder does not upload.** It rotates, hashes, manifests and evicts,
then writes into a directory. Shipping immutable hashed files to object storage
is solved; the recorder's own contribution would be a worse implementation of
it and one more thing to fail in-process.

**The capture path never blocks and never parses.** Both rules exist to keep
the recorder from manufacturing the loss it was built to measure.

**No accelerated capture framework, and the host is sized for the archive.**
The measured load is two orders of magnitude below where kernel bypass earns
its kernel module, none of the loss actually observed is capture loss, and the
`libpcap` seam keeps the option a link-time change if that ever stops being
true. What is *not* comfortable at the measured rates is the staging buffer,
which is where a host choice actually shows up.

**Loss is attributed in the archive, not inferred outside it.** `epb_dropcount`
is written per datagram, so an archive is self-describing about its own
trustworthiness.

**Recorded sites replace the venue comparison.** The comparison a source-
agnostic tool can make is against the same feed seen elsewhere, and it isolates
publisher loss more cleanly than a venue comparison does.

---

## Non-goals

No venue clients, credentials, or comparison against a venue's own service —
including in the analysis tier, which decodes multicast and folds a book but
takes no authenticated venue input. See *Why no venue comparison, and what
takes its place*.

No feed spec changes.

No new conformance rule set. That rule set is the analysis tier; rules are
proposed upstream, not forked here.

No book state in the recorder. A book is analysis, it is per-flavour, and it is
exactly the kind of long-lived state that goes silently wrong in a process
nobody is watching.

No replacement of the reference parsers or the demo stack. Those show a
consumer how to consume; this measures whether there was anything correct to
consume.
