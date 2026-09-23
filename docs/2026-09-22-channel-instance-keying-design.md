# Channel-instance keying for gap detection, recovery state and the snapshot cycle — design

Reviewed against `GLOSSARY.md` at `glossary/v1.3.0`, the version
`.github/skills/code-review/GLOSSARY.md` carries.

Predecessor: [Per-publisher sequence tracking](superpowers/specs/2026-08-10-per-channel-seq-tracking-design.md)
· [plan](superpowers/plans/2026-08-10-per-publisher-seq-tracking.md). That pair
established the glossary's key inside the three parsers. This one carries it
across the parser-to-book-builder boundary and into both book-builders.

## Problem

The glossary's Transport table defines the unit, and it is not the channel:

> **Channel** — A logical shard of the instrument set, named by `Channel ID`
> (`u8`) in the datagram header. **Two redundant paths may carry the same
> channel.**
>
> **Channel instance** — One path's view of one channel, keyed
> `(source IP address, Channel ID, destination port)`. The unit that owns a
> sequence series, a `Reset Count`, and a snapshot cycle.

and the note below the table makes the requirement normative:

> **Sequencing keys on the channel instance, never the channel.**
> `Sequence Number`, `Reset Count`, and the snapshot cycle belong to one path's
> view of a channel. Redundant paths carrying the same channel run as separate
> processes on separate hosts and cannot share a counter, so a subscriber
> binding more than one sees an independent series per instance and MUST key gap
> detection and recovery state on `(source IP address, Channel ID, destination
> port)`. Each host publishes on a distinct destination port by deployment
> convention, defined out of band. Arbitrating between instances of the same
> channel is a separate concern from sequencing within one.

Both book-builders key that state on the `Channel ID` alone, and the record they
key it from cannot carry anything else.

### What the tree keys on

| Structure | Declared | Key today | What owns it per the glossary |
|---|---|---|---|
| `instKey` | `go/marketbyorder-bot/shard.go:15` | `{ch uint8, id uint32}` | channel instance + `Instrument ID` |
| `instKey` | `go/marketbyprice-bot/shard.go:27` | `{ch uint8, id uint32}` | channel instance + `Instrument ID` |
| `Coordinator.resetCount` | `go/marketbyorder-bot/coordinator.go:24` | `map[uint8]uint8` per `Channel ID` | channel instance |
| `Coordinator.resetCount` | `go/marketbyprice-bot/coordinator.go:53` | `map[uint8]uint8` per `Channel ID` | channel instance |
| `Coordinator.snapshotRoute` | `go/marketbyorder-bot/coordinator.go:27` | `map[snapKey]int`, `snapKey{ch, snap}` | channel instance |
| `Shard.snapCtx` | `go/marketbyorder-bot/shard.go:66` | `map[snapKey]SnapshotContext` | channel instance |
| `Coordinator.open` | `go/marketbyprice-bot/coordinator.go:55` | `map[uint8]openGroup` per `Channel ID` | channel instance |
| `Coordinator.seqLast` | `go/marketbyorder-bot/coordinator.go:26` | `map[string]uint64` keyed on the port role token | channel instance — and it is never read |
| `Coordinator.manifest` | `go/marketbyprice-bot/coordinator.go:54` | one `ManifestState` for the process | channel instance |
| `SnapshotWriter.dirty`, `lastWrittenAt` | `go/marketbyprice-bot/snapshot_writer.go:42`, `:49` | `map[instKey]…` | channel instance + `Instrument ID` |
| `SnapshotWriter.dirty` | `go/marketbyorder-bot/snapshot_writer.go:24` | `map[uint32]*dirtyEntry`, beside a constructor-fixed `channel` (`:26`) | channel instance + `Instrument ID` |
| `SnapshotWriter.generation` | `go/marketbyorder-bot/snapshot_writer.go:27`, `go/marketbyprice-bot/snapshot_writer.go:52` | one counter per shard | channel instance |
| `seqTracker.last` | `go/marketbyorder-parser/runner.go:39`, `go/marketbyprice-parser/runner.go:39` | `map[pubKey]uint64`, `pubKey{src, ch}` | correct in effect, see below |

Four consequences, all silent.

**The snapshot cycle collapses.** `marketbyprice-bot` holds one open snapshot
group per `Channel ID` (`coordinator.go:55`), and stamps each `snapshot_level`
with that group's instrument (`coordinator.go:114-126`) because the wire omits
`instrument_id` on a level. Two paths carrying one channel give one map entry:
the second path's `SnapshotBegin` overwrites the first's, and the first path's
levels are stamped with the second path's instrument and filed into its shadow.
`marketbyorder-bot` reaches the same end by a different route — `snapshotRoute`
is keyed `(Channel ID, Snapshot ID)`, and `Shard.applySnapshotOrder`
(`shard.go:184-200`) then scans every instrument the shard owns for one whose
`OpenSnapshot.SnapshotID` matches, without filtering on the channel at all.

**`Reset Count` is arbitrated by a guess.** `Reset Count` is per channel
instance, so two instances hold two independent, differing, steady values. Held
per `Channel ID`, the alternation between two instances of one channel reads as a
reset on every datagram.

**A reset wipes work that never reset.** A shard handles `msgReset` by wiping the
channel's share of its maps and then calling `SnapshotWriter.Reset(ctx)`
(`go/marketbyorder-bot/shard.go:505-511`, `go/marketbyprice-bot/dispatch.go:468-475`).
That call takes no key: `doReset` replaces `dirty` and `lastWrittenAt` whole and
bumps one `generation`
(`go/marketbyorder-bot/snapshot_writer.go:92-96`,
`go/marketbyprice-bot/snapshot_writer.go:134-142`), and `flushDue` abandons any
batch it had already extracted when the generation moves. One shard serves every
channel for its id-modulo, so this already discards another channel's pending
`level_snapshots` rows, and it will discard the other instance's the moment an
instance can reset alone.

**A manifest bump prunes across the boundary.** `marketbyprice-bot` holds the
refdata manifest as one `ManifestState` (`coordinator.go:54`), and `applyManifest`
broadcasts `msgManifestPrune` carrying only the new `Manifest Seq`
(`coordinator.go:200-220`). `Shard.pruneManifest` (`dispatch.go:303-329`) then
walks every entry in `refdata` and drops the instrument, its book, its buffered
deltas and its gauges wherever `ManifestSeq` is below the cutoff, with no channel
or instance filter. A manifest published by one path therefore evicts the other
path's instruments. `marketbyorder-bot` is not exposed to this: its `manifest`
field is parity bookkeeping and drives no prune (`coordinator.go:25`).

### The identity exists upstream and is dropped at the boundary

The parsers already hold it. `seqTracker.observe`
(`go/marketbyorder-parser/runner.go:54`) keys on `pubKey{src netip.Addr, ch
uint8}` and the tracker is a local of `Runner.receive` (`runner.go:178`), one per
port-role goroutine — so gap detection is keyed per
`(source IP address, Channel ID, destination port)` in effect, by the tracker's
lifetime rather than by its key. The two gap counters carry the identity as
labels, `{port, source_ip, channel_id}`
(`go/marketbyorder-parser/metrics.go:107`, `:112`).

The record does not. `Record` (`go/marketbyorder-parser/parser.go:10-23`,
duplicated at `go/marketbyprice-parser/parser.go:10`,
`go/marketbyorder-bot/record.go:5` and `go/marketbyprice-bot/record.go:5`)
carries `ChannelID uint8` and `Port string`, and

> **Port role** — One of exactly `mktdata`, `refdata`, `snapshot`. Use these
> tokens verbatim.

is what `Port` holds: `decodeMessage` sets `Port: port`
(`go/marketbyorder-parser/marketbyorder.go:58`) from the label in
`Runner.ports` (`runner.go:120-124`), which is one of those three tokens. It is
a port role, not a destination port, and there is no source IP address on the
record at all. So no book-builder can key correctly on the records as they
stand, whatever it does internally.

### The constraint being removed

Four comments in the tree record a deployment in which redundant paths are
distinguished by `Channel ID` on one set of ports:

- `go/marketbyorder-bot/coordinator.go:19-23`
- `go/marketbyorder-bot/shard.go:91-94`
- `go/marketbyprice-bot/coordinator.go:47-52`
- `go/marketbyprice-bot/dispatch.go:334-337`

Each says that "a group can carry two redundant publishers interleaved on the
same ports under different channel_ids", and each uses that to justify keying on
the `Channel ID`. The glossary's model is the opposite: two paths **may** carry
the same channel, and each host publishes on **a distinct destination port**.
Both statements cannot stay in the tree. The glossary wins — it is the
authority, and `.github/skills/code-review/SKILL.md` says a local definition
does not override it — so the comments go and the identity becomes a type.

### `seqLast` is write-only

`Coordinator.seqLast` in `marketbyorder-bot` is assigned at `coordinator.go:59`,
cleared at `:140`, and read nowhere. `grep -rn 'seqLast\|SeqLast' go/` returns
five lines, four of them those two sites and the declaration, and the fifth a
comment naming the field. So that book-builder has no datagram sequence
continuity check: a lost `SnapshotBegin` or `SnapshotEnd` is invisible to the
snapshot association, while a field named for the check reads as though one were
being made. `marketbyprice-bot` has no equivalent field at all.

This matters here rather than separately, because sequence continuity on the
`snapshot` port is the only true discriminator between two groups that share a
`Snapshot ID`, and a continuity check is only sound when it is keyed on the
channel instance. It is the same change.

## Success criteria

1. Gap detection, recovery state, `Reset Count` and the snapshot cycle are keyed
   on `(source IP address, Channel ID, destination port)` in both
   book-builders and in both parsers, as a declared type rather than as a
   property of which goroutine holds a map.
2. Two paths carrying one `Channel ID`, on distinct destination ports, produce
   two independent sequence series, two `Reset Count` values and two snapshot
   cycles, and neither wipes, overwrites or reads the other's in-process state.
3. Both instances' rows survive in every table that retains rows — `events`,
   `level_snapshots`, `wire_snapshots`, `wire_levels` and `channel_health` are
   all plain `MergeTree`. `instruments` is the one table that does not, and the
   criterion is scoped around it rather than over it: it is
   `ReplacingMergeTree(recv_ts) ORDER BY (channel_id, instrument_id)`
   (`demo/clickhouse/init/02_schema_mbo.sql:24-25`,
   `demo/clickhouse/init/03_schema_mbp.sql:25-26`), so two instances of one
   channel collapse to one row and the last writer wins. The two columns record
   which instance that row came from; keeping one row per instance is a sort-key
   change and a table rebuild, and it is in *Out of scope*. In-process refdata is
   unaffected — it is keyed by `instKey`, which carries the instance.
4. A `Reset Count` change on one instance drains the shards and wipes that
   instance's books, refdata, buffered deltas and pending snapshot rows, and
   only that instance's.
5. A datagram-sequence discontinuity on an instance's `snapshot` port
   invalidates that instance's open snapshot group, so a lost `SnapshotBegin` or
   `SnapshotEnd` cannot leave levels filed against a stale group.
6. Every persisted row states which channel instance produced it.
7. No insert is ever accepted with a field the table has no column for. Exactly
   one window exists in which a process reads a field the other side is not yet
   writing — a book-builder rolled ahead of its parser — and in it the
   book-builder's behaviour is defined rather than guessed: it degrades to the
   `Channel ID` key, counts every such record, and runs no continuity check on
   records it cannot attribute. See *Migration and compatibility order*.

Explicitly **not** a success criterion: folding two instances of one channel
into one book. See *Out of scope*.

## The identity the record gains

### The fields

Two, on all four `Record` declarations:

| Go field | JSON key | Go type | Why that type |
|---|---|---|---|
| `SourceAddr` | `source_addr` | `netip.Addr` | Comparable, so it is usable directly in a map key with no allocation per datagram — the reason `pubKey` already holds one (`go/marketbyorder-parser/runner.go:29-30`). It implements `MarshalText`/`UnmarshalText`, so it encodes as a dotted quad string in JSONL and decodes on the book-builder side with no helper. The zero `Addr` is the one value that is not a dotted quad: it marshals to `""` and unmarshals back to the zero `Addr`, which is what makes an unstamped record recognisable — and why a ClickHouse row is built through the helper under *The column is forward-only* rather than from the field. |
| `DstPort` | `dst_port` | `uint16` | The destination port **number**. A UDP port is a `u16`; `portConfig.Port` is an `int` only because `net.UDPAddr.Port` is. |

Both are unconditional: no `omitempty`. A record whose instance identity is
absent must be visibly absent rather than indistinguishable from a record that
was never stamped.

**The spellings are the recorder's**, not new ones. `recorder.datagram`,
`recorder.era`, `recorder.segment_coverage`, `recorder.sequence_gap`,
`recorder.conformance_finding`, `recorder.event` and `recorder.instrument` all
carry `source_addr IPv4` and `dst_port UInt16`
(`rust/recorder/dz-recorder-clickhouse/db/clickhouse/001_recorder_rows.sql:139-144`,
`005_recorder_market_data.sql:56-59`). Those columns land in the same ClickHouse
server as the ones this design adds, and a reader joining a loss question to a
market data question should not have to remember two spellings for one
identifier.

### The role string stays alongside

`Port` stays, with its three verbatim tokens, for two reasons.

The parsers exempt the `refdata` port role from datagram-sequence gap tracking
by name — `port != "refdata"` at `go/marketbyorder-parser/runner.go:199`,
because refdata is low-rate periodic-retransmit traffic whose datagram-sequence
gaps are not a loss signal. The book-builder's new continuity check needs the
same exemption, and the token is what carries it.

And the number does not imply the role without an out-of-band mapping. The
recorder's own argument is that "`port_role` is recoverable from `dst_port`"
(`docs/superpowers/specs/2026-09-05-recorder-market-data-rows-design.md:235`),
and that is true *at the recorder*, which holds the feed's `(group, port)`
triples in its configuration. A book-builder reading a unix socket holds no such
configuration: it never sees a CLI flag naming a port, and `dst_port` alone
would tell it nothing. So the role travels on the record, and the number is
identity.

`Port` is therefore read for the first time in a book-builder by this change.
Today its only consumer in either book-builder is the write-only `seqLast`
assignment.

### Where the stamp happens

In `Runner.receive`, in the loop that already stamps `RecvTSNS` and
`RecvTSKind` (`go/marketbyorder-parser/runner.go:224-229`,
`go/marketbyprice-parser/runner.go:240-245`). Both values are in hand there:
`src` is returned by `readDatagram` (`runner.go:188`) and normalised by
`srcAddr`, and the destination port number reaches the goroutine by passing the
whole `portConfig` into `receive` instead of only `pc.Label`
(`runner.go:144-148`).

So this is a plumb-through and not a lookup. In particular `Parser.ParseDatagram`
(`go/marketbyorder-parser/parser.go:31`,
`go/marketbyprice-parser/parser.go:39`) keeps its signature: one `Parser` is
shared by all three port-role goroutines, so per-goroutine state held on it
would be a data race — the reason the market-by-price parser
already returns `Defects` rather than accumulating them
(`go/marketbyprice-parser/runner.go:223-225`).

The stamping loop becomes a named function, `stampInstance`, so that it has a
seam a unit test can drive. `receive` itself needs a bound multicast socket and
is not reachable from the suite.

### The key is declared per module, not shared

`channelInstance`, a three-field comparable struct, declared once in each of the
four modules that need it — both parsers and both book-builders:

```go
// channelInstance is one path's view of one channel: the unit that owns a
// sequence series, a Reset Count and a snapshot cycle.
type channelInstance struct {
	addr netip.Addr
	ch   uint8
	port uint16
}
```

Four declarations rather than one in `go/internal/`, because `go/go.work` lists
nine separate Go modules and of the four only `marketbyprice-bot` depends on
`go/internal` today — `grep -ln 'go/internal' go/*/go.mod` names it,
`kernel-receiver` and `xdp-receiver`, and nothing else. `marketbyorder-bot`,
`marketbyorder-parser` and `marketbyprice-parser` would each gain a module
dependency and a `replace` directive for a three-field struct. The tree already duplicates `Record` four
times for exactly that reason, and the duplication is the cheaper edge.

It replaces `pubKey` in both parsers. Within one `receive` goroutine the `port`
field is constant, so it is redundant *there* — and that redundancy is the
point: the key becomes the definition of the channel instance that the record
stamp, the tracker and both book-builders all share, instead of a two-field key
whose third dimension is implied by a goroutine's lifetime.

## What each keyed structure becomes

| Module | Structure | Becomes |
|---|---|---|
| `go/marketbyorder-parser` | `pubKey{src, ch}` | deleted; `seqTracker.last` is `map[channelInstance]uint64` and `observe` takes a `channelInstance` |
| `go/marketbyprice-parser` | `pubKey{src, ch}` | the same |
| `go/marketbyorder-bot` | `instKey{ch, id}` | `instKey{inst channelInstance, id uint32}` — `instruments`, `refdata`, `deltaBuf` and `resetChannel` follow it |
| `go/marketbyorder-bot` | `Coordinator.resetCount` | `map[channelInstance]uint8` |
| `go/marketbyorder-bot` | `Coordinator.snapshotRoute` | deleted, replaced by `open map[channelInstance]openGroup` |
| `go/marketbyorder-bot` | `Shard.snapCtx`, `snapKey{ch, snap}` | `map[instKey]SnapshotContext` — one open cycle per instrument per instance |
| `go/marketbyorder-bot` | `Coordinator.seqLast` | `map[channelInstance]uint64`, and read |
| `go/marketbyorder-bot` | `SnapshotWriter.dirty`, `withInstrument`, `MarkDirty`, and the constructor's fixed `channel` | keyed on `instKey`; `channel_id` on a `level_snapshots` row comes from the key instead of the constructor argument |
| `go/marketbyprice-bot` | `instKey{ch, id}` | `instKey{inst channelInstance, id uint32}` |
| `go/marketbyprice-bot` | `Coordinator.resetCount` | `map[channelInstance]uint8` |
| `go/marketbyprice-bot` | `Coordinator.open` | `map[channelInstance]openGroup` |
| `go/marketbyprice-bot` | `Coordinator.manifest` | `map[channelInstance]ManifestState`, and `msgManifestPrune` carries the instance |
| both book-builders | `shardMsg.ch` | `shardMsg.inst channelInstance`, for `msgReset` and `msgManifestPrune` |
| both book-builders | `SnapshotWriter.Reset`, `doReset`, `generation` | `Reset(ctx, inst)`; `dirty` and `lastWrittenAt` lose only that instance's entries, and the generation is held per instance |

Five notes on what that table leaves implicit.

**One open group per channel instance is the sufficient state, and the protocol
is what makes it sufficient.** A publisher MUST NOT interleave snapshot groups
within one channel instance — stated at `go/marketbyprice-bot/coordinator.go:26-27`
and at `docs/2026-04-23-marketbyorder-plan.md:3066-3068`, and it is why
`marketbyprice-bot` holds one `openGroup` rather than a set. Two instances of one
channel are two publishers, and they do interleave with each other, so the
sufficient state is one open group **per channel instance** and one per channel
is not. That is the whole of the change to the open group: the cardinality the
protocol already implies, keyed on the unit the glossary already names.

**`snapshotRoute` goes away rather than being re-keyed.** Under that protocol an
id-keyed route is unambiguous while no `snapshot_begin` or `snapshot_end` is
lost: `Dispatch` deletes the route at `snapshot_end`
(`go/marketbyorder-bot/coordinator.go:85`) and the next group's `snapshot_begin`
claims the id afresh (`:68`), so two instruments never hold one `Snapshot ID`
at the same time within one instance. Re-keying it on the channel instance would
therefore fix the collapse between two paths and leave two things standing.

The first is that the association is resolved by a search rather than by the
group. `Shard.applySnapshotOrder` (`shard.go:184-200`) scans every instrument
the shard owns for one whose `OpenSnapshot.SnapshotID` matches, with no channel
and no instance filter, and Go's map iteration order decides which one it finds
when more than one matches. Two instances of one channel publish the same
`Snapshot ID` values at the same time, so two matching open shadows on one shard
is the steady state rather than the edge case, and the instrument a level is
filed against is then not determined by anything.

The second is what a lost `snapshot_end` leaves behind: the route entry is never
deleted, the instrument's shadow stays open, and the next group at the same id
is resolved against both. `Snapshot ID` is monotonic per
`(Channel ID, Instrument ID)`, not per channel, so the next instrument's cycle
routinely reaches that value — which is the defect
`go/marketbyprice-bot/coordinator.go:19-24` and
`go/marketbyprice-bot/README.md:156` name as the open issue against
`marketbyorder-bot`, reachable through loss rather than through interleaving.

So `marketbyorder-bot` adopts the shape `marketbyprice-bot` already proved: one
open group per channel instance, routed by the group and validated — never keyed
— by `Snapshot ID`. `Shard.applySnapshotOrder` stops scanning and resolves the
instrument from the record the coordinator stamped, the way `marketbyprice-bot`
does at `coordinator.go:124-126`; and the continuity check below is what closes
a group whose `snapshot_end` never arrived.

**`Shard.snapCtx` re-keys onto `instKey` rather than onto a snapshot key.** Its
only consumers are the `wire_snapshots` writes at `shard.go:458-460`, which need
the group's symbol and exponents. With the coordinator stamping the instrument,
the instrument is the key, and `Snapshot ID` stays inside the value as the
membership check it already is.

**The reset marker carries the instance, and so does the writer's reset.**
`shardMsg.ch uint8` (`go/marketbyorder-bot/shard.go:535`,
`go/marketbyprice-bot/shard.go:136`) becomes `inst channelInstance`, so
`resetChannel` cannot be reached with a bare `Channel ID`. That alone is not
enough: the shard follows the wipe with `SnapshotWriter.Reset(ctx)`, which takes
no key and replaces `dirty` and `lastWrittenAt` whole. `Reset` therefore becomes
`Reset(ctx, inst)` and `doReset` deletes only the entries whose `instKey` carries
that instance. The generation counter follows the same rule: it is held per
instance, `flushDue` records the generation of the instance whose batch it
extracted, and a reset on one instance no longer abandons a batch of rows
already computed for the other. Without this, a reset on one instance still
drops the other's pending `level_snapshots` rows even though its book is
untouched, and `level_snapshots` is where a reader looks to see that the spared
instance kept serving.

**The manifest is per channel instance, and so is the prune.**
`marketbyprice-bot`'s `Coordinator.manifest` becomes
`map[channelInstance]ManifestState`, `msgManifestPrune` carries the instance
beside the `Manifest Seq`, and `Shard.pruneManifest` skips entries whose
`instKey` names another instance. The one-generation grace window
(`go/marketbyprice-bot/dispatch.go:305-309`) is unchanged; it is the comparison
set that narrows. Without this the first manifest bump on one path deletes the
other path's instruments, books and buffered deltas outright — a wipe with no
`Reset Count` change behind it, and one no barrier or counter accounts for.
`runResetBarrier`'s `c.manifest = ManifestState{}` (`coordinator.go:256`) becomes
a delete of the resetting instance's entry, for the same reason.
`marketbyorder-bot` needs neither change: its `manifest` field is parity
bookkeeping and no prune reads it (`coordinator.go:25`).

## `seqLast` is read, not deleted

Deleting it would be honest about today and would throw away the only
discriminator available. So it is read.

**The check.** On every record, in `Coordinator.Dispatch`, for a record whose
port role is not `refdata` and whose instance identity is present — an
unidentified record is counted and skipped, for the reason given above:

- First sight of a channel instance establishes its baseline silently. This
  mirrors `seqTracker.observe` (`go/marketbyorder-parser/runner.go:46-53`) and
  for the same reason: a newly appearing path must not produce a phantom gap the
  size of its sequence.
- `seq <= last` is a reorder or a duplicate. Ignored, `last` unchanged.
- `seq > last+1` is a discontinuity on that instance. Count it, and if the port
  role is `snapshot`, delete that instance's entry from `open`.

Dropping the open group on a `snapshot`-port discontinuity is the point of
reading the field. `SnapshotBegin` opens the group, `SnapshotEnd` closes it, and
every `snapshot_order` between them is routed by it. Lose the `SnapshotEnd` and
the group stays open across the boundary into the next cycle, which shares its
`Snapshot ID` space; lose the `SnapshotBegin` and the levels of a cycle nobody
opened are routed by the *previous* cycle's group. Either way levels are filed
into a shadow they do not belong to, and nothing today says so. The
discontinuity is the signal, and it is only a signal per instance: two paths
interleaved under one key produce a discontinuity on every datagram, which is
why this cannot be added before the key changes.

`marketbyprice-bot` gains the same check, on the same key, with the same
`snapshot`-port consequence against its `open` map. It has no field to read
today, so there the field is new rather than promoted — but it is the same
mechanism and belongs in the same change, because `open` is the thing the check
protects and both book-builders hold one.

Rejected: deleting `seqLast` and relying on `snapshot_id` mismatch alone. The
mismatch check already exists (`go/marketbyprice-bot/coordinator.go:106`) and
catches a level whose id differs from the open group's. It cannot catch a level
whose id *matches* a group that a lost `SnapshotEnd` left open, which is
precisely the case two cycles of one instrument produce.

## The comment becomes a type

The four comments are deleted. What replaces them is not a comment in another
place: it is `channelInstance` itself, and the fact that every structure listed
above takes one. A `resetChannel` that takes a `channelInstance` cannot be
called with a bare `Channel ID`, so the constraint the comments recorded is
unstatable rather than merely undocumented.

Two things the code gains instead of the prose:

- `resetChannel(inst channelInstance)` in both book-builders, replacing
  `resetChannel(ch uint8)` (`go/marketbyorder-bot/shard.go:95`,
  `go/marketbyprice-bot/dispatch.go:338`). The existing behaviour — wipe one
  channel's share of a shard, not the whole shard — is kept and narrowed: a
  reset on one instance spares the other instance of the same channel, which is
  what the glossary requires and what the comments were reaching for by the
  wrong route.
- A counter for records arriving with no instance identity:
  `dz_mbo_bot_unidentified_records_total` and
  `dz_mbp_bot_unidentified_records_total`, incremented when `source_addr` is the
  zero `netip.Addr` or `dst_port` is 0. That is what a book-builder deployed
  ahead of its parser sees, and it is the one state in which the book-builder
  degrades to keying on the `Channel ID`. Silent degradation to the behaviour
  being removed is the failure this design is most exposed to, so it is
  counted.

An unidentified record is counted and then kept out of the continuity check
entirely — not run through it on the degraded key. Two instances of one channel
arriving under one key are two ascending sequence series interleaved, which the
check below reads as a discontinuity on very nearly every datagram: on the
`snapshot` port that deletes the open group on every datagram, and no snapshot
cycle ever completes. The degraded window has to behave as the tree behaves
today: no instance keying, and none of the checks that only instance keying
makes sound. So the rule is exact — a record with the zero `netip.Addr` or
`dst_port` 0 increments the counter, skips the continuity check, and is
otherwise dispatched as it is today.

The deployment constraint that does survive is the glossary's own, and it is not
ours to enforce: "Each host publishes on a distinct destination port by
deployment convention, defined out of band." A subscriber cannot check it. What
it can do is key on what it observes, which is this change.

## Blast radius

### Every consumer of `Record`

| Consumer | Change |
|---|---|
| `go/marketbyorder-parser/parser.go:10` `Record` | two fields |
| `go/marketbyprice-parser/parser.go:10` `Record` | two fields |
| `go/marketbyorder-bot/record.go:5` `Record` | two fields |
| `go/marketbyprice-bot/record.go:5` `Record` | two fields |
| `go/marketbyorder-parser/runner.go`, `go/marketbyprice-parser/runner.go` | `portConfig` into `receive`; `stampInstance` |
| `go/marketbyorder-bot/coordinator.go`, `shard.go` | the keying above |
| `go/marketbyprice-bot/coordinator.go`, `shard.go`, `dispatch.go` | the keying above, and the manifest |
| `go/marketbyorder-bot/snapshot_writer.go` | re-keyed onto `instKey`; `Reset(ctx, inst)`; the row key |
| `go/marketbyprice-bot/snapshot_writer.go` | `Reset(ctx, inst)`; the row key |
| `go/marketbyorder-bot/events_writer.go`, `go/marketbyprice-bot/events_writer.go` | the row key |
| `go/marketbyorder-bot/main.go`, `go/marketbyprice-bot/main.go` | every `instKey` literal and `NewSnapshotWriter` call site |
| `go/internal/clickhouse/client.go` | the insert setting |
| `go/marketbyorder-bot/clickhouse.go`, `go/topofbook-bot/clickhouse.go` | the insert setting |

### The sinks need no change

`JSONFileSink.Write` encodes `&records[i]` whole
(`go/marketbyorder-parser/sink_json.go:32`) and `SocketSink` marshals the same
struct, so both carry the new keys as soon as the struct has them. On the
reading side both book-builders decode with `encoding/json`, which ignores an
unknown key — `json.Unmarshal` at `go/marketbyorder-bot/bot.go:90` and a
`json.Decoder` at `go/marketbyprice-bot/bot.go:111`, neither calling
`DisallowUnknownFields`. That asymmetry is what makes the rollout order below
safe in one direction.

One JSONL example in the live documentation shows the record shape and gains the
two keys: `go/marketbyprice-parser/README.md:69`.

### ClickHouse

`source_addr IPv4` and `dst_port UInt16` on every table whose rows describe one
channel instance's view:

| File | Tables |
|---|---|
| `demo/clickhouse/init/02_schema_mbo.sql` | `marketbyorder.events`, `level_snapshots`, `wire_snapshots`, `channel_health`, `instruments` |
| `demo/clickhouse/init/03_schema_mbp.sql` | `marketbyprice.events`, `level_snapshots`, `wire_levels`, `channel_health`, `instruments` |

`instruments` is included for the recorder's stated reason: an era is opened per
path, and two paths keying together would let one path's exponents decode the
other path's prices
(`docs/superpowers/specs/2026-09-05-recorder-market-data-rows-design.md:489-492`).
Both tables are `ReplacingMergeTree(recv_ts)` ordered on
`(channel_id, instrument_id)` (`demo/clickhouse/init/02_schema_mbo.sql:24-25`,
`demo/clickhouse/init/03_schema_mbp.sql:25-26`), and adding a column does not
change that: two instances of one channel collapse to one row and the newest
`recv_ts` wins, before this change and after it. What the columns buy on
`instruments` is therefore narrower than on the other eight tables — the
surviving row states which instance wrote it, rather than both rows surviving —
and an operator joining `level_snapshots`, which keeps every instance's rows, to
`instruments` for exponents reads one instance's definition for both. The
in-process hazard the recorder names is closed regardless, because refdata is
keyed by `instKey` and `instKey` carries the instance. Keeping one row per
instance needs the sort key, which is a table rebuild and is in *Out of scope*.

**No `port_role` column.** Here the recorder's argument does apply — the role is
recoverable from the number by anyone holding the feed's port assignment, which
is the operator querying ClickHouse — so the name beside the number would
restate a fact already in the row.

**`ORDER BY` does not change.** ClickHouse cannot prepend a column to a sort
key, so the columns are added and the existing keys stand. This costs the
`ReplacingMergeTree` collapse on `instruments`, which stays keyed on
`(channel_id, instrument_id)` and so still keeps one row per channel rather than
per instance. That is a table rebuild, out of scope here, and named in
*Out of scope* rather than left implied.

**Is a migration needed: yes, and the init files alone are not it.** The init
files run from `/docker-entrypoint-initdb.d` on first container boot only and
are skipped afterwards (`demo/clickhouse/init/01_schema.sql:2-3`), which is why
`demo/clickhouse/migrations/001_add_stale_to_level_snapshots.sql` exists as the
one `ALTER` for an existing volume. So: the columns are added to both init files
*and* re-stated as `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` in a new
`demo/clickhouse/migrations/002_add_channel_instance_columns.sql`, for the ten
tables above.

**No view is re-stated, because none exists.** The recorder's migration set has a
house rule here — "A view's `SELECT *` is expanded when the view is created, not
when it is queried", so a column added to a table forces every `SELECT *` view
over it to be re-created
(`rust/recorder/dz-recorder-clickhouse/db/clickhouse/010_recorder_book_key.sql:259-269`),
and `011` records the negative case in the same words: "AND NO VIEW RE-STATED,
because nothing selects from `recorder.event`". The rule is checked rather than
assumed: `grep -rn -i VIEW demo/clickhouse/` returns nothing. The
`marketbyorder` and `marketbyprice` databases hold ten tables and no views, so
this migration re-states none.

**The column is forward-only.** `source_addr IPv4 DEFAULT toIPv4(0)` and
`dst_port UInt16 DEFAULT 0` — `0.0.0.0` and `0` on every row written before the
migration, and on every row a book-builder writes while reading a parser that
does not yet stamp. The venue-observation design refuses exactly those sentinels
for the recorder's own rows
(`docs/superpowers/specs/2026-09-09-recorder-venue-observation-design.md:97`),
and its reason does not reach here: those rows are pre-existing rather than
newly written provenance-free, and there is no honest alternative for a row
nobody stamped. The sentinel is also self-consistent for the one query that
matters — see *Grafana* — because every pre-migration row shares it and
therefore groups exactly as it does today.

**A `DEFAULT` only applies to a column the row does not name, so the writer
serializes the sentinel itself.** A row is a `map[string]any` encoded with
`encoding/json` (`go/internal/clickhouse/client.go:202-208`,
`go/marketbyorder-bot/clickhouse.go:153-159`), and a `netip.Addr` in that map
encodes through `MarshalText`. The zero `netip.Addr` marshals to the empty
string, so a row built straight from `Record.SourceAddr` names `source_addr`
with `""` — a value the `IPv4` parser refuses. The column default never runs,
because the column was named; the insert is refused; and since the batcher posts
a whole batch in one request, one unidentified record fails every row batched
beside it. That is the opposite of the compatibility window this design claims,
so the serialization is stated rather than left to the field's type:

> Every writer puts `source_addr` into the row as a **string**, produced by one
> helper per book-builder: the dotted quad when `SourceAddr` is a valid IPv4
> address, and the literal `"0.0.0.0"` otherwise. `dst_port` goes in as the
> `uint16` it is; `0` is a valid `UInt16` and needs no special case.

Writing the sentinel rather than omitting the key keeps one row shape per table,
which is what makes "this table's row carries both keys" a property a test can
assert; and it leaves the row identical to what the column default would have
produced, so a pre-migration row and a mid-deploy row group together in the
Grafana panel below. It also keeps the loaded value independent of
`input_format_defaults_for_omitted_fields`, which is a different setting from
the one pinned below and one this design does not touch.

The helper is a ClickHouse concern only. The record on the unix socket keeps the
`netip.Addr` and the empty string is correct there, because
`netip.Addr.UnmarshalText` maps empty text back to the zero `Addr` — which is
exactly how a book-builder recognises an unidentified record and counts it.

### The insert format, and why the schema leads

Every insert path in the tree posts `INSERT INTO <table> FORMAT JSONEachRow`
with **no column list**: `go/internal/clickhouse/client.go:213` (used by
`marketbyprice-bot`), `go/marketbyorder-bot/clickhouse.go:164`, and
`buildInsertURL` at `go/topofbook-bot/clickhouse.go:291`. The rows are
`map[string]any` naming their own fields, so the server matches by name.

None of the three sets `input_format_skip_unknown_fields`, which defaults to
`1`. The recorder measured what that means against the 24.8 the container suite
pins, and `demo/docker-compose.yml:157` pins the same version: an insert naming a
column the table does not have is **answered `200`, the row lands, and the field
is discarded**
(`rust/recorder/dz-recorder-clickhouse/src/config.rs:195-226`). At `0` the same
insert is refused with `Code: 117 ... Unknown field found while parsing
JSONEachRow format` as a `400`, and the batch stays unloaded until the migration
is applied.

So a book-builder rolled ahead of its migration today writes a whole window of
rows whose new column is unwritten, and every acknowledgement is a success. This
design pins the setting to `0` in all three insert paths, and does so **before**
any row carries the new key. That turns the one silent direction of a rolling
deploy into a refused batch that loads by itself once the column exists.

It refuses that direction and only that one. A book-builder *behind* the schema
omits a known field rather than naming an unknown one, which is
`input_format_defaults_for_omitted_fields` — a different setting this one does
not touch — so a rollback still loads and reads back as the type's default.

The rows themselves gain the key in the `map[string]any` literals, not
automatically: `EventsWriter.Write` enumerates its keys explicitly
(`go/marketbyorder-bot/events_writer.go:28-46`), so adding a `Record` field adds
no JSON key to any row until a writer is changed. That is a property worth
stating, because it decouples the record change from the schema change.

### Grafana

One panel per dashboard reads the sequence-gap shape and partitions on the
channel:

- `demo/grafana/dashboards/marketbyorder.json`, panel 20, "Sequence gaps
  (per-instrument)": `PARTITION BY channel_id, instrument_id`
- `demo/grafana/dashboards/marketbyprice.json`, panel 20, same title, same
  partition

Both become `PARTITION BY source_addr, dst_port, channel_id, instrument_id`.
Without that, `lagInFrame` over `per_instrument_seq` compares one instance's
sequence to the other's and reports the difference as missing messages — the
same defect the panel exists to detect, expressed as a false positive. No
dashboard template variable filters on `channel_id`
(`marketbyorder.json` and `marketbyprice.json` both declare `symbol`,
`candle_interval`, `show_trades`), so nothing else in Grafana moves.

### Outside the blast radius

- **`go/topofbook-bot` and `go/topofbook-parser`.** The top-of-book pair keeps
  its own `Record` (`go/topofbook-parser/tob/parser.go:6`,
  `go/topofbook-bot/record.go:7`) and maintains no per-instance recovery state:
  it has no snapshot cycle and no `Reset Count` handling to key. It is touched
  in one place only, `buildInsertURL`, for the insert setting, because that
  function is the third copy of the path the setting has to be pinned in.
- **`rust/recorder`.** Already keyed on the channel instance throughout, and the
  precedent this design follows.
- **The wire format.** Nothing here changes a datagram. `Channel ID` is byte 3
  of the datagram header and stays a `u8`; the source IP address and the
  destination port are properties of the UDP datagram that carried it, read from
  the socket and never from the payload.

## Migration and compatibility order

Records flow between separately deployed processes: `demo/docker-compose.yml`
builds `dz/marketbyorder-parser`, `dz/marketbyorder-bot`,
`dz/marketbyprice-parser` and `dz/marketbyprice-bot` as four images, and each
parser hands records to its book-builder over a unix socket. So the two sides
roll separately and the order is part of the design.

**Three steps, in this order.**

1. **The schema leads.** Both init files, the `002` migration, and
   `input_format_skip_unknown_fields=0` in all three insert paths. No binary
   writes the new column yet, and none names an unknown field, so this step is
   invisible to a running stack. It must be first: the setting is what makes
   step 3's ordering error loud, and pinning it after a row already carries the
   key would refuse that row instead of catching the mistake.
2. **The parsers emit.** `channelInstance`, the re-keyed trackers, and the
   stamp. Book-builders on the old build ignore the two unknown JSON keys —
   `encoding/json` drops them — so a parser ahead of its book-builder is inert.
3. **The book-builders require.** The re-keyed structures, the continuity check,
   and the row key. A book-builder ahead of its parser reads the zero
   `netip.Addr` and port `0`, keys every instance of a channel together, and
   behaves exactly as it does today — which is why
   `unidentified_records_total` exists rather than a hard refusal: a stack
   mid-deploy must keep serving, and the counter is what makes the window
   visible instead of silent. "Exactly as it does today" is load-bearing and has
   two consequences in the code: the continuity check does not run on such a
   record, and its rows carry the serialized sentinel rather than the zero
   `netip.Addr`, so the batch still loads.

**Parsers before book-builders, never the other way round**, and the reason is
asymmetric tolerance rather than preference. A parser ahead of its book-builder
emits fields nobody reads, which `encoding/json` discards by construction. A
book-builder ahead of its parser reads fields nobody writes, and the zero value
of both is a valid map key — so it keys on a sentinel and the defect is
reintroduced with no error anywhere. One direction is inert; the other is
silent.

**Rollback.** Step 3 rolls back to step 2's behaviour: rows omit the two
columns, which load as the type's default under
`input_format_defaults_for_omitted_fields`, and keying returns to the
`Channel ID`. Step 2 rolls back to step 1. Step 1 does not roll back — a column
is additive and the setting refuses only the direction that was already wrong.

## Testing strategy

- **The key itself.** `seqTracker.observe` driven with two `channelInstance`
  values differing only in `port`: independent baselines, and an alternation
  between them reporting no loss. Extends `TestSeqTracker` in both parsers.
- **The stamp.** `stampInstance` over a slice of records asserts both fields on
  every one, including the record types that carry no `instrument_id`.
- **Round trip.** A record encoded by `JSONFileSink` and decoded by each
  book-builder's `Record` carries the same `netip.Addr` and port. This is the
  cross-module contract and the one thing no single module's suite covers.
- **Two instances of one channel, end to end in-process.** The property under
  test, and it is the same shape in both book-builders: feed two instances
  differing only in `dst_port`, carrying one `Channel ID` and one
  `Instrument ID`, with interleaved `SnapshotBegin`/levels/`SnapshotEnd` groups
  and differing steady `Reset Count` values. Assert two books, two open groups,
  no reset barrier, and every level filed against the group its own instance
  opened.
- **Reset isolation.** A `Reset Count` change on one instance wipes that
  instance and spares the other instance of the same channel. This extends the
  two existing pairs — `TestDispatch_ResetOnOneChannelSparesTheOther` and
  `TestDispatch_InterleavedChannelsWithDistinctResetCountsRunNoBarrier`, present
  in both `go/marketbyorder-bot/coordinator_test.go` and
  `go/marketbyprice-bot/coordinator_test.go` — from two channels to two
  instances of one channel, which is the case neither covers. The spared side is
  asserted on the pending rows as well as on the book: the other instance's
  instrument is left dirty in the `SnapshotWriter` and its `level_snapshots`
  rows must still be written after the reset.
- **Manifest isolation.** In `marketbyprice-bot`, a `manifest_summary` on one
  instance with a raised `Manifest Seq` leaves the other instance's instruments,
  books and buffered deltas standing, and still prunes its own. The existing
  prune tests cover one instance only, so the isolation assertion is new.
- **The continuity check.** A `snapshot`-port sequence discontinuity drops that
  instance's open group and not the other instance's; a `refdata`-port
  discontinuity drops nothing; a reorder (`seq <= last`) drops nothing; first
  sight of an instance reports no gap; and a run of unidentified records
  alternating between two paths raises `unidentified_records_total`, raises no
  gap count and drops no group.
- **Fixtures give the two instances disjoint sequence ranges.** Every test that
  interleaves two instances does so with sequence numbers that do not overlap —
  one instance low, the other far above it. Two instances at the same sequence
  numbers, folded onto one key, read as reorders and duplicates rather than as
  discontinuities, and `seq <= last` is ignored: a channel-keyed implementation
  passes such a fixture and the test asserts nothing.
- **The insert setting.** The query string of a posted batch carries
  `input_format_skip_unknown_fields=0`, asserted on the value and not only on
  the key, in all three paths. `TestBuildInsertURL`
  (`go/topofbook-bot/clickhouse_test.go:16`) is the precedent and the other two
  already stand up an `httptest` server the query string is readable from.
- **The row key.** Each writer's row map carries `source_addr` and `dst_port`
  with the record's values, per table — and, for an unidentified record, the
  **encoded batch** carries `"source_addr":"0.0.0.0"` rather than
  `"source_addr":""`. Asserting on the row map alone would pass against a map
  holding a zero `netip.Addr`, which is the value that fails the insert, so the
  assertion is on the body the batcher posts to the `httptest` server.
- **The DDL.** There is no automated suite over `demo/clickhouse/`. Said plainly
  rather than covered by something that would look like a gate: the check is
  `clickhouse-client --multiquery` over both init files against the pinned 24.8,
  then the `002` migration against a volume created from the previous init
  files, then `DESCRIBE TABLE` on all ten. The `002` migration and the init
  files are two statements of one fact, and applying only one of them is the
  error to look for.
- **Race detector.** The two-instance in-process test under `-race` in both
  book-builders, since the instance key now reaches map operations on both the
  coordinator goroutine and every shard goroutine.

## Decisions

| | |
|---|---|
| `source_addr` / `dst_port`, not `source_ip` / `port_number` | The recorder's columns already use these, in the same ClickHouse server. |
| The existing metric label stays `source_ip` | `dz_mbo_parser_datagram_seq_gaps_total{port,source_ip,channel_id}` and its market-by-price twin keep their label names. Renaming a label breaks every dashboard and alert reading them for no keying benefit; a rename is a separate change with its own transition. |
| `netip.Addr`, not `string` | Comparable, no allocation per datagram, and it is what `pubKey` already holds. |
| `channelInstance` per module, not in `go/internal` | Three of the five modules have no dependency on `go/internal`; `Record` is already duplicated four times on the same reasoning. |
| `seqLast` read, not deleted | Sequence continuity on the `snapshot` port is the only discriminator between two groups sharing a `Snapshot ID`, and the field is already the right shape once it is keyed. |
| The four comments deleted | The glossary is the authority and says two paths may carry one channel. A comment asserting the opposite cannot stay beside code that keys on the instance. |
| `snapshotRoute` deleted rather than re-keyed | Re-keying fixes the collapse between instances and leaves the collapse within one, which `marketbyprice-bot`'s open-group shape already solves. |
| No `port_role` column | Recoverable from `dst_port` for the operator holding the port assignment. |
| No `ORDER BY` change | ClickHouse cannot prepend a sort-key column; a rebuild is a separate change. |
| Schema, then parsers, then book-builders | The only order in which no live process reads a field the other side is not writing, and no insert is silently accepted with a field the table lacks. |
| Writers serialize `"0.0.0.0"`, they do not omit the key | A `DEFAULT` applies only to a column the row does not name, and a zero `netip.Addr` names it with `""`, which the `IPv4` parser refuses and which fails every row in the same batch. One row shape per table is also the property a test can assert. |
| An unidentified record skips the continuity check | Two instances folded onto one key are two interleaved sequence series; checked, they report a discontinuity on nearly every datagram and no snapshot cycle completes. The degraded window must behave as the tree behaves today. |
| The manifest and its prune key on the instance | `pruneManifest` deletes by `Manifest Seq` alone, so one path's bump evicts the other path's instruments — a wipe with no `Reset Count` behind it and no counter accounting for it. |
| `SnapshotWriter.Reset` takes the instance | It replaces `dirty` and `lastWrittenAt` whole and bumps one generation, so a reset on one instance drops pending rows for a book that never reset. |
| `instruments` keeps one row per channel | The sort key is `(channel_id, instrument_id)` and a `ReplacingMergeTree` collapse follows it. The columns name the surviving row's instance; keeping both is a table rebuild. |

## Out of scope / non-goals

- **Arbitrating between instances of one channel.** The glossary separates the
  concerns explicitly: "Arbitrating between instances of the same channel is a
  separate concern from sequencing within one." Two instances therefore remain
  two books, two shadow states and two sets of rows, exactly as two `Channel
  ID`s do today — so this change is a refinement of the existing cardinality
  rather than an increase in it, and it introduces no requirement for an
  arbiter. Folding them is future work, and the recorder's `book_top` records why
  the fold is the interesting design:
  "`source_addr` and `dst_port` are absent because a book is one book whichever
  redundant path delivered the message that moved it"
  (`docs/superpowers/specs/2026-09-05-recorder-market-data-rows-design.md:489`).
- **Binding more than one destination port per port role.** `Runner` takes three
  port numbers (`go/marketbyorder-parser/main.go:20-22`) and opens one socket
  each, so one parser process still sees one channel instance per port role. The
  record shape stops assuming that; making a parser bind a second `mktdata` port
  is a separate change that this one is the prerequisite for.
- **Renaming `Port` to `PortRole` on the record.** The field holds a port role
  and is named for a port, which is a glossary finding on its own. It is a
  cross-process JSON key rename with its own transition, and bundling it here
  would put a compatibility question that has nothing to do with keying inside
  the one change that must not get its rollout order wrong.
- **The `instruments` sort key.** Stays `(channel_id, instrument_id)`, so the
  `ReplacingMergeTree` collapse still keeps one row per channel. The columns
  record which instance wrote it; the collapse is a table rebuild. Adding
  `source_addr` and `dst_port` to the key means creating the table afresh,
  copying every row into it and renaming — ClickHouse cannot prepend to a sort
  key in place — and the copy has to settle what a pre-migration row's instance
  is, which is the same question the sentinel answers by refusing to. Until then
  an operator reading `instruments` for exponents reads one instance's
  definition for both, while `level_snapshots` beside it keeps both instances'
  rows. Success criterion 3 is scoped around exactly that.
- **The parser metric label rename.** See *Decisions*.
- **`go/topofbook-*` keying.** No per-instance recovery state to key.
