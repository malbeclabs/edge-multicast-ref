# Channel-instance keying — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Key gap detection, recovery state, `Reset Count` and the snapshot cycle
on `(source IP address, Channel ID, destination port)` — the channel instance —
in both parsers and both book-builders, by carrying that identity on the record
instead of dropping it at the parser boundary.

**Design:** `docs/2026-09-22-channel-instance-keying-design.md`

**Tech stack:** Go 1.25, five `package main` modules under `go/` plus
`go/internal`, Prometheus client, standard `testing`. ClickHouse 24.8 as pinned
by `demo/docker-compose.yml`. Grafana 11.2.0 dashboards as JSON. No new
dependency anywhere; `netip` is standard library and already imported by both
parsers' `runner.go`.

---

## Scope

Thirteen tasks, all in this repository. No wire-format change at any point: the
source IP address and the destination port are properties of the UDP datagram
read from the socket, never of the payload.

The worked case throughout is the design's: **two channel instances of one
`Channel ID`, on distinct destination ports, carrying one `Instrument ID`** —
against the one instance per `Channel ID` the tree assumes today.

---

## The ordering constraint, which is the whole shape of this plan

Records cross a process boundary. `demo/docker-compose.yml` builds each parser
and each book-builder as its own image, and they meet over a unix socket, so the
two halves roll separately. The design's three-step order is not a preference:

| Step | Landed out of order | What a running stack does |
|---|---|---|
| The schema and the insert setting (tasks 1–3) | after a row carries the key | the batch is refused instead of the mistake being caught, or — without the setting — the row lands `200` with the column unwritten and nothing says so |
| The parsers emit (tasks 4–6) | after the book-builders require | the book-builder keys every instance of a channel on a zero `netip.Addr` and port `0`, behaves exactly as it does today, and reports nothing |
| The book-builders require (tasks 7–12) | — | correct |

The direction is asymmetric by construction and that is what makes it safe.
`encoding/json` drops an unknown key (`json.Unmarshal` at
`go/marketbyorder-bot/bot.go:90`, a `json.Decoder` at
`go/marketbyprice-bot/bot.go:111`, neither calling `DisallowUnknownFields`), so a
parser ahead of its book-builder is **inert**. The zero value of both new fields
is a valid map key, so a book-builder ahead of its parser is **silent**. Task 12
adds the counter that makes that window visible, and it is in the same task as
the row key for that reason.

**Tasks 1–6 change no observed behaviour.** The suite that passes today must
still pass, unchanged, at the end of every one of them. That is what makes them
independently gateable.

---

## Global constraints

- **Vocabulary:** `.github/skills/code-review/GLOSSARY.md` at `glossary/v1.3.0`
  governs every identifier, comment, test name, column name, metric name, log
  field and commit message. `channel instance` for the unit;
  `channel` only for the `Channel ID` shard; `port role` with the three tokens
  `mktdata`, `refdata`, `snapshot` verbatim; `datagram` never `frame`; `era`
  never `epoch`; `book-builder` for the binary and `book engine` where the
  in-process component is meant; `source` never bare — `source_addr`,
  `source_id`, `source IP address` are the qualified forms this plan uses; and
  the word the glossary bans outright in every sense stays out of prose,
  identifiers, test names and comments alike.
- **The glossary is the authority over a local comment.** Four comments in the
  tree assert the opposite of the Transport table and task 13 removes them. A
  task that finds itself re-arguing them has hit the boundary this plan exists
  to move.
- **`gofmt` before every commit.** `cd go/<module> && gofmt -w .` then confirm
  `gofmt -l .` prints nothing. The Go blocks in this plan are logically exact
  but their inline-comment whitespace is not guaranteed `gofmt`-clean.
- **Tests run per module, never with a shared target directory.**
  `cd go/<module> && go test ./... -run <Name> -v`. `go/go.work` spans nine
  modules; a build driven from the workspace root reports one module's error
  against another's change.
- **Every test must be shown to kill its mutant.** Revert the change, watch the
  new test fail, restore it. Each task below names its mutant explicitly. This
  plan adds several tests whose subject is an absence — a barrier that does
  *not* fire, a group that is *not* overwritten — and a test of an absence that
  passes against the unfixed tree asserts nothing.
- **No network, no privilege, no venue, in any test in this plan.** Every unit is
  reachable with what the suites already have: in-process shards, `httptest`
  servers, and record literals. `Runner.receive` needs a bound multicast socket
  and is deliberately not a test subject — task 5 extracts the seam instead.
- **No venue names and no host counts.** This repository is public. Example
  addresses come from the documentation range (`198.51.100.0/24`), not from a
  private or multicast range.
- **Two instances in a fixture get disjoint sequence ranges.** Every test that
  interleaves two channel instances gives them sequence numbers that do not
  overlap — one instance from 1, the other from 1,000,000. Identical or
  overlapping ranges make the folded key read as reorders and duplicates, and
  `seq <= last` is ignored by design, so a `Channel ID`-keyed implementation
  passes the fixture and the test asserts nothing. The same applies to
  `Reset Count`: differing steady values, not one shared value.
- **Commit before any step that rewrites files in place**, and in particular
  before task 3's DDL run against a container volume.
- **Commit messages:** all lowercase, no `Co-Authored-By`, no attribution
  footer of any kind, per the repository's own rules.

---

## The pieces where the obvious implementation is the wrong one

Stated up front, because each was found by reading the code and each is a task
below that would otherwise be written wrong.

- **`input_format_skip_unknown_fields` must be pinned before any row carries the
  new key, not with it.** It defaults to `1`, and at `1` an insert naming a
  column the table does not have is answered `200` and the field is discarded
  (measured against 24.8 at
  `rust/recorder/dz-recorder-clickhouse/src/config.rs:195-226`). Pinning it in
  the same task as the row key would refuse the first mis-ordered batch instead
  of making the ordering impossible to get wrong.
- **Adding a field to `Record` adds no ClickHouse column.** Every row is an
  explicit `map[string]any` literal (`go/marketbyorder-bot/events_writer.go:28-46`),
  so the record change and the row change are genuinely separable. A task that
  assumes the rows follow the struct will find nothing to do and conclude
  wrongly that the rows are done.
- **`Parser.ParseDatagram` keeps its signature.** One `Parser` is shared by all
  three port-role goroutines, so per-goroutine state on it is a data race — the
  reason `marketbyprice-parser` returns `Defects` rather than accumulating them
  (`go/marketbyprice-parser/runner.go:223-225`). The stamp goes in the runner,
  beside `RecvTSNS`, not in the decoder.
- **`marketbyorder-bot`'s `snapshotRoute` is deleted, not re-keyed.** Not
  because two instruments hold one `Snapshot ID` at once — the publisher MUST
  NOT interleave snapshot groups within an instance
  (`go/marketbyprice-bot/coordinator.go:26-27`,
  `docs/2026-04-23-marketbyorder-plan.md:3066-3068`) and `Dispatch` deletes the
  route at `snapshot_end` (`go/marketbyorder-bot/coordinator.go:85`), so within
  one instance and with no loss the id is unambiguous. It goes because the
  instrument is found by a **search**, not by the group:
  `Shard.applySnapshotOrder` (`shard.go:184-200`) scans every instrument the
  shard owns for a matching open `Snapshot ID`, with no channel or instance
  filter, and map iteration order picks between matches. Two instances of one
  channel publish the same ids at the same time, so two matches on one shard is
  the steady state. Re-keying the route leaves that scan in place; routing by
  the open group and stamping the instrument removes it.
- **`ORDER BY` cannot gain a prepended column.** The migration adds columns and
  leaves every sort key alone. A task that tries to put `source_addr` at the
  front of `marketbyorder.instruments`' key is attempting a table rebuild.
- **A `DEFAULT` never rescues a column the row names.** A row is a
  `map[string]any` encoded with `encoding/json`, and a zero `netip.Addr` in it
  encodes as `""`, which the `IPv4` parser refuses — and one refused row fails
  every row batched with it, because `send` posts the whole batch as one body
  (`go/internal/clickhouse/client.go:202-208`). Task 12 serializes the sentinel
  explicitly. A task that puts `rec.SourceAddr` into the row map and relies on
  `DEFAULT toIPv4(0)` produces the unloadable batch this plan exists to avoid.
- **`SnapshotWriter.Reset` is not scoped to the reset.** It replaces `dirty` and
  `lastWrittenAt` whole and bumps one generation
  (`go/marketbyorder-bot/snapshot_writer.go:92-96`,
  `go/marketbyprice-bot/snapshot_writer.go:134-142`). Narrowing `resetChannel`'s
  loops to the instance and leaving that call alone spares the other instance's
  book and still drops its pending `level_snapshots` rows.
- **`marketbyprice-bot`'s manifest is one value for the whole process, and its
  prune has no key at all.** `applyManifest` broadcasts `msgManifestPrune` with
  only a `Manifest Seq` (`coordinator.go:200-220`) and `Shard.pruneManifest`
  walks all of `refdata` (`dispatch.go:303-329`). Once `instKey` carries the
  instance, one path's manifest bump deletes the other path's instruments unless
  the manifest is keyed too. Task 11 covers it.

---

## File Structure

**Create**

- `demo/clickhouse/migrations/002_add_channel_instance_columns.sql` — the
  `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` set for the ten tables, for volumes
  that predate this change.

**Modify**

- `demo/clickhouse/init/02_schema_mbo.sql`, `demo/clickhouse/init/03_schema_mbp.sql`
- `go/internal/clickhouse/client.go` + `client_test.go`
- `go/marketbyorder-bot/clickhouse.go` + `clickhouse_test.go`
- `go/topofbook-bot/clickhouse.go` + `clickhouse_test.go`
- `go/marketbyorder-parser/`: `runner.go`, `parser.go`, `seqtracker_test.go`,
  `sink_json_test.go`, `README.md`
- `go/marketbyprice-parser/`: the same set, plus `README.md`'s JSONL example
- `go/marketbyorder-bot/`: `main.go`, `record.go`, `coordinator.go`, `shard.go`,
  `events_writer.go`, `snapshot_writer.go`, `metrics.go`, `main_test.go`,
  `coordinator_test.go`, `shard_test.go`, `snapshot_writer_test.go`,
  `events_writer_test.go`, `bot_test.go`, `parity_test.go`, `README.md`
- `go/marketbyprice-bot/`: `main.go`, `record.go`, `coordinator.go`, `shard.go`,
  `dispatch.go`, `events_writer.go`, `snapshot_writer.go`, `metrics.go`,
  `coordinator_test.go`, `dispatch_test.go`, `shard_test.go`,
  `snapshot_writer_test.go`, `events_writer_test.go`, `bench_buffer_test.go`,
  `README.md`
- `demo/grafana/dashboards/marketbyorder.json`, `demo/grafana/dashboards/marketbyprice.json`
- `docs/README.md`

`main.go` is in both lists for a reason a compiler finds before a reviewer does:
`go/marketbyorder-bot/main.go:94` holds the literal `instKey{0, instID}` inside
the `withInstrument` closure, and `:90` calls
`NewSnapshotWriter(enq, *depth, *coalesceMS, metrics, 0, …)` — a fixed
`Channel ID` of `0` where the writer now needs the instance. Its market-by-price
twin is `go/marketbyprice-bot/main.go:123`. `go/marketbyorder-bot/main_test.go:63`
constructs the same writer. The `instKey` literals are wider than the inventory
above suggests, so the gate is the full suite in every module and `go build ./...`
in both book-builders, not a file list:
`go/marketbyorder-bot/shard_test.go` and `parity_test.go` hold `instKey{0, …}`
literals, and `go/marketbyprice-bot/shard_test.go`,
`snapshot_writer_test.go`, `dispatch_test.go` and `coordinator_test.go` hold
theirs.

**Delete**

- `pubKey` in both parsers' `runner.go`; `snapKey` and `Coordinator.snapshotRoute`
  in `go/marketbyorder-bot`; the four comments listed in task 13.

---

## Task 1: The insert paths refuse a field the table has no column for

**Files:** `go/internal/clickhouse/client.go` + `client_test.go`,
`go/marketbyorder-bot/clickhouse.go` + `clickhouse_test.go`,
`go/topofbook-bot/clickhouse.go` + `clickhouse_test.go`

First, and before any schema or row change, because this is the gate that makes
every later ordering error loud. Three copies of one insert path:
`go/internal/clickhouse/client.go:213` (used by `marketbyprice-bot`),
`go/marketbyorder-bot/clickhouse.go:164`, and `buildInsertURL` at
`go/topofbook-bot/clickhouse.go:291`. Each posts
`INSERT INTO <table> FORMAT JSONEachRow` with no column list and sets no input
format setting.

- [ ] **Step 1: Write the failing test** in each of the three modules. The
  existing batcher tests already stand up an `httptest` server whose handler can
  read `r.URL.Query()`; `TestBuildInsertURL`
  (`go/topofbook-bot/clickhouse_test.go:16`) asserts the URL directly. Assert
  the **value**, `input_format_skip_unknown_fields=0`, not the key alone — a
  test that looks for the name would pass over `=1`.
- [ ] **Step 2: Run, watch it fail.** The query string carries `database` and
  `query` only.
- [ ] **Step 3: Add `q.Set("input_format_skip_unknown_fields", "0")`** beside the
  existing `q.Set` calls in all three.
- [ ] **Step 4: Full suite in all three modules**, then commit.

> **The mutant is the default.** Set the value to `1`, or drop the `q.Set` line,
> and all three tests must fail. If one passes, it is asserting on a URL it built
> itself rather than on the one the batcher posts. The behaviour under test is
> that a loader ahead of its migration is **refused** rather than answered `200`
> with the field discarded, which is the one direction of a rolling deploy whose
> symptom is silence.

---

## Task 2: The columns, in both init files

**Files:** `demo/clickhouse/init/02_schema_mbo.sql`,
`demo/clickhouse/init/03_schema_mbp.sql`

Ten tables gain two columns:

```sql
    source_addr        IPv4 DEFAULT toIPv4(0),
    dst_port           UInt16 DEFAULT 0,
```

- `marketbyorder.instruments`, `events`, `level_snapshots`, `wire_snapshots`,
  `channel_health`
- `marketbyprice.instruments`, `events`, `level_snapshots`, `wire_levels`,
  `channel_health`

No `port_role` column: it is recoverable from `dst_port` for an operator holding
the feed's port assignment. No `ORDER BY` change on any table.

- [ ] **Step 1: Add the columns**, placed immediately after `channel_id` in each
  table so the identity block reads as a block.
- [ ] **Step 2: Annotate the forward-only cost** in a comment on each
  `level_snapshots`, in the same form
  `demo/clickhouse/init/02_schema_mbo.sql:85` already uses for `stale`: a row
  written before this change reads `0.0.0.0` and `0`, and there is no honest
  default for a row nobody stamped.
- [ ] **Step 3: Confirm no view is re-stated.** `grep -rn -i VIEW demo/clickhouse/`
  must return nothing. The recorder's house rule is that a view's `SELECT *` is
  expanded at creation, so a column added to a table forces every such view to be
  re-created
  (`rust/recorder/dz-recorder-clickhouse/db/clickhouse/010_recorder_book_key.sql:259-269`).
  These two databases hold ten tables and no views, so the rule is satisfied by
  there being nothing to re-state — recorded here because it was checked, not
  assumed.
- [ ] **Step 4: Commit.**

> **The mutant is a table left out.** There is no automated suite over
> `demo/clickhouse/`, said plainly rather than covered by a check that would look
> like a gate. The manual gate is task 3's: `DESCRIBE TABLE` on all ten. A table
> missed here writes its rows through an insert that task 1 now refuses, so the
> omission surfaces as a refused batch naming the table rather than as silence —
> which is the whole reason task 1 is first.

---

## Task 3: The migration, for volumes that predate the change

**Files:** `demo/clickhouse/migrations/002_add_channel_instance_columns.sql`

The init files run from `/docker-entrypoint-initdb.d` on first container boot and
are skipped afterwards (`demo/clickhouse/init/01_schema.sql:2-3`), which is why
`001_add_stale_to_level_snapshots.sql` exists. The init change and this file are
two statements of one fact and both are required.

- [ ] **Step 1: Write the twenty `ALTER` statements**, in the form `001` uses:
  `ALTER TABLE <db>.<table> ADD COLUMN IF NOT EXISTS source_addr IPv4 DEFAULT toIPv4(0);`
  and the `dst_port` twin, for the ten tables of task 2.
- [ ] **Step 2: Header comment** naming the ordering rule this file belongs to:
  the schema leads the binary, and the setting pinned in task 1 is what enforces
  it.
- [ ] **Step 3: Verify against the pinned server.** `clickhouse-server:24.8` as
  `demo/docker-compose.yml:157` pins it. Two runs:
  (a) a fresh volume loaded from the modified init files, then `DESCRIBE TABLE`
  on all ten — every one carries both columns;
  (b) a volume created from the **pre-change** init files, then this migration,
  then the same `DESCRIBE TABLE` — every one carries both columns, and a row
  inserted before the migration reads back `0.0.0.0` and `0`.
- [ ] **Step 4: Commit.** Commit before step 3, not after: that step rewrites a
  container volume.

> **The mutant is applying only one of the two files.** Revert task 2's init
> change and run (a): the fresh volume lacks the columns and `DESCRIBE TABLE`
> says so. Revert this file and run (b): the upgraded volume lacks them. Each
> file covers exactly one of the two deployments and neither covers the other,
> which is why a single-file version of this change reads as done and is not.

---

## Task 4: `channelInstance`, and the parsers' trackers keyed on it

**Files:** `go/marketbyorder-parser/runner.go` + `seqtracker_test.go`,
`go/marketbyprice-parser/runner.go` + `seqtracker_test.go`

`pubKey{src netip.Addr, ch uint8}` (`runner.go:31` in both) becomes:

```go
// channelInstance is one path's view of one channel: the unit that owns a
// sequence series, a Reset Count and a snapshot cycle. Keyed on the source IP
// address, the Channel ID and the destination port, per GLOSSARY.md's Transport
// table. Two redundant paths may carry one Channel ID, so the Channel ID alone
// is not the unit.
//
// netip.Addr rather than a string: comparable, usable directly as a map key,
// and no allocation per datagram.
type channelInstance struct {
	addr netip.Addr
	ch   uint8
	port uint16
}
```

`seqTracker.last` becomes `map[channelInstance]uint64` and `observe` takes
`(inst channelInstance, seq uint64)`. Its existing semantics are unchanged and
are the model the book-builders' check follows in task 10: first sight
establishes the baseline silently; `seq <= last` is a reorder or a duplicate and
returns `(0, 0)`; `seq > last+1` returns the gap and its magnitude.

Within one `receive` goroutine `port` is constant, so the field is redundant
there. That is deliberate: the type becomes the one definition of the channel
instance that the stamp, the tracker and both book-builders share, rather than a
two-field key whose third dimension is implied by a goroutine's lifetime.

- [ ] **Step 1: Extend `TestSeqTracker`** in both parsers with a case driving
  `observe` with two `channelInstance` values differing **only** in `port` —
  same `netip.Addr`, same `Channel ID`. Each establishes its own baseline
  silently, and an alternating interleave of the two ascending series reports
  zero gaps and zero missing datagrams.
- [ ] **Step 2: Run, watch it fail** — `channelInstance` is undefined, and once
  declared, the two-field key collapses the two.
- [ ] **Step 3: Replace the type and the signature.** `receive` builds the key
  once per datagram from `src`, `ch` and its own port number, which arrives in
  task 5; until then it passes the role's configured port by threading
  `portConfig` through, which is the same edit and may be done here.
- [ ] **Step 4: Full suite in both parsers**, `gofmt`, commit.

> **The mutant is the missing field.** Remove `port` from `channelInstance` and
> the new case must fail: the two ports share one entry, the second port's first
> datagram is read against the first port's `last`, and the alternation reports
> loss on nearly every datagram. That false-loss report is the defect the field
> exists to prevent, so a test that still passes without it is testing the map
> and not the key.

---

## Task 5: The runner stamps the instance on every record

**Files:** `go/marketbyorder-parser/runner.go`, `go/marketbyprice-parser/runner.go`,
`go/marketbyorder-parser/parser.go`, `go/marketbyprice-parser/parser.go`

`Record` gains, in both parsers:

```go
	SourceAddr     netip.Addr     `json:"source_addr"`
	DstPort        uint16         `json:"dst_port"`
```

No `omitempty` on either: an absent identity must be visibly absent rather than
indistinguishable from a record nobody stamped.

The stamp goes where `RecvTSNS` and `RecvTSKind` already go —
`go/marketbyorder-parser/runner.go:224-229`,
`go/marketbyprice-parser/runner.go:240-245` — and that loop becomes a named
function so it has a seam the suite can drive. `Runner.receive` needs a bound
multicast socket and stays untested.

```go
// stampInstance stamps receive-side identity onto every record of one datagram:
// the channel instance it arrived on, and when and how the receive time was
// taken. The decoder cannot do this — one Parser is shared by all three
// port-role goroutines, so anything per-goroutine held on it is a data race.
func stampInstance(records []Record, inst channelInstance, recvNS uint64, recvKind string) {
	for i := range records {
		records[i].SourceAddr = inst.addr
		records[i].DstPort = inst.port
		records[i].RecvTSNS = recvNS
		records[i].RecvTSKind = recvKind
	}
}
```

`Parser.ParseDatagram` keeps its signature in both parsers
(`go/marketbyorder-parser/parser.go:31`, `go/marketbyprice-parser/parser.go:39`).
The metric and latency observations stay in `receive`'s loop; only the field
assignment moves.

- [ ] **Step 1: Write the failing test** in both parsers: `stampInstance` over a
  slice holding one record of each shape — one with an `instrument_id`, one
  without (`heartbeat`), one `snapshot_order`/`snapshot_level` — asserts both new
  fields on every element, and that `RecvTSNS`/`RecvTSKind` are still set.
- [ ] **Step 2: Run, watch it fail** — undefined function.
- [ ] **Step 3: Add the fields, add `stampInstance`, call it from `receive`,**
  and pass the whole `portConfig` into `receive` (`runner.go:144-148`) so the
  destination port number reaches the goroutine. Keep the `Label` in the metric
  label positions unchanged.
- [ ] **Step 4: Full suite in both parsers**, `gofmt`, commit.

> **The mutant is the record without an `instrument_id`.** Guard the assignment
> on `records[i].InstrumentID != 0` and the test must fail on the `heartbeat`
> element. A `heartbeat` and a `manifest_summary` are channel-scoped and carry no
> instrument, and they are exactly the records the coordinator reads `Reset Count`
> and the sequence number from — an identity stamped only on instrument records
> would leave the reset barrier keyed on nothing.

---

## Task 6: The record's two fields on the book-builder side, and the round trip

**Files:** `go/marketbyorder-bot/record.go`, `go/marketbyprice-bot/record.go`,
`go/marketbyorder-parser/sink_json_test.go`,
`go/marketbyprice-parser/sink_json_test.go`,
`go/marketbyorder-bot/bot_test.go`, `go/marketbyprice-bot/bot_test.go`

The same two fields, with the same JSON keys, on both book-builders' `Record`
(`go/marketbyorder-bot/record.go:5`, `go/marketbyprice-bot/record.go:5`). No
keying changes yet — after this task both book-builders decode the identity and
ignore it, which is a deliberate intermediate state and the last one that
changes no behaviour.

The sinks need no change: `JSONFileSink.Write` encodes `&records[i]` whole
(`go/marketbyorder-parser/sink_json.go:32`) and `SocketSink` marshals the same
struct.

- [ ] **Step 1: Write the failing tests.** Parser side: the JSONL line a
  `JSONFileSink` writes carries `"source_addr":"198.51.100.11"` and
  `"dst_port":19000`. Book-builder side: that same line, decoded into the
  book-builder's `Record`, yields the identical `netip.Addr` and port.
- [ ] **Step 2: Run, watch them fail.**
- [ ] **Step 3: Add the fields** to both book-builders' `Record`.
- [ ] **Step 4: Update `go/marketbyprice-parser/README.md:69`**, the one live
  JSONL example of the record shape, and the two parser READMEs' record-shape
  prose.
- [ ] **Step 5: Full suite in all four modules**, `gofmt`, commit.

> **The mutant is the JSON tag.** Rename the tag on either side — `source_ip` for
> `source_addr`, or `port_number` for `dst_port` — and the round-trip test must
> fail while each module's own suite still passes. That is the point of testing
> the round trip rather than each struct: the four `Record` declarations are
> duplicated across separate Go modules with no compiler relating them, so the
> JSON key is the only thing holding them together and nothing else in the tree
> checks it.

---

## Task 7: `marketbyorder-bot` — `resetCount` keyed on the channel instance

**Files:** `go/marketbyorder-bot/coordinator.go` + `coordinator_test.go`

`Coordinator.resetCount` (`coordinator.go:24`) becomes
`map[channelInstance]uint8`, and `Dispatch`'s barrier trigger
(`coordinator.go:53-58`) reads it by the instance the record was stamped with.
`runResetBarrier` (`coordinator.go:113`) takes the held record's instance and
adopts its `Reset Count` for that instance alone.

- [ ] **Step 1: Write the failing tests.** Two instances of one `Channel ID`
  differing only in `DstPort`, with differing but steady `Reset Count` values,
  interleaved: **no** barrier fires. Then a `Reset Count` change on one: exactly
  one barrier, and the other instance's `Reset Count` is untouched. These extend
  `TestDispatch_InterleavedChannelsWithDistinctResetCountsRunNoBarrier` and
  `TestDispatch_ResetOnOneChannelSparesTheOther`
  (`coordinator_test.go:363`, `:382`) from two channels to two instances of one,
  which is the case neither covers.
- [ ] **Step 2: Run, watch the first fail** — under the `Channel ID` key the
  alternation reads as a reset on every datagram and the barrier fires on each.
- [ ] **Step 3: Re-key the field**, and re-key `snapshotRoute`'s barrier-time
  clearing loop (`coordinator.go:135-139`) to match; it is replaced wholesale in
  task 8 but must compile here.
- [ ] **Step 4: Full suite**, `gofmt`, commit.

> **The mutant is the two-field key.** Key `resetCount` on `rec.ChannelID` again
> and the interleave test must fail with a barrier count in the thousands where
> zero was expected. Watch the *count*, not just the pass: a barrier that fires
> once looks like a legitimate first-sight baseline, and the defect is that it
> fires on every alternation.

---

## Task 8: `marketbyorder-bot` — the open snapshot group, per instance

**Files:** `go/marketbyorder-bot/coordinator.go`, `shard.go`,
`coordinator_test.go`, `shard_test.go`

`Coordinator.snapshotRoute map[snapKey]int` (`coordinator.go:27`) and
`snapKey` (`shard.go:24`) are **deleted**. In their place, the shape
`marketbyprice-bot` already proved (`go/marketbyprice-bot/coordinator.go:12-32`):

```go
open map[channelInstance]openGroup
```

with `openGroup{instrumentID uint32, snapshotID uint32, shard int}`. Registered
on `snapshot_begin`, cleared on `snapshot_end`, and used to **stamp** the
instrument onto each `snapshot_order` the way
`go/marketbyprice-bot/coordinator.go:124-126` does — the wire omits
`instrument_id` on a snapshot order because the containing `SnapshotBegin`
implies it.

`Snapshot ID` validates membership and is never the key.

**One open group per channel instance is sufficient, and the protocol says so.**
A publisher MUST NOT interleave snapshot groups within one channel instance
(`go/marketbyprice-bot/coordinator.go:26-27`,
`docs/2026-04-23-marketbyorder-plan.md:3066-3068`). Two instances of one channel
are two publishers and do interleave with each other, which is why the map is
keyed on `channelInstance` and not on `Channel ID`. Nothing in this task widens
the state to more than one open group per instance, and a task that finds itself
needing to has hit a protocol question this plan does not answer.

Two consequences:

- `Shard.applySnapshotOrder` (`shard.go:184-200`) stops scanning every instrument
  for one whose `OpenSnapshot.SnapshotID` matches, and resolves the instrument
  from the stamped record.
- `Shard.snapCtx` (`shard.go:66`) re-keys from `snapKey` onto `instKey`. Its only
  consumers are the `wire_snapshots` writes at `shard.go:458-460`, which need the
  group's symbol and exponents; with the instrument stamped, the instrument is
  the key.

`SnapshotOrderDroppedTotal` keeps its meaning: a snapshot order with no open
group for its instance is dropped and counted.

> **What (b) is not.** It is not two *instruments* mid-cycle at one
> `Snapshot ID` inside a single instance. That case is unreachable: the
> publisher does not interleave groups within an instance, and the route entry
> is deleted at each `snapshot_end` (`coordinator.go:85`), so the next group
> claims the id afresh and the unfixed tree passes such a test. The reachable
> within-one-instance case is a **lost** `snapshot_end` leaving a shadow open
> across the cycle boundary, and that is task 10's subject rather than this
> one's — the continuity check closes the group. This task's job is only that
> the instrument is resolved from the group rather than searched for.

- [ ] **Step 1: Write the failing tests.** (a) Two instances of one
  `Channel ID`, each opening a group for a different `Instrument ID`,
  interleaved: each instance's snapshot orders reach its own instrument's shadow
  and neither group is overwritten. Give the two instruments ids that land on
  **different shards** under `id % n`, so the misrouting a single route entry
  causes is deterministic rather than a matter of map order. (b) Two instances
  of one `Channel ID`, each opening a group for a different `Instrument ID`
  whose ids land on the **same** shard under `id % n`, both mid-cycle at the
  same `Snapshot ID` — two open shadows, one shard, one id, which is the steady
  state for redundant paths because `Snapshot ID` is monotonic per
  `(Channel ID, Instrument ID)` and both paths run the same cycles: each
  instance's orders reach the instrument its own group named, and the two
  shadows hold disjoint order sets. This case needs no key change in the shard
  and passes at the end of this task. (c) A snapshot order
  arriving after its `snapshot_end` is dropped and counted, not routed — the
  `marketbyorder` twin of
  `TestDispatch_StrayLevelAfterSnapshotEndIsDroppedNotRouted`
  (`go/marketbyprice-bot/coordinator_test.go:461`).
- [ ] **Step 2: Run, watch (a) and (b) fail** — (a) because one `Channel ID`
  gives one route entry and the second `snapshot_begin` overwrites the first,
  (b) because `applySnapshotOrder` scans for a matching open `Snapshot ID` and
  two shadows on that shard match, so the orders split between them in whatever
  order the map yields.
- [ ] **Step 3: Delete `snapshotRoute` and `snapKey`; add `open`;** stamp on
  `snapshot_order`; simplify `applySnapshotOrder`; re-key `snapCtx`; narrow
  `resetChannel`'s `snapCtx` loop (`shard.go:111-115`) to the instance.
- [ ] **Step 4: Full suite**, `gofmt`, commit.

> **Two mutants, and both must be killed.** Re-key `open` on `rec.ChannelID`
> alone: test (a) must fail, with the second instance's group having overwritten
> the first's and the first's orders filed into the second instrument's shadow.
> Then restore `applySnapshotOrder`'s scan over `s.instruments` for a matching
> `OpenSnapshot.SnapshotID`, keeping the per-instance route: test (b) must fail,
> because two shadows on one shard match one id and the orders land in whichever
> the scan reaches first. Killing only the first leaves the search that
> `go/marketbyprice-bot/coordinator.go:19-24` names as the open issue against
> this book-builder; killing only the second leaves the one this plan is for.
> If (b) passes intermittently under that mutant, it is reading map order rather
> than the fix: assert on both shadows' contents, not on one.

---

## Task 9: `marketbyorder-bot` — `instKey`, the reset marker and the SnapshotWriter on the channel instance

**Files:** `go/marketbyorder-bot/shard.go` + `shard_test.go`,
`snapshot_writer.go` + `snapshot_writer_test.go`, `main.go` + `main_test.go`,
`parity_test.go`

`instKey{ch uint8, id uint32}` (`shard.go:15`) becomes
`instKey{inst channelInstance, id uint32}`. Everything keyed by it follows:
`instruments`, `refdata`, `deltaBuf`, `snapCtx`, and `resetChannel`, whose
signature becomes `resetChannel(inst channelInstance)` (`shard.go:95`).

The shard's routing hash is unchanged: `int(rec.InstrumentID) % c.n`. One
`Instrument ID` still lands on one shard whichever instance carried it, so
per-instrument FIFO holds and the two instances' books are two entries in one
shard's maps rather than work on two shards.

**Three things the key change drags with it, and none of them compiles or
behaves correctly if left out.**

`shardMsg.ch uint8` (`shard.go:535`) becomes `inst channelInstance`, so the
reset marker names the instance to wipe and `resetChannel` cannot be reached
with a bare `Channel ID`.

`SnapshotWriter` is re-keyed. Today it holds `dirty map[uint32]*dirtyEntry`
beside a `channel uint8` fixed by its constructor (`snapshot_writer.go:24-26`),
`MarkDirty` takes a bare `uint32` (`:53`), `withInstrument` is
`func(uint32, func(*Instrument))` (`:25`), and every `level_snapshots` row it
writes carries `"channel_id": w.channel` (`:256`, `:273`) — which `main.go:90`
passes as the literal `0`. `dirty` becomes `map[instKey]*dirtyEntry`,
`MarkDirty` and `withInstrument` take an `instKey`, the `channelID` constructor
parameter goes, and the row's `channel_id` comes from the key. This is the shape
`marketbyprice-bot`'s writer already has, and its own comment says why
(`go/marketbyprice-bot/snapshot_writer.go:38-42`): a shard owns instruments
across every channel for its id-modulo, so an id-only key folds two books into
one entry and persists whichever flushed last.

`SnapshotWriter.Reset` is scoped. The shard calls it straight after the wipe
(`shard.go:505-511`), and `doReset` replaces `dirty` whole and bumps one
`generation` (`snapshot_writer.go:92-96`), so a reset on one instance discards
pending `level_snapshots` rows for every book on that shard. It becomes
`Reset(ctx, inst channelInstance)`, deleting only the `dirty` entries whose key
carries that instance; `generation` becomes per instance
(`map[channelInstance]uint64`), and `flushDue` compares the generation of the
instance whose batch it extracted.

`main.go:90-96` and `main_test.go:63` are updated with the constructor and the
closure. `parity_test.go:76-80` holds the same pair.

- [ ] **Step 1: Write the failing tests.** (a) Two instances of one
  `Channel ID` carrying one `Instrument ID`: two independent books, two
  independent per-instrument sequence positions, and a per-instrument gap on one
  raising `per_instrument_gaps_total` without demoting the other. (b) A
  `Reset Count` change on one instance: `resetChannel` wipes that instance's
  instruments, refdata and buffered deltas and leaves the other instance's
  `StatusReady` book standing. (c) The spared instance's **pending rows**
  survive that reset: mark both instances' instruments dirty, reset one, drive
  the writer's tick, and the spared instance's `level_snapshots` rows are
  enqueued. (d) A `level_snapshots` row carries the `channel_id` of the instance
  whose book it read, not `0`.
- [ ] **Step 2: Run, watch them fail** — (a) and (b) because one `instKey` means
  the two instances share one `Instrument`, one sequence position and one delta
  buffer, and one reset wipes both; (c) because `Reset` clears the whole map;
  (d) because the row takes `w.channel`, which is the constructor's `0`.
- [ ] **Step 3: Re-key `instKey` and thread the instance through** `apply`,
  `handle`, `bufferDelta`, `replayBuffer`, `refdataFor` and `resetChannel`; carry
  it on `shardMsg`; re-key the `SnapshotWriter` and narrow its `Reset`; update
  `main.go`, `main_test.go` and `parity_test.go`.
- [ ] **Step 4: Full suite, then `-race`**, `gofmt`, commit.

> **Three mutants.** Drop `inst` from `instKey`: (a) and (b) must fail twice
> over — the interleaved deltas of two instances are applied to one
> `Instrument`, so each instance's `per_instrument_seq` reads as a gap in the
> other's, and the reset takes both books. Restore `doReset`'s whole-map
> replacement while keeping the narrowed `resetChannel`: (c) must fail, and this
> is the mutant the obvious implementation leaves alive, because every other
> assertion in this task passes against it. Put the constructor's fixed channel
> back into the row: (d) must fail.
>
> The half that must *not* be wiped is the assertion to check most carefully in
> all three: a test asserting only that the right book was wiped passes against
> a `resetChannel` that wipes everything.

---

## Task 10: The datagram sequence continuity check, in both book-builders

**Files:** `go/marketbyorder-bot/coordinator.go` + `coordinator_test.go`,
`go/marketbyprice-bot/coordinator.go` + `coordinator_test.go`,
both `metrics.go`

`marketbyorder-bot`'s `Coordinator.seqLast` (`coordinator.go:26`) is **read**,
not deleted: it becomes `map[channelInstance]uint64` and drives a check.
`marketbyprice-bot` gains the same field and the same check — it has none today,
and `open` is what the check protects, which both book-builders hold.

Per record, when the record's port role is not `refdata` — the exemption the
parsers already apply at `go/marketbyorder-parser/runner.go:199`, because
refdata is low-rate periodic-retransmit traffic whose datagram-sequence gaps are
not a loss signal — **and when the record carries an instance identity at all**:

- First sight of an instance sets the baseline silently. Same reason as
  `seqTracker.observe` (`go/marketbyorder-parser/runner.go:46-53`): a newly
  appearing path must not report a phantom gap the size of its sequence.
- `seq <= last`: reorder or duplicate. Ignored, `last` unchanged.
- `seq > last+1`: a discontinuity on that instance. Count it, and **if the port
  role is `snapshot`, delete that instance's entry from `open`.**

A new counter in each: `datagram_seq_gaps_total` labelled `{port}`, under the
existing `dz_mbo_bot` / `dz_mbp_bot` namespaces.

**A record with the zero `netip.Addr` or `dst_port` 0 is excluded from the
check**, counted by task 12's `unidentified_records_total`, and otherwise
dispatched as it is today. This is the second half of the compatibility window
and it is not optional: a book-builder rolled ahead of its parser sees every
instance of a channel folded onto one key, and two ascending sequence series
interleaved under one key produce `seq > last+1` on very nearly every datagram.
Run through the check, that counts a phantom gap per datagram and — on the
`snapshot` port — deletes the open group on every datagram, so no snapshot cycle
ever completes. The window is meant to degrade to today's behaviour, and today
there is no check.

Dropping the open group on a `snapshot`-port discontinuity is why the field is
read. Lose the `SnapshotEnd` and the group stays open across the boundary into
the next cycle, which shares its `Snapshot ID` space; lose the `SnapshotBegin`
and a cycle nobody opened is routed by the previous cycle's group. The existing
`snapshot_id` mismatch check (`go/marketbyprice-bot/coordinator.go:106`) catches
neither, because in both cases the id *matches* a group that is open.

- [ ] **Step 1: Write the failing tests**, in both book-builders. (a) A
  `snapshot`-port discontinuity on one instance drops that instance's open group
  and leaves the other instance's standing; the next snapshot order on the first
  instance is dropped and counted. (b) A `refdata`-port discontinuity drops no
  group. (c) A reorder (`seq <= last`) drops nothing and leaves `last` unchanged.
  (d) First sight of an instance reports no gap. (e) A datagram lost on the
  `mktdata` port raises the counter and drops no group. (f) A run of records
  with the zero `netip.Addr` and `dst_port` 0, alternating between two ascending
  sequence series, raises no gap count and drops no group — and, once task 12
  lands, raises `unidentified_records_total` once per record. Give the two
  series disjoint ranges, per *Global constraints*; at overlapping ranges the
  folded key reads as duplicates and (f) passes against a check that is not
  excluded at all.
- [ ] **Step 2: Run, watch them fail** — in `marketbyorder-bot` because nothing
  reads the field, in `marketbyprice-bot` because there is no field.
- [ ] **Step 3: Re-key `seqLast`, add the check, add the counter,** and clear the
  instance's entry in `runResetBarrier` rather than clearing the whole map
  (`go/marketbyorder-bot/coordinator.go:140`).
- [ ] **Step 4: Full suite in both, then `-race`**, `gofmt`, commit.

> **Four mutants.** Delete the `open` deletion: (a) must fail, and this is the
> assertion that makes the field read rather than merely written — a version that
> only increments the counter passes every other test in this plan.
> Drop the `refdata` exemption: (b) must fail. Change `seq <= last` to `seq < last`
> or `seq != last+1`: (c) must fail. Drop the unidentified-record exclusion:
> (f) must fail, with a gap count in the thousands and every open group gone.
> Test (d) is the one to distrust — it passes against a check that does nothing
> at all — so run it against the deleted-check mutant and confirm it is (a) and
> not (d) that fails.

---

## Task 11: `marketbyprice-bot` — `resetCount`, `open`, `instKey` and the manifest on the instance

**Files:** `go/marketbyprice-bot/coordinator.go` + `coordinator_test.go`,
`go/marketbyprice-bot/shard.go` + `shard_test.go`, `dispatch.go` +
`dispatch_test.go`, `snapshot_writer.go` + `snapshot_writer_test.go`, `main.go`

The market-by-price half of tasks 7 to 9. Its shapes are already the right ones
and only the key changes:

- `Coordinator.resetCount` (`coordinator.go:53`) → `map[channelInstance]uint8`
- `Coordinator.open` (`coordinator.go:55`) → `map[channelInstance]openGroup`
- `instKey` (`shard.go:27`) → `{inst channelInstance, id uint32}`, and with it
  `instruments`, `refdata`, `deltaBuf`, `touched`, `crossed`, and
  `resetChannel(inst channelInstance)` (`dispatch.go:338`)
- `shardMsg.ch` (`shard.go:136`) → `inst channelInstance`, carried by `msgReset`
  and by `msgManifestPrune`
- `SnapshotWriter.Reset` (`snapshot_writer.go:118`) → `Reset(ctx, inst)`;
  `doReset` (`:134-143`) deletes only that instance's `dirty` and
  `lastWrittenAt` entries instead of replacing both maps, and `generation`
  becomes one counter per instance. `dirty` is already keyed by `instKey`, so
  the key follows for free and only the reset has to be narrowed — which is
  exactly why it is easy to miss.
- `main.go:121-128`'s `withInstrument` closure follows `instKey`
- `OnDisconnect` (`coordinator.go:177`) clears the whole `open` map, which is
  correct unchanged: a socket drop invalidates every instance's in-flight group.

**The manifest, which is not just a key change.** `Coordinator.manifest` is one
`ManifestState` for the whole process (`coordinator.go:54`), and `applyManifest`
(`:200-220`) broadcasts `msgManifestPrune` carrying only the new `Manifest Seq`.
`Shard.pruneManifest` (`dispatch.go:303-329`) then walks every entry of
`refdata` and, below the cutoff, deletes the definition, the book, the buffered
deltas, `crossed` and `touched` — with no channel and no instance filter. Once
`instKey` carries the instance, that is one path deleting the other path's
instruments on its own manifest bump, with no `Reset Count` behind it and no
counter accounting for it. So:

- `manifest` becomes `map[channelInstance]ManifestState`, and `applyManifest`
  compares the new `Manifest Seq` against that instance's previous one.
- `msgManifestPrune` carries the instance, and `pruneManifest` skips entries
  whose `instKey` names another. The one-generation grace window
  (`dispatch.go:305-309`) is unchanged; it is the set it compares over that
  narrows.
- `runResetBarrier`'s `c.manifest = ManifestState{}` (`coordinator.go:256`)
  becomes a delete of the resetting instance's entry, and its comment goes with
  the four in task 13.

- [ ] **Step 1: Write the failing tests**, the twins of tasks 7, 8(a) and 9:
  no barrier on two instances with steady differing `Reset Count` values; a
  reset on one instance sparing the other's book **and its pending
  `level_snapshots` rows**; each instance's snapshot levels stamped with its own
  group's instrument. Extend
  `TestDispatch_InterleavedChannelsWithDistinctResetCountsRunNoBarrier`
  (`coordinator_test.go:316`), `TestDispatch_ResetOnOneChannelSparesTheOther`
  (`:335`) and `TestDispatch_SnapshotLevelStampedWithOpenGroupInstrument`
  (`:493`). Then the manifest test: both instances hold instruments at
  `Manifest Seq` 3; one instance publishes a `manifest_summary` at 5; that
  instance's stale instruments are pruned and the other instance's are all still
  present, books, buffered deltas and gauges included.
- [ ] **Step 2: Run, watch them fail.** The manifest test fails by deleting
  everything below the cutoff on both instances.
- [ ] **Step 3: Re-key all of the above**, narrow `resetChannel`'s five keyed
  loops (`dispatch.go:338-374`) and `pruneManifest`'s one, narrow
  `SnapshotWriter.Reset`, and update `main.go`.
- [ ] **Step 4: Full suite, then `-race`**, `gofmt`, commit.

> **Five mutants**, each killed on its own. `rec.ChannelID` back in `resetCount`
> fails the barrier count; back in `open` fails the stamping test with one
> instance's levels carrying the other's instrument; back in `instKey` fails the
> spared-book assertion. Drop the instance filter from `pruneManifest`: the
> manifest test must fail with the spared instance's instruments gone. Restore
> `doReset`'s whole-map replacement: the pending-rows assertion must fail while
> every book assertion still passes — that one is the whole point of asserting
> on the rows and not only on the books. Run all five reverts separately: a
> single combined revert can fail one test and obscure that another asserts
> nothing.

---

## Task 12: The rows state the instance, and the degraded window is counted

**Files:** both book-builders' `events_writer.go`, `snapshot_writer.go`,
`metrics.go`, `coordinator.go`, and the matching `_test.go` files

Two things, together because they are the same window.

**The row key.** Every row map gains `"source_addr"` and `"dst_port"` from the
record, for the ten tables of task 2. `EventsWriter.Write` enumerates its keys
explicitly (`go/marketbyorder-bot/events_writer.go:28-46`), so this is a
deliberate addition per table, not a consequence of task 6. `SnapshotWriter`'s
`level_snapshots` rows take them from the instance its `instKey` carries after
tasks 9 and 11.

**And the row's `source_addr` is a string the writer builds, never the
`netip.Addr` itself.** A row is a `map[string]any` encoded with `encoding/json`
(`go/internal/clickhouse/client.go:202-208`,
`go/marketbyorder-bot/clickhouse.go:153-159`), so a `netip.Addr` in the map goes
through `MarshalText` — and the zero `Addr` marshals to `""`. That is not valid
input for an `IPv4` column: the column names itself in the row, so
`DEFAULT toIPv4(0)` never runs, the server refuses the insert, and because
`send` posts a whole batch as one body, one unidentified record fails every row
batched beside it. A book-builder deployed ahead of its parser would then load
nothing at all, which is the opposite of the window this plan is built around.
So each book-builder gets one helper, used by every row map:

```go
// rowSourceAddr renders a channel instance's address for an IPv4 column.
// A ClickHouse IPv4 column rejects the empty string that a zero netip.Addr
// marshals to, and a column named in the row never takes its DEFAULT, so an
// unidentified record is written as the sentinel the column defaults to.
func rowSourceAddr(a netip.Addr) string {
	if !a.Is4() {
		return "0.0.0.0"
	}
	return a.String()
}
```

`Is4` rather than `IsValid`: the column is `IPv4`, `srcAddr` already calls
`Unmap` (`go/marketbyorder-parser/runner.go:74-81`), and an address that is not
IPv4 after that has no representation in the column at all.
`dst_port` needs no helper: it is a `uint16`, and `0` is a valid `UInt16`.
The record on the unix socket is untouched — `""` is correct there, because
`netip.Addr.UnmarshalText` maps empty text back to the zero `Addr`, which is how
the counter below recognises an unidentified record.

Writing the sentinel rather than omitting the key keeps one row shape per table,
leaves the loaded value identical to the column default so pre-migration and
mid-deploy rows group together in task 13's panel, and keeps the result
independent of `input_format_defaults_for_omitted_fields` — a different setting
from the one task 1 pins, and one this plan does not touch.

**The counter.** `unidentified_records_total` in each book-builder, incremented
when a record arrives with the zero `netip.Addr` or `dst_port` 0. That is what a
book-builder deployed ahead of its parser sees, and the one state in which it
degrades to keying on the `Channel ID`. Silent degradation to the behaviour this
plan removes is the failure the whole change is most exposed to, so it is
counted rather than refused: a stack mid-deploy has to keep serving.

- [ ] **Step 1: Write the failing tests.** (a) Per table, the row map carries
  both keys with the record's values. (b) **The encoded batch**, not only the
  row map: enqueue one fully stamped record and one unidentified record, let the
  batcher post to an `httptest` server, and assert the request body's JSONEachRow
  lines carry `"source_addr":"198.51.100.11"` and `"source_addr":"0.0.0.0"` —
  and that neither line carries `"source_addr":""`. (c) A record with a zero
  `netip.Addr` raises the counter, a record with `dst_port` 0 raises it, and a
  fully stamped record does not.
- [ ] **Step 2: Run, watch them fail.**
- [ ] **Step 3: Add the keys, the helper and the counter.**
- [ ] **Step 4: Full suite in both**, `gofmt`, commit.

> **Three mutants.** Drop either key from any one row map and that table's test
> must fail. Put `rec.SourceAddr` into the row map in place of the helper: (a)
> may well still pass, because a `netip.Addr` and its string compare equal to
> nothing in particular until they are encoded — (b) must fail, on the
> `"source_addr":""` the unidentified record then produces. That asymmetry is
> the reason (b) asserts on the posted body rather than on the map. Then — the
> one that matters for the counter — make it increment unconditionally, or
> never: the fully-stamped case must fail on the first, the zero cases on the
> second. A counter that is always zero is the exact shape of the silence this
> change exists to remove, and it is the shape a test asserting only "the counter
> exists" would accept.

---

## Task 13: The comments go, the panels partition on the instance, the prose follows

**Files:** `go/marketbyorder-bot/coordinator.go`, `shard.go`,
`go/marketbyprice-bot/coordinator.go`, `dispatch.go`,
`go/marketbyorder-bot/coordinator_test.go`,
`go/marketbyprice-bot/coordinator_test.go`,
`demo/grafana/dashboards/marketbyorder.json`,
`demo/grafana/dashboards/marketbyprice.json`,
both book-builder READMEs, `docs/README.md`

**The four comments are deleted**, each of which says that "a group can carry two
redundant publishers interleaved on the same ports under different channel_ids"
and uses it to justify keying on the `Channel ID`:

- `go/marketbyorder-bot/coordinator.go:19-23`
- `go/marketbyorder-bot/shard.go:91-94`
- `go/marketbyprice-bot/coordinator.go:47-52`
- `go/marketbyprice-bot/dispatch.go:334-337`

and the two test comments that restate them
(`go/marketbyorder-bot/coordinator_test.go:359-360`,
`go/marketbyprice-bot/coordinator_test.go:310-311`). What replaces them is not
another comment: it is `channelInstance` and the fact that `resetChannel` cannot
be called with a bare `Channel ID`. The constraint they recorded is now
unstatable rather than merely undocumented.

`docs/superpowers/plans/2026-08-10-per-publisher-seq-tracking.md` keeps every one
of its copies. It is a dated document and a record of the code as it stood that
day.

**The panels.** `PARTITION BY channel_id, instrument_id` becomes
`PARTITION BY source_addr, dst_port, channel_id, instrument_id` in panel 20,
"Sequence gaps (per-instrument)", of both dashboards
(`demo/grafana/dashboards/marketbyorder.json:1407`,
`demo/grafana/dashboards/marketbyprice.json:1345`). Without it `lagInFrame`
over `per_instrument_seq` compares one instance's sequence to the other's and
reports the difference as missing messages — the panel's own defect, as a false
positive.

The panel's `description` moves with its query. Both read "Missing messages
detected from the dense per-(channel,instrument) sequence (per_instrument_seq)"
(`marketbyorder.json:1411`, `marketbyprice.json:1349`), and after this change
the sequence the panel reads is dense per channel instance and instrument, not
per channel and instrument. A description naming the old partition is the one
piece of this change an operator reads before deciding whether to trust the
number.

**The prose.** `go/marketbyprice-bot/README.md:20` ("Each
`(channel_id, instrument_id)`"), `:154`, `:156`, and
`go/marketbyorder-bot/README.md:13`, `:25`, restated on the channel instance.

- [ ] **Step 1: Delete the four comments and the two test comments.**
- [ ] **Step 2: Edit both dashboard `rawSql` strings, and both panel
  `description` strings with them.**
- [ ] **Step 3: Update both book-builder READMEs.** The `docs/README.md` row for
  this pair landed with the documents themselves.
- [ ] **Step 4: Full suite in all five Go modules, `-race` in both
  book-builders, `gofmt -l .` clean in each.**
- [ ] **Step 5: Verify the dashboards render** against the demo stack, with two
  instances of one `Channel ID` present in `events`, and confirm the panel
  reports no missing messages where none are missing.
- [ ] **Step 6: Commit.**

> **The mutant is the panel.** Revert either `PARTITION BY` and step 5 must show
> the gap panel climbing against a feed losing nothing. This one has no unit test
> and the plan says so plainly rather than adding a check that would look like a
> gate: the query lives in a Grafana JSON string and nothing in the repository
> executes it. Step 5 is the gate, and it is a manual one.
> The comment deletions have no mutant and need none — they are prose, and the
> type is what enforces what they used to assert.

---

## Verification, end to end

After task 13, on the demo stack with two channel instances of one `Channel ID`
on distinct destination ports:

- `dz_mbo_bot_channel_resets_total` and `dz_mbp_bot_channel_resets_total` flat
  while both instances publish steady, differing `Reset Count` values.
- `dz_mbo_bot_unidentified_records_total` and its market-by-price twin at zero
  once both halves are deployed, and non-zero for exactly the window in which a
  book-builder ran ahead of its parser.
- `SELECT DISTINCT source_addr, dst_port, channel_id FROM marketbyorder.events`
  returns two rows for one `channel_id`. The same query over
  `marketbyorder.instruments` returns **one**, and that is the designed
  behaviour rather than a symptom: the table is
  `ReplacingMergeTree(recv_ts) ORDER BY (channel_id, instrument_id)`, the sort
  key does not change here, and the columns record which instance's definition
  survived. The design names it under *Out of scope*.
- `dz_mbp_bot` shows no instrument count dropping on the instance that did not
  publish the manifest bump, across at least two `Manifest Seq` increments.
- Panel 20 on both dashboards reports no missing messages.
- `dz_mbo_bot_snapshot_order_dropped_total` and
  `dz_mbp_bot_snapshot_level_dropped_total` flat, where before the change each
  instance's snapshot cycle discarded the other's levels.

## Out of scope

Named in the design and repeated here so no task reaches for them: arbitrating
between two instances of one channel into one book; binding more than one
destination port per port role in a parser; renaming `Record.Port` to
`PortRole`; changing any `ORDER BY`, including `marketbyorder.instruments`' and
`marketbyprice.instruments'` `(channel_id, instrument_id)`; renaming the parsers'
`source_ip` metric label; and keying anything in `go/topofbook-bot` or
`go/topofbook-parser`, which hold no per-instance recovery state. `go/topofbook-bot`
is touched by task 1 only, because `buildInsertURL` is the third copy of the
insert path the setting has to be pinned in.
