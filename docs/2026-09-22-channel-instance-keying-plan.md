# Channel-instance keying — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Carry `(source IP address, Channel ID, destination port)` — the
channel instance — on the record instead of dropping it at the parser boundary,
and key on it in both parsers and both book-builders: **gap detection on the
channel instance, and recovery state on the publisher channel**
`(source IP address, Channel ID)`, which is the same identity with the port role
dropped. One publisher is three channel instances, one per port role, and an
instrument's definition, deltas and snapshot cycle arrive on all three, so a
book keyed on the finer value is three books that never meet. The design's
*Two keys, because one publisher is three channel instances* settles which
structure takes which.

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

Two worked cases throughout, both the design's, and every task is against one
of them:

- **Two paths carrying one `Channel ID`**, on distinct destination ports and
  from distinct source IP addresses, carrying one `Instrument ID` — against the
  one publisher per `Channel ID` the tree assumes today. This is what the
  publisher channel separates.
- **One publisher's three port roles carrying one `Instrument ID`** — its
  definition on `refdata`, its deltas on `mktdata`, its cycle on `snapshot`.
  This is what the publisher channel must *not* separate, and it is the
  regression a book keyed on the channel instance produces.

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
- **Two paths in a fixture get disjoint sequence ranges.** Every test that
  interleaves two paths gives them sequence numbers that do not overlap — one
  from 1, the other from 1,000,000, a separation wider than task 10's
  `reorderWindow`. Identical or overlapping ranges make the folded key read as
  reorders and duplicates, and that branch is ignored by design, so a
  `Channel ID`-keyed implementation passes the fixture and the test asserts
  nothing. The same applies to `Reset Count`: differing steady values, not one
  shared value.
- **Every book-builder fixture drives all three port roles.** A fixture that
  feeds only `mktdata` records cannot see the split this plan's second worked
  case is about: the definition has to arrive on `refdata`, the cycle on
  `snapshot`, and the assertion has to reach the symbol and the exponents those
  records carried. A test whose records all share one `DstPort` passes against
  an `instKey` keyed on the destination port.
- **Commit before any step that rewrites files in place**, and in particular
  before task 3's DDL run against a container volume.
- **Commit messages:** all lowercase, no `Co-Authored-By`, no attribution
  footer of any kind, per the repository's own rules.

---

## The pieces where the obvious implementation is the wrong one

Stated up front, because each was found by reading the code and each is a task
below that would otherwise be written wrong.

- **One publisher is three channel instances, so the book cannot key on one.**
  Each port role is a separate instance with its own series
  (`rust/publisher/dz-publisher-egress/src/instance.rs:19-23`), and the
  publisher keys only its `Sequencer` that finely (`sequencer.rs:36-39`) while
  its era covers all three roles (`era.rs:47-50`). Put the destination port in
  `instKey` and `applyInstrumentDefinition`'s write at
  `go/marketbyorder-bot/shard.go:150-154` lands under a key no reader uses:
  `s.refdata[k]` at `shard.go:446` and `:468` are reached from `snapshot` and
  `mktdata` records and both miss every time — empty symbol, exponent `0`, no
  book ever `StatusReady`. `instKey`, `instruments`, `refdata`, `deltaBuf`,
  `resetCount`, `open`, the manifest and the `SnapshotWriter` take
  `publisherChannel`; only `seqTracker.last` and `seqLast` take
  `channelInstance`.
- **`input_format_skip_unknown_fields` must be pinned before any row carries the
  new key, not with it.** It defaults to `1`, and at `1` an insert naming a
  column the table does not have is answered `200` and the field is discarded
  (measured against 24.8 at
  `rust/recorder/dz-recorder-clickhouse/src/config.rs:195-226`). Pinning it in
  the same task as the row key would refuse the first mis-ordered batch instead
  of making the ordering impossible to get wrong.
- **A refused batch is destroyed, not held.** "The batch loads on its own once
  the column exists" is the recorder's property, whose rows are objects in
  storage (`rust/recorder/dz-recorder-clickhouse/src/config.rs:210-215`). All
  three Go paths hold the batch in memory only and, on a refusal, log it, count
  it and truncate the buffer: `go/marketbyorder-bot/clickhouse.go:117-125`,
  `go/internal/clickhouse/client.go:159-173`, and
  `go/topofbook-bot/clickhouse.go:265-274` with `buf.Reset()` in its caller at
  `:203-210`. So the rollout order is a deploy gate and not a recoverable
  mistake: a book-builder landed ahead of its migration loses that window's
  rows. Watch `clickhouse_rows_dropped_total{reason="write_failed"}` in both
  book-builders and `{reason=~"http_4.."}` in `go/topofbook-bot` throughout
  tasks 7 to 12.
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
  NOT interleave snapshot groups within a channel
  (`go/marketbyprice-bot/coordinator.go:26-27`,
  `docs/2026-04-23-marketbyorder-plan.md:3066-3068`) and `Dispatch` deletes the
  route at `snapshot_end` (`go/marketbyorder-bot/coordinator.go:85`), so within
  one publisher channel and with no loss the id is unambiguous. It goes because the
  instrument is found by a **search**, not by the group:
  `Shard.applySnapshotOrder` (`shard.go:184-200`) scans every instrument the
  shard owns for a matching open `Snapshot ID`, with no channel or instance
  filter, and map iteration order picks between matches. Two publishers of one
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
  loops to the publisher channel and leaving that call alone spares the other
  path's book and still drops its pending `level_snapshots` rows.
- **`marketbyprice-bot`'s manifest is one value for the whole process, and its
  prune has no key at all.** `applyManifest` broadcasts `msgManifestPrune` with
  only a `Manifest Seq` (`coordinator.go:200-220`) and `Shard.pruneManifest`
  walks all of `refdata` (`dispatch.go:303-329`). Once `instKey` carries the
  publisher channel, one path's manifest bump deletes the other path's
  instruments unless the manifest is keyed too. Task 11 covers it.

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
  `go/internal/sink/json_test.go`, `README.md`
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

## Task 4: `channelInstance` and `publisherChannel`, and the parsers' trackers keyed on the first

**Files:** `go/internal/channel/channel.go` + `channel_test.go`,
`go/marketbyorder-parser/runner.go` + `seqtracker_test.go`,
`go/marketbyprice-parser/runner.go` + `seqtracker_test.go`,
`go/marketbyorder-bot/go.mod` (the one module that gains the dependency)

Both types are declared **once**, in `go/internal`, for the reason the design
gives under *Two keys*: three of the four modules that need them already depend
on it after #153, so the duplication this plan originally chose now costs four
definitions to save one `require` line. `marketbyorder-bot` gains the dependency
and the `replace` directive; the other three already have both.

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

// publisherChannel is one publisher's view of one channel across all three of
// its port roles: the unit that owns an era, a book, its reference data and
// its snapshot cycle. A channel instance with the port role dropped, because
// an instrument's definition arrives on refdata, its deltas on mktdata and its
// cycle on snapshot, and those are one instrument.
type publisherChannel struct {
	addr netip.Addr
	ch   uint8
}

func (i channelInstance) channel() publisherChannel {
	return publisherChannel{addr: i.addr, ch: i.ch}
}
```

Both types go in both parsers and both book-builders. The parsers hold no book
and use only `channelInstance`; declaring `publisherChannel` there anyway would
be dead code, so tasks 7 onward declare it in the book-builders and this task
declares `channelInstance` alone in the parsers. The `channel()` method travels
with `publisherChannel`.

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
`go/internal/sink/json_test.go` (one file, not one per parser: #153 moved the
sinks into `go/internal`),
`go/marketbyorder-bot/bot_test.go`, `go/marketbyprice-bot/bot_test.go`

The same two fields, with the same JSON keys, on both book-builders' `Record`
(`go/marketbyorder-bot/record.go:5`, `go/marketbyprice-bot/record.go:5`). No
keying changes yet — after this task both book-builders decode the identity and
ignore it, which is a deliberate intermediate state and the last one that
changes no behaviour.

The sinks need no change: `JSONFileSink.Write` encodes `&records[i]` whole
(`go/internal/sink/json.go`, where #153 moved it from each parser's `sink_json.go`) and `SocketSink` marshals the same
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

## Task 7: `marketbyorder-bot` — `publisherChannel`, and `resetCount` keyed on it

**Files:** `go/marketbyorder-bot/coordinator.go` + `coordinator_test.go`

`publisherChannel` and `channel()` are declared here, in the shape task 4 gives
them. `Coordinator.resetCount` (`coordinator.go:24`) becomes
`map[publisherChannel]uint8`, and `Dispatch`'s barrier trigger
(`coordinator.go:53-58`) reads it by `channel()` of the instance the record was
stamped with. `runResetBarrier` (`coordinator.go:113`) takes the held record's
publisher channel and adopts its `Reset Count` for that publisher channel alone.

Per publisher channel and not per channel instance, because one era covers all
three of a publisher's port roles — "the block's three port roles share it,
because a restart is one event for the whole feed"
(`rust/publisher/dz-publisher-egress/src/era.rs:47-50`). Keyed finer, one
restart would raise three barriers and each would wipe a third of a book.

- [ ] **Step 1: Write the failing tests.** Two paths carrying one `Channel ID`,
  differing in `SourceAddr` and `DstPort`, with differing but steady
  `Reset Count` values, interleaved: **no** barrier fires. Then a `Reset Count`
  change on one: exactly one barrier, and the other path's `Reset Count` is
  untouched. Then one publisher's three port roles at one steady `Reset Count`:
  **no** barrier, and one `resetCount` entry rather than three. These extend
  `TestDispatch_InterleavedChannelsWithDistinctResetCountsRunNoBarrier` and
  `TestDispatch_ResetOnOneChannelSparesTheOther`
  (`coordinator_test.go:363`, `:382`) from two channels to two paths carrying
  one, which is the case neither covers.
- [ ] **Step 2: Run, watch the first fail** — under the `Channel ID` key the
  alternation reads as a reset on every datagram and the barrier fires on each.
- [ ] **Step 3: Re-key the field**, and re-key `snapshotRoute`'s barrier-time
  clearing loop (`coordinator.go:135-139`) to match; it is replaced wholesale in
  task 8 but must compile here.
- [ ] **Step 4: Full suite**, `gofmt`, commit.

> **Two mutants.** Key `resetCount` on `rec.ChannelID` again and the interleave
> test must fail with a barrier count in the thousands where zero was expected.
> Watch the *count*, not just the pass: a barrier that fires once looks like a
> legitimate first-sight baseline, and the defect is that it fires on every
> alternation. Then key it on the full `channelInstance`: the three-port-role
> test must fail with three `resetCount` entries for one publisher, which is
> the over-fine key this plan's second worked case exists to catch.

---

## Task 8: `marketbyorder-bot` — the open snapshot group, per publisher channel

**Files:** `go/marketbyorder-bot/coordinator.go`, `shard.go`,
`coordinator_test.go`, `shard_test.go`

**#139 landed the model this task originally proposed**, so what is left is a
re-key and not a replacement. `Coordinator.snapshotRoute map[snapKey]int` and
`snapKey` are gone; `applySnapshotOrder` already resolves the instrument from
the open group rather than scanning for a matching `Snapshot ID`; and
`Shard.snapCtx` is already `map[instKey]SnapshotContext` (`shard.go:79`).

What survives is the same defect in the two structures that replaced them, both
keyed on bare `Channel ID`:

```go
Coordinator.open map[uint8]openRoute  // coordinator.go:27
Shard.open       map[uint8]openGroup  // shard.go:80
```

Both re-key onto `publisherChannel`. Two paths carrying one `Channel ID` are two
publishers, each running its own snapshot cycle, and a single entry per
`Channel ID` means the second path's `snapshot_begin` overwrites the first's —
which is the misrouting this task exists to prevent, unchanged by #139.

`Snapshot ID` validates membership and is never the key.

**`Shard.clearShadows` (`shard.go:149`) needs the same narrowing `resetChannel`
gets.** It does `s.open = map[uint8]openGroup{}` and
`s.snapCtx = map[instKey]SnapshotContext{}`, clearing *every* channel's state on
one path's disconnect. Once the key carries the path, one path dropping its
socket must not discard the other's open groups or snapshot contexts: both loops
filter on the publisher channel, the way `resetChannel` filters `s.instruments`
and `s.refdata` on `k.ch == ch` (`shard.go:110-119`). `clearShadows` takes the
disconnecting publisher channel as an argument to do it.

**One open group per publisher channel is sufficient, and the protocol says
so.** A publisher MUST NOT interleave snapshot groups within one channel
(`go/marketbyprice-bot/coordinator.go:26-27`,
`docs/2026-04-23-marketbyorder-plan.md:3066-3068`). Two paths carrying one
channel are two publishers and do interleave with each other, which is why the
map is keyed on `publisherChannel` and not on `Channel ID`. The finer key buys
nothing above it: a snapshot group is carried wholly on the `snapshot` port, so
a publisher channel has exactly one port role that can open one. Nothing in this
task widens the state to more than one open group per publisher channel, and a
task that finds itself needing to has hit a protocol question this plan does not
answer.

One consequence, where #139 left two:

- `Shard.snapCtx` is already keyed on `instKey`, and `instKey` carries the
  publisher channel after task 4, so it follows the re-key with no change of its
  own. Its consumers are the `wire_snapshots` writes, which need the group's
  symbol and exponents.

`SnapshotOrderDroppedTotal` keeps its meaning: a snapshot order with no open
group for its publisher channel is dropped and counted.

> **What (b) is not.** It is not two *instruments* mid-cycle at one
> `Snapshot ID` inside a single publisher channel. That case is unreachable: the
> publisher does not interleave groups within a channel, and the route entry
> is deleted at each `snapshot_end` (`coordinator.go:85`), so the next group
> claims the id afresh and the unfixed tree passes such a test. The reachable
> within-one-publisher case is a **lost** `snapshot_end` leaving a shadow open
> across the cycle boundary, and that is task 10's subject rather than this
> one's — the continuity check closes the group. This task's job is only that
> the instrument is resolved from the group rather than searched for.

- [ ] **Step 1: Write the failing tests.** (a) Two paths carrying one
  `Channel ID`, each opening a group for a different `Instrument ID`,
  interleaved: each path's snapshot orders reach its own instrument's shadow
  and neither group is overwritten. Give the two instruments ids that land on
  **different shards** under `id % n`, so the misrouting a single route entry
  causes is deterministic rather than a matter of map order. (b) Two paths
  carrying one `Channel ID`, each opening a group for a different `Instrument ID`
  whose ids land on the **same** shard under `id % n`, both mid-cycle at the
  same `Snapshot ID` — two open shadows, one shard, one id, which is the steady
  state for redundant paths because `Snapshot ID` is monotonic per
  `(Channel ID, Instrument ID)` and both paths run the same cycles: each
  path's orders reach the instrument its own group named, and the two
  shadows hold disjoint order sets. This case needs no key change in the shard
  and passes at the end of this task. (c) A snapshot order
  arriving after its `snapshot_end` is dropped and counted, not routed — the
  `marketbyorder` twin of
  `TestDispatch_StrayLevelAfterSnapshotEndIsDroppedNotRouted`
  (`go/marketbyprice-bot/coordinator_test.go:461`). (d) Two paths carrying one
  `Channel ID`, each with an open group, one path disconnecting: the surviving
  path keeps its open group and its snapshot context, and its next
  `snapshot_order` is still routed. This is the `clearShadows` case and it fails
  on the current tree, where one disconnect clears both maps for every channel.
- [ ] **Step 2: Run, watch (a) and (b) fail** — (a) because one `Channel ID`
  gives one route entry and the second path's `snapshot_begin` overwrites the
  first's,
  (b) because `applySnapshotOrder` scans for a matching open `Snapshot ID` and
  two shadows on that shard match, so the orders split between them in whatever
  order the map yields.
- [ ] **Step 3: Re-key `Coordinator.open` and `Shard.open`** from `uint8` onto
  `publisherChannel`; give `clearShadows` the disconnecting publisher channel and
  filter both its loops on it; narrow `resetChannel`'s loops
  (`shard.go:110-119`) to the publisher channel.
- [ ] **Step 4: Full suite**, `gofmt`, commit.

> **Three mutants, and all must be killed.** Re-key `open` on `rec.ChannelID`
> alone: test (a) must fail, with the second path's group having overwritten
> the first's and the first's orders filed into the second instrument's
> shadow.
> Re-key it on the full `channelInstance`: nothing in this task fails, because a
> group lives on one port role — which is why the over-fine key is caught in
> task 9 on `refdata` and not here.
> Then revert `clearShadows` to clearing both maps wholesale: test (d) must
> fail, with the surviving path's open group and snapshot context gone and its
> next `snapshot_order` dropped for want of a group. That mutant is the one
> #139 introduced and this task inherits — the other two are about the key, and
> this one is about who a disconnect is allowed to affect.

---

## Task 9: `marketbyorder-bot` — `instKey`, the reset marker and the SnapshotWriter on the publisher channel

**Files:** `go/marketbyorder-bot/shard.go` + `shard_test.go`,
`snapshot_writer.go` + `snapshot_writer_test.go`, `main.go` + `main_test.go`,
`parity_test.go`

`instKey{ch uint8, id uint32}` (`shard.go:15`) becomes
`instKey{pc publisherChannel, id uint32}`. Everything keyed by it follows:
`instruments`, `refdata`, `deltaBuf`, `snapCtx`, and `resetChannel`, whose
signature becomes `resetChannel(pc publisherChannel)` (`shard.go:95`).

**`publisherChannel` and not `channelInstance`, and this is the task where the
difference is load-bearing.** `applyInstrumentDefinition` writes `s.refdata[k]`
from a record that arrived on the `refdata` port (`shard.go:150-154`), and the
reads at `shard.go:446` and `:468` are reached from `snapshot` and `mktdata`
records. With the destination port in the key those are three keys for one
instrument and both reads miss on every record: empty symbol, exponent `0`, and
a book that never reaches `StatusReady`. `channel()` of the record's instance is
what makes the three port roles meet.

The shard's routing hash is unchanged: `int(rec.InstrumentID) % c.n`. One
`Instrument ID` still lands on one shard whichever path carried it, so
per-instrument FIFO holds and the two paths' books are two entries in one
shard's maps rather than work on two shards.

**Three things the key change drags with it, and none of them compiles or
behaves correctly if left out.**

`shardMsg.ch uint8` (`shard.go:535`) becomes `pc publisherChannel`, so the
reset marker names the publisher channel to wipe and `resetChannel` cannot be
reached with a bare `Channel ID`.

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
`generation` (`snapshot_writer.go:92-96`), so a reset on one path discards
pending `level_snapshots` rows for every book on that shard. It becomes
`Reset(ctx, pc publisherChannel)`, deleting only the `dirty` entries whose key
carries that publisher channel; `generation` becomes per publisher channel
(`map[publisherChannel]uint64`), and `flushDue` compares the generation of the
publisher channel whose batch it extracted.

`main.go:90-96` and `main_test.go:63` are updated with the constructor and the
closure. `parity_test.go:76-80` holds the same pair.

- [ ] **Step 1: Write the failing tests.** (a) Two paths carrying one
  `Channel ID` and one `Instrument ID`: two independent books, two
  independent per-instrument sequence positions, and a per-instrument gap on one
  raising `per_instrument_gaps_total` without demoting the other. (b) A
  `Reset Count` change on one path: `resetChannel` wipes that publisher
  channel's instruments, refdata and buffered deltas and leaves the other
  path's `StatusReady` book standing. (c) The spared path's **pending rows**
  survive that reset: mark both paths' instruments dirty, reset one, drive
  the writer's tick, and the spared path's `level_snapshots` rows are
  enqueued. (d) A `level_snapshots` row carries the `channel_id` of the book it
  read, not `0`. (e) **One publisher, three port roles, one book:** an
  `instrument_definition` on the `refdata` port, a `snapshot_begin`/`_end` pair
  on the `snapshot` port and a delta on the `mktdata` port, one `SourceAddr`,
  one `Channel ID`, one `Instrument ID`, three different `DstPort` values.
  Exactly one entry in `instruments` and in `refdata`, one book reaching
  `StatusReady`, and the definition's symbol and exponents on the rows the other
  two port roles produce.
- [ ] **Step 2: Run, watch them fail** — (a) and (b) because one `instKey` means
  the two paths share one `Instrument`, one sequence position and one delta
  buffer, and one reset wipes both; (c) because `Reset` clears the whole map;
  (d) because the row takes `w.channel`, which is the constructor's `0`. (e)
  passes before the change and after it, and is the guard on the over-fine key
  rather than on the under-fine one — run it against the `channelInstance`
  mutant below.
- [ ] **Step 3: Re-key `instKey` and thread the publisher channel through** `apply`,
  `handle`, `bufferDelta`, `replayBuffer`, `refdataFor` and `resetChannel`; carry
  it on `shardMsg`; re-key the `SnapshotWriter` and narrow its `Reset`; update
  `main.go`, `main_test.go` and `parity_test.go`.
- [ ] **Step 4: Full suite, then `-race`**, `gofmt`, commit.

> **Four mutants.** Drop `pc` from `instKey`: (a) and (b) must fail twice
> over — the interleaved deltas of two paths are applied to one
> `Instrument`, so each path's `per_instrument_seq` reads as a gap in the
> other's, and the reset takes both books. Put the **full `channelInstance`**
> into `instKey` instead: (e) must fail, with an empty symbol and a zero
> exponent on the `mktdata` and `snapshot` rows, because the definition landed
> under the `refdata` port's key. That mutant passes (a) through (d) unchanged,
> which is exactly why (e) exists. Restore `doReset`'s whole-map
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

- First sight of a channel instance sets the baseline silently. Same reason as
  `seqTracker.observe` (`go/marketbyorder-parser/runner.go:46-53`): a newly
  appearing path must not report a phantom gap the size of its sequence.
- `seq <= last`, and then two cases **nested inside that branch**, because
  `last - seq` is `uint64` subtraction and wraps to a huge value whenever
  `seq > last`. Evaluated as a sibling, the second of these would match every
  ordinary forward gap and re-baseline on it.
  - `last - seq <= reorderWindow`: reorder or duplicate. Ignored, `last`
    unchanged.
  - `last - seq > reorderWindow`: a restart the era did not announce.
    Re-baseline that instance (`last = seq`), count it in
    `seq_rebaselined_total`, and apply the same `snapshot`-port consequence as
    a discontinuity.
- `seq > last+1`: a discontinuity on that instance. Count it in
  `datagram_seq_gaps_total`, and **if the port role is `snapshot`, delete
  `inst.channel()`'s entry from `open`.** This is the ordinary-loss path and it
  must never touch `seq_rebaselined_total`.

Two new counters in each: `datagram_seq_gaps_total` and
`seq_rebaselined_total`, both labelled `{port}`, under the existing
`dz_mbo_bot` / `dz_mbp_bot` namespaces.

`reorderWindow` is a package constant, `1 << 12`, and not a flag.

**Why the fourth rule is not optional.** The barrier is what normally clears a
baseline, and an ordinary restart of this publisher reaches it:
`EraStore::begin_era` returns `previous.wrapping_add(1)`
(`rust/publisher/dz-publisher-egress/src/era.rs:190-211`), so `Reset Count`
moves and `runResetBarrier` re-baselines. But the era lives in a file, and a
publisher whose state directory does not survive its own restart reads no file
and resolves to `FIRST_ERA` every time (`era.rs:60-69`), so the era never moves
while the series returns to 0 on every start. `Sequencer::register` names that
combination "the one combination a subscriber cannot interpret"
(`sequencer.rs:59-65`). Without the fourth rule `seqLast` stays pinned high,
every later datagram takes the reorder branch, and the check — and with it the
snapshot-group invalidation this task exists for — is dead for the life of the
process with no counter moving. The residual is bounded rather than removed: a
restart while `last` is itself below `reorderWindow` is not distinguishable from
a reorder, but the series climbs back past `last` within `reorderWindow`
datagrams and the check resumes on its own.

**A re-baseline wipes nothing.** No barrier, no shard drain, no book dropped:
with `Reset Count` unchanged the publisher has said the era is still running,
and a wipe against that field would be a guess. The counter is the output.

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
  `snapshot`-port discontinuity on one path drops that publisher channel's open
  group and leaves the other path's standing; the next snapshot order on the
  first path is dropped and counted. (b) A `refdata`-port discontinuity drops no
  group. (c) A reorder (`seq <= last`) drops nothing and leaves `last` unchanged.
  (d) First sight of an instance reports no gap. (e) A datagram lost on the
  `mktdata` port raises `datagram_seq_gaps_total` by one, leaves
  `seq_rebaselined_total` at **zero** and drops no group. Both counters are
  asserted, because this is the case the unnested `last - seq > reorderWindow`
  rule gets wrong: on `seq > last` that subtraction wraps and an ordinary loss
  takes the re-baseline path, which a test asserting only "the counter" would
  not catch. (f) A run of records
  with the zero `netip.Addr` and `dst_port` 0, alternating between two ascending
  sequence series, raises no gap count and drops no group — and, once task 12
  lands, raises `unidentified_records_total` once per record. Give the two
  series disjoint ranges, per *Global constraints*; at overlapping ranges the
  folded key reads as duplicates and (f) passes against a check that is not
  excluded at all. (g) A series climbing well past `reorderWindow` and then
  restarting at 0 with `Reset Count` unchanged: `seq_rebaselined_total` rises by
  one, that publisher channel's open group is dropped, and — the assertion that
  matters — **a discontinuity introduced after the restart is still reported**.
  (h) An ordinary restart, with `Reset Count` moved, re-baselines through
  `runResetBarrier` and leaves `seq_rebaselined_total` at zero. (i) A barrier on
  one path leaves the other path's `seqLast` entry intact.
- [ ] **Step 2: Run, watch them fail** — in `marketbyorder-bot` because nothing
  reads the field, in `marketbyprice-bot` because there is no field.
- [ ] **Step 3: Re-key `seqLast`, add the check, the re-baseline rule and both
  counters,** and make `runResetBarrier` delete only the entries whose
  `channelInstance.channel()` is the resetting publisher channel, rather than
  assigning `c.seqLast = map[string]uint64{}`
  (`go/marketbyorder-bot/coordinator.go:141`), which empties the map for every
  path it holds.
- [ ] **Step 4: Full suite in both, then `-race`**, `gofmt`, commit.

> **Eight mutants.** Flatten the re-baseline rule out of the `seq <= last`
> branch, so `last - seq > reorderWindow` is evaluated as a sibling: (e) must
> fail, because on an ordinary forward gap that `uint64` subtraction wraps and
> the loss re-baselines instead of being reported. This is the mutant the
> rules-as-siblings wording produced, and (e) only catches it because it
> asserts `seq_rebaselined_total` is zero rather than asserting "the counter".
> Delete the re-baseline rule: (g) must fail on its last
> assertion — the post-restart discontinuity goes unreported — while (a) to (f)
> all still pass, which is what makes that assertion and not the counter the
> subject. Raise `reorderWindow` above the fixture's separation: (g) must fail
> the same way. Restore `c.seqLast = map[string]uint64{}` in the barrier: (i)
> must fail. Delete the `open` deletion: (a) must fail, and this is the
> assertion that makes the field read rather than merely written — a version that
> only increments the counter passes every other test in this plan.
> Drop the `refdata` exemption: (b) must fail. Change `seq <= last` to `seq < last`
> or `seq != last+1`: (c) must fail. Drop the unidentified-record exclusion:
> (f) must fail, with a gap count in the thousands and every open group gone.
> Test (d) is the one to distrust — it passes against a check that does nothing
> at all — so run it against the deleted-check mutant and confirm it is (a) and
> not (d) that fails.

---

## Task 11: `marketbyprice-bot` — `resetCount`, `open`, `instKey` and the manifest on the publisher channel

**Files:** `go/marketbyprice-bot/coordinator.go` + `coordinator_test.go`,
`go/marketbyprice-bot/shard.go` + `shard_test.go`, `dispatch.go` +
`dispatch_test.go`, `snapshot_writer.go` + `snapshot_writer_test.go`, `main.go`

The market-by-price half of tasks 7 to 9. Its shapes are already the right ones
and only the key changes:

- `Coordinator.resetCount` (`coordinator.go:53`) → `map[publisherChannel]uint8`
- `Coordinator.open` (`coordinator.go:55`) → `map[publisherChannel]openGroup`
- `instKey` (`shard.go:27`) → `{pc publisherChannel, id uint32}`, and with it
  `instruments`, `refdata`, `deltaBuf`, `touched`, `crossed`, and
  `resetChannel(pc publisherChannel)` (`dispatch.go:338`)
- `shardMsg.ch` (`shard.go:136`) → `pc publisherChannel`, carried by `msgReset`
  and by `msgManifestPrune`
- `SnapshotWriter.Reset` (`snapshot_writer.go:118`) → `Reset(ctx, pc)`;
  `doReset` (`:134-143`) deletes only that publisher channel's `dirty` and
  `lastWrittenAt` entries instead of replacing both maps, and `generation`
  becomes one counter per publisher channel. `dirty` is already keyed by
  `instKey`, so
  the key follows for free and only the reset has to be narrowed — which is
  exactly why it is easy to miss.
- `main.go:121-128`'s `withInstrument` closure follows `instKey`
- `OnDisconnect` (`coordinator.go:177`) clears the whole `open` map, which is
  correct unchanged: a socket drop invalidates every path's in-flight group.

**The manifest, which is not just a key change.** `Coordinator.manifest` is one
`ManifestState` for the whole process (`coordinator.go:54`), and `applyManifest`
(`:200-220`) broadcasts `msgManifestPrune` carrying only the new `Manifest Seq`.
`Shard.pruneManifest` (`dispatch.go:303-329`) then walks every entry of
`refdata` and, below the cutoff, deletes the definition, the book, the buffered
deltas, `crossed` and `touched` — with no channel and no path filter. Once
`instKey` carries the publisher channel, that is one path deleting the other
path's instruments on its own manifest bump, with no `Reset Count` behind it and
no counter accounting for it. So:

- `manifest` becomes `map[publisherChannel]ManifestState`, and `applyManifest`
  compares the new `Manifest Seq` against that publisher channel's previous one.
- `msgManifestPrune` carries the publisher channel, and `pruneManifest` skips
  entries whose `instKey` names another. The one-generation grace window
  (`dispatch.go:305-309`) is unchanged; it is the set it compares over that
  narrows.
- `runResetBarrier`'s `c.manifest = ManifestState{}` (`coordinator.go:256`)
  becomes a delete of the resetting publisher channel's entry, and its comment
  goes with the four in task 13.

- [ ] **Step 1: Write the failing tests**, the twins of tasks 7, 8(a), 9(b),
  9(c) and 9(e): no barrier on two paths with steady differing `Reset Count`
  values; a reset on one path sparing the other's book **and its pending
  `level_snapshots` rows**; each path's snapshot levels stamped with its own
  group's instrument; and one publisher's three port roles building one book
  from a `refdata` definition, `mktdata` deltas and a `snapshot` cycle. Extend
  `TestDispatch_InterleavedChannelsWithDistinctResetCountsRunNoBarrier`
  (`coordinator_test.go:316`), `TestDispatch_ResetOnOneChannelSparesTheOther`
  (`:335`) and `TestDispatch_SnapshotLevelStampedWithOpenGroupInstrument`
  (`:493`). Then the manifest test: both instances hold instruments at
  `Manifest Seq` 3; one path publishes a `manifest_summary` at 5; that
  path's stale instruments are pruned and the other path's are all still
  present, books, buffered deltas and gauges included.
- [ ] **Step 2: Run, watch them fail.** The manifest test fails by deleting
  everything below the cutoff on both paths.
- [ ] **Step 3: Re-key all of the above**, narrow `resetChannel`'s five keyed
  loops (`dispatch.go:338-374`) and `pruneManifest`'s one, narrow
  `SnapshotWriter.Reset`, and update `main.go`.
- [ ] **Step 4: Full suite, then `-race`**, `gofmt`, commit.

> **Six mutants**, each killed on its own. `rec.ChannelID` back in `resetCount`
> fails the barrier count; back in `open` fails the stamping test with one
> path's levels carrying the other's instrument; back in `instKey` fails the
> spared-book assertion. The full `channelInstance` in `instKey` instead: the
> three-port-role test must fail with the definition unreachable from the
> `mktdata` and `snapshot` records. Drop the path filter from `pruneManifest`:
> the manifest test must fail with the spared path's instruments gone. Restore
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
deliberate addition per table, not a consequence of task 6. `SnapshotWriter`'s `level_snapshots` rows are the exception, and the one place
the pair cannot both be read from a record. It flushes on a tick from
accumulated book state with no `Record` in hand, and its `instKey` carries
`publisherChannel{addr, ch}` after task 4 — no destination port, deliberately,
because a book is assembled from all three port roles and belongs to none. So
`source_addr` comes from the `instKey` and **`dst_port` is written as `0`**, the
sentinel for "assembled from the publisher channel rather than received on one
port". Criterion 7 is scoped around this the way criterion 4 is scoped around
`instruments`. A task that finds itself threading a port into the
`SnapshotWriter` to satisfy the criterion literally has picked the wrong one of
the two: the port would name whichever role happened to write last, which is
worse than the sentinel because it reads as a fact.

**And the row's `source_addr` is a string the writer builds, never the
`netip.Addr` itself.** A row is a `map[string]any` encoded with `encoding/json`
(`go/internal/clickhouse/client.go:202-208`,
`go/marketbyorder-bot/clickhouse.go:153-159`), so a `netip.Addr` in the map goes
through `MarshalText` — and the zero `Addr` marshals to `""`. That is not valid
input for an `IPv4` column: the column names itself in the row, so
`DEFAULT toIPv4(0)` never runs, the server refuses the insert, and because
`send` posts a whole batch as one body, one unidentified record fails every row
batched beside it — and a refused batch here is **dropped**, not queued
(`go/internal/clickhouse/client.go:159-173`), so those rows never arrive. A
book-builder deployed ahead of its parser would then load nothing at all, which
is the opposite of the window this plan is built around.
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
another comment: it is `channelInstance` and `publisherChannel`, and the fact
that `resetChannel` cannot be called with a bare `Channel ID`. The constraint they recorded is now
unstatable rather than merely undocumented.

`docs/superpowers/plans/2026-08-10-per-publisher-seq-tracking.md` keeps every one
of its copies. It is a dated document and a record of the code as it stood that
day.

**The panels.** `PARTITION BY channel_id, instrument_id` becomes
`PARTITION BY source_addr, channel_id, instrument_id` in panel 20,
"Sequence gaps (per-instrument)", of both dashboards
(`demo/grafana/dashboards/marketbyorder.json:1407`,
`demo/grafana/dashboards/marketbyprice.json:1345`). Without the address
`lagInFrame` over `per_instrument_seq` compares one path's sequence to the
other's and reports the difference as missing messages — the panel's own defect,
as a false positive. `dst_port` stays **out** of the partition:
`per_instrument_seq` is dense per publisher channel and instrument, and only
`mktdata`-port rows carry it (`go/marketbyorder-bot/events_writer.go:63-82`,
and the panel's own `WHERE per_instrument_seq > 0`), so the column is constant
inside every partition today and would split the series the day it is not.

The panel's `description` moves with its query. Both read "Missing messages
detected from the dense per-(channel,instrument) sequence (per_instrument_seq)"
(`marketbyorder.json:1411`, `marketbyprice.json:1349`), and after this change
the sequence the panel reads is dense per publisher channel and instrument, not
per channel and instrument. A description naming the old partition is the one
piece of this change an operator reads before deciding whether to trust the
number.

**The prose.** `go/marketbyprice-bot/README.md:20` ("Each
`(channel_id, instrument_id)`"), `:154`, `:156`, and
`go/marketbyorder-bot/README.md:13`, `:25`, restated on the publisher channel
for book state and on the channel instance for the sequence series.

- [ ] **Step 1: Delete the four comments and the two test comments.**
- [ ] **Step 2: Edit both dashboard `rawSql` strings, and both panel
  `description` strings with them.**
- [ ] **Step 3: Update both book-builder READMEs.** The `docs/README.md` row for
  this pair landed with the documents themselves.
- [ ] **Step 4: Full suite in all five Go modules, `-race` in both
  book-builders, `gofmt -l .` clean in each.**
- [ ] **Step 5: Verify the dashboards render** against the demo stack, with two
  paths carrying one `Channel ID` present in `events`, and confirm the panel
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

After task 13, on the demo stack with two paths carrying one `Channel ID` from
distinct source IP addresses on distinct destination ports:

- `dz_mbo_bot_channel_resets_total` and `dz_mbp_bot_channel_resets_total` flat
  while both paths publish steady, differing `Reset Count` values.
- Every instrument reaches `StatusReady` with a non-empty symbol and its real
  exponents, which is the end-to-end form of task 9(e): a book assembled across
  the three port roles rather than three fragments that never meet.
- `dz_mbo_bot_clickhouse_rows_dropped_total{reason="write_failed"}`, its
  market-by-price twin and `dz_bot_clickhouse_rows_dropped_total{reason=~"http_4.."}`
  all flat across the whole rollout. A refused batch is destroyed, so a
  non-zero rate here is rows already lost and not rows waiting.
- `dz_mbo_bot_seq_rebaselined_total` and its twin at zero against a publisher
  whose era store is healthy, and non-zero only against one whose era does not
  survive its own restart.
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
- `dz_mbp_bot` shows no instrument count dropping on the path that did not
  publish the manifest bump, across at least two `Manifest Seq` increments.
- Panel 20 on both dashboards reports no missing messages.
- `dz_mbo_bot_snapshot_order_dropped_total` and
  `dz_mbp_bot_snapshot_level_dropped_total` flat, where before the change each
  path's snapshot cycle discarded the other's levels.

## Out of scope

Named in the design and repeated here so no task reaches for them: arbitrating
between two paths carrying one channel into one book; binding more than one
destination port per port role in a parser; renaming `Record.Port` to
`PortRole`; changing any `ORDER BY`, including `marketbyorder.instruments`' and
`marketbyprice.instruments'` `(channel_id, instrument_id)`; renaming the parsers'
`source_ip` metric label; and keying anything in `go/topofbook-bot` or
`go/topofbook-parser`, which hold no per-instance recovery state. `go/topofbook-bot`
is touched by task 1 only, because `buildInsertURL` is the third copy of the
insert path the setting has to be pinned in.
