# Market-by-Price bot: persistence layer design

**Status:** implemented on `feat/marketbyprice-bot-persistence`. Two decisions below
were made during implementation rather than up front and are marked as such:
channel-scoped records are written by the Coordinator, and `Shard.persists` fails
closed.
**Parent spec:** [`2026-08-02-marketbyprice-design.md`](2026-08-02-marketbyprice-design.md), Components 3 and 4
**Delivers:** PR 4 of the parent spec's five-PR sequence

## Why this document exists

The parent spec specifies the ClickHouse schema (Component 3) and names the files
this PR adds, but it does not settle how the writers attach to a sharded,
multi-channel bot, and it leaves one question unanswered that materially changes
whether the replay table is useful. This document records those decisions and the
reasoning behind them. Where it is silent, the parent spec governs.

The book engine shipped in PR 3 (#34). It maintains correct books and emits
`ChannelEvent`s that nothing consumes: `Shard.handle` ends in `_ = evs` with a
comment pointing at this plan.

## Scope

In: `go/internal/clickhouse` (new shared package), `clickhouse.go`,
`events_writer.go`, `snapshot_writer.go`, the metrics those populate, `main.go`
and `Dockerfile` wiring, `--symbol` gating, and
`demo/clickhouse/init/03_schema_mbp.sql`.

Out: the demo stack (compose, Prometheus, Grafana, `docs/hyperliquid.md`) — that
is PR 5, and it is blocked on the live feed's multicast group, port sets, and
channel ID. Also out: migrating `marketbyorder-bot` and `topofbook-bot` onto the
shared client. See Open items.

## Decisions

### The ClickHouse client is extracted, not copied a third time

`marketbyorder-bot/clickhouse.go` (208 lines) and `topofbook-bot/clickhouse.go`
(318 lines) are two copies of the same idea that have already drifted: different
config shapes, `log` versus `log/slog`, different structure. Adding a third copy
would make the drift worse and give any client bug three places to be fixed.

The batching client moves to `go/internal/clickhouse`, the shared module the
receivers already consume. Market-by-price is its first consumer. The existing
two bots keep their copies for now — converting them means reconciling two
divergent designs and re-testing two shipped modules, which does not belong in
this PR.

### Metrics cross the boundary through an observer interface

`go/internal` has no Prometheus dependency today; it pulls Bubble Tea and Lip
Gloss for the receivers' TUI. Adding Prometheus to it would force one metric
namespace and one metric set on every future consumer, including the receivers,
which do not want them.

Instead the package defines a small interface:

```go
type Observer interface {
	RowsWritten(table string, n int)
	RowsDropped(table, reason string, n int)
	WriteError(table, reason string)
	BatchDuration(table string, d time.Duration)
	BufferedRows(table string, n int)
}
```

Each bot implements it over its own metrics, keeping its own names and namespace.
A nil `Observer` is valid and means "do not report", so the package is usable
without a metrics backend and testable in isolation.

### The snapshot writer is per shard, and its dirty map is keyed by `instKey`

Each `Shard` owns a `*SnapshotWriter`, matching `marketbyorder-bot`. The flush
loop reaches instruments through a `withInstrument` closure that runs under that
shard's own mutex, so a flush never contends with another shard's goroutine.

One change from the sibling is mandatory rather than stylistic. Market-by-order
keys its dirty map by a bare `uint32` instrument ID and hardcodes `channel 0`.
This bot keys all state by `(channel_id, instrument_id)`, and a shard owns
instruments across every channel for its id-modulo. A `uint32` key would collide
two channels' books onto one dirty entry and persist whichever flushed last,
silently. The dirty map is therefore keyed by `instKey`.

The rejected alternative was a single process-wide writer: one goroutine and a
simpler reset, but its flush loop serializes across every shard and takes each
shard's mutex in turn, contending with the shard goroutines. That contention
grows with instrument count, which is when it can least be afforded.

### `wire_levels` reads group identity from the last `SnapshotBegin`, accepted or not

`wire_levels` denormalizes `snapshot_id`, `anchor_seq`, `total_levels`,
`last_instrument_seq` and `depth_bound` onto every row. Those five come from
`SnapshotBegin`, not from the `SnapshotLevel` records themselves.

The obvious implementation reads them from the open shadow — and would capture
almost nothing. In steady state a ready, current instrument *declines* its
periodic snapshot at `SnapshotBegin`, so no shadow exists, yet the publisher
still sends every level of that group. Reading from the shadow would populate the
replay table only while instruments are recovering, and leave it near-empty
whenever the feed is healthy. That inverts what the table is for.

Each instrument therefore records the identity of its last `SnapshotBegin`
regardless of whether a shadow opened, and `wire_levels` rows read from that. The
cost is five fields per instrument.

### Channel-scoped records are written by the Coordinator, not by a shard

Decided during execution; the original data-flow diagram routed every table
through `Shard.handle`.

`heartbeat`, `manifest_summary` and `end_of_session` never reach a shard at all —
`Coordinator.Dispatch` classifies them itself and `Shard.apply` has no case for
them — so `channel_health` is written by the Coordinator directly.

`batch_boundary` is the subtler one, and it must be written there too. A boundary
carries no `instrument_id`, and every shard needs it because each evaluates
crossed-book for the instruments *it* touched, so the Coordinator broadcasts it
to all N. Writing the row from the shard side therefore turned one wire message
into N near-identical `events` rows — and inconsistent ones, since `handle`
resolves refdata for `instKey{rec.ChannelID, 0}` and only the shard owning
instrument 0 ever produced a symbol for it. The broadcast stays (the crossed-book
evaluation depends on it) and `applyBatchBoundary` still returns its event, but
the shard does not persist it.

Two consequences follow for the shard path. First, `Shard.persists` can fail
**closed**: with a filter active an empty symbol is no longer persisted, because
in the shard path an empty symbol only ever means the instrument's definition has
not arrived yet — routine at cold start, since the refdata cycle lags mktdata —
and not "this is a channel-scoped record". Second, `events` stays an applied-delta
log: `per_instrument_gap` and `malformed_delta` both carry an ordinary delta
`Record.Type` while having applied nothing, so `handle` gates on `ChannelEvent.Kind`
rather than letting the writer switch on `Record.Type` alone.

### `--symbol` gates persistence and read-out, never the book engine

The flag is declared today and does nothing (`_ = symbolFilter`, `main.go:51`) —
a user-facing flag that silently has no effect, the same class of defect the PR 3
review flagged for `seqLast`. This PR gives it the behaviour its help text
already promises.

When set, only matching symbols are written to ClickHouse and included in level
read-out. The book engine still tracks every instrument, because sequencing, gap
detection and the delta buffer are only correct if every record is processed.
Filtering is a persistence and presentation concern, never a correctness one.

`wire_levels` is written unconditionally in the sense that there is no separate
opt-in flag, matching `marketbyorder-bot`'s `wire_snapshots`. It is still subject
to `--symbol` like every other table: "always on" means no second switch, not
exempt from the filter.

## Components and data flow

```
parser socket
   -> Bot.read            (decodes with UseNumber)
   -> Coordinator         (routes; stamps snapshot_level with the open group)
        |-> EventsWriter.Write(ev)          -> channel_health (heartbeat,
        |                                      manifest_summary, end_of_session)
        |                                   -> events (batch_boundary)
        `-> Shard.handle   <- this PR replaces `_ = evs` here
              |-> EventsWriter.Write(ev)    -> events / instruments / wire_levels
              `-> SnapshotWriter.MarkDirty(k)
                       |                    -> level_snapshots (coalesced)
                       `-> internal/clickhouse.Client -> batched HTTP JSONEachRow
```

`Shard.handle` becomes:

```go
evs := s.apply(rec)
for _, ev := range evs {
	if persist && persistableFromShard(ev.Kind) {
		s.eventsW.Write(ev, rec.ChannelID, def.Symbol, def.PriceExponent, def.QtyExponent)
	}
	if ev.Kind == KindAppliedDelta || ev.Kind == KindAppliedSnapshot {
		s.sw.MarkDirty(instKey{rec.ChannelID, ev.InstrumentID})
	}
}
```

`EventsWriter.Write` takes the channel and the instrument's symbol and exponents
alongside the event, matching the sibling's signature: raw prices and quantities
are scaled into human units at the persistence boundary, never in book state.

That condition is why PR 3's review promoted the event kinds to constants and gave
the non-mutating paths their own: only a real book mutation may dirty an
instrument for snapshotting. A `batch_boundary` or `instrument_definition`
marking a book dirty would write an unchanged book on every boundary.

`EventsWriter` is stateless — it maps a record to rows and enqueues. The batcher
is already the asynchronous boundary and already drops to a counter when full, so
no second queue sits in front of it.

## Schema

`demo/clickhouse/init/03_schema_mbp.sql`, database `marketbyprice`. Five
`MergeTree` tables partitioned by day with a 30-day TTL, except `instruments`.
Columns follow the parent spec's Component 3. Points worth restating:

- `instruments` — `ReplacingMergeTree(recv_ts) ORDER BY (channel_id, instrument_id)`, no TTL.
- `events` — per-message log, `ORDER BY (symbol, recv_ts, kind)`, with per-kind
  columns for `level_update`, `book_clear`, `trade`, `liquidation`,
  `batch_boundary` and `instrument_reset`.
- `level_snapshots` — coalesced top-N, plus `crossed UInt8` and
  `depth_bound Nullable(UInt32)`. `LevelSnapshot` already carries both.
- `wire_levels` — raw `SnapshotLevel` capture, `ORDER BY (channel_id,
  instrument_id, snapshot_id, side, price)`.
- `channel_health` — heartbeats, manifest summaries, end-of-session.

`order_count` and `level_index` are `Nullable`. The wire uses `0xFFFF` as "not
supplied" and null is how that is spelled in SQL; zero is a real count.

## Metrics

PR 3 deliberately dropped metrics that nothing populated. This PR restores only
those it actually implements:

| Metric | Source |
|---|---|
| `clickhouse_rows_written_total{table}` | client observer |
| `clickhouse_rows_dropped_total{table,reason}` | client observer |
| `clickhouse_write_errors_total{table,reason}` | client observer |
| `clickhouse_batch_duration_seconds{table}` | client observer |
| `clickhouse_buffered_rows{table}` | client observer |
| `snapshot_writes_total` | snapshot writer |
| `snapshot_coalesces_total` | snapshot writer |
| `snapshot_lag_ms` | snapshot writer |
| `book_levels{symbol,side}` | snapshot writer flush |
| `book_top_price{symbol,side}` | snapshot writer flush |
| `book_top_qty{symbol,side}` | snapshot writer flush |
| `book_spread_bps{symbol}` | snapshot writer flush |

`depth_bounded_instruments` and `instruments_total` stay dropped — this PR still
does not populate them. Registering a metric with no writer is the defect the PR 3
review already caught once. A per-shard flush only ever sees the instruments that
are currently dirty, so neither gauge can be computed correctly from it without a
separate full sweep, which nothing needs yet.

This does not leave PR 5 short. The parent spec's dashboard panel for instruments
with a non-zero or unknown depth bound is served from `level_snapshots.depth_bound`
in ClickHouse, which this PR does write, rather than from a Prometheus gauge.

## Reset and disconnect

A channel reset runs the coordinator's existing barrier, which drains every shard
before wiping state. Each shard resets its snapshot writer as part of that: the
writer clears its dirty map and bumps a generation counter, so a flush batch
extracted before the reset is abandoned rather than written against post-reset
state.

`OnDisconnect` deliberately does **not** reset the snapshot writer. It clears
in-flight shadows because a half-built shadow spans the break, but live books stay
valid and keep being served, so pending dirty entries still point at real state.
Resetting there would discard queued writes for books that never changed.

## Error handling

A write failure is counted and dropped, never fatal and never a reason to reset a
channel — persistence is an observer of the feed, not part of it. A full buffer
drops the row and increments `clickhouse_rows_dropped_total{reason="buffer_full"}`
rather than blocking, because blocking would back-pressure into the shard
goroutine and from there into the socket read loop. An empty ClickHouse URL
disables persistence entirely and the bot runs as it does today.

## Build system

Taking a dependency on `go/internal` is not free, and two things must be handled
or the change breaks outside local `go test`.

**The container build.** `go/marketbyprice-bot/Dockerfile` copies every workspace
member's `go.mod` and `go.sum` so `go mod download` resolves, then copies only the
bot's own source, with a comment stating the other modules' source is not needed.
That stops being true the moment the bot imports `internal/clickhouse`, so the
build stage must also copy `go/internal/` source. This is the same class of
cross-module Dockerfile churn the PR 3 plan warned about.

**The standalone build.** `GOWORK=off go build` for darwin and linux is a Done
criterion carried over from PR 3. It keeps working through a `require` plus
`replace ... => ../internal` in the bot's `go.mod`, but both must be committed
along with a refreshed `go.sum`.

One risk to settle early rather than discover late: `go/internal` currently
requires Bubble Tea and Lip Gloss for the receivers' TUI. Module graph pruning
should keep them out of the bot's build, since it imports only
`internal/clickhouse`, but `go mod tidy` may still record them in the bot's
`go.sum`. Verify this in the first implementation step. If the graph turns out to
drag the TUI dependencies in, the fallback is to give the ClickHouse client its
own module rather than accept unrelated dependencies in a market data binary.

## Testing

- `internal/clickhouse` unit tests against an `httptest` server: batch-size and
  interval flushing, buffer-full drops, HTTP 4xx/5xx classification, drain-on-shutdown, nil observer.
- `EventsWriter` table tests mapping one record of each kind to expected rows,
  including the `Nullable` sentinel handling for `order_count` and `level_index`.
- `SnapshotWriter`: coalescing within the interval, the `instKey` keying that a
  bare `uint32` would collide, generation invalidation across a reset, and that a
  non-mutating `ChannelEvent` kind does not dirty a book.
- `wire_levels` capture for a *declined* snapshot, which is the case the obvious
  implementation misses.
- `--symbol` gating: filtered symbols absent from every table while the book
  engine still applies their deltas and keeps sequencing correct.

## Size

Estimated ~780 non-test Go lines, against the parent spec's ~550 estimate and the
repository's ~500 guideline. Splitting was considered and declined: the seam would
put the schema and the event log in one PR and the snapshot writer in another, and
the reviewer would see writers land before the tables they write into. The size is
flagged in the PR description instead.

## Open items

- `marketbyorder-bot` and `topofbook-bot` still carry their own ClickHouse
  clients. File an issue to migrate both onto `go/internal/clickhouse` once this
  lands, reconciling their divergent designs as its own change.
- PR 5 remains blocked on the live feed's multicast group, market-by-price port
  sets, and channel ID, per the parent spec's open item.
