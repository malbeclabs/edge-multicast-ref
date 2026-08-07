# marketbyprice-bot

Reference consumer for the DoubleZero [Market-by-Price feed](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-price/spec.md). It reads decoded records from [`marketbyprice-parser`](../marketbyprice-parser/README.md) over a Unix socket and maintains a price-keyed (L2) order book per instrument, with Prometheus metrics.

The engine maintains correct book state and exposes it through `ComputeLevels`. When `--clickhouse-url` is set, the `ChannelEvent` stream and the level read-out are persisted to ClickHouse across five tables — see [Persistence](#persistence) below.

## Input

One JSON record per line on the parser's Unix socket. The envelope is the parser's `Record` — see the [parser README](../marketbyprice-parser/README.md) for the field list and the per-message-type `fields` payloads.

Two properties of that envelope matter here:

- **`snapshot_level` records carry no `instrument_id`.** The wire omits it because the containing `SnapshotBegin` implies it. The coordinator stamps the identity from the currently-open snapshot group before routing.
- **An absent `order_count` means the `0xFFFF` sentinel, not zero.** The parser omits the key when the venue does not expose a count. Zero is a real count and is emitted as `0`.

The bot reconnects to the socket with exponential backoff (250 ms doubling to 5 s) and survives the parser restarting under it.

## State machine

Each `(channel_id, instrument_id)` sits in one of three statuses:

| Status | Meaning |
|---|---|
| `awaiting-snapshot` | No usable book. Deltas are buffered, not applied. |
| `ready` | Book is usable and deltas apply in sequence. |
| `gap` | A per-instrument sequence gap was confirmed. Deltas buffer until the next snapshot repairs it. |

The spec's five-state machine collapses to three because two of its states are represented orthogonally: *awaiting-refdata* is absence from the shard's instrument map, and *building-snapshot* is `OpenSnapshot != nil`, deliberately independent of serving status so that building a snapshot can never make a good book unavailable.

Transitions: a committed snapshot moves any status to `ready`. A confirmed per-instrument gap or a delta-buffer eviction moves `ready` to `gap`. An `InstrumentReset` moves any status to `awaiting-snapshot` and records a required anchor.

## Feed-specific behavior

**The `Last Instrument Seq` discriminator.** A periodic snapshot for an already-`ready` instrument is ignored unless its `Last Instrument Seq` exceeds the instrument's own tracker — that is, unless it was captured after deltas this subscriber never applied. `Anchor Seq` is deliberately *not* used for this: it is a channel-wide `mktdata` sequence that advances on every other instrument's deltas and on every heartbeat, so comparing it would rebuild every good book on every snapshot rotation.

**Shadow commit.** A snapshot is built into a shadow (`PendingSnapshot`), never into the live book. On any validation failure — wrong `Snapshot ID`, wrong `Anchor Seq`, or a level count that does not match `Total Levels` — only the shadow is discarded. Status and the live book are untouched. This departs from the spec's literal "discard the partial book and revert to awaiting-snapshot" for an already-`ready` instrument, because dropping a book the deltas are keeping correct costs a full round-robin cycle of availability and buys nothing.

**Depth bound.** `DepthBound` is `nil` (unknown) until a `SnapshotBegin` establishes it, and MUST NOT default to `0`. A wire value of `0` is the publisher's positive claim that it carries the complete book; a non-zero `N` means level state beyond `N` per side is *unknown rather than empty*. A never-snapshotted instrument has made no such claim, and defaulting it to `0` would manufacture one.

**Crossed-book monitoring.** The inside market is compared at each consistency point and a crossing is counted. On a channel where no `BatchBoundary` has been seen, every applied delta is a consistency point. Once boundaries appear, evaluation defers to them and covers only instruments touched since the previous boundary — intermediate states inside a batch are explicitly not consistency points, so a transient cross there is legal rather than a defect. This is **observability only**: it never changes status, discards a book, or triggers a re-bootstrap.

**Bounded delta buffer.** Deltas for instruments that are not `ready` are buffered, ordered by `mktdata` sequence, bounded at 200,000 records per shard. On overflow the instrument holding the most buffered records is evicted wholesale and marked `gap`; it recovers on its next snapshot like any other gap instrument. Sustained overflow means the publisher's snapshot cycle period is too long for the deployment's memory budget — the cycle-period knob and the subscriber-memory knob are the same knob — which is why it is counted rather than silently absorbed.

## Level read-out

`ComputeLevels(inst, n)` returns the best `n` levels per side, scaled from raw integers by the instrument's `Price Exponent` and `Qty Exponent`. Prices and quantities are held **raw** (`int64`/`uint64`) everywhere in the engine and scaled only here; book state never holds floats.

Rank is derived by sorting price keys at read time, never stored — the spec forbids keying book state on rank, because a positional key is invalidated by every insertion at a better price.

**`CumulativeQty` is exhaustive depth only when `DepthBound` is a non-nil `0`.** Under a non-zero bound the levels beyond it are unknown rather than empty, so summing it understates available liquidity — the exact failure `Depth Bound` exists to prevent. Under a `nil` bound nothing is known about completeness at all.

An `order_count` of `0xFFFF` on the wire reads out as `0`, because the sentinel means *absent*. Do not read it as a real count of 65535.

## Persistence

When `--clickhouse-url` is non-empty, the bot writes to five ClickHouse tables:

| Table | Source | Contents |
|---|---|---|
| `instruments` | `instrument_definition` | Refdata: symbol, exponents, contract terms. |
| `events` | Applied deltas, trades, liquidations, instrument resets, batch boundaries | One row per applied `level_update` or `book_clear`, per trade, per liquidation, per `InstrumentReset` — and one row per `BatchBoundary`, written by the Coordinator. See the caveats below. |
| `wire_levels` | `snapshot_level` | Raw snapshot levels, captured for replay — including levels belonging to a *declined* snapshot, since the publisher sends every level of a group regardless of whether this subscriber needed it. |
| `channel_health` | `heartbeat`, `manifest_summary`, `end_of_session` | Channel-scoped records that carry no instrument. |
| `level_snapshots` | `ComputeLevels`, via the snapshot writer | The top-N read-out per instrument, coalesced to at most one write per `--coalesce-ms` per instrument. |

**`events` is not one row per `ChannelEvent`.** The mapping is deliberately narrower than the event stream in both directions:

- **`applied_snapshot` produces no row.** Its `Record.Type` is `snapshot_end`, which the writer has no case for. The committed book is captured in `level_snapshots` instead, and the group's raw levels in `wire_levels`.
- **`per_instrument_gap` and `malformed_delta` produce no row.** Both report a record that was seen and deliberately *not* applied — a gapped delta is buffered for replay, and a malformed `BookClear` is discarded without advancing the sequence trackers — yet both carry an ordinary delta `Record.Type` (`level_update` and `book_clear` respectively). Persisting them would make them indistinguishable from real applied deltas in a table defined as an applied-delta log. They remain visible as `per_instrument_gaps_total` and in the log.
- **`batch_boundary` produces exactly one row per wire message.** It is channel-scoped, carries no `instrument_id` and no symbol, and is broadcast to every shard so each can evaluate crossed-book for its own instruments. The Coordinator writes the single row; the shards do not.
- **`instrument_definition` writes to `instruments`, not `events`.**

**`events` is an applied-delta log, not a wire capture.** A delta that arrives while an instrument is `awaiting-snapshot` or `gap` is buffered, not applied. Once a snapshot commits and `replayBuffer` replays the buffer against the now-`ready` book, each buffered delta IS applied to the book — but that replay path discards the resulting event rather than handing it to the writer, so those deltas never produce an `events` row. This is pre-existing engine behavior, orthogonal to `--symbol` filtering, and is deliberately not being changed here. Do not treat `events` as a complete record of every delta seen on the wire — treat it as a complete record of every delta applied outside of buffered replay. The book itself (and therefore `level_snapshots`) is unaffected: every delta is applied exactly once, buffered or not.

**An empty `--clickhouse-url` disables persistence entirely**, not just partially: the client is `nil`, every writer call returns before it builds a row, and the bot runs exactly as it did before persistence existed — no writes attempted, no errors, and every `clickhouse_*` metric plus `snapshot_writes_total` holds at `0`. The nil client is handed to the writers through a guard rather than assigned directly, because a typed nil pointer stored in an interface is not `== nil` and would silently defeat every one of those checks.

**`--clickhouse-batch-size`, `--clickhouse-batch-interval` and `--clickhouse-buffer-size` must all be positive.** The client rejects a non-positive value at construction and the bot exits with the offending table named. They are validated rather than clamped because each failure mode is bad in its own way: a non-positive interval panics the batcher's ticker in a goroutine that nothing recovers, and a non-positive buffer makes the queue unbuffered, so the deliberately non-blocking enqueue drops very nearly every row to a counter.

**A write failure is counted and dropped, never propagated to the feed.** A batch that fails to insert increments `clickhouse_write_errors_total{table,reason}` and `clickhouse_rows_dropped_total{table,reason="write_failed"}`, then the batch is discarded. The batching client is the asynchronous boundary between the book engine and ClickHouse, so a wedged or unreachable database degrades to data loss for that table — it never backpressures into the socket read loop or slows the feed.

**`--symbol` gates persistence and read-out only — never the book engine.** Every instrument is fully processed regardless of the filter: sequencing, per-instrument gap detection, and the delta buffer are only correct if every record is applied, so a filtered-out symbol's book is maintained exactly as if it were unfiltered. What the filter controls is whether that symbol's rows reach ClickHouse and whether its book-state gauges (`book_levels`, `book_top_price`, `book_top_qty`, `book_spread_bps`) get updated: a filtered symbol produces no rows in `instruments`, `events`, `wire_levels`, or `level_snapshots`.

The filter **fails closed**. An instrument whose definition has not arrived yet resolves to an empty symbol, which does not match any filter and is therefore not persisted. That state is routine rather than exotic — the refdata cycle lags mktdata, so at cold start an `InstrumentReset`, a captured `SnapshotLevel`, and even a committed book's read-out can all reach the persistence boundary before the instrument's own definition does. Failing open there would leak exactly the instruments the operator filtered out, under a blank symbol.

Channel-scoped rows are never filtered, and never pass through this check at all: `heartbeat`, `manifest_summary`, `end_of_session` and `BatchBoundary` are written by the Coordinator, which holds no symbol filter, because they describe the channel rather than an instrument.

**`publisher_send_ts` on `level_snapshots` is the send timestamp of the last record the book applied**, not the moment the read-out was taken. A read-out is computed by this process and has no send timestamp of its own, so each instrument carries the send timestamp of the last record that actually changed its book — an applied delta, or the `SnapshotEnd` that committed a shadow — alongside its sequence trackers. The schema derives `wire_latency_ms` as `recv_ts - publisher_send_ts`, so stamping the flush time into both columns would pin that measurement at `0.0` for every row that could ever exist.

**`cumulative_qty` in `level_snapshots` is exhaustive depth only when `depth_bound` is a non-null `0`.** Under a non-zero bound, or a null bound, levels beyond what was captured are unknown rather than empty, so summing `cumulative_qty` as if it were the whole book understates available liquidity.

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `--socket` | *(required)* | Path to the parser's Unix socket. |
| `--shards` | `0` | Instrument shards, keyed `instrument_id % n`. `0` derives `GOMAXPROCS-2`, clamped to `[1,8]`. |
| `--depth` | `20` | Read-out depth, levels per side, passed to `ComputeLevels` by the snapshot writer. |
| `--symbol` | *(empty)* | Comma-separated symbol filter. Empty means no filter. Gates persistence and read-out only — see [Persistence](#persistence). |
| `--metrics-addr` | `127.0.0.1:9094` | Prometheus `/metrics` listen address. |
| `-v` | `false` | Debug logging. |
| `--version` | | Print version and exit. |
| `--clickhouse-url` | *(empty)* | ClickHouse HTTP endpoint. Empty disables persistence entirely. |
| `--clickhouse-database` | `marketbyprice` | ClickHouse database. |
| `--clickhouse-batch-size` | `500` | Rows per insert batch. |
| `--clickhouse-batch-interval` | `1s` | Maximum time between insert batches. |
| `--clickhouse-buffer-size` | `20000` | Per-table row buffer; rows are dropped when full. |
| `--coalesce-ms` | `50` | Minimum interval between `level_snapshots` writes, per instrument. |

## Metrics

Namespace `dz_mbp_bot`.

| Metric | Meaning |
|---|---|
| `build_info` | Build version and commit; value always 1. |
| `uptime_seconds` | Seconds since process start. |
| `socket_connected` | 1 while connected to the parser socket. |
| `socket_reconnects_total{reason}` | Socket reconnections. |
| `socket_to_bot_latency_seconds{type}` | Parser kernel receive to bot dispatch. |
| `records_total{type}` | Records consumed, by record type. |
| `decode_errors_total` | Unparseable lines from the socket. |
| `book_divergence_total{kind}` | Publisher/subscriber disagreements on a `LevelUpdate`: `new_on_present`, `change_on_absent`, `delete_nonzero_qty`, `zero_qty_wrong_action`. Counted without altering the applied result. |
| `crossed_book_events_total` | Crossed inside-market observations at consistency points. |
| `crossed_instruments{shard}` | Instruments currently crossed, per shard. Shards own disjoint instruments, so take `sum()` for the process total. |
| `per_instrument_gaps_total` | Confirmed per-instrument sequence gaps. |
| `instrument_resets_total{reason}` | `InstrumentReset` messages applied. |
| `channel_resets_total` | `Reset Count` era changes, each draining every shard. |
| `snapshot_discarded_total{reason}` | Snapshots discarded: `stale_anchor`, `short`, `mismatch`, `other`. A `snapshot_end` with no open shadow is not a discard — it is the healthy path where a ready, current instrument declined the begin. |
| `snapshot_level_dropped_total` | `SnapshotLevel` records misrouted — no open group at the coordinator, or a `Snapshot ID` that does not match the open group. Levels belonging to a snapshot a ready-and-current instrument declined are NOT counted: the publisher sends every level of a group regardless, so counting them would bury the misroute signal under healthy steady state. |
| `deltas_discarded_total{reason}` | Deltas seen and not applied. `stale_seq` is a duplicate or late frame — benign in bursts, but a sustained climb on one instrument with no matching applied traffic means a snapshot set the sequence tracker ahead of reality and the book is wedged. |
| `delta_buffered_records{shard}` | Deltas currently buffered, per shard. Take `sum()` for the process total. |
| `delta_buffer_overflow_total` | Buffer evictions. Sustained non-zero means the snapshot cycle is too long for the memory budget. |
| `clickhouse_rows_written_total{table}` | Rows successfully inserted, per table. |
| `clickhouse_rows_dropped_total{table,reason}` | Rows dropped before or during insert: `buffer_full` (the per-table channel was full) or `write_failed` (the insert errored and the batch was discarded). |
| `clickhouse_write_errors_total{table,reason}` | Insert errors, per table. |
| `clickhouse_batch_duration_seconds{table}` | Time spent inserting one batch, per table. |
| `clickhouse_buffered_rows{table}` | Rows currently queued for insert, per table. |
| `snapshot_writes_total` | `level_snapshots` writes committed by the snapshot writer. |
| `snapshot_coalesces_total` | Times a `MarkDirty` was absorbed into an already-pending write instead of starting a new one. |
| `snapshot_lag_ms` | Time from an instrument being marked dirty to its snapshot actually being written. |
| `book_levels{symbol,side}` | Levels present in the most recent read-out, per symbol and side. |
| `book_top_price{symbol,side}` | Best price in the most recent read-out, per symbol and side. |
| `book_top_qty{symbol,side}` | Quantity at the best price in the most recent read-out, per symbol and side. |
| `book_spread_bps{symbol}` | Best-ask/best-bid spread in basis points, per symbol. |

Every metric listed above is written by code in this binary — no collector is registered without a writer that populates it, because a registered collector nothing writes exports `0` forever, which reads as "configured and failing" rather than "absent". The `book_*` gauges are the read-out, so they populate only for symbols that pass the `--symbol` filter; a filtered-out symbol's gauges are simply never set, not set to zero. The `clickhouse_*` counters are labelled by table, not by symbol, and keep incrementing normally for unfiltered traffic — a filtered symbol just contributes no rows to any of them.

## Architecture

```
parser socket → Bot (read loop) → Coordinator (1 goroutine) → N Shards → Instrument
```

The **Coordinator** owns channel-scoped state and routes each record to exactly one shard, or broadcasts it. It is not safe for concurrent callers: the bot read loop is the only caller. It also runs two barriers — a **reset barrier** on a `Reset Count` change, which drains every shard, wipes all state, and re-dispatches the triggering record as the first record of the new era; and a **FIFO fence** for channel-scoped records like `EndOfSession`, which orders them strictly after all preceding instrument records.

Snapshot routing follows the **currently-open group**, never `{channel, snapshot_id}`. `Snapshot ID` is monotonic per `(channel_id, instrument_id)`, not per channel, so two instruments routinely hold the same value within one cycle and an id-keyed route delivers levels to the wrong shard, where they are silently dropped.

Each **Shard** owns a disjoint set of instruments and all their book state. Its goroutine is the sole writer; `mu` guards book mutation so a reader can take a consistent level snapshot.

Shards report state changes outward as `ChannelEvent`s, which the `EventsWriter` persists — see [Persistence](#persistence). Only `applied_delta` and `applied_snapshot` assert that book state actually changed; every other kind reports a record that was seen and deliberately not applied, so a consumer must not persist it as a mutation. The full set is:

| Kind | Book changed | Meaning |
|---|---|---|
| `applied_delta` | yes | A `LevelUpdate` or `BookClear` applied in sequence. |
| `applied_snapshot` | yes | A snapshot shadow validated and committed. |
| `instrument_definition` | no | Refdata only. |
| `instrument_reset` | no | The book was cleared and a required anchor recorded. |
| `trade` | no | A `Trade` or `Liquidation`; no book effect. |
| `batch_boundary` | no | A consistency point; carries no instrument. |
| `per_instrument_gap` | no | A confirmed sequence gap; the record was buffered, not applied. |
| `malformed_delta` | no | A `BookClear` the engine rejected; nothing was applied and the sequence trackers did not advance. |

There is no `channel_reset` kind. A `Reset Count` era change is handled by draining every shard through the reset barrier, which produces no per-instrument event; it is observable as `channel_resets_total`.

Only `applied_delta` and `applied_snapshot` mark an instrument dirty for the snapshot writer. Dirtying on a non-mutating kind would rewrite an unchanged book on every batch boundary.
