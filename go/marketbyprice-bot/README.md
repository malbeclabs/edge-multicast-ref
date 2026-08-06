# marketbyprice-bot

Reference consumer for the DoubleZero [Market-by-Price feed](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-price/spec.md). It reads decoded records from [`marketbyprice-parser`](../marketbyprice-parser/README.md) over a Unix socket and maintains a price-keyed (L2) order book per instrument, with Prometheus metrics.

**Persistence is not implemented yet.** The engine maintains correct book state and exposes it through `ComputeLevels`, but nothing writes it anywhere. A follow-on plan adds the persistence layer that consumes the `ChannelEvent` stream and the level read-out.

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

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `--socket` | *(required)* | Path to the parser's Unix socket. |
| `--shards` | `0` | Instrument shards, keyed `instrument_id % n`. `0` derives `GOMAXPROCS-2`, clamped to `[1,8]`. |
| `--depth` | `20` | Read-out depth, levels per side. |
| `--symbol` | *(empty)* | Comma-separated symbol filter; empty means all. |
| `--metrics-addr` | `127.0.0.1:9094` | Prometheus `/metrics` listen address. |
| `-v` | `false` | Debug logging. |
| `--version` | | Print version and exit. |

`--depth` and `--symbol` are accepted but have no effect yet: both configure the level read-out, whose only consumer is the persistence layer in the follow-on plan.

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
| `snapshot_discarded_total{reason}` | Snapshots discarded: `stale_anchor`, `short`, `mismatch`, `no_open_snapshot`, `other`. |
| `snapshot_level_dropped_total` | `SnapshotLevel` records misrouted — no open group at the coordinator, or a `Snapshot ID` that does not match the open group. Levels belonging to a snapshot a ready-and-current instrument declined are NOT counted: the publisher sends every level of a group regardless, so counting them would bury the misroute signal under healthy steady state. |
| `delta_buffered_records{shard}` | Deltas currently buffered, per shard. Take `sum()` for the process total. |
| `delta_buffer_overflow_total` | Buffer evictions. Sustained non-zero means the snapshot cycle is too long for the memory budget. |

Every metric listed above is written by code in this binary. Book-state gauges and snapshot-writer metrics arrive with the persistence follow-on, alongside the subsystems that populate them — a registered collector nothing writes exports `0` forever, which reads as "configured and failing" rather than "absent".

## Architecture

```
parser socket → Bot (read loop) → Coordinator (1 goroutine) → N Shards → Instrument
```

The **Coordinator** owns channel-scoped state and routes each record to exactly one shard, or broadcasts it. It is not safe for concurrent callers: the bot read loop is the only caller. It also runs two barriers — a **reset barrier** on a `Reset Count` change, which drains every shard, wipes all state, and re-dispatches the triggering record as the first record of the new era; and a **FIFO fence** for channel-scoped records like `EndOfSession`, which orders them strictly after all preceding instrument records.

Snapshot routing follows the **currently-open group**, never `{channel, snapshot_id}`. `Snapshot ID` is monotonic per `(channel_id, instrument_id)`, not per channel, so two instruments routinely hold the same value within one cycle and an id-keyed route delivers levels to the wrong shard, where they are silently dropped.

Each **Shard** owns a disjoint set of instruments and all their book state. Its goroutine is the sole writer; `mu` guards book mutation so a reader can take a consistent level snapshot.

Shards report state changes outward as `ChannelEvent`s, which the persistence layer will consume. Only `applied_delta` and `applied_snapshot` assert that book state actually changed. `malformed_delta` reports a delta that arrived and was deliberately not applied — a consumer must not persist it as a mutation. The remaining kinds are `instrument_reset`, `channel_reset`, and `per_instrument_gap`.
