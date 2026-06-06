# DZ Market-by-Order Bot

> Implements the [Market-by-Order Feed](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-order/spec.md) spec.

Reference Go subscriber that consumes the DoubleZero Market-by-Order parser's Unix socket, maintains in-memory MBO order books per instrument, and persists per-event rows + coalesced top-N level snapshots + raw wire snapshots into ClickHouse.

Sibling to [topofbook-bot](../topofbook-bot/). Documentation will land as the implementation completes.

## Sharded dispatch

The bot shards record application across N worker goroutines keyed by
`instrument_id % N`. A single coordinator goroutine owns channel-scoped state
(`reset_count`, manifest, `snapshot_id → shard` routing) and forwards each
record to the owning shard; each shard exclusively owns its instruments,
refdata, per-instrument delta buffers, snapshot context, and its own
snapshot writer.

- `--shards N` — number of shards. `0` (default) derives N from `GOMAXPROCS`
  (`GOMAXPROCS-2`, clamped to `[1, 8]`). `--shards=1` is a valid degenerate
  single-worker mode behaviorally equivalent to the pre-sharding dispatcher.
- Per-instrument FIFO ordering and per-instrument sequence-gap detection are
  preserved. Cross-instrument global ordering is intentionally relaxed
  (ClickHouse rows are timestamped and queried per instrument).
- `end_of_session` / `batch_boundary` use an all-shard drain fence so their
  rows land after preceding instrument rows; `reset_count` changes use an
  in-band barrier that wipes all shard state before the new era.
- New metric: `dz_mbo_bot_snapshot_order_dropped_total` (snapshot_order with
  no registered route, e.g. begin missed or arrived post-end).

Design doc: `docs/2026-05-19-marketbyorder-bot-shard-dispatcher-design.md`.
