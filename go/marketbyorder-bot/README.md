# DZ Market-by-Order Book-builder

> Implements the [Market-by-Order Feed](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-order/spec.md) spec.

Reference Go subscriber that consumes the DoubleZero Market-by-Order parser's Unix socket, maintains in-memory MBO order books per instrument, and persists per-event rows + coalesced top-N level snapshots + raw wire snapshots into ClickHouse.

Sibling to [topofbook-bot](../topofbook-bot/). Documentation will land as the implementation completes.

## Sharded dispatch

The book-builder shards record application across N worker goroutines keyed by
`instrument_id % N`. A single coordinator goroutine owns channel-scoped state
(`reset_count`, manifest, the open snapshot group per channel) and forwards each
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
- `snapshot_order` carries no `instrument_id` — its `snapshot_begin` implies it —
  so a shard files it into the currently-open snapshot group for the channel,
  which that `snapshot_begin` established for one `(channel_id, instrument_id)`.
  `Snapshot ID` is monotonic per instrument rather than per channel, so two
  instruments routinely sit at the same value within one cycle: it validates
  membership and is never the key. The same pointer stamps the `wire_snapshots`
  row, so the row and the shadow that received the order always name one
  instrument.
- The open group is held once per channel, at the coordinator and again in the
  shard that owns its instrument, and it is released by its own group's
  `snapshot_end`, by a `reset_count` barrier, or by a parser socket drop. One
  entry per channel rather than one per `snapshot_id`: ids advance every cycle,
  so a group whose end is lost would otherwise leave its key behind for good.
- A socket drop ends every group in flight. The reader resumes on reconnect with
  no other signal and `reset_count` is untouched by a socket-only drop, so the
  coordinator clears its open groups and each shard drops its own open groups,
  half-built shadows and snapshot contexts. Live books and their statuses stay
  as they are — a shadow is never the live book — so instruments the deltas are
  keeping correct go on serving, and the next snapshot cycle rebuilds the rest.
- `dz_mbo_bot_snapshot_order_dropped_total` counts a `snapshot_order` the
  association cannot place: no open group at the coordinator or in the owning
  shard (an order ahead of its `snapshot_begin`, trailing its `snapshot_end`, or
  arriving after a reconnect ended the group), or a `snapshot_id` that disagrees
  with the open group's. The orders of a group a ready instrument declined are
  not drops — they have no shadow to join, and that is the steady state.
- `dz_mbo_bot_snapshot_discarded_total{reason="mismatch"}` counts a
  `snapshot_end` whose `snapshot_id` or `anchor_seq` disagrees with the shadow
  it meets, including an end whose id is ahead of that shadow: its own
  `snapshot_begin` was lost, so the shadow belongs to a cycle whose end will
  never arrive, and the end that names the later group discards it. An end
  *behind* the shadow is the delayed end — the group it names is already
  finished — and it leaves the live shadow to commit on its own end.

Design doc: `docs/2026-05-19-marketbyorder-bot-shard-dispatcher-design.md`.
