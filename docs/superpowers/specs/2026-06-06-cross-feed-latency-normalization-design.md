# Design: cross-feed latency normalization (source vs send, kernel-recv endpoint)

**Date:** 2026-06-06
**Branch:** `refactor/marketbyorder-rename` (follow-on work)

## Background

The Top-of-Book (TOB) and Market-by-Order (MBO) demo dashboards both display a
"wire latency" computed as `recv_ts − publisher_send_ts`, but the two feeds
populate `publisher_send_ts` from **different** wire timestamps, so the dashboards
silently measure different intervals:

- **TOB** stores the per-message **`SourceTimestamp`** (the Hyperliquid block
  `time`, i.e. when the validator set produced the block) in `publisher_send_ts`.
- **MBO** stores the frame header **`SendTimestamp`** (the publisher's multicast
  egress wall-clock) in `publisher_send_ts`.

This is the entire observed gap (e.g. ~262 ms TOB vs ~83 ms MBO for the same
symbol): TOB folds in the block→publisher segment; MBO starts its clock at
publisher egress. Reconstructing TOB's basis on the MBO side (`enter_ts` → bot)
gives ~347 ms, confirming the two are consistent once measured on the same basis.

Authoritative semantics (publisher repo `malbeclabs/hyperliquid`,
`docs/data-model.md`):

| Wire field | Meaning |
|---|---|
| TOB `Quote.source_timestamp` | block's `time` × 10⁶ (ms→ns) — block/venue time |
| TOB `Trade.source_timestamp` | fill `time` × 10⁶ |
| MBO `OrderAdd.enter_timestamp` | block `time` × 10⁶ |
| MBO `Trade.source_timestamp` | fill `time` × 10⁶ |
| MBO order_cancel/execute timestamp | event block time |
| frame header `send_timestamp_ns` (both) | publisher multicast egress wall-clock |

A second problem: neither bot records the **parser's kernel receive time**. The
TOB parser captures it via `SO_TIMESTAMPNS` and emits `parser_kernel_recv_ts_ns`,
but the bot discards it; the MBO parser does not capture it at all. Both bots use
`time.Now()` at unix-socket read as `recv_ts`, which adds parser→socket→bot
buffering noise to every latency number.

## Goal

Measure and expose, **consistently across both feeds**, two latencies anchored on
the kernel NIC receive time:

- **`source_latency`** = `parser_recv − source_ts` — block ("Tokyo") → your machine.
  The real end-to-end latency. Primary metric.
- **`send_latency`** = `parser_recv − send_ts` — publisher egress → your machine.
  The publisher-relay-to-subscriber segment. Secondary.

Plus a **sequence-gap** dashboard panel for each feed.

## Non-goals

- No renaming of metric namespaces (`dz_subscriber_*` for TOB vs `dz_mbo_*` for
  MBO is itself inconsistent, but renaming is out of scope and risky).
- `level_snapshots` (MBO, bot-derived) is excluded from latency: it has no wire
  source/send timestamp. Snapshot freshness is already covered by the existing
  `SnapshotLagMs` histogram.
- No change to the `--depth` flag, instrument/refdata handling, or book logic.

## Timestamp model (identical for both feeds)

Both parsers emit the **same three JSON fields** on every market-data record.
This normalization is the core of the change — it removes the ambiguity where
`ts` means "source" for TOB but "send" for MBO.

| JSON field | Type | Meaning | Per-feed source |
|---|---|---|---|
| `source_ts_ns` | uint64 ns (0 = absent) | block/venue time | TOB: quote/trade `SourceTimestamp`. MBO: `enter_ts` (order_add), event block-ts (order_cancel/order_execute), fill ts (trade), batch time (batch_boundary) |
| `send_ts_ns` | uint64 ns | publisher egress | frame header `SendTimestamp` (both). `ts` is normalized to equal this in both feeds. |
| `parser_kernel_recv_ts_ns` | uint64 ns | kernel NIC arrival | `SO_TIMESTAMPNS` (TOB already; **add to MBO**) |
| `recv_ts_kind` | string | `"kernel"` or `"app_fallback"` | which clock produced `parser_kernel_recv_ts_ns` |

Records with no meaningful source time (heartbeat, manifest_summary,
channel_reset, end_of_session) emit `source_ts_ns = 0` → `source_ts` NULL →
`source_latency` NULL.

`ts` is retained for backward compatibility but normalized to mean **send time**
in both feeds. Bots read the explicit fields above, not `ts`.

## ClickHouse schema (both feeds' market-data tables)

Applies to `topofbook.quotes`, `topofbook.trades`, `marketbyorder.events`, and
`marketbyorder.channel_health`. Replace the single `wire_latency_ms` with:

```sql
recv_ts            DateTime64(9),            -- parser kernel NIC recv (was bot time.Now)
publisher_send_ts  DateTime64(9),            -- frame egress (FIXES TOB, which stored source here)
source_ts          Nullable(DateTime64(9)),  -- block/venue time (NEW)
recv_ts_kind       LowCardinality(String),   -- "kernel" | "app_fallback"
send_latency_ms    Float64           MATERIALIZED (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(publisher_send_ts)) / 1e6,
source_latency_ms  Nullable(Float64) MATERIALIZED if(source_ts IS NULL, NULL,
                       (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(assumeNotNull(source_ts))) / 1e6),
```

Notes:
- `recv_ts` is repurposed to the kernel NIC time. `PARTITION BY toYYYYMMDD(recv_ts)`
  and `ORDER BY (symbol, recv_ts, …)` are unchanged and remain valid.
- Latencies are stored **signed/raw** (no `≥0` clamp) so clock skew is visible.
- MBO `events` keeps its existing `enter_ts` / `batch_ts` columns (domain fields);
  `source_ts` is the normalized cross-feed column and may duplicate `enter_ts`
  for order_add — that is intentional.
- The demo wipes the ClickHouse volume on each fresh boot, so this is a clean
  schema replacement, not a migration.

## Code changes

### MBO parser (`go/marketbyorder-parser/`)
- Port the TOB kernel-timestamp path: a `timestamp_linux.go` equivalent enabling
  `SO_TIMESTAMPNS` and reading the OOB control message via `ReadMsgUDP`, with a
  non-Linux / no-cmsg fallback to `time.Now()` (mirrors
  `go/topofbook-parser/timestamp_linux.go`).
- Thread `RecvTimestampNS` / `RecvTSKind` through `runner.go` into the Record.
- Add `source_ts_ns`, `send_ts_ns`, `parser_kernel_recv_ts_ns`, `recv_ts_kind` to
  the parser Record struct + JSON output; set `source_ts_ns` per message type in
  `decodeMessage` (order_add→enter, cancel/execute→event ts, trade→source, else 0).

### TOB parser (`go/topofbook-parser/`)
- Already captures the kernel recv time; keep it.
- Additionally emit `send_ts_ns` (frame header `SendTimestamp`, currently
  discarded for quotes/trades) and `source_ts_ns` (the value currently in `ts`).
- Normalize `ts` → send time.

### Both bots
- Read `source_ts_ns`, `send_ts_ns`, `parser_kernel_recv_ts_ns`, `recv_ts_kind`
  from JSON.
- `recv_ts` ← kernel time (`parser_kernel_recv_ts_ns`; fall back to bot read time
  only when `recv_ts_kind == "app_fallback"` AND the field is absent).
- Write the new columns; drop the old `publisher_send_ts = SourceTimestamp`
  (TOB) behavior.

## Prometheus (parser-side)

Replace each parser's single `wire_latency_seconds` histogram with two:
`source_latency_seconds` and `send_latency_seconds` (labelled by record
type/port, same buckets). Computed at the parser using the kernel recv time.
Negative observations are clamped to 0 for the histogram only (Prometheus
histograms can't represent negatives); the raw signed values still land in
ClickHouse. Metric namespaces are unchanged.

## Dashboards (`demo/grafana/dashboards/`)

For each of `topofbook.json` and `marketbyorder.json`:

- Replace the three "wire latency" panels with two latency groups:
  - **Source→recv (end-to-end)** — avg + p99 stat, p50/p95/p99 timeseries, from
    `source_latency_ms` (rows where `source_ts` is not null).
  - **Send→recv** — same shape, from `send_latency_ms`.
  - Display clamps negatives to 0 (`greatest(x, 0)`); keep symbol templating where
    the existing panel had it.
- Add a **Sequence gaps** panel:
  - **MBO:** window-diff `per_instrument_seq` per `(channel_id, instrument_id)`;
    a gap is `seq − prev_seq > 1`. Show gap-event count and total missing-message
    count over time.
  - **TOB:** window-diff `seq` per `channel_id`.

## Edge cases

- **Clock skew.** `source_latency` crosses the HL validator clock (Tokyo) and your
  kernel clock — it can be negative or inflated by NTP skew, and you don't control
  the validator clock. `send_latency` crosses the publisher host clock and yours
  (more controllable). Raw signed values are stored so skew is observable;
  dashboards clamp to ≥0 for display only.
- **Kernel-ts fallback.** If `SO_TIMESTAMPNS` is unavailable (non-Linux host or
  missing cmsg), `recv_ts_kind = "app_fallback"` and the parser app time is used.
  In the Linux demo containers this is always `"kernel"`.
- **No-source records.** Heartbeat / manifest_summary / channel_reset /
  end_of_session → `source_ts` NULL → `source_latency` NULL (excluded from
  source-latency aggregates by the `is not null` filter).
- **`level_snapshots`.** Untouched; excluded from the latency rework.

## Testing

- **MBO parser:** unit tests for kernel-ts extraction and the fallback path
  (mirroring TOB's `timestamp_*` tests).
- **Both parsers:** golden tests that JSON output carries `source_ts_ns`,
  `send_ts_ns`, `parser_kernel_recv_ts_ns`, `recv_ts_kind` with the correct values
  from sample frames, per record type (esp. the TOB send-vs-source split and the
  MBO per-type `source_ts`).
- **Both bots:** writer tests asserting the three timestamp columns map correctly
  per record type.
- **Schema:** the materialized `send_latency_ms` / `source_latency_ms` compute the
  expected milliseconds (incl. NULL `source_ts` → NULL latency) on known inputs.
- **E2E sanity:** after a live run, both feeds report comparable `source_latency`
  for the same symbol, and `source_latency ≈ send_latency + (send_ts − source_ts)`.
