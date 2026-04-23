# DZ Depth-of-Book Demo Stack — Design Spec

**Date:** 2026-04-23
**Status:** Draft
**Scope:** New `depthofbook-parser` Go binary, new `depthofbook-bot` Go binary, schema additions to the existing `demo/` Docker stack, new Grafana dashboard, rename of `go/example-bot/` to `go/topofbook-bot/`.

## Overview

Sibling pipeline to the existing top-of-book demo, consuming the [DoubleZero Depth-of-Book Feed v0.1.0](https://github.com/malbeclabs/edge-feed-spec/blob/main/depth-of-book/spec.md) (DZ-DOB). End-to-end:

```
publisher (Hyperliquid)
   ─── multicast UDP, 3 ports ──▶ depthofbook-parser
                                    (stateless wire decoder)
                                       │
                                       │ broadcast unix socket, JSONL
                                       │ one record per wire message
                                       ▼
                                 depthofbook-bot
                                    (book builder + persistor)
                                       │ ClickHouse HTTP (batched JSONEachRow)
                                       ▼
                                  ClickHouse  ◀──native──  Grafana
                                  database: depthofbook
```

Three new components: `go/depthofbook-parser/`, `go/depthofbook-bot/`, and additions to the existing `demo/` directory. Shares ClickHouse, Grafana, and `docker-compose.yml` with the TOB pipeline.

**Real-world driver:** a Hyperliquid publisher is being built in parallel and will deliver MBO data over UDP multicast. The demo runs against any DZ-DOB-compliant publisher.

**Non-goals for v1:**
- Pcap input (the wire decoder will be cleanly separable from the multicast receiver so this is an easy follow-up)
- Reconciliation between the bot's reconstructed book and any reference feed
- Order book viewer in the bot itself (Grafana is the viewer)
- Coexistence with `topofbook-bot` consuming the same parser socket — they're independent pipelines that just share ClickHouse and Grafana
- Authentication, TLS, or any networking beyond what TOB already uses

## Architecture

### Component responsibilities

| Component | Owns | Does NOT own |
|---|---|---|
| `depthofbook-parser` | Wire decoding, multicast join (3 ports), JSONL broadcast | Book state, refdata tracking, exponent application, symbol filtering |
| `depthofbook-bot` | State machine, book maintenance, level aggregation, persistence | Wire decoding, Grafana queries |
| ClickHouse | Tick storage, retention, derived columns | Bot logic, dashboard queries |
| Grafana | Visualization, templating | Data acquisition, alerting (out of scope) |

The split between parser and bot mirrors how production trading systems separate parsing from book building. The parser stays simple enough that pcap input or per-language re-implementations are practical. The bot owns all the spec's recovery logic in one place.

### Data flow

1. Parser binds three UDP sockets on `(multicast_group, refdata_port)`, `(group, mktdata_port)`, `(group, snapshot_port)`. Each runs in its own goroutine.
2. Parser decodes each wire frame, emits one JSONL `Record` per application message on a broadcast Unix socket.
3. Bot connects to the parser socket, reads JSONL line-by-line, dispatches each Record into its channel state machine.
4. Bot maintains an in-memory order book per `(channel_id, instrument_id)` keyed by `order_id`. Implements the spec's cold-start, snapshot reassembly, gap detection, and reset procedures.
5. Bot writes to ClickHouse on three paths:
   - Per-event rows (every order delta, trade, structural event) → `events` table
   - Wire-snapshot orders (every `SnapshotOrder`) → `wire_snapshots` table
   - Coalesced top-N level snapshots → `level_snapshots` table
   - InstrumentDefinition rows → `instruments` table
   - Heartbeats and ManifestSummary → `channel_health` table
6. Grafana queries ClickHouse via the native protocol; dashboards auto-provisioned.

## `depthofbook-parser`

Stateless wire decoder. No book state, no refdata cache, no symbol filtering. Emits raw integer prices and quantities — the bot scales them via `InstrumentDefinition` exponents. This keeps the parser truly stateless and makes the JSONL output ground-truth (no implicit conversions, no precision loss).

### File layout

```
go/depthofbook-parser/
├── main.go              # CLI flags, signal handling, sink and parser wiring
├── runner.go            # Three goroutines: refdata + mktdata + snapshot UDP receivers
├── parser.go            # Parser interface, Record output type, registry
├── depthofbook.go       # Parser impl: routes wire frames into Record stream
├── depthofbook_wire.go  # Binary frame decoder for all DZ-DOB message types
├── sink.go              # Reused interface: OutputSink { Write(records) error }
├── sink_socket.go       # Reused: broadcast Unix socket, drop-on-slow-consumer
├── sink_json.go         # Reused: JSONL file output (debugging only)
├── metrics.go           # Prometheus metrics + HTTP /metrics endpoint
├── Dockerfile
├── README.md
├── go.mod / go.sum
└── *_test.go
```

The `sink.go`, `sink_socket.go`, and `sink_json.go` files are copies of the TOB versions, not imports — keeps each binary self-contained per the existing repo convention. The `OutputSink` interface and `Record` envelope match TOB's structurally; the bot intentionally re-declares its own `Record` type matching the on-the-wire JSON.

### CLI flags

| Flag | Required | Default | Description |
|---|---|---|---|
| `--group` | yes | — | Multicast group IP (e.g., `239.10.10.20`) |
| `--refdata-port` | yes | — | UDP port for refdata channel |
| `--mktdata-port` | yes | — | UDP port for mktdata channel |
| `--snapshot-port` | yes | — | UDP port for snapshot channel |
| `--interface` | no | system default | Network interface for IGMP join (e.g., `doublezero1`) |
| `--output` | yes | — | `unix:///path/to/sock` or `file:///path/to/log` |
| `--format` | no | `json` | `json` (only one supported in v1; CSV does not handle the variable-width fields) |
| `--metrics-addr` | no | — | Prometheus endpoint, empty = no metrics server (e.g., `127.0.0.1:9091`) |
| `-v` | no | false | Debug logging |
| `--version` | no | — | Print version and exit |

### Wire message → Record mapping

Every wire message produces exactly one Record:

| Wire message | `type` | Port | Notes |
|---|---|---|---|
| `Heartbeat` (0x01) | `heartbeat` | mktdata | |
| `InstrumentDefinition` (0x02) | `instrument_definition` | refdata | |
| `Trade` (0x04) | `trade` | mktdata | Inherited from TOB byte-for-byte |
| `EndOfSession` (0x06) | `end_of_session` | mktdata | |
| `ManifestSummary` (0x07) | `manifest_summary` | refdata | |
| `OrderAdd` (0x10) | `order_add` | mktdata | |
| `OrderCancel` (0x11) | `order_cancel` | mktdata | |
| `OrderExecute` (0x12) | `order_execute` | mktdata | |
| `BatchBoundary` (0x13) | `batch_boundary` | mktdata | |
| `InstrumentReset` (0x14) | `instrument_reset` | mktdata | |
| `SnapshotBegin` (0x20) | `snapshot_begin` | snapshot | |
| `SnapshotOrder` (0x21) | `snapshot_order` | snapshot | |
| `SnapshotEnd` (0x22) | `snapshot_end` | snapshot | |

### Record envelope

```go
type Record struct {
    Type           string         `json:"type"`
    Timestamp      time.Time      `json:"ts"`            // Frame's send_ts (publisher wall-clock)
    ChannelID      uint8          `json:"channel_id"`
    Port           string         `json:"port"`          // "refdata" | "mktdata" | "snapshot"
    SequenceNumber uint64         `json:"seq"`           // Frame-level seq for THIS port
    ResetCount     uint8          `json:"reset_count"`
    InstrumentID   uint32         `json:"instrument_id,omitempty"`
    Fields         map[string]any `json:"fields,omitempty"`
}
```

`Symbol` is not populated by the parser — the bot resolves `instrument_id → symbol` from its refdata cache.

### `Fields` content per record type

All prices and quantities are emitted as **raw signed/unsigned integers** in the units defined by the wire format (i.e., `int64` for `price`, `uint64` for `qty`, no exponent applied). The bot applies the `price_exponent` / `qty_exponent` from `InstrumentDefinition` when scaling for storage and display.

`order_add`:
- `source_id` (uint16), `side` ("bid"|"ask"), `order_flags` (uint8)
- `per_instrument_seq` (uint32), `order_id` (uint64), `enter_ts` (RFC3339)
- `price_raw` (int64), `qty_raw` (uint64)

`order_cancel`:
- `source_id`, `cancel_reason` (string from spec table), `per_instrument_seq`, `order_id`, `timestamp`

`order_execute`:
- `source_id`, `aggressor_side` ("buy"|"sell"|"unknown"), `exec_flags` (uint8)
- `per_instrument_seq`, `order_id`, `trade_id`, `timestamp`
- `exec_price_raw`, `exec_qty_raw`

`trade`:
- `source_id`, `aggressor_side`, `trade_flags` (uint8)
- `source_timestamp`, `trade_price_raw`, `trade_qty_raw`
- `trade_id`, `cumulative_volume_raw`

`instrument_definition`:
- All 14 fields from the wire body (symbol, leg1, leg2, asset_class, price_exponent, qty_exponent, market_model, tick_size_raw, lot_size_raw, contract_value, expiry, settle_type, price_bound, manifest_seq)

`heartbeat`, `end_of_session`: just timestamp; no extra fields beyond envelope.

`manifest_summary`: `valid` (uint8), `manifest_seq` (uint16), `instrument_count` (uint32), `timestamp`

`batch_boundary`: `batch_id` (uint32), `batch_ts` (RFC3339)

`instrument_reset`: `reason` (string), `new_anchor_seq` (uint64), `timestamp`

`snapshot_begin`: `anchor_seq` (uint64), `total_orders` (uint32), `snapshot_id` (uint32), `last_instrument_seq` (uint32), `timestamp`

`snapshot_order`: `snapshot_id` (uint32), `order_id` (uint64), `side`, `order_flags`, `enter_ts`, `price_raw`, `qty_raw`

`snapshot_end`: `anchor_seq`, `snapshot_id`

### Prometheus metrics (prefix `dz_dob_parser_`)

- `ingress_packets_total{port}`, `ingress_bytes_total{port}`
- `parse_errors_total{port,reason}` — reasons: `bad_magic`, `schema_version`, `frame_length`, `truncated`, `other`
- `records_total{type}`
- `wire_latency_seconds{port}` — histogram of `now() - frame.send_ts` at parse time (includes clock skew)
- `socket_clients`
- `socket_client_drops_total{reason}` — `slow_writer`, `disconnected`
- `socket_records_sent_total`
- `sink_write_errors_total`
- `build_info{version,commit}`, `uptime_seconds`

## `depthofbook-bot`

Stateful book builder and persistor. Reads JSONL from the parser socket, applies the spec's subscriber algorithm per channel, maintains in-memory order books, and writes both per-event rows and coalesced level-snapshot rows to ClickHouse.

### File layout

```
go/depthofbook-bot/
├── main.go              # CLI flags, signal handling, wiring
├── bot.go               # Read parser socket, decode JSONL, dispatch to channel state
├── channel.go           # ChannelState struct + cold-start + steady-state algorithm
├── instrument.go        # Instrument book ops (apply OrderAdd/Cancel/Execute, snapshot reassembly)
├── levels.go            # Aggregate bids/asks order maps → top-N price levels with cumulative
├── record.go            # Wire-compatible Record type (mirrors parser's, kept independent)
├── metrics.go           # Prometheus metrics + HTTP /metrics endpoint
├── clickhouse.go        # ClickHouse HTTP writer (batched, per-table goroutines)
├── snapshot_writer.go   # Coalesced level-snapshot emission scheduler
├── events_writer.go     # Per-event ClickHouse enqueue logic
├── Dockerfile
├── README.md
├── go.mod / go.sum
└── *_test.go
```

### State model

Per channel:

```go
type ChannelState struct {
    ResetCount   uint8
    SeqLast      map[string]uint64       // port → last seq seen
    Refdata      *RefdataState           // ManifestSummary + InstrumentDefinitions
    Instruments  map[uint32]*Instrument
    DeltaBuffer  *MktdataBuffer          // ordered by mktdata_seq, buffers deltas for not-yet-ready instruments
}

type Instrument struct {
    ID                       uint32
    Symbol                   string
    PriceExponent            int8
    QtyExponent              int8
    Status                   InstrumentStatus    // awaiting-snapshot | building-snapshot | ready | gap
    Bids                     map[uint64]*RestingOrder    // by order_id
    Asks                     map[uint64]*RestingOrder
    LastAppliedMktdataSeq    uint64
    LastAppliedInstrumentSeq uint32
    OpenSnapshot             *PendingSnapshot
}

type RestingOrder struct {
    OrderID    uint64
    Side       uint8
    Flags      uint8
    EnterTS    time.Time
    Price      int64    // raw
    Quantity   uint64   // raw, decremented on partial fills
}
```

The bot implements the [Subscriber Algorithm](https://github.com/malbeclabs/edge-feed-spec/blob/main/depth-of-book/spec.md#subscriber-algorithm) section of the spec verbatim:

- **Cold start**: bind all three ports (parser does this; bot reads the merged stream), wait for refdata, buffer deltas, accept snapshot, replay buffered deltas with `mktdata_seq > anchor_seq`.
- **Steady state**: dispatch each delta on `Per-Instrument Seq` continuity. Gap → mark instrument `gap`, buffer further deltas, await next snapshot.
- **Snapshot while ready**: if `anchor_seq > last_applied_mktdata_seq`, undetected gap — re-bootstrap. Otherwise ignore.
- **Instrument reset**: discard book for that instrument, expect snapshot with `anchor_seq == new_anchor_seq`.
- **Channel reset** (`reset_count` change on any port): discard everything for the channel, restart cold start.

### Coalesced snapshot writer

For each `ready` instrument the bot maintains a `dirty` flag and a `next_allowed_write` timestamp. On every applied delta, `dirty = true`. A single goroutine ticks at fine resolution (10ms) and for each `dirty` instrument whose `next_allowed_write` is in the past, computes top-N levels (default 20), enqueues `2 × N` level rows to the ClickHouse writer, resets `dirty`, and bumps `next_allowed_write` by `coalesce_interval` (default 50ms, env-configurable).

This collapses bursts (e.g., a block batch with hundreds of events affecting one instrument) into one snapshot row per affected instrument per window.

### CLI flags

| Flag | Required | Default | Description |
|---|---|---|---|
| `--socket` | yes | — | Path to parser Unix socket |
| `--symbol` | no | empty | Comma-separated symbol filter (empty = all) |
| `--depth` | no | `20` | Snapshot depth (levels per side) |
| `--coalesce-ms` | no | `50` | Snapshot coalesce window in milliseconds |
| `--metrics-addr` | no | `127.0.0.1:9092` | Prometheus endpoint |
| `--clickhouse-url` | no | empty | HTTP endpoint; empty disables persistence |
| `--clickhouse-database` | no | `depthofbook` | |
| `--clickhouse-batch-size` | no | `1000` | Rows per batch flush |
| `--clickhouse-batch-interval` | no | `200ms` | Max time between flushes |
| `--clickhouse-buffer` | no | `100000` | Per-table channel capacity |
| `-v` | no | false | Debug logging |
| `--version` | no | — | Print version and exit |

### Prometheus metrics (prefix `dz_dob_bot_`)

Process: `build_info{version,commit}`, `uptime_seconds`, `socket_connected`.

Decode and intake:
- `records_total{type}`
- `decode_errors_total`
- `socket_reconnects_total{reason}` — `dial_failed`, `eof`, `read_error`
- `socket_to_bot_latency_seconds{type}` — histogram of `now() - record.ts` on receive

Book state:
- `instruments_total{status}` — gauge keyed by `awaiting-snapshot | building-snapshot | ready | gap`
- `instrument_resets_total{reason}`
- `channel_resets_total`
- `per_instrument_gaps_total`
- `book_orders{symbol,side}` — gauge: live order count
- `book_top_price{symbol,side}` — gauge: best price
- `book_top_qty{symbol,side}` — gauge: qty at best
- `book_spread_bps{symbol}` — gauge

Snapshot writer:
- `snapshot_writes_total`
- `snapshot_coalesces_total` — count of applied deltas that were collapsed into a single write
- `snapshot_lag_ms` — histogram of dirty-window age at write time

ClickHouse persistence:
- `clickhouse_rows_written_total{table}`
- `clickhouse_rows_dropped_total{table,reason}` — `buffer_full`, `write_failed`
- `clickhouse_write_errors_total{table,reason}` — `transport`, `http_4xx`, `http_5xx`, `new_request`
- `clickhouse_batch_duration_seconds{table}`
- `clickhouse_buffered_rows{table}` — gauge

Per-symbol gauge cardinality is bounded by the symbol filter; with no filter, the bot emits a warning that high-cardinality symbols may overwhelm Prometheus.

## ClickHouse schema (`depthofbook` database)

Five tables. All `MergeTree` family, daily-partitioned, 30-day TTL.

### `instruments` — slowly-changing dimension

```sql
CREATE TABLE instruments (
    recv_ts          DateTime64(9),
    channel_id       UInt8,
    instrument_id    UInt32,
    symbol           LowCardinality(String),
    leg1             LowCardinality(String),
    leg2             LowCardinality(String),
    asset_class      LowCardinality(String),
    market_model     LowCardinality(String),
    price_exponent   Int8,
    qty_exponent     Int8,
    tick_size        Float64,
    lot_size         Float64,
    contract_value   UInt64,
    expiry_ts        DateTime64(9),
    settle_type      LowCardinality(String),
    price_bound      LowCardinality(String),
    manifest_seq     UInt16
)
ENGINE = ReplacingMergeTree(recv_ts)
ORDER BY (channel_id, instrument_id);
```

Latest definition per `(channel_id, instrument_id)` survives; older versions are merged away.

### `events` — per-event log

```sql
CREATE TABLE events (
    recv_ts                DateTime64(9),
    publisher_send_ts      DateTime64(9),
    wire_latency_ms        Float64 MATERIALIZED (recv_ts - publisher_send_ts) * 1000,
    channel_id             UInt8,
    mktdata_seq            UInt64,
    reset_count            UInt8,
    kind                   LowCardinality(String),
    instrument_id          UInt32,
    symbol                 LowCardinality(String),
    source_id              UInt16,
    per_instrument_seq     UInt32,

    order_id               Nullable(UInt64),
    side                   LowCardinality(String),
    order_flags            UInt8,
    price                  Nullable(Float64),
    qty                    Nullable(Float64),
    enter_ts               Nullable(DateTime64(9)),

    exec_flags             UInt8,
    trade_id               Nullable(UInt64),
    aggressor_side         LowCardinality(String),

    cumulative_volume      Nullable(Float64),

    cancel_reason          LowCardinality(String),

    reset_reason           LowCardinality(String),
    new_anchor_seq         Nullable(UInt64),

    batch_id               Nullable(UInt32),
    batch_ts               Nullable(DateTime64(9))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (symbol, recv_ts, kind)
TTL toDateTime(recv_ts) + INTERVAL 30 DAY;
```

`kind` values: `order_add | order_cancel | order_execute | trade | instrument_reset | batch_boundary`.

Wide table with nullable per-kind columns is intentional; ClickHouse handles wide tables efficiently and avoids `UNION ALL`-style queries on the dashboard side.

### `level_snapshots` — bot-derived top-N depth, coalesced

```sql
CREATE TABLE level_snapshots (
    recv_ts             DateTime64(9),
    publisher_send_ts   DateTime64(9),
    wire_latency_ms     Float64 MATERIALIZED (recv_ts - publisher_send_ts) * 1000,
    channel_id          UInt8,
    instrument_id       UInt32,
    symbol              LowCardinality(String),
    last_applied_seq    UInt64,
    side                LowCardinality(String),
    level_idx           UInt16,
    price               Float64,
    qty                 Float64,
    order_count         UInt32,
    cumulative_qty      Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (symbol, recv_ts, side, level_idx)
TTL toDateTime(recv_ts) + INTERVAL 30 DAY;
```

`cumulative_qty` is computed at write time so the book-ladder visualization is a direct table render.

### `wire_snapshots` — raw `SnapshotOrder` capture

```sql
CREATE TABLE wire_snapshots (
    recv_ts             DateTime64(9),
    publisher_send_ts   DateTime64(9),
    channel_id          UInt8,
    instrument_id       UInt32,
    symbol              LowCardinality(String),
    snapshot_id         UInt32,
    anchor_seq          UInt64,
    total_orders        UInt32,
    last_instrument_seq UInt32,
    order_id            UInt64,
    side                LowCardinality(String),
    order_flags         UInt8,
    enter_ts            DateTime64(9),
    price               Float64,
    qty                 Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (channel_id, instrument_id, snapshot_id, side, order_id)
TTL toDateTime(recv_ts) + INTERVAL 30 DAY;
```

Group identity (`snapshot_id`, `anchor_seq`, `total_orders`, `last_instrument_seq`) denormalized onto every row — ClickHouse-idiomatic, no joins. A complete snapshot is `WHERE channel_id=X AND instrument_id=Y AND snapshot_id=Z ORDER BY side, order_id`.

### `channel_health`

```sql
CREATE TABLE channel_health (
    recv_ts             DateTime64(9),
    publisher_send_ts   DateTime64(9),
    wire_latency_ms     Float64 MATERIALIZED (recv_ts - publisher_send_ts) * 1000,
    channel_id          UInt8,
    kind                LowCardinality(String),
    manifest_seq        Nullable(UInt16),
    manifest_valid      Nullable(UInt8),
    instrument_count    Nullable(UInt32)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (channel_id, recv_ts)
TTL toDateTime(recv_ts) + INTERVAL 30 DAY;
```

`kind` values: `heartbeat | manifest_summary | end_of_session`.

## Grafana dashboard

`demo/grafana/dashboards/depthofbook.json`. Auto-provisioned via the existing `demo/grafana/provisioning/dashboards/dashboards.yaml` (it already wildcards the directory). The ClickHouse datasource provisioned for TOB is reused; the `depthofbook` database is selected explicitly per-query.

### Template variables

- `$symbol` — single-select, `SELECT DISTINCT symbol FROM depthofbook.level_snapshots WHERE $__timeFilter(recv_ts)`
- `$symbols` — multi-select, same source

### Panels

| # | Title | Type | Position | Query target |
|---|---|---|---|---|
| 1 | Book ladder — `$symbol` | Table (cell-bar render) | top-left, 8w × 14h | `level_snapshots`, latest `recv_ts` |
| 2 | Depth heatmap — `$symbol` | Heatmap | top-right, 16w × 14h | `level_snapshots` over time |
| 3 | Spread (bps) — `$symbol` | Time Series | row 2, 8w × 7h | `level_snapshots`, level_idx=0 per side |
| 4 | Top of book — multi-symbol | Table | row 2, 16w × 7h | `level_snapshots`, latest level_idx=0 |
| 5 | Trade tape — `$symbols` | Table | row 3, 12w × 9h | `events`, `kind='trade'` |
| 6 | Add/Cancel/Execute rate — `$symbols` | Time Series (stacked area) | row 3, 12w × 9h | `events`, `kind IN (...)` per `$__timeInterval` |
| 7 | Resting order count — `$symbol` | Time Series | row 4, 12w × 7h | `level_snapshots` order_count summed per side |
| 8 | Channel health | Time Series + Stat | row 4, 12w × 5h | `channel_health` + Prometheus `dz_dob_bot_*` |
| 9 | Active instrument count | Stat | row 4, 4w × 5h | `channel_health`, latest `manifest_summary` |

Specific Grafana JSON, color rules, and threshold values are detailed in the implementation plan. Panel 1's specific cell-bar render is a Grafana-version-dependent setting; if the table panel can't produce the Hyperliquid-style ladder cleanly, the implementer falls back to a Bar Chart panel with the same query.

The book ladder + heatmap pair is the centerpiece of the dashboard.

## Demo stack changes (`demo/`)

Two new services added to `demo/docker-compose.yml`:

```yaml
depthofbook-parser:
  build: ../go/depthofbook-parser
  network_mode: host
  command:
    - --group=${DZ_DOB_MULTICAST_GROUP}
    - --refdata-port=${DZ_DOB_REFDATA_PORT}
    - --mktdata-port=${DZ_DOB_MKTDATA_PORT}
    - --snapshot-port=${DZ_DOB_SNAPSHOT_PORT}
    - --interface=${DZ_INTERFACE}
    - --output=unix:///var/run/dz/dob.sock
    - --metrics-addr=127.0.0.1:9091
  volumes:
    - dz-sockets:/var/run/dz

depthofbook-bot:
  build: ../go/depthofbook-bot
  depends_on: [depthofbook-parser, clickhouse]
  command:
    - --socket=/var/run/dz/dob.sock
    - --symbol=${DZ_DOB_SYMBOLS}
    - --depth=${DZ_DOB_DEPTH:-20}
    - --coalesce-ms=${DZ_DOB_COALESCE_MS:-50}
    - --metrics-addr=0.0.0.0:9092
    - --clickhouse-url=http://clickhouse:8123
    - --clickhouse-database=depthofbook
  volumes:
    - dz-sockets:/var/run/dz
  ports: ["${DOB_BOT_METRICS_PORT:-9092}:9092"]
```

New schema file `demo/clickhouse/init/02_schema_dob.sql` creates the five `depthofbook.*` tables alongside the existing `topofbook.*` ones (the existing `01_schema.sql` runs first; ClickHouse executes init files in lexical order).

New dashboard file `demo/grafana/dashboards/depthofbook.json`. The existing provisioning YAML picks it up automatically.

`demo/.env.example` additions:

```bash
# Depth-of-book feed
DZ_DOB_MULTICAST_GROUP=239.10.10.20
DZ_DOB_REFDATA_PORT=7011
DZ_DOB_MKTDATA_PORT=7012
DZ_DOB_SNAPSHOT_PORT=7013
DZ_DOB_SYMBOLS=
DZ_DOB_DEPTH=20
DZ_DOB_COALESCE_MS=50
DOB_BOT_METRICS_PORT=9092
```

Existing `DZ_*` keys (TOB) remain unchanged.

## Rename: `go/example-bot/` → `go/topofbook-bot/`

The existing `example-bot` is in practice TOB-specific (consumes TOB Records, has TOB-shaped metrics, writes to the `topofbook` ClickHouse database). With a sibling DOB bot landing, the generic name becomes confusing.

Steps:
- `git mv go/example-bot go/topofbook-bot`
- Update `go.mod` module path inside the dir
- Update `go/go.work`
- Update `demo/docker-compose.yml` build path and service name
- Update top-level `README.md` references
- Update `go/topofbook-bot/README.md` title

One commit, no logic changes, no metric prefix changes (existing `dz_bot_*` Prometheus names stay).

## Error handling

### Parser

- UDP socket creation: fatal if `bind` or `IP_ADD_MEMBERSHIP` fails on any of the three ports
- Per-frame: bad magic / wrong schema version / wrong frame_length / truncated frame → `parse_errors_total{port,reason}++`, drop frame, continue
- Per-message: unknown `msg_type` → skip via `msg_length` and continue (forward-compat per spec)
- Sink errors: `sink_write_errors_total++`, log, continue
- Socket clients that block: drop and increment `socket_client_drops_total{reason="slow_writer"}`
- SIGINT/SIGTERM: graceful shutdown, sink Close(), exit 0

### Bot

- Parser-socket disconnect: exponential backoff reconnect (250ms → 500ms → 1s → 2s → 5s, capped). Log every reconnect attempt at INFO.
- JSON decode error: `decode_errors_total++`, drop the line, continue
- Per-instrument gap (per-instrument seq skip): instrument moves to `gap` status, deltas buffered, await recovery snapshot. `per_instrument_gaps_total++`.
- Channel-level seq gap on mktdata: noted, but no immediate action — per-instrument seq check on next delta per instrument is what triggers individual recovery.
- Channel reset (`reset_count` change observed on any port): channel state discarded, restart cold start. `channel_resets_total++`.
- Snapshot reassembly failure (mismatched `snapshot_id`, count off, etc.): discard partial book, revert instrument to `awaiting-snapshot`. Log warning.
- ClickHouse buffer full: drop incoming row, `clickhouse_rows_dropped_total{table,reason="buffer_full"}++`. Log warning at sustained drops.
- ClickHouse HTTP error: log, `clickhouse_write_errors_total{table,reason}++`, drop the batch — book state in memory is unaffected.
- SIGINT/SIGTERM: graceful shutdown — flush ClickHouse batchers, close socket, exit 0

### Bounded behaviors

- Cold-start delta buffer is bounded at 10000 messages per channel. On overflow, oldest deltas are dropped with a warning. (The spec doesn't specify a bound; this protects against publisher misbehavior.)
- Bot's symbol filter cardinality: if `--symbol` is empty the bot emits a startup warning if the active instrument count exceeds 100, since per-symbol gauges will be high-cardinality. Operation continues regardless.

## Testing

Standard Go unit tests via `go test ./...` per binary.

### Parser

- **Wire decoder** — golden-frame tests for each of the 13 message types. Specifically verifies:
  - Multiple messages packed in one frame
  - 1232-byte MTU edge case (max frame, just under GRE-affected MTU)
  - Bad magic / wrong schema_version / truncated frame each return their specific error code
  - Forward-compat: unknown msg_type is skipped via msg_length
- **Sink** — socket sink with N concurrent fake consumers verifying:
  - Each consumer receives every record (broadcast semantics)
  - Slow consumer is dropped and the drop is metrics'd; other consumers continue receiving
  - JSONL framing handles short reads / partial writes correctly

### Bot

- **Channel state machine** — sequences of synthetic Records driving:
  - Cold start: refdata → snapshot → buffered delta replay → ready
  - Per-instrument gap detection: ready → gap on per-instrument seq skip
  - Snapshot reassembly with anchor seq replay
  - Instrument reset: on `instrument_reset`, instrument re-enters awaiting-snapshot
  - Channel reset: `reset_count` change discards everything, restart
  - Snapshot-while-ready: `anchor_seq > last_applied_mktdata_seq` triggers re-bootstrap
- **Levels** — synthetic order map → top-N levels:
  - Ties on price (orders aggregate into one level)
  - Level cap at N (deeper orders don't appear)
  - Empty side
  - cumulative_qty correctness
- **Snapshot writer** — N rapid changes within window collapse to 1 write
- **ClickHouse writer** — against an in-test mock HTTP server:
  - Batching by size threshold
  - Batching by interval timeout
  - Drop-on-buffer-full
  - Per-table independent batching
  - HTTP error handling (4xx, 5xx, transport)

### Integration

No automated integration test against a live publisher in v1 (the Hyperliquid publisher is being built in parallel). The `docker compose up` against the real feed is the manual integration test. Smoke checks:

- Grafana dashboard renders without errors
- ClickHouse rows accumulate in all five tables
- Bot Prometheus metrics increment (records, snapshots, instruments)
- Channel resets and instrument resets surface correctly in dashboards

## Configuration surface (consolidated)

### Parser CLI flags

`--group`, `--refdata-port`, `--mktdata-port`, `--snapshot-port`, `--interface`, `--output`, `--format`, `--metrics-addr`, `-v`, `--version`

### Bot CLI flags

`--socket`, `--symbol`, `--depth`, `--coalesce-ms`, `--metrics-addr`, `--clickhouse-url`, `--clickhouse-database`, `--clickhouse-batch-size`, `--clickhouse-batch-interval`, `--clickhouse-buffer`, `-v`, `--version`

### `demo/.env` (new keys)

`DZ_DOB_MULTICAST_GROUP`, `DZ_DOB_REFDATA_PORT`, `DZ_DOB_MKTDATA_PORT`, `DZ_DOB_SNAPSHOT_PORT`, `DZ_DOB_SYMBOLS`, `DZ_DOB_DEPTH`, `DZ_DOB_COALESCE_MS`, `DOB_BOT_METRICS_PORT`

## Open items deferred to implementation

- Exact Grafana panel JSON, color rules, and thresholds — drafted in the plan, refined against real publisher data
- Specific cell-bar render mode for the book ladder panel (Grafana table or Bar Chart fallback) — decided when we can see real data in the dashboard
- Hyperliquid publisher's actual wire format conformance details (port assignments, channel sharding choice, recommended cycle period) — surfaced via configuration

## Future work (out of scope)

- Pcap input mode (decoder is cleanly separable from the multicast receiver to make this straightforward)
- A dedicated reconciliation job that compares the bot's reconstructed book against an external reference snapshot
- A standalone book viewer (CLI or web) that reads from the parser socket directly without ClickHouse
- Multi-channel sharded publisher support beyond the single-channel-per-publisher case
- Cross-feed dashboards correlating TOB, DOB, and (future) midpoint feeds for the same symbol
