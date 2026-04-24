CREATE DATABASE IF NOT EXISTS depthofbook;

-- Slowly-changing instrument dimension. ReplacingMergeTree keeps latest per (channel_id, instrument_id).
CREATE TABLE IF NOT EXISTS depthofbook.instruments (
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

-- Per-event log: order deltas + trades + structural events.
CREATE TABLE IF NOT EXISTS depthofbook.events (
    recv_ts                DateTime64(9),
    publisher_send_ts      DateTime64(9),
    wire_latency_ms        Float64 MATERIALIZED (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(publisher_send_ts)) / 1000000.0,
    channel_id             UInt8,
    mktdata_seq            UInt64,
    reset_count            UInt8,
    kind                   LowCardinality(String),
    instrument_id          UInt32,
    symbol                 LowCardinality(String),
    source_id              UInt16 DEFAULT 0,
    per_instrument_seq     UInt32 DEFAULT 0,

    order_id               Nullable(UInt64),
    side                   LowCardinality(String) DEFAULT '',
    order_flags            UInt8 DEFAULT 0,
    price                  Nullable(Float64),
    qty                    Nullable(Float64),
    enter_ts               Nullable(DateTime64(9)),

    exec_flags             UInt8 DEFAULT 0,
    trade_id               Nullable(UInt64),
    aggressor_side         LowCardinality(String) DEFAULT '',

    cumulative_volume      Nullable(Float64),

    cancel_reason          LowCardinality(String) DEFAULT '',

    reset_reason           LowCardinality(String) DEFAULT '',
    new_anchor_seq         Nullable(UInt64),

    batch_id               Nullable(UInt32),
    batch_ts               Nullable(DateTime64(9))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (symbol, recv_ts, kind)
TTL toDateTime(recv_ts) + INTERVAL 30 DAY;

-- Top-N depth, coalesced. Flat one-row-per-level layout for direct table/heatmap rendering.
CREATE TABLE IF NOT EXISTS depthofbook.level_snapshots (
    recv_ts             DateTime64(9),
    publisher_send_ts   DateTime64(9),
    wire_latency_ms     Float64 MATERIALIZED (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(publisher_send_ts)) / 1000000.0,
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

-- Raw SnapshotOrder capture, for full replay. Group identity denormalized onto every row.
CREATE TABLE IF NOT EXISTS depthofbook.wire_snapshots (
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

-- Channel health: heartbeats, manifest summaries, end-of-session signals.
CREATE TABLE IF NOT EXISTS depthofbook.channel_health (
    recv_ts             DateTime64(9),
    publisher_send_ts   DateTime64(9),
    wire_latency_ms     Float64 MATERIALIZED (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(publisher_send_ts)) / 1000000.0,
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
