CREATE DATABASE IF NOT EXISTS marketbyprice;

-- Slowly-changing instrument dimension. ReplacingMergeTree keeps the latest row
-- per (channel_id, instrument_id). No TTL: refdata must outlive the event window.
CREATE TABLE IF NOT EXISTS marketbyprice.instruments (
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

-- Per-message log: level deltas, clears, trades, liquidations, structural events.
CREATE TABLE IF NOT EXISTS marketbyprice.events (
    recv_ts                DateTime64(9),
    publisher_send_ts      DateTime64(9),
    source_ts              Nullable(DateTime64(9)),
    recv_ts_kind           LowCardinality(String) DEFAULT '',
    send_latency_ms        Float64 MATERIALIZED (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(publisher_send_ts)) / 1e6,
    source_latency_ms      Nullable(Float64) MATERIALIZED if(source_ts IS NULL, NULL, (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(assumeNotNull(source_ts))) / 1e6),
    channel_id             UInt8,
    mktdata_seq            UInt64,
    reset_count            UInt8,
    kind                   LowCardinality(String),
    instrument_id          UInt32,
    symbol                 LowCardinality(String),
    source_id              UInt16 DEFAULT 0,
    per_instrument_seq     UInt32 DEFAULT 0,

    -- level_update. order_count and level_index are Nullable because the wire
    -- sentinel 0xFFFF means "not supplied" and the parser omits the key; 0 is a
    -- real count and a real rank.
    side                   LowCardinality(String) DEFAULT '',
    price                  Nullable(Float64),
    qty                    Nullable(Float64),
    order_count            Nullable(UInt32),
    level_index            Nullable(UInt16),
    action                 LowCardinality(String) DEFAULT '',
    update_reason          LowCardinality(String) DEFAULT '',
    level_flags            UInt8 DEFAULT 0,

    -- book_clear
    clear_side             LowCardinality(String) DEFAULT '',
    clear_scope            LowCardinality(String) DEFAULT '',
    from_price             Nullable(Float64),
    clear_reason           LowCardinality(String) DEFAULT '',

    -- trade
    trade_id               Nullable(UInt64),
    aggressor_side         LowCardinality(String) DEFAULT '',
    cumulative_volume      Nullable(Float64),
    trade_flags            UInt8 DEFAULT 0,

    -- liquidation
    liquidation_flags      UInt8 DEFAULT 0,
    method                 LowCardinality(String) DEFAULT '',
    mark_price             Nullable(Float64),
    liquidated_user        String DEFAULT '',

    -- batch_boundary
    batch_id               Nullable(UInt32),
    batch_ts               Nullable(DateTime64(9)),

    -- instrument_reset
    reset_reason           LowCardinality(String) DEFAULT '',
    new_anchor_seq         Nullable(UInt64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (symbol, recv_ts, kind)
TTL toDateTime(recv_ts) + INTERVAL 30 DAY;

-- Coalesced top-N depth, one row per level for direct table and heatmap rendering.
CREATE TABLE IF NOT EXISTS marketbyprice.level_snapshots (
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
    order_count         Nullable(UInt32),
    cumulative_qty      Float64,
    stale               UInt8 DEFAULT 0,
    -- crossed: the book was crossed at the last consistency point. Observability
    -- only; a crossed book is still served.
    crossed             UInt8 DEFAULT 0,
    -- depth_bound: NULL unknown, 0 the publisher claims a complete book, N
    -- bounded at N levels per side. cumulative_qty is exhaustive ONLY when 0.
    depth_bound         Nullable(UInt32)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (symbol, recv_ts, side, level_idx)
TTL toDateTime(recv_ts) + INTERVAL 30 DAY;

-- Raw SnapshotLevel capture for full replay. Group identity is denormalized onto
-- every row from the instrument's last SnapshotBegin, accepted or declined.
CREATE TABLE IF NOT EXISTS marketbyprice.wire_levels (
    recv_ts             DateTime64(9),
    publisher_send_ts   DateTime64(9),
    channel_id          UInt8,
    instrument_id       UInt32,
    symbol              LowCardinality(String),
    snapshot_id         UInt32,
    anchor_seq          UInt64,
    total_levels        UInt32,
    last_instrument_seq UInt32,
    depth_bound         Nullable(UInt32),
    side                LowCardinality(String),
    price               Float64,
    qty                 Float64,
    order_count         Nullable(UInt32),
    level_flags         UInt8 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (channel_id, instrument_id, snapshot_id, side, price)
TTL toDateTime(recv_ts) + INTERVAL 30 DAY;

-- Channel health: heartbeats, manifest summaries, end-of-session signals.
CREATE TABLE IF NOT EXISTS marketbyprice.channel_health (
    recv_ts             DateTime64(9),
    publisher_send_ts   DateTime64(9),
    source_ts           Nullable(DateTime64(9)),
    recv_ts_kind        LowCardinality(String) DEFAULT '',
    send_latency_ms     Float64 MATERIALIZED (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(publisher_send_ts)) / 1e6,
    source_latency_ms   Nullable(Float64) MATERIALIZED if(source_ts IS NULL, NULL, (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(assumeNotNull(source_ts))) / 1e6),
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
