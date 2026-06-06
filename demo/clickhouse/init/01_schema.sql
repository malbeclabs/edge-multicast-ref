-- ClickHouse schema for the DZ top-of-book demo.
-- Loaded automatically on first container boot via
-- /docker-entrypoint-initdb.d. Subsequent boots skip this file.
--
-- Design notes:
--   * One table per record type keeps queries simple and indexing efficient.
--   * `send_latency_ms` (recv - publisher_send) and `source_latency_ms`
--     (recv - source_ts, NULL when source_ts is absent) are materialized so
--     queries can filter/aggregate on latency without recomputing timestamps.
--     Both include clock skew between publisher and subscriber hosts.
--   * ORDER BY (symbol, recv_ts) makes per-symbol time-range scans fast —
--     exactly the shape every dashboard panel uses.
--   * Daily partitioning keeps inserts cheap and lets operators drop old
--     data with a single ALTER TABLE ... DROP PARTITION.
--   * LowCardinality(String) saves memory and speeds up filters on
--     low-distinct-value columns (symbols, sides).

CREATE DATABASE IF NOT EXISTS topofbook;

CREATE TABLE IF NOT EXISTS topofbook.quotes (
    recv_ts             DateTime64(9),
    publisher_send_ts   DateTime64(9),
    source_ts           Nullable(DateTime64(9)),
    recv_ts_kind        LowCardinality(String) DEFAULT '',
    send_latency_ms     Float64 MATERIALIZED
        (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(publisher_send_ts)) / 1e6,
    source_latency_ms   Nullable(Float64) MATERIALIZED
        if(source_ts IS NULL, NULL,
           (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(assumeNotNull(source_ts))) / 1e6),
    channel_id          UInt8,
    seq                 UInt64,
    instrument_id       UInt32,
    symbol              LowCardinality(String),
    bid_price           Float64,
    bid_qty             Float64,
    ask_price           Float64,
    ask_qty             Float64,
    mid                 Float64 MATERIALIZED (bid_price + ask_price) / 2,
    spread              Float64 MATERIALIZED ask_price - bid_price,
    spread_bps          Float64 MATERIALIZED
        if((bid_price + ask_price) > 0,
           (ask_price - bid_price) / ((bid_price + ask_price) / 2) * 10000,
           0),
    source_id           UInt32
) ENGINE = MergeTree
  PARTITION BY toYYYYMMDD(recv_ts)
  ORDER BY (symbol, recv_ts)
  TTL toDateTime(recv_ts) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS topofbook.trades (
    recv_ts             DateTime64(9),
    publisher_send_ts   DateTime64(9),
    source_ts           Nullable(DateTime64(9)),
    recv_ts_kind        LowCardinality(String) DEFAULT '',
    send_latency_ms     Float64 MATERIALIZED
        (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(publisher_send_ts)) / 1e6,
    source_latency_ms   Nullable(Float64) MATERIALIZED
        if(source_ts IS NULL, NULL,
           (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(assumeNotNull(source_ts))) / 1e6),
    channel_id          UInt8,
    seq                 UInt64,
    instrument_id       UInt32,
    symbol              LowCardinality(String),
    price               Float64,
    qty                 Float64,
    cumulative_volume   Float64,
    aggressor_side      LowCardinality(String),   -- buy / sell / unknown
    trade_id            UInt64,
    source_id           UInt32
) ENGINE = MergeTree
  PARTITION BY toYYYYMMDD(recv_ts)
  ORDER BY (symbol, recv_ts)
  TTL toDateTime(recv_ts) + INTERVAL 30 DAY;

-- Refdata: most recent instrument definition per instrument_id.
-- Emitted periodically by the publisher; ReplacingMergeTree keeps the
-- latest row per (instrument_id) during merges.
CREATE TABLE IF NOT EXISTS topofbook.instruments (
    recv_ts             DateTime64(9),
    instrument_id       UInt32,
    symbol              LowCardinality(String),
    price_exponent      Int8,
    qty_exponent        Int8
) ENGINE = ReplacingMergeTree(recv_ts)
  ORDER BY (instrument_id);
