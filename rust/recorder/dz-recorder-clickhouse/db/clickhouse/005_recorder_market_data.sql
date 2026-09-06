-- Market data as rows: what the messages said, not how large they were.
--
-- `001` records the envelope — a sequence number, a length, an arrival. These
-- three record the content, and they sit beside those tables rather than
-- widening them: a channel instance owns a sequence number and an instrument
-- owns a price, and one key cannot be both.
--
-- ORDERED BY THE INSTRUMENT, AND BY THE CHANNEL INSTANCE TOO. `ReplacingMergeTree`
-- deduplicates on the whole sort key, so the key has to carry everything that
-- distinguishes two genuine rows. Without `source_addr` and `dst_port`, two
-- paths publishing one Channel ID collapse into one row; without `recv_ts`, a
-- duplicated datagram — same sequence number, same message index, different
-- arrival — deletes the original instead of sitting beside it. `datagram` in
-- `001` carries all three for exactly this reason.
--
-- They sit AFTER `instrument_id` rather than before it, which is the one place
-- these keys depart from `datagram`'s. Every question asked of `datagram` is per
-- channel instance; the dominant question here is per instrument over a window,
-- and a leading instance prefix makes that a full scan. The instance columns are
-- here for identity and for deduplication, not as the leading filter.
--
-- SYMBOL IS NEVER A KEY. `GLOSSARY.md` has it as display and filtering only, and
-- the reasons are three: a symbol is unique within a channel at an instant and
-- not across eras, so an instrument retired and another published later under
-- the same name are merged by a symbol join; it is `char[64]` of venue-chosen
-- text that padding, case or an upstream rename all move without any market data
-- moving; and two channels may legitimately carry one symbol, where a join
-- across them is a cross join. Every table here keys on
-- `(channel_id, instrument_id)` within an era and carries `symbol` for the WHERE
-- clause a human types.

CREATE TABLE IF NOT EXISTS recorder.event (
    recv_ts            DateTime64(9),
    send_ts            DateTime64(9),
    -- The venue's own event time, where the message carries one. Never part of
    -- an equivalence key: its resolution and its meaning differ between
    -- transports, so one book state carried over two of them hashes two ways and
    -- no pair is ever found.
    upstream_ts        Nullable(DateTime64(9)),
    send_recv_ms       Float64 MATERIALIZED
                         (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(send_ts)) / 1e6,
    recv_ts_kind       LowCardinality(String),

    site               LowCardinality(String),
    recorder           LowCardinality(String),
    env                LowCardinality(String),
    feed               LowCardinality(String),
    port_role          LowCardinality(String),
    source_addr        IPv4,
    channel_id         UInt8,
    dst_port           UInt16,

    sequence_number    UInt64,
    -- The wire value, as sent. A fact and never a key: it is a UInt8 and it
    -- wraps, so two eras 256 resets apart share a value.
    reset_count        UInt8,
    -- Monotonic per recorder run, and what places this row in the archive.
    --
    -- THE ERA IS NOT A COLUMN HERE, for the reason `datagram` has none: an era's
    -- anchor is only observable as the first datagram of that era *in this
    -- object*, so a stored anchor differs between two objects of one era and
    -- splits that era across sort-key prefixes. The era is resolved by range
    -- join to `era`, where the openings and their certainty already are.
    segment_seq        UInt64,
    -- In the sort key because a publisher may pack several messages for one
    -- instrument into one datagram: they share a sequence number and an arrival,
    -- and without this a run of genuine events collapses to whichever merged last.
    message_index      UInt8,

    -- From the message where it carries one, and from era-qualified reference
    -- data where it does not. `InstrumentReset` and the three snapshot messages
    -- carry no Source ID at all.
    source_id          UInt16,
    instrument_id      UInt32,
    symbol             LowCardinality(String),
    price_exp          Int8,
    qty_exp            Int8,
    per_instrument_seq Nullable(UInt32),

    message_type       LowCardinality(String),
    side_raw           Nullable(UInt8),
    action_raw         Nullable(UInt8),
    reason_raw         Nullable(UInt8),
    flags_raw          Nullable(UInt8),
    price_raw          Nullable(Int64),
    qty_raw            Nullable(UInt64),
    -- NULL where the wire said 0xFFFF. The depth specification is explicit that
    -- the sentinel is not a count and not a rank; written through as 65535 it
    -- becomes an instrument with sixty-five thousand orders at a level, and it
    -- survives every average taken over it. Top of book says *unavailable* with
    -- zero instead, which is why this translation is not a blanket rule.
    order_count        Nullable(UInt16),
    level_index        Nullable(UInt16),

    bid_px_raw         Nullable(Int64),
    bid_qty_raw        Nullable(UInt64),
    bid_source_count   Nullable(UInt16),
    ask_px_raw         Nullable(Int64),
    ask_qty_raw        Nullable(UInt64),
    ask_source_count   Nullable(UInt16),

    trade_id           Nullable(UInt64),
    cumulative_volume  Nullable(UInt64),

    snapshot_id        Nullable(UInt32),
    -- On a snapshot, the sequence number the book is true as of. On an
    -- `InstrumentReset`, `new_anchor_seq` — the terms of its own recovery, and
    -- the reason dropping it is unsafe rather than lossy: without it a snapshot
    -- already in flight when the reset was published is accepted, and a book the
    -- publisher had disowned is rebuilt from as certain.
    anchor_seq         Nullable(UInt64),
    total_levels       Nullable(UInt32),
    -- How many levels the cycle actually carried, on its `SnapshotEnd`. Against
    -- `total_levels` on the begin row this answers *was the snapshot complete*
    -- from rows alone, which is what makes persisting every level optional
    -- rather than the only way to ask.
    levels_seen        Nullable(UInt32),
    depth_bound        Nullable(UInt32),

    object_key         String,
    object_sha256      String,
    datagram_index     UInt64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (channel_id, instrument_id, sequence_number, message_index,
          source_addr, dst_port, site, recv_ts);

-- The era-scoped reference data, kept.
--
-- IT CARRIES THE CHANNEL INSTANCE, and that is not decoration. An `era_anchor_ts`
-- is only meaningful for one instance, because a Reset Count is that instance's:
-- two paths publishing one Channel ID open their eras independently, so a key
-- without the address and the port merges two eras that are not the same era and
-- lets one path's exponents decode the other path's prices. `port_role` is here
-- because reference data arrives on the refdata role, and a reader joining from
-- a mktdata event should see that the roles differ rather than discover it.
CREATE TABLE IF NOT EXISTS recorder.instrument (
    site           LowCardinality(String),
    recorder       LowCardinality(String),
    env            LowCardinality(String),
    feed           LowCardinality(String),
    port_role      LowCardinality(String),
    source_addr    IPv4,
    channel_id     UInt8,
    dst_port       UInt16,
    source_id      UInt16,
    instrument_id  UInt32,
    -- The sequence number this statement came into force at: a stable era-scoped
    -- identity where an anchor timestamp is not. It is the position of the
    -- definition that made the statement, identical in every object that carries
    -- it, so two loads of one era replace each other instead of accumulating.
    from_sequence  UInt64,
    reset_count    UInt8,
    symbol         String,
    price_exp      Int8,
    qty_exp        Int8,
    contract_value UInt64,
    first_seen_ts  DateTime64(9),
    last_seen_ts   DateTime64(9),
    manifest_seq   Nullable(UInt16),
    -- What a valid ManifestSummary said the published set held. Absent rather
    -- than zero while a summary is not valid yet: a zero reads as a feed
    -- publishing nothing. Against the count of distinct instruments observed, it
    -- is the only statement of published-set coverage an archive can make.
    declared_count Nullable(UInt32),
    object_key     String
)
ENGINE = ReplacingMergeTree(last_seen_ts)
PARTITION BY toYYYYMMDD(first_seen_ts)
ORDER BY (channel_id, instrument_id, from_sequence, source_addr, dst_port, site, recorder);

-- One row per change in an instrument's top of book, where a change is a change
-- in EITHER the visible top OR the certainty of it.
--
-- Emitting only on price movement loses the transition that matters most: a gap
-- or an InstrumentReset arrives, nothing later happens to move the top, and every
-- lookup from then on keeps returning a row that says the book is certain — which
-- is now false. A certainty transition therefore emits its own row, carrying the
-- same top as the row before it and a different verdict on whether it can be
-- believed.
CREATE TABLE IF NOT EXISTS recorder.book_top (
    recv_ts           DateTime64(9),
    send_ts           DateTime64(9),
    site              LowCardinality(String),
    recorder          LowCardinality(String),
    env               LowCardinality(String),
    feed              LowCardinality(String),
    -- Where this view of the book came from, as `site` names a recorder. Two
    -- recorders of one multicast feed are two observations; a multicast feed and
    -- some other transport carrying the same instruments are two observations.
    -- Nothing here knows which is which, and nothing should.
    observation       LowCardinality(String),
    source_addr       IPv4,
    channel_id        UInt8,
    dst_port          UInt16,
    source_id         UInt16,
    instrument_id     UInt32,
    symbol            LowCardinality(String),
    sequence_number   UInt64,
    message_index     UInt8,
    reset_count       UInt8,
    segment_seq       UInt64,
    bid_px_raw        Nullable(Int64),
    bid_qty_raw       Nullable(UInt64),
    bid_source_count  Nullable(UInt16),
    ask_px_raw        Nullable(Int64),
    ask_qty_raw       Nullable(UInt64),
    ask_source_count  Nullable(UInt16),
    price_exp         Int8,
    qty_exp           Int8,
    -- The equivalence key: a hash over the instrument and both sides, and over
    -- nothing else. No timestamp, because a timestamp is the quantity being
    -- measured. No sequence number or Reset Count, because two observation points
    -- on two transports do not share them. No bytes, because a hash over the
    -- payload is a function of the schema version and the batching, so a
    -- publisher upgrade repartitions the key space and the race reports nothing.
    state_key         UInt64,
    book_certain      UInt8,
    uncertain_since   Nullable(UInt64),
    uncertain_reason  LowCardinality(String),   -- none | gap | instrument_reset | no_anchor
    object_key        String
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (channel_id, instrument_id, recv_ts, sequence_number,
          message_index, observation);

-- THE RETENTION SPLIT, ONE TABLE FURTHER DOWN THAN `002` PUT IT.
--
-- `event` is to `book_top` what `datagram` is to `sequence_gap`: the expensive
-- base that every question is asked against, and the one whose row count is not
-- a function of the datagram count but of how many messages a publisher packed
-- into each one. A burst batched into one datagram is one transport row and
-- hundreds of these.
--
-- A whole number of days, for the reason `002` gives: a TTL that does not align
-- to the partition is a treadmill of part rewrites rather than a partition drop.
ALTER TABLE recorder.event
    MODIFY TTL toDateTime(recv_ts) + INTERVAL 2 DAY;

-- `book_top` is per change rather than per message and is what a dashboard
-- actually asks, so it is worth far more history than its base. Kept long, and
-- stated here rather than left to be inferred from an absent line.
ALTER TABLE recorder.book_top
    MODIFY TTL toDateTime(recv_ts) + INTERVAL 30 DAY;

-- `recorder.instrument` has no TTL, deliberately. It is tens of bytes per
-- instrument per era, and it is what makes every other row's `symbol` and
-- exponents mean anything after the fact — expiring it would leave prices that
-- no longer decode.
