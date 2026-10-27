-- The two pairing views number and pair within one feed.
--
-- `006`'s `book_top_occurrence` numbers a state's occurrences within
-- `(observation, channel_id, instrument_id, era, state_key)`, and its
-- `book_top_race` pairs them on `(channel_id, instrument_id, state_key,
-- occurrence)`. Neither names the feed, so where two feeds share a `Channel ID`
-- at one observation their tops for one instrument are numbered as one
-- sequence, and one feed's state is paired with the other's. Mainnet already
-- has that shape on Channel ID `1`, where `tob_edge_binance_spot` and
-- `tob_edge_phoenix` are both derived, and a publisher's two feeds on one shard
-- may now share an ID as well. `state_key` makes the crossing real rather than
-- theoretical: an empty top hashes alike on Top-of-Book and Market-by-Price, so
-- the two feeds' empty books meet in one key.
--
-- `feed` goes into the window's partition, after `observation`, and first into
-- the race's grouping. Where an ID carries one feed at an observation, which is
-- every row today except Channel ID `1`'s, the views return what they returned.
--
--
-- THE VIEWS CHANGE AND THE TABLE DOES NOT
--
-- `book_top`'s sort key, `(channel_id, instrument_id, recv_ts, sequence_number,
-- message_index, observation)`, has no feed. Adding one is a rebuild:
-- `MODIFY ORDER BY` only appends a column the same `ALTER` adds, and `feed` is
-- already a column. So the key stays, and two feeds' rows that agree on every
-- column of it are one row to the replacing merge. That needs two feeds at one
-- observation to deliver one instrument's top at one nanosecond with one
-- `Sequence Number` and one message index, which two channel instances with
-- their own sequence series do not do in practice, and it is stated here
-- rather than assumed away.
--
-- `state_key` is not re-keyed either. It is a stored value, computed by the
-- deriver (`book.rs`) and compared across observers, and it may not move: a
-- second definition would make every row written before it unpairable with
-- every row written after.
--
-- **The rule for a reader:** a query over `book_top` or `book_top_settled`
-- about one instrument filters on `feed`. A `Channel ID` and an `Instrument ID`
-- name an instrument within one feed, not across two.
--
--
-- APPLYING IT
--
-- `CREATE OR REPLACE VIEW` only, so applying it twice, or during a rolling
-- upgrade, leaves the same two views. A view holds no rows, so nothing is
-- rewritten or backfilled. `book_top_race` gains `feed` as its first column: a
-- reader that selects by name is unaffected, and one that selects `*` sees it
-- first. `012` grants the dashboards' reader neither view, so nothing is
-- re-granted. Rolling back is re-creating both as `006` states them.
--
-- `book_top_settled` stays as `010` states it, and it already selects `feed`.


-- The occurrence ordinal, per observation point and feed. Everything else is
-- `006`'s, and `006` says why.
CREATE OR REPLACE VIEW recorder.book_top_occurrence AS
SELECT
    b.observation,
    b.site,
    b.recorder,
    b.env,
    b.feed,
    b.source_addr,
    b.channel_id,
    b.dst_port,
    b.source_id,
    b.instrument_id,
    b.symbol,
    b.state_key,
    b.recv_ts,
    b.send_ts,
    b.sequence_number,
    b.price_exp,
    b.qty_exp,
    b.book_certain,
    b.uncertain_reason,
    e.anchor_ts      AS era_anchor_ts,
    e.anchor_seq     AS era_anchor_seq,
    e.anchor_certain AS anchor_certain,
    row_number() OVER (
        PARTITION BY b.observation, b.feed, b.channel_id, b.instrument_id,
                     e.anchor_ts, b.state_key
        ORDER BY b.recv_ts
    ) AS occurrence
FROM recorder.book_top_settled AS b
ASOF LEFT JOIN recorder.era_opening AS e
    ON  b.site        = e.site
    AND b.recorder    = e.recorder
    AND b.source_addr = e.source_addr
    AND b.channel_id  = e.channel_id
    AND b.dst_port    = e.dst_port
    AND e.anchor_ts  <= b.recv_ts
WHERE b.from_anchor = 0;


-- The pairing, per feed. Everything else is `006`'s, and `006` says why.
CREATE OR REPLACE VIEW recorder.book_top_race AS
SELECT
    feed,
    channel_id,
    instrument_id,
    state_key,
    occurrence,
    any(symbol)                            AS symbol,
    uniqExact(observation)                 AS observations,
    arraySort(groupUniqArray(observation)) AS observed_by,
    argMin(observation, recv_ts)           AS first_observation,
    argMax(observation, recv_ts)           AS last_observation,
    min(recv_ts)                           AS first_recv_ts,
    max(recv_ts)                           AS last_recv_ts,
    if(uniqExact(observation) > 1,
       (toUnixTimestamp64Nano(max(recv_ts)) - toUnixTimestamp64Nano(min(recv_ts))) / 1e6,
       NULL)                               AS lead_ms,
    min(book_certain)                      AS book_certain,
    (uniqExact(price_exp) = 1) AND (uniqExact(qty_exp) = 1) AS exponents_agree
FROM recorder.book_top_occurrence
GROUP BY feed, channel_id, instrument_id, state_key, occurrence;
