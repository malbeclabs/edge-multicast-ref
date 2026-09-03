-- The era index, as a rank rather than a stored number.
--
-- `era_index` is dense_rank() over the era openings, and it lives here rather
-- than in a column because a stored rank is renumbered by any later-arriving
-- *earlier* object — which is exactly what a backfill or a recovered segment is.
-- Recomputable, and never a number anybody has to trust because it was written
-- down once.
--
-- The objection to a window function over `datagram` rows does not apply here: a
-- query's window may not contain a transition, so a rank over datagram rows
-- depends on how much of the stream the query happened to select. `era` holds
-- EVERY opening by construction, so the rank over it is complete whatever the
-- query selects.
--
-- Partitioned by site and recorder as well as by the instance, because an
-- anchor is a *receive* stamp and therefore one site's observation of that era.
-- Two vantages of one instance are two observations, and merging them hides a
-- recorder that is missing the feed.
--
-- `continuation = 0` is the filter: a boundary the evidence settled as a
-- continuation of the preceding segment's era opens no era, so it is recorded
-- and not ranked.
--
-- FINAL because `era` is a ReplacingMergeTree whose version is `anchor_certain`:
-- without it a boundary that has since been settled would still be read at its
-- unsettled value until a merge happened to run.
CREATE OR REPLACE VIEW recorder.era_ranked AS
SELECT
    site,
    recorder,
    feed,
    source_addr,
    channel_id,
    dst_port,
    anchor_ts,
    anchor_seq,
    reset_count,
    segment_seq,
    anchor_certain,
    object_key,
    dense_rank() OVER (
        PARTITION BY site, recorder, source_addr, channel_id, dst_port
        ORDER BY anchor_ts
    ) AS era_index
FROM recorder.era FINAL
WHERE continuation = 0;

-- A datagram row resolved to its era, by range join on the latest anchor at or
-- before the receive stamp.
--
-- This is the join the base table pays nothing for. It is a view rather than a
-- materialised column for the reason above, and it is written once here rather
-- than by hand in every panel, because getting `<=` and `ORDER BY anchor_ts
-- DESC LIMIT 1` slightly wrong produces a plausible number.
--
-- ASOF LEFT JOIN is the shape: `era_anchor_ts <= recv_ts` takes the latest
-- opening not after the arrival, which is what "the era this datagram is in"
-- means. LEFT, because a datagram whose era row has not been loaded yet is still
-- a datagram, and a join that dropped it would understate the traffic.
CREATE OR REPLACE VIEW recorder.datagram_in_era AS
SELECT
    d.*,
    e.era_index      AS era_index,
    e.anchor_ts      AS era_anchor_ts,
    e.anchor_certain AS anchor_certain
FROM recorder.datagram AS d
ASOF LEFT JOIN recorder.era_ranked AS e
    ON  d.site        = e.site
    AND d.recorder    = e.recorder
    AND d.source_addr = e.source_addr
    AND d.channel_id  = e.channel_id
    AND d.dst_port    = e.dst_port
    AND e.anchor_ts  <= d.recv_ts;
