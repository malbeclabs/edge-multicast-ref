-- The era, as a thing a query can resolve: the settled row, the range join, and
-- the rank.
--
-- Three views, in the order of what they cost, and the order matters because the
-- cheap one is the one a panel should be using.
--
--
-- WHY `era_index` IS NOT A COLUMN, AND WHAT IT COSTS TO BE A RANK
--
-- A stored rank is renumbered by any later-arriving *earlier* object — which is
-- exactly what a backfill or a recovered segment is — so `era_index` lives in a
-- view. The objection to a window function over `datagram` rows does not apply
-- to `era`: a query's window may not contain a transition, so a rank over
-- datagram rows depends on how much of the stream the query happened to select,
-- while `era` holds every opening by construction.
--
-- What it does cost is stated plainly here rather than found later. **A dense
-- rank is defined over all history, so a predicate on time cannot be pushed
-- through the window function.** Asking for the last hour still ranks
-- everything. `recorder.era` is kept indefinitely and carries one row per
-- channel instance per segment — 1,440 segments a day at the default rotation,
-- so tens of thousands of rows a day and millions within a year or two; see 001
-- for the arithmetic. That combination is the anatomy of a query whose cost
-- grows with the age of the deployment and which no caller can prune.
--
-- So the rank is **not** on the path anything queries by default:
--
--   * `era_settled`     the ReplacingMergeTree collapse, as an aggregate rather
--                       than `FINAL`. Prunable by `anchor_ts`.
--   * `datagram_in_era` a datagram resolved to its era **by the anchor**, with
--                       no rank and no window. This is what a panel joins.
--   * `era_ranked`      the rank. An all-history query by construction, for a
--                       reader that wants human-readable era numbers over a
--                       bounded set of instances.
--
-- **An era's stable identity is its anchor, not its index.** The anchor is a
-- receive stamp on a row that already exists; the index is a position in a
-- sequence that renumbers when an earlier era arrives late. `sequence_gap`
-- carries `era_anchor_ts` for that reason, and a query that groups by the anchor
-- is both cheaper and more stable than one that groups by the rank.


-- 1. The settled row, without `FINAL`.
--
-- `FINAL` forces merge-on-read on every query and is not prunable; this is the
-- same collapse written as an aggregate over the sort key, which is exactly what
-- `ReplacingMergeTree(anchor_certain)` does on merge — `max` on the version and
-- `argMax` on everything else, so a settled row wins over an unsettled one
-- whichever order the two loads happened in.
--
-- The GROUP BY is the table's sort key and nothing else. That is what makes this
-- equivalent to the engine's own collapse rather than an aggregate that happens
-- to look like it, and it is why `feed` is an `argMax` and not a grouping key.
CREATE OR REPLACE VIEW recorder.era_settled AS
SELECT
    site,
    recorder,
    source_addr,
    channel_id,
    dst_port,
    anchor_ts,
    argMax(feed, anchor_certain)          AS feed,
    argMax(anchor_seq, anchor_certain)    AS anchor_seq,
    argMax(reset_count, anchor_certain)   AS reset_count,
    argMax(segment_seq, anchor_certain)   AS segment_seq,
    max(anchor_certain)                   AS anchor_certain,
    argMax(continuation, anchor_certain)  AS continuation,
    argMax(object_key, anchor_certain)    AS object_key,
    argMax(object_sha256, anchor_certain) AS object_sha256
FROM recorder.era
GROUP BY site, recorder, source_addr, channel_id, dst_port, anchor_ts;


-- 2. A datagram resolved to its era, by range join on the anchor.
--
-- The join the base table pays nothing for, and the one a panel should use. No
-- window function and no `FINAL`, so a predicate on `recv_ts` prunes the
-- datagram side and a predicate on `anchor_ts` prunes the era side — both are
-- partitioned by day.
--
-- It carries the era's identity and not its index: `era_anchor_ts` is what
-- `sequence_gap` already carries, `reset_count` is the wire fact, and
-- `anchor_certain` is what says whether a finding drawn from this era may be
-- escalated past `unverifiable`. A reader that wants the human-readable number
-- joins `era_ranked` on the anchor, having narrowed the instances first.
--
-- ASOF LEFT JOIN is the shape: `anchor_ts <= recv_ts` takes the latest opening
-- not after the arrival, which is what "the era this datagram is in" means.
-- LEFT, because a datagram whose era row has not been loaded yet is still a
-- datagram, and a join that dropped it would understate the traffic.
--
-- `continuation = 0` is the filter: a boundary the evidence settled as a
-- continuation of the preceding segment's era opens no era, so it is recorded
-- and never resolved to.
CREATE OR REPLACE VIEW recorder.datagram_in_era AS
SELECT
    d.*,
    e.anchor_ts      AS era_anchor_ts,
    e.anchor_seq     AS era_anchor_seq,
    e.reset_count    AS era_reset_count,
    e.anchor_certain AS anchor_certain
FROM recorder.datagram AS d
ASOF LEFT JOIN (
    SELECT * FROM recorder.era_settled WHERE continuation = 0
) AS e
    ON  d.site        = e.site
    AND d.recorder    = e.recorder
    AND d.source_addr = e.source_addr
    AND d.channel_id  = e.channel_id
    AND d.dst_port    = e.dst_port
    AND e.anchor_ts  <= d.recv_ts;


-- 3. The rank.
--
-- **This is an all-history query by construction.** A dense rank is a position
-- in a sequence that starts at the instance's first era ever, so a predicate on
-- `anchor_ts` outside this view cannot be pushed through the window function and
-- a query for one hour ranks every era ever recorded for the instances it
-- selects. Its cost therefore grows with the age of the deployment.
--
-- What *does* prune is a predicate on the partitioning columns of the window —
-- `site`, `recorder`, `source_addr`, `channel_id`, `dst_port` — because a filter
-- on the PARTITION BY columns is valid before the window rather than after it.
-- So narrowing to the instances in question is the difference between a query
-- that scales with one instance's history and one that scales with every
-- instance's.
--
-- Use it for a report about one channel instance's eras. Do not put it behind a
-- time-ranged panel: that panel wants `datagram_in_era` or `sequence_gap`, both
-- of which key on the anchor and both of which prune.
--
-- Partitioned by site and recorder as well as by the instance, because an anchor
-- is a *receive* stamp and therefore one site's observation of that era. Two
-- vantages of one instance are two observations, and merging them hides a
-- recorder that is missing the feed.
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
FROM recorder.era_settled
WHERE continuation = 0;
