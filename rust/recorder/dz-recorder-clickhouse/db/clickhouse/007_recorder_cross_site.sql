-- Cross-site: the same datagram, seen from somewhere else, as a query.
--
-- `(channel instance, sequence number)` identifies a datagram independently of
-- who received it, so two sites' recordings of one feed join on that key in
-- rows they have already written. No new table, for the reason `006` needed
-- none: both sides' rows land in the same tables and the comparison is a
-- query.
--
-- This is the pass that lets a gap row say `publisher`. Nothing else can:
-- `publisher` requires a datagram absent from *every* site with no recorder
-- overflow anywhere, and one vantage has neither half of that. A loader over
-- one object therefore writes `unverifiable` with `seen_elsewhere` null, and
-- this file is where that null becomes a `0` or a `1`.
--
--
-- WHY A VIEW, AND NOT A STEP IN THE LOADER OR A SECOND DERIVATION THAT WRITES
--
-- The choice is decided by what happens when a site is late, absent or loaded
-- twice, and only one of the three shapes survives all three.
--
--   * **In the loader.** A verdict decided while an object is being loaded is
--     decided against whatever else had arrived by then, and the other site's
--     object may arrive an hour later or tomorrow. `publisher` written then is
--     an accusation drawn from a race, and — worse — it is never retracted,
--     because the row that would retract it belongs to a different object and
--     a different key. The loader also holds no `SELECT` grant at all (`004`),
--     which is not an obstacle to be worked around: an account that cannot read
--     `datagram` cannot become the most expensive query on the cluster.
--
--   * **A second derivation that writes the answer back.** This is the shape
--     the word *precomputed* suggests, and `sequence_gap` cannot take it
--     today. It is a `ReplacingMergeTree` with **no version column**, so the
--     later of two rows on one sort key is whichever the merge happens to keep
--     — and a re-run of the original object after the upgrade pass would
--     restore the un-upgraded row, silently demoting a settled `publisher`
--     back to `unverifiable` or, in the other direction, keeping a `publisher`
--     the evidence has since withdrawn. `era` takes late evidence safely only
--     because it carries `ReplacingMergeTree(anchor_certain)`; giving
--     `sequence_gap` the equivalent is a table recreation and a decision of its
--     own, not a side effect of adding a join. A materialised view is not the
--     escape either: it fires on the block being inserted and can no more see
--     the other site's rows arriving tomorrow than the loader can.
--
--   * **A view.** It is recomputed on every read, so a site that arrives late
--     changes the answer the next time the question is asked, and a site that
--     never arrives leaves the answer unknown rather than wrong. Idempotence
--     costs nothing: the counting below is over **distinct vantages**, so the
--     same object loaded twice is one absence and not two, which is the same
--     reason `006` counts `uniqExact(observation)` rather than rows.
--
-- What a view gives up is that a reader pays for the join, and the answer to
-- that is below: for the verdict it pays over the *derived* grains only.
--
--
-- THE VERDICT IS DECIDED ON ROWS THAT OUTLIVE `datagram`, AND THAT IS THE
-- DESIGN RATHER THAN AN ECONOMY
--
-- The obvious join is gap-to-`datagram`: expand the missing sequence numbers
-- and look for them at the other sites. That join answers *present* correctly
-- and answers *absent* catastrophically, because `datagram` is the one table
-- with a TTL (`002`). Two days on, every site's rows are gone, every sequence
-- number looks absent everywhere, and a query that read absence off that would
-- promote every stale gap in the archive to `publisher` — the exact failure
-- `seen_elsewhere` exists to prevent, arriving on a timer.
--
-- So absence is read from the two rows that have no TTL and are three orders of
-- magnitude smaller than the base rows:
--
--   * `segment_coverage` says a site covered `[first_seq, last_seq]` of an
--     instance over a segment. Without a covering row a site is not *silent*
--     about that sequence number — it is not speaking at all, and the two are
--     not the same thing.
--   * `sequence_gap` — that site's own gap rows — says which sequence numbers
--     inside a covered range it did not receive.
--
-- Within a covered range those two are exhaustive: a site held a sequence
-- number if and only if it covered it and recorded no gap over it. That makes
-- the whole verdict a join over rows that are kept indefinitely, and it is why
-- a panel that could never afford a self-join over `datagram` can afford this
-- one. `datagram` is read for exactly one thing below — `sent_from_ts` and
-- `sent_to_ts`, the publisher's own send stamps, which only a site that
-- actually received the datagram can supply — and those go null when the base
-- rows expire, which costs a verdict nothing.
--
--
-- RECORDER OVERFLOW IS WHAT MAKES AN ABSENCE INADMISSIBLE, AND IT IS ALREADY
-- IN THE ROWS TWICE
--
-- A site that dropped datagrams itself cannot contribute an absence: its gap
-- may be its own ring, and counting it as evidence about the publisher is the
-- subtraction `drop_scope` exists to forbid. Nothing new is invented for this;
-- both existing carriers are required to agree.
--
--   * **The segment.** `segment_coverage.capture_drop_total` is cumulative and
--     never resets, so the total is a statement about the host's whole history
--     and only the *delta* over the preceding segment says anything about this
--     window. `segment_overflow` below computes that delta, and it is `NULL` —
--     not zero — when the preceding segment is missing, because a hole in
--     `segment_seq` is precisely where an unaccounted burst hides.
--
--   * **The gap row.** `unexplained_count` is already the loader's own verdict
--     on whether a per-instance subtraction is valid *at the scope the archive
--     declared*: null at capture-handle scope whenever the handle admitted
--     anything, and null at port-role scope whenever the role carried more than
--     one instance and admitted anything. A site whose own row cannot say what
--     it lost cannot tell us what the publisher lost.
--
-- An absence counts only when the segment's delta is exactly zero, the site's
-- own residue is the whole of its gap, and its era boundary is settled. Any
-- other combination leaves the answer unknown, and unknown is `NULL`.
--
--
-- NULL IS "NOT YET KNOWN", AND IT IS NOT "NO"
--
-- `seen_elsewhere` is three-valued and every one of the three is load-bearing:
--
--   1  present at another vantage — this was not a publisher gap, whatever else
--      it was
--   0  absent at every vantage that could speak, with none of them overflowing,
--      none of them silent, and at least one of them a different *site*
--   NULL  nobody else could speak yet
--
-- A `NULL` that a panel reads as `0` is this column's whole failure mode, and
-- it is why the escalation below tests `seen_elsewhere = 0` explicitly rather
-- than `seen_elsewhere != 1`: the second promotes on ignorance.
--
--
-- PRESENCE IS ADMISSIBLE FROM ANY VANTAGE; ABSENCE ONLY FROM ANOTHER SITE
--
-- The asymmetry is deliberate. A second recorder in the same rack holding the
-- datagram is conclusive that the publisher sent it, so presence anywhere but
-- here sets `seen_elsewhere = 1`. That same recorder *missing* it is not
-- independent evidence — it shares our switch, our uplink and our load — so
-- only a different `site` may contribute an absence. Two vantages of one
-- instance are two observations, and the recorder is never folded.
--
--
-- WHAT THIS STILL CANNOT SEE, WRITTEN DOWN RATHER THAN DISCOVERED
--
-- There is no registry of sites here, so "every site" means every site the
-- archive holds a coverage row for. A site that has been down all day is
-- invisible to this join and cannot block a verdict. What *is* caught is the
-- narrower and far commoner case: a site with coverage for the instance on the
-- gap's own day but none over the gap's window — up, and not reporting here.
-- The day is `toYYYYMMDD(before_ts)`, which is `sequence_gap`'s own partition
-- key, so the census and the gap prune together rather than on a constant
-- somebody picked.
--
--
-- THE VIEWS, AND WHICH ONE A PANEL READS
--
--   segment_overflow          a segment's admitted loss as a delta over its
--                             predecessor, `NULL` where there is no predecessor
--   gap_missing_seq           every gap expanded into its missing sequence
--                             numbers, which is what makes the join an equality
--   instance_vantage_day      which vantages the archive knew for an instance
--                             on a day, so *silent* is distinguishable from
--                             *absent*
--   gap_vantage_seq           what each other vantage says about each of our
--                             missing sequence numbers
--   gap_cross_site_evidence   the same, folded to one row per gap
--   gap_sent_elsewhere        the publisher's send stamps, from `datagram`
--   sequence_gap_cross_site   **the one a panel reads**: every column of
--                             `sequence_gap` with the cross-site answer filled
--                             in, and the evidence for it beside
--
-- Filter it on `before_ts`. The chain prunes on that predicate all the way
-- down — it is `sequence_gap`'s partition key and the census's join key — and
-- without one the expansion runs over every gap the archive has ever recorded.


-- 1. What a segment admitted losing, as a delta rather than as a total.
--
-- The counter is cumulative and never resets: a host that dropped a burst an
-- hour ago carries it for ever, so a rule reading the total would find no site
-- admissible on any host that ever overflowed. The delta is the reading, and it
-- exists only where the preceding segment does.
--
-- `ASOF LEFT JOIN` on `start_ts` takes the nearest earlier segment, which is
-- the same shape `003` resolves an era with. `segment_seq` cannot order it:
-- it restarts at 0 on every recorder run, so two runs sorted by it interleave.
-- Adjacency is then checked on `segment_seq` rather than assumed, and a hole —
-- an object evicted, lost with a shipper, or never written — leaves the delta
-- `NULL`. That null is the point: a missing segment is exactly where an
-- unaccounted burst hides, and calling it zero would admit the absence it
-- conceals.
--
-- `present` is carried out of the subquery because an `ASOF LEFT JOIN` fills an
-- unmatched row with column defaults and not with nulls, and a defaulted
-- `segment_seq` of 0 is adjacent to segment 1 by arithmetic.
CREATE OR REPLACE VIEW recorder.segment_overflow AS
SELECT
    c.site,
    c.recorder,
    c.env,
    c.feed,
    c.source_addr,
    c.channel_id,
    c.dst_port,
    c.segment_seq,
    c.start_ts,
    c.end_ts,
    c.first_seq,
    c.last_seq,
    c.drop_scope,
    c.capture_drop_total,
    -- Saturating, as the loader's own subtraction is: a counter that went
    -- backwards is a host that rebooted or an interface that was replaced, and
    -- a wrapped subtraction there reports eighteen quintillion drops.
    if(p.present = 1 AND p.segment_seq + 1 = c.segment_seq,
       c.capture_drop_total - least(p.capture_drop_total, c.capture_drop_total),
       NULL) AS capture_drop_delta,
    -- Unknown, admitted, or clean — and never the first two collapsed into one.
    if(isNull(capture_drop_delta), NULL, toUInt8(capture_drop_delta = 0)) AS overflow_free
FROM recorder.segment_coverage AS c
ASOF LEFT JOIN (
    SELECT
        site,
        recorder,
        source_addr,
        channel_id,
        dst_port,
        segment_seq,
        start_ts,
        capture_drop_total,
        1 AS present
    FROM recorder.segment_coverage
) AS p
    ON  p.site        = c.site
    AND p.recorder    = c.recorder
    AND p.source_addr = c.source_addr
    AND p.channel_id  = c.channel_id
    AND p.dst_port    = c.dst_port
    AND p.start_ts    < c.start_ts;


-- 2. Each gap, expanded into the sequence numbers it is missing.
--
-- The design's own instruction, and not a workaround: a column store has no
-- correlated subqueries, so "was this sequence number seen anywhere else"
-- cannot be a per-row subselect. Expanding first turns it into an equality
-- join on the sort key's own leading columns — and it attributes per datagram
-- rather than per range, so a gap half of which appears at another site is
-- reported as half rather than as present.
--
-- `FINAL`, and here rather than in each of the three views that read this one.
-- A re-run after an analyser fix is a replace, so between a re-load and the
-- merge that follows it one gap is in the table twice; expanded twice it would
-- count one site's single absence as two, which is exactly the miscount the
-- verdict must not be built on.
--
-- THE EXPANSION IS BOUNDED, AND WHAT IS CUT OFF SAYS SO. `range` over a gap of
-- billions is a query that fails rather than answers, and a view that throws
-- takes down every panel reading it. A gap wider than a million sequence values
-- inside one era is a stream restart or a misread era boundary rather than
-- loss, so it is expanded to the bound and marked incomplete — and an
-- incomplete expansion can never reach `publisher`, because part of the range
-- was never checked anywhere.
CREATE OR REPLACE VIEW recorder.gap_missing_seq AS
SELECT
    site,
    recorder,
    env,
    feed,
    port_role,
    group_addr,
    source_addr,
    channel_id,
    dst_port,
    era_anchor_ts,
    anchor_certain,
    missing_from,
    missing_to,
    missing_count,
    before_ts,
    after_ts,
    unexplained_count,
    verdict,
    toUInt8(missing_count <= 1048576) AS expansion_complete,
    arrayJoin(range(missing_from, missing_from + least(missing_count, 1048576))) AS sequence_number
FROM recorder.sequence_gap FINAL;


-- 3. Which vantages the archive knows for an instance on a day.
--
-- The census that makes *silent* distinguishable from *absent*. A site with
-- coverage of this instance today and none over this gap's window was up and
-- not reporting here, and a window a site is not reporting in is not a window
-- in which it reported nothing — so it blocks a verdict rather than being
-- counted absent.
--
-- Keyed on the day because that is `sequence_gap`'s partition key and
-- `segment_coverage`'s, so the census and the gap prune on the same expression.
-- A gap whose bracket crosses midnight is judged on its opening day, which is
-- the day its own row is partitioned into.
CREATE OR REPLACE VIEW recorder.instance_vantage_day AS
SELECT
    source_addr,
    channel_id,
    dst_port,
    toYYYYMMDD(start_ts) AS day,
    -- `toString` because a tuple of `LowCardinality` columns inside an array is
    -- not comparable against a tuple of plain strings, and this array is read
    -- with `has`.
    groupUniqArray((toString(site), toString(recorder))) AS vantages
FROM recorder.segment_coverage FINAL
GROUP BY source_addr, channel_id, dst_port, day;


-- 4. What every other vantage says about each of our missing sequence numbers.
--
-- One row per (gap, sequence number, other vantage that covered it). The
-- coverage row is what admits a vantage to the conversation at all; its own gap
-- rows are what it says once admitted; and within a covered range those two are
-- exhaustive, so `missed = 0` means *held* and needs no `datagram` row to prove
-- it.
--
-- The time overlap is a guard against the sequence space repeating. A `Reset
-- Count` wraps and an instance long-lived enough reuses a sequence number in a
-- later era, so a coverage row is admitted only when its window overlaps the
-- bracket the missing datagram was sent in. Overlap and not containment:
-- segments rotate on their own clocks at each site, so a gap of ours routinely
-- spans two of theirs.
CREATE OR REPLACE VIEW recorder.gap_vantage_seq AS
SELECT
    m.site            AS site,
    m.recorder        AS recorder,
    m.source_addr     AS source_addr,
    m.channel_id      AS channel_id,
    m.dst_port        AS dst_port,
    m.era_anchor_ts   AS era_anchor_ts,
    m.missing_from    AS missing_from,
    m.sequence_number AS sequence_number,
    m.expansion_complete AS expansion_complete,
    o.site            AS other_site,
    o.recorder        AS other_recorder,
    o.overflow_free   AS other_overflow_free,
    t.present         AS missed,
    -- A vantage at another site. A second recorder in our own rack shares our
    -- switch, our uplink and our load, so what it *missed* is not independent
    -- evidence — while what it *held* is conclusive, which is why this gates
    -- only the absence columns and never `held`.
    toUInt8(o.site != m.site) AS independent,
    -- Held: covered by that vantage and absent from its own gap rows. Within a
    -- covered range those are the only two possibilities, which is what lets
    -- this be answered without reading a base row that may have expired.
    toUInt8(t.present = 0) AS held,
    -- An absence is evidence only from another **site**, whose switch, uplink
    -- and load are not ours; whose segment admitted nothing over the window;
    -- whose own residue accounts for the whole of its own gap; and whose era
    -- boundary is settled. Anything else is an absence nobody may use.
    toUInt8(ifNull(t.present = 1
                   AND independent = 1
                   AND o.overflow_free = 1
                   AND t.unexplained_count = t.missing_count
                   AND t.anchor_certain = 1, 0)) AS absence_admissible
FROM recorder.gap_missing_seq AS m
INNER JOIN recorder.segment_overflow AS o
    ON  o.source_addr = m.source_addr
    AND o.channel_id  = m.channel_id
    AND o.dst_port    = m.dst_port
LEFT JOIN (
    SELECT
        site,
        recorder,
        source_addr,
        channel_id,
        dst_port,
        sequence_number,
        unexplained_count,
        missing_count,
        anchor_certain,
        toUInt8(1) AS present
    FROM recorder.gap_missing_seq
) AS t
    ON  t.site            = o.site
    AND t.recorder        = o.recorder
    AND t.source_addr     = m.source_addr
    AND t.channel_id      = m.channel_id
    AND t.dst_port        = m.dst_port
    AND t.sequence_number = m.sequence_number
WHERE (o.site != m.site OR o.recorder != m.recorder)
  AND m.sequence_number >= o.first_seq
  AND m.sequence_number <= o.last_seq
  AND o.start_ts <= m.after_ts
  AND o.end_ts   >= m.before_ts;


-- 5. The same, folded back to one row per gap.
--
-- Distinct **vantages** and distinct **sequence numbers**, never rows: a re-run
-- after an analyser fix is a replace, and between the second load and the merge
-- one site's rows are in the tables twice. Counted as rows, one site's single
-- absence would be two absences and a gap seen from one vantage would look like
-- a gap seen from two — which is the one arithmetic error that could promote a
-- verdict on evidence nobody has. It is the same reason `006` counts
-- `uniqExact(observation)`.
CREATE OR REPLACE VIEW recorder.gap_cross_site_evidence AS
SELECT
    site,
    recorder,
    source_addr,
    channel_id,
    dst_port,
    era_anchor_ts,
    missing_from,
    uniqExact(sequence_number)                           AS seqs_expanded,
    min(expansion_complete)                              AS expansion_complete,
    uniqExactIf(sequence_number, held = 1)               AS seqs_held,
    uniqExactIf(sequence_number, absence_admissible = 1) AS seqs_absent,
    uniqExactIf(other_site, absence_admissible = 1)      AS absent_sites,
    -- A vantage at another site that also missed it and whose absence cannot be
    -- used: its own ring overflowed, its scope invalidates the subtraction, or
    -- its era boundary is unsettled. Its gap may be its own, and an absence that
    -- may be somebody's own ring is no evidence about a publisher — but it is a
    -- site that would otherwise have been evidence, so it blocks rather than
    -- abstains.
    --
    -- `independent = 1`, so a recorder in our own rack neither counts nor
    -- blocks. Its absence is not a second opinion about a publisher, and a
    -- deployment running two recorders at a site would otherwise never reach a
    -- verdict at all.
    uniqExactIf((toString(other_site), toString(other_recorder)),
                missed = 1 AND independent = 1
                AND absence_admissible = 0)              AS blocked_vantages,
    groupUniqArrayIf((toString(other_site), toString(other_recorder)),
                     held = 1)                           AS held_by,
    groupUniqArray((toString(other_site), toString(other_recorder))) AS spoke
FROM recorder.gap_vantage_seq
GROUP BY
    site, recorder, source_addr, channel_id, dst_port, era_anchor_ts,
    missing_from;


-- 6. The publisher's own send stamps, from a site that received the datagram.
--
-- The one place `datagram` is read, and the only thing that needs it: a site
-- has no clock reading for a datagram it never received, so its own
-- `before_ts`/`after_ts` bracket is the weaker answer and the send stamp
-- recovered from elsewhere is the stronger one.
--
-- No predicate on `recv_ts`, deliberately. A bound would have to allow for the
-- clock skew between two sites and for the path delta between them, and a bound
-- set too tight reads a datagram another site *has* as absent — which promotes
-- a verdict. Unbounded, the only error left is the harmless direction, and the
-- cost is bounded anyway: `datagram` carries a two-day TTL, so "every
-- partition" is three of them, and the join keys are the leading columns of its
-- own sort key.
CREATE OR REPLACE VIEW recorder.gap_sent_elsewhere AS
SELECT
    m.site          AS site,
    m.recorder      AS recorder,
    m.source_addr   AS source_addr,
    m.channel_id    AS channel_id,
    m.dst_port      AS dst_port,
    m.era_anchor_ts AS era_anchor_ts,
    m.missing_from  AS missing_from,
    uniqExact(m.sequence_number) AS seqs_held_in_base_rows,
    -- Nullable, so that a gap no other site held reads as a stamp nobody
    -- measured rather than as the epoch: an unmatched LEFT JOIN fills a
    -- plain DateTime64 with 1970 and a Nullable one with NULL.
    toNullable(min(d.send_ts)) AS sent_from_ts,
    toNullable(max(d.send_ts)) AS sent_to_ts,
    groupUniqArray((toString(d.site), toString(d.recorder))) AS held_by
FROM recorder.gap_missing_seq AS m
INNER JOIN recorder.datagram AS d
    ON  d.source_addr     = m.source_addr
    AND d.channel_id      = m.channel_id
    AND d.dst_port        = m.dst_port
    AND d.sequence_number = m.sequence_number
WHERE d.site != m.site OR d.recorder != m.recorder
GROUP BY
    m.site, m.recorder, m.source_addr, m.channel_id, m.dst_port,
    m.era_anchor_ts, m.missing_from;


-- 7. The gap rows, with the cross-site answer filled in.
--
-- Every column of `sequence_gap`, so a panel reads this in place of the table,
-- with `seen_elsewhere`, `sent_from_ts`, `sent_to_ts` and `verdict` resolved and
-- the evidence for them beside.
--
-- **The escalation only ever runs `unverifiable` to `publisher`.** `recorder`,
-- `upstream` and `path` are exculpatory verdicts already decided from evidence
-- one object holds, and nothing found at another site makes a gap our own ring
-- admitted anything other than ours. So this pass can add an accusation and can
-- never remove one, which is what keeps it from becoming a second place
-- attribution is decided.
--
-- Its conditions are conjunctive, and each one is a promotion this would
-- otherwise have made on ignorance: the answer is *known* absent rather than
-- merely not known present; our own residue is a real number and greater than
-- zero; our era boundary is settled; every missing sequence number was
-- accounted for by somebody; the range was expanded whole; no other vantage
-- missed it while unable to rule out its own overflow; and no site the archive
-- knew that day went quiet over this window.
CREATE OR REPLACE VIEW recorder.sequence_gap_cross_site AS
SELECT
    g.site               AS site,
    g.recorder           AS recorder,
    g.env                AS env,
    g.feed               AS feed,
    g.port_role          AS port_role,
    g.group_addr         AS group_addr,
    g.source_addr        AS source_addr,
    g.channel_id         AS channel_id,
    g.dst_port           AS dst_port,
    g.reset_count        AS reset_count,
    g.era_index          AS era_index,
    g.era_anchor_ts      AS era_anchor_ts,
    g.anchor_certain     AS anchor_certain,
    g.missing_from       AS missing_from,
    g.missing_to         AS missing_to,
    g.missing_count      AS missing_count,
    g.reference_seqs     AS reference_seqs,
    g.before_ts          AS before_ts,
    g.after_ts           AS after_ts,
    g.admitted_recorder  AS admitted_recorder,
    g.admitted_scope     AS admitted_scope,
    g.unexplained_count  AS unexplained_count,
    g.interface_drops    AS interface_drops,
    g.on_redundant_path  AS on_redundant_path,
    g.object_key         AS object_key,
    -- The evidence, so that a reader can see what the answer was made of rather
    -- than having to trust it.
    q.seqs_expanded      AS seqs_expanded,
    q.expansion_complete AS expansion_complete,
    greatest(q.seqs_held, s.seqs_held_in_base_rows) AS seqs_seen_elsewhere,
    q.seqs_absent        AS seqs_absent,
    q.absent_sites       AS absent_sites,
    q.blocked_vantages   AS blocked_vantages,
    -- Up today, and not reporting over this window. Same-site vantages are left
    -- out: their silence shares our switch, our uplink and our load, and says
    -- nothing about a publisher either way.
    length(arrayFilter(
        x -> (x.1 != toString(g.site)) AND NOT has(q.spoke, x),
        c.vantages)) AS silent_vantages,
    arrayDistinct(arrayConcat(q.held_by, s.held_by)) AS seen_at,
    -- 1 present elsewhere, 0 absent at every vantage that could speak, NULL not
    -- yet known. A NULL read as a 0 is this column's whole failure mode, which
    -- is why the escalation below tests `= 0` and never `!= 1`.
    multiIf(
        seqs_seen_elsewhere > 0, toNullable(toUInt8(1)),
        q.seqs_expanded > 0
            AND q.expansion_complete = 1
            AND q.seqs_absent = q.seqs_expanded
            AND q.absent_sites > 0
            AND q.blocked_vantages = 0
            AND silent_vantages = 0, toNullable(toUInt8(0)),
        NULL) AS seen_elsewhere,
    -- Null rather than our own bracket: a stamp nobody measured would be read
    -- as the publisher's, and the bracket is already on the row beside it.
    s.sent_from_ts       AS sent_from_ts,
    s.sent_to_ts         AS sent_to_ts,
    if(g.verdict = 'unverifiable'
       AND ifNull(seen_elsewhere = 0, 0)
       AND g.anchor_certain = 1
       AND ifNull(g.unexplained_count > 0, 0),
       'publisher',
       g.verdict) AS verdict
FROM recorder.sequence_gap AS g
FINAL
LEFT JOIN recorder.gap_cross_site_evidence AS q
    ON  q.site          = g.site
    AND q.recorder      = g.recorder
    AND q.source_addr   = g.source_addr
    AND q.channel_id    = g.channel_id
    AND q.dst_port      = g.dst_port
    AND q.era_anchor_ts = g.era_anchor_ts
    AND q.missing_from  = g.missing_from
LEFT JOIN recorder.gap_sent_elsewhere AS s
    ON  s.site          = g.site
    AND s.recorder      = g.recorder
    AND s.source_addr   = g.source_addr
    AND s.channel_id    = g.channel_id
    AND s.dst_port      = g.dst_port
    AND s.era_anchor_ts = g.era_anchor_ts
    AND s.missing_from  = g.missing_from
LEFT JOIN recorder.instance_vantage_day AS c
    ON  c.source_addr = g.source_addr
    AND c.channel_id  = g.channel_id
    AND c.dst_port    = g.dst_port
    AND c.day         = toYYYYMMDD(g.before_ts);
