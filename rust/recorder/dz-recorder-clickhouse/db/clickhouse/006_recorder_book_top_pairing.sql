-- The race, as a query over `book_top`: number the occurrences, then pair
-- ordinal to ordinal.
--
-- No new table, for the reason the cross-site loss comparison needed none: both
-- sides' rows land in one table and the comparison is a query. `observation`
-- names where a view of the book came from, as `site` names a recorder — two
-- recorders of one multicast feed are two observations, and a multicast feed and
-- some other transport carrying the same instruments are two observations.
-- Nothing here knows which kind it is looking at, and nothing should.
--
--
-- WHY THIS IS NOT AN `ASOF JOIN`, WHICH IS THE OBVIOUS MOVE
--
-- `state_key` is not unique and must not be: a book returning to a previous
-- state produces the same key again, which is the truth about the book rather
-- than a defect in the key. `ASOF` selects the nearest right-hand row
-- *independently for each left-hand row*, with no notion of consuming a match,
-- so when a state repeats quickly several occurrences at one observation point
-- all pair with the same occurrence at the other. The lead times that come out
-- are not wrong in a way anyone notices: they are plausible, biased, and
-- derived from counting one arrival several times. That is the failure this
-- file exists to make impossible, and it is invisible in every graph drawn
-- from it.
--
-- So the occurrences are numbered instead. Within one `(observation,
-- channel_id, instrument_id, era, state_key)` each row takes its ordinal by
-- `recv_ts`, and ordinal *n* at one observation point pairs with ordinal *n* at
-- the other. One-to-one by construction, no window function beyond
-- `row_number`, and it fails visibly rather than quietly.
--
--
-- AN UNPAIRED OCCURRENCE IS A ROW, NOT AN ABSENCE
--
-- The pairing is an aggregate over the ordinal and not a join between two named
-- observation points, so an occurrence only one of them saw survives as a row
-- with `observations = 1` and a null `lead_ms`. That is the fact worth seeing:
-- it usually means one observation point missed a state the other saw. A join
-- would have dropped it, and a zero lead would have entered every average taken
-- over the column.
--
-- It is also what keeps this generic. Nothing below names an observation point,
-- so two of them are a race and three of them are the same query — where a
-- self-join would have needed the names written into the schema, which is the
-- one thing `observation` was declared as an opaque string to avoid.
--
--
-- THE BOUND ON |Δt| IS THE CALLER'S PREDICATE, NOT A CONSTANT HERE
--
-- Ordinals that happen to align across an outage produce a pair that is
-- arithmetically fine and meaningless, and the remedy is a bound on the
-- difference of the arrival stamps. That bound is a property of the two paths
-- being compared — a metropolitan pair and an intercontinental one do not share
-- one — so `lead_ms` is a column and `WHERE abs(lead_ms) < ...` is the caller's.
-- A constant written here would be right for the first deployment and silently
-- wrong for the second.


-- 1. The book, collapsed.
--
-- `book_top` is a `ReplacingMergeTree` and reprocessing an object is a replace,
-- so between a re-load and the merge that follows it one top of book is in the
-- table twice. Numbering over that counts one arrival as two occurrences —
-- which is exactly the double count the ordinal exists to prevent — and the
-- surplus copy then pairs with nothing and is reported as a state the other
-- observation point missed. A duplicate would therefore not merely inflate a
-- count here: it would manufacture evidence of loss.
--
-- `FINAL` is what applies the collapse at read time, and it is affordable for
-- the reason `003` gives for `era`: it forces merge-on-read over the parts a
-- query reads, and `book_top` is partitioned by day, so a predicate on
-- `recv_ts` prunes the partitions first and `FINAL` pays for what is left.
--
-- Written once, here. Every view below reads this one rather than the table.
CREATE OR REPLACE VIEW recorder.book_top_settled AS
SELECT *
FROM recorder.book_top FINAL;


-- 2. The occurrence ordinal, per observation point.
--
-- THE ERA IS IN THE PARTITION BECAUSE AN `Instrument ID` IS ONLY UNIQUE WITHIN
-- ONE. An operator retiring an instrument and later publishing another under
-- the same identifiers produces two instruments, and numbering their states in
-- one sequence would pair a state of the first against a state of the second.
-- The era is resolved by range join on the anchor, exactly as `003` resolves a
-- datagram's, so nothing is stored that renumbers when an earlier object
-- arrives late.
--
-- `ASOF LEFT JOIN`, and LEFT is load-bearing: a book row whose era row has not
-- been loaded yet is still an observation of a state, and dropping it would
-- take an occurrence away from one observation point and leave the other's
-- looking unpaired — a missed state invented by a join.
--
-- `from_anchor = 0` IS FILTERED BEFORE THE WINDOW, IN THIS SAME SELECT, AND
-- THAT IS NOT AN OPTIMISATION. A snapshot anchors a book and never times one:
-- the runtime pulls it on its own cadence and the archive records when it was
-- published rather than when it was asked for, so its arrival stamp instead
-- measures the publisher's scheduler and is no observation of a market at all.
-- `WHERE` runs before a window, so an anchor row consumes no ordinal. Excluding
-- these rows *after* the numbering would leave every later occurrence at that
-- observation point numbered one too high, and the pairing off by one for the
-- rest of the era — which reads as a lead time rather than as a mistake.
--
-- `book_certain` is carried and never filtered, which is the opposite treatment
-- and has the opposite reason. Certainty is one observation point's own verdict
-- on its own book: a gap on one path makes that point uncertain and leaves the
-- other certain about the same state. Dropping uncertain rows from the
-- numbering would therefore shift one side's ordinals and not the other's, and
-- misalign every pair that followed. A caller that wants only believed states
-- writes a predicate over the output, which removes pairs rather than ordinals.
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
        PARTITION BY b.observation, b.channel_id, b.instrument_id,
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


-- 3. The pairing.
--
-- THE ERA IS DELIBERATELY NOT IN THIS GROUPING, AND THAT IS THE ONE PLACE THE
-- ORDINAL'S KEY AND THE PAIRING'S KEY DIFFER. An era's stored identity is its
-- anchor, and an anchor is a *receive* stamp — one observation point's
-- observation of that era. Two recorders of one multicast feed open their eras
-- at two instants; two transports do not share a sequence space at all, so they
-- share no era in any form. Grouping on any era column would therefore pair
-- nothing across observation points and report a total outage as a clean feed.
-- The era does its work above, where it keeps one point's own numbering from
-- running across a boundary; here the bound on `lead_ms` is what discards
-- ordinals that aligned by coincidence.
--
-- `uniqExact(observation)` RATHER THAN `count()`, for the case that survives
-- the paragraph above: an occurrence number restarts at each era, so one
-- observation point that crossed an era boundary inside the window can hold
-- ordinal *n* twice. Counting rows would report three observations of a state
-- two points saw; counting distinct observation points cannot, and the surplus
-- row can then only widen `lead_ms`, where the caller's bound already discards
-- it.
--
-- `exponents_agree` IS THE ASSERTION `state_key` MADE AND DID NOT HASH. The key
-- covers the raw prices and quantities and leaves `price_exp` and `qty_exp` out,
-- because within an era they are the same number and hashing them would make
-- the key depend on a restatement that moved no market data. That assumption is
-- checked here rather than trusted: a pair whose exponents disagree is two
-- different prices wearing one key, and it is visible as a column rather than
-- silently averaged into the result.
CREATE OR REPLACE VIEW recorder.book_top_race AS
SELECT
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
    -- Null rather than zero when only one observation point saw the state. A
    -- zero would be a lead time nobody measured, and it would enter every
    -- average taken over this column as evidence that the two paths tied.
    if(uniqExact(observation) > 1,
       (toUnixTimestamp64Nano(max(recv_ts)) - toUnixTimestamp64Nano(min(recv_ts))) / 1e6,
       NULL)                               AS lead_ms,
    -- The weakest verdict of the points that saw it: a pair is only as
    -- believable as the least believable side of it.
    min(book_certain)                      AS book_certain,
    (uniqExact(price_exp) = 1) AND (uniqExact(qty_exp) = 1) AS exponents_agree
FROM recorder.book_top_occurrence
GROUP BY channel_id, instrument_id, state_key, occurrence;
