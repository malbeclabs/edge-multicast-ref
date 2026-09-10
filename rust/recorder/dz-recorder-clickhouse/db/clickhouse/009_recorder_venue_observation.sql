-- The venue half of a feed race: its own two grains, and the race as a query.
--
-- A feed race compares what a venue said with what a publisher sent. `006` is
-- the publisher half of it — occurrences numbered per observation point and
-- paired ordinal to ordinal over `book_top`. This file is the other half.
--
--
-- WHY THESE ARE NEW TABLES AND NOT ROWS IN `book_top`
--
-- `book_top` cannot hold a venue-side row, for two reasons, and both are about
-- the columns rather than about the volume.
--
-- `event` and `book_top` carry eight non-nullable provenance columns —
-- `source_addr`, `channel_id`, `dst_port`, `source_id`, `instrument_id`,
-- `sequence_number`, `reset_count`, `segment_seq` — and they are in the sort
-- key. Each is a statement about a datagram on a channel instance, and a venue's
-- upstream message is not one: a websocket message, a tag-value message on a
-- session and a response body have no source address, no destination group, no
-- port role and no `Sequence Number`. There is no honest value and no sentinel
-- either, because every plausible one is also a real reading — channel `0` is a
-- channel, port `0` is a port a document can state, sequence `0` is the first
-- sequence of an era, and `0.0.0.0` reads as *unset* to a person and as a value
-- to a query. Making one of them nullable would weaken every publisher-side row
-- to accommodate a row of a different kind, which is the rule
-- `uncertain_reason` already states: a column is not nullable when a NULL
-- invites a join that drops the row.
--
-- And the pairing in `006` groups on `channel_id` and `instrument_id`, neither
-- of which a venue side may compute. The channel is the operator's mapping from
-- a shard to a channel and the whole adapter boundary exists so that a venue
-- cannot be handed one; the identifier is minted by the publisher's
-- reference-data registry and is unique only within an era. A venue knows the
-- symbol.
--
-- So the venue's rows are their own grains with their own provenance, and the
-- comparison is a query over them.
--
--
-- WHY THE KEY IS `book_key` AND NOT `state_key`
--
-- `state_key` is the equivalence key **two recorders of one multicast feed**
-- pair on: `state_key(channel_id, instrument_id, top)` folds both identifiers
-- into the accumulator before it folds a price, so it is transport-independent
-- and *observer-dependent*. Two recorders of one feed pair on it because both
-- read the same identifiers off the same datagrams.
--
-- A venue-side observer can compute neither identifier, so keyed on `state_key`
-- this race would return **zero pairs** and read as each side missing every
-- state the other saw — which is the failure that function's own comment names,
-- where a key that stops matching "looks like a quiet feed".
--
-- `book_key` is the book-only key: a hash over the two sides and nothing else,
-- computable by anyone holding a top of book. It is written by
-- `dz_recorder_events::book_key` and never by a second implementation, in SQL or
-- anywhere else — two hashes of one book state pair with nothing, and the
-- predicate that decides whether a side is absent is private in that crate for
-- exactly that reason.
--
--
-- WHAT THIS FILE DOES NOT YET JOIN, AND WHY IT IS WRITTEN AS THOUGH IT WILL
--
-- The publisher side does **not** feed the pairing below yet, and that is a
-- statement about `book_top` rather than about this design. `book_top` stores
-- `state_key` and has no `book_key` column, and the two ways to bridge that are
-- both refused: a new column on `book_top` is what the design this file
-- implements declines to add, and a second fold written in SQL is the second
-- implementation `book_key`'s own comment forbids.
--
-- What is written instead is the shape that admits the publisher side without
-- changing the answer. Nothing below names an observation point — the pairing is
-- an aggregate over the ordinal, exactly as `006`'s is — so two observations are
-- a race, three of them are the same query, and a publisher-side observation
-- enters it by contributing rows rather than by this view learning its name. The
-- day `book_top` carries a `book_key`, the change here is one branch of a union
-- and not a rewrite.
--
--
-- THE ARGUMENTS THIS FILE CITES RATHER THAN RESTATES
--
-- Every one of them is made in full in `006`'s header and carries over
-- unchanged. They are named here so that a reader knows they were considered,
-- and left there so that there is one copy of each to keep true:
--
--   * WHY THIS IS NOT AN `ASOF JOIN`, WHICH IS THE OBVIOUS MOVE. A book state
--     repeats, `ASOF` has no notion of consuming a match, and the lead times it
--     produces are plausible, biased, and derived from counting one arrival
--     several times.
--   * AN UNPAIRED OCCURRENCE IS A ROW, NOT AN ABSENCE. The pairing is an
--     aggregate over the ordinal rather than a join between two named points,
--     so a state one side saw survives with `observations = 1` and a null
--     `lead_ms`.
--   * THE BOUND ON |Δt| IS THE CALLER'S PREDICATE, NOT A CONSTANT HERE. The
--     bound is a property of the two paths being compared, so `lead_ms` is a
--     column and `WHERE abs(lead_ms) < ...` is the caller's.
--   * `FINAL` AND WHY A DUPLICATE IS WORSE THAN A DOUBLE COUNT. Numbering over
--     an unmerged re-load counts one arrival as two occurrences, and the surplus
--     copy then pairs with nothing — so a duplicate does not inflate a count, it
--     manufactures evidence of loss.
--
-- Two of `006`'s arguments have **no counterpart here**, and saying so is the
-- point of naming them:
--
--   * THE ERA. `006` resolves an era by range join, because an `Instrument ID`
--     is only unique within one. There is no era on this side in any form: an
--     era is a publisher's `Reset Count` span, and a venue has none. The symbol
--     is the instrument identity here, which is coarser — it cannot separate two
--     instruments that shared a symbol across an era boundary — and that cost is
--     stated rather than papered over with a column nothing could fill.
--   * `from_anchor = 0`, FILTERED BEFORE THE WINDOW. `006` excludes
--     snapshot-anchored rows because a snapshot is pulled on the publisher's own
--     cadence and times nothing. There is no such row here: every
--     `venue_book_top` row comes from an upstream message the venue produced, so
--     there is nothing to exclude and no ordinal to shift by excluding it late.


-- 1. The venue-side top of book.
--
-- One row per change in the top of book, as an observer of a venue's own
-- upstream states it.
--
-- THERE IS NO `book_certain` COLUMN, AND THAT IS DELIBERATE. On the publisher
-- side certainty falls when a gap in the publisher's own sequence space means
-- the book cannot be believed — an observation this tier can make, because the
-- gap is visible in the archive. A venue-side observation has no sequence space
-- of its own that this repository defines: what would make its book untrustworthy
-- is the venue's own resynchronisation, which is a different thing measured a
-- different way. One column carrying both meanings, minimum-aggregated over a
-- pair, mixes them silently. So the venue side says what it does not know, and
-- `venue_object.desync_count` is where the evidence lands instead.
--
-- `feed` IS IN THE SORT KEY HERE AND IS A LABEL THERE, WHICH IS NOT AN
-- INCONSISTENCY. `event`, `instrument` and `book_top` leave it out because it is
-- recoverable from the channel instance: no two feeds serve one `(source
-- address, destination port)`. There is no channel instance on this side, so
-- nothing else tells two feeds apart at one observation point, and leaving it
-- out would collapse two feeds' rows into one under `ReplacingMergeTree`.
--
-- `message_index` IS IN THE KEY FOR THE REASON `book_top` NEEDS IT. Two upstream
-- messages one instant apart move the book twice, and a key without the index
-- makes the second replace the first — a hole in the book's history that no
-- count would show.
--
-- `object_key` IS **NOT** IN THE KEY. A re-derivation of one object produces the
-- same `(observation, feed, symbol, recv_ts, message_index)` for the same
-- message, so the second load replaces the first, which is the whole of
-- `(object key, sha256)` idempotence at this grain. Keying on the object would
-- make a rebuilt object's rows sit beside the old ones and double every
-- occurrence — and here a duplicate manufactures evidence of loss rather than
-- inflating a count.
CREATE TABLE IF NOT EXISTS recorder.venue_book_top (
    recv_ts           DateTime64(9),
    -- Which observation point this recording is, as `site` names a host. The
    -- same opaque string `book_top.observation` is, and opaque for the same
    -- reason: nothing below names an observation point.
    observation       LowCardinality(String),
    env               LowCardinality(String),
    feed              LowCardinality(String),
    -- The upstream connection the message arrived on. Evidence and not a key: an
    -- adapter whose mapping depends on the connection reproduces nothing offline
    -- without it, and a row that cannot say which upstream it came from cannot
    -- be checked against that upstream's own logs.
    connection        LowCardinality(String),
    -- The venue's own session identity and its own number within that session,
    -- where the venue publishes them. NULL where it does not, and never zero:
    -- both are values a venue really sends.
    --
    -- NOTHING JOINS ON EITHER. A numbering whose resolution and meaning differ
    -- between transports would give one book state two keys, and a race keyed on
    -- it would find no pair. They are here because they separate a venue
    -- *resending* a state from the venue producing that state again — the same
    -- book and not the same event — and without them an unpaired occurrence has
    -- one fewer explanation available to it.
    upstream_sid      Nullable(UInt64),
    upstream_seq      Nullable(UInt64),
    -- The instrument as the venue names it, and the only instrument identity
    -- both sides of the race hold.
    symbol            LowCardinality(String),
    -- The exponents as the venue states them, through the adapter's own
    -- `InstrumentSpec`. Carried rather than assumed: `book_key` covers the raw
    -- prices and leaves these out, so `exponents_agree` below is what makes a
    -- disagreement visible instead of averaged.
    price_exp         Int8,
    qty_exp           Int8,
    bid_px_raw        Nullable(Int64),
    bid_qty_raw       Nullable(UInt64),
    -- NULL and never zero for *the venue did not say*. The top-of-book
    -- specification states this field as "0 if unavailable", so a zero is the
    -- multicast side's spelling of an absence — and `book_key` reads the two
    -- alike, so a zero here would leave the row and the key describing two
    -- different books.
    bid_source_count  Nullable(UInt16),
    ask_px_raw        Nullable(Int64),
    ask_qty_raw       Nullable(UInt64),
    ask_source_count  Nullable(UInt16),
    -- `dz_recorder_events::book_key`: a hash over the two sides and nothing
    -- else. Not `state_key`, which folds the `Channel ID` and the `Instrument ID`
    -- in first, and not a second implementation of either.
    book_key          UInt64,
    -- Which upstream message in the object moved the top, counted from zero.
    message_index     UInt64,
    object_key        String,
    object_sha256     String
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (observation, feed, symbol, recv_ts, message_index);


-- 2. The object a derivation read.
--
-- THE IDEMPOTENCE ROW. `(object_key, object_sha256)` is what a re-derivation
-- replaces on, so this is where a reader looks to see whether an object was read
-- at all, how much of it the adapter could parse, and what it refused. An object
-- derived twice is one row here.
--
-- WHY THE REFUSALS ARE HERE AND NOT ON A ROW. An adapter that refuses a message
-- costs **that message** and is counted; a derivation that stopped at the first
-- message a venue's own adapter could not parse would report the venue's feed as
-- having ended there, and those rows are indistinguishable from a venue that
-- went quiet. So the refusal has to be visible somewhere, and the object is the
-- grain it belongs to: it is a fact about a window rather than about a book
-- state, and there is by construction no row for the message it cost.
--
-- WHY `refusals` IS BY REASON AND NOT A BARE TOTAL. The four tokens are
-- `ParseError`'s own — `schema`, `unknown_field`, `malformed`, `truncated` —
-- and an operator acts differently on each: a schema refusal says the venue
-- changed its interface, a truncated one says the transport cut a message. A
-- bare total sends somebody to read the objects to find out which.
CREATE TABLE IF NOT EXISTS recorder.venue_object (
    recv_ts_start     DateTime64(9),
    recv_ts_end       DateTime64(9),
    observation       LowCardinality(String),
    env               LowCardinality(String),
    feed              LowCardinality(String),
    object_key        String,
    object_sha256     String,
    -- The archive format the object was written in, as its own header states it
    -- — not as a manifest beside it claims. A window derived by a reader that
    -- knew one version and an object written at another is the disagreement this
    -- column exists to make visible.
    format_version    UInt16,
    connections       Array(LowCardinality(String)),
    message_count     UInt64,
    refused_count     UInt64,
    -- Array(Tuple(String, UInt64)): `(reason, count)`, the same unnamed-tuple
    -- shape `segment_coverage.roles_joined` uses.
    refusals          Array(Tuple(String, UInt64)),
    event_count       UInt64,
    -- Events whose price or quantity could not be stated exactly at the
    -- instrument's own declared exponent. Counted rather than rounded, and the
    -- event is dropped: a rounded price is a price the venue did not quote, and
    -- a conversion taken as zero is a real-looking quote at nothing.
    unpriced_count    UInt64,
    -- Times the adapter said it no longer trusts its own book. Evidence and not
    -- a verdict; see the note on the absent `book_certain` above.
    desync_count      UInt64,
    book_top_count    UInt64,
    instrument_count  UInt32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(recv_ts_start)
-- The pair a re-derivation replaces on, and the observation and feed ahead of it
-- so that the partition prunes before the key is read.
ORDER BY (observation, feed, object_key, object_sha256);


-- THE RETENTION SPLIT, THE SAME ONE `005` MAKES ONE TABLE FURTHER DOWN.
--
-- `venue_book_top` is per change in the top rather than per upstream message, so
-- it is worth the same window `book_top` gets — and it has to be the *same*
-- window, because the race reads both sides and a pair whose publisher half has
-- expired is a state that reads as seen by one observation point only. Thirty
-- days, stated here rather than left to be inferred from an absent line.
--
-- A whole number of days, for the reason `002` gives: a TTL that does not align
-- to the partition is a treadmill of part rewrites rather than a partition drop.
ALTER TABLE recorder.venue_book_top
    MODIFY TTL toDateTime(recv_ts) + INTERVAL 30 DAY;

-- `recorder.venue_object` has no TTL, deliberately. It is one row per object,
-- which is `segment_coverage`'s cardinality, and it is the only thing that says
-- an object was derived at all — so expiring it turns a window nobody derived
-- into a window indistinguishable from one that held nothing.


-- 3. The book, collapsed.
--
-- `venue_book_top` is a `ReplacingMergeTree` and re-deriving an object is a
-- replace, so between a re-derivation and the merge that follows it one top of
-- book is in the table twice. `FINAL` applies the collapse at read time, for the
-- reason `006` gives in full: numbering over the duplicate counts one arrival as
-- two occurrences, and the surplus copy then pairs with nothing and is reported
-- as a state the other observation point missed — so a duplicate does not
-- inflate a count here, it manufactures evidence of loss.
--
-- Affordable for the reason `003` gives: it forces merge-on-read over the parts
-- a query reads, and this table is partitioned by day, so a predicate on
-- `recv_ts` prunes the partitions first and `FINAL` pays for what is left.
CREATE OR REPLACE VIEW recorder.venue_book_top_settled AS
SELECT *
FROM recorder.venue_book_top FINAL;


-- 4. The occurrence ordinal, per observation point.
--
-- Numbered rather than joined by proximity, which is `006`'s argument in full
-- and is not restated here: a book state repeats, `ASOF` has no notion of
-- consuming a match, and the lead times it produces are plausible, biased and
-- derived from counting one arrival several times.
--
-- `symbol_key` IS THE SYMBOL WITH ITS CASE FOLDED, AND THAT IS WHAT MAKES
-- `symbols_agree` A COLUMN RATHER THAN AN ASSUMPTION. The design's own cost
-- list says the comparison "depends on the venue's symbol and the published
-- symbol agreeing — which is a reference-data assertion, and worth being a
-- column rather than an assumption, the way `exponents_agree` is." A key on the
-- symbol exactly as each side spells it cannot carry that assertion at all:
-- a disagreement would produce no pair, which reads as a quiet feed on both
-- paths — the same failure keying on `state_key` would produce. So the ordinal
-- and the pairing key on the folded form, and the raw spellings are compared
-- below where a disagreement is a value somebody can see.
--
-- Case is the whole of the folding, deliberately. It is the divergence that
-- actually occurs between a venue's own naming and a published symbol, and it is
-- reversible: `symbols` on the pairing carries every spelling that went into a
-- pair, so nothing is lost. Anything more aggressive — stripping separators,
-- normalising a suffix — would start merging instruments, which is the one thing
-- an equivalence key must not do.
--
-- THERE IS NO ERA IN THIS PARTITION, AND NOTHING TO PUT THERE. `006` numbers
-- within an era because an `Instrument ID` is only unique within one. An era is
-- a publisher's `Reset Count` span and a venue has none, so the symbol is the
-- instrument identity and the numbering runs over the whole window. That is the
-- coarseness the design states: two instruments that shared a symbol across an
-- era boundary cannot be separated here.
--
-- AND NOTHING IS FILTERED OUT BEFORE THE WINDOW. `006` filters `from_anchor = 0`
-- there rather than after, so that an anchor row consumes no ordinal. Every row
-- here comes from an upstream message the venue produced, so there is no
-- equivalent row to exclude — and therefore no way for a late exclusion to leave
-- every later occurrence numbered one too high.
CREATE OR REPLACE VIEW recorder.venue_book_top_occurrence AS
SELECT
    observation,
    env,
    feed,
    connection,
    symbol,
    upper(trimBoth(symbol)) AS symbol_key,
    book_key,
    recv_ts,
    price_exp,
    qty_exp,
    upstream_sid,
    upstream_seq,
    object_key,
    row_number() OVER (
        PARTITION BY observation, feed, upper(trimBoth(symbol)), book_key
        ORDER BY recv_ts
    ) AS occurrence
FROM recorder.venue_book_top_settled;


-- 5. The race.
--
-- An aggregate over the ordinal and **not** a join between two named observation
-- points, which is what keeps an unpaired occurrence visible and what keeps this
-- generic: nothing below names an observation point, so two of them are a race
-- and three of them are the same query. `006` makes both arguments in full.
--
-- `uniqExact(observation)` RATHER THAN `count()`. `006` needs it for the era
-- boundary; here the reason is a re-derivation seen before its merge, which
-- `FINAL` above already collapses — so this is belt and braces, and it costs
-- nothing but says what the number means. Counting rows would report three
-- observations of a state two points saw.
--
-- `lead_ms` IS NULL AND NEVER ZERO WHEN ONE POINT SAW THE STATE. A zero would be
-- a lead time nobody measured, and it would enter every average over the column
-- as evidence that the two paths tied.
--
-- `exponents_agree` IS THE ASSERTION `book_key` MADE AND DID NOT HASH, exactly
-- as it is the assertion `state_key` made and did not hash in `006`. The key
-- covers the raw prices and quantities and leaves the exponents out, so a pair
-- whose exponents disagree is two different prices wearing one key — visible as
-- a column rather than silently averaged into the result.
--
-- `symbols_agree` IS THE SAME KIND OF ASSERTION ABOUT REFERENCE DATA. The key is
-- on the folded symbol, so a pair whose sides spell the instrument differently
-- is a pair — and this is what says so. `symbols` carries every spelling that
-- went into it, because "they disagree" without the strings is a finding nobody
-- can act on.
--
-- WHAT IS NOT HERE: a bound on |Δt|, and a verdict. The bound is a property of
-- the two paths being compared, so it is the caller's predicate over `lead_ms`.
-- And nothing here claims that a state the venue published and a publisher never
-- sent is the publisher's fault: that stays the loss derivation's question and
-- `007`'s. A venue-side observation adds one more thing that can be missing, not
-- an answer about whose fault it is.
CREATE OR REPLACE VIEW recorder.venue_book_top_race AS
SELECT
    feed,
    symbol_key,
    book_key,
    occurrence,
    uniqExact(observation)                 AS observations,
    arraySort(groupUniqArray(observation)) AS observed_by,
    argMin(observation, recv_ts)           AS first_observation,
    argMax(observation, recv_ts)           AS last_observation,
    min(recv_ts)                           AS first_recv_ts,
    max(recv_ts)                           AS last_recv_ts,
    if(uniqExact(observation) > 1,
       (toUnixTimestamp64Nano(max(recv_ts)) - toUnixTimestamp64Nano(min(recv_ts))) / 1e6,
       NULL)                               AS lead_ms,
    arraySort(groupUniqArray(symbol))      AS symbols,
    (uniqExact(symbol) = 1)                AS symbols_agree,
    (uniqExact(price_exp) = 1) AND (uniqExact(qty_exp) = 1) AS exponents_agree
FROM recorder.venue_book_top_occurrence
GROUP BY feed, symbol_key, book_key, occurrence;
