-- The book-only key on the publisher side, so the race in `009` has two sides.
--
-- `009` builds the venue half of a feed race and pairs occurrences on
-- `book_key` — a hash over the two sides of a top and over nothing else,
-- computable by anyone holding a top of book. It then says, in its own header,
-- that the publisher side does not feed that pairing because `book_top` stores
-- `state_key` and has no `book_key` column. This file adds the column, and the
-- pairing's other branch with it.
--
--
-- WHY NOT `state_key`, WHICH `book_top` ALREADY HAS
--
-- `state_key(channel_id, instrument_id, top)` eats both identifiers into the
-- FNV-1a accumulator **before** it eats a price, so the value cannot be turned
-- back into a hash of the sides alone: the chain is one-way, and there is no
-- arithmetic in SQL or anywhere else that recovers one key from the other.
--
-- That is not a defect in `state_key`. It is what makes it the key two recorders
-- of one multicast feed pair on — both read the same `Channel ID` and the same
-- `Instrument ID` off the same datagrams — and what makes it useless to an
-- observer that never saw a datagram, which can name neither. The channel is the
-- operator's mapping from a shard to a channel and the identifier is minted by
-- the publisher's reference-data registry. Keyed on `state_key`, a cross-observer
-- race returns zero pairs and reads as each side missing every state the other
-- saw — the failure that function's own comment names, where a key that stops
-- matching "looks like a quiet feed".
--
-- So the two keys sit side by side on the row and answer two questions. Nothing
-- is renamed and no stored value moves.
--
--
-- WHY A COLUMN IS ADMISSIBLE HERE, GIVEN WHAT THE DESIGN REFUSED
--
-- The venue-observation design refuses "no new column on `event` or `book_top`,
-- and no existing one made nullable", and the reason it gives is about a
-- nullable provenance column: one weakens every publisher-side row to
-- accommodate a row of a different kind.
--
-- `book_key` is neither. It is not nullable — every publisher-side row states
-- it. And it is not provenance: it says nothing about where the row came from,
-- being a second hash of data the row already carries, the two sides of the top
-- it already stores. So the refusal's reason does not reach it, and the design
-- says so in an amendment rather than being quietly re-read.
--
--
-- WHAT THIS COSTS, PLAINLY
--
-- **A row written before this migration has no `book_key`, and there is no
-- honest DEFAULT for it.** `derivation` in `008` could default to `archive`
-- because that was the truth about every row already written. There is no
-- equivalent here: the value is a hash of a book, nobody computed it for those
-- rows, and the two sides are on the row but the fold is not something SQL may
-- do — a second implementation of it would be a second key, and two hashes of
-- one book state pair with nothing.
--
-- So the column is forward-only: those rows read as zero, and the
-- cross-observer race below excludes zero. Left in, every pre-migration row of
-- one feed and symbol would land in one equivalence class, pair with nothing on
-- the venue side, and be reported as a state the venue never saw — which is not
-- a count inflated but evidence of loss manufactured, the failure `006` and
-- `009` both name.
--
-- The exclusion is in the same `WHERE` as `from_anchor = 0` and only one of
-- them needs to be there. An anchored row shares its ordinal partition with the
-- rows that follow it, so excluding *that* one late would renumber them; a zero
-- key is its own partition and renumbers nothing whenever it goes. It is
-- written here because this is where a row that is not an occurrence of a book
-- is dropped, and because a window that never numbers it is cheaper than one
-- that numbers it and throws the numbers away.
--
-- The cost of the exclusion is one book in 2^64 per observation point: a fold
-- that really came out zero is dropped, because nothing distinguishes it from a
-- column nobody wrote. A forward-only column is normal. Stating that the race
-- covers rows written after this file, rather than implying it covers the
-- window `book_top` keeps, is the part that is not optional.
--
--
-- WHY `005` DECLARES THE COLUMN TOO
--
-- For the reason `008` gives at length: `005`'s `CREATE TABLE` is the
-- authoritative definition of `book_top` — it is what the row type is held
-- against, column for column, in `tests/ddl.rs` — so a deployment created from
-- scratch has this column before this file runs, and every statement here is
-- then a no-op. This file is for the deployments that applied `005` when it did
-- not: their table exists, `CREATE TABLE IF NOT EXISTS` will not alter it, and
-- an `ALTER` is the only thing that reaches it. `IF NOT EXISTS` throughout for
-- that reason.
--
-- Cheap for the reason `008` gives as well: adding a column to a MergeTree is a
-- metadata change, existing parts are not rewritten, and a read of a part
-- written before this file materialises the type's default. Not a mutation, and
-- no merge treadmill on the largest of the market data tables.
--
-- IN NO `ORDER BY`. Deduplication must not change. `book_top`'s sort key is left
-- exactly as `005` states it: a key that carried this column would make one
-- change in a top two rows whenever a re-derivation computed the fold
-- differently, which is the one thing a replacing engine must not be asked to
-- tolerate.
--
-- NO GRANT CHANGE. `004` grants `INSERT ON recorder.book_top` at table level and
-- not per column, so the loader account reaches this column with no further
-- grant.

ALTER TABLE recorder.book_top
    ADD COLUMN IF NOT EXISTS book_key UInt64
    AFTER state_key;


-- The collapse, re-stated over the table as it now is.
--
-- **A view's `SELECT *` is expanded when the view is created, not when it is
-- read.** `006` declares `book_top_settled` as `SELECT * FROM recorder.book_top
-- FINAL`, and on a deployment that is being upgraded the files are applied in
-- order — so `006` re-creates that view over a `book_top` that does not carry
-- `book_key` yet, and the `ALTER` above runs afterwards. Without this statement
-- the collapsed view would be missing the column until somebody applied the
-- whole set a second time, and the branch below — which reads `book_key` from it
-- — would fail to create at all.
--
-- Re-stated rather than worked around, so that the one collapse view is the same
-- view on every deployment, fresh or upgraded. `006` is still where the argument
-- for it lives: numbering over an unmerged re-load counts one arrival as two
-- occurrences, and the surplus copy then pairs with nothing, so a duplicate here
-- does not inflate a count — it manufactures evidence of loss.
CREATE OR REPLACE VIEW recorder.book_top_settled AS
SELECT *
FROM recorder.book_top FINAL;


-- The publisher side's occurrences, numbered on the key both sides can compute.
--
-- The counterpart of `009`'s `venue_book_top_occurrence`, and deliberately not a
-- change to `006`'s `book_top_occurrence`: that one numbers within an era, on
-- `(channel_id, instrument_id, state_key)`, and it is what two recorders of one
-- multicast feed pair on. Nothing about it moves. This is a second reading of
-- the same rows for a different question, and the two coexist for the reason the
-- two keys do.
--
-- THE ARGUMENTS THIS VIEW CITES RATHER THAN RESTATES. Every one is made in full
-- in `006`'s header, and `009` names the same list:
--
--   * WHY THIS IS NOT AN `ASOF JOIN`. A book state repeats, `ASOF` has no notion
--     of consuming a match, and the lead times it produces are plausible,
--     biased, and derived from counting one arrival several times.
--   * `from_anchor = 0` FILTERED BEFORE THE WINDOW. A snapshot anchors a book
--     and never times one, and `WHERE` runs before a window — so an anchor row
--     consumes no ordinal. Excluding it after the numbering would leave every
--     later occurrence at that observation point numbered one too high.
--   * `FINAL` BENEATH THE NUMBERING, which is `book_top_settled` above.
--
-- WHAT IT DOES NOT CARRY, AND WHY THE OMISSIONS ARE THE POINT. No `channel_id`,
-- no `instrument_id`, no `sequence_number`, no era: those are the publisher-side
-- identities a venue side cannot name, and a race that grouped on any of them
-- would find nothing across observers while both paths read as clean. What is
-- left is what both sides hold — the feed, the symbol, the book, the arrival.
--
-- `book_certain` IS NOT CARRIED EITHER, AND THAT IS NOT AN OVERSIGHT. `006`
-- carries it and takes `min` over a pair, because there certainty means one
-- thing: a gap in the publisher's own sequence space. `009` gives its own reason
-- for having no such column at all — what makes a venue-side book untrustworthy
-- is the venue's own resynchronisation, a different thing measured a different
-- way — and one column carrying both meanings, minimum-aggregated over a pair,
-- mixes them silently. So the cross-observer race does not offer the verdict,
-- and a caller that wants only believed publisher-side states asks `006`.
--
-- `book_key != 0` IS THE FORWARD-ONLY EXCLUSION, argued in this file's header:
-- a row written before the column existed carries a hash of no book, and left
-- in it would pair with nothing and be reported as a state the venue never saw.
CREATE OR REPLACE VIEW recorder.publisher_book_top_occurrence AS
SELECT
    observation,
    env,
    feed,
    symbol,
    -- The symbol with its case folded, which is `009`'s key and its argument:
    -- a key on the symbol exactly as each side spells it produces no pair where
    -- the spellings differ, and no pair reads as a quiet feed on both paths.
    -- The raw spelling travels beside it so the pairing can report the
    -- disagreement as a value somebody can see.
    upper(trimBoth(symbol)) AS symbol_key,
    book_key,
    recv_ts,
    price_exp,
    qty_exp,
    row_number() OVER (
        PARTITION BY observation, feed, upper(trimBoth(symbol)), book_key
        ORDER BY recv_ts
    ) AS occurrence
FROM recorder.book_top_settled
WHERE from_anchor = 0 AND book_key != 0;


-- Both sides of the race, which is the branch `009` was written to admit.
--
-- `009` declares this view with the venue branch alone and says why: nothing in
-- the pairing above it names an observation point, so a side enters the race by
-- contributing rows rather than by the pairing learning its name. This is that
-- contribution, and it is one branch of a union rather than a rewrite of
-- anything.
--
-- `UNION ALL` AND NOT `UNION`. A distinct-ing union would collapse two
-- observation points that saw one book at one instant into a single row, which
-- is precisely the pair the race exists to report. There is nothing to
-- de-duplicate here either: the collapse each branch needs is applied beneath
-- it, on its own table.
--
-- THE COLUMNS ARE LISTED ON BOTH BRANCHES rather than taken with `*`. A union
-- resolves its branches by position, so a column added to one side's occurrence
-- view would silently line up against a different column on the other — and
-- both branches carry `Int8` exponents and `UInt64` keys, so a transposition
-- would type-check and pair the wrong things.
--
-- `feed` IS AN ASSERTION ABOUT CONFIGURATION, the way the symbol is one about
-- reference data. Two sides that name one feed differently produce no pair, and
-- unlike the symbol there is no folding that could rescue it: a feed name is a
-- deployment's own token. It is grouped on rather than compared because a race
-- across two feeds is not a race.
CREATE OR REPLACE VIEW recorder.feed_race_occurrence AS
SELECT
    observation,
    env,
    feed,
    symbol,
    symbol_key,
    book_key,
    recv_ts,
    price_exp,
    qty_exp,
    occurrence
FROM recorder.venue_book_top_occurrence
UNION ALL
SELECT
    observation,
    env,
    feed,
    symbol,
    symbol_key,
    book_key,
    recv_ts,
    price_exp,
    qty_exp,
    occurrence
FROM recorder.publisher_book_top_occurrence;
