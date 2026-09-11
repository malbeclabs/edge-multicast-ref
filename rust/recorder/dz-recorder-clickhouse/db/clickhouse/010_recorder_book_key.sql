-- The book-only key on the publisher side, so the race in `009` has two sides.
--
-- `009` builds the venue half of a feed race and pairs occurrences on
-- `book_key` — a hash over the two sides of a top and over nothing else,
-- computable by anyone holding a top of book. It declares the seam a side
-- enters the race by, `recorder.feed_race_occurrence`, with the venue branch
-- alone, and says in its own header why the other branch is this file's: the
-- publisher side needs a `book_key` on `book_top`, and `009` adds no column to
-- that table. This file adds the column, and that branch with it.
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
-- So the column is forward-only: those rows read as zero, and the publisher
-- branch of the cross-observer race below excludes zero. Left in, every
-- pre-migration row of one feed and symbol would land in one equivalence class,
-- pair with nothing on the venue side, and be reported as a state the venue
-- never saw — which is not a count inflated but evidence of loss manufactured,
-- the failure `006` and `009` both name.
--
-- The exclusion is in the same `WHERE` as `from_anchor = 0` and only one of
-- them needs to be there. An anchored row shares its ordinal partition with the
-- rows that follow it, so excluding *that* one late would renumber them; a zero
-- key is its own partition and renumbers nothing whenever it goes. It is
-- written here because this is where the rows that must take no ordinal are
-- dropped, and because a window that never numbers one is cheaper than one that
-- numbers it and throws the numbers away.
--
-- That `WHERE` is not the whole of the list, and the section below names the
-- publisher-side rows it cannot reach: the ones whose top did not move.
--
-- The cost of the exclusion is one book in 2^64 per observation point: a fold
-- that really came out zero is dropped, because nothing distinguishes it from a
-- column nobody wrote. A forward-only column is normal. Stating that the race
-- covers rows written after this file, rather than implying it covers the
-- window `book_top` keeps, is the part that is not optional.
--
-- THE EXCLUSION IS ON THIS BRANCH AND NOT ON THE VENUE'S, and that is an
-- asymmetry to write down rather than a symmetry to restore. `009` declares
-- `venue_book_top` with `book_key` in its `CREATE TABLE`, so no row on that
-- side was written before the column existed and a zero there is never
-- ambiguous: it is a fold that really came out zero. The filter here exists to
-- resolve an ambiguity that side does not have, and a `book_key != 0` on the
-- venue branch would tell a reader it does — which is the opposite of true,
-- and worse documentation than the asymmetry.
--
-- What it leaves is one book in 2^64 per venue observation point that this side
-- drops and that side keeps, so it reads as a state only the venue saw. That
-- lands in a reading which already tolerates it: a single `observations = 1` is
-- a question and not yet loss, in the words of this file's own section on what
-- the race says honestly. Filtering it out on the venue side instead would take
-- a real observation out of a view that says it carries every one, to make two
-- `WHERE` clauses look alike.
--
--
-- APPLY THIS FILE BEFORE THE BINARY THAT WRITES THE COLUMN
--
-- The order matters in both directions and the section above states only one of
-- them. A row written before the `ALTER` reads as zero, and the publisher
-- branch below excludes zero. The other direction is a binary that writes
-- `book_key` against a `book_top` this `ALTER` has not reached, and at a
-- server's own defaults nothing about it fails.
--
-- The sink posts `INSERT INTO recorder.book_top FORMAT JSONEachRow` and the
-- rows name their own fields, so a column is matched by name at the server.
-- `input_format_skip_unknown_fields` defaults to 1 — checked against the 24.8
-- the suite pins, where such an insert returns 200 and the field is discarded.
-- At that default the insert succeeds, the batch is acknowledged, and every
-- publisher-side row that binary writes lands with `book_key = 0`; the
-- exclusion below drops all of them and the cross-observer race reads as a
-- venue-only race, with no error, no refused batch, no metric, and a new
-- observation point that looks like one nobody configured.
--
-- SO THE LOADER DOES NOT LEAVE IT AT THE DEFAULT.
-- `ClickHouseConfig::insert_url` carries `input_format_skip_unknown_fields=0`
-- on every insert it posts, which makes that direction a refused batch: the
-- server answers `Code: 117 ... Unknown field found while parsing JSONEachRow
-- format: book_key` as a 400, a 400 is not worth retrying, the refusal names
-- every object in the batch, and those objects stay unloaded until this file
-- has been applied — after which they load by themselves. The price of the
-- wrong order is a stalled load that names the column, which is the trade this
-- file argues for in every other section: an error somebody reads beats a
-- silence that looks like a clean feed.
--
-- IT REFUSES THAT DIRECTION AND NOT A ROLLBACK. A binary older than the schema
-- sends no unknown field — it omits a known one — and an omitted field is
-- `input_format_defaults_for_omitted_fields`, a different setting the loader
-- does not touch. Measured against the same 24.8: a row omitting `book_key` is
-- accepted and the column reads as the zero the exclusion drops, with that
-- other setting at its default and at 0 alike. So a binary rolled back is still
-- a binary that loads, and both of those readings are asserted against a real
-- server in the container suite rather than argued here.
--
-- The schema is applied before the binary is rolled all the same, and the rule
-- is not weaker for being enforced: a load that stops is a feed with a hole in
-- it until somebody applies the file. That is the rule the feed runbook states
-- for rolling subscribers before publishers, and it is stated there for this
-- one too.
--
--
-- WHAT THE ORDINAL DOES NOT SEPARATE: A PUBLISHER-SIDE ROW THAT MOVED NO TOP
--
-- Ordinal *n* against ordinal *n* holds only while both sides number the same
-- occurrences of a book state, and they do not. **A publisher-side row is
-- written when the top moved or when the certainty of it moved; a venue-side
-- row only when the top moved.** Three kinds of publisher-side row therefore
-- carry `from_anchor = 0`, a written `book_key`, and the same top the row
-- before them carried:
--
--   * A GAP. `Book::observe_sequence` pushes a change for every established
--     book on the channel when a hole in the `mktdata` sequence is detected,
--     each carrying that book's current top: the gap belongs to the channel
--     instance, and nobody can say which instrument's deltas were in it.
--   * THE RESTORE. A `Quote` that puts certainty back restates the top the book
--     already had, and the row exists because a change is a change in the
--     visible top **or** in the certainty of it.
--   * AN UNANCHORED BOOK. `Book::level` on a book with no anchor writes one row
--     with no prices, because absence cannot be told from a silent feed.
--
-- The venue derivation has neither concept. It returns on an unchanged top and
-- on an empty book nothing was applied to, so it writes no counterpart to any
-- of the three.
--
-- One sequence hole is therefore two publisher-side occurrences of book state K
-- where the venue has one. Ordinal 1 still pairs. Ordinal 2 is the gap's row
-- alone, reported as a state the venue never saw — and when the venue next
-- reaches K that occurrence is *its* ordinal 2, so it pairs with the gap row,
-- and `lead_ms` is measured between two arrivals of two different states. It
-- comes out a plausible wrong number rather than a visible mistake, which is
-- the failure this file's cost section names, reached from the other end.
--
-- WHY IT IS STATED HERE AND NOT FILTERED HERE. Nothing on the row says the top
-- moved. The restore row is indistinguishable in SQL from a genuine repeat of a
-- state — identical two sides, identical key, `book_certain` back to 1 — and
-- what separates them is a fact the derivation had and no column carries.
-- Comparing a row with its predecessor in SQL would reach the first two kinds
-- and neither of the two exposures below, so it would close part of the drift
-- and leave this file claiming a property it still did not have.
--
-- AND IT IS `006`'S EXPOSURE TOO, which is the argument for fixing it in one
-- place rather than in this branch. `state_key` folds the same top, so a gap at
-- one recorder gives that observation point an extra occurrence of a state the
-- other saw once, and the publisher-side race has paired ordinals across that
-- since it was written. The fix belongs where both views would read it — a
-- derivation that states whether the top moved, or an occurrence grain of its
-- own — and not in one branch of one union, where it would leave two readings
-- of one table counting the occurrences of one row set differently.
--
-- TWO MORE THINGS THE TWO SIDES' ORDINALS DO NOT AGREE ON, named so that the
-- list is the whole list. An anchored row takes no ordinal here, for `006`'s
-- reason, and the venue side has no anchors at all — so a state the publisher
-- reached by applying a snapshot is an occurrence the venue counted and this
-- side did not. And two redundant paths recorded at one observation point are
-- two books in the deriver and two rows at two receive stamps, where the venue
-- holds one book and writes one row.
--
-- SO WHAT THIS RACE SAYS HONESTLY. A pair with `observations = 2` inside a
-- caller's bound on |Δt| is two observations of one book state and a lead time
-- between them. A run of `observations = 1` says the two sides' ordinals did
-- not line up, in the same shape whether the cause is a state one side missed
-- or a row the other side numbered that was never a move — so it is a question
-- and not yet loss, and it stays that until the derivation says which
-- publisher-side rows were moves.
--
--
-- WHY `005` DECLARES THE COLUMN TOO
--
-- For the reason `008` gives at length: `005`'s `CREATE TABLE` is the
-- authoritative definition of `book_top` — it is what the row type is held
-- against, column for column, in `tests/ddl.rs` — so a deployment created from
-- scratch has this column before this file runs, and **the `ALTER` below** is
-- then a no-op. That `ALTER` is for the deployments that applied `005` when the
-- column did not exist: their table exists, `CREATE TABLE IF NOT EXISTS` will
-- not alter it, and an `ALTER` is the only thing that reaches it. `IF NOT
-- EXISTS` on it for that reason.
--
-- THE REST OF THIS FILE IS NOT OPTIONAL ON ANY DEPLOYMENT, and reading "then a
-- no-op" across the whole of it is how a deployment created from scratch ends
-- up with a one-sided race. Only the `ALTER` is one.
-- `publisher_book_top_occurrence` is declared here and nowhere else;
-- `feed_race_occurrence` arrives from `009` with the venue branch alone and is
-- replaced here by both; `book_top_settled` is re-stated for the reason that
-- statement gives about a view's `SELECT *`; and the `DROP` at the end is the
-- only other statement a fresh deployment has nothing to do. A deployment that
-- applied `009` and skipped this file has a race that pairs the venue against
-- itself.
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
-- THE COARSENESS THAT HAS A COST, AND IT IS THE CHANNEL. A symbol is
-- `char[64]` of venue-chosen text that is unique within a channel at an instant
-- and not across eras, and the deriver's own reference data says what keying on
-- it does: it silently merges two instruments. One observation point records
-- every channel of a feed into one `book_top`, so during a re-shard overlap,
-- where one symbol is published on two channels of one feed, both channels'
-- occurrences of one book state are numbered in a single sequence — 2n
-- publisher ordinals against the venue's n.
--
-- Numbering per channel would not repair that and would break the pairing
-- outright: a venue side cannot name a channel, so two channels each numbering
-- from 1 give one symbol two ordinal-1 rows and the venue's one pairs with
-- whichever of them it is grouped with. The coarseness is the price of a key
-- both sides can compute, as the era boundary is on the venue side of `009`,
-- and it is stated rather than left to be found.
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
-- THE WINDOW'S ORDER IS TOTAL, AND IT IS `recv_ts` THAT IS NOT. `009` makes
-- this argument for the venue branch and it holds here unchanged. Equal receive
-- stamps are ordinary on this side too: one datagram carries many messages and
-- every row it produced takes that datagram's stamp, which is what
-- `message_index` exists for, and one sequence hole writes a row for every
-- established book on the channel at one stamp. Ordered on the stamp alone,
-- `row_number()` is free to number two rows at one stamp either way — and it
-- may answer differently after a merge, so the same rows numbered on two runs
-- pair differently and neither `observations` nor `lead_ms` is reproducible.
--
-- The tie is broken by what the rows already carry, and these five columns are
-- total because they are `005`'s own sort key: `(channel_id, instrument_id,
-- recv_ts, sequence_number, message_index, observation)` is what the replacing
-- engine collapses on, so beneath `FINAL` no two rows share it — and the
-- partition above already fixes `observation`. `source_addr` and `dst_port` are
-- not in that key and are not wanted here: two paths' rows of one message at
-- one stamp are one row beneath `FINAL` rather than a tie this order cannot
-- break.
--
-- THE CHANNEL COMES BEFORE THE SEQUENCE, because the two count different
-- things: a `Sequence Number` belongs to one channel instance and
-- `message_index` restarts in each datagram, so comparing either across two
-- channels compares readings from two counters. Being ordered by a column is
-- not being grouped on one — the paragraphs above say why none of these is in
-- the partition, and a tie-break costs nothing across observers because no
-- other side has to agree about it.
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
        ORDER BY recv_ts, channel_id, instrument_id, sequence_number, message_index
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


-- The name the union above made wrong, dropped where a deployment may hold it.
--
-- `009` declares the race as `recorder.feed_race`, which is what it is once
-- both branches of the seam reach it: the pairing is an aggregate over the
-- ordinal and names no observation point, so it is one query for one side or
-- for ten and it takes the seam's own name. Before this file existed that view
-- was `recorder.venue_book_top_race`, and the name was true of it — there was
-- one branch and it was the venue's. It is not true of a view whose rows may be
-- either side's: `observations = 1` there is as likely publisher-only as
-- venue-only, and `observed_by` names publisher observation points, so anyone
-- filtering it as the venue's own race gets the opposite of what they expect.
--
-- THE RENAME IS IN `009` AND NOT HERE, so that the query keeps one definition.
-- A second copy of that aggregate under a second name would be two definitions
-- to keep true, and the one that drifts is the one nobody reads. What is left
-- for this file is the old name on a deployment that applied `009` before the
-- rename: the view reads `feed_race_occurrence`, so the branch added above
-- makes it the two-sided race under a name that says venue. A view holds no
-- rows, so dropping it loses nothing that a re-application of `009` does not
-- put back under the name it now has. `IF EXISTS` because on a deployment that
-- never had it there is nothing there.
DROP VIEW IF EXISTS recorder.venue_book_top_race;
