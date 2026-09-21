-- Where a row's datagrams went: kept and verified, or derived and dropped.
--
-- A recorder can derive rows two ways. Archive mode writes hashed, manifested
-- objects and a loader derives from them, having checked each object's sha256
-- against its manifest before a single row is derived — so the bytes are still
-- there to derive again, under a rule that does not exist yet or after a bug in
-- the derivation is fixed. Inline mode derives from datagrams as they arrive and
-- keeps none of them: nothing verified those bytes and nothing can re-derive
-- them.
--
-- Both write into these eight tables. Without this column they are
-- indistinguishable, and a query cannot tell a finding drawn from verified
-- evidence from one drawn from a window nobody can go back to.
--
-- WHY A COLUMN AND NOT AN INFERENCE. An inline row's `object_sha256` is the
-- empty string, so in principle a reader could test for that. That is exactly
-- why this column exists instead: an empty digest field is a trap, a query that
-- reads it as *verified* is wrong in the direction that matters, and nothing
-- about the name `object_sha256` warns anybody. Provenance is not something a
-- reader should have to reconstruct from the absence of a value.
--
-- WHY `DEFAULT 'archive'`. Every row written before this migration came from a
-- stored object — inline mode did not exist — so the default is the truth about
-- the existing data rather than a convenience. It also means this file can be
-- applied to a live deployment before the binary that writes the column is
-- rolled, which is the order a deploy wants: the schema leads.
--
-- THE MARKET DATA TABLES TOO. `event`, `instrument` and `book_top` are derived
-- from the same datagrams by the same pass, so a reader joining an event row to
-- the datagram row it came from must find the same answer on both. A provenance
-- that covered only the five original tables would be a column that stops being
-- true exactly where the join gets interesting.
--
-- WHY THIS IS CHEAP ON A HUNDRED MILLION ROWS A DAY. Adding a column with a
-- DEFAULT to a MergeTree is a metadata change. Existing parts are not rewritten;
-- a read of a part written before this migration materialises the default. So
-- this is not a mutation and does not put the largest table through a merge
-- treadmill — which the retention file's own history is a warning about.
--
-- WHY IT IS IN NO `ORDER BY`. Deduplication must not change. A datagram
-- recorded once is one row, and provenance in a sort key would make two modes'
-- views of the same datagram two rows that never collapse. Every sort key in
-- 001 is left exactly as it is, deliberately, and a later file that adds this
-- column to one of them has broken the deduplication these tables rest on.
--
-- NO GRANT CHANGE. 004 grants `INSERT ON recorder.<table>` at table level, not
-- per column, so the loader account reaches this column with no further grant.
-- A per-column grant would have needed updating here, which is one reason it is
-- not written that way.
--
-- WHAT THIS FILE IS FOR, GIVEN THAT 001 AND 005 DECLARE THE COLUMN TOO. Those
-- two are the authoritative definitions of these tables — their `CREATE TABLE`
-- blocks are what the row types are held against, column for column, in
-- `tests/ddl.rs` — so the column is declared there and a deployment created from
-- scratch has it before this file runs. The `ALTER`s are for the deployments
-- that applied 001 when it did not: their tables exist, `CREATE TABLE IF NOT
-- EXISTS` will not alter them, and an `ALTER` is the only thing that reaches
-- them. On a fresh deployment every `ALTER` below is a no-op, which is why they
-- are safe to apply unconditionally and in order, and `IF NOT EXISTS` on every
-- one of them for that reason, as `CREATE TABLE IF NOT EXISTS` makes 001
-- re-appliable.
--
-- THE TWO `CREATE OR REPLACE VIEW` STATEMENTS AT THE END ARE NOT IN THAT CLASS,
-- and reading "then a no-op" across the whole of this file is how a deployment
-- ends up holding a view under one name and another deployment holding a
-- different one. Those two run on every deployment, fresh and upgraded, and they
-- carry no `IF NOT EXISTS`: a view is replaced rather than skipped, which is
-- exactly what makes the one statement correct on both. The block above them
-- states why they are here.

ALTER TABLE recorder.datagram
    ADD COLUMN IF NOT EXISTS derivation LowCardinality(String) DEFAULT 'archive'
    AFTER object_sha256;

ALTER TABLE recorder.era
    ADD COLUMN IF NOT EXISTS derivation LowCardinality(String) DEFAULT 'archive'
    AFTER object_sha256;

-- Before `build_version` here, so the provenance of the rows sits beside the
-- provenance of the recorder that produced them rather than after it.
ALTER TABLE recorder.segment_coverage
    ADD COLUMN IF NOT EXISTS derivation LowCardinality(String) DEFAULT 'archive'
    AFTER object_sha256;

ALTER TABLE recorder.sequence_gap
    ADD COLUMN IF NOT EXISTS derivation LowCardinality(String) DEFAULT 'archive'
    AFTER object_key;

ALTER TABLE recorder.conformance_finding
    ADD COLUMN IF NOT EXISTS derivation LowCardinality(String) DEFAULT 'archive'
    AFTER object_key;

-- The market data grains, from 005. Same rule, same reason: an event row and the
-- datagram row it was derived from must not disagree about whether anybody kept
-- the bytes.
ALTER TABLE recorder.event
    ADD COLUMN IF NOT EXISTS derivation LowCardinality(String) DEFAULT 'archive'
    AFTER object_sha256;

ALTER TABLE recorder.instrument
    ADD COLUMN IF NOT EXISTS derivation LowCardinality(String) DEFAULT 'archive'
    AFTER object_key;

ALTER TABLE recorder.book_top
    ADD COLUMN IF NOT EXISTS derivation LowCardinality(String) DEFAULT 'archive'
    AFTER object_key;


-- THE TWO `SELECT *` VIEWS OVER THE TABLES ABOVE, RE-STATED OVER THEM AS THEY
-- NOW ARE.
--
-- **A view's `SELECT *` is expanded when the view is created, not when it is
-- read.** `003` declares `era_opening` as `SELECT * FROM recorder.era FINAL`
-- and `datagram_in_era` as a `d.*` over `recorder.datagram` — a qualified star
-- freezes exactly as a bare one does — and a deployment being upgraded applies
-- these files in order, so `003` expands both stars over tables that do not
-- carry `derivation` and the `ALTER`s above run afterwards. Without the two
-- statements below, one view name stands for two column lists: no `derivation`
-- on every deployment upgraded in file order, `derivation` on every deployment
-- created since, and which one a deployment holds settled by nothing but how
-- long it has been running.
--
-- Nothing fails while no query reads `derivation` through either view, and
-- `006` and `007` both take named columns out of them. The first query, panel
-- or migration that reaches `era_opening.derivation` or
-- `datagram_in_era.derivation` fails with `UNKNOWN_IDENTIFIER` on the
-- deployments that have been running longest and passes everywhere it was
-- written and tested — the failure landing on the oldest and least disposable
-- deployments, and at query time rather than at deploy time. A later
-- `CREATE OR REPLACE VIEW` that itself read the column through one of these
-- views would fail to create, which is the same defect turned into a failed
-- schema apply. `derivation` is provenance, so the query most likely to reach
-- it is a query about whether a finding may be trusted.
--
-- A view holds no rows, so re-creating it loses nothing. This is a metadata
-- change on both deployments — a no-op on a fresh one, the repair on an
-- upgraded one — and the text is `003`'s text unchanged, so that one view name
-- means one view on every deployment. `003` is still where the argument for
-- each of them lives: the partitioned collapse under `era_opening`, and the
-- `ASOF LEFT JOIN` on the anchor that resolves a datagram to its era. This is
-- the hazard `010` states at length above its own re-statement of
-- `book_top_settled`, reached earlier and closed here.
--
-- TWO, AND THE OTHER SIX TABLES ABOVE ARE NOT AN OVERSIGHT. Three of the eight
-- carry a star view over them. `recorder.era` and `recorder.datagram` are
-- below. `recorder.book_top`'s is `006`'s `book_top_settled`, and `010`
-- re-states that view after its own `ALTER` — so the freeze this file opens on
-- it is closed by the time the set has been applied, and a second re-statement
-- here would duplicate a repair rather than make one. `segment_coverage`,
-- `sequence_gap`, `conformance_finding`, `event` and `instrument` have no star
-- view over them anywhere in the set, so for those five there is nothing to
-- re-state.
--
-- WHY `datagram_in_era` KEEPS ITS `d.*`. A star over the largest table in the
-- schema is what this whole class of defect rests on, and naming `datagram`'s
-- columns here would stop the class recurring on this one view. It is left a
-- star deliberately. A named list buys that safety by requiring a second edit
-- before a column added to `datagram` reaches the view a panel reads, and
-- nothing fails when that edit is forgotten — the column is simply absent, for
-- a reader who cannot tell the omission from a decision. The star has the
-- opposite failure: it is loud, it is caught in the file that moves the column
-- list, and the rule that such a file re-states every star over that table is
-- asserted over these files in `tests/ddl.rs`. Between a safety that depends on
-- nobody forgetting and a hazard a test refuses to let past, this file takes
-- the second.

CREATE OR REPLACE VIEW recorder.era_opening AS
SELECT *
FROM recorder.era FINAL
WHERE continuation = 0;


CREATE OR REPLACE VIEW recorder.datagram_in_era AS
SELECT
    d.*,
    e.anchor_ts      AS era_anchor_ts,
    e.anchor_seq     AS era_anchor_seq,
    e.reset_count    AS era_reset_count,
    e.anchor_certain AS anchor_certain
FROM recorder.datagram AS d
ASOF LEFT JOIN recorder.era_opening AS e
    ON  d.site        = e.site
    AND d.recorder    = e.recorder
    AND d.source_addr = e.source_addr
    AND d.channel_id  = e.channel_id
    AND d.dst_port    = e.dst_port
    AND e.anchor_ts  <= d.recv_ts;
