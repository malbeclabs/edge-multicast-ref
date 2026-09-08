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
-- Both write into these five tables. Without this column they are
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
-- WHAT THIS FILE IS FOR, GIVEN THAT 001 DECLARES THE COLUMN TOO. 001 is the
-- authoritative definition of these tables — its `CREATE TABLE` blocks are what
-- the row types are held against, column for column, in `tests/ddl.rs` — so the
-- column is declared there and a deployment created from scratch has it before
-- this file runs. This file is for the deployments that applied 001 when it did
-- not: their tables exist, `CREATE TABLE IF NOT EXISTS` will not alter them, and
-- an `ALTER` is the only thing that reaches them. On a fresh deployment every
-- statement below is a no-op, which is why it is safe to apply unconditionally
-- and in order.
--
-- `IF NOT EXISTS` throughout for that reason, as `CREATE TABLE IF NOT EXISTS`
-- makes 001 re-appliable.

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
