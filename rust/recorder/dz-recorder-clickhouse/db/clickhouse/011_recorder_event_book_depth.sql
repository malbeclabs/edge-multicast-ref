-- How deep the book went, and whether the message beside the number moved it.
--
-- Two columns on `recorder.event` for a deployment that applied `005` before
-- they were in it. `005` declares both, so every statement here is a no-op on a
-- fresh deployment — the shape `008` and `010` have, and for the same reason.
--
-- BOTH FILES, NOT ONE. `every_column_has_a_field_and_every_field_has_a_column`
-- reads the `CREATE TABLE` and not the migration list, so a column that exists
-- only as an additive `ALTER` fails it — which is that test noticing a fresh
-- deployment would not have the column at all.
--
-- WHAT A ROW WRITTEN BEFORE THIS FILE READS AS. `status_after` reads as the
-- empty string, which is the type default and also what the deriver writes for a
-- book it can say nothing about; `book_levels_after` reads as 0, and a 0 under an
-- empty status states nothing rather than an empty book. So no `DEFAULT` clause
-- is needed and none is given.
--
-- Adding a column to a MergeTree is a metadata change: existing parts are not
-- rewritten. Not a mutation, which matters most here — this is the largest of
-- the market data tables.
--
-- IN NO `ORDER BY`, NO TTL CHANGE, NO GRANT CHANGE (`004` grants at table
-- level), AND NO VIEW RE-STATED, because nothing selects from `recorder.event`.
--
-- THE SCHEMA IS APPLIED BEFORE THE BINARY IS ROLLED, as `010`'s header and the
-- feed runbook state: `input_format_skip_unknown_fields` is 0, so a loader
-- writing these columns against a table without them is refused with a 400 that
-- names the objects, and they stay unloaded until this file is applied.

ALTER TABLE recorder.event
    ADD COLUMN IF NOT EXISTS book_levels_after UInt32
    AFTER depth_bound;

ALTER TABLE recorder.event
    ADD COLUMN IF NOT EXISTS status_after LowCardinality(String)
    AFTER book_levels_after;
