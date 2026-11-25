-- How bad a finding is, and which group it is on.
--
-- Two columns on `recorder.conformance_finding` for a deployment that applied
-- `001` before they were in it. `001` declares both, so every statement here is
-- a no-op on a fresh deployment — the shape `008`, `010` and `011` have, and for
-- the same reason: `every_column_has_a_field_and_every_field_has_a_column` reads
-- the `CREATE TABLE` and not the migration list.
--
-- `severity` IS THE RULE SET'S OWN. The rule set states `must`, `should` or
-- `info` on every entry of its report, and the runner used to parse the report
-- and drop it, so a reader that wanted to rank a violation kept its own copy of
-- the catalog's severities — a copy that goes stale the first time a rule
-- changes. The runner now writes the severity of the entry the row speaks for,
-- lower-cased as the report spells it. In no sort key: a rule's severity is a
-- property of the rule set version, which is already in the key.
--
-- `group_addr` IS WHAT A SUBSCRIBER JOINS. A reader keys a feed on its
-- multicast group, and the row stated a feed, a port role and a destination
-- port but not the group, so the reader rebuilt a mapping the manifest already
-- holds. The runner now writes the group the manifest joined for the row's port
-- role and destination port, the pair `segment_coverage.roles_joined` records.
-- In no sort key.
--
-- WHAT A ROW WRITTEN BEFORE THIS FILE READS AS. `severity` reads as '', which a
-- reader takes as unknown and never as `info`: an old violation of a `must`
-- rule is not a minor one. `group_addr` reads as `0.0.0.0`, which is also what
-- the runner writes where the manifest names no one group for the pair, and a
-- reader must not resolve it by guessing from the feed's name. Both defaults
-- are stated rather than left to the type, so they are the documented meaning
-- of an absent value and not an accident of it.
--
-- Adding a column with a DEFAULT to a MergeTree is a metadata change: existing
-- parts are not rewritten, and a read of one materialises the default. No TTL
-- change (the table has none, and `002` says why), no grant change (`004`
-- grants at table level), and no view re-stated, because no star view reads
-- `recorder.conformance_finding`.
--
-- APPLY THIS FILE BEFORE THE BINARY THAT WRITES THE COLUMNS. The insert URL
-- sets `input_format_skip_unknown_fields` to 0, so a loader that writes
-- `severity` against a table without it is refused with `Code: 117`, and every
-- judged batch stalls until this file is applied. That is loud and reversible,
-- but it is a stalled runner. A host that judges no feed writes neither column
-- and is unaffected either way.

ALTER TABLE recorder.conformance_finding
    ADD COLUMN IF NOT EXISTS severity LowCardinality(String) DEFAULT ''
    AFTER rule_id;

ALTER TABLE recorder.conformance_finding
    ADD COLUMN IF NOT EXISTS group_addr IPv4 DEFAULT toIPv4('0.0.0.0')
    AFTER feed;
