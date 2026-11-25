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
--
-- `group_addr` GOES LAST IN THE SORT KEY, AND THE KEY KEEPS NO FEED. The key
-- `001` gave the table names no feed and no group, so two feeds' findings that
-- agree on the rule, the source, the channel, the port, the window start, the
-- vantage and the version were one row to the replacing merge, and `FINAL`
-- kept one feed's violation and dropped the other's. Channel grain makes that
-- ordinary rather than rare: every such row has source 0.0.0.0, and feeds that
-- rotate on one clock open their windows at one instant. `MODIFY ORDER BY`
-- appends only a column the same `ALTER` adds, so `group_addr` can go in and
-- `feed` and `object_key`, which `001` already has, cannot. The group tells two
-- feeds apart, because a subscriber joins a feed by it. What stays one row is
-- two objects of one feed opening at one instant, which a recorder restarted
-- inside one tick does: the counts a reader shows come from
-- `conformance_judged`, whose key has the object, and this table is the detail
-- behind them.
--
-- THE COLUMN HAS NO STATED DEFAULT. ClickHouse refuses to append a column with
-- a default expression to a key (`Code: 36`). The type's own default is
-- 0.0.0.0, which is the value a stated one would give.
--
-- Appending a column the same statement adds leaves every existing part in
-- key order, because the column is one value in all of them, so nothing is
-- rewritten. The primary index stays the old key, which is a prefix of the
-- new one. On a fresh deployment, and on a second apply, the column already
-- exists and the key already ends in it, so the statement is a no-op.
--
-- WHAT A ROW WRITTEN BEFORE THIS FILE READS AS. `severity` reads as '', which a
-- reader takes as unknown and never as `info`: an old violation of a `must`
-- rule is not a minor one. `group_addr` reads as `0.0.0.0`, which is also what
-- the runner writes where the manifest names no one group for the pair, and a
-- reader must not resolve it by guessing from the feed's name. `severity`'s
-- default is stated, and `group_addr`'s is the type's for the reason above;
-- both are the documented meaning of an absent value and not an accident of it.
--
-- Adding a column to a MergeTree is a metadata change: existing
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
    ADD COLUMN IF NOT EXISTS group_addr IPv4
    AFTER feed,
    MODIFY ORDER BY (rule_id, source_addr, channel_id, dst_port, window_start, site, recorder, rule_set_version, group_addr);
