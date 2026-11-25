-- Which objects the conformance runner judged, and how much of each it graded.
--
-- `conformance_finding` holds only violations by default, because it has no TTL
-- and a row for every rule on every instance of every object is mostly passes.
-- Then an object with no finding row was either judged clean or never judged,
-- and only the runner's ledger, which lives on the loader's host, could tell the
-- two apart. A reader that takes "no violations" for "conforming" turns a feed
-- nobody judged green. This table is what a reader tells them apart by, and the
-- runner writes it for every object it judges, whatever verdicts it keeps.
--
-- ONE ROW PER JUDGED OBJECT AND CHANNEL INSTANCE that the run's finding rows are
-- placed on, channel grain included at `source_addr` `0.0.0.0`. The counts are
-- the finding rows that keeping every verdict would have written for the
-- instance, one per rule after the runner's reduction, so `pass + violation +
-- unverifiable + na` is the number of rules that reached it. `violation_must`,
-- `_should` and `_info` split `violation` by the stated severity, and a
-- severity the runner does not know counts in `violation` and in none of the
-- three, so the split can be checked against the total. `refused` is the
-- verdicts the run declined to place, on the object's first row: they belong to
-- no one instance, and here they are counted beside the coverage they reduce.
--
-- AN OBJECT THAT PLACED NOTHING STILL WRITES ONE ROW, at `group_addr`,
-- `source_addr` and `dst_port` all zero and `channel_id` 0, with every count but
-- `refused` zero. Without it a judged object that yielded nothing would read
-- exactly like an object never judged. An object the runner gave up on writes
-- nothing here: it was not judged, and a row would say it was.
--
-- WHAT A READER MAY CONCLUDE. An object with a row was judged, by the
-- `rule_set_version` on the row. An object without one was not judged, or not
-- yet: the runner works oldest first and holds judgements in the sink until
-- they land. Zero violations over a row with `pass > 0` is "nothing found wrong
-- in what was graded", which is weaker than "conforms", and `na` and
-- `unverifiable` are here so a reader can show the difference. The window is
-- the object's, so judging lags capture, and a reader asking about the last few
-- minutes of wall-clock time finds nothing and must not read that as a gap.
--
-- THE KEY IS `conformance_finding`'s WITH THE RULE TAKEN OUT AND THE GROUP PUT
-- IN. The row is about an instance and not a rule, and a dashboard keys a feed
-- on its group, so `feed` and `group_addr` lead. `site` and `recorder` are in it
-- for the reason `001` gives for the finding table: two vantages of one
-- instance are two observations. `rule_set_version` is in it so that a re-judge
-- under a new version stands beside the old one, and `ReplacingMergeTree(run_ts)`
-- so that a re-run of one version replaces only its own row. The runner writes
-- one row per distinct key in a run, so no two rows of one run collapse.
--
-- `derivation`, AS ON EVERY GRAIN. Always `archive`: the runner judges bytes it
-- kept and checked against their manifest. In no sort key, for the reason `008`
-- gives.
--
-- NO TTL, as for `conformance_finding` and for the same reason, which `002`
-- states. The rows are a few per object, not one per rule.
--
-- RE-APPLY `004` AFTER THIS FILE, OR THE TABLE BELOW IS UNWRITABLE
--
-- `004_recorder_loader_user.sql` is where the grant for this table lives,
-- written there exactly as it is written here:
--
--   GRANT INSERT ON recorder.conformance_judged TO dz_loader;
--
-- at table level, as for every other grain. It is not in this file for the
-- reasons `009` gives at length: a `GRANT` names `dz_loader`, which only `004`
-- creates, and a file holding a privilege statement is one a row writer must
-- not apply. So the hazard is a partial apply: an operator applies this file,
-- because it is the new thing, and the runner's first judged batch fails with
-- `Not enough privileges` on a table the message names and a file it does not.
-- `004` is idempotent, so re-applying it is always safe. On a fresh install by
-- the numbers `004` runs first and its grant names a table that does not exist
-- yet, which ClickHouse accepts and honours once this file creates it. A
-- reader's `SELECT` is the reader's deployment's business, as in `012`.
--
-- THEN ROLL THE LOADER THAT WRITES IT. As `014` says, the insert URL sets
-- `input_format_skip_unknown_fields` to 0, and an insert into a table that does
-- not exist is refused outright, so a loader rolled before this file is a
-- stalled runner until the file is applied. A host that judges no feed writes
-- no row here and is unaffected either way.

CREATE TABLE IF NOT EXISTS recorder.conformance_judged (
    run_ts           DateTime64(9),       -- when the rule set ran
    rule_set_version LowCardinality(String),
    site             LowCardinality(String),
    recorder         LowCardinality(String),
    env              LowCardinality(String),
    feed             LowCardinality(String),
    group_addr       IPv4,
    source_addr      IPv4,                -- 0.0.0.0 at channel grain
    channel_id       UInt8,
    dst_port         UInt16,
    window_start     DateTime64(9),
    window_end       DateTime64(9),
    object_key       String,
    derivation       LowCardinality(String) DEFAULT 'archive',
    pass             UInt32,
    violation        UInt32,
    unverifiable     UInt32,
    na               UInt32,
    violation_must   UInt32,
    violation_should UInt32,
    violation_info   UInt32,
    refused          UInt32
)
ENGINE = ReplacingMergeTree(run_ts)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (feed, group_addr, source_addr, channel_id, dst_port, window_start, site, recorder, rule_set_version);
