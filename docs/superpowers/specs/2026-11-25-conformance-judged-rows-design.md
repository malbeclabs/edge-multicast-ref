# Conformance coverage a reader can see: `conformance_judged`, and a finding's severity

**Status:** draft, pending review
**Date:** 2026-11-25
**Applies to:** `dz-recorder-load`'s conformance runner, `dz-recorder-rows`, `dz-recorder-clickhouse` and its migrations
**Authority:** `2026-09-06-recorder-conformance-runner-design.md`, whose runner this extends and whose `KeptVerdicts` default this answers; [`edge-feed-spec`](https://github.com/malbeclabs/edge-feed-spec) and its `GLOSSARY.md`, which own the rule set, its severities and the vocabulary.

---

## Naming

This repository is public. This document names no venue, host, dashboard or
issue tracker, and gives no count of publishers or recorder sites.
`GLOSSARY.md` governs the vocabulary: `datagram`, `era`, `channel` for the
`Channel ID` shard, `port role`, `feed`, and `source` never bare.

---

## The question

A reader of `conformance_finding` cannot answer the two questions a
conformance column is for.

1. **Was this feed judged at all, and how much of it?** The runner writes
   only `violation` rows by default (`verdicts = "violations"`), because the
   table has no TTL and a row for every rule on every instance of every object
   is mostly passes. Then an object with no row was either judged clean or
   never judged. The judge ledger can tell the two apart, but it lives on the
   loader's host and no reader of the table can see it. A column that reads
   "no violations" as "conforming" turns a feed nobody judged green.
2. **How bad is a violation?** The rule set states a severity (`must`,
   `should`, `info`) on every entry of its report. The runner parses the
   report and drops it, so a reader has to keep its own copy of the
   catalog's severities, and that copy goes stale the first time a rule
   changes.

A third gap follows from the reader's side. A dashboard keys a feed on its
multicast **group address**, which is what a subscriber joins. The finding row
states a feed, a port role and a destination port, but not the group, so the
reader has to rebuild a mapping from feed names that the manifest already holds.

## The change

### 1. `conformance_judged`: one row per judged object and channel instance, always written

A new table and a new `Grain`, written for **every** object the runner judges,
whatever `verdicts` is set to. Its rows are the coverage the finding table
leaves out under `violations`. They are counts, not verdicts, so they cost a
few rows per object where `verdicts = "all"` costs one row per rule.

```sql
CREATE TABLE IF NOT EXISTS recorder.conformance_judged (
    run_ts           DateTime64(9),
    rule_set_version LowCardinality(String),
    site             LowCardinality(String),
    recorder         LowCardinality(String),
    env              LowCardinality(String),
    feed             LowCardinality(String),
    group_addr       IPv4,
    source_addr      IPv4,       -- 0.0.0.0 at channel grain, as in conformance_finding
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
```

- **Grain.** One row per `(object, channel instance)` that the finding rows
  of the run are placed on, channel grain included. The counts are the
  finding rows that `verdicts = "all"` would have written for that instance:
  one per rule, after the reduction in `rows.rs`, so `pass + violation +
  unverifiable + na` is the number of rules that reached the instance.
  `violation_must`, `_should` and `_info` split `violation` by the stated
  severity. A severity the runner does not know counts in `violation` and in
  none of the three, so the split can be checked against the total.
- **`refused`** is the number of verdicts the runner declined to place in
  this run (no instance, an undescribed instance, overflow, an unresolved
  channel). These are already counted in metrics. Here they are also
  counted beside the coverage they reduce, on the object's first row.
- **An object that produced no placed verdict at all** still writes one row,
  at `group_addr`, `source_addr` and `dst_port` all zero and `channel_id` 0,
  with every count but `refused` zero. Without it, a judged object that
  yielded nothing would read exactly like an object never judged.
- **A given-up object** (`ConformanceRefusal::GivenUp`) writes nothing here.
  It was not judged, and a row would claim it was. A reader sees it as
  unjudged, which is what it is, and the metric counts it.
- **No TTL**, as for `conformance_finding`, and for the same reason. The rows
  are a few per object, not one per rule.
- **Identity and window are the manifest's**, as in `finding_rows`.
- **`derivation`** is always `archive`, and in no sort key, because every grain
  carries its provenance (`provenance_is_on_every_grain_and_in_no_sort_key`).

### 2. `severity` and `group_addr` on `conformance_finding`

```sql
ALTER TABLE recorder.conformance_finding
    ADD COLUMN IF NOT EXISTS severity   LowCardinality(String) DEFAULT '' AFTER rule_id,
    ADD COLUMN IF NOT EXISTS group_addr IPv4 DEFAULT toIPv4('0.0.0.0') AFTER feed;
```

- `severity` is the stated severity of the entry the row speaks for (`first`
  in `rows.rs`), lower-cased, as the report spells it. It is in no sort key: a
  rule's severity is a property of the rule set version, which is already in
  the key. Rows written before this change read `''`, and a reader treats that
  as unknown, never as `info`.
- `group_addr` is the group the manifest joined for the row's port role and
  destination port, the same lookup `segment_coverage.roles_joined` records.
  It is `0.0.0.0` where the manifest names no group for that pair, which a
  reader must not resolve by guessing. A port joined in more than one group
  for the same role is `0.0.0.0` too, for the same reason. It is in no sort
  key.

`RuleOutcome` gains `severity: Option<String>`, with `#[serde(default)]`. A
report that does not state it parses as before and writes `''`.

### 3. Grants

`004` gains `GRANT INSERT ON recorder.conformance_judged TO dz_loader`, at table
level, as for the other grains. It goes in `004` and not in `015` because the
schema files hold no grants (`the_access_management_list_matches_the_files_that_grant`).
So `015` says to re-apply `004` after it, as `009` does. A reader's `SELECT`
grant is the reader's deployment's business, as in `012`.

## Ordering, because the sink refuses unknown columns

The insert URL sets `input_format_skip_unknown_fields=0`. A loader that writes
`severity` before `014` is applied is refused with `Code: 117`, and every
judged batch stalls until the migration is applied. This is loud and
reversible, but it is a stalled runner. So:

1. Apply `014_recorder_conformance_severity.sql` and
   `015_recorder_conformance_judged.sql`.
2. Re-apply `004_recorder_loader_user.sql`, for the new grant. Without it every
   judged batch is refused for privilege, which is the same stall.
3. Roll out the loader that writes them (0.8.0).

A host that judges no feed writes neither table, and is unaffected either way.

## What a reader may and may not conclude

- An object with a `conformance_judged` row **was judged**, by the
  `rule_set_version` on the row. Its counts are the coverage reached. An
  object without one was not judged, or not yet: the runner works oldest
  first and holds judgements in the sink until they land.
- **Zero violations over rows with `pass > 0` is "nothing found wrong in what
  was graded"**, which is weaker than "conforms". `na` and `unverifiable` are
  in the counts so that the reader can show the difference.
- A window is the object's window. Judging lags capture, by the time it takes
  to complete an object plus the runner's backlog and flush delay. A reader
  that asks about the last fifteen minutes of wall-clock time will find
  nothing and must not read that as a gap in the feed.

## Not changed

- `verdicts = "violations"` stays the default. This change removes the reason
  for anyone to set `all` just to get coverage.
- The judge ledger stays on the host and is still what decides whether an
  object is judged again.
- Nothing here derives `publisher` or `seen_elsewhere`. The cross-site pass is
  still not here.
