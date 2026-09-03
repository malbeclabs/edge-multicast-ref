//! The checked-in schema, held against the row types that fill it.
//!
//! This is the seam the whole tier can drift at silently. A row struct and a
//! `CREATE TABLE` live in two files and two languages, and `JSONEachRow` with
//! unknown-field skipping on somewhere would accept a renamed field and drop the
//! value. So every column is matched against every field here, both ways.
//!
//! The column extraction depends on the DDL's own formatting — a column
//! definition is a line indented exactly four spaces — which is a fair trade:
//! it makes the files consistently formatted as well as consistently named.
#![forbid(unsafe_code)]

use std::collections::BTreeSet;

use dz_recorder_clickhouse::{migrations, schema, Migration};
use dz_recorder_rows::Grain;

/// The columns one `CREATE TABLE recorder.<table>` block declares, in order.
fn columns(sql: &str, table: &str) -> Vec<String> {
    let needle = format!("CREATE TABLE IF NOT EXISTS recorder.{table} (");
    let start = sql
        .find(&needle)
        .unwrap_or_else(|| panic!("the schema declares no `{table}`"));
    let body = &sql[start + needle.len()..];
    let end = body
        .find("\n)")
        .unwrap_or_else(|| panic!("`{table}` has no closing parenthesis"));

    body[..end]
        .lines()
        .filter_map(|line| {
            // Exactly four spaces, then an identifier: a column definition. A
            // comment line and the continuation of a materialised expression are
            // both excluded by that, the first by its `--` and the second by
            // being indented further.
            let rest = line.strip_prefix("    ")?;
            if rest.starts_with(' ') || rest.starts_with("--") {
                return None;
            }
            let name = rest.split_whitespace().next()?;
            // Digits included: `object_sha256` is a column name.
            name.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                .then(|| name.to_owned())
        })
        .collect()
}

fn rows_sql() -> &'static str {
    migrations()
        .into_iter()
        .find(|m| m.name == "001_recorder_rows.sql")
        .expect("the tables are in 001")
        .sql
}

/// A field with no column, or a column with no field, fails here.
#[test]
fn every_column_has_a_field_and_every_field_has_a_column() {
    let sql = rows_sql();
    for (grain, fields) in [
        (Grain::Datagram, field_names(&fixtures::datagram())),
        (Grain::Era, field_names(&fixtures::era())),
        (
            Grain::SegmentCoverage,
            field_names(&fixtures::segment_coverage()),
        ),
        (Grain::SequenceGap, field_names(&fixtures::sequence_gap())),
        (
            Grain::ConformanceFinding,
            field_names(&fixtures::conformance_finding()),
        ),
    ] {
        let declared: BTreeSet<String> = columns(sql, grain.table()).into_iter().collect();
        let mut expected = fields;
        if grain == Grain::Datagram {
            // The one column the loader never sends: the engine computes it, and
            // inserting into a MATERIALIZED column is an error.
            expected.insert("send_recv_ms".to_owned());
        }
        assert_eq!(
            declared, expected,
            "{grain}: the schema and the row type disagree about columns"
        );
    }
}

/// The DDL's sort keys are the ones the row types were shaped for.
///
/// Each of these is a place where the design's own DDL gave a key that would
/// collapse two genuine rows into one under `ReplacingMergeTree`, and the reason
/// is stated in the file's own header. Asserting them here means a later edit to
/// a key has to be a deliberate one.
#[test]
fn the_sort_keys_are_the_ones_the_rows_were_shaped_for() {
    let sql = rows_sql();

    // `recv_ts` last, or a network duplicate and two eras sharing a wrapped
    // `Reset Count` both collapse.
    assert!(
        sql.contains(
            "ORDER BY (source_addr, channel_id, dst_port, sequence_number, site, recv_ts)"
        ),
        "the datagram sort key changed"
    );
    // No `era_index` anywhere in the base table, because a stored rank is
    // renumbered by any later-arriving earlier object.
    let datagram = columns(sql, "datagram");
    assert!(
        !datagram.iter().any(|c| c == "era_index"),
        "an era_index reappeared in the base table: {datagram:?}"
    );

    // Keyed on the anchor, which is unique per instance per site, so a settled
    // boundary replaces the unsettled row rather than sitting beside it.
    assert!(
        sql.contains("ENGINE = ReplacingMergeTree(anchor_certain)"),
        "the era table's version column changed: late evidence must upgrade a \
         verdict and never regress it"
    );
    assert!(
        sql.contains("ORDER BY (site, recorder, source_addr, channel_id, dst_port, anchor_ts)"),
        "the era sort key changed"
    );

    // `segment_seq` restarts at 0 on every recorder run, so a key without the
    // site and the recorder in it merges two hosts' segments.
    assert!(
        sql.contains(
            "ORDER BY (source_addr, channel_id, dst_port, segment_seq, site, recorder, start_ts)"
        ),
        "the coverage sort key changed"
    );

    // Partitioned like every other table here, and it was the one exception:
    // unpartitioned it is a table whose merges grow with the age of the
    // deployment and whose TTL would be a full rewrite rather than a decision.
    assert!(
        sql.contains("PARTITION BY toYYYYMMDD(anchor_ts)"),
        "the era table is not partitioned"
    );
    // `era_anchor_ts` and not `era_index`: the index a loader computes is local
    // to the object it loaded.
    assert!(
        sql.contains(
            "ORDER BY (source_addr, channel_id, dst_port, era_anchor_ts, missing_from, site)"
        ),
        "the gap sort key changed"
    );
}

/// Every table is partitioned by a day, and none is an exception.
///
/// `era` was, and the exception was not a decision — it was the one table whose
/// growth nobody had put a number on. An unpartitioned table that is kept
/// indefinitely is one whose merges grow with the age of the deployment and
/// whose TTL, if one is ever wanted, is a full rewrite rather than a decision
/// somebody can take.
#[test]
fn every_table_is_partitioned_by_a_day() {
    let sql = rows_sql();
    let expected = [
        (Grain::Datagram, "toYYYYMMDD(recv_ts)"),
        (Grain::Era, "toYYYYMMDD(anchor_ts)"),
        (Grain::SegmentCoverage, "toYYYYMMDD(start_ts)"),
        (Grain::SequenceGap, "toYYYYMMDD(before_ts)"),
        (Grain::ConformanceFinding, "toYYYYMMDD(window_start)"),
    ];
    for (grain, partition) in expected {
        assert!(
            sql.contains(&format!("PARTITION BY {partition}")),
            "{grain} is not partitioned by {partition}"
        );
    }
    // One `PARTITION BY` per table, so a table added later without one fails
    // here rather than being noticed on a graph months afterwards.
    assert_eq!(
        sql.matches("PARTITION BY ").count(),
        Grain::COUNT,
        "a table has no PARTITION BY, or one has two"
    );
}

/// `era`'s row rate is stated, with the arithmetic, because the rank over it is
/// unbounded and the crossover is invisible without the number.
///
/// It is not what "one row per reset" suggests: a loader writes one row per
/// channel instance per *object*, so this is `segment_coverage`'s cardinality
/// rather than a reset's.
#[test]
fn the_era_tables_row_rate_is_stated_with_its_arithmetic() {
    let sql = rows_sql();
    let era = sql
        .split("CREATE TABLE IF NOT EXISTS recorder.era (")
        .next()
        .expect("the table has a header")
        .rsplit("-- 2.")
        .next()
        .expect("the header is numbered");

    assert!(
        era.contains("THE ROW RATE"),
        "the rate is not stated: {era}"
    );
    assert!(
        era.contains("1,440 segments a day"),
        "the ceiling the rotation interval sets has to be in the arithmetic"
    );
    assert!(
        era.contains("same cardinality as segment_coverage"),
        "the comparison that makes the number legible"
    );
    assert!(
        era.contains("kept indefinitely"),
        "and that this is the table whose size is a function of the \
         deployment's age"
    );
}

/// What is not known reaches the store as `NULL`, so the column has to be
/// `Nullable` — a zero in any of these is a measurement nobody made.
#[test]
fn the_columns_that_can_be_unknown_are_nullable() {
    let sql = rows_sql();
    for column in [
        "unexplained_count",
        "interface_drops",
        "seen_elsewhere",
        "on_redundant_path",
        "sent_from_ts",
        "sent_to_ts",
    ] {
        let line = sql
            .lines()
            .find(|l| l.trim_start().starts_with(&format!("{column} ")))
            .unwrap_or_else(|| panic!("no `{column}` column"));
        assert!(
            line.contains("Nullable("),
            "`{column}` must be Nullable: {line}"
        );
    }
}

/// Only `datagram` has a TTL, and the other four say in the file that they have
/// none.
#[test]
fn the_retention_split_expires_the_base_rows_and_keeps_the_derived_ones() {
    let retention = migration("002_recorder_retention.sql");
    assert!(
        retention.sql.contains("ALTER TABLE recorder.datagram")
            && retention.sql.contains("MODIFY TTL"),
        "the base table has no TTL"
    );
    for table in [
        "era",
        "segment_coverage",
        "sequence_gap",
        "conformance_finding",
    ] {
        assert!(
            !retention
                .sql
                .contains(&format!("ALTER TABLE recorder.{table}")),
            "`{table}` is a derived grain and must not expire"
        );
        assert!(
            retention.sql.contains(&format!("recorder.{table}")),
            "`{table}`'s absence of a TTL has to be stated, not inferred from \
             the absence of a line"
        );
    }
    // The measurement the sizing rests on, so a later reader can check it rather
    // than take it.
    assert!(
        retention.sql.contains("80,000 datagrams a minute"),
        "the sizing has to state the measurement it came from"
    );
}

/// The rank is a view over the openings, and it is not on the path a panel
/// queries.
///
/// A dense rank is defined over all history, so a predicate on time cannot be
/// pushed through the window function: a query for one hour ranks every era ever
/// recorded for the instances it selects, and `era` is kept indefinitely. So the
/// rank exists, is documented as the all-history query it is, and the two views
/// a panel actually joins carry the era's *identity* — the anchor — instead.
#[test]
fn the_era_index_is_a_rank_over_the_openings_and_not_a_column() {
    let view = migration("003_recorder_era_rank.sql");
    assert!(view.sql.contains("dense_rank() OVER ("));
    assert!(
        view.sql
            .contains("PARTITION BY site, recorder, source_addr, channel_id, dst_port"),
        "an anchor is a receive stamp, so it is one site's observation"
    );
    assert!(
        view.sql.contains("WHERE continuation = 0"),
        "a boundary settled as a continuation is recorded and not ranked"
    );
    // The cost is stated in the file, because the crossover is invisible
    // without it and the failure mode is a table too big to fix cheaply by the
    // time somebody reads a graph.
    assert!(
        view.sql.contains("all-history query by construction"),
        "the rank's cost has to be stated where somebody about to use it reads"
    );
}

/// The collapse is `FINAL`, over a table that is partitioned.
///
/// `FINAL` is what applies `ReplacingMergeTree(anchor_certain)` at read time, so
/// a boundary the archive has since settled reads at its settled value rather
/// than waiting for a merge. It forces merge-on-read, which is why the partition
/// on `era` is what makes it affordable: a predicate on `anchor_ts` prunes
/// first, and `FINAL` pays for what is left.
///
/// It was briefly a hand-written `max`/`argMax` collapse instead. That is
/// recorded in the file as the worse trade it is: a hand-written collapse has to
/// match the engine's semantics exactly and keep matching them as columns are
/// added.
#[test]
fn the_era_opening_is_collapsed_by_final_over_a_partitioned_table() {
    let sql = migration("003_recorder_era_rank.sql").sql;
    assert!(
        sql.contains("FROM recorder.era FINAL"),
        "the collapse is the engine's own"
    );
    assert!(
        sql.contains("WHERE continuation = 0"),
        "a boundary settled as a continuation opens no era"
    );
    assert!(
        sql.contains("affordable because the table underneath it is partitioned"),
        "why `FINAL` is acceptable has to be stated beside it"
    );
    // Exactly one view reads the base table, so the collapse and the filter are
    // written once and the other two views build on it.
    assert_eq!(
        sql.matches("recorder.era FINAL").count(),
        1,
        "the collapse is written once"
    );
    // Once as a join and once as a scan, which is the two views a caller uses.
    assert_eq!(
        sql.matches("recorder.era_opening AS e").count(),
        1,
        "the range join builds on it"
    );
    assert_eq!(
        sql.matches("FROM recorder.era_opening").count(),
        1,
        "and so does the rank"
    );
}

/// The join a panel runs carries the era's identity and no rank./// The join a panel runs carries the era's identity and no rank.
///
/// The anchor is a receive stamp on a row that already exists; the index is a
/// position in a sequence that renumbers when an earlier era arrives late. So
/// the cheap view keys on the anchor, and both sides of the join prune.
#[test]
fn resolving_a_datagram_to_its_era_needs_no_window_and_no_final() {
    let view = migration("003_recorder_era_rank.sql");
    let datagram_in_era = view
        .sql
        .split("CREATE OR REPLACE VIEW recorder.datagram_in_era AS")
        .nth(1)
        .expect("the view is declared")
        .split("CREATE OR REPLACE VIEW")
        .next()
        .expect("the view ends");

    assert!(datagram_in_era.contains("ASOF LEFT JOIN"));
    assert!(datagram_in_era.contains("e.anchor_ts  <= d.recv_ts"));
    assert!(
        datagram_in_era.contains("era_anchor_ts") && datagram_in_era.contains("anchor_certain"),
        "the era's identity, and what says whether a finding may be escalated"
    );
    assert!(
        !datagram_in_era.contains("era_index"),
        "the rank is an all-history computation and must not be on this path"
    );
    assert!(
        !datagram_in_era.contains("dense_rank"),
        "nor the window that produces it"
    );
    assert!(
        datagram_in_era.contains("recorder.era_opening"),
        "the collapse and the `continuation = 0` filter are written once, in \
         the view this builds on"
    );
}

/// Every file splits into statements a server takes one at a time, and no
/// statement is a fragment of prose.
#[test]
fn every_migration_splits_into_whole_statements() {
    for migration in migrations() {
        let statements = migration.statements();
        assert!(
            !statements.is_empty(),
            "{}: no statement at all",
            migration.name
        );
        for statement in &statements {
            assert!(
                statement.ends_with(';'),
                "{}: a statement that is not terminated: {statement}",
                migration.name
            );
            let code: String = statement
                .lines()
                .filter(|l| !l.trim_start().starts_with("--"))
                .collect::<Vec<_>>()
                .join(" ");
            assert!(
                code.trim().len() > 1,
                "{}: a statement that is nothing but comments",
                migration.name
            );
        }
    }

    // The five tables and the database, and nothing split across two of them.
    let statements = migration("001_recorder_rows.sql").statements();
    assert_eq!(statements.len(), 6, "one database and five tables");
    for grain in Grain::ALL {
        assert_eq!(
            statements
                .iter()
                .filter(|s| s.contains(&format!("recorder.{} (", grain.table())))
                .count(),
            1,
            "{grain}"
        );
    }
}

/// This repository is public, so the schema names no venue and no real network.
#[test]
fn the_schema_names_no_venue_and_no_address_outside_the_documentation_ranges() {
    for migration in migrations() {
        for line in migration.sql.lines() {
            assert!(
                !line.contains("10.") || line.trim_start().starts_with("--"),
                "{}: {line}",
                migration.name
            );
            assert!(
                !line.contains("192.168.") && !line.contains("239."),
                "{}: an address outside the documentation ranges: {line}",
                migration.name
            );
        }
        // The port-role tokens the glossary mandates, and no alias.
        assert!(
            !migration.sql.contains("marketdata"),
            "{}: the token is `mktdata`",
            migration.name
        );
    }
}

fn migration(name: &str) -> Migration {
    migrations()
        .into_iter()
        .find(|m| m.name == name)
        .unwrap_or_else(|| panic!("no migration named {name}"))
}

fn field_names<T: serde::Serialize>(row: &T) -> BTreeSet<String> {
    serde_json::to_value(row)
        .expect("a row serialises")
        .as_object()
        .expect("a row is an object")
        .keys()
        .cloned()
        .collect()
}

/// One of each row, filled with anything: only the field names are read.
mod fixtures {
    use std::net::Ipv4Addr;

    use dz_recorder_rows::{
        ConformanceFinding, Datagram, DropScope, Era, FindingVerdict, Nanos, PortRoleLabel,
        RecvTsKindLabel, SegmentCoverage, SequenceGap, Verdict,
    };

    const ADDR: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 10);

    pub fn datagram() -> Datagram {
        Datagram {
            recv_ts: Nanos(0),
            send_ts: Nanos(0),
            recv_ts_kind: RecvTsKindLabel::KernelSoftware,
            source_addr: ADDR,
            channel_id: 0,
            dst_port: 0,
            feed: String::new(),
            port_role: PortRoleLabel::Mktdata,
            group_addr: ADDR,
            sequence_number: 0,
            reset_count: 0,
            segment_seq: 0,
            payload_len: 0,
            wire_payload_len: 0,
            drop_delta: 0,
            site: String::new(),
            recorder: String::new(),
            env: String::new(),
            drop_scope: DropScope::PortRole,
            object_key: String::new(),
            object_sha256: String::new(),
        }
    }

    pub fn era() -> Era {
        Era {
            site: String::new(),
            recorder: String::new(),
            feed: String::new(),
            source_addr: ADDR,
            channel_id: 0,
            dst_port: 0,
            anchor_ts: Nanos(0),
            anchor_seq: 0,
            reset_count: 0,
            segment_seq: 0,
            anchor_certain: 0,
            continuation: 0,
            object_key: String::new(),
            object_sha256: String::new(),
        }
    }

    pub fn segment_coverage() -> SegmentCoverage {
        SegmentCoverage {
            site: String::new(),
            recorder: String::new(),
            env: String::new(),
            feed: String::new(),
            source_addr: ADDR,
            channel_id: 0,
            dst_port: 0,
            segment_seq: 0,
            start_ts: Nanos(0),
            end_ts: Nanos(0),
            first_seq: 0,
            last_seq: 0,
            datagram_count: 0,
            reset_counts_seen: Vec::new(),
            capture_drop_total: 0,
            interface_drop_total: 0,
            drop_scope: DropScope::PortRole,
            roles_joined: Vec::new(),
            object_key: String::new(),
            object_sha256: String::new(),
            build_version: String::new(),
            build_commit: String::new(),
            config_hash: String::new(),
        }
    }

    pub fn sequence_gap() -> SequenceGap {
        SequenceGap {
            site: String::new(),
            recorder: String::new(),
            env: String::new(),
            feed: String::new(),
            port_role: PortRoleLabel::Mktdata,
            group_addr: ADDR,
            source_addr: ADDR,
            channel_id: 0,
            dst_port: 0,
            reset_count: 0,
            era_index: 0,
            era_anchor_ts: Nanos(0),
            anchor_certain: 0,
            missing_from: 0,
            missing_to: 0,
            missing_count: 0,
            reference_seqs: 0,
            before_ts: Nanos(0),
            after_ts: Nanos(0),
            sent_from_ts: None,
            sent_to_ts: None,
            admitted_recorder: 0,
            admitted_scope: DropScope::PortRole,
            unexplained_count: None,
            interface_drops: None,
            seen_elsewhere: None,
            on_redundant_path: None,
            verdict: Verdict::Unverifiable,
            object_key: String::new(),
        }
    }

    pub fn conformance_finding() -> ConformanceFinding {
        ConformanceFinding {
            run_ts: Nanos(0),
            rule_id: String::new(),
            rule_set_version: String::new(),
            site: String::new(),
            recorder: String::new(),
            env: String::new(),
            feed: String::new(),
            port_role: PortRoleLabel::Mktdata,
            source_addr: ADDR,
            channel_id: 0,
            dst_port: 0,
            window_start: Nanos(0),
            window_end: Nanos(0),
            verdict: FindingVerdict::Pass,
            detail: String::new(),
            object_key: String::new(),
            first_seq: 0,
            last_seq: 0,
        }
    }
}

/// Deduplication is merge-time, and the schema says so where a consumer reads.
///
/// `ReplacingMergeTree` collapses rows when it merges parts, so until a merge
/// runs a re-load's duplicates are *visible*. That is correct for idempotence
/// and surprising for everything downstream: a data-quality check that counts
/// rows reads a re-load as a doubling, which has already produced one false
/// finding that had to be retracted.
#[test]
fn the_schema_says_deduplication_is_merge_time() {
    let sql = rows_sql();
    // Both halves, because only one of them is the one usually quoted and the
    // other is the one that surprised this file's own author.
    assert!(
        sql.contains("MERGE-TIME ACROSS INSERTS AND INSERT-TIME WITHIN ONE"),
        "the timing has to be stated where somebody counting rows will read it"
    );
    assert!(
        sql.contains("optimize_on_insert"),
        "the setting that makes the within-one-insert case what it is"
    );
    assert!(
        sql.contains("FINAL"),
        "and the query that is exact has to be shown beside the one that is fast"
    );
    assert!(
        sql.contains("upper bound"),
        "because that is what an approximate count is, and never an equality"
    );
}

/// The retention file states the part count, not only the row count, and says
/// the TTL is never applied by hand.
///
/// Both come from incidents: a row count does not predict what a TTL costs, and
/// a hand-applied TTL was silently reverted by a nightly sync for six days.
#[test]
fn the_retention_file_states_its_part_count_and_where_it_lives() {
    let sql = migration("002_recorder_retention.sql").sql;
    assert!(
        sql.contains("THE PART COUNT THE TTL IMPLIES"),
        "a row count does not predict what retention costs"
    );
    assert!(
        sql.contains("parts per daily partition"),
        "the number has to be there, not just the warning"
    );
    assert!(
        sql.contains("part *drop* rather than as a row-level mutation"),
        "why a whole-day window against a daily partition is the cheap shape"
    );
    assert!(
        sql.contains("NEVER APPLIED BY HAND"),
        "the other incident, and the reason this file exists"
    );
}

/// The loader's account is checked in, bounded, and not applied by anything that
/// writes rows.
#[test]
fn the_loader_account_is_bounded_and_kept_out_of_the_schema() {
    let user = migration("004_recorder_loader_user.sql").sql;

    // The ceiling that matters, and the one that keeps a later query from
    // becoming the most expensive on the cluster.
    assert!(
        user.contains("max_bytes_to_read"),
        "no read ceiling: {user}"
    );
    assert!(user.contains("max_threads = 1"), "no thread cap");
    assert!(
        user.contains("CREATE QUOTA"),
        "a profile bounds a query, a quota bounds a day"
    );

    // INSERT on all five, SELECT on exactly the two the adjacency check reads.
    for grain in Grain::ALL {
        assert!(
            user.contains(&format!("GRANT INSERT ON recorder.{}", grain.table())),
            "{grain} cannot be written"
        );
    }
    assert!(user.contains("GRANT SELECT ON recorder.segment_coverage"));
    assert!(user.contains("GRANT SELECT ON recorder.era TO"));
    assert!(
        !user.contains("GRANT SELECT ON recorder.datagram"),
        "the largest table by three orders of magnitude, and nothing reads it"
    );
    assert!(
        !user.contains("GRANT ALTER") && !user.contains("GRANT CREATE"),
        "a loader that could alter a table could apply a schema nobody reviewed"
    );

    // The password is a parameter, never a literal in a file this repository
    // holds.
    assert!(user.contains("{password:String}"), "{user}");
    for leak in ["IDENTIFIED BY '", "sha256_hash BY '"] {
        assert!(!user.contains(leak), "a literal credential: {user}");
    }

    // And it is not in what a test or a schema deploy applies.
    assert!(
        !schema().iter().any(|m| m.name.contains("loader_user")),
        "the account is applied by an administrator, not by the row writer"
    );
    assert_eq!(schema().len(), migrations().len() - 1);
}

/// The account file can be applied in the order it is written.
///
/// Every edge here is a name the server resolves as it stores an entity, not a
/// preference: `SETTINGS PROFILE 'dz_loader'` is resolved when the user is
/// stored, and the quota and the grants name a user that has to exist by then.
/// Written the other way round the first statement fails and nothing is
/// created — and a re-run after a partial fix finds the user already there
/// behind `IF NOT EXISTS` and leaves it without its ceilings for ever, which is
/// the one outcome the file exists to prevent. Nothing in this repository
/// applies `004` — it needs a password and access-management rights — so the
/// order is asserted here or nowhere.
#[test]
fn the_account_file_creates_the_profile_before_the_user_that_names_it() {
    // The SQL of each statement, without the prose above it: a comment block is
    // kept inside the statement it precedes, and the prose in this file names
    // the very statements under test here.
    let statements: Vec<String> = migration("004_recorder_loader_user.sql")
        .statements()
        .iter()
        .map(|s| {
            s.lines()
                .filter(|l| !l.trim_start().starts_with("--"))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .collect();
    let first = |needle: &str| {
        statements
            .iter()
            .position(|s| s.contains(needle))
            .unwrap_or_else(|| panic!("no statement contains {needle}: {statements:#?}"))
    };

    let profile = first("CREATE SETTINGS PROFILE");
    let user = first("CREATE USER");
    let quota = first("CREATE QUOTA");
    let grant = first("GRANT ");

    assert!(
        profile < user,
        "the profile is resolved when the user is stored, not on the first query"
    );
    assert!(user < quota, "the quota names the user in its TO clause");
    assert!(user < grant, "a grant names a user that has to exist");
}
