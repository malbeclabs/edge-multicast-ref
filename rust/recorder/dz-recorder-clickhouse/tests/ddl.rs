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

use dz_recorder_clickhouse::{migrations, Migration};
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

    // `era_anchor_ts` and not `era_index`: the index a loader computes is local
    // to the object it loaded.
    assert!(
        sql.contains(
            "ORDER BY (source_addr, channel_id, dst_port, era_anchor_ts, missing_from, site)"
        ),
        "the gap sort key changed"
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

/// The rank lives in a view over the era openings, filtered to the boundaries
/// that actually open one.
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
    assert!(
        view.sql.contains("FROM recorder.era FINAL"),
        "without FINAL a settled boundary reads at its unsettled value until a \
         merge happens to run"
    );
    // And the range join the base table pays nothing for, written once.
    assert!(view.sql.contains("ASOF LEFT JOIN"));
    assert!(view.sql.contains("e.anchor_ts  <= d.recv_ts"));
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
