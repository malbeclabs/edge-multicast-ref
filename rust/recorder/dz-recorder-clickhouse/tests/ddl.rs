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

/// The grains `001` declares: the envelope of a datagram, and what is derived
/// from it.
const TRANSPORT_GRAINS: [Grain; 5] = [
    Grain::Datagram,
    Grain::Era,
    Grain::SegmentCoverage,
    Grain::SequenceGap,
    Grain::ConformanceFinding,
];

/// The grains `005` declares: what the messages said.
const MARKET_DATA_GRAINS: [Grain; 3] = [Grain::Event, Grain::Instrument, Grain::BookTop];

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
    sql_of("001_recorder_rows.sql")
}

/// The market data tables, which are `005` rather than `001`.
fn market_data_sql() -> &'static str {
    sql_of("005_recorder_market_data.sql")
}

/// The pairing views, which are `006`.
fn pairing_sql() -> &'static str {
    sql_of("006_recorder_book_top_pairing.sql")
}

/// The cross-site views, which are `007`.
fn cross_site_sql() -> &'static str {
    sql_of("007_recorder_cross_site.sql")
}

/// One `CREATE OR REPLACE VIEW recorder.<name>` statement, up to the next one.
fn view_body(sql: &'static str, name: &str) -> &'static str {
    let needle = format!("CREATE OR REPLACE VIEW recorder.{name} AS");
    let start = sql
        .find(&needle)
        .unwrap_or_else(|| panic!("the schema declares no view `{name}`"));
    let body = &sql[start..];
    body.find("\nCREATE OR REPLACE VIEW")
        .map_or(body, |end| &body[..end])
}

fn sql_of(name: &str) -> &'static str {
    migrations()
        .into_iter()
        .find(|m| m.name == name)
        .unwrap_or_else(|| panic!("{name} is a migration"))
        .sql
}

/// A field with no column, or a column with no field, fails here.
#[test]
fn every_column_has_a_field_and_every_field_has_a_column() {
    for (sql, grain, fields) in [
        (
            rows_sql(),
            Grain::Datagram,
            field_names(&fixtures::datagram()),
        ),
        (rows_sql(), Grain::Era, field_names(&fixtures::era())),
        (
            rows_sql(),
            Grain::SegmentCoverage,
            field_names(&fixtures::segment_coverage()),
        ),
        (
            rows_sql(),
            Grain::SequenceGap,
            field_names(&fixtures::sequence_gap()),
        ),
        (
            rows_sql(),
            Grain::ConformanceFinding,
            field_names(&fixtures::conformance_finding()),
        ),
        (
            market_data_sql(),
            Grain::Event,
            field_names(&fixtures::event()),
        ),
        (
            market_data_sql(),
            Grain::Instrument,
            field_names(&fixtures::instrument()),
        ),
        (
            market_data_sql(),
            Grain::BookTop,
            field_names(&fixtures::book_top()),
        ),
    ] {
        let declared: BTreeSet<String> = columns(sql, grain.table()).into_iter().collect();
        let mut expected = fields;
        if grain == Grain::Datagram || grain == Grain::Event {
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

/// The `ORDER BY (...)` of one table, as one line.
///
/// The key may wrap, so this joins until the closing parenthesis rather than
/// reading a line: a key that fits on one line and a key that does not are the
/// same key, and a test that could only read the first would quietly stop
/// checking the moment one grew.
fn sort_key(sql: &str, table: &str) -> String {
    let after = sql
        .split_once(&format!("CREATE TABLE IF NOT EXISTS recorder.{table} ("))
        .unwrap_or_else(|| panic!("{table} is declared"))
        .1;
    let key = after
        .split_once("ORDER BY (")
        .unwrap_or_else(|| panic!("{table} has an ORDER BY"))
        .1;
    let key = key
        .split_once(");")
        .unwrap_or_else(|| panic!("{table}'s ORDER BY is closed"))
        .0;
    key.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The market data keys carry everything that distinguishes two genuine rows.
///
/// `ReplacingMergeTree` deduplicates on the whole sort key, so a key missing an
/// identity column does not merely sort badly — it deletes rows. Each assertion
/// here is a row that would have been lost.
#[test]
fn the_market_data_sort_keys_carry_what_distinguishes_two_rows() {
    let sql = market_data_sql();
    let event = sort_key(sql, "event");

    // Two paths publishing one Channel ID. Without these, one collapses into the
    // other and the feed reads as though a publisher went backwards.
    assert!(event.contains("source_addr"), "event key: {event}");
    assert!(event.contains("dst_port"), "event key: {event}");
    // A duplicated datagram: same sequence number, same message index, different
    // arrival. Without this it deletes the original rather than sitting beside it.
    assert!(event.contains("recv_ts"), "event key: {event}");
    // Several messages for one instrument packed into one datagram.
    assert!(event.contains("message_index"), "event key: {event}");
    // And the instrument leads, because the dominant question is per instrument
    // over a window — the one place these keys depart from `datagram`'s.
    assert!(
        event.starts_with("channel_id, instrument_id"),
        "event key does not lead with the instrument: {event}"
    );
    // And no era column, for the reason `datagram` has none: an era's anchor is
    // only observable as the first datagram of that era *in this object*, so a
    // stored one splits an era across prefixes. The era is a range join.
    assert!(
        !event.contains("era_anchor_ts"),
        "event stores a per-object era anchor: {event}"
    );

    let book_top = sort_key(sql, "book_top");
    assert!(book_top.contains("message_index"), "book_top: {book_top}");
    assert!(book_top.contains("observation"), "book_top: {book_top}");

    // An era belongs to one channel instance, so an instrument table keyed
    // without the address and the port merges two eras that are not the same era.
    let instrument = sort_key(sql, "instrument");
    assert!(instrument.contains("source_addr"), "{instrument}");
    assert!(instrument.contains("dst_port"), "{instrument}");
    // Keyed on where the statement came into force, which is identical in every
    // object that carries it, so two loads of one era replace rather than
    // accumulate.
    assert!(instrument.contains("from_sequence"), "{instrument}");

    // The rest of the identity block, on all three. Two recorders at one site
    // see the same datagrams and agree on channel, instrument, sequence number
    // and index; `recv_ts` differing is two clocks not colliding rather than a
    // key, and `book_top` folds site and recorder into `observation`. `env` is
    // the same argument across a boundary nothing else in the row crosses, and
    // `feed` is what stops two feeds sharing a Channel ID and an Instrument ID
    // from merging on the strength of that coincidence.
    for (name, key) in [
        ("event", &event),
        ("book_top", &book_top),
        ("instrument", &instrument),
    ] {
        for column in ["env", "feed"] {
            assert!(
                key.contains(column),
                "{name} key omits {column}, so two of them merge: {key}"
            );
        }
    }
    assert!(event.contains("recorder"), "event key: {event}");
    assert!(instrument.contains("recorder"), "{instrument}");
    assert!(
        book_top.contains("observation"),
        "book_top names its vantage through `observation`: {book_top}"
    );

    // And `port_role` is in none of them, deliberately: it is recoverable from
    // `dst_port`, which is in every key that has a channel instance in it, so
    // keying on the name beside the number widens every key to restate a fact.
    // `book_top` has no such column at all, because a book spans port roles.
    for (name, key) in [
        ("event", &event),
        ("book_top", &book_top),
        ("instrument", &instrument),
    ] {
        assert!(
            !key.contains("port_role"),
            "{name} key restates the port as a name as well as a number: {key}"
        );
    }
}

/// The retention split, one table further down than `002` put it.
#[test]
fn the_market_data_retention_expires_the_events_and_keeps_the_book() {
    let sql = market_data_sql();
    assert!(
        sql.contains("ALTER TABLE recorder.event")
            && sql.contains("MODIFY TTL toDateTime(recv_ts) + INTERVAL 2 DAY"),
        "the expensive base table has no TTL"
    );
    assert!(
        sql.contains("ALTER TABLE recorder.book_top")
            && sql.contains("MODIFY TTL toDateTime(recv_ts) + INTERVAL 30 DAY"),
        "the derived table's longer window is not stated"
    );
    // `instrument` is what makes every other row's symbol and exponents mean
    // anything after the fact. Expiring it leaves prices that no longer decode.
    assert!(
        !sql.contains("ALTER TABLE recorder.instrument"),
        "reference data must not expire"
    );
    // Whole days, so a TTL is a partition drop rather than a treadmill of part
    // rewrites — the reason `002` gives for the same shape.
    for window in ["INTERVAL 2 DAY", "INTERVAL 30 DAY"] {
        assert!(
            sql.contains(window),
            "{window} is not a whole number of days"
        );
    }
}

/// The columns that can be unknown are nullable, on the market data tables too.
#[test]
fn the_market_data_columns_that_can_be_unknown_are_nullable() {
    let sql = market_data_sql();
    for column in [
        // The sentinel translation's destination. A count the venue does not
        // expose is absent, not sixty-five thousand.
        "order_count        Nullable(UInt16)",
        "level_index        Nullable(UInt16)",
        // A message that carries no venue time.
        "upstream_ts        Nullable(DateTime64(9))",
        // The reset's recovery anchor, and the snapshot's.
        "anchor_seq         Nullable(UInt64)",
        // Absent rather than zero: a zero reads as a feed publishing nothing.
        "declared_count Nullable(UInt32)",
        // Certain rows have no sequence number to point at.
        "uncertain_since   Nullable(UInt64)",
    ] {
        assert!(sql.contains(column), "not nullable: {column}");
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
    let expected = [
        (rows_sql(), Grain::Datagram, "toYYYYMMDD(recv_ts)"),
        (rows_sql(), Grain::Era, "toYYYYMMDD(anchor_ts)"),
        (rows_sql(), Grain::SegmentCoverage, "toYYYYMMDD(start_ts)"),
        (rows_sql(), Grain::SequenceGap, "toYYYYMMDD(before_ts)"),
        (
            rows_sql(),
            Grain::ConformanceFinding,
            "toYYYYMMDD(window_start)",
        ),
        (market_data_sql(), Grain::Event, "toYYYYMMDD(recv_ts)"),
        (
            market_data_sql(),
            Grain::Instrument,
            "toYYYYMMDD(first_seen_ts)",
        ),
        (market_data_sql(), Grain::BookTop, "toYYYYMMDD(recv_ts)"),
    ];
    for (sql, grain, partition) in expected {
        assert!(
            sql.contains(&format!("PARTITION BY {partition}")),
            "{grain} is not partitioned by {partition}"
        );
    }
    // One `PARTITION BY` per table, so a table added later without one fails
    // here rather than being noticed on a graph months afterwards.
    assert_eq!(
        rows_sql().matches("PARTITION BY ").count(),
        TRANSPORT_GRAINS.len(),
        "a table in 001 has no PARTITION BY, or one has two"
    );
    assert_eq!(
        market_data_sql().matches("PARTITION BY ").count(),
        MARKET_DATA_GRAINS.len(),
        "a table in 005 has no PARTITION BY, or one has two"
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

/// The pairing numbers the occurrences, and never joins on the key alone.
///
/// `state_key` is not unique and must not be — a book returning to a previous
/// state produces the same key again — so a join on the key is a cross product
/// on any instrument that oscillates, and `ASOF` is the obvious repair and the
/// wrong one: it selects the nearest right-hand row independently for each
/// left-hand row, with no notion of consuming a match, so several occurrences at
/// one observation point all pair with the same occurrence at the other. The
/// lead times that come out are plausible, biased, and counted from one arrival
/// several times, which is why the reasoning is required to be in the file and
/// not only in a review.
#[test]
fn the_race_numbers_the_occurrences_rather_than_pairing_by_proximity() {
    let sql = pairing_sql();
    assert!(
        sql.contains("row_number() OVER ("),
        "the ordinal is a window function over the rows and nothing else"
    );
    assert!(
        sql.contains("PARTITION BY b.observation, b.channel_id, b.instrument_id,")
            && sql.contains("e.anchor_ts, b.state_key"),
        "the ordinal is per observation point, per instrument, per era, per state"
    );
    assert!(
        sql.contains("ORDER BY b.recv_ts"),
        "and it is ordered by the arrival, which is what a race compares"
    );

    let race = view_body(sql, "book_top_race");
    assert!(
        !race.contains("ASOF") && !race.contains("JOIN"),
        "the pairing is an aggregate over the ordinal, not a join: {race}"
    );
    assert!(
        sql.contains("no notion of consuming a match"),
        "why `ASOF` is wrong here belongs beside the thing that does not use it"
    );
}

/// A snapshot-derived row is excluded *before* the numbering.
///
/// A snapshot anchors a book and never times one: the runtime pulls it on its
/// own cadence and the archive records when it was published rather than when it
/// was asked for, so its arrival stamp measures the publisher's scheduler. The
/// filter is in the same statement as the window, where `WHERE` runs first and
/// an anchor row consumes no ordinal. Filtered afterwards it would leave every
/// later occurrence at that observation point numbered one too high — which does
/// not read as a mistake downstream, it reads as a lead time.
#[test]
fn an_anchor_row_takes_no_ordinal_because_it_is_filtered_before_the_window() {
    let occurrence = view_body(pairing_sql(), "book_top_occurrence");
    assert!(
        occurrence.contains("row_number() OVER (")
            && occurrence.contains("WHERE b.from_anchor = 0"),
        "the exclusion and the numbering are one statement: {occurrence}"
    );
    assert!(
        !view_body(pairing_sql(), "book_top_race").contains("from_anchor"),
        "so nothing below has to remember to repeat it"
    );
    assert!(
        pairing_sql().contains("measures the publisher's scheduler"),
        "why a snapshot is not an observation has to be stated where it is excluded"
    );
}

/// The era is in the numbering and not in the pairing, and that asymmetry is the
/// point.
///
/// An `Instrument ID` is unique within an era, so one point's own ordinals must
/// not run across a boundary. But an era's stored identity is its anchor, and an
/// anchor is a *receive* stamp — one observation point's observation of that
/// era. Two recorders of one feed open their eras at two instants and two
/// transports share no sequence space at all, so a pairing grouped on any era
/// column pairs nothing across observation points and reports a total outage as
/// a clean feed.
#[test]
fn the_pairing_groups_on_the_state_and_the_ordinal_and_on_no_era() {
    let sql = pairing_sql();
    assert!(
        sql.contains("GROUP BY channel_id, instrument_id, state_key, occurrence"),
        "the pairing key is the state and its ordinal"
    );
    let race = view_body(sql, "book_top_race");
    assert!(
        !race.contains("GROUP BY channel_id, instrument_id, state_key, occurrence, era")
            && !race.contains("era_anchor_ts,\n"),
        "an era column in the grouping would pair nothing at all: {race}"
    );
    assert!(
        sql.contains("uniqExact(observation)"),
        "and distinct observation points are counted, not rows, because an \
         ordinal restarts at each era"
    );
}

/// An occurrence with no counterpart is a row, and its lead time is null.
///
/// The fact worth seeing: it usually means one observation point missed a state
/// the other saw. A join would have dropped it, and a zero lead would have
/// entered every average over the column as evidence that the two paths tied.
#[test]
fn an_unpaired_occurrence_is_visible_and_carries_no_lead_time() {
    let race = view_body(pairing_sql(), "book_top_race");
    assert!(
        race.contains("if(uniqExact(observation) > 1,"),
        "the lead exists only where there were two points to measure between"
    );
    assert!(
        race.contains("NULL)") && race.contains("AS lead_ms"),
        "and is null rather than zero otherwise: {race}"
    );
    assert!(
        race.contains("groupUniqArray(observation)"),
        "the row names the points that saw the state, so an unpaired one is \
         readable rather than merely present"
    );
    assert!(
        pairing_sql().contains("bound is a property of the two paths"),
        "the |Δt| bound is the caller's, and why has to be written down"
    );
}

/// The replacing collapse is applied once, below everything that numbers.
///
/// A re-run after a fix is a replace, so between the second load and the merge
/// one arrival is in the table twice. Numbered without the collapse the
/// duplicate becomes a second occurrence, and it does not merely inflate a
/// count: the surplus occurrences pair with each other and the last one at each
/// point pairs with nothing, so a re-load reports states both points saw as
/// states one of them missed.
#[test]
fn the_collapse_is_applied_once_beneath_the_numbering() {
    let sql = pairing_sql();
    assert_eq!(
        sql.matches("recorder.book_top FINAL").count(),
        1,
        "the collapse is written once"
    );
    assert!(
        view_body(sql, "book_top_occurrence").contains("FROM recorder.book_top_settled AS b"),
        "and the numbering reads the collapsed view rather than the table"
    );
    assert!(
        !view_body(sql, "book_top_race").contains("FINAL"),
        "nothing above it pays for the collapse a second time"
    );
    assert!(
        sql.contains("manufacture evidence of loss"),
        "what a duplicate would do here is worse than a double count, and the \
         file has to say so"
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

    // The three views of the pairing, and nothing split across two of them.
    let pairing = migration("006_recorder_book_top_pairing.sql").statements();
    assert_eq!(pairing.len(), 3, "three views");
    for view in ["book_top_settled", "book_top_occurrence", "book_top_race"] {
        assert_eq!(
            pairing
                .iter()
                .filter(|s| s.contains(&format!("CREATE OR REPLACE VIEW recorder.{view} AS")))
                .count(),
            1,
            "{view}"
        );
    }

    // The seven views of the cross-site join, and nothing split across two.
    let cross_site = migration("007_recorder_cross_site.sql").statements();
    assert_eq!(cross_site.len(), 7, "seven views");
    for view in [
        "segment_overflow",
        "gap_missing_seq",
        "instance_vantage_day",
        "gap_vantage_seq",
        "gap_cross_site_evidence",
        "gap_sent_elsewhere",
        "sequence_gap_cross_site",
    ] {
        assert_eq!(
            cross_site
                .iter()
                .filter(|s| s.contains(&format!("CREATE OR REPLACE VIEW recorder.{view} AS")))
                .count(),
            1,
            "{view}"
        );
    }

    // The five tables and the database, and nothing split across two of them.
    let statements = migration("001_recorder_rows.sql").statements();
    assert_eq!(statements.len(), 6, "one database and five tables");
    for grain in TRANSPORT_GRAINS {
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

/// The escalation reads a `NULL` as unknown and never as a `no`.
///
/// `seen_elsewhere` is three-valued and the whole column exists for the third
/// value: `1` present elsewhere, `0` absent at every vantage that could speak,
/// `NULL` nobody else could speak yet. A condition written `!= 1` promotes on
/// every one of those nulls — a site that has not loaded, a site that
/// overflowed, a site that went quiet — and each of those is a `publisher`
/// finding drawn from an archive that did not look.
#[test]
fn the_cross_site_escalation_tests_absence_and_never_the_absence_of_presence() {
    let view = view_body(cross_site_sql(), "sequence_gap_cross_site");
    assert!(
        view.contains("ifNull(seen_elsewhere = 0, 0)"),
        "the promotion is on a known absence: {view}"
    );
    assert!(
        !view.contains("seen_elsewhere != 1") && !view.contains("seen_elsewhere <> 1"),
        "and never on the absence of a presence: {view}"
    );
    // And only ever upwards from `unverifiable`. The other three verdicts are
    // exculpatory and decided from evidence one object holds; nothing found at
    // another site makes a gap our own ring admitted anything other than ours.
    assert!(
        view.contains("if(g.verdict = 'unverifiable'"),
        "the escalation runs from one verdict only: {view}"
    );
    assert_eq!(
        view.matches("'publisher'").count(),
        1,
        "and writes the accusation in exactly one place"
    );
    assert!(
        cross_site_sql().contains("promotes on ignorance"),
        "why `!= 1` is wrong has to be written where the condition is"
    );
}

/// Absence is decided on rows that have no TTL, and the base rows are read for
/// one thing only.
///
/// The obvious join expands the missing sequence numbers and looks for them in
/// `datagram` at the other sites. That answers *present* correctly and *absent*
/// catastrophically: `datagram` is the one table `002` expires, so two days on
/// every sequence number looks absent everywhere and every stale gap in the
/// archive is promoted to `publisher` on a timer.
#[test]
fn the_cross_site_absence_is_read_from_the_rows_that_outlive_the_datagrams() {
    let sql = cross_site_sql();
    assert_eq!(
        sql.matches("recorder.datagram").count(),
        1,
        "the base rows are read once, and it is not for the verdict"
    );
    assert!(
        view_body(sql, "gap_sent_elsewhere").contains("recorder.datagram"),
        "the one read is the send stamps, which only a site that received the \
         datagram can supply"
    );
    for view in ["gap_vantage_seq", "gap_cross_site_evidence"] {
        assert!(
            !view_body(sql, view).contains("recorder.datagram"),
            "{view} decides admissibility and must not read an expiring table"
        );
    }
    assert!(
        view_body(sql, "gap_vantage_seq").contains("recorder.segment_overflow")
            && view_body(sql, "gap_vantage_seq").contains("recorder.gap_missing_seq"),
        "it reads the coverage rows and the other sites' own gap rows, which \
         within a covered range are exhaustive"
    );
}

/// Both sides of the vantage join are bounded in time, because the sequence
/// space repeats.
///
/// A `Reset Count` restarts the numbering, so `(instance, sequence number)` is a
/// key one instance revisits era after era — which is why `era_anchor_ts` is in
/// `sequence_gap`'s sort key at all. Bounding only the coverage row leaves the
/// other half open: a gap that vantage recorded at this number in an earlier era
/// answers for the datagram missing now, as *missed* and, with its own stale
/// residue, as an admissible absence. That is the accusing direction, on
/// evidence about a different datagram.
#[test]
fn the_cross_site_vantage_join_bounds_the_gap_rows_as_well_as_the_coverage_rows() {
    let view = view_body(cross_site_sql(), "gap_vantage_seq");
    assert!(
        view.contains("AND o.start_ts <= m.after_ts")
            && view.contains("AND o.end_ts   >= m.before_ts"),
        "a coverage row speaks only over the bracket the datagram was sent in: {view}"
    );
    // Against the admitting segment's window and never our own bracket: two
    // sites' brackets are readings of two clocks at two ends of a path, and
    // requiring theirs to overlap ours rejects the ordinary case where both
    // really did miss the datagram — which reads as *held*, and exonerates.
    assert!(
        view.contains("arrayFilter(x -> x.1 <= o.end_ts AND x.2 >= o.start_ts"),
        "and its gap rows are narrowed to that same window, on that same host's \
         clock: {view}"
    );
    // In the match and not after it: a vantage whose only gap at this number is
    // an old one held the datagram now, and a filter applied to the result would
    // drop its row and turn a site that spoke into a site that was silent.
    assert!(
        view.contains(
            "GROUP BY site, recorder, source_addr, channel_id, dst_port, sequence_number"
        ),
        "folded to one row per vantage and number, so the window narrows the \
         evidence rather than the rows: {view}"
    );
    assert!(
        cross_site_sql().contains("THE SEQUENCE SPACE REPEATS"),
        "and why both halves need it is written where the join is"
    );
}

/// Overflow is read as a delta, and a missing predecessor is unknown rather
/// than clean.
///
/// `capture_drop_total` is cumulative and never resets, so a host that dropped a
/// burst an hour ago carries it for ever: a rule reading the total would find no
/// site admissible on any host that ever overflowed, and one reading a
/// defaulted predecessor as zero would admit exactly the absence a missing
/// segment conceals.
#[test]
fn the_cross_site_overflow_test_is_a_delta_with_no_predecessor_left_unknown() {
    let view = view_body(cross_site_sql(), "segment_overflow");
    assert!(
        view.contains("p.present = 1 AND p.segment_seq + 1 = c.segment_seq"),
        "adjacency is checked and never assumed: {view}"
    );
    assert!(
        view.contains("NULL) AS capture_drop_delta"),
        "and a delta with no predecessor is null rather than zero: {view}"
    );
    assert!(
        view.contains("if(isNull(capture_drop_delta), NULL, toUInt8(capture_drop_delta = 0))"),
        "so unknown and clean stay two answers: {view}"
    );
    assert!(
        cross_site_sql().contains("where an unaccounted burst hides"),
        "why a hole is not a zero has to be stated where the null is written"
    );
}

/// The evidence is counted in distinct vantages and distinct sequence numbers,
/// never in rows.
///
/// A re-run after an analyser fix is a replace, and between the second load and
/// the merge every row is in the tables twice. Counted as rows, one site's
/// single absence is two — and since the verdict turns on how many sites agreed,
/// that is the one arithmetic error here that promotes a finding on evidence
/// nobody has. It is the same reason `006` counts `uniqExact(observation)`.
#[test]
fn the_cross_site_evidence_counts_distinct_vantages_and_never_rows() {
    let view = view_body(cross_site_sql(), "gap_cross_site_evidence");
    assert!(
        view.contains("uniqExactIf(other_site, absence_admissible = 1)      AS absent_sites"),
        "the sites that agreed are distinct sites: {view}"
    );
    assert!(
        !view.contains("count()") && !view.contains("countIf(") && !view.contains("sum("),
        "and nothing here counts rows: {view}"
    );
    assert!(
        view_body(cross_site_sql(), "gap_missing_seq").contains("recorder.sequence_gap FINAL"),
        "the collapse is applied once, beneath the expansion"
    );
    assert_eq!(
        cross_site_sql()
            .matches("recorder.sequence_gap FINAL")
            .count(),
        1,
        "and written once, so nothing above pays for it twice"
    );
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
        BookTop, ConformanceFinding, Datagram, DropScope, Era, Event, FindingVerdict, Instrument,
        MessageTypeLabel, Nanos, PortRoleLabel, RecvTsKindLabel, SegmentCoverage, SequenceGap,
        UncertainReason, Verdict,
    };

    const ADDR: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 10);

    pub fn event() -> Event {
        Event {
            recv_ts: Nanos(0),
            send_ts: Nanos(0),
            upstream_ts: None,
            recv_ts_kind: RecvTsKindLabel::KernelSoftware,
            site: String::new(),
            recorder: String::new(),
            env: String::new(),
            feed: String::new(),
            port_role: PortRoleLabel::Mktdata,
            source_addr: ADDR,
            channel_id: 0,
            dst_port: 0,
            sequence_number: 0,
            reset_count: 0,
            segment_seq: 0,
            message_index: 0,
            source_id: 0,
            instrument_id: 0,
            symbol: String::new(),
            price_exp: 0,
            qty_exp: 0,
            per_instrument_seq: None,
            message_type: MessageTypeLabel::Quote,
            side_raw: None,
            action_raw: None,
            reason_raw: None,
            flags_raw: None,
            price_raw: None,
            qty_raw: None,
            order_count: None,
            level_index: None,
            bid_px_raw: None,
            bid_qty_raw: None,
            bid_source_count: None,
            ask_px_raw: None,
            ask_qty_raw: None,
            ask_source_count: None,
            trade_id: None,
            cumulative_volume: None,
            snapshot_id: None,
            anchor_seq: None,
            total_levels: None,
            levels_seen: None,
            depth_bound: None,
            object_key: String::new(),
            object_sha256: String::new(),
            datagram_index: 0,
        }
    }

    pub fn instrument() -> Instrument {
        Instrument {
            site: String::new(),
            recorder: String::new(),
            env: String::new(),
            feed: String::new(),
            port_role: PortRoleLabel::Refdata,
            source_addr: ADDR,
            channel_id: 0,
            dst_port: 0,
            source_id: 0,
            instrument_id: 0,
            from_sequence: 0,
            reset_count: 0,
            symbol: String::new(),
            price_exp: 0,
            qty_exp: 0,
            contract_value: 0,
            first_seen_ts: Nanos(0),
            last_seen_ts: Nanos(0),
            manifest_seq: None,
            declared_count: None,
            object_key: String::new(),
        }
    }

    pub fn book_top() -> BookTop {
        BookTop {
            recv_ts: Nanos(0),
            send_ts: Nanos(0),
            site: String::new(),
            recorder: String::new(),
            env: String::new(),
            feed: String::new(),
            observation: String::new(),
            source_addr: ADDR,
            channel_id: 0,
            dst_port: 0,
            source_id: 0,
            instrument_id: 0,
            symbol: String::new(),
            sequence_number: 0,
            message_index: 0,
            reset_count: 0,
            segment_seq: 0,
            bid_px_raw: None,
            bid_qty_raw: None,
            bid_source_count: None,
            ask_px_raw: None,
            ask_qty_raw: None,
            ask_source_count: None,
            price_exp: 0,
            qty_exp: 0,
            state_key: 0,
            from_anchor: 0,
            book_certain: 1,
            uncertain_since: None,
            uncertain_reason: UncertainReason::None,
            object_key: String::new(),
        }
    }

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

    // INSERT on all five, and nothing else.
    for grain in Grain::ALL {
        assert!(
            user.contains(&format!("GRANT INSERT ON recorder.{}", grain.table())),
            "{grain} cannot be written"
        );
    }
    // And SELECT on nothing at all: the adjacency check reads the preceding
    // trailer from the loader's own ledger and from what it is still holding,
    // never from the destination, and `--check`'s `SELECT 1` reads no table.
    // An unused grant in the file whose argument is least privilege is the one
    // a later reader takes as permission to write the query it describes.
    assert!(
        !user.contains("GRANT SELECT"),
        "a read privilege for a read nothing performs: {user}"
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
