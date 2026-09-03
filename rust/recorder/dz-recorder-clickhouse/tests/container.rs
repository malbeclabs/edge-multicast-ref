//! The DDL against a real server: a load, a re-load, the arithmetic, and the
//! TTL.
//!
//! Behind `--features clickhouse-tests`, because `cargo test` must need nothing
//! of the host. When the feature is on these tests **fail rather than skip** if
//! the server is absent: a gate that passes when it could not run reports a
//! clean schema for a schema nobody applied.
//!
//! `DZ_LOADER_CLICKHOUSE_URL` points at the server, defaulting to the local
//! address the demo's container publishes. The tests create their own database
//! and drop it, so they cannot disturb one that is being used.
#![cfg(feature = "clickhouse-tests")]
#![forbid(unsafe_code)]

mod common;

use common::batch;
use dz_recorder_clickhouse::{migrations, ClickHouseConfig, ClickHouseSink};
use dz_recorder_replay::Fault;
use dz_recorder_rows::{Grain, RowSink};

const URL_ENV: &str = "DZ_LOADER_CLICKHOUSE_URL";
const DEFAULT_URL: &str = "http://127.0.0.1:8123";

/// A database of this test's own, so a run cannot disturb a live one.
struct Scratch {
    sink: ClickHouseSink<dz_recorder_clickhouse::HttpTransport>,
    database: String,
}

impl Scratch {
    fn open(tag: &str) -> Self {
        let endpoint = std::env::var(URL_ENV).unwrap_or_else(|_| DEFAULT_URL.to_owned());
        let database = format!("recorder_test_{tag}");

        // Against the server's own default database, to create ours.
        let admin = ClickHouseSink::over_http(ClickHouseConfig {
            endpoint: endpoint.clone(),
            database: "default".to_owned(),
            ..ClickHouseConfig::default()
        });
        admin
            .statement(&format!("DROP DATABASE IF EXISTS {database}"))
            .unwrap_or_else(|e| {
                panic!(
                    "this suite is enabled, so a server that is not there is a failure and \
                     never a skip. Set {URL_ENV} or start the container the demo \
                     provisions: {e}"
                )
            });
        admin
            .statement(&format!("CREATE DATABASE {database}"))
            .expect("the scratch database is creatable");

        let sink = ClickHouseSink::over_http(ClickHouseConfig {
            endpoint,
            database: database.clone(),
            ..ClickHouseConfig::default()
        });
        // The checked-in files, applied exactly as a deploy applies them — with
        // `recorder.` rewritten to this test's database, which is the only thing
        // a scratch run may change about them.
        for migration in migrations() {
            for statement in migration.statements() {
                let statement = statement.replace("recorder.", &format!("{database}."));
                if statement.contains("CREATE DATABASE") {
                    continue;
                }
                sink.statement(&statement)
                    .unwrap_or_else(|e| panic!("{}: {statement}\n{e}", migration.name));
            }
        }
        Self { sink, database }
    }

    /// One scalar, as text.
    fn scalar(&self, sql: &str) -> String {
        self.sink
            .statement(sql)
            .expect("the query runs")
            .trim()
            .to_owned()
    }

    fn count(&self, table: &str) -> u64 {
        self.scalar(&format!(
            "SELECT count() FROM {}.{table} FINAL",
            self.database
        ))
        .parse()
        .expect("count() is a number")
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = self
            .sink
            .statement(&format!("DROP DATABASE IF EXISTS {}", self.database));
    }
}

/// Every row lands in the column the DDL declares, which is the thing no
/// literal-based test can prove: a `JSONEachRow` body is accepted or refused by
/// the server and by nobody else.
#[test]
fn the_checked_in_ddl_accepts_what_the_loader_sends() {
    let mut scratch = Scratch::open("load");
    let rows = batch(500, Fault::SequenceGap);
    let expected: Vec<(Grain, usize)> = Grain::ALL.iter().map(|g| (*g, rows.rows(*g))).collect();

    let written = scratch.sink.write_batch(rows).expect("the batch lands");
    for (grain, count) in expected {
        assert_eq!(written.rows(grain), count as u64, "{grain}");
        assert_eq!(scratch.count(grain.table()), count as u64, "{grain}");
    }
}

/// A re-run after an analyser fix is a replace and not a duplication.
///
/// This is the property the whole tier's idempotence rests on, and it is one
/// only a real merge can demonstrate: `ReplacingMergeTree` collapses rows on the
/// sort key, and `OPTIMIZE ... FINAL` is what makes that happen now rather than
/// eventually.
#[test]
fn loading_the_same_object_twice_replaces_rather_than_duplicates() {
    let mut scratch = Scratch::open("reload");
    let rows = batch(500, Fault::SequenceGap);
    let once = rows.clone();
    let datagrams = rows.rows(Grain::Datagram) as u64;
    let gaps = rows.rows(Grain::SequenceGap) as u64;

    scratch.sink.write_batch(rows).expect("the first load");
    scratch.sink.write_batch(once).expect("the second load");
    scratch
        .sink
        .statement(&format!(
            "OPTIMIZE TABLE {}.datagram FINAL DEDUPLICATE",
            scratch.database
        ))
        .expect("the merge runs");
    scratch
        .sink
        .statement(&format!(
            "OPTIMIZE TABLE {}.sequence_gap FINAL DEDUPLICATE",
            scratch.database
        ))
        .expect("the merge runs");

    assert_eq!(scratch.count("datagram"), datagrams);
    assert_eq!(scratch.count("sequence_gap"), gaps);
}

/// Span minus count, over the real rows, in the engine's own arithmetic.
///
/// Valid at *this* grain and invalid one grain up: a datagram carrying no quote
/// still consumes a sequence number, so the same subtraction against a decoded
/// per-message table reports a fixed fraction of every feed as missing at every
/// site at once. The fixture is heartbeats for exactly that reason.
#[test]
fn span_minus_count_is_the_loss_at_the_datagram_grain() {
    let mut scratch = Scratch::open("arithmetic");
    let clean = batch(500, Fault::None);
    scratch.sink.write_batch(clean).expect("the clean load");
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT max(sequence_number) - min(sequence_number) + 1 - count() \
             FROM {}.datagram FINAL",
            scratch.database
        )),
        "0",
        "a clean segment of heartbeats has no missing sequence value"
    );

    let mut scratch = Scratch::open("arithmetic_gap");
    let with_gap = batch(500, Fault::SequenceGap);
    let missing: u64 = with_gap.sequence_gap.iter().map(|g| g.missing_count).sum();
    scratch.sink.write_batch(with_gap).expect("the load");
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT toString(max(sequence_number) - min(sequence_number) + 1 - count()) \
             FROM {}.datagram FINAL",
            scratch.database
        )),
        missing.to_string(),
        "the engine and the deriver disagree about how much is missing"
    );
}

/// The era rank is a view over the openings, and it is dense.
#[test]
fn the_era_rank_view_numbers_the_openings_densely() {
    let mut scratch = Scratch::open("era");
    let rows = batch(200, Fault::ResetCountAdvance);
    scratch.sink.write_batch(rows).expect("the load");

    assert_eq!(
        scratch.scalar(&format!(
            "SELECT groupArray(era_index) FROM (SELECT era_index FROM {}.era_ranked \
             ORDER BY anchor_ts)",
            scratch.database
        )),
        "[1,2]",
        "two openings, numbered from one"
    );
    // And a datagram resolves to an era by range join, with the base table
    // storing no rank at all.
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT countDistinct(era_index) FROM {}.datagram_in_era",
            scratch.database
        )),
        "2"
    );
}

/// The base rows expire and the derived rows do not.
///
/// Inserted dated past the window, then merged: the TTL is a property of the
/// table and only a merge applies it, so a test that asserted the TTL clause
/// alone would assert that somebody typed it.
#[test]
fn the_ttl_expires_the_base_rows_and_leaves_the_derived_ones() {
    let mut scratch = Scratch::open("ttl");
    let rows = batch(100, Fault::SequenceGap);
    let gaps = rows.rows(Grain::SequenceGap) as u64;
    let coverage = rows.rows(Grain::SegmentCoverage) as u64;
    scratch.sink.write_batch(rows).expect("the load");

    // The fixture's own stamps are in the past by years, so the TTL applies to
    // every base row it wrote.
    for table in ["datagram", "sequence_gap", "segment_coverage", "era"] {
        scratch
            .sink
            .statement(&format!(
                "OPTIMIZE TABLE {}.{table} FINAL",
                scratch.database
            ))
            .expect("the merge runs");
    }

    assert_eq!(
        scratch.count("datagram"),
        0,
        "the base rows are past the retention window and a merge has run"
    );
    assert_eq!(
        scratch.count("sequence_gap"),
        gaps,
        "the finding survives the rows it was derived from — which is the whole \
         point of the split"
    );
    assert_eq!(
        scratch.count("segment_coverage"),
        coverage,
        "and so does what says whether the window was ever covered: that is the \
         difference between `no loss` and `nothing kept`"
    );
}
