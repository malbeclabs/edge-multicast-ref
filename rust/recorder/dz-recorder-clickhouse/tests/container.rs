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
/// Applied by the one test that is about retention. See [`Scratch::open`].
const RETENTION: &str = "002_recorder_retention.sql";

/// A database of this test's own, so a run cannot disturb a live one.
struct Scratch {
    sink: ClickHouseSink<dz_recorder_clickhouse::HttpTransport>,
    database: String,
}

impl Scratch {
    /// The tables and the views, and **not** the retention migration.
    ///
    /// The fixtures here carry the synthetic publisher's own receive stamps,
    /// which are years in the past on purpose: nine populated digits, so a
    /// writer or a reader that rounded to microseconds could not pass a
    /// comparison against them. A row-level TTL is applied as the part is
    /// written, so a two-day `datagram` TTL expires every row this suite inserts
    /// in the same step that inserts it — and every count below would come back
    /// zero with every insert answered `200`, which is indistinguishable from a
    /// schema that never accepted the rows.
    ///
    /// So retention is applied by the one test that is about retention, after it
    /// has established that the rows were there to begin with. That is also the
    /// stronger assertion: a TTL test over rows that were never loaded passes
    /// vacuously.
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
        let scratch = Self { sink, database };
        for migration in migrations() {
            if migration.name == RETENTION {
                continue;
            }
            scratch.apply(migration.name);
        }
        scratch
    }

    /// One checked-in file, applied exactly as a deploy applies it — with
    /// `recorder.` rewritten to this test's database, which is the only thing a
    /// scratch run may change about them.
    fn apply(&self, name: &str) {
        let migration = migrations()
            .into_iter()
            .find(|m| m.name == name)
            .unwrap_or_else(|| panic!("no migration named {name}"));
        for statement in migration.statements() {
            let statement = statement.replace("recorder.", &format!("{}.", self.database));
            if statement.contains("CREATE DATABASE") {
                continue;
            }
            self.sink
                .statement(&statement)
                .unwrap_or_else(|e| panic!("{}: {statement}\n{e}", migration.name));
        }
    }

    /// `OPTIMIZE ... FINAL`, which is what makes a merge happen now rather than
    /// eventually: both the replacing collapse and the TTL are things a merge
    /// applies.
    ///
    /// `FINAL` alone and never `DEDUPLICATE`: the collapse under test is the one
    /// the sort key performs, and `DEDUPLICATE` removes identical rows whatever
    /// the engine — so it would pass over a key that deduplicates nothing.
    fn merge(&self, table: &str) {
        self.sink
            .statement(&format!("OPTIMIZE TABLE {}.{table} FINAL", self.database))
            .unwrap_or_else(|e| panic!("merging {table}: {e}"));
    }

    /// One scalar, as text.
    ///
    /// The query is in the panic message, because a failure here is a query the
    /// server refused and its own message names the reason.
    fn scalar(&self, sql: &str) -> String {
        self.sink
            .statement(sql)
            .unwrap_or_else(|e| panic!("{sql}\n{e}"))
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
    scratch.merge("datagram");
    scratch.merge("sequence_gap");

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
    // And a datagram resolves to an era by range join on the anchor, with no
    // rank on that path and no `FINAL`: the rank is an all-history computation
    // and a panel must not be behind one.
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT countDistinct(era_anchor_ts) FROM {}.datagram_in_era",
            scratch.database
        )),
        "2",
        "each datagram resolved to one of the two openings"
    );
    // Every datagram resolved to something: a LEFT join that dropped rows would
    // understate the traffic, and one that resolved them all to the *first* era
    // would hide the reset.
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT count() FROM {}.datagram_in_era WHERE era_anchor_ts > 0",
            scratch.database
        )),
        scratch.count("datagram").to_string()
    );
    // The settled view is the engine's own collapse: one row per opening, as
    // `FINAL` would have given.
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT count() FROM {}.era_settled",
            scratch.database
        )),
        "2"
    );
}

/// The base rows expire and the derived rows do not.
///
/// Retention is applied *after* the load and the rows are counted before it, so
/// the assertion is that the TTL took the base rows and left the derived ones —
/// not that a table nobody loaded is empty. A TTL test over rows that were never
/// there passes vacuously, which is the failure mode this ordering rules out.
///
/// The TTL is a property of the table and only a merge applies it, so a test
/// asserting the clause alone would assert that somebody typed it.
#[test]
fn the_ttl_expires_the_base_rows_and_leaves_the_derived_ones() {
    let mut scratch = Scratch::open("ttl");
    let rows = batch(100, Fault::SequenceGap);
    let datagrams = rows.rows(Grain::Datagram) as u64;
    let gaps = rows.rows(Grain::SequenceGap) as u64;
    let coverage = rows.rows(Grain::SegmentCoverage) as u64;
    let eras = rows.rows(Grain::Era) as u64;
    assert!(datagrams > 0 && gaps > 0 && coverage > 0 && eras > 0);
    scratch.sink.write_batch(rows).expect("the load");

    // Before retention: everything is there. Without this the assertions below
    // would hold over a database nothing had ever been loaded into.
    assert_eq!(scratch.count("datagram"), datagrams);
    assert_eq!(scratch.count("sequence_gap"), gaps);

    // Now the retention migration. The fixture's own stamps are years in the
    // past, so every base row it wrote is outside the window.
    scratch.apply(RETENTION);
    for table in ["datagram", "sequence_gap", "segment_coverage", "era"] {
        scratch.merge(table);
    }

    assert_eq!(
        scratch.count("datagram"),
        0,
        "the base rows are outside the retention window and a merge has run"
    );
    assert_eq!(
        scratch.count("sequence_gap"),
        gaps,
        "the finding survives the rows it was derived from, which is the whole \
         point of the split"
    );
    assert_eq!(
        scratch.count("segment_coverage"),
        coverage,
        "and so does what says whether the window was ever covered: that is the \
         difference between `no loss` and `nothing kept`"
    );
    assert_eq!(scratch.count("era"), eras, "and the era boundaries");
}
