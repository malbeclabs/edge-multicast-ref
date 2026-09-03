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

use common::{batch, batch_on_role};
use dz_edge_core::PortRole;
use dz_recorder_clickhouse::{migrations, schema, ClickHouseConfig, ClickHouseSink};
use dz_recorder_replay::Fault;
use dz_recorder_rows::{Grain, RowSink};

/// One instant for every sink call in this file.
///
/// The sinks take the clock as a parameter, so a test states it rather than
/// sleeping: what is under test here is what a sink writes, never when it
/// decides to.
const NOW: u64 = 1_700_000_000_000_000_000;

const URL_ENV: &str = "DZ_LOADER_CLICKHOUSE_URL";
const DEFAULT_URL: &str = "http://127.0.0.1:8123";
/// Applied by the one test that is about retention. See [`Scratch::open`].
const RETENTION: &str = "002_recorder_retention.sql";

/// A database of this test's own, so a run cannot disturb a live one.
struct Scratch {
    sink: ClickHouseSink<dz_recorder_clickhouse::HttpTransport>,
    database: String,
    /// Kept so a test can build a second sink with different insert bounds
    /// against the same scratch database.
    config: ClickHouseConfig,
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

        let config = ClickHouseConfig {
            endpoint,
            database: database.clone(),
            // Every test but the two about parts wants a post per batch, so
            // that what is asserted is the schema rather than the coalescing.
            insert_min_rows: 1,
            ..ClickHouseConfig::default()
        };
        let sink = ClickHouseSink::over_http(config.clone());
        let scratch = Self {
            sink,
            database,
            config,
        };
        // The schema, and not the account: `004` takes a password parameter
        // and grants privileges, and the container runs with access management
        // off. Retention is applied by the one test that is about retention.
        for migration in schema() {
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

    let written = scratch
        .sink
        .write_batch(rows, NOW)
        .expect("the batch lands");
    for (grain, count) in expected {
        assert_eq!(written.accepted.rows(grain), count as u64, "{grain}");
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

    scratch.sink.write_batch(rows, NOW).expect("the first load");
    scratch
        .sink
        .write_batch(once, NOW)
        .expect("the second load");
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
    scratch
        .sink
        .write_batch(clean, NOW)
        .expect("the clean load");
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
    scratch.sink.write_batch(with_gap, NOW).expect("the load");
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

/// **Rows per part**, which is the number merge pressure is actually set by.
///
/// An insert is one atomic block and becomes one part, so a sink that posted per
/// object would write one part per object per lane — and merge work never shows
/// up in a query log, only as the gap between a provider's CPU graph and
/// query-attributed CPU. This is the assertion that holds the coalescing to its
/// purpose against a real server's own `system.parts`, rather than against what
/// the sink believes it sent.
///
/// The three objects are on the three **port roles**, so their rows have
/// disjoint sort keys and nothing collapses: what is measured here is parts, and
/// the collapse has a test of its own below.
#[test]
fn coalescing_produces_parts_at_or_above_the_floor_and_never_single_digit_ones() {
    let scratch = Scratch::open("parts");
    let mut sink = ClickHouseSink::over_http(ClickHouseConfig {
        insert_min_rows: 90,
        ..scratch.config.clone()
    });
    let mut datagrams = 0u64;
    // 32, 33 and 34 rows: 32 and 65 are held, and 99 crosses the floor — so the
    // third `write_batch` is what posts and the flush afterwards has nothing
    // left. Landings are counted wherever they happen, because which call
    // crosses the floor is arithmetic and not the property under test.
    let mut landed = Vec::new();
    for (count, role) in [
        (30, PortRole::Mktdata),
        (31, PortRole::Refdata),
        (32, PortRole::Snapshot),
    ] {
        let rows = batch_on_role(count, role);
        datagrams += rows.rows(Grain::Datagram) as u64;
        landed.extend(sink.write_batch(rows, NOW).expect("accepted").landed);
    }
    landed.extend(sink.flush(NOW).expect("posted"));
    assert_eq!(landed.len(), 3, "three objects, and all three landed");
    assert_eq!(sink.held_objects(), 0, "with nothing left held");

    // Every row landed, and `FINAL` changes nothing because the keys are
    // disjoint.
    let raw: u64 = scratch
        .scalar(&format!(
            "SELECT count() FROM {}.datagram",
            scratch.database
        ))
        .parse()
        .expect("count() is a number");
    assert_eq!(raw, datagrams);
    assert_eq!(scratch.count("datagram"), datagrams);

    // And in **one** part, not three: `active` only, because an inactive part is
    // one a merge has already replaced.
    let parts = scratch.scalar(&format!(
        "SELECT count() FROM system.parts WHERE database = '{}' AND table = 'datagram' \
         AND active",
        scratch.database
    ));
    assert_eq!(parts, "1", "three objects, one insert, one part");

    // The plan's other half: no configuration produces a part of single-digit
    // rows. Asserted from the server's own accounting.
    let smallest: u64 = scratch
        .scalar(&format!(
            "SELECT min(rows) FROM system.parts WHERE database = '{}' AND active AND rows > 0",
            scratch.database
        ))
        .parse()
        .expect("min(rows) is a number");
    assert!(
        smallest >= 10,
        "a part of {smallest} rows is the profile the coalescing exists to prevent"
    );
}

/// **An insert block is collapsed on the sort key before the part is written**,
/// and a consumer counting rows has to know it.
///
/// `optimize_on_insert` is on by default, so the engine applies the
/// `ReplacingMergeTree` collapse to the block being inserted rather than only
/// when parts merge. "Deduplication is merge-time" is therefore true *across*
/// inserts and false *within* one — and coalescing objects into one insert is
/// what moves rows from the first case into the second.
///
/// The fixture makes that visible: the synthetic publisher starts every stream
/// at sequence 0 with the same receive stamps, so objects of 30, 31, 32 and 33
/// datagrams on one instance are prefixes of one another. 126 rows go in and 33
/// come out, because `object_key` is not in the sort key. A real recorder cannot
/// produce this — it would be one datagram written into two segments — and this
/// test exists because the arithmetic surprised the author of the DDL comment.
#[test]
fn an_insert_block_is_collapsed_on_the_sort_key_before_the_part_is_written() {
    let scratch = Scratch::open("insert_collapse");
    let mut sink = ClickHouseSink::over_http(ClickHouseConfig {
        insert_min_rows: 1_000_000,
        ..scratch.config.clone()
    });
    let mut sent = 0u64;
    for count in [30, 31, 32, 33] {
        let rows = batch(count, Fault::None);
        sent += rows.rows(Grain::Datagram) as u64;
        sink.write_batch(rows, NOW).expect("held");
    }
    assert_eq!(sent, 126, "what the sink put in the body");
    sink.flush(NOW).expect("posted");

    let raw: u64 = scratch
        .scalar(&format!(
            "SELECT count() FROM {}.datagram",
            scratch.database
        ))
        .parse()
        .expect("count() is a number");
    assert_eq!(
        raw, 33,
        "the block was collapsed on the sort key at insert time, not at merge time"
    );
    // `FINAL` adds nothing, because the collapse already happened.
    assert_eq!(scratch.count("datagram"), 33);
    // One part, and its own row count agrees.
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT sum(rows) FROM system.parts WHERE database = '{}' AND table = 'datagram' \
             AND active",
            scratch.database
        )),
        "33"
    );
}

/// And without the floor, the same three objects are three parts — which is what
/// says the assertion above is measuring the coalescing and not the engine.
#[test]
fn posting_per_object_would_produce_one_part_per_object() {
    let scratch = Scratch::open("parts_per_object");
    let mut sink = ClickHouseSink::over_http(ClickHouseConfig {
        insert_min_rows: 1,
        ..scratch.config.clone()
    });
    for (count, role) in [
        (30, PortRole::Mktdata),
        (31, PortRole::Refdata),
        (32, PortRole::Snapshot),
    ] {
        sink.write_batch(batch_on_role(count, role), NOW)
            .expect("posted immediately");
    }

    let parts = scratch.scalar(&format!(
        "SELECT count() FROM system.parts WHERE database = '{}' AND table = 'datagram' \
         AND active",
        scratch.database
    ));
    assert_eq!(
        parts, "3",
        "one part per object, which is the profile the floor exists to prevent"
    );
}

/// The era rank is a view over the openings, and it is dense.
#[test]
fn the_era_rank_view_numbers_the_openings_densely() {
    let mut scratch = Scratch::open("era");
    let rows = batch(200, Fault::ResetCountAdvance);
    scratch.sink.write_batch(rows, NOW).expect("the load");

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
    // The collapsed view: one row per boundary that opens an era, with the
    // continuations recorded and filtered out.
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT count() FROM {}.era_opening",
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
    scratch.sink.write_batch(rows, NOW).expect("the load");

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
