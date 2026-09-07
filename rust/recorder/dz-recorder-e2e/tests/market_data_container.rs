//! The same chain, ending in a real column store.
//!
//! `archive_to_market_data` proves the rows are right. This proves the schema
//! takes them: the checked-in DDL applied to a real server, the rows inserted
//! through the real sink, and the counts and the values read back out with SQL.
//! That is the thing no literal-based test can establish — a `JSONEachRow` body
//! is accepted or refused by the server and by nobody else, so a column whose
//! type cannot hold what the deriver puts in it is only ever found here.
//!
//! Behind `--features clickhouse-tests`, because `cargo test` must need nothing
//! of the host. When the feature is on these tests **fail rather than skip** if
//! the server is absent: a gate that passes when it could not run reports a
//! clean schema for a schema nobody applied.
//!
//! `DZ_LOADER_CLICKHOUSE_URL` points at the server and defaults to the address
//! the demo's container publishes, exactly as `dz-recorder-clickhouse`'s own
//! container suite does. The tests create a database of their own and drop it.
#![cfg(feature = "clickhouse-tests")]
#![forbid(unsafe_code)]

mod common;
mod depth;

use common::record_feed;
use depth::{
    depth_stream, derive, ANCHOR_ASK_PRICE, ANCHOR_BID_PRICE, DEPTH_ROLES, INSTRUMENT, LEVELS,
    SOURCE_ID, SYMBOL,
};
use dz_edge_core::Feed;
use dz_edge_mbp::{MarketByPrice, MAGIC_MBP};
use dz_recorder_clickhouse::{schema, ClickHouseConfig, ClickHouseSink};
use dz_recorder_rows::{Grain, RowBatch, RowSink};

const NOW: u64 = 1_700_000_000_000_000_000;
const URL_ENV: &str = "DZ_LOADER_CLICKHOUSE_URL";
const DEFAULT_URL: &str = "http://127.0.0.1:8123";

/// A database of this test's own, so a run cannot disturb a live one.
struct Scratch {
    sink: ClickHouseSink<dz_recorder_clickhouse::HttpTransport>,
    database: String,
}

impl Scratch {
    /// The tables, and **not** their TTLs.
    ///
    /// The fixture carries the recorder's own receive stamps, which are in the
    /// past on purpose — nine populated digits, so a writer that rounded to
    /// microseconds could not pass a comparison against them. `event` carries a
    /// two-day row-level TTL, and a row-level TTL is applied as the part is
    /// written: every row this suite inserts would expire in the step that
    /// inserted it, every insert would be answered `200`, and every count below
    /// would come back zero — which is indistinguishable from a schema that
    /// never accepted the rows at all.
    ///
    /// The retention migration is skipped for the same reason in
    /// `dz-recorder-clickhouse`'s own container suite. Retention is that
    /// suite's subject and not this one's; what is under test here is whether
    /// the columns hold what the deriver produces.
    fn open(tag: &str) -> Self {
        let endpoint = std::env::var(URL_ENV).unwrap_or_else(|_| DEFAULT_URL.to_owned());
        let database = format!("recorder_e2e_{tag}");

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
            // A post per batch, so what is asserted is the schema rather than
            // the coalescing.
            insert_min_rows: 1,
            ..ClickHouseConfig::default()
        });
        let scratch = Self { sink, database };
        for migration in schema() {
            for statement in migration.statements() {
                if statement.contains("MODIFY TTL") || statement.contains("CREATE DATABASE") {
                    continue;
                }
                let statement = statement.replace("recorder.", &format!("{}.", scratch.database));
                scratch
                    .sink
                    .statement(&statement)
                    .unwrap_or_else(|e| panic!("{}: {statement}\n{e}", migration.name));
            }
        }
        scratch
    }

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

/// One archive, through the deriver, into the schema, and back out with SQL.
#[test]
fn the_checked_in_ddl_holds_what_the_deriver_produces() {
    let mut scratch = Scratch::open("market_data");
    let sent = depth_stream();
    let recorded = record_feed(&sent, DEPTH_ROLES, MarketByPrice::NAME);
    let derived = derive(&recorded, MAGIC_MBP, true);

    let expected = [
        (Grain::Event, derived.event.len()),
        (Grain::Instrument, derived.instrument.len()),
        (Grain::BookTop, derived.book_top.len()),
    ];
    let batch = RowBatch {
        object_key: recorded.manifest.object_key.clone(),
        object_sha256: recorded.manifest.sha256.clone(),
        event: derived.event,
        instrument: derived.instrument,
        book_top: derived.book_top,
        ..RowBatch::default()
    };
    let accepted = scratch
        .sink
        .write_batch(batch, NOW)
        .expect("the batch lands");

    for (grain, count) in expected {
        assert!(count > 0, "{grain} had nothing to insert");
        assert_eq!(accepted.accepted.rows(grain), count as u64, "{grain}");
        assert_eq!(scratch.count(grain.table()), count as u64, "{grain}");
    }

    // The identity block came through as columns and not as text: the values
    // below are read back by SQL, which is the server's opinion of the types
    // rather than the sink's.
    let db = &scratch.database;
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT DISTINCT symbol FROM {db}.event FINAL WHERE instrument_id = {INSTRUMENT}"
        )),
        SYMBOL
    );
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT DISTINCT source_id FROM {db}.event FINAL WHERE instrument_id = {INSTRUMENT}"
        )),
        SOURCE_ID.to_string()
    );
    assert_eq!(
        scratch.scalar(&format!("SELECT DISTINCT feed FROM {db}.event FINAL")),
        MarketByPrice::NAME
    );

    // The cycle answers *was it complete* from the begin and the end alone,
    // which is what makes persisting the levels optional. Read as one row, so
    // the comparison is the server's.
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT max(total_levels) = max(levels_seen) FROM {db}.event FINAL \
             WHERE message_type IN ('SnapshotBegin', 'SnapshotEnd')"
        )),
        "1"
    );

    // The anchored state is in `book_top` at the prices the cycle carried, and
    // `from_anchor` is what excludes it from a pairing.
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT count() FROM {db}.book_top FINAL WHERE from_anchor = 1 \
             AND bid_px_raw = {ANCHOR_BID_PRICE} AND ask_px_raw = {ANCHOR_ASK_PRICE}"
        )),
        "1"
    );
    // And a cleared side is NULL rather than a zero somebody later averages.
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT count() FROM {db}.book_top FINAL WHERE bid_px_raw IS NULL"
        )),
        "1"
    );
    // Every level the cycle carried is its own row when the switch asked for
    // them, and each one inherited the instrument from its begin.
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT count() FROM {db}.event FINAL WHERE message_type = 'SnapshotLevel' \
             AND instrument_id = {INSTRUMENT}"
        )),
        LEVELS.len().to_string()
    );
}

/// Re-deriving the same object is a replace and not a duplication.
///
/// The property the whole tier's idempotence rests on, and one only a real merge
/// can demonstrate: the market data tables are `ReplacingMergeTree` like the
/// transport ones, and `OPTIMIZE ... FINAL` is what makes the collapse happen
/// now rather than eventually.
#[test]
fn loading_one_object_twice_replaces_its_market_data_rather_than_doubling_it() {
    let mut scratch = Scratch::open("market_data_reload");
    let sent = depth_stream();
    let recorded = record_feed(&sent, DEPTH_ROLES, MarketByPrice::NAME);
    let derived = derive(&recorded, MAGIC_MBP, true);
    let expected = [
        (Grain::Event, derived.event.len() as u64),
        (Grain::Instrument, derived.instrument.len() as u64),
        (Grain::BookTop, derived.book_top.len() as u64),
    ];
    let batch = RowBatch {
        object_key: recorded.manifest.object_key.clone(),
        object_sha256: recorded.manifest.sha256.clone(),
        event: derived.event,
        instrument: derived.instrument,
        book_top: derived.book_top,
        ..RowBatch::default()
    };

    scratch
        .sink
        .write_batch(batch.clone(), NOW)
        .expect("the first load");
    scratch
        .sink
        .write_batch(batch, NOW)
        .expect("the second load");
    for (grain, _) in expected {
        scratch
            .sink
            .statement(&format!(
                "OPTIMIZE TABLE {}.{} FINAL",
                scratch.database,
                grain.table()
            ))
            .unwrap_or_else(|e| panic!("merging {}: {e}", grain.table()));
    }

    for (grain, count) in expected {
        assert_eq!(scratch.count(grain.table()), count, "{grain}");
    }
}
