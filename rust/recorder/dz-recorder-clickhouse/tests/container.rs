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

use common::{
    batch, batch_on_role, cross_site_fixture, midday_ns, now_ns, race_fixture,
    ABSENT_BUT_A_SITE_OVERFLOWED, ABSENT_EVERYWHERE, A_SITE_IS_UP_AND_SILENT, MISSING_FROM,
    MISSING_TO, NOBODY_ELSE_HAS_LOADED, PRESENT_AT_ANOTHER_SITE, REPEATED,
};
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

    /// One gap row of *our* site, read back through the cross-site view.
    ///
    /// The case is the `Channel ID`, so a failing assertion names the case
    /// rather than a row number.
    fn cross_site(&self, case: u8, columns: &str) -> String {
        self.scalar(&format!(
            "SELECT {columns} FROM {}.sequence_gap_cross_site \
             WHERE site = 'one' AND channel_id = {case}",
            self.database
        ))
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
    landed.extend(sink.flush(NOW).expect("posted").objects);
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

    // The plan's other half — no part of single-digit rows — scoped to
    // `datagram`, which is the grain the whole argument is about.
    //
    // Not database-wide, and the reason is not convenience: `era` and
    // `segment_coverage` carry on the order of one row per channel instance per
    // object, so in a three-object fixture they have three rows in total and
    // their part is three rows however the sink is configured. There is no
    // batching policy that makes a table with three rows in it hold a part of
    // fifty. What coalescing buys those grains is a part *per insert* instead of
    // one per object, which is the assertion below.
    let smallest: u64 = scratch
        .scalar(&format!(
            "SELECT min(rows) FROM system.parts WHERE database = '{}' \
             AND table = 'datagram' AND active AND rows > 0",
            scratch.database
        ))
        .parse()
        .expect("min(rows) is a number");
    assert!(
        smallest >= 10,
        "a datagram part of {smallest} rows is the profile the coalescing exists to prevent"
    );

    // And every grain that got rows got **one** part, not one per object. That
    // is what the coalescing buys the small grains, where a row floor cannot.
    for grain in [Grain::Datagram, Grain::Era, Grain::SegmentCoverage] {
        assert_eq!(
            scratch.scalar(&format!(
                "SELECT count() FROM system.parts WHERE database = '{}' AND table = '{}' \
                 AND active",
                scratch.database,
                grain.table()
            )),
            "1",
            "{grain} should be one part for three objects"
        );
    }
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

/// A repeating state pairs one-to-one, and the occurrence nobody matched stays
/// visible.
///
/// This is the assertion the occurrence ordinal exists for, and only a server
/// can make it. The obvious shape — `ASOF JOIN` on the key — has no notion of
/// consuming a match, so each of the three occurrences at one point pairs with
/// whichever occurrence at the other is nearest, one arrival is counted several
/// times, and the lead times that come out are plausible and biased. Numbering
/// the occurrences and pairing ordinal to ordinal is one-to-one by
/// construction, and what it cannot pair it *shows*.
#[test]
fn a_repeating_state_pairs_one_to_one_and_the_unpaired_occurrence_stays_visible() {
    let mut scratch = Scratch::open("pairing");
    let base = now_ns();
    let rows = race_fixture(base);
    let tops = rows.rows(Grain::BookTop) as u64;
    scratch.sink.write_batch(rows, NOW).expect("the load");
    assert_eq!(
        scratch.count("book_top"),
        tops,
        "every fixture row is in the table, or nothing below is about the view"
    );

    // Four occurrences of the repeated state, three of them seen by both
    // observation points and the fourth by one. Not three, which is what
    // dropping the unpaired row would give, and not six, which is what pairing
    // by proximity would.
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT groupArray(observations) FROM (SELECT observations FROM \
             {}.book_top_race WHERE state_key = {REPEATED} ORDER BY occurrence)",
            scratch.database
        )),
        "[2,2,2,1]",
        "a repeating state pairs one-to-one, and the fourth occurrence is unpaired"
    );

    // Unpaired means visible and *unmeasured*. A zero lead would be a
    // measurement nobody made, and it would enter every average over the column
    // as evidence that the two paths tied.
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT concat(toString(count()), ' ', arrayStringConcat(any(observed_by), ',')) \
             FROM {}.book_top_race WHERE state_key = {REPEATED} AND observations = 1 \
             AND isNull(lead_ms)",
            scratch.database
        )),
        "1 a",
        "the unpaired occurrence is a row that names the point that saw it"
    );

    // Every pair is the two milliseconds the fixture stated. A pairing that
    // matched the wrong occurrences would still produce numbers, and they would
    // be multiples of twenty.
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT groupUniqArray(round(lead_ms, 3)) FROM {}.book_top_race \
             WHERE state_key = {REPEATED} AND observations = 2",
            scratch.database
        )),
        "[2]",
        "the lead is the one the fixture stated, so the ordinals lined up"
    );

    // The snapshot row consumed no ordinal. A snapshot anchors a book and never
    // times one: the runtime pulls it on its own cadence and the archive records
    // when it was published, so a race that counted it would measure the
    // publisher's scheduler and would renumber everything after it.
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT max(occurrence) FROM {}.book_top_occurrence \
             WHERE observation = 'b' AND state_key = {REPEATED}",
            scratch.database
        )),
        "3",
        "four rows at `b`, one of them an anchor, and three ordinals"
    );
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT count() FROM {}.book_top_occurrence",
            scratch.database
        )),
        (tops - 1).to_string(),
        "the anchor row is excluded from the numbering and nothing else is"
    );
}

/// Loading the same object twice does not manufacture evidence of loss.
///
/// `book_top` is a `ReplacingMergeTree` and a re-run after a fix is a replace,
/// so between the second load and the merge that follows it one arrival is in
/// the table twice. Numbered without the collapse, the duplicate becomes a
/// second occurrence — and the surplus occurrence at each point then pairs with
/// the surplus at the other while the *last* one at each pairs with nothing. So
/// a re-load would not merely inflate a count: it would report states that both
/// observation points saw as states one of them missed.
#[test]
fn a_re_load_before_the_merge_does_not_invent_occurrences() {
    let mut scratch = Scratch::open("pairing_reload");
    let base = now_ns();
    scratch
        .sink
        .write_batch(race_fixture(base), NOW)
        .expect("the first load");
    scratch
        .sink
        .write_batch(race_fixture(base), NOW)
        .expect("the second load");

    // Deliberately no `OPTIMIZE`: the window between a re-load and the merge is
    // exactly the window this is about, and a test that merged first would
    // assert the engine's behaviour rather than the view's.
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT groupArray(observations) FROM (SELECT observations FROM \
             {}.book_top_race WHERE state_key = {REPEATED} ORDER BY occurrence)",
            scratch.database
        )),
        "[2,2,2,1]",
        "the collapse is applied at read time, so a re-load changes nothing"
    );
}

/// The whole cross-site fixture, loaded object by object as five loaders would
/// have written it.
fn load_cross_site(scratch: &mut Scratch, base: u64) {
    for batch in cross_site_fixture(base) {
        scratch
            .sink
            .write_batch(batch, NOW)
            .expect("every object lands");
    }
}

/// A datagram one site has and another does not was not a publisher gap.
///
/// The first thing the join is for, and the cheapest to get wrong in the other
/// direction: a site with a gap and no way to look anywhere else reports the
/// strongest finding this tier makes on the weakest evidence it has. Here the
/// other site covered the range and recorded no gap over it, so it held the
/// three datagrams — and `seen_elsewhere` is `1` however loud our own gap is.
#[test]
fn a_datagram_another_site_holds_is_not_a_publisher_gap() {
    let mut scratch = Scratch::open("cross_site_present");
    let base = midday_ns();
    load_cross_site(&mut scratch, base);

    assert_eq!(
        scratch.cross_site(
            PRESENT_AT_ANOTHER_SITE,
            "concat(ifNull(toString(seen_elsewhere), 'unknown'), ' ', verdict)"
        ),
        "1 unverifiable",
        "present at another site, and therefore never the publisher's"
    );
    assert_eq!(
        scratch.cross_site(PRESENT_AT_ANOTHER_SITE, "toString(seqs_seen_elsewhere)"),
        (MISSING_TO - MISSING_FROM + 1).to_string(),
        "all three of them, attributed per datagram rather than per range"
    );
    assert_eq!(
        scratch.cross_site(
            PRESENT_AT_ANOTHER_SITE,
            "arrayStringConcat(arrayMap(x -> x.1, seen_at), ',')"
        ),
        "two",
        "and the row names the site that has them"
    );
    // The send stamps come from the site that received the datagrams, because
    // we have no clock reading for a datagram we never received. They bracket
    // the three arrivals the fixture stated, a millisecond apart.
    assert_eq!(
        scratch.cross_site(
            PRESENT_AT_ANOTHER_SITE,
            "toString(toUnixTimestamp64Nano(sent_to_ts) - toUnixTimestamp64Nano(sent_from_ts))"
        ),
        "2000000",
        "the publisher's own stamps, recovered from a site that has them"
    );
}

/// Absent from every site, with no recorder overflow anywhere: the finding.
///
/// Every exculpatory answer has been tested and failed — our own drops admit
/// nothing, the interface delta is zero, no redundant instance carried it — and
/// the one remaining explanation is now supported rather than assumed. This is
/// the only verdict in the tier that accuses anybody, and the only place it is
/// ever written.
#[test]
fn absent_from_every_site_with_no_overflow_anywhere_is_the_publisher() {
    let mut scratch = Scratch::open("cross_site_publisher");
    let base = midday_ns();
    load_cross_site(&mut scratch, base);

    assert_eq!(
        scratch.cross_site(
            ABSENT_EVERYWHERE,
            "concat(ifNull(toString(seen_elsewhere), 'unknown'), ' ', verdict)"
        ),
        "0 publisher",
        "known absent, and therefore escalated"
    );
    assert_eq!(
        scratch.cross_site(
            ABSENT_EVERYWHERE,
            "concat(toString(seqs_absent), '/', toString(seqs_expanded), ' ', \
             toString(absent_sites), ' ', toString(blocked_vantages), ' ', \
             toString(silent_vantages))"
        ),
        "3/3 1 0 0",
        "every missing sequence number accounted for, by one other site, with \
         nothing blocked and nobody silent"
    );
    // And the loader's own row is untouched: `publisher` is the view's answer
    // and never something a single vantage wrote down.
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT concat(verdict, ' ', ifNull(toString(seen_elsewhere), 'unknown')) \
             FROM {}.sequence_gap FINAL WHERE site = 'one' AND channel_id = {ABSENT_EVERYWHERE}",
            scratch.database
        )),
        "unverifiable unknown",
        "the stored row still says what one site could see, which is nothing"
    );
}

/// A site that overflowed in the window cannot contribute an absence.
///
/// The case a careless implementation gets wrong, because it looks exactly like
/// the one above: the datagram is missing at both sites and nothing else
/// differs. What differs is that the other site's window segment admitted two
/// drops its predecessor had not, so its three missing datagrams may be its own
/// ring rather than the publisher's silence — and an absence that may be
/// somebody's own ring is no evidence about a publisher at all.
///
/// The counter is read as a delta and never as a total, which is the other half
/// of the same trap: both segments carry a non-zero cumulative count, and a rule
/// reading the total would find no site admissible on any host that ever
/// overflowed.
#[test]
fn a_site_that_overflowed_in_the_window_cannot_contribute_an_absence() {
    let mut scratch = Scratch::open("cross_site_overflow");
    let base = midday_ns();
    load_cross_site(&mut scratch, base);

    assert_eq!(
        scratch.cross_site(
            ABSENT_BUT_A_SITE_OVERFLOWED,
            "concat(ifNull(toString(seen_elsewhere), 'unknown'), ' ', verdict)"
        ),
        "unknown unverifiable",
        "an absence that may be that site's own ring promotes nothing"
    );
    assert_eq!(
        scratch.cross_site(
            ABSENT_BUT_A_SITE_OVERFLOWED,
            "concat(toString(seqs_absent), ' ', toString(absent_sites), ' ', \
             toString(blocked_vantages))"
        ),
        "0 0 1",
        "it missed the same three and none of them counts"
    );
    // The delta is what decides it, and the delta exists only where the
    // preceding segment does. A hole in `segment_seq` is precisely where an
    // unaccounted burst hides, so it reads unknown rather than clean.
    assert_eq!(
        scratch.scalar(&format!(
            "SELECT groupArray(ifNull(toString(overflow_free), 'unknown')) FROM \
             (SELECT overflow_free FROM {}.segment_overflow WHERE site = 'two' \
              AND channel_id = {ABSENT_BUT_A_SITE_OVERFLOWED} ORDER BY segment_seq)",
            scratch.database
        )),
        "['unknown','0']",
        "no predecessor is unknown, and a delta of two is not clean"
    );
}

/// A site that is up and not reporting is not a site that reported nothing.
///
/// The two look identical from a query that only counts absences: one other
/// site covered the range, missed the same three admissibly, and nothing was
/// blocked — every condition the case above needed. What stops it is a third
/// site with coverage of this instance earlier the same day and none over this
/// window. It was there and it is not speaking here, so "absent from every
/// site" is a claim the archive cannot make.
#[test]
fn a_site_that_is_up_and_silent_over_the_window_blocks_the_verdict() {
    let mut scratch = Scratch::open("cross_site_silent");
    let base = midday_ns();
    load_cross_site(&mut scratch, base);

    assert_eq!(
        scratch.cross_site(
            A_SITE_IS_UP_AND_SILENT,
            "concat(ifNull(toString(seen_elsewhere), 'unknown'), ' ', verdict)"
        ),
        "unknown unverifiable",
        "a window a site is not reporting in is not a window it reported \
         nothing in"
    );
    assert_eq!(
        scratch.cross_site(
            A_SITE_IS_UP_AND_SILENT,
            "concat(toString(seqs_absent), '/', toString(seqs_expanded), ' ', \
             toString(absent_sites), ' ', toString(blocked_vantages), ' ', \
             toString(silent_vantages))"
        ),
        "3/3 1 0 1",
        "everything the publisher case needed, and one site that went quiet"
    );
}

/// One vantage alone never says `publisher`, and its answer is `NULL`.
///
/// Not `0`. A zero here is the claim that the archive looked everywhere and
/// found nothing, and this is the state where it has not looked anywhere at
/// all — which is the distinction the whole column exists to keep.
#[test]
fn one_vantage_alone_leaves_the_answer_unknown() {
    let mut scratch = Scratch::open("cross_site_alone");
    let base = midday_ns();
    load_cross_site(&mut scratch, base);

    assert_eq!(
        scratch.cross_site(
            NOBODY_ELSE_HAS_LOADED,
            "concat(ifNull(toString(seen_elsewhere), 'unknown'), ' ', verdict, ' ', \
             toString(seqs_expanded))"
        ),
        "unknown unverifiable 0",
        "nobody else could speak, so nothing is known and nothing is promoted"
    );
}

/// Loading a site twice is one absence and not two.
///
/// A re-run after an analyser fix is a replace, and between the second load and
/// the merge that follows it every row is in the tables twice. Counted as rows
/// rather than as distinct vantages, one site's single absence would be two —
/// and "absent from every site" is a claim about how many sites agreed, so a
/// double count is the one arithmetic error here that can promote a verdict on
/// evidence nobody has.
///
/// Deliberately no `OPTIMIZE`: the window between a re-load and the merge is
/// exactly the window this is about.
#[test]
fn loading_a_site_twice_is_one_absence_and_not_two() {
    let mut scratch = Scratch::open("cross_site_reload");
    let base = midday_ns();
    load_cross_site(&mut scratch, base);
    load_cross_site(&mut scratch, base);

    assert_eq!(
        scratch.scalar(&format!(
            "SELECT count() FROM {}.sequence_gap_cross_site WHERE site = 'one' \
             AND channel_id = {ABSENT_EVERYWHERE}",
            scratch.database
        )),
        "1",
        "one gap, one row, whatever the loader did twice"
    );
    assert_eq!(
        scratch.cross_site(
            ABSENT_EVERYWHERE,
            "concat(ifNull(toString(seen_elsewhere), 'unknown'), ' ', verdict, ' ', \
             toString(seqs_absent), ' ', toString(absent_sites))"
        ),
        "0 publisher 3 1",
        "one site absent, not two, and three sequence numbers, not six"
    );
    // The two cases that must stay unknown stay unknown, because a double count
    // could also manufacture the *other* half of the conjunction.
    for case in [ABSENT_BUT_A_SITE_OVERFLOWED, A_SITE_IS_UP_AND_SILENT] {
        assert_eq!(
            scratch.cross_site(case, "ifNull(toString(seen_elsewhere), 'unknown')"),
            "unknown",
            "case {case}"
        );
    }
}

/// The verdict survives the base rows it was drawn from, and does not invert
/// when they go.
///
/// `datagram` is the one table with a TTL, and the obvious cross-site join —
/// expand the missing sequence numbers and look for them at the other sites —
/// reads *absent* off it. Two days on, every site's rows are gone, every
/// sequence number looks absent everywhere, and a verdict drawn from that
/// promotes every stale gap in the archive to `publisher` on a timer. So
/// presence is read from `segment_coverage` and the other sites' own gap rows,
/// which have no TTL: within a covered range a site held a sequence number if
/// and only if it covered it and recorded no gap over it.
///
/// `TRUNCATE` rather than a TTL, because what is under test is a table with no
/// base rows in it and not the clause that eventually empties one — and the
/// fixture is stamped today on purpose, so the two-day window would not take it.
#[test]
fn the_cross_site_answer_outlives_the_base_rows_it_was_drawn_from() {
    let mut scratch = Scratch::open("cross_site_expiry");
    let base = midday_ns();
    load_cross_site(&mut scratch, base);
    assert_eq!(
        scratch.cross_site(
            PRESENT_AT_ANOTHER_SITE,
            "ifNull(toString(seen_elsewhere), 'unknown')"
        ),
        "1",
        "or nothing below is about what happens when the rows go"
    );

    scratch.scalar(&format!("TRUNCATE TABLE {}.datagram", scratch.database));
    assert_eq!(scratch.count("datagram"), 0);

    assert_eq!(
        scratch.cross_site(
            PRESENT_AT_ANOTHER_SITE,
            "concat(ifNull(toString(seen_elsewhere), 'unknown'), ' ', verdict)"
        ),
        "1 unverifiable",
        "the site that held it still says so, from rows that outlive the datagrams"
    );
    assert_eq!(
        scratch.cross_site(
            PRESENT_AT_ANOTHER_SITE,
            "ifNull(toString(sent_from_ts), 'unknown')"
        ),
        "unknown",
        "and the one thing only a base row could say goes unknown rather than \
         to the epoch"
    );
    assert_eq!(
        scratch.cross_site(ABSENT_EVERYWHERE, "verdict"),
        "publisher",
        "while the finding is unchanged, because it never rested on them"
    );
}
