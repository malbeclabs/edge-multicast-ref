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
use dz_recorder_venue::VenueGrain;

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

/// The grains `009` declares: what a venue's own upstream said, and the object
/// it was read out of.
///
/// [`VenueGrain::ALL`] and never a second list of them, for the reason
/// `dz-recorder-venue`'s own column-name test gives: a third grain added next
/// year reaches every loop below by being added to the enumeration, and a
/// hard-coded pair here would leave it out of the DDL check and out of the
/// `GRANT INSERT` check — a table the loader cannot write, found on the first
/// insert of a deployment rather than here.
const VENUE_GRAINS: [VenueGrain; VenueGrain::COUNT] = VenueGrain::ALL;

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

/// The venue-side tables and the race, which are `009`.
fn venue_sql() -> &'static str {
    sql_of("009_recorder_venue_observation.sql")
}

/// The book-only key on the publisher side, and the branch it feeds, which are
/// `010`.
fn book_key_sql() -> &'static str {
    sql_of("010_recorder_book_key.sql")
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

    // The venue-side grains of `009`, held the same way and for the same
    // reason. A separate loop because they are a different `Grain` enum in a
    // different crate: the two sides of the race deliberately do not share a
    // row vocabulary, which is the whole point of them being separate tables.
    //
    // Sized by [`VenueGrain::COUNT`], because this is the one venue loop a
    // grain cannot be added to by adding it to the enumeration: it pairs each
    // grain with a fixture of its row type, and there is no fixture to derive.
    // The width is what makes a third grain a compile error here rather than a
    // table nobody held against its struct.
    let held: [(VenueGrain, BTreeSet<String>); VenueGrain::COUNT] = [
        (
            VenueGrain::BookTop,
            field_names(&fixtures::venue_book_top()),
        ),
        (VenueGrain::Object, field_names(&fixtures::venue_object())),
    ];
    for (grain, fields) in held {
        let declared: BTreeSet<String> = columns(venue_sql(), grain.table()).into_iter().collect();
        assert_eq!(
            declared, fields,
            "{grain}: the schema and the row type disagree about columns"
        );
    }
}

/// **The venue-side tables declare no publisher provenance.**
///
/// The other half of `dz-recorder-venue`'s own column-name literal: that one
/// holds the *row types*, and this holds the *DDL*, because a column can be
/// added to a table without a field ever being added to a struct — and a column
/// that exists reads as a column somebody may fill.
///
/// Each of these is a statement about a datagram on a channel instance, and a
/// venue's upstream message is not one. The request this design answers asked
/// for exactly them.
#[test]
fn the_venue_side_tables_declare_no_publisher_provenance() {
    for grain in VENUE_GRAINS {
        let declared = columns(venue_sql(), grain.table());
        for column in [
            "channel_id",
            "instrument_id",
            "sequence_number",
            "reset_count",
            "segment_seq",
            "drop_delta",
            "era",
            "era_index",
            // And the three the eight-column argument also names, which a
            // reader reaching for "provenance" would add next.
            "source_addr",
            "dst_port",
            "source_id",
        ] {
            assert!(
                !declared.iter().any(|c| c == column),
                "{grain} declares `{column}`: {declared:?}"
            );
        }
    }

    // The near miss, stated so that the absence above is not read as the venue's
    // own numbering being thrown away. It is kept, under a name that says whose
    // it is.
    let book = columns(venue_sql(), VenueGrain::BookTop.table());
    assert!(book.iter().any(|c| c == "upstream_seq"), "{book:?}");
    assert!(book.iter().any(|c| c == "upstream_sid"), "{book:?}");
}

/// The race is keyed on `book_key`, and on `state_key` nowhere.
///
/// `state_key` folds the `Channel ID` and the `Instrument ID` into the
/// accumulator before it folds a price, and a venue side can compute neither.
/// Keyed on it this race would return zero pairs and read as each side missing
/// every state the other saw.
#[test]
fn the_feed_race_is_keyed_on_the_book_and_never_on_the_state_key() {
    let sql = venue_sql();
    assert!(
        !sql.lines()
            .any(|line| line.contains("state_key") && !line.trim_start().starts_with("--")),
        "a venue-side statement keys on `state_key`"
    );

    let occurrence = view_body(sql, "venue_book_top_occurrence");
    assert!(
        occurrence.contains("PARTITION BY observation, feed, upper(trimBoth(symbol)), book_key"),
        "the ordinal is not numbered per observation on the book: {occurrence}"
    );
    assert!(
        occurrence.contains("ORDER BY recv_ts"),
        "the ordinal is not taken by arrival: {occurrence}"
    );

    let race = view_body(sql, "feed_race");
    assert!(
        race.contains("GROUP BY feed, symbol_key, book_key, occurrence"),
        "the pairing does not group on the ordinal: {race}"
    );
    // Over the seam a side is admitted at, and not over one side's own
    // occurrences: the pairing never learns an observation point's name, so a
    // side enters by contributing rows.
    assert!(
        race.contains("FROM recorder.feed_race_occurrence"),
        "the pairing reads one side directly, so the other cannot enter: {race}"
    );
    // An aggregate over the ordinal and not a join between two named points, so
    // that an occurrence one side saw survives as a row. `006` makes the
    // argument; this holds the shape.
    assert!(
        !race.contains("JOIN"),
        "the pairing became a join, and an unpaired occurrence is now an absence: {race}"
    );
    assert!(
        race.contains("uniqExact(observation)"),
        "the observations are distinct points and not rows: {race}"
    );
}

/// **The symbol is folded once, and the fold says which case it folds.**
///
/// The fold appears twice in the ordinal — as the `symbol_key` a reader selects
/// and as the partition the numbering runs over — and the pairing groups on the
/// column. Two spellings of it are two folds: a `symbol_key` that folded one way
/// beside a partition that folded another numbers a state's occurrences under a
/// key nobody selected, and the pairing then groups the wrong rows together
/// while every string on the row still looks right.
///
/// `upper` is ASCII case and `upperUTF8` is the Unicode one, so which is written
/// is part of what the fold means. The file states it; this holds the code to
/// it, and holds the fold to case and the padding around it — anything that
/// stripped a separator or normalised a suffix would start merging instruments.
#[test]
fn the_venue_symbol_is_folded_once_and_the_fold_is_ascii_case() {
    const FOLD: &str = "upper(trimBoth(symbol))";
    let sql = venue_sql();
    let occurrence = view_body(sql, "venue_book_top_occurrence");
    assert_eq!(
        occurrence.matches(FOLD).count(),
        2,
        "the `symbol_key` column and the numbering's partition are not one fold: \
         {occurrence}"
    );
    assert!(
        occurrence.contains(&format!("{FOLD} AS symbol_key")),
        "the folded symbol is not the column a reader selects: {occurrence}"
    );

    let statements: Vec<&str> = sql
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect();
    for second_fold in [
        "upperUTF8",
        "lower(",
        "lowerUTF8",
        "replaceAll",
        "replaceRegexpAll",
    ] {
        assert!(
            !statements.iter().any(|line| line.contains(second_fold)),
            "`{second_fold}` folds the symbol a second way, and two folds pair nothing"
        );
    }
}

/// **Both sides of the race reach the pairing, and by the same seam.**
///
/// The gap `010` closes: `book_top` stored `state_key` only, so the publisher
/// side had nothing a venue side could join on and `009`'s race aggregated over
/// venue-side occurrences alone — two recordings of one upstream raced against
/// each other, which is not a feed race.
///
/// `009` declares the seam with one branch and `010` adds the other, so this
/// holds both halves: the venue branch where it is declared, and the publisher
/// branch where the column it reads is added.
#[test]
fn the_race_reads_a_seam_both_sides_of_the_race_reach() {
    let venue = view_body(venue_sql(), "feed_race_occurrence");
    assert!(
        venue.contains("FROM recorder.venue_book_top_occurrence"),
        "the venue side is not a branch of the seam: {venue}"
    );

    let seam = view_body(book_key_sql(), "feed_race_occurrence");
    assert!(
        seam.contains("FROM recorder.venue_book_top_occurrence"),
        "the venue branch was dropped when the publisher branch arrived: {seam}"
    );
    assert!(
        seam.contains("FROM recorder.publisher_book_top_occurrence"),
        "the publisher side still does not enter the race: {seam}"
    );
    // `UNION ALL` and never `UNION`: a distinct-ing union collapses two
    // observation points that saw one book at one instant into a single row,
    // which is exactly the pair the race exists to report.
    assert!(
        seam.contains("UNION ALL"),
        "the union de-duplicates, and a pair is what it would remove: {seam}"
    );
    assert!(
        !seam.contains("SELECT *"),
        "a union resolves its branches by position, so `*` would line one \
         side's column up against the other's: {seam}"
    );

    // The publisher branch is numbered the way the venue branch is, on the
    // book-only key and never on `state_key`: that one folds the `Channel ID`
    // and the `Instrument ID` in before it folds a price, and a venue side can
    // compute neither.
    let occurrence = view_body(book_key_sql(), "publisher_book_top_occurrence");
    assert!(
        occurrence.contains("PARTITION BY observation, feed, upper(trimBoth(symbol)), book_key"),
        "the ordinal is not numbered per observation on the book: {occurrence}"
    );
    assert!(
        !occurrence
            .lines()
            .any(|line| line.contains("state_key") && !line.trim_start().starts_with("--")),
        "the publisher branch keys on the observer-dependent key: {occurrence}"
    );
    // A snapshot anchors a book and never times one, and `WHERE` runs before a
    // window — so the anchored row consumes no ordinal, which is `006`'s
    // argument and applies here unchanged.
    assert!(
        occurrence.contains("WHERE from_anchor = 0"),
        "a snapshot-anchored row takes an ordinal: {occurrence}"
    );
    // And a row written before the column existed carries a hash of no book.
    // Left in, it would pair with nothing and be reported as a state the venue
    // never saw, which manufactures evidence of loss rather than inflating a
    // count.
    assert!(
        occurrence.contains("book_key != 0"),
        "a row from before the column exists enters the race: {occurrence}"
    );
}

/// **No migration folds a book state itself.**
///
/// `book_key` is written by `dz_recorder_events::book_key` and never by a second
/// implementation, in SQL or anywhere else. The temptation is real and the file
/// that adds the column is where it would land: the two sides are on the row, a
/// hash function is one call away, and a fold written here would agree with the
/// shared one on nothing — because the shared one reads a zero source count as
/// the absence the top-of-book specification says it is, and the predicate that
/// decides whether a side is absent is private in that crate for this reason.
///
/// Two hashes of one book state pair with nothing, and the failure is silent:
/// the query runs, the rows are all there, and the race simply reports no pair,
/// which is indistinguishable from a feed nobody was racing.
#[test]
fn no_migration_computes_a_book_key_of_its_own() {
    for migration in migrations() {
        for line in migration.sql.lines() {
            if line.trim_start().starts_with("--") {
                continue;
            }
            for fold in [
                "cityHash64",
                "sipHash64",
                "sipHash128",
                "farmHash64",
                "farmFingerprint64",
                "xxHash64",
                "xxh3",
                "murmurHash",
                "MurmurHash",
                "halfMD5",
                "javaHash",
                "metroHash64",
                "wyHash64",
            ] {
                assert!(
                    !line.contains(fold),
                    "{}: a fold of its own, where the key is one function's: {line}",
                    migration.name
                );
            }
        }
    }
}

/// The second key reaches no sort key, so deduplication does not move.
///
/// A `book_top` row is one change in one top of book. A key carrying the fold
/// would make one change two rows the moment a re-derivation computed the fold
/// differently — which is the one thing a replacing engine must not be asked to
/// tolerate, and the rule `008` states for `derivation` in the same words.
///
/// **No escape hatch for a window's `PARTITION BY`.** The guard carried one, and
/// it was unreachable: `sort_key_clauses` only starts capturing at a line
/// beginning `ORDER BY` or `PRIMARY KEY`, and in every file here a
/// `PARTITION BY` — a table's or a window's — is on a line of its own above one
/// of those. So the words could only ever appear in a captured clause if a sort
/// key wrapped onto a line carrying them, and in that one case the hatch would
/// have swallowed exactly the hit this test exists to catch.
#[test]
fn the_book_only_key_is_in_no_sort_key() {
    for sql in [
        market_data_sql(),
        pairing_sql(),
        venue_sql(),
        book_key_sql(),
    ] {
        for clause in sort_key_clauses(sql) {
            assert!(
                !clause.contains("book_key"),
                "the book-only key reached a table's sort key: {clause}"
            );
        }
    }
    // And `005`'s key is the one it always was, stated as a literal so that a
    // column appended to its tail fails here rather than being noticed when a
    // dashboard starts double-counting.
    assert_eq!(
        sort_key(market_data_sql(), "book_top"),
        "channel_id, instrument_id, recv_ts, sequence_number, message_index, observation",
        "`book_top`'s sort key moved"
    );
}

/// **The publisher ordinal is numbered in a total order, and not on the stamp
/// alone.**
///
/// `009` makes the argument for the venue branch and it holds here: ordered on
/// the receive stamp alone, `row_number()` is free to number two rows at one
/// stamp either way and may answer differently after a merge, so the same rows
/// pair differently on two runs and neither `observations` nor `lead_ms` is
/// reproducible. Equal stamps are ordinary on this side too — one datagram
/// carries many messages, which is what `message_index` exists for, and one
/// sequence hole writes a row for every established book on the channel at that
/// one stamp.
///
/// The tie-break is `005`'s own sort key without the `observation` the partition
/// already fixes, and that is what makes the order total: beneath `FINAL` no two
/// rows share that key. Held as a literal and against the key itself, because a
/// tie-break with a column missing still parses and is wrong only sometimes.
#[test]
fn the_publisher_ordinal_is_numbered_in_a_total_order() {
    let occurrence = view_body(book_key_sql(), "publisher_book_top_occurrence");
    assert!(
        occurrence.contains(
            "ORDER BY recv_ts, channel_id, instrument_id, sequence_number, message_index"
        ),
        "the ordinal is taken on the receive stamp alone, so two rows at one \
         stamp are numbered whichever way a merge left them: {occurrence}"
    );
    let key = sort_key(market_data_sql(), "book_top");
    for column in [
        "channel_id",
        "instrument_id",
        "sequence_number",
        "message_index",
    ] {
        assert!(
            key.contains(column),
            "`{column}` breaks the tie above and is not in `book_top`'s sort \
             key, so two rows may still share the whole order: {key}"
        );
    }
    assert!(
        book_key_sql().contains("THE WINDOW'S ORDER IS TOTAL"),
        "why a stamp is not an order has to be stated where the window is"
    );
}

/// **The file says which publisher-side rows are not moves of a top.**
///
/// A publisher-side row is written when the top moved *or* when the certainty of
/// it moved; a venue-side row only when the top moved. So a gap, a restore and
/// an unanchored book each write a row carrying the top the row before it
/// carried, the venue side writes no counterpart to any of them, and one
/// sequence hole leaves the two sides' ordinals for that book state off by one
/// — after which the venue's next occurrence pairs with the gap's row and
/// `lead_ms` is measured between two arrivals of two different states.
///
/// Nothing on the row says the top moved, so this file cannot filter them and
/// the property has to be stated as absent rather than implied. That is what
/// this holds: the exposure named where the branch is declared, each producer
/// named by the code that writes it, and the drift named as `006`'s too — which
/// is the argument for fixing it in the derivation rather than in one branch of
/// one union.
#[test]
fn the_publisher_branch_says_which_rows_are_not_moves_of_a_top() {
    let sql = book_key_sql();
    assert!(
        sql.contains("A PUBLISHER-SIDE ROW THAT MOVED NO TOP"),
        "the exposure is not named where the branch is declared"
    );
    // Each producer by the name of what writes it, because "some rows repeat a
    // top" is a warning nobody can check against the code.
    for producer in [
        "Book::observe_sequence",
        "A `Quote` that puts certainty back",
        "Book::level",
    ] {
        assert!(
            sql.contains(producer),
            "the row `{producer}` writes is not named, so the list cannot be \
             checked against the derivation"
        );
    }
    assert!(
        sql.contains("plausible wrong number"),
        "the drift is stated without the lead time it produces, which is the \
         half nobody notices"
    );
    assert!(
        sql.contains("`006`'S EXPOSURE TOO"),
        "the publisher-side race has the same drift, and a fix in this branch \
         alone would leave two readings of one table counting differently"
    );
    // And the `WHERE` below does not claim to be the whole of the list.
    assert!(
        sql.contains("That `WHERE` is not the whole of the list"),
        "the exclusions read as dropping every row that is not an occurrence"
    );
}

/// **The coarseness of a symbol without a channel, stated with its cost.**
///
/// The partition is the feed and the folded symbol and never the channel,
/// because a venue side cannot name one. A symbol is `char[64]` of venue-chosen
/// text that is unique within a channel at an instant, so during a re-shard
/// overlap one symbol published on two channels of one feed has both channels'
/// occurrences of a book state numbered in a single sequence: 2n publisher
/// ordinals against the venue's n. `009` states the era-boundary coarseness on
/// its own side, and this is the same kind of statement about this one.
#[test]
fn the_publisher_branch_states_the_coarseness_of_a_symbol_without_a_channel() {
    let sql = book_key_sql();
    let occurrence = view_body(sql, "publisher_book_top_occurrence");
    let partition = occurrence
        .lines()
        .find(|line| line.contains("PARTITION BY"))
        .expect("the ordinal has no partition at all");
    assert!(
        !partition.contains("channel_id"),
        "the partition names the channel, which a venue side cannot: {partition}"
    );
    assert!(
        sql.contains("AND IT IS THE CHANNEL"),
        "the cost of the omission is not stated where the omission is"
    );
    assert!(
        sql.contains("re-shard overlap"),
        "the case that pays the cost has to be named, not implied"
    );
}

/// **The schema is applied before the binary that writes the column.**
///
/// The header covers the other direction thoroughly — a row written before the
/// `ALTER` reads as zero, and the race excludes zero. This direction is the one
/// with no symptom at all: the sink posts `FORMAT JSONEachRow` with the field
/// names, `input_format_skip_unknown_fields` defaults to 1, so a binary carrying
/// `book_key` against a table the `ALTER` has not reached has its insert
/// accepted and the field dropped. Every publisher-side row lands with
/// `book_key = 0`, the exclusion drops all of them, and the cross-observer race
/// reads as a venue-only race with no error anywhere.
///
/// There is no test that can catch the ordering itself — the column half of this
/// module's own guard proves the field and the column agree *in the tree*, and
/// says nothing about a server nobody migrated. So the statement is the guard,
/// and this is what holds it.
#[test]
fn the_schema_is_applied_before_the_binary_that_writes_the_column() {
    let sql = book_key_sql();
    assert!(
        sql.contains("APPLY THIS FILE BEFORE THE BINARY THAT WRITES THE COLUMN"),
        "the direction with no symptom is not stated where an operator applies \
         the file"
    );
    assert!(
        sql.contains("input_format_skip_unknown_fields"),
        "the setting that turns a wrong order into silence is not named"
    );
    assert!(
        sql.contains("FORMAT JSONEachRow"),
        "how a column is matched at the server is what makes a field droppable"
    );
    assert!(
        sql.contains("INSERT INTO recorder.book_top"),
        "the insert the statement is about has to be the one the sink posts"
    );
}

/// **Only the `ALTER` is a no-op on a deployment created from scratch.**
///
/// `005` declares the column, so a fresh table has it before this file runs —
/// and the file used to say that every statement here was then a no-op, which is
/// true of one of them. `publisher_book_top_occurrence` is declared nowhere
/// else, `feed_race_occurrence` arrives from `009` with one branch, and a
/// deployment that read this file as optional would have a race that pairs the
/// venue against itself.
#[test]
fn only_the_alter_is_a_no_op_on_a_fresh_deployment() {
    let sql = book_key_sql();
    assert!(
        sql.contains("THE REST OF THIS FILE IS NOT OPTIONAL ON ANY DEPLOYMENT"),
        "the file reads as optional where `005` already declares the column"
    );
    // The two views that are not a re-statement of anything, held by name: a
    // file describing itself as a no-op has to be wrong about these two before
    // it is wrong about anything.
    for view in ["publisher_book_top_occurrence", "feed_race_occurrence"] {
        assert!(
            sql.contains(&format!("CREATE OR REPLACE VIEW recorder.{view} AS")),
            "{view} is not declared here, so the claim about it is stale"
        );
    }
    assert!(
        !view_body(venue_sql(), "feed_race_occurrence")
            .contains("recorder.publisher_book_top_occurrence"),
        "`009` already carries the publisher branch, and the statement about \
         what this file adds is no longer true"
    );
}

/// **The two files describe each other as they are.**
///
/// `010` said `009` "says, in its own header, that the publisher side does not
/// feed that pairing" — which `009` did say, before this change gave it the seam
/// and a branch to name. Two files in one tree disagreeing about what one of
/// them says is worse than either being silent, and the quotation is the part
/// that goes stale: it is a claim about another file that nothing else reads.
#[test]
fn the_two_files_describe_each_other_as_they_are() {
    assert!(
        venue_sql().contains("`010` adds the column and the second branch"),
        "`009` no longer names the file that completes its seam"
    );
    assert!(
        book_key_sql().contains("It declares the seam a side"),
        "`010` does not say what `009` now does"
    );
    assert!(
        !book_key_sql().contains("does not feed that pairing"),
        "`010` still quotes a header `009` no longer has"
    );
}

/// **The race is named for the race, and the name it had is dropped.**
///
/// `009` declared `venue_book_top_race`, and the name was true of it: one
/// branch, and the branch was the venue's. Once `010` adds the publisher branch
/// to the seam it aggregates both sides — `observations = 1` is as likely
/// publisher-only as venue-only and `observed_by` names publisher observation
/// points — so anyone filtering it as the venue's own race gets the opposite of
/// what they expect.
///
/// The rename is in `009`, so the query keeps one definition rather than two to
/// keep true. What is left for `010` is the old name on a deployment that
/// applied `009` before the rename, where the view still reads
/// `feed_race_occurrence` and silently becomes the two-sided race under a name
/// that says venue.
#[test]
fn the_race_is_named_for_the_race_and_the_old_name_is_dropped() {
    assert!(
        venue_sql().contains("CREATE OR REPLACE VIEW recorder.feed_race AS"),
        "the race is not declared under the name of the seam it reads"
    );
    for sql in [venue_sql(), book_key_sql()] {
        assert!(
            !sql.contains("CREATE OR REPLACE VIEW recorder.venue_book_top_race"),
            "the race is still declared under a name that says one side"
        );
    }
    assert!(
        book_key_sql().contains("DROP VIEW IF EXISTS recorder.venue_book_top_race;"),
        "a deployment that applied `009` before the rename keeps a view whose \
         name says venue and whose rows are both sides'"
    );
    // `IF EXISTS`, because on every other deployment there is nothing there and
    // a migration that failed on its own first application is a migration
    // nobody can re-run.
    assert!(
        book_key_sql().contains("DROP VIEW IF EXISTS"),
        "the drop fails on a deployment that never had the view"
    );
}

/// `symbols_agree` and `exponents_agree` are columns rather than assumptions.
///
/// The key covers the raw prices and leaves the exponents out, so a pair whose
/// exponents disagree is two different prices wearing one key. And the key is on
/// the symbol with its case folded, so a pair whose sides spell the instrument
/// differently is a pair — which is only safe if the disagreement is visible.
#[test]
fn the_feed_race_carries_its_reference_data_assertions_as_columns() {
    let race = view_body(venue_sql(), "feed_race");
    assert!(
        race.contains("(uniqExact(symbol) = 1)                AS symbols_agree"),
        "the symbols are assumed rather than compared: {race}"
    );
    assert!(
        race.contains("(uniqExact(price_exp) = 1) AND (uniqExact(qty_exp) = 1) AS exponents_agree"),
        "the exponents are assumed rather than compared: {race}"
    );
    // The strings themselves, because "they disagree" without them is a finding
    // nobody can act on.
    assert!(
        race.contains("arraySort(groupUniqArray(symbol))      AS symbols"),
        "a disagreement is reported without the spellings: {race}"
    );
    // Null and never zero for a state one point saw: a zero would be a lead time
    // nobody measured, entering every average as evidence that the paths tied.
    assert!(
        race.contains("if(uniqExact(observation) > 1,") && race.contains("NULL)"),
        "an unpaired occurrence gets a measured lead: {race}"
    );
    // And no bound written here. It is a property of the two paths being
    // compared, so it is the caller's predicate over `lead_ms`.
    assert!(
        !race.contains("abs(lead_ms)"),
        "a bound on the difference was written into the view: {race}"
    );
}

/// The collapse is applied once, beneath the numbering.
///
/// Numbering over an unmerged re-derivation counts one arrival as two
/// occurrences, and the surplus copy then pairs with nothing — so a duplicate
/// does not inflate a count here, it manufactures evidence of loss.
#[test]
fn the_venue_race_numbers_a_collapsed_table() {
    let sql = venue_sql();
    assert!(
        view_body(sql, "venue_book_top_settled").contains("recorder.venue_book_top FINAL"),
        "the collapse is not applied"
    );
    assert!(
        view_body(sql, "venue_book_top_occurrence")
            .contains("FROM recorder.venue_book_top_settled"),
        "the ordinal is numbered over the raw table"
    );
    assert_eq!(
        sql.matches("recorder.venue_book_top FINAL").count(),
        1,
        "written once, so nothing above pays for it twice"
    );
    assert!(
        sql.contains("manufactures evidence of loss"),
        "what a duplicate would do here is worse than a double count, and the \
         file has to say so"
    );
}

/// The venue-side retention keeps the same window the publisher side keeps.
///
/// A pair whose publisher half has expired is a state that reads as seen by one
/// observation point only, which is the strongest thing this race says — so the
/// two windows have to be the same one.
#[test]
fn the_venue_side_retention_matches_the_side_it_races() {
    let sql = venue_sql();
    assert!(
        sql.contains("ALTER TABLE recorder.venue_book_top")
            && sql.contains("MODIFY TTL toDateTime(recv_ts) + INTERVAL 30 DAY"),
        "the venue side does not keep the window `book_top` keeps"
    );
    assert!(
        market_data_sql().contains("MODIFY TTL toDateTime(recv_ts) + INTERVAL 30 DAY"),
        "the two windows are no longer the same number"
    );
    // The object row is what says an object was derived at all, so expiring it
    // turns a window nobody derived into one indistinguishable from a window
    // that held nothing.
    assert!(
        !sql.contains("ALTER TABLE recorder.venue_object"),
        "the row that says an object was derived must not expire"
    );
    assert!(
        sql.contains("recorder.venue_object` has no TTL"),
        "its absence of a TTL has to be stated, not inferred from the absence \
         of a line"
    );
}

/// This repository links no venue, and the schema names none.
///
/// The tables are the venue *side* of a race and are named for that, not for any
/// venue: a table name carrying one would be a schema that has to change to
/// record a second, and this repository would name the first.
#[test]
fn the_venue_side_tables_name_the_side_and_never_a_venue() {
    for grain in VENUE_GRAINS {
        assert!(
            grain.table().starts_with("venue_"),
            "{grain} does not say which side of the race it is"
        );
    }
    // The loader account can write them, or the tables are unwritable and the
    // grant would be found on the first insert of a deployment.
    let user = migration("004_recorder_loader_user.sql").sql;
    for grain in VENUE_GRAINS {
        assert!(
            user.contains(&format!("GRANT INSERT ON recorder.{}", grain.table())),
            "{grain} cannot be written"
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

    // The vantage, on all three. Two recorders at one site see the same
    // datagrams and agree on channel, instrument, sequence number and index;
    // `recv_ts` differing is two clocks not colliding rather than a key, so
    // `recorder` is what keeps them apart — and `book_top` folds site and
    // recorder into `observation`.
    assert!(event.contains("recorder"), "event key: {event}");
    assert!(instrument.contains("recorder"), "{instrument}");
    assert!(
        book_top.contains("observation"),
        "book_top names its vantage through `observation`: {book_top}"
    );

    // And the columns that are labels rather than keys, in none of the three.
    //
    // `env`: one database holds one environment, which is the convention `001`
    // already follows for `datagram`, `era`, `segment_coverage` and
    // `sequence_gap`. Keying on it here would make these three the only tables
    // in the recorder that do.
    //
    // `feed`: recoverable from the channel instance, because no two feeds serve
    // one `(source address, destination port)`. The coincidence a key has to
    // survive is a Channel ID collision, and `dst_port` is what survives it.
    //
    // `port_role`: recoverable from `dst_port` for the same reason, so keying on
    // the name beside the number widens every key to restate a fact. `book_top`
    // has no such column at all, because a book spans port roles.
    for (name, key) in [
        ("event", &event),
        ("book_top", &book_top),
        ("instrument", &instrument),
    ] {
        for column in ["env", "feed", "port_role"] {
            assert!(
                !key.contains(column),
                "{name} keys on {column}, which is a label the rest of the \
                 recorder does not key on: {key}"
            );
        }
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

/// **The venue sort keys carry what distinguishes two genuine rows, and the
/// object is what separates two of them.**
///
/// `ReplacingMergeTree` deduplicates on the whole sort key, so a key missing an
/// identity column does not sort badly — it deletes rows. `message_index`
/// restarts at zero in every object, so it is a record's position *within* one
/// and separates nothing across two: a rotation closes one object and opens the
/// next, and a clock coarser than the gap between them stamps records either
/// side of the boundary alike. Without `object_key` in the key, two genuine book
/// states collapse into one, and the loss is a row that was never written rather
/// than a count that is wrong.
///
/// The occurrence view's tie-break reads the object before the record index for
/// exactly that case, and it cannot repair this one: a row a merge removed is not
/// there to be numbered. So the key holds the object too, and in the same order
/// — a key and a numbering that disagreed about which orders two records would
/// be two orders over one set of rows.
#[test]
fn the_venue_sort_keys_carry_what_distinguishes_two_rows() {
    let book = sort_key(venue_sql(), "venue_book_top");
    assert_eq!(
        book, "observation, feed, symbol, recv_ts, object_key, message_index, change_index",
        "the venue book sort key changed"
    );
    let occurrence = view_body(venue_sql(), "venue_book_top_occurrence");
    assert!(
        occurrence.contains("ORDER BY recv_ts, object_key, message_index, change_index"),
        "the sort key and the ordinal's tie-break disagree about the object: {occurrence}"
    );

    // The digest is deliberately not in it, and the reason is what each table is
    // for. Two digests under one key are one window the archive re-published, so
    // the rows of the object that is there now must replace the rows of the one
    // that was.
    assert!(
        !book.contains("object_sha256"),
        "a re-recorded window doubles instead of replacing: {book}"
    );
    // And the idempotence row is the other way round, because it is the ledger
    // of what was read rather than the book.
    assert_eq!(
        sort_key(venue_sql(), "venue_object"),
        "observation, feed, object_key, object_sha256",
        "the venue object sort key changed"
    );
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

/// The migration that declares one grain's table.
///
/// Five grains are in `001` and the three market data ones in `005`. A test that
/// assumed one file would not fail on the grains it could not find — `columns`
/// panics rather than returning nothing, which is what makes that safe to rely
/// on here.
fn sql_declaring(grain: Grain) -> &'static str {
    match grain {
        Grain::Event | Grain::Instrument | Grain::BookTop => market_data_sql(),
        Grain::Datagram
        | Grain::Era
        | Grain::SegmentCoverage
        | Grain::SequenceGap
        | Grain::ConformanceFinding => rows_sql(),
    }
}

/// Provenance is on every grain, and in no sort key.
///
/// A datagram recorded once is one row whichever mode derived it. Put
/// `derivation` in a sort key and the archive-derived row and the live-derived
/// row of the same datagram stop collapsing under `ReplacingMergeTree` — so a
/// window loaded both ways doubles, and every count over it is wrong in a
/// direction nobody would suspect. The column exists to be *read*, and this is
/// where that stays true.
///
/// Both halves matter and neither is enough alone. Without the column check the
/// sort-key check passes over a table that has no provenance at all; without the
/// sort-key check a later migration can quietly break deduplication. The grain
/// enumeration is what makes a grain added next year fail here rather than ship
/// rows nobody can attribute.
///
/// **The venue grains are attributed by their object and not by a mode, and the
/// column half is `Grain`'s enumeration.** `derivation` distinguishes a row
/// derived from an archived object from one derived live over the same
/// datagrams, and the venue side has no live path to distinguish: a derivation
/// takes an object, so `object_key` and `object_sha256` on every venue row are
/// the whole of their provenance and are what a reader reads instead. Held
/// below as an assertion rather than left as a reason, because "there is no
/// live venue path" is a claim a later migration can falsify. The sort-key half
/// still walks `009`, because a column reaching a venue sort key is worth
/// knowing about whether or not it is this one.
#[test]
fn provenance_is_on_every_grain_and_in_no_sort_key() {
    for grain in Grain::ALL {
        let sql = sql_declaring(grain);
        let declared = columns(sql, grain.table());
        assert!(
            declared.iter().any(|c| c == "derivation"),
            "{grain} declares no derivation column: {declared:?}"
        );
    }

    // The venue grains, whose provenance is the object they were derived from.
    for grain in VENUE_GRAINS {
        let declared = columns(venue_sql(), grain.table());
        for column in ["object_key", "object_sha256"] {
            assert!(
                declared.iter().any(|c| c == column),
                "{grain} declares no `{column}`, so a venue-side row states no \
                 provenance at all: {declared:?}"
            );
        }
        assert!(
            !declared.iter().any(|c| c == "derivation"),
            "{grain} declares `derivation`, so there is a second venue derivation \
             mode and the object is no longer the whole of the provenance"
        );
    }

    let mut clauses = 0;
    for sql in [
        rows_sql(),
        market_data_sql(),
        pairing_sql(),
        cross_site_sql(),
        venue_sql(),
        book_key_sql(),
    ] {
        for clause in sort_key_clauses(sql) {
            clauses += 1;
            assert!(
                !clause.contains("derivation"),
                "provenance reached a sort key: {clause}"
            );
        }
    }
    // The walker found something, and found all of it. A guard that reads more
    // than one line is a guard whose *reading* is now the thing that can
    // regress, and a walker that quietly went back to the first line would leave
    // this green over exactly the hazard it was widened for. Thirteen: the ten
    // table sort keys, plus the bare `ORDER BY` in each of `006`'s, `009`'s and
    // `010`'s window specifications, which are checked like any other because a
    // column reaching a window's ordering is worth knowing about too. `003`'s is
    // in a file this test does not read.
    assert_eq!(clauses, 13, "the sort-key walker stopped finding clauses");
}

/// Every `ORDER BY` and `PRIMARY KEY` clause in one file, each as one string.
///
/// **A clause is not a line.** Four of the ten table sort keys wrap onto a
/// continuation line, so a guard reading only the line that begins `ORDER BY`
/// reads two thirds of what it is guarding — and a column appended to the tail
/// of a wrapped key passes it. The clause is accumulated from its first line
/// until the parenthesis depth it opened returns to zero, which is what closes a
/// tuple sort key on a later line and closes a bare one immediately.
fn sort_key_clauses(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut open: Option<(String, i32)> = None;
    for line in sql.lines() {
        let trimmed = line.trim_start();
        let (mut clause, mut depth) = match open.take() {
            Some(state) => state,
            None if trimmed.starts_with("ORDER BY") || trimmed.starts_with("PRIMARY KEY") => {
                (String::new(), 0)
            }
            None => continue,
        };
        clause.push(' ');
        clause.push_str(trimmed);
        depth += line.matches('(').count() as i32 - line.matches(')').count() as i32;
        // Depth back to zero ends the clause, which is the closing parenthesis
        // of a tuple key and the first line of a bare one. `;` ends it too, for
        // a statement that closes without one.
        if depth <= 0 || line.contains(';') {
            out.push(clause);
        } else {
            open = Some((clause, depth));
        }
    }
    // A clause that never closed is still a clause, and dropping it silently is
    // how a walker stops reading a file without failing.
    if let Some((clause, _)) = open {
        out.push(clause);
    }
    out
}

/// The walker reads a whole clause, and not the line it starts on.
///
/// The mutant this kills is the guard as it was: `derivation` appended to the
/// continuation line of a wrapped sort key. Asserted over a literal rather than
/// over the migrations, because the migrations must never carry that column in a
/// sort key — so the only way to hold the *reading* is to write the hazard out
/// here.
#[test]
fn the_sort_key_walker_reads_a_clause_that_wraps() {
    let wrapped = "ENGINE = ReplacingMergeTree\n                   PARTITION BY toYYYYMMDD(recv_ts)\n                   ORDER BY (channel_id, instrument_id, sequence_number,\n                   \x20         source_addr, derivation, recv_ts);\n";
    let clauses = sort_key_clauses(wrapped);
    assert_eq!(clauses.len(), 1, "{clauses:?}");
    assert!(
        clauses[0].contains("derivation"),
        "a column on the continuation line was not read: {clauses:?}"
    );

    // A bare key inside a window specification closes on its own line, and does
    // not swallow everything up to the next semicolon.
    let windowed = "        ORDER BY anchor_ts\n        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW\n";
    let clauses = sort_key_clauses(windowed);
    assert_eq!(
        clauses,
        vec![" ORDER BY anchor_ts".to_owned()],
        "{clauses:?}"
    );

    // And a single-line tuple key is one clause, not the rest of the file.
    let single = "ORDER BY (a, b, c);\nSOMETHING ELSE derivation\n";
    let clauses = sort_key_clauses(single);
    assert_eq!(
        clauses,
        vec![" ORDER BY (a, b, c);".to_owned()],
        "{clauses:?}"
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

    // `009`'s two, and the window specification's `PARTITION BY` which is not a
    // table's — hence the count is taken over the `CREATE TABLE` half of the
    // file alone rather than over the whole of it.
    let tables = venue_sql()
        .split("CREATE OR REPLACE VIEW")
        .next()
        .expect("the tables precede the views");
    assert!(tables.contains("PARTITION BY toYYYYMMDD(recv_ts)"));
    assert!(tables.contains("PARTITION BY toYYYYMMDD(recv_ts_start)"));
    assert_eq!(
        tables.matches("PARTITION BY ").count(),
        VENUE_GRAINS.len(),
        "a table in 009 has no PARTITION BY, or one has two"
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

    // The two tables, the TTL and the four views of `009`, and nothing split
    // across two of them.
    let venue = migration("009_recorder_venue_observation.sql").statements();
    assert_eq!(venue.len(), 7, "two tables, one TTL, four views");
    for view in [
        "venue_book_top_settled",
        "venue_book_top_occurrence",
        "feed_race_occurrence",
        "feed_race",
    ] {
        assert_eq!(
            venue
                .iter()
                .filter(|s| s.contains(&format!("CREATE OR REPLACE VIEW recorder.{view} AS")))
                .count(),
            1,
            "{view}"
        );
    }

    // The `ALTER`, the three views and the `DROP` of `010`, and nothing split
    // across two of them. `book_top_settled` is among them deliberately: a
    // view's `SELECT *` is expanded when the view is created, so on a
    // deployment upgraded in file order `006` freezes that view's column list
    // before the `ALTER` here runs.
    let book_key = migration("010_recorder_book_key.sql").statements();
    assert_eq!(book_key.len(), 5, "one ALTER, three views, one DROP");
    assert_eq!(
        book_key
            .iter()
            .filter(|s| s.contains("ALTER TABLE recorder.book_top"))
            .count(),
        1,
        "the column is added once"
    );
    // And the name the union made wrong is dropped once, where a deployment
    // that applied `009` before the rename may still hold it.
    assert_eq!(
        book_key
            .iter()
            .filter(|s| s.contains("DROP VIEW IF EXISTS recorder.venue_book_top_race"))
            .count(),
        1,
        "the old race name is dropped once"
    );
    for view in [
        "book_top_settled",
        "publisher_book_top_occurrence",
        "feed_race_occurrence",
    ] {
        assert_eq!(
            book_key
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

/// One statement of one migration: the code with the prose dropped, and the
/// line its first word is on.
///
/// The line is the statement's own and not the line its comment block starts
/// on. Every statement in these files carries a page of argument above it, so a
/// failure citing the comment would send a reader hundreds of lines away from
/// the thing it is about.
#[derive(Debug, Clone)]
struct Statement {
    file: &'static str,
    /// Position in [`schema`], which is the order a deploy applies the files in
    /// — and what makes "a later file" something a test can decide.
    order: usize,
    /// One-based, as an editor counts.
    line: usize,
    /// The statement, one trimmed line per line, with every comment line gone.
    code: String,
}

impl Statement {
    /// Where a failure sends a reader.
    fn at(&self) -> String {
        format!("{}:{}", self.file, self.line)
    }

    /// The statement on one line, for a phrase that wraps across two of them.
    fn flat(&self) -> String {
        self.code.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    /// Apply order, which is what "later than" is decided on.
    fn position(&self) -> (usize, usize) {
        (self.order, self.line)
    }
}

/// Every statement of every migration, in the order a deploy applies them.
///
/// Held against [`Migration::statements`] file by file, because that is the
/// splitter the deploy and the container suite use: a walker that read a
/// different set of statements than the one applied would check a schema
/// nobody has.
fn schema_statements() -> Vec<Statement> {
    let mut out: Vec<Statement> = Vec::new();
    for (order, migration) in schema().into_iter().enumerate() {
        let mut code = String::new();
        let mut first = 0usize;
        for (index, raw) in migration.sql.lines().enumerate() {
            let trimmed = raw.trim();
            if trimmed.is_empty() || trimmed.starts_with("--") {
                continue;
            }
            if code.is_empty() {
                first = index + 1;
            }
            code.push_str(trimmed);
            code.push('\n');
            if trimmed.ends_with(';') {
                out.push(Statement {
                    file: migration.name,
                    order,
                    line: first,
                    code: std::mem::take(&mut code),
                });
            }
        }
        // A statement with no terminator is a file this walker stopped reading
        // part way through, and dropping the tail silently is how a walker
        // stops covering a file without failing.
        assert!(
            code.is_empty(),
            "{}: the statement starting at line {first} is not terminated, so \
             this walker stopped reading the file",
            migration.name
        );
        assert_eq!(
            out.iter().filter(|s| s.order == order).count(),
            migration.statements().len(),
            "{}: this walker and `Migration::statements` disagree about how \
             many statements the file holds, so one of them is not reading the \
             file a deploy applies",
            migration.name
        );
    }
    out
}

/// What a statement's `SELECT *` is a star over, resolved through whatever the
/// `FROM` or the `JOIN` bound the alias to.
///
/// [`None`] for a projection that lists its columns, which is what most of
/// these files' views do. An explicit list does not pick up a new column either
/// — and that is the difference this test rests on: a list is a decision a
/// reader can see in the file, and a star is a column list frozen at the
/// instant the view was created.
///
/// The alias and not the first `FROM`: a join's right-hand side is a table too,
/// and a walker that took the first table it saw would watch the columns of the
/// wrong one.
fn starred_object(code: &str) -> Option<String> {
    let alias = code.lines().find_map(|line| {
        let projected = line.trim();
        // `SELECT *` on one line and a `d.*,` on a line of its own are both
        // shapes these files use.
        let projected = projected
            .strip_prefix("SELECT")
            .unwrap_or(projected)
            .trim()
            .trim_end_matches(',');
        if projected == "*" {
            Some(String::new())
        } else {
            projected.strip_suffix(".*").map(str::to_owned)
        }
    })?;

    let mut tokens = code
        .split_whitespace()
        .map(|token| token.trim_end_matches([';', ',']));
    let mut first_from: Option<String> = None;
    let mut bound: Vec<(String, String)> = Vec::new();
    while let Some(token) = tokens.next() {
        if token != "FROM" && token != "JOIN" {
            continue;
        }
        let Some(object) = tokens
            .next()
            .and_then(|next| next.strip_prefix("recorder."))
            .map(str::to_owned)
        else {
            // A join over a subquery names no table here, and the subquery's
            // own `FROM` is read on its own turn round this loop.
            continue;
        };
        if first_from.is_none() {
            first_from = Some(object.clone());
        }
        // `FROM recorder.t FINAL AS b` is the shape a collapse takes, so the
        // alias may be one token further along.
        let mut next = tokens.next();
        if next == Some("FINAL") {
            next = tokens.next();
        }
        if next == Some("AS") {
            if let Some(name) = tokens.next() {
                bound.push((name.to_owned(), object));
            }
        }
    }

    if alias.is_empty() {
        return first_from;
    }
    bound
        .into_iter()
        .find(|(name, _)| *name == alias)
        .map(|(_, object)| object)
}

/// Whether an `ALTER` moves the column *list* of its table.
///
/// `MODIFY COLUMN` is not one of these, deliberately. A view's `SELECT *` is
/// expanded into a list of column *names* when the view is created and the
/// types are resolved when it is read — so a view created earlier picks up a
/// type change on its own and cannot pick up an added, dropped or renamed
/// column, and only the second kind needs re-stating. `MODIFY TTL` is not one
/// either,
/// which is what the four `ALTER`s in `002`, `005` and `009` are: retention
/// moves no column, and a test that treated it as a column change would demand
/// a re-statement for every TTL decision.
fn changes_a_column_list(flat: &str) -> bool {
    ["ADD COLUMN", "DROP COLUMN", "RENAME COLUMN"]
        .iter()
        .any(|phrase| flat.contains(phrase))
}

/// The `SELECT *` views a released migration already froze, named here rather
/// than hidden behind a weaker parse.
///
/// `008` adds `derivation` to `recorder.era` and to `recorder.datagram` and
/// re-states no view — it contains no `CREATE OR REPLACE VIEW` at all — so
/// `003`'s `era_opening` and `datagram_in_era` are expanded without that column
/// on every deployment that was upgraded and with it on every deployment
/// created since. Two column lists under one view name.
///
/// It is latent only because nothing reads `derivation` through either view
/// yet: `006` takes three columns out of `era_opening` and the cross-site views
/// take named columns out of `datagram_in_era`. The first query that reaches
/// `era_opening.derivation` breaks on the deployments that have been running
/// longest, which is the opposite of the order anybody tests in.
///
/// **Not repaired here.** The repair is a `CREATE OR REPLACE VIEW` for each in
/// `008`, which changes a migration every deployment has already applied and
/// wants its own argument about what a released file may be amended to say. It
/// is tracked as a follow-up against `008_recorder_derivation.sql`.
///
/// Listed rather than allowed silently, and every entry is asserted below to
/// still be a violation — so the day `008` re-states them, this list fails
/// until the entry is deleted.
const FROZEN_BY_A_RELEASED_MIGRATION: [(&str, &str); 2] = [
    ("era_opening", "008_recorder_derivation.sql"),
    ("datagram_in_era", "008_recorder_derivation.sql"),
];

/// **A `SELECT *` view is re-stated by the file that moves its table's column
/// list.**
///
/// A view's `SELECT *` is expanded when the view is created, not when it is
/// read. So a file that adds a column to a table over which an earlier file
/// declared a star view leaves that view carrying the old column list on every
/// deployment upgraded in file order, while a deployment created from scratch
/// gets the new one — and nothing fails until a query reads the new column
/// through the view, on the deployments that have been running longest.
///
/// This is what holds `010`'s re-statement of `book_top_settled` in place.
/// Nothing else does: `Scratch::open` drops the database and applies `schema()`
/// in order, and `005` declares `book_key` on `book_top` itself, so the column
/// is already there when `006` expands its star, so the container suite returns
/// the same answers with that statement deleted. A server-based test reaches
/// this only with a second fixture that applies `005` without the column and
/// then the rest of the set in order; this needs no server at all.
///
/// **The reading is the end state and not every intermediate one.** The rule is
/// that a view's *last* declaration comes after the last `ALTER` that moves its
/// table's column list, which is the question "does a deployment that applied
/// every file in order hold the same view as one created from scratch". `008`
/// adds `derivation` to `recorder.book_top` and re-states nothing, and `010`
/// re-states `book_top_settled` afterwards — so the freeze `008` opened is
/// closed by the time the set has been applied, and this test does not report
/// it. Reported, it would be a finding that outlives its own repair; the two
/// entries in [`FROZEN_BY_A_RELEASED_MIGRATION`] are the ones no later file
/// closes.
#[test]
fn a_select_star_view_is_re_stated_after_a_column_reaches_its_table() {
    let statements = schema_statements();

    let mut declarations: Vec<(String, &Statement)> = Vec::new();
    let mut column_alters: Vec<(String, &Statement)> = Vec::new();
    for statement in &statements {
        let flat = statement.flat();
        if let Some(rest) = flat.strip_prefix("CREATE OR REPLACE VIEW recorder.") {
            let name = rest.split_whitespace().next().expect("a view has a name");
            declarations.push((name.to_owned(), statement));
        } else if let Some(rest) = flat.strip_prefix("ALTER TABLE recorder.") {
            if changes_a_column_list(&flat) {
                let table = rest
                    .split_whitespace()
                    .next()
                    .expect("an ALTER names a table");
                column_alters.push((table.to_owned(), statement));
            }
        }
    }

    // A walker that found no star, or no `ALTER`, would pass over anything. The
    // two named here are `003`'s star over `recorder.era` and `008`'s column on
    // that table — the pair the exception list is about, and neither of them the
    // statement this test exists to hold in place, so a failure below is the
    // property and not the parse.
    assert!(
        declarations
            .iter()
            .any(|(name, statement)| name == "era_opening"
                && statement.file == "003_recorder_era_rank.sql"),
        "the walker read no `era_opening` in `003`, so it is reading no views"
    );
    assert!(
        column_alters
            .iter()
            .any(|(table, statement)| table == "era"
                && statement.file == "008_recorder_derivation.sql"),
        "the walker read no column `ALTER` on `recorder.era` in `008`, so it is \
         reading no column changes"
    );

    let names: BTreeSet<String> = declarations.iter().map(|(name, _)| name.clone()).collect();
    let last_declaration = |name: &str| -> &Statement {
        declarations
            .iter()
            .filter(|(declared, _)| declared == name)
            .map(|(_, statement)| *statement)
            .max_by_key(|statement| statement.position())
            .expect("a name taken from the declarations is declared")
    };
    let last_column_alter = |table: &str| -> Option<&Statement> {
        column_alters
            .iter()
            .filter(|(altered, _)| altered == table)
            .map(|(_, statement)| *statement)
            .max_by_key(|statement| statement.position())
    };

    let mut stars_read = 0usize;
    let mut exceptions_taken: BTreeSet<(String, &str)> = BTreeSet::new();
    for name in &names {
        let declaration = last_declaration(name);
        let Some(starred) = starred_object(&declaration.code) else {
            continue;
        };
        stars_read += 1;

        // What can move the column list the star was expanded from. A star over
        // a view is chased through to the table underneath it, because
        // re-stating an inner view does not refresh an outer view's `SELECT *`
        // either: the outer list was expanded from the inner one at the instant
        // the outer view was created. There is no such view in these files
        // today, and one would be a worse freeze than the one this test is
        // about rather than a case it may skip.
        let mut hazards: Vec<(&Statement, String)> = Vec::new();
        let mut object = starred.clone();
        for _ in 0..=names.len() {
            if let Some(alter) = last_column_alter(&object) {
                hazards.push((
                    alter,
                    format!("adds, drops or renames a column on `recorder.{object}`"),
                ));
            }
            if !names.contains(&object) {
                break;
            }
            let inner = last_declaration(&object);
            hazards.push((
                inner,
                format!(
                    "re-states `recorder.{object}`, the view this star's column \
                     list was expanded from"
                ),
            ));
            match starred_object(&inner.code) {
                Some(next) => object = next,
                None => break,
            }
        }

        for (hazard, what) in hazards {
            if hazard.position() <= declaration.position() {
                continue;
            }
            let excepted = FROZEN_BY_A_RELEASED_MIGRATION
                .iter()
                .any(|(view, file)| *view == name.as_str() && *file == hazard.file);
            assert!(
                excepted,
                "`recorder.{name}` is declared `SELECT *` over \
                 `recorder.{starred}` at {declared}, and {altered} {what} — with \
                 no re-statement of `recorder.{name}` after it.\n\n\
                 A view's `SELECT *` is expanded into a column list when the \
                 view is created and never when it is read. So a deployment \
                 upgraded in file order holds this view without the new column \
                 while a deployment created from scratch holds it with, which \
                 is two column lists under one view name. Nothing fails while \
                 no query reads the new column through the view, and the first \
                 one that does breaks on the deployments that have been \
                 running longest.\n\n\
                 Re-state the view in {file}, after the statement above: an \
                 `ALTER` that moves a column list and a `CREATE OR REPLACE \
                 VIEW` for every star over that table belong in one file, \
                 which is the whole of the rule this test holds.",
                declared = declaration.at(),
                altered = hazard.at(),
                file = hazard.file,
            );
            exceptions_taken.insert((name.clone(), hazard.file));
        }
    }

    assert!(
        stars_read >= 4,
        "the walker read {stars_read} `SELECT *` views and these files declare \
         four, so the parse is reading a projection it does not understand"
    );

    // Every exception is still a violation. An entry that has stopped being one
    // is a repair nobody deleted the exception for, and a list that outlives
    // what it excuses is how an allow-list becomes the rule.
    for (view, file) in FROZEN_BY_A_RELEASED_MIGRATION {
        assert!(
            exceptions_taken.contains(&(view.to_owned(), file)),
            "{file} does not freeze `recorder.{view}`, so delete that entry \
             from `FROZEN_BY_A_RELEASED_MIGRATION` rather than leaving a list \
             that excuses nothing"
        );
    }
}

/// The star walker reads the projections these files are written in, and the
/// alias rather than the first table it sees.
///
/// Over literals rather than over the migrations, for the reason the sort key
/// walker's own test gives: what the test above pins is that the files carry no
/// frozen view, so the only way to hold the *reading* is to write the shapes out
/// here. A walker that resolved no star would make that test pass over
/// anything.
#[test]
fn the_star_walker_reads_a_bare_star_a_qualified_one_and_no_star_at_all() {
    // `003`'s and `006`'s shape: a bare star on the `SELECT` line, over a
    // collapsed table, with and without a `WHERE`.
    assert_eq!(
        starred_object("SELECT *\nFROM recorder.era FINAL\nWHERE continuation = 0;\n").as_deref(),
        Some("era")
    );
    assert_eq!(
        starred_object("SELECT *\nFROM recorder.book_top FINAL;\n").as_deref(),
        Some("book_top")
    );

    // `003`'s other shape: a qualified star on its own line, resolved through
    // the alias. Both orders are written out, because a walker that took the
    // first `FROM` would pass the first of them and watch the wrong table in
    // the second.
    let left = "SELECT\nd.*,\ne.anchor_ts AS era_anchor_ts\nFROM recorder.datagram AS d\n\
                ASOF LEFT JOIN recorder.era AS e\nON e.site = d.site;\n";
    assert_eq!(starred_object(left).as_deref(), Some("datagram"));
    let right = "SELECT\ne.*,\nd.site\nFROM recorder.datagram AS d\n\
                 ASOF LEFT JOIN recorder.era AS e\nON e.site = d.site;\n";
    assert_eq!(starred_object(right).as_deref(), Some("era"));

    // `FINAL` between the table and its alias, which is where a collapse goes.
    assert_eq!(
        starred_object("SELECT b.*\nFROM recorder.book_top FINAL AS b;\n").as_deref(),
        Some("book_top")
    );

    // A column list is not a star, and neither is a product.
    assert_eq!(
        starred_object("SELECT\nobservation,\nfeed\nFROM recorder.book_top;\n"),
        None
    );
    assert_eq!(
        starred_object("SELECT\nprice_exp * qty_exp AS scaled\nFROM recorder.book_top;\n"),
        None
    );

    // And a column change is told from a retention decision, which is the
    // other half of the reading: a `MODIFY TTL` freezes no view.
    assert!(changes_a_column_list(
        "ALTER TABLE recorder.book_top ADD COLUMN IF NOT EXISTS book_key UInt64 AFTER state_key;"
    ));
    assert!(!changes_a_column_list(
        "ALTER TABLE recorder.datagram MODIFY TTL toDateTime(recv_ts) + INTERVAL 2 DAY;"
    ));
    assert!(!changes_a_column_list(
        "ALTER TABLE recorder.book_top MODIFY COLUMN book_key UInt64;"
    ));
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

    use dz_recorder_venue::{RefusalCount, VenueBookTop, VenueObjectRow};

    use dz_recorder_rows::{
        BookTop, ConformanceFinding, Datagram, Derivation, DropScope, Era, Event, FindingVerdict,
        Instrument, MessageTypeLabel, Nanos, PortRoleLabel, RecvTsKindLabel, SegmentCoverage,
        SequenceGap, UncertainReason, Verdict,
    };

    const ADDR: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 10);

    pub fn venue_book_top() -> VenueBookTop {
        VenueBookTop {
            recv_ts: Nanos(1),
            observation: "site-1/recorder-1".to_owned(),
            env: "env".to_owned(),
            feed: "feed".to_owned(),
            connection: "mktdata".to_owned(),
            upstream_sid: Some(1),
            upstream_seq: Some(2),
            symbol: "AAA".to_owned(),
            price_exp: -2,
            qty_exp: 0,
            bid_px_raw: Some(1),
            bid_qty_raw: Some(1),
            bid_source_count: None,
            ask_px_raw: Some(2),
            ask_qty_raw: Some(1),
            ask_source_count: None,
            book_key: 3,
            message_index: 4,
            change_index: 5,
            object_key: "object".to_owned(),
            object_sha256: "sha".to_owned(),
        }
    }

    pub fn venue_object() -> VenueObjectRow {
        VenueObjectRow {
            recv_ts_start: Nanos(1),
            recv_ts_end: Nanos(2),
            observation: "site-1/recorder-1".to_owned(),
            env: "env".to_owned(),
            feed: "feed".to_owned(),
            object_key: "object".to_owned(),
            object_sha256: "sha".to_owned(),
            format_version: 1,
            connections: vec!["mktdata".to_owned()],
            message_count: 1,
            refused_count: 1,
            refusals: vec![RefusalCount("malformed".to_owned(), 1)],
            event_count: 1,
            unpriced_count: 0,
            unknown_instrument_count: 0,
            desync_count: 0,
            unattributed_count: 0,
            book_top_count: 1,
            instrument_count: 1,
        }
    }

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
            derivation: Derivation::Archive,
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
            derivation: Derivation::Archive,
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
            book_key: 0,
            from_anchor: 0,
            book_certain: 1,
            uncertain_since: None,
            uncertain_reason: UncertainReason::None,
            object_key: String::new(),
            derivation: Derivation::Archive,
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
            derivation: Derivation::Archive,
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
            derivation: Derivation::Archive,
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
            derivation: Derivation::Archive,
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
            derivation: Derivation::Archive,
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
            derivation: Derivation::Archive,
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
