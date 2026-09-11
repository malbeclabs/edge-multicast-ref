//! Every venue-side row, against a literal JSON object with the column names
//! the DDL uses.
//!
//! `dz-recorder-rows`' own `tests/column_names.rs` is the pattern, and the
//! argument for it is the same: holding a row against a literal rather than
//! against a round trip is what makes a rename fail, because a round trip
//! agrees with itself. A field renamed in the struct serialises and
//! deserialises perfectly and lands in a column that does not exist, where
//! `JSONEachRow` either refuses the row or — worse, with
//! `input_format_skip_unknown_fields` on somewhere — accepts it and drops the
//! value.
//!
//! It is extended here with an **absence**, which is the shape this tier is
//! most exposed on: the publisher provenance a venue-side row must not have,
//! held against the column-name literals, so that adding one of those columns
//! is a test failure rather than a review comment.
#![forbid(unsafe_code)]

use dz_recorder_rows::Nanos;
use dz_recorder_venue::{RefusalCount, VenueBookTop, VenueGrain, VenueObjectRow};
use serde_json::{json, Value};

const KEY: &str = "feed=top-of-book/env=test/site=site-1/recorder=recorder-1/\
                   date=2026-09-09/hour=12/1-2-3.dzus";
const SHA: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// **The publisher-provenance columns a venue-side row must not have.**
///
/// Held as literals, spelled exactly as the publisher-side DDL spells them, so
/// that adding one to a row type fails here. Each is a statement about a
/// datagram on a channel instance and a venue's upstream message is not one:
/// the channel is the operator's mapping, the identifier is minted by the
/// publisher's registry, the two counters and the segment belong to a sequence
/// space the venue side has none of, `drop_delta` is what a capture handle lost,
/// and an era is a `Reset Count` span.
///
/// `era_index` is here beside `era` because the rank is what a query would
/// reach for and a row would be the wrong place for either.
const NO_PUBLISHER_PROVENANCE: [&str; 8] = [
    "channel_id",
    "instrument_id",
    "sequence_number",
    "reset_count",
    "segment_seq",
    "drop_delta",
    "era",
    "era_index",
];

fn as_json<T: serde::Serialize>(row: &T) -> Value {
    serde_json::to_value(row).expect("a row serialises")
}

#[test]
fn a_venue_book_top_row_carries_exactly_the_venue_book_top_columns() {
    let row = VenueBookTop {
        recv_ts: Nanos(1_700_000_000_123_456_789),
        observation: "site-1/recorder-1".to_owned(),
        env: "test".to_owned(),
        feed: "top-of-book".to_owned(),
        connection: "mktdata".to_owned(),
        upstream_sid: Some(9),
        upstream_seq: Some(5_001),
        symbol: "AAA".to_owned(),
        price_exp: -2,
        qty_exp: 0,
        bid_px_raw: Some(10_050),
        bid_qty_raw: Some(3),
        bid_source_count: None,
        ask_px_raw: Some(10_060),
        ask_qty_raw: Some(4),
        ask_source_count: Some(2),
        book_key: 0xdead_beef_dead_beef,
        message_index: 7,
        change_index: 2,
        object_key: KEY.to_owned(),
        object_sha256: SHA.to_owned(),
    };

    assert_eq!(
        as_json(&row),
        json!({
            // A bare integer for a DateTime64(9).
            "recv_ts": 1_700_000_000_123_456_789u64,
            "observation": "site-1/recorder-1",
            "env": "test",
            "feed": "top-of-book",
            "connection": "mktdata",
            "upstream_sid": 9,
            "upstream_seq": 5_001,
            "symbol": "AAA",
            "price_exp": -2,
            "qty_exp": 0,
            "bid_px_raw": 10_050,
            "bid_qty_raw": 3,
            // Unknown is null, and a zero would be a count the venue stated.
            "bid_source_count": Value::Null,
            "ask_px_raw": 10_060,
            "ask_qty_raw": 4,
            "ask_source_count": 2,
            "book_key": 0xdead_beef_dead_beefu64,
            "message_index": 7,
            // The third top change of that record, which is what a batched
            // payload produces and what the record's own index cannot say.
            "change_index": 2,
            "object_key": KEY,
            "object_sha256": SHA,
        })
    );
}

#[test]
fn a_venue_object_row_carries_exactly_the_venue_object_columns() {
    let row = VenueObjectRow {
        recv_ts_start: Nanos(1_700_000_000_000_000_000),
        recv_ts_end: Nanos(1_700_000_060_000_000_000),
        observation: "site-1/recorder-1".to_owned(),
        env: "test".to_owned(),
        feed: "top-of-book".to_owned(),
        object_key: KEY.to_owned(),
        object_sha256: SHA.to_owned(),
        format_version: 1,
        connections: vec!["mktdata".to_owned(), "catalogue".to_owned()],
        message_count: 100,
        refused_count: 2,
        refusals: vec![
            RefusalCount("malformed".to_owned(), 1),
            RefusalCount("truncated".to_owned(), 1),
        ],
        event_count: 98,
        unpriced_count: 1,
        unknown_instrument_count: 1,
        desync_count: 0,
        unattributed_count: 0,
        book_top_count: 40,
        instrument_count: 3,
    };

    assert_eq!(
        as_json(&row),
        json!({
            "recv_ts_start": 1_700_000_000_000_000_000u64,
            "recv_ts_end": 1_700_000_060_000_000_000u64,
            "observation": "site-1/recorder-1",
            "env": "test",
            "feed": "top-of-book",
            "object_key": KEY,
            "object_sha256": SHA,
            "format_version": 1,
            "connections": ["mktdata", "catalogue"],
            "message_count": 100,
            "refused_count": 2,
            // Array(Tuple(String, UInt64)): an unnamed tuple is an array, the
            // same shape `segment_coverage.roles_joined` already reaches its
            // column as.
            "refusals": [["malformed", 1], ["truncated", 1]],
            "event_count": 98,
            "unpriced_count": 1,
            "unknown_instrument_count": 1,
            "desync_count": 0,
            "unattributed_count": 0,
            "book_top_count": 40,
            "instrument_count": 3,
        })
    );
}

/// **The receive-stamp taxonomy is one type on both sides of the archive.**
///
/// This crate reads an upstream object through `dz-recorder-archive` and writes
/// rows shaped by `dz-recorder-rows`, so it is a place that can hold the two
/// together — and the two are on either side of a dependency edge, so neither
/// can hold the other.
///
/// The assignment is the assertion: it compiles only while the label the object
/// header and the venue manifest carry and the label the `recv_ts_kind` column
/// holds are one type. Two enumerations with identical variants and identical
/// tokens, each pinned against its own literals, agree until one side is
/// renamed — and then a query filtering on the token across the two archives
/// returns the rows of one of them and says nothing.
#[test]
fn the_receive_stamp_taxonomy_is_one_type_on_both_sides_of_the_archive() {
    let from_the_object: dz_recorder_rows::RecvTsKindLabel =
        dz_recorder_archive::upstream::RecvTsKindLabel::KernelSoftware;
    assert_eq!(
        as_json(&from_the_object),
        json!("kernel-software"),
        "the token a manifest spells and the token a column holds have parted"
    );
    let fallback: dz_recorder_rows::RecvTsKindLabel =
        dz_recorder_archive::upstream::RecvTsKindLabel::ApplicationFallback;
    assert_eq!(as_json(&fallback), json!("application-fallback"));

    // And both are derived from the kind rather than spelled a third time.
    assert_eq!(
        dz_recorder_core::RecvTsKindLabel::of(dz_recorder_core::RecvTsKind::KernelSoftware),
        from_the_object
    );
    assert_eq!(
        dz_recorder_core::RecvTsKindLabel::of(dz_recorder_core::RecvTsKind::ApplicationFallback),
        fallback
    );
}

/// **Neither venue-side grain carries publisher provenance.**
///
/// The plan's centre, as an absence held against column-name literals: the
/// request this design answers asked for exactly these columns to be filled in,
/// and each has a plausible value that is also a real reading — channel `0` is a
/// channel, sequence `0` is the first sequence of an era — so they are absent
/// rather than nullable.
///
/// Both halves are here and neither is enough alone. The **enumeration** over
/// `VenueGrain::ALL` is what makes a grain added next year fail this rather than
/// slip past with a channel on it; the **literals** are what make a rename of
/// one of the seven fail rather than pass.
#[test]
fn the_venue_side_rows_carry_no_publisher_provenance() {
    let rows: Vec<(VenueGrain, Value)> = vec![
        (VenueGrain::BookTop, as_json(&book_top())),
        (VenueGrain::Object, as_json(&object())),
    ];
    assert_eq!(
        rows.len(),
        VenueGrain::COUNT,
        "a grain was added and is not held against the absence"
    );

    for (grain, row) in &rows {
        let fields = row.as_object().expect("a row is an object");
        for column in NO_PUBLISHER_PROVENANCE {
            assert!(
                !fields.contains_key(column),
                "{grain} declares `{column}`, which is a statement about a datagram \
                 on a channel instance — and a venue's upstream message is not one"
            );
        }
    }

    // And the one that would look like a near miss: a venue's own session
    // sequence is kept, under a name that says whose it is. The two must not be
    // one column, because writing a venue's counter into a `sequence_number`
    // would make the cross-site views compare two unrelated series and report a
    // venue's session resend as a publisher's gap.
    let book = rows[0].1.as_object().expect("a row is an object");
    assert!(book.contains_key("upstream_seq"));
    assert!(!book.contains_key("sequence_number"));
}

/// A grain names its table once, because the name is the metric label and the
/// file name too.
#[test]
fn a_venue_grain_names_its_table_once() {
    let tables: Vec<&str> = VenueGrain::ALL.iter().map(|g| g.table()).collect();
    assert_eq!(tables, vec!["venue_book_top", "venue_object"]);
    for grain in VenueGrain::ALL {
        assert_eq!(grain.to_string(), grain.table());
        // And the tables are prefixed, because they sit in one database beside
        // the publisher side's eight and a reader has to be able to tell at a
        // glance which side of the race a table is.
        assert!(grain.table().starts_with("venue_"), "{grain}");
    }
    let mut indexes: Vec<usize> = VenueGrain::ALL.iter().map(|g| g.index()).collect();
    indexes.dedup();
    assert_eq!(
        indexes.len(),
        VenueGrain::COUNT,
        "two grains share an index"
    );
}

/// Every row reads back as itself, because a golden fixture reads rows back.
#[test]
fn every_venue_row_reads_back_as_itself() {
    let book = book_top();
    let round: VenueBookTop =
        serde_json::from_value(as_json(&book)).expect("a book row reads back");
    assert_eq!(round, book);

    let object = object();
    let round: VenueObjectRow =
        serde_json::from_value(as_json(&object)).expect("an object row reads back");
    assert_eq!(round, object);
}

/// What is not known reaches the column as `null`, and never as a zero.
///
/// Four fields, and for each of them a zero is a reading the venue could have
/// stated: a session identifier of zero, a sequence of zero, a resting quantity
/// of zero at a price, and — the one that matters most — a source count of zero,
/// which is the top-of-book field's own spelling of *unavailable* on the other
/// side of the race.
#[test]
fn what_the_venue_did_not_state_is_null_and_never_zero() {
    let row = VenueBookTop {
        upstream_sid: None,
        upstream_seq: None,
        bid_px_raw: None,
        bid_qty_raw: None,
        bid_source_count: None,
        ask_source_count: None,
        ..book_top()
    };
    let json = as_json(&row);
    for column in [
        "upstream_sid",
        "upstream_seq",
        "bid_px_raw",
        "bid_qty_raw",
        "bid_source_count",
        "ask_source_count",
    ] {
        assert_eq!(
            json.get(column),
            Some(&Value::Null),
            "{column} must reach the column as null"
        );
    }
}

fn book_top() -> VenueBookTop {
    VenueBookTop {
        recv_ts: Nanos(1_700_000_000_123_456_789),
        observation: "site-1/recorder-1".to_owned(),
        env: "test".to_owned(),
        feed: "top-of-book".to_owned(),
        connection: "mktdata".to_owned(),
        upstream_sid: Some(9),
        upstream_seq: Some(5_001),
        symbol: "AAA".to_owned(),
        price_exp: -2,
        qty_exp: 0,
        bid_px_raw: Some(10_050),
        bid_qty_raw: Some(3),
        bid_source_count: None,
        ask_px_raw: Some(10_060),
        ask_qty_raw: Some(4),
        ask_source_count: Some(2),
        book_key: 42,
        message_index: 7,
        change_index: 0,
        object_key: KEY.to_owned(),
        object_sha256: SHA.to_owned(),
    }
}

fn object() -> VenueObjectRow {
    VenueObjectRow {
        recv_ts_start: Nanos(1_700_000_000_000_000_000),
        recv_ts_end: Nanos(1_700_000_060_000_000_000),
        observation: "site-1/recorder-1".to_owned(),
        env: "test".to_owned(),
        feed: "top-of-book".to_owned(),
        object_key: KEY.to_owned(),
        object_sha256: SHA.to_owned(),
        format_version: 1,
        connections: vec!["mktdata".to_owned()],
        message_count: 10,
        refused_count: 0,
        refusals: Vec::new(),
        event_count: 10,
        unpriced_count: 0,
        unknown_instrument_count: 0,
        desync_count: 0,
        unattributed_count: 0,
        book_top_count: 4,
        instrument_count: 1,
    }
}
