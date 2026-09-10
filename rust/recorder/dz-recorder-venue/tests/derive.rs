//! The derivation, driven over fixture objects by a fixture adapter.
#![forbid(unsafe_code)]

mod common;

use common::{
    object_bytes, FixtureAdapter, FixtureObject, Listing, FIXTURE_CHANNEL, FIXTURE_FIRST_SEQ,
    FIXTURE_KEY, FIXTURE_SHA,
};
use dz_recorder_venue::{
    derive_venue_object, CollectingSink, DeriveError, RefusalCount, VenueBookTop,
};

const BASE: u64 = 1_700_000_000_000_000_000;

fn adapter() -> FixtureAdapter {
    FixtureAdapter::new(vec![Listing::new("AAA", -2, 0)])
}

/// One row, as the tests compare them.
type Observed = (
    u64,
    String,
    Option<i64>,
    Option<u64>,
    Option<i64>,
    Option<u64>,
);

/// The rows, as `(recv_ts, symbol, bid_px, bid_qty, ask_px, ask_qty)`.
fn tops(sink: &CollectingSink) -> Vec<Observed> {
    sink.book_tops()
        .iter()
        .map(|row| {
            (
                row.recv_ts.0,
                row.symbol.clone(),
                row.bid_px_raw,
                row.bid_qty_raw,
                row.ask_px_raw,
                row.ask_qty_raw,
            )
        })
        .collect()
}

/// A fixture adapter over a fixture object produces the expected rows.
#[test]
fn a_fixture_adapter_over_a_fixture_object_produces_the_expected_rows() {
    let mut object = FixtureObject::of(
        BASE,
        &[
            "chan=113 pubseq=990001 seq=5001 sid=7 quote AAA 100.50 3 100.60 4",
            // The same top restated. Not a change, so not a row: a change is a
            // change in the visible top.
            "chan=113 pubseq=990001 seq=5002 sid=7 quote AAA 100.50 3 100.60 4",
            "chan=113 pubseq=990001 seq=5003 sid=7 quote AAA 100.51 3 100.60 4",
            // A trade moves no book.
            "chan=113 pubseq=990001 seq=5004 sid=7 trade AAA 100.51 1",
            // One side gone is a book too, and a different one.
            "chan=113 pubseq=990001 seq=5005 sid=7 quote AAA - - 100.60 4",
        ],
    );
    let mut sink = CollectingSink::new();
    let derived = derive_venue_object(&mut adapter(), &mut object, &mut sink).expect("the object");

    assert_eq!(
        tops(&sink),
        vec![
            (
                BASE,
                "AAA".to_owned(),
                Some(10_050),
                Some(3),
                Some(10_060),
                Some(4)
            ),
            (
                BASE + 2_000_000,
                "AAA".to_owned(),
                Some(10_051),
                Some(3),
                Some(10_060),
                Some(4)
            ),
            (
                BASE + 4_000_000,
                "AAA".to_owned(),
                None,
                None,
                Some(10_060),
                Some(4)
            ),
        ],
        "a change is a change in the visible top, and nothing else is a row"
    );

    assert_eq!(derived.message_count, 5);
    assert_eq!(derived.event_count, 5);
    assert_eq!(derived.refused_count, 0);
    assert_eq!(derived.book_top_count, 3);
    assert_eq!(derived.instrument_count, 1);

    // One batch, both grains: the object is the unit that either landed or did
    // not.
    assert_eq!(sink.batches.len(), 1);
    let object_row = sink.objects();
    assert_eq!(object_row.len(), 1);
    let object_row = object_row[0];
    assert_eq!(object_row.object_key, FIXTURE_KEY);
    assert_eq!(object_row.object_sha256, FIXTURE_SHA);
    assert_eq!(object_row.recv_ts_start.0, BASE);
    assert_eq!(object_row.recv_ts_end.0, BASE + 4_000_000);
    assert_eq!(object_row.connections, vec!["mktdata".to_owned()]);
    assert_eq!(object_row.book_top_count, 3);
}

/// **An adapter that refuses a message costs that message and is counted, not
/// the object.**
///
/// The mutant this kills is a refusal that ends the object. A derivation that
/// stopped at the first message a venue's own adapter could not parse would
/// report the venue's feed as having ended there — and the rows are
/// indistinguishable from a venue that went quiet, so nothing downstream could
/// tell.
#[test]
fn one_refused_message_costs_that_message_and_not_the_object() {
    let mut object = FixtureObject::of(
        BASE,
        &[
            "chan=113 pubseq=990001 seq=5001 sid=7 quote AAA 100.50 3 100.60 4",
            // The middle message the adapter cannot parse.
            "chan=113 pubseq=990001 seq=5002 sid=7 refuse malformed",
            "chan=113 pubseq=990001 seq=5003 sid=7 quote AAA 100.70 5 100.80 6",
        ],
    );
    let mut sink = CollectingSink::new();
    let derived = derive_venue_object(&mut adapter(), &mut object, &mut sink).expect("the object");

    // The rows either side of the refusal.
    assert_eq!(
        tops(&sink),
        vec![
            (
                BASE,
                "AAA".to_owned(),
                Some(10_050),
                Some(3),
                Some(10_060),
                Some(4)
            ),
            (
                BASE + 2_000_000,
                "AAA".to_owned(),
                Some(10_070),
                Some(5),
                Some(10_080),
                Some(6)
            ),
        ],
        "the message after the refusal was not derived: a refusal ended the object"
    );

    // And a count of one refusal, under the reason the adapter gave. Not a bare
    // total: an operator acts differently on a schema refusal — the venue
    // changed its interface — than on a truncated one.
    assert_eq!(derived.refused_count, 1);
    assert_eq!(
        derived.refusals,
        vec![RefusalCount("malformed".to_owned(), 1)]
    );
    assert_eq!(derived.message_count, 3, "every message was read");
    assert_eq!(sink.objects()[0].refused_count, 1);
    assert_eq!(
        sink.objects()[0].refusals,
        vec![RefusalCount("malformed".to_owned(), 1)]
    );
}

/// Every refusal reason is counted under its own token.
#[test]
fn refusals_are_counted_by_the_reason_the_adapter_gave() {
    let mut object = FixtureObject::of(
        BASE,
        &[
            "chan=113 pubseq=990001 seq=1 sid=7 quote AAA 1.00 1 2.00 1",
            "chan=113 pubseq=990001 seq=2 sid=7 refuse schema",
            "chan=113 pubseq=990001 seq=3 sid=7 refuse truncated",
            "chan=113 pubseq=990001 seq=4 sid=7 refuse truncated",
            "chan=113 pubseq=990001 seq=5 sid=7 refuse unknown_field",
        ],
    );
    let mut sink = CollectingSink::new();
    let derived = derive_venue_object(&mut adapter(), &mut object, &mut sink).expect("the object");
    assert_eq!(derived.refused_count, 4);
    assert_eq!(
        derived.refusals,
        vec![
            RefusalCount("schema".to_owned(), 1),
            RefusalCount("truncated".to_owned(), 2),
            RefusalCount("unknown_field".to_owned(), 1),
        ]
    );
}

/// The venue's own message identity reaches the row, and reaches only its own
/// two columns.
#[test]
fn the_venues_own_message_identity_is_kept_as_evidence() {
    let mut object = FixtureObject::of(
        BASE,
        &["chan=113 pubseq=990001 seq=5001 sid=42 quote AAA 100.50 3 100.60 4"],
    );
    let mut sink = CollectingSink::new();
    derive_venue_object(&mut adapter(), &mut object, &mut sink).expect("the object");
    let row = sink.book_tops()[0];
    assert_eq!(row.upstream_sid, Some(42));
    assert_eq!(row.upstream_seq, Some(5_001));
    assert_eq!(row.connection, "mktdata");
}

/// **A venue-side row carries no publisher provenance, and the derivation had
/// it.**
///
/// This is the plan's centre: the request asked for exactly these columns to be
/// filled in. Both values are in the derivation's hand twice over — in the
/// object's own key, and in the payload bytes the adapter reads and reports — so
/// this is an assertion about what the rows do not take rather than a
/// tautology about what was never available.
///
/// The absence is held against the **column-name literals** in
/// `tests/column_names.rs`. Here it is held against the *values*, which is the
/// other half: a column called something else that happened to carry the channel
/// would pass a name check and fail this one.
#[test]
fn the_venue_side_rows_carry_no_publisher_provenance() {
    let mut adapter = adapter();
    let mut object = FixtureObject::of(
        BASE,
        &[
            "chan=113 pubseq=990001 seq=5001 sid=7 quote AAA 100.50 3 100.60 4",
            "chan=113 pubseq=990001 seq=5002 sid=7 quote AAA 100.51 3 100.60 4",
        ],
    );
    let mut sink = CollectingSink::new();
    derive_venue_object(&mut adapter, &mut object, &mut sink).expect("the object");

    // The values were there. Without this the test below asserts nothing.
    assert_eq!(
        adapter.channels_read,
        vec![FIXTURE_CHANNEL, FIXTURE_CHANNEL],
        "the fixture adapter did not read the channel, so nothing is being withheld"
    );
    assert_eq!(
        adapter.publisher_sequences_read,
        vec![FIXTURE_FIRST_SEQ, FIXTURE_FIRST_SEQ],
        "the fixture adapter did not read the publisher sequence number, so \
         nothing is being withheld"
    );
    assert!(
        FIXTURE_KEY.contains(&format!("channel={FIXTURE_CHANNEL}"))
            && FIXTURE_KEY.contains(&format!("first_seq={FIXTURE_FIRST_SEQ}")),
        "the object's own key states both, so the derivation holds them: {FIXTURE_KEY}"
    );

    // And neither reached a row. Every field but `object_key`, which carries
    // the key verbatim because that is what a re-derivation is idempotent on.
    for row in sink.book_tops() {
        let json = serde_json::to_value(row).expect("a row serialises");
        let fields = json.as_object().expect("a row is an object");
        for (name, value) in fields {
            if name == "object_key" {
                continue;
            }
            assert_ne!(
                value,
                &serde_json::json!(FIXTURE_CHANNEL),
                "{name} carries the channel"
            );
            assert_ne!(
                value,
                &serde_json::json!(FIXTURE_FIRST_SEQ),
                "{name} carries a publisher sequence number"
            );
        }
    }
    for row in sink.objects() {
        let json = serde_json::to_value(row).expect("a row serialises");
        for (name, value) in json.as_object().expect("a row is an object") {
            if name == "object_key" {
                continue;
            }
            assert_ne!(
                value,
                &serde_json::json!(FIXTURE_CHANNEL),
                "{name} carries the channel"
            );
            assert_ne!(
                value,
                &serde_json::json!(FIXTURE_FIRST_SEQ),
                "{name} carries a publisher sequence number"
            );
        }
    }
}

/// The venue's own sequence number reaches `upstream_seq` and nothing that
/// joins.
///
/// The distinction the whole column set rests on: a venue's counter is a
/// different series with a different owner and different loss semantics, so it
/// is evidence rather than a key — and writing it into a `sequence_number`
/// column would make the cross-site views compare two unrelated counters and
/// report a venue's session resend as a publisher's gap.
#[test]
fn a_venues_own_sequence_is_evidence_and_never_a_sequence_number() {
    let mut object = FixtureObject::of(
        BASE,
        &["chan=113 pubseq=990001 seq=5001 sid=7 quote AAA 100.50 3 100.60 4"],
    );
    let mut sink = CollectingSink::new();
    derive_venue_object(&mut adapter(), &mut object, &mut sink).expect("the object");
    let row = sink.book_tops()[0];
    // The venue's own number, and never the publisher-shaped one the object's
    // key states.
    assert_eq!(row.upstream_seq, Some(5_001));
    assert_ne!(row.upstream_seq, Some(FIXTURE_FIRST_SEQ));
    let json = serde_json::to_value(row).expect("a row serialises");
    let fields = json.as_object().expect("a row is an object");
    assert!(!fields.contains_key("sequence_number"));
    assert!(!fields.contains_key("channel_id"));
}

/// A delta book is accumulated, and the top is the best of each side.
#[test]
fn a_delta_book_is_accumulated_and_the_top_is_the_best_of_each_side() {
    let mut object = FixtureObject::of(
        BASE,
        &[
            "chan=113 pubseq=990001 seq=1 sid=7 level AAA bid 100.00 5",
            "chan=113 pubseq=990001 seq=2 sid=7 level AAA bid 101.00 6",
            // Behind the top: no change to the visible top, so no row.
            "chan=113 pubseq=990001 seq=3 sid=7 level AAA bid 99.00 7",
            "chan=113 pubseq=990001 seq=4 sid=7 level AAA ask 103.00 8",
            // A quantity of zero removes the level, so the top falls back.
            "chan=113 pubseq=990001 seq=5 sid=7 level AAA bid 101.00 0",
            "chan=113 pubseq=990001 seq=6 sid=7 clear AAA both",
        ],
    );
    let mut sink = CollectingSink::new();
    derive_venue_object(&mut adapter(), &mut object, &mut sink).expect("the object");
    assert_eq!(
        tops(&sink)
            .iter()
            .map(|(_, _, bid, _, ask, _)| (*bid, *ask))
            .collect::<Vec<_>>(),
        vec![
            (Some(10_000), None),
            (Some(10_100), None),
            (Some(10_100), Some(10_300)),
            (Some(10_000), Some(10_300)),
            (None, None),
        ]
    );
    // A level's order count is orders at a price, and a quote's source count is
    // upstreams contributing to a top. Different quantities, so a delta-derived
    // top leaves it absent rather than putting one in the other's column.
    for row in sink.book_tops() {
        assert_eq!(row.bid_source_count, None);
        assert_eq!(row.ask_source_count, None);
    }
}

/// An instrument the adapter discovers mid-object is admitted by the next poll.
///
/// The listings are polled once per message, which is the only cadence that is a
/// function of the object rather than of when the derivation ran — and therefore
/// the only one under which the same object derived twice produces one set of
/// rows.
#[test]
fn an_instrument_discovered_mid_object_is_admitted_by_the_next_poll() {
    let mut adapter = FixtureAdapter::new(vec![Listing::new("AAA", -2, 0)]);
    let mut object = FixtureObject::of(
        BASE,
        &[
            "chan=113 pubseq=990001 seq=1 sid=7 quote AAA 100.50 3 100.60 4",
            "chan=113 pubseq=990001 seq=2 sid=7 listing BBB -1 0",
            "chan=113 pubseq=990001 seq=3 sid=7 quote BBB 5.5 1 5.6 2",
        ],
    );
    let mut sink = CollectingSink::new();
    let derived = derive_venue_object(&mut adapter, &mut object, &mut sink).expect("the object");
    assert_eq!(derived.instrument_count, 2);
    assert_eq!(
        sink.book_tops()
            .iter()
            .map(|r| (r.symbol.clone(), r.price_exp, r.bid_px_raw))
            .collect::<Vec<_>>(),
        vec![
            ("AAA".to_owned(), -2, Some(10_050)),
            // At the exponent the venue stated for *this* instrument, not the
            // other one's.
            ("BBB".to_owned(), -1, Some(55)),
        ]
    );
    assert_eq!(
        adapter.polls, 4,
        "one poll per message, plus the one that ends the object"
    );
}

/// A value the venue's own exponent cannot state exactly is counted, and the
/// event is dropped.
///
/// Never rounded and never taken as zero. A rounded price is a price the venue
/// did not quote, and a conversion taken as zero is a real-looking quote at
/// nothing — the shipped defect the adapter boundary was shaped around.
#[test]
fn a_price_the_exponent_cannot_state_is_counted_and_not_rounded() {
    let mut object = FixtureObject::of(
        BASE,
        &[
            "chan=113 pubseq=990001 seq=1 sid=7 quote AAA 100.50 3 100.60 4",
            // Three decimals at an exponent of -2.
            "chan=113 pubseq=990001 seq=2 sid=7 quote AAA 100.505 3 100.60 4",
            "chan=113 pubseq=990001 seq=3 sid=7 quote AAA 100.52 3 100.60 4",
        ],
    );
    let mut sink = CollectingSink::new();
    let derived = derive_venue_object(&mut adapter(), &mut object, &mut sink).expect("the object");
    assert_eq!(derived.unpriced_count, 1);
    assert_eq!(
        sink.book_tops()
            .iter()
            .map(|r| r.bid_px_raw)
            .collect::<Vec<_>>(),
        vec![Some(10_050), Some(10_052)],
        "the value that could not be stated exactly became a row"
    );
}

/// The same object derived twice produces the same rows.
///
/// Acceptance: a venue-side object re-derived twice produces one set of rows,
/// not two. The row values are identical, so `(object key, sha256)` and the sort
/// key make the second load a replace rather than an accumulation.
#[test]
fn an_object_derived_twice_produces_the_same_rows() {
    let lines = [
        "chan=113 pubseq=990001 seq=1 sid=7 quote AAA 100.50 3 100.60 4",
        "chan=113 pubseq=990001 seq=2 sid=7 listing BBB -1 0",
        "chan=113 pubseq=990001 seq=3 sid=7 quote BBB 5.5 1 5.6 2",
        "chan=113 pubseq=990001 seq=4 sid=7 refuse malformed",
        "chan=113 pubseq=990001 seq=5 sid=7 quote AAA 100.51 3 100.60 4",
    ];

    let derive_once = || {
        let mut object = FixtureObject::of(BASE, &lines);
        let mut sink = CollectingSink::new();
        let derived =
            derive_venue_object(&mut adapter(), &mut object, &mut sink).expect("the object");
        let rows: Vec<VenueBookTop> = sink.book_tops().into_iter().cloned().collect();
        (derived, rows, sink.objects()[0].clone())
    };

    let first = derive_once();
    let second = derive_once();
    assert_eq!(
        first.0, second.0,
        "the counts moved between two derivations"
    );
    assert_eq!(first.1, second.1, "the rows moved between two derivations");
    assert_eq!(first.2, second.2, "the object row moved");
    assert!(!first.1.is_empty(), "an empty comparison proves nothing");
}

/// A truncated object is a refusal that names the object, all the way up.
///
/// Acceptance: the derivation does not hand back the rows it had managed to
/// derive from a half-read object. A partial window loaded as though it were
/// whole is a venue that went quiet.
#[test]
fn a_truncated_object_is_refused_by_the_derivation() {
    let whole = object_bytes(
        BASE,
        &[
            "chan=113 pubseq=990001 seq=1 sid=7 quote AAA 100.50 3 100.60 4",
            "chan=113 pubseq=990001 seq=2 sid=7 quote AAA 100.51 3 100.60 4",
        ],
    );
    let mut object = FixtureObject::over("site-1/recorder-1", whole[..whole.len() - 4].to_vec());
    let mut sink = CollectingSink::new();
    match derive_venue_object(&mut adapter(), &mut object, &mut sink) {
        Err(DeriveError::Format(e)) => {
            assert!(e.to_string().contains(FIXTURE_KEY), "{e}");
        }
        other => panic!("a truncated object was derived: {other:?}"),
    }
    assert!(
        sink.batches.is_empty(),
        "rows from a half-read object reached the sink"
    );
}

/// A connection the caller did not declare refuses the object.
///
/// `Payload::connection` is the only thing that distinguishes one upstream's
/// data from another's, and an adapter's mapping may depend on it. Substituting
/// one would hand the adapter a payload attributed to an upstream it did not
/// come from.
#[test]
fn a_connection_the_caller_did_not_declare_refuses_the_object() {
    let mut object = FixtureObject::of(
        BASE,
        &["chan=113 pubseq=990001 seq=1 sid=7 quote AAA 1.00 1 2.00 1"],
    )
    .with_no_declared_connections();
    let mut sink = CollectingSink::new();
    match derive_venue_object(&mut adapter(), &mut object, &mut sink) {
        Err(DeriveError::UndeclaredConnection {
            object_key,
            connection,
        }) => {
            assert_eq!(object_key, FIXTURE_KEY);
            assert_eq!(connection, "mktdata");
        }
        other => panic!("an undeclared connection was accepted: {other:?}"),
    }
}

/// An object holding nothing is a defect and not a quiet window.
///
/// An empty rotation is not published — a gap in the sequence of objects is how
/// a reader learns the archive has one — so an object with no messages has no
/// receive window to state, and stating one as zero would put a row in a
/// partition dated 1970.
#[test]
fn an_object_holding_no_messages_is_refused() {
    let mut object = FixtureObject::of(BASE, &[]);
    let mut sink = CollectingSink::new();
    match derive_venue_object(&mut adapter(), &mut object, &mut sink) {
        Err(DeriveError::EmptyObject { object_key }) => assert_eq!(object_key, FIXTURE_KEY),
        other => panic!("an empty object was derived: {other:?}"),
    }
}

/// Two observation points of one market produce rows that pair.
///
/// The property the race rests on, asserted here rather than only in SQL: two
/// observers of the same book compute the same `book_key`, so a pairing on it
/// finds them. Keyed on `state_key` neither could compute one at all.
#[test]
fn two_observation_points_of_one_book_agree_on_the_key() {
    let lines = ["chan=113 pubseq=990001 seq=1 sid=7 quote AAA 100.50 3 100.60 4"];
    let keys: Vec<u64> = ["site-1/recorder-1", "site-2/recorder-1"]
        .iter()
        .map(|observation| {
            let mut object = FixtureObject::at(BASE, observation, &lines);
            let mut sink = CollectingSink::new();
            derive_venue_object(&mut adapter(), &mut object, &mut sink).expect("the object");
            sink.book_tops()[0].book_key
        })
        .collect();
    assert_eq!(
        keys[0], keys[1],
        "two observers of one book computed two keys"
    );

    // And it is the function the publisher side uses, not a copy of it.
    let expected = dz_recorder_events::book_key(&dz_recorder_events::Top {
        bid: dz_recorder_events::Side {
            price_raw: Some(10_050),
            qty_raw: Some(3),
            source_count: None,
        },
        ask: dz_recorder_events::Side {
            price_raw: Some(10_060),
            qty_raw: Some(4),
            source_count: None,
        },
    });
    assert_eq!(keys[0], expected);
}
