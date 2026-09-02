//! The snapshot: pulled, framed, and going nowhere.
//!
//! The cadence, the rotation across instruments and the framing belong to the
//! runtime because they are what a subscriber's recovery depends on; the book
//! belongs to the adapter because it is the venue's microstructure. Neither can
//! drive the other, so the runtime asks — and this is the asking.
//!
//! What the runtime cannot do yet is *send* the result, for two reasons that are
//! both somewhere else: the design names `[[feed]] snapshot_port` and no
//! snapshot interval, so there is nothing to pace against; and the three
//! snapshot message types have no `EgressMessageType` to be counted under. So
//! the framing is asserted here and the hole is one function wide.

mod harness;

use dz_adapter_core::Side;
use dz_publisher_runtime::SnapshotError;
use harness::{feed, harness, FakeAdapter};

#[test]
fn a_pulled_snapshot_frames_as_a_begin_the_levels_and_an_end() {
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["A-B"]).with_book(&[
        (Side::Bid, "100.25", "2.500"),
        (Side::Bid, "100.24", "5.000"),
        (Side::Ask, "100.75", "1.250"),
    ]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    let snapshot = h
        .publisher
        .snapshot(&adapter, instrument, 10)
        .expect("the adapter holds a book for this instrument");

    // The level count the begin declares is what was actually written, so a
    // subscriber counting fewer than promised has genuinely lost one rather
    // than been told a number the publisher invented.
    assert_eq!(snapshot.begin.total_levels, 3);
    assert_eq!(snapshot.levels.len(), 3);
    assert_eq!(snapshot.begin.instrument_id, 1);
    assert_eq!(snapshot.begin.depth_bound, 10);

    // The end repeats the identifiers, so a subscriber that lost either one
    // knows it did.
    assert_eq!(snapshot.end.instrument_id, snapshot.begin.instrument_id);
    assert_eq!(snapshot.end.snapshot_id, snapshot.begin.snapshot_id);
    assert_eq!(snapshot.end.anchor_seq, snapshot.begin.anchor_seq);
    // Not zero: zero is what an uninitialised field would read as, so the ids
    // start at one and leave zero meaning "no snapshot".
    assert_ne!(snapshot.begin.snapshot_id, 0);

    // Scaled at the instrument's own exponents, price -2 and quantity -3, and
    // in the order the adapter wrote them: outward from the top of each side,
    // which is the order a subscriber applies them in.
    assert_eq!(snapshot.levels[0].price_raw, 10_025);
    assert_eq!(snapshot.levels[0].qty_raw, 2_500);
    assert_eq!(snapshot.levels[1].price_raw, 10_024);
    assert_eq!(snapshot.levels[1].qty_raw, 5_000);
    assert_eq!(snapshot.levels[2].price_raw, 10_075);
    // This feed's sentinel for `Order Count` absent, transcribed: `0xFFFF`, and
    // the *opposite* value from top-of-book's `Source Count`, where zero means
    // unavailable. Two specifications, one question, opposite answers.
    for level in &snapshot.levels {
        assert_eq!(level.order_count, 0xFFFF);
    }

    // No depth delta has been sent for this instrument in this era, and that is
    // what a subscriber initialises its own tracker to after applying the
    // snapshot.
    assert_eq!(snapshot.begin.last_instrument_seq, 0);

    // And nothing reached the wire, which is the hole rather than a defect: the
    // top-of-book feed has no snapshot port role and there is no metric label
    // for a snapshot message.
    assert!(!h.mktdata.type_ids().contains(&0x30));
    assert!(h
        .refdata
        .type_ids()
        .iter()
        .all(|id| *id == 0x02 || *id == 0x07));
}

#[test]
fn opening_a_snapshot_does_not_reset_the_per_instrument_sequence() {
    // The one thing that ends the sequence's era is a `Reset Count` change. A
    // subscriber that missed a snapshot and then saw a delta numbered 1 could
    // not tell a fresh post-snapshot delta from a late duplicate of an old one.
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["A-B"]).with_book(&[(Side::Bid, "100.25", "2.500")]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    let before = h
        .publisher
        .depth_lowering_mut()
        .sequence_mut()
        .stamp(instrument);
    let snapshot = h
        .publisher
        .snapshot(&adapter, instrument, 10)
        .expect("framed");
    assert_eq!(snapshot.begin.last_instrument_seq, before);
    let after = h
        .publisher
        .depth_lowering_mut()
        .sequence_mut()
        .stamp(instrument);
    assert_eq!(after, before + 1, "the snapshot restarted the sequence");
}

#[test]
fn a_snapshot_of_a_book_that_has_not_bootstrapped_is_not_a_lowering_refusal() {
    // `AdapterError::NotReady` is a slot to skip and come back to, and it is
    // deliberately not counted as a scaling failure: an operator acts
    // differently on *this instrument's exponent is wrong* and *this
    // instrument's book is still warming up*.
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    let error = h
        .publisher
        .snapshot(&adapter, instrument, 10)
        .expect_err("the adapter holds no book");
    assert!(
        matches!(
            error,
            SnapshotError::Adapter(dz_adapter_core::AdapterError::NotReady { .. })
        ),
        "the adapter's own refusal was folded into a lowering refusal: {error}"
    );
    assert_eq!(
        h.publisher.refusals().total(),
        0,
        "an adapter that is not ready is not a lowering refusal"
    );
}
