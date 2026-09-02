//! The snapshot: pulled, framed, and on the snapshot port role.
//!
//! The cadence, the rotation across instruments and the framing belong to the
//! runtime because they are what a subscriber's recovery depends on; the book
//! belongs to the adapter because it is the venue's microstructure. Neither can
//! drive the other, so the runtime asks — and this is the asking, the framing
//! and the sending.
//!
//! **The cadence is still the caller's**, and that is a missing key rather than
//! a missing implementation: the design names `[[feed]] snapshot_port` and no
//! snapshot interval, and inventing one is the one thing this crate must not do.
//! So [`Publisher::snapshot`] is called by whoever holds a policy, and
//! `run` does not call it.

mod harness;

use dz_adapter_core::{EventSink, Side};
use dz_edge_mbp::{SnapshotBegin, SnapshotEnd, SnapshotLevel};
use dz_publisher_runtime::SnapshotError;
use harness::{depth_feed, feed, harness, FakeAdapter, SOURCE_ID};

/// Message header flag bit 0: set on the snapshot port, cleared elsewhere.
/// Transcribed from the specification rather than read off the codec.
const FLAG_SNAPSHOT: u16 = 0x0001;
/// `Order Count` absent, on this feed. The opposite value from top-of-book's
/// `Source Count`, where zero means unavailable.
const U16_UNAVAILABLE: u16 = 0xFFFF;
const SIDE_BID: u8 = 0;
const SIDE_ASK: u8 = 1;

fn depth() -> harness::Harness {
    harness(depth_feed())
}

#[test]
fn a_pulled_snapshot_reaches_the_snapshot_port_as_a_begin_the_levels_and_an_end() {
    let mut h = depth();
    let mut adapter = FakeAdapter::new(&["A-B"]).with_book(&[
        (Side::Bid, "100.25", "2.500"),
        (Side::Bid, "100.24", "5.000"),
        (Side::Ask, "100.75", "1.250"),
    ]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    let framed = h
        .publisher
        .snapshot(&adapter, instrument, 10)
        .expect("the adapter holds a book for this instrument");

    // ---- the framing, as it went out ----
    // Three message types rather than one, because a snapshot is one book state
    // cut across datagrams and a subscriber has to be able to tell whether it
    // received all of it. Type ids transcribed from the market-by-price
    // specification: `0x20` begin, `0x42` level, `0x22` end.
    let messages = h.snapshot().messages_with_flags();
    let type_ids: Vec<u8> = messages.iter().map(|(id, _, _)| *id).collect();
    assert_eq!(type_ids, [0x20, 0x42, 0x42, 0x42, 0x22]);

    // The builder owns `Flags`, and the bit follows the port role rather than a
    // call site: a caller cannot set it on a mktdata message and cannot clear
    // it here.
    for (type_id, flags, _) in &messages {
        assert_eq!(
            *flags & FLAG_SNAPSHOT,
            FLAG_SNAPSHOT,
            "message {type_id:#04x} on the snapshot port did not carry the snapshot flag"
        );
    }

    let begin = SnapshotBegin::decode(&messages[0].2).expect("composed by this publisher");
    let levels: Vec<SnapshotLevel> = messages[1..4]
        .iter()
        .map(|(_, _, bytes)| SnapshotLevel::decode(bytes).expect("composed"))
        .collect();
    let end = SnapshotEnd::decode(&messages[4].2).expect("composed");

    // The level count the begin declares is what was actually written, so a
    // subscriber counting fewer than promised has genuinely lost one rather
    // than been told a number the publisher invented.
    assert_eq!(begin.total_levels, 3);
    assert_eq!(begin.instrument_id, 1);
    assert_eq!(begin.depth_bound, 10);
    // No depth delta has been sent for this instrument in this era, and that is
    // what a subscriber initialises its own tracker to after applying the
    // snapshot.
    assert_eq!(begin.last_instrument_seq, 0);
    // Not zero: zero is what an uninitialised field would read as, so the ids
    // start at one and leave zero meaning "no snapshot".
    assert_ne!(begin.snapshot_id, 0);

    // The end repeats the identifiers, so a subscriber that lost either one
    // knows it did.
    assert_eq!(end.instrument_id, begin.instrument_id);
    assert_eq!(end.snapshot_id, begin.snapshot_id);
    assert_eq!(end.anchor_seq, begin.anchor_seq);

    // Every level ties back to the begin, and they are in the order the adapter
    // wrote them: outward from the top of each side, which is the order a
    // subscriber applies them in.
    for level in &levels {
        assert_eq!(level.snapshot_id, begin.snapshot_id);
        // This feed's sentinel for `Order Count` absent.
        assert_eq!(level.order_count, U16_UNAVAILABLE);
        assert_eq!(level.level_flags, 0);
    }
    // Scaled at the instrument's own exponents, price -2 and quantity -3.
    assert_eq!(
        (levels[0].side, levels[0].price_raw, levels[0].qty_raw),
        (SIDE_BID, 10_025, 2_500)
    );
    assert_eq!(
        (levels[1].side, levels[1].price_raw, levels[1].qty_raw),
        (SIDE_BID, 10_024, 5_000)
    );
    assert_eq!(
        (levels[2].side, levels[2].price_raw, levels[2].qty_raw),
        (SIDE_ASK, 10_075, 1_250)
    );

    // What was returned is what went out.
    assert_eq!(framed.begin, begin);
    assert_eq!(framed.levels, levels);
    assert_eq!(framed.end, end);

    // ---- one book state, one datagram's worth of numbering ----
    // Packed and flushed together rather than one level per datagram: a
    // subscriber applies all of it or none of it, and a datagram per level
    // would spend a sequence number per level for nothing.
    assert_eq!(h.snapshot().len(), 1);
    for (index, (sequence, era)) in h.snapshot().headers().iter().enumerate() {
        assert_eq!(*sequence, index as u64);
        assert_eq!(*era, harness::MBP_ERA);
    }

    // And nothing snapshot-shaped reached the live port, which is the port
    // role's own guarantee: a snapshot on the mktdata port would land as a
    // duplicate of a live datagram and be discarded.
    for id in h.mktdata().type_ids() {
        assert!(
            !matches!(id, 0x20 | 0x42 | 0x22),
            "message {id:#04x} reached the mktdata port"
        );
    }
}

#[test]
fn the_snapshot_anchor_is_the_live_sequence_the_book_state_is_true_as_of() {
    // `Anchor Seq` is what tells a subscriber which live messages to apply
    // after the snapshot and which to discard, so it has to be the live
    // series' own position rather than a number of the snapshot series.
    let mut h = depth();
    let mut adapter = FakeAdapter::new(&["A-B"]).with_book(&[(Side::Bid, "100.25", "2.500")]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    // Three live datagrams on the mktdata series first.
    for step in 0..3 {
        h.publisher.event(harness::bid_level(instrument, step));
    }
    let framed = h
        .publisher
        .snapshot(&adapter, instrument, 10)
        .expect("framed");

    assert_eq!(h.mktdata().len(), 3);
    assert_eq!(
        framed.begin.anchor_seq, 3,
        "the anchor is the next live sequence number, which is the point the \
         book state is true as of"
    );
    // And the deltas already sent are what a subscriber resumes from.
    assert_eq!(framed.begin.last_instrument_seq, 3);
}

#[test]
fn opening_a_snapshot_does_not_reset_the_per_instrument_sequence() {
    // The one thing that ends the sequence's era is a `Reset Count` change. A
    // subscriber that missed a snapshot and then saw a delta numbered 1 could
    // not tell a fresh post-snapshot delta from a late duplicate of an old one.
    let mut h = depth();
    let mut adapter = FakeAdapter::new(&["A-B"]).with_book(&[(Side::Bid, "100.25", "2.500")]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.publisher.event(harness::bid_level(instrument, 1));
    h.publisher.event(harness::bid_level(instrument, 2));
    let framed = h
        .publisher
        .snapshot(&adapter, instrument, 10)
        .expect("framed");
    assert_eq!(framed.begin.last_instrument_seq, 2);

    h.publisher.event(harness::bid_level(instrument, 3));
    let levels: Vec<_> = h
        .mktdata()
        .messages()
        .iter()
        .filter(|(type_id, _)| *type_id == 0x40)
        .map(|(_, bytes)| dz_edge_mbp::LevelUpdate::decode(bytes).expect("composed"))
        .collect();
    assert_eq!(
        levels[2].per_instrument_seq, 3,
        "the snapshot restarted the sequence"
    );
}

#[test]
fn a_snapshot_of_a_book_that_has_not_bootstrapped_is_not_a_lowering_refusal() {
    // `AdapterError::NotReady` is a slot to skip and come back to, and it is
    // deliberately not counted as a scaling failure: an operator acts
    // differently on *this instrument's exponent is wrong* and *this
    // instrument's book is still warming up*.
    let mut h = depth();
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
    assert_eq!(h.publisher.refusals().total(), 0);
    // Nothing partial went out: an incomplete snapshot is worse than none,
    // because a subscriber cannot tell a refused level from a lost one.
    assert!(h.snapshot().datagrams().is_empty());
}

#[test]
fn a_publisher_that_emits_no_depth_feed_has_no_snapshot_to_serve() {
    // A top-of-book publisher has no book state to serve and no port to serve
    // it on, and asking for one is the caller's mistake rather than a runtime
    // condition.
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["A-B"]).with_book(&[(Side::Bid, "100.25", "2.500")]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    let error = h
        .publisher
        .snapshot(&adapter, instrument, 10)
        .expect_err("this publisher carries no snapshot port role");
    assert!(matches!(error, SnapshotError::NoDepthFeed), "{error}");
}

#[test]
fn a_snapshot_level_the_exponent_cannot_state_exactly_refuses_the_whole_snapshot() {
    // The refusal is recorded by the sink and surfaced by `finish`, because
    // `SnapshotSink::level` is called from inside the adapter's own loop over
    // its book and an adapter has nothing useful to do with a scaling refusal
    // on one level. Nothing partial is returned or sent.
    let mut h = depth();
    let mut adapter = FakeAdapter::new(&["A-B"]).with_book(&[
        (Side::Bid, "100.25", "2.500"),
        // Three decimal places at a price exponent of -2.
        (Side::Bid, "100.241", "5.000"),
    ]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    let error = h
        .publisher
        .snapshot(&adapter, instrument, 10)
        .expect_err("one level cannot be stated exactly");
    assert!(matches!(error, SnapshotError::Lowering(_)), "{error}");
    assert!(
        h.snapshot().datagrams().is_empty(),
        "a snapshot missing a level it declared reached the wire"
    );
    // The publisher is still running, and the instrument's own identity is
    // untouched.
    assert_eq!(h.publisher.refdata().published(), 1);
    let _ = SOURCE_ID;
}
