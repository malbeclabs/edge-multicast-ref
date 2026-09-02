//! A snapshot pulled from an adapter, framed.
//!
//! The framing is the runtime's and the book is the adapter's, so the test
//! drives it the way the runtime will: open a framer, hand it to
//! `Adapter::snapshot`, and close it.

use dz_adapter_core::{
    Adapter, AdapterError, EventSink, InstrumentRef, ListingSink, ParseError, Payload, Scalar,
    Side, SnapshotSink,
};
use dz_edge_core::AppMessage;
use dz_edge_mbp::{SnapshotBegin, SnapshotEnd, SnapshotLevel, SIDE_ASK, SIDE_BID, U16_UNAVAILABLE};
use dz_publisher_lowering::{DepthLowering, Instrument, InstrumentTable, LoweringError, SourceId};

const PRICE_EXPONENT: i8 = -4;
const QTY_EXPONENT: i8 = -2;
const INSTRUMENT_ID: u32 = 41;

fn source_id() -> SourceId {
    SourceId::new(7).expect("7 is an assigned production id")
}

fn table() -> InstrumentTable {
    let mut instruments = InstrumentTable::new();
    instruments.admit(Instrument {
        instrument_id: INSTRUMENT_ID,
        price_exponent: PRICE_EXPONENT,
        qty_exponent: QTY_EXPONENT,
    });
    instruments
}

fn handle() -> InstrumentRef {
    InstrumentRef::from_admission(0)
}

/// An adapter that holds a fixed book and nothing else.
///
/// Its only job is to write levels when asked, which is the half of the
/// snapshot that belongs to a venue. `bootstrapped` is what makes the
/// not-ready path reachable, because that is the case the runtime has to handle
/// without restarting.
struct FakeBook {
    bids: Vec<(&'static str, &'static str)>,
    asks: Vec<(&'static str, &'static str)>,
    bootstrapped: bool,
}

impl FakeBook {
    fn two_sided() -> Self {
        Self {
            // Outward from the top of each side, which is the order a
            // subscriber applies them in.
            bids: vec![("0.41", "5"), ("0.40", "7")],
            asks: vec![("0.43", "9")],
            bootstrapped: true,
        }
    }
}

impl Adapter for FakeBook {
    fn message_types(&self) -> &[&'static str] {
        &["book"]
    }

    fn poll_listings(&mut self, _out: &mut dyn ListingSink) {}

    fn on_payload(
        &mut self,
        _payload: &Payload<'_>,
        _out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        Ok(())
    }

    fn snapshot(
        &self,
        instrument: InstrumentRef,
        out: &mut dyn SnapshotSink,
    ) -> Result<(), AdapterError> {
        if instrument != handle() {
            return Err(AdapterError::UnknownInstrument);
        }
        if !self.bootstrapped {
            return Err(AdapterError::NotReady {
                detail: "no snapshot has seeded this book",
            });
        }
        for (px, qty) in &self.bids {
            out.level(Side::Bid, Scalar::text(px), Scalar::text(qty), Some(2));
        }
        for (px, qty) in &self.asks {
            out.level(Side::Ask, Scalar::text(px), Scalar::text(qty), None);
        }
        Ok(())
    }
}

#[test]
fn a_pulled_snapshot_frames_as_begin_then_its_levels_then_end() {
    let instruments = table();
    let mut lowering = DepthLowering::new(&instruments, source_id());
    let adapter = FakeBook::two_sided();

    let mut framer = lowering
        .open_snapshot(handle(), 9_001, 1_700_000_000, 25)
        .expect("the table holds this instrument");
    adapter
        .snapshot(handle(), &mut framer)
        .expect("the book is bootstrapped");
    let snapshot = framer.finish().expect("every level states exactly");

    assert_eq!(
        snapshot.begin,
        SnapshotBegin {
            instrument_id: INSTRUMENT_ID,
            anchor_seq: 9_001,
            total_levels: 3,
            snapshot_id: 1,
            // No delta has been sent for this instrument in this era, and zero
            // is what the specification defines for exactly that.
            last_instrument_seq: 0,
            timestamp_ns: 1_700_000_000,
            depth_bound: 25,
        }
    );
    assert_eq!(
        snapshot.begin.total_levels as usize,
        snapshot.levels.len(),
        "the count the begin declares is what was actually written, so a \
         subscriber counting fewer has genuinely lost one"
    );
    assert_eq!(
        snapshot.end,
        SnapshotEnd {
            instrument_id: INSTRUMENT_ID,
            anchor_seq: 9_001,
            snapshot_id: 1,
        },
        "the end repeats the identifiers so a subscriber that lost the begin \
         knows it did"
    );

    // The levels, in the order the adapter wrote them, at the instrument's own
    // exponents, each tied to this snapshot.
    assert_eq!(
        snapshot.levels,
        vec![
            SnapshotLevel {
                snapshot_id: 1,
                price_raw: 4_100,
                qty_raw: 500,
                order_count: 2,
                side: SIDE_BID,
                level_flags: 0,
            },
            SnapshotLevel {
                snapshot_id: 1,
                price_raw: 4_000,
                qty_raw: 700,
                order_count: 2,
                side: SIDE_BID,
                level_flags: 0,
            },
            SnapshotLevel {
                snapshot_id: 1,
                price_raw: 4_300,
                qty_raw: 900,
                // This feed's sentinel for "not exposed", which is not
                // top-of-book's.
                order_count: U16_UNAVAILABLE,
                side: SIDE_ASK,
                level_flags: 0,
            },
        ]
    );

    // Every message the framing produced encodes at its declared size, which
    // is the property the datagram builder above will rely on.
    let mut begin = [0u8; SnapshotBegin::SIZE];
    snapshot.begin.encode_into(&mut begin);
    assert_eq!(begin[0], SnapshotBegin::TYPE_ID);
    let mut end = [0u8; SnapshotEnd::SIZE];
    snapshot.end.encode_into(&mut end);
    assert_eq!(end[0], SnapshotEnd::TYPE_ID);
}

#[test]
fn the_begin_declares_the_sequence_the_deltas_have_reached() {
    // `Last Instrument Seq` is what a subscriber initialises its own tracker to
    // after applying the snapshot, which is the whole reason the counter lives
    // in the lowering rather than in the adapter.
    use dz_adapter_core::{ClearScope, Presence};

    let instruments = table();
    let mut lowering = DepthLowering::new(&instruments, source_id());

    let _ = lowering.lower_level(
        handle(),
        1,
        Side::Bid,
        Scalar::text("0.41"),
        Scalar::text("5"),
        None,
        Presence::New,
    );
    let _ = lowering.lower_clear(handle(), 2, ClearScope::BothSides);

    let framer = lowering.open_snapshot(handle(), 9_002, 1, 0).expect("held");
    let snapshot = framer.finish().expect("an empty book frames");
    assert_eq!(snapshot.begin.last_instrument_seq, 2);
    assert_eq!(snapshot.begin.total_levels, 0, "an empty book is a book");
}

#[test]
fn opening_a_snapshot_does_not_reset_the_delta_series() {
    // **Explicitly forbidden.** A subscriber that missed a snapshot and then
    // saw a delta numbered 1 could not tell a fresh post-snapshot delta from a
    // late duplicate of an old one. Keeping the series monotonic within the era
    // is what makes "at or below what I applied" mean duplicate and "more than
    // one above" mean gap.
    use dz_adapter_core::Presence;

    let instruments = table();
    let mut lowering = DepthLowering::new(&instruments, source_id());

    let _ = lowering.lower_level(
        handle(),
        1,
        Side::Bid,
        Scalar::text("0.41"),
        Scalar::text("5"),
        None,
        Presence::New,
    );
    let _ = lowering
        .open_snapshot(handle(), 9_003, 1, 0)
        .expect("held")
        .finish()
        .expect("frames");

    let after = lowering
        .lower_level(
            handle(),
            2,
            Side::Bid,
            Scalar::text("0.42"),
            Scalar::text("6"),
            None,
            Presence::Change,
        )
        .expect("lowers");
    assert_eq!(
        after.per_instrument_seq, 2,
        "the snapshot must not restart it"
    );
}

#[test]
fn two_snapshots_for_one_instrument_carry_different_ids() {
    // The id is what stops two overlapping snapshots being interleaved into one
    // wrong book, so it has to move even when nothing else about the
    // instrument has.
    let instruments = table();
    let mut lowering = DepthLowering::new(&instruments, source_id());
    let adapter = FakeBook::two_sided();

    let mut first = lowering.open_snapshot(handle(), 1, 1, 0).expect("held");
    adapter.snapshot(handle(), &mut first).expect("writes");
    let first = first.finish().expect("frames");

    let mut second = lowering.open_snapshot(handle(), 2, 2, 0).expect("held");
    adapter.snapshot(handle(), &mut second).expect("writes");
    let second = second.finish().expect("frames");

    assert_ne!(first.begin.snapshot_id, second.begin.snapshot_id);
    // And every level carries its own snapshot's id, which is what a subscriber
    // groups them by.
    for level in &second.levels {
        assert_eq!(level.snapshot_id, second.begin.snapshot_id);
    }
}

#[test]
fn a_level_that_cannot_be_stated_exactly_refuses_the_whole_snapshot() {
    // Nothing partial: an incomplete snapshot is worse than none, because a
    // subscriber cannot tell the difference between a level that was refused
    // and a level that was lost.
    struct TooPrecise;

    impl Adapter for TooPrecise {
        fn message_types(&self) -> &[&'static str] {
            &[]
        }
        fn poll_listings(&mut self, _out: &mut dyn ListingSink) {}
        fn on_payload(
            &mut self,
            _payload: &Payload<'_>,
            _out: &mut dyn EventSink,
        ) -> Result<(), ParseError> {
            Ok(())
        }
        fn snapshot(
            &self,
            _instrument: InstrumentRef,
            out: &mut dyn SnapshotSink,
        ) -> Result<(), AdapterError> {
            out.level(Side::Bid, Scalar::text("0.41"), Scalar::text("5"), None);
            out.level(Side::Bid, Scalar::text("0.41005"), Scalar::text("5"), None);
            Ok(())
        }
    }

    let instruments = table();
    let mut lowering = DepthLowering::new(&instruments, source_id());

    let mut framer = lowering.open_snapshot(handle(), 1, 1, 0).expect("held");
    TooPrecise.snapshot(handle(), &mut framer).expect("writes");

    let error = framer.finish().expect_err("a fifth decimal place at -4");
    assert_eq!(error.reason(), "too_precise");
    assert_eq!(error.field(), Some("snapshot_price"));
}

#[test]
fn an_unheld_handle_cannot_open_a_snapshot() {
    let instruments = table();
    let mut lowering = DepthLowering::new(&instruments, source_id());

    assert_eq!(
        lowering
            .open_snapshot(InstrumentRef::from_admission(9_999), 1, 1, 0)
            .expect_err("not held"),
        LoweringError::UnknownInstrument
    );
}

#[test]
fn an_adapter_that_is_not_ready_is_the_runtimes_to_skip() {
    // Not an error to restart on: a rotation that finds one instrument not
    // bootstrapped skips that slot and comes back, which is the difference
    // between one dormant instrument and a restart loop. The framer is simply
    // discarded, and the only cost is a snapshot id.
    let instruments = table();
    let mut lowering = DepthLowering::new(&instruments, source_id());
    let adapter = FakeBook {
        bootstrapped: false,
        ..FakeBook::two_sided()
    };

    let mut framer = lowering.open_snapshot(handle(), 1, 1, 0).expect("held");
    assert!(matches!(
        adapter.snapshot(handle(), &mut framer),
        Err(AdapterError::NotReady { .. })
    ));
}
