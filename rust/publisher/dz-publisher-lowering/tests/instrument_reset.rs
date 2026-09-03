//! Announcing that one instrument's state is being discarded.
//!
//! The message a publisher owes when it has lost confidence in its own book for
//! one instrument, and the capability the boundary was missing: an adapter owns
//! its book, so it is the only layer that can tell it has stopped being right,
//! and before `EventSink::desynchronised` existed it had no way to say so.
//!
//! Its three alternatives were all wrong. Publishing on leaves every later
//! absolute quantity at that price wrong for the rest of the era. Emitting a
//! clear tells subscribers the levels are gone when they are not — `Clear` is
//! documented as *not* a resynchronisation signal for exactly that reason. And
//! dropping the event silently is publishing on with less evidence.

use dz_adapter_core::{ClearScope, Desync, InstrumentRef, Presence, Scalar, Side};
use dz_edge_mbp::{
    RESET_PUBLISHER_INCONSISTENCY, RESET_UNSPECIFIED, RESET_UPSTREAM_GAP, RESET_VENUE_RESYNC,
};
use dz_publisher_lowering::{DepthLowering, Instrument, InstrumentTable, LoweringError, SourceId};

fn source_id() -> SourceId {
    SourceId::new(7).expect("7 is an assigned production id")
}

fn table() -> InstrumentTable {
    let mut instruments = InstrumentTable::new();
    instruments.admit(Instrument {
        instrument_id: 41,
        price_exponent: -4,
        qty_exponent: -2,
        quoted_per_contract: None,
    });
    instruments
}

fn handle() -> InstrumentRef {
    InstrumentRef::from_admission(0)
}

#[test]
fn each_thing_a_venue_can_say_reaches_its_own_wire_reason() {
    let instruments = table();
    let mut depth = DepthLowering::new(source_id());

    for (reason, expected) in [
        (Desync::Unspecified, RESET_UNSPECIFIED),
        (Desync::VenueResync, RESET_VENUE_RESYNC),
        (Desync::UpstreamGap, RESET_UPSTREAM_GAP),
    ] {
        let reset = depth
            .lower_instrument_reset(&instruments, handle(), 1_700, reason, 9_001)
            .expect("lowers");
        assert_eq!(reset.reason, expected, "{reason:?}");
        assert_eq!(reset.instrument_id, 41);
        assert_eq!(reset.timestamp_ns, 1_700);
    }
}

#[test]
fn a_silence_is_not_upgraded_into_a_claim_about_which_reason_it_was() {
    // `Unspecified` stays unspecified. An adapter that cannot say which of the
    // two observations it has is telling the truth, and a lowering that picked
    // one for it would put a claim on the wire nobody made — the same rule
    // `Presence::Unknown` follows on the action table.
    let instruments = table();
    let mut depth = DepthLowering::new(source_id());
    let reset = depth
        .lower_instrument_reset(&instruments, handle(), 1, Desync::Unspecified, 1)
        .expect("lowers");
    assert_eq!(reset.reason, RESET_UNSPECIFIED);
    assert_ne!(
        reset.reason, RESET_PUBLISHER_INCONSISTENCY,
        "an adapter cannot report a publisher-side integrity failure, so the \
         lowering must not report one on its behalf"
    );
}

#[test]
fn the_anchor_is_the_sequence_the_send_path_gave_it() {
    // `New Anchor Seq` is a promise about where the recovery snapshot will
    // apply, and the reset takes effect immediately — so the anchor is where
    // the stream is *now*, which only the send path knows. The specification's
    // own conformance subscriber grades a mismatch a violation, and the
    // off-by-one it catches is reading the number off the last delta instead.
    let instruments = table();
    let mut depth = DepthLowering::new(source_id());

    for sequence in [0, 1, 9_001, u64::MAX] {
        let reset = depth
            .lower_instrument_reset(&instruments, handle(), 1, Desync::UpstreamGap, sequence)
            .expect("lowers");
        assert_eq!(reset.new_anchor_seq, sequence);
    }
}

#[test]
fn a_reset_does_not_restart_the_per_instrument_series() {
    // **Explicitly not an era change.** The series restarts on a `Reset Count`
    // change and at no other time; the channel is intact here and every other
    // instrument on it is unaffected, which is the whole point of a
    // per-instrument signal. A restart would make the next delta look like the
    // first of an era to every subscriber on the channel.
    let instruments = table();
    let mut depth = DepthLowering::new(source_id());

    let first = depth
        .lower_clear(&instruments, handle(), 1, ClearScope::BothSides)
        .expect("lowers");
    assert_eq!(first.per_instrument_seq, 1);

    depth
        .lower_instrument_reset(&instruments, handle(), 2, Desync::UpstreamGap, 9_001)
        .expect("lowers");

    let after = depth
        .lower_level(
            &instruments,
            handle(),
            3,
            Side::Bid,
            Scalar::text("0.41"),
            Scalar::text("5"),
            None,
            Presence::New,
        )
        .expect("lowers");
    assert_eq!(
        after.per_instrument_seq, 2,
        "the reset must neither restart the series nor spend a number"
    );
}

#[test]
fn an_unheld_handle_cannot_announce_a_reset() {
    let instruments = table();
    let mut depth = DepthLowering::new(source_id());
    assert_eq!(
        depth
            .lower_instrument_reset(
                &instruments,
                InstrumentRef::from_admission(9_999),
                1,
                Desync::UpstreamGap,
                1,
            )
            .expect_err("not held"),
        LoweringError::UnknownInstrument
    );
}

#[test]
fn the_depth_feed_carries_it_and_top_of_book_does_not() {
    // `0x14` is in the market-by-price table and not in top-of-book's, so the
    // codec refuses it on the wrong feed — which is what the feed's own message
    // table is for. Asserted here because this is the crate that would compose
    // it for the wrong one.
    use dz_edge_core::Feed;
    assert!(dz_edge_mbp::MarketByPrice::carries(0x14));
    assert!(!dz_edge_tob::TopOfBook::carries(0x14));
}
