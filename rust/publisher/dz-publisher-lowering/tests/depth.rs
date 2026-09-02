//! `Action`, the sequence, and the two pairings the specification forbids.

use dz_adapter_core::{ClearScope, InstrumentRef, Presence, Scalar, Side};
use dz_edge_core::AppMessage;
use dz_edge_mbp::{
    ACTION_CHANGE, ACTION_DELETE, ACTION_NEW, ACTION_UNKNOWN, CLEAR_ASK, CLEAR_BID, CLEAR_BOTH,
    SCOPE_ENTIRE_SIDE, SCOPE_FROM_PRICE, SIDE_ASK, SIDE_BID, U16_UNAVAILABLE,
};
use dz_publisher_lowering::{DepthLowering, Instrument, InstrumentTable, LoweringError, SourceId};

const PRICE_EXPONENT: i8 = -4;
const QTY_EXPONENT: i8 = -2;

fn source_id() -> SourceId {
    SourceId::new(7).expect("7 is an assigned production id")
}

fn table() -> InstrumentTable {
    let mut instruments = InstrumentTable::new();
    instruments.admit(Instrument {
        instrument_id: 41,
        price_exponent: PRICE_EXPONENT,
        qty_exponent: QTY_EXPONENT,
        quoted_per_contract: None,
    });
    instruments
}

fn handle() -> InstrumentRef {
    InstrumentRef::from_admission(0)
}

#[test]
fn the_action_table_is_exhausted_against_zero_and_non_zero_quantity() {
    // **The whole derivation, as a table.** Six cells: three `Presence` values
    // against a zero and a non-zero quantity. The two pairings the
    // specification forbids are the two that do not appear, and they do not
    // appear because there is no way to write them - which is the difference
    // between unrepresentable and merely refused.
    //
    // The defect this is aimed at reached live traffic: a publisher numbering
    // the action table from `New` instead of `Unknown` emitted every removal as
    // a change carrying zero. Self-consistent, so no round-trip test could see
    // it. The values below are transcribed from the specification's own table,
    // not read off the derivation they check.
    let instruments = table();
    let mut lowering = DepthLowering::new(&instruments, source_id());

    let cases = [
        (Presence::Unknown, "0", ACTION_DELETE),
        (Presence::New, "0", ACTION_DELETE),
        (Presence::Change, "0", ACTION_DELETE),
        (Presence::Unknown, "5", ACTION_UNKNOWN),
        (Presence::New, "5", ACTION_NEW),
        (Presence::Change, "5", ACTION_CHANGE),
    ];

    for (presence, qty, expected) in cases {
        let level = lowering
            .lower_level(
                handle(),
                1,
                Side::Bid,
                Scalar::text("0.41"),
                Scalar::text(qty),
                None,
                presence,
            )
            .expect("lowers");

        assert_eq!(
            level.action, expected,
            "{presence:?} with quantity {qty} produced action {}",
            level.action
        );

        // The two directions of the one rule, restated as properties so a
        // future change to the table above cannot satisfy the letter and break
        // the rule: a zero quantity is a removal, and a removal carries zero.
        assert_eq!(
            level.qty_raw == 0,
            level.action == ACTION_DELETE,
            "quantity zero and the removal action must come together in both \
             directions"
        );
    }
}

#[test]
fn a_removal_is_reachable_from_every_presence_and_no_other_action_is() {
    // Stated the other way round from the table above, because this is the
    // property a reader is most likely to doubt: an adapter that says `New` and
    // then sends a zero quantity does not get `New` on the wire. It cannot,
    // because the quantity is read first and unconditionally.
    let instruments = table();
    let mut lowering = DepthLowering::new(&instruments, source_id());

    for presence in [Presence::Unknown, Presence::New, Presence::Change] {
        let level = lowering
            .lower_level(
                handle(),
                1,
                Side::Ask,
                Scalar::text("0.41"),
                Scalar::fixed(0, QTY_EXPONENT),
                None,
                presence,
            )
            .expect("lowers");
        assert_eq!(level.action, ACTION_DELETE, "{presence:?}");
    }
}

#[test]
fn order_count_absent_is_this_feeds_sentinel_and_not_top_of_books() {
    // Two specifications answer the identical question with opposite values:
    // this feed says "not exposed" with `0xFFFF` and treats `0` as a real
    // count, while top-of-book's source count says "unavailable" with `0`. A
    // subscriber normalising the two fields into one reads "no orders" as a
    // fact on one plane and as absence on the other, so the two must never
    // share a helper that picks a side.
    let instruments = table();
    let mut lowering = DepthLowering::new(&instruments, source_id());

    let absent = lowering
        .lower_level(
            handle(),
            1,
            Side::Bid,
            Scalar::text("0.41"),
            Scalar::text("5"),
            None,
            Presence::New,
        )
        .expect("lowers");
    assert_eq!(absent.order_count, U16_UNAVAILABLE);

    let real_zero = lowering
        .lower_level(
            handle(),
            1,
            Side::Bid,
            Scalar::text("0.41"),
            Scalar::text("5"),
            Some(0),
            Presence::New,
        )
        .expect("lowers");
    assert_eq!(
        real_zero.order_count, 0,
        "zero is a real count on this feed and must reach the wire as one"
    );

    // Nothing at the boundary can state a level's rank at emission time, so it
    // is absent rather than guessed.
    assert_eq!(absent.level_index, U16_UNAVAILABLE);
}

#[test]
fn the_sequence_starts_at_one_is_dense_and_is_shared_with_clears() {
    // Three properties in one test because they are one property: the series is
    // what lets a subscriber localise a channel gap, and it only works if the
    // numbers are contiguous and if both message types that mutate the book
    // take theirs from the same counter.
    let instruments = table();
    let mut lowering = DepthLowering::new(&instruments, source_id());

    let first = lowering
        .lower_level(
            handle(),
            1,
            Side::Bid,
            Scalar::text("0.41"),
            Scalar::text("5"),
            None,
            Presence::New,
        )
        .expect("lowers");
    assert_eq!(
        first.per_instrument_seq, 1,
        "the first delta of an era is 1"
    );

    let clear = lowering
        .lower_clear(handle(), 2, ClearScope::BothSides)
        .expect("lowers");
    assert_eq!(
        clear.per_instrument_seq, 2,
        "a clear mutates the book, so it takes the next number in the same series"
    );

    let third = lowering
        .lower_level(
            handle(),
            3,
            Side::Ask,
            Scalar::text("0.43"),
            Scalar::text("7"),
            None,
            Presence::Change,
        )
        .expect("lowers");
    assert_eq!(third.per_instrument_seq, 3, "dense, with no skips");
}

#[test]
fn each_instrument_has_its_own_series() {
    // The field narrows a gap to the instruments that were in the lost frame,
    // which it can only do if the numbers are per instrument.
    let mut instruments = InstrumentTable::new();
    let first = instruments.admit(Instrument {
        instrument_id: 41,
        price_exponent: PRICE_EXPONENT,
        qty_exponent: QTY_EXPONENT,
        quoted_per_contract: None,
    });
    let second = instruments.admit(Instrument {
        instrument_id: 42,
        price_exponent: PRICE_EXPONENT,
        qty_exponent: QTY_EXPONENT,
        quoted_per_contract: None,
    });
    let mut lowering = DepthLowering::new(&instruments, source_id());

    for instrument in [first, second, first] {
        let _ = lowering.lower_clear(instrument, 1, ClearScope::BothSides);
    }

    assert_eq!(
        lowering
            .lower_clear(second, 2, ClearScope::BothSides)
            .expect("lowers")
            .per_instrument_seq,
        2,
        "the second instrument is on its own count, not the global one"
    );
}

#[test]
fn a_refused_message_consumes_no_sequence_number() {
    // A number spent on a message that never reached the wire is a phantom gap
    // every subscriber reads as packet loss - and the refusal is reachable, not
    // theoretical: a price the instrument's exponent cannot state exactly is
    // refused rather than rounded.
    let instruments = table();
    let mut lowering = DepthLowering::new(&instruments, source_id());

    let refused = lowering.lower_level(
        handle(),
        1,
        Side::Bid,
        Scalar::text("0.41005"),
        Scalar::text("5"),
        None,
        Presence::New,
    );
    assert!(matches!(refused, Err(LoweringError::Scale { .. })));

    let next = lowering
        .lower_level(
            handle(),
            2,
            Side::Bid,
            Scalar::text("0.41"),
            Scalar::text("5"),
            None,
            Presence::New,
        )
        .expect("lowers");
    assert_eq!(
        next.per_instrument_seq, 1,
        "the refused message must not have taken 1"
    );
}

#[test]
fn ending_an_era_restarts_every_series_at_one() {
    let instruments = table();
    let mut lowering = DepthLowering::new(&instruments, source_id());

    let _ = lowering.lower_clear(handle(), 1, ClearScope::BothSides);
    let _ = lowering.lower_clear(handle(), 2, ClearScope::BothSides);
    lowering.sequence_mut().end_era();

    assert_eq!(
        lowering
            .lower_clear(handle(), 3, ClearScope::BothSides)
            .expect("lowers")
            .per_instrument_seq,
        1,
        "a reset-count change restarts the series"
    );
}

#[test]
fn every_clear_scope_reaches_its_own_pair_of_bytes() {
    let instruments = table();
    let mut lowering = DepthLowering::new(&instruments, source_id());

    let cases = [
        (
            ClearScope::EntireSide(Side::Bid),
            CLEAR_BID,
            SCOPE_ENTIRE_SIDE,
            0,
        ),
        (
            ClearScope::EntireSide(Side::Ask),
            CLEAR_ASK,
            SCOPE_ENTIRE_SIDE,
            0,
        ),
        (ClearScope::BothSides, CLEAR_BOTH, SCOPE_ENTIRE_SIDE, 0),
        (
            ClearScope::FromPrice {
                side: Side::Ask,
                px: Scalar::text("0.43"),
            },
            CLEAR_ASK,
            SCOPE_FROM_PRICE,
            4_300,
        ),
    ];

    for (scope, side, wire_scope, from_price) in cases {
        let clear = lowering.lower_clear(handle(), 1, scope).expect("lowers");
        assert_eq!(clear.clear_side, side, "{scope:?}");
        assert_eq!(clear.scope, wire_scope, "{scope:?}");
        assert_eq!(clear.from_price_raw, from_price, "{scope:?}");
    }
}

#[test]
fn a_price_bounded_clear_of_both_sides_cannot_be_lowered_because_it_cannot_be_stated() {
    // The pairing the specification forbids, and the codec refuses at the push.
    // Here there is nothing to refuse: every price-bounded scope names exactly
    // one side, so the lowering never composes the pairing and the codec's own
    // validation is unreachable from this path.
    //
    // Asserted by construction rather than by assertion - every scope that
    // carries a price is lowered and then validated, and the validation the
    // codec would have failed passes for all of them.
    let instruments = table();
    let mut lowering = DepthLowering::new(&instruments, source_id());

    for side in [Side::Bid, Side::Ask] {
        let clear = lowering
            .lower_clear(
                handle(),
                1,
                ClearScope::FromPrice {
                    side,
                    px: Scalar::text("0.41"),
                },
            )
            .expect("lowers");
        assert_ne!(clear.clear_side, CLEAR_BOTH);
        clear
            .validate()
            .expect("the codec's own refusal is unreachable from this path");
    }
}

#[test]
fn an_unheld_handle_is_refused_before_a_number_is_taken() {
    let instruments = table();
    let mut lowering = DepthLowering::new(&instruments, source_id());
    let forged = InstrumentRef::from_admission(9_999);

    assert_eq!(
        lowering
            .lower_level(
                forged,
                1,
                Side::Bid,
                Scalar::text("0.41"),
                Scalar::text("5"),
                None,
                Presence::New,
            )
            .expect_err("not held"),
        LoweringError::UnknownInstrument
    );
    assert_eq!(
        lowering
            .lower_clear(forged, 1, ClearScope::BothSides)
            .expect_err("not held"),
        LoweringError::UnknownInstrument
    );

    // And the held instrument's series is untouched by either refusal.
    assert_eq!(
        lowering
            .lower_clear(handle(), 1, ClearScope::BothSides)
            .expect("lowers")
            .per_instrument_seq,
        1
    );
}

#[test]
fn a_side_reaches_its_own_byte() {
    let instruments = table();
    let mut lowering = DepthLowering::new(&instruments, source_id());

    for (side, expected) in [(Side::Bid, SIDE_BID), (Side::Ask, SIDE_ASK)] {
        let level = lowering
            .lower_level(
                handle(),
                1,
                side,
                Scalar::text("0.41"),
                Scalar::text("5"),
                None,
                Presence::New,
            )
            .expect("lowers");
        assert_eq!(level.side, expected, "{side:?}");
    }
}
