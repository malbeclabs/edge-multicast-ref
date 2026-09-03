//! The handle-to-instrument resolution, and what it refuses.
//!
//! An `InstrumentRef` is a handle, not a capability: it carries no proof of its
//! own origin, because the runtime that mints one lives in a different crate
//! from the boundary that carries it. So the guarantee that an adapter cannot
//! name an `Instrument ID` is only as good as this table's refusals, and these
//! are them.

use dz_adapter_core::{InstrumentRef, Scalar, SideUpdate};
use dz_publisher_lowering::{Instrument, InstrumentTable, Lowering, LoweringError};

fn instrument(instrument_id: u32) -> Instrument {
    Instrument {
        instrument_id,
        price_exponent: -4,
        qty_exponent: -2,
        quoted_per_contract: None,
    }
}

fn present() -> SideUpdate<'static> {
    SideUpdate::Present {
        px: Scalar::text("0.41"),
        qty: Scalar::text("5"),
        source_count: None,
    }
}

#[test]
fn admission_hands_back_a_handle_that_resolves_to_what_was_admitted() {
    let mut instruments = InstrumentTable::new();
    let first = instruments.admit(instrument(41));
    let second = instruments.admit(instrument(42));

    assert_ne!(first, second);
    assert_eq!(instruments.get(first).expect("held").instrument_id, 41);
    assert_eq!(instruments.get(second).expect("held").instrument_id, 42);
    assert_eq!(instruments.len(), 2);
}

#[test]
fn a_handle_the_table_never_held_is_refused_rather_than_resolved() {
    // The forged handle. Nothing stops an adapter constructing one, so the
    // refusal has to be here - and it has to be an error the runtime counts,
    // not a panic and not a silent drop.
    let mut instruments = InstrumentTable::new();
    instruments.admit(instrument(41));

    let forged = InstrumentRef::from_admission(9_999);
    assert_eq!(
        instruments.get(forged).expect_err("not held"),
        LoweringError::UnknownInstrument
    );

    let lowering = Lowering::new(source_id());
    let error = lowering
        .lower_quote(&instruments, forged, 7, present(), SideUpdate::Gone)
        .expect_err("an unheld handle cannot be lowered");
    assert_eq!(error.reason(), "unknown_instrument");
    assert_eq!(error.field(), None);
}

#[test]
fn a_withdrawn_instrument_leaves_a_hole_rather_than_shifting_its_neighbours() {
    // The failure this guards against: an adapter still carrying a handle for a
    // withdrawn instrument publishes a quote under whichever `Instrument ID`
    // moved into that slot. Slots never shift and are never reused, so a stale
    // handle resolves to nothing instead of to somebody else.
    let mut instruments = InstrumentTable::new();
    let first = instruments.admit(instrument(41));
    let second = instruments.admit(instrument(42));

    instruments.withdraw(first);

    assert_eq!(
        instruments.get(first).expect_err("withdrawn"),
        LoweringError::UnknownInstrument
    );
    assert_eq!(
        instruments.get(second).expect("untouched").instrument_id,
        42,
        "a withdrawal must not move another instrument's handle"
    );
    assert_eq!(instruments.len(), 1, "the withdrawn one is not published");

    // And the next admission takes a fresh slot rather than the hole.
    let third = instruments.admit(instrument(43));
    assert_ne!(third, first);
    assert_eq!(instruments.get(third).expect("held").instrument_id, 43);
}

#[test]
fn withdrawing_twice_and_withdrawing_nothing_are_both_the_state_asked_for() {
    let mut instruments = InstrumentTable::new();
    let only = instruments.admit(instrument(41));

    instruments.withdraw(only);
    instruments.withdraw(only);
    instruments.withdraw(InstrumentRef::from_admission(9_999));

    assert!(instruments.is_empty());
}

/// An assigned production id, which is what a publisher runs under. Zero would
/// be refused by the type - see `tests/source_id.rs`.
fn source_id() -> dz_publisher_lowering::SourceId {
    dz_publisher_lowering::SourceId::new(7).expect("7 is in the assigned range")
}

#[test]
fn a_restated_instrument_keeps_the_handle_the_adapter_is_carrying() {
    // Withdrawing and re-admitting would strand every copy of the handle the
    // adapter holds; leaving the table alone while republishing the definition
    // would put one scale on refdata and another on the quotes. Restating in
    // place is the third option, and it is the one that keeps both true.
    let mut instruments = InstrumentTable::new();
    let held = instruments.admit(instrument(41));

    assert!(instruments.replace(
        held,
        Instrument {
            instrument_id: 41,
            price_exponent: -6,
            qty_exponent: -2,
            quoted_per_contract: None,
        }
    ));
    assert_eq!(
        instruments.get(held).expect("still held").price_exponent,
        -6
    );
    assert_eq!(instruments.len(), 1, "restating is not admitting");

    // A withdrawn instrument has nothing to restate, and the caller wanted the
    // state it already has.
    instruments.withdraw(held);
    assert!(!instruments.replace(held, instrument(41)));
    assert!(instruments.is_empty());
}

#[test]
fn a_publisher_can_admit_an_instrument_while_it_is_lowering() {
    // The property the table-as-argument shape exists for, asserted rather
    // than described: the reference-data owner needs the table mutably to
    // admit, and a lowering that held it immutably for its own lifetime would
    // make a publisher stop lowering in order to list. For the depth lowering
    // that would be worse than inconvenient — releasing the borrow means
    // rebuilding it, and rebuilding it restarts the per-instrument sequence,
    // which no subscriber can tell apart from a channel reset.
    use dz_adapter_core::{ClearScope, Presence, Side};
    use dz_publisher_lowering::DepthLowering;

    let mut instruments = InstrumentTable::new();
    let first = instruments.admit(instrument(41));
    let mut lowering = DepthLowering::new(source_id());

    let one = lowering
        .lower_clear(&instruments, first, 1, ClearScope::BothSides)
        .expect("lowers");
    assert_eq!(one.per_instrument_seq, 1);

    // Admitting mid-stream, which the borrow checker would have refused.
    let second = instruments.admit(instrument(42));

    let two = lowering
        .lower_clear(&instruments, first, 2, ClearScope::BothSides)
        .expect("lowers");
    assert_eq!(
        two.per_instrument_seq, 2,
        "the sequence carried across the admission"
    );
    assert!(lowering
        .lower_level(
            &instruments,
            second,
            3,
            Side::Bid,
            Scalar::text("0.41"),
            Scalar::text("5"),
            None,
            Presence::New,
        )
        .is_ok());
}
