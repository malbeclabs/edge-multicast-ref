//! The factor between a venue's quoted unit and the wire's.
//!
//! The values below are the ones a venue quoting a contract actually produces,
//! and they are derived here by hand rather than taken from the conversion
//! under test:
//!
//! A contract priced `6.3191`, held by the venue as `631_910_000` at `-8`, on
//! an instrument whose contract is `0.000100` of the underlying, is
//! `6.3191 / 0.0001 = 63_191.0` of the underlying — `6_319_100_000_000` at the
//! wire's `-8`. One contract of size, held as `100` at `-2`, is
//! `1.00 × 0.0001 = 0.0001` of the underlying, which is `10_000` at `-8`.
//!
//! Those are the same two integers the venue's own scale produces today, which
//! is the point: the conversion moved above the boundary and the wire did not
//! change.

use dz_adapter_core::{InstrumentRef, Scalar, SideUpdate};
use dz_publisher_lowering::{
    price_for, qty_for, ContractSize, Instrument, InstrumentTable, Lowering, LoweringError,
    SourceId,
};

/// A contract worth `0.000100` of the underlying, stated as the venue states
/// it.
fn contract() -> ContractSize {
    ContractSize::from_scalar(Scalar::text("0.000100")).expect("strictly positive and exact")
}

/// An instrument quoted per contract, declaring the underlying's exponents.
fn per_contract(size: ContractSize) -> Instrument {
    Instrument {
        instrument_id: 41,
        price_exponent: -8,
        qty_exponent: -8,
        quoted_per_contract: Some(size),
    }
}

fn source_id() -> SourceId {
    SourceId::new(7).expect("7 is an assigned production id")
}

#[test]
fn a_price_is_divided_by_the_contract_and_a_quantity_is_multiplied_by_it() {
    // One number, applied in opposite directions, which is what makes one
    // number enough: a price is *per contract* and a size is *in contracts*.
    let inst = per_contract(contract());

    assert_eq!(
        price_for(&inst, Scalar::fixed(631_910_000, -8), "price"),
        Ok(6_319_100_000_000)
    );
    assert_eq!(qty_for(&inst, Scalar::fixed(100, -2), "qty"), Ok(10_000));
}

#[test]
fn a_venue_whose_units_are_the_wires_converts_nothing() {
    // The ordinary case, and it must stay exactly what it was before this
    // factor existed - every other instrument in the fleet goes through here.
    let inst = Instrument {
        instrument_id: 41,
        price_exponent: -4,
        qty_exponent: -2,
        quoted_per_contract: None,
    };

    assert_eq!(price_for(&inst, Scalar::text("0.41"), "price"), Ok(4_100));
    assert_eq!(qty_for(&inst, Scalar::text("5"), "qty"), Ok(500));
}

#[test]
fn a_contract_that_does_not_divide_the_price_is_its_own_refusal() {
    // **The reason this is not a `ScaleError`.** The upstream's format is fine
    // and the exponent is fine; what does not hold is that the instrument's
    // declared contract size divides what the venue quoted. An operator sent to
    // look at the upstream by a `malformed` count would find nothing wrong
    // there.
    let inst = per_contract(ContractSize::new(3, -6).expect("strictly positive"));

    let error = price_for(&inst, Scalar::fixed(631_910_000, -8), "price")
        .expect_err("three does not divide this");
    assert_eq!(error, LoweringError::InexactContract { field: "price" });
    assert_eq!(error.reason(), "inexact_contract");
    assert_eq!(error.field(), Some("price"));
}

#[test]
fn a_decimal_loss_stays_a_decimal_loss_even_with_a_contract_in_play() {
    // The two failures arrive at the same call site and mean different things,
    // so they must not collapse. A contract of exactly one converts nothing, so
    // anything refused here is refused for the exponent.
    let inst = Instrument {
        instrument_id: 41,
        price_exponent: -4,
        qty_exponent: -2,
        quoted_per_contract: Some(ContractSize::new(1, 0).expect("one")),
    };

    let error = price_for(&inst, Scalar::fixed(41_005, -5), "price")
        .expect_err("a fifth decimal place at -4");
    assert_eq!(
        error.reason(),
        "too_precise",
        "a contract of one cannot be the cause"
    );
}

#[test]
fn a_contract_size_must_be_strictly_positive() {
    // Zero makes every price division undefined and every quantity zero; a
    // negative one has no meaning. Neither is representable rather than
    // refused later, per message.
    assert_eq!(ContractSize::new(0, -6), None);
    assert_eq!(ContractSize::new(-100, -6), None);
    assert_eq!(ContractSize::from_scalar(Scalar::text("0")), None);
    assert_eq!(ContractSize::from_scalar(Scalar::text("-0.0001")), None);
    assert!(ContractSize::from_scalar(Scalar::fixed(100, -6)).is_some());
}

#[test]
fn a_contract_size_too_precise_to_state_is_refused_at_admission() {
    // Ten decimal places is past what this reads, and refusing it here means
    // the instrument is never published - which is the right place for it. The
    // alternative is an instrument that is admitted and then refuses every
    // message it ever carries.
    assert_eq!(
        ContractSize::from_scalar(Scalar::text("0.0000000001")),
        None
    );
    assert!(ContractSize::from_scalar(Scalar::text("0.000000001")).is_some());
}

#[test]
fn the_two_scalar_shapes_state_one_contract_size_identically() {
    let from_text = ContractSize::from_scalar(Scalar::text("0.000100")).expect("exact");
    let from_integers = ContractSize::from_scalar(Scalar::fixed(100, -6)).expect("exact");

    let a = per_contract(from_text);
    let b = per_contract(from_integers);
    for value in [Scalar::fixed(631_910_000, -8), Scalar::fixed(1, 0)] {
        assert_eq!(
            price_for(&a, value, "price"),
            price_for(&b, value, "price"),
            "the shape a venue stated its contract in must not change the wire"
        );
    }
}

#[test]
fn a_quote_for_a_per_contract_instrument_reaches_the_wire_in_underlying_units() {
    // End to end, because the conversion has to reach the message and not just
    // the helper: a venue that states its own book's integers gets a quote in
    // the units its `InstrumentDefinition` declared.
    let mut instruments = InstrumentTable::new();
    instruments.admit(per_contract(contract()));
    let lowering = Lowering::new(&instruments, source_id());

    let quote = lowering
        .lower_quote(
            InstrumentRef::from_admission(0),
            1,
            SideUpdate::Present {
                px: Scalar::fixed(468_460_000, -8),
                qty: Scalar::fixed(100, -2),
                source_count: None,
            },
            SideUpdate::Present {
                px: Scalar::fixed(631_910_000, -8),
                qty: Scalar::fixed(200, -2),
                source_count: None,
            },
        )
        .expect("the contract divides both prices");

    assert_eq!(quote.bid_price, 4_684_600_000_000);
    assert_eq!(quote.bid_qty, 10_000);
    assert_eq!(quote.ask_price, 6_319_100_000_000);
    assert_eq!(quote.ask_qty, 20_000);
}

#[test]
fn a_negative_quantity_is_still_malformed_with_a_contract_in_play() {
    let inst = per_contract(contract());
    let error = qty_for(&inst, Scalar::fixed(-100, -2), "qty").expect_err("never negative");
    assert_eq!(error.reason(), "malformed");
}
