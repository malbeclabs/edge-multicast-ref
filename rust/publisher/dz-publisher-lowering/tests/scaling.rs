//! Exact or refused, and the same answer whichever shape the venue states it
//! in.

use dz_adapter_core::{InstrumentRef, Scalar, SideUpdate};
use dz_edge_core::fixed_point::ScaleError;
use dz_edge_core::AppMessage;
use dz_publisher_lowering::{
    price_at, qty_at, Instrument, InstrumentTable, Lowering, LoweringError,
};

const PRICE_EXPONENT: i8 = -4;
const QTY_EXPONENT: i8 = -2;

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

fn encoded(quote: &dz_edge_tob::Quote) -> [u8; dz_edge_tob::Quote::SIZE] {
    let mut bytes = [0u8; dz_edge_tob::Quote::SIZE];
    quote.encode_into(&mut bytes);
    bytes
}

#[test]
fn the_two_scalar_shapes_carrying_one_value_lower_to_identical_bytes() {
    // The property that lets a venue whose book already holds integers keep
    // them as integers. Its own reason for refusing to render them back to a
    // decimal string is that the round-trip would be a second scaling that
    // could drift; this asserts there is nothing to drift, because both shapes
    // reach the same bytes through the same rescale.
    let instruments = table();
    let lowering = Lowering::new(&instruments, source_id());
    let instrument = InstrumentRef::from_admission(0);

    // 0.41 stated as text, and stated as an integer at an exponent of the
    // venue's own choosing - here two decimal places, four coarser than the
    // instrument's.
    let as_text = SideUpdate::Present {
        px: Scalar::text("0.41"),
        qty: Scalar::text("5"),
        source_count: None,
    };
    let as_integers = SideUpdate::Present {
        px: Scalar::fixed(41, -2),
        qty: Scalar::fixed(5, 0),
        source_count: None,
    };

    let from_text = lowering
        .lower_quote(instrument, 7, as_text, SideUpdate::Gone)
        .expect("exact at this exponent");
    let from_integers = lowering
        .lower_quote(instrument, 7, as_integers, SideUpdate::Gone)
        .expect("exact at this exponent");

    assert_eq!(from_text, from_integers);
    assert_eq!(encoded(&from_text), encoded(&from_integers));
    assert_eq!(from_text.bid_price, 4_100);
    assert_eq!(from_text.bid_qty, 500);
}

#[test]
fn a_value_too_precise_for_the_exponent_is_refused_rather_than_rounded() {
    // Both shapes, because the refusal must not depend on which one the venue
    // used - and the alternative is not hypothetical: one publisher's live path
    // rounds here, and reports nothing.
    let instruments = table();
    let lowering = Lowering::new(&instruments, source_id());
    let instrument = InstrumentRef::from_admission(0);

    // Five decimal places at an exponent that carries four.
    for px in [Scalar::text("0.41005"), Scalar::fixed(41_005, -5)] {
        let refused = lowering.lower_quote(
            instrument,
            7,
            SideUpdate::Present {
                px,
                qty: Scalar::text("5"),
                source_count: None,
            },
            SideUpdate::Gone,
        );

        let error = refused.expect_err("a fifth decimal place cannot be stated at -4");
        assert_eq!(error.reason(), "too_precise");
        assert_eq!(error.field(), Some("bid_price"));
        assert!(matches!(
            error,
            LoweringError::Scale {
                source: ScaleError::TooPrecise { beyond: 1 },
                ..
            }
        ));
    }
}

#[test]
fn both_shapes_report_the_same_distance_past_the_cut() {
    // `beyond` is what tells an operator by how much the instrument's exponent
    // is wrong, so the two shapes agreeing on the number is what makes it
    // actionable rather than an artefact of how the venue stated the value.
    for (text, fixed) in [
        ("0.41005", Scalar::fixed(41_005, -5)),
        ("0.410005", Scalar::fixed(410_005, -6)),
        ("0.4100005", Scalar::fixed(4_100_005, -7)),
    ] {
        let from_text =
            price_at(Scalar::text(text), PRICE_EXPONENT).expect_err("too precise for the exponent");
        let from_fixed = price_at(fixed, PRICE_EXPONENT).expect_err("too precise for the exponent");
        assert_eq!(from_text, from_fixed, "{text} disagrees with its integer");
    }
}

#[test]
fn trailing_zeros_carry_no_precision() {
    // A venue stating 0.4100 at six decimal places is stating 0.41, and the
    // rescale must say so rather than refuse two zeros. Rounding and refusing
    // are both wrong here; only exactness is right.
    assert_eq!(
        price_at(Scalar::fixed(410_000, -6), PRICE_EXPONENT),
        Ok(4_100)
    );
    assert_eq!(
        price_at(Scalar::text("0.410000"), PRICE_EXPONENT),
        Ok(4_100)
    );
}

#[test]
fn each_scale_failure_reaches_its_own_reason() {
    // The three cases are three different operator actions, so they must not
    // collapse into one "bad number": a value too precise means this
    // instrument's exponent is wrong, a value that is not a decimal means the
    // upstream changed its format, and a value that does not fit means the
    // field is too narrow for what the venue quoted.
    let cases = [
        (Scalar::text("0.41005"), "too_precise"),
        (Scalar::text("not a number"), "malformed"),
        (Scalar::text("99999999999999999999"), "overflow"),
        (Scalar::fixed(41_005, -5), "too_precise"),
        (Scalar::fixed(i64::MAX, 4), "overflow"),
    ];

    let mut seen: Vec<&str> = Vec::new();
    for (value, reason) in cases {
        let error = LoweringError::Scale {
            field: "bid_price",
            source: price_at(value, PRICE_EXPONENT).expect_err("refused"),
        };
        assert_eq!(error.reason(), reason, "{value:?}");
        seen.push(error.reason());
    }
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(
        seen.len(),
        3,
        "all three reasons must be reachable, or one of them is unobservable"
    );
}

#[test]
fn a_negative_quantity_is_malformed_whichever_shape_states_it() {
    // A quantity is never negative. Taking the magnitude would publish resting
    // size the venue never quoted, and refusing it as "too precise" would send
    // an operator to the wrong place.
    assert_eq!(
        qty_at(Scalar::text("-5"), QTY_EXPONENT),
        Err(ScaleError::Malformed)
    );
    assert_eq!(
        qty_at(Scalar::fixed(-5, 0), QTY_EXPONENT),
        Err(ScaleError::Malformed)
    );
    // And the sign is refused before the precision, so the reason names the
    // sign rather than whatever the rescale would have said.
    assert_eq!(
        qty_at(Scalar::fixed(-5_001, -3), QTY_EXPONENT),
        Err(ScaleError::Malformed)
    );
}

#[test]
fn a_negative_price_is_carried_because_some_venues_quote_one() {
    assert_eq!(price_at(Scalar::text("-0.41"), PRICE_EXPONENT), Ok(-4_100));
    assert_eq!(price_at(Scalar::fixed(-41, -2), PRICE_EXPONENT), Ok(-4_100));
}

#[test]
fn zero_is_zero_at_every_exponent() {
    // Taken before any power of ten is computed, so an exponent difference
    // that would overflow while describing a value that is not there still
    // gives the right answer.
    assert_eq!(price_at(Scalar::fixed(0, 120), PRICE_EXPONENT), Ok(0));
    assert_eq!(price_at(Scalar::fixed(0, -120), PRICE_EXPONENT), Ok(0));
}

/// An assigned production id, which is what a publisher runs under. Zero would
/// be refused by the type - see `tests/source_id.rs`.
fn source_id() -> dz_publisher_lowering::SourceId {
    dz_publisher_lowering::SourceId::new(7).expect("7 is in the assigned range")
}
