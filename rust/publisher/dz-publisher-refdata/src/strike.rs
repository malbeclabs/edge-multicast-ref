//! Composing a `StrikeInterval` from what a venue states.

use dz_adapter_core::{Scalar, Strike, StrikeBound};
use dz_edge_refdata::{
    StrikeInterval, BOUND_LOWER_INCLUSIVE, BOUND_LOWER_PRESENT, BOUND_UPPER_INCLUSIVE,
    BOUND_UPPER_PRESENT,
};
use dz_publisher_lowering::{price_at, SourceId};

use crate::refusal::Refusal;

/// The `Strike Exponent` of a strike with no bound to scale. A pending strike
/// carries no value, so the field says nothing; zero is the value that says so.
const PENDING_EXPONENT: i8 = 0;

/// The `StrikeInterval` an instrument publishes for a stated strike.
///
/// # The exponent is chosen here, and the same value always gets the same one
///
/// A tick size is converted at the exponent the venue declares for prices. A
/// strike has no such declaration: `Strike Exponent` scales the value the venue
/// compares with the strike, which is not a price of the contract, and the wire
/// has one field for both bounds. So the exponent is the coarsest one, never
/// above zero, that states every present bound exactly — the venue's digits with
/// their trailing zeros dropped.
///
/// Dropping them is what makes two statements of one strike compose to the same
/// bytes. A venue that reports `0.0926679` on one path and `0.09266790` on
/// another has stated one strike, and a registry that compared the bytes of the
/// two would publish a correction that corrects nothing.
///
/// # Errors
///
/// [`Refusal::StrikeUnrepresentable`] for a bound that is not a decimal, does
/// not fit an `i64` at the exponent the other bound needs, or needs an exponent
/// below `i8::MIN`, and for a fixed strike with neither bound.
/// [`Refusal::StrikeEmpty`] for an interval no value is inside.
pub fn compose(
    strike: &Strike<'_>,
    instrument_id: u32,
    source_id: SourceId,
) -> Result<StrikeInterval, Refusal> {
    let source_id = source_id.get();
    let (lower, upper, fixing_time_ns) = match *strike {
        Strike::Pending => {
            return Ok(StrikeInterval::pending(
                instrument_id,
                source_id,
                PENDING_EXPONENT,
            ))
        }
        Strike::Fixed {
            lower,
            upper,
            fixing_time_ns,
        } => (lower, upper, fixing_time_ns),
    };
    if lower.is_none() && upper.is_none() {
        // What `Pending` is on the wire. A venue that has fixed a strike has
        // fixed at least one end of it.
        return Err(Refusal::StrikeUnrepresentable);
    }

    let mut strike_exponent = 0i8;
    for bound in [lower, upper].into_iter().flatten() {
        strike_exponent = strike_exponent.min(coarsest_exponent(bound.value)?);
    }
    let at = |bound: Option<StrikeBound<'_>>| match bound {
        None => Ok(0),
        Some(bound) => {
            price_at(bound.value, strike_exponent).map_err(|_| Refusal::StrikeUnrepresentable)
        }
    };

    let mut bound_flags = 0u8;
    if let Some(bound) = lower {
        bound_flags |= BOUND_LOWER_PRESENT;
        if bound.inclusive {
            bound_flags |= BOUND_LOWER_INCLUSIVE;
        }
    }
    if let Some(bound) = upper {
        bound_flags |= BOUND_UPPER_PRESENT;
        if bound.inclusive {
            bound_flags |= BOUND_UPPER_INCLUSIVE;
        }
    }
    let composed = StrikeInterval {
        instrument_id,
        source_id,
        strike_exponent,
        bound_flags,
        lower_bound: at(lower)?,
        upper_bound: at(upper)?,
        fixing_time_ns: fixing_time_ns.unwrap_or(0),
    };
    // Everything else `defect` names is ruled out by construction above: the
    // flags are built from the bounds, and an absent bound is zero.
    if composed.defect().is_some() {
        return Err(Refusal::StrikeEmpty);
    }
    Ok(composed)
}

/// The coarsest exponent, never above zero, at which this value is exact.
fn coarsest_exponent(value: Scalar<'_>) -> Result<i8, Refusal> {
    let digits = match value {
        // The digits after the point, less its trailing zeros. A text that is
        // not a decimal is refused by the conversion that follows, which names
        // it; here it only has to yield an exponent.
        Scalar::Text(text) => text
            .split_once('.')
            .map_or(0, |(_, fraction)| fraction.trim_end_matches('0').len()),
        Scalar::Fixed { mantissa, exponent } => {
            let mut mantissa = mantissa;
            let mut digits = i32::from(exponent).min(0).unsigned_abs() as usize;
            while digits > 0 && mantissa % 10 == 0 {
                mantissa /= 10;
                digits -= 1;
            }
            digits
        }
    };
    i8::try_from(digits)
        .map(|digits| -digits)
        .map_err(|_| Refusal::StrikeUnrepresentable)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SOURCE: u16 = 3;

    fn source() -> SourceId {
        SourceId::new(SOURCE).expect("a source id")
    }

    fn fixed<'a>(lower: Option<StrikeBound<'a>>, upper: Option<StrikeBound<'a>>) -> Strike<'a> {
        Strike::Fixed {
            lower,
            upper,
            fixing_time_ns: None,
        }
    }

    fn text(value: &str) -> Scalar<'_> {
        Scalar::text(value)
    }

    #[test]
    fn a_pending_strike_has_no_bound() {
        let composed = compose(&Strike::Pending, 7, source()).unwrap();
        assert_eq!(composed, StrikeInterval::pending(7, SOURCE, 0));
        assert!(composed.is_pending());
    }

    #[test]
    fn at_or_above_is_an_inclusive_lower_bound_at_the_venues_digits() {
        let strike = fixed(Some(StrikeBound::inclusive(text("82506.23"))), None);
        let composed = compose(&strike, 7, source()).unwrap();
        assert_eq!(
            (
                composed.strike_exponent,
                composed.bound_flags,
                composed.lower_bound,
                composed.upper_bound
            ),
            (-2, 0b0011, 8_250_623, 0)
        );
        assert_eq!((composed.instrument_id, composed.source_id), (7, SOURCE));
    }

    #[test]
    fn below_is_an_exclusive_upper_bound() {
        let strike = fixed(None, Some(StrikeBound::exclusive(text("74200"))));
        let composed = compose(&strike, 7, source()).unwrap();
        assert_eq!(
            (
                composed.strike_exponent,
                composed.bound_flags,
                composed.lower_bound,
                composed.upper_bound
            ),
            (0, 0b0100, 0, 74_200)
        );
    }

    #[test]
    fn both_bounds_share_the_exponent_the_finer_one_needs() {
        let strike = fixed(
            Some(StrikeBound::inclusive(text("92700"))),
            Some(StrikeBound::inclusive(text("92799.99"))),
        );
        let composed = compose(&strike, 7, source()).unwrap();
        assert_eq!(
            (
                composed.strike_exponent,
                composed.bound_flags,
                composed.lower_bound,
                composed.upper_bound
            ),
            (-2, 0b1111, 9_270_000, 9_279_999)
        );
    }

    /// One strike stated with different digits is one strike.
    #[test]
    fn trailing_zeros_do_not_change_what_is_composed() {
        let stated = |value| {
            compose(
                &fixed(Some(StrikeBound::inclusive(value)), None),
                7,
                source(),
            )
            .unwrap()
        };
        let whole = stated(text("0.0926679"));
        assert_eq!((whole.strike_exponent, whole.lower_bound), (-7, 926_679));
        assert_eq!(stated(text("0.09266790")), whole);
        assert_eq!(stated(Scalar::fixed(9_266_790, -8)), whole);
        assert_eq!(stated(text("2678.90")), stated(text("2678.9")));
        assert_eq!(stated(text("92700.00")), stated(Scalar::fixed(927, 2)));
    }

    #[test]
    fn a_negative_bound_is_carried() {
        let strike = fixed(Some(StrikeBound::exclusive(text("-1.5"))), None);
        let composed = compose(&strike, 7, source()).unwrap();
        assert_eq!((composed.strike_exponent, composed.lower_bound), (-1, -15));
    }

    #[test]
    fn the_fixing_time_is_carried_where_the_venue_gives_one() {
        let strike = Strike::Fixed {
            lower: Some(StrikeBound::inclusive(text("1"))),
            upper: None,
            fixing_time_ns: Some(1_700_000_000_000_000_000),
        };
        let composed = compose(&strike, 7, source()).unwrap();
        assert_eq!(composed.fixing_time_ns, 1_700_000_000_000_000_000);
    }

    #[test]
    fn a_fixed_strike_with_neither_bound_is_refused() {
        assert_eq!(
            compose(&fixed(None, None), 7, source()),
            Err(Refusal::StrikeUnrepresentable)
        );
    }

    #[test]
    fn a_bound_that_is_not_a_decimal_or_does_not_fit_is_refused() {
        for value in [
            text("TBD"),
            text(""),
            text("1e5"),
            text("92233720368547758.08"),
            Scalar::fixed(i64::MAX, 1),
        ] {
            assert_eq!(
                compose(
                    &fixed(Some(StrikeBound::inclusive(value)), None),
                    7,
                    source()
                ),
                Err(Refusal::StrikeUnrepresentable),
                "{value:?}"
            );
        }
    }

    /// The finer bound sets the exponent, and the other one has to fit at it.
    #[test]
    fn a_bound_that_overflows_at_the_other_bounds_exponent_is_refused() {
        let strike = fixed(
            Some(StrikeBound::inclusive(text("0.000000001"))),
            Some(StrikeBound::inclusive(text("9223372036854775807"))),
        );
        assert_eq!(
            compose(&strike, 7, source()),
            Err(Refusal::StrikeUnrepresentable)
        );
    }

    #[test]
    fn an_interval_no_value_is_inside_is_refused() {
        for (lower, upper) in [
            (
                StrikeBound::inclusive(text("201")),
                StrikeBound::inclusive(text("200")),
            ),
            (
                StrikeBound::exclusive(text("200")),
                StrikeBound::inclusive(text("200.0")),
            ),
        ] {
            assert_eq!(
                compose(&fixed(Some(lower), Some(upper)), 7, source()),
                Err(Refusal::StrikeEmpty)
            );
        }
        // A point is an interval: equal bounds, both inclusive.
        let point = fixed(
            Some(StrikeBound::inclusive(text("9"))),
            Some(StrikeBound::inclusive(text("9.0"))),
        );
        assert!(compose(&point, 7, source()).is_ok());
    }
}
