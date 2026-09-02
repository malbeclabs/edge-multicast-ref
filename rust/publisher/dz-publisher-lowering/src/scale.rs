//! Venue units to the instrument's exponent, exactly or not at all.
//!
//! Every number that reaches the wire passes through here, and nothing here
//! rounds. The wire carries a price as an integer at the instrument's `Price
//! Exponent` and a quantity at its `Qty Exponent`; a value that cannot be
//! stated exactly at that exponent is refused and counted, never nudged to the
//! nearest one that can.
//!
//! That is not a matter of taste. One existing publisher's live market-data
//! path converts through `f64` and `.round()` and takes the failure as
//! `unwrap_or(0)`, so a value it cannot convert reaches subscribers as a price
//! of zero with the side's *updated* flag set — a real-looking quote at
//! nothing. The same repository holds an exact, string-only conversion that
//! path does not call. Both halves of that outcome are unreachable from here:
//! there is one conversion, it refuses rather than rounds, and the refusal is a
//! `Result` the caller cannot ignore.

use dz_adapter_core::Scalar;
use dz_edge_core::fixed_point::{self, ScaleError};

/// A price at `exponent`, signed because some venues quote negative prices.
///
/// # Errors
///
/// [`ScaleError`], whose three cases are three different operator actions.
pub fn price_at(value: Scalar<'_>, exponent: i8) -> Result<i64, ScaleError> {
    match value {
        Scalar::Text(text) => fixed_point::parse_signed(text, exponent),
        Scalar::Fixed {
            mantissa,
            exponent: from,
        } => rescale_signed(mantissa, from, exponent),
    }
}

/// A quantity at `exponent`.
///
/// # Errors
///
/// [`ScaleError`] as [`price_at`], plus [`ScaleError::Malformed`] for a
/// negative value: a quantity is never negative, and silently taking the
/// magnitude of one would publish resting size the venue never quoted.
pub fn qty_at(value: Scalar<'_>, exponent: i8) -> Result<u64, ScaleError> {
    match value {
        Scalar::Text(text) => fixed_point::parse_unsigned(text, exponent),
        Scalar::Fixed {
            mantissa,
            exponent: from,
        } => {
            // Refused before the rescale, so a negative quantity reports being
            // negative rather than whatever the rescale would have said about
            // its precision. `parse_unsigned` refuses the sign in the same
            // order, and for the same reason.
            if mantissa < 0 {
                return Err(ScaleError::Malformed);
            }
            let raw = rescale_signed(mantissa, from, exponent)?;
            u64::try_from(raw).map_err(|_| ScaleError::Malformed)
        }
    }
}

/// Restate `mantissa * 10^from` as an integer at `to`.
///
/// The whole function is the two directions and the one refusal:
///
/// - `from == to` — already there.
/// - `from > to` — the target exponent is finer, so the value gains digits.
///   Always exact if it fits, [`ScaleError::Overflow`] if it does not.
/// - `from < to` — the target exponent is coarser, so digits are dropped.
///   Exact only if every dropped digit is zero, and [`ScaleError::TooPrecise`]
///   otherwise, carrying how far the value's precision reaches past the cut so
///   the operator knows by how much the exponent is wrong.
fn rescale_signed(mantissa: i64, from: i8, to: i8) -> Result<i64, ScaleError> {
    // Zero is zero at every exponent, and taking it first means the powers of
    // ten below never have to be computed for it — including for exponent
    // differences that would overflow while describing a value that is not
    // there.
    if mantissa == 0 {
        return Ok(0);
    }

    // i16, because `from - to` spans -254..=254 and would wrap in i8.
    let shift = i16::from(from) - i16::from(to);
    match shift.signum() {
        0 => Ok(mantissa),
        1 => {
            let factor = power_of_ten(shift)?;
            mantissa.checked_mul(factor).ok_or(ScaleError::Overflow)
        }
        _ => {
            let digits = -shift;
            let divisor = power_of_ten(digits)?;
            let remainder = mantissa % divisor;
            if remainder == 0 {
                Ok(mantissa / divisor)
            } else {
                Err(ScaleError::TooPrecise {
                    beyond: precision_beyond(remainder, digits),
                })
            }
        }
    }
}

/// `10^digits` as an `i64`, or [`ScaleError::Overflow`].
///
/// An exponent difference wider than an `i64` can hold a power of ten for is
/// `Overflow` and not a panic: the difference comes from a venue's own stated
/// exponent, which is input rather than something this crate controls.
fn power_of_ten(digits: i16) -> Result<i64, ScaleError> {
    debug_assert!(digits > 0, "callers handle zero and negative shifts");
    u32::try_from(digits)
        .ok()
        .and_then(|digits| 10i64.checked_pow(digits))
        .ok_or(ScaleError::Overflow)
}

/// How far a dropped remainder's precision reaches past the cut point, in
/// digits.
///
/// `1` means the value needed one more decimal place than the exponent allows,
/// which is the common case and the one an operator can act on directly.
/// Trailing zeros in the remainder do not count, because a zero carries no
/// precision: dropping `50` from a two-digit cut loses one digit, not two.
///
/// This is the same quantity `fixed_point`'s text path reports, computed the
/// same way round, so a `Text` and a `Fixed` carrying the same value that is
/// too precise for the same exponent report the same number.
fn precision_beyond(remainder: i64, digits: i16) -> u32 {
    let mut trailing_zeros = 0u32;
    let mut rest = remainder;
    while rest % 10 == 0 {
        trailing_zeros += 1;
        rest /= 10;
    }
    // `digits` is positive and at most 254 here, and `trailing_zeros` is
    // strictly below it because a non-zero remainder has a non-zero digit
    // inside the cut.
    u32::try_from(digits).unwrap_or(u32::MAX) - trailing_zeros
}
