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

use crate::error::LoweringError;
use crate::instrument::Instrument;

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

/// A price for this instrument, at its wire exponent, through its contract
/// factor if it has one.
///
/// # Errors
///
/// [`LoweringError::Scale`] naming `field` for a value that cannot be stated at
/// the exponent, and [`LoweringError::InexactContract`] for one the contract
/// size does not divide.
pub fn price_for(
    inst: &Instrument,
    value: Scalar<'_>,
    field: &'static str,
) -> Result<i64, LoweringError> {
    match inst.quoted_per_contract {
        None => price_at(value, inst.price_exponent).map_err(LoweringError::scale(field)),
        // A price is quoted **per contract**, so reaching the underlying
        // divides by how much of the underlying a contract is.
        Some(contract) => {
            let (mantissa, exponent) = fixed_parts(value, inst.price_exponent, field)?;
            let (factor, factor_exponent) = contract.parts();
            let shift =
                i16::from(exponent) - i16::from(factor_exponent) - i16::from(inst.price_exponent);
            let raw = exact(i128::from(mantissa), i128::from(factor), shift, field)?;
            i64::try_from(raw).map_err(|_| LoweringError::Scale {
                field,
                source: ScaleError::Overflow,
            })
        }
    }
}

/// A quantity for this instrument, at its wire exponent, through its contract
/// factor if it has one.
///
/// # Errors
///
/// As [`price_for`], plus [`ScaleError::Malformed`] for a negative value.
pub fn qty_for(
    inst: &Instrument,
    value: Scalar<'_>,
    field: &'static str,
) -> Result<u64, LoweringError> {
    match inst.quoted_per_contract {
        None => qty_at(value, inst.qty_exponent).map_err(LoweringError::scale(field)),
        // A quantity is **in contracts**, so reaching the underlying multiplies
        // by the same factor a price divides by. The two directions are what
        // makes one number enough.
        Some(contract) => {
            let (mantissa, exponent) = fixed_parts(value, inst.qty_exponent, field)?;
            if mantissa < 0 {
                return Err(LoweringError::Scale {
                    field,
                    source: ScaleError::Malformed,
                });
            }
            let (factor, factor_exponent) = contract.parts();
            let product = i128::from(mantissa).checked_mul(i128::from(factor)).ok_or(
                LoweringError::Scale {
                    field,
                    source: ScaleError::Overflow,
                },
            )?;
            let shift =
                i16::from(exponent) + i16::from(factor_exponent) - i16::from(inst.qty_exponent);
            let raw = exact(product, 1, shift, field)?;
            u64::try_from(raw).map_err(|_| LoweringError::Scale {
                field,
                source: ScaleError::Overflow,
            })
        }
    }
}

/// The integer and exponent behind a `Scalar`, parsing text at the instrument's
/// own exponent first.
///
/// Text is converted before the contract factor rather than after, because
/// `fixed_point` is the one exact decimal reader and re-implementing it here to
/// keep a rational in flight would be a second implementation of the function
/// this crate exists to have exactly one of. The cost is that a venue quoting
/// per contract as decimal text is held to the instrument's exponent twice —
/// which is the truthful outcome, since both statements have to be exact.
fn fixed_parts(
    value: Scalar<'_>,
    text_exponent: i8,
    field: &'static str,
) -> Result<(i64, i8), LoweringError> {
    match value {
        Scalar::Fixed { mantissa, exponent } => Ok((mantissa, exponent)),
        Scalar::Text(text) => {
            let mantissa = fixed_point::parse_signed(text, text_exponent)
                .map_err(LoweringError::scale(field))?;
            Ok((mantissa, text_exponent))
        }
    }
}

/// `numerator × 10^shift / denominator`, exactly or not at all.
///
/// One function for both directions, because the two failures have to stay
/// apart: a decimal shift that discards a non-zero digit is a precision loss at
/// the instrument's exponent, while a division the contract size does not
/// complete is a statement about the contract. They arrive at the same call
/// site and mean different things to whoever reads the metric.
fn exact(
    numerator: i128,
    denominator: i128,
    shift: i16,
    field: &'static str,
) -> Result<i128, LoweringError> {
    let overflow = || LoweringError::Scale {
        field,
        source: ScaleError::Overflow,
    };

    let (numerator, denominator) = if shift >= 0 {
        (
            numerator
                .checked_mul(power_of_ten_i128(shift)?.ok_or_else(overflow)?)
                .ok_or_else(overflow)?,
            denominator,
        )
    } else {
        (
            numerator,
            denominator
                .checked_mul(power_of_ten_i128(-shift)?.ok_or_else(overflow)?)
                .ok_or_else(overflow)?,
        )
    };

    if numerator % denominator != 0 {
        // Which of the two it is, told apart by whether a power of ten alone
        // would have divided it. The contract factor is the only other thing in
        // the denominator, so if ten divides what remains the loss is decimal.
        let decimal_only = is_power_of_ten(denominator);
        return Err(if decimal_only {
            LoweringError::Scale {
                field,
                source: ScaleError::TooPrecise { beyond: 1 },
            }
        } else {
            LoweringError::InexactContract { field }
        });
    }
    Ok(numerator / denominator)
}

/// `10^digits` as an `i128`, or `None` past what one holds.
fn power_of_ten_i128(digits: i16) -> Result<Option<i128>, LoweringError> {
    Ok(u32::try_from(digits)
        .ok()
        .and_then(|d| 10i128.checked_pow(d)))
}

/// Whether a value is a power of ten, for telling a decimal loss from a
/// contract one.
///
/// A free function rather than a trait method: one caller, and `is_` on a
/// by-value receiver is the shape a lint reads as a mistake.
fn is_power_of_ten(mut value: i128) -> bool {
    if value <= 0 {
        return false;
    }
    while value % 10 == 0 {
        value /= 10;
    }
    value == 1
}
