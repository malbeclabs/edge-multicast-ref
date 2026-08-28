//! Exact decimal-string to fixed-point conversion.
//!
//! A quoted price or size arrives as a decimal string; the wire carries it as
//! an integer at an instrument's implied decimal exponent, where the true
//! value is `raw * 10^exponent`. This module performs that conversion with
//! integer arithmetic only, refusing anything it cannot represent exactly.

/// Why a decimal string could not be converted at a given exponent.
///
/// The three cases are kept apart because a publisher counts them under
/// different reasons and an operator acts differently on each: `TooPrecise`
/// means our exponent is wrong for this instrument, `Malformed` means the
/// upstream source changed its format, and `Overflow` means the field is too
/// narrow for the value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ScaleError {
    #[error("value carries {beyond} digit(s) of precision beyond the exponent")]
    TooPrecise { beyond: u32 },

    #[error("not a decimal number in the accepted grammar")]
    Malformed,

    #[error("scaled value does not fit the target integer")]
    Overflow,
}

/// Convert a decimal string to a signed fixed-point integer at `exponent`.
///
/// A leading `-` is accepted; some venues quote negative prices.
pub fn parse_signed(text: &str, exponent: i8) -> Result<i64, ScaleError> {
    let (negative, rest) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (int_part, frac_part) = split_digits(rest)?;
    let magnitude = scale_digits(int_part, frac_part, exponent)?;
    to_signed(magnitude, negative)
}

/// Convert a decimal string to an unsigned fixed-point integer at `exponent`.
///
/// A leading sign is refused: a quantity is never negative, and accepting `-0`
/// or `-5` here would silently discard a sign the caller did not expect.
pub fn parse_unsigned(text: &str, exponent: i8) -> Result<u64, ScaleError> {
    if text.starts_with('-') {
        return Err(ScaleError::Malformed);
    }
    let (int_part, frac_part) = split_digits(text)?;
    scale_digits(int_part, frac_part, exponent)
}

/// Validate the narrow grammar and split `s` (no sign) into its integer and
/// fractional digit runs, as borrowed slices of `s`.
///
/// Accepts exactly: one or more decimal digits, optionally followed by a `.`
/// and one or more decimal digits. Everything else, including the empty
/// string, is `Malformed`. The fractional half is `""` when there is no `.`.
fn split_digits(s: &str) -> Result<(&str, &str), ScaleError> {
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return Err(ScaleError::Malformed);
    }

    let mut dot_pos: Option<usize> = None;
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'0'..=b'9' => {}
            b'.' if dot_pos.is_none() => dot_pos = Some(i),
            _ => return Err(ScaleError::Malformed),
        }
    }

    match dot_pos {
        None => Ok((s, "")),
        Some(pos) => {
            // A dot with nothing before it (".5") or nothing after it ("5.")
            // is refused; both digit runs are required.
            if pos == 0 || pos == bytes.len() - 1 {
                return Err(ScaleError::Malformed);
            }
            Ok((&s[..pos], &s[pos + 1..]))
        }
    }
}

/// Scale a validated value - `int_part` and `frac_part` are the digit runs
/// either side of the decimal point, as produced by `split_digits` - to a raw
/// unsigned magnitude at `exponent`, using only checked integer arithmetic.
fn scale_digits(int_part: &str, frac_part: &str, exponent: i8) -> Result<u64, ScaleError> {
    let len = int_part.len() + frac_part.len();
    let frac_len = frac_part.len();
    // The digit run int_part++frac_part * 10^(-frac_len) is the true value;
    // dividing by 10^exponent to reach the raw integer means shifting the
    // decimal point by `frac_len + exponent` digits to the left.
    let shift = frac_len as i64 + exponent as i64;

    if shift <= 0 {
        // The exponent asks for more digits than the string carries after
        // the point: pad with zeros on the right. Always exact.
        let pad = (-shift) as usize;
        accumulate(
            int_part
                .bytes()
                .chain(frac_part.bytes())
                .chain(std::iter::repeat_n(b'0', pad)),
        )
    } else {
        let drop = shift as usize;
        // The last `at_risk` digits of the whole value - read right to left,
        // i.e. from the end of frac_part back through int_part - are the
        // ones the exponent's cut point discards.
        let at_risk = drop.min(len);
        let tail = frac_part.bytes().rev().chain(int_part.bytes().rev());
        if let Some(beyond) = precision_beyond(tail.take(at_risk), shift) {
            return Err(ScaleError::TooPrecise { beyond });
        }

        if drop >= len {
            // Every digit was at risk and none were non-zero: nothing kept.
            Ok(0)
        } else {
            // Keep the leading `keep` digits. This split point may land
            // inside int_part or inside frac_part, so it is index
            // arithmetic against `int_part.len()` rather than a single
            // `split_at` on a combined string.
            let keep = len - drop;
            if keep <= int_part.len() {
                accumulate(int_part.bytes().take(keep))
            } else {
                accumulate(
                    int_part
                        .bytes()
                        .chain(frac_part.bytes().take(keep - int_part.len())),
                )
            }
        }
    }
}

/// Fold an iterator of ASCII decimal digit bytes into a `u64`, checking every
/// multiply and add.
fn accumulate(digit_bytes: impl Iterator<Item = u8>) -> Result<u64, ScaleError> {
    let mut value: u64 = 0;
    for b in digit_bytes {
        let digit = u64::from(b - b'0');
        value = value.checked_mul(10).ok_or(ScaleError::Overflow)?;
        value = value.checked_add(digit).ok_or(ScaleError::Overflow)?;
    }
    Ok(value)
}

/// How far the value's precision extends past the exponent's cut point.
///
/// `tail` must yield digit bytes in right-to-left order - nearest the cut
/// point first - covering exactly the digits the cut point discards. `shift`
/// is the distance from the cut point to the very end of the value (the
/// digit `tail` yields first sits `shift` positions beyond the exponent, the
/// next one `shift - 1`, and so on). Returns `None` when every one of those
/// digits is `'0'`, meaning no precision is actually lost.
fn precision_beyond(mut tail: impl Iterator<Item = u8>, shift: i64) -> Option<u32> {
    tail.position(|b| b != b'0')
        .map(|j| (shift - j as i64) as u32)
}

/// Apply a sign to a magnitude already known to fit `u64`, refusing anything
/// that does not fit `i64` at that sign.
fn to_signed(magnitude: u64, negative: bool) -> Result<i64, ScaleError> {
    /// The magnitude of `i64::MIN`, which has no positive `i64` counterpart.
    const MIN_MAGNITUDE: u64 = 1u64 << 63;

    if negative {
        if magnitude == MIN_MAGNITUDE {
            Ok(i64::MIN)
        } else if magnitude <= i64::MAX as u64 {
            // Cannot overflow: the guard above admits only magnitudes up to
            // i64::MAX, and the one i64 whose negation would overflow -
            // i64::MIN - is already handled by the MIN_MAGNITUDE arm.
            Ok(-(magnitude as i64))
        } else {
            Err(ScaleError::Overflow)
        }
    } else if magnitude <= i64::MAX as u64 {
        Ok(magnitude as i64)
    } else {
        Err(ScaleError::Overflow)
    }
}
