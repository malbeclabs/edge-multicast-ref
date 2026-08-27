/// Whether a value fitted its fixed-width field or had to be truncated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fit {
    Fitted,
    /// The value exceeded the field and was truncated to it. The specification
    /// permits this, but a publisher should report it once per reference-data
    /// load rather than silently or per message.
    Truncated,
    /// The value cannot be honestly represented in this field: it contains
    /// bytes outside ASCII, or an interior NUL byte that would truncate it
    /// for any reader treating the field as NUL-terminated.
    ///
    /// The specification describes these fields as NUL-padded ASCII. A value
    /// that violates this is still encoded - padded or truncated as usual -
    /// but reported here instead of silently accepted, so a publisher can
    /// count and log it once per reference-data load.
    ///
    /// Takes precedence over `Truncated` when a value is both unrepresentable
    /// (non-ASCII or NUL-containing) and too long: whether the field also had
    /// to be truncated is secondary to the fact that its contents cannot be
    /// trusted as ASCII.
    Unrepresentable,
}

/// Left-justify `value` into an `N`-byte field, NUL-padded, reporting whether it
/// had to be truncated, and whether it can be honestly represented as NUL-padded ASCII.
///
/// NUL and not space: the specification says these fields are null-padded, and a
/// space-padded value decodes to a different symbol.
///
/// Truncation cuts at `N` bytes, but never splits a multi-byte UTF-8 sequence:
/// if the byte at the cut point is a continuation byte, the cut backs up to the
/// nearest character boundary so the field never contains half a code point.
///
/// The specification describes these fields as NUL-padded ASCII. A value that
/// contains bytes outside ASCII or an interior NUL byte (which would truncate
/// the field for any reader that treats it as NUL-terminated) is still encoded -
/// this function never panics and never returns an error - but is reported as
/// `Fit::Unrepresentable` rather than `Fit::Fitted` or `Fit::Truncated`, so
/// the caller can decide what to do about it.
#[must_use]
pub fn pad_ascii<const N: usize>(value: &str) -> ([u8; N], Fit) {
    let bytes = value.as_bytes();
    let not_ascii = !value.is_ascii() || bytes.contains(&0);

    let (field, truncated) = if bytes.len() <= N {
        let mut field = [0u8; N];
        field[..bytes.len()].copy_from_slice(bytes);
        (field, false)
    } else {
        let mut cut = N;
        while cut > 0 && !value.is_char_boundary(cut) {
            cut -= 1;
        }
        let mut field = [0u8; N];
        field[..cut].copy_from_slice(&bytes[..cut]);
        (field, true)
    };

    let fit = if not_ascii {
        Fit::Unrepresentable
    } else if truncated {
        Fit::Truncated
    } else {
        Fit::Fitted
    };
    (field, fit)
}
