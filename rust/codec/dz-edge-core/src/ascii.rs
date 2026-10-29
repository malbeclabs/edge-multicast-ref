/// Whether a value fitted its fixed-width field or had to be truncated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fit {
    Fitted,
    /// The value exceeded the field and was truncated to it, at a code point
    /// boundary. The specification permits this, but a publisher should report
    /// it once per reference-data load rather than silently or per message.
    Truncated,
    /// The value cannot be honestly represented in this field: it contains an
    /// interior NUL byte. The specification ends the text at the first NUL, so a
    /// reader would see a shorter value than the one written.
    ///
    /// The value is still encoded - padded or truncated as usual - but reported
    /// here instead of silently accepted, so a publisher can count and log it
    /// once per reference-data load.
    ///
    /// Takes precedence over `Truncated` when a value is both: whether the field
    /// also had to be truncated is secondary to the fact that a reader cannot
    /// recover what was written.
    ///
    /// Bytes above `0x7F` are not a reason. `char[N]` is UTF-8 text
    /// (edge-feed-spec#104), and a `&str` is always valid UTF-8.
    Unrepresentable,
}

/// Left-justify `value` into an `N`-byte `char[N]` field, NUL-padded, reporting
/// whether it had to be truncated, and whether it can be honestly represented.
///
/// `char[N]` is UTF-8 text, left-justified and NUL-padded. A `&str` is valid
/// UTF-8 by construction, so what is left to check is length and an interior
/// NUL.
///
/// NUL and not space: the specification says these fields are null-padded, and a
/// space-padded value decodes to a different symbol.
///
/// Truncation cuts at `N` bytes, but never splits a multi-byte UTF-8 sequence:
/// if the byte at the cut point is a continuation byte, the cut backs up to the
/// nearest character boundary so the field never contains half a code point,
/// which the specification requires.
///
/// A value with an interior NUL byte (which ends the text for every reader) is
/// still encoded - this function never panics and never returns an error - but
/// is reported as `Fit::Unrepresentable` rather than `Fit::Fitted` or
/// `Fit::Truncated`, so the caller can decide what to do about it.
///
/// The specification also asks a publisher for Normalization Form C. That is
/// not done here: normalizing changes the bytes, and a caller that keys an
/// identity on them has to decide when that change happens.
#[must_use]
pub fn pad_text<const N: usize>(value: &str) -> ([u8; N], Fit) {
    let bytes = value.as_bytes();
    let interior_nul = bytes.contains(&0);

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

    let fit = if interior_nul {
        Fit::Unrepresentable
    } else if truncated {
        Fit::Truncated
    } else {
        Fit::Fitted
    };
    (field, fit)
}

/// [`pad_text`], under the name it had while `char[N]` was ASCII.
///
/// The same function: it no longer reports a value above `0x7F` as
/// [`Fit::Unrepresentable`].
#[must_use]
pub fn pad_ascii<const N: usize>(value: &str) -> ([u8; N], Fit) {
    pad_text::<N>(value)
}
