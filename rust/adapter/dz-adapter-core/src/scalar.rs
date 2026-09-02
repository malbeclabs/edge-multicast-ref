//! A price or a quantity, as the venue states it.

/// One numeric value on its way to the wire, before the instrument's exponent
/// has been applied to it.
///
/// The wire carries prices at the instrument's `Price Exponent` and quantities
/// at its `Qty Exponent`, both declared in `InstrumentDefinition`. Producing
/// those integers belongs to the layer above, and this is what it is given.
///
/// This type performs no arithmetic. It is a statement of what the venue has,
/// in the units the venue has it, and nothing here can be scaled, compared or
/// tested for zero — a value that has not been converted has not yet been
/// judged, and every judgement worth making is made on the converted integer
/// where a refusal can be counted by reason.
///
/// # Why there are two variants
///
/// Both shapes occur in production, and neither can be dropped.
///
/// [`Text`](Self::Text) is what an upstream that quotes decimal strings hands
/// over — the common case, and the one where exactness is won or lost. The
/// conversion has three distinct failure modes and an operator acts differently
/// on each: a value too precise for the exponent means the exponent is wrong for
/// this instrument, a value that is not a decimal at all means the upstream
/// changed its format, and a value that does not fit means the field is too
/// narrow. A venue converting inline reports none of the three.
///
/// That is not a hypothetical cost. One existing publisher holds two
/// implementations of this conversion: an exact, string-only one that refuses
/// what it cannot represent, and one through `f64` with a `.round()`. The
/// rounding one is what its live market-data path calls, and it takes the
/// failure as `.unwrap_or(0)` — so a value it cannot convert is published as a
/// price of zero, with the side's *updated* flag set, which is a real-looking
/// quote at nothing rather than a counted refusal.
///
/// [`Fixed`](Self::Fixed) is for a venue whose own book already holds integers.
/// Forcing it to render them back to a decimal string for this interface to
/// re-parse would be a second scaling, and an existing publisher's own reason
/// for refusing to do that is exact: a string round-trip *"would be a second
/// scaling that could drift"*, which would break the hash join it runs between
/// two of its feeds. So the integers are taken as integers, **with the exponent
/// they are already at**, and rescaled to the instrument's exactly or refused.
///
/// # Why there is no third variant
///
/// There is deliberately no way to supply an integer already at the
/// *instrument's* exponent. That variant would be the one shape that hands the
/// scaling decision back to the venue, and scaling is the decision this type
/// exists to take away. `Fixed` states the exponent it is at precisely so that
/// the rescale stays above this boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scalar<'a> {
    /// The venue's own decimal text, e.g. `"1234.56"`. Converted exactly or
    /// refused; never rounded.
    Text(&'a str),

    /// An integer whose true value is `mantissa * 10^exponent`. Rescaled to the
    /// instrument's exponent exactly or refused; never rounded.
    Fixed { mantissa: i64, exponent: i8 },
}

impl<'a> Scalar<'a> {
    /// The venue's decimal text.
    #[must_use]
    pub const fn text(value: &'a str) -> Self {
        Self::Text(value)
    }

    /// An integer at a stated exponent, where the true value is
    /// `mantissa * 10^exponent`.
    #[must_use]
    pub const fn fixed(mantissa: i64, exponent: i8) -> Self {
        Self::Fixed { mantissa, exponent }
    }
}
