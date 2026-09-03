//! The factor between a venue's quoted unit and the wire's.

use dz_adapter_core::Scalar;
use dz_edge_core::fixed_point;

/// How much of the underlying one contract is.
///
/// Strictly positive, and exact: `mantissa × 10^exponent` with `mantissa > 0`.
/// A zero would make every price division undefined and every quantity zero,
/// and a negative one has no meaning — so neither is representable.
///
/// See [`InstrumentSpec::quoted_per_contract`](dz_adapter_core::InstrumentSpec::quoted_per_contract)
/// for why this exists at all: the short version is that `Scalar::Fixed`
/// expresses a decimal rescale and a contract size need not be a power of ten,
/// so a venue that quotes per contract cannot bridge to the wire's units
/// through an exponent alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContractSize {
    mantissa: i64,
    exponent: i8,
}

impl ContractSize {
    /// The internal exponent a decimal contract size is parsed at.
    ///
    /// Nine places is far past any contract size a venue states, and being
    /// fixed means two venues stating the same size reach the same integers.
    const TEXT_EXPONENT: i8 = -9;

    /// A contract size, or `None` for one that is not strictly positive.
    #[must_use]
    pub const fn new(mantissa: i64, exponent: i8) -> Option<Self> {
        if mantissa <= 0 {
            return None;
        }
        Some(Self { mantissa, exponent })
    }

    /// The contract size a venue stated, as the reference-data owner reads it
    /// off an [`InstrumentSpec`](dz_adapter_core::InstrumentSpec).
    ///
    /// `None` for a value that is not strictly positive, and for decimal text
    /// that cannot be stated exactly at nine places — which is a refusal at
    /// admission rather than per message, and the right place for it: an
    /// instrument whose contract size we cannot represent must not be
    /// published at all.
    #[must_use]
    pub fn from_scalar(value: Scalar<'_>) -> Option<Self> {
        match value {
            Scalar::Fixed { mantissa, exponent } => Self::new(mantissa, exponent),
            Scalar::Text(text) => {
                let mantissa = fixed_point::parse_signed(text, Self::TEXT_EXPONENT).ok()?;
                Self::new(mantissa, Self::TEXT_EXPONENT)
            }
        }
    }

    /// The factor as an integer and the decimal places it sits at.
    #[must_use]
    pub const fn parts(self) -> (i64, i8) {
        (self.mantissa, self.exponent)
    }
}
