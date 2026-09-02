//! Why a normalized event could not be lowered.

use dz_edge_core::fixed_point::ScaleError;

/// The two ways lowering fails, kept apart because an operator acts differently
/// on each.
///
/// Neither is a reason to end a connection or a process. Both are per-event:
/// the runtime counts the event, drops it, and takes the next one — a single
/// instrument whose exponent is wrong must not darken a feed.
///
/// # These variants are metric reasons
///
/// The three [`ScaleError`] cases are carried through rather than folded into
/// one "bad number", because each is a different operator action and the
/// distinction is lost the moment they are merged: a value too precise for the
/// exponent means the exponent is wrong for this instrument, a value that is
/// not a decimal means the upstream changed its format, and a value that does
/// not fit means the field is too narrow for what the venue quoted.
///
/// [`Self::reason`] is the label token each is counted under. The normative
/// metric set has no family dedicated to a lowering refusal yet, so the series
/// these land on is the runtime's decision when it lands; what this crate owes
/// is that the three stay distinguishable until then.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LoweringError {
    /// The event names an instrument the table does not hold.
    ///
    /// Reachable, and deliberately so: an `InstrumentRef` is a handle rather
    /// than a capability — it carries no proof of its own origin, because the
    /// runtime that mints one lives in a different crate from the boundary that
    /// carries it. So a handle can be forged, and it can outlive its
    /// instrument's withdrawal. This is where either is refused, once, where
    /// the refusal can be counted, instead of resolving to some other
    /// instrument's `Instrument ID` and publishing a quote under it.
    #[error("event names an instrument the table does not hold")]
    UnknownInstrument,

    /// A price or quantity could not be represented exactly at the instrument's
    /// exponent.
    ///
    /// `field` names the wire field, for the log line. It is never a label:
    /// the reason is.
    #[error("{field}: {source}")]
    Scale {
        field: &'static str,
        #[source]
        source: ScaleError,
    },
}

impl LoweringError {
    /// The label token this failure is counted under.
    #[must_use]
    pub const fn reason(self) -> &'static str {
        match self {
            Self::UnknownInstrument => "unknown_instrument",
            Self::Scale { source, .. } => match source {
                ScaleError::TooPrecise { .. } => "too_precise",
                ScaleError::Malformed => "malformed",
                ScaleError::Overflow => "overflow",
            },
        }
    }

    /// The wire field the failure is about, where there is one.
    #[must_use]
    pub const fn field(self) -> Option<&'static str> {
        match self {
            Self::UnknownInstrument => None,
            Self::Scale { field, .. } => Some(field),
        }
    }

    /// Attach a field name to a scaling refusal, for use as a `map_err`
    /// argument so the field is named once per call site rather than in every
    /// arm of the conversion.
    pub(crate) fn scale(field: &'static str) -> impl Fn(ScaleError) -> Self {
        move |source| Self::Scale { field, source }
    }
}
