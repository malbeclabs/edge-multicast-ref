//! The strike of a contract, as the venue states it.

use crate::scalar::Scalar;

/// One end of the interval a contract pays inside.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrikeBound<'a> {
    /// The venue's own value, unconverted. The layer above chooses the one
    /// exponent both ends are carried at, exactly or not at all.
    pub value: Scalar<'a>,
    /// Whether a settlement value equal to this bound is inside the interval:
    /// "at or above" and "at or below" are inclusive, "above" and "below" are
    /// not.
    pub inclusive: bool,
}

impl<'a> StrikeBound<'a> {
    /// A bound a settlement value may equal.
    #[must_use]
    pub const fn inclusive(value: Scalar<'a>) -> Self {
        Self {
            value,
            inclusive: true,
        }
    }

    /// A bound a settlement value must be strictly beyond.
    #[must_use]
    pub const fn exclusive(value: Scalar<'a>) -> Self {
        Self {
            value,
            inclusive: false,
        }
    }
}

/// What a venue says about the strike of an instrument whose contract pays on
/// one.
///
/// There are three things a venue can say and this type has two of them. The
/// third, *this contract pays on no strike*, is said by never calling
/// [`ListingSink::strike`](crate::ListingSink::strike) for the instrument. It
/// is not a variant because it is not a state an instrument moves through: an
/// instrument is published with a strike or without one, and stays that way.
///
/// # Derive the bounds from the comparison, not from which values are set
///
/// A venue can set a value its comparison does not use: a cap of `0` on a
/// contract that pays "at or above" a floor. That contract has a lower bound
/// and no upper one. An adapter reads the venue's stated comparison and builds
/// the bounds from it; only the adapter knows the venue's vocabulary for one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strike<'a> {
    /// The venue has listed the instrument and has not fixed its strike yet.
    Pending,
    /// The venue has fixed the strike. At least one bound is present; a strike
    /// stated with neither is refused, because that is what `Pending` looks
    /// like on the wire.
    Fixed {
        lower: Option<StrikeBound<'a>>,
        upper: Option<StrikeBound<'a>>,
        /// When the venue fixed it, in nanoseconds since the Unix epoch, where
        /// the venue says. `None` for a strike fixed at listing and for a venue
        /// that gives no time.
        fixing_time_ns: Option<u64>,
    },
}
