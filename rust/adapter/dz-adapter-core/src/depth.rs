//! `Depth Bound`: how much of a book a snapshot carries, stated rather than
//! implied.

use core::num::NonZeroU32;

/// How deep the book one snapshot carries goes.
///
/// # The wire's zero is a claim, not an absence
///
/// The field is a `u32`, and `0` **is a positive claim that this snapshot
/// carries the complete book**. Any other value is the number of levels per
/// side beyond which level state is *unknown rather than empty*. That
/// distinction is the whole point of the field: this repository's own
/// subscriber sums a snapshot's levels into available liquidity only when the
/// bound is zero, and treats every level past a non-zero bound as unknown.
///
/// So a bound nobody stated cannot be defaulted to zero. An existing depth
/// publisher's own encoder says it in as many words — *"defaulting to `0` would
/// make a never-snapshotted instrument assert completeness, which is the exact
/// failure this field exists to prevent"* — and the venue that ships it reached
/// its own `0` only through a venue-specific argument about its upstream, with
/// a check against a full-depth REST book gating the rollout. The number is the
/// same; the evidence behind it is not, and that evidence lives in the adapter.
///
/// # Two cases, and no way to avoid choosing
///
/// [`Complete`](Self::Complete) has to be typed, and [`Levels`](Self::Levels)
/// cannot hold zero, so the two states the wire carries are exactly the two
/// this type has and neither is reachable by accident. It is returned from
/// [`Adapter::snapshot`](crate::Adapter::snapshot) rather than passed to it,
/// which is what makes it unforgettable: an adapter cannot write a level
/// without also saying how deep the book those levels came from goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DepthBound {
    /// Every resting level is in this snapshot.
    ///
    /// The strongest thing a publisher can say about a book, and the one a
    /// subscriber is entitled to sum. An empty book is legitimately complete: a
    /// venue with no resting interest in an instrument has a complete book of
    /// nothing, which is not the same as a bound of nothing.
    Complete,

    /// Bounded at this many levels per side; past it, level state is unknown.
    ///
    /// Non-zero by construction, because zero is [`Complete`](Self::Complete)
    /// and a bound of zero would be a claim of completeness written by
    /// arithmetic.
    Levels(NonZeroU32),
}

impl DepthBound {
    /// A bound of `levels` per side, or `None` for zero.
    ///
    /// `None` rather than a silent promotion to [`Complete`](Self::Complete):
    /// an adapter that computed zero levels of depth has either an empty book —
    /// which is `Complete` and should say so — or a bug, and turning the second
    /// into the strongest claim on the feed is how this field gets its bad
    /// reputation.
    #[must_use]
    pub const fn levels(levels: u32) -> Option<Self> {
        match NonZeroU32::new(levels) {
            Some(levels) => Some(Self::Levels(levels)),
            None => None,
        }
    }

    /// The value the wire carries: `0` for [`Complete`](Self::Complete).
    #[must_use]
    pub const fn encoded(self) -> u32 {
        match self {
            Self::Complete => 0,
            Self::Levels(levels) => levels.get(),
        }
    }

    /// Whether this snapshot claims the whole book.
    #[must_use]
    pub const fn is_complete(self) -> bool {
        matches!(self, Self::Complete)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_encodes_the_wire_zero() {
        assert_eq!(DepthBound::Complete.encoded(), 0);
        assert!(DepthBound::Complete.is_complete());
    }

    #[test]
    fn a_bound_encodes_its_level_count() {
        let bound = DepthBound::levels(25).expect("25 is not zero");
        assert_eq!(bound.encoded(), 25);
        assert!(!bound.is_complete());
    }

    #[test]
    fn a_bound_of_zero_is_refused_rather_than_promoted() {
        // The one arithmetic path to a false claim of completeness, closed.
        // Promoting it to `Complete` would be this type deciding that a book it
        // was told nothing about is the whole book.
        assert_eq!(DepthBound::levels(0), None);
    }

    #[test]
    fn the_two_cases_are_the_two_the_wire_carries() {
        // Written as an exhaustive match rather than a count, so a third case
        // added later fails here - and the failure is the right one to have,
        // because a third case needs a wire state that does not exist.
        for bound in [
            DepthBound::Complete,
            DepthBound::levels(1).expect("1 is not zero"),
        ] {
            let encoded = match bound {
                DepthBound::Complete => 0,
                DepthBound::Levels(levels) => levels.get(),
            };
            assert_eq!(encoded, bound.encoded());
        }
    }
}
