//! The publisher's own identity on the wire.

/// A `Source ID` that the source registry's reserved ranges admit.
///
/// **The wire field is a `u16`, and three quarters of that space is a
/// conformance violation.** The registry states the ranges unconditionally:
/// `0` is reserved and MUST NOT be used on the wire, `1`–`1023` are the
/// assigned production matching engines, `1024`–`32767` are reserved for future
/// assignment, and `32768`–`65535` are private or experimental and a publisher
/// MAY use them for internal testing.
///
/// So a plain `u16` parameter on the lowering is a parameter with a wrong
/// answer available, and zero is the wrong answer a mis-read configuration file
/// hands you by default. This type is where that stops: the value is checked
/// once, where it is read, and every message afterwards carries something the
/// registry admits.
///
/// # What this does not check
///
/// *Which* assigned ID belongs to this publisher. That needs the registry
/// itself, and the specification's own conformance subscriber defers the same
/// question the same way — it refuses `0` unconditionally and range-checks only
/// when it has been given a registry. What is checkable without one is the
/// reserved ranges, and that is what is here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceId(u16);

impl SourceId {
    /// The assigned production range, inclusive.
    const ASSIGNED: std::ops::RangeInclusive<u16> = 1..=1023;
    /// The private and experimental range, inclusive.
    const PRIVATE: std::ops::RangeInclusive<u16> = 32768..=65535;

    /// A `Source ID`, or `None` for one the registry does not admit.
    ///
    /// `None` is a startup error for the caller to report against its own
    /// configuration key, which is why this is an `Option` and not one of the
    /// per-event errors: a publisher with no valid identity must not start, and
    /// it must not fail later, per message, once it has.
    #[must_use]
    pub const fn new(value: u16) -> Option<Self> {
        // `contains` is not const on RangeInclusive, so the bounds are read off
        // the constants rather than restated here - a second literal `1023` is
        // how the two would drift.
        let assigned = value >= *Self::ASSIGNED.start() && value <= *Self::ASSIGNED.end();
        let private = value >= *Self::PRIVATE.start() && value <= *Self::PRIVATE.end();
        if assigned || private {
            Some(Self(value))
        } else {
            None
        }
    }

    /// The value, for the wire.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}
