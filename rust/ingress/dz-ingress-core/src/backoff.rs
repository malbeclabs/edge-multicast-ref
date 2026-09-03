//! How long to wait before the next connect attempt.

use std::time::Duration;

use crate::error::ConfigError;

/// The two keys `[ingress]` gives, checked once.
///
/// Validated at construction rather than at use, because both mistakes
/// available here are silent at use: an inverted pair clamps on the first
/// delay, and a zero initial delay doubles to zero forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackoffPolicy {
    initial: Duration,
    max: Duration,
}

impl BackoffPolicy {
    /// A policy from the two configured durations.
    ///
    /// # Errors
    ///
    /// [`ConfigError::ZeroDuration`] for a zero initial delay, and
    /// [`ConfigError::BackoffInverted`] for a maximum below it. See
    /// [`ConfigError`] for why neither is quietly repaired.
    pub const fn new(initial: Duration, max: Duration) -> Result<Self, ConfigError> {
        if initial.is_zero() {
            return Err(ConfigError::ZeroDuration {
                key: "reconnect_backoff_initial",
            });
        }
        if max.as_nanos() < initial.as_nanos() {
            return Err(ConfigError::BackoffInverted { initial, max });
        }
        Ok(Self { initial, max })
    }

    /// The first delay, and the one a healthy connection resets to.
    #[must_use]
    pub const fn initial(self) -> Duration {
        self.initial
    }

    /// The ceiling every delay is capped at.
    #[must_use]
    pub const fn max(self) -> Duration {
        self.max
    }
}

/// The delay sequence: the initial delay, then doubling, capped.
///
/// # Why there is no jitter
///
/// A jittered sequence is the textbook answer, and it is the right one when
/// many clients reconnect in lockstep after one outage. It is not free here.
/// The configuration this family is held to has exactly two keys, so a jitter
/// fraction would be a third thing to spell and to agree on; and a random delay
/// is a second source of nondeterminism in the one part of this crate whose
/// correctness is a *sequence* — the test that proves the ceiling is reached
/// would have to assert a range instead of a value, which is how a cap that is
/// wrong by a factor of two goes unnoticed.
///
/// What buys most of jitter's benefit for a publisher's shape is already here:
/// the delay resets only after a connection that actually delivered a payload
/// (see [`Driver`](crate::Driver)), so a venue that accepts connections and
/// drops them does not get a herd back at the initial delay every time. If a
/// measured herd ever justifies jitter, it enters as an injected source of
/// randomness so that the sequence stays assertable — not as a call to a
/// thread-local generator inside here.
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    policy: BackoffPolicy,
    /// The delay the next call will return.
    next: Duration,
}

impl Backoff {
    /// A sequence at its start.
    #[must_use]
    pub const fn new(policy: BackoffPolicy) -> Self {
        Self {
            policy,
            next: policy.initial,
        }
    }

    /// The delay to wait now, advancing the sequence.
    pub fn next_delay(&mut self) -> Duration {
        let delay = self.next;
        // Saturating, then capped: a configured maximum near the end of the
        // range must not double past it and wrap to nothing, which would turn
        // the ceiling into a hot loop.
        self.next = self
            .next
            .checked_mul(2)
            .unwrap_or(self.policy.max)
            .min(self.policy.max);
        delay
    }

    /// Start the sequence again from the initial delay.
    ///
    /// Called by the driver for a connection that proved itself, and not merely
    /// for one that was accepted. See [`Driver`](crate::Driver) for what proof
    /// is and why accepting is not it.
    pub fn reset(&mut self) {
        self.next = self.policy.initial;
    }

    /// The delay the next call to [`next_delay`](Self::next_delay) will return.
    ///
    /// For a test or a log line; the driver does not need it.
    #[must_use]
    pub const fn peek(&self) -> Duration {
        self.next
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(initial_ms: u64, max_ms: u64) -> BackoffPolicy {
        BackoffPolicy::new(
            Duration::from_millis(initial_ms),
            Duration::from_millis(max_ms),
        )
        .expect("a valid policy")
    }

    #[test]
    fn the_sequence_doubles_from_the_initial_delay_and_stops_at_the_ceiling() {
        let mut backoff = Backoff::new(policy(500, 30_000));
        let delays: Vec<u128> = (0..9).map(|_| backoff.next_delay().as_millis()).collect();
        // The configured example values, spelled out. A cap that is applied one
        // step late, or a doubling that starts from the second delay, is a
        // different list.
        assert_eq!(
            delays,
            vec![500, 1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000, 30_000]
        );
    }

    #[test]
    fn a_reset_returns_to_the_initial_delay_rather_than_to_zero() {
        let mut backoff = Backoff::new(policy(500, 30_000));
        assert_eq!(backoff.next_delay(), Duration::from_millis(500));
        assert_eq!(backoff.next_delay(), Duration::from_millis(1_000));
        backoff.reset();
        // Not zero: a venue that closes a healthy connection on purpose - a
        // daily session boundary, a maintenance window - would otherwise be
        // reconnected against with no pause at all.
        assert_eq!(backoff.next_delay(), Duration::from_millis(500));
    }

    #[test]
    fn a_ceiling_near_the_end_of_the_range_does_not_wrap_to_no_delay() {
        let mut backoff = Backoff::new(
            BackoffPolicy::new(Duration::from_secs(u64::MAX / 3), Duration::MAX)
                .expect("a valid policy"),
        );
        let first = backoff.next_delay();
        let second = backoff.next_delay();
        assert!(second >= first, "the sequence went backwards: {second:?}");
        assert!(!second.is_zero());
    }

    #[test]
    fn a_zero_initial_delay_is_refused_rather_than_doubling_to_zero_forever() {
        let error = BackoffPolicy::new(Duration::ZERO, Duration::from_secs(30))
            .expect_err("zero must not be accepted");
        assert!(matches!(error, ConfigError::ZeroDuration { .. }), "{error}");
    }

    #[test]
    fn a_maximum_below_the_initial_delay_is_refused_rather_than_clamped() {
        let error = BackoffPolicy::new(Duration::from_secs(30), Duration::from_millis(500))
            .expect_err("a transposed pair must not be accepted");
        assert!(
            matches!(error, ConfigError::BackoffInverted { .. }),
            "{error}"
        );
    }
}
