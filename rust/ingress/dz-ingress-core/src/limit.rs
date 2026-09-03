//! `[ingress] rate_limit_per_second`, and what it does and does not limit.

use std::time::Duration;

/// Paces what the publisher sends upstream.
///
/// # What this limits
///
/// Outbound messages: the authentication and subscription messages an adapter
/// writes from
/// [`on_connected`](dz_adapter_core::Adapter::on_connected). That is the only
/// traffic a publisher originates — it is a subscriber to its venue, not a
/// participant — and a venue's published limit is what this exists to stay
/// under. A publisher whose reconnect storms through a hundred subscription
/// messages is one that gets itself rate-limited off, which then reconnects,
/// which subscribes again.
///
/// # What it must never be confused with
///
/// `dz_publisher_ingress_rate_limited_total` counts the venue rate-limiting
/// *us*. Deferring a send here is this publisher obeying its own configured
/// limit and is a healthy, expected thing; recording it on that series would
/// make one counter mean both "we are behaving" and "we were thrown off", and
/// the alert built on it would be useless in both directions. The driver
/// records that series only from a
/// [`DisconnectReason::RateLimit`](dz_adapter_core::DisconnectReason::RateLimit).
///
/// # Strict spacing, not a bucket
///
/// A token bucket lets `n` messages go at once after an idle period, and a
/// reconnect is exactly an idle period followed by every subscription at once —
/// so the burst a bucket permits is the one moment this limiter is for. Even
/// spacing at `1/rate` is what a venue's "n per second" is usually enforcing
/// anyway, and it has the property that matters for a test: the wait before
/// each send is a value, not a range.
///
/// The pacing survives a reconnect deliberately. A venue's limit is against an
/// address or an account, not against a socket, so resetting it on reconnect
/// would hand a reconnect loop a fresh allowance every time — which is the
/// specific way this ends in a ban rather than in a delay.
#[derive(Debug, Clone, Copy)]
pub struct RateLimiter {
    /// Nanoseconds between two sends. `None` disables the limiter, which is
    /// what `rate_limit_per_second = 0` means.
    interval_ns: Option<u64>,
    /// The steady-clock reading at which the next send may go. Zero until the
    /// first send, which is the origin of the steady clock and therefore
    /// already in the past.
    next_free_ns: u64,
}

impl RateLimiter {
    /// The finest rate this limiter can pace, one send per nanosecond.
    ///
    /// Above it the interval divides to zero and every send goes immediately,
    /// so the limiter would report itself enabled while pacing nothing.
    /// `Policy` refuses a configured rate above this — see
    /// [`ConfigError::RateTooFine`](crate::ConfigError::RateTooFine) — and the
    /// constructor below clamps, so neither route can produce a limiter that
    /// says it is pacing when it is not.
    pub const FINEST_PER_SECOND: u32 = 1_000_000_000;

    /// A limiter for `per_second` messages, or none at all for `0`.
    ///
    /// A rate above [`FINEST_PER_SECOND`](Self::FINEST_PER_SECOND) is clamped
    /// to it rather than accepted as a zero interval. `0` and *too fast to
    /// pace* are different states and only one of them is "no pacing": an
    /// interval of zero would leave [`is_enabled`](Self::is_enabled) answering
    /// true about a limiter that never delays anything, and a limiter that
    /// lies about itself is worse than either behaviour it could have had.
    /// Configuration refuses the value before it reaches here; this is what
    /// makes the type honest for every other caller.
    #[must_use]
    pub const fn new(per_second: u32) -> Self {
        Self {
            interval_ns: if per_second == 0 {
                None
            } else if per_second >= Self::FINEST_PER_SECOND {
                Some(1)
            } else {
                Some(1_000_000_000 / per_second as u64)
            },
            next_free_ns: 0,
        }
    }

    /// How long to wait before sending now, charging the send to the limiter.
    ///
    /// Returns [`Duration::ZERO`] when the send may go immediately. Called once
    /// per send and only when the send is about to happen: an allowance taken
    /// for a message that was then not sent paces every later message behind a
    /// send that never occurred.
    pub fn charge(&mut self, now_ns: u64) -> Duration {
        let Some(interval_ns) = self.interval_ns else {
            return Duration::ZERO;
        };
        let wait_ns = self.next_free_ns.saturating_sub(now_ns);
        // From `next_free` rather than from `now`, so that a run of sends is
        // spaced by the interval rather than each one waiting the full interval
        // from when it happened to be offered.
        self.next_free_ns = self.next_free_ns.max(now_ns).saturating_add(interval_ns);
        Duration::from_nanos(wait_ns)
    }

    /// Whether this limiter paces anything.
    #[must_use]
    pub const fn is_enabled(self) -> bool {
        self.interval_ns.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rate_finer_than_the_clock_paces_at_the_clock_and_does_not_claim_more() {
        // The silent misconfiguration this exists to refuse: integer division
        // by a rate above a billion gives an interval of zero, and a limiter
        // with a zero interval delays nothing while `is_enabled` still answers
        // true. Whatever it does, it must not say one thing and do another.
        let mut limiter = RateLimiter::new(RateLimiter::FINEST_PER_SECOND * 2);
        assert!(limiter.is_enabled(), "the operator asked for pacing");
        // Charged twice at the same instant: the second send is spaced by the
        // finest interval the clock has rather than let through.
        assert_eq!(limiter.charge(0), Duration::ZERO);
        assert_eq!(limiter.charge(0), Duration::from_nanos(1));
    }

    #[test]
    fn zero_is_no_pacing_and_says_so() {
        // The other state, kept distinct: *too fast to pace* and *not pacing*
        // are different answers to `is_enabled`.
        let mut limiter = RateLimiter::new(0);
        assert!(!limiter.is_enabled());
        assert_eq!(limiter.charge(0), Duration::ZERO);
        assert_eq!(limiter.charge(0), Duration::ZERO);
    }

    #[test]
    fn a_zero_rate_paces_nothing() {
        let mut limiter = RateLimiter::new(0);
        assert!(!limiter.is_enabled());
        for _ in 0..100 {
            assert_eq!(limiter.charge(0), Duration::ZERO);
        }
    }

    #[test]
    fn a_burst_is_spaced_at_the_interval_rather_than_let_through() {
        // Five per second: 200ms apart. Every message is offered at the same
        // instant, which is what a reconnect does.
        let mut limiter = RateLimiter::new(5);
        let waits: Vec<u128> = (0..4).map(|_| limiter.charge(0).as_millis()).collect();
        assert_eq!(waits, vec![0, 200, 400, 600]);
    }

    #[test]
    fn an_allowance_unused_for_a_while_does_not_accumulate() {
        let mut limiter = RateLimiter::new(5);
        assert_eq!(limiter.charge(0), Duration::ZERO);
        // Ten seconds later, fifty allowances would have accrued in a bucket.
        // Two sends then go through with only the interval between them, not
        // both at once.
        assert_eq!(limiter.charge(10_000_000_000), Duration::ZERO);
        assert_eq!(
            limiter.charge(10_000_000_000).as_millis(),
            200,
            "a second send at the same instant must be spaced"
        );
    }

    #[test]
    fn the_pacing_is_measured_from_the_last_send_and_not_from_the_request() {
        let mut limiter = RateLimiter::new(2);
        assert_eq!(limiter.charge(0), Duration::ZERO);
        // 400ms in, the next slot is at 500ms: 100ms to wait, not 500.
        assert_eq!(limiter.charge(400_000_000).as_millis(), 100);
    }
}
