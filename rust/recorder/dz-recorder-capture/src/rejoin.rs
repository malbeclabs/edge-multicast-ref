//! When a membership is replaced, and when a join that failed may be left to
//! try again.
//!
//! Both decisions are pure functions of elapsed time and configuration. The
//! clock stays in the drain thread that owns it, so the policy is testable
//! with no socket, no privileges and no waiting.

use std::time::Duration;

/// The rejoin policy for one membership.
///
/// A membership goes away with the interface it was joined on, and nothing
/// reports that: the socket stays open, stays readable, and is permanently
/// silent. Silence is therefore the only symptom there is, and a cadence to
/// act on it is the only remedy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rejoiner {
    stale_after: Option<Duration>,
}

impl Rejoiner {
    #[must_use]
    pub const fn new(stale_after: Option<Duration>) -> Self {
        Self { stale_after }
    }

    #[must_use]
    pub const fn stale_after(&self) -> Option<Duration> {
        self.stale_after
    }

    /// `silent_for` is measured by the caller, so that this stays a policy.
    #[must_use]
    pub fn should_rejoin(&self, silent_for: Duration) -> bool {
        match self.stale_after {
            Some(stale_after) => silent_for >= stale_after,
            // A feed may legitimately be quiet, and a recorder configured with
            // no cadence has said it does not want silence acted on.
            None => false,
        }
    }
}

/// Whether a failed join may be deferred to the rejoin cadence rather than
/// reported.
///
/// With no cadence there is nothing for a deferral to happen on, and a thread
/// that can only sleep is worse than an error a human sees.
#[must_use]
pub const fn can_defer_to_cadence(stale_after: Option<Duration>) -> bool {
    stale_after.is_some()
}
