//! One clock, four readings, and why nothing here reads the host directly.
//!
//! Two crates below this one already take their time through a trait of their
//! own, for two different reasons, and both traits have to be satisfied by the
//! same object or the publisher has two clocks that disagree:
//!
//! - [`dz_ingress_core::Clock`] — `wall_ns` stamps a payload's arrival,
//!   `steady_ns` measures the idle budget and the backoff, and `sleep` is every
//!   await in the transport half.
//! - [`dz_publisher_refdata::Clock`] — `monotonic_ns` paces the definition
//!   cycle, `unix_ns` is `ManifestSummary`'s own timestamp field.
//!
//! The pairs mean the same two things under different names, and the split
//! within each pair is load-bearing rather than stylistic: an interval measured
//! on a wall clock that a time daemon steps backwards either fires a guard on a
//! healthy connection or suppresses it on a dead one, and a monotonic reading in
//! a wire timestamp field is a count of nanoseconds since this process started,
//! which is not a timestamp a subscriber can compare against anything.
//!
//! [`Clock`] is the two traits together, so a publisher holds one object and
//! `steady_ns` and `monotonic_ns` cannot come from different origins.
//!
//! # No test in this crate sleeps
//!
//! Everything the runtime does that is hard to get right is a duration: the
//! heartbeat interval, the manifest cadence, the definition cycle's lap, the
//! idle guard's window. A test that exercised those by sleeping would take as
//! long as the policy it tests and still prove nothing about it — a 60-second
//! guard costs the suite a minute and is asserted by nobody. With the clock
//! passed in, every one of them is a value a test states and reads back.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use dz_ingress_core::BoxFuture;

/// The publisher's clock: both traits below this crate, on one object.
///
/// Sealed by nothing, and there is nothing to seal — the point is that a caller
/// with an implementation of both halves already has one of these.
pub trait Clock: dz_ingress_core::Clock + dz_publisher_refdata::Clock {}

impl<T> Clock for T where T: dz_ingress_core::Clock + dz_publisher_refdata::Clock {}

/// The host's clocks.
///
/// The monotonic origin is taken once, at construction, and shared by every
/// clone: two clones with different origins would be two monotonic timelines,
/// and the definition pacer and the idle guard would be measuring against
/// different ones.
#[derive(Debug, Clone)]
pub struct SystemClock {
    origin: Arc<std::time::Instant>,
}

impl SystemClock {
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: Arc::new(std::time::Instant::now()),
        }
    }

    fn elapsed_ns(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    /// Nanoseconds of Unix time.
    ///
    /// A host whose clock is set before 1970 has no sensible reading here, and
    /// `0` at least sorts before every real timestamp instead of wrapping to
    /// the far future, which is what a subtraction would do.
    fn unix(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| {
                u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
            })
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl dz_ingress_core::Clock for SystemClock {
    fn wall_ns(&self) -> u64 {
        self.unix()
    }

    fn steady_ns(&self) -> u64 {
        self.elapsed_ns()
    }

    fn sleep(&self, duration: Duration) -> BoxFuture<'_, ()> {
        Box::pin(tokio::time::sleep(duration))
    }
}

impl dz_publisher_refdata::Clock for SystemClock {
    fn monotonic_ns(&self) -> u64 {
        self.elapsed_ns()
    }

    fn unix_ns(&self) -> u64 {
        self.unix()
    }
}

/// A clock a caller sets by hand.
///
/// For tests, and for an offline re-run over an archive where the times that
/// matter are the ones the archive recorded rather than the ones the host is
/// experiencing now. Cloning hands back another handle onto the same readings,
/// so the test that advances the clock and the publisher that reads it can hold
/// one each.
///
/// [`sleep`](dz_ingress_core::Clock::sleep) returns an already-ready future and
/// does **not** advance the clock. It is not a simulated timer: nothing in this
/// crate's tests awaits it, because the tick loop is driven by calling
/// [`Publisher::tick`](crate::Publisher::tick) with a stated time rather than by
/// waiting for one. A caller that did await it in a loop would spin, which is
/// the honest behaviour for a clock that has been told nothing about time
/// passing.
#[derive(Debug, Clone, Default)]
pub struct ManualClock {
    readings: Arc<Mutex<Readings>>,
}

#[derive(Debug, Default)]
struct Readings {
    monotonic_ns: u64,
    unix_ns: u64,
}

impl ManualClock {
    /// Both readings at zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Both readings at a stated Unix time, with the monotonic reading starting
    /// from the same number.
    ///
    /// Convenient rather than meaningful: nothing may infer anything from a
    /// monotonic reading's absolute value, and starting the two together is
    /// only a way of writing one number in a test instead of two.
    #[must_use]
    pub fn at_unix_ns(unix_ns: u64) -> Self {
        Self {
            readings: Arc::new(Mutex::new(Readings {
                monotonic_ns: unix_ns,
                unix_ns,
            })),
        }
    }

    /// Move both readings forward, which is what time passing looks like.
    pub fn advance(&self, by: Duration) {
        let by = u64::try_from(by.as_nanos()).unwrap_or(u64::MAX);
        let mut readings = self.lock();
        readings.monotonic_ns = readings.monotonic_ns.saturating_add(by);
        readings.unix_ns = readings.unix_ns.saturating_add(by);
    }

    /// Move the Unix reading without touching the monotonic one, which is what
    /// a clock step looks like.
    pub fn step_unix_ns(&self, unix_ns: u64) {
        self.lock().unix_ns = unix_ns;
    }

    fn lock(&self) -> MutexGuard<'_, Readings> {
        // A poisoned lock means a test panicked while holding it, and the panic
        // is the failure worth reporting rather than this.
        self.readings
            .lock()
            .unwrap_or_else(|held| held.into_inner())
    }
}

impl dz_ingress_core::Clock for ManualClock {
    fn wall_ns(&self) -> u64 {
        self.lock().unix_ns
    }

    fn steady_ns(&self) -> u64 {
        self.lock().monotonic_ns
    }

    fn sleep(&self, _duration: Duration) -> BoxFuture<'_, ()> {
        Box::pin(std::future::ready(()))
    }
}

impl dz_publisher_refdata::Clock for ManualClock {
    fn monotonic_ns(&self) -> u64 {
        self.lock().monotonic_ns
    }

    fn unix_ns(&self) -> u64 {
        self.lock().unix_ns
    }
}
