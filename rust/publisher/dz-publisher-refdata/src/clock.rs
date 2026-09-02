//! The two clocks this crate reads, and why they cannot be one.

/// Time, injected rather than read.
///
/// The definition cycle is paced, and pacing is the one thing this crate does
/// that a test cannot observe by inspecting a value. A pacer that read
/// [`std::time::Instant::now`] internally could only be tested by sleeping,
/// which makes the test slow and — because a sleep is a lower bound, not an
/// interval — occasionally wrong. Every method that depends on time here takes
/// its reading from this trait, so a test states the time and asserts the
/// emission, and the recorder re-running a venue's listings offline states the
/// archive's time instead of the wall's.
///
/// # Two readings, deliberately
///
/// The cycle is paced against [`monotonic_ns`](Self::monotonic_ns) and
/// `ManifestSummary`'s timestamp comes from [`unix_ns`](Self::unix_ns), because
/// a single reading would make one of them wrong. A wall clock that steps —
/// which is the ordinary behaviour of a host being brought into sync — would
/// either stall the cycle for the length of a backwards step or burst it
/// forward on a forwards one, and bursting the cycle is the rule this crate
/// exists to keep. A monotonic reading in the manifest would be worse: the
/// field is a timestamp a subscriber compares against its own clock, and a
/// count of nanoseconds since this process started is not one.
pub trait Clock {
    /// Nanoseconds from an arbitrary fixed point, never decreasing.
    ///
    /// Only differences are meaningful. Nothing may be inferred from the
    /// absolute value, including that it starts near zero.
    fn monotonic_ns(&self) -> u64;

    /// Nanoseconds of Unix time, for the wire's timestamp fields.
    fn unix_ns(&self) -> u64;
}

/// The host's clocks.
#[derive(Debug)]
pub struct SystemClock {
    origin: std::time::Instant,
}

impl SystemClock {
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: std::time::Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn monotonic_ns(&self) -> u64 {
        // `Instant` differences are what this trait promises, so the origin is
        // taken once at construction and every reading is an elapsed time from
        // it. `as u64` would be a truncation past 584 years of uptime.
        u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    fn unix_ns(&self) -> u64 {
        // The constant below is the standard library's name for the start of
        // Unix time. The glossary's word for one of our own generations is
        // `era`, and this is not one of those.
        use std::time::SystemTime;
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |since| {
                u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
            })
    }
}

/// A clock a caller sets by hand.
///
/// For tests, and for an offline re-run over an archive, where the times that
/// matter are the ones the archive recorded rather than the ones the host is
/// experiencing now.
///
/// Cloning hands back another handle onto the same readings, so the caller that
/// advances the clock and the [`Registry`](crate::Registry) that reads it can
/// hold one each.
#[derive(Debug, Clone, Default)]
pub struct ManualClock {
    inner: std::sync::Arc<std::sync::Mutex<Readings>>,
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

    /// Move the monotonic reading forward. It is the only one that moves on its
    /// own, because it is the only one anything here paces against.
    pub fn advance(&self, by: std::time::Duration) {
        let by = u64::try_from(by.as_nanos()).unwrap_or(u64::MAX);
        let mut inner = self.lock();
        inner.monotonic_ns = inner.monotonic_ns.saturating_add(by);
        inner.unix_ns = inner.unix_ns.saturating_add(by);
    }

    /// Set the Unix reading without touching the monotonic one, which is what a
    /// clock step is.
    pub fn set_unix_ns(&self, unix_ns: u64) {
        self.lock().unix_ns = unix_ns;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Readings> {
        // A poisoned lock here means a test panicked while holding it, and the
        // panic is the failure worth reporting rather than this.
        self.inner.lock().unwrap_or_else(|held| held.into_inner())
    }
}

impl Clock for ManualClock {
    fn monotonic_ns(&self) -> u64 {
        self.lock().monotonic_ns
    }

    fn unix_ns(&self) -> u64 {
        self.lock().unix_ns
    }
}
