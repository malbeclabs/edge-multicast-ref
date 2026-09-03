//! The clock the driver reads and waits on, and why it is a parameter.
//!
//! Everything this crate does that is hard to get right is a *duration*: the
//! delay before the next connect attempt, the spacing between two upstream
//! sends, the budget a receive is given before silence counts as a lost
//! subscription. A test that exercises those by sleeping takes as long as the
//! policy it is testing and still proves nothing about it — a 30-second cap
//! costs the suite 30 seconds and is asserted by nobody. With the clock passed
//! in, the delay is a value a test reads back.
//!
//! The production implementation is one screenful and lives behind the `tokio`
//! feature, off by default, so that the default build of this crate names no
//! runtime at all.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// A future in a box, which is how every awaiting method in this crate is
/// declared.
///
/// The alternative is an `async fn` in the trait. Those are stable, but they
/// are not dyn-compatible, and [`Input`](crate::Input) has to be: the
/// `[ingress] kind` a configuration names is resolved to one transport out of a
/// closed set, which is a `Box<dyn Input>`. Boxing here rather than reaching
/// for a proc-macro to hide it keeps the allocation visible and keeps this
/// crate's dependency tree — the one every venue linking it inherits — down to
/// the boundary crate, an error derive and a deserializer.
///
/// The cost is one allocation per await point. Every one of them is per
/// connection or per upstream send; there is none on the payload path, which
/// does not await at all.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Time, as the driver needs it: two readings and a wait.
///
/// # Why two readings and not one
///
/// They are not interchangeable and using either for the other's job is a
/// defect that only appears in production.
///
/// A payload's receive timestamp is compared against the venue's own timestamp
/// inside the payload, so it has to be a wall clock — a monotonic reading has
/// an arbitrary origin and the subtraction is meaningless. The idle guard and
/// the backoff are the opposite case: they measure an interval on this host, and
/// a wall clock that a time daemon steps backwards would either fire the guard
/// on a healthy connection or suppress it on a dead one. So the driver stamps
/// with [`wall_ns`](Self::wall_ns) and measures with
/// [`steady_ns`](Self::steady_ns), and neither can be substituted for the other
/// by accident.
///
/// `Send + Sync` because the driver holds it by shared reference and the
/// runtime holds one clock for however many connections it drives.
pub trait Clock: Send + Sync {
    /// Nanoseconds since 1970-01-01 UTC. Stamps a payload's arrival.
    fn wall_ns(&self) -> u64;

    /// Nanoseconds since an arbitrary origin, never decreasing. Measures the
    /// idle budget and nothing else measures it.
    fn steady_ns(&self) -> u64;

    /// Wait. A zero duration must still yield, or a pathological policy becomes
    /// a loop that never lets its runtime do anything else.
    fn sleep(&self, duration: Duration) -> BoxFuture<'_, ()>;
}

/// The clock a running publisher uses.
///
/// Behind a feature because it is the only thing in this crate that names a
/// runtime, and a venue that brought its own must not inherit ours to get the
/// [`Clock`] trait. A transport crate in this family turns the feature on,
/// which is the right place for it: the transport already has the runtime.
#[cfg(feature = "tokio")]
#[derive(Debug)]
pub struct TokioClock {
    /// The origin of [`Clock::steady_ns`]. Taken once at construction, because
    /// `Instant` has no public origin and the difference is all the driver
    /// needs.
    origin: std::time::Instant,
}

#[cfg(feature = "tokio")]
impl TokioClock {
    /// A clock reading this host's time.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: std::time::Instant::now(),
        }
    }
}

#[cfg(feature = "tokio")]
impl Default for TokioClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "tokio")]
impl Clock for TokioClock {
    fn wall_ns(&self) -> u64 {
        // A clock set before 1970 is not a case with a sensible reading, and
        // `0` at least sorts before every real timestamp rather than wrapping
        // to the far future, which is what a subtraction would do.
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| {
                u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
            })
    }

    fn steady_ns(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    fn sleep(&self, duration: Duration) -> BoxFuture<'_, ()> {
        Box::pin(tokio::time::sleep(duration))
    }
}
