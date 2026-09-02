//! The two guards, and the one distinction that decides what each one measures.
//!
//! The publisher crates design states it in one sentence: *"Upstream liveness is
//! a property of the input connection and alone justifies a restart; feed
//! silence is a property of one channel's published set, and a channel whose
//! instruments are dormant is silent and healthy. Conflating them lets one quiet
//! channel restart every other."*
//!
//! So there are two silences and neither guard here measures the first one.
//! Upstream silence is `[ingress] idle_timeout`, it is measured by
//! [`dz_ingress_core::Driver`] against time since the last *payload*, and its
//! answer is a reconnect rather than an exit. What is left for this crate is the
//! part that is genuinely a publisher defect, and the part that is a publisher
//! that can no longer be trusted to describe itself.

use std::time::Duration;

use dz_publisher_metrics::ExitReason;

/// Why the process is ending.
///
/// Three of the metrics crate's four [`ExitReason`] values. The fourth, `panic`,
/// is not a guard: nothing here decides it, and an exit recorded under it is one
/// a panic hook records on the way out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Exit {
    /// The upstream is delivering and nothing is reaching the wire. See
    /// [`IdleGuard`].
    IdleGuard,
    /// The publisher can no longer describe itself truthfully. See
    /// [`ConsistencyGuard`].
    ConsistencyGuard(Inconsistency),
    /// `SIGTERM` or `SIGINT`.
    Signal,
}

impl Exit {
    /// The `reason` label this exit is counted under.
    #[must_use]
    pub const fn reason(&self) -> ExitReason {
        match self {
            Self::IdleGuard => ExitReason::IdleGuard,
            Self::ConsistencyGuard(_) => ExitReason::ConsistencyGuard,
            Self::Signal => ExitReason::Signal,
        }
    }
}

impl std::fmt::Display for Exit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IdleGuard => f.write_str(
                "the idle guard: the upstream is delivering and nothing is reaching the wire",
            ),
            Self::ConsistencyGuard(what) => write!(f, "the consistency guard: {what}"),
            Self::Signal => f.write_str("a signal"),
        }
    }
}

/// Upstream activity: whether a payload the adapter recognised has arrived.
///
/// Named rather than a bare `bool` so that the two arguments to
/// [`IdleGuard::check`] cannot be transposed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Upstream {
    Delivering,
    Silent,
}

/// The idle guard: **upstream in, nothing out**.
///
/// # What it does not measure, and why that is the whole design
///
/// Not *no datagrams on this channel*. A publisher heartbeats a quiet channel
/// precisely so that a subscriber can tell quiet from dead, and a channel whose
/// instruments are dormant is silent and healthy — a guard that ended the
/// process over that would restart a publisher every time a venue went quiet
/// overnight, and would restart the busy feeds along with the quiet one.
///
/// Not *no upstream payloads* either. That silence is the transport's, it is
/// measured by the driver against `[ingress] idle_timeout`, and its answer is to
/// reconnect — which is a far cheaper and more specific action than ending the
/// process.
///
/// What is left is the conjunction, and the conjunction is unambiguous: the
/// upstream has delivered messages the adapter recognised inside the window, and
/// **not one of them produced a message that reached the wire**. That is not a
/// dormant venue and it is not a dead socket. It is a mapping that has stopped
/// producing, an instrument set that resolved to nothing, or a published set the
/// adapter is no longer holding handles for — and none of those recover without
/// a restart, which is why this one ends the process.
///
/// # Recording, and the metric that goes with it
///
/// [`published`](Self::published) also sets
/// `dz_publisher_idle_guard_last_update_timestamp_seconds`, which is the series
/// an operator writes the staleness rule against. Its own HELP text asks for
/// that rule to be guarded on `dz_publisher_uptime_seconds`, because the gauge
/// is pre-created at 0 and `time() - 0` reads as an age of decades.
#[derive(Debug, Clone)]
pub struct IdleGuard {
    window: Duration,
    /// Monotonic, never wall: a time daemon stepping the wall clock backwards
    /// would fire this on a healthy publisher.
    last_upstream_ns: Option<u64>,
    last_published_ns: Option<u64>,
    /// When the window started counting for a publisher that has published
    /// nothing yet. Startup is not silence: an adapter that has not received
    /// its first payload has nothing to have failed to publish.
    started_ns: Option<u64>,
}

impl IdleGuard {
    /// A guard over `[[feed]] idle_guard`.
    #[must_use]
    pub const fn new(window: Duration) -> Self {
        Self {
            window,
            last_upstream_ns: None,
            last_published_ns: None,
            started_ns: None,
        }
    }

    /// An upstream message the adapter recognised.
    ///
    /// This and not the driver's byte count, deliberately: bytes off a socket
    /// include keepalives and acknowledgements, and a payload the adapter did
    /// not recognise is not data it was ever going to publish something for. A
    /// recognised message is, which is what makes the conjunction below mean
    /// what it says.
    pub fn upstream(&mut self, now_ns: u64) {
        self.started_ns.get_or_insert(now_ns);
        self.last_upstream_ns = Some(now_ns);
    }

    /// A message reached the wire.
    pub fn published(&mut self, now_ns: u64) {
        self.started_ns.get_or_insert(now_ns);
        self.last_published_ns = Some(now_ns);
    }

    /// The Unix timestamp of the last publication, for the gauge. `None` until
    /// there has been one.
    #[must_use]
    pub const fn last_published_ns(&self) -> Option<u64> {
        self.last_published_ns
    }

    /// Whether the guard has fired, and what the upstream was doing.
    ///
    /// Returns [`Exit::IdleGuard`] only for the conjunction: the publish
    /// silence has reached the window **and** the upstream delivered inside it.
    /// Every other combination is a publisher this guard has nothing to say
    /// about.
    #[must_use]
    pub fn check(&self, now_ns: u64) -> Option<Exit> {
        let window_ns = u64::try_from(self.window.as_nanos()).unwrap_or(u64::MAX);
        // Before the first upstream message there is no window: a publisher
        // waiting on its first connect has published nothing and owes nothing.
        let since_published = now_ns.saturating_sub(self.last_published_ns.or(self.started_ns)?);
        if since_published < window_ns {
            return None;
        }
        let upstream = match self.last_upstream_ns {
            Some(last) if now_ns.saturating_sub(last) < window_ns => Upstream::Delivering,
            _ => Upstream::Silent,
        };
        match upstream {
            Upstream::Delivering => Some(Exit::IdleGuard),
            // The venue has gone quiet. Healthy, and the transport's own idle
            // timeout is what decides whether the *connection* is still there.
            Upstream::Silent => None,
        }
    }
}

/// What the consistency guard found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Inconsistency {
    /// A transmitter whose failure scope says the publisher is dark has been
    /// dropped from its fan-out.
    ///
    /// The fan-out absorbs a member's failure rather than propagating it,
    /// because above it sits the only code that advances `Sequence Number` and
    /// a caller acting on a member's refusal acts on behalf of every member
    /// that took the datagram. So the dropped essential member is *exposed*
    /// instead, for a guard to read between ticks rather than mid-datagram — a
    /// process that exits inside a fan-out leaves a partial delivery no
    /// subscriber can reason about.
    EgressDark { sink: String },

    /// The upstream transport is not usable as configured.
    ///
    /// `IngressError::Fatal` — an endpoint that is not one, a credential path
    /// that is not there, a scheme the transport does not speak — which the
    /// driver reports rather than retrying, because retrying cannot fix it.
    ///
    /// Counted as an inconsistency rather than given a reason of its own,
    /// and the argument is not a shrug: the closed `ExitReason` set has no
    /// fourth guard, and this *is* the publisher no longer able to describe
    /// itself truthfully. A process with no upstream at all that keeps
    /// heartbeating a channel and republishing a `Valid` manifest is publishing
    /// exactly one untrue thing, continuously.
    UpstreamUnusable { detail: String },

    /// The reference-data state directory has stopped being writable.
    ///
    /// The registry stops minting on its own and says so; whether the process
    /// should end is documented there as the runtime's decision, and this is
    /// the decision. It ends: a publisher that cannot persist an `Instrument
    /// ID` publishes definitions whose IDs resolve to nothing after the next
    /// restart, and the next restart is the one thing nobody schedules.
    StateUnpersistable { detail: String },
}

impl std::fmt::Display for Inconsistency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UpstreamUnusable { detail } => write!(
                f,
                "the upstream transport is not usable as configured: {detail}"
            ),
            Self::EgressDark { sink } => write!(
                f,
                "the transmitter `{sink}` is no longer being sent to, and its failure darkens \
                 this publisher"
            ),
            Self::StateUnpersistable { detail } => write!(
                f,
                "the reference-data state directory is no longer writable: {detail}"
            ),
        }
    }
}

/// The consistency guard: the publisher can no longer describe itself
/// truthfully.
///
/// Two conditions, both read between ticks, both of them a state the publisher
/// cannot recover from in place. It is a type rather than two `if`s at the call
/// site so that the first finding is the one reported: a dark transmitter and an
/// unwritable state directory arriving in the same tick are one incident, and
/// the second is usually the consequence.
#[derive(Debug, Clone, Default)]
pub struct ConsistencyGuard {
    found: Option<Inconsistency>,
}

impl ConsistencyGuard {
    #[must_use]
    pub const fn new() -> Self {
        Self { found: None }
    }

    /// Record a finding, keeping the first.
    pub fn found(&mut self, what: Inconsistency) {
        if self.found.is_none() {
            self.found = Some(what);
        }
    }

    /// The finding, if there is one.
    #[must_use]
    pub const fn finding(&self) -> Option<&Inconsistency> {
        self.found.as_ref()
    }

    /// Whether the guard has fired.
    #[must_use]
    pub fn check(&self) -> Option<Exit> {
        self.found.clone().map(Exit::ConsistencyGuard)
    }
}
