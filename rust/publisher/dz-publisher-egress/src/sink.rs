//! The sink boundary, and the fan-out built on it.

use std::sync::Arc;

use dz_edge_core::PortRole;
use dz_publisher_metrics::PublisherMetrics;

use crate::error::SinkError;

/// What a failure of one transmitter costs.
///
/// The distinction is the design's, and it is not cosmetic: the mktdata socket
/// going away means this publisher is not publishing, which is a reason to end
/// the process and let the supervisor restart it somewhere the route works. A
/// second sink carrying a copy of the same datagrams going away costs a
/// consumer of that copy and nothing else, and ending the process over it turns
/// an auxiliary outage into a feed outage.
///
/// Stated per transmitter rather than inferred from its port role, because the
/// same port role can be both: the socket is essential, and a tee'd copy of the
/// same bytes on the same role is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureScope {
    /// A failure here darkens the publisher. The runtime's guard ends the
    /// process on it.
    Process,
    /// A failure here darkens only what this transmitter feeds.
    Channel,
}

/// Somewhere a composed datagram is sent.
///
/// # Why this is a trait
///
/// Three reasons, and the third is the one that decides it.
///
/// The first is testability, which the design names: everything above this
/// boundary — composing a datagram, numbering it, refusing a message the port
/// role does not carry, counting a failure under the right reason — is
/// exercised with no socket, no privileges and no network. A crate whose only
/// send path is a `UdpSocket` is a crate whose sequencing is tested by reading
/// a capture.
///
/// The second is that the socket is not the only destination a datagram
/// legitimately has. The conformance and recorder tiers answer *did the
/// publisher publish what the venue said?* by diffing what was composed here
/// against a capture, and a sink is where a composed datagram is handed to
/// something other than the wire without the composer knowing.
///
/// The third is that the set of destinations is not fixed. A reference stream —
/// a second copy of every datagram, carried to a consumer that is not a
/// multicast subscriber — hangs off exactly this seam, and [`Tee`] implements
/// this trait so that fanning out is invisible above it. Had the composer taken
/// a socket, adding the second destination would have meant changing the
/// composer, which is the code that owns `Sequence Number`: the one place in
/// this crate where a change is paid for by every subscriber at once.
///
/// # Contract
///
/// An implementation must not block the caller. The datagram is one already
/// composed and numbered, so an implementation that queues it has taken
/// responsibility for a number that is already spent.
pub trait DatagramSink {
    /// A stable name, for a log line and for [`Tee::dropped`]. Never a metric
    /// label: the normative families carry `port_role` and `channel_id`, and a
    /// per-sink label is not in the closed set.
    fn name(&self) -> &str;

    /// Send one complete datagram.
    ///
    /// # Errors
    ///
    /// [`SinkError`], whose [`reason`](SinkError::reason) is the label value
    /// the failure is counted under and whose
    /// [`is_transient`](SinkError::is_transient) decides whether this sink is
    /// worth trying again.
    fn send(&mut self, datagram: &[u8]) -> Result<(), SinkError>;

    /// What a failure of this sink costs. No default: a sink whose scope
    /// nobody stated would be given one by whichever answer this crate
    /// happened to pick, and both answers are wrong for some sink.
    fn failure_scope(&self) -> FailureScope;
}

/// Every datagram to several sinks.
///
/// One tee serves one port role, because that is the granularity the metric
/// families are labelled at and because its members all carry the same
/// numbered series.
///
/// # A member's failure never ends a send
///
/// This is not a convenience, and it is the reason the fan-out is a type rather
/// than a loop at the call site. Above this boundary sits the only code that
/// advances `Sequence Number`. If one member's refusal propagated as the
/// outcome of the send, then whatever a caller does about it — abort the tick,
/// retry the datagram, end the process — is a decision taken on behalf of every
/// other member, all of which took the datagram. A retry re-sends a number the
/// live members already have, which a conformant subscriber reads as a
/// duplicate and discards; that discards the *retry*, so a fresh datagram
/// carrying that number never arrives. An abort leaves the number spent with
/// nothing sent under it, which is a gap. Either way one auxiliary consumer's
/// broken pipe has become a defect in the mktdata series every subscriber
/// tracks.
///
/// So a member that fails is counted here, under its own reason, and — unless
/// the failure is transient — dropped from the fan-out. The send's outcome is
/// `Ok`. The two things a caller does need to know are exposed rather than
/// returned: [`Self::dropped`] names what is no longer being fed, and
/// [`Self::process_failure`] names a dropped member whose
/// [`FailureScope`] says the publisher is now dark, for the runtime's guard to
/// act on between ticks rather than mid-datagram.
///
/// The one outcome that *is* returned is a tee with nothing live left, which is
/// not a member's failure but the absence of any destination at all.
pub struct Tee {
    port_role: PortRole,
    metrics: Arc<PublisherMetrics>,
    members: Vec<Member>,
    process_failure: Option<String>,
}

struct Member {
    sink: Box<dyn DatagramSink>,
    live: bool,
    failures: u64,
}

impl Tee {
    /// An empty fan-out for one port role. `port_role` is the label every
    /// failure this tee absorbs is counted under, so it must be the role its
    /// members actually serve.
    #[must_use]
    pub fn new(port_role: PortRole, metrics: Arc<PublisherMetrics>) -> Self {
        Self {
            port_role,
            metrics,
            members: Vec::new(),
            process_failure: None,
        }
    }

    /// Add a destination. Order is the order datagrams are offered in, which
    /// matters only in that a member added first sees a datagram a few
    /// microseconds before the rest.
    pub fn add(&mut self, sink: Box<dyn DatagramSink>) {
        self.members.push(Member {
            sink,
            live: true,
            failures: 0,
        });
    }

    /// How many members are still being offered datagrams.
    ///
    /// A publisher should alert on this falling below what it configured. A
    /// dropped member is silent by design — that is the whole point of the
    /// paragraph on [`Tee`] — and this is where the silence is visible.
    #[must_use]
    pub fn live(&self) -> usize {
        self.members.iter().filter(|m| m.live).count()
    }

    /// The names of the members that have been dropped.
    pub fn dropped(&self) -> impl Iterator<Item = &str> {
        self.members
            .iter()
            .filter(|m| !m.live)
            .map(|m| m.sink.name())
    }

    /// Failures absorbed, live members and dropped ones together. Counted in
    /// the metric too; this is for a shutdown log line, and for a test.
    #[must_use]
    pub fn absorbed_failures(&self) -> u64 {
        self.members.iter().map(|m| m.failures).sum()
    }

    /// The name of a dropped member whose failure scope says this publisher is
    /// now dark, if there is one.
    ///
    /// Read between ticks by the runtime's guard. Not returned from
    /// [`DatagramSink::send`] and not acted on here: ending the process from
    /// inside a send abandons the datagrams the other members already took, and
    /// a process that exits mid-fan-out leaves a partial delivery no subscriber
    /// can reason about.
    #[must_use]
    pub fn process_failure(&self) -> Option<&str> {
        self.process_failure.as_deref()
    }
}

impl DatagramSink for Tee {
    fn name(&self) -> &str {
        self.port_role.as_str()
    }

    /// Offer the datagram to every live member.
    ///
    /// # Errors
    ///
    /// [`SinkError::NotRegistered`], and only that, when no member is live. A
    /// member's own failure is counted and absorbed; see [`Tee`].
    fn send(&mut self, datagram: &[u8]) -> Result<(), SinkError> {
        let mut delivered = 0usize;
        for member in &mut self.members {
            if !member.live {
                continue;
            }
            match member.sink.send(datagram) {
                Ok(()) => delivered += 1,
                Err(error) => {
                    member.failures += 1;
                    self.metrics
                        .egress()
                        .error(self.port_role, error.reason());
                    if !error.is_transient() {
                        member.live = false;
                        if member.sink.failure_scope() == FailureScope::Process
                            && self.process_failure.is_none()
                        {
                            self.process_failure = Some(member.sink.name().to_owned());
                        }
                    }
                }
            }
        }
        if delivered == 0 && self.live() == 0 {
            return Err(SinkError::NotRegistered);
        }
        Ok(())
    }

    /// The widest scope any member declares.
    ///
    /// A tee holding one essential transmitter is essential: the copy going to
    /// the auxiliary consumer does not make the mktdata socket optional.
    fn failure_scope(&self) -> FailureScope {
        if self
            .members
            .iter()
            .any(|m| m.sink.failure_scope() == FailureScope::Process)
        {
            FailureScope::Process
        } else {
            FailureScope::Channel
        }
    }
}
