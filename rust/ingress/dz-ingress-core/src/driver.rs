//! Where the two halves meet: an [`Input`] driving an
//! [`Adapter`](dz_adapter_core::Adapter).
//!
//! Everything in this module exists so that the boundary can be what it is. The
//! adapter is synchronous, allocation-free on its hot path, and a pure function
//! of its bytes and its own state — which is what lets a venue's mapping be
//! re-run offline over an archive and tested against a committed payload with
//! no network. None of that is free: something has to hold the socket, decide
//! when to reconnect, buffer what the adapter wants sent so that a synchronous
//! sink can be written to and an asynchronous socket written from, and notice
//! that nothing has arrived for a minute. This is that something, once, for
//! every venue.

use std::time::Duration;

use dz_adapter_core::{
    Adapter, ConnectionId, Desync, DisconnectReason, Event, EventSink, InstrumentRef, Payload,
    UpstreamSink,
};

use crate::backoff::Backoff;
use crate::clock::Clock;
use crate::config::Policy;
use crate::error::IngressError;
use crate::input::{Input, Received, UpstreamMessage};
use crate::limit::RateLimiter;
use crate::observer::IngressObserver;

/// What the adapter asked to be sent upstream, held until it can be.
///
/// # Why this exists at all
///
/// [`UpstreamSink`] is synchronous and sending is not. That mismatch is not an
/// oversight in the boundary; it is the boundary. An adapter that could await a
/// send would need a runtime, and a trait with an `async fn` on it pins every
/// venue to one runtime version and makes the offline replay impossible. So the
/// adapter writes what it wants sent into this, and the driver sends it
/// afterwards — the adapter says *what*, the driver owns *when*.
///
/// Allocating here is allowed for the same reason it is forbidden in
/// `on_payload`: this happens once per connection, and a subscription message
/// is a `String` the adapter built anyway.
#[derive(Debug, Default)]
pub struct UpstreamQueue {
    messages: Vec<Queued>,
}

/// One queued message, owned, because the adapter's borrow of it ended when
/// `on_connected` returned.
#[derive(Debug)]
enum Queued {
    Text(String),
    Binary(Vec<u8>),
}

impl UpstreamQueue {
    /// An empty queue.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            messages: Vec::new(),
        }
    }

    /// The queued messages, in the order the adapter wrote them.
    ///
    /// **The order is a guarantee, not an artefact.** An adapter that
    /// authenticates and then subscribes has written two messages whose order is
    /// the difference between a session and a rejection, and it has no other way
    /// to express that ordering.
    pub fn messages(&self) -> impl Iterator<Item = UpstreamMessage<'_>> + '_ {
        self.messages.iter().map(|queued| match queued {
            Queued::Text(text) => UpstreamMessage::Text(text),
            Queued::Binary(bytes) => UpstreamMessage::Binary(bytes),
        })
    }

    /// How many messages are queued.
    #[must_use]
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// Whether the adapter wrote nothing.
    ///
    /// Ordinary and not an error: a transport with no connection has nothing to
    /// say after one, and an adapter reading a local directory is a shape one
    /// publisher already runs. An adapter that *does* connect and writes
    /// nothing subscribes to nothing — which the idle guard catches, since no
    /// signature here could.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// Forget everything queued.
    pub fn clear(&mut self) {
        self.messages.clear();
    }
}

impl UpstreamSink for UpstreamQueue {
    fn send_text(&mut self, text: &str) {
        self.messages.push(Queued::Text(text.to_string()));
    }

    fn send_binary(&mut self, bytes: &[u8]) {
        self.messages.push(Queued::Binary(bytes.to_vec()));
    }
}

/// The event sink the adapter is handed for **one** payload, wrapping the
/// runtime's.
///
/// Two jobs, and both are things only the layer holding the payload can do.
///
/// `dz_publisher_ingress_messages_total` is recorded from here rather than by
/// the adapter, which is what the boundary's docstring promises: an adapter
/// gets the series by naming its message type, not by constructing a metric.
/// Counting it here also puts it in the one place that knows which connection
/// delivered it.
///
/// And the payload's own receive stamp is stated on the runtime's sink for
/// exactly as long as this exists — opened by [`open`](Self::open) before the
/// adapter is handed anything, closed by [`Drop`] when the mapping is over,
/// whether it returned, failed to parse, or unwound. That is the whole
/// mechanism behind `EventSink::payload_scope`, and it is what lets a runtime
/// measure `dz_publisher_venue_to_recv_latency_seconds` and
/// `dz_publisher_recv_to_send_latency_seconds` from an event it was handed. The
/// adapter passes nothing through and cannot forget to: it is not asked.
///
/// **An adapter cannot state a receive stamp of its own**, either. The scope
/// method is deliberately *not* forwarded, so a `payload_scope` call arriving
/// from the adapter — this is the sink it holds — reaches this wrapper and stops
/// here. Attribution is the driver's reading of its own clock, or the
/// transport's, and there is no path by which an adapter's guess can be
/// recorded as one.
struct PayloadSink<'a> {
    inner: &'a mut dyn EventSink,
    observer: &'a dyn IngressObserver,
    connection: &'static str,
}

impl<'a> PayloadSink<'a> {
    /// Open the scope for one payload and hand back the sink for it.
    ///
    /// The stamp is stated *before* the adapter can write anything, so the
    /// first event of the payload is as attributable as the last.
    fn open(
        inner: &'a mut dyn EventSink,
        observer: &'a dyn IngressObserver,
        connection: &'static str,
        recv_ts_ns: u64,
    ) -> Self {
        inner.payload_scope(Some(recv_ts_ns));
        Self {
            inner,
            observer,
            connection,
        }
    }
}

impl Drop for PayloadSink<'_> {
    /// Close the scope.
    ///
    /// In `Drop` rather than at the end of the driver's own block so that there
    /// is no path out of the mapping that leaves the stamp in force: an adapter
    /// that returned a parse error, or panicked and unwound through here, must
    /// not leave the next event the runtime sees — from a tick, or from the
    /// snapshot rotation — attributed to a payload that is over.
    fn drop(&mut self) {
        self.inner.payload_scope(None);
    }
}

impl EventSink for PayloadSink<'_> {
    fn upstream_message(&mut self, message_type: &'static str) {
        self.observer.message(message_type, self.connection);
        self.inner.upstream_message(message_type);
    }

    fn event(&mut self, event: Event<'_>) {
        self.inner.event(event);
    }

    /// Forwarded, because the runtime's recovery depends on it: the adapter is
    /// the only layer that can tell its own book has stopped being right, and a
    /// wrapper that swallowed the report would leave a subscriber applying
    /// deltas to a book the publisher already knows is diverged.
    fn desynchronised(&mut self, instrument: InstrumentRef, reason: Desync) {
        self.inner.desynchronised(instrument, reason);
    }

    /// **Not forwarded, and that is the mechanism.** The receive stamp is the
    /// driver's to state — see this type's own note. An adapter calling this
    /// reaches here and no further.
    fn payload_scope(&mut self, _recv_ts_ns: Option<u64>) {}
}

/// How one pass through connect, subscribe and receive ended.
enum Cycle {
    /// The connection did its job. The delay sequence starts over.
    Proven,
    /// Try again, further along the delay sequence.
    Retry,
    /// Retrying cannot help.
    Fatal(IngressError),
}

/// Why the receive loop stopped.
enum Stop {
    Reason(DisconnectReason),
    Fatal(IngressError),
}

/// Runs an [`Input`] against an [`Adapter`], forever.
///
/// # The two pairings it guarantees
///
/// **Every `on_connected` is followed by exactly one `on_disconnected`**, for
/// the same [`ConnectionId`], including when `on_connected` itself failed and
/// including on the way out through a fatal error. An adapter can therefore
/// reset **that connection's** state in `on_disconnected` rather than
/// defensively at the top of `on_connected` as well — and an adapter tracking
/// the upstream's own sequence numbering has to reset it somewhere, or the first
/// payload of the new connection is read as a gap.
///
/// **That reset is keyed by `conn`, and not unconditional.** One driver runs per
/// source and every one of them hands its events to *one* adapter object, so
/// this method is called once per connection ending and not once per adapter.
/// An adapter that clears "the" cursor here is correct with one source and wrong
/// with two, silently: a comparison connection flaps, the primary's cursor is
/// cleared, and the primary's next payload is read as a discontinuity — which an
/// adapter that answers discontinuities with a reset turns into an
/// `InstrumentReset` and a recovery snapshot on the live wire, from a connection
/// that publishes nothing. See
/// [`Adapter::on_disconnected`](dz_adapter_core::Adapter::on_disconnected),
/// which states the obligation.
///
/// **`on_connected` runs on every successful connect, reconnects included.**
/// This is the whole reason that method exists. A venue's subscriptions live on
/// its session, not on ours, so a reconnect that does not re-subscribe produces
/// a publisher with an open socket, a healthy-looking connection gauge, and no
/// data.
///
/// # What it decides, with the reason in each case
///
/// - **`connection_state` goes to 1 after the subscriptions are sent**, not
///   when the socket came up. A connection subscribed to nothing is not one an
///   alert should call healthy.
/// - **`reconnects_total` counts only an established, subscribed connection
///   ending.** A connect attempt that never succeeded has no reason in that
///   label set — see [`IngressError::Connect`], which carries a taxonomy of
///   its own that `connect_failures_total` counts by instead.
/// - **A parse error ends the payload and nothing else.** It is counted, the
///   payload is dropped, the connection stays up, and it is never retried. The
///   boundary's rustdoc states that contract; this is where it is kept. Retrying
///   a payload an adapter has already rejected can only produce the same
///   rejection, and dropping the connection over one bad message hands a venue
///   the ability to darken a feed with a typo.
/// - **The delay sequence resets only for a connection that delivered a
///   payload.** A venue that accepts a socket and closes it — the usual shape of
///   being throttled, or of an expired credential — would otherwise be
///   reconnected against at the initial delay indefinitely. And a connection
///   that ended in `rate_limit` never resets, however much it delivered: the
///   venue has just said we are going too fast, and starting the sequence over
///   is how that becomes a ban.
/// - **The idle guard measures time since the last payload**, not since the
///   last anything. See [`Received::Liveness`].
/// - **Every event the adapter emits is attributable to the payload that
///   produced it**, and nothing else is. The driver states the payload's
///   receive stamp on the runtime's sink through
///   [`EventSink::payload_scope`](dz_adapter_core::EventSink::payload_scope)
///   around the `on_payload` call and withdraws it afterwards, because it holds
///   the stamp and the adapter is not asked to carry one. See [`PayloadSink`].
pub struct Driver<'a> {
    input: &'a mut dyn Input,
    adapter: &'a mut dyn Adapter,
    clock: &'a dyn Clock,
    observer: &'a dyn IngressObserver,
    policy: Policy,
    backoff: Backoff,
    limiter: RateLimiter,
}

impl<'a> Driver<'a> {
    /// A driver for one connection and one adapter.
    ///
    /// The clock and the observer are shared references because a runtime
    /// driving several connections holds one of each: a publisher taking
    /// first-copy-wins from two upstreams has two drivers, two `Input`s, and one
    /// registry.
    pub fn new(
        input: &'a mut dyn Input,
        adapter: &'a mut dyn Adapter,
        clock: &'a dyn Clock,
        observer: &'a dyn IngressObserver,
        policy: Policy,
    ) -> Self {
        Self {
            input,
            adapter,
            clock,
            observer,
            policy,
            backoff: Backoff::new(policy.backoff),
            limiter: RateLimiter::new(policy.rate_limit_per_second),
        }
    }

    /// Connect, subscribe, receive, reconnect — until something retrying cannot
    /// fix.
    ///
    /// Returns only on [`IngressError::Fatal`]. There is deliberately no
    /// attempt limit: a publisher whose upstream is down should keep trying at
    /// the ceiling and leave the alerting to
    /// `dz_publisher_ingress_connection_state`, which is pre-created at 0 for
    /// exactly that case. A driver that gave up would turn a recoverable
    /// outage into an operator action.
    pub async fn run(&mut self, events: &mut dyn EventSink) -> IngressError {
        loop {
            match self.cycle(events).await {
                Cycle::Fatal(error) => return error,
                Cycle::Proven => {
                    self.backoff.reset();
                    self.wait().await;
                }
                Cycle::Retry => self.wait().await,
            }
        }
    }

    /// The delay before the next attempt, taken from the sequence.
    async fn wait(&mut self) {
        let delay = self.backoff.next_delay();
        self.clock.sleep(delay).await;
    }

    /// One connect, subscribe and receive.
    async fn cycle(&mut self, events: &mut dyn EventSink) -> Cycle {
        let connection = self.input.connection();

        match self.input.connect(self.policy.connect_timeout).await {
            Ok(()) => {}
            Err(error) if error.is_fatal() => {
                if let IngressError::Connect { reason, .. } = &error {
                    self.observer.connect_failure(*reason);
                }
                return Cycle::Fatal(error);
            }
            // Nothing was established, so nothing ended: no `on_disconnected`,
            // no reconnect counted, and the state gauge is already 0. What is
            // counted is the failure itself, by reason — a fatal one too, since
            // the last thing a publisher does before exiting is the number
            // somebody will want.
            Err(error) => {
                if let IngressError::Connect { reason, .. } = &error {
                    self.observer.connect_failure(*reason);
                }
                return Cycle::Retry;
            }
        }

        // A queue per connection, not one reused. What the adapter wants sent is
        // whatever it writes now; carrying over a message from a previous
        // connection would send a subscription the adapter has since decided
        // against.
        let mut queue = UpstreamQueue::new();
        if let Err(error) = self.adapter.on_connected(connection, &mut queue) {
            // Counted at the observer, which has no series for it. The
            // connection is torn down rather than left open subscribed to
            // nothing, and the attempt goes back through the delay sequence:
            // the usual cause is a credential the adapter could not read yet,
            // and hammering it does not make it readable.
            self.observer.adapter_error(error);
            self.end(connection, DisconnectReason::RemoteClose, false)
                .await;
            return Cycle::Retry;
        }
        if let Err(error) = self.flush(&queue).await {
            let reason = error
                .disconnect_reason()
                .unwrap_or(DisconnectReason::RemoteClose);
            let fatal = error.is_fatal();
            self.end(connection, reason, false).await;
            return if fatal {
                Cycle::Fatal(error)
            } else {
                Cycle::Retry
            };
        }
        self.observer.connection_state(connection.as_str(), true);

        let (stop, delivered) = self.pump(events, connection).await;
        let reason = match &stop {
            Stop::Reason(reason) => *reason,
            Stop::Fatal(error) => error
                .disconnect_reason()
                .unwrap_or(DisconnectReason::RemoteClose),
        };
        self.end(connection, reason, true).await;
        match stop {
            Stop::Fatal(error) => Cycle::Fatal(error),
            Stop::Reason(reason) => {
                if delivered && reason != DisconnectReason::RateLimit {
                    Cycle::Proven
                } else {
                    Cycle::Retry
                }
            }
        }
    }

    /// Send what the adapter queued, paced.
    async fn flush(&mut self, queue: &UpstreamQueue) -> Result<(), IngressError> {
        for message in queue.messages() {
            let wait = self.limiter.charge(self.clock.steady_ns());
            if !wait.is_zero() {
                self.clock.sleep(wait).await;
            }
            self.input.send(message).await?;
        }
        Ok(())
    }

    /// Receive until the connection ends.
    ///
    /// Returns why, and whether this connection ever delivered a payload —
    /// which is the evidence the delay sequence resets on.
    async fn pump(&mut self, events: &mut dyn EventSink, connection: ConnectionId) -> (Stop, bool) {
        let mut delivered = false;
        let mut last_payload_ns = self.clock.steady_ns();
        loop {
            // Recomputed every time round, because the budget is what is left
            // of the guard and not the whole of it: a connection answering a
            // keepalive every ten seconds must still be cut off at the guard,
            // not ten seconds after each one.
            let budget = self.policy.idle_timeout.map(|limit| {
                let waited =
                    Duration::from_nanos(self.clock.steady_ns().saturating_sub(last_payload_ns));
                limit.saturating_sub(waited)
            });
            match self.input.recv(budget).await {
                Ok(Received::Payload { bytes, ts_ns }) => {
                    let payload = Payload {
                        bytes,
                        // The transport's own timestamp when it has a better
                        // one than this - a kernel stamp predates every
                        // scheduling delay between the packet and this line.
                        recv_ts_ns: ts_ns.unwrap_or_else(|| self.clock.wall_ns()),
                        connection,
                    };
                    self.observer.bytes(bytes.len() as u64);
                    // The scope opens here and closes when `sink` is dropped at
                    // the end of this arm, so every event the adapter emits -
                    // and only those - is attributable to this payload.
                    {
                        let mut sink = PayloadSink::open(
                            &mut *events,
                            self.observer,
                            connection.as_str(),
                            payload.recv_ts_ns,
                        );
                        if let Err(error) = self.adapter.on_payload(&payload, &mut sink) {
                            // Counted, and that is all. Not retried: the same
                            // bytes through the same adapter produce the same
                            // error. Not fatal to the connection: one unreadable
                            // message must not darken a feed.
                            self.observer.parse_error(error);
                        }
                    }
                    delivered = true;
                    last_payload_ns = self.clock.steady_ns();
                }
                // The socket is alive and the subscription may not be. Nothing
                // to record and, deliberately, nothing to reset.
                Ok(Received::Liveness) => {}
                Ok(Received::Idle) => return (Stop::Reason(DisconnectReason::Timeout), delivered),
                Err(error) if error.is_fatal() => return (Stop::Fatal(error), delivered),
                Err(error) => {
                    let reason = error
                        .disconnect_reason()
                        .unwrap_or(DisconnectReason::RemoteClose);
                    return (Stop::Reason(reason), delivered);
                }
            }
        }
    }

    /// Tell the adapter the connection is gone, record it, and release it.
    ///
    /// `counted` is whether this connection reached the state
    /// `dz_publisher_ingress_reconnects_total` counts the ending of. The
    /// rate-limit series is recorded either way: a venue that rate-limits our
    /// subscription has rate-limited us, whether or not we got as far as
    /// receiving anything.
    async fn end(&mut self, connection: ConnectionId, reason: DisconnectReason, counted: bool) {
        self.adapter.on_disconnected(connection, reason);
        self.observer.connection_state(connection.as_str(), false);
        if reason == DisconnectReason::RateLimit {
            self.observer.rate_limited();
        }
        if counted {
            self.observer.reconnect(reason);
        }
        self.input.shutdown().await;
    }
}
