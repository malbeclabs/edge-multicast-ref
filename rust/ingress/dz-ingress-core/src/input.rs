//! The transport trait: everything that connects, waits, and can fail on a
//! socket.

use std::time::Duration;

use dz_adapter_core::ConnectionId;

use crate::clock::BoxFuture;
use crate::error::IngressError;

/// What one call to [`Input::recv`] produced.
///
/// Three cases, and the second is the one that exists for a specific silent
/// failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Received<'a> {
    /// Bytes for the adapter, borrowed from the transport's own receive buffer.
    ///
    /// `ts_ns` is the transport's timestamp for the arrival, when it has one
    /// better than the driver's — a kernel receive timestamp is the case that
    /// matters, since it predates every scheduling delay between the packet
    /// and the read. `None` means the driver stamps it from
    /// [`Clock::wall_ns`](crate::Clock::wall_ns) at the moment it takes the
    /// payload, which is the closest reading available.
    Payload { bytes: &'a [u8], ts_ns: Option<u64> },

    /// Traffic that proves the connection is alive and carries nothing for the
    /// adapter: a keepalive, a protocol acknowledgement, an answered ping.
    ///
    /// **This is not a payload and the driver must not treat it as one.** A
    /// websocket whose subscription the venue quietly dropped still answers
    /// pings forever, so an idle guard that any traffic satisfied would never
    /// fire on the one failure it exists for. The driver's budget therefore
    /// counts time since the last *payload*, and this case only tells it the
    /// socket has not gone away.
    Liveness,

    /// The budget given to [`recv`](Input::recv) elapsed with nothing received.
    ///
    /// Not an error: the transport did what it was asked. What it means is the
    /// driver's decision, not the transport's.
    Idle,
}

/// One message to send upstream, as the adapter wrote it.
///
/// Borrowed, because it is read straight out of the queue the driver buffered
/// it into and written straight to the socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamMessage<'a> {
    /// A text message, for a transport that distinguishes one.
    Text(&'a str),
    /// A binary message.
    Binary(&'a [u8]),
}

/// Which clock gave a payload its receive timestamp.
///
/// # Not the metrics crate's `timestamp_kind`, and the distinction matters
///
/// That label — `exchange_recv`, `matching_engine`, `gateway_send`,
/// `block_time` — says which of the *venue's* timestamps a venue-to-receive
/// latency observation was measured against. It is a property of the field an
/// adapter read out of a payload, not of how we stamped the arrival, and this
/// enum is deliberately not spelled like it: two taxonomies sharing a name is
/// how one gets recorded under the other's label.
///
/// # Where this one goes, which is not a metric
///
/// The boundary's [`Payload`](dz_adapter_core::Payload) does not carry it per
/// payload, on the grounds that a transport stamps every payload the same way,
/// so the kind is a property of the connection and belongs where the ingress
/// metrics are recorded once. That reasoning is right and the destination does
/// not exist: the `dz_publisher_ingress_*` family is closed and has neither a
/// series nor a label for it. So this is an accessor for a startup log line and
/// an archive header — the two places that can carry it — and nothing here
/// records it. Naming it at least stops a third spelling appearing when one of
/// those needs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StampSource {
    /// The kernel stamped the packet on arrival, before any scheduling delay
    /// between the packet and the read.
    Kernel,
    /// The transport read the clock once the read returned.
    Read,
}

/// A source of upstream payloads: the connection, the reconnect, the send.
///
/// Object-safe, because `[ingress] kind` picks one of a closed set at startup
/// and the picked one is a `Box<dyn Input>`. Every awaiting method therefore
/// returns a [`BoxFuture`] rather than being an `async fn`, which is the same
/// choice made explicit — see [`BoxFuture`] for why not the macro that hides
/// it.
///
/// # What an implementation owns, and what it must not decide
///
/// It owns the socket, the protocol handshake, TLS, its own keepalives, and the
/// classification of a failure into an [`IngressError`]. That last one is the
/// load-bearing part: the reason a connection ended is a metric label with four
/// values, and the transport is the only layer that can see a close code, a
/// handshake status or a read that timed out.
///
/// It does not own *when* to connect, how long to wait before trying again,
/// what to do about a payload, or whether silence means anything. Those are
/// [`Driver`](crate::Driver)'s, so that they are one implementation for every
/// transport and every venue rather than one per publisher — which is what the
/// two existing publishers have, each with its own reconnection, backoff and
/// rate limiting.
///
/// # The timeouts are arguments, not policy
///
/// [`connect`](Self::connect) and [`recv`](Self::recv) are handed their
/// deadline instead of the driver racing them against a timer it holds. Two
/// reasons, and the second is not stylistic. The policy stays in one place, so
/// a transport cannot quietly hold a different one. And a receive that is
/// abandoned mid-message by dropping its future may leave a partially-read
/// message in the transport's buffer — a hazard whose symptom is one corrupt
/// payload after a busy period, which is close to undiagnosable. Handing the
/// budget in leaves cancellation to the layer that knows what it is cancelling.
pub trait Input: Send {
    /// The name of this connection, as every metric label carries it.
    ///
    /// A `&'static str` inside, declared at startup, because
    /// `dz_publisher_ingress_connection_state` is pre-created at 0 for each
    /// declared name — that is what lets an `== 0` alert fire for a publisher
    /// whose upstream never came up at all.
    fn connection(&self) -> ConnectionId;

    /// Which clock stamps an arrival on this transport. See [`StampSource`].
    fn stamp_source(&self) -> StampSource {
        StampSource::Read
    }

    /// Establish the connection, giving up after `timeout`.
    ///
    /// Called again for every reconnect, on the same object: an implementation
    /// must be able to connect after it has been [`shutdown`](Self::shutdown),
    /// and must not carry per-connection state across one.
    ///
    /// # Errors
    ///
    /// [`IngressError::Connect`] for a refusal, a timeout, a failed
    /// negotiation — anything the driver should retry.
    /// [`IngressError::Fatal`] for a configuration this transport cannot use at
    /// all, which stops the driver instead of retrying it forever.
    fn connect(&mut self, timeout: Duration) -> BoxFuture<'_, Result<(), IngressError>>;

    /// Send one message the adapter wrote.
    ///
    /// # Errors
    ///
    /// [`IngressError::Ended`] when the connection could not carry it. The
    /// adapter is not told about a send failure and has nothing to do about
    /// one: it owns no transport, and the driver's answer is to reconnect and
    /// ask it to write its subscriptions again.
    fn send<'a>(
        &'a mut self,
        message: UpstreamMessage<'a>,
    ) -> BoxFuture<'a, Result<(), IngressError>>;

    /// Wait for the next thing from upstream, for at most `budget`.
    ///
    /// `None` waits indefinitely, which is what a connection with no idle guard
    /// configured gets. A `budget` that elapses is [`Received::Idle`] and not an
    /// error.
    ///
    /// The returned [`Received`] borrows the transport's receive buffer, so the
    /// driver cannot ask for the next one until it has finished with this one.
    /// That is the borrow checker enforcing the thing that makes the boundary's
    /// borrowed payloads sound.
    ///
    /// # Errors
    ///
    /// [`IngressError::Ended`] carrying the reason, when the connection is
    /// gone.
    fn recv<'a>(
        &'a mut self,
        budget: Option<Duration>,
    ) -> BoxFuture<'a, Result<Received<'a>, IngressError>>;

    /// Release the connection.
    ///
    /// Infallible: this is called on a path that has already decided to
    /// reconnect, and a close that fails changes nothing about that. An
    /// implementation should attempt an orderly close and give up quickly — a
    /// transport that waits for a peer which has already gone turns every
    /// reconnect into a stall.
    fn shutdown(&mut self) -> BoxFuture<'_, ()>;
}
