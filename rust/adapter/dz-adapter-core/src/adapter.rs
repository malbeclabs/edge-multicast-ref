//! The trait itself.

use crate::error::{AdapterError, ParseError};
use crate::instrument::InstrumentRef;
use crate::payload::{ConnectionId, DisconnectReason, Payload};
use crate::sink::{EventSink, ListingSink, SnapshotSink, UpstreamSink};

/// What a venue implements.
///
/// Object-safe, so a binary linking several adapters can hold one behind a
/// `Box<dyn Adapter>` and pick it from configuration. `Send`, because the
/// runtime drives it from a thread that is not the one that built it; not
/// `Sync`, because nothing calls into one adapter from two places at once and
/// requiring it would force interior mutability on every implementation for a
/// concurrency nobody needs.
///
/// # What an implementation must not do
///
/// Every method below is called from the publisher's own loop. None of them may
/// block, sleep, take a lock held across a call, or perform I/O. The transport
/// is elsewhere and owns everything that waits.
///
/// [`on_payload`](Self::on_payload) additionally may not allocate, and may not
/// depend on anything but its argument and the adapter's own state. That is
/// what makes it re-runnable offline over an archive of what the upstream
/// actually sent, which is the mechanism by which *did the publisher publish
/// what the venue said?* becomes a diff rather than an argument.
pub trait Adapter: Send {
    /// The upstream message types this adapter counts individually.
    ///
    /// Declared up front so every series exists from startup rather than
    /// appearing the first time one arrives — a panel with no data because a
    /// message type has not been seen yet is indistinguishable from one that
    /// stopped. Anything passed to
    /// [`EventSink::upstream_message`](crate::EventSink::upstream_message) that
    /// is not named here is counted under `other`.
    ///
    /// Keep this to the upstream's own vocabulary and to a bounded set. A
    /// message type named after the subscription that carried it is one series
    /// per instrument, which is the cardinality this label is guarded against.
    fn message_types(&self) -> &[&'static str];

    /// Offer the instruments this adapter wants published, and withdraw the
    /// ones that have ended.
    ///
    /// Drained by the runtime on its own cadence. **Required rather than
    /// defaulted**, and for the reason the codec requires every message type to
    /// implement its channel stamp: an adapter that publishes nothing is a
    /// thing that exists — a venue whose universe another component discovers
    /// is a shape one publisher already runs — but it must say so with an empty
    /// body rather than inherit silence from a default nobody read.
    ///
    /// Re-offering an instrument already admitted is free and returns the same
    /// handle, so an implementation may simply offer its current set each time.
    fn poll_listings(&mut self, out: &mut dyn ListingSink);

    /// Write whatever must be sent upstream after a connection is established.
    ///
    /// Called on every successful connect, reconnects included, which is what
    /// makes a subscription that was silently lost come back. Authentication
    /// and subscription frames go here.
    ///
    /// Defaulted, because a transport with no connection has nothing to say
    /// here: an adapter reading a local directory or a file is a shape one
    /// publisher already runs, and forcing an empty body on it would be
    /// ceremony. An adapter that *does* connect and leaves this defaulted
    /// subscribes to nothing and receives nothing; the runtime's idle guard is
    /// what catches that, rather than this signature.
    ///
    /// # Errors
    ///
    /// [`AdapterError`] when the adapter cannot compose what it needs to send.
    /// The transport counts it and retries under its own backoff.
    fn on_connected(
        &mut self,
        conn: ConnectionId,
        out: &mut dyn UpstreamSink,
    ) -> Result<(), AdapterError> {
        let _ = (conn, out);
        Ok(())
    }

    /// Told that a connection has ended, and why.
    ///
    /// For an adapter that must invalidate per-connection state — a sequence it
    /// was tracking against the upstream's own numbering, a book it can no
    /// longer trust to be current. Defaulted for the same reason as
    /// [`on_connected`](Self::on_connected).
    fn on_disconnected(&mut self, conn: ConnectionId, reason: DisconnectReason) {
        let _ = (conn, reason);
    }

    /// One upstream payload in; zero or more market events out.
    ///
    /// The whole of the venue's mapping, and the only method that has to be
    /// fast. Synchronous, allocation-free, and a pure function of the payload
    /// and the adapter's own state — see the trait's own note on why that last
    /// property is worth the constraint.
    ///
    /// Emitting nothing is ordinary: a heartbeat, an acknowledgement, or an
    /// update for an instrument this adapter holds no handle for are all
    /// `Ok(())` with no event. Only a payload the adapter cannot read is an
    /// error.
    ///
    /// # Errors
    ///
    /// [`ParseError`], whose variant is the reason the failure is counted
    /// under. One error ends this payload; it does not end the connection, and
    /// the transport does not retry it.
    fn on_payload(
        &mut self,
        payload: &Payload<'_>,
        out: &mut dyn EventSink,
    ) -> Result<(), ParseError>;

    /// Write the book this adapter currently holds for one instrument.
    ///
    /// **Pulled rather than pushed.** The snapshot cadence, the rotation across
    /// instruments and the framing belong to the runtime, because they are what
    /// a subscriber's recovery depends on; the book belongs to the adapter,
    /// because it is the venue's microstructure. Neither can drive the other,
    /// so the runtime asks.
    ///
    /// Defaulted to writing nothing, which is correct for a top-of-book adapter:
    /// that feed has no snapshot port and nothing to write.
    ///
    /// # Errors
    ///
    /// [`AdapterError::NotReady`] when the book has not bootstrapped yet — the
    /// runtime skips this instrument's slot and comes back, which is the
    /// difference between one dormant instrument and a restart loop.
    /// [`AdapterError::UnknownInstrument`] when the handle is not one this
    /// adapter holds, which is a disagreement between two admitted sets and
    /// never something to retry.
    fn snapshot(
        &self,
        instrument: InstrumentRef,
        out: &mut dyn SnapshotSink,
    ) -> Result<(), AdapterError> {
        let _ = (instrument, out);
        Ok(())
    }
}
