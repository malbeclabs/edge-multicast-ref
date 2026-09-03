//! The trait itself.

use crate::depth::DepthBound;
use crate::error::{AdapterError, ParseError};
use crate::instrument::InstrumentRef;
use crate::payload::{ConnectionId, DisconnectReason, Payload};
use crate::sink::{EventSink, ListingSink, SnapshotSink, UpstreamSink};
use crate::timestamp::VenueTimestampKind;

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

    /// Which of the venue's own clocks the `source_ts_ns` on this adapter's
    /// events was read from, or `None` where the venue publishes no timestamp
    /// of its own.
    ///
    /// One answer for the adapter rather than one per event, because
    /// [`Event`](crate::Event) carries one `source_ts_ns` field and a venue
    /// reads it out of the same place in every payload. The value is a metric
    /// label: it is the `timestamp_kind` of
    /// `dz_publisher_venue_to_recv_latency_seconds`, whose other half is the
    /// payload's own receive stamp — see
    /// [`EventSink::payload_scope`](crate::EventSink::payload_scope) for how
    /// that reaches a sink.
    ///
    /// Declared here rather than derived from the events, and read once at
    /// startup, for the reason [`message_types`](Self::message_types) is
    /// declared: a child series that appears the first time one is observed is
    /// a panel that is empty for two indistinguishable reasons.
    ///
    /// # Defaulted, and what a default costs
    ///
    /// `None` is a real answer and not a placeholder: a venue that publishes no
    /// timestamp of its own has nothing to declare, and an adapter for one
    /// would otherwise have to name a clock it never read. It is also what an
    /// adapter written against an earlier tag of this crate answers, which is
    /// why the method could be added at all.
    ///
    /// What it costs is stated rather than hidden. An adapter that does read a
    /// venue timestamp into `source_ts_ns` and leaves this defaulted publishes
    /// a latency the runtime cannot label, so
    /// `dz_publisher_venue_to_recv_latency_seconds` stays at zero across all
    /// four of its pre-created children — the shape of a feed that has stopped
    /// — and `dz_publisher_venue_timestamps_available` reads 0, which claims
    /// the venue exposes no clock at all. Neither is a failure the runtime can
    /// detect: `source_ts_ns` is a bare `u64` and a stamp read from the venue
    /// is indistinguishable from one an adapter filled in.
    fn source_timestamp_kind(&self) -> Option<VenueTimestampKind> {
        None
    }

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
    /// # Per-connection state must be keyed by `conn`
    ///
    /// **One adapter serves every source a publisher opens.** A publisher with
    /// several `[[source]]` blocks drives one connection per source and hands
    /// every payload to *this* object, which tells them apart by
    /// [`Payload::connection`](crate::Payload::connection) — and by the `conn`
    /// argument here and on [`on_disconnected`](Self::on_disconnected).
    ///
    /// So state that belongs to a connection has to be stored per `conn` and
    /// not per adapter. An adapter that keeps one upstream sequence cursor, or
    /// one authentication token, or one "have I subscribed yet" flag, is
    /// correct with one source and wrong the moment a second is configured —
    /// and the way it is wrong is silent: a comparison connection flaps, this
    /// method resets the state the *primary* was using, and the next primary
    /// payload is read against a cursor that belongs to nothing.
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
    ///
    /// # Invalidate `conn`'s state, and nothing else's
    ///
    /// One adapter serves every source, so this is called once per *connection*
    /// ending and not once per adapter. Clearing state unconditionally is
    /// correct only for a publisher with one source; with two it is the bug
    /// this paragraph exists to prevent, and it reaches the wire.
    ///
    /// Concretely: a comparison connection flaps, an adapter that clears "the"
    /// upstream sequence cursor here clears the primary's, and the primary's
    /// next payload is read as a discontinuity. An adapter that answers that
    /// with an `InstrumentReset` puts one, and a recovery snapshot, on the live
    /// wire — from a connection that publishes nothing. Migration to a second
    /// source is one configuration block and one line in the venue's `main`,
    /// with the adapter untouched, so this is a paragraph an adapter author has
    /// to have read *before* that day rather than after it.
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

    /// Write the book this adapter currently holds for one instrument, and say
    /// how deep that book goes.
    ///
    /// **Pulled rather than pushed.** The snapshot cadence, the rotation across
    /// instruments and the framing belong to the runtime, because they are what
    /// a subscriber's recovery depends on; the book belongs to the adapter,
    /// because it is the venue's microstructure. Neither can drive the other,
    /// so the runtime asks.
    ///
    /// # The `Depth Bound` is returned, and that is the whole reason it is
    ///
    /// [`DepthBound`] is the answer to *is this the complete book, or the top N
    /// of it?*, and it is the one field of a snapshot that only the layer
    /// holding the book can fill in. It is returned rather than passed in
    /// because a return value cannot be forgotten: an adapter cannot write a
    /// level without stating the depth those levels were drawn from, and the
    /// runtime cannot supply a default for it — the wire's `0` means *complete*,
    /// so the default a runtime would reach for is the strongest claim on the
    /// feed. See [`DepthBound`] for what that claim costs when it is wrong.
    ///
    /// A bounded book writes its levels outward from the top of each side and
    /// returns [`DepthBound::Levels`]; a book with everything in it returns
    /// [`DepthBound::Complete`], and does so with no levels at all when the
    /// venue has no resting interest — an empty book is a book, and refusing to
    /// snapshot one would hold a quiet instrument out of the rotation.
    ///
    /// # Errors
    ///
    /// [`AdapterError::NotReady`] when the book has not bootstrapped yet — the
    /// runtime skips this instrument's slot and comes back, which is the
    /// difference between one dormant instrument and a restart loop.
    /// [`AdapterError::UnknownInstrument`] when the handle is not one this
    /// adapter holds, which is a disagreement between two admitted sets and
    /// never something to retry.
    ///
    /// # Defaulted, and what the default now says
    ///
    /// The default refuses with [`AdapterError::Internal`], because there is no
    /// honest depth to report for a book that was never written. A top-of-book
    /// adapter is never asked — the runtime pulls snapshots only for a depth
    /// feed, which has the port for them — so the default costs it nothing. A
    /// *depth* adapter that leaves this defaulted is a defect, and this reports
    /// it as one; the alternative it replaced was a snapshot of no levels
    /// claiming to be a complete book.
    fn snapshot(
        &self,
        instrument: InstrumentRef,
        out: &mut dyn SnapshotSink,
    ) -> Result<DepthBound, AdapterError> {
        let _ = (instrument, out);
        Err(AdapterError::Internal {
            detail: "this adapter holds no book: `snapshot` is not implemented",
        })
    }
}
