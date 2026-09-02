//! Where an adapter writes what it produces.
//!
//! Every one of these is passed in as `&mut dyn` rather than returned as a
//! collection, and that shape carries three things at once. Nothing allocates on
//! the highest-frequency path in the process. The [`Adapter`](crate::Adapter)
//! trait stays object-safe, so a binary can hold one behind a `Box` and choose
//! it from configuration. And a sink can grow a method without breaking a venue
//! that does not call it, which a return type could not do — the crates are
//! consumed as tagged releases, and a boundary whose every extension is a
//! breaking change strands its consumers on old tags.

use crate::event::{Desync, Event, Side};
use crate::instrument::{InstrumentRef, InstrumentSpec};
use crate::scalar::Scalar;

/// Where market events go.
pub trait EventSink {
    /// Name the upstream message type this adapter has just recognised, before
    /// emitting the events it produced.
    ///
    /// This is what `dz_publisher_ingress_messages_total{message_type}` counts,
    /// and calling it is how an adapter gets that series for free rather than
    /// constructing a metric. Called once per upstream message, so a payload
    /// carrying a batch of them calls it once per member.
    ///
    /// The value must be one the adapter declared in
    /// [`Adapter::message_types`](crate::Adapter::message_types); anything else
    /// is counted under `other`. That bucket is not a failure — it is the guard
    /// on a label whose values belong to the upstream's vocabulary, where many
    /// APIs name a message after the subscription that carried it, which is one
    /// series per instrument.
    fn upstream_message(&mut self, message_type: &'static str);

    /// Emit one market event.
    ///
    /// Taking `Event` by value and not by reference is deliberate: it borrows
    /// from the payload, so it is two machine words and a handful of fields,
    /// and a reference to it would be an indirection to something already on
    /// the stack.
    fn event(&mut self, event: Event<'_>);

    /// The receive stamp of the payload whose events follow, and its end.
    ///
    /// `Some(recv_ts_ns)` is
    /// [`Payload::recv_ts_ns`](crate::Payload::recv_ts_ns) for the payload
    /// about to be mapped; `None` says that mapping has finished and no
    /// payload is in force. An event reported between the two is attributable
    /// to that payload; an event reported outside them — a runtime's own tick,
    /// a replay, a sink written to by something that is not an adapter — is
    /// attributable to nothing, and a sink that holds this in an `Option` gets
    /// that distinction for free rather than carrying a stale reading.
    ///
    /// # Why this is not a parameter on [`event`](Self::event)
    ///
    /// It is the other half of two latency families and an adapter has no part
    /// in either. `dz_publisher_venue_to_recv_latency_seconds` is
    /// `recv_ts_ns` minus the venue's own timestamp — which arrives as
    /// `Event::source_ts_ns`, so a sink needs both at once — and
    /// `dz_publisher_recv_to_send_latency_seconds` is `recv_ts_ns` to the
    /// moment the datagram left. Neither is something an adapter can compute,
    /// and both are lost if the payload cannot be reached from the sink.
    ///
    /// **So the runtime calls this, and an adapter never does.** An adapter is
    /// handed a sink for the duration of one
    /// [`Adapter::on_payload`](crate::Adapter::on_payload) and decides for
    /// itself when and whether to write to it; asking it to also pass its own
    /// payload through would be a convention every implementation had to
    /// remember, and the failure of forgetting would be a silent zero rather
    /// than a compile error. A driver holds the payload and the sink, so it can
    /// state this once around the call and be right for every event the adapter
    /// emits, including none.
    ///
    /// # Defaulted, and what a default costs
    ///
    /// Ignoring this is a sink that cannot attribute an event to a payload, and
    /// the cost is precisely the two families above: they exist, are pre-created
    /// at every label value, and stay at zero — which is indistinguishable from
    /// a publisher whose data has stopped. It is defaulted rather than required
    /// because a sink that merely records events — a test harness, an offline
    /// re-lowering — has no clock to measure against and nothing to do with it.
    /// A runtime that transmits should implement it.
    fn payload_scope(&mut self, recv_ts_ns: Option<u64>) {
        let _ = recv_ts_ns;
    }

    /// This adapter no longer trusts its own book for one instrument.
    ///
    /// **The one thing a venue knows that nothing else can.** An adapter owns
    /// its book, so it is the only layer that can tell it has stopped being
    /// right: a delta it could not route, a size it could not read, an upstream
    /// that resynchronised underneath it. Everything above this boundary sees
    /// only the events that did come out.
    ///
    /// What happens next is not the adapter's to decide, and that is why this
    /// says nothing about it. The runtime pauses the instrument, announces the
    /// discard on the wire, and schedules the recovery snapshot a subscriber
    /// needs before it can apply another delta — spec-timed work, on a port
    /// this boundary cannot reach.
    ///
    /// # Why the alternatives are worse
    ///
    /// The three things an adapter can do without this are all wrong. Publish
    /// on, and every later absolute quantity at that price is wrong for the
    /// rest of the era — a level update states the resting quantity, so a
    /// subscriber that missed one is not corrected by the next. Emit a clear,
    /// and it has told subscribers the levels are gone when they are not:
    /// `Event::Clear` is documented as **not** a resynchronisation signal
    /// precisely so that a subscriber applying one stays ready. Or drop the
    /// event silently, which is publishing on with less evidence.
    ///
    /// # Defaulted, and what a default costs
    ///
    /// Ignoring this is a runtime that has not implemented recovery, and the
    /// cost is a subscriber applying deltas to a book the publisher already
    /// knows is diverged. It is defaulted rather than required only because a
    /// sink that merely records events — a test harness, an offline
    /// re-lowering — has nothing to do with it. A runtime that transmits must
    /// implement it.
    fn desynchronised(&mut self, instrument: InstrumentRef, reason: Desync) {
        let _ = (instrument, reason);
    }
}

/// Where an adapter declares the instruments it wants published.
pub trait ListingSink {
    /// Offer one instrument for publication.
    ///
    /// Returns the handle to carry for it, or `None` when the runtime's
    /// selection policy declined: over the published cap, or not admissible.
    /// **A `None` is ordinary and is not an error** — a venue whose universe
    /// exceeds what a feed publishes is the normal case, and the policy that
    /// decides is the playbook's rather than the venue's.
    ///
    /// Offering the same instrument twice returns the handle already minted for
    /// it. An adapter may therefore re-offer its whole set without tracking
    /// what it has already offered, which is what makes a poll cheap to write
    /// correctly.
    fn list(&mut self, spec: &InstrumentSpec<'_>) -> Option<InstrumentRef>;

    /// Withdraw an instrument that has reached the end of its life.
    ///
    /// The runtime stops defining it and stops counting it in the manifest. It
    /// does not reuse its `Instrument ID`: a subscriber holding a book keyed on
    /// one must never find it pointing at something else.
    fn delist(&mut self, instrument: InstrumentRef);
}

/// Where an adapter writes the book it holds, when asked for a snapshot.
///
/// Levels are written outward from the top of each side, which is the order a
/// subscriber applies them in and the order a bounded snapshot truncates from
/// the far end of. The framing around them — the begin, the level count, the
/// declared depth bound, the end, and the sequence they are consistent as of —
/// is the runtime's, because it is what a subscriber's state machine depends on.
pub trait SnapshotSink {
    /// One resting price level.
    fn level(&mut self, side: Side, px: Scalar<'_>, qty: Scalar<'_>, order_count: Option<u16>);
}

/// Where an adapter writes what it needs to send upstream.
///
/// Used from [`Adapter::on_connected`](crate::Adapter::on_connected) to
/// authenticate and subscribe. The adapter says *what* to send; the transport
/// owns *when*, and owns the reconnection, the backoff and the rate limit that
/// decide it. An adapter that opened its own socket here would be reimplementing
/// the half of the problem this boundary exists to take away.
pub trait UpstreamSink {
    /// Send a text message, for a transport that distinguishes one.
    fn send_text(&mut self, text: &str);

    /// Send a binary message.
    fn send_binary(&mut self, bytes: &[u8]);
}
