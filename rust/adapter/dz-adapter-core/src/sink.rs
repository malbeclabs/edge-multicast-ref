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

use crate::event::{Event, Side};
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
