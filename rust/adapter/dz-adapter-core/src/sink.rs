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
    /// This call is the message boundary a sink sees, so it comes first: ahead
    /// of that message's [`upstream_identity`](Self::upstream_identity), where
    /// the upstream numbers its own, and ahead of its events.
    ///
    /// The value must be one the adapter declared in
    /// [`Adapter::message_types`](crate::Adapter::message_types); anything else
    /// is counted under `other`. That bucket is not a failure — it is the guard
    /// on a label whose values belong to the upstream's vocabulary, where many
    /// APIs name a message after the subscription that carried it, which is one
    /// series per instrument.
    fn upstream_message(&mut self, message_type: &'static str);

    /// Name *which* upstream message this is, where the upstream numbers its
    /// own.
    ///
    /// [`upstream_message`](Self::upstream_message) states the kind and this
    /// states the identity: `sid` is the upstream's own session or connection
    /// identifier and `seq` its own number within that session. Neither is a
    /// `Sequence Number` in this family's sense — that one belongs to a channel
    /// instance and is minted above this boundary — and an adapter passes both
    /// through exactly as the upstream stated them, without renumbering,
    /// rebasing or filling in a gap.
    ///
    /// **Both are `Option` because a venue that publishes one and not the other
    /// is ordinary.** Plenty number their messages and never name the session;
    /// some do the reverse. A pair of sentinels would make an adapter invent a
    /// value for whichever half it does not have, and `0` is a number the other
    /// kind of upstream really sends.
    ///
    /// **Neither value is ever a key.** The rule the `event.upstream_ts` column
    /// already carries applies unchanged here: a numbering whose resolution and
    /// meaning differ between transports would give one book state two hashes,
    /// and a race keyed on it would find no pair and read as a quiet feed. This
    /// is evidence a query reads, and nothing joins on it.
    ///
    /// # When to call it
    ///
    /// **After [`upstream_message`](Self::upstream_message) for the message this
    /// identifies, and before any [`event`](Self::event) that message produced.**
    /// That is the whole of the order, and it is required rather than
    /// conventional: what a sink holds is the identity of the message whose
    /// events *follow*.
    ///
    /// The kind states the message boundary, so the identity belongs inside it.
    /// It is stated at most once per upstream message — a payload carrying a
    /// batch states each member's after that member's kind — and it is **in
    /// force only until the next `upstream_message`**. A sink must not carry it
    /// across one, and an adapter whose upstream numbers some members and not
    /// others simply does not call this for the others, which leaves those
    /// events with no identity rather than the previous member's.
    ///
    /// An adapter that states the identity *after* the events it belongs to
    /// attributes every one of them to the message before, and the first events
    /// of a connection to nothing. Every row is still written and every row is
    /// still plausible, so the misattribution has no symptom: it is off by one
    /// message, uniformly, and the value it puts in the column is a real number
    /// the upstream really sent.
    ///
    /// # Nothing can check this, which is why it is written down
    ///
    /// The order is unenforced and unenforceable from above. A runtime hands an
    /// adapter a sink for one
    /// [`Adapter::on_payload`](crate::Adapter::on_payload) and does not decode
    /// the venue's bytes, so it cannot tell which call belongs to which upstream
    /// message; the only layer that knows is the adapter. There is no return
    /// value to refuse with, no compile error to fail with — a defaulted method
    /// called in the wrong place still type-checks — and no counter that would
    /// move, because the right number of calls is made in the wrong order.
    ///
    /// So this is what an adapter owes: the two calls in that order, around the
    /// events of one message. A venue's own tests are where it is held, and the
    /// shape that makes it easy to hold is stating both at the top of the branch
    /// that decoded the message, before anything is emitted.
    ///
    /// # What each side does with it
    ///
    /// **A publisher does nothing.** A venue's own session numbering is a
    /// transport's number rather than a book's, which is why the row tables
    /// refuse it along with the rest of a venue's provenance, and why nothing
    /// stated here reaches the wire.
    ///
    /// **A recorder keeps it, because for a recorder it is evidence.** It
    /// separates a venue resending a state from the venue producing that state
    /// again — two things that are the same book and not the same event — and
    /// without it an unpaired occurrence has one fewer explanation available to
    /// it. The only other way to obtain it is to decode the payload a second
    /// time beside the adapter, and two decoders of one venue is a race
    /// measuring its own decoders.
    ///
    /// # Defaulted, and what a default costs
    ///
    /// Ignoring this is the publisher's case, so the default is what a publisher
    /// wants and costs it nothing. It is defaulted rather than required because
    /// requiring it would make every sink in the workspace, and every venue's,
    /// change in order to discard a value — which is the whole of "no adapter
    /// changes", and is why the default is not optional.
    fn upstream_identity(&mut self, sid: Option<u64>, seq: Option<u64>) {
        let _ = (sid, seq);
    }

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

/// The shard an instrument is admitted to when nothing names one.
///
/// One token, spelled once, because both sides resolve to it: a configuration
/// block that states no shard, and [`ListingSink::list`], which forwards here.
/// Two spellings of one shard would be two published sets and two channels for
/// what an operator wrote down as one, so a block that states this token
/// explicitly is refused at load rather than accepted as a synonym.
pub const DEFAULT_SHARD: &str = "default";

/// Where an adapter declares the instruments it wants published.
///
/// # An implementor written before shards no longer compiles
///
/// [`list_on`](Self::list_on) is required and [`list`](Self::list) is
/// defaulted, so a sink that implements only `list` is a compile error rather
/// than a publisher that admits everything to one shard:
///
/// ```compile_fail,E0046
/// use dz_adapter_core::{InstrumentRef, InstrumentSpec, ListingSink};
///
/// struct Everything;
///
/// impl ListingSink for Everything {
///     fn list(&mut self, _spec: &InstrumentSpec<'_>) -> Option<InstrumentRef> {
///         Some(InstrumentRef::from_admission(0))
///     }
///     fn delist(&mut self, _instrument: InstrumentRef) {}
/// }
/// ```
///
/// The one method an implementor writes instead, which is the same sink with
/// the shard it was already being handed:
///
/// ```
/// use dz_adapter_core::{InstrumentRef, InstrumentSpec, ListingSink};
///
/// struct Everything;
///
/// impl ListingSink for Everything {
///     fn list_on(&mut self, _shard: &str, _spec: &InstrumentSpec<'_>) -> Option<InstrumentRef> {
///         Some(InstrumentRef::from_admission(0))
///     }
///     fn delist(&mut self, _instrument: InstrumentRef) {}
/// }
/// ```
pub trait ListingSink {
    /// Offer one instrument for publication on a named shard.
    ///
    /// Returns the handle to carry for it, or `None` when the runtime declined:
    /// over the published cap, not admissible, or naming a shard this publisher
    /// was not configured with. **A `None` is ordinary and is not an error** — a
    /// venue whose universe exceeds what a feed publishes is the normal case,
    /// and the policy that decides is the playbook's rather than the venue's.
    ///
    /// Offering the same instrument twice returns the handle already minted for
    /// it. An adapter may therefore re-offer its whole set without tracking
    /// what it has already offered, which is what makes a poll cheap to write
    /// correctly. Re-offering it under a *different* shard does **not** move
    /// it: the shard is fixed at admission, because no message in the family
    /// says that an instrument moved, and a subscriber on the channel it left
    /// would see it stop updating — which is indistinguishable from a market
    /// that went quiet.
    ///
    /// # A shard is the most a venue may say about where an instrument goes
    ///
    /// The name is the venue's own word for a partition it computes. Which
    /// `Channel ID`, group, port, sequence series and era that shard resolves
    /// to is the operator's, stated in configuration, and unreachable from
    /// here — there is no parameter to pass one through. An adapter with no
    /// partition to state calls [`list`](Self::list) and never sees a shard at
    /// all.
    ///
    /// # Required, while [`list`](Self::list) is defaulted
    ///
    /// The other direction is the trap. Defaulting `list_on` to `list` would
    /// leave an implementor that had not been updated admitting its whole
    /// universe to [`DEFAULT_SHARD`] — every shard collapsed onto one channel,
    /// with no error, no counter and no log. Requiring this one makes that
    /// omission a compile error, and the crate pays the version bump that
    /// costs.
    ///
    /// **The bump is not free.** A venue *calls* this trait rather than
    /// implementing it, so the break falls on implementors — and implementors
    /// outside this workspace exist: a test double for a venue's own adapter
    /// implements this sink even where the adapter under test only calls it.
    /// What makes the break worth asking for is not that nobody pays it. It is
    /// where it lands: a compile error in a test double is found by the next
    /// `cargo test`, and a silently collapsed published set is found by a
    /// subscriber holding definitions for instruments no message will ever
    /// arrive for on the channel it is bound to.
    fn list_on(&mut self, shard: &str, spec: &InstrumentSpec<'_>) -> Option<InstrumentRef>;

    /// Offer one instrument on the default shard.
    ///
    /// What an adapter that computes no partition calls, and what every adapter
    /// written before shards existed already calls. Defaulted rather than
    /// required because [`DEFAULT_SHARD`] is the answer for a venue that has
    /// nothing to say here, and a sink that has to spell it out is a sink that
    /// can spell it differently.
    fn list(&mut self, spec: &InstrumentSpec<'_>) -> Option<InstrumentRef> {
        self.list_on(DEFAULT_SHARD, spec)
    }

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
/// Reached from two methods, and the difference between them is *when*:
/// [`Adapter::on_connected`](crate::Adapter::on_connected) to authenticate and
/// subscribe at logon, and
/// [`Adapter::poll_upstream`](crate::Adapter::poll_upstream) for whatever is
/// still outstanding on a connection that is already open — an instrument
/// admitted after the subscription was composed, or a request the repair path
/// needs.
///
/// The adapter says *what* to send; the transport owns *when*, and owns the
/// reconnection, the backoff and the rate limit that decide it. An adapter that
/// opened its own socket here would be reimplementing the half of the problem
/// this boundary exists to take away.
///
/// **Nothing here is deduplicated.** The runtime does not understand a venue's
/// bytes, so it cannot tell a subscription already sent from a new one. That
/// costs nothing at logon, where the connection is new and so is everything
/// written to it, and it is the whole contract of `poll_upstream`, which is
/// asked repeatedly.
pub trait UpstreamSink {
    /// Send a text message, for a transport that distinguishes one.
    fn send_text(&mut self, text: &str);

    /// Send a binary message.
    fn send_binary(&mut self, bytes: &[u8]);
}
