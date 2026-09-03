//! The composed publisher: the wiring, the ticks, the guards and the teardown.
//!
//! Everything in this module is synchronous and takes its time through an
//! injected [`Clock`]. That is what makes the wiring testable: a normalized
//! event goes in through [`EventSink`], a datagram comes out of a
//! [`DatagramSink`](dz_publisher_egress::DatagramSink), and there is no socket,
//! no filesystem, no privilege and no sleep anywhere between. [`crate::run()`]
//! is the layer that supplies the real implementations and the waiting; nothing
//! it adds decides anything.
//!
//! # What this type owns, and what it only holds
//!
//! It owns the routing — which lowering an event goes through, which feed
//! carries the result, and which port role it is pushed onto — the cadences,
//! the guards, and the order of the teardown. It owns none of the things the
//! crates it holds own: not the `Instrument ID`, not the exponents, not
//! `Update Flags`, not `Action`, not `Per-Instrument Seq`, not `Sequence
//! Number`, not `Reset Count`, not the datagram cap, and not one metric name.
//!
//! # The routing, and why `Trade` is lowered once
//!
//! | Event | Top-of-book feed | Market-by-price feed |
//! |---|---|---|
//! | `Quote` | `0x03` on mktdata | — |
//! | `Trade` | `0x04` on mktdata | `0x04` on mktdata |
//! | `Level` | — | `0x40` on mktdata |
//! | `Clear` | — | `0x41` on mktdata |
//! | a pulled snapshot | — | `0x20`/`0x42`/`0x22` on snapshot |
//!
//! `Trade` is the row that needs an argument. The wire's cross-specification
//! policy requires `0x04` to be **byte-for-byte identical** across a venue's
//! sibling feeds, and in one existing publisher that obligation is held by a
//! doc comment across two separate encoder implementations, checked by hand.
//! `dz-publisher-lowering` made it one function; this makes it one *value*. The
//! trade is lowered once and the same `Trade` is handed to both send paths, so
//! the two are not two things that agree — they are one thing, and there is no
//! second call site to drift.
//!
//! An event no enabled feed carries — a `Quote` on a publisher that emits only
//! depth, or a variant a later boundary release adds — is counted and dropped
//! **before** it is lowered. See [`Publisher::unroutable`].
//!
//! # The instrument table is borrowed per call, and that is the whole reason
//!
//! [`Lowering`] and [`DepthLowering`] take `&InstrumentTable` per call rather
//! than holding one. Holding it would borrow the table immutably for as long as
//! the publisher was lowering anything, and the reference-data owner needs it
//! mutably to admit and withdraw — so a publisher would have to stop lowering to
//! admit an instrument. For [`DepthLowering`] it is worse than awkward: it
//! carries `Per-Instrument Seq`, and rebuilding it to release a borrow would
//! restart that sequence, which no subscriber can tell apart from a channel
//! reset. So the registry owns the table, this type holds both lowerings for the
//! life of the era, and the table is passed at each call.

use std::sync::Arc;

use dz_adapter_core::{
    Adapter, DepthBound, Desync, Event, EventSink, InstrumentRef, VenueTimestampKind,
};
use dz_edge_core::fixed_point::ScaleError;
use dz_edge_mbp::MarketByPrice;
use dz_edge_refdata::{InstrumentDefinition, ManifestSummary};
use dz_edge_tob::TopOfBook;
use dz_publisher_lowering::{DepthLowering, Lowering, LoweringError, Snapshot, SourceId};
use dz_publisher_metrics::{
    EgressMessageType, EventKind, LoweringRefusalReason, PublisherMetrics, RefdataLoadErrorReason,
    TimestampKind,
};
use dz_publisher_refdata::{Counts, Registry, StateStore};

use crate::clock::Clock;
use crate::config::EmittedFeed;
use crate::guard::{ConsistencyGuard, Exit, IdleGuard, Inconsistency};
use crate::pipeline::FeedPipeline;
use crate::rotation::SnapshotRotation;

/// How often the runtime drains the adapter's listings.
///
/// A constant and not a configuration key, because the design names no key for
/// it and inventing one would be a value an operator could set wrong for no
/// benefit. A second is affordable by construction: the boundary promises an
/// adapter may re-offer its whole set on every poll, and the registry's
/// re-offer path is one hash lookup, one composition on stack values, and a
/// comparison — so a poll that changes nothing writes nothing and touches no
/// disk.
pub const LISTING_POLL: std::time::Duration = std::time::Duration::from_secs(1);

/// The send paths this publisher operates, one per enabled `[[feed]]` block.
///
/// Two typed fields rather than a collection, because
/// [`FeedPipeline`] is generic over the wire feed — `Magic` belongs to the feed
/// — so the two are different types and a `Vec` of them would need dynamic
/// dispatch on the datagram path to buy nothing. Every field is an `Option`
/// because a publisher emits one feed or several, which is what `[[feed]]`
/// being an array is for.
///
/// A publisher with neither is refused before this type is built; see
/// [`crate::StartupError::NoEnabledFeed`].
#[derive(Default)]
pub struct Feeds {
    pub top_of_book: Option<FeedPipeline<TopOfBook>>,
    pub market_by_price: Option<FeedPipeline<MarketByPrice>>,
}

impl Feeds {
    /// Every enabled feed's `Channel ID`.
    #[must_use]
    pub fn channel_ids(&self) -> Vec<u8> {
        let mut ids: Vec<u8> = [
            self.top_of_book.as_ref().map(FeedPipeline::channel_id),
            self.market_by_price.as_ref().map(FeedPipeline::channel_id),
        ]
        .into_iter()
        .flatten()
        .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// A dropped fan-out member whose failure darkens this publisher, on any
    /// feed and any port role.
    #[must_use]
    pub fn dark_transmitter(&self) -> Option<String> {
        self.top_of_book
            .as_ref()
            .and_then(FeedPipeline::dark_transmitter)
            .or_else(|| {
                self.market_by_price
                    .as_ref()
                    .and_then(FeedPipeline::dark_transmitter)
            })
            .map(str::to_owned)
    }
}

/// Why a pulled snapshot did not reach the wire.
///
/// Four cases, because the caller does different things with them. An adapter
/// that is not ready is a slot to skip and come back to — one dormant
/// instrument rather than a restart loop — and it is deliberately not counted
/// as a lowering refusal, because an operator acts differently on *this
/// instrument's exponent is wrong* and *this instrument's book is still warming
/// up*. Collapsing the two would put the second in the bucket that sends
/// someone to look at reference data.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// This publisher emits no feed with a snapshot port role.
    ///
    /// A caller's mistake rather than a runtime condition: a top-of-book
    /// publisher has no book state to serve and no port to serve it on.
    #[error("this publisher emits no feed that carries a snapshot port role")]
    NoDepthFeed,

    /// The adapter could not answer: a book that has not bootstrapped, or a
    /// handle it does not hold.
    #[error(transparent)]
    Adapter(#[from] dz_adapter_core::AdapterError),

    /// The framing refused it: an unknown handle, or the first level whose
    /// price or quantity the instrument's exponents cannot state exactly.
    #[error(transparent)]
    Lowering(#[from] LoweringError),

    /// The snapshot framed and did not send.
    #[error(transparent)]
    Egress(#[from] dz_publisher_egress::EgressError),
}

/// Lowering refusals, by the reason each is distinguishable under.
///
/// # There is no series for these, and this is why the numbers are here instead
///
/// `LoweringError::reason` keeps five reasons apart because an operator acts
/// differently on each: a value too precise for the exponent means the exponent
/// is wrong for that instrument, a value that is not a decimal means the
/// upstream changed its format, a value that does not fit means the field is too
/// narrow, a contract size that does not divide means the size is wrong or the
/// venue has started quoting on a finer grid than its own contract admits, and
/// an unknown handle means the adapter is carrying one the table does not hold.
///
/// The normative `dz_publisher_*` set has no family for any of them, and the set
/// is closed by a governing playbook. Every candidate is worse than none:
/// `ingress_parse_errors_total` is about reading an upstream payload and its
/// four reasons do not include these, `egress_errors_total`'s five values are
/// about a datagram and a socket, and folding a scaling refusal into either
/// makes an existing panel mean two things in exactly the incident where it is
/// being read. `dz-publisher-egress` already met this and answered it the same
/// way — `EgressError::reason` returns `None` for the one failure the closed set
/// has no reason for, and keeps the failure distinguishable in the error.
///
/// The family now exists — `dz_publisher_lowering_refusals_total{reason}`, with
/// exactly these five values — and every refusal reaches it. The counts stay
/// because the exit report prints them, and because a number a process can read
/// back out of itself is what makes a test able to assert one without scraping.
///
/// It is a **proposed** addition to the normative set rather than one the
/// governing playbook already carries: the metrics crate keeps proposals in a
/// list of their own for exactly that reason, and each says so in its own help
/// text.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Refusals {
    /// An event naming a handle the instrument table does not hold: forged, or
    /// outliving its instrument's withdrawal.
    pub unknown_instrument: u64,
    /// The instrument's contract size does not divide the value exactly.
    pub inexact_contract: u64,
    /// More precision than the instrument's exponent can state.
    pub too_precise: u64,
    /// Not a decimal number in the accepted grammar.
    pub malformed: u64,
    /// Exact, and past what the wire's integer can hold.
    pub overflow: u64,
}

impl Refusals {
    /// Count one refusal under its own reason.
    ///
    /// An exhaustive match over both enumerations rather than a lookup on
    /// `LoweringError::reason`'s token, so that a reason added on either side
    /// fails to compile here instead of being counted under whichever bucket a
    /// fallback arm named.
    fn record(&mut self, error: LoweringError, metrics: &PublisherMetrics) {
        // One match for both the count and the label, so the two cannot
        // disagree about which reason a refusal was. Splitting them into two
        // matches is how a series and a report come to tell different stories
        // about the same event.
        let reason = match error {
            LoweringError::UnknownInstrument => {
                self.unknown_instrument += 1;
                LoweringRefusalReason::UnknownInstrument
            }
            LoweringError::InexactContract { .. } => {
                self.inexact_contract += 1;
                LoweringRefusalReason::InexactContract
            }
            LoweringError::Scale { source, .. } => match source {
                ScaleError::TooPrecise { .. } => {
                    self.too_precise += 1;
                    LoweringRefusalReason::TooPrecise
                }
                ScaleError::Malformed => {
                    self.malformed += 1;
                    LoweringRefusalReason::Malformed
                }
                ScaleError::Overflow => {
                    self.overflow += 1;
                    LoweringRefusalReason::Overflow
                }
            },
        };
        metrics.lowering().refusal(reason);
    }

    /// Every reason and its count, in the tokens `LoweringError::reason` uses.
    #[must_use]
    pub const fn by_reason(&self) -> [(&'static str, u64); 5] {
        [
            ("unknown_instrument", self.unknown_instrument),
            ("inexact_contract", self.inexact_contract),
            ("too_precise", self.too_precise),
            ("malformed", self.malformed),
            ("overflow", self.overflow),
        ]
    }

    #[must_use]
    pub const fn total(&self) -> u64 {
        self.unknown_instrument
            + self.inexact_contract
            + self.too_precise
            + self.malformed
            + self.overflow
    }
}

/// One step of the teardown, in the order it happens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeardownStep {
    /// Nothing more will arrive from upstream.
    IngressStopped,
    /// `Valid` is 0 and nothing further is admitted.
    AdmissionsClosed,
    /// The last `ManifestSummary`, carrying `Valid = 0`, is on every feed's
    /// refdata port.
    FinalManifestSent,
    /// `EndOfSession` is on every feed's mktdata port.
    EndOfSessionSent,
    /// Every port role's open datagram has been sent.
    PortsFlushed,
    /// `dz_publisher_exit_reason_total` has moved.
    ExitRecorded,
}

impl TeardownStep {
    /// The order every teardown follows. Transcribed here so that a test
    /// asserts the sequence against a stated list rather than against whatever
    /// the code did.
    pub const ORDER: [Self; 6] = [
        Self::IngressStopped,
        Self::AdmissionsClosed,
        Self::FinalManifestSent,
        Self::EndOfSessionSent,
        Self::PortsFlushed,
        Self::ExitRecorded,
    ];
}

/// What a teardown did.
#[derive(Debug, Clone)]
pub struct Teardown {
    steps: Vec<TeardownStep>,
    exit: Exit,
}

impl Teardown {
    /// The steps, in the order they happened.
    #[must_use]
    pub fn steps(&self) -> &[TeardownStep] {
        &self.steps
    }

    /// Why the process is ending.
    #[must_use]
    pub const fn exit(&self) -> &Exit {
        &self.exit
    }
}

/// The composed publisher.
///
/// Generic over the state store and the clock, which is how the same wiring runs
/// against a real state directory and a real clock in production and against
/// [`MemoryStore`](dz_publisher_refdata::MemoryStore) and
/// [`ManualClock`](crate::ManualClock) in a test — with no `cfg(test)` anywhere
/// and no second implementation of anything.
pub struct Publisher<S: StateStore, K: Clock + Clone> {
    metrics: Arc<PublisherMetrics>,
    refdata: Registry<S, K>,
    clock: K,
    lowering: Lowering,
    /// Held for the life of the era and **never rebuilt**, because it carries
    /// `Per-Instrument Seq`. Rebuilding it mid-era restarts a sequence a
    /// subscriber reads as a channel reset.
    depth: DepthLowering,
    feeds: Feeds,
    idle: IdleGuard,
    consistency: ConsistencyGuard,
    /// One buffer for the life of the process; `definition_tick` clears and
    /// fills it.
    definitions: Vec<InstrumentDefinition>,
    /// The counts already forwarded to the metric registry, so each tick
    /// forwards the delta.
    forwarded: Counts,
    refusals: Refusals,
    unroutable: u64,
    /// Instruments announced as discarded, each with the anchor its
    /// `InstrumentReset` promised. See [`Self::owed_snapshots`].
    owed: Vec<(InstrumentRef, u64)>,
    /// The receive stamp of the payload currently being mapped, stated by the
    /// transport's wrapper before the adapter is handed anything and withdrawn
    /// when the mapping ends. `None` means no payload is in force — a snapshot
    /// pulled on the runtime's own cadence, a definition from the refdata
    /// cycle — and neither latency family is observed for those, because
    /// neither arrived from upstream.
    payload_recv_ts_ns: Option<u64>,
    /// Which venue clock this adapter's `source_ts_ns` values carry, read once
    /// at startup. `None` for a venue that publishes none, which is a real
    /// answer and not a missing one.
    venue_timestamp_kind: Option<TimestampKind>,
    /// Monotonic. When the adapter's listings were last drained.
    last_poll_ns: Option<u64>,
    /// The periodic snapshot rotation, when the depth feed configures a cycle.
    /// `None` is a publisher that emits recovery snapshots and no others.
    snapshots: Option<SnapshotRotation>,
    seeded: bool,
}

impl<S: StateStore, K: Clock + Clone> Publisher<S, K> {
    /// Compose a publisher over an opened reference-data registry and the built
    /// send paths.
    ///
    /// Both are arguments and neither is opened here, which is the whole reason
    /// the composition is testable: the registry arrives having already claimed
    /// its state directory (or a memory store standing in for one), and the send
    /// paths arrive holding fan-outs whose members may be recording sinks rather
    /// than sockets.
    ///
    /// **One registry serves every feed**, and that is right rather than a
    /// simplification. `Instrument ID` identity is the one thing there can only
    /// be one of, so two registries would be two ID spaces and a published ID
    /// would resolve to two different definitions. `Manifest Seq` is a property
    /// of the published set, which is the same set on every feed. And the
    /// manifest's own redundant `Channel ID` field is stamped by the builder
    /// from the datagram that frames it, so one composed manifest is truthful
    /// on every feed's refdata port.
    #[must_use]
    pub fn new(
        metrics: Arc<PublisherMetrics>,
        refdata: Registry<S, K>,
        clock: K,
        source_id: SourceId,
        feeds: Feeds,
        idle_guard: std::time::Duration,
    ) -> Self {
        let snapshots = feeds
            .market_by_price
            .as_ref()
            .and_then(FeedPipeline::snapshot_cycle)
            .map(SnapshotRotation::new);
        Self {
            metrics,
            refdata,
            clock,
            lowering: Lowering::new(source_id),
            depth: DepthLowering::new(source_id),
            feeds,
            idle: IdleGuard::new(idle_guard),
            consistency: ConsistencyGuard::new(),
            definitions: Vec::new(),
            forwarded: Counts::default(),
            refusals: Refusals::default(),
            unroutable: 0,
            owed: Vec::new(),
            payload_recv_ts_ns: None,
            venue_timestamp_kind: None,
            last_poll_ns: None,
            snapshots,
            seeded: false,
        }
    }

    /// The instruments owing a recovery snapshot, each with the anchor its
    /// reset promised, drained.
    ///
    /// **The anchor travels with the debt, and that is not an optimisation.**
    /// The specification obliges a snapshot with `Anchor Seq` *equal to* the
    /// value the reset named — not equal to wherever the stream has reached by
    /// the time the book is captured. Those differ by at least one, because the
    /// reset's own datagram advanced the sequence, and a snapshot anchored a
    /// number later is one a subscriber discards: it records the reset's anchor
    /// as the minimum it will accept. The instrument would then wait forever,
    /// having been told to expect something that never came.
    ///
    /// Draining rather than reading, and keeping the **latest** anchor for an
    /// instrument owed twice: the second reset supersedes the first, and a
    /// snapshot at the older anchor is one the second reset has already told
    /// subscribers to discard.
    pub fn owed_snapshots(&mut self) -> Vec<(InstrumentRef, u64)> {
        let mut owed = std::mem::take(&mut self.owed);
        // Instrument ascending, anchor **descending**, so the first entry for
        // each instrument is its latest reset — which is the one `dedup_by_key`
        // keeps. Sorting both ascending would keep the earliest anchor, the one
        // the later reset has already told subscribers to discard.
        owed.sort_unstable_by_key(|(instrument, anchor)| (*instrument, std::cmp::Reverse(*anchor)));
        owed.dedup_by_key(|(instrument, _)| *instrument);
        owed
    }

    /// Read the adapter's venue-timestamp declaration, once, at startup.
    ///
    /// Once rather than per event because it is a property of the adapter and
    /// not of a message — which is also this boundary's limit: a venue exposing
    /// a matching-engine stamp on trades and a gateway stamp on quotes cannot
    /// say which an individual event used, so the gauge that counts how many
    /// kinds are available can only ever read 0 or 1 through here.
    pub fn declare_venue_timestamps(&mut self, adapter: &dyn Adapter) {
        self.venue_timestamp_kind = adapter.source_timestamp_kind().map(timestamp_kind);
        self.metrics
            .latency()
            .set_venue_timestamps_available(i64::from(self.venue_timestamp_kind.is_some()));
    }

    /// Record build identity, once, at startup.
    pub fn record_build_info(&self, version: &str, commit: &str, toolchain: &str) {
        self.metrics
            .process()
            .set_build_info(version, commit, toolchain);
    }

    /// Drain the adapter's listings if the poll is due.
    ///
    /// The adapter is an argument rather than a field, for the same reason the
    /// instrument table is an argument to the lowering:
    /// [`dz_ingress_core::Driver`] holds the adapter mutably for as long as it
    /// is driving, so a publisher that also held one could never poll. Passing
    /// it in at the two call sites that need it — this one and
    /// [`Self::snapshot`] — is what lets the driver keep its borrow.
    ///
    /// Returns whether the listings were drained.
    pub fn poll_listings(&mut self, adapter: &mut dyn Adapter) -> bool {
        let now_ns = dz_publisher_refdata::Clock::monotonic_ns(&self.clock);
        let due = self
            .last_poll_ns
            .is_none_or(|last| now_ns.saturating_sub(last) >= nanos(LISTING_POLL));
        if !due {
            return false;
        }
        self.last_poll_ns = Some(now_ns);
        adapter.poll_listings(&mut self.refdata);
        // The seed limit gives way to the cap, and the manifest becomes
        // `Valid`, once — and only after the *first* poll has returned. Calling
        // it earlier would spend the headroom the cap leaves above the seed on
        // whatever the venue happened to list first.
        if !self.seeded {
            self.refdata.seeding_complete();
            self.seeded = true;
        }
        true
    }

    /// One pass over everything that is a question of *when*.
    ///
    /// Returns the exit a guard decided on, if one did. Called from a loop whose
    /// interval does not matter: every cadence in here is read off the clock as
    /// a debt rather than counted in ticks, which is what
    /// [`DefinitionPacer`](dz_publisher_refdata::DefinitionPacer) does too, so a
    /// runtime ticking every 10ms and one ticking every 250ms lap the definition
    /// set in the same time and neither can be made to burst by ticking slowly.
    ///
    /// The definition tick is drained **once** and packed onto every feed's
    /// refdata port. Draining per feed would ask the pacer for the lap's debt
    /// twice and emit twice as much of the set per tick, which is the burst the
    /// pacer exists to prevent arriving through the caller.
    #[must_use]
    pub fn tick(&mut self) -> Option<Exit> {
        let now_mono = dz_publisher_refdata::Clock::monotonic_ns(&self.clock);
        let now_unix = self.clock.unix_ns();

        self.refdata.definition_tick(&mut self.definitions);
        let manifest = self.refdata.manifest();

        if let Some(pipeline) = self.feeds.top_of_book.as_mut() {
            tick_pipeline(pipeline, &self.definitions, &manifest, now_mono, now_unix);
        }
        if let Some(pipeline) = self.feeds.market_by_price.as_mut() {
            tick_pipeline(pipeline, &self.definitions, &manifest, now_mono, now_unix);
        }

        self.forward_counts();
        self.check_consistency();

        // The consistency guard is read first, deliberately. A transmitter
        // whose failure darkens this publisher *explains* publish silence, so
        // reporting the idle guard instead would send an operator to look at the
        // mapping when the socket is the answer.
        self.consistency
            .check()
            .or_else(|| self.idle.check(now_mono))
    }

    /// Pull one instrument's book from the adapter, frame it, and send it on the
    /// snapshot port role.
    ///
    /// **Pulled rather than pushed.** The cadence, the rotation across
    /// instruments and the framing belong to the runtime because they are what a
    /// subscriber's recovery depends on; the book belongs to the adapter because
    /// it is the venue's microstructure. So the runtime asks.
    ///
    /// **The `Depth Bound` is not a parameter**, and that is the point: it is
    /// whatever [`Adapter::snapshot`] returned. A bound this method accepted
    /// would be one its callers had to supply, and the value a caller with no
    /// book reaches for is `0` — which on the wire is a positive claim that the
    /// snapshot carries the complete book. See
    /// [`DepthBound`].
    ///
    /// The pacing is still the caller's, but there is now a rotation to call:
    /// [`periodic_snapshot`](Self::periodic_snapshot) drives `[[feed]]
    /// snapshot_cycle`.
    ///
    /// Returns the snapshot as it went out, so a caller can log the level count
    /// and a test can assert the framing.
    ///
    /// # Errors
    ///
    /// [`SnapshotError`], which keeps four causes apart. Nothing partial is
    /// framed: an incomplete snapshot is worse than none, because a subscriber
    /// cannot tell a refused level from a lost one.
    pub fn snapshot(
        &mut self,
        adapter: &dyn Adapter,
        instrument: InstrumentRef,
    ) -> Result<Snapshot, SnapshotError> {
        // The point in the live stream this book state is true as of, which is
        // what tells a subscriber which live messages to apply after it and
        // which to discard.
        let anchor = self
            .feeds
            .market_by_price
            .as_ref()
            .ok_or(SnapshotError::NoDepthFeed)?
            .mktdata_sequence()
            .unwrap_or(0);
        self.snapshot_anchored_at(adapter, instrument, anchor)
    }

    /// The recovery snapshot an [`InstrumentReset`] obliged, at the anchor that
    /// reset promised.
    ///
    /// **Not the live sequence.** A subscriber records the reset's anchor as
    /// the minimum `Anchor Seq` it will accept for that instrument, so a
    /// snapshot captured later and anchored where the stream has since reached
    /// is one it discards — leaving the instrument waiting for something that
    /// already went past. The anchor comes from
    /// [`owed_snapshots`](Self::owed_snapshots), which carries it for exactly
    /// this reason.
    ///
    /// # Errors
    ///
    /// As [`snapshot`](Self::snapshot).
    pub fn snapshot_anchored_at(
        &mut self,
        adapter: &dyn Adapter,
        instrument: InstrumentRef,
        anchor: u64,
    ) -> Result<Snapshot, SnapshotError> {
        let now_unix = self.clock.unix_ns();
        let Some(_) = self.feeds.market_by_price.as_ref() else {
            return Err(SnapshotError::NoDepthFeed);
        };
        let mut framer =
            self.depth
                .open_snapshot(self.refdata.instruments(), instrument, anchor, now_unix)?;
        // The adapter's refusal is carried through rather than folded into a
        // lowering refusal; see `SnapshotError`. What it returns instead of a
        // refusal is the depth the levels it just wrote were drawn from, which
        // is the one field of the framing that is the venue's.
        let depth_bound: DepthBound = adapter.snapshot(instrument, &mut framer)?;
        let snapshot = framer.finish(depth_bound)?;
        self.feeds
            .market_by_price
            .as_mut()
            .expect("checked above")
            .send_snapshot(&snapshot, now_unix)?;
        Ok(snapshot)
    }

    /// The next periodic snapshot the rotation owes, taken if one is due.
    ///
    /// `None` covers three states that are all *nothing to do now*: this
    /// publisher configured no `[[feed]] snapshot_cycle`, the derived tick has
    /// not elapsed, or the published set is empty. `Some` carries the outcome of
    /// the one instrument that fell due — including its refusal, because a
    /// caller that discarded it would turn a book that never bootstraps into
    /// silence nobody reads.
    ///
    /// # Why this exists at all
    ///
    /// A recovery snapshot answers a reset the publisher itself announced. It
    /// does nothing for the subscriber that joins mid-session, and that
    /// subscriber cannot build a book without one: a `LevelUpdate` states the
    /// resting quantity at a price, so a subscriber with no starting state is
    /// not corrected by the next message — it is wrong at every price it has
    /// never seen an update for, indefinitely. Both shipped publishers carry a
    /// periodic snapshot for exactly this reason and both set it to five
    /// seconds; a runtime with a snapshot port and no cadence is the outlier.
    ///
    /// # `NotReady` is not a failure here
    ///
    /// An adapter whose book has not bootstrapped refuses, the rotation has
    /// already stepped past it, and it comes back on the next lap. That is the
    /// documented contract of [`AdapterError::NotReady`](dz_adapter_core::AdapterError::NotReady)
    /// and the difference between one dormant instrument and a feed whose
    /// snapshots stop; a caller should log it at most quietly.
    pub fn periodic_snapshot(
        &mut self,
        adapter: &dyn Adapter,
    ) -> Option<Result<Snapshot, SnapshotError>> {
        let now_mono = dz_publisher_refdata::Clock::monotonic_ns(&self.clock);
        let due = self
            .snapshots
            .as_mut()?
            .due(now_mono, self.refdata.instruments())?;
        Some(self.snapshot(adapter, due))
    }

    /// One full pass of the snapshot rotation, if this publisher runs one.
    ///
    /// For a log line at startup: a depth feed with no cadence is a feed no
    /// joining subscriber can bootstrap from, and that is worth stating rather
    /// than leaving to be inferred from silence.
    #[must_use]
    pub fn snapshot_cycle(&self) -> Option<std::time::Duration> {
        self.snapshots.as_ref().map(SnapshotRotation::cycle)
    }

    /// Shut down, in the order below, and record the exit.
    ///
    /// # The order, and why it is this one
    ///
    /// 1. **The ingress is already stopped.** The caller's obligation, not this
    ///    method's: the driver holds the adapter, and a payload arriving after
    ///    `EndOfSession` would be lowered onto a channel that has already said
    ///    it is finished.
    /// 2. **Admissions close.** `Valid` returns to 0 and nothing further is
    ///    admitted, so no `Instrument ID` is minted and persisted for an
    ///    instrument no definition cycle will publish. The published set stays
    ///    as it is — it is still what the last manifest described.
    /// 3. **The final manifest goes out, carrying `Valid = 0`.** Before
    ///    `EndOfSession` and not after: it is a statement about the
    ///    reference-data set, it goes on the refdata port, and a subscriber that
    ///    stops reading at `EndOfSession` would never see it if the order were
    ///    reversed. Sending it first means a subscriber briefly sees a
    ///    non-authoritative set while mktdata is still live, which is exactly
    ///    the truth.
    /// 4. **`EndOfSession` goes out on mktdata.** The terminal statement for the
    ///    channel, and therefore last: anything after it contradicts it. On
    ///    every feed, because every feed's mktdata channel is ending.
    /// 5. **Every port role flushes.** A datagram left open holds a number that
    ///    has been assigned, and abandoning it is a gap for no reason.
    /// 6. **The exit is recorded**, so that `dz_publisher_exit_reason_total`
    ///    carries the reason before the last scrape.
    ///
    /// There is deliberately no *final snapshot* step. A snapshot describes a
    /// book a subscriber is about to be told has ended, and sending one on the
    /// way down would spend a snapshot series' numbers to describe state nobody
    /// can use.
    ///
    /// Releasing the sockets and the state directory is not a step here. Both
    /// are released by dropping this value, which is what also happens when the
    /// process is killed rather than asked — a teardown whose correctness
    /// depended on a close call would be a teardown that only works on the
    /// polite path.
    pub fn shut_down(&mut self, exit: Exit) -> Teardown {
        let now_mono = dz_publisher_refdata::Clock::monotonic_ns(&self.clock);
        let now_unix = self.clock.unix_ns();
        let mut steps = vec![TeardownStep::IngressStopped];

        self.refdata.begin_shutdown();
        steps.push(TeardownStep::AdmissionsClosed);

        let manifest = self.refdata.manifest();
        if let Some(pipeline) = self.feeds.top_of_book.as_mut() {
            let _ = pipeline.send_manifest(&manifest, now_mono, now_unix);
        }
        if let Some(pipeline) = self.feeds.market_by_price.as_mut() {
            let _ = pipeline.send_manifest(&manifest, now_mono, now_unix);
        }
        steps.push(TeardownStep::FinalManifestSent);

        if let Some(pipeline) = self.feeds.top_of_book.as_mut() {
            let _ = pipeline.send_end_of_session(now_mono, now_unix);
        }
        if let Some(pipeline) = self.feeds.market_by_price.as_mut() {
            let _ = pipeline.send_end_of_session(now_mono, now_unix);
        }
        steps.push(TeardownStep::EndOfSessionSent);

        if let Some(pipeline) = self.feeds.top_of_book.as_mut() {
            let _ = pipeline.flush(now_unix);
        }
        if let Some(pipeline) = self.feeds.market_by_price.as_mut() {
            let _ = pipeline.flush(now_unix);
        }
        steps.push(TeardownStep::PortsFlushed);

        self.forward_counts();
        self.metrics.process().exit(exit.reason());
        steps.push(TeardownStep::ExitRecorded);

        Teardown { steps, exit }
    }

    /// Lowering refusals, by reason. See [`Refusals`].
    #[must_use]
    pub const fn refusals(&self) -> Refusals {
        self.refusals
    }

    /// Events no enabled feed carried.
    ///
    /// A `Quote` on a publisher that emits only depth, a `Level` on one that
    /// emits only top-of-book, or an event variant a later boundary release
    /// adds that this build does not know. **Refused before the lowering rather
    /// than after**, which is the load-bearing part for the depth path:
    /// lowering a `Level` stamps `Per-Instrument Seq`, and a number spent on a
    /// message that never reached the wire is a gap every subscriber reads as
    /// packet loss.
    #[must_use]
    pub const fn unroutable(&self) -> u64 {
        self.unroutable
    }

    /// The reference-data owner, for a diagnostic and for a test.
    #[must_use]
    pub const fn refdata(&self) -> &Registry<S, K> {
        &self.refdata
    }

    /// The send paths, for a diagnostic and for a test.
    #[must_use]
    pub const fn feeds(&self) -> &Feeds {
        &self.feeds
    }

    /// The metric registry every crate below this one records through.
    #[must_use]
    pub fn metrics(&self) -> &Arc<PublisherMetrics> {
        &self.metrics
    }

    /// The depth lowering, which carries `Per-Instrument Seq`.
    ///
    /// Exposed so that a `Reset Count` change can end the era, which is the one
    /// thing that ends it — not a snapshot, and not a reconnect that did not
    /// change the reset count.
    pub fn depth_lowering_mut(&mut self) -> &mut DepthLowering {
        &mut self.depth
    }

    /// Forward the reference-data owner's counts to the registry, as deltas.
    ///
    /// The refdata crate constructs no metric — the normative set is closed and
    /// a series is not its to invent — so what it publishes is numbers, each
    /// documented against the family it belongs to. This is the other half of
    /// that arrangement, and every mapping here is the one that crate's own
    /// documentation states. `declined_at_cap` maps to nothing, deliberately: it
    /// is the selection policy working, and a series that climbed whenever a
    /// venue listed more instruments than a feed publishes would be alerting on
    /// the normal case.
    fn forward_counts(&mut self) {
        let counts = self.refdata.counts();
        let refdata = self.metrics.refdata();
        for _ in 0..counts.admitted.saturating_sub(self.forwarded.admitted) {
            refdata.new_listing();
        }
        for _ in 0..counts.delisted.saturating_sub(self.forwarded.delisted) {
            refdata.delisting();
        }
        for _ in 0..counts
            .definitions_emitted
            .saturating_sub(self.forwarded.definitions_emitted)
        {
            refdata.definition_emitted();
        }
        // The refdata crate's own mapping: an instrument the venue listed and
        // whose numbers cannot be stated on the wire is a reference-data load
        // that did not fully load, under the load-error family's `schema`
        // reason.
        for _ in 0..counts
            .declined_unrepresentable
            .saturating_sub(self.forwarded.declined_unrepresentable)
        {
            refdata.load_error(RefdataLoadErrorReason::Schema);
        }
        self.forwarded = counts;

        let published = i64::try_from(self.refdata.published()).unwrap_or(i64::MAX);
        refdata.set_instruments_current(published);
        // Per `Channel ID`, because the manifest is what a subscriber to that
        // channel reconciles against — and every feed advertises the same
        // published set, which is why one registry serves them all.
        for channel_id in self.feeds.channel_ids() {
            refdata.set_manifest_seq(channel_id, u64::from(self.refdata.manifest_seq()));
            refdata.set_manifest_valid(channel_id, self.refdata.is_valid());
        }
        self.metrics.book().set_instruments_published(published);
    }

    /// Read the two states the publisher cannot recover from in place.
    fn check_consistency(&mut self) {
        if let Some(sink) = self.feeds.dark_transmitter() {
            self.consistency.found(Inconsistency::EgressDark { sink });
        }
        if let Some(fault) = self.refdata.fault() {
            let detail = fault.to_string();
            self.consistency
                .found(Inconsistency::StateUnpersistable { detail });
        }
    }

    /// One message reached the wire.
    ///
    /// This is where `dz_publisher_recv_to_send_latency_seconds` would be
    /// observed and is not: it wants the interval between the payload arriving
    /// and the datagram leaving, and there is no way here to reach the first
    /// half of it. `Payload::recv_ts_ns` belongs to the driver, and
    /// `EventSink` — which is the whole of what this type is handed — does not
    /// carry it. Named here rather than left as a silently empty family; see
    /// the crate documentation.
    fn published(&mut self, now_mono_ns: u64, now_unix_ns: u64) {
        self.idle.published(now_mono_ns);
        self.metrics
            .process()
            .set_idle_guard_last_update(unix_seconds(now_unix_ns));
    }

    /// The two families that measure from a payload's arrival.
    ///
    /// Observed only for a message whose event arrived **inside a payload
    /// scope**: a snapshot pulled on the runtime's cadence, a definition from
    /// the refdata cycle and a heartbeat never came from upstream, so a
    /// latency measured for one would be measuring this process against
    /// itself.
    ///
    /// Both differences are taken against the **wall** clock, because
    /// `recv_ts_ns` is a wall or kernel reading and nothing in the types stops
    /// the wrong pairing. A monotonic reading differenced against a wall stamp
    /// is a number with no meaning that a histogram will happily accept.
    ///
    /// `venue_to_recv` needs a kind to label the observation with, so an
    /// adapter that reads a venue clock and does not declare which one leaves
    /// it unobserved rather than mislabelled.
    fn observe_arrival_latency(&mut self, source_ts_ns: u64, kind: EventKind, sent_unix_ns: u64) {
        let Some(recv_ts_ns) = self.payload_recv_ts_ns else {
            return;
        };
        let latency = self.metrics.latency();
        // Saturating, because a venue clock ahead of ours is a clock-skew
        // observation and not a negative duration. Zero is the honest floor.
        latency.observe_recv_to_send(kind, seconds(sent_unix_ns.saturating_sub(recv_ts_ns)));
        if let Some(kind) = self.venue_timestamp_kind {
            latency.observe_venue_to_recv(kind, seconds(recv_ts_ns.saturating_sub(source_ts_ns)));
        }
    }
}

/// A nanosecond difference as the seconds a histogram takes.
const fn seconds(nanos: u64) -> f64 {
    nanos as f64 / 1_000_000_000.0
}

/// The event-kind label for the send-side latency family.
///
/// The mapping the label's own vocabulary implies: everything that changes a
/// book is a book update, and a trade is a trade. `Event` is
/// `#[non_exhaustive]`, so a variant added upstream lands on the wildcard —
/// which is why it is written as an exhaustive-looking match with the wildcard
/// last and a comment rather than as a lookup: a new variant is a decision
/// somebody has to make here.
const fn event_kind(event: &Event<'_>) -> EventKind {
    match event {
        Event::Quote { .. } | Event::Level { .. } | Event::Clear { .. } => EventKind::BookUpdate,
        Event::Trade { .. } => EventKind::Trade,
        // A new feed's message is a book update until somebody decides
        // otherwise, which is the safer default: the alternative labels it a
        // trade and puts it in a panel counting executions.
        _ => EventKind::BookUpdate,
    }
}

/// The metric label for what an adapter declared.
///
/// Exhaustive, so a fifth kind on either side fails to compile here. The two
/// copies exist because `dz-adapter-core` must depend on nothing, and they are
/// held to each other by a test in the transport crate.
const fn timestamp_kind(kind: VenueTimestampKind) -> TimestampKind {
    match kind {
        VenueTimestampKind::ExchangeRecv => TimestampKind::ExchangeRecv,
        VenueTimestampKind::MatchingEngine => TimestampKind::MatchingEngine,
        VenueTimestampKind::GatewaySend => TimestampKind::GatewaySend,
        VenueTimestampKind::BlockTime => TimestampKind::BlockTime,
    }
}

/// Time an encode and a send, and record it under the message type's own label.
///
/// A free function taking the registry rather than a method taking `&self`, so
/// that the borrow it needs is of one field: the closure it wraps holds
/// `&mut self.feeds`, and a method would borrow the whole of `self` alongside
/// it.
///
/// Two clock reads per message, on the hot path, and they are worth it: this is
/// the only normative latency family the runtime can honestly observe — the two
/// that measure from a payload's arrival cannot be reached from `EventSink` at
/// all — and a family nobody ever writes to is indistinguishable from a
/// publisher that has stopped.
fn timed<T>(
    metrics: &PublisherMetrics,
    message_type: EgressMessageType,
    send: impl FnOnce() -> T,
) -> T {
    let started = std::time::Instant::now();
    let outcome = send();
    metrics
        .latency()
        .observe_encode_duration(message_type, started.elapsed().as_secs_f64());
    outcome
}

/// Everything one feed's send path owes a tick.
///
/// A free function generic over the feed, because the two send paths are
/// different types and this is the same behaviour for both. The definitions and
/// the manifest arrive as arguments rather than being drained here, so that the
/// pacer is asked once per tick however many feeds are enabled.
fn tick_pipeline<F: EmittedFeed>(
    pipeline: &mut FeedPipeline<F>,
    definitions: &[InstrumentDefinition],
    manifest: &ManifestSummary,
    now_mono_ns: u64,
    now_unix_ns: u64,
) {
    if pipeline.heartbeat_due(now_mono_ns) {
        let _ = pipeline.send_heartbeat(now_mono_ns, now_unix_ns);
    }
    for definition in definitions {
        let _ = pipeline.pack_definition(definition, now_unix_ns);
    }
    if pipeline.manifest_due(now_mono_ns) {
        let _ = pipeline.send_manifest(manifest, now_mono_ns, now_unix_ns);
    }
    // After the packing, so a definition tick that did not fill a datagram
    // still reaches the wire this tick rather than waiting for the one that
    // does. The refdata port is where the pacing already bounds the volume.
    let _ = pipeline.flush(now_unix_ns);
}

impl<S: StateStore, K: Clock + Clone> EventSink for Publisher<S, K> {
    /// The upstream said something the adapter recognised.
    ///
    /// This is where the idle guard's *upstream* half comes from, and it is the
    /// right signal rather than the convenient one: bytes off a socket include
    /// keepalives and acknowledgements, and a payload the adapter did not
    /// recognise was never going to produce a message. A recognised message
    /// was, which is what makes *upstream in, nothing out* mean something.
    ///
    /// The metric itself is recorded by the driver, which is the only layer that
    /// knows which connection delivered it.
    fn upstream_message(&mut self, _message_type: &'static str) {
        let now_ns = dz_publisher_refdata::Clock::monotonic_ns(&self.clock);
        self.idle.upstream(now_ns);
    }

    /// The adapter no longer trusts its own book for one instrument.
    ///
    /// Three things happen, in this order, and the order is the contract: the
    /// discard is announced on the wire anchored at the number its own datagram
    /// takes, the instrument is recorded as owing a recovery snapshot, and
    /// nothing else about the channel changes — every other instrument on it is
    /// unaffected, which is the whole point of a per-instrument signal.
    ///
    /// The snapshot is **owed, not sent here.** A subscriber discards any
    /// snapshot for the instrument with an older anchor, so it has to be
    /// captured after this message and before the next delta for that
    /// instrument — and capturing a book costs a walk of it, which does not
    /// belong inside an adapter's callback. [`owed_snapshots`](Self::owed_snapshots)
    /// is what the caller drains.
    fn desynchronised(&mut self, instrument: InstrumentRef, reason: Desync) {
        let now_mono = dz_publisher_refdata::Clock::monotonic_ns(&self.clock);
        let now_unix = self.clock.unix_ns();

        let Some(pipeline) = self.feeds.market_by_price.as_mut() else {
            // No depth feed carries `0x14`, so there is nothing to announce and
            // nothing to recover. Counted as unroutable, which is what every
            // other event no enabled feed carries is counted as.
            self.unroutable += 1;
            return;
        };
        // The anchor is where the stream is *now*: the reset takes effect
        // immediately, so it is the number the datagram carrying it will take,
        // read off the send path because nothing else knows it.
        let anchor = pipeline.mktdata_sequence().unwrap_or(0);

        let lowered = self.depth.lower_instrument_reset(
            self.refdata.instruments(),
            instrument,
            now_unix,
            reason,
            anchor,
        );
        match lowered {
            Ok(reset) => {
                let sent = timed(&self.metrics, EgressMessageType::InstrumentReset, || {
                    self.feeds
                        .market_by_price
                        .as_mut()
                        .expect("checked above")
                        .send_instrument_reset(&reset, now_mono, now_unix)
                });
                if sent.is_ok() {
                    // Recorded only once the announcement reached the wire. A
                    // snapshot owed for a reset no subscriber saw would arrive
                    // with an anchor nobody is waiting for.
                    self.owed.push((instrument, reset.new_anchor_seq));
                    self.published(now_mono, now_unix);
                }
            }
            Err(error) => self.refusals.record(error, &self.metrics),
        }
    }

    /// The transport states which payload is being mapped, and withdraws it.
    ///
    /// Not something an adapter passes through: the wrapper the driver builds
    /// opens the scope before the adapter is handed anything and closes it on
    /// drop, so a parse error, an early return and an unwind all close it.
    /// There is nothing for an adapter to remember, which is why it cannot
    /// forget.
    fn payload_scope(&mut self, recv_ts_ns: Option<u64>) {
        self.payload_recv_ts_ns = recv_ts_ns;
    }

    fn event(&mut self, event: Event<'_>) {
        // Read once, before the match, so every arm labels its observation the
        // same way and a new arm cannot forget to.
        let kind = event_kind(&event);
        let now_mono = dz_publisher_refdata::Clock::monotonic_ns(&self.clock);
        let now_unix = self.clock.unix_ns();
        let lowering = self.lowering;

        match event {
            Event::Quote {
                instrument,
                source_ts_ns,
                bid,
                ask,
            } => {
                // Refused before the lowering when no feed carries it, which
                // costs nothing here and is the same rule the depth arms below
                // need for a stronger reason.
                let Some(_) = self.feeds.top_of_book.as_ref() else {
                    self.unroutable += 1;
                    return;
                };
                let lowered = lowering.lower_quote(
                    self.refdata.instruments(),
                    instrument,
                    source_ts_ns,
                    bid,
                    ask,
                );
                match lowered {
                    Ok(quote) => {
                        let sent = timed(&self.metrics, EgressMessageType::Quote, || {
                            self.feeds
                                .top_of_book
                                .as_mut()
                                .expect("checked above")
                                .send_quote(&quote, now_mono, now_unix)
                        });
                        if sent.is_ok() {
                            self.published(now_mono, now_unix);
                            self.observe_arrival_latency(source_ts_ns, kind, now_unix);
                        }
                    }
                    Err(error) => self.refusals.record(error, &self.metrics),
                }
            }

            Event::Trade {
                instrument,
                source_ts_ns,
                px,
                qty,
                aggressor,
                trade_id,
                cumulative_volume,
                flags,
            } => {
                let lowered = lowering.lower_trade(
                    self.refdata.instruments(),
                    instrument,
                    source_ts_ns,
                    px,
                    qty,
                    aggressor,
                    trade_id,
                    cumulative_volume,
                    flags,
                );
                match lowered {
                    // **One value, both feeds.** The wire requires `0x04` to be
                    // byte-for-byte identical across a venue's sibling feeds,
                    // and this is the mechanism: there is one lowered trade and
                    // no second call site to drift. A trade also stamps no
                    // `Per-Instrument Seq` — the message has no such field, and
                    // it is not a book mutation.
                    Ok(trade) => {
                        let mut reached = false;
                        timed(&self.metrics, EgressMessageType::Trade, || {
                            if let Some(pipeline) = self.feeds.top_of_book.as_mut() {
                                reached |= pipeline.send_trade(&trade, now_mono, now_unix).is_ok();
                            }
                            if let Some(pipeline) = self.feeds.market_by_price.as_mut() {
                                reached |= pipeline.send_trade(&trade, now_mono, now_unix).is_ok();
                            }
                        });
                        if reached {
                            self.published(now_mono, now_unix);
                        }
                    }
                    Err(error) => self.refusals.record(error, &self.metrics),
                }
            }

            Event::Level {
                instrument,
                source_ts_ns,
                side,
                px,
                qty,
                order_count,
                presence,
            } => {
                // Before the lowering, and here that is the load-bearing order:
                // `lower_level` stamps `Per-Instrument Seq`, and a number spent
                // on a message that no feed will carry is a gap every
                // subscriber reads as packet loss.
                let Some(_) = self.feeds.market_by_price.as_ref() else {
                    self.unroutable += 1;
                    return;
                };
                let lowered = self.depth.lower_level(
                    self.refdata.instruments(),
                    instrument,
                    source_ts_ns,
                    side,
                    px,
                    qty,
                    order_count,
                    presence,
                );
                match lowered {
                    Ok(level) => {
                        let sent = timed(&self.metrics, EgressMessageType::LevelUpdate, || {
                            self.feeds
                                .market_by_price
                                .as_mut()
                                .expect("checked above")
                                .send_level(&level, now_mono, now_unix)
                        });
                        if sent.is_ok() {
                            self.published(now_mono, now_unix);
                            self.observe_arrival_latency(source_ts_ns, kind, now_unix);
                        }
                    }
                    Err(error) => self.refusals.record(error, &self.metrics),
                }
            }

            Event::Clear {
                instrument,
                source_ts_ns,
                scope,
            } => {
                let Some(_) = self.feeds.market_by_price.as_ref() else {
                    self.unroutable += 1;
                    return;
                };
                let lowered = self.depth.lower_clear(
                    self.refdata.instruments(),
                    instrument,
                    source_ts_ns,
                    scope,
                );
                match lowered {
                    Ok(clear) => {
                        let sent = timed(&self.metrics, EgressMessageType::BookClear, || {
                            self.feeds
                                .market_by_price
                                .as_mut()
                                .expect("checked above")
                                .send_book_clear(&clear, now_mono, now_unix)
                        });
                        if sent.is_ok() {
                            self.published(now_mono, now_unix);
                            self.observe_arrival_latency(source_ts_ns, kind, now_unix);
                        }
                    }
                    Err(error) => self.refusals.record(error, &self.metrics),
                }
            }

            // A variant a later boundary release adds — the market-by-order
            // ones, when `dz-edge-mbo` lands. Counted and dropped without being
            // lowered, for the same reason the depth arms check first.
            _ => self.unroutable += 1,
        }
    }
}

fn nanos(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Nanoseconds of Unix time as the seconds a gauge carries.
fn unix_seconds(unix_ns: u64) -> f64 {
    unix_ns as f64 / 1e9
}
