//! The composed publisher: the wiring, the ticks, the guards and the teardown.
//!
//! Everything in this module is synchronous and takes its time through an
//! injected [`Clock`]. That is what makes the wiring testable: a normalized
//! event goes in through [`EventSink`], a datagram comes out of a
//! [`DatagramSink`](dz_publisher_egress::DatagramSink), and there is no socket,
//! no filesystem, no privilege and no sleep anywhere between. [`crate::run()`] is
//! the layer that supplies the real implementations and the waiting; nothing it
//! adds decides anything.
//!
//! # What this type owns, and what it only holds
//!
//! It owns the routing — which lowering an event goes through and which port
//! role the result is pushed onto — the cadences, the guards, and the order of
//! the teardown. It owns none of the things the crates it holds own: not the
//! `Instrument ID`, not the exponents, not `Update Flags`, not `Action`, not
//! `Per-Instrument Seq`, not `Sequence Number`, not `Reset Count`, not the
//! datagram cap, and not one metric name.
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
//! reset. So the registry owns the table, this type holds the lowerings for the
//! life of the era, and the table is passed at each call.

use std::sync::Arc;

use dz_adapter_core::{Adapter, Event, EventSink};
use dz_edge_core::fixed_point::ScaleError;
use dz_edge_refdata::InstrumentDefinition;
use dz_publisher_lowering::{DepthLowering, Lowering, LoweringError, SourceId};
use dz_publisher_metrics::{EgressMessageType, PublisherMetrics, RefdataLoadErrorReason};
use dz_publisher_refdata::{Counts, Registry, StateStore};

use crate::clock::Clock;
use crate::guard::{ConsistencyGuard, Exit, IdleGuard, Inconsistency};
use crate::pipeline::FeedPipeline;

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
/// So this crate keeps the numbers, reports them, and does not invent a series.
/// **What is owed is a playbook addition:**
/// `dz_publisher_lowering_refusals_total{reason}` with these five values.
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
    fn record(&mut self, error: LoweringError) {
        match error {
            LoweringError::UnknownInstrument => self.unknown_instrument += 1,
            LoweringError::InexactContract { .. } => self.inexact_contract += 1,
            LoweringError::Scale { source, .. } => match source {
                ScaleError::TooPrecise { .. } => self.too_precise += 1,
                ScaleError::Malformed => self.malformed += 1,
                ScaleError::Overflow => self.overflow += 1,
            },
        }
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

/// Why a pulled snapshot did not frame.
///
/// Two cases and not one, because the caller does opposite things with them. An
/// adapter that is not ready is a slot to skip and come back to — one dormant
/// instrument rather than a restart loop — and it is deliberately not counted
/// as a lowering refusal, because an operator acts differently on *this
/// instrument's exponent is wrong* and *this instrument's book is still warming
/// up*. Collapsing the two would put the second in the bucket that sends
/// someone to look at reference data.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// The adapter could not answer: a book that has not bootstrapped, or a
    /// handle it does not hold.
    #[error(transparent)]
    Adapter(#[from] dz_adapter_core::AdapterError),

    /// The framing refused it: an unknown handle, or the first level whose
    /// price or quantity the instrument's exponents cannot state exactly.
    #[error(transparent)]
    Lowering(#[from] LoweringError),
}

/// One step of the teardown, in the order it happens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeardownStep {
    /// Nothing more will arrive from upstream.
    IngressStopped,
    /// `Valid` is 0 and nothing further is admitted.
    AdmissionsClosed,
    /// The last `ManifestSummary`, carrying `Valid = 0`, is on the refdata
    /// port.
    FinalManifestSent,
    /// `EndOfSession` is on the mktdata port.
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
    /// `Per-Instrument Seq`. Nothing in this build can put a depth message on
    /// the wire — see [`crate::StartupError::UnsupportedFeedSpec`] — and it is
    /// held anyway so that the one thing that must not happen when depth lands
    /// cannot: rebuilding it mid-era restarts a sequence a subscriber reads as a
    /// channel reset.
    depth: DepthLowering,
    feed: FeedPipeline,
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
    /// Monotonic. When the adapter's listings were last drained.
    last_poll_ns: Option<u64>,
    seeded: bool,
}

impl<S: StateStore, K: Clock + Clone> Publisher<S, K> {
    /// Compose a publisher over an opened reference-data registry and a built
    /// send path.
    ///
    /// Both are arguments and neither is opened here, which is the whole reason
    /// the composition is testable: the registry arrives having already claimed
    /// its state directory (or a memory store standing in for one), and the send
    /// path arrives holding fan-outs whose members may be recording sinks rather
    /// than sockets.
    #[must_use]
    pub fn new(
        metrics: Arc<PublisherMetrics>,
        refdata: Registry<S, K>,
        clock: K,
        source_id: SourceId,
        feed: FeedPipeline,
        idle_guard: std::time::Duration,
    ) -> Self {
        Self {
            metrics,
            refdata,
            clock,
            lowering: Lowering::new(source_id),
            depth: DepthLowering::new(source_id),
            feed,
            idle: IdleGuard::new(idle_guard),
            consistency: ConsistencyGuard::new(),
            definitions: Vec::new(),
            forwarded: Counts::default(),
            refusals: Refusals::default(),
            unroutable: 0,
            last_poll_ns: None,
            seeded: false,
        }
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
    /// instrument table is an argument to the lowering: [`dz_ingress_core::Driver`]
    /// holds the adapter mutably for as long as it is driving, so a publisher
    /// that also held one could never poll. Passing it in at the two call sites
    /// that need it — this one and [`Self::snapshot`] — is what lets the driver
    /// keep its borrow.
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
    #[must_use]
    pub fn tick(&mut self) -> Option<Exit> {
        let now_mono = dz_publisher_refdata::Clock::monotonic_ns(&self.clock);
        let now_unix = self.clock.unix_ns();

        if self.feed.heartbeat_due(now_mono) {
            let _ = self.feed.send_heartbeat(now_mono, now_unix);
        }

        self.refdata.definition_tick(&mut self.definitions);
        for definition in &self.definitions {
            let _ = self.feed.pack_definition(definition, now_unix);
        }

        if self.feed.manifest_due(now_mono) {
            let manifest = self.refdata.manifest();
            let _ = self.feed.send_manifest(&manifest, now_mono, now_unix);
        }

        // After the packing, so a definition tick that did not fill a datagram
        // still reaches the wire this tick rather than waiting for the one that
        // does. The refdata port is where the pacing already bounds the volume.
        let _ = self.feed.flush(now_mono, now_unix);

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

    /// Write one instrument's book to the snapshot port.
    ///
    /// **Not wired to a cadence, and the reason is a missing key rather than a
    /// missing implementation.** The snapshot is pulled because the cadence, the
    /// rotation across instruments and the framing are the runtime's while the
    /// book is the adapter's — and the design's configuration names
    /// `snapshot_port` and no snapshot interval, so there is nothing to pace
    /// against. Inventing a key here would be the one thing this crate is not
    /// allowed to do.
    ///
    /// It is unreachable for a second reason as well: the three snapshot message
    /// types have no `EgressMessageType` to be counted under, exactly as the
    /// depth messages do not. So this method takes the framer as far as the
    /// lowering goes and hands the result back rather than sending it, which
    /// leaves the hole one function wide and visible.
    ///
    /// # Errors
    ///
    /// [`SnapshotError`], which keeps the adapter's own refusal apart from the
    /// framing's. Nothing partial is returned either way: an incomplete
    /// snapshot is worse than none, because a subscriber cannot tell a refused
    /// level from a lost one.
    pub fn snapshot(
        &mut self,
        adapter: &dyn Adapter,
        instrument: dz_adapter_core::InstrumentRef,
        depth_bound: u32,
    ) -> Result<dz_publisher_lowering::Snapshot, SnapshotError> {
        let now_unix = self.clock.unix_ns();
        let anchor = self.feed.mktdata_sequence().unwrap_or(0);
        let mut framer = self.depth.open_snapshot(
            self.refdata.instruments(),
            instrument,
            anchor,
            now_unix,
            depth_bound,
        )?;
        // The adapter's refusal is carried through rather than folded into a
        // lowering refusal; see `SnapshotError`.
        adapter.snapshot(instrument, &mut framer)?;
        Ok(framer.finish()?)
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
    ///    channel, and therefore last: anything after it contradicts it.
    /// 5. **Both port roles flush.** A datagram left open holds a number that
    ///    has been assigned, and abandoning it is a gap for no reason.
    /// 6. **The exit is recorded**, so that `dz_publisher_exit_reason_total`
    ///    carries the reason before the last scrape.
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
        let _ = self.feed.send_manifest(&manifest, now_mono, now_unix);
        steps.push(TeardownStep::FinalManifestSent);

        let _ = self.feed.send_end_of_session(now_mono, now_unix);
        steps.push(TeardownStep::EndOfSessionSent);

        let _ = self.feed.flush(now_mono, now_unix);
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

    /// Events this build had no feed to carry.
    ///
    /// Depth events, and any event variant a later boundary release adds that
    /// this build does not know. **Refused before the lowering rather than
    /// after**, which is the load-bearing part: lowering a depth event stamps
    /// `Per-Instrument Seq`, and a number spent on a message that never reached
    /// the wire is a gap every subscriber reads as packet loss. See
    /// [`crate::StartupError::UnsupportedFeedSpec`] for why there is no feed.
    #[must_use]
    pub const fn unroutable(&self) -> u64 {
        self.unroutable
    }

    /// The reference-data owner, for a diagnostic and for a test.
    #[must_use]
    pub const fn refdata(&self) -> &Registry<S, K> {
        &self.refdata
    }

    /// The send path, for a diagnostic and for a test.
    #[must_use]
    pub const fn feed(&self) -> &FeedPipeline {
        &self.feed
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
        refdata.set_manifest_seq(
            self.feed.channel_id(),
            u64::from(self.refdata.manifest_seq()),
        );
        refdata.set_manifest_valid(self.feed.channel_id(), self.refdata.is_valid());
        self.metrics.book().set_instruments_published(published);
    }

    /// Read the two states the publisher cannot recover from in place.
    fn check_consistency(&mut self) {
        if let Some(sink) = self.feed.dark_transmitter() {
            let sink = sink.to_owned();
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

    fn event(&mut self, event: Event<'_>) {
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
                let lowered = lowering.lower_quote(
                    self.refdata.instruments(),
                    instrument,
                    source_ts_ns,
                    bid,
                    ask,
                );
                match lowered {
                    Ok(quote) => {
                        let started = std::time::Instant::now();
                        let sent = self.feed.send_quote(&quote, now_mono, now_unix);
                        self.metrics.latency().observe_encode_duration(
                            EgressMessageType::Quote,
                            started.elapsed().as_secs_f64(),
                        );
                        if sent.is_ok() {
                            self.published(now_mono, now_unix);
                        }
                    }
                    Err(error) => self.refusals.record(error),
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
                    Ok(trade) => {
                        let started = std::time::Instant::now();
                        let sent = self.feed.send_trade(&trade, now_mono, now_unix);
                        self.metrics.latency().observe_encode_duration(
                            EgressMessageType::Trade,
                            started.elapsed().as_secs_f64(),
                        );
                        if sent.is_ok() {
                            self.published(now_mono, now_unix);
                        }
                    }
                    Err(error) => self.refusals.record(error),
                }
            }

            // Depth, and any variant a later boundary release adds. Counted and
            // dropped **without being lowered**, because lowering one stamps
            // `Per-Instrument Seq` and a number spent on a message that never
            // reached the wire is a gap every subscriber reads as packet loss.
            // See `Publisher::unroutable`.
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
