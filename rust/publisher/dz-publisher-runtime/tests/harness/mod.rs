//! What the tests in this crate stand a publisher up against.
//!
//! Nothing here opens a socket, touches a filesystem, needs a privilege or
//! sleeps. That is not a convenience: every property worth asserting about this
//! crate is a property of a duration, an ordering or a byte, and a test that
//! provoked one by waiting would take as long as the policy it tests and still
//! prove nothing about it.
//!
//! - The datagram sinks are recorders, so what reaches "the wire" is a
//!   `Vec<Vec<u8>>` a test decodes with the codec.
//! - The state directory is `MemoryStore`, so a persisted `Instrument ID` and a
//!   write that fails are both stated rather than arranged.
//! - The clock is `ManualClock`, so a heartbeat interval, a manifest cadence and
//!   an idle-guard window are values a test sets.
//!
//! Every address here is from RFC 5737 or MCAST-TEST-NET, which is the rule this
//! repository checks because it is public.

#![allow(dead_code)]

use std::cell::{Cell, RefCell};
use std::net::Ipv4Addr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use dz_adapter_core::{
    Adapter, AssetClass, EventSink, InstrumentRef, InstrumentSpec, ListingSink, MarketModel,
    ParseError, Payload, PriceBound, Scalar, SettleType,
};
use dz_edge_core::{Datagram, PortRole};
use dz_edge_tob::MAGIC_TOB;
use dz_publisher_egress::{DatagramSink, EgressEndpoint, FailureScope, SinkError, Tee};
use dz_publisher_lowering::SourceId;
use dz_publisher_metrics::{PublisherMetrics, PublisherMetricsConfig};
use dz_publisher_refdata::{CycleSchedule, MemoryStore, Registry, RegistryConfig, SelectionPolicy};
use dz_publisher_runtime::{Feed, FeedPipeline, FeedSpec, ManualClock, Publisher};

/// The documentation-range source address every endpoint here sends from.
pub const SOURCE: Ipv4Addr = Ipv4Addr::new(203, 0, 113, 7);
/// MCAST-TEST-NET.
pub const GROUP: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 4);
pub const MKTDATA_PORT: u16 = 30001;
pub const REFDATA_PORT: u16 = 30002;
pub const CHANNEL_ID: u8 = 3;
/// In the assigned production range, which is what `SourceId` admits.
pub const SOURCE_ID: u16 = 41;

/// What a recording sink kept.
#[derive(Clone, Default)]
pub struct Recorder {
    datagrams: Rc<RefCell<Vec<Vec<u8>>>>,
}

impl Recorder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every datagram this recorder was handed, in order.
    #[must_use]
    pub fn datagrams(&self) -> Vec<Vec<u8>> {
        self.datagrams.borrow().clone()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.datagrams.borrow().len()
    }

    /// Every message in every datagram, as `(type_id, bytes)`, in order.
    ///
    /// Decoded with the codec's own walk and against the feed's own magic, so
    /// what a test asserts is what a subscriber would read rather than what the
    /// runtime believes it wrote.
    #[must_use]
    pub fn messages(&self) -> Vec<(u8, Vec<u8>)> {
        let mut out = Vec::new();
        for datagram in self.datagrams.borrow().iter() {
            let decoded =
                Datagram::decode(datagram, MAGIC_TOB).expect("the runtime composed a datagram");
            for message in decoded.messages() {
                out.push((message.type_id, message.bytes.to_vec()));
            }
        }
        out
    }

    /// The type ids of every message, in order.
    #[must_use]
    pub fn type_ids(&self) -> Vec<u8> {
        self.messages().into_iter().map(|(id, _)| id).collect()
    }

    /// The `(sequence_number, reset_count)` of each datagram, in order.
    #[must_use]
    pub fn headers(&self) -> Vec<(u64, u8)> {
        self.datagrams
            .borrow()
            .iter()
            .map(|datagram| {
                let decoded = Datagram::decode(datagram, MAGIC_TOB).expect("composed");
                (
                    decoded.header().sequence_number,
                    decoded.header().reset_count,
                )
            })
            .collect()
    }
}

/// A [`DatagramSink`] that keeps what it is given, and can be told to refuse.
pub struct RecordingSink {
    name: &'static str,
    scope: FailureScope,
    recorder: Recorder,
    refusing: Rc<Cell<bool>>,
}

impl RecordingSink {
    #[must_use]
    pub fn new(name: &'static str, scope: FailureScope) -> Self {
        Self {
            name,
            scope,
            recorder: Recorder::new(),
            refusing: Rc::new(Cell::new(false)),
        }
    }

    #[must_use]
    pub fn recorder(&self) -> Recorder {
        self.recorder.clone()
    }

    /// A handle that makes every later send fail non-transiently, which is what
    /// a socket whose route has gone does.
    #[must_use]
    pub fn refusal_switch(&self) -> Rc<Cell<bool>> {
        Rc::clone(&self.refusing)
    }
}

impl DatagramSink for RecordingSink {
    fn name(&self) -> &str {
        self.name
    }

    fn send(&mut self, datagram: &[u8]) -> Result<(), SinkError> {
        if self.refusing.get() {
            // Not transient, so the fan-out drops this member rather than
            // retrying it - which is what makes the process-scope failure
            // observable to the consistency guard.
            return Err(SinkError::NotRegistered);
        }
        self.recorder.datagrams.borrow_mut().push(datagram.to_vec());
        Ok(())
    }

    fn failure_scope(&self) -> FailureScope {
        self.scope
    }
}

/// A publisher composed over recorders, and the handles a test drives it with.
pub struct Harness {
    pub publisher: Publisher<MemoryStore, ManualClock>,
    pub clock: ManualClock,
    pub mktdata: Recorder,
    pub refdata: Recorder,
    pub mktdata_refusal: Rc<Cell<bool>>,
    pub metrics: Arc<PublisherMetrics>,
}

/// The `[[feed]]` a harness publishes, with every value stated.
#[must_use]
pub fn feed() -> Feed {
    Feed {
        spec: FeedSpec::TopOfBook,
        channel_id: CHANNEL_ID,
        source_id: SourceId::new(SOURCE_ID).expect("in the assigned range"),
        group: GROUP,
        mktdata_port: MKTDATA_PORT,
        refdata_port: REFDATA_PORT,
        snapshot_port: None,
        heartbeat_interval: Duration::from_secs(1),
        definition_cycle: Duration::from_secs(30),
        manifest_cadence: Duration::from_secs(1),
        idle_guard: Duration::from_secs(60),
    }
}

/// A publisher with the stated feed.
#[must_use]
pub fn harness(feed: Feed) -> Harness {
    harness_inner(feed, false)
}

/// The same, over a state directory whose writes fail.
///
/// Stated rather than arranged: a full or read-only directory is a behaviour a
/// real filesystem produces only if a test can build a broken one, and building
/// one costs privileges a suite should not need.
#[must_use]
pub fn harness_with_broken_writes(feed: Feed) -> Harness {
    harness_inner(feed, true)
}

fn harness_inner(feed: Feed, break_writes: bool) -> Harness {
    let clock = ManualClock::at_unix_ns(1_700_000_000_000_000_000);
    let metrics = Arc::new(PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "a-venue",
        source_id: feed.source_id.get(),
        port_roles: feed.spec.port_roles(),
        connections: &["upstream"],
        channel_ids: &[feed.channel_id],
        ingress_message_types: &["quote", "trade"],
    }));

    let mktdata_sink = RecordingSink::new("mktdata", FailureScope::Process);
    let mktdata = mktdata_sink.recorder();
    let mktdata_refusal = mktdata_sink.refusal_switch();
    let refdata_sink = RecordingSink::new("refdata", FailureScope::Process);
    let refdata = refdata_sink.recorder();

    let mut mktdata_tee = Tee::new(PortRole::Mktdata, Arc::clone(&metrics));
    mktdata_tee.add(Box::new(mktdata_sink));
    let mut refdata_tee = Tee::new(PortRole::Refdata, Arc::clone(&metrics));
    refdata_tee.add(Box::new(refdata_sink));

    let pipeline = FeedPipeline::new(
        &feed,
        Arc::clone(&metrics),
        // Era 2: not the first, so a test that reads the header sees a value
        // the store could only have got from a previous run.
        dz_edge_core::ResetCount(2),
        EgressEndpoint::new(PortRole::Mktdata, SOURCE, MKTDATA_PORT),
        mktdata_tee,
        EgressEndpoint::new(PortRole::Refdata, SOURCE, REFDATA_PORT),
        refdata_tee,
    );

    let store = MemoryStore::new();
    if break_writes {
        store.break_writes("the state directory is read-only");
    }
    let registry = Registry::open(
        RegistryConfig {
            source_id: feed.source_id,
            channel_id: feed.channel_id,
            selection: SelectionPolicy::new(8, 16, 8).expect("a coherent policy"),
            schedule: CycleSchedule::new(feed.definition_cycle, 1232, 1),
        },
        store,
        clock.clone(),
    )
    .expect("an empty memory store is a cold start");

    let publisher = Publisher::new(
        Arc::clone(&metrics),
        registry,
        clock.clone(),
        feed.source_id,
        pipeline,
        feed.idle_guard,
    );

    Harness {
        publisher,
        clock,
        mktdata,
        refdata,
        mktdata_refusal,
        metrics,
    }
}

/// One instrument, stated in full, so a test's expected bytes can be
/// transcribed from it.
#[must_use]
pub fn spec(symbol: &str) -> InstrumentSpec<'_> {
    InstrumentSpec {
        symbol,
        leg1: None,
        leg2: None,
        asset_class: AssetClass::CryptoSpot,
        // Two decimal places for price and three for quantity, so a test can
        // transcribe the scaled integers by hand and a transposition of the two
        // exponents would fail.
        price_exponent: -2,
        qty_exponent: -3,
        market_model: MarketModel::Clob,
        tick_size: Scalar::text("0.01"),
        lot_size: Scalar::text("0.001"),
        contract_value: None,
        quoted_per_contract: None,
        expiry_ns: None,
        settle_type: SettleType::NotApplicable,
        price_bound: PriceBound::NonNegative,
    }
}

/// An adapter that offers the symbols it was built with and emits whatever a
/// test hands it.
///
/// It parses no payloads, because nothing in this crate's tests is about a
/// venue's mapping: the mapping is `dz-adapter-core`'s boundary and
/// `dz-publisher-lowering`'s tests. What is exercised here is what happens to an
/// event *after* the boundary.
pub struct FakeAdapter {
    symbols: Vec<String>,
    handles: Vec<InstrumentRef>,
    declined: usize,
    withdrawing: Vec<InstrumentRef>,
    /// The book this adapter writes when the runtime pulls a snapshot: one
    /// entry per resting level, outward from the top of each side, in the
    /// venue's own decimal text.
    book: Vec<(dz_adapter_core::Side, String, String)>,
}

impl FakeAdapter {
    #[must_use]
    pub fn new(symbols: &[&str]) -> Self {
        Self {
            symbols: symbols.iter().map(|s| (*s).to_owned()).collect(),
            handles: Vec::new(),
            declined: 0,
            withdrawing: Vec::new(),
            book: Vec::new(),
        }
    }

    /// Give this adapter a book, so the runtime has something to pull.
    #[must_use]
    pub fn with_book(mut self, levels: &[(dz_adapter_core::Side, &str, &str)]) -> Self {
        self.book = levels
            .iter()
            .map(|(side, px, qty)| (*side, (*px).to_owned(), (*qty).to_owned()))
            .collect();
        self
    }

    /// Withdraw a handle on the next poll, which is how a venue says an
    /// instrument has reached the end of its life.
    pub fn withdraw(&mut self, handle: InstrumentRef) {
        self.withdrawing.push(handle);
    }

    /// The handles the runtime admitted, in offer order.
    #[must_use]
    pub fn handles(&self) -> &[InstrumentRef] {
        &self.handles
    }

    /// How many offers the selection policy declined.
    #[must_use]
    pub const fn declined(&self) -> usize {
        self.declined
    }
}

impl Adapter for FakeAdapter {
    fn message_types(&self) -> &[&'static str] {
        &["quote", "trade"]
    }

    fn poll_listings(&mut self, out: &mut dyn ListingSink) {
        for handle in self.withdrawing.drain(..) {
            out.delist(handle);
        }
        self.handles.clear();
        self.declined = 0;
        for symbol in &self.symbols {
            match out.list(&spec(symbol)) {
                Some(handle) => self.handles.push(handle),
                None => self.declined += 1,
            }
        }
    }

    fn on_payload(
        &mut self,
        _payload: &Payload<'_>,
        _out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        Ok(())
    }

    fn snapshot(
        &self,
        _instrument: InstrumentRef,
        out: &mut dyn dz_adapter_core::SnapshotSink,
    ) -> Result<(), dz_adapter_core::AdapterError> {
        if self.book.is_empty() {
            // The runtime skips this instrument's slot and comes back, which is
            // the difference between one dormant instrument and a restart loop.
            return Err(dz_adapter_core::AdapterError::NotReady {
                detail: "the book has not bootstrapped",
            });
        }
        for (side, px, qty) in &self.book {
            out.level(*side, Scalar::text(px), Scalar::text(qty), None);
        }
        Ok(())
    }
}

/// A configuration document, section by section, so a test can replace one
/// section and leave the rest valid.
///
/// Written as text rather than built from the typed sections on purpose: what
/// these tests are about is what an operator writes and what happens to it, and
/// a document assembled from already-typed values would skip the parse that is
/// the thing under test.
#[derive(Debug, Clone)]
pub struct Doc {
    pub root: String,
    pub egress: String,
    pub feed: String,
    pub refdata: String,
    pub metrics: String,
    pub ingress: String,
    pub adapter: String,
}

impl Default for Doc {
    /// Every section, valid, with the values the design's own configuration
    /// block states where it states one.
    fn default() -> Self {
        Self {
            root: "venue = \"a-venue\"\n".to_owned(),
            egress: "[egress]\nttl = 1\n".to_owned(),
            feed: format!(
                "[[feed]]\n\
                 spec = \"top-of-book\"\n\
                 enabled = true\n\
                 channel_id = {CHANNEL_ID}\n\
                 source_id = {SOURCE_ID}\n\
                 multicast_group = \"{GROUP}\"\n\
                 mktdata_port = {MKTDATA_PORT}\n\
                 refdata_port = {REFDATA_PORT}\n\
                 heartbeat_interval = \"1s\"\n\
                 definition_cycle = \"30s\"\n\
                 manifest_cadence = \"1s\"\n\
                 idle_guard = \"60s\"\n"
            ),
            refdata: "[refdata]\n\
                      state_dir = \"/var/lib/a-publisher\"\n\
                      [refdata.selection]\n\
                      bootstrap_top_n = 8\n\
                      max_published = 16\n\
                      warn_published_above = 8\n"
                .to_owned(),
            metrics: "[metrics]\nenabled = false\nlisten_addr = \"127.0.0.1:9100\"\n".to_owned(),
            // `uds`, which is the one kind this crate's dev-dependencies mark
            // as linked. See the manifest: a transport is linked when the crate
            // implementing it is in the build, this crate links none, and the
            // marker feature is turned on for the test build so that a valid
            // document can be resolved end to end.
            ingress: "[ingress]\nkind = \"uds\"\nconnect_timeout = \"5s\"\n".to_owned(),
            adapter: "[adapter]\nkind = \"a-venue\"\n".to_owned(),
        }
    }
}

impl Doc {
    /// A document every section of which is valid, for a test to break one
    /// thing in.
    ///
    /// Named rather than reached through `Default::default`, because every
    /// caller replaces one section immediately afterwards and that is the whole
    /// shape of these tests: what each one is about is the single difference
    /// from a document that works.
    #[must_use]
    pub fn valid() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn render(&self) -> String {
        [
            &self.root,
            &self.egress,
            &self.feed,
            &self.refdata,
            &self.metrics,
            &self.ingress,
            &self.adapter,
        ]
        .iter()
        .map(|section| section.as_str())
        .collect::<Vec<_>>()
        .join("\n")
    }
}

/// A two-sided quote, in the venue's own decimal text.
///
/// The values are the ones every expected byte in these tests is transcribed
/// from: at the instrument's price exponent of -2 and quantity exponent of -3,
/// `"100.25"` is `10_025` and `"2.500"` is `2_500`.
#[must_use]
pub fn quote(instrument: InstrumentRef, source_ts_ns: u64) -> dz_adapter_core::Event<'static> {
    use dz_adapter_core::{Event, SideUpdate};
    Event::Quote {
        instrument,
        source_ts_ns,
        bid: SideUpdate::Present {
            px: Scalar::text("100.25"),
            qty: Scalar::text("2.500"),
            source_count: None,
        },
        ask: SideUpdate::Present {
            px: Scalar::text("100.75"),
            qty: Scalar::text("1.250"),
            source_count: None,
        },
    }
}

/// A quote whose ask side has nothing resting on it.
#[must_use]
pub fn one_sided_quote(
    instrument: InstrumentRef,
    source_ts_ns: u64,
) -> dz_adapter_core::Event<'static> {
    use dz_adapter_core::{Event, SideUpdate};
    Event::Quote {
        instrument,
        source_ts_ns,
        bid: SideUpdate::Present {
            px: Scalar::text("100.25"),
            qty: Scalar::text("2.500"),
            source_count: None,
        },
        ask: SideUpdate::Gone,
    }
}

/// One execution.
#[must_use]
pub fn trade(instrument: InstrumentRef, source_ts_ns: u64) -> dz_adapter_core::Event<'static> {
    use dz_adapter_core::{Aggressor, Event, TradeFlags};
    Event::Trade {
        instrument,
        source_ts_ns,
        px: Scalar::text("100.50"),
        qty: Scalar::text("0.750"),
        aggressor: Aggressor::Buy,
        trade_id: Some(987_654),
        cumulative_volume: None,
        flags: TradeFlags::NONE,
    }
}

/// One depth event, which this build has no feed to carry.
#[must_use]
pub fn level(instrument: InstrumentRef, source_ts_ns: u64) -> dz_adapter_core::Event<'static> {
    use dz_adapter_core::{Event, Presence, Side};
    Event::Level {
        instrument,
        source_ts_ns,
        side: Side::Bid,
        px: Scalar::text("100.25"),
        qty: Scalar::text("2.500"),
        order_count: None,
        presence: Presence::New,
    }
}

/// A quote whose price the instrument's exponent cannot state exactly.
///
/// Three decimal places at a price exponent of -2. Refused rather than rounded,
/// which is the whole argument for the scaling being above the boundary: the
/// publisher that rounds takes the failure as zero and puts a real-looking bid
/// at nothing on the wire.
#[must_use]
pub fn too_precise_quote(
    instrument: InstrumentRef,
    source_ts_ns: u64,
) -> dz_adapter_core::Event<'static> {
    use dz_adapter_core::{Event, SideUpdate};
    Event::Quote {
        instrument,
        source_ts_ns,
        bid: SideUpdate::Present {
            px: Scalar::text("100.251"),
            qty: Scalar::text("2.500"),
            source_count: None,
        },
        ask: SideUpdate::Gone,
    }
}
