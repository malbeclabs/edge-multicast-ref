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
//! It owns the routing — which lowering an event goes through, which shard's
//! feed carries the result, and which port role it is pushed onto — the
//! cadences, the guards, and the order of the teardown. It owns none of the things the
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
//! policy requires `0x04` to be **byte-for-byte identical** across the feeds in
//! the family a venue publishes, and in one existing publisher that obligation
//! is held by a doc comment across two separate encoder implementations,
//! checked by hand.
//! `dz-publisher-lowering` made it one function; this makes it one *value*. The
//! trade is lowered once and the same `Trade` is handed to both send paths, so
//! the two are not two things that agree — they are one thing, and there is no
//! second call site to drift.
//!
//! An event no enabled feed carries — a `Quote` on a publisher that emits only
//! depth, or a variant a later boundary release adds — is counted and dropped
//! **before** it is lowered. See [`Publisher::unroutable`].
//!
//! # Which shard, and where that answer comes from
//!
//! Every row above is a row about one shard's feeds. The shard is the
//! instrument's, recorded by the reference-data owner when the venue admitted
//! it, and it is resolved once per event into an index into [`Feeds`] — never
//! compared as a name on the datagram path, and never taken from anything the
//! adapter states per message. An adapter that could name a shard per event
//! would be an adapter deciding which channel a message leaves on, and the
//! whole boundary is built on it deciding none of that.
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
    Adapter, AdapterError, DepthBound, Desync, Event, EventSink, InstrumentRef, VenueTimestampKind,
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
use crate::pipeline::{DroppedSink, FeedPipeline};
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

/// One shard's send paths: a channel instance per feed specification it carries.
///
/// A shard is a partition of the instrument set and a channel instance is the
/// unit of sequencing, so a shard carrying both specifications is two channel
/// instances over one published set. This type is that pairing, and it is a
/// type rather than a position in two collections because the pairing is what
/// routing depends on: an instrument's quotes and its levels have to leave by
/// the channels of the *same* shard, and a shape that holds them apart can be
/// built with them mismatched.
///
/// # Constructed with at least one send path, so the name is always readable
///
/// Every shard reaching this type came from at least one enabled `[[feed]]`
/// block, because the shard set is derived from those blocks.
/// [`new`](Self::new) is the only way in and it refuses the empty pair, so
/// [`name`](Self::name) is total.
///
/// What is *not* enforced here is that every shard carries the same
/// specifications. `Config::resolve` refuses a document where one does and
/// another does not — see [`crate::StartupError::ShardSpecsDisagree`] — and
/// that is where the policy belongs. Were it ever relaxed, the consequence
/// here is a message counted as unroutable rather than a message on the wrong
/// channel, because the question is asked of the instrument's own shard.
///
/// Not an enum of the three combinations, which is the other way to make the
/// empty pair unrepresentable: the variant holding both feeds is a
/// [`FeedPipeline`] larger than the ones holding one, so every entry in a
/// publisher's vector would be sized for it. A document carries the same
/// specifications on every shard, so that padding would be paid on all of them
/// or none.
pub struct ShardFeeds {
    /// The shard these send paths carry, as the reference-data owner keys its
    /// published sets on.
    ///
    /// Derived in [`new`](Self::new) from a send path rather than authored
    /// beside them, so it cannot name a shard other than the one whose
    /// channels the messages leave by. That is the distinction from a name held
    /// in a second list: a list can fall out of step with the positions it
    /// names, and this cannot fall out of step with anything.
    name: String,
    top_of_book: Option<FeedPipeline<TopOfBook>>,
    market_by_price: Option<FeedPipeline<MarketByPrice>>,
}

impl ShardFeeds {
    /// One shard's send paths, from the blocks that composed for it.
    ///
    /// `None` when neither specification composed, which no resolved document
    /// produces: the shard set is the distinct shards *of the enabled blocks*,
    /// so a shard with neither is a shard nothing named. It is returned rather
    /// than asserted because the caller has somewhere honest to put it — see
    /// [`crate::StartupError::ShardWithNoFeed`] — and because dropping it
    /// silently would shift every later shard's index one off the registry's.
    #[must_use]
    pub fn new(
        top_of_book: Option<FeedPipeline<TopOfBook>>,
        market_by_price: Option<FeedPipeline<MarketByPrice>>,
    ) -> Option<Self> {
        let name = top_of_book
            .as_ref()
            .map(FeedPipeline::shard)
            .or_else(|| market_by_price.as_ref().map(FeedPipeline::shard))?
            .to_owned();
        Some(Self {
            name,
            top_of_book,
            market_by_price,
        })
    }

    /// The shard these send paths carry.
    ///
    /// Both send paths carry the same name: they are composed from one shard's
    /// blocks, and two spellings of one shard are refused at load.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// This shard's top-of-book send path, or `None` if it carries no
    /// top-of-book block.
    #[must_use]
    pub const fn top_of_book(&self) -> Option<&FeedPipeline<TopOfBook>> {
        self.top_of_book.as_ref()
    }

    /// This shard's top-of-book send path, to send on.
    pub const fn top_of_book_mut(&mut self) -> Option<&mut FeedPipeline<TopOfBook>> {
        self.top_of_book.as_mut()
    }

    /// This shard's market-by-price send path, or `None` if it carries no depth
    /// block.
    #[must_use]
    pub const fn market_by_price(&self) -> Option<&FeedPipeline<MarketByPrice>> {
        self.market_by_price.as_ref()
    }

    /// This shard's market-by-price send path, to send on.
    pub const fn market_by_price_mut(&mut self) -> Option<&mut FeedPipeline<MarketByPrice>> {
        self.market_by_price.as_mut()
    }

    /// The `Channel ID`s this shard publishes on: one per block it carries.
    ///
    /// A shard carrying both specifications is two channel instances over one
    /// published set, so a gauge keyed on `Channel ID` and fed from a shard's
    /// reference data has two numbers to write and not one.
    pub fn channel_ids(&self) -> impl Iterator<Item = u8> + '_ {
        self.top_of_book()
            .map(FeedPipeline::channel_id)
            .into_iter()
            .chain(self.market_by_price().map(FeedPipeline::channel_id))
    }

    /// A dropped fan-out member whose failure darkens this shard, on either
    /// feed and any port role.
    #[must_use]
    pub fn dark_transmitter(&self) -> Option<&str> {
        self.top_of_book()
            .and_then(FeedPipeline::dark_transmitter)
            .or_else(|| {
                self.market_by_price()
                    .and_then(FeedPipeline::dark_transmitter)
            })
    }

    /// Every fan-out member of this shard, on either feed and any port role,
    /// that is no longer being fed. See [`FeedPipeline::dropped_sinks`].
    #[must_use]
    pub fn dropped_sinks(&self) -> Vec<DroppedSink<'_>> {
        let mut dropped: Vec<DroppedSink<'_>> = Vec::new();
        if let Some(pipeline) = self.top_of_book() {
            dropped.extend(pipeline.dropped_sinks());
        }
        if let Some(pipeline) = self.market_by_price() {
            dropped.extend(pipeline.dropped_sinks());
        }
        dropped
    }

    /// Everything this shard's send paths owe a tick, given its reference data.
    ///
    /// The definitions and the manifest arrive as arguments because a shard's
    /// pacer is drained once per tick however many of that shard's feeds are
    /// enabled: draining per feed would ask for the lap's debt once for each
    /// and emit that many times as much of the set, which is the burst the
    /// pacer exists to prevent arriving through the caller.
    pub fn tick(
        &mut self,
        definitions: &[InstrumentDefinition],
        manifest: &ManifestSummary,
        now_mono_ns: u64,
        now_unix_ns: u64,
    ) {
        if let Some(pipeline) = self.top_of_book_mut() {
            tick_pipeline(pipeline, definitions, manifest, now_mono_ns, now_unix_ns);
        }
        if let Some(pipeline) = self.market_by_price_mut() {
            tick_pipeline(pipeline, definitions, manifest, now_mono_ns, now_unix_ns);
        }
    }
}

#[cfg(test)]
mod shard_feeds_tests {
    use super::ShardFeeds;

    /// The property the rest of this module is written against: after
    /// construction there is always a send path to read a name off, so
    /// [`ShardFeeds::name`] is total and no caller has to handle a shard that
    /// carries nothing.
    ///
    /// Tested directly because it cannot be reached through a document —
    /// `Config::shards()` is the distinct shards of the enabled blocks — and an
    /// invariant no document can violate is one a later refactor can, which is
    /// what `StartupError::ShardWithNoFeed` exists for.
    #[test]
    fn a_shard_with_neither_specification_is_not_a_shard() {
        assert!(ShardFeeds::new(None, None).is_none());
    }
}

/// The send paths this publisher operates: one entry per shard, in the order
/// the document states them.
///
/// # The position is the routing's, and the name is the registry's
///
/// The index is a shard's position in that order, resolved once per event from
/// the instrument's admitted shard and never from a name — a string compared
/// per message would put the size of the shard set on the datagram path. It is
/// an index into [`Registry`]'s shard list as well, built from the same
/// `Config::shards()`, and nothing below this type re-checks that the entry at
/// an index is the shard the caller meant. What keeps them in step is that
/// there is one list: the two specifications of a shard travel together in
/// [`ShardFeeds`], so a document interleaving its blocks cannot land them in
/// two different orders.
///
/// # Why the specifications are not one collection
///
/// [`FeedPipeline`] is generic over the wire feed — `Magic` belongs to the feed
/// — so the two specifications are different types, and a collection *of feeds*
/// would need dynamic dispatch on the datagram path to buy nothing. A shard
/// names both of its own, each monomorphized, and a vector of shards is one
/// indexed load.
///
/// A publisher with no feed at all is refused before this type is built; see
/// [`crate::StartupError::NoEnabledFeed`].
#[derive(Default)]
pub struct Feeds {
    shards: Vec<ShardFeeds>,
}

impl Feeds {
    /// Append one shard's send paths, at the next shard index.
    ///
    /// Called once per shard, in the order the document states them, because
    /// that order is the index every event resolves against.
    pub fn push(&mut self, shard: ShardFeeds) {
        self.shards.push(shard);
    }

    /// How many shards this publisher carries.
    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// One shard's send paths. `None` past the last shard.
    #[must_use]
    pub fn shard(&self, shard: usize) -> Option<&ShardFeeds> {
        self.shards.get(shard)
    }

    /// One shard's send paths, to send on.
    pub fn shard_mut(&mut self, shard: usize) -> Option<&mut ShardFeeds> {
        self.shards.get_mut(shard)
    }

    /// Every shard, in document order.
    pub fn shards(&self) -> impl Iterator<Item = &ShardFeeds> + '_ {
        self.shards.iter()
    }

    /// Every shard, in document order, to send on.
    pub fn shards_mut(&mut self) -> impl Iterator<Item = &mut ShardFeeds> + '_ {
        self.shards.iter_mut()
    }

    /// The name of the shard at an index, as the reference-data owner keys its
    /// published sets on. `None` past the last shard.
    #[must_use]
    pub fn shard_name(&self, shard: usize) -> Option<&str> {
        self.shard(shard).map(ShardFeeds::name)
    }

    /// One shard's top-of-book send path, or `None` if this publisher carries
    /// no top-of-book feed for it.
    #[must_use]
    pub fn top_of_book_on(&self, shard: usize) -> Option<&FeedPipeline<TopOfBook>> {
        self.shard(shard).and_then(ShardFeeds::top_of_book)
    }

    /// One shard's top-of-book send path, to send on.
    pub fn top_of_book_on_mut(&mut self, shard: usize) -> Option<&mut FeedPipeline<TopOfBook>> {
        self.shard_mut(shard).and_then(ShardFeeds::top_of_book_mut)
    }

    /// One shard's market-by-price send path, or `None` if this publisher
    /// carries no depth feed for it.
    #[must_use]
    pub fn market_by_price_on(&self, shard: usize) -> Option<&FeedPipeline<MarketByPrice>> {
        self.shard(shard).and_then(ShardFeeds::market_by_price)
    }

    /// One shard's market-by-price send path, to send on.
    pub fn market_by_price_on_mut(
        &mut self,
        shard: usize,
    ) -> Option<&mut FeedPipeline<MarketByPrice>> {
        self.shard_mut(shard)
            .and_then(ShardFeeds::market_by_price_mut)
    }

    /// Whether any shard carries a top-of-book feed.
    ///
    /// The question a publisher answers before it considers a handle at all: a
    /// publisher that emits only depth carries a `Quote` for no instrument.
    #[must_use]
    pub fn carries_top_of_book(&self) -> bool {
        self.shards().any(|shard| shard.top_of_book().is_some())
    }

    /// Whether any shard carries a market-by-price feed.
    #[must_use]
    pub fn carries_market_by_price(&self) -> bool {
        self.shards().any(|shard| shard.market_by_price().is_some())
    }

    /// Every enabled feed's `Channel ID`, across every shard.
    ///
    /// Sorted and deduplicated because it is read to pre-create series and to
    /// iterate channels, not to count blocks. Two blocks sharing a number are
    /// refused at load — see [`crate::StartupError::DuplicateChannelId`] — so
    /// the deduplication removes nothing a document can produce.
    #[must_use]
    pub fn channel_ids(&self) -> Vec<u8> {
        let mut ids: Vec<u8> = self.shards().flat_map(ShardFeeds::channel_ids).collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// The `Channel ID`s one shard publishes on. See
    /// [`ShardFeeds::channel_ids`].
    pub fn channel_ids_on(&self, shard: usize) -> impl Iterator<Item = u8> + '_ {
        self.shard(shard)
            .into_iter()
            .flat_map(ShardFeeds::channel_ids)
    }

    /// A dropped fan-out member whose failure darkens this publisher, on any
    /// shard, any feed and any port role.
    #[must_use]
    pub fn dark_transmitter(&self) -> Option<String> {
        self.shards()
            .find_map(ShardFeeds::dark_transmitter)
            .map(str::to_owned)
    }

    /// Every fan-out member, on any shard, any feed and any port role, that is
    /// no longer being fed. See [`FeedPipeline::dropped_sinks`].
    #[must_use]
    pub fn dropped_sinks(&self) -> Vec<DroppedSink<'_>> {
        self.shards().flat_map(ShardFeeds::dropped_sinks).collect()
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

/// Snapshots that were asked for and did not go out.
///
/// # Why these are counted at all
///
/// A refused snapshot is the one failure on this path that is invisible in
/// every other number. The datagram counters keep moving, the sequence series
/// stays dense, the heartbeat is on time, and the aggregate snapshot rate looks
/// normal because the *other* instruments are being served — while one
/// instrument's book never bootstraps and no subscriber can build it. The
/// refusal reached a log line and nothing else, and a log line 100 times a
/// second is not a record: it is what makes an operator turn the log off.
///
/// # Two counts, because the caller does two different things
///
/// [`AdapterError::NotReady`] is expected: the rotation has already stepped
/// past the instrument, it comes back on the next lap, and one dormant
/// instrument is not a feed whose snapshots stop. It is counted rather than
/// discarded precisely because *never ready* and *not ready yet* are the same
/// line — the difference is only visible as a number that stops growing or does
/// not.
///
/// Everything else is a refusal somebody has to act on: an exponent that cannot
/// state a level exactly, a handle the adapter does not hold, a socket that
/// refused the framed snapshot.
///
/// There is no metric family for either. The normative `dz_publisher_*` set is
/// closed by a governing playbook and has no home for a *snapshot that was not
/// taken* — `egress_errors_total` is about a datagram that failed to leave, and
/// a snapshot refused before framing never became one. Same answer as
/// [`Refusals`], for the same reason, and the exit report prints both.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SnapshotRefusals {
    /// The adapter's book had not bootstrapped, or its session had not
    /// authenticated: [`AdapterError::NotReady`].
    pub not_ready: u64,
    /// Every other refusal, on the framing, the adapter or the socket.
    pub refused: u64,
}

impl SnapshotRefusals {
    /// Count one refusal.
    ///
    /// An exhaustive match rather than a fallback branch, so that a cause
    /// added to [`SnapshotError`] has to be classified here instead of landing
    /// in whichever bucket a `_` named.
    fn record(&mut self, error: &SnapshotError) {
        match error {
            SnapshotError::Adapter(AdapterError::NotReady { .. }) => self.not_ready += 1,
            SnapshotError::Adapter(
                AdapterError::UnknownInstrument | AdapterError::Internal { .. },
            )
            | SnapshotError::NoDepthFeed
            | SnapshotError::Lowering(_)
            | SnapshotError::Egress(_) => self.refused += 1,
        }
    }

    #[must_use]
    pub const fn total(&self) -> u64 {
        self.not_ready + self.refused
    }
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
    /// fallback branch named.
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
    /// The last `ManifestSummary`, carrying `Valid = 0`, is on every channel
    /// instance's refdata port, each describing its own shard's published set.
    FinalManifestSent,
    /// `EndOfSession` is on every channel instance's mktdata port.
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
    /// Snapshots asked for and not sent. See [`SnapshotRefusals`].
    snapshot_refusals: SnapshotRefusals,
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
    /// One periodic snapshot rotation per shard, indexed as
    /// [`Feeds`] is. `None` in a slot is a shard whose depth block configures
    /// no cycle, which is a shard emitting recovery snapshots and no others.
    ///
    /// **One rotation per shard rather than one per publisher**, because
    /// `[[feed]] snapshot_cycle` is one full pass over the published set of the
    /// channel it is configured on. A single rotation shared across shards
    /// would give each shard's instruments a fraction of the configured rate —
    /// one part in the number of shards — so a subscriber joining mid-session
    /// on any one channel waits that many cycles for its book, while the key
    /// still reads as honoured.
    snapshots: Vec<Option<SnapshotRotation>>,
    /// Which shard the search for a due snapshot starts at. See
    /// [`Publisher::due_snapshot`].
    snapshot_cursor: usize,
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
    /// **One registry serves every channel instance**, and that is right rather
    /// than a simplification. `Instrument ID` identity is the one thing there
    /// can only be one of, so two registries would be two ID spaces and a
    /// published ID would resolve to two different definitions. What is per
    /// shard is inside that one registry — the published membership, the
    /// `Manifest Seq` that describes it and the pacer that emits it — because
    /// those are properties of a channel and the identity is a property of the
    /// process.
    #[must_use]
    pub fn new(
        metrics: Arc<PublisherMetrics>,
        refdata: Registry<S, K>,
        clock: K,
        source_id: SourceId,
        feeds: Feeds,
        idle_guard: std::time::Duration,
    ) -> Self {
        // One per shard, in the send paths' own order, so that the rotation at
        // an index and the shard at that index are the same channel instance. A
        // shard that carries no depth block, or one whose block states no
        // cycle, holds `None` rather than being left out: leaving it out would
        // shift every later shard's rotation onto another shard's instruments.
        let snapshots = feeds
            .shards()
            .map(|shard| {
                shard
                    .market_by_price()
                    .and_then(FeedPipeline::snapshot_cycle)
                    .map(SnapshotRotation::new)
            })
            .collect();
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
            snapshot_refusals: SnapshotRefusals::default(),
            unroutable: 0,
            owed: Vec::new(),
            payload_recv_ts_ns: None,
            venue_timestamp_kind: None,
            last_poll_ns: None,
            snapshots,
            snapshot_cursor: 0,
            seeded: false,
        }
    }

    /// The instruments owing a recovery snapshot, each with the anchor its
    /// reset promised, drained.
    ///
    /// **The anchor travels with the debt, and that is not an optimisation.**
    /// The specification obliges a snapshot with `Anchor Seq` *equal to* the
    /// value the reset named — not equal to wherever the feed has reached by
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
    /// The definition tick is drained **once per shard** and packed onto every
    /// feed of that shard. Per shard is the unit, and both neighbouring
    /// choices are wrong in opposite directions: draining per feed would ask
    /// that shard's pacer for the lap's debt once for each feed carrying it and
    /// emit that many times as much of the set per tick, which is the burst the
    /// pacer exists to prevent arriving through the caller, while draining once
    /// for the process would share one lap's debt out over every channel and
    /// leave each one's definitions arriving at a fraction of the cycle it
    /// configured.
    #[must_use]
    pub fn tick(&mut self) -> Option<Exit> {
        let now_mono = dz_publisher_refdata::Clock::monotonic_ns(&self.clock);
        let now_unix = self.clock.unix_ns();

        for index in 0..self.feeds.shard_count() {
            let Some(name) = self.feeds.shard_name(index) else {
                continue;
            };
            self.refdata.definition_tick(name, &mut self.definitions);
            // A shard the reference-data owner has no published set for cannot
            // come out of one document, because the send paths and the registry
            // are configured from the same blocks. If it ever did, a manifest
            // composed from another shard's set would be a false statement
            // about this channel's `Instrument Count`, so nothing is the honest
            // answer.
            let Some(manifest) = self.refdata.manifest(name) else {
                continue;
            };
            if let Some(shard) = self.feeds.shard_mut(index) {
                shard.tick(&self.definitions, &manifest, now_mono, now_unix);
            }
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
        // The point in the live feed this book state is true as of, which is
        // what tells a subscriber which live messages to apply after it and
        // which to discard — read off **this instrument's own shard**. Another
        // shard's market-by-price send path is another channel instance's
        // sequence series, and the subscriber compares the anchor against the
        // numbers it has seen on its own channel: anchored from the wrong shard
        // it is a wrong answer rather than a late one.
        let shard = match self.depth_shard(instrument) {
            Ok(shard) => shard,
            Err(error) => {
                self.snapshot_refusals.record(&error);
                return Err(error);
            }
        };
        let anchor = shard
            .and_then(|shard| self.feeds.market_by_price_on(shard))
            .map_or(0, |pipeline| pipeline.mktdata_sequence().unwrap_or(0));
        self.snapshot_anchored_at(adapter, instrument, anchor)
    }

    /// The recovery snapshot an [`InstrumentReset`] obliged, at the anchor that
    /// reset promised.
    ///
    /// **Not the live sequence.** A subscriber records the reset's anchor as
    /// the minimum `Anchor Seq` it will accept for that instrument, so a
    /// snapshot captured later and anchored where the feed has since reached
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
        // **Counted here rather than by the caller**, because both entry points
        // reach this one and a count a caller has to remember to take is a count
        // that is missing from whichever path was added last. See
        // [`SnapshotRefusals`].
        let outcome = self.capture_and_send(adapter, instrument, anchor);
        if let Err(error) = &outcome {
            self.snapshot_refusals.record(error);
        }
        outcome
    }

    /// The capture, the framing and the send, with the counting left to
    /// [`snapshot_anchored_at`](Self::snapshot_anchored_at).
    fn capture_and_send(
        &mut self,
        adapter: &dyn Adapter,
        instrument: InstrumentRef,
        anchor: u64,
    ) -> Result<Snapshot, SnapshotError> {
        let now_unix = self.clock.unix_ns();
        let shard = self.depth_shard(instrument)?;
        let mut framer =
            self.depth
                .open_snapshot(self.refdata.instruments(), instrument, anchor, now_unix)?;
        // The adapter's refusal is carried through rather than folded into a
        // lowering refusal; see `SnapshotError`. What it returns instead of a
        // refusal is the depth the levels it just wrote were drawn from, which
        // is the one field of the framing that is the venue's.
        let depth_bound: DepthBound = adapter.snapshot(instrument, &mut framer)?;
        let snapshot = framer.finish(depth_bound)?;
        shard
            .and_then(|shard| self.feeds.market_by_price_on_mut(shard))
            .expect("the framing resolved this instrument, so it is on a shard with a depth feed")
            .send_snapshot(&snapshot, now_unix)?;
        Ok(snapshot)
    }

    /// Which shard's depth send path serves an instrument's snapshots.
    ///
    /// `Ok(None)` is a handle the published set does not hold, and it is
    /// deliberately not refused here: the framing below refuses it as an
    /// unknown instrument, which is the reason an operator acts on. What is
    /// refused here is a publisher whose blocks for that instrument's shard
    /// carry no depth feed — that shard has no book to serve and no port to
    /// serve it on, whatever the other shards carry, and it is
    /// [`SnapshotError::NoDepthFeed`] per shard for the same reason it was ever
    /// per publisher.
    ///
    /// # Errors
    ///
    /// [`SnapshotError::NoDepthFeed`], and nothing else.
    fn depth_shard(&self, instrument: InstrumentRef) -> Result<Option<usize>, SnapshotError> {
        let shard = self.shard_of(instrument);
        if self.market_by_price_carries(shard) {
            Ok(shard)
        } else {
            Err(SnapshotError::NoDepthFeed)
        }
    }

    /// The next periodic snapshot the rotation owes, taken if one is due.
    ///
    /// `None` covers three states that are all *nothing to do now*: no shard
    /// configured a `[[feed]] snapshot_cycle`, no shard's derived tick has
    /// elapsed, or every published set is empty. `Some` carries the outcome of
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
        let due = self.due_snapshot(now_mono)?;
        Some(self.snapshot(adapter, due))
    }

    /// The next instrument any shard's rotation owes, at most one per call.
    ///
    /// The shards are searched from where the last due instrument was found
    /// rather than from the first every time. One call serves one instrument —
    /// a snapshot is a group of datagrams, so that is the unit of progress —
    /// and a search that always started at the first shard would let it take
    /// every call it is due for while the last shard's rotation waited behind
    /// it.
    fn due_snapshot(&mut self, now_mono_ns: u64) -> Option<InstrumentRef> {
        let shards = self.snapshots.len();
        if shards == 0 {
            return None;
        }
        for offset in 0..shards {
            let shard = (self.snapshot_cursor + offset) % shards;
            let Some(name) = self.feeds.shard_name(shard) else {
                continue;
            };
            // **This shard's published count, never the process's.** The tick
            // is the cycle divided by the set one pass has to cover, and that
            // set is the channel's; divided by every channel's instruments,
            // each shard is paced as slowly as there are shards while
            // `[[feed]] snapshot_cycle` still reads as honoured.
            let Some(published) = self.refdata.published_on(name) else {
                continue;
            };
            let Some(rotation) = self.snapshots[shard].as_mut() else {
                continue;
            };
            let due = rotation.due(
                now_mono_ns,
                self.refdata.instruments(),
                published,
                // The slots are shared across shards, so a rotation that
                // walked all of them would spend most of its ticks on
                // instruments another channel serves and lap its own set as
                // many times too slowly as there are shards.
                |instrument| self.refdata.shard_of(instrument) == Some(shard),
            );
            if let Some(instrument) = due {
                self.snapshot_cursor = (shard + 1) % shards;
                return Some(instrument);
            }
        }
        None
    }

    /// One full pass of a snapshot rotation, if this publisher runs one.
    ///
    /// For a log line at startup: a depth feed with no cadence is a feed no
    /// joining subscriber can bootstrap from, and that is worth stating rather
    /// than leaving to be inferred from silence. The first shard that runs one
    /// answers for the publisher, because what the line says is that a rotation
    /// runs at all — every shard's cycle is its own block's key, and a shard
    /// that configures none is the case this is `None` for.
    #[must_use]
    pub fn snapshot_cycle(&self) -> Option<std::time::Duration> {
        self.snapshots
            .iter()
            .flatten()
            .map(SnapshotRotation::cycle)
            .next()
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
    ///    the truth. One per channel instance, each carrying **its own** shard's
    ///    published set, because that is the set the subscribers on that channel
    ///    have been collecting definitions against.
    /// 4. **`EndOfSession` goes out on mktdata.** The terminal statement for the
    ///    channel, and therefore last: anything after it contradicts it. On
    ///    every channel instance, because every one of them is ending.
    ///
    /// The order is a **per channel instance** order — every message in it is a
    /// statement about one channel — and it is held here by phase rather than
    /// by instance: every manifest precedes every `EndOfSession`, which implies
    /// the constraint within each instance and costs nothing to read.
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

        for index in 0..self.feeds.shard_count() {
            let Some(name) = self.feeds.shard_name(index) else {
                continue;
            };
            let Some(manifest) = self.refdata.manifest(name) else {
                continue;
            };
            let Some(shard) = self.feeds.shard_mut(index) else {
                continue;
            };
            if let Some(pipeline) = shard.top_of_book_mut() {
                let _ = pipeline.send_manifest(&manifest, now_mono, now_unix);
            }
            if let Some(pipeline) = shard.market_by_price_mut() {
                let _ = pipeline.send_manifest(&manifest, now_mono, now_unix);
            }
        }
        steps.push(TeardownStep::FinalManifestSent);

        // Every channel instance of every shard, one step at a time rather than
        // one shard at a time: the steps are ordered against each other across
        // the whole publisher, so an `EndOfSession` must not be sent on one
        // shard while another shard's manifest is still unsent.
        for shard in self.feeds.shards_mut() {
            if let Some(pipeline) = shard.top_of_book_mut() {
                let _ = pipeline.send_end_of_session(now_mono, now_unix);
            }
            if let Some(pipeline) = shard.market_by_price_mut() {
                let _ = pipeline.send_end_of_session(now_mono, now_unix);
            }
        }
        steps.push(TeardownStep::EndOfSessionSent);

        for shard in self.feeds.shards_mut() {
            if let Some(pipeline) = shard.top_of_book_mut() {
                let _ = pipeline.flush(now_unix);
            }
            if let Some(pipeline) = shard.market_by_price_mut() {
                let _ = pipeline.flush(now_unix);
            }
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

    /// Snapshots asked for and not sent. See [`SnapshotRefusals`].
    #[must_use]
    pub const fn snapshot_refusals(&self) -> SnapshotRefusals {
        self.snapshot_refusals
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

    /// Every fan-out member that is no longer being fed, on any feed and any
    /// port role.
    ///
    /// **Read between ticks, because a send cannot report it.** A member whose
    /// failure is not transient is counted and dropped and the send still
    /// succeeds — the only correct outcome, since propagating it would put a
    /// decision about `Sequence Number` in the hands of one auxiliary
    /// consumer's broken socket. The cost is that the fan-out goes quiet
    /// silently, and this is the reading that ends the silence: the runtime
    /// names each entry the first time it sees it, and the exit report names
    /// them all. See [`FeedPipeline::dropped_sinks`](crate::FeedPipeline::dropped_sinks).
    #[must_use]
    pub fn dropped_sinks(&self) -> Vec<DroppedSink<'_>> {
        self.feeds.dropped_sinks()
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

    /// Which shard's send paths carry an instrument's messages.
    ///
    /// Resolved once per event, beside the instrument lookup the lowering
    /// performs anyway, and from the shard the venue admitted the instrument to
    /// rather than from anything it states per message: routing an adapter
    /// could influence per event is routing an adapter decides, and the whole
    /// boundary is built on it deciding none of this.
    ///
    /// `None` is a handle the published set does not hold — forged, or
    /// outliving its instrument's withdrawal. It is deliberately not refused
    /// where it is resolved: the instrument table and the published set are
    /// cleared together, so the lowering refuses the same handle as an unknown
    /// instrument, and that is the reason an operator can act on. Counting it
    /// as unroutable instead would say the message had nowhere to go rather
    /// than that the handle was not this publisher's.
    fn shard_of(&self, instrument: InstrumentRef) -> Option<usize> {
        self.refdata.shard_of(instrument)
    }

    /// Whether a message for an instrument on this shard reaches a top-of-book
    /// feed.
    ///
    /// Two questions, asked in this order. A publisher that emits no
    /// top-of-book feed at all carries the message for no instrument, and that
    /// is settled before any handle is considered. A publisher that emits one
    /// carries this message only if *this instrument's* shard has a block for
    /// it — an instrument admitted to a shard with no top-of-book block has
    /// quotes that reach no wire, and they are counted rather than dropped
    /// silently.
    fn top_of_book_carries(&self, shard: Option<usize>) -> bool {
        self.feeds.carries_top_of_book()
            && shard.is_none_or(|shard| self.feeds.top_of_book_on(shard).is_some())
    }

    /// Whether a message for an instrument on this shard reaches a
    /// market-by-price feed. As [`top_of_book_carries`](Self::top_of_book_carries).
    fn market_by_price_carries(&self, shard: Option<usize>) -> bool {
        self.feeds.carries_market_by_price()
            && shard.is_none_or(|shard| self.feeds.market_by_price_on(shard).is_some())
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
        // Per `Channel ID`, and read from the shard that owns that channel.
        // `Manifest Seq` increments when the published set changes *on this
        // channel*, so one process-wide value written to every series would
        // move a quiet channel's gauge for an admission its subscribers cannot
        // see — and an operator watching the gauge that mirrors the wire would
        // be looking at a number no datagram carries.
        for shard in 0..self.feeds.shard_count() {
            let Some(name) = self.feeds.shard_name(shard) else {
                continue;
            };
            let Some(manifest_seq) = self.refdata.manifest_seq(name) else {
                continue;
            };
            let valid = self.refdata.is_valid(name);
            // The count this shard's channels actually state on the wire.
            // `published()` is the process's, which is the cap's number: written
            // to every channel it would report N times what any subscriber will
            // ever receive a message for.
            let on_shard = self.refdata.published_on(name).unwrap_or(0);
            for channel_id in self.feeds.channel_ids_on(shard) {
                refdata.set_manifest_seq(channel_id, u64::from(manifest_seq));
                refdata.set_manifest_valid(channel_id, valid);
                refdata.set_instruments_current(channel_id, on_shard);
            }
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
/// the manifest arrive as arguments rather than being drained here, so that a
/// shard's pacer is asked once per tick however many of that shard's feeds are
/// enabled.
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

        let shard = self.shard_of(instrument);
        if !self.market_by_price_carries(shard) {
            // No depth feed on this instrument's shard carries `0x14`, so there
            // is nothing to announce and nothing to recover. Counted as
            // unroutable, which is what every other event no enabled feed
            // carries is counted as.
            self.unroutable += 1;
            return;
        }
        // The anchor is where **this instrument's own channel** is now: the
        // reset takes effect immediately, so it is the number the datagram
        // carrying it will take, read off that shard's send path because
        // nothing else knows it. Read off another shard's it is a number from
        // another channel instance's series, which the subscriber will compare
        // against its own — a wrong answer, not a late one.
        let anchor = shard
            .and_then(|shard| self.feeds.market_by_price_on(shard))
            .map_or(0, |pipeline| pipeline.mktdata_sequence().unwrap_or(0));

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
                    shard
                        .and_then(|shard| self.feeds.market_by_price_on_mut(shard))
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
        // Read once, before the match, so every branch labels its observation
        // the same way and a new branch cannot forget to.
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
                // Resolved once, here, and every send below is an indexed load
                // on it. A name compared per message would put the size of the
                // shard set on the datagram path for an answer the admission
                // already settled.
                let shard = self.shard_of(instrument);
                // Refused before the lowering when no feed on this instrument's
                // shard carries it, which costs nothing here and is the same
                // rule the depth branches below need for a stronger reason.
                if !self.top_of_book_carries(shard) {
                    self.unroutable += 1;
                    return;
                }
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
                            shard
                                .and_then(|shard| self.feeds.top_of_book_on_mut(shard))
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
                let shard = self.shard_of(instrument);
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
                    // **One value, both of the shard's feeds.** The wire
                    // requires `0x04` to be byte-for-byte identical across the
                    // feeds in the family a venue publishes, and this is the
                    // mechanism: there is one lowered trade and no second call
                    // site to drift. Shards make it two sends of one value per
                    // shard instead of two per process, which is more sends and
                    // no more values. A trade also stamps no `Per-Instrument
                    // Seq` — the message has no such field, and it is not a
                    // book mutation.
                    Ok(trade) => {
                        let mut reached = false;
                        timed(&self.metrics, EgressMessageType::Trade, || {
                            if let Some(pipeline) =
                                shard.and_then(|shard| self.feeds.top_of_book_on_mut(shard))
                            {
                                reached |= pipeline.send_trade(&trade, now_mono, now_unix).is_ok();
                            }
                            if let Some(pipeline) =
                                shard.and_then(|shard| self.feeds.market_by_price_on_mut(shard))
                            {
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
                let shard = self.shard_of(instrument);
                // Before the lowering, and here that is the load-bearing order:
                // `lower_level` stamps `Per-Instrument Seq`, and a number spent
                // on a message that no feed will carry is a gap every
                // subscriber reads as packet loss.
                if !self.market_by_price_carries(shard) {
                    self.unroutable += 1;
                    return;
                }
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
                            shard
                                .and_then(|shard| self.feeds.market_by_price_on_mut(shard))
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
                let shard = self.shard_of(instrument);
                if !self.market_by_price_carries(shard) {
                    self.unroutable += 1;
                    return;
                }
                let lowered = self.depth.lower_clear(
                    self.refdata.instruments(),
                    instrument,
                    source_ts_ns,
                    scope,
                );
                match lowered {
                    Ok(clear) => {
                        let sent = timed(&self.metrics, EgressMessageType::BookClear, || {
                            shard
                                .and_then(|shard| self.feeds.market_by_price_on_mut(shard))
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
            // lowered, for the same reason the depth branches check first.
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
