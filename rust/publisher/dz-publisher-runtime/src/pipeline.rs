//! One feed's send path: its port roles, one era, and the cadences.
//!
//! Everything below the datagram belongs to `dz-publisher-egress` and nothing
//! here re-decides any of it — not the 1,232-byte cap, not `Sequence Number`,
//! not `Reset Count`, not the port role a message is allowed on, not the source
//! address. What this type owns is the part that is a question of *when*:
//! heartbeats, the manifest cadence, and the flush.
//!
//! # Generic over the feed, because `Magic` belongs to the feed
//!
//! A datagram's `Magic` is what rejects one misrouted from a sibling feed, so
//! it is [`Feed`](dz_edge_core::Feed)'s and
//! [`ChannelEgress`] is generic over it. That makes a send path
//! `FeedPipeline<TopOfBook>` or `FeedPipeline<MarketByPrice>`, decided at
//! compile time, with no dynamic dispatch on the datagram path.
//!
//! It also means the specification is a type-level fact rather than a field —
//! see [`EmittedFeed`] — and that matters here for one specific reason: the
//! codec will not stop a `Quote` being pushed into a market-by-price datagram.
//! `DatagramBuilder::push` checks `PORT_ROLES` and nothing checks feed
//! membership. So the routing above this type has to know which specification
//! it is holding, and `F::SPEC` is a thing it cannot be wrong about.
//!
//! # There is no `mtu`
//!
//! [`ChannelEgress::new`] takes a path MTU, and the value handed to it here is
//! [`MAX_DATAGRAM_SIZE`] itself. The cap is mandated, the builder clamps to it,
//! and the configuration document has no key that can express a larger one —
//! which is the difference between this and the publisher that shipped 1448
//! bytes to production from a key exactly like the one that is missing.
//!
//! # It flushes per message on the live path, and packs the rest
//!
//! `Message Count` runs to 255, so several messages in one datagram is
//! representable. Which of the two a message gets depends on what it is for:
//!
//! - **The live path flushes per message.** A datagram [`ChannelEgress`] has
//!   composed is already numbered, and a numbered datagram that waits is a
//!   datagram whose `Send Timestamp` is a lie and whose successors are held
//!   behind it. Batching therefore needs a stated window, an operator would
//!   have to be able to see and set that window, and no key in the design names
//!   one — so the choice here is the one that needs no key: latency, and a
//!   datagram per message.
//! - **The definition cycle and a snapshot pack.** Both are bulk state the
//!   publisher chose the moment of, both are already paced or bounded by
//!   something else, and a snapshot in particular is *one book state* — a
//!   subscriber applies all of it or none of it, so splitting it one level per
//!   datagram would cost a sequence number per level for nothing.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use dz_edge_core::{AppMessage, EndOfSession, Heartbeat, PortRole, ResetCount, MAX_DATAGRAM_SIZE};
use dz_edge_mbp::{BookClear, InstrumentReset, LevelUpdate};
use dz_edge_refdata::{InstrumentDefinition, ManifestSummary};
use dz_edge_tob::{Quote, Trade};
use dz_publisher_egress::{ChannelEgress, EgressEndpoint, EgressError, Tee};
use dz_publisher_lowering::Snapshot;
use dz_publisher_metrics::{EgressMessageType, PublisherMetrics};

use crate::config::{EmittedFeed, Feed, FeedSpec};

/// One port role's fan-out and the identity it sends under.
///
/// The two travel together so that the identity the sequencer numbers under and
/// the identity the socket sends from cannot disagree: a series keyed on a
/// source address the socket is not bound to looks dense here and arrives on
/// the wire under another identity entirely.
pub struct Port {
    /// From the transmitter, so the composer inherits the socket's identity
    /// rather than being told it a second time.
    pub endpoint: EgressEndpoint,
    /// The fan-out. One member is the transmitter; a second would be the tee.
    pub sink: Tee,
}

/// The port roles one feed operates.
pub struct Ports {
    pub mktdata: Port,
    pub refdata: Port,
    /// Depth feeds only, and required for one. See
    /// [`FeedSpec::has_snapshot_port`].
    pub snapshot: Option<Port>,
}

/// The send path for one `[[feed]]` block.
///
/// One [`ChannelEgress`] per port role and not one shared: the roles are
/// separate channel instances with independent sequence series, and two roles
/// sharing a composer would share one datagram under construction and one set
/// of numbers.
pub struct FeedPipeline<F: EmittedFeed> {
    channel_id: u8,
    mktdata: ChannelEgress<F, Tee>,
    refdata: ChannelEgress<F, Tee>,
    snapshot: Option<ChannelEgress<F, Tee>>,
    heartbeat_interval_ns: u64,
    manifest_cadence_ns: u64,
    /// One full pass of the snapshot rotation, when this feed configures one.
    /// Held here rather than passed to the publisher because it is a key of the
    /// feed block, and only a feed with a snapshot port role can have it.
    snapshot_cycle: Option<Duration>,
    /// Monotonic. When a mktdata datagram last left, so that a heartbeat is
    /// *"sent when there is no other traffic"* rather than sent on a timer
    /// alongside traffic.
    last_mktdata_ns: Option<u64>,
    /// Monotonic. When the manifest is next due.
    next_manifest_ns: Option<u64>,
    feed: PhantomData<F>,
}

impl<F: EmittedFeed> FeedPipeline<F> {
    /// Compose a feed's send path over already-built fan-outs.
    ///
    /// The fan-outs are arguments rather than opened here, which is what makes
    /// every property of this type testable with no socket: a test hands it
    /// [`Tee`]s holding recording sinks, and [`crate::run()`] hands it ones
    /// holding [`MulticastTransmitter`](dz_publisher_egress::MulticastTransmitter)s.
    ///
    /// `era` is what [`EraStore::begin_era`](dz_publisher_egress::EraStore::begin_era)
    /// returned for this feed, and every role is numbered in it: a restart is
    /// one event for the whole feed, and every series it carries restarts
    /// together.
    ///
    /// # Panics
    ///
    /// If `feed.spec` is not `F::SPEC`. A caller that composed a
    /// market-by-price send path from a top-of-book block has mismatched the
    /// two halves of one feed, and the result would be datagrams carrying one
    /// feed's `Magic` and the other's ports. It is checked rather than trusted
    /// because [`crate::run()`] resolves the type from the value and a
    /// refactor could invert that; it happens before a socket is used and
    /// before a single datagram.
    #[must_use]
    pub fn new(feed: &Feed, metrics: Arc<PublisherMetrics>, era: ResetCount, ports: Ports) -> Self {
        assert_eq!(
            feed.spec,
            F::SPEC,
            "a {} send path was composed from a {} feed block",
            F::SPEC.as_str(),
            feed.spec.as_str()
        );
        assert_eq!(
            ports.snapshot.is_some(),
            F::SPEC.has_snapshot_port(),
            "the snapshot port role does not match what {} carries",
            F::SPEC.as_str()
        );
        // The cap, not a configured value. See the module note.
        let mtu = MAX_DATAGRAM_SIZE as u16;
        let open = |port: Port, metrics: Arc<PublisherMetrics>| {
            let mut egress = ChannelEgress::new(port.endpoint, port.sink, metrics, era, mtu);
            egress.register(feed.channel_id);
            egress
        };
        let mktdata = open(ports.mktdata, Arc::clone(&metrics));
        let refdata = open(ports.refdata, Arc::clone(&metrics));
        let snapshot = ports.snapshot.map(|port| open(port, Arc::clone(&metrics)));
        Self {
            channel_id: feed.channel_id,
            mktdata,
            refdata,
            snapshot,
            heartbeat_interval_ns: nanos(feed.heartbeat_interval),
            manifest_cadence_ns: nanos(feed.manifest_cadence),
            snapshot_cycle: feed.snapshot_cycle,
            last_mktdata_ns: None,
            next_manifest_ns: None,
            feed: PhantomData,
        }
    }

    /// One full pass of this feed's snapshot rotation, if it configures one.
    ///
    /// `None` is a feed that emits recovery snapshots and no others, which is
    /// what `[[feed]] snapshot_cycle` being absent means.
    #[must_use]
    pub const fn snapshot_cycle(&self) -> Option<Duration> {
        self.snapshot_cycle
    }

    /// The specification this send path composes, which is `F`'s and not a
    /// field.
    #[must_use]
    pub const fn spec(&self) -> FeedSpec {
        F::SPEC
    }

    #[must_use]
    pub const fn channel_id(&self) -> u8 {
        self.channel_id
    }

    /// One `0x03 Quote`, sent.
    ///
    /// # Errors
    ///
    /// [`EgressError`], already counted under its own reason by the composer.
    /// Every case drops this one message and leaves the publisher running.
    pub fn send_quote(
        &mut self,
        quote: &Quote,
        now_mono_ns: u64,
        send_ts_ns: u64,
    ) -> Result<(), EgressError> {
        self.send_mktdata(quote, EgressMessageType::Quote, now_mono_ns, send_ts_ns)
    }

    /// One `0x04 Trade`, sent.
    ///
    /// Takes an already-lowered `Trade` rather than lowering one, and that is
    /// the mechanism behind the cross-specification obligation: the wire
    /// requires `0x04` to be **byte-for-byte identical** across a venue's
    /// sibling feeds, and a publisher emitting two of them sends *one* lowered
    /// value to both send paths rather than two values that agree.
    ///
    /// # Errors
    ///
    /// As [`send_quote`](Self::send_quote).
    pub fn send_trade(
        &mut self,
        trade: &Trade,
        now_mono_ns: u64,
        send_ts_ns: u64,
    ) -> Result<(), EgressError> {
        self.send_mktdata(trade, EgressMessageType::Trade, now_mono_ns, send_ts_ns)
    }

    /// One `0x40 LevelUpdate`, sent.
    ///
    /// # Errors
    ///
    /// As [`send_quote`](Self::send_quote).
    pub fn send_level(
        &mut self,
        level: &LevelUpdate,
        now_mono_ns: u64,
        send_ts_ns: u64,
    ) -> Result<(), EgressError> {
        self.send_mktdata(
            level,
            EgressMessageType::LevelUpdate,
            now_mono_ns,
            send_ts_ns,
        )
    }

    /// One `0x41 BookClear`, sent.
    ///
    /// On the mktdata port role and in the same series as `LevelUpdate`,
    /// because both mutate the book and their relative order is significant.
    ///
    /// # Errors
    ///
    /// As [`send_quote`](Self::send_quote).
    /// `0x14 InstrumentReset` on the market-data port role.
    ///
    /// **The anchor has to be the number this datagram takes**, and this is the
    /// only layer that knows it — which is why the caller composes the message
    /// with [`DepthLowering::lower_instrument_reset`](dz_publisher_lowering::DepthLowering::lower_instrument_reset)
    /// against [`mktdata_sequence`](Self::mktdata_sequence) rather than being
    /// handed a number. Stated here because a caller that read it off the last
    /// delta would be one behind, which is the off-by-one the specification's
    /// own conformance subscriber grades a violation.
    ///
    /// # Errors
    ///
    /// As [`send_book_clear`](Self::send_book_clear).
    pub fn send_instrument_reset(
        &mut self,
        reset: &InstrumentReset,
        now_mono_ns: u64,
        send_ts_ns: u64,
    ) -> Result<(), EgressError> {
        self.send_mktdata(
            reset,
            EgressMessageType::InstrumentReset,
            now_mono_ns,
            send_ts_ns,
        )
    }

    pub fn send_book_clear(
        &mut self,
        clear: &BookClear,
        now_mono_ns: u64,
        send_ts_ns: u64,
    ) -> Result<(), EgressError> {
        self.send_mktdata(clear, EgressMessageType::BookClear, now_mono_ns, send_ts_ns)
    }

    /// One instrument's book state, on the snapshot port role.
    ///
    /// The begin, every level, and the end, **packed and then flushed
    /// together**: it is one book state, a subscriber applies all of it or none
    /// of it, and one level per datagram would spend a sequence number per
    /// level for nothing. A snapshot larger than one datagram simply fills
    /// several, in order, on the snapshot series.
    ///
    /// The port role is what carries the snapshot flag in every message header,
    /// which the builder owns — so a snapshot message cannot reach the live
    /// port and a live message cannot reach this one.
    ///
    /// # Errors
    ///
    /// [`EgressError`], counted under its own reason. A refusal partway through
    /// leaves what was already sent on the wire; the begin's `Total Levels` is
    /// what lets a subscriber notice, which is why that field is what was
    /// actually written rather than what was intended.
    pub fn send_snapshot(
        &mut self,
        snapshot: &Snapshot,
        send_ts_ns: u64,
    ) -> Result<(), EgressError> {
        let Some(egress) = self.snapshot.as_mut() else {
            // Unreachable through `new`, which refuses the mismatch, and
            // written rather than unwrapped because a panic on the publish path
            // is a publisher that goes dark.
            return Ok(());
        };
        let channel_id = self.channel_id;
        egress.push(
            channel_id,
            &snapshot.begin,
            EgressMessageType::SnapshotBegin,
            send_ts_ns,
        )?;
        for level in &snapshot.levels {
            egress.push(
                channel_id,
                level,
                EgressMessageType::SnapshotLevel,
                send_ts_ns,
            )?;
        }
        egress.push(
            channel_id,
            &snapshot.end,
            EgressMessageType::SnapshotEnd,
            send_ts_ns,
        )?;
        egress.flush(channel_id, send_ts_ns)
    }

    /// One `0x01 Heartbeat`, sent.
    ///
    /// `channel_id` on the message is set from this feed's own even though the
    /// builder stamps the datagram's over it afterwards: a truthful field costs
    /// nothing and the two cannot then disagree.
    ///
    /// # Errors
    ///
    /// As [`send_quote`](Self::send_quote).
    pub fn send_heartbeat(&mut self, now_mono_ns: u64, send_ts_ns: u64) -> Result<(), EgressError> {
        let heartbeat = Heartbeat {
            channel_id: self.channel_id,
            timestamp_ns: send_ts_ns,
        };
        // The interval is recorded whether or not the send succeeded, which
        // `send_mktdata` does: a failing send retried every tick is a syscall
        // per tick that learns nothing, and the failure is already counted.
        self.send_mktdata(
            &heartbeat,
            EgressMessageType::Heartbeat,
            now_mono_ns,
            send_ts_ns,
        )
    }

    /// One `0x06 EndOfSession`, sent.
    ///
    /// The terminal statement on the mktdata port. Nothing may follow it, which
    /// is why [`crate::Publisher::shut_down`] sends it after the final manifest
    /// and before the last flush.
    ///
    /// # Errors
    ///
    /// As [`send_quote`](Self::send_quote).
    pub fn send_end_of_session(
        &mut self,
        now_mono_ns: u64,
        send_ts_ns: u64,
    ) -> Result<(), EgressError> {
        let end = EndOfSession {
            timestamp_ns: send_ts_ns,
        };
        self.send_mktdata(
            &end,
            EgressMessageType::EndOfSession,
            now_mono_ns,
            send_ts_ns,
        )
    }

    /// One `0x02 InstrumentDefinition`, packed onto the refdata port.
    ///
    /// **Packed and not flushed.** The definition cycle's whole point is that a
    /// lap is spread across the cycle rather than emitted as a burst, and the
    /// pacer has already decided how many this tick owes — so the ones it owes
    /// go into as few datagrams as they fit in, which is what
    /// [`definitions_per_datagram`](dz_publisher_refdata::definitions_per_datagram)
    /// paces against.
    ///
    /// # Errors
    ///
    /// As [`send_quote`](Self::send_quote).
    pub fn pack_definition(
        &mut self,
        definition: &InstrumentDefinition,
        send_ts_ns: u64,
    ) -> Result<(), EgressError> {
        self.refdata.push(
            self.channel_id,
            definition,
            EgressMessageType::InstrumentDefinition,
            send_ts_ns,
        )
    }

    /// One `0x07 ManifestSummary`, sent.
    ///
    /// # Errors
    ///
    /// As [`send_quote`](Self::send_quote).
    pub fn send_manifest(
        &mut self,
        manifest: &ManifestSummary,
        now_mono_ns: u64,
        send_ts_ns: u64,
    ) -> Result<(), EgressError> {
        self.next_manifest_ns = Some(now_mono_ns.saturating_add(self.manifest_cadence_ns));
        self.refdata.push(
            self.channel_id,
            manifest,
            EgressMessageType::ManifestSummary,
            send_ts_ns,
        )?;
        self.refdata.flush(self.channel_id, send_ts_ns)
    }

    /// Send whatever is still open on any port role.
    ///
    /// # Errors
    ///
    /// The first [`EgressError`] any role produced. Every role is attempted
    /// even after one fails: they are separate channel instances, and one
    /// socket error must not leave another role's datagram sitting in memory
    /// with its number already assigned.
    pub fn flush(&mut self, send_ts_ns: u64) -> Result<(), EgressError> {
        let mktdata = self.mktdata.flush_all(send_ts_ns);
        let refdata = self.refdata.flush_all(send_ts_ns);
        let snapshot = match self.snapshot.as_mut() {
            Some(egress) => egress.flush_all(send_ts_ns),
            None => Ok(()),
        };
        mktdata.and(refdata).and(snapshot)
    }

    /// Whether a heartbeat is due: nothing has left the mktdata port for the
    /// interval.
    #[must_use]
    pub fn heartbeat_due(&self, now_mono_ns: u64) -> bool {
        match self.last_mktdata_ns {
            // Nothing has ever left. The first heartbeat is due immediately,
            // which is what makes a subscriber that joined before the first
            // message able to tell this channel exists.
            None => true,
            Some(last) => now_mono_ns.saturating_sub(last) >= self.heartbeat_interval_ns,
        }
    }

    /// Whether the manifest is due.
    #[must_use]
    pub fn manifest_due(&self, now_mono_ns: u64) -> bool {
        self.next_manifest_ns.is_none_or(|next| now_mono_ns >= next)
    }

    /// A dropped fan-out member whose failure darkens this publisher, on any
    /// port role.
    ///
    /// Read between ticks by [`ConsistencyGuard`](crate::ConsistencyGuard). Not
    /// returned from a send: ending the process from inside a fan-out abandons
    /// the datagrams the other members already took.
    #[must_use]
    pub fn dark_transmitter(&self) -> Option<&str> {
        self.mktdata
            .sink()
            .process_failure()
            .or_else(|| self.refdata.sink().process_failure())
            .or_else(|| {
                self.snapshot
                    .as_ref()
                    .and_then(|egress| egress.sink().process_failure())
            })
    }

    /// The live member count of one port role's fan-out, for the log line a
    /// dropped member deserves. `None` for a role this feed does not operate.
    #[must_use]
    pub fn live_sinks(&self, port_role: PortRole) -> Option<usize> {
        match port_role {
            PortRole::Mktdata => Some(self.mktdata.sink().live()),
            PortRole::Refdata => Some(self.refdata.sink().live()),
            PortRole::Snapshot => self.snapshot.as_ref().map(|e| e.sink().live()),
        }
    }

    /// The `Sequence Number` this feed's mktdata series will stamp next.
    ///
    /// This is a snapshot's `Anchor Seq`: the point in the live stream the book
    /// state is true as of, which is what tells a subscriber which live
    /// messages to apply after it and which to discard.
    #[must_use]
    pub fn mktdata_sequence(&self) -> Option<u64> {
        self.mktdata
            .sequencer()
            .current(&self.mktdata.endpoint().instance(self.channel_id))
            .map(|sequence| sequence.sequence_number())
    }

    fn send_mktdata<M: AppMessage>(
        &mut self,
        message: &M,
        message_type: EgressMessageType,
        now_mono_ns: u64,
        send_ts_ns: u64,
    ) -> Result<(), EgressError> {
        // Before the push, so that a message the port role refuses still counts
        // as an attempt this feed made: the alternative is a refused message
        // type provoking a heartbeat on every tick.
        self.last_mktdata_ns = Some(now_mono_ns);
        self.mktdata
            .push(self.channel_id, message, message_type, send_ts_ns)?;
        self.mktdata.flush(self.channel_id, send_ts_ns)
    }
}

fn nanos(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
