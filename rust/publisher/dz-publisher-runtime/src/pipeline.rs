//! One feed's send path: two port roles, one era, and the cadences.
//!
//! Everything below the datagram belongs to `dz-publisher-egress` and nothing
//! here re-decides any of it — not the 1,232-byte cap, not `Sequence Number`,
//! not `Reset Count`, not the port role a message is allowed on, not the source
//! address. What this type owns is the part that is a question of *when*:
//! heartbeats, the manifest cadence, and the flush.
//!
//! # There is no `mtu`
//!
//! [`ChannelEgress::new`] takes a path MTU, and the value handed to it here is
//! [`MAX_DATAGRAM_SIZE`] itself. The cap is mandated, the builder clamps to it,
//! and the configuration document has no key that can express a larger one —
//! which is the difference between this and the publisher that shipped 1448
//! bytes to production from a key exactly like the one that is missing.
//!
//! # It flushes per message rather than batching
//!
//! `Message Count` runs to 255, so several messages in one datagram is
//! representable and this does not do it. The reason is
//! [`ChannelEgress`]'s own: a datagram it has composed is already numbered, and
//! a numbered datagram that waits is a datagram whose `Send Timestamp` is a lie
//! and whose successors are held behind it. Batching therefore needs a stated
//! window, an operator would have to be able to see and set that window, and no
//! key in the design names one — so the choice here is the one that needs no
//! key: latency, and a datagram per message. A batch window is an additive
//! change to the document when someone measures a reason for it.

use std::sync::Arc;

use dz_edge_core::{EndOfSession, Heartbeat, PortRole, ResetCount, MAX_DATAGRAM_SIZE};
use dz_edge_refdata::{InstrumentDefinition, ManifestSummary};
use dz_edge_tob::{Quote, TopOfBook, Trade};
use dz_publisher_egress::{ChannelEgress, EgressEndpoint, EgressError, Tee};
use dz_publisher_metrics::{EgressMessageType, PublisherMetrics};

use crate::config::{Feed, FeedSpec};

/// The send path for one `[[feed]]` block.
///
/// Two [`ChannelEgress`] instances and not one: the port roles are separate
/// channel instances with independent sequence series, and two roles sharing one
/// composer would share one datagram under construction and one set of numbers.
pub struct FeedPipeline {
    spec: FeedSpec,
    channel_id: u8,
    mktdata: ChannelEgress<TopOfBook, Tee>,
    refdata: ChannelEgress<TopOfBook, Tee>,
    heartbeat_interval_ns: u64,
    manifest_cadence_ns: u64,
    /// Monotonic. When a mktdata datagram last left, so that a heartbeat is
    /// *"sent when there is no other traffic"* rather than sent on a timer
    /// alongside traffic.
    last_mktdata_ns: Option<u64>,
    /// Monotonic. When the manifest is next due.
    next_manifest_ns: Option<u64>,
}

impl FeedPipeline {
    /// Compose a feed's send path over two already-built fan-outs.
    ///
    /// The fan-outs are arguments rather than opened here, which is what makes
    /// every property of this type testable with no socket: a test hands it two
    /// [`Tee`]s holding recording sinks, and [`crate::run()`] hands it two
    /// holding [`MulticastTransmitter`](dz_publisher_egress::MulticastTransmitter)s.
    ///
    /// `era` is what [`EraStore::begin_era`](dz_publisher_egress::EraStore::begin_era)
    /// returned for this feed, and both roles are numbered in it: a restart is
    /// one event for the whole feed.
    #[must_use]
    pub fn new(
        feed: &Feed,
        metrics: Arc<PublisherMetrics>,
        era: ResetCount,
        mktdata_endpoint: EgressEndpoint,
        mktdata: Tee,
        refdata_endpoint: EgressEndpoint,
        refdata: Tee,
    ) -> Self {
        // The cap, not a configured value. See the module note.
        let mtu = MAX_DATAGRAM_SIZE as u16;
        let mut mktdata =
            ChannelEgress::new(mktdata_endpoint, mktdata, Arc::clone(&metrics), era, mtu);
        let mut refdata = ChannelEgress::new(refdata_endpoint, refdata, metrics, era, mtu);
        mktdata.register(feed.channel_id);
        refdata.register(feed.channel_id);
        Self {
            spec: feed.spec,
            channel_id: feed.channel_id,
            mktdata,
            refdata,
            heartbeat_interval_ns: nanos(feed.heartbeat_interval),
            manifest_cadence_ns: nanos(feed.manifest_cadence),
            last_mktdata_ns: None,
            next_manifest_ns: None,
        }
    }

    #[must_use]
    pub const fn spec(&self) -> FeedSpec {
        self.spec
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

    /// One `0x01 Heartbeat`, sent.
    ///
    /// `channel_id` on the message is set from this feed's own, even though the
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
    /// computes the pacing against.
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

    /// Send whatever is still open on either port role.
    ///
    /// # Errors
    ///
    /// The first [`EgressError`] either role produced. Both are attempted even
    /// after one fails: they are separate channel instances, and one socket
    /// error must not leave the other role's datagram sitting in memory with its
    /// number already assigned.
    pub fn flush(&mut self, now_mono_ns: u64, send_ts_ns: u64) -> Result<(), EgressError> {
        let mktdata = self.mktdata.flush_all(send_ts_ns);
        let refdata = self.refdata.flush_all(send_ts_ns);
        self.last_mktdata_ns = Some(now_mono_ns);
        mktdata.and(refdata)
    }

    /// Whether a heartbeat is due: nothing has left the mktdata port for the
    /// interval.
    #[must_use]
    pub fn heartbeat_due(&self, now_mono_ns: u64) -> bool {
        match self.last_mktdata_ns {
            // Nothing has ever left. The first heartbeat is due immediately,
            // which is what makes a subscriber that joined before the first
            // quote able to tell this channel exists.
            None => true,
            Some(last) => now_mono_ns.saturating_sub(last) >= self.heartbeat_interval_ns,
        }
    }

    /// Whether the manifest is due.
    #[must_use]
    pub fn manifest_due(&self, now_mono_ns: u64) -> bool {
        self.next_manifest_ns.is_none_or(|next| now_mono_ns >= next)
    }

    /// A dropped fan-out member whose failure darkens this publisher, on either
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
    }

    /// The live member count of one port role's fan-out, for the log line a
    /// dropped member deserves.
    #[must_use]
    pub fn live_sinks(&self, port_role: PortRole) -> usize {
        match port_role {
            PortRole::Mktdata => self.mktdata.sink().live(),
            PortRole::Refdata => self.refdata.sink().live(),
            // No snapshot port role is composed for a top-of-book feed, and
            // there is no depth feed to compose one for; see
            // `StartupError::UnsupportedFeedSpec`.
            PortRole::Snapshot => 0,
        }
    }

    /// The `Sequence Number` this feed's mktdata series will stamp next, for a
    /// snapshot's anchor and for a log line.
    #[must_use]
    pub fn mktdata_sequence(&self) -> Option<u64> {
        self.mktdata
            .sequencer()
            .current(&self.mktdata.endpoint().instance(self.channel_id))
            .map(|sequence| sequence.sequence_number())
    }

    fn send_mktdata<M: dz_edge_core::AppMessage>(
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
