//! Composing datagrams for one port role and handing them to a sink.

use std::collections::HashMap;
use std::sync::Arc;

use dz_edge_core::{AppMessage, DatagramBuilder, EncodeError, Feed, PortRole, ResetCount};
use dz_publisher_metrics::{EgressMessageType, PublisherMetrics};

use crate::error::EgressError;
use crate::instance::{ChannelInstance, EgressEndpoint};
use crate::sequencer::Sequencer;
use crate::sink::DatagramSink;

/// One port role's send path: the sequencer, a datagram under construction per
/// `Channel ID`, and the sink the finished datagrams go to.
///
/// The design names `MulticastTransmitter` and `DatagramSink`; this is the
/// piece between them, and it is where every obligation a feed specification
/// puts on the send path is discharged:
///
/// - The 1,232-byte cap, through [`DatagramBuilder`], which clamps its own
///   capacity so that no `mtu` this type is constructed with can raise it.
/// - `Sequence Number`, dense per channel instance, through [`Sequencer`].
/// - `Reset Count`, from the era the store handed out at startup.
/// - The snapshot flag, which follows from the port role and is the builder's.
/// - A message refused on a port role its specification does not list, counted
///   rather than fatal.
///
/// A publisher that composes datagrams any other way has re-decided all five.
///
/// # It never queues
///
/// A datagram this type composes is numbered, and a numbered datagram that
/// waits is a datagram whose `Send Timestamp` is a lie and whose successors are
/// held behind it. So a sink that refuses one loses it, and the loss is
/// counted: a queue in front of a multicast socket adds latency to every
/// datagram after it in order to hide a loss the subscriber's own sequence
/// tracker would have reported anyway.
///
/// # One instance per port role
///
/// The port roles are separate channel instances with independent series. Two
/// roles sharing one of these would share one datagram under construction and
/// one set of numbers.
pub struct ChannelEgress<F: Feed, S: DatagramSink> {
    endpoint: EgressEndpoint,
    sink: S,
    metrics: Arc<PublisherMetrics>,
    sequencer: Sequencer,
    mtu: u16,
    open: HashMap<u8, Open<F>>,
}

/// A datagram under construction, and what is in it.
struct Open<F: Feed> {
    builder: DatagramBuilder<F>,
    /// What has been packed, by message type, so that
    /// `dz_publisher_egress_messages_total` counts messages that were *sent*.
    ///
    /// Counting at the push instead would report messages this publisher
    /// composed and lost, which makes the ratio between messages and datagrams
    /// meaningless in exactly the incident where it is being read. At most one
    /// entry per message type, so the scan is over a handful of elements, and
    /// the allocation is amortised across every datagram on this `Channel ID`.
    tally: Vec<(EgressMessageType, u64)>,
}

impl<F: Feed> Open<F> {
    fn push<M: AppMessage>(
        &mut self,
        message: &M,
        message_type: EgressMessageType,
    ) -> Result<(), EncodeError> {
        self.builder.push(message)?;
        match self
            .tally
            .iter_mut()
            .find(|(seen, _)| *seen == message_type)
        {
            Some((_, count)) => *count += 1,
            None => self.tally.push((message_type, 1)),
        }
        Ok(())
    }
}

impl<F: Feed, S: DatagramSink> ChannelEgress<F, S> {
    /// A send path for one port role.
    ///
    /// `endpoint` comes from the transmitter — see
    /// [`MulticastTransmitter::endpoint`](crate::MulticastTransmitter::endpoint) —
    /// so that the identity the series is numbered under is the identity the
    /// socket sends from.
    ///
    /// `era` is what [`EraStore::begin_era`](crate::EraStore::begin_era)
    /// returned for this feed. Every channel instance registered here is
    /// numbered in it.
    ///
    /// `mtu` is a path MTU, not a cap: the builder clamps it to the mandated
    /// 1,232 bytes, so a value read from a configuration key that an operator
    /// set too high takes effect as the cap and nothing worse.
    #[must_use]
    pub fn new(
        endpoint: EgressEndpoint,
        sink: S,
        metrics: Arc<PublisherMetrics>,
        era: ResetCount,
        mtu: u16,
    ) -> Self {
        Self {
            endpoint,
            sink,
            metrics,
            sequencer: Sequencer::new(era),
            mtu,
            open: HashMap::new(),
        }
    }

    /// Begin a sequence series for a `Channel ID` on this port role.
    ///
    /// Idempotent; see [`Sequencer::register`].
    pub fn register(&mut self, channel_id: u8) -> bool {
        self.sequencer.register(self.endpoint.instance(channel_id))
    }

    #[must_use]
    pub const fn endpoint(&self) -> EgressEndpoint {
        self.endpoint
    }

    #[must_use]
    pub const fn port_role(&self) -> PortRole {
        self.endpoint.port_role
    }

    /// The sequencer, for a diagnostic. The send path owns the advancing.
    #[must_use]
    pub const fn sequencer(&self) -> &Sequencer {
        &self.sequencer
    }

    #[must_use]
    pub const fn sink(&self) -> &S {
        &self.sink
    }

    /// Pack a message into this `Channel ID`'s datagram, flushing first if it
    /// will not fit.
    ///
    /// `message_type` is the label the message is counted under; it must be
    /// the type of `message`. The mapping from a codec type to the metric
    /// vocabulary has no home either crate can host — see the crate docs — so
    /// it is passed rather than derived.
    ///
    /// `send_timestamp_ns` is read by the caller immediately before the call,
    /// because a message that does not fit flushes the datagram it did not fit
    /// in, and that datagram's `Send Timestamp` is the instant it leaves the
    /// host.
    ///
    /// # Errors
    ///
    /// [`EgressError`], counted under its own reason before it is returned.
    /// Every case drops this one message and leaves the publisher running:
    ///
    /// - a `Channel ID` nobody registered,
    /// - a message the port role does not carry,
    /// - a message no datagram can carry, which is the mandated cap being
    ///   enforced rather than the message being truncated,
    /// - the sink refusing the datagram the message displaced.
    pub fn push<M: AppMessage>(
        &mut self,
        channel_id: u8,
        message: &M,
        message_type: EgressMessageType,
        send_timestamp_ns: u64,
    ) -> Result<(), EgressError> {
        let result = self.push_inner(channel_id, message, message_type, send_timestamp_ns);
        if let Err(error) = &result {
            self.record(error);
        }
        result
    }

    /// Stamp and send this `Channel ID`'s datagram, if it holds anything.
    ///
    /// A tick that packed nothing sends nothing: `Message Count` has range
    /// 1-255, so an empty datagram is not representable, and no number is
    /// spent on one.
    ///
    /// # Errors
    ///
    /// [`EgressError`] from the sink, counted under its reason before it is
    /// returned. The datagram is lost and its number is spent; see
    /// [`Sequencer::advance`].
    pub fn flush(&mut self, channel_id: u8, send_timestamp_ns: u64) -> Result<(), EgressError> {
        let result = self.flush_inner(channel_id, send_timestamp_ns);
        if let Err(error) = &result {
            self.record(error);
        }
        result
    }

    /// Flush every `Channel ID` with a datagram open.
    ///
    /// Every one is attempted even after one fails, because they are separate
    /// channel instances: one socket error must not leave another shard's
    /// datagram sitting in memory with its number already assigned. The first
    /// failure is returned, and all of them are counted.
    ///
    /// # Errors
    ///
    /// The first [`EgressError`] any of the flushes produced.
    pub fn flush_all(&mut self, send_timestamp_ns: u64) -> Result<(), EgressError> {
        let mut first = None;
        let channel_ids: Vec<u8> = self.open.keys().copied().collect();
        for channel_id in channel_ids {
            if let Err(error) = self.flush(channel_id, send_timestamp_ns) {
                if first.is_none() {
                    first = Some(error);
                }
            }
        }
        match first {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn push_inner<M: AppMessage>(
        &mut self,
        channel_id: u8,
        message: &M,
        message_type: EgressMessageType,
        send_timestamp_ns: u64,
    ) -> Result<(), EgressError> {
        let instance = self.endpoint.instance(channel_id);
        let refused = match self.open_for(instance)?.push(message, message_type) {
            Ok(()) => return Ok(()),
            Err(refused) => refused,
        };
        // A full datagram is not a refusal of the message: it is this
        // datagram's capacity being reached, which is what finishing a datagram
        // is for. The message is retried on a fresh one, once.
        if !matches!(
            refused,
            EncodeError::DatagramFull { .. } | EncodeError::MessageCountExhausted { .. }
        ) {
            return Err(refused.into());
        }
        // The flush error is returned rather than swallowed, and the message is
        // dropped rather than packed into a fresh datagram. A sink that just
        // refused a datagram will refuse the next one for the same reason, and
        // packing the message anyway spends a second number to lose a second
        // datagram. Counted once, by the outer `push`.
        self.flush_inner(channel_id, send_timestamp_ns)?;
        self.open_for(instance)?
            .push(message, message_type)
            // Nothing fits, in an empty datagram of the mandated capacity. The
            // message is refused rather than truncated: a truncated message is
            // framed by a Length field that no longer describes it, so every
            // message behind it in the datagram is mis-parsed too.
            .map_err(EgressError::from)
    }

    fn flush_inner(&mut self, channel_id: u8, send_timestamp_ns: u64) -> Result<(), EgressError> {
        let Some(open) = self.open.remove(&channel_id) else {
            return Ok(());
        };
        let instance = self.endpoint.instance(channel_id);
        let sequence_number = self
            .sequencer
            .current(&instance)
            .map(|sequence| sequence.sequence_number());
        let Open { builder, tally } = open;
        // The clock is read by the caller and stamped here, as late as the
        // builder allows: this is the value every latency measurement over this
        // feed is built on.
        let Some(datagram) = builder.finish(send_timestamp_ns) else {
            return Ok(());
        };
        self.sequencer.advance(&instance);
        self.sink.send(&datagram)?;
        let egress = self.metrics.egress();
        let port_role = self.endpoint.port_role;
        egress.datagram(port_role);
        egress.bytes(port_role, datagram.len() as u64);
        for (message_type, count) in tally {
            for _ in 0..count {
                egress.message(port_role, message_type);
            }
            // The heartbeat gauge is an egress series, and this is the only
            // code that knows a heartbeat reached the wire — the runtime knows
            // when it decided to send one, which is a different fact and the
            // one that is useless in an incident. Set from `Send Timestamp`
            // rather than from a second clock read, so the gauge and the
            // datagram agree: the field is nanoseconds since the Unix epoch,
            // which is what makes the division exact enough to be a timestamp.
            //
            // Recording it here is why a publisher gets the series "whether or
            // not anyone thought about it". Nothing else would: the gauge is
            // pre-created at 0, and a staleness alert on a series nobody ever
            // sets cannot be distinguished from a publisher that has stopped
            // heartbeating.
            if message_type == EgressMessageType::Heartbeat {
                egress.set_heartbeat_last_sent(
                    port_role,
                    channel_id,
                    send_timestamp_ns as f64 / 1e9,
                );
            }
        }
        if let Some(sequence_number) = sequence_number {
            egress.set_sequence(port_role, channel_id, sequence_number);
        }
        Ok(())
    }

    /// The datagram under construction for a channel instance, started if
    /// there is none.
    ///
    /// The number is taken from the sequencer here, when the datagram is
    /// started, and spent when it is sent — so an unregistered channel instance
    /// is refused before a single message is packed under a number that does
    /// not exist.
    fn open_for(&mut self, instance: ChannelInstance) -> Result<&mut Open<F>, EgressError> {
        let sequence = self
            .sequencer
            .current(&instance)
            .ok_or(EgressError::NotRegistered { instance })?;
        let port_role = self.endpoint.port_role;
        let mtu = self.mtu;
        Ok(self
            .open
            .entry(instance.channel_id)
            .or_insert_with(|| Open {
                builder: DatagramBuilder::new(sequence, port_role, mtu),
                tally: Vec::new(),
            }))
    }

    /// Count a failure under its own reason.
    ///
    /// The single place this crate writes `dz_publisher_egress_errors_total`
    /// from the composer's side. A failure absorbed by a [`Tee`](crate::Tee) is
    /// counted there instead and never reaches here, so nothing is counted
    /// twice.
    fn record(&self, error: &EgressError) {
        if let Some(reason) = error.reason() {
            self.metrics.egress().error(self.endpoint.port_role, reason);
        }
    }
}
