//! The wire side: an archive of datagrams, read back as a stream of messages.
//!
//! **The framing is stripped here, and that is the first of Mode C's three
//! requirements.** Datagram batching is time-dependent — how many messages a
//! publisher packed into one datagram is a function of when its ticks fell and
//! how full the buffer was — while the messages inside it are not. So the
//! comparison is at message grain, the datagram a message arrived in is carried
//! as provenance, and nothing about it is ever compared.
//!
//! Nothing here repairs anything. A datagram this build cannot decode is
//! counted, not discarded silently and not reconstructed: an archive holds
//! bytes precisely so that the datagram nothing can explain survives to be
//! looked at.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddrV4;

use dz_edge_core::{Datagram, DatagramHeader, PortRole, TYPE_END_OF_SESSION, TYPE_HEARTBEAT};
use dz_edge_mbp::{
    BookClear, InstrumentReset, LevelUpdate, SnapshotBegin, SnapshotEnd, SnapshotLevel,
};
use dz_edge_refdata::{InstrumentDefinition, ManifestSummary};
use dz_edge_tob::{Quote, Trade};
use dz_publisher_lowering::SourceId;
use dz_recorder_core::{RecordedDatagram, RecvTsKind, Source};

use crate::error::RelowerError;
use crate::finding::Caveat;
use crate::join::JoinKey;
use crate::refdata::ArchivedRefdata;

/// One decoded message, either side of the comparison.
///
/// The four types a normalized venue event can become. Every other message in
/// the family is timed by the publisher rather than derived from an upstream
/// payload, and is excluded from the join — see [`Skipped`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageBody {
    Quote(Quote),
    Trade(Trade),
    Level(LevelUpdate),
    Clear(BookClear),
}

impl MessageBody {
    /// The message type, as the codec names it.
    #[must_use]
    pub const fn message_type(&self) -> &'static str {
        match self {
            Self::Quote(_) => "Quote",
            Self::Trade(_) => "Trade",
            Self::Level(_) => "LevelUpdate",
            Self::Clear(_) => "BookClear",
        }
    }

    /// The key this message joins on.
    #[must_use]
    pub const fn join_key(&self) -> JoinKey {
        match self {
            Self::Quote(quote) => JoinKey::of_quote(quote),
            Self::Trade(trade) => JoinKey::of_trade(trade),
            Self::Level(level) => JoinKey::of_level(level),
            Self::Clear(clear) => JoinKey::of_clear(clear),
        }
    }
}

/// A message that moves book state without being a venue event.
///
/// **Deliberately not a `MessageBody` variant.** That enum is what a re-lowering
/// *compares*, and every one of these is excluded from a comparison for a reason
/// that has not changed: a reset and a snapshot are the publisher's own
/// statements about its book, produced by the runtime rather than lowered from
/// an upstream payload, so a re-lowering has nothing to produce them from and
/// their absence from it means nothing. Widening `MessageBody` would turn four
/// stated exclusions into an implicit one.
///
/// They are surfaced because a consumer that *builds* a book rather than
/// comparing one needs exactly these four and can get them nowhere else: a
/// snapshot cycle is the only anchor a delta book has, and a reset is the only
/// statement that the book before it is not to be trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateBody {
    Reset(InstrumentReset),
    SnapshotBegin(SnapshotBegin),
    SnapshotLevel(SnapshotLevel),
    SnapshotEnd(SnapshotEnd),
}

impl StateBody {
    /// The message type, as the codec names it.
    #[must_use]
    pub const fn message_type(&self) -> &'static str {
        match self {
            Self::Reset(_) => "InstrumentReset",
            Self::SnapshotBegin(_) => "SnapshotBegin",
            Self::SnapshotLevel(_) => "SnapshotLevel",
            Self::SnapshotEnd(_) => "SnapshotEnd",
        }
    }
}

/// One state message the archive holds, and where it was.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateMessage {
    pub body: StateBody,
    pub provenance: WireProvenance,
}

/// A reference-data message, and where it was.
///
/// [`ArchivedRefdata`](crate::refdata::ArchivedRefdata) consumes these and keeps
/// what it needs for a comparison, which is a set rather than a history: it keys
/// by symbol and pins the first statement, because its two archives carry no key
/// that orders one against the other and so no instant at which to switch
/// exponents is defensible.
///
/// A consumer holding **one** archive is not in that position — every definition
/// here arrives at a sequence number — so it can place a restatement exactly, and
/// needs the position in order to. That is the whole reason these are surfaced
/// rather than only accumulated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReferenceMessage {
    pub body: ReferenceBody,
    pub provenance: WireProvenance,
}

/// The two messages that describe instruments rather than markets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceBody {
    Definition(InstrumentDefinition),
    Manifest(ManifestSummary),
}

/// Where a message was on the wire.
///
/// **Provenance, never compared.** Nothing here enters a comparison: put the
/// timing fields in one and a publisher that packed two messages differently
/// would be reported as defective, and put the addressing fields in one and a
/// feed served by a second path would be. What they are for is finding the
/// datagram again — an operator handed a finding opens the archive at this
/// sequence number — and, for a consumer that derives rows rather than findings,
/// saying which channel instance a message belongs to.
///
/// The two halves are there for those two different reasons and it is worth
/// keeping them apart in one's head. [`datagram_index`](Self::datagram_index),
/// [`message_index`](Self::message_index) and the timestamps are **timing**: a
/// batching or pacing decision moves every one of them. [`src`](Self::src),
/// [`dst`](Self::dst), [`channel_id`](Self::channel_id) and
/// [`role`](Self::role) are **identity**: no publisher decision moves them, and
/// together they are the channel instance a sequence number is only meaningful
/// under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireProvenance {
    /// Position of the datagram in the archive, counting from 0 over everything
    /// the source yielded.
    pub datagram_index: u64,
    /// Position of the message inside that datagram, counting from 0.
    pub message_index: u8,
    pub channel_id: u8,
    pub sequence_number: u64,
    pub reset_count: u8,
    pub send_timestamp_ns: u64,
    pub recv_ts_ns: u64,
    /// How [`recv_ts_ns`](Self::recv_ts_ns) was taken. A latency derived from an
    /// application fallback stamp measures this process, not the path, so a
    /// consumer that reports one has to be able to say which it had.
    pub recv_ts_kind: RecvTsKind,
    pub role: PortRole,
    /// The publisher's address and port, as the datagram was received.
    ///
    /// Half of the channel instance. Two redundant publishers serving one
    /// `Channel ID` are told apart by nothing else, and a sequence number read
    /// without it reads one publisher's advance as the other's backward motion.
    pub src: SocketAddrV4,
    /// The group and port the datagram was addressed to.
    ///
    /// The other half. The port is the one the channel instance is keyed on; the
    /// group is not part of that key but is what a subscriber joined, so a
    /// consumer reporting on a feed's delivery needs it and cannot recover it
    /// from anywhere else in this type.
    pub dst: SocketAddrV4,
}

impl core::fmt::Display for WireProvenance {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "datagram {} (channel {}, seq {}, reset {}), message {}",
            self.datagram_index,
            self.channel_id,
            self.sequence_number,
            self.reset_count,
            self.message_index
        )
    }
}

/// One message the archive holds, and where it was.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireMessage {
    pub body: MessageBody,
    pub provenance: WireProvenance,
}

/// What the wire side read and did not compare.
///
/// Every one of these is a deliberate exclusion with its own reason, and they
/// are counted rather than dropped so that a reader can tell a comparison that
/// checked a feed from one that checked nothing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Skipped {
    /// Datagrams whose `Magic` belongs to another feed. Not an error: an archive
    /// may hold several feeds, or traffic that is not one of ours at all.
    pub foreign_magic: u64,
    /// Datagrams this build could not decode — an unsupported schema version, a
    /// declared length out of range, a message count that does not match the
    /// body. Each is a finding for the conformance tier and not for this one,
    /// and each is a datagram whose messages cannot be joined.
    pub undecodable: u64,
    /// `Heartbeat` and `EndOfSession`. Timed by the publisher, and derived from
    /// no upstream payload: a re-lowering has nothing to produce them from and
    /// their absence from it means nothing.
    pub control: u64,
    /// `InstrumentDefinition` and `ManifestSummary`. These are *consumed* rather
    /// than skipped — they are the reference data — but they are not joined: the
    /// definition cycle's pacing is the runtime's.
    pub reference_data: u64,
    /// `SnapshotBegin`, `SnapshotLevel`, `SnapshotEnd`.
    ///
    /// The snapshot is **pulled** by the runtime on its own cadence, from the
    /// adapter's own book, and the payload archive records nothing about when it
    /// asked. Re-lowering a snapshot offline would compare a book state the
    /// re-lowering took at one instant against one the publisher took at
    /// another, which is Mode B's problem and not soluble here.
    ///
    /// Skipped by the *comparison*, which is what this counter reports, and
    /// still surfaced on [`state_messages`](WireCapture::state_messages) for a
    /// consumer that anchors a book with them.
    pub snapshot: u64,
    /// `InstrumentReset`.
    ///
    /// Counted here rather than as an unknown type, which is where it went
    /// before and was wrong: the codec has a decoder for it, so it was never a
    /// message this build could not read. It is skipped for the same reason the
    /// snapshot is — the publisher's statement about its own book, lowered from
    /// no upstream payload — and is surfaced for the same reason.
    pub reset: u64,
    /// Message types this build has no decoder for. The specification requires a
    /// decoder to skip them using the length field and continue, which is what
    /// happens.
    pub unknown_type: u64,
}

/// Everything the multicast archive says.
///
/// Built by [`absorb`](Self::absorb), once per archive: a feed's `mktdata` and
/// `refdata` port roles may be one source or two, and several calls accumulate.
#[derive(Debug, Clone, Default)]
pub struct WireCapture {
    messages: Vec<WireMessage>,
    state: Vec<StateMessage>,
    reference: Vec<ReferenceMessage>,
    refdata: ArchivedRefdata,
    skipped: Skipped,
    datagrams: u64,
    /// Every `Source ID` seen on a joined message, so a capture holding two
    /// publishers can be refused rather than averaged over.
    observed_source_ids: BTreeSet<u16>,
    /// The eras each channel was seen in, for
    /// [`Caveat::EraChangeInsideWindow`].
    eras: BTreeMap<u8, BTreeSet<u8>>,
}

impl WireCapture {
    /// An empty capture.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Read one archive to exhaustion, taking every message of `expected_magic`
    /// it holds.
    ///
    /// `expected_magic` is required and has no default, for the reason the
    /// codec's own walk requires it: `Magic` is the only thing that stops a
    /// datagram misrouted from a sibling feed being parsed at the wrong layout,
    /// and only the caller knows which feed it believes it is holding.
    ///
    /// # Errors
    ///
    /// [`RelowerError::MulticastArchive`] if the source fails before it is
    /// exhausted. A partial window must not be compared: every message after the
    /// tear would be reported as one the publisher never sent.
    pub fn absorb<S: Source + ?Sized>(
        &mut self,
        source: &mut S,
        expected_magic: u16,
    ) -> Result<(), RelowerError> {
        loop {
            let datagram = source.next().map_err(RelowerError::MulticastArchive)?;
            let Some(datagram) = datagram else { break };
            let index = self.datagrams;
            self.datagrams += 1;
            self.absorb_datagram(&datagram, index, expected_magic);
        }
        self.refdata.finalise();
        Ok(())
    }

    fn absorb_datagram(
        &mut self,
        datagram: &RecordedDatagram<'_>,
        datagram_index: u64,
        expected_magic: u16,
    ) {
        // Peeked before the full decode so that a datagram from another feed is
        // counted as foreign rather than as undecodable: the two mean entirely
        // different things about an archive, and one of them is not a problem.
        match DatagramHeader::peek(datagram.payload) {
            Ok(header) if header.magic != expected_magic => {
                self.skipped.foreign_magic += 1;
                return;
            }
            Ok(_) => {}
            Err(_) => {
                self.skipped.undecodable += 1;
                return;
            }
        }

        let decoded = match Datagram::decode(datagram.payload, expected_magic) {
            Ok(decoded) => decoded,
            Err(_) => {
                self.skipped.undecodable += 1;
                return;
            }
        };
        let header = *decoded.header();
        self.eras
            .entry(header.channel_id)
            .or_default()
            .insert(header.reset_count);

        for (message_index, message) in decoded.messages().enumerate() {
            let provenance = WireProvenance {
                datagram_index,
                // A datagram's `Message Count` is a `u8`, so the index fits one;
                // the cast cannot truncate a count the walk validated.
                message_index: u8::try_from(message_index).unwrap_or(u8::MAX),
                channel_id: header.channel_id,
                sequence_number: header.sequence_number,
                reset_count: header.reset_count,
                send_timestamp_ns: header.send_timestamp_ns,
                recv_ts_ns: datagram.recv_ts_ns,
                recv_ts_kind: datagram.recv_ts_kind,
                role: datagram.role,
                src: datagram.src,
                dst: datagram.dst,
            };
            self.absorb_message(
                message.type_id,
                message.bytes,
                header.schema_version,
                provenance,
            );
        }
    }

    fn absorb_message(
        &mut self,
        type_id: u8,
        bytes: &[u8],
        schema_version: u8,
        provenance: WireProvenance,
    ) {
        use dz_edge_core::AppMessage;

        match type_id {
            Quote::TYPE_ID => match Quote::decode(bytes) {
                Ok(quote) => self.push(MessageBody::Quote(quote), quote.source_id, provenance),
                Err(_) => self.skipped.undecodable += 1,
            },
            Trade::TYPE_ID => match Trade::decode(bytes) {
                Ok(trade) => self.push(MessageBody::Trade(trade), trade.source_id, provenance),
                Err(_) => self.skipped.undecodable += 1,
            },
            LevelUpdate::TYPE_ID => match LevelUpdate::decode(bytes) {
                Ok(level) => self.push(MessageBody::Level(level), level.source_id, provenance),
                Err(_) => self.skipped.undecodable += 1,
            },
            BookClear::TYPE_ID => match BookClear::decode(bytes) {
                Ok(clear) => self.push(MessageBody::Clear(clear), clear.source_id, provenance),
                Err(_) => self.skipped.undecodable += 1,
            },
            InstrumentDefinition::TYPE_ID => {
                self.skipped.reference_data += 1;
                // Decoded at the generation the datagram header declared: this
                // is the one message whose layout changed between generations,
                // and an archive can hold either.
                match InstrumentDefinition::decode(bytes, schema_version) {
                    Ok(definition) => {
                        self.refdata.observe_definition(&definition);
                        self.reference.push(ReferenceMessage {
                            body: ReferenceBody::Definition(definition),
                            provenance,
                        });
                    }
                    Err(_) => self.skipped.undecodable += 1,
                }
            }
            ManifestSummary::TYPE_ID => {
                self.skipped.reference_data += 1;
                match ManifestSummary::decode(bytes) {
                    Ok(summary) => {
                        self.refdata.observe_manifest(&summary);
                        self.reference.push(ReferenceMessage {
                            body: ReferenceBody::Manifest(summary),
                            provenance,
                        });
                    }
                    Err(_) => self.skipped.undecodable += 1,
                }
            }
            TYPE_HEARTBEAT | TYPE_END_OF_SESSION => self.skipped.control += 1,
            SnapshotBegin::TYPE_ID => {
                self.skipped.snapshot += 1;
                match SnapshotBegin::decode(bytes) {
                    Ok(begin) => self.push_state(StateBody::SnapshotBegin(begin), provenance),
                    Err(_) => self.skipped.undecodable += 1,
                }
            }
            SnapshotLevel::TYPE_ID => {
                self.skipped.snapshot += 1;
                match SnapshotLevel::decode(bytes) {
                    Ok(level) => self.push_state(StateBody::SnapshotLevel(level), provenance),
                    Err(_) => self.skipped.undecodable += 1,
                }
            }
            SnapshotEnd::TYPE_ID => {
                self.skipped.snapshot += 1;
                match SnapshotEnd::decode(bytes) {
                    Ok(end) => self.push_state(StateBody::SnapshotEnd(end), provenance),
                    Err(_) => self.skipped.undecodable += 1,
                }
            }
            InstrumentReset::TYPE_ID => {
                self.skipped.reset += 1;
                match InstrumentReset::decode(bytes) {
                    Ok(reset) => self.push_state(StateBody::Reset(reset), provenance),
                    Err(_) => self.skipped.undecodable += 1,
                }
            }
            _ => self.skipped.unknown_type += 1,
        }
    }

    fn push(&mut self, body: MessageBody, source_id: u16, provenance: WireProvenance) {
        self.observed_source_ids.insert(source_id);
        self.messages.push(WireMessage { body, provenance });
    }

    /// A state message never touches `observed_source_ids`.
    ///
    /// None of the four carries a `Source ID` on the wire, so a capture that
    /// counted them would be counting a value it invented. That set is what
    /// refuses an archive holding two publishers, and a refusal has to rest on
    /// what was actually read.
    fn push_state(&mut self, body: StateBody, provenance: WireProvenance) {
        self.state.push(StateMessage { body, provenance });
    }

    /// The reference-data messages, in the order the archive holds them, each
    /// with the position it was carried at.
    #[must_use]
    pub fn reference_messages(&self) -> &[ReferenceMessage] {
        &self.reference
    }

    /// The state messages, in the order the archive holds them.
    ///
    /// Order is the whole contract here: a snapshot cycle is a `SnapshotBegin`,
    /// its levels and a `SnapshotEnd`, and a consumer reads it as a run. Nothing
    /// groups them, because grouping would have to decide what an incomplete
    /// cycle is and that is the consumer's judgement, not the walk's.
    #[must_use]
    pub fn state_messages(&self) -> &[StateMessage] {
        &self.state
    }

    /// The messages, in the order the archive holds them.
    #[must_use]
    pub fn messages(&self) -> &[WireMessage] {
        &self.messages
    }

    /// The published set, as reconstructed from the definitions the archive
    /// carried.
    #[must_use]
    pub const fn refdata(&self) -> &ArchivedRefdata {
        &self.refdata
    }

    /// What was read and not joined.
    #[must_use]
    pub const fn skipped(&self) -> Skipped {
        self.skipped
    }

    /// How many datagrams the sources yielded.
    #[must_use]
    pub const fn datagrams(&self) -> u64 {
        self.datagrams
    }

    /// The publisher's identity, as the archive states it.
    ///
    /// **Reconstructed, not configured.** The `Source ID` is on the wire in
    /// every message this comparison joins and in every `InstrumentDefinition`
    /// at schema 3, so taking it from a configuration file would be the same
    /// mistake as taking the exponents from a live registry: a re-lowering
    /// stamped with today's identity over an archive written under another one
    /// reports a field difference on every message, and a re-lowering stamped
    /// with the *wrong* identity that happens to match reports nothing.
    ///
    /// The definitions are preferred over the messages because they are the
    /// publisher's own statement of who it is; the messages are the fallback for
    /// a window whose refdata port role was not recorded.
    ///
    /// # Errors
    ///
    /// [`RelowerError::AmbiguousSourceId`] when the capture holds two — which is
    /// two publishers on one channel, a finding for another tier and not
    /// something to pick from. [`RelowerError::NoSourceIdInArchive`] when it
    /// holds none the registry admits, which includes the `0` a schema-1
    /// definition leaves in a field that did not exist yet.
    pub fn source_id(&self) -> Result<SourceId, RelowerError> {
        let from_definitions: BTreeSet<u16> =
            self.refdata.source_ids().filter(|id| *id != 0).collect();
        let candidates = if from_definitions.is_empty() {
            &self.observed_source_ids
        } else {
            &from_definitions
        };

        let mut admitted = candidates.iter().filter_map(|id| SourceId::new(*id));
        let first = admitted
            .next()
            .ok_or_else(|| RelowerError::NoSourceIdInArchive {
                found: candidates.iter().copied().collect(),
            })?;
        if let Some(second) = admitted.next() {
            return Err(RelowerError::AmbiguousSourceId {
                first: first.get(),
                second: second.get(),
            });
        }
        Ok(first)
    }

    /// The caveats the wire side owes: the reference data's, plus what the
    /// datagram headers say about the window.
    #[must_use]
    pub fn caveats(&self) -> Vec<Caveat> {
        let mut caveats = self.refdata.caveats().to_vec();
        for (channel_id, eras) in &self.eras {
            if eras.len() > 1 {
                caveats.push(Caveat::EraChangeInsideWindow {
                    channel_id: *channel_id,
                });
            }
        }
        caveats
    }
}
