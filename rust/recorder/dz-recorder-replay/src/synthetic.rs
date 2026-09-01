//! A synthetic publisher: a known datagram stream, straight into a [`Sink`].
//!
//! No socket, no privileges and no network — every test that uses it runs in CI.
//! The datagrams carry a real 24-byte datagram header, built at the offsets
//! `dz-edge-core`'s builder writes and read back by nobody here, so the
//! archive's coverage rows are exercised without a decoder anywhere in the
//! record path.
//!
//! The faults are the design's list, and each is emitted verbatim: what a
//! recorder must never do is repair, normalise or drop one of them, because the
//! datagram most worth having in an archive is the one nothing can explain.

use std::net::{Ipv4Addr, SocketAddrV4};

use dz_edge_core::{AppMessage, Heartbeat, PortRole, DATAGRAM_HEADER_SIZE, SCHEMA_VERSION};
use dz_recorder_core::{RecvTsKind, Sink, SinkError};

use crate::owned::OwnedDatagram;

/// The delimiter the synthetic stream carries.
///
/// Deliberately not any real feed's `Magic`. The record path never compares it,
/// and a synthetic stream that borrowed a live feed's delimiter would invite a
/// reader to treat replayed test traffic as that feed's own.
pub const SYNTHETIC_MAGIC: u16 = 0x5A53;

/// A schema generation no build implements, for [`Fault::UnknownSchemaVersion`].
pub const UNKNOWN_SCHEMA_VERSION: u8 = 0xFE;

/// A declared length above the mandated cap, for
/// [`Fault::OversizedDeclaredLength`].
pub const OVERSIZED_DECLARED_LEN: u16 = 9000;

/// A stamp with all nine digits populated, so a writer or a reader that rounds
/// to microseconds cannot pass a comparison against it.
pub const FIRST_RECV_TS_NS: u64 = 1_700_000_000_123_456_789;

/// Documentation-range addresses (RFC 5737 and RFC 6676). The repository is
/// public and a placeholder that looks like a real host is a leak waiting to be
/// copied into a config.
pub const PRIMARY_SOURCE: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 10);
pub const SECOND_SOURCE: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 11);
pub const GROUP: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 10);

const SOURCE_PORT: u16 = 50_000;
const MKTDATA_PORT: u16 = 40_000;

/// The port a role's datagrams arrive on.
///
/// One port per role, because the archive now records which port each role was
/// joined on: a fixture that sent every role to one port would state a join
/// intent no reader could map back to a coverage row, which is the confusion
/// that intent exists to remove.
#[must_use]
pub const fn port_for(role: PortRole) -> u16 {
    match role {
        PortRole::Mktdata => MKTDATA_PORT,
        PortRole::Refdata => MKTDATA_PORT + 1,
        PortRole::Snapshot => MKTDATA_PORT + 2,
    }
}
/// Observed, and non-zero: a synthesised zero is *not observed*, and a stream
/// that used zero could not tell the two apart on replay.
const TTL: u8 = 8;

/// The channel the clean stream advances.
pub const CHANNEL_ID: u8 = 1;
/// Configured, and silent, for [`Fault::SilentChannel`].
pub const SILENT_CHANNEL_ID: u8 = 2;

/// The faults an archive must carry through untouched.
///
/// Each is a thing a real publisher, a real network or a real recorder does. A
/// recorder that parsed would drop several of them, and the evidence needed to
/// diagnose the bug would be exactly what the bug destroyed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Nothing injected: contiguous sequence numbers on one channel instance.
    None,
    /// The publisher skipped a run of sequence numbers.
    SequenceGap,
    /// A sequence number below the one before it, with no reset behind it.
    BackwardMotion,
    /// `Reset Count` advances and the sequence space restarts, which is not
    /// backward motion and must not be read as it.
    ResetCountAdvance,
    /// A second publisher appears on the same channel and port, advancing its
    /// own sequence space.
    NewSourceAddress,
    /// One of two publishers stops. Silence from a source that was there is a
    /// finding; silence from one that never was is not.
    SourceAddressDisappears,
    /// The same datagram delivered twice.
    Duplicate,
    /// Two adjacent datagrams arriving in the wrong order.
    ReorderedPair,
    /// A declared datagram length above the mandated cap. A decoder rejects it;
    /// the archive must hold it.
    OversizedDeclaredLength,
    /// A schema version this build does not implement. Same reason, more so.
    UnknownSchemaVersion,
    /// A configured channel that emits nothing at all. No data looks exactly
    /// like a clean feed, which is why the intent is recorded elsewhere.
    SilentChannel,
}

/// A window during which the capture handle was starved.
///
/// The datagrams in it never reach the sink, and the next one that does carries
/// their count in its `drop_delta` — which is exactly what `SO_RXQ_OVFL` reports
/// after a paused drain thread resumes. A window with nothing after it has
/// nothing to carry its count, so a stream that means to admit its losses does
/// not end inside one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StarvationWindow {
    /// Index into the stream the publisher emitted, not into what was written.
    pub first: usize,
    pub count: usize,
}

/// Emits a known datagram stream straight into a [`Sink`], with no socket
/// anywhere, which is what makes the whole record path exercisable in CI.
///
/// Bodies are real heartbeats built with `dz-edge-core`'s *encoder*: nothing
/// here decodes, and the payloads are still decodable by whatever analyses them
/// later.
///
/// [`Sink`]: dz_recorder_core::Sink
#[derive(Debug, Clone)]
pub struct SyntheticPublisher {
    datagram_count: usize,
    fault: Fault,
    role: PortRole,
    starvation: Vec<StarvationWindow>,
    /// Every `fallback_every`-th datagram is stamped by the application rather
    /// than by the kernel, so the per-block exception is exercised rather than
    /// assumed.
    fallback_every: usize,
}

impl SyntheticPublisher {
    /// A contiguous stream on one channel instance, all stamps from the kernel.
    #[must_use]
    pub fn clean(datagram_count: usize) -> Self {
        Self {
            datagram_count,
            fault: Fault::None,
            role: PortRole::Mktdata,
            starvation: Vec::new(),
            fallback_every: 97,
        }
    }

    #[must_use]
    pub fn with_fault(datagram_count: usize, fault: Fault) -> Self {
        Self {
            fault,
            ..Self::clean(datagram_count)
        }
    }

    /// The port role every datagram carries.
    ///
    /// The body is the `mktdata` heartbeat whatever this says: on another role
    /// the stream exercises the archive's role plumbing, not a spec-valid feed.
    #[must_use]
    pub fn on_role(mut self, role: PortRole) -> Self {
        self.role = role;
        self
    }

    #[must_use]
    pub fn starved(mut self, windows: &[StarvationWindow]) -> Self {
        self.starvation = windows.to_vec();
        self
    }

    /// Exactly what reaches the sink, in order.
    ///
    /// Starved datagrams are absent and their count rides on the next one that
    /// survived, so this is both the stream and the claim about it.
    #[must_use]
    pub fn datagrams(&self) -> Vec<OwnedDatagram> {
        let emitted = self.emitted();
        let mut out = Vec::with_capacity(emitted.len());
        let mut pending_drops = 0u32;
        for (i, mut dg) in emitted.into_iter().enumerate() {
            if self.starved_at(i) {
                pending_drops += 1;
                continue;
            }
            dg.drop_delta = std::mem::take(&mut pending_drops);
            out.push(dg);
        }
        out
    }

    /// Writes the stream into a sink and returns what was written.
    ///
    /// Straight into the [`Sink`], because no socket is wanted: the archive is
    /// the thing under test, and a loopback group would add a kernel to it.
    pub fn publish_into<S: Sink>(&self, sink: &mut S) -> Result<Vec<OwnedDatagram>, SinkError> {
        let datagrams = self.datagrams();
        for dg in &datagrams {
            sink.write(&dg.as_recorded())?;
        }
        sink.flush()?;
        Ok(datagrams)
    }

    fn starved_at(&self, index: usize) -> bool {
        self.starvation
            .iter()
            .any(|w| index >= w.first && index < w.first + w.count)
    }

    /// The stream as the publisher and the network produced it, before any
    /// starvation of ours removed anything from it.
    fn emitted(&self) -> Vec<OwnedDatagram> {
        let n = self.datagram_count;
        let mut out = Vec::with_capacity(n + 1);
        let mut seq = 0u64;
        let mut reset_count = 0u8;
        let mut second_seq = 0u64;

        for i in 0..n {
            match self.fault {
                // A run of sequence numbers the publisher never sent. The
                // datagram count is unchanged: what is missing is in the
                // numbering, which is the only place a reader can see it.
                Fault::SequenceGap if i == n / 2 => seq += 7,
                Fault::BackwardMotion if i == n / 2 => {
                    out.push(self.datagram(i, seq.saturating_sub(3), reset_count, PRIMARY_SOURCE));
                    continue;
                }
                Fault::ResetCountAdvance if i == n / 2 => {
                    reset_count += 1;
                    seq = 0;
                }
                Fault::NewSourceAddress if i > n / 2 && i % 2 == 1 => {
                    out.push(self.datagram(i, second_seq, 0, SECOND_SOURCE));
                    second_seq += 1;
                    continue;
                }
                Fault::SourceAddressDisappears if i < n / 2 && i % 2 == 1 => {
                    out.push(self.datagram(i, second_seq, 0, SECOND_SOURCE));
                    second_seq += 1;
                    continue;
                }
                Fault::Duplicate if i == n / 2 => {
                    // Delivered twice, byte for byte, including its stamp: a
                    // recorder that de-duplicated would destroy the evidence
                    // that the network duplicates.
                    let dg = self.datagram(i, seq, reset_count, PRIMARY_SOURCE);
                    out.push(dg.clone());
                    out.push(dg);
                    seq += 1;
                    continue;
                }
                Fault::ReorderedPair if i == n / 2 => {
                    out.push(self.datagram(i, seq + 1, reset_count, PRIMARY_SOURCE));
                    out.push(self.datagram(i + 1, seq, reset_count, PRIMARY_SOURCE));
                    seq += 2;
                    continue;
                }
                _ => {}
            }
            out.push(self.datagram(i, seq, reset_count, PRIMARY_SOURCE));
            seq += 1;
        }

        // Two faults are properties of the whole stream rather than of one
        // datagram in it.
        match self.fault {
            Fault::ReorderedPair => {
                // The pair was emitted together, so one index went unused.
                out.truncate(n);
            }
            Fault::SilentChannel => {
                // SILENT_CHANNEL_ID is configured and emits nothing. There is
                // nothing to add: the absence is the fault, and only the
                // manifest's statement of intent can distinguish it from a
                // channel that was never asked for.
            }
            _ => {}
        }
        out
    }

    fn datagram(&self, index: usize, seq: u64, reset_count: u8, source: Ipv4Addr) -> OwnedDatagram {
        let schema_version = match self.fault {
            Fault::UnknownSchemaVersion if index.is_multiple_of(10) => UNKNOWN_SCHEMA_VERSION,
            _ => SCHEMA_VERSION,
        };
        let declared_len = match self.fault {
            Fault::OversizedDeclaredLength if index.is_multiple_of(10) => {
                Some(OVERSIZED_DECLARED_LEN)
            }
            _ => None,
        };
        let recv_ts_ns = FIRST_RECV_TS_NS + index as u64 * 1_000_037;
        let recv_ts_kind = if self.fallback_every != 0 && index.is_multiple_of(self.fallback_every)
        {
            RecvTsKind::ApplicationFallback
        } else {
            RecvTsKind::KernelSoftware
        };

        let payload = datagram_bytes(
            CHANNEL_ID,
            seq,
            reset_count,
            schema_version,
            declared_len,
            recv_ts_ns,
        );
        let payload_len = payload.len();

        OwnedDatagram {
            payload,
            src: SocketAddrV4::new(source, SOURCE_PORT),
            dst: SocketAddrV4::new(GROUP, port_for(self.role)),
            role: self.role,
            recv_ts_ns,
            recv_ts_kind,
            // Set by `datagrams`, which is where the starvation windows are.
            drop_delta: 0,
            ttl: Some(TTL),
            // The synthetic publisher stands in for socket mode: it emits a
            // payload and nothing that was ever on a wire, so the archive
            // synthesises the header and the wire length is what we hold.
            link_headers: None,
            wire_payload_len: u32::try_from(payload_len).unwrap_or(u32::MAX),
        }
    }
}

/// One datagram: the 24-byte header, then one heartbeat.
///
/// The header is written at the offsets the spec's table states — `Magic` 0,
/// `Schema Version` 2, `Channel ID` 3, `Sequence Number` 4, `Send Timestamp` 12,
/// `Message Count` 20, `Reset Count` 21, `Frame Length` 22 — by hand rather than
/// through the builder, so `declared_len` can state a length the builder would
/// never produce. Everything is little-endian, as the wire is.
#[must_use]
pub fn datagram_bytes(
    channel_id: u8,
    sequence_number: u64,
    reset_count: u8,
    schema_version: u8,
    declared_len: Option<u16>,
    send_timestamp_ns: u64,
) -> Vec<u8> {
    let mut buf = vec![0u8; DATAGRAM_HEADER_SIZE + Heartbeat::SIZE];
    Heartbeat {
        channel_id,
        timestamp_ns: send_timestamp_ns,
    }
    .encode_into(&mut buf[DATAGRAM_HEADER_SIZE..]);

    let len = declared_len.unwrap_or_else(|| u16::try_from(buf.len()).expect("a small constant"));
    buf[0..2].copy_from_slice(&SYNTHETIC_MAGIC.to_le_bytes());
    buf[2] = schema_version;
    buf[3] = channel_id;
    buf[4..12].copy_from_slice(&sequence_number.to_le_bytes());
    buf[12..20].copy_from_slice(&send_timestamp_ns.to_le_bytes());
    buf[20] = 1;
    buf[21] = reset_count;
    buf[22..24].copy_from_slice(&len.to_le_bytes());
    buf
}
