//! An owned datagram, for the callers that cannot hold a borrow.
//!
//! [`RecordedDatagram`] borrows its payload from the receive buffer, which is
//! what keeps the record path allocation-free. Three callers cannot live with
//! that: an archive iterator has to hand out values that outlive the reader's
//! buffer, a test comparing what was emitted against what came back has to hold
//! both streams at once, and a hand-off between threads has to own what it
//! carries.
//!
//! It lives beside [`RecordedDatagram`] rather than in the replay crate, where
//! the first two callers put it, because the third is a live capture handing
//! datagrams to a derivation in the same process — and reaching the type through
//! the archive reader would have pulled a pcapng parser and a decompressor into
//! a path that reads no archive at all.

use std::net::SocketAddrV4;

use crate::{RecordedDatagram, RecvTsKind};
use dz_edge_core::PortRole;

/// The same fields as [`RecordedDatagram`], owning its payload.
///
/// Field-for-field identical on purpose: the round trip compares whole values,
/// so a field added to the borrowed form is compared without anyone having to
/// remember to add it to an assertion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedDatagram {
    pub payload: Vec<u8>,
    pub src: SocketAddrV4,
    pub dst: SocketAddrV4,
    pub role: PortRole,
    pub recv_ts_ns: u64,
    pub recv_ts_kind: RecvTsKind,
    pub drop_delta: u32,
    /// `None` means *not observed*, and never zero. A synthesised header has
    /// nowhere to write *absent*, so a zero in the archive's IPv4 header of a
    /// synthesised section comes back as `None` rather than as a TTL of zero
    /// somebody will later average.
    pub ttl: Option<u8>,
    /// The Ethernet, IPv4 and UDP bytes as they arrived, when the archive
    /// vouches for them. `None` means they were synthesised and are therefore
    /// not evidence about the wire.
    pub link_headers: Option<Vec<u8>>,
    /// What was sent, which exceeds `payload.len()` when the capture length cut
    /// the datagram short.
    pub wire_payload_len: u32,
}

impl OwnedDatagram {
    #[must_use]
    pub fn from_recorded(dg: &RecordedDatagram<'_>) -> Self {
        Self {
            payload: dg.payload.to_vec(),
            src: dg.src,
            dst: dg.dst,
            role: dg.role,
            recv_ts_ns: dg.recv_ts_ns,
            recv_ts_kind: dg.recv_ts_kind,
            drop_delta: dg.drop_delta,
            ttl: dg.ttl,
            link_headers: dg.link_headers.map(<[u8]>::to_vec),
            wire_payload_len: dg.wire_payload_len,
        }
    }

    /// Borrows it back, so a synthetic stream reaches a [`Sink`] through exactly
    /// the type a live capture would use.
    ///
    /// [`Sink`]: crate::Sink
    #[must_use]
    pub fn as_recorded(&self) -> RecordedDatagram<'_> {
        RecordedDatagram {
            payload: &self.payload,
            src: self.src,
            dst: self.dst,
            role: self.role,
            recv_ts_ns: self.recv_ts_ns,
            recv_ts_kind: self.recv_ts_kind,
            drop_delta: self.drop_delta,
            ttl: self.ttl,
            link_headers: self.link_headers.as_deref(),
            wire_payload_len: self.wire_payload_len,
        }
    }
}
