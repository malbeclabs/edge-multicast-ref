//! One received datagram, the identity a datagram is tracked under, and the
//! scope its admitted losses may be subtracted at.

use dz_edge_core::PortRole;
use std::net::{Ipv4Addr, SocketAddrV4};

/// How [`RecordedDatagram::recv_ts_ns`] was obtained.
///
/// Carried rather than assumed: a stamp the kernel did not produce must not be
/// mistaken for one it did. A latency computed from an application-level
/// fallback is measuring the recorder's own scheduler, and an archive that
/// cannot say which kind it holds cannot be trusted for latency at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvTsKind {
    /// `SO_TIMESTAMPNS`, or libpcap's nanosecond precision.
    KernelSoftware,
    /// The control message was absent and we stamped it ourselves.
    ApplicationFallback,
}

/// One received datagram and everything known about its arrival.
///
/// The payload is borrowed from the receive buffer. The record path does not
/// allocate per datagram.
#[derive(Debug)]
pub struct RecordedDatagram<'a> {
    pub payload: &'a [u8],
    /// Together with the `Channel ID` and `dst` port, the channel-instance
    /// identity.
    pub src: SocketAddrV4,
    /// Group and port.
    pub dst: SocketAddrV4,
    pub role: PortRole,
    pub recv_ts_ns: u64,
    pub recv_ts_kind: RecvTsKind,
    /// Datagrams the capture handle lost between the previous one and this one.
    ///
    /// This is the quantity pcapng's `epb_dropcount` is defined as, so loss
    /// attribution travels inside the archive rather than beside it.
    pub drop_delta: u32,
    /// `None` when the capture mode did not observe it — never zero for
    /// *not observed*, because zero is a TTL a datagram can actually carry.
    pub ttl: Option<u8>,
    /// The Ethernet, IPv4 and UDP bytes exactly as they arrived, when the
    /// capture mode read them off the interface. `None` means the mode saw only
    /// a payload and the archive must synthesise a header around it.
    ///
    /// Carried, rather than rebuilt at the writer from `src`, `dst` and `ttl`,
    /// because rebuilding is what a socket capture has to do and recording the
    /// interface exists precisely to avoid it: the identification field, the
    /// fragmentation flags and the checksums are evidence a subscriber's socket
    /// discards, and an archive that reconstructs them cannot tell a reader
    /// whether a datagram was fragmented or delivered twice. Borrowed, so
    /// carrying them costs no allocation.
    pub link_headers: Option<&'a [u8]>,
    /// The payload's length on the wire, which exceeds `payload.len()` when the
    /// capture length cut it short.
    ///
    /// pcapng distinguishes the captured length from the original length, and
    /// declaring them equal asserts *not truncated*. A datagram over the
    /// mandated cap is a publisher violation worth recording as one; archiving
    /// its first 1232 bytes as though that were the whole thing turns the
    /// violation into a clean datagram, and discarding it turns the violation
    /// into a sequence gap the publisher is then blamed for.
    pub wire_payload_len: u32,
}

/// The scope [`RecordedDatagram::drop_delta`] may be subtracted at.
///
/// A ring counts frames dropped *before* demultiplexing, so its loss belongs to
/// the capture handle and to no port role in particular. Charging it to the role
/// of the datagram that happened to arrive next is a guess: forty dropped
/// mktdata frames land on refdata, and an analysis tier subtracting per-role
/// capture drops from per-role sequence gaps then reads a forty-datagram mktdata
/// gap with nothing admitted behind it — the false publisher-loss finding this
/// design exists to prevent. We cannot know which role's frames a ring dropped,
/// and a guess recorded as a number is worse than a stated scope.
///
/// Socket mode really does hold one accumulator per role, because it holds one
/// socket per role. Which of the two applies is configured rather than inferred,
/// so whatever wires up a recorder states it, the archive carries it in the
/// section header and in the manifest, and the analysis tier subtracts under it.
///
/// It lives beside the delta it qualifies, and not in the crate that writes the
/// archive: a number and the scope it is valid at are one fact, and a tier that
/// can name the delta but not the scope has to invent a second taxonomy for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureDropScope {
    /// One loss accumulator per port role, as socket mode has. A per-instance
    /// sum of `drop_delta` is a valid subtraction.
    PortRole,
    /// One loss accumulator for every role on the handle, as a ring has. Per
    /// instance the number means nothing, so no per-instance subtraction is
    /// valid at this scope.
    CaptureHandle,
}

impl CaptureDropScope {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PortRole => "port-role",
            Self::CaptureHandle => "capture-handle",
        }
    }
}

/// The only correct key for anything that tracks a sequence space.
///
/// An operator may run two publishers serving the same `Channel ID` to the same
/// group and port, each advancing its own sequence space and its own
/// `Reset Count`. A tracker keyed any less finely reads every alternation as
/// backward motion in one direction, and lets one publisher's heartbeats cover
/// the other's total outage in the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChannelInstance {
    pub source: Ipv4Addr,
    pub channel_id: u8,
    pub dst_port: u16,
}

impl ChannelInstance {
    #[must_use]
    pub const fn new(source: Ipv4Addr, channel_id: u8, dst_port: u16) -> Self {
        Self {
            source,
            channel_id,
            dst_port,
        }
    }
}
