//! One pcapng segment: a section header, an interface per port role, and an
//! Enhanced Packet Block per datagram.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io::{self, Write};
use std::net::Ipv4Addr;
use std::time::Duration;

use dz_edge_core::{PortRole, MAX_DATAGRAM_SIZE};
use dz_recorder_core::{
    ChannelInstance, RecordedDatagram, RecorderIdentity, RecvTsKind, SinkError,
};
use pcap_file::pcapng::blocks::enhanced_packet::{EnhancedPacketBlock, EnhancedPacketOption};
use pcap_file::pcapng::blocks::interface_description::{
    InterfaceDescriptionBlock, InterfaceDescriptionOption,
};
use pcap_file::pcapng::blocks::interface_statistics::{
    InterfaceStatisticsBlock, InterfaceStatisticsOption,
};
use pcap_file::pcapng::blocks::section_header::{SectionHeaderBlock, SectionHeaderOption};
use pcap_file::pcapng::{PcapNgBlock, PcapNgWriter};
use pcap_file::{DataLink, Endianness};

use crate::manifest::{CoverageTracker, InstanceCoverage, JoinedRole};

/// The scope a segment's capture-drop totals may be subtracted at, re-exported
/// so a caller that configures this writer names it from here.
///
/// It is defined beside the `drop_delta` it qualifies rather than here, because
/// the analysis tier subtracts under it and must not link a writer to say so.
pub use dz_recorder_core::CaptureDropScope;

/// 14 bytes of Ethernet, 20 of IPv4, 8 of UDP.
pub const LINK_HEADER_LEN: usize = 42;

/// The longest link headers a capture can hand over: Ethernet, an IPv4 header
/// carrying every option it has room for, and UDP.
///
/// `LINK_HEADER_LEN` is the synthesised case and not a bound. `AF_PACKET` mode
/// slices the headers off the frame as they arrived, so an IPv4 header with
/// options is longer, and a scratch buffer sized for the synthesised case
/// reallocates on the record path for exactly those datagrams.
pub const MAX_LINK_HEADER_LEN: usize = dz_recorder_core::MAX_LINK_HEADER_SIZE;

/// Every port role, in the order that fixes `interface_id`.
///
/// All three interfaces are described in every segment whatever the recorder
/// joined, so `interface_id` means the same thing in every segment of every run
/// and a reader maps it to a port role without reading options at all. Which of
/// them was actually joined is the manifest's business.
pub const ALL_ROLES: [PortRole; 3] = [PortRole::Mktdata, PortRole::Refdata, PortRole::Snapshot];

/// The `recv_ts_kind` a section is assumed to hold. A datagram that fell back
/// to an application stamp is marked on itself, because the exception is what
/// needs saying.
const SECTION_RECV_TS_KIND: &str = "kernel-software";
const FALLBACK_MARK: &str = "recv_ts_kind=application-fallback";

/// Whether the 42 bytes in front of each payload were observed or invented.
///
/// This is the capture mode's claim, stated once in the section header before
/// any datagram is written. A datagram whose own headers disagree with it —
/// `AF_PACKET` mode that had to fall back to a payload, or socket mode handed
/// captured bytes — is marked on itself and counted in the manifest, the way the
/// stamp kind already works. The section cannot know the exception in advance,
/// and a claim the datagrams contradict must not be able to pass silently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkHeaders {
    /// `AF_PACKET` mode: the fields were on the wire.
    Captured,
    /// Socket mode: the fields are assembled from what the socket reported, and
    /// no reader may mistake one for a captured field.
    Synthesised,
}

impl LinkHeaders {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Captured => "captured",
            Self::Synthesised => "synthesised",
        }
    }

    /// The per-datagram exception, in the same key=value form the section states
    /// its claim in, so one reader parses both.
    const fn mark(self) -> &'static str {
        match self {
            Self::Captured => "link_headers=captured",
            Self::Synthesised => "link_headers=synthesised",
        }
    }
}

/// What an `isb_osdrop` written at capture-handle scope is, on the block that
/// carries it. Repeating one measured total across the interfaces is not a
/// per-role attribution, and a reader summing them would invent loss.
const HANDLE_SCOPE_MARK: &str = "capture_drop_scope=capture-handle; isb_osdrop is the capture \
     handle's total for this segment, the same value on every interface, and must not be summed \
     across them";

/// One port role the recorder was asked to join, and where.
///
/// The group, the port and the interface travel with the role because the
/// archive's whole claim about an absent feed rests on them: a snapshot port
/// that was joined on the wrong port is silent in exactly the way a snapshot
/// port that was never joined is, and a reader holding only the role cannot
/// tell those apart or map a coverage row's port back to a stated intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleJoin {
    pub role: PortRole,
    pub group: Ipv4Addr,
    pub port: u16,
    /// The interface the join named. `None` when it was left to route
    /// discovery — absent rather than invented, because an interface the
    /// recorder cannot name is not one it may claim.
    pub interface: Option<String>,
    /// The local source address at join time, when the join reported one.
    pub source: Option<Ipv4Addr>,
}

impl RoleJoin {
    /// The join as a configuration states it, before an interface or a source
    /// address has been observed.
    ///
    /// The group and the port are arguments and not defaults, because a join
    /// with neither is not a join a reader can check anything against.
    #[must_use]
    pub const fn on(role: PortRole, group: Ipv4Addr, port: u16) -> Self {
        Self {
            role,
            group,
            port,
            interface: None,
            source: None,
        }
    }

    /// The manifest's row, so a reader answers the coverage question without
    /// opening the object.
    #[must_use]
    pub fn as_row(&self) -> JoinedRole {
        JoinedRole {
            role: self.role.as_str().to_owned(),
            group: self.group,
            port: self.port,
            interface: self.interface.clone(),
            source: self.source,
        }
    }

    /// What the Interface Description block says, key=value because replay and
    /// an operator read the same string.
    fn describe(&self) -> String {
        let mut out = format!(
            "port_role={}; joined=true; group={}; port={}",
            self.role.as_str(),
            self.group,
            self.port
        );
        if let Some(interface) = &self.interface {
            out.push_str("; interface=");
            out.push_str(interface);
        }
        if let Some(source) = self.source {
            out.push_str(&format!("; source={source}"));
        }
        out
    }
}

#[derive(Debug, Clone)]
pub struct SegmentWriterConfig {
    pub identity: RecorderIdentity,
    /// What the recorder was asked to join, and where, in any order: a port that
    /// was never joined produces no data, and no data looks exactly like a clean
    /// feed.
    pub roles_joined: Vec<RoleJoin>,
    pub link_headers: LinkHeaders,
    /// The scope the segment's capture-drop totals may be subtracted at, stated
    /// rather than derived: the capture handles know it and nothing else can.
    pub capture_drop_scope: CaptureDropScope,
}

/// What one closed segment amounts to, from counters held while it was open.
#[derive(Debug, Clone, Default)]
pub struct SegmentStats {
    pub datagram_count: u64,
    pub payload_byte_count: u64,
    pub start_ns: u64,
    pub end_ns: u64,
    pub capture_drop_total: u64,
    pub interface_drop_total: u64,
    pub short_datagrams: u64,
    pub instances_dropped: u64,
    /// Datagrams whose link-header provenance contradicted the section's claim.
    pub link_header_exceptions: u64,
    pub instances: BTreeMap<ChannelInstance, InstanceCoverage>,
}

#[derive(Debug, Default, Clone, Copy)]
struct RoleCounters {
    received: u64,
    capture_drops: u64,
    interface_drops: u64,
}

pub struct SegmentWriter<W: Write> {
    inner: PcapNgWriter<Counted<W>>,
    roles_joined: Vec<PortRole>,
    /// The section's claim, kept so a datagram that contradicts it can say so.
    link_headers: LinkHeaders,
    link_header_exceptions: u64,
    capture_drop_scope: CaptureDropScope,
    /// Reused across datagrams: the record path does not allocate per datagram.
    scratch: Vec<u8>,
    per_role: [RoleCounters; ALL_ROLES.len()],
    /// Every drop delta the segment saw, whatever role carried it. This is the
    /// quantity the capture handle actually measured, and at capture-handle
    /// scope it is the only one there is.
    capture_drops: u64,
    coverage: CoverageTracker,
    datagram_count: u64,
    payload_byte_count: u64,
    first_ns: Option<u64>,
    last_ns: u64,
}

impl<W: Write> SegmentWriter<W> {
    pub fn new(writer: W, cfg: &SegmentWriterConfig) -> Result<Self, SinkError> {
        let section = SectionHeaderBlock {
            // Stated by the byte-order magic, and not the host's: a reader on
            // any machine sees the bytes that arrived.
            endianness: Endianness::Little,
            options: section_options(cfg),
            ..Default::default()
        };
        // Through a counting writer, because pcap-file writes the section
        // header inside this constructor and reports no length for it: counting
        // at the writer is the only count that includes it, and `rotate_bytes`
        // is a bound on the segment on disk — the section header's few hundred
        // bytes of provenance included.
        let mut inner = PcapNgWriter::with_section_header(Counted::new(writer), section)
            .map_err(encode_error)?;

        // The maximum link header, not the minimum: a captured AF_PACKET
        // datagram at the cap with IPv4 options is 1314 bytes of block data, and
        // a declared snaplen below what a block actually holds is a
        // contradiction a strict reader is entitled to flag.
        let snaplen = u32::try_from(MAX_DATAGRAM_SIZE + MAX_LINK_HEADER_LEN)
            .expect("the datagram cap is a small constant");
        for role in ALL_ROLES {
            let join = cfg.roles_joined.iter().find(|j| j.role == role);
            let idb = InterfaceDescriptionBlock {
                linktype: DataLink::ETHERNET,
                snaplen,
                options: vec![
                    InterfaceDescriptionOption::IfName(Cow::Borrowed(role.as_str())),
                    InterfaceDescriptionOption::IfDescription(Cow::Owned(match join {
                        Some(join) => join.describe(),
                        // No group and no port, rather than zeros: a role
                        // nobody joined has no address to state.
                        None => format!("port_role={}; joined=false", role.as_str()),
                    })),
                    // pcapng's default is 10^-6. A recorder taking kernel
                    // nanosecond stamps and writing them at microsecond
                    // resolution silently discards the three digits the whole
                    // latency argument rests on.
                    InterfaceDescriptionOption::IfTsResol(9),
                ],
            };
            inner.write_pcapng_block(idb).map_err(encode_error)?;
        }

        Ok(Self {
            inner,
            roles_joined: ALL_ROLES
                .into_iter()
                .filter(|role| cfg.roles_joined.iter().any(|j| j.role == *role))
                .collect(),
            link_headers: cfg.link_headers,
            link_header_exceptions: 0,
            capture_drop_scope: cfg.capture_drop_scope,
            scratch: Vec::with_capacity(MAX_DATAGRAM_SIZE + MAX_LINK_HEADER_LEN),
            per_role: [RoleCounters::default(); ALL_ROLES.len()],
            capture_drops: 0,
            coverage: CoverageTracker::default(),
            datagram_count: 0,
            payload_byte_count: 0,
            first_ns: None,
            last_ns: 0,
        })
    }

    pub fn write(&mut self, dg: &RecordedDatagram<'_>) -> Result<(), SinkError> {
        self.coverage.observe(dg);

        let claimed = self.link_headers;
        let scratch = &mut self.scratch;
        scratch.clear();
        let (link_len, observed) = match dg.link_headers {
            // Preferred whenever the capture mode read them off the interface:
            // the identification field, the fragmentation flags and the
            // checksums are evidence a rebuild cannot produce, and recording
            // the interface exists precisely to keep them.
            Some(headers) => {
                scratch.extend_from_slice(headers);
                (headers.len(), LinkHeaders::Captured)
            }
            None => {
                synthesise_link_headers(scratch, dg);
                (LINK_HEADER_LEN, LinkHeaders::Synthesised)
            }
        };
        scratch.extend_from_slice(dg.payload);

        let mut options = Vec::new();
        if dg.drop_delta != 0 {
            // Only when non-zero: a zero option on every datagram is noise.
            options.push(EnhancedPacketOption::DropCount(u64::from(dg.drop_delta)));
        }
        if dg.recv_ts_kind == RecvTsKind::ApplicationFallback {
            options.push(EnhancedPacketOption::Comment(Cow::Borrowed(FALLBACK_MARK)));
        }
        if observed != claimed {
            options.push(EnhancedPacketOption::Comment(Cow::Borrowed(
                observed.mark(),
            )));
            self.link_header_exceptions += 1;
        }

        let epb = EnhancedPacketBlock {
            interface_id: role_index(dg.role),
            timestamp: Duration::from_nanos(dg.recv_ts_ns),
            // The on-wire length, not the length held: pcapng's original_len
            // equal to the captured length asserts *not truncated*, which turns
            // a publisher's over-cap datagram into a clean one and hides the
            // violation worth recording.
            original_len: u32::try_from(link_len)
                .unwrap_or(u32::MAX)
                .saturating_add(dg.wire_payload_len),
            data: Cow::Borrowed(scratch),
            options,
        };
        self.inner
            .write_block(&epb.into_block())
            .map_err(encode_error)?;

        let per_role_delta = match self.capture_drop_scope {
            CaptureDropScope::PortRole => u64::from(dg.drop_delta),
            // The ring dropped frames before it could tell the roles apart, so
            // this datagram's role is not evidence about whose frames went.
            CaptureDropScope::CaptureHandle => 0,
        };
        let counters = &mut self.per_role[role_index(dg.role) as usize];
        counters.received += 1;
        counters.capture_drops += per_role_delta;
        self.capture_drops += u64::from(dg.drop_delta);

        self.datagram_count += 1;
        self.payload_byte_count += dg.payload.len() as u64;
        self.first_ns = Some(self.first_ns.unwrap_or(dg.recv_ts_ns));
        self.last_ns = dg.recv_ts_ns;
        Ok(())
    }

    /// Loss upstream of the capture point, reported by whatever scrapes the
    /// interface. Its own category, never folded into publisher loss.
    pub fn record_interface_drops(&mut self, role: PortRole, delta: u64) {
        self.per_role[role_index(role) as usize].interface_drops += delta;
    }

    pub fn flush(&mut self) -> Result<(), SinkError> {
        self.inner.get_mut().flush().map_err(SinkError::Io)
    }

    #[must_use]
    pub fn bytes_written(&self) -> u64 {
        self.inner.get_ref().bytes
    }

    #[must_use]
    pub fn datagram_count(&self) -> u64 {
        self.datagram_count
    }

    /// The record path's buffer as it stands.
    ///
    /// Exposed so that "no allocation per datagram" is checkable rather than
    /// asserted in a comment: a captured link header carrying IPv4 options is
    /// longer than the synthesised 42 bytes, and a buffer sized for the short
    /// case grows on the record path for exactly those datagrams.
    #[must_use]
    pub fn scratch_capacity(&self) -> usize {
        self.scratch.capacity()
    }

    /// Appends the Interface Statistics blocks and gives the writer back.
    pub fn finish(mut self) -> Result<(W, SegmentStats), SinkError> {
        let end_ns = self.last_ns;
        for role in std::mem::take(&mut self.roles_joined) {
            let c = self.per_role[role_index(role) as usize];
            let capture_drops = match self.capture_drop_scope {
                CaptureDropScope::PortRole => c.capture_drops,
                CaptureDropScope::CaptureHandle => self.capture_drops,
            };
            let mut options = vec![
                InterfaceStatisticsOption::IsbIfRecv(c.received),
                // Ours, and the quantity a sequence gap is subtracted
                // against before it is reported as publisher loss.
                InterfaceStatisticsOption::IsbOsDrop(capture_drops),
                InterfaceStatisticsOption::IsbIfDrop(c.interface_drops),
            ];
            if self.capture_drop_scope == CaptureDropScope::CaptureHandle {
                // The section states the scope; the block carrying a total that
                // is not this interface's states it again, because a reader
                // reaching for `isb_osdrop` may never read the section comment.
                options.push(InterfaceStatisticsOption::Comment(Cow::Borrowed(
                    HANDLE_SCOPE_MARK,
                )));
            }
            let isb = InterfaceStatisticsBlock {
                interface_id: role_index(role),
                // pcap-file 2.0.0 writes this field with one `write_u64`
                // instead of the spec's Timestamp (High) / Timestamp (Low) pair
                // of 32-bit words — its `enhanced_packet.rs` splits correctly,
                // this block does not. On a little-endian section that puts the
                // low half first, and a conforming reader shows a nonsense
                // capture-end time on every segment. The halves are swapped
                // here so the bytes on disk are the ones the spec asks for; the
                // dependency is pinned at `=2.0.0`, so do not "fix" this back.
                timestamp: end_ns.rotate_left(32),
                options,
            };
            self.inner.write_pcapng_block(isb).map_err(encode_error)?;
        }

        let stats = SegmentStats {
            datagram_count: self.datagram_count,
            payload_byte_count: self.payload_byte_count,
            start_ns: self.first_ns.unwrap_or(0),
            end_ns,
            capture_drop_total: self.capture_drops,
            interface_drop_total: self.per_role.iter().map(|c| c.interface_drops).sum(),
            short_datagrams: self.coverage.short_datagrams(),
            instances_dropped: self.coverage.instances_dropped(),
            link_header_exceptions: self.link_header_exceptions,
            instances: self.coverage.coverage(),
        };

        let mut writer = self.inner.into_inner().inner;
        writer.flush().map_err(SinkError::Io)?;
        Ok((writer, stats))
    }
}

/// `interface_id` is a constant function of the port role, so a reader needs no
/// options to map one and the mapping does not move between segments.
#[must_use]
pub const fn role_index(role: PortRole) -> u32 {
    match role {
        PortRole::Mktdata => 0,
        PortRole::Refdata => 1,
        PortRole::Snapshot => 2,
    }
}

fn section_options(cfg: &SegmentWriterConfig) -> Vec<SectionHeaderOption<'static>> {
    let id = &cfg.identity;
    // Key=value, because this is read by a program at least as often as by a
    // person: replay recovers the stamp kind and the header provenance here.
    let mut comment = format!(
        "site={}; recorder={}; env={}; build_version={}; build_commit={}; config_hash={}; \
         link_headers={}; recv_ts_kind={}; capture_drop_scope={}",
        id.site,
        id.recorder,
        id.env,
        id.build_version,
        id.build_commit,
        id.config_hash,
        cfg.link_headers.as_str(),
        SECTION_RECV_TS_KIND,
        cfg.capture_drop_scope.as_str(),
    );
    if cfg.link_headers == LinkHeaders::Synthesised {
        // Not observed must never be readable as an observed zero.
        comment.push_str(
            "; synthesised_fields=mac,ip_id,ip_checksum,udp_checksum; \
             ttl_zero_means_unobserved",
        );
    }
    vec![
        SectionHeaderOption::Hardware(Cow::Owned(id.hardware())),
        SectionHeaderOption::OS(Cow::Owned(format!(
            "{}/{}",
            std::env::consts::OS,
            std::env::consts::ARCH
        ))),
        SectionHeaderOption::UserApplication(Cow::Owned(format!(
            "dz-recorder/{}",
            env!("CARGO_PKG_VERSION")
        ))),
        SectionHeaderOption::Comment(Cow::Owned(comment)),
    ]
}

/// Ethernet, IPv4 and UDP in front of a payload that arrived without them.
///
/// Reached only when the capture mode observed no headers, because rebuilding
/// captured bytes discards the evidence they carry. The MAC addresses, the IP
/// identification and both checksums are zero because nothing observed them —
/// a plausible-looking value here would be a fact the archive cannot support.
fn synthesise_link_headers(out: &mut Vec<u8>, dg: &RecordedDatagram<'_>) {
    // The length fields describe the datagram that arrived, so a payload the
    // capture length cut short still states how long it was. Computed wide and
    // clamped: UDP over IPv4 sits exactly at this boundary, `wire_payload_len`
    // is a public field on a public struct, and 65508 bytes of it would
    // otherwise panic in debug and wrap to a total length of 27 in release. An
    // IPv4 header cannot express a longer datagram at all; the length that
    // arrived survives in the packet block's `original_len`.
    let ip_total_len = u16::try_from(20 + 8 + u64::from(dg.wire_payload_len)).unwrap_or(u16::MAX);
    let udp_len = u16::try_from(8 + u64::from(dg.wire_payload_len)).unwrap_or(u16::MAX);

    out.extend_from_slice(&[0u8; 12]);
    out.extend_from_slice(&0x0800u16.to_be_bytes());

    out.push(0x45);
    out.push(0);
    out.extend_from_slice(&ip_total_len.to_be_bytes());
    out.extend_from_slice(&[0, 0]);
    out.extend_from_slice(&[0, 0]);
    out.push(dg.ttl.unwrap_or(0));
    out.push(17);
    out.extend_from_slice(&[0, 0]);
    out.extend_from_slice(&dg.src.ip().octets());
    out.extend_from_slice(&dg.dst.ip().octets());

    out.extend_from_slice(&dg.src.port().to_be_bytes());
    out.extend_from_slice(&dg.dst.port().to_be_bytes());
    out.extend_from_slice(&udp_len.to_be_bytes());
    out.extend_from_slice(&[0, 0]);
}

/// Counts every byte handed to the file.
///
/// The section header is written inside `PcapNgWriter`'s constructor, which
/// reports no length, so a per-block tally starts short by the whole of it —
/// a few hundred bytes of identity, build and configuration — and every segment
/// then rotates that much late.
struct Counted<W: Write> {
    inner: W,
    bytes: u64,
}

impl<W: Write> Counted<W> {
    const fn new(inner: W) -> Self {
        Self { inner, bytes: 0 }
    }
}

impl<W: Write> Write for Counted<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.bytes += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn encode_error(e: pcap_file::PcapError) -> SinkError {
    match e {
        pcap_file::PcapError::IoError(io) => SinkError::Io(io),
        other => SinkError::Encode(other.to_string()),
    }
}
