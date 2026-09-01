//! `AF_PACKET` mode: the default capture, because it records what the network
//! delivered to the interface rather than what one socket survived.
//!
//! Four things follow from that choice, and each is why a piece of this module
//! exists.
//!
//! `src`, `dst`, `ttl`, the payload and the headers themselves come off the
//! captured frame, so nothing is synthesised and nothing is guessed — and a
//! fragmented frame is therefore skipped and counted rather than parsed into a
//! plausible-looking datagram it is not. A datagram the capture length cut short
//! is the other way round: it is archived as what was captured, declaring the
//! length that arrived, because a truncated datagram in the archive is evidence
//! of a publisher over the cap and a gap is not.
//!
//! A capture on an interface sees the whole interface, so a BPF filter derived
//! from the configured group and ports is not an optimisation: without it the
//! recorder archives every datagram on the wire.
//!
//! The ring's own drop counter is the same quantity `SO_RXQ_OVFL` is in socket
//! mode, and it is polled per read batch rather than per datagram, so the delta
//! since the previous poll is attributed to the first datagram of the batch —
//! which is what pcapng's `epb_dropcount` means and the best attribution a ring
//! can offer. The interface's own drops stay a separate category: "gap, no
//! capture drops, interface drops rising" is loss upstream of the capture
//! point, and folding it into publisher loss is how a switch problem becomes a
//! publisher finding.
//!
//! The multicast socket is still opened and joined, and its receive path is
//! drained and discarded. The socket exists for the IGMP membership and for
//! nothing else, or the network has no reason to deliver the traffic to this
//! host at all.
//!
//! Everything above goes through `libpcap` rather than reaching past it to a
//! raw `AF_PACKET` socket. That seam keeps every `unsafe` block inside the FFI
//! crate, and it is what makes an accelerated capture framework a link-time
//! change if the measured load ever justifies one — the design rejects such a
//! framework on the measured load, and that rejection is only cheap to revisit
//! while the seam is intact.

use crate::rejoin::Rejoiner;
use crate::socket::{
    bind_or_retry, bump, hand_over, rejoin, Arrival, BindPlan, CaptureCounters, CaptureStats,
    Captured, Extent, Offered, PendingLoss, PortBinding, SourceGate, SourceKey, SourceVerdict,
};
use crate::OverflowTracker;
use dz_edge_core::{PortRole, MAX_DATAGRAM_SIZE};
use dz_recorder_core::{RecordedDatagram, RecvTsKind, Source, SourceError, MAX_LINK_HEADER_SIZE};
use nix::errno::Errno;
use nix::sys::socket::{recv, MsgFlags};
use std::collections::BTreeSet;
use std::fmt::{Display, Write as _};
use std::io;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub use pcap::{Precision, Stat};

/// The groups and ports one capture handle is allowed to see.
///
/// A capture is on an interface, not on a socket, so this is the only thing
/// standing between the recorder and every datagram on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedFilter {
    groups: Vec<Ipv4Addr>,
    ports: Vec<u16>,
}

impl FeedFilter {
    #[must_use]
    pub fn new(group: Ipv4Addr, ports: &[u16]) -> Self {
        Self::from_parts(&[group], ports)
    }

    #[must_use]
    pub fn from_bindings(bindings: &[PortBinding]) -> Self {
        let groups: Vec<Ipv4Addr> = bindings.iter().map(|b| b.group).collect();
        let ports: Vec<u16> = bindings.iter().map(|b| b.port).collect();
        Self::from_parts(&groups, &ports)
    }

    /// Both lists are sorted and de-duplicated, so that two hosts recording the
    /// same feed compile the same program whatever order the roles were
    /// configured in: the filter string is provenance as much as it is a filter.
    fn from_parts(groups: &[Ipv4Addr], ports: &[u16]) -> Self {
        Self {
            groups: groups
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            ports: ports
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        }
    }

    #[must_use]
    pub fn groups(&self) -> &[Ipv4Addr] {
        &self.groups
    }

    #[must_use]
    pub fn ports(&self) -> &[u16] {
        &self.ports
    }
}

/// The BPF expression for one feed.
///
/// A filter with no group and no port would be `udp`, which is the whole
/// interface; [`AfPacketSource::open`] refuses that configuration rather than
/// compiling it.
#[must_use]
pub fn bpf_filter_for(feed: &FeedFilter) -> String {
    let mut filter = String::from("udp");
    if !feed.groups.is_empty() {
        filter.push_str(" and ");
        filter.push_str(&disjunction("dst host", feed.groups.iter()));
    }
    if !feed.ports.is_empty() {
        filter.push_str(" and ");
        filter.push_str(&disjunction("dst port", feed.ports.iter()));
    }
    filter
}

/// `dst host a` for one value, `(dst host a or dst host b)` for several: a
/// single term needs no parentheses, and a disjunction beside an `and` does.
fn disjunction<T: Display>(term: &str, values: impl ExactSizeIterator<Item = T>) -> String {
    let parenthesise = values.len() > 1;
    let mut out = String::new();
    if parenthesise {
        out.push('(');
    }
    for (i, value) in values.enumerate() {
        if i > 0 {
            out.push_str(" or ");
        }
        // Writing to a String cannot fail.
        let _ = write!(out, "{term} {value}");
    }
    if parenthesise {
        out.push(')');
    }
    out
}

const ETHERNET_HEADER_LEN: usize = 14;
const ETHERTYPE_IPV4: u16 = 0x0800;
const IPV4_MIN_HEADER_LEN: usize = 20;
/// With every option present.
const IPV4_MAX_HEADER_LEN: usize = 60;
const UDP_HEADER_LEN: usize = 8;
const IPPROTO_UDP: u8 = 17;
const IPV4_MORE_FRAGMENTS: u16 = 0x2000;
const IPV4_FRAGMENT_OFFSET: u16 = 0x1fff;

/// One captured link-layer frame, read as the datagram it carries.
///
/// Every field here was observed. That is the whole reason this mode is the
/// default, so nothing in it is optional and nothing in it is a stand-in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsedFrame<'a> {
    pub src: SocketAddrV4,
    pub dst: SocketAddrV4,
    pub ttl: u8,
    /// The Ethernet, IPv4 and UDP bytes exactly as they arrived. Carried into
    /// the archive rather than rebuilt from `src`, `dst` and `ttl`: rebuilding is
    /// what a socket capture has to do, and recording the interface exists
    /// precisely to avoid it.
    pub link_headers: &'a [u8],
    /// As much of the payload as the capture length held.
    pub payload: &'a [u8],
    /// The payload's length on the wire, which exceeds `payload.len()` when the
    /// capture length cut the datagram short. Declaring them equal would archive
    /// a datagram over the mandated cap as a whole one and turn a publisher
    /// violation into a clean datagram.
    pub wire_payload_len: u32,
}

/// Why a frame that reached the ring is not archived.
///
/// Each variant is counted, because a frame the recorder discarded silently is
/// indistinguishable from one the network never delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameSkip {
    /// Shorter than the headers it claims to carry.
    TooShort,
    NotIpv4,
    NotUdp,
    /// A fragment carries either no UDP header at all or a partial payload, and
    /// both parse into a datagram no publisher sent.
    Fragmented,
    /// The headers contradict each other, or claim more than the frame that
    /// arrived.
    Malformed,
    /// The capture length cut the frame inside its own headers, so it carries no
    /// ports: no port role owns it and there is no interface in the archive to
    /// write it to. A datagram whose *payload* was cut short is archived — see
    /// [`ParsedFrame::wire_payload_len`] — because a truncated datagram in the
    /// archive is evidence and a gap is not.
    HeadersCut,
}

/// The link-layer frame parsed as Ethernet, IPv4 and UDP — or the reason it was
/// not.
///
/// The IPv4 total length decides where the datagram ends, not the end of the
/// frame: a short datagram is padded to Ethernet's minimum frame size, and
/// taking the padding as payload would archive bytes no publisher sent.
///
/// `on_wire_len` is libpcap's length for the frame as it arrived, which exceeds
/// `frame.len()` when the capture length cut it short. It is what separates a
/// datagram over the mandated cap — archived as what was captured, declaring
/// the length that was sent — from headers claiming more than ever arrived.
pub fn classify_frame(frame: &[u8], on_wire_len: usize) -> Result<ParsedFrame<'_>, FrameSkip> {
    let ethernet = frame
        .get(..ETHERNET_HEADER_LEN)
        .ok_or(FrameSkip::TooShort)?;
    if u16::from_be_bytes([ethernet[12], ethernet[13]]) != ETHERTYPE_IPV4 {
        return Err(FrameSkip::NotIpv4);
    }
    let ip = &frame[ETHERNET_HEADER_LEN..];
    if ip.len() < IPV4_MIN_HEADER_LEN {
        return Err(FrameSkip::TooShort);
    }
    if ip[0] >> 4 != 4 {
        return Err(FrameSkip::NotIpv4);
    }
    let header_len = usize::from(ip[0] & 0x0f) * 4;
    let total_len = usize::from(u16::from_be_bytes([ip[2], ip[3]]));
    if header_len < IPV4_MIN_HEADER_LEN || total_len < header_len + UDP_HEADER_LEN {
        return Err(FrameSkip::Malformed);
    }
    if ip[9] != IPPROTO_UDP {
        return Err(FrameSkip::NotUdp);
    }
    if u16::from_be_bytes([ip[6], ip[7]]) & (IPV4_MORE_FRAGMENTS | IPV4_FRAGMENT_OFFSET) != 0 {
        return Err(FrameSkip::Fragmented);
    }
    // A total length beyond the frame that arrived is a header contradiction.
    // Beyond the bytes captured, it is only our own capture length, and what was
    // captured is still evidence.
    if on_wire_len.max(frame.len()) < ETHERNET_HEADER_LEN + total_len {
        return Err(FrameSkip::Malformed);
    }

    let udp = ip
        .get(header_len..ip.len().min(total_len))
        .ok_or(FrameSkip::HeadersCut)?;
    if udp.len() < UDP_HEADER_LEN {
        return Err(FrameSkip::HeadersCut);
    }
    let udp_len = usize::from(u16::from_be_bytes([udp[4], udp[5]]));
    // Both lengths are the publisher's own, so they contradict each other
    // whether or not the capture length cut the frame.
    if udp_len < UDP_HEADER_LEN || udp_len > total_len - header_len {
        return Err(FrameSkip::Malformed);
    }
    let wire_payload_len = udp_len - UDP_HEADER_LEN;
    Ok(ParsedFrame {
        src: SocketAddrV4::new(
            Ipv4Addr::new(ip[12], ip[13], ip[14], ip[15]),
            u16::from_be_bytes([udp[0], udp[1]]),
        ),
        dst: SocketAddrV4::new(
            Ipv4Addr::new(ip[16], ip[17], ip[18], ip[19]),
            u16::from_be_bytes([udp[2], udp[3]]),
        ),
        ttl: ip[8],
        link_headers: &frame[..ETHERNET_HEADER_LEN + header_len + UDP_HEADER_LEN],
        payload: &udp
            [UDP_HEADER_LEN..UDP_HEADER_LEN + (udp.len() - UDP_HEADER_LEN).min(wire_payload_len)],
        wire_payload_len: u32::try_from(wire_payload_len).unwrap_or(u32::MAX),
    })
}

/// libpcap reports a realtime `timeval` whose fraction is nanoseconds when
/// nanosecond precision is in effect — which is verified at open, never
/// assumed.
#[must_use]
pub fn stamp_ns(secs: i64, nanos: i64) -> u64 {
    let secs = u64::try_from(secs).unwrap_or(0);
    let nanos = u64::try_from(nanos).unwrap_or(0);
    secs.saturating_mul(1_000_000_000).saturating_add(nanos)
}

/// The byte-order magic libpcap writes for a microsecond savefile.
const PCAP_MAGIC_MICRO: u32 = 0xa1b2_c3d4;
/// And for a nanosecond one.
const PCAP_MAGIC_NANO: u32 = 0xa1b2_3c4d;

/// The precision a handle is actually running at, read back from the savefile
/// header it would write.
///
/// libpcap silently gives microseconds when a nanosecond request is not
/// honoured — the setter's return code is the only signal, and the `pcap` crate
/// discards it — and a microsecond archive is indistinguishable from a
/// nanosecond one that happens to end in three zeros. The savefile magic is
/// derived from the handle's own precision, so it answers the question the
/// crate's API otherwise cannot.
#[must_use]
pub fn precision_from_savefile_magic(header: &[u8]) -> Option<Precision> {
    let magic = u32::from_ne_bytes(header.get(..4)?.try_into().ok()?);
    // The header carries the writing host's byte order. We wrote it ourselves,
    // but matching the swapped form too costs nothing and cannot be ambiguous.
    match_magic(magic).or_else(|| match_magic(magic.swap_bytes()))
}

fn match_magic(magic: u32) -> Option<Precision> {
    match magic {
        PCAP_MAGIC_NANO => Some(Precision::Nano),
        PCAP_MAGIC_MICRO => Some(Precision::Micro),
        _ => None,
    }
}

/// Ask the open handle what precision it is on, by having it write a savefile
/// header and reading the magic back.
///
/// The probe file is removed immediately. A probe that cannot be performed is a
/// failure rather than an assumption: an archive whose stamps might be
/// microseconds cannot be told from one whose stamps are not.
fn verify_precision(
    cap: &pcap::Capture<pcap::Active>,
    dir: &Path,
) -> Result<Precision, SourceError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let path = dir.join(format!(
        "dz-recorder-precision-{}-{nanos}.pcap",
        std::process::id()
    ));
    let outcome = savefile_precision(cap, &path);
    // Whatever happened, the probe leaves nothing behind.
    let _ = std::fs::remove_file(&path);
    outcome
}

fn savefile_precision(
    cap: &pcap::Capture<pcap::Active>,
    path: &Path,
) -> Result<Precision, SourceError> {
    let unverifiable = |reason: String| {
        io_error(format!(
            "the capture handle's timestamp precision could not be verified: {reason}"
        ))
    };
    let mut savefile = cap
        .savefile(path)
        .map_err(|e| unverifiable(e.to_string()))?;
    savefile.flush().map_err(|e| unverifiable(e.to_string()))?;
    drop(savefile);
    let header = std::fs::read(path)?;
    precision_from_savefile_magic(&header)
        .ok_or_else(|| unverifiable("it wrote no recognisable savefile magic".to_owned()))
}

/// What one `stats()` poll added to each category since the previous poll.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RingDelta {
    /// Frames the kernel could not fit in the ring. Ours, and attributed to a
    /// datagram.
    pub capture_drops: u32,
    /// Frames the interface or its driver dropped. Upstream of the capture
    /// point, and never a datagram's `drop_delta`.
    pub interface_drops: u32,
    pub received: u32,
}

/// Turns libpcap's three running totals into deltas.
///
/// Every counter here is cumulative, is never reset, and wraps, so the
/// arithmetic is [`OverflowTracker`]'s: the first poll establishes the baseline
/// rather than reporting the whole counter as a loss. Where the capture-drop
/// delta then waits for a datagram to carry it is [`PendingLoss`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RingAccounting {
    capture: OverflowTracker,
    interface: OverflowTracker,
    received: OverflowTracker,
}

impl RingAccounting {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            capture: OverflowTracker::new(),
            interface: OverflowTracker::new(),
            received: OverflowTracker::new(),
        }
    }

    pub fn poll(&mut self, stat: &Stat) -> RingDelta {
        RingDelta {
            capture_drops: self.capture.delta(stat.dropped),
            interface_drops: self.interface.delta(stat.if_dropped),
            received: self.received.delta(stat.received),
        }
    }
}

/// What only `AF_PACKET` mode can admit, alongside the counters both modes
/// share.
#[derive(Debug, Default)]
pub struct AfPacketCounters {
    interface_drops: AtomicU64,
    ring_received: AtomicU64,
    skipped_non_ipv4: AtomicU64,
    skipped_non_udp: AtomicU64,
    skipped_fragmented: AtomicU64,
    skipped_malformed: AtomicU64,
    skipped_short: AtomicU64,
    skipped_unmapped_port: AtomicU64,
}

/// One read of everything `AF_PACKET` mode admits about itself.
///
/// A drop counter is evidence only as a rate: these are cumulative and are
/// never reset, so whatever reads them alerts on the delta and never on the
/// total.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AfPacketStats {
    /// The categories both capture modes report. `overflow_drops` holds the
    /// ring's own drops here, which is the same quantity `SO_RXQ_OVFL` is in
    /// socket mode.
    pub capture: CaptureStats,
    /// Dropped by the interface or its driver, upstream of the capture point.
    /// Its own category, never folded into publisher loss.
    pub interface_drops: u64,
    /// Frames libpcap admitted through the filter.
    pub ring_received: u64,
    pub skipped_non_ipv4: u64,
    pub skipped_non_udp: u64,
    pub skipped_fragmented: u64,
    pub skipped_malformed: u64,
    /// Frames shorter than the headers they claim, and frames the capture
    /// length cut short inside those headers.
    pub skipped_short: u64,
    /// Admitted by the filter but carrying a port no configured role owns, so
    /// there is no interface in the archive to write it to.
    pub skipped_unmapped_port: u64,
}

impl AfPacketCounters {
    #[must_use]
    pub fn snapshot(&self, capture: CaptureStats) -> AfPacketStats {
        let read = |c: &AtomicU64| c.load(Ordering::Relaxed);
        AfPacketStats {
            capture,
            interface_drops: read(&self.interface_drops),
            ring_received: read(&self.ring_received),
            skipped_non_ipv4: read(&self.skipped_non_ipv4),
            skipped_non_udp: read(&self.skipped_non_udp),
            skipped_fragmented: read(&self.skipped_fragmented),
            skipped_malformed: read(&self.skipped_malformed),
            skipped_short: read(&self.skipped_short),
            skipped_unmapped_port: read(&self.skipped_unmapped_port),
        }
    }

    fn count_skip(&self, skip: FrameSkip) {
        bump(
            match skip {
                FrameSkip::TooShort | FrameSkip::HeadersCut => &self.skipped_short,
                FrameSkip::NotIpv4 => &self.skipped_non_ipv4,
                FrameSkip::NotUdp => &self.skipped_non_udp,
                FrameSkip::Fragmented => &self.skipped_fragmented,
                FrameSkip::Malformed => &self.skipped_malformed,
            },
            1,
        );
    }
}

/// Which port role owns a destination port.
///
/// The capture is on an interface, so the role is not a property of the handle
/// the way it is in socket mode: it is read off the datagram's own destination
/// port.
#[derive(Debug, Clone, Default)]
pub struct PortMap {
    roles: Vec<(u16, PortRole)>,
}

impl PortMap {
    #[must_use]
    pub fn from_bindings(bindings: &[PortBinding]) -> Self {
        Self {
            roles: bindings.iter().map(|b| (b.port, b.role)).collect(),
        }
    }

    /// First binding wins, so a port configured twice cannot silently change
    /// which role an archive attributes it to.
    #[must_use]
    pub fn role_for(&self, port: u16) -> Option<PortRole> {
        self.roles
            .iter()
            .find(|(p, _)| *p == port)
            .map(|(_, role)| *role)
    }
}

/// `AF_PACKET` mode's parameters.
///
/// Taken as values rather than read from a file, so the capture crate is usable
/// from a test with no configuration at all.
#[derive(Debug, Clone)]
pub struct AfPacketSourceConfig {
    /// The capture device: the interface the feed arrives on, by name.
    pub device: String,
    /// The address of that interface, for the membership join. Explicit, never
    /// the default route: the IGMP report has to leave by the interface the
    /// feed arrives on.
    pub interface: Ipv4Addr,
    /// One per port role. Both the filter and the port-to-role map come from
    /// these.
    pub bindings: Vec<PortBinding>,
    /// The `AF_PACKET` ring.
    pub buffer_bytes: u64,
    /// The mandated datagram cap plus the longest Ethernet, IPv4 and UDP
    /// headers that can precede it — 82, not the synthesised 42: an IPv4 header
    /// carrying options is what the difference is for.
    pub snaplen: usize,
    /// The capture read timeout, and the granularity at which the drain thread
    /// observes the stop flag.
    pub read_timeout: Duration,
    /// The bounded channel between the drain thread and the record loop.
    pub queue_capacity: usize,
    /// How many datagrams one `stats()` poll covers. Polling per datagram would
    /// spend a syscall per datagram to learn nothing more: a ring cannot
    /// attribute a drop more finely than the batch it was noticed in.
    pub stats_poll_batch: u32,
    /// Silence after which a membership is replaced. `None` disables both the
    /// rejoin cadence and the deferral of a failed join.
    pub stale_after: Option<Duration>,
    /// `SO_RCVBUF` on the membership socket. Small on purpose: nothing reads
    /// that socket's data, and its overflow is not the archive's loss.
    pub membership_recv_buffer_bytes: usize,
    /// Gates counting and alerting, never the archive.
    pub expected_sources: Vec<Ipv4Addr>,
    /// The bound on per-source state.
    pub max_tracked_sources: usize,
    /// Where the precision probe writes its one file. `None` is the system
    /// temporary directory.
    pub precision_probe_dir: Option<PathBuf>,
}

impl AfPacketSourceConfig {
    #[must_use]
    pub fn new(device: impl Into<String>, interface: Ipv4Addr, bindings: Vec<PortBinding>) -> Self {
        Self {
            device: device.into(),
            interface,
            bindings,
            buffer_bytes: 64 * 1024 * 1024,
            snaplen: MAX_DATAGRAM_SIZE + MAX_LINK_HEADER_SIZE,
            read_timeout: Duration::from_millis(100),
            queue_capacity: 8192,
            stats_poll_batch: 64,
            stale_after: Some(Duration::from_secs(30)),
            membership_recv_buffer_bytes: 1 << 20,
            expected_sources: Vec::new(),
            max_tracked_sources: 4096,
            precision_probe_dir: None,
        }
    }

    /// The BPF expression this configuration compiles to.
    #[must_use]
    pub fn filter(&self) -> String {
        bpf_filter_for(&FeedFilter::from_bindings(&self.bindings))
    }

    /// The membership socket's plan, which is socket mode's plan: the join and
    /// the rejoin are the same code, because there is only one correct way to do
    /// them.
    #[must_use]
    pub fn membership_plan(&self, binding: PortBinding) -> BindPlan {
        BindPlan {
            binding,
            interface: self.interface,
            recv_buffer_bytes: self.membership_recv_buffer_bytes,
            read_timeout: self.read_timeout,
        }
    }
}

/// The socket that exists only so the network delivers the traffic to this
/// host.
///
/// Its receive path is drained and discarded — the archive comes from the ring
/// — but it must be drained, or its receive queue fills and the kernel starts
/// accounting an overflow nobody is reading. That overflow is deliberately not
/// counted: it is not the archive's loss.
struct Membership {
    plan: BindPlan,
    stale_after: Option<Duration>,
    socket: Option<OwnedFd>,
    stop: Arc<AtomicBool>,
    counters: Arc<CaptureCounters>,
    fatal: Arc<Mutex<Option<String>>>,
}

impl Membership {
    fn run(mut self) {
        let rejoiner = Rejoiner::new(self.stale_after);
        // Only ever holds one datagram, and never the same one twice.
        let mut sink = vec![0u8; MAX_DATAGRAM_SIZE];
        while !self.stopped() {
            let fd = match self.socket.take() {
                Some(fd) => fd,
                None => match bind_or_retry(&self.plan, self.stale_after) {
                    Ok(Some(fd)) => fd,
                    Ok(None) => {
                        bump(&self.counters.bind_retries, 1);
                        self.wait(self.stale_after.unwrap_or(self.plan.read_timeout));
                        continue;
                    }
                    Err(e) => {
                        report_fatal(
                            &self.fatal,
                            &format!("membership {}: {e}", self.plan.binding.role.as_str()),
                        );
                        return;
                    }
                },
            };
            if self.discard(&fd, rejoiner, &mut sink) == Flow::Stop {
                return;
            }
            // A membership that failed is rejoined, but not in a hot loop.
            self.wait(self.plan.read_timeout);
        }
    }

    fn discard(&self, fd: &OwnedFd, rejoiner: Rejoiner, sink: &mut [u8]) -> Flow {
        let mut last_datagram = Instant::now();
        loop {
            if self.stopped() {
                return Flow::Stop;
            }
            match recv(fd.as_raw_fd(), sink, MsgFlags::empty()) {
                Ok(_) => last_datagram = Instant::now(),
                Err(Errno::EAGAIN | Errno::EINTR) => {
                    // Silence is the only symptom a stranded membership has.
                    if rejoiner.should_rejoin(last_datagram.elapsed()) {
                        match rejoin(fd, &self.plan) {
                            Ok(()) => bump(&self.counters.rejoins, 1),
                            // The interface may be mid-reprovision; the next
                            // cadence tries again, which is why there is one.
                            Err(_) => bump(&self.counters.rejoin_failures, 1),
                        }
                        last_datagram = Instant::now();
                    }
                }
                Err(_) => {
                    bump(&self.counters.read_errors, 1);
                    // The socket, not the datagram, is what failed, and the
                    // traffic depends on the membership it carried: rebinding
                    // is the only thing that can fix it.
                    return Flow::Continue;
                }
            }
        }
    }

    fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    /// Sleeps in read-timeout slices so the stop flag is still observed.
    fn wait(&self, total: Duration) {
        let slice = self.plan.read_timeout.max(Duration::from_millis(1));
        let mut waited = Duration::ZERO;
        while waited < total && !self.stopped() {
            thread::sleep(slice);
            waited += slice;
        }
    }
}

/// Whether the thread carries on or is done.
#[derive(Debug, PartialEq, Eq)]
enum Flow {
    Continue,
    Stop,
}

fn report_fatal(fatal: &Mutex<Option<String>>, reason: &str) {
    let mut fatal = fatal
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if fatal.is_none() {
        *fatal = Some(reason.to_owned());
    }
}

/// One datagram off the ring, with everything borrowed from the ring's own
/// buffer already copied out.
#[derive(Debug, Clone, Copy)]
struct Landed {
    extent: Extent,
    src: SocketAddrV4,
    dst: SocketAddrV4,
    ttl: u8,
    recv_ts_ns: u64,
}

enum Read {
    Datagram(Landed),
    Skipped(FrameSkip),
    /// The read timeout expired: one of the two points at which the stop flag
    /// is observed.
    Quiet,
    /// `pcap_breakloop` was called, which is how a stop reaches a thread parked
    /// in the ring's own `poll`.
    Broken,
    Failed(String),
}

/// The one thread that reads the ring, and does nothing else.
struct RingDrain {
    cap: pcap::Capture<pcap::Active>,
    ledger: RingLedger,
    stop: Arc<AtomicBool>,
    fatal: Arc<Mutex<Option<String>>>,
}

impl RingDrain {
    fn run(self) {
        let Self {
            mut cap,
            mut ledger,
            stop,
            fatal,
        } = self;
        // The baseline: a fresh handle's totals are its zero point, not a loss.
        ledger.poll_ring(|| cap.stats().ok());
        while !stop.load(Ordering::Relaxed) {
            let mut buf = ledger.take_buffer();
            match read_one(&mut cap, &mut buf) {
                // The ring keeps dropping while the feed is quiet — a burst
                // that overflows it and then silence is the shape of the
                // outage most worth alerting on — and stats() is only read on
                // the delivery path, so those drops would stay invisible until
                // a datagram happened to arrive. The delta an alert fires on
                // would arrive after the burst it describes, or never.
                //
                // A quiet read is also when there is budget for the syscall: it
                // costs one poll per read timeout, and only while nothing is
                // being delivered.
                Read::Quiet => {
                    ledger.recycle(buf);
                    ledger.poll_ring(|| cap.stats().ok());
                }
                // Deliberate, and the flag it answers is already set.
                Read::Broken => return,
                Read::Skipped(skip) => {
                    ledger.ring_counters.count_skip(skip);
                    ledger.recycle(buf);
                }
                Read::Failed(reason) => {
                    bump(&ledger.counters.read_errors, 1);
                    ledger.recycle(buf);
                    // Reopening the handle would mean requesting and verifying
                    // the precision again, so that decision belongs to whoever
                    // built this source rather than to this thread.
                    report_fatal(&fatal, &format!("capture ring: {reason}"));
                    return;
                }
                Read::Datagram(landed) => {
                    if ledger.deliver(buf, landed, || cap.stats().ok()) == Flow::Stop {
                        return;
                    }
                }
            }
        }
    }
}

/// Everything the ring drain does to a datagram between the read and the record
/// loop, and every counter it keeps.
///
/// Split from the capture handle so that all of it — the port role, the loss
/// attribution, the handover — is exercisable with no device and no privileges.
struct RingLedger {
    roles: PortMap,
    gate: SourceGate,
    ring: RingAccounting,
    /// Loss owed to the next datagram that reaches the record loop: the ring's
    /// own drops, and every datagram the record loop could not take.
    pending: PendingLoss,
    stats_poll_batch: u32,
    since_poll: u32,
    tx: SyncSender<Captured>,
    free: Receiver<Vec<u8>>,
    /// The buffer of a datagram that never left this thread. Held rather than
    /// freed: the pool only refills from the record loop, and a thread that is
    /// dropping has no record loop to refill it.
    spare: Option<Vec<u8>>,
    counters: Arc<CaptureCounters>,
    ring_counters: Arc<AfPacketCounters>,
}

impl RingLedger {
    /// `stats` is a closure so the ring is polled only at a batch boundary, and
    /// so this stays callable with no handle to poll.
    fn deliver<F>(&mut self, buf: Vec<u8>, landed: Landed, stats: F) -> Flow
    where
        F: FnOnce() -> Option<Stat>,
    {
        // The role is resolved first: a datagram that cannot be archived must
        // not consume a drop delta the next one should have carried.
        let Some(role) = self.roles.role_for(landed.dst.port()) else {
            bump(&self.ring_counters.skipped_unmapped_port, 1);
            self.recycle(buf);
            return Flow::Continue;
        };

        // Polled per read batch, and the delta attributed to the first datagram
        // after it that reaches the record loop: that is what epb_dropcount
        // means, and the finest attribution a ring can offer.
        if self.since_poll == 0 {
            self.poll_ring(stats);
        }
        self.since_poll = (self.since_poll + 1) % self.stats_poll_batch;

        let arrival = Arrival {
            src: landed.src,
            dst: landed.dst,
            role,
            recv_ts_ns: landed.recv_ts_ns,
            // libpcap's stamp is the kernel's, at the nanosecond precision this
            // handle verified at open.
            recv_ts_kind: RecvTsKind::KernelSoftware,
            // Replaced at the handover by everything owed, which is this and
            // whatever the datagrams that did not get through were carrying.
            drop_delta: self.pending.owed(),
            // Observed, off the captured IPv4 header. Nothing here is
            // synthesised, which is the whole reason this mode is the default.
            ttl: Some(landed.ttl),
        };

        bump(&self.counters.datagrams, 1);
        if landed.extent.truncated() {
            // The capture length could not hold it whole, which at a snaplen of
            // the mandated cap plus its headers means a publisher over the cap.
            // Counted, because that is what makes it a finding rather than a
            // curiosity in the archive.
            bump(&self.counters.truncated_datagrams, 1);
        }
        if self
            .gate
            .observe(SourceKey::new(*landed.src.ip(), landed.dst.port()))
            == SourceVerdict::Unexpected
        {
            bump(&self.counters.unexpected_source_datagrams, 1);
        }
        let evicted = self.gate.take_evictions();
        if evicted > 0 {
            bump(&self.counters.source_evictions, evicted);
        }

        let captured = Captured::new(arrival, buf, landed.extent, 0);
        match hand_over(&self.tx, &mut self.pending, captured, &self.counters) {
            Offered::Accepted => Flow::Continue,
            Offered::Dropped(returned) => {
                self.recycle(returned);
                Flow::Continue
            }
            Offered::Disconnected => Flow::Stop,
        }
    }

    fn poll_ring<F>(&mut self, stats: F)
    where
        F: FnOnce() -> Option<Stat>,
    {
        match stats() {
            Some(stat) => {
                let delta = self.ring.poll(&stat);
                bump(&self.ring_counters.ring_received, u64::from(delta.received));
                bump(
                    &self.ring_counters.interface_drops,
                    u64::from(delta.interface_drops),
                );
                // Only the capture drops are owed to a datagram; an interface
                // drop is loss upstream of the capture point and stays its own
                // category. Owed to the accumulator rather than handed to the
                // next datagram, because that datagram may not get through.
                bump(
                    &self.counters.overflow_drops,
                    u64::from(delta.capture_drops),
                );
                self.pending.owe(delta.capture_drops);
            }
            // A failed poll must not be read as "no drops": the tracker keeps
            // its baseline and simply has nothing new to attribute.
            None => bump(&self.counters.read_errors, 1),
        }
    }

    fn take_buffer(&mut self) -> Vec<u8> {
        let mut buf = self
            .spare
            .take()
            .or_else(|| self.free.try_recv().ok())
            .unwrap_or_default();
        // The captured link headers travel in front of the payload, in the one
        // buffer, so the room for both is the cap plus the largest header an
        // IPv4 datagram can arrive with.
        buf.resize(
            MAX_DATAGRAM_SIZE + ETHERNET_HEADER_LEN + IPV4_MAX_HEADER_LEN + UDP_HEADER_LEN,
            0,
        );
        buf
    }

    fn recycle(&mut self, buf: Vec<u8>) {
        self.spare = Some(buf);
    }
}

/// Split out so the borrow of the ring's own buffer ends before anything else on
/// the drain thread is touched: the payload is copied into `buf` here, and
/// nothing borrowed from the ring escapes.
fn read_one(cap: &mut pcap::Capture<pcap::Active>, buf: &mut [u8]) -> Read {
    match cap.next_packet() {
        Ok(packet) => {
            let recv_ts_ns = stamp_ns(packet.header.ts.tv_sec, packet.header.ts.tv_usec);
            // The frame's length on the wire, which exceeds what was captured
            // when the capture length cut it short.
            let on_wire_len = usize::try_from(packet.header.len).unwrap_or(usize::MAX);
            match classify_frame(packet.data, on_wire_len) {
                Ok(parsed) => {
                    // Headers first and the payload after them, so one buffer
                    // and one copy carry both across the channel.
                    let headers_len = parsed.link_headers.len().min(buf.len());
                    buf[..headers_len].copy_from_slice(&parsed.link_headers[..headers_len]);
                    let payload_len = parsed.payload.len().min(buf.len() - headers_len);
                    buf[headers_len..headers_len + payload_len]
                        .copy_from_slice(&parsed.payload[..payload_len]);
                    Read::Datagram(Landed {
                        extent: Extent {
                            headers_len,
                            payload_len,
                            wire_payload_len: parsed.wire_payload_len,
                        },
                        src: parsed.src,
                        dst: parsed.dst,
                        ttl: parsed.ttl,
                        recv_ts_ns,
                    })
                }
                Err(skip) => Read::Skipped(skip),
            }
        }
        Err(pcap::Error::TimeoutExpired) => Read::Quiet,
        Err(pcap::Error::NoMorePackets) => Read::Broken,
        Err(e) => Read::Failed(e.to_string()),
    }
}

/// Live `AF_PACKET` capture as a [`Source`].
///
/// One thread reads the ring; one thread per port role holds the membership the
/// traffic arrives on. Dropping the source stops all of them.
pub struct AfPacketSource {
    rx: Receiver<Captured>,
    /// In immediate mode libpcap parks in `poll` with no timeout, so the stop
    /// flag alone would not be seen until the next datagram arrived — on a dark
    /// feed, never. `pcap_breakloop` is thread-safe and wakes that poll, so it
    /// is what actually stops the ring thread; the flag is what keeps it
    /// stopped.
    break_loop: pcap::BreakLoop,
    pool: SyncSender<Vec<u8>>,
    threads: Vec<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
    counters: Arc<CaptureCounters>,
    ring_counters: Arc<AfPacketCounters>,
    fatal: Arc<Mutex<Option<String>>>,
    current: Option<Captured>,
    poll_interval: Duration,
    precision: Precision,
    filter: String,
}

impl AfPacketSource {
    /// Opens the ring, compiles the filter, verifies the timestamp precision,
    /// and joins the group.
    ///
    /// Each of those fails loudly, because each is otherwise discovered from
    /// the archive months later: an unfiltered capture, microsecond stamps, or
    /// traffic the network was never asked to deliver.
    pub fn open(config: &AfPacketSourceConfig) -> Result<Self, SourceError> {
        if config.bindings.is_empty() {
            return Err(io_error(
                "a capture with no port roles would archive every datagram on the interface"
                    .to_owned(),
            ));
        }
        let filter = config.filter();
        let device = config.device.as_str();

        let mut cap = pcap::Capture::from_device(device)
            .map_err(|e| io_error(format!("capture device {device}: {e}")))?
            // Each datagram as it lands, rather than when a buffer fills: a
            // held-back batch is latency the archive cannot undo.
            .immediate_mode(true)
            .precision(Precision::Nano)
            .buffer_size(clamp_i32(config.buffer_bytes))
            // Never above the mandated cap plus the longest link headers that
            // can precede it: the same discipline the configuration applies,
            // for the same reason — a capture length that can express a larger
            // datagram is how the cap drifts. Clamping to the *synthesised*
            // header size instead is the other way to be wrong: it cuts the
            // tail off a compliant datagram whose IPv4 header carries options,
            // and the recorder then reports the publisher for it.
            .snaplen(clamp_i32(
                config.snaplen.min(MAX_DATAGRAM_SIZE + MAX_LINK_HEADER_SIZE),
            ))
            // Requested, and honoured only while immediate mode is off: with it
            // on, libpcap on Linux parks in poll with no timeout, and
            // pcap_breakloop is what a stop travels through instead.
            .timeout(clamp_i32(config.read_timeout.as_millis()))
            .open()
            .map_err(|e| io_error(format!("capture device {device}: {e}")))?;

        // Before anything is read, so no unfiltered traffic is ever buffered.
        cap.filter(&filter, true)
            .map_err(|e| io_error(format!("filter `{filter}`: {e}")))?;

        let probe_dir = config
            .precision_probe_dir
            .clone()
            .unwrap_or_else(std::env::temp_dir);
        let precision = verify_precision(&cap, &probe_dir)?;
        if precision != Precision::Nano {
            // A microsecond archive is indistinguishable from a nanosecond one
            // that happens to end in three zeros, so this is a refusal and not
            // a warning.
            return Err(io_error(format!(
                "capture device {device} is stamping at microsecond precision: libpcap did not \
                 honour the nanosecond request"
            )));
        }

        let (tx, rx) = mpsc::sync_channel(config.queue_capacity.max(1));
        let (pool_tx, pool_rx) = mpsc::sync_channel(config.queue_capacity.max(1));
        let mut source = Self {
            rx,
            break_loop: cap.breakloop_handle(),
            pool: pool_tx,
            threads: Vec::new(),
            stop: Arc::new(AtomicBool::new(false)),
            counters: Arc::new(CaptureCounters::default()),
            ring_counters: Arc::new(AfPacketCounters::default()),
            fatal: Arc::new(Mutex::new(None)),
            current: None,
            poll_interval: config.read_timeout.max(Duration::from_millis(1)),
            precision,
            filter,
        };
        match source.spawn_all(config, cap, tx, pool_rx) {
            Ok(()) => Ok(source),
            Err(e) => {
                source.shutdown();
                Err(e)
            }
        }
    }

    fn spawn_all(
        &mut self,
        config: &AfPacketSourceConfig,
        cap: pcap::Capture<pcap::Active>,
        tx: SyncSender<Captured>,
        pool_rx: Receiver<Vec<u8>>,
    ) -> Result<(), SourceError> {
        let drain = RingDrain {
            cap,
            ledger: RingLedger {
                roles: PortMap::from_bindings(&config.bindings),
                gate: SourceGate::with_expected_sources(
                    config.expected_sources.iter().copied(),
                    config.max_tracked_sources,
                ),
                ring: RingAccounting::new(),
                pending: PendingLoss::new(),
                stats_poll_batch: config.stats_poll_batch.max(1),
                since_poll: 0,
                tx,
                free: pool_rx,
                spare: None,
                counters: Arc::clone(&self.counters),
                ring_counters: Arc::clone(&self.ring_counters),
            },
            stop: Arc::clone(&self.stop),
            fatal: Arc::clone(&self.fatal),
        };
        self.threads.push(
            thread::Builder::new()
                .name("capture-ring".to_owned())
                .spawn(move || drain.run())
                .map_err(SourceError::Io)?,
        );

        for binding in config.bindings.iter().copied() {
            let plan = config.membership_plan(binding);
            // A join that hits the reprovision case is not an error: that
            // role's thread starts unjoined and retries on the cadence.
            let socket = bind_or_retry(&plan, config.stale_after)?;
            let membership = Membership {
                plan,
                stale_after: config.stale_after,
                socket,
                stop: Arc::clone(&self.stop),
                counters: Arc::clone(&self.counters),
                fatal: Arc::clone(&self.fatal),
            };
            self.threads.push(
                thread::Builder::new()
                    .name(format!("membership-{}", binding.role.as_str()))
                    .spawn(move || membership.run())
                    .map_err(SourceError::Io)?,
            );
        }
        Ok(())
    }

    /// The precision this handle was verified to be running at, never the one
    /// it was asked for.
    #[must_use]
    pub const fn precision(&self) -> Precision {
        self.precision
    }

    /// The compiled BPF expression, for the archive's provenance.
    #[must_use]
    pub fn filter(&self) -> &str {
        &self.filter
    }

    #[must_use]
    pub fn stats(&self) -> AfPacketStats {
        self.ring_counters.snapshot(self.counters.snapshot())
    }

    /// Frames the ring could not hold. Ours, and carried on the datagrams.
    #[must_use]
    pub fn capture_drops(&self) -> u64 {
        self.counters.snapshot().overflow_drops
    }

    /// Frames the interface or its driver dropped, upstream of the capture
    /// point. Its own category: folding this into publisher loss is how a
    /// switch problem becomes a publisher finding.
    #[must_use]
    pub fn interface_drops(&self) -> u64 {
        self.stats().interface_drops
    }

    /// The frame parse, exposed so the decision it makes — archive it, or skip
    /// it and count it — is testable with no device and no privileges.
    #[must_use]
    pub fn parse_frame(frame: &[u8], on_wire_len: usize) -> Option<ParsedFrame<'_>> {
        classify_frame(frame, on_wire_len).ok()
    }

    /// The same parse, carrying the reason a frame was skipped.
    pub fn classify_frame(frame: &[u8], on_wire_len: usize) -> Result<ParsedFrame<'_>, FrameSkip> {
        classify_frame(frame, on_wire_len)
    }

    /// The flag every thread and [`Source::next`] observes. Handed out so a
    /// signal handler can stop a blocked recorder.
    #[must_use]
    pub fn stop_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.stop)
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
        // Wakes the ring thread out of its poll. A no-op once that thread has
        // dropped the handle, which is the only ordering that matters here.
        self.break_loop.breakloop();
    }

    fn shutdown(&mut self) {
        // Through stop(), so that a caller who set the flag through
        // stop_flag() alone still leaves no thread parked in poll.
        self.stop();
        for handle in self.threads.drain(..) {
            let _ = handle.join();
        }
    }

    fn take_fatal(&self) -> Option<String> {
        self.fatal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

impl Source for AfPacketSource {
    fn next(&mut self) -> Result<Option<RecordedDatagram<'_>>, SourceError> {
        if let Some(done) = self.current.take() {
            let _ = self.pool.try_send(done.into_buffer());
        }
        loop {
            if let Some(reason) = self.take_fatal() {
                return Err(SourceError::HandleLost(reason));
            }
            if self.stop.load(Ordering::Relaxed) {
                return Ok(None);
            }
            match self.rx.recv_timeout(self.poll_interval) {
                Ok(captured) => {
                    let held = self.current.insert(captured);
                    return Ok(Some(held.recorded()));
                }
                // A live feed may be quiet. Only the stop flag ends this.
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return Ok(None),
            }
        }
    }
}

impl Drop for AfPacketSource {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn io_error(message: String) -> SourceError {
    SourceError::Io(io::Error::other(message))
}

/// libpcap takes these as `int`. A configured value above that is clamped
/// rather than refused: a ring smaller than asked for is a counter to watch,
/// not a reason not to record.
fn clamp_i32<T: TryInto<i32>>(value: T) -> i32 {
    value.try_into().unwrap_or(i32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    const GROUP: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 10);
    const SOURCE: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
    const MKTDATA_PORT: u16 = 40000;

    fn stat(received: u32, dropped: u32, if_dropped: u32) -> Stat {
        Stat {
            received,
            dropped,
            if_dropped,
        }
    }

    /// Polled on every datagram, so a test says which drops each one is offered
    /// against rather than counting up to a batch boundary.
    fn ledger(tx: SyncSender<Captured>, counters: &Arc<CaptureCounters>) -> RingLedger {
        let (pool_tx, pool_rx) = mpsc::sync_channel(1);
        // The record loop is what refills the pool, and these tests are it.
        drop(pool_tx);
        RingLedger {
            roles: PortMap::from_bindings(&[PortBinding::new(
                PortRole::Mktdata,
                GROUP,
                MKTDATA_PORT,
            )]),
            gate: SourceGate::with_expected_sources([], 8),
            ring: RingAccounting::new(),
            pending: PendingLoss::new(),
            stats_poll_batch: 1,
            since_poll: 0,
            tx,
            free: pool_rx,
            spare: None,
            counters: Arc::clone(counters),
            ring_counters: Arc::new(AfPacketCounters::default()),
        }
    }

    fn landed_on(port: u16, extent: Extent) -> Landed {
        Landed {
            extent,
            src: SocketAddrV4::new(SOURCE, 41000),
            dst: SocketAddrV4::new(GROUP, port),
            ttl: 31,
            recv_ts_ns: 1_700_000_000_000_000_000,
        }
    }

    fn whole(payload_len: usize) -> Extent {
        Extent {
            headers_len: 0,
            payload_len,
            wire_payload_len: u32::try_from(payload_len).expect("a small fixture"),
        }
    }

    fn buffer() -> Vec<u8> {
        vec![0u8; MAX_DATAGRAM_SIZE]
    }

    #[test]
    fn a_ring_drop_during_silence_is_counted_before_the_feed_speaks_again() {
        // The alerting case: a burst overflows the ring and the feed then goes
        // quiet. Polling only on the delivery path leaves those drops
        // unaccounted until a datagram happens to arrive — which, for the
        // outage worth alerting on, may be a long time or never. The quiet read
        // is where the drain polls instead.
        let (tx, _rx) = mpsc::sync_channel(4);
        let counters = Arc::new(CaptureCounters::default());
        let mut ledger = ledger(tx, &counters);

        ledger.poll_ring(|| Some(stat(100, 0, 0)));
        assert_eq!(counters.snapshot().overflow_drops, 0, "the baseline");

        // No datagram between these two polls: exactly what a quiet read is.
        ledger.poll_ring(|| Some(stat(100, 7, 0)));

        assert_eq!(
            counters.snapshot().overflow_drops,
            7,
            "a drop nothing delivered is still a drop"
        );
        // Owed, not lost: it still has to reach the archive on the next
        // datagram that gets through, which is what epb_dropcount means.
        assert_eq!(ledger.pending.owed(), 7);
    }

    #[test]
    fn the_ring_delta_of_a_datagram_we_dropped_rides_on_the_next_one_that_gets_through() {
        // The misattribution this whole mode exists to prevent: a busy record
        // loop fills the channel, a ring burst's delta rides on a datagram we
        // then cannot hand over, and the archive shows the gap with a capture
        // drop total of zero. Both the burst and the datagram we dropped are
        // owed to the next datagram that survives.
        let counters = Arc::new(CaptureCounters::default());
        let (tx, rx) = mpsc::sync_channel(1);
        let mut ledger = ledger(tx, &counters);
        // The poll a fresh handle makes before any datagram: the baseline.
        ledger.poll_ring(|| Some(stat(0, 0, 0)));

        assert_eq!(
            ledger.deliver(buffer(), landed_on(MKTDATA_PORT, whole(24)), || Some(stat(
                1, 0, 0
            ))),
            Flow::Continue
        );
        // The channel is full now, the ring has lost 40 frames, and the
        // interface has dropped 6 upstream of the capture point.
        assert_eq!(
            ledger.deliver(buffer(), landed_on(MKTDATA_PORT, whole(24)), || Some(stat(
                2, 40, 6
            ))),
            Flow::Continue
        );
        let first = rx.recv().expect("the datagram that got through");
        assert_eq!(
            first.recorded().drop_delta,
            0,
            "the baseline admits nothing"
        );

        ledger.deliver(buffer(), landed_on(MKTDATA_PORT, whole(24)), || {
            Some(stat(3, 40, 6))
        });
        let next = rx.recv().expect("the datagram after the one we dropped");
        assert_eq!(
            next.recorded().drop_delta,
            41,
            "the 40 the ring lost and the datagram we dropped, and not the 6 the              interface dropped upstream of us"
        );
        let stats = counters.snapshot();
        assert_eq!(stats.queue_drops, 1);
        assert_eq!(stats.overflow_drops, 40, "the ring's own, counted once");
    }

    #[test]
    fn a_datagram_no_port_role_owns_does_not_consume_the_delta_the_next_one_carries() {
        let counters = Arc::new(CaptureCounters::default());
        let (tx, rx) = mpsc::sync_channel(1);
        let mut ledger = ledger(tx, &counters);
        ledger.poll_ring(|| Some(stat(0, 0, 0)));
        ledger.poll_ring(|| Some(stat(1, 7, 0)));

        ledger.deliver(buffer(), landed_on(9, whole(24)), || Some(stat(1, 7, 0)));
        ledger.deliver(buffer(), landed_on(MKTDATA_PORT, whole(24)), || {
            Some(stat(2, 7, 0))
        });
        let held = rx.recv().expect("the datagram a role does own");
        assert_eq!(held.recorded().drop_delta, 7);
    }

    #[test]
    fn a_datagram_over_the_cap_is_archived_with_its_headers_and_declares_what_arrived() {
        // Archiving the first 1232 bytes as though that were the whole datagram
        // turns a publisher violation into a clean datagram; discarding it turns
        // the violation into a sequence gap the publisher is blamed for.
        let counters = Arc::new(CaptureCounters::default());
        let (tx, rx) = mpsc::sync_channel(1);
        let mut ledger = ledger(tx, &counters);
        ledger.poll_ring(|| Some(stat(0, 0, 0)));

        let headers: Vec<u8> = (0..42u8).collect();
        let mut buf = vec![0u8; MAX_DATAGRAM_SIZE + headers.len()];
        buf[..headers.len()].copy_from_slice(&headers);
        let extent = Extent {
            headers_len: headers.len(),
            payload_len: MAX_DATAGRAM_SIZE,
            wire_payload_len: 1300,
        };
        ledger.deliver(buf, landed_on(MKTDATA_PORT, extent), || Some(stat(1, 0, 0)));

        let held = rx.recv().expect("the datagram");
        let dg = held.recorded();
        assert_eq!(
            dg.link_headers,
            Some(&headers[..]),
            "captured off the interface, never rebuilt"
        );
        assert_eq!(dg.payload.len(), MAX_DATAGRAM_SIZE, "what was captured");
        assert_eq!(dg.wire_payload_len, 1300, "what was sent");
        assert_eq!(
            counters.snapshot().truncated_datagrams,
            1,
            "a counter is what makes it a publisher finding"
        );
    }
}
