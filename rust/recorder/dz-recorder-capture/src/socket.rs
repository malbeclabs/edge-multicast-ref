//! Socket mode: the fallback capture, for hosts where `CAP_NET_RAW` is not
//! available.
//!
//! A socket capture records what one subscriber's socket saw, which forces two
//! things `AF_PACKET` mode never has to do. The IPv4 and UDP headers are
//! synthesised back around the payload, so every synthesised field has to be
//! marked as such and *not observed* must never be written as a plausible
//! value. And the datagrams the socket itself dropped are invisible on the
//! wire, so the recorder has to admit them: `SO_RXQ_OVFL` is that admission,
//! and without it every gap we caused is charged to the publisher.
//!
//! Everything here that decides something — the overflow baseline, the header
//! synthesis, the rejoin cadence, the bind-retry rule, the bound on per-source
//! state — is a value or a function that never touches a socket, because the
//! record path has to be testable in CI with no privileges and no network.

use crate::rejoin::{can_defer_to_cadence, Rejoiner};
use dz_edge_core::{PortRole, MAX_DATAGRAM_SIZE};
use dz_recorder_core::{ChannelInstance, RecordedDatagram, RecvTsKind, Source, SourceError};
use nix::errno::Errno;
use nix::sys::socket::sockopt::{
    IpAddMembership, IpDropMembership, Ipv4PacketInfo, Ipv4RecvTtl, RcvBuf, ReceiveTimeout,
    ReceiveTimestampns, ReuseAddr, ReusePort, RxqOvfl,
};
use nix::sys::socket::{
    bind, recvmsg, setsockopt, socket, AddressFamily, ControlMessageOwned, IpMembershipRequest,
    MsgFlags, RecvMsg, SockFlag, SockProtocol, SockType, SockaddrIn,
};
use nix::sys::time::{TimeSpec, TimeVal, TimeValLike};
use std::collections::{BTreeSet, HashMap};
use std::hash::Hash;
use std::io::IoSliceMut;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Turns `SO_RXQ_OVFL`'s running total into the per-datagram delta the archive
/// stores.
///
/// The counter is per capture handle, it is never reset, and it wraps — so the
/// arithmetic is wrapping, and the first datagram on a handle establishes the
/// baseline. Reporting the whole counter on the first datagram would have every
/// fresh recorder invent an outage at startup.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OverflowTracker {
    last: Option<u32>,
}

impl OverflowTracker {
    #[must_use]
    pub const fn new() -> Self {
        Self { last: None }
    }

    pub fn delta(&mut self, total: u32) -> u32 {
        let delta = match self.last {
            Some(last) => total.wrapping_sub(last),
            None => 0,
        };
        self.last = Some(total);
        delta
    }
}

/// Loss owed to the next datagram that actually reaches the record loop.
///
/// A datagram the record loop could not take is loss the recorder caused, and
/// the delta that datagram was carrying goes back to the buffer pool with it.
/// Both are datagrams lost between the one before and the one after, which is
/// what [`RecordedDatagram::drop_delta`] and pcapng's `epb_dropcount` are
/// defined as, so they are charged to whichever datagram gets through next.
/// Unattributed, the archive shows a sequence gap with nothing admitted behind
/// it, and the analysis tier charges the gap to the publisher.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PendingLoss {
    owed: u32,
}

impl PendingLoss {
    #[must_use]
    pub const fn new() -> Self {
        Self { owed: 0 }
    }

    /// Loss this accumulator carries from here on, whatever becomes of the
    /// datagram it arrived with. Saturating, because a delta that wrapped would
    /// report an outage as a clean stretch.
    pub fn owe(&mut self, lost: u32) {
        self.owed = self.owed.saturating_add(lost);
    }

    /// The delta the next datagram to reach the record loop must declare.
    #[must_use]
    pub const fn owed(&self) -> u32 {
        self.owed
    }

    /// That datagram reached the record loop, so the debt travelled with it.
    pub fn settled(&mut self) {
        self.owed = 0;
    }

    /// It did not. It is itself one more datagram lost between the previous one
    /// and the next, and everything it declared is still owed.
    pub fn undelivered(&mut self) {
        self.owe(1);
    }
}

/// What one `recvmsg` reported about a datagram beyond its bytes.
///
/// Every field is optional because every field is a control message that may
/// not be there, and the difference between *not reported* and a value is the
/// whole reason this type exists.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ArrivalMetadata {
    /// `SCM_TIMESTAMPNS`, realtime nanoseconds.
    pub kernel_ts_ns: Option<u64>,
    /// `SO_RXQ_OVFL`, a running total for this handle.
    pub overflow_total: Option<u32>,
    /// `IP_TTL` from `IP_RECVTTL`.
    pub ttl: Option<u8>,
    /// `ipi_addr` from `IP_PKTINFO`: the address the datagram was actually
    /// sent to, rather than the group we believe we joined.
    pub local_dst: Option<Ipv4Addr>,
}

/// Everything known about one datagram's arrival except its bytes.
///
/// Split from the payload so that synthesis is a pure function over what the
/// kernel said, and so that the payload can stay in the buffer it was read
/// into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Arrival {
    pub src: SocketAddrV4,
    pub dst: SocketAddrV4,
    pub role: PortRole,
    pub recv_ts_ns: u64,
    pub recv_ts_kind: RecvTsKind,
    pub drop_delta: u32,
    pub ttl: Option<u8>,
}

impl Arrival {
    /// `wire_payload_len` and `link_headers` are arguments rather than derived
    /// from `payload`, because deriving them is exactly how a datagram over the
    /// cap gets archived as a whole one and how a captured header gets replaced
    /// by a rebuilt one.
    #[must_use]
    pub const fn attach<'a>(
        &self,
        payload: &'a [u8],
        wire_payload_len: u32,
        link_headers: Option<&'a [u8]>,
    ) -> RecordedDatagram<'a> {
        RecordedDatagram {
            payload,
            src: self.src,
            dst: self.dst,
            role: self.role,
            recv_ts_ns: self.recv_ts_ns,
            recv_ts_kind: self.recv_ts_kind,
            drop_delta: self.drop_delta,
            ttl: self.ttl,
            link_headers,
            wire_payload_len,
        }
    }
}

/// Per-handle synthesis state: what the handle joined, and its overflow
/// baseline.
#[derive(Debug, Clone, Copy)]
pub struct Synthesiser {
    joined: SocketAddrV4,
    role: PortRole,
    overflow: OverflowTracker,
}

impl Synthesiser {
    #[must_use]
    pub const fn new(joined: SocketAddrV4, role: PortRole) -> Self {
        Self {
            joined,
            role,
            overflow: OverflowTracker::new(),
        }
    }

    /// `fallback_ts_ns` is a closure so the clock is only read when the kernel
    /// did not stamp the datagram, and so this stays testable without one.
    pub fn arrival<F>(
        &mut self,
        src: SocketAddrV4,
        meta: &ArrivalMetadata,
        fallback_ts_ns: F,
    ) -> Arrival
    where
        F: FnOnce() -> u64,
    {
        let (recv_ts_ns, recv_ts_kind) = match meta.kernel_ts_ns {
            Some(ns) => (ns, RecvTsKind::KernelSoftware),
            // A latency computed from this measures our own scheduler, so the
            // kind travels with the stamp and is never inferred downstream.
            None => (fallback_ts_ns(), RecvTsKind::ApplicationFallback),
        };
        Arrival {
            src,
            dst: SocketAddrV4::new(
                meta.local_dst.unwrap_or(*self.joined.ip()),
                self.joined.port(),
            ),
            role: self.role,
            recv_ts_ns,
            recv_ts_kind,
            drop_delta: meta.overflow_total.map_or(0, |t| self.overflow.delta(t)),
            ttl: meta.ttl,
        }
    }
}

/// The finest key a capture handle can form honestly.
///
/// [`ChannelInstance`] is the correct key for anything tracking a sequence
/// space, but its `Channel ID` shard is inside the payload, and the record path
/// parses nothing. Source address and destination port are the two shards a
/// socket reports; the analysis tier refines them into the full instance
/// offline, from the archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SourceKey {
    pub source: Ipv4Addr,
    pub dst_port: u16,
}

impl SourceKey {
    #[must_use]
    pub const fn new(source: Ipv4Addr, dst_port: u16) -> Self {
        Self { source, dst_port }
    }

    /// The instance this key becomes once something that does parse supplies
    /// the `Channel ID`.
    #[must_use]
    pub const fn instance(self, channel_id: u8) -> ChannelInstance {
        ChannelInstance::new(self.source, channel_id, self.dst_port)
    }
}

/// Whether a key had been seen before this sighting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sighting {
    First,
    Again,
}

/// Bounded per-key state with least-recently-seen eviction.
///
/// An any-source join accepts datagrams from any sender, so the key space is
/// not ours to trust: unbounded state keyed on it is a host a stranger can fill.
/// Eviction is by least recently seen because the keys worth remembering are
/// the ones still arriving.
#[derive(Debug)]
pub struct LastSeen<K> {
    capacity: usize,
    seen: HashMap<K, u64>,
    order: BTreeSet<(u64, K)>,
    tick: u64,
    evictions: u64,
}

impl<K: Copy + Eq + Hash + Ord> LastSeen<K> {
    /// A capacity of zero is raised to one: a bound that admits nothing would
    /// make every datagram a first sighting.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            seen: HashMap::new(),
            order: BTreeSet::new(),
            tick: 0,
            evictions: 0,
        }
    }

    pub fn observe(&mut self, key: K) -> Sighting {
        self.tick += 1;
        let now = self.tick;
        match self.seen.insert(key, now) {
            Some(previous) => {
                self.order.remove(&(previous, key));
                self.order.insert((now, key));
                Sighting::Again
            }
            None => {
                self.order.insert((now, key));
                while self.seen.len() > self.capacity {
                    let Some(oldest) = self.order.pop_first() else {
                        break;
                    };
                    self.seen.remove(&oldest.1);
                    self.evictions += 1;
                }
                Sighting::First
            }
        }
    }

    #[must_use]
    pub fn contains(&self, key: &K) -> bool {
        self.seen.contains_key(key)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }

    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Evictions since the last call, so a thread can fold them into a shared
    /// counter without two threads clobbering each other's total.
    pub fn take_evictions(&mut self) -> u64 {
        std::mem::take(&mut self.evictions)
    }
}

/// Whether a datagram's source is one the recorder was told to expect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceVerdict {
    Expected,
    Unexpected,
}

/// The expected-source list, and the bounded state keyed on what arrives.
///
/// The list gates counting and alerting and nothing else. A wrongly recorded
/// datagram is filterable afterwards on the source address; a wrongly dropped
/// one is gone.
#[derive(Debug)]
pub struct SourceGate {
    expected: Arc<BTreeSet<Ipv4Addr>>,
    seen: LastSeen<SourceKey>,
}

impl SourceGate {
    #[must_use]
    pub fn new(expected: Arc<BTreeSet<Ipv4Addr>>, capacity: usize) -> Self {
        Self {
            expected,
            seen: LastSeen::with_capacity(capacity),
        }
    }

    #[must_use]
    pub fn with_expected_sources<I>(expected: I, capacity: usize) -> Self
    where
        I: IntoIterator<Item = Ipv4Addr>,
    {
        Self::new(Arc::new(expected.into_iter().collect()), capacity)
    }

    /// Records the sighting and judges the source. Nothing here can refuse
    /// delivery: it returns a verdict, not a decision.
    pub fn observe(&mut self, key: SourceKey) -> SourceVerdict {
        self.seen.observe(key);
        // An empty list is not an expectation, so it cannot be violated.
        if self.expected.is_empty() || self.expected.contains(&key.source) {
            SourceVerdict::Expected
        } else {
            SourceVerdict::Unexpected
        }
    }

    #[must_use]
    pub fn tracked(&self) -> usize {
        self.seen.len()
    }

    pub fn take_evictions(&mut self) -> u64 {
        self.seen.take_evictions()
    }
}

/// Everything socket mode admits about itself, shared with the drain threads.
///
/// A drop counter is evidence only as a rate: these are cumulative and are
/// never reset, so whatever reads them alerts on the delta and never on the
/// total.
#[derive(Debug, Default)]
pub struct CaptureCounters {
    pub(crate) datagrams: AtomicU64,
    pub(crate) overflow_drops: AtomicU64,
    pub(crate) queue_drops: AtomicU64,
    pub(crate) unexpected_source_datagrams: AtomicU64,
    pub(crate) source_evictions: AtomicU64,
    pub(crate) truncated_datagrams: AtomicU64,
    pub(crate) cmsg_truncations: AtomicU64,
    pub(crate) read_errors: AtomicU64,
    pub(crate) rejoins: AtomicU64,
    pub(crate) rejoin_failures: AtomicU64,
    pub(crate) bind_retries: AtomicU64,
}

/// One consistent-enough read of [`CaptureCounters`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CaptureStats {
    pub datagrams: u64,
    /// Datagrams the kernel dropped because the receive queue was full — and,
    /// in `AF_PACKET` mode, the frames it could not fit in the ring. One
    /// quantity: what the recorder itself lost.
    pub overflow_drops: u64,
    /// Datagrams dropped because the record loop was behind. The drain thread
    /// never waits for it.
    pub queue_drops: u64,
    pub unexpected_source_datagrams: u64,
    pub source_evictions: u64,
    /// Datagrams the capture could not hold whole. The archive holds what was
    /// captured and declares the length that arrived, and this counter is what
    /// makes a publisher over the mandated cap a finding rather than a curiosity
    /// nobody looks for.
    pub truncated_datagrams: u64,
    pub cmsg_truncations: u64,
    pub read_errors: u64,
    pub rejoins: u64,
    pub rejoin_failures: u64,
    pub bind_retries: u64,
}

impl CaptureCounters {
    #[must_use]
    pub fn snapshot(&self) -> CaptureStats {
        let read = |c: &AtomicU64| c.load(Ordering::Relaxed);
        CaptureStats {
            datagrams: read(&self.datagrams),
            overflow_drops: read(&self.overflow_drops),
            queue_drops: read(&self.queue_drops),
            unexpected_source_datagrams: read(&self.unexpected_source_datagrams),
            source_evictions: read(&self.source_evictions),
            truncated_datagrams: read(&self.truncated_datagrams),
            cmsg_truncations: read(&self.cmsg_truncations),
            read_errors: read(&self.read_errors),
            rejoins: read(&self.rejoins),
            rejoin_failures: read(&self.rejoin_failures),
            bind_retries: read(&self.bind_retries),
        }
    }
}

pub(crate) fn bump(counter: &AtomicU64, by: u64) {
    counter.fetch_add(by, Ordering::Relaxed);
}

/// One port role's group and port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PortBinding {
    pub role: PortRole,
    pub group: Ipv4Addr,
    pub port: u16,
}

impl PortBinding {
    #[must_use]
    pub const fn new(role: PortRole, group: Ipv4Addr, port: u16) -> Self {
        Self { role, group, port }
    }
}

/// Everything one socket needs to exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BindPlan {
    pub binding: PortBinding,
    /// Explicit, never the default route: the IGMP report has to leave by the
    /// interface the feed arrives on, and the routing table's preference is not
    /// that guarantee.
    pub interface: Ipv4Addr,
    pub recv_buffer_bytes: usize,
    /// Short, so a blocked read observes the stop flag promptly.
    pub read_timeout: Duration,
}

/// Whether a failed bind or join is the reprovision case.
///
/// An interface that is not there *yet* is a transient state of a host being
/// built, not a reason to end the source.
#[must_use]
pub const fn is_reprovision_error(errno: Errno) -> bool {
    matches!(
        errno,
        Errno::ENODEV | Errno::EADDRNOTAVAIL | Errno::ENETDOWN | Errno::ENETUNREACH
    )
}

/// Open a socket for one port role and join the group on an explicit interface.
///
/// `SO_TIMESTAMPNS`, `SO_RXQ_OVFL`, `IP_RECVTTL` and `IP_PKTINFO` are all set
/// here so that a single `recvmsg` answers when the datagram arrived, what the
/// handle has dropped, what TTL it carried and where it was actually sent.
pub fn bind_multicast(plan: &BindPlan) -> Result<OwnedFd, Errno> {
    let fd = socket(
        AddressFamily::Inet,
        SockType::Datagram,
        SockFlag::empty(),
        SockProtocol::Udp,
    )?;

    // Another process on this host may be a legitimate subscriber to the same
    // group and port; a recorder that displaces it is worse than no recorder.
    setsockopt(&fd, ReuseAddr, &true)?;
    setsockopt(&fd, ReusePort, &true)?;

    bind(
        fd.as_raw_fd(),
        &SockaddrIn::from(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, plan.binding.port)),
    )?;

    setsockopt(&fd, ReceiveTimestampns, &true)?;
    setsockopt(&fd, RxqOvfl, &1)?;
    setsockopt(&fd, Ipv4RecvTtl, &true)?;
    setsockopt(&fd, Ipv4PacketInfo, &true)?;

    // The kernel silently clamps this to net.core.rmem_max, which is the right
    // behaviour here: a request above the limit is not a reason to refuse to
    // record, it is a reason for the overflow counter to be watched.
    setsockopt(&fd, RcvBuf, &plan.recv_buffer_bytes)?;
    setsockopt(
        &fd,
        ReceiveTimeout,
        &TimeVal::microseconds(timeout_micros(plan.read_timeout)),
    )?;

    setsockopt(
        &fd,
        IpAddMembership,
        &IpMembershipRequest::new(plan.binding.group, Some(plan.interface)),
    )?;

    Ok(fd)
}

/// `Ok(None)` means *not now*: retry on the rejoin cadence.
///
/// `ENODEV` from `IP_ADD_MEMBERSHIP` against an interface that has not been
/// provisioned yet is the case this exists for. Propagating it ends the task
/// before any drain thread exists, so nothing ever retries and the source is
/// dark until a human notices. With no cadence to defer to, failing loudly
/// beats a thread that can only sleep.
pub fn bind_or_retry(
    plan: &BindPlan,
    stale_after: Option<Duration>,
) -> Result<Option<OwnedFd>, SourceError> {
    match bind_multicast(plan) {
        Ok(fd) => Ok(Some(fd)),
        Err(errno) if is_reprovision_error(errno) && can_defer_to_cadence(stale_after) => Ok(None),
        Err(errno) => Err(SourceError::Io(std::io::Error::from(errno))),
    }
}

/// Replace a membership that has stopped reporting.
///
/// `AF_PACKET` mode joins the same way and rejoins on the same cadence — its
/// socket exists only for the membership — so both modes share this rather than
/// each having its own idea of what a replacement is.
pub(crate) fn rejoin(fd: &OwnedFd, plan: &BindPlan) -> Result<(), Errno> {
    let request = IpMembershipRequest::new(plan.binding.group, Some(plan.interface));
    // A failed drop is not interesting: the membership being replaced is the
    // one we already suspect is gone.
    let _ = setsockopt(fd, IpDropMembership, &request);
    setsockopt(fd, IpAddMembership, &request)
}

fn timeout_micros(timeout: Duration) -> i64 {
    i64::try_from(timeout.as_micros()).unwrap_or(i64::MAX)
}

fn now_realtime_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

/// Where a captured datagram's bytes are in the buffer that carries them, and
/// how much of the datagram they are.
///
/// The link headers, where the capture mode read any, occupy the front of the
/// buffer and the payload follows them, so one buffer and one copy carry both.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Extent {
    /// Zero when the capture mode saw only a payload.
    pub(crate) headers_len: usize,
    pub(crate) payload_len: usize,
    /// The payload's length on the wire, which exceeds `payload_len` for a
    /// datagram over the mandated cap.
    pub(crate) wire_payload_len: u32,
}

impl Extent {
    /// Whether less of the datagram was captured than was sent.
    pub(crate) fn truncated(&self) -> bool {
        u64::from(self.wire_payload_len) > u64::try_from(self.payload_len).unwrap_or(u64::MAX)
    }
}

/// A datagram on its way from a drain thread to the record loop.
///
/// It owns its buffer because it crosses a thread boundary; the buffer is
/// recycled rather than freed, so the steady state allocates nothing per
/// datagram. Shared with `AF_PACKET` mode, which crosses the same boundary with
/// the same bounded channel and the same buffer pool.
#[derive(Debug)]
pub(crate) struct Captured {
    arrival: Arrival,
    buf: Vec<u8>,
    extent: Extent,
    /// Which drain thread's buffer pool this came from.
    origin: usize,
}

impl Captured {
    pub(crate) const fn new(arrival: Arrival, buf: Vec<u8>, extent: Extent, origin: usize) -> Self {
        Self {
            arrival,
            buf,
            extent,
            origin,
        }
    }

    pub(crate) fn recorded(&self) -> RecordedDatagram<'_> {
        let (headers, payload) = self.buf.split_at(self.extent.headers_len);
        self.arrival.attach(
            &payload[..self.extent.payload_len],
            self.extent.wire_payload_len,
            (!headers.is_empty()).then_some(headers),
        )
    }

    /// Hands the buffer back for reuse, which is the only thing left to do with
    /// a datagram the record loop has finished with.
    pub(crate) fn into_buffer(self) -> Vec<u8> {
        self.buf
    }
}

/// What happened to a datagram offered to the record loop.
#[derive(Debug)]
pub(crate) enum Offered {
    Accepted,
    /// The record loop is behind. The buffer comes back for reuse.
    Dropped(Vec<u8>),
    /// Nothing is reading any more.
    Disconnected,
}

/// Offer a datagram to the record loop, never waiting for it.
///
/// A drain thread that blocks here overflows the receive queue behind it, which
/// converts a slow writer into false publisher-loss findings in every archive
/// written during it. So a full channel is a drop and a counter — the same rule
/// as the archive's staging watermark, applied one layer up.
pub(crate) fn offer(
    tx: &SyncSender<Captured>,
    captured: Captured,
    counters: &CaptureCounters,
) -> Offered {
    match tx.try_send(captured) {
        Ok(()) => Offered::Accepted,
        Err(TrySendError::Full(returned)) => {
            bump(&counters.queue_drops, 1);
            Offered::Dropped(returned.buf)
        }
        Err(TrySendError::Disconnected(_)) => Offered::Disconnected,
    }
}

/// Offer a datagram, and keep the loss of one that does not survive the offer.
///
/// The delta the datagram declares is everything `pending` is owed, this
/// datagram's own losses included: both capture modes owe their losses to the
/// accumulator before handing a datagram over, because the datagram may not get
/// through, and a delta that leaves on one that does not is loss the archive
/// never hears about.
pub(crate) fn hand_over(
    tx: &SyncSender<Captured>,
    pending: &mut PendingLoss,
    mut captured: Captured,
    counters: &CaptureCounters,
) -> Offered {
    captured.arrival.drop_delta = pending.owed();
    match offer(tx, captured, counters) {
        Offered::Accepted => {
            pending.settled();
            Offered::Accepted
        }
        Offered::Dropped(returned) => {
            pending.undelivered();
            Offered::Dropped(returned)
        }
        // Nothing is reading, so nothing will carry it and there is no archive
        // left to carry it into.
        Offered::Disconnected => Offered::Disconnected,
    }
}

/// The control buffer for one `recvmsg`, sized from `nix` rather than guessed:
/// one `CMSG_SPACE` each for `SCM_TIMESTAMPNS` (a `timespec`), `SO_RXQ_OVFL`
/// (`u32`), `IP_TTL` (`c_int`) and `IP_PKTINFO` (an interface index and two
/// addresses).
fn control_buffer() -> Vec<u8> {
    nix::cmsg_space!(TimeSpec, u32, i32, [u8; 12])
}

fn arrival_metadata(
    msg: &RecvMsg<'_, '_, SockaddrIn>,
    counters: &CaptureCounters,
) -> ArrivalMetadata {
    let mut meta = ArrivalMetadata::default();
    let Ok(cmsgs) = msg.cmsgs() else {
        // The control buffer was too small for what the kernel sent. Counted,
        // because it explains an archive full of fallback stamps.
        bump(&counters.cmsg_truncations, 1);
        return meta;
    };
    for cmsg in cmsgs {
        match cmsg {
            ControlMessageOwned::ScmTimestampns(ts) => {
                let secs = u64::try_from(ts.tv_sec()).unwrap_or(0);
                let nanos = u64::try_from(ts.tv_nsec()).unwrap_or(0);
                meta.kernel_ts_ns = Some(secs * 1_000_000_000 + nanos);
            }
            ControlMessageOwned::RxqOvfl(total) => meta.overflow_total = Some(total),
            ControlMessageOwned::Ipv4Ttl(ttl) => meta.ttl = u8::try_from(ttl).ok(),
            ControlMessageOwned::Ipv4PacketInfo(info) => {
                meta.local_dst = Some(Ipv4Addr::from(u32::from_be(info.ipi_addr.s_addr)));
            }
            _ => {}
        }
    }
    meta
}

/// One port role's drain thread: it drains, and it does nothing else.
struct Drain {
    plan: BindPlan,
    stale_after: Option<Duration>,
    origin: usize,
    socket: Option<OwnedFd>,
    tx: SyncSender<Captured>,
    free: Receiver<Vec<u8>>,
    /// The buffer of a datagram that never left this thread. Held rather than
    /// freed: the pool only refills from the record loop, and a thread that is
    /// dropping has no record loop to refill it.
    spare: Option<Vec<u8>>,
    gate: SourceGate,
    /// One `recvmsg`'s worth of control messages, reused: the record path
    /// allocates nothing per datagram.
    control: Vec<u8>,
    /// Loss owed to the next datagram that reaches the record loop, so that a
    /// datagram this thread had to drop still reaches the archive as the loss it
    /// is.
    pending: PendingLoss,
    stop: Arc<AtomicBool>,
    counters: Arc<CaptureCounters>,
    fatal: Arc<Mutex<Option<String>>>,
}

impl Drain {
    fn run(mut self) {
        let rejoiner = Rejoiner::new(self.stale_after);
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
                        self.report_fatal(&e.to_string());
                        return;
                    }
                },
            };
            // The overflow baseline belongs to the handle: a replacement handle
            // starts its own rather than inheriting a total it never saw.
            let mut synth = Synthesiser::new(
                SocketAddrV4::new(self.plan.binding.group, self.plan.binding.port),
                self.plan.binding.role,
            );
            if self.drain(&fd, &mut synth, rejoiner) == Ended::Stopped {
                return;
            }
            // A handle that failed is rebound, but not in a hot loop.
            self.wait(self.plan.read_timeout);
        }
    }

    fn drain(&mut self, fd: &OwnedFd, synth: &mut Synthesiser, rejoiner: Rejoiner) -> Ended {
        let mut last_datagram = Instant::now();
        loop {
            if self.stopped() {
                return Ended::Stopped;
            }
            match self.receive(fd, synth) {
                Step::Handed => last_datagram = Instant::now(),
                Step::Quiet => {
                    if rejoiner.should_rejoin(last_datagram.elapsed()) {
                        self.rejoin(fd);
                        last_datagram = Instant::now();
                    }
                }
                // The handle, not the datagram, is what failed. Ending the
                // drain sends this thread back through bind_or_retry, which is
                // the only thing that can fix it.
                Step::HandleLost => return Ended::HandleLost,
                Step::Disconnected => return Ended::Stopped,
            }
        }
    }

    /// One `recvmsg` and everything that happens to what it returned. Split
    /// from the loop so the accounting is exercisable on any datagram socket,
    /// with no privileges and no network.
    fn receive(&mut self, fd: &OwnedFd, synth: &mut Synthesiser) -> Step {
        let mut buf = self.take_buffer();
        let received = {
            let mut iov = [IoSliceMut::new(&mut buf)];
            // MSG_TRUNC, so the kernel reports the datagram's whole length even
            // when the buffer was shorter than it. Without it a datagram over
            // the mandated cap arrives trimmed with its true size unrecoverable,
            // and the archive can only declare the trimmed one as whole — which
            // turns a publisher violation into a clean datagram.
            match recvmsg::<SockaddrIn>(
                fd.as_raw_fd(),
                &mut iov,
                Some(&mut self.control),
                MsgFlags::MSG_TRUNC,
            ) {
                // Copied out so the borrow of the buffer ends here and the
                // buffer itself can travel with the datagram.
                Ok(msg) => Ok((
                    msg.bytes,
                    msg.address,
                    msg.flags,
                    arrival_metadata(&msg, &self.counters),
                )),
                Err(errno) => Err(errno),
            }
        };

        let (wire_len, address, flags, meta) = match received {
            Ok(received) => received,
            Err(Errno::EAGAIN | Errno::EINTR) => {
                self.recycle(buf);
                return Step::Quiet;
            }
            Err(_) => {
                bump(&self.counters.read_errors, 1);
                self.recycle(buf);
                return Step::HandleLost;
            }
        };

        let src = address.map_or_else(
            || SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0),
            |addr| SocketAddrV4::new(addr.ip(), addr.port()),
        );
        let extent = Extent {
            headers_len: 0,
            // What the buffer actually holds. The rest of the datagram is gone,
            // and wire_payload_len is where the archive says so.
            payload_len: wire_len.min(buf.len()),
            wire_payload_len: u32::try_from(wire_len).unwrap_or(u32::MAX),
        };
        if extent.truncated() || flags.contains(MsgFlags::MSG_TRUNC) {
            // The buffer is the mandated cap, so this is a publisher over it,
            // and a counter is what makes that a finding rather than a curiosity
            // in the archive.
            bump(&self.counters.truncated_datagrams, 1);
        }
        match self.deliver(synth.arrival(src, &meta, now_realtime_ns), buf, extent) {
            Offered::Accepted => Step::Handed,
            Offered::Dropped(returned) => {
                self.recycle(returned);
                Step::Handed
            }
            Offered::Disconnected => Step::Disconnected,
        }
    }

    /// The accounting a datagram gets between the read and the record loop.
    fn deliver(&mut self, arrival: Arrival, buf: Vec<u8>, extent: Extent) -> Offered {
        bump(&self.counters.datagrams, 1);
        bump(&self.counters.overflow_drops, u64::from(arrival.drop_delta));
        // The socket's own delta is owed to the accumulator, not to this
        // datagram: this datagram may not be the one that gets through.
        self.pending.owe(arrival.drop_delta);
        if self
            .gate
            .observe(SourceKey::new(*arrival.src.ip(), self.plan.binding.port))
            == SourceVerdict::Unexpected
        {
            bump(&self.counters.unexpected_source_datagrams, 1);
        }
        let evicted = self.gate.take_evictions();
        if evicted > 0 {
            bump(&self.counters.source_evictions, evicted);
        }

        let captured = Captured::new(arrival, buf, extent, self.origin);
        hand_over(&self.tx, &mut self.pending, captured, &self.counters)
    }

    fn rejoin(&self, fd: &OwnedFd) {
        match rejoin(fd, &self.plan) {
            Ok(()) => bump(&self.counters.rejoins, 1),
            // The interface may be mid-reprovision; the next cadence tries
            // again, which is the whole point of having one.
            Err(_) => bump(&self.counters.rejoin_failures, 1),
        }
    }

    fn take_buffer(&mut self) -> Vec<u8> {
        let mut buf = self
            .spare
            .take()
            .or_else(|| self.free.try_recv().ok())
            .unwrap_or_default();
        buf.resize(MAX_DATAGRAM_SIZE, 0);
        buf
    }

    fn recycle(&mut self, buf: Vec<u8>) {
        self.spare = Some(buf);
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

    fn report_fatal(&self, reason: &str) {
        let mut fatal = self
            .fatal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if fatal.is_none() {
            *fatal = Some(format!("{}: {reason}", self.plan.binding.role.as_str()));
        }
    }
}

/// What one `recvmsg` came to.
#[derive(Debug, PartialEq, Eq)]
enum Step {
    /// Handed to the record loop, or dropped and accounted for. Either way a
    /// datagram arrived, which is what the rejoin cadence watches.
    Handed,
    /// The read timeout expired, and silence is the only symptom a stranded
    /// membership has.
    Quiet,
    HandleLost,
    /// Nothing is reading the channel any more.
    Disconnected,
}

#[derive(Debug, PartialEq, Eq)]
enum Ended {
    Stopped,
    HandleLost,
}

/// Socket mode's parameters.
///
/// Taken as values rather than read from a file, so that the capture crate is
/// usable from a test with no configuration at all.
#[derive(Debug, Clone)]
pub struct SocketSourceConfig {
    /// The interface the feed arrives on.
    pub interface: Ipv4Addr,
    /// One per port role.
    pub bindings: Vec<PortBinding>,
    /// `SO_RCVBUF`, per socket.
    pub recv_buffer_bytes: usize,
    /// The bounded channel between each drain thread and the record loop.
    pub queue_capacity: usize,
    /// The socket read timeout, and the granularity at which a drain thread
    /// observes the stop flag.
    pub read_timeout: Duration,
    /// Silence after which a membership is replaced. `None` disables both the
    /// rejoin cadence and the deferral of a failed bind.
    pub stale_after: Option<Duration>,
    /// Gates counting and alerting, never the archive.
    pub expected_sources: Vec<Ipv4Addr>,
    /// The bound on per-source state.
    pub max_tracked_sources: usize,
}

impl SocketSourceConfig {
    #[must_use]
    pub fn new(interface: Ipv4Addr, bindings: Vec<PortBinding>) -> Self {
        Self {
            interface,
            bindings,
            recv_buffer_bytes: 16 * 1024 * 1024,
            queue_capacity: 8192,
            read_timeout: Duration::from_millis(100),
            stale_after: Some(Duration::from_secs(30)),
            expected_sources: Vec::new(),
            max_tracked_sources: 4096,
        }
    }

    #[must_use]
    pub fn plan(&self, binding: PortBinding) -> BindPlan {
        BindPlan {
            binding,
            interface: self.interface,
            recv_buffer_bytes: self.recv_buffer_bytes,
            read_timeout: self.read_timeout,
        }
    }
}

/// Live socket capture as a [`Source`].
///
/// One drain thread per port role, each pushing into a bounded channel this
/// drains. The threads outlive nothing: dropping the source stops them.
pub struct SocketSource {
    rx: Receiver<Captured>,
    pools: Vec<SyncSender<Vec<u8>>>,
    threads: Vec<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
    counters: Arc<CaptureCounters>,
    fatal: Arc<Mutex<Option<String>>>,
    current: Option<Captured>,
    poll_interval: Duration,
}

impl SocketSource {
    /// Binds every port role and starts draining.
    ///
    /// A bind that hits the reprovision case is not an error here: that role's
    /// thread starts unbound and retries on the cadence.
    pub fn bind(config: &SocketSourceConfig) -> Result<Self, SourceError> {
        let (tx, rx) = mpsc::sync_channel(config.queue_capacity.max(1));
        let mut source = Self {
            rx,
            pools: Vec::new(),
            threads: Vec::new(),
            stop: Arc::new(AtomicBool::new(false)),
            counters: Arc::new(CaptureCounters::default()),
            fatal: Arc::new(Mutex::new(None)),
            current: None,
            poll_interval: config.read_timeout.max(Duration::from_millis(1)),
        };
        match source.spawn_all(config, tx) {
            Ok(()) => Ok(source),
            Err(e) => {
                source.shutdown();
                Err(e)
            }
        }
    }

    fn spawn_all(
        &mut self,
        config: &SocketSourceConfig,
        tx: SyncSender<Captured>,
    ) -> Result<(), SourceError> {
        let expected: Arc<BTreeSet<Ipv4Addr>> =
            Arc::new(config.expected_sources.iter().copied().collect());
        for (origin, binding) in config.bindings.iter().copied().enumerate() {
            let plan = config.plan(binding);
            let socket = bind_or_retry(&plan, config.stale_after)?;
            let (pool_tx, pool_rx) = mpsc::sync_channel(config.queue_capacity.max(1));
            self.pools.push(pool_tx);
            let drain = Drain {
                plan,
                stale_after: config.stale_after,
                origin,
                socket,
                tx: tx.clone(),
                free: pool_rx,
                spare: None,
                gate: SourceGate::new(Arc::clone(&expected), config.max_tracked_sources),
                control: control_buffer(),
                pending: PendingLoss::new(),
                stop: Arc::clone(&self.stop),
                counters: Arc::clone(&self.counters),
                fatal: Arc::clone(&self.fatal),
            };
            let handle = thread::Builder::new()
                .name(format!("capture-{}", binding.role.as_str()))
                .spawn(move || drain.run())
                .map_err(SourceError::Io)?;
            self.threads.push(handle);
        }
        Ok(())
    }

    #[must_use]
    pub fn stats(&self) -> CaptureStats {
        self.counters.snapshot()
    }

    #[must_use]
    pub fn counters(&self) -> &Arc<CaptureCounters> {
        &self.counters
    }

    /// The flag every drain thread and [`Source::next`] observes. Handed out so
    /// a signal handler can stop a blocked recorder.
    #[must_use]
    pub fn stop_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.stop)
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    fn shutdown(&mut self) {
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

    fn return_buffer(&self, captured: Captured) {
        if let Some(pool) = self.pools.get(captured.origin) {
            let _ = pool.try_send(captured.into_buffer());
        }
    }
}

/// What a bounded wait found.
///
/// A live source has no end, so `Source::next` cannot report "nothing yet" — its
/// `Ok(None)` means the source is finished, and a caller that read a timeout as
/// that would treat a quiet feed as a dead one. An explicit outcome keeps the
/// two apart.
#[derive(Debug)]
pub enum Waited<'a> {
    Datagram(RecordedDatagram<'a>),
    /// The deadline passed with nothing received. The source is still alive.
    TimedOut,
    /// The stop flag was set, or every drain thread is gone.
    Ended,
}

impl SocketSource {
    /// Waits at most `timeout` for the next datagram.
    ///
    /// [`Source::next`] blocks until a datagram arrives or the source is
    /// stopped, which is right for a recorder and wrong for anything that has to
    /// make progress on a schedule: a test waiting on a count hangs on a lost
    /// datagram instead of failing with what it did receive, and a caller
    /// wanting to do something else every second has nowhere to do it.
    ///
    /// The wait ends early on the stop flag and on a lost handle, exactly as the
    /// unbounded form does.
    pub fn next_within(&mut self, timeout: Duration) -> Result<Waited<'_>, SourceError> {
        if let Some(done) = self.current.take() {
            self.return_buffer(done);
        }
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(reason) = self.take_fatal() {
                return Err(SourceError::HandleLost(reason));
            }
            if self.stop.load(Ordering::Relaxed) {
                return Ok(Waited::Ended);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(Waited::TimedOut);
            }
            // Never past the deadline, and never past the poll interval either,
            // so the stop flag is still observed on the same cadence.
            match self.rx.recv_timeout(left.min(self.poll_interval)) {
                Ok(captured) => {
                    let held = self.current.insert(captured);
                    return Ok(Waited::Datagram(held.recorded()));
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return Ok(Waited::Ended),
            }
        }
    }
}

impl Source for SocketSource {
    fn next(&mut self) -> Result<Option<RecordedDatagram<'_>>, SourceError> {
        if let Some(done) = self.current.take() {
            self.return_buffer(done);
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

impl Drop for SocketSource {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nix::sys::socket::{send, socketpair};

    const GROUP: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 1);
    const PORT: u16 = 40000;

    fn synthesiser() -> Synthesiser {
        Synthesiser::new(SocketAddrV4::new(GROUP, PORT), PortRole::Mktdata)
    }

    fn arrival() -> Arrival {
        synthesiser().arrival(
            SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 1), 41000),
            &ArrivalMetadata::default(),
            || 1,
        )
    }

    fn buffer() -> Vec<u8> {
        vec![0u8; MAX_DATAGRAM_SIZE]
    }

    /// A payload the cap has room for, so nothing here is truncated by accident.
    fn extent() -> Extent {
        Extent {
            headers_len: 0,
            payload_len: 24,
            wire_payload_len: 24,
        }
    }

    fn captured() -> Captured {
        Captured::new(arrival(), buffer(), extent(), 0)
    }

    /// A drain thread with no socket: everything after the read is exercisable
    /// without one, which is where the loss accounting lives.
    fn drain(tx: SyncSender<Captured>, counters: &Arc<CaptureCounters>) -> Drain {
        let (pool_tx, pool_rx) = mpsc::sync_channel(1);
        // The record loop is what refills the pool, and these tests are it.
        drop(pool_tx);
        Drain {
            plan: BindPlan {
                binding: PortBinding::new(PortRole::Mktdata, GROUP, PORT),
                interface: Ipv4Addr::new(192, 0, 2, 7),
                recv_buffer_bytes: 1 << 20,
                read_timeout: Duration::from_millis(10),
            },
            stale_after: None,
            origin: 0,
            socket: None,
            tx,
            free: pool_rx,
            spare: None,
            gate: SourceGate::with_expected_sources([], 8),
            control: control_buffer(),
            pending: PendingLoss::new(),
            stop: Arc::new(AtomicBool::new(false)),
            counters: Arc::clone(counters),
            fatal: Arc::new(Mutex::new(None)),
        }
    }

    #[test]
    fn a_full_queue_drops_and_counts_rather_than_waiting() {
        // A drain thread that waits here overflows the receive queue behind it,
        // which turns a slow record loop into false publisher-loss findings.
        let counters = CaptureCounters::default();
        let (tx, _rx) = mpsc::sync_channel(1);
        assert!(matches!(
            offer(&tx, captured(), &counters),
            Offered::Accepted
        ));
        assert!(matches!(
            offer(&tx, captured(), &counters),
            Offered::Dropped(_)
        ));
        assert_eq!(counters.snapshot().queue_drops, 1);
    }

    #[test]
    fn a_dropped_datagram_returns_its_buffer_for_reuse() {
        let counters = CaptureCounters::default();
        let (tx, _rx) = mpsc::sync_channel(1);
        let _ = offer(&tx, captured(), &counters);
        let Offered::Dropped(buf) = offer(&tx, captured(), &counters) else {
            panic!("a full channel drops");
        };
        assert_eq!(buf.len(), MAX_DATAGRAM_SIZE);
    }

    #[test]
    fn a_record_loop_that_has_gone_away_ends_the_drain() {
        let counters = CaptureCounters::default();
        let (tx, rx) = mpsc::sync_channel(1);
        drop(rx);
        assert!(matches!(
            offer(&tx, captured(), &counters),
            Offered::Disconnected
        ));
    }

    #[test]
    fn a_datagram_we_dropped_is_admitted_on_the_next_one_that_gets_through() {
        // A datagram the record loop could not take is a datagram lost between
        // the one before it and the one after it, which is what drop_delta and
        // epb_dropcount mean. Nothing else can carry it: its buffer goes back to
        // the pool and its delta would go with it, and the archive would then
        // show a gap with nothing admitted behind it.
        let counters = Arc::new(CaptureCounters::default());
        let (tx, rx) = mpsc::sync_channel(1);
        let mut drain = drain(tx, &counters);

        assert!(matches!(
            drain.deliver(arrival(), buffer(), extent()),
            Offered::Accepted
        ));
        assert!(matches!(
            drain.deliver(arrival(), buffer(), extent()),
            Offered::Dropped(_)
        ));
        let first = rx.recv().expect("the datagram that got through");
        assert_eq!(first.recorded().drop_delta, 0);

        assert!(matches!(
            drain.deliver(arrival(), buffer(), extent()),
            Offered::Accepted
        ));
        let next = rx.recv().expect("the datagram after the one we dropped");
        assert_eq!(next.recorded().drop_delta, 1, "the datagram we dropped");
        assert_eq!(counters.snapshot().queue_drops, 1);
    }

    #[test]
    fn the_overflow_delta_of_a_datagram_we_dropped_is_not_dropped_with_it() {
        // The whole misattribution in one case: a receive-queue burst whose
        // delta rode on the datagram the record loop could not take. Lose it and
        // the archive admits none of a gap it caused twice over.
        let counters = Arc::new(CaptureCounters::default());
        let (tx, rx) = mpsc::sync_channel(1);
        let mut drain = drain(tx, &counters);
        drain.deliver(arrival(), buffer(), extent());

        let mut burst = arrival();
        burst.drop_delta = 40;
        assert!(matches!(
            drain.deliver(burst, buffer(), extent()),
            Offered::Dropped(_)
        ));
        rx.recv().expect("the datagram that got through");

        drain.deliver(arrival(), buffer(), extent());
        let next = rx.recv().expect("the datagram after the one we dropped");
        assert_eq!(
            next.recorded().drop_delta,
            41,
            "the 40 the socket lost, and the datagram we dropped"
        );
    }

    #[test]
    fn a_datagram_over_the_cap_is_archived_as_received_and_declares_its_real_length() {
        // Trimming it to the cap and declaring it whole turns a publisher
        // violation into a clean datagram, and the true size is unrecoverable
        // afterwards. MSG_TRUNC is what keeps it knowable.
        //
        // A datagram socket pair: the read path is the same recvmsg, and it
        // needs no privileges, no interface and no network.
        let (sender, receiver) = socketpair(
            AddressFamily::Unix,
            SockType::Datagram,
            None,
            SockFlag::empty(),
        )
        .expect("a socket pair");
        setsockopt(&receiver, ReceiveTimeout, &TimeVal::milliseconds(20)).expect("read timeout");
        let over_cap = vec![7u8; MAX_DATAGRAM_SIZE + 68];
        send(sender.as_raw_fd(), &over_cap, MsgFlags::empty()).expect("send");

        let counters = Arc::new(CaptureCounters::default());
        let (tx, rx) = mpsc::sync_channel(1);
        let mut drain = drain(tx, &counters);
        assert_eq!(drain.receive(&receiver, &mut synthesiser()), Step::Handed);

        let held = rx.recv().expect("the datagram");
        let dg = held.recorded();
        assert_eq!(
            dg.payload.len(),
            MAX_DATAGRAM_SIZE,
            "as much of it as the cap has room for"
        );
        assert_eq!(
            dg.wire_payload_len as usize,
            over_cap.len(),
            "the length the kernel reported, not the length we hold"
        );
        assert_eq!(dg.link_headers, None, "socket mode captures none");
        assert_eq!(counters.snapshot().truncated_datagrams, 1);
    }

    #[test]
    fn a_datagram_within_the_cap_is_declared_whole() {
        let (sender, receiver) = socketpair(
            AddressFamily::Unix,
            SockType::Datagram,
            None,
            SockFlag::empty(),
        )
        .expect("a socket pair");
        setsockopt(&receiver, ReceiveTimeout, &TimeVal::milliseconds(20)).expect("read timeout");
        send(sender.as_raw_fd(), &[7u8; 24], MsgFlags::empty()).expect("send");

        let counters = Arc::new(CaptureCounters::default());
        let (tx, rx) = mpsc::sync_channel(1);
        let mut drain = drain(tx, &counters);
        assert_eq!(drain.receive(&receiver, &mut synthesiser()), Step::Handed);

        let held = rx.recv().expect("the datagram");
        let dg = held.recorded();
        assert_eq!(dg.payload.len(), 24);
        assert_eq!(dg.wire_payload_len, 24);
        assert_eq!(counters.snapshot().truncated_datagrams, 0);
    }

    #[test]
    fn a_quiet_socket_is_not_a_datagram_and_not_a_lost_handle() {
        let (_sender, receiver) = socketpair(
            AddressFamily::Unix,
            SockType::Datagram,
            None,
            SockFlag::empty(),
        )
        .expect("a socket pair");
        setsockopt(&receiver, ReceiveTimeout, &TimeVal::milliseconds(5)).expect("read timeout");
        let counters = Arc::new(CaptureCounters::default());
        let (tx, _rx) = mpsc::sync_channel(1);
        let mut drain = drain(tx, &counters);
        assert_eq!(drain.receive(&receiver, &mut synthesiser()), Step::Quiet);
        assert_eq!(counters.snapshot().datagrams, 0);
        assert_eq!(counters.snapshot().read_errors, 0);
    }
}
