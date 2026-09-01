//! The archive, read back as a [`Source`].

use std::fs::File;
use std::io::{BufReader, Cursor, Read};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::Path;
use std::time::Duration;

use dz_edge_core::PortRole;
use dz_recorder_core::{
    CaptureDropScope, RecordedDatagram, RecorderIdentity, RecvTsKind, Source, SourceError,
};
use pcap_file::pcap::PcapReader;
use pcap_file::pcapng::blocks::enhanced_packet::{EnhancedPacketBlock, EnhancedPacketOption};
use pcap_file::pcapng::blocks::interface_description::{
    InterfaceDescriptionBlock, InterfaceDescriptionOption,
};
use pcap_file::pcapng::blocks::section_header::{SectionHeaderBlock, SectionHeaderOption};
use pcap_file::pcapng::{Block, PcapNgReader};
use pcap_file::{DataLink, PcapError};

use crate::owned::OwnedDatagram;

/// Ethernet comes first, then the IPv4 header — whose length is read rather
/// than assumed — then 8 bytes of UDP.
///
/// Stated here rather than imported from the writer's `LINK_HEADER_LEN`: a
/// reader that agreed with the writer by construction would not be checking
/// anything, and this reader is also pointed at archives the writer did not
/// write.
const ETHERNET_HEADER_LEN: usize = 14;
const ETHERTYPE_IPV4: u16 = 0x0800;
const IP_PROTO_UDP: u8 = 17;

/// pcapng's default when an interface declares no `if_tsresol`.
///
/// It is 10^-6, and `pcap-file` 2.0.0 hands the block's integer back as if it
/// were nanoseconds whatever the interface says — so the resolution has to be
/// applied here. A microsecond archive is otherwise indistinguishable from a
/// nanosecond one that happens to end in three zeros, which is the whole reason
/// the writer states `if_tsresol` at all.
const DEFAULT_TS_RESOL: u8 = 6;

/// The bound on interface description blocks one section may declare.
///
/// A recorder writes three, one per port role. `mergecap` output and a foreign
/// capture can carry more, and a generous bound costs nothing to a real file
/// while keeping a crafted one from growing this reader's memory. The archive's
/// own `CoverageTracker` caps its instance set for the same reason.
const MAX_INTERFACES: usize = 4096;

/// Why the stream ended, which a reader has to know before trusting a count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Termination {
    /// Not ended yet.
    Running,
    /// The last block ended on a block boundary: the archive is whole.
    Eof,
    /// A partial block. A recorder killed mid-write leaves one, and an
    /// interrupted copy or upload out of the completed directory leaves one in a
    /// compressed object; returning an error for the whole file would discard
    /// every datagram before the tear, so replay yields what survived and says
    /// so here.
    ///
    /// What survives is block-granular, and a compressed object's blocks are
    /// large: zstd cannot decode a partial block, so a tear costs the whole
    /// block it lands in — up to 128 KiB of datagrams — not the few bytes that
    /// went missing.
    Truncated,
    /// A block that is not a clean tear: bytes present, and not interpretable.
    /// Distinct from [`Termination::Truncated`] because corruption in the middle
    /// of an object and a recorder killed at the end of one are different
    /// findings with different responses. The datagrams before it were still
    /// yielded.
    ///
    /// This is not a corruption *detector*. A zstd frame carries no content
    /// checksum unless the compressor asked for one, so most single-byte damage
    /// decodes to different bytes with no error at all — nothing here can see
    /// that. What answers it is the manifest's sha256 of the object, checked
    /// before an object is replayed rather than after a finding is drawn from
    /// it.
    Failed,
    /// A whole, undamaged block this reader will not read as a datagram: a
    /// frame that is not IPv4 UDP, or an `interface_id` that names no port
    /// role. `mergecap` output and any mixed capture hold one, and an archive
    /// this recorder did not write is a supported input, so this is neither a
    /// tear nor corruption — reporting it as either sends an operator after a
    /// disk that is fine.
    ///
    /// The stream ends here rather than skipping the block: what follows a
    /// block this reader cannot interpret is not known to be datagrams either,
    /// and a replay that stepped over it would be short with nothing recording
    /// why. The datagrams before it were still yielded.
    Rejected,
}

/// Whether the 42 bytes in front of each payload were observed or invented,
/// as the section header states it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkHeaderProvenance {
    /// `AF_PACKET` mode: the fields were on the wire.
    Captured,
    /// Socket mode: assembled from what the socket reported. An IPv4 header has
    /// no way to express *absent*, so a field the kernel did not report was
    /// written as zero and must not be read back as an observation.
    Synthesised,
    /// The section says nothing — a pcapng this recorder did not write. The
    /// bytes are then all there is, so they are reported as they stand.
    Unstated,
}

impl LinkHeaderProvenance {
    /// A synthesised zero TTL is *not observed*; a captured zero is a TTL.
    const fn ttl(self, byte: u8) -> Option<u8> {
        match self {
            Self::Synthesised if byte == 0 => None,
            _ => Some(byte),
        }
    }
}

const PCAPNG_MAGIC: [u8; 4] = [0x0a, 0x0d, 0x0d, 0x0a];

/// The two capture formats this reader accepts.
///
/// pcapng is what this recorder writes. Classic `pcap` is what everything else
/// writes — `tcpdump` by default, and the capture an operator takes beside a
/// recorder when they want to check it against independent tooling. Refusing
/// that file would make the design's own acceptance step unexecutable with our
/// own tools.
///
/// A classic capture carries none of the metadata pcapng was chosen for: no
/// section header, so no identity and no declared stamp kind; no interface
/// block, so no port role and no timestamp resolution beyond what the file
/// magic says; and no drop count, so the recorder's own loss is unstated rather
/// than zero. Everything it cannot say is therefore reported as unknown, and
/// the port role has to come from the caller's mapping.
enum Reader {
    PcapNg(Box<PcapNgReader<Box<dyn Read>>>),
    Pcap(Box<PcapReader<Box<dyn Read>>>),
}

/// What the section header says applies to every block in it.
#[derive(Debug, Clone, Copy)]
struct SectionDefaults {
    /// The kind the section declares. A datagram that fell back carries its own
    /// comment and overrides this — section default plus per-block exception,
    /// the same discipline `epb_dropcount` uses.
    recv_ts_kind: RecvTsKind,
    link_headers: LinkHeaderProvenance,
    /// `None` when the section states no scope, which is every capture this
    /// recorder did not write. A default here would read as a measurement: it
    /// would license a per-role subtraction on an archive that never claimed one
    /// was valid, which is how a ring's losses get charged to a publisher.
    capture_drop_scope: Option<CaptureDropScope>,
}

impl Default for SectionDefaults {
    fn default() -> Self {
        Self {
            // An archive that does not say is not vouching for a kernel stamp.
            // Reading one in anyway is how a latency number that measures a
            // scheduler gets presented as a network measurement.
            recv_ts_kind: RecvTsKind::ApplicationFallback,
            link_headers: LinkHeaderProvenance::Unstated,
            capture_drop_scope: None,
        }
    }
}

/// What an `interface_id` resolves to.
#[derive(Debug, Clone, Copy)]
struct InterfaceMeta {
    /// `None` when the interface names something that is not a port role, which
    /// is a pcapng this recorder did not write.
    role: Option<PortRole>,
    ts_resol: u8,
}

/// An archive read back, presenting exactly as a live capture does.
///
/// The `Source` symmetry is the load-bearing property of the design, so
/// everything a live capture states about an arrival is recovered here: the
/// payload verbatim, `src` and `dst` from the IP and UDP headers, `role` from
/// the interface the block references, `recv_ts_ns` through the interface's own
/// resolution, `recv_ts_kind` from the section default and the block's
/// exception, and `drop_delta` from `epb_dropcount`, whose absence means zero
/// rather than unknown.
pub struct ArchiveSource {
    reader: Reader,
    section: SectionDefaults,
    /// Who wrote the section, when the section says. Held beside
    /// [`SectionDefaults`] rather than inside it because it owns its strings and
    /// the defaults travel by value to every block.
    identity: Option<RecorderIdentity>,
    /// Our own copy, indexed by `interface_id`. `pcap-file` keeps one too, but
    /// reading it would borrow the reader that is already lending out the block.
    interfaces: Vec<InterfaceMeta>,
    /// Reused across datagrams, so replay copies each payload once.
    payload: Vec<u8>,
    /// The captured Ethernet, IPv4 and UDP bytes, reused the same way. Kept so
    /// that recording a replayed archive preserves what was on the wire instead
    /// of synthesising a header over it.
    link_bytes: Vec<u8>,
    port_roles: PortRoles,
    termination: Termination,
    last_error: Option<String>,
}

/// Destination port to port role, for an archive whose interfaces do not name
/// one.
///
/// This recorder writes the role into every Interface Description block, so its
/// own archives need none of this. A capture taken beside it does not: `tcpdump`
/// names its interface `eth0`, and the role has to come from somewhere else.
///
/// The port is the right somewhere. A port role *is* a port in this design —
/// the channel instance keys on the destination port, and the recorder's own
/// configuration states the mapping — so resolving the role from the port is
/// reading the same fact from the other side rather than guessing.
///
/// Without this, the design's own acceptance step is not executable: comparing a
/// recorder's archive against a capture taken at the same point by independent
/// tooling needs something able to read that capture.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PortRoles {
    roles: Vec<(u16, PortRole)>,
}

impl PortRoles {
    /// Panics on a port claimed twice, because a port with two roles is a
    /// configuration a caller cannot have meant and a silent first-wins would
    /// attribute datagrams to whichever was listed first.
    #[must_use]
    pub fn new(roles: &[(u16, PortRole)]) -> Self {
        for (i, (port, _)) in roles.iter().enumerate() {
            assert!(
                !roles[i + 1..].iter().any(|(other, _)| other == port),
                "port {port} is given more than one port role"
            );
        }
        Self {
            roles: roles.to_vec(),
        }
    }

    #[must_use]
    pub fn role_for(&self, port: u16) -> Option<PortRole> {
        self.roles
            .iter()
            .find_map(|(p, role)| (*p == port).then_some(*role))
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.roles.is_empty()
    }
}

impl ArchiveSource {
    /// Opens a local archive, `zstd`-decoding it when the name says so.
    ///
    /// The extension is the signal because it is what the compressor wrote and
    /// what an object key carries: `.pcapng.zst` or `.pcapng`.
    pub fn open(path: &Path) -> Result<Self, SourceError> {
        let file = BufReader::new(File::open(path).map_err(SourceError::Io)?);
        let reader: Box<dyn Read> = if path.extension().is_some_and(|e| e == "zst") {
            Box::new(zstd::stream::Decoder::new(file).map_err(SourceError::Io)?)
        } else {
            Box::new(file)
        };
        Self::from_reader(reader)
    }

    /// For a caller that already holds the bytes — an object fetched into
    /// memory, or a segment under construction in a test.
    ///
    /// The format is read from the file's own magic rather than from its name.
    /// A name can be wrong, and the one case that matters most here is a capture
    /// somebody else took: it is `.pcap` by convention and classic by content,
    /// and guessing from either would be guessing.
    pub fn from_reader(reader: Box<dyn Read>) -> Result<Self, SourceError> {
        let mut magic = [0u8; 4];
        let mut reader = reader;
        // A tear inside the very first block reaches us here, because there is
        // not even a magic number left to read. It is the same refusal the
        // format's own reader would have given, and it has to say so: a source
        // that opens on nothing and reports a clean end is the worst available
        // answer.
        reader.read_exact(&mut magic).map_err(|e| {
            SourceError::MalformedArchive(format!("the archive ends inside its first block: {e}"))
        })?;
        // Put the magic back: both readers want to parse their own header.
        let restored: Box<dyn Read> = Box::new(Cursor::new(magic.to_vec()).chain(reader));

        let (reader, section, identity) = if magic == PCAPNG_MAGIC {
            let r = PcapNgReader::new(restored).map_err(open_error)?;
            let section = r.section();
            let defaults = section_defaults(section);
            let identity = section_identity(section);
            (Reader::PcapNg(Box::new(r)), defaults, identity)
        } else {
            // A classic capture states nothing about how it was made, and the
            // defaults are what "states nothing" means: no vouched-for kernel
            // stamp, and no claim about the link headers either way.
            let r = PcapReader::new(restored).map_err(open_error)?;
            // The link layer, checked once at the header rather than misparsed
            // per packet. A capture taken with `tcpdump -i any` is Linux "cooked"
            // and its sixteen-byte header is not an Ethernet one — reading it as
            // Ethernet finds a plausible-looking IPv4 header at the wrong offset,
            // which is worse than refusing.
            let link = r.header().datalink;
            if link != DataLink::ETHERNET {
                return Err(SourceError::MalformedArchive(format!(
                    "a classic capture with link type {link:?}, and this reader parses \
                     Ethernet: take the capture on the interface the feed arrives on \
                     rather than on `any`"
                )));
            }
            (Reader::Pcap(Box::new(r)), SectionDefaults::default(), None)
        };
        Ok(Self {
            reader,
            section,
            identity,
            interfaces: Vec::new(),
            payload: Vec::new(),
            link_bytes: Vec::new(),
            port_roles: PortRoles::default(),
            termination: Termination::Running,
            last_error: None,
        })
    }

    /// Resolve the port role from the destination port for blocks whose
    /// interface does not name one.
    ///
    /// An archive this recorder wrote needs none: its interfaces carry the role.
    /// A capture taken beside it does, and reading one is how a recorder's
    /// archive gets compared against independent tooling.
    #[must_use]
    pub fn with_port_roles(mut self, port_roles: PortRoles) -> Self {
        self.port_roles = port_roles;
        self
    }

    /// Why the stream ended. Meaningless before it has.
    #[must_use]
    pub const fn terminated_by(&self) -> Termination {
        self.termination
    }

    /// The error behind any termination other than [`Termination::Eof`].
    #[must_use]
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    /// What the section says about the 42 bytes in front of each payload, so a
    /// consumer knows whether a header field is an observation.
    #[must_use]
    pub const fn link_headers(&self) -> LinkHeaderProvenance {
        self.section.link_headers
    }

    /// The stamp kind the section declares, before any block's exception.
    #[must_use]
    pub const fn section_recv_ts_kind(&self) -> RecvTsKind {
        self.section.recv_ts_kind
    }

    /// The scope the section declares its capture drops at, or `None` when it
    /// declares none.
    ///
    /// This is the field the whole subtraction rests on: `drop_delta` at
    /// [`CaptureDropScope::PortRole`] may be subtracted from a per-role gap,
    /// and at [`CaptureDropScope::CaptureHandle`] it may not, because a ring
    /// counts frames dropped before it could tell the roles apart. A loader that
    /// had to be told out of band, or to read the sidecar manifest, is a loader
    /// that can be told wrong — which is exactly what carrying provenance inside
    /// the object exists to prevent.
    ///
    /// `None` is *unknown*, never a scope: a classic capture states nothing, and
    /// answering with a default would license a subtraction the archive never
    /// claimed was valid.
    #[must_use]
    pub const fn capture_drop_scope(&self) -> Option<CaptureDropScope> {
        self.section.capture_drop_scope
    }

    /// Which recorder, which build and which configuration wrote this section,
    /// or `None` when the section does not say.
    ///
    /// An object separated from its context — copied, renamed, or pulled out of
    /// a bucket by hand — must still be able to say where it came from, and a
    /// finding is only attributable if that travels with the bytes rather than
    /// in a manifest beside them.
    ///
    /// All six fields or none: an identity missing one of them names no
    /// recorder, and half a provenance presented as a whole one is worse than
    /// admitting the archive is somebody else's.
    #[must_use]
    pub const fn identity(&self) -> Option<&RecorderIdentity> {
        self.identity.as_ref()
    }

    /// Fills `self.payload` and returns everything else about the arrival.
    ///
    /// Split out from [`Source::next`] so the block — which borrows the reader —
    /// is dropped before a datagram borrowing `self.payload` is handed back.
    /// Everything below touches individual fields rather than `self`, for the
    /// same reason: the block on loan from `self.reader` is alive throughout.
    fn next_arrival(&mut self) -> Result<Option<Arrival>, SourceError> {
        loop {
            if self.termination != Termination::Running {
                // An ended stream stays ended. A reader that re-parses the torn
                // block would hand back the same error for ever.
                return Ok(None);
            }
            // A classic capture is a flat list of packets: no blocks, no
            // interfaces, no section to restate. Everything after this point is
            // shared, because what a datagram *is* does not depend on the
            // container it arrived in.
            if let Reader::Pcap(reader) = &mut self.reader {
                let Some(result) = reader.next_packet() else {
                    self.termination = Termination::Eof;
                    return Ok(None);
                };
                let packet = match result {
                    Ok(packet) => packet,
                    Err(e) => return Err(self.end_on(classic_error(e))),
                };
                let arrival = match read_classic(
                    self.section,
                    &self.port_roles,
                    &packet.data,
                    packet.timestamp,
                    packet.orig_len,
                ) {
                    Ok(arrival) => arrival,
                    Err(e) => return Err(self.end_on(e)),
                };
                self.payload.clear();
                self.payload
                    .extend_from_slice(&packet.data[arrival.payload_at..arrival.payload_end]);
                self.link_bytes.clear();
                return Ok(Some(arrival));
            }

            let Reader::PcapNg(reader) = &mut self.reader else {
                unreachable!("the classic arm returns above")
            };
            let Some(result) = reader.next_block() else {
                self.termination = Termination::Eof;
                return Ok(None);
            };
            match result {
                Ok(Block::EnhancedPacket(epb)) => {
                    // The refusal is recorded here rather than left to the
                    // caller's error handling, because the `Iterator` adapter
                    // maps the error to `None` by design and a stream that
                    // ended on a refused block while still reporting `Running`
                    // is a short replay that reads as a complete one — which is
                    // a sequence gap with no admitted loss behind it, and so a
                    // publisher finding drawn from our own truncation.
                    let arrival =
                        match read_packet(self.section, &self.interfaces, &self.port_roles, &epb) {
                            Ok(arrival) => arrival,
                            Err(e) => {
                                self.termination = Termination::Rejected;
                                self.last_error = Some(e.to_string());
                                return Err(e);
                            }
                        };
                    self.payload.clear();
                    self.payload
                        .extend_from_slice(&epb.data[arrival.payload_at..arrival.payload_end]);
                    self.link_bytes.clear();
                    if arrival.link_headers == LinkHeaderProvenance::Captured {
                        self.link_bytes
                            .extend_from_slice(&epb.data[..arrival.payload_at]);
                    }
                    return Ok(Some(arrival));
                }
                // Both of these appear in the block stream after the first
                // section header, which the reader consumed on construction.
                Ok(Block::InterfaceDescription(idb)) => {
                    // Capped, because this is the one set in the replay path an
                    // archive can grow: a `.pcapng.zst` of nothing but interface
                    // description blocks compresses thousands to one, so a small
                    // file walks this Vec toward the memory of whatever is
                    // replaying it. Three roles are what a recorder writes and a
                    // foreign capture has its own; a file past this bound is not
                    // an archive with many interfaces, it is a file built to be
                    // read. Rejected the way a malformed block is, so a replay
                    // that stopped early can never read as a complete one.
                    if self.interfaces.len() >= MAX_INTERFACES {
                        let e = SourceError::MalformedArchive(format!(
                            "the archive declares more than {MAX_INTERFACES} interfaces"
                        ));
                        self.termination = Termination::Rejected;
                        self.last_error = Some(e.to_string());
                        return Err(e);
                    }
                    self.interfaces.push(interface_meta(&idb));
                }
                Ok(Block::SectionHeader(shb)) => {
                    // A new section restates everything: `mergecap` produces
                    // one per input, and its interfaces are its own.
                    self.section = section_defaults(&shb);
                    self.identity = section_identity(&shb);
                    self.interfaces.clear();
                }
                // Interface Statistics and anything else: not a datagram, and
                // not this reader's business.
                Ok(_) => {}
                Err(e) => {
                    let text = describe(&e);
                    let (termination, error) = classify(e);
                    self.termination = termination;
                    self.last_error = Some(text);
                    return Err(error);
                }
            }
        }
    }
}

/// Ends the stream on a refusal and records why, so no caller can read a short
/// replay as a complete one.
impl ArchiveSource {
    fn end_on(&mut self, e: SourceError) -> SourceError {
        self.termination = Termination::Rejected;
        self.last_error = Some(e.to_string());
        e
    }
}

/// One packet of a classic capture, read as a datagram.
///
/// The container states nothing, so nothing is inferred from it: the role comes
/// from the caller's port mapping, the stamp kind stays whatever the section
/// defaults say a silent container means, and the recorder's own loss is zero
/// because a classic capture has nowhere to record one — an absence, not a
/// measurement of none.
fn read_classic(
    section: SectionDefaults,
    port_roles: &PortRoles,
    data: &[u8],
    timestamp: Duration,
    orig_len: u32,
) -> Result<Arrival, SourceError> {
    let link = parse_link_headers(data)?;
    let role = port_roles.role_for(link.dst.port()).ok_or_else(|| {
        SourceError::MalformedArchive(format!(
            "a classic capture names no port role, and none is configured for \
             destination port {}",
            link.dst.port()
        ))
    })?;
    Ok(Arrival {
        src: link.src,
        dst: link.dst,
        role,
        recv_ts_ns: u64::try_from(timestamp.as_nanos()).unwrap_or(u64::MAX),
        recv_ts_kind: section.recv_ts_kind,
        drop_delta: 0,
        ttl: section.link_headers.ttl(link.ttl),
        link_headers: section.link_headers,
        wire_payload_len: orig_len
            .saturating_sub(u32::try_from(link.payload_at).unwrap_or(u32::MAX))
            .max(u32::try_from(link.payload_end - link.payload_at).unwrap_or(u32::MAX)),
        payload_at: link.payload_at,
        payload_end: link.payload_end,
    })
}

fn classic_error(e: PcapError) -> SourceError {
    SourceError::MalformedArchive(format!("reading a classic capture: {e}"))
}

/// Recovers everything a live capture would have stated about the arrival.
fn read_packet(
    section: SectionDefaults,
    interfaces: &[InterfaceMeta],
    port_roles: &PortRoles,
    epb: &EnhancedPacketBlock<'_>,
) -> Result<Arrival, SourceError> {
    let iface = interfaces
        .get(epb.interface_id as usize)
        .copied()
        .ok_or_else(|| {
            SourceError::MalformedArchive(format!(
                "interface_id {} has no interface description block",
                epb.interface_id
            ))
        })?;
    // The frame first, because the destination port in it is the fallback the
    // port role is resolved from.
    let link = parse_link_headers(&epb.data)?;

    // Recovered from the block the packet references rather than assumed from
    // its index: the index is stable in this recorder's own archives, and an
    // archive is read by things this recorder did not write. Such an archive
    // names its interface after a device, so the role falls back to the
    // destination port — which is what a port role is.
    let role = iface
        .role
        .or_else(|| port_roles.role_for(link.dst.port()))
        .ok_or_else(|| {
            SourceError::MalformedArchive(format!(
                "interface_id {} names no port role, and no port role is \
                 configured for destination port {}",
                epb.interface_id,
                link.dst.port()
            ))
        })?;
    let mut recv_ts_kind = section.recv_ts_kind;
    let mut link_headers = section.link_headers;
    let mut drop_delta = 0u32;
    for option in &epb.options {
        match option {
            // Absent means zero, not unknown: the writer omits the option when
            // the delta was zero, because one on every datagram is noise.
            EnhancedPacketOption::DropCount(n) => {
                drop_delta = u32::try_from(*n).unwrap_or(u32::MAX);
            }
            // The section states the kind; a datagram that fell back states its
            // own. The exception is what needs saying.
            EnhancedPacketOption::Comment(c) => {
                if let Some(kind) = recv_ts_kind_of(c) {
                    recv_ts_kind = kind;
                }
                // Provenance is stated per section and excepted per datagram,
                // the same way the stamp kind is. Ignoring the exception is how
                // a zero the writer synthesised comes back as a TTL somebody
                // measured, which is the reading `ttl: Option<u8>` exists to
                // make impossible.
                if let Some(provenance) = link_headers_of(c) {
                    link_headers = provenance;
                }
            }
            _ => {}
        }
    }

    Ok(Arrival {
        src: link.src,
        dst: link.dst,
        role,
        recv_ts_ns: to_nanos(timestamp_units(epb), iface.ts_resol),
        recv_ts_kind,
        drop_delta,
        ttl: link_headers.ttl(link.ttl),
        link_headers,
        // Never less than what we actually hold: a foreign archive with a
        // nonsensical original length must not claim fewer bytes were sent than
        // the ones in front of us.
        wire_payload_len: epb
            .original_len
            .saturating_sub(u32::try_from(link.payload_at).unwrap_or(u32::MAX))
            .max(u32::try_from(link.payload_end - link.payload_at).unwrap_or(u32::MAX)),
        payload_at: link.payload_at,
        payload_end: link.payload_end,
    })
}

/// Which kind of ending this error is.
///
/// A partial block is a recorder killed mid-write, and the datagrams before it
/// have already been yielded; anything else is corruption or a failing disk,
/// which is a different finding and must not be reported as a tear.
fn classify(e: PcapError) -> (Termination, SourceError) {
    let tear = || {
        (
            Termination::Truncated,
            SourceError::MalformedArchive("the archive ends inside a block".to_owned()),
        )
    };
    match e {
        PcapError::IncompleteBuffer => tear(),
        PcapError::IoError(io) if io.kind() == std::io::ErrorKind::UnexpectedEof => tear(),
        PcapError::IoError(io) => (Termination::Failed, SourceError::Io(io)),
        other => (
            Termination::Failed,
            SourceError::MalformedArchive(other.to_string()),
        ),
    }
}

impl Source for ArchiveSource {
    /// `Ok(None)` is the end of a whole archive.
    ///
    /// A tear, corruption or a refused block yields every datagram before it and
    /// then returns the error, so a caller cannot mistake a short archive for a
    /// complete one: a short replay read as complete becomes a sequence gap, and
    /// a sequence gap with no admitted loss behind it becomes a publisher
    /// finding — which is the one mistake this whole design exists to prevent.
    fn next(&mut self) -> Result<Option<RecordedDatagram<'_>>, SourceError> {
        let Some(a) = self.next_arrival()? else {
            return Ok(None);
        };
        Ok(Some(RecordedDatagram {
            payload: &self.payload,
            src: a.src,
            dst: a.dst,
            role: a.role,
            recv_ts_ns: a.recv_ts_ns,
            recv_ts_kind: a.recv_ts_kind,
            drop_delta: a.drop_delta,
            ttl: a.ttl,
            // Some only when the archive vouches for them. A synthesised
            // header is not evidence, and handing it on as captured is how a
            // reconstruction becomes a claim about the wire.
            link_headers: (!self.link_bytes.is_empty()).then_some(self.link_bytes.as_slice()),
            wire_payload_len: a.wire_payload_len,
        }))
    }
}

/// Iteration ends at a tear, at corruption and at a refused block as well as at
/// the end, and [`ArchiveSource::terminated_by`] says which — because an archive
/// that iterated to a shorter count in silence is the failure this whole type
/// exists to make visible. The error is swallowed here so that `.count()` and
/// `.collect()` stay usable, which is exactly why nothing may end this stream
/// without recording why it ended.
impl Iterator for ArchiveSource {
    type Item = OwnedDatagram;

    fn next(&mut self) -> Option<Self::Item> {
        match Source::next(self) {
            Ok(Some(dg)) => Some(OwnedDatagram::from_recorded(&dg)),
            Ok(None) | Err(_) => None,
        }
    }
}

/// Everything about one arrival except the payload, which the caller has just
/// copied out of the block.
struct Arrival {
    src: SocketAddrV4,
    dst: SocketAddrV4,
    role: PortRole,
    recv_ts_ns: u64,
    recv_ts_kind: RecvTsKind,
    drop_delta: u32,
    ttl: Option<u8>,
    /// Per datagram, not per section: a section may claim captured headers and
    /// except an individual datagram, and the exception is what decides whether
    /// the bytes we hand back were on the wire.
    link_headers: LinkHeaderProvenance,
    /// From the block's original length rather than from what it holds, which is
    /// the only place the writer recorded it: a datagram the capture length cut
    /// short must not replay as one that arrived whole.
    wire_payload_len: u32,
    payload_at: usize,
    payload_end: usize,
}

struct LinkFields {
    src: SocketAddrV4,
    dst: SocketAddrV4,
    ttl: u8,
    payload_at: usize,
    payload_end: usize,
}

/// Recovers the addresses, the ports and the TTL from the headers in front of
/// the payload.
///
/// The IPv4 header length is read rather than assumed: this reader is pointed at
/// captured frames as well as at synthesised ones, and a captured header may
/// carry options.
fn parse_link_headers(data: &[u8]) -> Result<LinkFields, SourceError> {
    if data.len() < ETHERNET_HEADER_LEN + 20 + 8 {
        return Err(SourceError::MalformedArchive(format!(
            "a packet block of {} bytes cannot hold Ethernet, IPv4 and UDP",
            data.len()
        )));
    }
    let ethertype = u16::from_be_bytes([data[12], data[13]]);
    if ethertype != ETHERTYPE_IPV4 {
        return Err(SourceError::MalformedArchive(format!(
            "ethertype {ethertype:#06x} is not IPv4"
        )));
    }

    let ip = &data[ETHERNET_HEADER_LEN..];
    if ip[0] >> 4 != 4 {
        return Err(SourceError::MalformedArchive(format!(
            "IP version {} is not 4",
            ip[0] >> 4
        )));
    }
    let ihl = usize::from(ip[0] & 0x0f) * 4;
    if ihl < 20 || ip.len() < ihl + 8 {
        return Err(SourceError::MalformedArchive(format!(
            "an IPv4 header of {ihl} bytes does not fit the block"
        )));
    }
    if ip[9] != IP_PROTO_UDP {
        return Err(SourceError::MalformedArchive(format!(
            "IP protocol {} is not UDP",
            ip[9]
        )));
    }
    let ttl = ip[8];
    let src_ip = Ipv4Addr::new(ip[12], ip[13], ip[14], ip[15]);
    let dst_ip = Ipv4Addr::new(ip[16], ip[17], ip[18], ip[19]);

    let udp = &ip[ihl..];
    let src_port = u16::from_be_bytes([udp[0], udp[1]]);
    let dst_port = u16::from_be_bytes([udp[2], udp[3]]);
    let udp_len = usize::from(u16::from_be_bytes([udp[4], udp[5]]));

    let payload_at = ETHERNET_HEADER_LEN + ihl + 8;
    // The UDP length decides where the payload ends when the block holds more
    // than the datagram: a captured frame shorter than 60 bytes arrives padded
    // to the Ethernet minimum, and that padding is not payload.
    let payload_end = match udp_len.checked_sub(8) {
        Some(n) if payload_at + n <= data.len() => payload_at + n,
        _ => data.len(),
    };

    Ok(LinkFields {
        src: SocketAddrV4::new(src_ip, src_port),
        dst: SocketAddrV4::new(dst_ip, dst_port),
        ttl,
        payload_at,
        payload_end,
    })
}

/// The block's integer, in whatever units its interface declared.
///
/// `pcap-file` 2.0.0 parses the two 32-bit halves and hands them back as a
/// `Duration` of nanoseconds regardless of `if_tsresol`, so this undoes that
/// assumption rather than inheriting it.
fn timestamp_units(epb: &EnhancedPacketBlock<'_>) -> u64 {
    u64::try_from(epb.timestamp.as_nanos()).unwrap_or(u64::MAX)
}

/// `if_tsresol` is decimal when the top bit is clear and binary when it is set.
///
/// All 128 values of the low seven bits are legal bytes, and most of them name a
/// resolution no clock has: 10^-127 s is finer, and one second coarser, than a
/// `u64` of nanoseconds can express. One arrives here from a corrupt or hostile
/// Interface Description Block, so each value has to land somewhere chosen:
///
/// - Finer than a nanosecond is truncated to what nanoseconds can say about it.
///   A `u64` of 10^-48 s units is under one nanosecond however large it is, so
///   the answer is 0 — the truncation `recv_ts_ns`'s own unit forces, arrived at
///   the same way 10^-12 s units are, not a rejection of the block.
/// - Coarser, where the product does not fit, saturates at `u64::MAX` rather
///   than wrapping: a wrapped stamp is a plausible time that is wrong, and
///   nothing downstream can tell it from a measured one.
fn to_nanos(units: u64, resol: u8) -> u64 {
    /// The largest power of ten a `u128` holds: 10^39 does not.
    const MAX_POW10: u32 = 38;

    let units = u128::from(units);
    let ns = if resol & 0x80 == 0 {
        let decimals = u32::from(resol & 0x7f);
        if decimals <= 9 {
            units * 10u128.pow(9 - decimals)
        } else {
            // The exponent is capped before it is raised, not after: 10^39
            // already exceeds `u128`, and `pow` panics on that in a debug build
            // and wraps to a nonsense divisor in a release one. Capping loses
            // nothing, because 10^38 already divides every `u64` to zero.
            units / 10u128.pow((decimals - 9).min(MAX_POW10))
        }
    } else {
        // Seven bits cap the shift at 127, which a `u128` holds, and the
        // multiply comes first so a coarse resolution keeps its precision.
        let bits = u32::from(resol & 0x7f);
        units * 1_000_000_000 / (1u128 << bits)
    };
    u64::try_from(ns).unwrap_or(u64::MAX)
}

fn section_defaults(shb: &SectionHeaderBlock<'_>) -> SectionDefaults {
    let mut defaults = SectionDefaults::default();
    for option in &shb.options {
        if let SectionHeaderOption::Comment(comment) = option {
            if let Some(kind) = recv_ts_kind_of(comment) {
                defaults.recv_ts_kind = kind;
            }
            if let Some(provenance) = link_headers_of(comment) {
                defaults.link_headers = provenance;
            }
            if let Some(scope) = capture_drop_scope_of(comment) {
                defaults.capture_drop_scope = Some(scope);
            }
        }
    }
    defaults
}

/// The recorder's identity out of the section comment, or `None` unless every
/// field is there.
///
/// The comment is the one place the writer states all six, and a section that
/// states some of them is a pcapng somebody else wrote whose comment happens to
/// share a key. Assembling a `RecorderIdentity` out of those would put a
/// recorder name on an object no recorder of ours produced.
fn section_identity(shb: &SectionHeaderBlock<'_>) -> Option<RecorderIdentity> {
    shb.options.iter().find_map(|option| {
        let SectionHeaderOption::Comment(comment) = option else {
            return None;
        };
        Some(RecorderIdentity {
            site: value_of(comment, "site")?.to_owned(),
            recorder: value_of(comment, "recorder")?.to_owned(),
            env: value_of(comment, "env")?.to_owned(),
            build_version: value_of(comment, "build_version")?.to_owned(),
            build_commit: value_of(comment, "build_commit")?.to_owned(),
            config_hash: value_of(comment, "config_hash")?.to_owned(),
        })
    })
}

fn interface_meta(idb: &InterfaceDescriptionBlock<'_>) -> InterfaceMeta {
    let mut meta = InterfaceMeta {
        role: None,
        ts_resol: DEFAULT_TS_RESOL,
    };
    for option in &idb.options {
        match option {
            InterfaceDescriptionOption::IfName(name) => meta.role = port_role_of(name),
            InterfaceDescriptionOption::IfTsResol(resol) => meta.ts_resol = *resol,
            _ => {}
        }
    }
    meta
}

/// The spec's three tokens, and no alias: a port role with two spellings is a
/// join that silently returns nothing.
fn port_role_of(name: &str) -> Option<PortRole> {
    match name {
        "mktdata" => Some(PortRole::Mktdata),
        "refdata" => Some(PortRole::Refdata),
        "snapshot" => Some(PortRole::Snapshot),
        _ => None,
    }
}

fn link_headers_of(comment: &str) -> Option<LinkHeaderProvenance> {
    match value_of(comment, "link_headers") {
        Some("captured") => Some(LinkHeaderProvenance::Captured),
        Some("synthesised") => Some(LinkHeaderProvenance::Synthesised),
        _ => None,
    }
}

/// The two tokens `CaptureDropScope::as_str` writes, and no other: a scope this
/// reader does not recognise is not a scope it may subtract under.
fn capture_drop_scope_of(comment: &str) -> Option<CaptureDropScope> {
    match value_of(comment, "capture_drop_scope") {
        Some("port-role") => Some(CaptureDropScope::PortRole),
        Some("capture-handle") => Some(CaptureDropScope::CaptureHandle),
        _ => None,
    }
}

fn recv_ts_kind_of(comment: &str) -> Option<RecvTsKind> {
    match value_of(comment, "recv_ts_kind") {
        Some("kernel-software") => Some(RecvTsKind::KernelSoftware),
        Some("application-fallback") => Some(RecvTsKind::ApplicationFallback),
        _ => None,
    }
}

/// The writer's comments are `key=value` pairs separated by `;`, because they
/// are read by a program at least as often as by a person.
fn value_of<'a>(comment: &'a str, key: &str) -> Option<&'a str> {
    comment
        .split(';')
        .filter_map(|part| part.trim().split_once('='))
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v.trim())
}

/// A tear inside the very first block leaves nothing to open.
///
/// It is reported here, naming the tear, rather than handed back as an archive
/// that opens and ends cleanly with nothing in it: an empty replay of a
/// non-empty object is the same misattribution as a short one, one layer down.
/// A compressed object reaches this more often than a plain one, because zstd
/// cannot decode a partial block at all — a tear inside the first block of a
/// small object leaves no datagram to survive.
fn open_error(e: PcapError) -> SourceError {
    match classify(e) {
        (Termination::Truncated, _) => {
            SourceError::MalformedArchive("the archive ends inside its first block".to_owned())
        }
        (_, error) => error,
    }
}

/// The error and its cause, because the cause is the evidence for the verdict.
///
/// `pcap-file` reports every read failure as "Error reading bytes" and carries
/// the reason underneath, and the reason is the whole distinction: zstd calls a
/// partial frame an `incomplete frame` and every corruption something else.
fn describe(e: &PcapError) -> String {
    match std::error::Error::source(e) {
        Some(cause) => format!("{e}: {cause}"),
        None => e.to_string(),
    }
}
