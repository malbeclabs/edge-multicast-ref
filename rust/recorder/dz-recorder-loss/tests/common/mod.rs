//! One datagram stream, and the three ways these tests present it: straight to
//! the deriver, straight to the live health tier, and through a real archive.
//!
//! The conformant datagrams come from the real [`DatagramBuilder`] and the real
//! message types, so nothing here re-implements a publisher. The one thing
//! written by hand is a header with every field a caller's to state wrongly —
//! the builder stamps `Magic`, the schema version and the declared length
//! itself, which is what a builder is for and why a datagram a decoder would
//! refuse has to be assembled somewhere with no opinion.
#![allow(dead_code)]
#![forbid(unsafe_code)]

use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use dz_edge_core::{
    ChannelSequence, DatagramBuilder, Feed, PortRole, ResetCount, DATAGRAM_HEADER_SIZE,
    SCHEMA_VERSION,
};
use dz_edge_tob::{Quote, TopOfBook, QUOTE_ASK_UPDATED, QUOTE_BID_UPDATED};
use dz_recorder_archive::rotate::{ArchiveWriter, ArchiveWriterConfig};
use dz_recorder_archive::writer::{LinkHeaders, RoleJoin};
use dz_recorder_archive::Compression;
use dz_recorder_core::{
    CaptureDropScope, ChannelInstance, Observer, RecordedDatagram, RecorderIdentity, RecvTsKind,
    Sink, Source, SourceError,
};
use dz_recorder_health::{
    FeedSeries, HealthMetrics, HealthMetricsConfig, HealthObserver, InstanceLimits,
};
use dz_recorder_loss::{DeriverLimits, LossDeriver, LossReport};
use dz_recorder_replay::{ArchiveSource, OwnedDatagram, Termination};

/// MCAST-TEST-NET (RFC 2365) and the RFC 5737 documentation ranges throughout.
/// An address in a test is copied into a configuration sooner or later, and an
/// address outside those ranges names a network somebody really runs.
pub const GROUP: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 10);
pub const PUBLISHER_A: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
/// A second publisher serving the same `Channel ID` to the same group and port:
/// a distinct channel instance with its own sequence space and its own
/// `Reset Count`.
pub const PUBLISHER_B: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 2);
/// Not part of the channel instance, so one value does for everything here.
pub const EGRESS_PORT: u16 = 41_000;
pub const CHANNEL: u8 = 1;

/// A receive stamp with all nine digits populated, so an archive that rounded
/// to microseconds could not pass a comparison against it.
pub const FIRST_RECV_TS_NS: u64 = 1_772_000_000_123_456_789;
/// Coprime with every power of ten, for the same reason.
pub const RECV_TS_STEP_NS: u64 = 7_654_321;

/// Below the mandated cap, so `remaining` ends a datagram here rather than the
/// clamp.
pub const MTU: u16 = 1200;

pub fn feed() -> &'static str {
    TopOfBook::NAME
}

pub fn port_of(role: PortRole) -> u16 {
    match role {
        PortRole::Mktdata => 40_000,
        PortRole::Refdata => 40_001,
        PortRole::Snapshot => 40_002,
    }
}

#[must_use]
pub fn instance(source: Ipv4Addr, role: PortRole) -> ChannelInstance {
    ChannelInstance::new(source, CHANNEL, port_of(role))
}

/// One conformant datagram, from the real encoder.
///
/// `mktdata` only: every message type in these crates lists that role and the
/// builder refuses the others, which is a property of the messages rather than
/// of the recorder. A test needing another role states its header itself.
#[must_use]
pub fn encoded(channel_id: u8, sequence_number: u64, reset_count: u8) -> Vec<u8> {
    let sequence = ChannelSequence::resume(channel_id, ResetCount(reset_count), sequence_number);
    let mut builder = DatagramBuilder::<TopOfBook>::new(sequence, PortRole::Mktdata, MTU);
    builder
        .push(&quote())
        .expect("the builder accepts a quote on mktdata");
    builder
        .finish(FIRST_RECV_TS_NS - 1_000_000)
        .expect("a datagram holding a message is emittable")
}

fn quote() -> Quote {
    Quote {
        instrument_id: 11,
        source_id: 7,
        update_flags: QUOTE_BID_UPDATED | QUOTE_ASK_UPDATED,
        source_timestamp_ns: FIRST_RECV_TS_NS - 2_000_000,
        bid_price: 1_234_500,
        bid_qty: 10,
        ask_price: 1_234_600,
        ask_qty: 12,
        bid_source_count: 2,
        ask_source_count: 3,
    }
}

/// A datagram header with every field a caller's to state, correctly or not.
///
/// The offsets are the spec's field table: `Magic` at 0, `Schema Version` at 2,
/// `Channel ID` at 3, `Sequence Number` at 4, `Send Timestamp` at 12,
/// `Message Count` at 20, `Reset Count` at 21, `Frame Length` at 22.
#[derive(Debug, Clone, Copy)]
pub struct Header {
    pub magic: u16,
    pub schema_version: u8,
    pub channel_id: u8,
    pub sequence_number: u64,
    pub send_timestamp_ns: u64,
    pub msg_count: u8,
    pub reset_count: u8,
    pub datagram_len: u16,
}

/// The zero-filled body a hand-written header frames. The deriver reads 24 bytes
/// and never looks past them, so the body's only job is to exist.
const BODY_LEN: usize = 16;

impl Header {
    #[must_use]
    pub fn conformant(channel_id: u8, sequence_number: u64, reset_count: u8) -> Self {
        Self {
            magic: TopOfBook::MAGIC,
            schema_version: SCHEMA_VERSION,
            channel_id,
            sequence_number,
            send_timestamp_ns: FIRST_RECV_TS_NS - 1_000_000,
            msg_count: 1,
            reset_count,
            datagram_len: u16::try_from(DATAGRAM_HEADER_SIZE + BODY_LEN)
                .expect("a datagram is small"),
        }
    }

    #[must_use]
    pub fn bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; DATAGRAM_HEADER_SIZE + BODY_LEN];
        buf[0..2].copy_from_slice(&self.magic.to_le_bytes());
        buf[2] = self.schema_version;
        buf[3] = self.channel_id;
        buf[4..12].copy_from_slice(&self.sequence_number.to_le_bytes());
        buf[12..20].copy_from_slice(&self.send_timestamp_ns.to_le_bytes());
        buf[20] = self.msg_count;
        buf[21] = self.reset_count;
        buf[22..24].copy_from_slice(&self.datagram_len.to_le_bytes());
        buf
    }
}

/// The receive side: what a capture would have said about each arrival.
///
/// Stamps advance by a fixed step rather than by a clock read, because a run's
/// timestamps are asserted to the nanosecond and a test cannot assert against a
/// value it did not choose.
#[derive(Debug, Clone, Default)]
pub struct Stream {
    next_ts_ns: u64,
    pub sent: Vec<OwnedDatagram>,
}

impl Stream {
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_ts_ns: FIRST_RECV_TS_NS,
            sent: Vec::new(),
        }
    }

    /// One encoded datagram on `mktdata`, from `source`.
    pub fn send(&mut self, source: Ipv4Addr, sequence_number: u64, reset_count: u8) {
        self.arrive(
            encoded(CHANNEL, sequence_number, reset_count),
            source,
            PortRole::Mktdata,
            0,
        );
    }

    /// The same, addressed to another group on the same port — which is what an
    /// AF_PACKET filter delivers when two groups share a port.
    pub fn send_to_group(
        &mut self,
        source: Ipv4Addr,
        sequence_number: u64,
        reset_count: u8,
        group: Ipv4Addr,
    ) {
        let payload = encoded(CHANNEL, sequence_number, reset_count);
        let wire_payload_len = u32::try_from(payload.len()).expect("a datagram is small");
        self.sent.push(OwnedDatagram {
            payload,
            src: SocketAddrV4::new(source, EGRESS_PORT),
            dst: SocketAddrV4::new(group, port_of(PortRole::Mktdata)),
            role: PortRole::Mktdata,
            recv_ts_ns: self.next_ts_ns,
            recv_ts_kind: RecvTsKind::KernelSoftware,
            drop_delta: 0,
            ttl: Some(31),
            link_headers: None,
            wire_payload_len,
        });
        self.next_ts_ns += RECV_TS_STEP_NS;
    }

    /// The same, behind `drop_delta` datagrams the capture handle lost between
    /// the previous one and this one — which is what pcapng's `epb_dropcount`
    /// is defined as.
    pub fn send_after_loss(
        &mut self,
        source: Ipv4Addr,
        sequence_number: u64,
        reset_count: u8,
        drop_delta: u32,
    ) {
        self.arrive(
            encoded(CHANNEL, sequence_number, reset_count),
            source,
            PortRole::Mktdata,
            drop_delta,
        );
    }

    /// One hand-written datagram, on any role.
    pub fn send_header(&mut self, source: Ipv4Addr, role: PortRole, header: Header) {
        self.arrive(header.bytes(), source, role, 0);
    }

    /// The same, behind admitted loss.
    pub fn send_header_after_loss(
        &mut self,
        source: Ipv4Addr,
        role: PortRole,
        header: Header,
        drop_delta: u32,
    ) {
        self.arrive(header.bytes(), source, role, drop_delta);
    }

    /// Bytes nobody claims are a datagram.
    pub fn send_bytes(&mut self, source: Ipv4Addr, role: PortRole, payload: Vec<u8>) {
        self.arrive(payload, source, role, 0);
    }

    fn arrive(&mut self, payload: Vec<u8>, source: Ipv4Addr, role: PortRole, drop_delta: u32) {
        let wire_payload_len = u32::try_from(payload.len()).expect("a datagram is small");
        self.sent.push(OwnedDatagram {
            payload,
            src: SocketAddrV4::new(source, EGRESS_PORT),
            dst: SocketAddrV4::new(GROUP, port_of(role)),
            role,
            recv_ts_ns: self.next_ts_ns,
            recv_ts_kind: RecvTsKind::KernelSoftware,
            drop_delta,
            // Socket mode reports it through `IP_RECVTTL`, and a non-zero value
            // is what distinguishes an observation from the zero a synthesised
            // header writes for *absent*.
            ttl: Some(4),
            link_headers: None,
            wire_payload_len,
        });
        self.next_ts_ns += RECV_TS_STEP_NS;
    }

    /// The receive stamp the `index`-th datagram arrived with.
    #[must_use]
    pub fn ts_of(&self, index: usize) -> u64 {
        self.sent[index].recv_ts_ns
    }

    /// Every port role the stream used, in a stable order, for the archive's
    /// stated joins.
    #[must_use]
    pub fn roles(&self) -> Vec<PortRole> {
        let mut roles = Vec::new();
        for dg in &self.sent {
            if !roles.contains(&dg.role) {
                roles.push(dg.role);
            }
        }
        roles
    }
}

/// A stream presented as a [`Source`], so every test reaches the deriver through
/// [`LossDeriver::drive`] rather than through a per-datagram call a real caller
/// would not make.
pub struct StreamSource<'a> {
    datagrams: &'a [OwnedDatagram],
    next: usize,
    /// The index at which the source fails instead of yielding, for the one test
    /// about an incomplete window.
    fail_at: Option<usize>,
}

impl<'a> StreamSource<'a> {
    #[must_use]
    pub fn new(datagrams: &'a [OwnedDatagram]) -> Self {
        Self {
            datagrams,
            next: 0,
            fail_at: None,
        }
    }

    #[must_use]
    pub fn failing_at(datagrams: &'a [OwnedDatagram], index: usize) -> Self {
        Self {
            datagrams,
            next: 0,
            fail_at: Some(index),
        }
    }
}

impl Source for StreamSource<'_> {
    fn next(&mut self) -> Result<Option<RecordedDatagram<'_>>, SourceError> {
        if self.fail_at == Some(self.next) {
            return Err(SourceError::MalformedArchive("a tear".to_owned()));
        }
        let Some(dg) = self.datagrams.get(self.next) else {
            return Ok(None);
        };
        self.next += 1;
        Ok(Some(dg.as_recorded()))
    }
}

/// Drives a stream through the deriver, in receive order.
#[must_use]
pub fn derive(scope: CaptureDropScope, sent: &[OwnedDatagram]) -> LossReport {
    let mut deriver = LossDeriver::new(scope);
    let mut source = StreamSource::new(sent);
    deriver.drive(&mut source).expect("the stream is whole");
    deriver.finish()
}

/// The same, at bounds a test can bring within reach of a small fixture.
pub fn derive_with_limits(
    scope: CaptureDropScope,
    sent: &[OwnedDatagram],
    limits: DeriverLimits,
) -> LossReport {
    let mut deriver = LossDeriver::with_limits(scope, limits);
    let mut source = StreamSource::new(sent);
    deriver.drive(&mut source).expect("the stream is whole");
    deriver.finish()
}

pub fn identity() -> RecorderIdentity {
    RecorderIdentity {
        site: "site-1".to_owned(),
        recorder: "recorder-1".to_owned(),
        env: "test".to_owned(),
        build_version: "0.1.0".to_owned(),
        build_commit: "0000000".to_owned(),
        config_hash: "a".repeat(64),
    }
}

fn join(role: PortRole) -> RoleJoin {
    RoleJoin {
        role,
        group: GROUP,
        port: port_of(role),
        interface: Some("gre1".to_owned()),
        source: Some(Ipv4Addr::new(192, 0, 2, 10)),
    }
}

/// Writes a stream into a real archive, publishes it, and replays it back
/// through the deriver.
///
/// The datagrams go in through [`Sink::write`], which is the call a drain thread
/// makes, and come back through [`ArchiveSource`], which is the same `Source`
/// trait a live capture presents. Nothing here is a shortcut past either path.
#[must_use]
pub fn through_an_archive(scope: CaptureDropScope, stream: &Stream) -> LossReport {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let staging = dir.path().join("staging");
    let completed = dir.path().join("completed");
    let cfg = ArchiveWriterConfig {
        staging_dir: staging,
        completed_dir: completed,
        // Nothing here rotates by accident: these tests are about the sequence
        // numbers, and rotation has its own suite elsewhere.
        rotate_bytes: 1 << 30,
        rotate_interval: Duration::from_secs(3600),
        staging_max: 1 << 40,
        compression: Compression::Zstd { level: 1 },
        identity: identity(),
        feed: feed().to_owned(),
        roles_joined: stream.roles().into_iter().map(join).collect(),
        link_headers: LinkHeaders::Synthesised,
        // The writer and the deriver name one scope, so there is nothing here
        // to map between and nothing to get backwards.
        capture_drop_scope: scope,
    };

    let mut writer = ArchiveWriter::new(cfg, 0).expect("the archive opens");
    for dg in &stream.sent {
        Sink::write(&mut writer, &dg.as_recorded()).expect("the write path never fails the caller");
    }
    // The write path counts rather than propagating, so a dropped datagram is
    // silent unless it is asserted — and a datagram the archive dropped is a
    // sequence gap with nothing admitted behind it.
    assert_eq!(
        writer.datagrams_dropped_total(),
        0,
        "the archive dropped a datagram: {:?}",
        writer.last_error()
    );
    writer
        .rotate_at(FIRST_RECV_TS_NS + 60_000_000_000)
        .expect("rotation")
        .expect("a segment that held datagrams produces an object");
    let published = writer
        .wait_completed()
        .expect("the compressor thread publishes exactly one object")
        .expect("publication");

    replay_through_deriver(scope, &published.segment.path)
}

fn replay_through_deriver(scope: CaptureDropScope, object: &Path) -> LossReport {
    let mut source = ArchiveSource::open(object).expect("the archive opens");
    let mut deriver = LossDeriver::new(scope);
    deriver.drive(&mut source).expect("the archive replays");
    // A short replay read as a complete window is loss the publisher gets
    // charged for, so the termination is asserted rather than assumed.
    assert_eq!(
        source.terminated_by(),
        Termination::Eof,
        "the archive did not end cleanly: {:?}",
        source.last_error()
    );
    deriver.finish()
}

/// The live half: the real [`HealthObserver`], fed as the datagrams are
/// captured, and read back through its rendered exposition.
///
/// Scraped rather than inspected. The exposition is what a dashboard reads, so a
/// comparison against the tier's internals would prove the two agree about
/// something no panel can see.
#[must_use]
pub fn observe_live(stream: &Stream) -> Scrape {
    let roles = stream.roles();
    let channels = [CHANNEL];
    let sources = [PUBLISHER_A, PUBLISHER_B];
    let feeds = [FeedSeries {
        feed: feed(),
        port_roles: &roles,
        channel_ids: &channels,
        expected_sources: &sources,
        expected_magic: Some(TopOfBook::MAGIC),
    }];
    let identity = identity();
    let metrics = Arc::new(HealthMetrics::new(&HealthMetricsConfig {
        site: &identity.site,
        recorder: &identity.recorder,
        feeds: &feeds,
    }));
    let mut observer = HealthObserver::new(
        Arc::clone(&metrics),
        feed(),
        InstanceLimits::default(),
        // The scope the offline pass assumes for the same stream, so the two
        // counts this test compares are counts of the same thing.
        CaptureDropScope::PortRole,
    )
    .expect("the feed was declared to the metric set");
    for dg in &stream.sent {
        observer.on_datagram(&dg.as_recorded());
    }
    Scrape {
        text: metrics.render(),
    }
}

/// A Prometheus text exposition, read by name and labels.
pub struct Scrape {
    text: String,
}

impl Scrape {
    /// The one series of `name` carrying every label pair given.
    ///
    /// Exactly one: a helper that summed several matches would let a wrong label
    /// set pass as a right one, and a label set is what these two halves are
    /// being compared on.
    #[must_use]
    pub fn value(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        let mut found: Option<u64> = None;
        for line in self.text.lines() {
            let Some(rest) = line.strip_prefix(name) else {
                continue;
            };
            let Some(rest) = rest.strip_prefix('{') else {
                continue;
            };
            let Some((rendered, value)) = rest.split_once("} ") else {
                continue;
            };
            if !labels
                .iter()
                .all(|(key, want)| rendered.contains(&format!("{key}=\"{want}\"")))
            {
                continue;
            }
            let value: f64 = value.trim().parse().expect("a rendered metric value");
            assert!(
                found.is_none(),
                "{name} has more than one series matching {labels:?}"
            );
            found = Some(value as u64);
        }
        found.unwrap_or_else(|| panic!("no series {name} matches {labels:?}"))
    }

    /// One channel instance's series.
    ///
    /// The tier keys its instance series on `(feed, port_role, channel,
    /// source)`, folding the destination port into the role — exact only while
    /// one role means one port, and it does here.
    #[must_use]
    pub fn instance_value(&self, name: &str, source: Ipv4Addr, role: PortRole) -> u64 {
        let channel = CHANNEL.to_string();
        let source = source.to_string();
        self.value(
            name,
            &[
                ("feed", feed()),
                ("port_role", role.as_str()),
                ("channel", &channel),
                ("source", &source),
            ],
        )
    }

    /// One port role's series.
    #[must_use]
    pub fn role_value(&self, name: &str, role: PortRole) -> u64 {
        self.value(name, &[("feed", feed()), ("port_role", role.as_str())])
    }
}
