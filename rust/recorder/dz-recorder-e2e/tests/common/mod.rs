//! What the two sides need in order to meet, and nothing either side already
//! has.
//!
//! The publisher half here is the real [`DatagramBuilder`] and the real message
//! types; the recorder half is the real [`ArchiveWriter`] and the real
//! [`ArchiveSource`]. Nothing in this file re-implements either — a round trip
//! against a writer written for the test would prove only that the test agrees
//! with itself.
//!
//! The one thing written by hand is a datagram header with every field a
//! caller's to state wrongly. [`DatagramBuilder`] refuses most publisher
//! violations by construction, which is what a builder is for, so the bytes for
//! those have to come from somewhere that has no opinion.
#![allow(dead_code)]
#![forbid(unsafe_code)]

/// Validation against edge-feed-spec's own rule set. Behind the feature,
/// because it needs a tool built from that repository.
#[cfg(feature = "conformance")]
pub mod conformance;

use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::time::Duration;

use dz_edge_core::{
    ChannelSequence, DatagramBuilder, Feed, PortRole, ResetCount, DATAGRAM_HEADER_SIZE,
    SCHEMA_VERSION,
};
use dz_edge_refdata::instrument_definition::InstrumentDefinition;
use dz_edge_refdata::{
    ManifestSummary, ASSET_CLASS_CRYPTO_SPOT, MARKET_MODEL_CLOB, PRICE_BOUND_NON_NEGATIVE,
    SETTLE_TYPE_CASH, SYMBOL_LEN,
};
use dz_edge_tob::{Quote, TopOfBook, Trade, AGGRESSOR_BUY, QUOTE_ASK_UPDATED, QUOTE_BID_UPDATED};
use dz_recorder_archive::manifest::{InstanceCoverage, SegmentManifest};
use dz_recorder_archive::rotate::{ArchiveWriter, ArchiveWriterConfig};
use dz_recorder_archive::writer::{CaptureDropScope, LinkHeaders, RoleJoin};
use dz_recorder_archive::Compression;
use dz_recorder_core::{ChannelInstance, CompletedSegment, RecorderIdentity, RecvTsKind, Sink};
use dz_recorder_replay::{ArchiveSource, OwnedDatagram, Termination};
use tempfile::TempDir;

/// MCAST-TEST-NET (RFC 2365) and the RFC 5737 documentation ranges, throughout.
/// An address in a test is copied into a configuration sooner or later, and an
/// address on `239.0.0.0/8` or `10.0.0.0/8` names a network somebody really
/// runs.
pub const GROUP: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 10);
pub const PUBLISHER_A: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
/// A second publisher serving the same `Channel ID` to the same group and port,
/// which is a distinct channel instance with its own sequence space.
pub const PUBLISHER_B: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 2);
/// The source port a publisher sends from. It is not part of the channel
/// instance, so it is one value for everything here.
pub const EGRESS_PORT: u16 = 41000;
pub const JOIN_INTERFACE: &str = "gre1";
pub const JOIN_SOURCE: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 10);

/// A receive stamp with all nine digits populated, so an archive that rounds to
/// microseconds cannot pass a comparison against it.
pub const FIRST_RECV_TS_NS: u64 = 1_772_000_000_123_456_789;
/// Coprime with every power of ten, for the same reason.
pub const RECV_TS_STEP_NS: u64 = 7_654_321;

/// The MTU a publisher packs to. Below the mandated cap, so `remaining` is what
/// ends a datagram here rather than the clamp.
pub const PUBLISHER_MTU: u16 = 1200;

/// Every port role, as a recorder host would be configured for a depth feed.
pub const ALL_ROLES: &[PortRole] = &[PortRole::Mktdata, PortRole::Refdata, PortRole::Snapshot];

pub fn port_of(role: PortRole) -> u16 {
    match role {
        PortRole::Mktdata => 40000,
        PortRole::Refdata => 40001,
        PortRole::Snapshot => 40002,
    }
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

pub fn join(role: PortRole) -> RoleJoin {
    RoleJoin {
        role,
        group: GROUP,
        port: port_of(role),
        interface: Some(JOIN_INTERFACE.to_owned()),
        source: Some(JOIN_SOURCE),
    }
}

pub fn archive_config(
    staging: &Path,
    completed: &Path,
    roles_joined: Vec<RoleJoin>,
) -> ArchiveWriterConfig {
    ArchiveWriterConfig {
        staging_dir: staging.to_path_buf(),
        completed_dir: completed.to_path_buf(),
        // Large enough that nothing here rotates by accident: these tests are
        // about the datagrams, and rotation has its own.
        rotate_bytes: 1 << 30,
        rotate_interval: Duration::from_secs(3600),
        staging_max: 1 << 40,
        compression: Compression::Zstd { level: 1 },
        identity: identity(),
        feed: TopOfBook::NAME.to_owned(),
        roles_joined,
        // Socket mode's provenance and its drop scope, because socket mode is
        // the mode a publisher's own egress reaches through a socket: it really
        // does hold one loss accumulator per port role, and its link headers
        // are assembled rather than observed.
        link_headers: LinkHeaders::Synthesised,
        capture_drop_scope: CaptureDropScope::PortRole,
    }
}

/// The object, its manifest, and the directory both live in.
pub struct Recorded {
    /// Held so the archive outlives the test that reads it.
    _dir: TempDir,
    pub object: PathBuf,
    pub segment: CompletedSegment,
    pub manifest: SegmentManifest,
}

impl Recorded {
    /// The coverage row for one channel instance, named by what a test knows:
    /// the publisher's address, the `Channel ID` it stamped and the port role it
    /// sent on.
    pub fn coverage(
        &self,
        source: Ipv4Addr,
        channel_id: u8,
        role: PortRole,
    ) -> Option<&InstanceCoverage> {
        self.manifest
            .instances
            .get(&ChannelInstance::new(source, channel_id, port_of(role)))
    }

    /// The same, and a failure that names the instance rather than unwrapping a
    /// `None`.
    pub fn expect_coverage(
        &self,
        source: Ipv4Addr,
        channel_id: u8,
        role: PortRole,
    ) -> &InstanceCoverage {
        self.coverage(source, channel_id, role).unwrap_or_else(|| {
            panic!(
                "the manifest describes no instance ({source}, channel {channel_id}, {}); it \
                 describes {:?}",
                role.as_str(),
                self.manifest.instances.keys().collect::<Vec<_>>()
            )
        })
    }
}

/// Writes a stream into a real archive and publishes it.
///
/// The datagrams go in through [`Sink::write`], which is the same call a drain
/// thread makes, so nothing here is a shortcut past the write path.
pub fn record(sent: &[OwnedDatagram], roles_joined: &[PortRole]) -> Recorded {
    record_with(roles_joined, |writer| {
        for dg in sent {
            Sink::write(writer, &dg.as_recorded()).expect("the write path never fails the caller");
        }
        sent.len() as u64
    })
}

/// The same, for a caller whose datagrams arrive rather than exist: the socket
/// suite writes each one as it comes off the capture, which is the loop a
/// recorder host runs.
///
/// `fill` returns how many datagrams it offered, so the drop assertion below
/// covers a caller that offered more than the archive accepted.
pub fn record_with<F>(roles_joined: &[PortRole], fill: F) -> Recorded
where
    F: FnOnce(&mut ArchiveWriter) -> u64,
{
    record_joined(roles_joined.iter().copied().map(join).collect(), fill)
}

/// The same again, for a caller that states the joins itself.
///
/// The socket suite binds real ports rather than the fixed ones `port_of`
/// states, and a manifest whose stated intent disagreed with the ports the
/// datagrams actually arrived on would be describing a recorder nobody ran.
pub fn record_joined<F>(roles_joined: Vec<RoleJoin>, fill: F) -> Recorded
where
    F: FnOnce(&mut ArchiveWriter) -> u64,
{
    let dir = tempfile::tempdir().expect("a temporary directory");
    let staging = dir.path().join("staging");
    let completed = dir.path().join("completed");
    let cfg = archive_config(&staging, &completed, roles_joined);

    let mut writer = ArchiveWriter::new(cfg, 0).expect("the archive opens");
    let offered = fill(&mut writer);
    // The write path counts rather than propagating, so a dropped datagram is
    // silent here unless it is asserted. A recorder that quietly drops what it
    // cannot parse is the failure this whole crate exists to rule out, so the
    // assertion belongs where every test passes through.
    assert_eq!(
        writer.datagrams_dropped_total(),
        0,
        "the archive dropped a datagram: {:?}",
        writer.last_error()
    );
    assert_eq!(writer.datagrams_written_total(), offered);

    writer
        .rotate_at(FIRST_RECV_TS_NS + 1_000_000_000)
        .expect("rotation")
        .expect("a segment that held datagrams produces an object");
    let published = writer
        .wait_completed()
        .expect("the compressor thread publishes exactly one object")
        .expect("publication");
    let manifest = read_manifest(&completed);

    Recorded {
        _dir: dir,
        object: published.segment.path.clone(),
        segment: published.segment,
        manifest,
    }
}

/// Replays a whole archive, asserting that it was whole.
///
/// A helper that accepted a tear in silence would let a short replay pass as a
/// complete one, which is a sequence gap with nothing admitted behind it — and
/// so a publisher finding drawn from our own truncation.
pub fn replay(path: &Path) -> Vec<OwnedDatagram> {
    let mut source = ArchiveSource::open(path).expect("the archive opens");
    let datagrams: Vec<OwnedDatagram> = (&mut source).collect();
    assert_eq!(
        source.terminated_by(),
        Termination::Eof,
        "the archive did not end cleanly: {:?}",
        source.last_error()
    );
    datagrams
}

fn read_manifest(completed: &Path) -> SegmentManifest {
    let entry = std::fs::read_dir(completed)
        .expect("the completed directory exists")
        .filter_map(Result::ok)
        .find(|e| e.file_name().to_string_lossy().ends_with(".manifest.json"))
        .expect("every object lands with a manifest beside it");
    let json = std::fs::read_to_string(entry.path()).expect("the manifest is readable");
    serde_json::from_str(&json).unwrap_or_else(|e| panic!("the manifest deserialises: {e}: {json}"))
}

/// The receive side: what a capture would have said about each datagram's
/// arrival.
///
/// Stamps advance by a fixed step rather than by a clock read, because the round
/// trip is asserted to the nanosecond and a test cannot assert against a value
/// it did not choose.
pub struct Wire {
    next_ts_ns: u64,
    pub sent: Vec<OwnedDatagram>,
}

impl Default for Wire {
    fn default() -> Self {
        Self::new()
    }
}

impl Wire {
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_ts_ns: FIRST_RECV_TS_NS,
            sent: Vec::new(),
        }
    }

    /// One datagram arriving from `source` on `role`'s port.
    pub fn arrive(&mut self, payload: Vec<u8>, source: Ipv4Addr, role: PortRole) {
        self.arrive_after_loss(payload, source, role, 0);
    }

    /// The same, behind `drop_delta` datagrams the capture handle lost between
    /// the previous one and this one. That is what pcapng's `epb_dropcount` is
    /// defined as, so it has to survive the round trip or a gap in the archive
    /// is charged to the publisher by default.
    pub fn arrive_after_loss(
        &mut self,
        payload: Vec<u8>,
        source: Ipv4Addr,
        role: PortRole,
        drop_delta: u32,
    ) {
        let wire_payload_len = u32::try_from(payload.len()).expect("a datagram is small");
        self.sent.push(OwnedDatagram {
            payload,
            src: SocketAddrV4::new(source, EGRESS_PORT),
            dst: SocketAddrV4::new(GROUP, port_of(role)),
            role,
            recv_ts_ns: self.next_ts_ns,
            // The archive's section claims a kernel stamp and marks the
            // exception per datagram, so a stream of these exercises the claim
            // rather than the exception. The socket suite is where the kind is
            // observed rather than stated.
            recv_ts_kind: RecvTsKind::KernelSoftware,
            drop_delta,
            // Socket mode reports it through `IP_RECVTTL`, and a non-zero value
            // is what distinguishes an observation from the zero a synthesised
            // header writes for *absent*.
            ttl: Some(4),
            // Socket mode sees only a payload, so the archive synthesises the
            // 42 bytes in front of it.
            link_headers: None,
            wire_payload_len,
        });
        self.next_ts_ns += RECV_TS_STEP_NS;
    }
}

/// A datagram header with every field a caller's to state, correctly or not.
///
/// [`DatagramBuilder::finish`] stamps `Magic`, the schema version, the message
/// count and the declared length itself, and no argument can ask it for another
/// value — which is the point of a builder and the reason a publisher violation
/// has to be assembled somewhere else. The offsets are the spec's field table:
/// `Magic` at 0, `Schema Version` at 2, `Channel ID` at 3, `Sequence Number` at
/// 4, `Send Timestamp` at 12, `Message Count` at 20, `Reset Count` at 21,
/// `Frame Length` at 22.
#[derive(Debug, Clone, Copy)]
pub struct RawHeader {
    pub magic: u16,
    pub schema_version: u8,
    pub channel_id: u8,
    pub sequence_number: u64,
    pub send_timestamp_ns: u64,
    pub msg_count: u8,
    pub reset_count: u8,
    pub datagram_len: u16,
}

impl RawHeader {
    /// A header a conformant publisher would emit, for a caller about to spoil
    /// exactly one field of it.
    #[must_use]
    pub fn conformant(channel_id: u8, sequence_number: u64, body_len: usize) -> Self {
        Self {
            magic: TopOfBook::MAGIC,
            schema_version: SCHEMA_VERSION,
            channel_id,
            sequence_number,
            send_timestamp_ns: FIRST_RECV_TS_NS - 1_000,
            msg_count: 1,
            reset_count: 0,
            datagram_len: u16::try_from(DATAGRAM_HEADER_SIZE + body_len)
                .expect("a datagram is small"),
        }
    }

    #[must_use]
    pub fn bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; DATAGRAM_HEADER_SIZE];
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

    /// The header in front of a body.
    #[must_use]
    pub fn followed_by(&self, body: &[u8]) -> Vec<u8> {
        let mut out = self.bytes();
        out.extend_from_slice(body);
        out
    }
}

/// The messages one datagram is to carry, so a test states the shape of a
/// datagram and the real builder produces the bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Msg {
    Quote(u32),
    Trade(u32),
    Heartbeat,
    ManifestSummary(u16),
    InstrumentDefinition(u32),
}

/// Encodes one datagram through the real builder.
///
/// The sequence, the `Channel ID` and the era all come from the
/// `ChannelSequence` the caller advances, exactly as a publisher's egress layer
/// supplies them.
#[must_use]
pub fn encode(sequence: ChannelSequence, role: PortRole, msgs: &[Msg]) -> Vec<u8> {
    let mut builder = DatagramBuilder::<TopOfBook>::new(sequence, role, PUBLISHER_MTU);
    for msg in msgs {
        push(&mut builder, *msg);
    }
    builder
        .finish(sequence.sequence_number() * 1_000 + 1_772_000_000_000_000_000)
        .expect("a datagram with at least one message is emittable")
}

fn push(builder: &mut DatagramBuilder<TopOfBook>, msg: Msg) {
    let pushed = match msg {
        Msg::Quote(instrument_id) => builder.push(&quote(instrument_id)),
        Msg::Trade(instrument_id) => builder.push(&trade(instrument_id)),
        Msg::Heartbeat => builder.push(&dz_edge_core::Heartbeat {
            // Overwritten by the builder with the datagram's own `Channel ID`,
            // so a builder-framed message cannot disagree with its header.
            channel_id: 0,
            timestamp_ns: 1_772_000_000_000_000_001,
        }),
        Msg::ManifestSummary(manifest_seq) => builder.push(&ManifestSummary {
            channel_id: 0,
            valid: 1,
            manifest_seq,
            instrument_count: 3,
            timestamp_ns: 1_772_000_000_000_000_002,
        }),
        Msg::InstrumentDefinition(instrument_id) => {
            builder.push(&instrument_definition(instrument_id))
        }
    };
    pushed.unwrap_or_else(|e| panic!("the builder refused {msg:?}: {e}"));
}

fn quote(instrument_id: u32) -> Quote {
    Quote {
        instrument_id,
        source_id: 7,
        update_flags: QUOTE_BID_UPDATED | QUOTE_ASK_UPDATED,
        source_timestamp_ns: 1_772_000_000_000_000_003,
        bid_price: 1_234_500,
        bid_qty: 10,
        ask_price: 1_234_600,
        ask_qty: 12,
        bid_source_count: 2,
        ask_source_count: 3,
    }
}

fn trade(instrument_id: u32) -> Trade {
    Trade {
        instrument_id,
        source_id: 7,
        aggressor_side: AGGRESSOR_BUY,
        trade_flags: 0,
        source_timestamp_ns: 1_772_000_000_000_000_004,
        trade_price: 1_234_550,
        trade_qty: 4,
        trade_id: 99,
        cumulative_volume: 4_000,
    }
}

fn instrument_definition(instrument_id: u32) -> InstrumentDefinition {
    let mut symbol = [b' '; SYMBOL_LEN];
    symbol[..5].copy_from_slice(b"AAA-B");
    InstrumentDefinition {
        instrument_id,
        source_id: 7,
        symbol,
        leg1: *b"AAA     ",
        leg2: *b"B       ",
        asset_class: ASSET_CLASS_CRYPTO_SPOT,
        price_exponent: -2,
        qty_exponent: -4,
        market_model: MARKET_MODEL_CLOB,
        tick_size: 1,
        lot_size: 1,
        contract_value: 1,
        expiry_ns: 0,
        settle_type: SETTLE_TYPE_CASH,
        price_bound: PRICE_BOUND_NON_NEGATIVE,
        manifest_seq: 1,
    }
}

/// A sequence starting at 0 in the era a channel that has never reset
/// advertises.
#[must_use]
pub fn fresh(channel_id: u8) -> ChannelSequence {
    ChannelSequence::new(channel_id, ResetCount::NEVER_RESET)
}
