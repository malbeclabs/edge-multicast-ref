//! An archive in memory, built by the real encoder.
//!
//! The datagram shape is `dz-recorder-replay`'s, so a fixture here is the same
//! thing a recorder writes. Nothing is hand-encoded: a test that built its own
//! bytes would stop testing the codec the moment the two disagreed.

#![allow(dead_code)]

use std::net::{Ipv4Addr, SocketAddrV4};

use dz_edge_core::{
    ChannelSequence, DatagramBuilder, Feed, Heartbeat, PortRole, ResetCount, MAX_DATAGRAM_SIZE,
};
use dz_edge_mbp::{
    BookClear, InstrumentReset, LevelUpdate, SnapshotBegin, SnapshotEnd, SnapshotLevel,
};
use dz_edge_refdata::{InstrumentDefinition, ManifestSummary, LEG_LEN, SYMBOL_LEN};
use dz_edge_tob::{Quote, Trade};
use dz_recorder_core::{RecordedDatagram, RecorderIdentity, RecvTsKind, Source, SourceError};

pub use dz_recorder_replay::synthetic::{GROUP, PRIMARY_SOURCE};
pub use dz_recorder_replay::OwnedDatagram;

pub const SOURCE_ID: u16 = 1_000;
pub const CHANNEL_ID: u8 = 1;
pub const SOURCE_PORT: u16 = 50_000;
pub const MKTDATA_PORT: u16 = 40_000;
pub const AAA: u32 = 11;
pub const BBB: u32 = 12;

pub const SIDE_BID: u8 = 0;
pub const ACTION_NEW: u8 = 1;
pub const BOTH_UPDATED: u8 = 0x03;
pub const AGGRESSOR_BUY: u8 = 1;
pub const RESET_UPSTREAM_GAP: u8 = 3;
pub const ABSENT_U16: u16 = 0xFFFF;

/// The port a role's datagrams arrive on, one per role, as a recorder joins them.
#[must_use]
pub const fn port_for(role: PortRole) -> u16 {
    match role {
        PortRole::Mktdata => MKTDATA_PORT,
        PortRole::Refdata => MKTDATA_PORT + 1,
        PortRole::Snapshot => MKTDATA_PORT + 2,
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Msg {
    Quote(Quote),
    Trade(Trade),
    Level(LevelUpdate),
    Clear(BookClear),
    Definition(InstrumentDefinition),
    Manifest(ManifestSummary),
    Reset(InstrumentReset),
    SnapshotBegin(SnapshotBegin),
    SnapshotLevel(SnapshotLevel),
    SnapshotEnd(SnapshotEnd),
    /// Carried for what it is *not*: a heartbeat is a datagram the transport
    /// tier writes a row for and this tier writes nothing for, so a fixture
    /// that leaves it out states a ratio no real feed has.
    Heartbeat(Heartbeat),
}

impl Msg {
    fn push<F: Feed>(self, builder: &mut DatagramBuilder<F>) {
        let pushed = match self {
            Self::Quote(m) => builder.push(&m),
            Self::Trade(m) => builder.push(&m),
            Self::Level(m) => builder.push(&m),
            Self::Clear(m) => builder.push(&m),
            Self::Definition(m) => builder.push(&m),
            Self::Manifest(m) => builder.push(&m),
            Self::Reset(m) => builder.push(&m),
            Self::SnapshotBegin(m) => builder.push(&m),
            Self::SnapshotLevel(m) => builder.push(&m),
            Self::SnapshotEnd(m) => builder.push(&m),
            Self::Heartbeat(m) => builder.push(&m),
        };
        pushed.expect("the fixture builds datagrams the codec accepts");
    }
}

/// Pack messages one per datagram, from a stated sequence number.
///
/// One per datagram by default because these tests are about *what a message
/// became*, and a batching decision moving `message_index` around would make
/// every assertion about a row also an assertion about the packing.
#[must_use]
pub fn pack<F: Feed>(messages: &[Msg], role: PortRole, first_sequence: u64) -> Vec<OwnedDatagram> {
    pack_from::<F>(messages, role, first_sequence, 0, PRIMARY_SOURCE)
}

#[must_use]
pub fn pack_from<F: Feed>(
    messages: &[Msg],
    role: PortRole,
    first_sequence: u64,
    reset_count: u8,
    source: Ipv4Addr,
) -> Vec<OwnedDatagram> {
    let mut sequence = ChannelSequence::resume(CHANNEL_ID, ResetCount(reset_count), first_sequence);
    let mut out = Vec::new();
    for (index, message) in messages.iter().enumerate() {
        let mut builder = DatagramBuilder::<F>::new(
            sequence,
            role,
            u16::try_from(MAX_DATAGRAM_SIZE).expect("the mandated cap fits a u16"),
        );
        message.push(&mut builder);
        // Time advances with the sequence number so that groups packed by
        // separate calls land in the archive in the order they were appended.
        // A flat clock would make every group simultaneous, and resolution is
        // by arrival time.
        let send_ts = 1_700_000_000_000_000_000 + (first_sequence + index as u64) * 1_000_037;
        let payload = builder.finish(send_ts).expect("one message is a datagram");
        out.push(recorded(payload, role, source, send_ts));
        sequence.advance();
    }
    out
}

/// Pack messages `per_datagram` at a time, from a stated sequence number.
///
/// Batching is the publisher's decision and nobody else's: it moves the
/// sequence number a message arrives under, its `message_index`, and the
/// arrival stamp it shares with everything packed beside it. None of that is a
/// statement about the market, so a fixture that says the same things two ways
/// is what holds anything derived from the state to being a function of the
/// state.
///
/// It is also the burst, which [`pack`] cannot express: a publisher under load
/// puts an update run into one datagram, and that is the case a
/// messages-per-datagram ratio is taken to find out about. A fixture that only
/// ever packed one message per datagram would measure a ratio of one and prove
/// nothing about the packing.
#[must_use]
pub fn pack_batched<F: Feed>(
    messages: &[Msg],
    role: PortRole,
    first_sequence: u64,
    per_datagram: usize,
) -> Vec<OwnedDatagram> {
    assert!(per_datagram > 0, "a datagram carries at least one message");
    let mut sequence = ChannelSequence::resume(CHANNEL_ID, ResetCount(0), first_sequence);
    let mut out = Vec::new();
    for (batch_index, batch) in messages.chunks(per_datagram).enumerate() {
        let mut builder = DatagramBuilder::<F>::new(
            sequence,
            role,
            u16::try_from(MAX_DATAGRAM_SIZE).expect("the mandated cap fits a u16"),
        );
        for message in batch {
            message.push(&mut builder);
        }
        let send_ts = 1_700_000_000_000_000_000 + (first_sequence + batch_index as u64) * 1_000_037;
        let payload = builder.finish(send_ts).expect("the batch fits a datagram");
        out.push(recorded(payload, role, PRIMARY_SOURCE, send_ts));
        sequence.advance();
    }
    out
}

/// One datagram as a recorder wrote it down.
fn recorded(payload: Vec<u8>, role: PortRole, source: Ipv4Addr, send_ts: u64) -> OwnedDatagram {
    let payload_len = payload.len();
    OwnedDatagram {
        payload,
        src: SocketAddrV4::new(source, SOURCE_PORT),
        dst: SocketAddrV4::new(GROUP, port_for(role)),
        role,
        recv_ts_ns: send_ts + 42_000,
        recv_ts_kind: RecvTsKind::KernelSoftware,
        drop_delta: 0,
        ttl: Some(8),
        link_headers: None,
        wire_payload_len: u32::try_from(payload_len).expect("under the cap"),
    }
}

/// An archive of datagrams, read back as a [`Source`].
#[derive(Debug, Clone, Default)]
pub struct DatagramLog {
    datagrams: Vec<OwnedDatagram>,
    at: usize,
}

impl DatagramLog {
    #[must_use]
    pub fn new(datagrams: Vec<OwnedDatagram>) -> Self {
        Self { datagrams, at: 0 }
    }

    pub fn extend(&mut self, datagrams: Vec<OwnedDatagram>) {
        self.datagrams.extend(datagrams);
    }
}

impl Source for DatagramLog {
    fn next(&mut self) -> Result<Option<RecordedDatagram<'_>>, SourceError> {
        let Some(datagram) = self.datagrams.get(self.at) else {
            return Ok(None);
        };
        self.at += 1;
        Ok(Some(datagram.as_recorded()))
    }
}

#[must_use]
pub fn identity() -> RecorderIdentity {
    RecorderIdentity {
        site: "site".to_owned(),
        recorder: "recorder".to_owned(),
        env: "env".to_owned(),
        build_version: "0.1.0".to_owned(),
        build_commit: "commit".to_owned(),
        config_hash: "hash".to_owned(),
    }
}

#[must_use]
pub fn symbol(text: &str) -> [u8; SYMBOL_LEN] {
    let mut out = [0_u8; SYMBOL_LEN];
    out[..text.len()].copy_from_slice(text.as_bytes());
    out
}

#[must_use]
pub fn definition(instrument_id: u32, name: &str, price_exponent: i8) -> InstrumentDefinition {
    InstrumentDefinition {
        instrument_id,
        source_id: SOURCE_ID,
        symbol: symbol(name),
        leg1: [0_u8; LEG_LEN],
        leg2: [0_u8; LEG_LEN],
        asset_class: 0,
        price_exponent,
        qty_exponent: 0,
        market_model: 0,
        tick_size: 1,
        lot_size: 1,
        contract_value: 100,
        expiry_ns: 0,
        settle_type: 0,
        price_bound: 0,
        manifest_seq: 3,
    }
}
