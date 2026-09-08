//! A synthetic upstream and a synthetic archive, both in memory.
//!
//! Nothing here touches a filesystem, a socket, a device or a clock. Both sides
//! of every comparison in these tests are values written out by hand, which is
//! the only arrangement under which a test of *did the publisher publish what
//! the venue said?* can itself be trusted: the expected wire messages are
//! transcribed from the specification's own tables and from the codec's
//! constants, never produced by the lowering the comparison re-runs.
//!
//! The datagram shape is `dz-recorder-replay`'s [`OwnedDatagram`], so these
//! archives are the same thing the record path writes and the replay path yields
//! — and the documentation-range addresses are that crate's own, because a
//! placeholder that looks like a real host is a leak waiting to be copied into a
//! config.
//!
//! One `mod common;` per test binary, and each binary uses the part of it that
//! its own subject needs — hence the allowance below, which is the ordinary
//! cost of sharing a fixture between integration tests rather than a sign that
//! something here is unused.
#![allow(dead_code)]

use std::collections::BTreeMap;

use dz_adapter_core::{
    Adapter, Aggressor, AssetClass, ClearScope, ConnectionId, Event, EventSink, InstrumentRef,
    InstrumentSpec, ListingSink, MarketModel, ParseError, Payload, Presence, PriceBound, Scalar,
    SettleType, Side, SideUpdate, TradeFlags,
};
use dz_edge_core::{
    ChannelSequence, DatagramBuilder, Feed, Heartbeat, PortRole, ResetCount, MAX_DATAGRAM_SIZE,
};
use dz_edge_mbp::{
    BookClear, InstrumentReset, LevelUpdate, SnapshotBegin, SnapshotEnd, SnapshotLevel,
};
use dz_edge_refdata::{InstrumentDefinition, ManifestSummary, LEG_LEN, SYMBOL_LEN};
use dz_edge_tob::{Quote, Trade};
use dz_recorder_core::{RecordedDatagram, RecvTsKind, Source, SourceError};
use dz_recorder_relower::{ArchivedPayload, PayloadArchive, PayloadLog};

pub use dz_recorder_replay::synthetic::{GROUP, PRIMARY_SOURCE};
pub use dz_recorder_replay::OwnedDatagram;

/// The publisher identity every fixture uses.
///
/// In the registry's assigned production range, which is what
/// `SourceId::new` admits: `0` is reserved, `1`–`1023` are assigned.
pub const SOURCE_ID: u16 = 1000;

/// The channel every fixture publishes on.
pub const CHANNEL_ID: u8 = 1;

const SOURCE_PORT: u16 = 50_000;
const MKTDATA_PORT: u16 = 40_000;
const TTL: u8 = 8;

/// One message, on its way into a datagram.
///
/// An enumeration rather than a generic parameter because a datagram carries
/// several message types at once, which is the whole reason the framing has to
/// be stripped before anything can be compared.
#[derive(Debug, Clone, Copy)]
pub enum Msg {
    Quote(Quote),
    Trade(Trade),
    Level(LevelUpdate),
    Clear(BookClear),
    Definition(InstrumentDefinition),
    Manifest(ManifestSummary),
    Heartbeat(Heartbeat),
    /// The snapshot port role's own, for the case where a snapshot must be
    /// skipped rather than joined.
    SnapshotEnd(SnapshotEnd),
    SnapshotBegin(SnapshotBegin),
    SnapshotLevel(SnapshotLevel),
    /// The publisher disowning its own book for one instrument.
    Reset(InstrumentReset),
}

impl Msg {
    fn push<F: Feed>(self, builder: &mut DatagramBuilder<F>) {
        let pushed = match self {
            Self::Quote(message) => builder.push(&message),
            Self::Trade(message) => builder.push(&message),
            Self::Level(message) => builder.push(&message),
            Self::Clear(message) => builder.push(&message),
            Self::Definition(message) => builder.push(&message),
            Self::Manifest(message) => builder.push(&message),
            Self::Heartbeat(message) => builder.push(&message),
            Self::SnapshotEnd(message) => builder.push(&message),
            Self::SnapshotBegin(message) => builder.push(&message),
            Self::SnapshotLevel(message) => builder.push(&message),
            Self::Reset(message) => builder.push(&message),
        };
        pushed.expect("the fixture builds datagrams the codec accepts");
    }
}

/// The port a role's datagrams arrive on. One port per role, as a recorder
/// joins them.
#[must_use]
pub const fn port_for(role: PortRole) -> u16 {
    match role {
        PortRole::Mktdata => MKTDATA_PORT,
        PortRole::Refdata => MKTDATA_PORT + 1,
        PortRole::Snapshot => MKTDATA_PORT + 2,
    }
}

/// How a fixture frames and paces its datagrams.
///
/// Every field here is one the comparison must be blind to. A test that changes
/// them and expects the same verdict is the test that keeps the tool usable.
#[derive(Debug, Clone, Copy)]
pub struct Framing {
    /// Messages per datagram.
    pub per_datagram: usize,
    /// The first `Sequence Number`.
    pub first_sequence: u64,
    pub reset_count: u8,
    /// The first send timestamp, and the step between datagrams.
    pub first_send_ts_ns: u64,
    pub send_step_ns: u64,
}

impl Framing {
    /// One message per datagram, from sequence 0.
    #[must_use]
    pub const fn tight() -> Self {
        Self {
            per_datagram: 1,
            first_sequence: 0,
            reset_count: 0,
            first_send_ts_ns: 1_700_000_000_000_000_000,
            send_step_ns: 1_000_037,
        }
    }

    /// Batched `per_datagram` at a time, from a different sequence, in a
    /// different era, on a different clock.
    #[must_use]
    pub const fn batched(per_datagram: usize) -> Self {
        Self {
            per_datagram,
            first_sequence: 9_000,
            reset_count: 4,
            first_send_ts_ns: 1_800_000_000_000_000_000,
            send_step_ns: 250_000_000,
        }
    }
}

/// Pack messages into datagrams of one feed, on one port role.
#[must_use]
pub fn pack<F: Feed>(messages: &[Msg], role: PortRole, framing: Framing) -> Vec<OwnedDatagram> {
    let mut sequence = ChannelSequence::resume(
        CHANNEL_ID,
        ResetCount(framing.reset_count),
        framing.first_sequence,
    );
    let mut out = Vec::new();
    for (chunk_index, chunk) in messages.chunks(framing.per_datagram.max(1)).enumerate() {
        let mut builder = DatagramBuilder::<F>::new(
            sequence,
            role,
            u16::try_from(MAX_DATAGRAM_SIZE).expect("the mandated cap fits a u16"),
        );
        for message in chunk {
            message.push(&mut builder);
        }
        let send_ts = framing.first_send_ts_ns + chunk_index as u64 * framing.send_step_ns;
        let payload = builder
            .finish(send_ts)
            .expect("a chunk holds at least one message");
        let payload_len = payload.len();
        out.push(OwnedDatagram {
            payload,
            src: std::net::SocketAddrV4::new(PRIMARY_SOURCE, SOURCE_PORT),
            dst: std::net::SocketAddrV4::new(GROUP, port_for(role)),
            role,
            // A subscriber's own receive clock, which the comparison never
            // reads. Deliberately unrelated to the send stamp above.
            recv_ts_ns: send_ts + 42_000,
            recv_ts_kind: RecvTsKind::KernelSoftware,
            drop_delta: 0,
            ttl: Some(TTL),
            link_headers: None,
            wire_payload_len: u32::try_from(payload_len).expect("a datagram is under the cap"),
        });
        sequence.advance();
    }
    out
}

/// An archive of datagrams, read back as a [`Source`].
///
/// In memory, because a comparison is a pure function of two byte streams and a
/// test of one must not need a filesystem to run.
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

    /// Append another stream, so one archive can hold several port roles.
    pub fn extend(&mut self, datagrams: Vec<OwnedDatagram>) {
        self.datagrams.extend(datagrams);
    }

    /// Deliver the datagrams in a different order.
    ///
    /// Network reordering, which a recorder archives as it arrives. The
    /// comparison must be blind to it.
    pub fn rotate(&mut self, by: usize) {
        if !self.datagrams.is_empty() {
            let by = by % self.datagrams.len();
            self.datagrams.rotate_left(by);
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.datagrams.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.datagrams.is_empty()
    }

    /// How many datagrams have been handed out.
    #[must_use]
    pub const fn served(&self) -> usize {
        self.at
    }
}

impl Source for DatagramLog {
    fn next(&mut self) -> Result<Option<RecordedDatagram<'_>>, SourceError> {
        let datagram = match self.datagrams.get(self.at) {
            Some(datagram) => datagram,
            None => return Ok(None),
        };
        self.at += 1;
        Ok(Some(datagram.as_recorded()))
    }
}

/// A source that fails part-way through, for the truncation case.
#[derive(Debug)]
pub struct FailingSource {
    pub after: usize,
    pub inner: DatagramLog,
}

impl Source for FailingSource {
    fn next(&mut self) -> Result<Option<RecordedDatagram<'_>>, SourceError> {
        if self.inner.served() >= self.after {
            return Err(SourceError::MalformedArchive("torn segment".to_owned()));
        }
        self.inner.next()
    }
}

/// A payload archive that fails part-way through, for the truncation case.
#[derive(Debug)]
pub struct FailingPayloads {
    pub after: u64,
    pub served: u64,
    pub inner: PayloadLog,
}

impl FailingPayloads {
    #[must_use]
    pub fn new(after: u64, inner: PayloadLog) -> Self {
        Self {
            after,
            served: 0,
            inner,
        }
    }
}

impl PayloadArchive for FailingPayloads {
    fn next(&mut self) -> Result<Option<ArchivedPayload<'_>>, SourceError> {
        if self.served >= self.after {
            return Err(SourceError::MalformedArchive("torn payload log".to_owned()));
        }
        self.served += 1;
        self.inner.next()
    }
}

/// What the archive says about one instrument.
///
/// The fixture states the wire values directly, because the archive is the
/// control: these are the numbers the publisher published, and the re-lowering
/// has to arrive at them from the venue's own text.
#[derive(Debug, Clone, Copy)]
pub struct Listed {
    pub symbol: &'static str,
    pub instrument_id: u32,
    pub price_exponent: i8,
    pub qty_exponent: i8,
    pub contract_value: u64,
}

impl Listed {
    #[must_use]
    pub const fn new(symbol: &'static str, instrument_id: u32, price: i8, qty: i8) -> Self {
        Self {
            symbol,
            instrument_id,
            price_exponent: price,
            qty_exponent: qty,
            contract_value: 0,
        }
    }

    /// The `InstrumentDefinition` the archive carries for it.
    #[must_use]
    pub fn definition(&self, manifest_seq: u16) -> InstrumentDefinition {
        let mut definition = InstrumentDefinition {
            instrument_id: self.instrument_id,
            source_id: SOURCE_ID,
            symbol: [0u8; SYMBOL_LEN],
            leg1: [0u8; LEG_LEN],
            leg2: [0u8; LEG_LEN],
            asset_class: 1,
            price_exponent: self.price_exponent,
            qty_exponent: self.qty_exponent,
            market_model: 1,
            tick_size: 1,
            lot_size: 1,
            contract_value: self.contract_value,
            expiry_ns: 0,
            settle_type: 0,
            price_bound: 0,
            manifest_seq,
        };
        definition.set_symbol(self.symbol);
        definition
    }
}

/// The reference-data datagrams for a published set: one definition each, then a
/// valid manifest declaring the count.
#[must_use]
pub fn refdata_datagrams<F: Feed>(listed: &[Listed], manifest_seq: u16) -> Vec<OwnedDatagram> {
    let mut messages: Vec<Msg> = listed
        .iter()
        .map(|instrument| Msg::Definition(instrument.definition(manifest_seq)))
        .collect();
    messages.push(Msg::Manifest(ManifestSummary {
        channel_id: CHANNEL_ID,
        valid: 1,
        manifest_seq,
        instrument_count: u32::try_from(listed.len()).expect("a small fixture"),
        timestamp_ns: 1_700_000_000_000_000_000,
    }));
    pack::<F>(&messages, PortRole::Refdata, Framing::tight())
}

/// An adapter over a one-line-per-payload upstream.
///
/// The framing is deliberately trivial and deliberately textual: what it
/// exercises is the boundary and the lowering, not a parser. Every price and
/// quantity crosses as [`Scalar::Text`] — the venue's own decimal, converted
/// exactly or refused above the boundary — which is the shape both existing
/// upstreams have.
///
/// ```text
/// q SYMBOL ts bid_px bid_qty ask_px ask_qty   a two-sided quote; "-" is a gone side
/// t SYMBOL ts px qty aggressor trade_id       one execution
/// l SYMBOL ts side px qty presence            one price level, absolute quantity
/// c SYMBOL ts side                            clear one whole side
/// h                                           a heartbeat: no event
/// ```
#[derive(Debug, Clone)]
pub struct LineAdapter {
    /// The venue's universe: symbol, and the exponents the venue states.
    universe: Vec<(&'static str, i8, i8)>,
    handles: BTreeMap<String, InstrumentRef>,
}

impl LineAdapter {
    /// A venue whose listings state exactly the archive's exponents.
    #[must_use]
    pub fn over(listed: &[Listed]) -> Self {
        Self {
            universe: listed
                .iter()
                .map(|instrument| {
                    (
                        instrument.symbol,
                        instrument.price_exponent,
                        instrument.qty_exponent,
                    )
                })
                .collect(),
            handles: BTreeMap::new(),
        }
    }

    /// A venue whose listings state exponents of their own.
    ///
    /// For the test that the archive's exponents are the ones used: a venue
    /// restating its scale must not change how yesterday's bytes are read.
    #[must_use]
    pub fn over_stating(universe: &[(&'static str, i8, i8)]) -> Self {
        Self {
            universe: universe.to_vec(),
            handles: BTreeMap::new(),
        }
    }
}

impl Adapter for LineAdapter {
    fn message_types(&self) -> &[&'static str] {
        &["quote", "trade", "level", "clear", "heartbeat"]
    }

    fn poll_listings(&mut self, out: &mut dyn ListingSink) {
        for (symbol, price_exponent, qty_exponent) in &self.universe {
            if self.handles.contains_key(*symbol) {
                continue;
            }
            let spec = InstrumentSpec {
                symbol,
                leg1: None,
                leg2: None,
                asset_class: AssetClass::CryptoSpot,
                price_exponent: *price_exponent,
                qty_exponent: *qty_exponent,
                market_model: MarketModel::Clob,
                tick_size: Scalar::text("0.01"),
                lot_size: Scalar::text("1"),
                contract_value: None,
                quoted_per_contract: None,
                expiry_ns: None,
                settle_type: SettleType::NotApplicable,
                price_bound: PriceBound::NonNegative,
            };
            if let Some(handle) = out.list(&spec) {
                self.handles.insert((*symbol).to_owned(), handle);
            }
        }
    }

    fn on_payload(
        &mut self,
        payload: &Payload<'_>,
        out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        let line = core::str::from_utf8(payload.bytes)
            .map_err(|_| ParseError::malformed("payload is not text"))?;
        let mut fields = line.split_whitespace();
        let kind = fields
            .next()
            .ok_or_else(|| ParseError::truncated("empty payload"))?;

        if kind == "h" {
            out.upstream_message("heartbeat");
            return Ok(());
        }

        let symbol = fields
            .next()
            .ok_or_else(|| ParseError::truncated("no symbol"))?;
        let source_ts_ns: u64 = fields
            .next()
            .ok_or_else(|| ParseError::truncated("no timestamp"))?
            .parse()
            .map_err(|_| ParseError::malformed("timestamp is not a number"))?;

        // An update for an instrument this adapter holds no handle for is
        // ordinary and is not an error: the runtime declined to publish it, or
        // the archive never defined it.
        let Some(instrument) = self.handles.get(symbol).copied() else {
            out.upstream_message("quote");
            return Ok(());
        };

        match kind {
            "q" => {
                out.upstream_message("quote");
                let bid = side_update(&mut fields)?;
                let ask = side_update(&mut fields)?;
                out.event(Event::Quote {
                    instrument,
                    source_ts_ns,
                    bid,
                    ask,
                });
            }
            "t" => {
                out.upstream_message("trade");
                let px = Scalar::text(next(&mut fields, "px")?);
                let qty = Scalar::text(next(&mut fields, "qty")?);
                let aggressor = match next(&mut fields, "aggressor")? {
                    "buy" => Aggressor::Buy,
                    "sell" => Aggressor::Sell,
                    "unknown" => Aggressor::Unknown,
                    _ => return Err(ParseError::malformed("aggressor is not a known side")),
                };
                let trade_id: u64 = next(&mut fields, "trade_id")?
                    .parse()
                    .map_err(|_| ParseError::malformed("trade id is not a number"))?;
                out.event(Event::Trade {
                    instrument,
                    source_ts_ns,
                    px,
                    qty,
                    aggressor,
                    trade_id: Some(trade_id),
                    cumulative_volume: None,
                    flags: TradeFlags::NONE,
                });
            }
            "l" => {
                out.upstream_message("level");
                let side = side(next(&mut fields, "side")?)?;
                let px = Scalar::text(next(&mut fields, "px")?);
                let qty = Scalar::text(next(&mut fields, "qty")?);
                let presence = match next(&mut fields, "presence")? {
                    "new" => Presence::New,
                    "change" => Presence::Change,
                    "unknown" => Presence::Unknown,
                    _ => return Err(ParseError::malformed("presence is not a known hint")),
                };
                out.event(Event::Level {
                    instrument,
                    source_ts_ns,
                    side,
                    px,
                    qty,
                    order_count: None,
                    presence,
                });
            }
            "c" => {
                out.upstream_message("clear");
                let side = side(next(&mut fields, "side")?)?;
                out.event(Event::Clear {
                    instrument,
                    source_ts_ns,
                    scope: ClearScope::EntireSide(side),
                });
            }
            _ => return Err(ParseError::schema("unknown upstream record type")),
        }
        Ok(())
    }
}

fn next<'a>(
    fields: &mut impl Iterator<Item = &'a str>,
    what: &'static str,
) -> Result<&'a str, ParseError> {
    fields.next().ok_or(ParseError::Truncated { detail: what })
}

fn side(token: &str) -> Result<Side, ParseError> {
    match token {
        "bid" => Ok(Side::Bid),
        "ask" => Ok(Side::Ask),
        _ => Err(ParseError::malformed("side is not bid or ask")),
    }
}

/// One side of a quote: a price and a quantity, or `- -` for a side with
/// nothing resting.
fn side_update<'a>(
    fields: &mut impl Iterator<Item = &'a str>,
) -> Result<SideUpdate<'a>, ParseError> {
    let px = next(fields, "px")?;
    let qty = next(fields, "qty")?;
    if px == "-" {
        return Ok(SideUpdate::Gone);
    }
    Ok(SideUpdate::Present {
        px: Scalar::text(px),
        qty: Scalar::text(qty),
        source_count: None,
    })
}

/// The upstream window, as an archive of payloads.
#[must_use]
pub fn payloads(lines: &[&str]) -> PayloadLog {
    let connection = ConnectionId::new("mktdata");
    let mut log = PayloadLog::new();
    for (index, line) in lines.iter().enumerate() {
        log.push(
            line.as_bytes(),
            1_700_000_000_000_000_000 + index as u64 * 1_000,
            connection,
        );
    }
    log
}
