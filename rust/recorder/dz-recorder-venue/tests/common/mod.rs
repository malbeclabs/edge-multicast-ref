//! A fixture adapter and a fixture object, so that a derivation can be
//! exercised with no venue, no socket and no filesystem.
//!
//! **The fixture adapter is not a venue.** It reads a line format invented for
//! these tests, and its whole job is to be a thing that implements the boundary:
//! it offers listings, it names its message types, it emits events, and it
//! refuses a payload it is told to refuse. What is under test is the derivation
//! that drives it.
//!
//! Its payload format deliberately **carries a channel and a sequence number**
//! that the adapter parses and reports. That is what makes
//! `the_venue_side_rows_carry_no_publisher_provenance` an assertion rather than
//! a tautology: the two values a venue-side row must not have were in the
//! derivation's hand, in the bytes *and* in the object's own key, and the test
//! is that neither reached a row.
//!
//! Shared by more than one test binary, so not every item is used by every one
//! of them, which is what this allows.
#![allow(dead_code)]

use std::collections::BTreeMap;

use dz_adapter_core::{
    Adapter, AssetClass, ClearScope, Desync, Event, EventSink, InstrumentRef, InstrumentSpec,
    ListingSink, MarketModel, ParseError, Payload, PriceBound, Scalar, SettleType, Side,
    SideUpdate, UpstreamSink,
};
use dz_recorder_archive::upstream::{
    UpstreamConnection, UpstreamFormatError, UpstreamMessage, UpstreamSegmentWriter,
};
use dz_recorder_core::RecvTsKind;
use dz_recorder_venue::object::{VenueObject, VenueObjectId};

/// The channel the fixture's messages state, and which no row may carry.
///
/// Distinctive rather than small, so that a test can assert the value reached no
/// column at all: a `1` would collide with a quantity.
pub const FIXTURE_CHANNEL: u8 = 113;

/// The publisher-shaped sequence number the fixture states, and which no row
/// may carry.
///
/// **Distinct from the venue's own session sequence**, which the fixture states
/// separately and which *does* reach a row, as `upstream_seq`. The two being one
/// number is what would make this fixture unable to tell the difference between
/// evidence a venue-side row keeps and provenance it must not have.
pub const FIXTURE_FIRST_SEQ: u64 = 990_001;

/// The object key of every fixture object.
///
/// It carries the channel and the first sequence number, in the key itself, so
/// that the derivation genuinely has both in hand.
pub const FIXTURE_KEY: &str = "feed=top-of-book/env=test/site=site-1/recorder=recorder-1/\
                               date=2026-09-09/hour=12/channel=113-first_seq=990001-4.dzus";

pub const FIXTURE_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";

/// One instrument the fixture adapter will offer.
#[derive(Debug, Clone)]
pub struct Listing {
    pub symbol: String,
    pub price_exponent: i8,
    pub qty_exponent: i8,
}

impl Listing {
    pub fn new(symbol: &str, price_exponent: i8, qty_exponent: i8) -> Self {
        Self {
            symbol: symbol.to_owned(),
            price_exponent,
            qty_exponent,
        }
    }
}

/// An adapter over a line format, for driving a derivation.
///
/// One line per **archived record**, which is one payload the adapter is handed:
///
/// ```text
/// chan=<u8> pubseq=<u64> seq=<u64> sid=<u64> <op> ...
///   quote  <symbol> <bid_px[@sources]|-> <bid_qty|-> <ask_px[@sources]|-> <ask_qty|->
///   level  <symbol> <bid|ask> <px> <qty>
///   clear  <symbol> <both|bid|ask>
///   trade  <symbol> <px> <qty>
///   listing <symbol> <price_exp> <qty_exp>
///   delist <symbol>    -- withdrawn on the next poll, where `delist` lives
///   refuse <schema|unknown_field|malformed|truncated>
///   unscoped <symbol>   -- closes the payload scope, then emits a quote
/// ```
///
/// **One record may carry several of the venue's own messages, and one of those
/// may report several events.** That is what the adapter boundary permits and
/// what the fixture has to be able to produce, because it is the shape a row's
/// identity depends on:
///
/// * `|` starts another **member** of the record. `upstream_message` and
///   `upstream_identity` are called again, exactly as an adapter unpacking a
///   batch calls them, and the header is stated once on the first member.
/// * `;` reports another **event of the same member**, with no new boundary
///   between them.
///
/// Every member and every event of one record is handed the record's own
/// receive stamp, because that is the only stamp the transport took — which is
/// the whole reason a row needs an ordinal of its own.
#[derive(Debug, Default)]
pub struct FixtureAdapter {
    /// Everything this adapter will offer on its next poll. Its whole set every
    /// time, which the boundary states is free.
    listings: Vec<Listing>,
    handles: BTreeMap<String, InstrumentRef>,
    /// The channels the adapter read off the payloads, in order.
    ///
    /// Read and reported so that a test can prove the value was *available* to
    /// the derivation. An adapter has no way to put it in a row and this is not
    /// one: it is a witness.
    pub channels_read: Vec<u8>,
    /// The publisher-shaped sequence numbers the adapter read off the payloads.
    ///
    /// A witness for the same reason [`channels_read`](Self::channels_read) is.
    /// The venue's own session sequence is a different number and reaches a row;
    /// this one must not.
    pub publisher_sequences_read: Vec<u64>,
    /// The venue's own session sequence numbers the adapter read.
    pub sequences_read: Vec<u64>,
    /// Symbols a `delist` line named, withdrawn on the next poll.
    ///
    /// Held rather than acted on immediately because `delist` is a
    /// [`ListingSink`] method and a payload is handed an [`EventSink`] — which
    /// is also how a venue's own adapter would have to do it.
    to_delist: Vec<String>,
    pub polls: u64,
}

impl FixtureAdapter {
    pub fn new(listings: Vec<Listing>) -> Self {
        Self {
            listings,
            ..Self::default()
        }
    }
}

const MESSAGE_TYPES: &[&str] = &["quote", "level", "clear", "trade", "listing"];

impl Adapter for FixtureAdapter {
    fn message_types(&self) -> &[&'static str] {
        MESSAGE_TYPES
    }

    fn poll_listings(&mut self, out: &mut dyn ListingSink) {
        self.polls += 1;
        // The withdrawals first, so that a record that delisted a symbol and a
        // later one that relisted it are two listings rather than one.
        for symbol in std::mem::take(&mut self.to_delist) {
            // **The handle is kept**, deliberately. An adapter that goes on
            // reporting events on a handle it has delisted is a thing the sink
            // contract permits it to do, and this is how a test gets one.
            if let Some(handle) = self.handles.get(&symbol) {
                out.delist(*handle);
            }
            self.listings.retain(|listing| listing.symbol != symbol);
        }
        for listing in &self.listings {
            let spec = InstrumentSpec {
                symbol: &listing.symbol,
                leg1: None,
                leg2: None,
                asset_class: AssetClass::CryptoSpot,
                price_exponent: listing.price_exponent,
                qty_exponent: listing.qty_exponent,
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
                self.handles.insert(listing.symbol.clone(), handle);
            }
        }
    }

    fn on_connected(
        &mut self,
        _conn: dz_adapter_core::ConnectionId,
        _out: &mut dyn UpstreamSink,
    ) -> Result<(), dz_adapter_core::AdapterError> {
        Ok(())
    }

    fn on_payload(
        &mut self,
        payload: &Payload<'_>,
        out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        let text =
            std::str::from_utf8(payload.bytes).map_err(|_| ParseError::malformed("not utf-8"))?;

        let mut identity = None;
        for member in text.split('|') {
            let mut events = member.split(';');
            let first = events
                .next()
                .ok_or_else(|| ParseError::truncated("member"))?;
            let mut fields = first.split_whitespace();

            // The header is on the record and not on each member of it: a
            // batch is one thing the transport delivered.
            if identity.is_none() {
                let channel = fields
                    .next()
                    .and_then(|f| f.strip_prefix("chan="))
                    .and_then(|v| v.parse::<u8>().ok())
                    .ok_or_else(|| ParseError::malformed("chan"))?;
                let publisher_seq = fields
                    .next()
                    .and_then(|f| f.strip_prefix("pubseq="))
                    .and_then(|v| v.parse::<u64>().ok())
                    .ok_or_else(|| ParseError::malformed("pubseq"))?;
                let seq = fields
                    .next()
                    .and_then(|f| f.strip_prefix("seq="))
                    .and_then(|v| v.parse::<u64>().ok())
                    .ok_or_else(|| ParseError::malformed("seq"))?;
                let sid = fields
                    .next()
                    .and_then(|f| f.strip_prefix("sid="))
                    .and_then(|v| v.parse::<u64>().ok())
                    .ok_or_else(|| ParseError::malformed("sid"))?;
                self.channels_read.push(channel);
                self.publisher_sequences_read.push(publisher_seq);
                self.sequences_read.push(seq);
                identity = Some((sid, seq));
            }
            let (sid, seq) = identity.expect("the header was read on the first member");

            let op = fields.next().ok_or_else(|| ParseError::truncated("op"))?;
            // Before the boundary is stated, because a refusal costs the whole
            // record and states no message at all.
            if op == "refuse" {
                let reason = fields.next().unwrap_or("malformed");
                return Err(match reason {
                    "schema" => ParseError::schema("fixture"),
                    "unknown_field" => ParseError::unknown_field("fixture"),
                    "truncated" => ParseError::truncated("fixture"),
                    _ => ParseError::malformed("fixture"),
                });
            }

            // The boundary's own order: the kind, then the identity of the
            // message the events belong to, then the events. Once per member,
            // which is once per message the venue sent.
            out.upstream_message(
                MESSAGE_TYPES
                    .iter()
                    .find(|t| **t == op)
                    .copied()
                    .unwrap_or("other"),
            );
            out.upstream_identity(Some(sid), Some(seq));
            self.emit(op, &mut fields, payload, out)?;

            // The rest of this member's events, under the boundary already
            // stated: one venue message that reported more than one event.
            for event in events {
                let mut fields = event.split_whitespace();
                let op = fields.next().ok_or_else(|| ParseError::truncated("op"))?;
                self.emit(op, &mut fields, payload, out)?;
            }
        }
        Ok(())
    }
}

impl FixtureAdapter {
    /// One event of one member, as its own op names it.
    fn emit<'a>(
        &mut self,
        op: &str,
        mut fields: &mut impl Iterator<Item = &'a str>,
        payload: &Payload<'_>,
        out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        match op {
            "listing" => {
                let symbol = fields.next().ok_or_else(|| ParseError::truncated("sym"))?;
                let price_exponent = fields
                    .next()
                    .and_then(|v| v.parse::<i8>().ok())
                    .ok_or_else(|| ParseError::malformed("price_exp"))?;
                let qty_exponent = fields
                    .next()
                    .and_then(|v| v.parse::<i8>().ok())
                    .ok_or_else(|| ParseError::malformed("qty_exp"))?;
                // Held for the next poll, exactly as a venue that discovers an
                // instrument mid-session holds it: nothing here mints a handle.
                self.listings
                    .push(Listing::new(symbol, price_exponent, qty_exponent));
            }
            // Withdrawn on the next poll, where `delist` lives. A venue that
            // relists the symbol afterwards is a second listing, and the
            // exponents it states then are its own.
            "delist" => {
                let symbol = fields.next().ok_or_else(|| ParseError::truncated("sym"))?;
                self.to_delist.push(symbol.to_owned());
            }
            "quote" => {
                let symbol = fields.next().ok_or_else(|| ParseError::truncated("sym"))?;
                let instrument = *self
                    .handles
                    .get(symbol)
                    .ok_or_else(|| ParseError::unknown_field("symbol"))?;
                let bid = side(&mut fields)?;
                let ask = side(&mut fields)?;
                out.event(Event::Quote {
                    instrument,
                    source_ts_ns: payload.recv_ts_ns.saturating_sub(1_000),
                    bid,
                    ask,
                });
            }
            "level" => {
                let symbol = fields.next().ok_or_else(|| ParseError::truncated("sym"))?;
                let instrument = *self
                    .handles
                    .get(symbol)
                    .ok_or_else(|| ParseError::unknown_field("symbol"))?;
                let side = match fields.next() {
                    Some("bid") => Side::Bid,
                    Some("ask") => Side::Ask,
                    _ => return Err(ParseError::malformed("side")),
                };
                let px = fields.next().ok_or_else(|| ParseError::truncated("px"))?;
                let qty = fields.next().ok_or_else(|| ParseError::truncated("qty"))?;
                out.event(Event::Level {
                    instrument,
                    source_ts_ns: payload.recv_ts_ns.saturating_sub(1_000),
                    side,
                    px: Scalar::text(px),
                    qty: Scalar::text(qty),
                    order_count: None,
                    presence: dz_adapter_core::Presence::Unknown,
                });
            }
            "clear" => {
                let symbol = fields.next().ok_or_else(|| ParseError::truncated("sym"))?;
                let instrument = *self
                    .handles
                    .get(symbol)
                    .ok_or_else(|| ParseError::unknown_field("symbol"))?;
                let scope = match fields.next() {
                    Some("both") => ClearScope::BothSides,
                    Some("bid") => ClearScope::EntireSide(Side::Bid),
                    Some("ask") => ClearScope::EntireSide(Side::Ask),
                    _ => return Err(ParseError::malformed("scope")),
                };
                out.event(Event::Clear {
                    instrument,
                    source_ts_ns: payload.recv_ts_ns.saturating_sub(1_000),
                    scope,
                });
            }
            "trade" => {
                let symbol = fields.next().ok_or_else(|| ParseError::truncated("sym"))?;
                let instrument = *self
                    .handles
                    .get(symbol)
                    .ok_or_else(|| ParseError::unknown_field("symbol"))?;
                let px = fields.next().ok_or_else(|| ParseError::truncated("px"))?;
                let qty = fields.next().ok_or_else(|| ParseError::truncated("qty"))?;
                out.event(Event::Trade {
                    instrument,
                    source_ts_ns: payload.recv_ts_ns.saturating_sub(1_000),
                    px: Scalar::text(px),
                    qty: Scalar::text(qty),
                    aggressor: dz_adapter_core::Aggressor::Unknown,
                    trade_id: None,
                    cumulative_volume: None,
                    flags: dz_adapter_core::TradeFlags::NONE,
                });
            }
            // The one op that closes the payload scope the derivation opened
            // and then reports an event anyway, which the sink's contract
            // permits an adapter to do. What follows is attributable to no
            // upstream message, and this is how a test gets one.
            "unscoped" => {
                let symbol = fields.next().ok_or_else(|| ParseError::truncated("sym"))?;
                let instrument = *self
                    .handles
                    .get(symbol)
                    .ok_or_else(|| ParseError::unknown_field("symbol"))?;
                out.payload_scope(None);
                out.event(Event::Quote {
                    instrument,
                    source_ts_ns: payload.recv_ts_ns.saturating_sub(1_000),
                    bid: SideUpdate::Present {
                        px: Scalar::text("1.00"),
                        qty: Scalar::text("1"),
                        source_count: None,
                    },
                    ask: SideUpdate::Present {
                        px: Scalar::text("2.00"),
                        qty: Scalar::text("1"),
                        source_count: None,
                    },
                });
            }
            "desync" => {
                let symbol = fields.next().ok_or_else(|| ParseError::truncated("sym"))?;
                let instrument = *self
                    .handles
                    .get(symbol)
                    .ok_or_else(|| ParseError::unknown_field("symbol"))?;
                out.desynchronised(instrument, Desync::VenueResync);
            }
            _ => return Err(ParseError::schema("op")),
        }
        Ok(())
    }
}

fn side<'a, I: Iterator<Item = &'a str>>(fields: &mut I) -> Result<SideUpdate<'a>, ParseError> {
    let px = fields.next().ok_or_else(|| ParseError::truncated("px"))?;
    let qty = fields.next().ok_or_else(|| ParseError::truncated("qty"))?;
    if px == "-" || qty == "-" {
        return Ok(SideUpdate::Gone);
    }
    // `<px>` or `<px>@<sources>`: how many upstreams contributed to this side of
    // the top, which only a quote states. A `Level` has no way to say it, so the
    // fixture's level lines have no equivalent.
    let (px, source_count) = match px.split_once('@') {
        Some((px, sources)) => (
            px,
            Some(
                sources
                    .parse::<u16>()
                    .map_err(|_| ParseError::malformed("source_count"))?,
            ),
        ),
        None => (px, None),
    };
    Ok(SideUpdate::Present {
        px: Scalar::text(px),
        qty: Scalar::text(qty),
        source_count,
    })
}

/// The connection every fixture object declares.
pub const CONNECTION: &str = "mktdata";

/// A fixture object held in memory.
///
/// The reference the tests drive: no filesystem, and the messages are the exact
/// bytes an archived object would have carried, because they are written and
/// read back through the archive's own writer and reader.
pub struct FixtureObject {
    id: VenueObjectId,
    inner: dz_recorder_venue::object::ArchivedVenueObject<std::io::Cursor<Vec<u8>>>,
}

impl FixtureObject {
    /// An object of these lines, one upstream message each, a millisecond
    /// apart, starting at `base`.
    pub fn of(base: u64, lines: &[&str]) -> Self {
        Self::at(base, "site-1/recorder-1", lines)
    }

    /// The same, at a named observation point.
    pub fn at(base: u64, observation: &str, lines: &[&str]) -> Self {
        let connections = vec![UpstreamConnection::new(
            CONNECTION,
            RecvTsKind::KernelSoftware,
        )];
        let mut writer =
            UpstreamSegmentWriter::open(Vec::new(), &connections).expect("the header is writable");
        for (index, line) in lines.iter().enumerate() {
            writer
                .write_message(0, base + index as u64 * 1_000_000, line.as_bytes())
                .expect("the message is writable");
        }
        let bytes = writer.finish().expect("the segment flushes");
        Self::over(observation, bytes)
    }

    /// An object over bytes the caller composed, so a test can truncate one.
    pub fn over(observation: &str, bytes: Vec<u8>) -> Self {
        let id = VenueObjectId {
            object_key: FIXTURE_KEY.to_owned(),
            object_sha256: FIXTURE_SHA.to_owned(),
            observation: observation.to_owned(),
            env: "test".to_owned(),
            feed: "top-of-book".to_owned(),
            connections: vec![dz_adapter_core::ConnectionId::new(CONNECTION)],
        };
        let inner = dz_recorder_venue::object::ArchivedVenueObject::open(
            id.clone(),
            std::io::Cursor::new(bytes),
        )
        .expect("the fixture object is readable");
        Self { id, inner }
    }

    /// The same object with no connection declared by the caller, so that a
    /// name the object states resolves to nothing.
    pub fn with_no_declared_connections(mut self) -> Self {
        self.id.connections.clear();
        self
    }
}

impl VenueObject for FixtureObject {
    fn id(&self) -> &VenueObjectId {
        &self.id
    }

    fn format_version(&self) -> u16 {
        self.inner.format_version()
    }

    fn declared_connections(&self) -> Vec<String> {
        self.inner.declared_connections()
    }

    fn next_message(&mut self) -> Result<Option<UpstreamMessage<'_>>, UpstreamFormatError> {
        self.inner.next_message()
    }
}

/// The bytes of an object of these lines, so a test can cut one short.
pub fn object_bytes(base: u64, lines: &[&str]) -> Vec<u8> {
    let connections = vec![UpstreamConnection::new(
        CONNECTION,
        RecvTsKind::KernelSoftware,
    )];
    let mut writer =
        UpstreamSegmentWriter::open(Vec::new(), &connections).expect("the header is writable");
    for (index, line) in lines.iter().enumerate() {
        writer
            .write_message(0, base + index as u64 * 1_000_000, line.as_bytes())
            .expect("the message is writable");
    }
    writer.finish().expect("the segment flushes")
}
