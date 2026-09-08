//! A normalized-event record encoding, and the built-in adapter that reads it.
//!
//! Two things want the same encoding, pointed in opposite directions.
//!
//! **A venue whose integration is not Rust** cannot implement the adapter
//! trait, and would therefore be outside this whole boundary.
//! `[adapter] kind = "uds"` lets that integration be another process in any
//! language: it writes records, and [`UdsAdapter`] reads them. It costs a
//! serialization format and a copy per event, which is exactly why a Rust
//! venue should not use it — the design says so in as many words, and this
//! crate existing is not an argument against that.
//!
//! **A reference stream for the offline comparison** needs a publisher's own
//! normalized events written down. That is [`RecordWriter`], the same encoding
//! written rather than read.
//!
//! It is **not** what `[adapter.tee]` carries, and the distinction matters
//! because the two answer different questions. The tee carries byte-identical
//! copies of composed *wire* datagrams, so what it feeds a recorder can be
//! diffed against a subscriber-site archive datagram for datagram — that stream
//! exists today. This encoding is a publisher's *normalized events*, upstream of
//! the lowering, which is what makes a mapping defect visible at all: a tee'd
//! datagram reproduces the defect faithfully on both sides. Neither replaces the
//! other, and the offline re-lowering is the reason this half exists.
//!
//! # What this crate is not
//!
//! It opens no socket. Reading bytes off a Unix socket is a transport's job and
//! belongs to `dz-ingress-*`; [`UdsAdapter`] is an [`Adapter`], so it is handed
//! payloads and asked what they mean. That split is the same one the boundary
//! rests on everywhere else, and it is why this adapter is testable with no
//! socket at all.

#![forbid(unsafe_code)]

pub mod record;

pub use record::{
    decode, record_len, RecordError, RecordWriteError, RecordWriter, HEADER, VERSION,
};

use std::collections::HashMap;

use dz_adapter_core::{
    Adapter, ConnectionId, DisconnectReason, EventSink, InstrumentRef, InstrumentSpec, ListingSink,
    ParseError, Payload,
};

/// The upstream message type this adapter counts.
///
/// One, and it is this encoding's own name rather than a venue's: whatever the
/// process on the other end calls its messages, what arrives here is a record.
const MESSAGE_TYPES: &[&str] = &["record"];

/// The built-in adapter for a source that is another process.
///
/// # What it decides, which is almost nothing
///
/// It resolves a record's symbol to the handle the runtime minted, and hands
/// the event on. It holds no book, applies no microstructure and refuses no
/// value — because the process on the other side already did all of that, and
/// this is the seam it crosses rather than a second opinion about it.
///
/// # The listings are the caller's
///
/// A record names a symbol, so this adapter can only resolve symbols it was
/// told to offer. Which instruments those are is configuration — the source
/// process and the publisher have to agree out of band, and there is no
/// discovery in this encoding. Stated because it is the one thing a reader
/// will expect and not find.
pub struct UdsAdapter {
    /// The specifications this adapter offers, from configuration.
    listings: Vec<UdsListing>,
    /// How many have been offered. Offering is idempotent at the sink, but
    /// re-walking the set every poll is work proportional to the set rather
    /// than to what changed.
    offered: usize,
    /// Symbol to the handle the runtime minted for it.
    admitted: HashMap<String, InstrumentRef>,
}

/// One instrument a source process will send records for, as configuration
/// states it.
///
/// An owned mirror of [`InstrumentSpec`] and the one place in this family where
/// such a mirror is right: the spec borrows, and this has to outlive the poll
/// that offers it.
#[derive(Debug, Clone)]
pub struct UdsListing {
    pub symbol: String,
    pub leg1: Option<String>,
    pub leg2: Option<String>,
    pub asset_class: dz_adapter_core::AssetClass,
    pub price_exponent: i8,
    pub qty_exponent: i8,
    pub market_model: dz_adapter_core::MarketModel,
    pub tick_size: String,
    pub lot_size: String,
    pub contract_value: Option<String>,
    pub quoted_per_contract: Option<String>,
    pub expiry_ns: Option<u64>,
    pub settle_type: dz_adapter_core::SettleType,
    pub price_bound: dz_adapter_core::PriceBound,
}

impl UdsListing {
    fn spec(&self) -> InstrumentSpec<'_> {
        use dz_adapter_core::Scalar;
        InstrumentSpec {
            symbol: &self.symbol,
            leg1: self.leg1.as_deref(),
            leg2: self.leg2.as_deref(),
            asset_class: self.asset_class,
            price_exponent: self.price_exponent,
            qty_exponent: self.qty_exponent,
            market_model: self.market_model,
            tick_size: Scalar::Text(&self.tick_size),
            lot_size: Scalar::Text(&self.lot_size),
            contract_value: self.contract_value.as_deref().map(Scalar::Text),
            quoted_per_contract: self.quoted_per_contract.as_deref().map(Scalar::Text),
            expiry_ns: self.expiry_ns,
            settle_type: self.settle_type,
            price_bound: self.price_bound,
        }
    }
}

impl UdsAdapter {
    /// An adapter that will offer `listings` and read records for them.
    #[must_use]
    pub fn new(listings: Vec<UdsListing>) -> Self {
        Self {
            listings,
            offered: 0,
            admitted: HashMap::new(),
        }
    }

    /// The handle for a symbol, if the runtime admitted it.
    #[must_use]
    pub fn handle(&self, symbol: &str) -> Option<InstrumentRef> {
        self.admitted.get(symbol).copied()
    }
}

impl Adapter for UdsAdapter {
    fn message_types(&self) -> &[&'static str] {
        MESSAGE_TYPES
    }

    fn poll_listings(&mut self, out: &mut dyn ListingSink) {
        while self.offered < self.listings.len() {
            let listing = &self.listings[self.offered];
            self.offered += 1;
            if let Some(handle) = out.list(&listing.spec()) {
                self.admitted.insert(listing.symbol.clone(), handle);
            }
        }
    }

    fn on_disconnected(&mut self, _conn: ConnectionId, _reason: DisconnectReason) {
        // Nothing to invalidate. This adapter holds no book, and the handles it
        // holds are the runtime's and survive a reconnect — a source process
        // restarting does not un-admit an instrument.
    }

    /// One payload, which is **one or more** records.
    ///
    /// A transport that reads a byte stream hands over whatever arrived, so a
    /// payload may hold several records or the caller may have already split
    /// them. Both work: the loop steps by each record's declared length, and a
    /// trailing partial record is [`ParseError::Truncated`] rather than a
    /// silent stop — a reader that quietly ignored a partial tail would lose
    /// one event per read on a stream that never aligns.
    ///
    /// A record whose version or kind this build does not know is **skipped**,
    /// not failed. That is the reason the length comes first: a newer writer
    /// stays readable, and one unknown record does not end the stream.
    fn on_payload(
        &mut self,
        payload: &Payload<'_>,
        out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        let mut rest = payload.bytes;
        while !rest.is_empty() {
            let Some(len) = record_len(rest) else {
                return Err(ParseError::truncated("a record is split across payloads"));
            };
            let (record, tail) = rest.split_at(len);
            rest = tail;

            out.upstream_message("record");
            match decode(record, |symbol| self.admitted.get(symbol).copied()) {
                Ok(Some(event)) => out.event(event),
                // A record for an instrument this runtime did not admit. The
                // selection policy is the runtime's, and a source offering more
                // than it admits is the normal case.
                Ok(None) => {}
                Err(RecordError::Version { .. } | RecordError::Kind { .. }) => {}
                Err(RecordError::Truncated) => {
                    return Err(ParseError::truncated("a record declared more than it held"))
                }
                Err(RecordError::Malformed { detail }) => {
                    return Err(ParseError::malformed(detail))
                }
            }
        }
        Ok(())
    }
}
