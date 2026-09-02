//! An adapter, held behind a `Box<dyn Adapter>` and driven the way the runtime
//! will drive one.
//!
//! Two properties, and both are about the trait rather than about this adapter.
//! It is object-safe, which is what lets configuration select between several
//! linked implementations. And it can be exercised end to end with no network,
//! no socket, no runtime and no clock — which is the property a venue's own
//! mapping tests depend on, and the same property that makes the offline
//! re-lowering comparison possible at all.

use dz_adapter_core::{
    Adapter, AdapterError, AssetClass, ConnectionId, DisconnectReason, Event, EventSink,
    InstrumentRef, InstrumentSpec, ListingSink, MarketModel, ParseError, Payload, Presence,
    PriceBound, Scalar, SettleType, Side, SideUpdate, SnapshotSink, TradeFlags, UpstreamSink,
};

// ---------------------------------------------------------------- the runtime

/// What the runtime does with an admitted set, reduced to what a test needs:
/// hand out dense handles, and remember what was offered.
#[derive(Default)]
struct Listings {
    admitted: Vec<String>,
    delisted: Vec<InstrumentRef>,
    /// Instruments beyond this are declined, standing in for the selection
    /// policy's published cap.
    cap: usize,
}

impl ListingSink for Listings {
    fn list(&mut self, spec: &InstrumentSpec<'_>) -> Option<InstrumentRef> {
        if let Some(index) = self.admitted.iter().position(|s| s == spec.symbol) {
            // Re-offering is free and returns the handle already minted, which
            // is what lets an adapter offer its whole set on every poll.
            return Some(InstrumentRef::from_admission(index as u32));
        }
        if self.admitted.len() >= self.cap {
            return None;
        }
        self.admitted.push(spec.symbol.to_string());
        Some(InstrumentRef::from_admission(
            (self.admitted.len() - 1) as u32,
        ))
    }

    fn delist(&mut self, instrument: InstrumentRef) {
        self.delisted.push(instrument);
    }
}

#[derive(Default)]
struct Events {
    message_types: Vec<&'static str>,
    quotes: usize,
    levels: usize,
    trades: usize,
    clears: usize,
}

impl EventSink for Events {
    fn upstream_message(&mut self, message_type: &'static str) {
        self.message_types.push(message_type);
    }

    fn event(&mut self, event: Event<'_>) {
        match event {
            Event::Quote { .. } => self.quotes += 1,
            Event::Trade { .. } => self.trades += 1,
            Event::Level { .. } => self.levels += 1,
            Event::Clear { .. } => self.clears += 1,
            _ => {}
        }
    }
}

#[derive(Default)]
struct Frames(Vec<String>);

impl UpstreamSink for Frames {
    fn send_text(&mut self, text: &str) {
        self.0.push(text.to_string());
    }

    fn send_binary(&mut self, bytes: &[u8]) {
        self.0.push(format!("{} bytes", bytes.len()));
    }
}

#[derive(Default)]
struct Levels(Vec<(Side, String, String)>);

impl SnapshotSink for Levels {
    fn level(&mut self, side: Side, px: Scalar<'_>, qty: Scalar<'_>, _order_count: Option<u16>) {
        let render = |s: Scalar<'_>| match s {
            Scalar::Text(t) => t.to_string(),
            Scalar::Fixed { mantissa, exponent } => format!("{mantissa}e{exponent}"),
        };
        self.0.push((side, render(px), render(qty)));
    }
}

// ----------------------------------------------------------------- the adapter

/// A venue whose payloads are one ASCII line each. Enough to exercise every
/// method on the trait and nothing more.
///
/// Its book is one price level, which is what `snapshot` writes back — the
/// shape that matters is that the book lives here and the framing does not.
#[derive(Default)]
struct LineAdapter {
    admitted: Option<InstrumentRef>,
    resting: Option<(String, String)>,
    subscribed: bool,
    disconnects: Vec<DisconnectReason>,
}

impl Adapter for LineAdapter {
    fn message_types(&self) -> &[&'static str] {
        &["quote", "level", "trade"]
    }

    fn poll_listings(&mut self, out: &mut dyn ListingSink) {
        if self.admitted.is_some() {
            return;
        }
        self.admitted = out.list(&InstrumentSpec {
            symbol: "EXAMPLE-1",
            leg1: None,
            leg2: None,
            asset_class: AssetClass::PredictionBinary,
            price_exponent: -2,
            qty_exponent: 0,
            market_model: MarketModel::Clob,
            tick_size: Scalar::text("0.01"),
            lot_size: Scalar::text("1"),
            contract_value: None,
            expiry_ns: None,
            settle_type: SettleType::Cash,
            price_bound: PriceBound::UnitInterval,
        });
    }

    fn on_connected(
        &mut self,
        _conn: ConnectionId,
        out: &mut dyn UpstreamSink,
    ) -> Result<(), AdapterError> {
        self.subscribed = true;
        out.send_text("subscribe EXAMPLE-1");
        Ok(())
    }

    fn on_disconnected(&mut self, _conn: ConnectionId, reason: DisconnectReason) {
        self.subscribed = false;
        self.disconnects.push(reason);
    }

    fn on_payload(
        &mut self,
        payload: &Payload<'_>,
        out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        let line = core::str::from_utf8(payload.bytes)
            .map_err(|_| ParseError::malformed("payload is not utf-8"))?;
        let Some(instrument) = self.admitted else {
            // Nothing admitted yet, so nothing to attribute this to. Ordinary,
            // and not an error.
            return Ok(());
        };
        let mut fields = line.split(' ');
        let kind = fields.next().ok_or(ParseError::truncated("empty line"))?;

        match kind {
            "quote" => {
                let bid = fields.next().ok_or(ParseError::truncated("bid"))?;
                let ask = fields.next().ok_or(ParseError::truncated("ask"))?;
                out.upstream_message("quote");
                out.event(Event::Quote {
                    instrument,
                    source_ts_ns: payload.recv_ts_ns,
                    bid: if bid == "-" {
                        SideUpdate::Gone
                    } else {
                        SideUpdate::Present {
                            px: Scalar::text(bid),
                            qty: Scalar::text("1"),
                            source_count: None,
                        }
                    },
                    // Both sides are read out of this adapter's own view every
                    // time. A venue with nothing new to say emits no quote at
                    // all rather than a quote that says nothing, because the
                    // wire has no way to carry "this side did not move".
                    ask: if ask == "-" {
                        SideUpdate::Gone
                    } else {
                        SideUpdate::Present {
                            px: Scalar::text(ask),
                            qty: Scalar::text("1"),
                            source_count: None,
                        }
                    },
                });
                Ok(())
            }
            "level" => {
                let px = fields.next().ok_or(ParseError::truncated("px"))?;
                let qty = fields.next().ok_or(ParseError::truncated("qty"))?;
                out.upstream_message("level");
                // The book is this adapter's, and it is what `snapshot` reads.
                if qty == "0" {
                    self.resting = None;
                } else {
                    self.resting = Some((px.to_string(), qty.to_string()));
                }
                out.event(Event::Level {
                    instrument,
                    source_ts_ns: payload.recv_ts_ns,
                    side: Side::Bid,
                    px: Scalar::text(px),
                    qty: Scalar::text(qty),
                    order_count: None,
                    // A venue whose upstream does not distinguish an insertion
                    // from a change says so. It never says anything about a
                    // removal, because a removal is derived from the quantity.
                    presence: Presence::Unknown,
                });
                Ok(())
            }
            "trade" => {
                let px = fields.next().ok_or(ParseError::truncated("px"))?;
                out.upstream_message("trade");
                out.event(Event::Trade {
                    instrument,
                    source_ts_ns: payload.recv_ts_ns,
                    px: Scalar::text(px),
                    qty: Scalar::text("1"),
                    aggressor: dz_adapter_core::Aggressor::Buy,
                    trade_id: None,
                    cumulative_volume: None,
                    flags: TradeFlags::NONE,
                });
                Ok(())
            }
            other => {
                let _ = other;
                Err(ParseError::schema("unrecognised line kind"))
            }
        }
    }

    fn snapshot(
        &self,
        instrument: InstrumentRef,
        out: &mut dyn SnapshotSink,
    ) -> Result<(), AdapterError> {
        if Some(instrument) != self.admitted {
            return Err(AdapterError::UnknownInstrument);
        }
        let Some((px, qty)) = &self.resting else {
            return Err(AdapterError::NotReady { detail: "no book" });
        };
        out.level(Side::Bid, Scalar::text(px), Scalar::text(qty), None);
        Ok(())
    }
}

// ------------------------------------------------------------------- the tests

fn payload<'a>(line: &'a str, connection: ConnectionId) -> Payload<'a> {
    Payload {
        bytes: line.as_bytes(),
        recv_ts_ns: 1_000,
        connection,
    }
}

#[test]
fn an_adapter_is_object_safe_and_drives_end_to_end() {
    let mut adapter: Box<dyn Adapter> = Box::new(LineAdapter::default());
    let conn = ConnectionId::new("mktdata");

    let mut listings = Listings {
        cap: 4,
        ..Listings::default()
    };
    adapter.poll_listings(&mut listings);
    assert_eq!(listings.admitted, vec!["EXAMPLE-1".to_string()]);

    let mut frames = Frames::default();
    adapter.on_connected(conn, &mut frames).expect("subscribe");
    assert_eq!(frames.0, vec!["subscribe EXAMPLE-1".to_string()]);

    let mut events = Events::default();
    adapter
        .on_payload(&payload("quote 0.41 -", conn), &mut events)
        .expect("quote");
    adapter
        .on_payload(&payload("level 0.41 5", conn), &mut events)
        .expect("level");
    adapter
        .on_payload(&payload("trade 0.41", conn), &mut events)
        .expect("trade");

    assert_eq!(events.quotes, 1);
    assert_eq!(events.levels, 1);
    assert_eq!(events.trades, 1);
    assert_eq!(events.message_types, vec!["quote", "level", "trade"]);

    let mut levels = Levels::default();
    adapter
        .snapshot(InstrumentRef::from_admission(0), &mut levels)
        .expect("snapshot");
    assert_eq!(
        levels.0,
        vec![(Side::Bid, "0.41".to_string(), "5".to_string())]
    );

    adapter.on_disconnected(conn, DisconnectReason::RemoteClose);
}

#[test]
fn re_offering_an_instrument_returns_the_handle_already_minted() {
    let mut adapter = LineAdapter::default();
    let mut listings = Listings {
        cap: 4,
        ..Listings::default()
    };

    adapter.poll_listings(&mut listings);
    let first = adapter.admitted;
    adapter.admitted = None;
    adapter.poll_listings(&mut listings);

    assert_eq!(first, adapter.admitted);
    assert_eq!(listings.admitted.len(), 1, "the same symbol admitted twice");
}

#[test]
fn a_declined_listing_is_ordinary() {
    // The published cap is the playbook's policy, not the venue's, and an
    // adapter has to keep working when it says no.
    let mut adapter = LineAdapter::default();
    let mut listings = Listings::default(); // cap 0: everything declined

    adapter.poll_listings(&mut listings);
    assert!(adapter.admitted.is_none());

    let mut events = Events::default();
    adapter
        .on_payload(
            &payload("quote 0.41 -", ConnectionId::new("mktdata")),
            &mut events,
        )
        .expect("a payload for an unadmitted instrument is not an error");
    assert_eq!(events.quotes, 0);
}

#[test]
fn a_payload_the_adapter_cannot_read_names_its_own_reason() {
    let mut adapter = LineAdapter::default();
    let conn = ConnectionId::new("mktdata");
    let mut listings = Listings {
        cap: 4,
        ..Listings::default()
    };
    adapter.poll_listings(&mut listings);
    let mut events = Events::default();

    let unknown = adapter
        .on_payload(&payload("candle 0.41", conn), &mut events)
        .expect_err("unrecognised kind");
    assert_eq!(unknown.as_str(), "schema");

    let short = adapter
        .on_payload(&payload("quote", conn), &mut events)
        .expect_err("missing fields");
    assert_eq!(short.as_str(), "truncated");

    // A failed payload emits nothing, and does not end the connection.
    assert_eq!(events.quotes, 0);
}

#[test]
fn a_book_that_has_not_bootstrapped_is_not_ready_rather_than_broken() {
    // The distinction the snapshot rotation acts on: skip this slot and come
    // back, versus a disagreement between two admitted sets.
    let mut adapter = LineAdapter::default();
    let mut listings = Listings {
        cap: 4,
        ..Listings::default()
    };
    adapter.poll_listings(&mut listings);
    let mut levels = Levels::default();

    assert_eq!(
        adapter.snapshot(InstrumentRef::from_admission(0), &mut levels),
        Err(AdapterError::NotReady { detail: "no book" })
    );
    assert_eq!(
        adapter.snapshot(InstrumentRef::from_admission(9), &mut levels),
        Err(AdapterError::UnknownInstrument)
    );
}

#[test]
fn a_top_of_book_adapter_needs_no_snapshot_and_no_connection() {
    // Everything a feed with no snapshot port and no outbound connection has to
    // write is nothing, and the defaults are what let it write nothing.
    struct Minimal;

    impl Adapter for Minimal {
        fn message_types(&self) -> &[&'static str] {
            &[]
        }
        fn poll_listings(&mut self, _out: &mut dyn ListingSink) {}
        fn on_payload(
            &mut self,
            _payload: &Payload<'_>,
            _out: &mut dyn EventSink,
        ) -> Result<(), ParseError> {
            Ok(())
        }
    }

    let adapter: Box<dyn Adapter> = Box::new(Minimal);
    let mut levels = Levels::default();
    assert!(adapter
        .snapshot(InstrumentRef::from_admission(0), &mut levels)
        .is_ok());
    assert!(levels.0.is_empty());
}
