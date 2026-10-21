//! `Venue::threaded`: the variant a runtime picks the input model from, and the
//! inline handle it leaves in `adapter`.

use std::time::Duration;

use dz_adapter_core::threaded::{ClaimTable, DecodedSink, Decoder, Inline, Order, ThreadedAdapter};
use dz_adapter_core::{
    Adapter, ConnectionId, Event, EventSink, InstrumentRef, ListingSink, ParseError, Payload,
    Scalar, SideUpdate,
};
use dz_ingress_core::{BoxFuture, IngressError, Input, Received, UpstreamMessage};
use dz_venue_composition::Venue;

/// Every payload is one quote for instrument 0, keyed by its first byte.
#[derive(Default)]
struct OneQuote {
    inline: Option<Inline>,
}

struct OneQuoteDecoder;

impl Decoder for OneQuoteDecoder {
    fn decode(
        &mut self,
        payload: &Payload<'_>,
        out: &mut dyn DecodedSink,
    ) -> Result<(), ParseError> {
        let key = *payload
            .bytes
            .first()
            .ok_or(ParseError::truncated("empty"))?;
        out.upstream_message("quote");
        out.push(
            Event::Quote {
                instrument: InstrumentRef::from_admission(0),
                source_ts_ns: 0,
                bid: SideUpdate::Present {
                    px: Scalar::fixed(i64::from(key), 0),
                    qty: Scalar::fixed(1, 0),
                    source_count: None,
                },
                ask: SideUpdate::Gone,
                venue_update_id: None,
            },
            Order::Supersedes(u64::from(key)),
        );
        Ok(())
    }
}

impl ThreadedAdapter for OneQuote {
    fn decoder(&self, _conn: ConnectionId, _claims: ClaimTable) -> Box<dyn Decoder> {
        Box::new(OneQuoteDecoder)
    }
}

impl Adapter for OneQuote {
    fn message_types(&self) -> &[&'static str] {
        &["quote"]
    }

    fn poll_listings(&mut self, _out: &mut dyn ListingSink) {}

    fn on_payload(
        &mut self,
        payload: &Payload<'_>,
        out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        let mut inline = self.inline.take().unwrap_or_default();
        let result = inline.on_payload(self, payload, out);
        self.inline = Some(inline);
        result
    }
}

/// A transport that is never connected.
struct Unused;

impl Input for Unused {
    fn connection(&self) -> ConnectionId {
        ConnectionId::new("primary")
    }

    fn connect(&mut self, _timeout: Duration) -> BoxFuture<'_, Result<(), IngressError>> {
        Box::pin(std::future::pending())
    }

    fn send<'a>(
        &'a mut self,
        _message: UpstreamMessage<'a>,
    ) -> BoxFuture<'a, Result<(), IngressError>> {
        Box::pin(std::future::pending())
    }

    fn recv<'a>(
        &'a mut self,
        _budget: Option<Duration>,
    ) -> BoxFuture<'a, Result<Received<'a>, IngressError>> {
        Box::pin(std::future::pending())
    }

    fn shutdown(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(std::future::ready(()))
    }
}

#[derive(Default)]
struct Events(Vec<String>);

impl EventSink for Events {
    fn upstream_message(&mut self, _message_type: &'static str) {}

    fn event(&mut self, event: Event<'_>) {
        self.0.push(format!("{event:?}"));
    }
}

#[test]
fn an_inline_venue_carries_no_threaded_adapter() {
    let venue = Venue::new(Box::new(OneQuote::default()), vec![Box::new(Unused)]);
    assert!(venue.threaded.is_none());
}

#[test]
fn a_threaded_venue_is_picked_by_its_variant_and_still_drives_inline() {
    let mut venue = Venue::threaded(Box::new(OneQuote::default()), vec![Box::new(Unused)]);
    assert!(venue.threaded.is_some());
    assert_eq!(venue.sources.len(), 1);
    assert!(venue.collectors.is_empty());
    assert_eq!(venue.adapter.message_types(), ["quote"]);

    // `adapter` is the same object, driven inline through the shared lock.
    let mut events = Events::default();
    for bytes in [b"\x02", b"\x02", b"\x01", b"\x03"] {
        let payload = Payload {
            bytes,
            recv_ts_ns: 1,
            connection: ConnectionId::new("primary"),
        };
        venue
            .adapter
            .on_payload(&payload, &mut events)
            .expect("parses");
    }
    assert_eq!(events.0.len(), 2, "keys 2 and 3, once each: {:?}", events.0);

    // And the threaded handle reaches that object too.
    let shared = venue.threaded.as_ref().expect("threaded");
    let held = shared.lock().expect("uncontended");
    let _decoder = held.decoder(ConnectionId::new("primary"), ClaimTable::new());
}
