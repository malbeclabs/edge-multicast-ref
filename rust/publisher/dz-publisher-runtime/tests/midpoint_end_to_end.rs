//! A VWAP, from an adapter's event to the midpoint feed's ports.
//!
//! Expected values are transcribed from the midpoint specification and the
//! harness instrument's exponents (price -2, quantity -3), not read off the
//! code under test.

mod harness;
use harness::Arrive as _;

use std::num::NonZeroU32;
use std::time::Duration;

use dz_adapter_core::{
    Adapter, Event, EventSink, InstrumentRef, ListingSink, ParseError, Payload, Scalar, Strike,
};
use dz_edge_midpoint::Midpoint;
use dz_edge_refdata::InstrumentDefinition;
use harness::{feed, harness, midpoint_feed, spec, FakeAdapter, MID_ERA, SOURCE_ID};

const MIDPOINT: u8 = 0x03;
const DEFINITION: u8 = 0x02;
const STRIKE: u8 = 0x09;
const METHOD_VWAP: u8 = 5;
const QUALITY_STALE: u8 = 0x01;
const QUALITY_SYNTHETIC: u8 = 0x08;

fn vwap(instrument: InstrumentRef, volume: &'static str, stale: bool) -> Event<'static> {
    Event::Vwap {
        instrument,
        window_end_ns: 1_700_000_000_000_000_001,
        window_ms: NonZeroU32::new(60_000).expect("not zero"),
        compute_ts_ns: 1_700_000_000_000_000_009,
        px: Scalar::text("100.25"),
        volume: Scalar::text(volume),
        stale,
    }
}

fn midpoints(h: &harness::Harness) -> Vec<Midpoint> {
    h.mktdata()
        .messages()
        .iter()
        .filter(|(type_id, _)| *type_id == MIDPOINT)
        .map(|(_, bytes)| Midpoint::decode(bytes).expect("this publisher composed it"))
        .collect()
}

/// One lap of the definition cycle, a second at a time.
fn lap(h: &mut harness::Harness) {
    for _ in 0..31 {
        h.clock.advance(Duration::from_secs(1));
        let _ = h.publisher.tick();
    }
}

#[test]
fn a_vwap_reaches_the_midpoint_feed_as_a_midpoint() {
    let mut h = harness(midpoint_feed());
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.publisher.arrive(vwap(instrument, "2.500", false));
    h.publisher.arrive(vwap(instrument, "0", true));
    lap(&mut h);

    let sent = midpoints(&h);
    assert_eq!(sent.len(), 2);
    let full = sent[0];
    assert_eq!(full.instrument_id, 1);
    assert_eq!(full.source_id, SOURCE_ID);
    assert_eq!(full.method, METHOD_VWAP);
    assert_eq!(full.quality_flags, 0);
    assert_eq!(full.input_timestamp_ns, 1_700_000_000_000_000_001);
    assert_eq!(full.compute_timestamp_ns, 1_700_000_000_000_000_009);
    assert_eq!(full.price, 10_025);
    assert_eq!(full.window_ms, 60_000);
    assert_eq!(full.volume, 2_500);

    // An empty window carries the price over, and says so.
    let empty = sent[1];
    assert_eq!(empty.volume, 0);
    assert_eq!(empty.price, 10_025);
    assert_eq!(empty.quality_flags, QUALITY_STALE | QUALITY_SYNTHETIC);

    // The definition carries the same `Source ID` the midpoint does, which the
    // specification requires.
    let definitions: Vec<InstrumentDefinition> = h
        .refdata()
        .messages()
        .iter()
        .filter(|(type_id, _)| *type_id == DEFINITION)
        .map(|(_, bytes)| InstrumentDefinition::decode(bytes, 3).expect("composed"))
        .collect();
    assert!(!definitions.is_empty());
    for definition in &definitions {
        assert_eq!(definition.instrument_id, 1);
        assert_eq!(definition.source_id, SOURCE_ID);
        assert_eq!(definition.price_exponent, -2);
        assert_eq!(definition.qty_exponent, -3);
    }

    // Numbered from zero in the midpoint feed's own era.
    for (index, (sequence, era)) in h.mktdata().headers().iter().enumerate() {
        assert_eq!(*sequence, index as u64);
        assert_eq!(*era, MID_ERA);
    }
    assert_eq!(h.publisher.refusals().total(), 0);
    assert_eq!(h.publisher.unroutable(), 0);
}

#[test]
fn a_vwap_has_nowhere_to_go_without_a_midpoint_feed() {
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.publisher.arrive(vwap(instrument, "1", false));
    assert_eq!(h.publisher.unroutable(), 1);
    assert!(h.mktdata().datagrams().is_empty());
}

#[test]
fn a_quote_has_nowhere_to_go_on_a_midpoint_feed() {
    // Both are `0x03`, so a quote that reached this feed would decode as a
    // midpoint.
    let mut h = harness(midpoint_feed());
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.publisher.arrive(harness::quote(instrument, 1));
    assert_eq!(h.publisher.unroutable(), 1);
    assert!(h.mktdata().datagrams().is_empty());
}

#[test]
fn a_value_the_exponent_cannot_state_is_refused() {
    let mut h = harness(midpoint_feed());
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.publisher.arrive(vwap(instrument, "2.5005", false));
    assert_eq!(h.publisher.refusals().total(), 1);
    assert!(midpoints(&h).is_empty());
}

/// One instrument that pays on a strike.
struct Struck;

impl Adapter for Struck {
    fn message_types(&self) -> &[&'static str] {
        &[]
    }

    fn poll_listings(&mut self, out: &mut dyn ListingSink) {
        if let Some(handle) = out.list(&spec("STRUCK")) {
            out.strike(handle, &Strike::Pending);
        }
    }

    fn on_payload(
        &mut self,
        _payload: &Payload<'_>,
        _out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        Ok(())
    }
}

#[test]
fn a_definition_with_a_strike_is_sent_alone_on_a_feed_that_carries_no_strike() {
    // The midpoint feed does not carry `StrikeInterval`, and the builder
    // refuses it. Sending the pair would lose the definition with it.
    let mut h = harness(midpoint_feed());
    assert!(h.publisher.poll_listings(&mut Struck));
    lap(&mut h);

    let type_ids = h.refdata().type_ids();
    assert!(type_ids.contains(&DEFINITION), "{type_ids:?}");
    assert!(!type_ids.contains(&STRIKE), "{type_ids:?}");
}
