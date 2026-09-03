//! The proof the wiring exists: a fake adapter's events reach a fake
//! `DatagramSink` as datagrams.
//!
//! Everything else in this crate's tests asserts one piece. This asserts the
//! path — an instrument offered through [`ListingSink`], a normalized event
//! emitted through [`EventSink`], and bytes coming out of a
//! [`DatagramSink`](dz_publisher_egress::DatagramSink) that decode as the
//! specification's own messages, with the fields the venue stated, scaled at
//! the instrument's own exponents, numbered, in an era, on the port role the
//! specification allows.
//!
//! **Every expected value here is transcribed by hand** — from the design, from
//! the specifications' message tables, or from the arithmetic the instrument's
//! exponents imply. None of it is read off the code under test, which is what
//! makes it evidence rather than a restatement.

mod harness;

use dz_adapter_core::EventSink;
use dz_edge_refdata::InstrumentDefinition;
use dz_edge_tob::{Quote, Trade};
use dz_publisher_runtime::Exit;
use harness::{depth_feed, feed, harness, FakeAdapter, SOURCE_ID};

// The wire values, transcribed from the top-of-book specification's own tables.
// `dz-edge-tob` exports each of these as a constant; the literals are written
// out here so that a test failure means the encoder disagrees with the
// specification rather than that it agrees with itself.
const QUOTE_BID_UPDATED: u8 = 0x01;
const QUOTE_ASK_UPDATED: u8 = 0x02;
const QUOTE_BID_GONE: u8 = 0x04;
const QUOTE_ASK_GONE: u8 = 0x08;
const AGGRESSOR_BUY: u8 = 1;

#[test]
fn a_fake_adapters_events_reach_a_fake_datagram_sink_as_datagrams() {
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["A-B", "C-D"]);

    // 1. The adapter offers instruments and gets handles. It names no
    //    `Instrument ID` and cannot: the handle is a dense index the runtime
    //    minted, and the wire id is the reference-data owner's.
    h.publisher.poll_listings(&mut adapter);
    assert_eq!(adapter.handles().len(), 2);
    assert_eq!(adapter.declined(), 0);
    let first = adapter.handles()[0];
    let second = adapter.handles()[1];

    // 2. The adapter emits normalized events, in the venue's own decimal text.
    h.publisher.upstream_message("quote");
    h.publisher
        .event(harness::quote(first, 1_700_000_000_000_000_001));
    h.publisher.upstream_message("trade");
    h.publisher
        .event(harness::trade(second, 1_700_000_000_000_000_002));
    h.publisher.upstream_message("quote");
    h.publisher
        .event(harness::one_sided_quote(second, 1_700_000_000_000_000_003));

    // 3. Ticks, so the definition cycle and the manifest reach the refdata port
    //    too. Two of them, and the reason is the pacer's: the first tick with a
    //    published set starts the lap and owes nothing, which is what makes a
    //    long idle period safe — a publisher whose venue listed nothing for an
    //    hour must not owe the whole set the moment something appears.
    let _ = h.publisher.tick();
    h.clock.advance(std::time::Duration::from_secs(20));
    let _ = h.publisher.tick();

    // ---- what reached the mktdata port ----
    let mktdata = h.mktdata().messages();
    let quotes: Vec<Quote> = mktdata
        .iter()
        .filter(|(type_id, _)| *type_id == 0x03)
        .map(|(_, bytes)| Quote::decode(bytes).expect("this publisher composed it"))
        .collect();
    let trades: Vec<Trade> = mktdata
        .iter()
        .filter(|(type_id, _)| *type_id == 0x04)
        .map(|(_, bytes)| Trade::decode(bytes).expect("composed"))
        .collect();
    assert_eq!(quotes.len(), 2, "both quotes reached the wire");
    assert_eq!(trades.len(), 1);

    // The two-sided quote. `Instrument ID`s are minted from 1 in offer order,
    // which is the reference-data owner's own rule.
    let two_sided = quotes[0];
    assert_eq!(two_sided.instrument_id, 1);
    assert_eq!(two_sided.source_id, SOURCE_ID);
    assert_eq!(two_sided.source_timestamp_ns, 1_700_000_000_000_000_001);
    // Both sides present, so both *updated* bits and neither *gone* bit. The
    // byte is derived from the pair of sides above this boundary and an adapter
    // cannot author one.
    assert_eq!(
        two_sided.update_flags,
        QUOTE_BID_UPDATED | QUOTE_ASK_UPDATED
    );
    // Scaled at the instrument's exponents: price -2, quantity -3. Transcribed
    // arithmetic, and a transposition of the two exponents would fail here.
    assert_eq!(two_sided.bid_price, 10_025);
    assert_eq!(two_sided.bid_qty, 2_500);
    assert_eq!(two_sided.ask_price, 10_075);
    assert_eq!(two_sided.ask_qty, 1_250);
    // Zero is this field's "the venue does not say", and the opposite value
    // from the depth feed's sentinel for the same question.
    assert_eq!(two_sided.bid_source_count, 0);
    assert_eq!(two_sided.ask_source_count, 0);

    // The one-sided quote: the ask's *gone* bit, and zeros on that side, which
    // the specification requires. Zero is an in-range price on the wire, so the
    // flag is what says the zeros mean nothing.
    let one_sided = quotes[1];
    assert_eq!(one_sided.instrument_id, 2);
    assert_eq!(one_sided.update_flags, QUOTE_BID_UPDATED | QUOTE_ASK_GONE);
    assert_eq!(one_sided.ask_price, 0);
    assert_eq!(one_sided.ask_qty, 0);
    assert_eq!(one_sided.bid_price, 10_025);
    // Never both bits of one side: they are mutually exclusive, which is what
    // two `SideUpdate` cases buys.
    assert_eq!(
        one_sided.update_flags & (QUOTE_BID_UPDATED | QUOTE_BID_GONE),
        QUOTE_BID_UPDATED
    );
    assert_ne!(
        one_sided.update_flags & QUOTE_ASK_UPDATED,
        QUOTE_ASK_UPDATED
    );

    // The trade.
    let trade = trades[0];
    assert_eq!(trade.instrument_id, 2);
    assert_eq!(trade.source_id, SOURCE_ID);
    assert_eq!(trade.aggressor_side, AGGRESSOR_BUY);
    assert_eq!(trade.trade_price, 10_050);
    assert_eq!(trade.trade_qty, 750);
    assert_eq!(trade.trade_id, 987_654);
    // The specification's sentinel for a running total the venue does not
    // publish.
    assert_eq!(trade.cumulative_volume, 0);
    assert_eq!(trade.trade_flags, 0);

    // ---- what reached the refdata port ----
    let refdata = h.refdata().messages();
    let definitions: Vec<InstrumentDefinition> = refdata
        .iter()
        .filter(|(type_id, _)| *type_id == 0x02)
        // Schema 3, which is the one generation this build emits: a publisher
        // speaks one, and a mixture would make the version byte meaningless.
        .map(|(_, bytes)| InstrumentDefinition::decode(bytes, 3).expect("composed"))
        .collect();
    assert!(
        !definitions.is_empty(),
        "no definition reached the refdata port, so no `Instrument ID` on the \
         mktdata port resolves to anything"
    );
    for definition in &definitions {
        assert!(matches!(definition.instrument_id, 1 | 2));
        assert_eq!(definition.source_id, SOURCE_ID);
        // The exponents the venue stated, published so a subscriber can
        // interpret every price and quantity above.
        assert_eq!(definition.price_exponent, -2);
        assert_eq!(definition.qty_exponent, -3);
    }

    // ---- and the datagrams themselves ----
    // Numbered densely from zero in the era the store handed out, per channel
    // instance, on each port role independently.
    for (index, (sequence, era)) in h.mktdata().headers().iter().enumerate() {
        assert_eq!(*sequence, index as u64);
        assert_eq!(*era, 2);
    }
    for (index, (sequence, era)) in h.refdata().headers().iter().enumerate() {
        assert_eq!(*sequence, index as u64);
        assert_eq!(*era, 2);
    }
    // Under the mandated cap, which is the builder's and not a configured
    // value: 1,232 bytes, to leave room for GRE encapsulation.
    for datagram in h
        .mktdata()
        .datagrams()
        .iter()
        .chain(h.refdata().datagrams().iter())
    {
        assert!(datagram.len() <= 1232, "{} bytes", datagram.len());
    }

    // Nothing was refused, and nothing was dropped for want of a feed.
    assert_eq!(h.publisher.refusals().total(), 0);
    assert_eq!(h.publisher.unroutable(), 0);
}

#[test]
fn the_egress_series_move_without_anyone_having_thought_about_them() {
    // The enforcement claim, tested: a publisher transmitting through
    // `dz-publisher-egress` emits `dz_publisher_egress_*` whether or not anyone
    // remembered to. Nothing in this test records a metric.
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    h.publisher.event(harness::quote(adapter.handles()[0], 1));
    let _ = h.publisher.tick();
    h.clock.advance(std::time::Duration::from_secs(20));
    let _ = h.publisher.tick();

    let exposition = h.metrics.render();
    for expected in [
        "dz_publisher_egress_datagrams_total",
        "dz_publisher_egress_messages_total",
        "dz_publisher_egress_bytes_total",
        "dz_publisher_egress_sequence_current",
        "dz_publisher_refdata_instruments_current",
        "dz_publisher_refdata_manifest_valid",
    ] {
        assert!(exposition.contains(expected), "{expected} is missing");
    }
    // The venue label, applied by the registry's constructor. There is no path
    // to a series without it.
    assert!(exposition.contains("venue=\"a-venue\""));
    assert!(exposition.contains(&format!("source_id=\"{SOURCE_ID}\"")));
    // A quote counted on the mktdata port role, by message type.
    assert!(
        exposition.lines().any(|line| {
            line.contains("egress_messages_total")
                && line.contains("message_type=\"quote\"")
                && line.contains("port_role=\"mktdata\"")
                && line.trim_end().ends_with(" 1")
        }),
        "{exposition}"
    );
}

#[test]
fn a_refused_scaling_drops_one_message_and_leaves_the_publisher_running() {
    // The refusal that matters most, because the alternative shipped: a
    // publisher whose live path rounds through `f64` takes the failure as zero
    // and puts a real-looking bid at nothing on the wire, with the side flagged
    // present.
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.publisher.event(harness::too_precise_quote(instrument, 1));
    assert_eq!(h.publisher.refusals().too_precise, 1);
    assert_eq!(h.publisher.refusals().total(), 1);
    assert!(
        !h.mktdata().type_ids().contains(&0x03),
        "a quote the exponent cannot state exactly reached the wire"
    );

    // And the next one goes out, which is the half that says a single
    // instrument's wrong exponent must not darken a feed.
    h.publisher.event(harness::quote(instrument, 2));
    let quotes: Vec<Quote> = h
        .mktdata()
        .messages()
        .iter()
        .filter(|(type_id, _)| *type_id == 0x03)
        .map(|(_, bytes)| Quote::decode(bytes).expect("composed"))
        .collect();
    assert_eq!(quotes.len(), 1);
    assert_eq!(quotes[0].bid_price, 10_025);
}

#[test]
fn an_event_no_enabled_feed_carries_is_dropped_without_spending_a_sequence_number() {
    // This publisher emits top-of-book only, so a depth event has nowhere to
    // go — and the *order* of the two things it does about that is the point:
    // counted and dropped **before** the lowering, so no `Per-Instrument Seq`
    // is stamped. A number spent on a message that never reached the wire is a
    // gap every subscriber reads as packet loss.
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    for step in 0..4 {
        h.publisher.event(harness::bid_level(instrument, step));
    }
    h.publisher.event(harness::clear(instrument, 5));
    assert_eq!(h.publisher.unroutable(), 5);
    assert_eq!(h.publisher.refusals().total(), 0, "not a lowering refusal");
    assert!(h.mktdata().datagrams().is_empty());

    // The counter the depth lowering carries has not moved, which is the thing
    // a depth feed would inherit.
    assert_eq!(
        h.publisher
            .depth_lowering_mut()
            .sequence_mut()
            .last(instrument),
        0
    );
}

#[test]
fn a_quote_has_nowhere_to_go_on_a_publisher_that_emits_only_depth() {
    // The same rule from the other side. A top-of-book event on a depth-only
    // publisher is not a defect in the adapter — a venue's adapter emits what
    // its upstream says, and which feeds this process publishes is
    // configuration — so it is counted and dropped rather than refused loudly.
    let mut h = harness(depth_feed());
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.publisher.event(harness::quote(instrument, 1));
    assert_eq!(h.publisher.unroutable(), 1);
    assert!(!h.mktdata().type_ids().contains(&0x03));
}

#[test]
fn an_event_naming_a_withdrawn_instrument_is_refused_rather_than_republished() {
    // An `InstrumentRef` is a handle and not a capability: it carries no proof
    // of its own origin and it can outlive its instrument's withdrawal. Slots
    // are never reused, so the refusal is countable instead of the event being
    // published under whichever `Instrument ID` moved into that slot.
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["A-B", "C-D"]);
    h.publisher.poll_listings(&mut adapter);
    let withdrawn = adapter.handles()[0];

    // The venue delists the first instrument, through the boundary, on the next
    // poll.
    let mut shorter = FakeAdapter::new(&["C-D"]);
    shorter.withdraw(withdrawn);
    h.clock.advance(dz_publisher_runtime::LISTING_POLL);
    h.publisher.poll_listings(&mut shorter);
    assert_eq!(h.publisher.refdata().published(), 1);

    h.publisher.event(harness::quote(withdrawn, 9));
    assert_eq!(h.publisher.refusals().unknown_instrument, 1);
    assert!(!h.mktdata().type_ids().contains(&0x03));
}

#[test]
fn a_signal_shuts_the_composed_publisher_down_cleanly() {
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    h.publisher.event(harness::quote(adapter.handles()[0], 1));
    let teardown = h.publisher.shut_down(Exit::Signal);
    assert_eq!(teardown.steps().len(), 6);
    assert_eq!(h.mktdata().type_ids().last(), Some(&0x06));
}
