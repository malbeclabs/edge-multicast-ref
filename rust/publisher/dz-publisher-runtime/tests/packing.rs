//! The live path packs what arrived together and sends it when the input drains.

mod harness;

use std::time::Duration;

use dz_adapter_core::EventSink;
use dz_edge_core::Datagram;
use dz_edge_mbp::MAGIC_MBP;
use dz_publisher_refdata::Clock as _;
use harness::{depth_feed, feed, harness, FakeAdapter};

fn depth() -> harness::Harness {
    harness(depth_feed())
}

fn send_timestamps(recorder: &harness::Recorder) -> Vec<u64> {
    recorder
        .datagrams()
        .iter()
        .map(|datagram| {
            Datagram::decode(datagram, MAGIC_MBP)
                .expect("composed")
                .header()
                .send_timestamp_ns
        })
        .collect()
}

fn rendered(exposition: &str, prefix: &str) -> f64 {
    exposition
        .lines()
        .filter(|line| line.starts_with(prefix))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

#[test]
fn events_that_arrived_together_leave_as_one_datagram_when_the_input_drains() {
    let mut h = depth();
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    for step in 0..5 {
        h.publisher.event(harness::bid_level(instrument, step));
    }
    assert_eq!(
        h.mktdata().len(),
        0,
        "nothing leaves before the input drains"
    );

    h.publisher.drained();
    assert_eq!(h.mktdata().len(), 1);
    assert_eq!(h.mktdata().messages().len(), 5);
    assert_eq!(h.mktdata().headers(), vec![(0, h.mktdata().headers()[0].1)]);

    h.publisher.drained();
    assert_eq!(
        h.mktdata().len(),
        1,
        "a drain with nothing packed sends nothing"
    );
}

#[test]
fn the_send_timestamp_is_the_flush_and_not_the_first_message() {
    let mut h = depth();
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.publisher.event(harness::bid_level(instrument, 1));
    h.clock.advance(Duration::from_millis(3));
    h.publisher.event(harness::bid_level(instrument, 2));
    h.clock.advance(Duration::from_millis(2));
    let flushed_at = h.clock.unix_ns();
    h.publisher.drained();

    assert_eq!(send_timestamps(h.mktdata()), vec![flushed_at]);
}

#[test]
fn the_tick_sends_what_the_input_never_drained_on() {
    let mut h = depth();
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.publisher.event(harness::bid_level(instrument, 1));
    let _ = h.publisher.tick();

    assert_eq!(h.mktdata().messages().len(), 1);
}

#[test]
fn a_snapshot_is_anchored_after_what_was_packed_has_left() {
    let mut h = depth();
    let mut adapter =
        FakeAdapter::new(&["A-B"]).with_book(&[(dz_adapter_core::Side::Bid, "100.25", "2.500")]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    for step in 0..3 {
        h.publisher.event(harness::bid_level(instrument, step));
    }
    let framed = h.publisher.snapshot(&adapter, instrument).expect("framed");

    assert_eq!(h.mktdata().len(), 1);
    assert_eq!(h.mktdata().headers()[0].0, 0);
    assert_eq!(framed.begin.anchor_seq, 1);
    assert_eq!(framed.begin.last_instrument_seq, 3);
}

#[test]
fn recv_to_send_is_measured_to_the_flush() {
    let mut h = depth();
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    let recv_ts_ns = h.clock.unix_ns();
    h.publisher.payload_scope(Some(recv_ts_ns));
    h.publisher.event(harness::bid_level(instrument, 1));
    h.publisher.payload_scope(None);
    h.clock.advance(Duration::from_millis(7));
    h.publisher.drained();

    let exposition = h.metrics.render();
    assert_eq!(
        rendered(
            &exposition,
            "dz_publisher_recv_to_send_latency_seconds_count"
        ),
        1.0
    );
    let sum = rendered(&exposition, "dz_publisher_recv_to_send_latency_seconds_sum");
    assert!(
        (sum - 0.007).abs() < 1e-9,
        "measured {sum}s, not the 7ms to the flush"
    );
}

#[test]
fn a_top_of_book_quote_leaves_without_waiting_for_the_drain() {
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.publisher.event(harness::quote(instrument, 1));
    h.publisher.event(harness::quote(instrument, 2));

    assert_eq!(h.mktdata().len(), 2);
}
