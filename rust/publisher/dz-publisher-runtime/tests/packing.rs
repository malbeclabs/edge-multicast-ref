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

#[test]
fn a_tick_that_sends_a_late_batch_sends_no_heartbeat_behind_it() {
    let mut h = depth();
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.publisher.event(harness::bid_level(instrument, 1));
    h.clock.advance(Duration::from_secs(2));
    let _ = h.publisher.tick();

    assert_eq!(h.mktdata().type_ids(), vec![0x40]);
}

#[test]
fn a_datagram_that_never_left_is_not_measured() {
    let mut h = depth();
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.only().mktdata_refusal.set(true);
    h.only().reference_refusal.set(true);
    // The first refused send drops every member; the next finds none live.
    h.publisher.event(harness::bid_level(instrument, 0));
    h.publisher.drained();
    h.publisher.payload_scope(Some(h.clock.unix_ns()));
    h.publisher.event(harness::bid_level(instrument, 1));
    h.publisher.payload_scope(None);
    h.publisher.drained();

    let exposition = h.metrics.render();
    assert_eq!(
        rendered(
            &exposition,
            "dz_publisher_recv_to_send_latency_seconds_count"
        ),
        0.0
    );
}

#[test]
fn a_datagram_the_mtu_sent_is_measured_when_it_left() {
    let mut h = depth();
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.publisher.payload_scope(Some(h.clock.unix_ns()));
    let mut total = 0;
    while h.mktdata().len() == 0 {
        h.publisher.event(harness::bid_level(instrument, total));
        total += 1;
    }
    h.publisher.payload_scope(None);
    let first = h.mktdata().messages().len() as u64;
    h.clock.advance(Duration::from_millis(7));
    h.publisher.drained();

    let exposition = h.metrics.render();
    assert_eq!(
        rendered(
            &exposition,
            "dz_publisher_recv_to_send_latency_seconds_count"
        ),
        total as f64
    );
    let sum = rendered(&exposition, "dz_publisher_recv_to_send_latency_seconds_sum");
    let expected = (total - first) as f64 * 0.007;
    assert!(
        (sum - expected).abs() < 1e-9,
        "measured {sum}s, not 0 for the {first} the MTU sent and 7ms for the rest"
    );
}

#[test]
fn a_top_of_book_quote_is_published_and_measured_at_its_send() {
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.publisher.payload_scope(Some(h.clock.unix_ns()));
    h.publisher.event(harness::quote(instrument, 1));
    h.publisher.payload_scope(None);

    let exposition = h.metrics.render();
    assert_eq!(
        rendered(
            &exposition,
            "dz_publisher_recv_to_send_latency_seconds_count"
        ),
        1.0
    );
    assert!(
        rendered(
            &exposition,
            "dz_publisher_channel_last_published_timestamp_seconds"
        ) > 0.0
    );
}
