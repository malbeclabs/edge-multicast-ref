//! The two families that measure from a payload's arrival.
//!
//! Both existed, were pre-created, and could never move: `EventSink` did not
//! carry the receive stamp of the payload an event came from, and nothing
//! declared which venue clock a `source_ts_ns` was read off. A histogram that
//! can never be written to is indistinguishable from a publisher that stopped.
//!
//! What closed it is a scope the **transport** opens and closes around the
//! adapter's mapping, so there is nothing for an adapter to pass through and
//! therefore nothing to forget. These tests state the scope the way the driver
//! does, and assert what the runtime then observes.

mod harness;

use dz_adapter_core::{EventSink, VenueTimestampKind};
use harness::{feed, harness, FakeAdapter};

/// One millisecond from the venue's stamp to ours, and one more to the send.
const SOURCE_TS_NS: u64 = 1_700_000_000_000_000_000;
const RECV_TS_NS: u64 = SOURCE_TS_NS + 1_000_000;

/// An adapter that declares a venue clock, since the default is to declare
/// none.
struct Declaring(FakeAdapter);

impl dz_adapter_core::Adapter for Declaring {
    fn message_types(&self) -> &[&'static str] {
        self.0.message_types()
    }
    fn poll_listings(&mut self, out: &mut dyn dz_adapter_core::ListingSink) {
        self.0.poll_listings(out);
    }
    fn on_payload(
        &mut self,
        payload: &dz_adapter_core::Payload<'_>,
        out: &mut dyn EventSink,
    ) -> Result<(), dz_adapter_core::ParseError> {
        self.0.on_payload(payload, out)
    }
    fn source_timestamp_kind(&self) -> Option<VenueTimestampKind> {
        Some(VenueTimestampKind::MatchingEngine)
    }
}

fn rendered_count(exposition: &str, prefix: &str) -> u64 {
    exposition
        .lines()
        .filter(|line| line.starts_with(prefix))
        .filter_map(|line| line.rsplit(' ').next())
        .filter_map(|value| value.parse::<f64>().ok())
        .map(|value| value as u64)
        .sum()
}

#[test]
fn an_event_that_arrived_in_a_payload_moves_both_families() {
    let mut h = harness(feed());
    let mut adapter = Declaring(FakeAdapter::new(&["ONE"]));
    h.publisher.declare_venue_timestamps(&adapter);
    h.publisher.poll_listings(&mut adapter);

    // The gauge that says a venue clock is available at all, which only a
    // declaration can set.
    assert_eq!(
        rendered_count(
            &h.metrics.render(),
            "dz_publisher_venue_timestamps_available"
        ),
        1,
        "the declaration must reach the gauge"
    );

    // The scope the driver's wrapper opens before the adapter is handed
    // anything, stated here the same way.
    h.publisher.payload_scope(Some(RECV_TS_NS));
    h.publisher.event(harness::quote(
        dz_adapter_core::InstrumentRef::from_admission(0),
        SOURCE_TS_NS,
    ));
    h.publisher.payload_scope(None);

    let exposition = h.metrics.render();
    assert_eq!(
        rendered_count(
            &exposition,
            "dz_publisher_venue_to_recv_latency_seconds_count"
        ),
        1,
        "the venue-to-receive family observed the arrival"
    );
    assert_eq!(
        rendered_count(
            &exposition,
            "dz_publisher_recv_to_send_latency_seconds_count"
        ),
        1,
        "and so did the receive-to-send one"
    );
    // Labelled with what the adapter declared, and not with another kind.
    // Labels render alphabetically alongside the constant ones, so the pair is
    // matched rather than the line's start.
    assert!(
        exposition.lines().any(|line| {
            line.starts_with("dz_publisher_venue_to_recv_latency_seconds_count")
                && line.contains(r#"timestamp_kind="matching_engine""#)
                && line.ends_with(" 1")
        }),
        "the observation carries the kind the adapter declared"
    );
    // And no other kind was observed, so the label is the adapter's statement
    // rather than a guess.
    assert!(
        exposition.lines().all(|line| {
            !line.starts_with("dz_publisher_venue_to_recv_latency_seconds_count")
                || line.contains(r#"timestamp_kind="matching_engine""#)
                || line.ends_with(" 0")
        }),
        "another kind was observed"
    );
}

#[test]
fn an_event_that_came_from_no_payload_moves_neither() {
    // **The rule that keeps the number meaningful.** A snapshot pulled on the
    // runtime's own cadence, a definition from the refdata cycle and a
    // heartbeat never arrived from upstream — a latency measured for one would
    // be measuring this process against itself, and it would land in the same
    // histogram as the real observations.
    let mut h = harness(feed());
    let mut adapter = Declaring(FakeAdapter::new(&["ONE"]));
    h.publisher.declare_venue_timestamps(&adapter);
    h.publisher.poll_listings(&mut adapter);

    // No scope stated, which is what the runtime's own paths look like.
    h.publisher.event(harness::quote(
        dz_adapter_core::InstrumentRef::from_admission(0),
        SOURCE_TS_NS,
    ));

    let exposition = h.metrics.render();
    assert_eq!(
        rendered_count(
            &exposition,
            "dz_publisher_venue_to_recv_latency_seconds_count"
        ),
        0
    );
    assert_eq!(
        rendered_count(
            &exposition,
            "dz_publisher_recv_to_send_latency_seconds_count"
        ),
        0
    );
}

#[test]
fn a_scope_that_closed_does_not_attribute_the_next_event() {
    // The scope is withdrawn on drop of the wrapper, so an event produced
    // outside one - by any path that is not a payload being mapped - is not
    // attributed to whatever arrived last.
    let mut h = harness(feed());
    let mut adapter = Declaring(FakeAdapter::new(&["ONE"]));
    h.publisher.declare_venue_timestamps(&adapter);
    h.publisher.poll_listings(&mut adapter);
    let one = dz_adapter_core::InstrumentRef::from_admission(0);

    h.publisher.payload_scope(Some(RECV_TS_NS));
    h.publisher.event(harness::quote(one, SOURCE_TS_NS));
    h.publisher.payload_scope(None);
    h.publisher.event(harness::quote(one, SOURCE_TS_NS));

    assert_eq!(
        rendered_count(
            &h.metrics.render(),
            "dz_publisher_recv_to_send_latency_seconds_count"
        ),
        1,
        "only the one inside the scope"
    );
}

#[test]
fn a_venue_that_declares_no_clock_leaves_the_venue_family_alone() {
    // `None` is a real answer, not a missing one — and the receive-to-send half
    // is still observable, because it needs no venue clock at all.
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["ONE"]);
    h.publisher.declare_venue_timestamps(&adapter);
    h.publisher.poll_listings(&mut adapter);

    assert_eq!(
        rendered_count(
            &h.metrics.render(),
            "dz_publisher_venue_timestamps_available"
        ),
        0
    );

    h.publisher.payload_scope(Some(RECV_TS_NS));
    h.publisher.event(harness::quote(
        dz_adapter_core::InstrumentRef::from_admission(0),
        SOURCE_TS_NS,
    ));
    h.publisher.payload_scope(None);

    let exposition = h.metrics.render();
    assert_eq!(
        rendered_count(
            &exposition,
            "dz_publisher_venue_to_recv_latency_seconds_count"
        ),
        0,
        "an observation with no kind to label it is not made"
    );
    assert_eq!(
        rendered_count(
            &exposition,
            "dz_publisher_recv_to_send_latency_seconds_count"
        ),
        1,
        "this half never needed one"
    );
}
