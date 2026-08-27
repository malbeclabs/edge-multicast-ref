//! Every metric family in the normative set must render under its exact
//! name. This is the test that stops a rename: change any name below in the
//! implementation without updating this list, and this test fails.

use dz_edge_core::PortRole;
use dz_publisher_metrics::{
    EgressErrorReason, EventKind, ExitReason, InconsistencyKind, ParseErrorReason,
    PublisherMetrics, ReconnectReason, RecoveryOutcome, RefdataLoadErrorReason, TimestampKind,
};

const NORMATIVE_NAMES: &[&str] = &[
    "dz_publisher_ingress_messages_total",
    "dz_publisher_ingress_bytes_total",
    "dz_publisher_ingress_duplicates_total",
    "dz_publisher_ingress_parse_errors_total",
    "dz_publisher_ingress_connection_state",
    "dz_publisher_ingress_reconnects_total",
    "dz_publisher_ingress_rate_limited_total",
    "dz_publisher_book_updates_total",
    "dz_publisher_book_inconsistency_total",
    "dz_publisher_book_recovery_total",
    "dz_publisher_instruments_tracked",
    "dz_publisher_instruments_published",
    "dz_publisher_refdata_definitions_emitted_total",
    "dz_publisher_refdata_instruments_current",
    "dz_publisher_refdata_load_duration_seconds",
    "dz_publisher_refdata_load_errors_total",
    "dz_publisher_refdata_last_refresh_timestamp_seconds",
    "dz_publisher_refdata_new_listings_total",
    "dz_publisher_refdata_delistings_total",
    "dz_publisher_refdata_manifest_seq",
    "dz_publisher_refdata_manifest_valid",
    "dz_publisher_egress_datagrams_total",
    "dz_publisher_egress_messages_total",
    "dz_publisher_egress_bytes_total",
    "dz_publisher_egress_errors_total",
    "dz_publisher_egress_sequence_current",
    "dz_publisher_egress_heartbeat_last_sent_timestamp_seconds",
    "dz_publisher_venue_to_recv_latency_seconds",
    "dz_publisher_venue_timestamps_available",
    "dz_publisher_recv_to_send_latency_seconds",
    "dz_publisher_book_update_duration_seconds",
    "dz_publisher_encode_duration_seconds",
    "dz_publisher_build_info",
    "dz_publisher_uptime_seconds",
    "dz_publisher_idle_guard_last_update_timestamp_seconds",
    "dz_publisher_exit_reason_total",
];

fn touch_every_family(m: &PublisherMetrics) {
    m.ingress().message("trade", "primary");
    m.ingress().bytes(1);
    m.ingress().duplicate();
    m.ingress().parse_error(ParseErrorReason::Malformed);
    m.ingress().set_connection_state("primary", true);
    m.ingress().reconnect(ReconnectReason::Timeout);
    m.ingress().rate_limited();

    m.book().update();
    m.book().inconsistency(InconsistencyKind::SequenceGap);
    m.book().recovery(RecoveryOutcome::Success);
    m.book().set_instruments_tracked(1);
    m.book().set_instruments_published(1);

    m.refdata().definition_emitted();
    m.refdata().set_instruments_current(1);
    m.refdata().observe_load_duration(0.5);
    m.refdata().load_error(RefdataLoadErrorReason::Timeout);
    m.refdata().set_last_refresh_timestamp(1.0);
    m.refdata().new_listing();
    m.refdata().delisting();
    m.refdata().set_manifest_seq(1, 1);
    m.refdata().set_manifest_valid(1, true);

    m.egress().datagram(PortRole::Mktdata);
    m.egress().message(PortRole::Mktdata, "trade");
    m.egress().bytes(PortRole::Mktdata, 1);
    m.egress()
        .error(PortRole::Mktdata, EgressErrorReason::SocketError);
    m.egress().set_sequence(PortRole::Mktdata, 1, 1);
    m.egress().set_heartbeat_last_sent(1, 1.0);

    m.latency()
        .observe_venue_to_recv(TimestampKind::ExchangeRecv, 0.001);
    m.latency().set_venue_timestamps_available(1);
    m.latency()
        .observe_recv_to_send(EventKind::BookUpdate, 0.001);
    m.latency().observe_book_update_duration(0.001);
    m.latency().observe_encode_duration("trade", 0.001);

    m.process().set_build_info("1.0.0", "abc123", "1.88.0");
    m.process().set_uptime(1.0);
    m.process().set_idle_guard_last_update(1.0);
    m.process().exit(ExitReason::Signal);
}

#[test]
fn every_normative_family_renders_under_its_exact_name() {
    let metrics = PublisherMetrics::new("test-venue", 7, &[PortRole::Mktdata]);
    touch_every_family(&metrics);
    let rendered = metrics.render();

    for name in NORMATIVE_NAMES {
        assert!(
            rendered.contains(&format!("# TYPE {name} ")),
            "missing normative metric family {name} in:\n{rendered}"
        );
    }
}
