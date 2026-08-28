//! Every family whose labels are entirely a closed set (an enum, or the
//! port roles a publisher was constructed with) must render at 0 the
//! instant `PublisherMetrics::new` returns, before anything has been
//! recorded. A dashboard panel or a `== 0` alert on one of these series
//! must see an actual zero, not "no data" - and an operator must be able
//! to tell "this publisher has sent nothing yet" from "this publisher
//! does not implement this metric".
//!
//! Families whose labels are open-ended (the upstream source's own
//! `message_type` vocabulary, a deployment-chosen `connection` or
//! `channel_id`, or caller-supplied `build_info` values) are the
//! deliberate opposite: they must stay absent until first touched, so
//! their absence keeps meaning something.

use dz_edge_core::PortRole;
use dz_publisher_metrics::{PublisherMetrics, PublisherMetricsConfig};

/// Finds the sample line for `metric` (an exact family name, or a
/// histogram sub-series such as `..._count`) carrying every label in
/// `labels`, regardless of the order Prometheus renders labels in.
fn find_sample<'a>(rendered: &'a str, metric: &str, labels: &[(&str, &str)]) -> &'a str {
    rendered
        .lines()
        .find(|line| {
            line.strip_prefix(metric)
                .is_some_and(|rest| rest.starts_with('{'))
                && labels
                    .iter()
                    .all(|(k, v)| line.contains(&format!("{k}=\"{v}\"")))
        })
        .unwrap_or_else(|| {
            panic!("no sample line for {metric} with labels {labels:?} in:\n{rendered}")
        })
}

fn assert_zero(rendered: &str, metric: &str, labels: &[(&str, &str)]) {
    let line = find_sample(rendered, metric, labels);
    assert!(
        line.ends_with(" 0"),
        "expected {metric}{labels:?} to render 0 at construction, got: {line}"
    );
}

#[test]
fn every_closed_label_family_renders_zero_at_construction() {
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata, PortRole::Refdata],
        connections: &[],
        channel_ids: &[],
    });
    let rendered = metrics.render();

    for reason in ["schema", "unknown_field", "malformed", "truncated"] {
        assert_zero(
            &rendered,
            "dz_publisher_ingress_parse_errors_total",
            &[("reason", reason)],
        );
    }

    for reason in ["timeout", "remote_close", "rate_limit", "auth_expired"] {
        assert_zero(
            &rendered,
            "dz_publisher_ingress_reconnects_total",
            &[("reason", reason)],
        );
    }

    for kind in [
        "missing_level",
        "crossed_book",
        "snapshot_mismatch",
        "sequence_gap",
    ] {
        assert_zero(
            &rendered,
            "dz_publisher_book_inconsistency_total",
            &[("kind", kind)],
        );
    }

    for outcome in ["success", "failed"] {
        assert_zero(
            &rendered,
            "dz_publisher_book_recovery_total",
            &[("outcome", outcome)],
        );
    }

    for reason in ["timeout", "rate_limit", "schema", "unavailable"] {
        assert_zero(
            &rendered,
            "dz_publisher_refdata_load_errors_total",
            &[("reason", reason)],
        );
    }

    for reason in ["idle_guard", "consistency_guard", "signal", "panic"] {
        assert_zero(
            &rendered,
            "dz_publisher_exit_reason_total",
            &[("reason", reason)],
        );
    }

    for port_role in ["mktdata", "refdata"] {
        assert_zero(
            &rendered,
            "dz_publisher_egress_datagrams_total",
            &[("port_role", port_role)],
        );
        assert_zero(
            &rendered,
            "dz_publisher_egress_bytes_total",
            &[("port_role", port_role)],
        );
        for reason in [
            "mtu_exceeded",
            "send_would_block",
            "socket_error",
            "not_registered",
            "wrong_port_role",
        ] {
            assert_zero(
                &rendered,
                "dz_publisher_egress_errors_total",
                &[("port_role", port_role), ("reason", reason)],
            );
        }
    }

    for timestamp_kind in [
        "exchange_recv",
        "matching_engine",
        "gateway_send",
        "block_time",
    ] {
        assert_zero(
            &rendered,
            "dz_publisher_venue_to_recv_latency_seconds_count",
            &[("timestamp_kind", timestamp_kind)],
        );
    }

    for event_kind in ["book_update", "trade"] {
        assert_zero(
            &rendered,
            "dz_publisher_recv_to_send_latency_seconds_count",
            &[("event_kind", event_kind)],
        );
    }
}

#[test]
fn egress_datagrams_total_only_covers_the_supplied_port_roles() {
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata, PortRole::Refdata],
        connections: &[],
        channel_ids: &[],
    });
    let rendered = metrics.render();

    for port_role in ["mktdata", "refdata"] {
        assert_zero(
            &rendered,
            "dz_publisher_egress_datagrams_total",
            &[("port_role", port_role)],
        );
    }

    let has_snapshot = rendered.lines().any(|line| {
        line.starts_with("dz_publisher_egress_datagrams_total{")
            && line.contains("port_role=\"snapshot\"")
    });
    assert!(
        !has_snapshot,
        "snapshot was not supplied to PublisherMetrics::new and must not appear:\n{rendered}"
    );
}

#[test]
fn open_label_families_are_absent_until_touched_then_present() {
    // What is left after the config makes every other label value
    // knowable: the upstream source's own message vocabulary, which only
    // that source can enumerate, and the build labels the caller supplies.
    const OPEN_LABEL_FAMILIES: &[&str] = &[
        "dz_publisher_ingress_messages_total",
        "dz_publisher_build_info",
    ];

    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata],
        connections: &["primary"],
        channel_ids: &[1],
    });
    let rendered = metrics.render();
    for name in OPEN_LABEL_FAMILIES {
        assert!(
            !rendered.contains(&format!("# TYPE {name} ")),
            "{name} is labelled by an open-ended value and must be absent until touched, \
             but rendered:\n{rendered}"
        );
    }

    metrics.ingress().message("trade", "primary");
    metrics
        .process()
        .set_build_info("1.0.0", "abc123", "1.88.0");

    let rendered = metrics.render();
    for name in OPEN_LABEL_FAMILIES {
        assert!(
            rendered.contains(&format!("# TYPE {name} ")),
            "{name} should render once touched, but did not appear in:\n{rendered}"
        );
    }
}

#[test]
fn declared_connections_render_at_zero_from_startup() {
    // The alert that means "my feed is down" is `== 0` on this series. A
    // publisher whose upstream never came up would never touch it, so
    // without pre-creation the series would not exist and the alert could
    // not fire in the one case it exists for.
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata],
        connections: &["primary", "backup"],
        channel_ids: &[],
    });
    let rendered = metrics.render();

    for connection in ["primary", "backup"] {
        assert_zero(
            &rendered,
            "dz_publisher_ingress_connection_state",
            &[("connection", connection)],
        );
    }
}

#[test]
fn declared_channel_ids_render_at_zero_from_startup() {
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata],
        connections: &[],
        channel_ids: &[0, 7],
    });
    let rendered = metrics.render();

    for channel_id in ["0", "7"] {
        assert_zero(
            &rendered,
            "dz_publisher_egress_sequence_current",
            &[("port_role", "mktdata"), ("channel_id", channel_id)],
        );
        assert_zero(
            &rendered,
            "dz_publisher_egress_heartbeat_last_sent_timestamp_seconds",
            &[("channel_id", channel_id)],
        );
        assert_zero(
            &rendered,
            "dz_publisher_refdata_manifest_seq",
            &[("channel_id", channel_id)],
        );
        assert_zero(
            &rendered,
            "dz_publisher_refdata_manifest_valid",
            &[("channel_id", channel_id)],
        );
    }
}

#[test]
fn egress_message_types_are_precreated_only_on_the_port_roles_that_permit_them() {
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata, PortRole::Refdata],
        connections: &[],
        channel_ids: &[],
    });
    let rendered = metrics.render();

    for message_type in ["heartbeat", "end_of_session", "quote", "trade"] {
        assert_zero(
            &rendered,
            "dz_publisher_egress_messages_total",
            &[("port_role", "mktdata"), ("message_type", message_type)],
        );
    }
    for message_type in ["instrument_definition", "manifest_summary"] {
        assert_zero(
            &rendered,
            "dz_publisher_egress_messages_total",
            &[("port_role", "refdata"), ("message_type", message_type)],
        );
    }

    // A quote on the refdata port is not a series that can ever be written
    // to, so pre-creation must not assert one.
    let has_impossible_pair = rendered.lines().any(|line| {
        line.starts_with("dz_publisher_egress_messages_total{")
            && line.contains("port_role=\"refdata\"")
            && line.contains("message_type=\"quote\"")
    });
    assert!(
        !has_impossible_pair,
        "the specification does not permit a quote on the refdata port:\n{rendered}"
    );
}

#[test]
fn every_egress_message_type_precreates_an_encode_duration_series() {
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata],
        connections: &[],
        channel_ids: &[],
    });
    let rendered = metrics.render();

    for message_type in [
        "heartbeat",
        "end_of_session",
        "quote",
        "trade",
        "instrument_definition",
        "manifest_summary",
    ] {
        assert_zero(
            &rendered,
            "dz_publisher_encode_duration_seconds_count",
            &[("message_type", message_type)],
        );
    }
}

#[test]
fn set_build_info_renders_the_gauge_at_one_with_its_labels() {
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[],
        connections: &[],
        channel_ids: &[],
    });
    metrics
        .process()
        .set_build_info("1.2.3", "deadbeef", "1.88.0");

    let rendered = metrics.render();
    let line = find_sample(
        &rendered,
        "dz_publisher_build_info",
        &[
            ("version", "1.2.3"),
            ("commit", "deadbeef"),
            ("toolchain", "1.88.0"),
        ],
    );
    assert!(line.ends_with(" 1"), "expected build_info to be 1: {line}");
}
