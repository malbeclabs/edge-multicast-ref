//! The ingress families, from the transport half to a registry.
//!
//! `dz-ingress-core` records every `dz_publisher_ingress_*` family through a
//! trait rather than a metrics client, so that a venue linking a transport does
//! not inherit a Prometheus client to be told a socket closed. This is the test
//! of the implementation the runtime owes it: every mapping is one-to-one by
//! name, and both taxonomies the boundary declares a second time are mapped by
//! exhaustive match.

mod harness;

use std::sync::Arc;

use dz_adapter_core::{AdapterError, DisconnectReason, ParseError};
use dz_ingress_core::IngressObserver;
use dz_publisher_metrics::{PublisherMetrics, PublisherMetricsConfig};
use dz_publisher_runtime::MetricsObserver;

fn metrics() -> Arc<PublisherMetrics> {
    Arc::new(PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "a-venue",
        source_id: harness::SOURCE_ID,
        port_roles: &[dz_edge_core::PortRole::Mktdata],
        connections: &["upstream"],
        channel_ids: &[harness::CHANNEL_ID],
        ingress_message_types: &["quote"],
    }))
}

/// Whether a rendered exposition carries a sample of `family` with every one of
/// `labels` and the value `value`.
fn sample(exposition: &str, family: &str, labels: &[&str], value: &str) -> bool {
    exposition.lines().any(|line| {
        line.starts_with(family)
            && labels.iter().all(|label| line.contains(label))
            && line.trim_end().ends_with(value)
    })
}

#[test]
fn every_parse_error_reason_reaches_the_series_it_is_the_label_of() {
    // The boundary's `ParseError` variants *are* the label values: an adapter
    // cannot fail to parse without the right series moving, and cannot invent a
    // fifth reason a dashboard has no panel for. `ParseError::ALL` is the
    // boundary's own list, so a variant added there without a mapping here
    // fails to compile in `observer.rs` and fails this test's count.
    let metrics = metrics();
    let observer = MetricsObserver::new(Arc::clone(&metrics));

    for error in ParseError::ALL {
        observer.parse_error(error);
    }

    let exposition = metrics.render();
    for reason in ["schema", "unknown_field", "malformed", "truncated"] {
        assert!(
            sample(
                &exposition,
                "dz_publisher_ingress_parse_errors_total",
                &[&format!("reason=\"{reason}\"")],
                " 1"
            ),
            "`{reason}` did not move:\n{exposition}"
        );
    }
}

#[test]
fn every_disconnect_reason_reaches_the_reconnect_series() {
    let metrics = metrics();
    let observer = MetricsObserver::new(Arc::clone(&metrics));

    for reason in DisconnectReason::ALL {
        observer.reconnect(reason);
    }

    let exposition = metrics.render();
    for reason in ["timeout", "remote_close", "rate_limit", "auth_expired"] {
        assert!(
            sample(
                &exposition,
                "dz_publisher_ingress_reconnects_total",
                &[&format!("reason=\"{reason}\"")],
                " 1"
            ),
            "`{reason}` did not move:\n{exposition}"
        );
    }
}

#[test]
fn the_connection_state_is_pre_created_at_zero_and_then_set() {
    // The series exists from startup, which is what lets an `== 0` alert fire
    // on a publisher whose upstream never came up at all — the case the metric
    // most exists for.
    let metrics = metrics();
    let observer = MetricsObserver::new(Arc::clone(&metrics));
    assert!(sample(
        &metrics.render(),
        "dz_publisher_ingress_connection_state",
        &["connection=\"upstream\""],
        " 0"
    ));

    observer.connection_state("upstream", true);
    assert!(sample(
        &metrics.render(),
        "dz_publisher_ingress_connection_state",
        &["connection=\"upstream\""],
        " 1"
    ));
}

#[test]
fn a_message_type_the_publisher_did_not_declare_is_folded_to_other() {
    // The guard on a label whose values belong to the upstream's vocabulary:
    // many APIs name a message after the subscription that carried it, which is
    // one series per instrument.
    let metrics = metrics();
    let observer = MetricsObserver::new(Arc::clone(&metrics));
    observer.message("quote", "upstream");
    observer.message("book.ETH-USD.depth20", "upstream");

    let exposition = metrics.render();
    assert!(sample(
        &exposition,
        "dz_publisher_ingress_messages_total",
        &["message_type=\"quote\""],
        " 1"
    ));
    assert!(sample(
        &exposition,
        "dz_publisher_ingress_messages_total",
        &["message_type=\"other\""],
        " 1"
    ));
    assert!(
        !exposition.contains("ETH-USD"),
        "an undeclared message type became a series of its own:\n{exposition}"
    );
}

#[test]
fn bytes_duplicates_and_rate_limits_reach_their_own_families() {
    let metrics = metrics();
    let observer = MetricsObserver::new(Arc::clone(&metrics));
    observer.bytes(4_096);
    observer.duplicate();
    observer.rate_limited();

    let exposition = metrics.render();
    assert!(sample(
        &exposition,
        "dz_publisher_ingress_bytes_total",
        &[],
        " 4096"
    ));
    assert!(sample(
        &exposition,
        "dz_publisher_ingress_duplicates_total",
        &[],
        " 1"
    ));
    assert!(sample(
        &exposition,
        "dz_publisher_ingress_rate_limited_total",
        &[],
        " 1"
    ));
}

#[test]
fn an_adapter_failure_is_counted_where_the_closed_family_set_has_nowhere_for_it() {
    // `adapter_error` has a caller in the transport half and no family. An
    // adapter that cannot compose its own subscription is a real, retried
    // failure: it is not a parse error, and the four reconnect reasons all
    // describe a session that ended rather than one that never got going. The
    // metric name set is closed by a governing playbook, so the number is kept
    // and reported rather than a sixth series invented.
    let metrics = metrics();
    let observer = MetricsObserver::new(Arc::clone(&metrics));
    assert_eq!(observer.adapter_errors(), 0);

    observer.adapter_error(AdapterError::NotReady {
        detail: "the credential file is not readable yet",
    });
    observer.adapter_error(AdapterError::UnknownInstrument);

    assert_eq!(observer.adapter_errors(), 2);
    let exposition = metrics.render();
    assert!(
        !exposition.contains("adapter_error"),
        "a series was invented for a failure the closed set has no reason for"
    );
}
