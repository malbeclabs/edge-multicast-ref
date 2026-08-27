//! The venue registry must refuse to shadow the normative `dz_publisher_`
//! namespace, and otherwise behave like an ordinary Prometheus registry.

use prometheus::{IntCounter, Opts};

use dz_publisher_metrics::{MetricsError, PublisherMetrics};

#[test]
fn rejects_a_name_beginning_with_the_reserved_prefix() {
    let metrics = PublisherMetrics::new("test-venue", 1, &[]);
    let counter =
        IntCounter::with_opts(Opts::new("dz_publisher_sneaky_total", "should be rejected"))
            .unwrap();

    let err = metrics
        .venue_registry()
        .register(Box::new(counter))
        .unwrap_err();

    assert!(
        matches!(err, MetricsError::ReservedNamePrefix(name) if name == "dz_publisher_sneaky_total")
    );
}

#[test]
fn accepts_a_name_outside_the_reserved_prefix() {
    let metrics = PublisherMetrics::new("test-venue", 1, &[]);
    let counter = IntCounter::with_opts(Opts::new(
        "venue_specific_widget_total",
        "a venue-only series",
    ))
    .unwrap();
    counter.inc();

    metrics
        .venue_registry()
        .register(Box::new(counter))
        .expect("a non-reserved name must be accepted");

    let rendered = metrics.render();
    assert!(rendered.contains("venue_specific_widget_total"));
}
