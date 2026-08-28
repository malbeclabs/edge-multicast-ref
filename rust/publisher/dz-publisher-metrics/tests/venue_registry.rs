//! The venue registry must refuse to shadow the normative `dz_publisher_`
//! namespace, and otherwise behave like an ordinary Prometheus registry.

use prometheus::core::{Collector, Desc};
use prometheus::proto::{Gauge, Metric, MetricFamily, MetricType};
use prometheus::{IntCounter, IntCounterVec, Opts};

use dz_publisher_metrics::{MetricsError, PublisherMetrics, PublisherMetricsConfig};

#[test]
fn rejects_a_name_beginning_with_the_reserved_prefix() {
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[],
        connections: &[],
        channel_ids: &[],
        ingress_message_types: &[],
    });
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
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[],
        connections: &[],
        channel_ids: &[],
        ingress_message_types: &[],
    });
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
    // A venue series must carry the same identity as the normative set.
    // Without it, two publishers for different venues scraped into one
    // Prometheus produce venue series identical apart from job/instance,
    // and any dashboard row selecting `{venue="..."}` matches nothing.
    let line = rendered
        .lines()
        .find(|line| line.starts_with("venue_specific_widget_total{"))
        .unwrap_or_else(|| panic!("venue series must render:\n{rendered}"));
    assert!(line.contains("venue=\"test-venue\""), "{line}");
    assert!(line.contains("source_id=\"1\""), "{line}");
}

#[test]
fn rejects_a_collector_carrying_a_reserved_label_name() {
    // This registry applies venue and source_id as constant labels. A
    // collector carrying either name of its own renders a sample with a
    // repeated label name, which the text parser rejects for the entire
    // scrape - so one venue series would take every metric down with it.
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[],
        connections: &[],
        channel_ids: &[],
        ingress_message_types: &[],
    });

    for reserved in ["venue", "source_id"] {
        let counter = IntCounterVec::new(
            Opts::new("venue_widget_total", "carries a reserved label"),
            &[reserved],
        )
        .unwrap();

        let err = metrics
            .venue_registry()
            .register(Box::new(counter))
            .unwrap_err();

        assert!(
            matches!(&err, MetricsError::ReservedLabelName { label, .. } if label == reserved),
            "expected a reserved-label rejection for {reserved}, got {err:?}"
        );
    }

    // The same applies to a constant label, which never reaches
    // `variable_labels`.
    let counter = IntCounter::with_opts(
        Opts::new("venue_other_total", "carries a reserved constant label")
            .const_label("venue", "smuggled"),
    )
    .unwrap();
    assert!(matches!(
        metrics.venue_registry().register(Box::new(counter)),
        Err(MetricsError::ReservedLabelName { .. })
    ));
}

/// Describes a name outside the reserved namespace but collects one inside
/// it. Nothing in the `Collector` contract requires the two to agree, so
/// registration alone cannot make the guarantee hold.
struct LyingCollector {
    desc: Desc,
}

impl Collector for LyingCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut gauge = Gauge::default();
        gauge.set_value(1.0);
        let mut metric = Metric::default();
        metric.set_gauge(gauge);

        let mut family = MetricFamily::default();
        family.set_name("dz_publisher_ingress_messages_total".to_string());
        family.set_help("shadowing the normative namespace".to_string());
        family.set_field_type(MetricType::GAUGE);
        family.set_metric(vec![metric]);
        vec![family]
    }
}

#[test]
fn render_drops_a_venue_family_that_collects_into_the_reserved_namespace() {
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[],
        connections: &[],
        channel_ids: &[],
        ingress_message_types: &[],
    });

    let collector = LyingCollector {
        desc: Desc::new(
            "venue_honest_looking_total".to_string(),
            "passes the registration check".to_string(),
            vec![],
            std::collections::HashMap::new(),
        )
        .unwrap(),
    };
    metrics
        .venue_registry()
        .register(Box::new(collector))
        .expect("the described name is outside the reserved prefix");

    let rendered = metrics.render();

    // Two `# TYPE` blocks for one family make Prometheus reject the whole
    // scrape, so one misbehaving venue collector would take every metric
    // down with it rather than only its own.
    let type_lines = rendered
        .lines()
        .filter(|line| line == &"# TYPE dz_publisher_ingress_messages_total gauge")
        .count();
    assert_eq!(
        type_lines, 0,
        "the shadowing family must not render:\n{rendered}"
    );
}

/// Gathers a family the text encoder cannot encode. `UNTYPED` hits an
/// `unimplemented!()` inside the encoder, so no handling of the returned
/// `Result` would catch it.
struct UnencodableCollector {
    desc: Desc,
    field_type: MetricType,
    include_metric: bool,
}

impl Collector for UnencodableCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut family = MetricFamily::default();
        family.set_name("venue_unencodable_total".to_string());
        family.set_help("gathers something the encoder rejects".to_string());
        family.set_field_type(self.field_type);
        if self.include_metric {
            let mut gauge = Gauge::default();
            gauge.set_value(1.0);
            let mut metric = Metric::default();
            metric.set_gauge(gauge);
            family.set_metric(vec![metric]);
        }
        vec![family]
    }
}

#[test]
fn render_survives_a_venue_collector_the_encoder_cannot_encode() {
    // `render` is called on the metrics server's accept-loop thread. A
    // panic there unwinds that thread while the listener keeps accepting,
    // so scrapes hang with no response and nothing to explain it.
    for (field_type, include_metric) in [(MetricType::UNTYPED, true), (MetricType::GAUGE, false)] {
        let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
            venue: "test-venue",
            source_id: 1,
            port_roles: &[],
            connections: &[],
            channel_ids: &[],
            ingress_message_types: &[],
        });
        metrics.ingress().rate_limited();

        let collector = UnencodableCollector {
            desc: Desc::new(
                "venue_unencodable_total".to_string(),
                "passes the registration check".to_string(),
                vec![],
                std::collections::HashMap::new(),
            )
            .unwrap(),
            field_type,
            include_metric,
        };
        metrics
            .venue_registry()
            .register(Box::new(collector))
            .expect("the described name and labels are acceptable");

        let rendered = metrics.render();
        assert!(
            rendered.contains("dz_publisher_ingress_rate_limited_total"),
            "the normative set must still render:\n{rendered}"
        );
        assert!(
            !rendered.contains("venue_unencodable_total"),
            "the unencodable family must be dropped:\n{rendered}"
        );
    }
}
