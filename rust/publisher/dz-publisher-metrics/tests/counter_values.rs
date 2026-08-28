use dz_publisher_metrics::{PortRole, PublisherMetrics, PublisherMetricsConfig};

#[test]
fn a_counter_incremented_twice_renders_the_value_two() {
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[],
        connections: &[],
        channel_ids: &[],
        ingress_message_types: &[],
    });

    metrics.ingress().rate_limited();
    metrics.ingress().rate_limited();

    let rendered = metrics.render();
    let sample_line = rendered
        .lines()
        .find(|line| line.starts_with("dz_publisher_ingress_rate_limited_total{"))
        .expect("counter sample line must be present");

    assert!(
        sample_line.ends_with(" 2"),
        "expected value 2, got: {sample_line}"
    );
}

#[test]
fn a_sequence_number_beyond_i64_saturates_rather_than_wrapping() {
    // `Sequence Number` is a `u64` on the wire and a Prometheus gauge is
    // `i64`. The conversion lives in this crate so the lossy step is not
    // repeated as an `as i64` at every call site, where it would wrap
    // silently into a negative sequence.
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata],
        connections: &[],
        channel_ids: &[1],
        ingress_message_types: &[],
    });

    metrics
        .egress()
        .set_sequence(PortRole::Mktdata, 1, u64::MAX);
    metrics.refdata().set_manifest_seq(1, u64::MAX);

    let rendered = metrics.render();
    for name in [
        "dz_publisher_egress_sequence_current",
        "dz_publisher_refdata_manifest_seq",
    ] {
        let value = rendered
            .lines()
            .find(|line| line.starts_with(&format!("{name}{{")))
            .and_then(|line| line.rsplit(' ').next())
            .unwrap_or_else(|| panic!("{name} must render:\n{rendered}"));
        assert!(
            !value.starts_with('-'),
            "a u64 past i64::MAX must clamp, not wrap: {name} rendered {value}"
        );
    }
}
