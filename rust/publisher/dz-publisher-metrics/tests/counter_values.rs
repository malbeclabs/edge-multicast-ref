use dz_publisher_metrics::PublisherMetrics;

#[test]
fn a_counter_incremented_twice_renders_the_value_two() {
    let metrics = PublisherMetrics::new("test-venue", 1, &[]);

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
