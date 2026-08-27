//! `venue` and `source_id` are applied to every series `PublisherMetrics`
//! exposes; there must be no path to a metric that skips them.

use dz_publisher_metrics::PublisherMetrics;

#[test]
fn every_family_carries_venue_and_source_id() {
    let metrics = PublisherMetrics::new("test-venue", 42, &[dz_edge_core::PortRole::Mktdata]);

    // Touch one representative from each accessor, including both a plain
    // metric and a vector metric, so their samples exist in the render.
    metrics.ingress().rate_limited();
    metrics.book().update();
    metrics.refdata().new_listing();
    metrics.egress().datagram(dz_edge_core::PortRole::Mktdata);
    metrics.latency().observe_book_update_duration(0.001);
    metrics.process().set_uptime(1.0);

    let rendered = metrics.render();

    let families = [
        "dz_publisher_ingress_rate_limited_total",
        "dz_publisher_book_updates_total",
        "dz_publisher_refdata_new_listings_total",
        "dz_publisher_egress_datagrams_total",
        "dz_publisher_book_update_duration_seconds",
        "dz_publisher_uptime_seconds",
    ];

    for family in families {
        let sample_line = rendered
            .lines()
            .find(|line| line.starts_with(family) && !line.starts_with("# "))
            .unwrap_or_else(|| panic!("no sample line for {family} in:\n{rendered}"));
        assert!(
            sample_line.contains("venue=\"test-venue\""),
            "{family} is missing the venue label: {sample_line}"
        );
        assert!(
            sample_line.contains("source_id=\"42\""),
            "{family} is missing the source_id label: {sample_line}"
        );
    }
}
