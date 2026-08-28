use dz_publisher_metrics::LATENCY_BUCKETS;

#[test]
fn latency_buckets_start_at_or_below_one_microsecond() {
    let first = *LATENCY_BUCKETS
        .first()
        .expect("bucket set must not be empty");
    assert!(
        first <= 0.000_001,
        "first bucket {first} must be at or below 1 microsecond, \
         or a sub-millisecond observation lands in the first bucket"
    );
}

#[test]
fn latency_buckets_are_strictly_increasing() {
    for pair in LATENCY_BUCKETS.windows(2) {
        assert!(
            pair[0] < pair[1],
            "buckets must be strictly increasing: {} then {}",
            pair[0],
            pair[1]
        );
    }
}
