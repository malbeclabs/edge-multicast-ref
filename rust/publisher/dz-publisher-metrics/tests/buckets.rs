use dz_edge_core::{AppMessage, DATAGRAM_HEADER_SIZE, MAX_DATAGRAM_SIZE};
use dz_publisher_metrics::{
    messages_per_datagram_buckets, INGRESS_BATCH_SIZE_BUCKETS, LATENCY_BUCKETS,
    QUOTES_PER_DATAGRAM, TRADES_PER_DATAGRAM,
};

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

#[test]
fn the_batch_size_buckets_end_at_the_largest_batch() {
    // The threaded runtime drains at most 32 events per batch, so a bucket
    // above it would never be reached and the top one is where a loop that
    // has fallen behind shows.
    assert_eq!(INGRESS_BATCH_SIZE_BUCKETS.last(), Some(&32.0));
    for pair in INGRESS_BATCH_SIZE_BUCKETS.windows(2) {
        assert!(pair[0] < pair[1]);
    }
}

#[test]
fn a_full_datagram_of_either_kind_has_a_bucket_of_its_own() {
    // The capacities are the codec's, re-derived here from its sizes.
    let room = MAX_DATAGRAM_SIZE - DATAGRAM_HEADER_SIZE;
    assert_eq!(
        QUOTES_PER_DATAGRAM,
        room / <dz_edge_tob::Quote as AppMessage>::SIZE
    );
    assert_eq!(
        TRADES_PER_DATAGRAM,
        room / <dz_edge_tob::Trade as AppMessage>::SIZE
    );
    let buckets = messages_per_datagram_buckets();
    for capacity in [QUOTES_PER_DATAGRAM, TRADES_PER_DATAGRAM] {
        for bound in [capacity - 1, capacity] {
            assert!(buckets.contains(&(bound as f64)), "{bound} in {buckets:?}");
        }
    }
    for low in [1.0, 2.0, 4.0, 8.0, 12.0, 16.0] {
        assert!(buckets.contains(&low));
    }
    for pair in buckets.windows(2) {
        assert!(pair[0] < pair[1], "sorted and deduplicated: {buckets:?}");
    }
}
