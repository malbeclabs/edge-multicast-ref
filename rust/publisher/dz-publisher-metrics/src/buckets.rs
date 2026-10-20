/// Shared histogram buckets for every latency series in [`crate::LatencyMetrics`].
///
/// One bucket set means two venues' percentiles are comparable. It starts at
/// a microsecond rather than a millisecond: a consumer measured a 0.29 ms
/// median to its venue's gateways, and a scale starting at 1 ms would have
/// put every interesting observation in the first bucket.
pub const LATENCY_BUCKETS: &[f64] = &[
    0.000_001, 0.000_005, 0.000_010, 0.000_025, 0.000_050, 0.000_100, 0.000_250, 0.000_500, 0.001,
    0.0025, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Buckets for `dz_publisher_refdata_load_duration_seconds`.
///
/// A reference-data load is a bulk, once-per-refresh operation measured in
/// tens of milliseconds to tens of seconds, not the microsecond-scale
/// per-message path [`LATENCY_BUCKETS`] targets. Reusing the microsecond
/// scale here would waste resolution on a range this metric never visits and
/// give it none where it matters; this coarser set spans that range instead.
pub const REFDATA_LOAD_DURATION_BUCKETS: &[f64] = &[
    0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
];

/// Buckets for `dz_publisher_ingress_batch_size`.
///
/// Powers of two up to 32, which is the most a threaded publisher drains in
/// one batch: a batch is a count of events, not a duration, and the question
/// it answers is whether the loop drains one at a time — the queue is keeping
/// up — or full batches, which is the loop falling behind its connections.
pub const INGRESS_BATCH_SIZE_BUCKETS: &[f64] = &[1.0, 2.0, 4.0, 8.0, 16.0, 32.0];
