//! Internal helpers for building `prometheus::Opts` / `HistogramOpts` that
//! always carry the `venue` and `source_id` constant labels. Nothing outside
//! this crate builds an `Opts` directly, which is what makes it impossible
//! for a publisher to emit a series that skips those two labels.

use std::collections::HashMap;

use prometheus::{HistogramOpts, Opts};

pub(crate) fn const_labels(venue: &str, source_id: u16) -> HashMap<String, String> {
    let mut labels = HashMap::with_capacity(2);
    labels.insert("venue".to_string(), venue.to_string());
    labels.insert("source_id".to_string(), source_id.to_string());
    labels
}

pub(crate) fn opts(name: &str, help: &str, const_labels: &HashMap<String, String>) -> Opts {
    Opts::new(name, help).const_labels(const_labels.clone())
}

pub(crate) fn histogram_opts(
    name: &str,
    help: &str,
    const_labels: &HashMap<String, String>,
    buckets: &[f64],
) -> HistogramOpts {
    HistogramOpts::new(name, help)
        .const_labels(const_labels.clone())
        .buckets(buckets.to_vec())
}
