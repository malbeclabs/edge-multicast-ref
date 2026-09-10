//! The `dz_recorder_inline_*` family: what this arrangement can be wrong about
//! that the other one cannot.
//!
//! # Its own family, beside the health tier's and not inside it
//!
//! `dz_recorder_*` means the same thing in both arrangements and is untouched
//! here. These series describe three places inline mode can lose or delay data
//! that archive mode has no equivalent of — the ring, the spool, and a stage
//! that stopped — and folding them into the health tier's registry would make a
//! dashboard built for archive mode show empty panels for series that cannot
//! exist there.
//!
//! Both are served from one port, because two would be a second target for an
//! operator to configure and a second thing to notice is missing.
//!
//! # Alert on the age, never on the eviction counter
//!
//! [`oldest_unposted_age_seconds`] is the number this arrangement is gated on.
//! A full spool evicts on every pass at steady state by design, so the eviction
//! counter rises whether or not anything is wrong; one window older than the
//! budget can hold is history already gone. The loader's own documentation makes
//! this argument about objects, and it is the same argument.
//!
//! [`oldest_unposted_age_seconds`]: InlineMetrics::observe

use std::sync::Mutex;

use prometheus::{IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder};

use crate::pipeline::InlineCounters;
use crate::ring::RingCounters;
use crate::spool::Spool;

/// One registry for the process, with a `feed` label.
#[derive(Debug)]
pub struct InlineMetrics {
    /// Held across a whole scrape, and the reason is not the registry.
    ///
    /// Prometheus counters cannot be assigned, only advanced, so a counter that
    /// mirrors a total the stages already keep is sampled by adding the
    /// difference — read, subtract, add. That is three steps, and the metrics
    /// endpoint serves every request on its own thread. Two scrapes landing
    /// together would each read the same value, each compute the same
    /// difference, and each add it: a `*_total` inflated for the life of the
    /// process, by an amount nothing records. So a scrape samples and renders
    /// under this, and the concurrency the endpoint has stops at the door.
    sampling: Mutex<()>,
    registry: Registry,
    ring_dropped: IntCounterVec,
    windows_derived: IntCounterVec,
    windows_empty: IntCounterVec,
    rows_derived: IntCounterVec,
    windows_landed: IntCounterVec,
    posts_failed: IntCounterVec,
    stage_restarts: IntCounterVec,
    windows_evicted: IntCounterVec,
    windows_discarded: IntCounterVec,
    spool_bytes: IntGaugeVec,
    spool_windows: IntGaugeVec,
    oldest_unposted_age_seconds: IntGaugeVec,
}

impl InlineMetrics {
    #[must_use]
    pub fn new(site: &str, recorder: &str) -> Self {
        let registry = Registry::new();
        let labels = [("site", site), ("recorder", recorder)];

        let counter = |name: &str, help: &str| {
            let opts = Opts::new(name, help).const_labels(
                labels
                    .iter()
                    .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                    .collect(),
            );
            let vec = IntCounterVec::new(opts, &["feed"]).expect("the metric is well formed");
            registry
                .register(Box::new(vec.clone()))
                .expect("the metric is registered once");
            vec
        };
        let gauge = |name: &str, help: &str| {
            let opts = Opts::new(name, help).const_labels(
                labels
                    .iter()
                    .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                    .collect(),
            );
            let vec = IntGaugeVec::new(opts, &["feed"]).expect("the metric is well formed");
            registry
                .register(Box::new(vec.clone()))
                .expect("the metric is registered once");
            vec
        };

        Self {
            ring_dropped: counter(
                "dz_recorder_inline_ring_dropped_total",
                "Datagrams the derivation never saw because it was behind. Every one is admitted \
                 in the drop_delta of a later datagram, so the gaps they cause are attributed to \
                 this recorder and not to the publisher. Alert on the delta.",
            ),
            windows_derived: counter(
                "dz_recorder_inline_windows_derived_total",
                "Windows turned into rows.",
            ),
            windows_empty: counter(
                "dz_recorder_inline_windows_empty_total",
                "Windows that closed with no datagram in them. Ordinary on a quiet feed, and a \
                 feed that is only this is a feed nothing is arriving on.",
            ),
            rows_derived: counter(
                "dz_recorder_inline_rows_derived_total",
                "Rows derived, across every grain.",
            ),
            windows_landed: counter(
                "dz_recorder_inline_windows_landed_total",
                "Windows whose rows the destination acknowledged and whose ledger entry is \
                 written. Not the same as derived: a window is landed only once both are true.",
            ),
            posts_failed: counter(
                "dz_recorder_inline_posts_failed_total",
                "Inserts the destination refused. The windows stay on disk and are retried, so \
                 this costs loading progress rather than rows — until the spool budget is \
                 reached.",
            ),
            stage_restarts: counter(
                "dz_recorder_inline_stage_restarts_total",
                "Times the derivation or the posting stage panicked and was begun again. Never \
                 zero-and-fine: any value above zero is a bug that was recovered from, and the \
                 capture kept running throughout.",
            ),
            windows_evicted: counter(
                "dz_recorder_inline_windows_evicted_total",
                "Windows dropped under the spool's byte budget, their rows lost for good. Do not \
                 alert on this: a full budget evicts on every pass at steady state. Alert on \
                 dz_recorder_inline_oldest_unposted_age_seconds.",
            ),
            windows_discarded: counter(
                "dz_recorder_inline_windows_discarded_total",
                "Spooled windows whose own digest did not match, or whose close never finished. \
                 Discarded rather than loaded in part, because half a window's rows read as a \
                 clean feed.",
            ),
            spool_bytes: gauge(
                "dz_recorder_inline_spool_bytes",
                "Rows on disk waiting for the destination, against the configured budget.",
            ),
            spool_windows: gauge(
                "dz_recorder_inline_spool_windows",
                "Windows on disk waiting for the destination.",
            ),
            oldest_unposted_age_seconds: gauge(
                "dz_recorder_inline_oldest_unposted_age_seconds",
                "How far behind the oldest window still waiting is. THIS IS THE NUMBER TO ALERT \
                 ON: a window older than the spool budget can hold is history already gone, and \
                 no re-run recovers it. Zero means nothing is waiting.",
            ),
            registry,
            sampling: Mutex::new(()),
        }
    }

    /// One scrape: sample every feed, then render, with nothing else in between.
    ///
    /// The sampling is the caller's closure because only the caller knows which
    /// feeds it is running and where their spools are — but *when* it happens
    /// is this type's, because a sample that races another sample corrupts a
    /// counter permanently. See [`Self::sampling`].
    pub fn scrape<F: FnOnce(&Self)>(&self, sample: F) -> String {
        let _held = self
            .sampling
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        sample(self);
        self.render()
    }

    /// Samples one feed's counters into the registry.
    ///
    /// Called on the scrape rather than on every window, because these are
    /// derived from atomics the stages already keep — a second write per window
    /// would put a registry lock on the derivation's path for a number nobody
    /// reads between scrapes.
    pub fn observe(
        &self,
        feed: &str,
        ring: &RingCounters,
        counters: &InlineCounters,
        spool: &Spool,
        now_ns: u64,
    ) {
        let label = [feed];
        set(&self.ring_dropped, &label, ring.dropped());
        set(&self.windows_derived, &label, counters.windows_derived());
        set(&self.windows_empty, &label, counters.windows_empty());
        set(&self.rows_derived, &label, counters.rows_derived());
        set(&self.windows_landed, &label, counters.windows_landed());
        set(&self.posts_failed, &label, counters.posts_failed());
        set(&self.stage_restarts, &label, counters.stage_restarts());
        set(&self.windows_evicted, &label, spool.windows_evicted_total());
        set(
            &self.windows_discarded,
            &label,
            spool.windows_discarded_total(),
        );

        self.spool_bytes
            .with_label_values(&label)
            .set(i64::try_from(spool.bytes()).unwrap_or(i64::MAX));
        self.spool_windows
            .with_label_values(&label)
            .set(i64::try_from(spool.windows()).unwrap_or(i64::MAX));
        self.oldest_unposted_age_seconds
            .with_label_values(&label)
            .set(i64::try_from(spool.oldest_age_seconds(now_ns)).unwrap_or(i64::MAX));
    }

    /// The exposition as it stands.
    ///
    /// Prefer [`scrape`](Self::scrape), which samples first and holds the two
    /// together. This is here for a caller that has already sampled and for the
    /// tests.
    #[must_use]
    pub fn render(&self) -> String {
        let mut out = String::new();
        let _ = TextEncoder::new().encode_utf8(&self.registry.gather(), &mut out);
        out
    }
}

/// Advances a counter to a cumulative total the stages already keep.
///
/// A counter cannot be assigned, so the difference is added. The stages own the
/// running totals — they are atomics on a hot path and a registry lock is not —
/// and this is the one place the two representations meet.
fn set(vec: &IntCounterVec, label: &[&str; 1], total: u64) {
    let counter = vec.with_label_values(label);
    counter.inc_by(total.saturating_sub(counter.get()));
}
