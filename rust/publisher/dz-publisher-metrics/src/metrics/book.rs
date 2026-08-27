use std::collections::HashMap;

use prometheus::{IntCounter, IntCounterVec, IntGauge, Registry};

use crate::labels::{InconsistencyKind, RecoveryOutcome};
use crate::opts::opts;

/// Metrics for the publisher's in-memory order book state.
pub struct BookMetrics {
    updates_total: IntCounter,
    inconsistency_total: IntCounterVec,
    recovery_total: IntCounterVec,
    instruments_tracked: IntGauge,
    instruments_published: IntGauge,
}

impl BookMetrics {
    pub(crate) fn new(registry: &Registry, labels: &HashMap<String, String>) -> Self {
        let updates_total = IntCounter::with_opts(opts(
            "dz_publisher_book_updates_total",
            "Order book updates applied.",
            labels,
        ))
        .expect("static metric definition");
        registry
            .register(Box::new(updates_total.clone()))
            .expect("static metric registration");

        let inconsistency_total = IntCounterVec::new(
            opts(
                "dz_publisher_book_inconsistency_total",
                "Order book inconsistencies detected, by kind.",
                labels,
            ),
            &["kind"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(inconsistency_total.clone()))
            .expect("static metric registration");
        // `kind` is a closed enum: pre-create every child so the family
        // exists at 0 from startup rather than appearing only after the
        // first detected inconsistency.
        for kind in InconsistencyKind::ALL {
            inconsistency_total.with_label_values(&[kind.as_str()]);
        }

        let recovery_total = IntCounterVec::new(
            opts(
                "dz_publisher_book_recovery_total",
                "Order book recovery attempts, by outcome.",
                labels,
            ),
            &["outcome"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(recovery_total.clone()))
            .expect("static metric registration");
        // `outcome` is a closed enum: pre-create every child so the family
        // exists at 0 from startup rather than appearing only after the
        // first recovery attempt.
        for outcome in RecoveryOutcome::ALL {
            recovery_total.with_label_values(&[outcome.as_str()]);
        }

        let instruments_tracked = IntGauge::with_opts(opts(
            "dz_publisher_instruments_tracked",
            "Instruments currently tracked in the publisher's in-memory book state.",
            labels,
        ))
        .expect("static metric definition");
        registry
            .register(Box::new(instruments_tracked.clone()))
            .expect("static metric registration");

        let instruments_published = IntGauge::with_opts(opts(
            "dz_publisher_instruments_published",
            "Instruments currently published on the egress feed.",
            labels,
        ))
        .expect("static metric definition");
        registry
            .register(Box::new(instruments_published.clone()))
            .expect("static metric registration");

        Self {
            updates_total,
            inconsistency_total,
            recovery_total,
            instruments_tracked,
            instruments_published,
        }
    }

    /// Records one order book update applied.
    pub fn update(&self) {
        self.updates_total.inc();
    }

    /// Records one order book inconsistency.
    pub fn inconsistency(&self, kind: InconsistencyKind) {
        self.inconsistency_total
            .with_label_values(&[kind.as_str()])
            .inc();
    }

    /// Records one order book recovery attempt.
    pub fn recovery(&self, outcome: RecoveryOutcome) {
        self.recovery_total
            .with_label_values(&[outcome.as_str()])
            .inc();
    }

    /// Sets the number of instruments currently tracked in book state.
    pub fn set_instruments_tracked(&self, n: i64) {
        self.instruments_tracked.set(n);
    }

    /// Sets the number of instruments currently published.
    pub fn set_instruments_published(&self, n: i64) {
        self.instruments_published.set(n);
    }
}
