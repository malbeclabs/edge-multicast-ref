use std::collections::HashMap;

use prometheus::{Gauge, IntCounterVec, IntGaugeVec, Registry};

use crate::labels::ExitReason;
use crate::opts::opts;

/// Process-level metrics: build identity, liveness, and shutdown reason.
pub struct ProcessMetrics {
    started: std::time::Instant,
    build_info: IntGaugeVec,
    uptime_seconds: Gauge,
    idle_guard_last_update_timestamp_seconds: Gauge,
    exit_reason_total: IntCounterVec,
}

impl ProcessMetrics {
    pub(crate) fn new(registry: &Registry, labels: &HashMap<String, String>) -> Self {
        // Not pre-created: `version`, `commit`, and `toolchain` are values
        // the caller supplies, not a closed set known at construction. A
        // publisher registers this once at startup via `set_build_info`.
        let build_info = IntGaugeVec::new(
            opts(
                "dz_publisher_build_info",
                "Always 1. Build identity, in its labels: version, commit, toolchain.",
                labels,
            ),
            &["version", "commit", "toolchain"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(build_info.clone()))
            .expect("static metric registration");

        let uptime_seconds = Gauge::with_opts(opts(
            "dz_publisher_uptime_seconds",
            "Seconds since process start. Maintained by this crate and refreshed on every \
             scrape, so a staleness rule may rely on it as a guard.",
            labels,
        ))
        .expect("static metric definition");
        registry
            .register(Box::new(uptime_seconds.clone()))
            .expect("static metric registration");

        let idle_guard_last_update_timestamp_seconds = Gauge::with_opts(opts(
            "dz_publisher_idle_guard_last_update_timestamp_seconds",
            "Unix timestamp the idle guard last observed activity. Registered from startup and so 0 until the first is recorded; guard any staleness rule on `and on() dz_publisher_uptime_seconds > 60`, or `time() - this` reads as an age of decades before it has ever been set.",
            labels,
        ))
        .expect("static metric definition");
        registry
            .register(Box::new(idle_guard_last_update_timestamp_seconds.clone()))
            .expect("static metric registration");

        let exit_reason_total = IntCounterVec::new(
            opts(
                "dz_publisher_exit_reason_total",
                "Process exits, by reason.",
                labels,
            ),
            &["reason"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(exit_reason_total.clone()))
            .expect("static metric registration");
        // `reason` is a closed enum: pre-create every child so the family
        // exists at 0 from startup rather than appearing only after the
        // process has already exited once.
        for reason in ExitReason::ALL {
            exit_reason_total.with_label_values(&[reason.as_str()]);
        }

        Self {
            started: std::time::Instant::now(),
            build_info,
            uptime_seconds,
            idle_guard_last_update_timestamp_seconds,
            exit_reason_total,
        }
    }

    /// Records build identity. Call once at startup; the value is always 1.
    pub fn set_build_info(&self, version: &str, commit: &str, toolchain: &str) {
        self.build_info
            .with_label_values(&[version, commit, toolchain])
            .set(1);
    }

    /// Brings `dz_publisher_uptime_seconds` up to date. Called on every
    /// render, so the gauge is correct at scrape time without a publisher
    /// having to run a ticker for it.
    pub(crate) fn refresh_uptime(&self) {
        self.uptime_seconds
            .set(self.started.elapsed().as_secs_f64());
    }

    /// Sets the Unix timestamp the idle guard last observed activity.
    pub fn set_idle_guard_last_update(&self, unix_seconds: f64) {
        self.idle_guard_last_update_timestamp_seconds
            .set(unix_seconds);
    }

    /// Records one process exit.
    pub fn exit(&self, reason: ExitReason) {
        self.exit_reason_total
            .with_label_values(&[reason.as_str()])
            .inc();
    }
}
