use std::collections::HashMap;

use dz_edge_core::PortRole;
use prometheus::{Gauge, Histogram, IntCounter, IntCounterVec, IntGaugeVec, Registry};

use crate::buckets::REFDATA_LOAD_DURATION_BUCKETS;
use crate::labels::channel_id_label;
use crate::labels::RefdataLoadErrorReason;
use crate::opts::{histogram_opts, opts};

/// Metrics for the reference-data load-and-distribution path.
pub struct RefdataMetrics {
    definitions_emitted_total: IntCounter,
    instruments_current: IntGaugeVec,
    load_duration_seconds: Histogram,
    load_errors_total: IntCounterVec,
    last_refresh_timestamp_seconds: Gauge,
    new_listings_total: IntCounter,
    delistings_total: IntCounter,
    manifest_seq: IntGaugeVec,
    manifest_valid: IntGaugeVec,
}

impl RefdataMetrics {
    pub(crate) fn new(
        registry: &Registry,
        labels: &HashMap<String, String>,
        channel_ids: &[u8],
        port_roles: &[PortRole],
    ) -> Self {
        // The manifest gauges belong to the refdata port. On a publisher
        // that does not operate one there is no manifest to report, and
        // `manifest_valid` pre-created at 0 is a wrong value rather than a
        // missing one: `== 0` on a gauge whose HELP reads "1 valid, 0 not"
        // is the obvious alert, and it would fire forever.
        let serves_refdata = port_roles.contains(&PortRole::Refdata);
        let definitions_emitted_total = IntCounter::with_opts(opts(
            "dz_publisher_refdata_definitions_emitted_total",
            "Instrument definitions emitted on the reference-data feed.",
            labels,
        ))
        .expect("static metric definition");
        registry
            .register(Box::new(definitions_emitted_total.clone()))
            .expect("static metric registration");

        let instruments_current = IntGaugeVec::new(
            opts(
                "dz_publisher_refdata_instruments_current",
                "Instruments in one Channel ID's published set: the `Instrument Count` that \
                 channel's manifest carries. Keyed by Channel ID because the specification counts \
                 the published set per channel; one process-wide total would report the whole \
                 process against a channel whose subscribers will only ever get messages for \
                 their own.",
                labels,
            ),
            &["channel_id"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(instruments_current.clone()))
            .expect("static metric registration");
        // Pre-created for every declared Channel ID, and — unlike the two
        // manifest gauges above — not gated on the refdata port role. 0 here
        // is a true statement rather than a wrong one: the channel exists and
        // holds nothing. That is also the case worth alerting on, because a
        // channel no instrument was ever admitted to is one whose subscribers
        // wait forever, and a series that only appears on first use cannot
        // carry an alert for the thing never happening.
        for channel_id in channel_ids {
            instruments_current.with_label_values(&[channel_id_label(*channel_id)]);
        }

        let load_duration_seconds = Histogram::with_opts(histogram_opts(
            "dz_publisher_refdata_load_duration_seconds",
            "Time to load reference data from the upstream source. Uses coarser buckets than the \
             per-message latency series: a load is a bulk, once-per-refresh operation on the \
             order of tens of milliseconds to tens of seconds, not the microsecond-scale path \
             those buckets target.",
            labels,
            REFDATA_LOAD_DURATION_BUCKETS,
        ))
        .expect("static metric definition");
        registry
            .register(Box::new(load_duration_seconds.clone()))
            .expect("static metric registration");

        let load_errors_total = IntCounterVec::new(
            opts(
                "dz_publisher_refdata_load_errors_total",
                "Reference-data load failures, by reason.",
                labels,
            ),
            &["reason"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(load_errors_total.clone()))
            .expect("static metric registration");
        // `reason` is a closed enum: pre-create every child so the family
        // exists at 0 from startup rather than appearing only after the
        // first load failure.
        for reason in RefdataLoadErrorReason::ALL {
            load_errors_total.with_label_values(&[reason.as_str()]);
        }

        let last_refresh_timestamp_seconds = Gauge::with_opts(opts(
            "dz_publisher_refdata_last_refresh_timestamp_seconds",
            "Unix timestamp of the last successful reference-data refresh. Registered from startup and so 0 until the first is recorded; guard any staleness rule on `and on() dz_publisher_uptime_seconds > 60`, or `time() - this` reads as an age of decades before it has ever been set.",
            labels,
        ))
        .expect("static metric definition");
        registry
            .register(Box::new(last_refresh_timestamp_seconds.clone()))
            .expect("static metric registration");

        let new_listings_total = IntCounter::with_opts(opts(
            "dz_publisher_refdata_new_listings_total",
            "New instrument listings observed in reference data.",
            labels,
        ))
        .expect("static metric definition");
        registry
            .register(Box::new(new_listings_total.clone()))
            .expect("static metric registration");

        let delistings_total = IntCounter::with_opts(opts(
            "dz_publisher_refdata_delistings_total",
            "Instrument delistings observed in reference data.",
            labels,
        ))
        .expect("static metric definition");
        registry
            .register(Box::new(delistings_total.clone()))
            .expect("static metric registration");

        let manifest_seq = IntGaugeVec::new(
            opts(
                "dz_publisher_refdata_manifest_seq",
                "Current manifest sequence number, by Channel ID.",
                labels,
            ),
            &["channel_id"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(manifest_seq.clone()))
            .expect("static metric registration");
        if serves_refdata {
            for channel_id in channel_ids {
                manifest_seq.with_label_values(&[channel_id_label(*channel_id)]);
            }
        }

        let manifest_valid = IntGaugeVec::new(
            opts(
                "dz_publisher_refdata_manifest_valid",
                "Whether the current manifest for a Channel ID is valid: 1 valid, 0 not.",
                labels,
            ),
            &["channel_id"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(manifest_valid.clone()))
            .expect("static metric registration");
        if serves_refdata {
            for channel_id in channel_ids {
                manifest_valid.with_label_values(&[channel_id_label(*channel_id)]);
            }
        }

        Self {
            definitions_emitted_total,
            instruments_current,
            load_duration_seconds,
            load_errors_total,
            last_refresh_timestamp_seconds,
            new_listings_total,
            delistings_total,
            manifest_seq,
            manifest_valid,
        }
    }

    /// Records one instrument definition emitted.
    ///
    /// Process-wide, and deliberately not keyed by Channel ID: this is a rate,
    /// and a rate over channels aggregates honestly. The per-channel form of
    /// the question is already answered by
    /// `dz_publisher_egress_sequence_current{port_role="refdata"}`, so a
    /// `channel_id` label here would add a series per declared channel that no
    /// query reads.
    pub fn definition_emitted(&self) {
        self.definitions_emitted_total.inc();
    }

    /// Sets the `Instrument Count` one Channel ID's published set carries.
    ///
    /// The count is a `usize` here and a `u32` on the wire, and a Prometheus
    /// gauge is `i64`; the saturating conversion is done here so the lossy step
    /// is not repeated as an `as i64` at every call site.
    pub fn set_instruments_current(&self, channel_id: u8, count: usize) {
        self.instruments_current
            .with_label_values(&[channel_id_label(channel_id)])
            .set(i64::try_from(count).unwrap_or(i64::MAX));
    }

    /// Records the duration of a reference-data load.
    pub fn observe_load_duration(&self, seconds: f64) {
        self.load_duration_seconds.observe(seconds);
    }

    /// Records one reference-data load failure.
    pub fn load_error(&self, reason: RefdataLoadErrorReason) {
        self.load_errors_total
            .with_label_values(&[reason.as_str()])
            .inc();
    }

    /// Sets the Unix timestamp of the last successful reference-data refresh.
    pub fn set_last_refresh_timestamp(&self, unix_seconds: f64) {
        self.last_refresh_timestamp_seconds.set(unix_seconds);
    }

    /// Records one new instrument listing.
    ///
    /// Process-wide, for the reason
    /// [`definition_emitted`](Self::definition_emitted) gives. The standing
    /// per-channel answer is
    /// [`set_instruments_current`](Self::set_instruments_current), which moves
    /// on the channel the instrument was admitted to; this counter says how
    /// often the published set changed at all.
    pub fn new_listing(&self) {
        self.new_listings_total.inc();
    }

    /// Records one instrument delisting.
    ///
    /// Process-wide, the mirror of [`new_listing`](Self::new_listing) and for
    /// the same reason. The two have to aggregate the same way, or the
    /// difference between them stops being the net change in the published
    /// set.
    pub fn delisting(&self) {
        self.delistings_total.inc();
    }

    /// Sets the current manifest sequence number for a Channel ID.
    /// `Sequence Number` is a `u64` on the wire and a Prometheus gauge is
    /// `i64`; the saturating conversion is done here so the lossy step is
    /// not repeated as an `as i64` at every call site.
    pub fn set_manifest_seq(&self, channel_id: u8, seq: u64) {
        self.manifest_seq
            .with_label_values(&[channel_id_label(channel_id)])
            .set(i64::try_from(seq).unwrap_or(i64::MAX));
    }

    /// Sets whether the current manifest for a Channel ID is valid.
    pub fn set_manifest_valid(&self, channel_id: u8, valid: bool) {
        self.manifest_valid
            .with_label_values(&[channel_id_label(channel_id)])
            .set(i64::from(valid));
    }
}
