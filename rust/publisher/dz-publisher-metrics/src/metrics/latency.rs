use std::collections::HashMap;

use prometheus::{Histogram, HistogramVec, IntGauge, Registry};

use crate::buckets::LATENCY_BUCKETS;
use crate::labels::{EgressMessageType, EventKind, TimestampKind};
use crate::opts::{histogram_opts, opts};

/// Latency metrics along the path from the venue to this publisher's own send.
pub struct LatencyMetrics {
    venue_to_recv_latency_seconds: HistogramVec,
    venue_timestamps_available: IntGauge,
    recv_to_send_latency_seconds: HistogramVec,
    book_update_duration_seconds: Histogram,
    encode_duration_seconds: HistogramVec,
}

impl LatencyMetrics {
    pub(crate) fn new(registry: &Registry, labels: &HashMap<String, String>) -> Self {
        let venue_to_recv_latency_seconds = HistogramVec::new(
            histogram_opts(
                "dz_publisher_venue_to_recv_latency_seconds",
                "Latency from a venue-supplied timestamp to this publisher's receipt of the \
                 message, by which upstream timestamp was used. This depends on the venue's clock \
                 agreeing with ours, and its resolution is bounded by the upstream source's own \
                 timestamp resolution: a millisecond-resolution upstream source gives millisecond \
                 quantisation however fine these buckets are.",
                labels,
                LATENCY_BUCKETS,
            ),
            &["timestamp_kind"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(venue_to_recv_latency_seconds.clone()))
            .expect("static metric registration");
        // `timestamp_kind` is a closed enum: pre-create every child so the
        // family exists at 0 from startup rather than appearing only after
        // the first observation.
        for kind in TimestampKind::ALL {
            venue_to_recv_latency_seconds.with_label_values(&[kind.as_str()]);
        }

        let venue_timestamps_available = IntGauge::with_opts(opts(
            "dz_publisher_venue_timestamps_available",
            "Count of upstream timestamp kinds this venue exposes; 0 where the venue exposes none.",
            labels,
        ))
        .expect("static metric definition");
        registry
            .register(Box::new(venue_timestamps_available.clone()))
            .expect("static metric registration");

        let recv_to_send_latency_seconds = HistogramVec::new(
            histogram_opts(
                "dz_publisher_recv_to_send_latency_seconds",
                "Latency from this publisher's receipt of an event to its egress send, by event \
                 kind.",
                labels,
                LATENCY_BUCKETS,
            ),
            &["event_kind"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(recv_to_send_latency_seconds.clone()))
            .expect("static metric registration");
        // `event_kind` is a closed enum: pre-create every child so the
        // family exists at 0 from startup rather than appearing only after
        // the first observation.
        for kind in EventKind::ALL {
            recv_to_send_latency_seconds.with_label_values(&[kind.as_str()]);
        }

        let book_update_duration_seconds = Histogram::with_opts(histogram_opts(
            "dz_publisher_book_update_duration_seconds",
            "Time to apply one order book update.",
            labels,
            LATENCY_BUCKETS,
        ))
        .expect("static metric definition");
        registry
            .register(Box::new(book_update_duration_seconds.clone()))
            .expect("static metric registration");

        let encode_duration_seconds = HistogramVec::new(
            histogram_opts(
                "dz_publisher_encode_duration_seconds",
                "Time to encode one outbound message, by message type.",
                labels,
                LATENCY_BUCKETS,
            ),
            &["message_type"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(encode_duration_seconds.clone()))
            .expect("static metric registration");
        // Outbound `message_type` is a closed enum: pre-create every child
        // so the family exists from startup. This one is not crossed with
        // port role, so every variant applies.
        for message_type in EgressMessageType::ALL {
            encode_duration_seconds.with_label_values(&[message_type.as_str()]);
        }

        Self {
            venue_to_recv_latency_seconds,
            venue_timestamps_available,
            recv_to_send_latency_seconds,
            book_update_duration_seconds,
            encode_duration_seconds,
        }
    }

    /// Records a venue-to-recv latency observation for `timestamp_kind`.
    pub fn observe_venue_to_recv(&self, timestamp_kind: TimestampKind, seconds: f64) {
        self.venue_to_recv_latency_seconds
            .with_label_values(&[timestamp_kind.as_str()])
            .observe(seconds);
    }

    /// Sets the count of upstream timestamp kinds this venue exposes.
    pub fn set_venue_timestamps_available(&self, n: i64) {
        self.venue_timestamps_available.set(n);
    }

    /// Records a recv-to-send latency observation for `event_kind`.
    pub fn observe_recv_to_send(&self, event_kind: EventKind, seconds: f64) {
        self.recv_to_send_latency_seconds
            .with_label_values(&[event_kind.as_str()])
            .observe(seconds);
    }

    /// Records the duration of one order book update.
    pub fn observe_book_update_duration(&self, seconds: f64) {
        self.book_update_duration_seconds.observe(seconds);
    }

    /// Records the duration of encoding one outbound message.
    pub fn observe_encode_duration(&self, message_type: EgressMessageType, seconds: f64) {
        self.encode_duration_seconds
            .with_label_values(&[message_type.as_str()])
            .observe(seconds);
    }
}
