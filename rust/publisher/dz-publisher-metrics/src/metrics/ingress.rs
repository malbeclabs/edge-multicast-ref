use std::collections::HashMap;

use prometheus::{IntCounter, IntCounterVec, IntGaugeVec, Registry};

use crate::labels::{ParseErrorReason, ReconnectReason};
use crate::opts::opts;

/// Metrics for the path from the upstream source into the publisher.
pub struct IngressMetrics {
    messages_total: IntCounterVec,
    bytes_total: IntCounter,
    duplicates_total: IntCounter,
    parse_errors_total: IntCounterVec,
    connection_state: IntGaugeVec,
    reconnects_total: IntCounterVec,
    rate_limited_total: IntCounter,
}

impl IngressMetrics {
    pub(crate) fn new(registry: &Registry, labels: &HashMap<String, String>) -> Self {
        let messages_total = IntCounterVec::new(
            opts(
                "dz_publisher_ingress_messages_total",
                "Upstream messages received, by the upstream source's own message_type vocabulary and by connection.",
                labels,
            ),
            &["message_type", "connection"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(messages_total.clone()))
            .expect("static metric registration");

        let bytes_total = IntCounter::with_opts(opts(
            "dz_publisher_ingress_bytes_total",
            "Bytes received from the upstream source.",
            labels,
        ))
        .expect("static metric definition");
        registry
            .register(Box::new(bytes_total.clone()))
            .expect("static metric registration");

        let duplicates_total = IntCounter::with_opts(opts(
            "dz_publisher_ingress_duplicates_total",
            "Ingress messages discarded as duplicates of an already-published message. With several \
             connections taking first-copy-wins, the ratio of this to published messages is the health \
             signal for that redundancy: it should track close to the redundant connection count, and a \
             fall toward zero means the redundancy has silently stopped doing anything.",
            labels,
        ))
        .expect("static metric definition");
        registry
            .register(Box::new(duplicates_total.clone()))
            .expect("static metric registration");

        let parse_errors_total = IntCounterVec::new(
            opts(
                "dz_publisher_ingress_parse_errors_total",
                "Ingress messages that failed to parse, by reason.",
                labels,
            ),
            &["reason"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(parse_errors_total.clone()))
            .expect("static metric registration");

        let connection_state = IntGaugeVec::new(
            opts(
                "dz_publisher_ingress_connection_state",
                "Whether an ingress connection is currently connected: 1 connected, 0 not.",
                labels,
            ),
            &["connection"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(connection_state.clone()))
            .expect("static metric registration");

        let reconnects_total = IntCounterVec::new(
            opts(
                "dz_publisher_ingress_reconnects_total",
                "Ingress reconnects, by reason.",
                labels,
            ),
            &["reason"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(reconnects_total.clone()))
            .expect("static metric registration");

        let rate_limited_total = IntCounter::with_opts(opts(
            "dz_publisher_ingress_rate_limited_total",
            "Times the upstream source rate-limited this publisher's ingress connection.",
            labels,
        ))
        .expect("static metric definition");
        registry
            .register(Box::new(rate_limited_total.clone()))
            .expect("static metric registration");

        Self {
            messages_total,
            bytes_total,
            duplicates_total,
            parse_errors_total,
            connection_state,
            reconnects_total,
            rate_limited_total,
        }
    }

    /// Records one ingress message. `message_type` is the upstream source's
    /// own vocabulary, not a taxonomy this crate owns.
    pub fn message(&self, message_type: &str, connection: &str) {
        self.messages_total
            .with_label_values(&[message_type, connection])
            .inc();
    }

    /// Records ingress bytes received.
    pub fn bytes(&self, n: u64) {
        self.bytes_total.inc_by(n);
    }

    /// Records one ingress message discarded as a duplicate.
    pub fn duplicate(&self) {
        self.duplicates_total.inc();
    }

    /// Records one ingress parse error.
    pub fn parse_error(&self, reason: ParseErrorReason) {
        self.parse_errors_total
            .with_label_values(&[reason.as_str()])
            .inc();
    }

    /// Sets whether `connection` is currently connected.
    pub fn set_connection_state(&self, connection: &str, connected: bool) {
        self.connection_state
            .with_label_values(&[connection])
            .set(i64::from(connected));
    }

    /// Records one ingress reconnect.
    pub fn reconnect(&self, reason: ReconnectReason) {
        self.reconnects_total
            .with_label_values(&[reason.as_str()])
            .inc();
    }

    /// Records one instance of the upstream source rate-limiting ingress.
    pub fn rate_limited(&self) {
        self.rate_limited_total.inc();
    }
}
