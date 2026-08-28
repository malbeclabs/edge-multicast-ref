use std::collections::{HashMap, HashSet};

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
    message_types: HashSet<String>,
}

/// The bucket every message type the publisher did not declare is counted
/// under. See [`IngressMetrics::message`].
const OTHER_MESSAGE_TYPE: &str = "other";

impl IngressMetrics {
    pub(crate) fn new(
        registry: &Registry,
        labels: &HashMap<String, String>,
        connections: &[&str],
        message_types: &[&str],
    ) -> Self {
        let messages_total = IntCounterVec::new(
            opts(
                "dz_publisher_ingress_messages_total",
                "Upstream messages received, by the upstream source's own message_type vocabulary \
                 and by connection. Only the message types the publisher declared are counted \
                 under their own name; anything else falls to `other`.",
                labels,
            ),
            &["message_type", "connection"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(messages_total.clone()))
            .expect("static metric registration");
        // Pre-created over both declared sets, `other` included: a series
        // that only appears once something unexpected arrives is one
        // nobody has a panel for.
        //
        // Only `message_type` is enforced on record, and the asymmetry is
        // deliberate. It is the upstream source's vocabulary, so an
        // unexpected value is ordinary and unbounded. A `connection` name
        // is the publisher author's own, so an undeclared one is a typo in
        // their config rather than data they do not control - and there
        // the series naming the mistake is worth more than one folded into
        // a bucket, which would hide it.
        for connection in connections {
            for message_type in message_types
                .iter()
                .copied()
                .chain(std::iter::once(OTHER_MESSAGE_TYPE))
            {
                messages_total.with_label_values(&[message_type, connection]);
            }
        }

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
             connections taking first-copy-wins, every copy after the first is discarded, so the ratio \
             of this to published messages is the health signal for that redundancy: it should track \
             close to one less than the connection count (three connections give a ratio of two), and a \
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
        // `reason` is a closed enum: pre-create every child so the family
        // exists at 0 from startup rather than appearing only after the
        // first parse error.
        for reason in ParseErrorReason::ALL {
            parse_errors_total.with_label_values(&[reason.as_str()]);
        }

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
        // Pre-created at 0 for every declared connection. This family is
        // the one an operator alerts on with `== 0`, and a publisher whose
        // upstream never came up at startup would otherwise never create
        // the series at all - so the alert would stay silent in exactly
        // the case it exists for.
        for connection in connections {
            connection_state.with_label_values(&[connection]);
        }

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
        // `reason` is a closed enum: pre-create every child so the family
        // exists at 0 from startup rather than appearing only after the
        // first reconnect.
        for reason in ReconnectReason::ALL {
            reconnects_total.with_label_values(&[reason.as_str()]);
        }

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
            message_types: message_types.iter().map(|t| (*t).to_string()).collect(),
        }
    }

    /// Records one ingress message. `message_type` is the upstream source's
    /// own vocabulary, not a taxonomy this crate owns.
    /// A `message_type` the publisher did not declare is counted under
    /// `other` rather than creating a series of its own.
    ///
    /// The label is the upstream source's vocabulary, so this crate cannot
    /// enumerate it - but it also cannot leave it open. Many upstream APIs
    /// name a message after the subscription that carried it, so the
    /// natural call passes something like `trades.BTC-PERP`, which is one
    /// series per instrument on the highest-frequency path in the crate.
    /// That is the cardinality this crate refuses elsewhere by not
    /// offering an `instrument_id` parameter at all; declaring the set
    /// closes the same door here.
    pub fn message(&self, message_type: &str, connection: &str) {
        let message_type = if self.message_types.contains(message_type) {
            message_type
        } else {
            OTHER_MESSAGE_TYPE
        };
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
