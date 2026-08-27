use std::collections::HashMap;

use dz_edge_core::PortRole;
use prometheus::{GaugeVec, IntCounterVec, IntGaugeVec, Registry};

use crate::labels::EgressErrorReason;
use crate::opts::opts;

/// Metrics for the path from the publisher onto the wire.
///
/// Every series here carries `port_role`. The two roles a channel may use
/// (`mktdata`/`refdata` and `snapshot`) are separate channel instances with
/// independent sequence series; do not aggregate across them.
pub struct EgressMetrics {
    datagrams_total: IntCounterVec,
    messages_total: IntCounterVec,
    bytes_total: IntCounterVec,
    errors_total: IntCounterVec,
    sequence_current: IntGaugeVec,
    heartbeat_last_sent_timestamp_seconds: GaugeVec,
}

impl EgressMetrics {
    /// `port_roles` is exactly the set of port roles this publisher
    /// operates. Only those roles get pre-created children on the
    /// port_role-labelled families below: a series for a role the
    /// publisher does not operate would assert a channel that does not
    /// exist.
    pub(crate) fn new(
        registry: &Registry,
        labels: &HashMap<String, String>,
        port_roles: &[PortRole],
    ) -> Self {
        let datagrams_total = IntCounterVec::new(
            opts(
                "dz_publisher_egress_datagrams_total",
                "Datagrams sent, by port role. The mktdata/refdata role and the snapshot role are \
                 separate channel instances; do not aggregate across them.",
                labels,
            ),
            &["port_role"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(datagrams_total.clone()))
            .expect("static metric registration");
        for port_role in port_roles {
            datagrams_total.with_label_values(&[port_role.as_str()]);
        }

        // Not pre-created: `message_type` is the upstream source's own
        // vocabulary, not a closed set known at construction.
        let messages_total = IntCounterVec::new(
            opts(
                "dz_publisher_egress_messages_total",
                "Messages sent, by port role and message type.",
                labels,
            ),
            &["port_role", "message_type"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(messages_total.clone()))
            .expect("static metric registration");

        let bytes_total = IntCounterVec::new(
            opts(
                "dz_publisher_egress_bytes_total",
                "Bytes sent, by port role.",
                labels,
            ),
            &["port_role"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(bytes_total.clone()))
            .expect("static metric registration");
        for port_role in port_roles {
            bytes_total.with_label_values(&[port_role.as_str()]);
        }

        let errors_total = IntCounterVec::new(
            opts(
                "dz_publisher_egress_errors_total",
                "Egress send failures, by port role and reason.",
                labels,
            ),
            &["port_role", "reason"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(errors_total.clone()))
            .expect("static metric registration");
        // `reason` is a closed enum; crossed with the supplied port roles,
        // both label dimensions here are closed sets known at construction.
        for port_role in port_roles {
            for reason in EgressErrorReason::ALL {
                errors_total.with_label_values(&[port_role.as_str(), reason.as_str()]);
            }
        }

        // Not pre-created: `channel_id` is a deployment choice, not a
        // closed set known at construction.
        let sequence_current = IntGaugeVec::new(
            opts(
                "dz_publisher_egress_sequence_current",
                "Current outbound sequence number, by port role and Channel ID. The mktdata/refdata \
                 role and the snapshot role are separate channel instances with independent \
                 sequence series; do not aggregate across them.",
                labels,
            ),
            &["port_role", "channel_id"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(sequence_current.clone()))
            .expect("static metric registration");

        // Not pre-created: `channel_id` is a deployment choice, not a
        // closed set known at construction.
        let heartbeat_last_sent_timestamp_seconds = GaugeVec::new(
            opts(
                "dz_publisher_egress_heartbeat_last_sent_timestamp_seconds",
                "Unix timestamp the last heartbeat was sent, by Channel ID.",
                labels,
            ),
            &["channel_id"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(heartbeat_last_sent_timestamp_seconds.clone()))
            .expect("static metric registration");

        Self {
            datagrams_total,
            messages_total,
            bytes_total,
            errors_total,
            sequence_current,
            heartbeat_last_sent_timestamp_seconds,
        }
    }

    /// Records one datagram sent on `port_role`.
    pub fn datagram(&self, port_role: PortRole) {
        self.datagrams_total
            .with_label_values(&[port_role.as_str()])
            .inc();
    }

    /// Records one message sent on `port_role`.
    pub fn message(&self, port_role: PortRole, message_type: &str) {
        self.messages_total
            .with_label_values(&[port_role.as_str(), message_type])
            .inc();
    }

    /// Records bytes sent on `port_role`.
    pub fn bytes(&self, port_role: PortRole, n: u64) {
        self.bytes_total
            .with_label_values(&[port_role.as_str()])
            .inc_by(n);
    }

    /// Records one egress send failure on `port_role`.
    pub fn error(&self, port_role: PortRole, reason: EgressErrorReason) {
        self.errors_total
            .with_label_values(&[port_role.as_str(), reason.as_str()])
            .inc();
    }

    /// Sets the current outbound sequence number for `port_role` and Channel ID.
    pub fn set_sequence(&self, port_role: PortRole, channel_id: u8, seq: i64) {
        self.sequence_current
            .with_label_values(&[port_role.as_str(), &channel_id.to_string()])
            .set(seq);
    }

    /// Sets the Unix timestamp the last heartbeat was sent for a Channel ID.
    pub fn set_heartbeat_last_sent(&self, channel_id: u8, unix_seconds: f64) {
        self.heartbeat_last_sent_timestamp_seconds
            .with_label_values(&[&channel_id.to_string()])
            .set(unix_seconds);
    }
}
