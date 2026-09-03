use std::collections::HashMap;

use prometheus::{IntCounterVec, Registry};

use crate::labels::LoweringRefusalReason;
use crate::opts::opts;

/// Metrics for the step between ingress and egress: turning a normalized event
/// into the wire's exact integers.
///
/// Its own group rather than a family on [`IngressMetrics`](crate::IngressMetrics)
/// or [`EgressMetrics`](crate::EgressMetrics), because a lowering refusal
/// happens after the payload was read and before a datagram existed. Grouping
/// it with either would put it on a dashboard row an operator reads as
/// "upstream" or as "the wire", and it is neither: it is this publisher's own
/// instrument table and exponents disagreeing with what the venue quoted.
pub struct LoweringMetrics {
    refusals_total: IntCounterVec,
}

impl LoweringMetrics {
    pub(crate) fn new(registry: &Registry, labels: &HashMap<String, String>) -> Self {
        let refusals_total = IntCounterVec::new(
            opts(
                "dz_publisher_lowering_refusals_total",
                "Normalized events refused before reaching the wire, by reason. A proposed \
                 addition to the normative set, not yet in the governing playbook: five reasons \
                 stay distinguishable in the returned error and no existing family can hold them \
                 - `dz_publisher_ingress_parse_errors_total` is about reading an upstream payload \
                 and names none of them, and `dz_publisher_egress_errors_total` is about a \
                 datagram, a port role and a socket, none of which a refused event ever reached. \
                 Every refusal is per-event: the event is dropped and the next one taken, so a \
                 rate here is one instrument's exponent or contract size being wrong rather than \
                 a feed being down.",
                labels,
            ),
            &["reason"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(refusals_total.clone()))
            .expect("static metric registration");
        // `reason` is a closed enum: pre-create every child so the family
        // exists at 0 from startup rather than appearing only after the
        // first refusal. A publisher whose instrument table and exponents
        // are right refuses nothing for its whole run, and a panel that
        // renders "no data" for that is indistinguishable from one whose
        // publisher does not implement the family at all.
        for reason in LoweringRefusalReason::ALL {
            refusals_total.with_label_values(&[reason.as_str()]);
        }

        Self { refusals_total }
    }

    /// Records one event refused by the lowering.
    ///
    /// Takes no `instrument_id`, like every other method in this crate: the
    /// instrument a refusal is about belongs in the log line beside the field
    /// name, and as a label it would multiply this family by the instrument
    /// count.
    ///
    /// Counts the family this crate proposes rather than one the playbook
    /// carries; see [`LoweringRefusalReason`].
    pub fn refusal(&self, reason: LoweringRefusalReason) {
        self.refusals_total
            .with_label_values(&[reason.as_str()])
            .inc();
    }
}
