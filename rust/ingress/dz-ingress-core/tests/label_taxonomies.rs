//! The boundary's two taxonomies against the label enums they are recorded as.
//!
//! [`IngressObserver`](dz_ingress_core::IngressObserver) hands the runtime a
//! [`DisconnectReason`] and a [`ParseError`], and the runtime has to turn each
//! into a label value. That mapping exists because this crate does not depend on
//! the metrics crate — a venue must not inherit a Prometheus client to be told a
//! socket closed — and a mapping nobody compiles is a mapping that drifts.
//!
//! So the mapping is written here, over a dev-dependency: dev-dependencies are
//! not inherited by a venue, which is what makes it affordable. Both matches are
//! exhaustive on both sides, so a variant added to either taxonomy without a
//! partner fails to compile rather than producing a series no dashboard groups
//! by. The tokens are then compared as strings, because two enums can be the
//! same arity and still disagree about a spelling — and the label value is what
//! reaches the dashboard.

use dz_adapter_core::{DisconnectReason, ParseError};
use dz_publisher_metrics::{ParseErrorReason, ReconnectReason};

/// The mapping the runtime performs for
/// `dz_publisher_ingress_reconnects_total{reason}`.
fn as_label(reason: DisconnectReason) -> ReconnectReason {
    match reason {
        DisconnectReason::Timeout => ReconnectReason::Timeout,
        DisconnectReason::RemoteClose => ReconnectReason::RemoteClose,
        DisconnectReason::RateLimit => ReconnectReason::RateLimit,
        DisconnectReason::AuthExpired => ReconnectReason::AuthExpired,
    }
}

/// The mapping the runtime performs for
/// `dz_publisher_ingress_parse_errors_total{reason}`.
fn as_parse_label(error: ParseError) -> ParseErrorReason {
    match error {
        ParseError::Schema { .. } => ParseErrorReason::Schema,
        ParseError::UnknownField { .. } => ParseErrorReason::UnknownField,
        ParseError::Malformed { .. } => ParseErrorReason::Malformed,
        ParseError::Truncated { .. } => ParseErrorReason::Truncated,
    }
}

#[test]
fn every_disconnect_reason_the_driver_can_reach_is_a_reconnect_label() {
    for reason in DisconnectReason::ALL {
        assert_eq!(
            reason.as_str(),
            as_label(reason).as_str(),
            "the two spellings of {reason:?} have diverged"
        );
    }
}

#[test]
fn every_parse_error_the_driver_can_record_is_a_parse_error_label() {
    for error in ParseError::ALL {
        assert_eq!(
            error.as_str(),
            as_parse_label(error).as_str(),
            "the two spellings of {error:?} have diverged"
        );
    }
}
