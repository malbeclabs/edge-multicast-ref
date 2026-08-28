//! Every `reason`/`kind`/`outcome` label value enum must return its exact
//! wire token from `as_str()`.

use dz_publisher_metrics::{
    EgressErrorReason, EventKind, ExitReason, InconsistencyKind, ParseErrorReason, ReconnectReason,
    RecoveryOutcome, RefdataLoadErrorReason, TimestampKind,
};

#[test]
fn parse_error_reason_tokens() {
    assert_eq!(ParseErrorReason::Schema.as_str(), "schema");
    assert_eq!(ParseErrorReason::UnknownField.as_str(), "unknown_field");
    assert_eq!(ParseErrorReason::Malformed.as_str(), "malformed");
    assert_eq!(ParseErrorReason::Truncated.as_str(), "truncated");
}

#[test]
fn reconnect_reason_tokens() {
    assert_eq!(ReconnectReason::Timeout.as_str(), "timeout");
    assert_eq!(ReconnectReason::RemoteClose.as_str(), "remote_close");
    assert_eq!(ReconnectReason::RateLimit.as_str(), "rate_limit");
    assert_eq!(ReconnectReason::AuthExpired.as_str(), "auth_expired");
}

#[test]
fn inconsistency_kind_tokens() {
    assert_eq!(InconsistencyKind::MissingLevel.as_str(), "missing_level");
    assert_eq!(InconsistencyKind::CrossedBook.as_str(), "crossed_book");
    assert_eq!(
        InconsistencyKind::SnapshotMismatch.as_str(),
        "snapshot_mismatch"
    );
    assert_eq!(InconsistencyKind::SequenceGap.as_str(), "sequence_gap");
}

#[test]
fn recovery_outcome_tokens() {
    assert_eq!(RecoveryOutcome::Success.as_str(), "success");
    assert_eq!(RecoveryOutcome::Failed.as_str(), "failed");
}

#[test]
fn refdata_load_error_reason_tokens() {
    assert_eq!(RefdataLoadErrorReason::Timeout.as_str(), "timeout");
    assert_eq!(RefdataLoadErrorReason::RateLimit.as_str(), "rate_limit");
    assert_eq!(RefdataLoadErrorReason::Schema.as_str(), "schema");
    assert_eq!(RefdataLoadErrorReason::Unavailable.as_str(), "unavailable");
}

#[test]
fn egress_error_reason_tokens() {
    assert_eq!(EgressErrorReason::MtuExceeded.as_str(), "mtu_exceeded");
    assert_eq!(
        EgressErrorReason::SendWouldBlock.as_str(),
        "send_would_block"
    );
    assert_eq!(EgressErrorReason::SocketError.as_str(), "socket_error");
    assert_eq!(EgressErrorReason::NotRegistered.as_str(), "not_registered");
    assert_eq!(EgressErrorReason::WrongPortRole.as_str(), "wrong_port_role");
}

#[test]
fn timestamp_kind_tokens() {
    assert_eq!(TimestampKind::ExchangeRecv.as_str(), "exchange_recv");
    assert_eq!(TimestampKind::MatchingEngine.as_str(), "matching_engine");
    assert_eq!(TimestampKind::GatewaySend.as_str(), "gateway_send");
    assert_eq!(TimestampKind::BlockTime.as_str(), "block_time");
}

#[test]
fn event_kind_tokens() {
    assert_eq!(EventKind::BookUpdate.as_str(), "book_update");
    assert_eq!(EventKind::Trade.as_str(), "trade");
}

#[test]
fn exit_reason_tokens() {
    assert_eq!(ExitReason::IdleGuard.as_str(), "idle_guard");
    assert_eq!(ExitReason::ConsistencyGuard.as_str(), "consistency_guard");
    assert_eq!(ExitReason::Signal.as_str(), "signal");
    assert_eq!(ExitReason::Panic.as_str(), "panic");
}
