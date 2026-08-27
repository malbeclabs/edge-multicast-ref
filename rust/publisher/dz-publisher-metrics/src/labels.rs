//! Closed label-value vocabularies.
//!
//! Every `reason`, `kind`, and `outcome` label in this crate is one of these
//! enums rather than a free-form string, so the taxonomy a dashboard groups
//! by cannot drift one call site at a time.

/// Why an ingress message failed to parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ParseErrorReason {
    Schema,
    UnknownField,
    Malformed,
    Truncated,
}

impl ParseErrorReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Schema => "schema",
            Self::UnknownField => "unknown_field",
            Self::Malformed => "malformed",
            Self::Truncated => "truncated",
        }
    }
}

/// Why an ingress connection reconnected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReconnectReason {
    Timeout,
    RemoteClose,
    RateLimit,
    AuthExpired,
}

impl ReconnectReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::RemoteClose => "remote_close",
            Self::RateLimit => "rate_limit",
            Self::AuthExpired => "auth_expired",
        }
    }
}

/// The kind of book inconsistency detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InconsistencyKind {
    MissingLevel,
    CrossedBook,
    SnapshotMismatch,
    SequenceGap,
}

impl InconsistencyKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MissingLevel => "missing_level",
            Self::CrossedBook => "crossed_book",
            Self::SnapshotMismatch => "snapshot_mismatch",
            Self::SequenceGap => "sequence_gap",
        }
    }
}

/// The outcome of a book recovery attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecoveryOutcome {
    Success,
    Failed,
}

impl RecoveryOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failed => "failed",
        }
    }
}

/// Why a reference-data load failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RefdataLoadErrorReason {
    Timeout,
    RateLimit,
    Schema,
    Unavailable,
}

impl RefdataLoadErrorReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::RateLimit => "rate_limit",
            Self::Schema => "schema",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Why an egress send failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EgressErrorReason {
    MtuExceeded,
    SendWouldBlock,
    SocketError,
    NotRegistered,
    WrongPortRole,
}

impl EgressErrorReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MtuExceeded => "mtu_exceeded",
            Self::SendWouldBlock => "send_would_block",
            Self::SocketError => "socket_error",
            Self::NotRegistered => "not_registered",
            Self::WrongPortRole => "wrong_port_role",
        }
    }
}

/// Which upstream timestamp a venue-to-recv latency observation is measured
/// against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimestampKind {
    ExchangeRecv,
    MatchingEngine,
    GatewaySend,
    BlockTime,
}

impl TimestampKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExchangeRecv => "exchange_recv",
            Self::MatchingEngine => "matching_engine",
            Self::GatewaySend => "gateway_send",
            Self::BlockTime => "block_time",
        }
    }
}

/// Which kind of event a recv-to-send latency observation covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventKind {
    BookUpdate,
    Trade,
}

impl EventKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BookUpdate => "book_update",
            Self::Trade => "trade",
        }
    }
}

/// Why the process exited.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExitReason {
    IdleGuard,
    ConsistencyGuard,
    Signal,
    Panic,
}

impl ExitReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::IdleGuard => "idle_guard",
            Self::ConsistencyGuard => "consistency_guard",
            Self::Signal => "signal",
            Self::Panic => "panic",
        }
    }
}
