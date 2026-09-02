//! What a transport reports, and what a misconfiguration reports.

use std::time::Duration;

use dz_adapter_core::DisconnectReason;

use crate::kind::Kind;

/// What went wrong on the upstream connection.
///
/// Three variants, because the driver takes exactly three different actions and
/// a transport that cannot say which one it wants has forced the driver to
/// guess. The distinction that matters most is the third: **an error retrying
/// cannot fix must be able to say so.** A publisher pointed at an endpoint that
/// does not parse as one, or holding a credential file that is not there, would
/// otherwise reconnect against it every 30 seconds for as long as nobody looks
/// at a dashboard.
///
/// `detail` is an owned `String` here, unlike the boundary's
/// [`ParseError`](dz_adapter_core::ParseError), which spends nothing on a
/// failure it may see thousands of times a second. These arise once per
/// connection at most, and what an operator needs from them — the address that
/// was refused, the status the handshake came back with — is not a
/// `&'static str`.
#[derive(Debug, thiserror::Error)]
pub enum IngressError {
    /// The connection was never established: refused, unresolvable, a TLS
    /// negotiation that failed, a handshake the far side rejected.
    ///
    /// **Deliberately not carrying a [`DisconnectReason`].** That taxonomy is a
    /// metric label with four values, all of which describe a session that
    /// existed and then stopped; none of them describes one that never started.
    /// Rather than fold a connect refusal into `remote_close` and make the
    /// reconnect counter mean two things, the driver counts nothing here and
    /// leaves `dz_publisher_ingress_connection_state` at 0 — which is the
    /// series that family's own documentation says exists for exactly this
    /// case, the publisher whose upstream never came up at all.
    #[error("upstream connection could not be established: {detail}")]
    Connect { detail: String },

    /// An established connection ended, for one of the four reasons the
    /// reconnect metric counts by.
    ///
    /// The transport chooses the reason, because it is the only layer that can:
    /// a handshake status, a close code, a read that timed out. See
    /// [`DisconnectReason`] for what the four are and why there are four.
    #[error("upstream connection ended: {detail}")]
    Ended {
        reason: DisconnectReason,
        detail: String,
    },

    /// Retrying cannot help, and the driver stops.
    ///
    /// For a configuration the transport can only discover at connect time — an
    /// endpoint that is not a valid one, a credential path that does not exist,
    /// a scheme this transport does not speak. The runtime's own restart policy
    /// then applies, which is the right layer: a process that exits loudly at
    /// startup is diagnosable, and a driver that hides the same fault behind a
    /// backoff is not.
    #[error("upstream connection is not usable as configured: {detail}")]
    Fatal { detail: String },
}

impl IngressError {
    /// A connection that was never established.
    pub fn connect(detail: impl Into<String>) -> Self {
        Self::Connect {
            detail: detail.into(),
        }
    }

    /// An established connection that ended, and why.
    pub fn ended(reason: DisconnectReason, detail: impl Into<String>) -> Self {
        Self::Ended {
            reason,
            detail: detail.into(),
        }
    }

    /// A fault retrying cannot fix.
    pub fn fatal(detail: impl Into<String>) -> Self {
        Self::Fatal {
            detail: detail.into(),
        }
    }

    /// The reason to tell the adapter, when this error ended a connection it
    /// had been told about.
    ///
    /// `None` for [`Connect`](Self::Connect), which ended nothing.
    #[must_use]
    pub const fn disconnect_reason(&self) -> Option<DisconnectReason> {
        match self {
            Self::Ended { reason, .. } => Some(*reason),
            // A fatal fault on a live connection still ends it, and the adapter
            // is still owed the pairing (see `Driver`), but the reason is the
            // least specific of the four rather than an invented fifth.
            Self::Fatal { .. } => Some(DisconnectReason::RemoteClose),
            Self::Connect { .. } => None,
        }
    }

    /// Whether the driver must stop rather than retry.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::Fatal { .. })
    }
}

/// Why an `[ingress]` section cannot be run.
///
/// Every variant names what *is* acceptable, not only what was wrong. The
/// audit's own lesson is the reason: a publisher with a misspelled section
/// parsed cleanly, fell back to a default, and ran a transport the operator did
/// not believe it was running. An error that says `"websocket" is not a
/// transport` and stops there invites the same guess a second time.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// `kind` is not one of the transports in this family.
    #[error(
        "`[ingress] kind = \"{token}\"` names no transport in this family; the built-in set is {}",
        Kind::TOKEN_LIST
    )]
    UnknownKind { token: String },

    /// `kind` names a transport in the family that this binary was not built
    /// with.
    ///
    /// A different error from [`UnknownKind`](Self::UnknownKind) on purpose,
    /// and the difference is the operator's next action: one is a typo to fix
    /// in the file, the other is a build to redo. Collapsing them into
    /// *unknown* sends someone hunting for a spelling mistake in a value that
    /// is spelled correctly.
    #[error(
        "`[ingress] kind = \"{token}\"` is a transport in this family, but this binary was not \
         built with it; linked in this binary: {}",
        Kind::linked_list()
    )]
    KindNotLinked { token: &'static str },

    /// The backoff would start above its own ceiling.
    ///
    /// Refused rather than silently clamped: a pair that way round is a
    /// transposed pair of keys, and clamping it produces a publisher that waits
    /// its maximum before its first retry while the file says it waits half a
    /// second.
    #[error(
        "`[ingress] reconnect_backoff_initial` ({initial:?}) is longer than \
         `reconnect_backoff_max` ({max:?})"
    )]
    BackoffInverted { initial: Duration, max: Duration },

    /// A zero backoff.
    ///
    /// Refused because doubling it never leaves zero: a venue that accepts a
    /// connection and drops it becomes an unbounded reconnect loop against
    /// that venue, which is how a publisher gets its address banned rather
    /// than merely staying down.
    #[error("`[ingress] {key}` must be greater than zero")]
    ZeroDuration { key: &'static str },
}
