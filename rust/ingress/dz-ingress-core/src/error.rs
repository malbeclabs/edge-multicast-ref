//! What a transport reports, and what a misconfiguration reports.

use std::time::Duration;

use dz_adapter_core::DisconnectReason;

use crate::kind::Kind;

/// Why a connection was never established.
///
/// **The transport's vocabulary, and it lives here rather than in
/// `dz-adapter-core` for a reason.** That crate carries the taxonomies an
/// *adapter* can state — a parse failure, a disconnect it was told about — and
/// a connect failure is not one of them: an adapter owns no transport and
/// cannot observe a name that would not resolve or a handshake rejected for
/// credentials. Putting it there would have given the boundary a word no
/// implementor of it can say.
///
/// The seven values are the ones an operator acts differently on, and the split
/// among the last three is the one worth defending: a handshake rejected for
/// bad credentials is a secret to rotate, one rejected for too many connections
/// is a limit to respect, and one rejected for anything else is a venue change
/// to read about. Leaving all three in a detail string would have made the
/// three indistinguishable in the only place anybody looks.
///
/// Mirrors `dz_publisher_metrics::ConnectFailureReason`, which is two copies of
/// one taxonomy — the cost of this crate not making a venue link a Prometheus
/// client. They are held to each other by an exhaustive match in both
/// directions in `tests/label_taxonomies.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectFailureReason {
    /// The far side refused the connection outright.
    Refused,
    /// The endpoint's name would not resolve.
    Unresolved,
    /// The TLS negotiation failed: a certificate, a chain, a protocol version.
    Tls,
    /// The transport's own connect budget elapsed with nothing established.
    Timeout,
    /// The handshake was rejected for credentials.
    Unauthorized,
    /// The handshake was rejected for too many connections.
    RateLimit,
    /// The handshake was rejected for anything else.
    Rejected,
}

impl ConnectFailureReason {
    /// Every value, in the order the metrics crate declares them.
    pub const ALL: [Self; 7] = [
        Self::Refused,
        Self::Unresolved,
        Self::Tls,
        Self::Timeout,
        Self::Unauthorized,
        Self::RateLimit,
        Self::Rejected,
    ];

    /// The label value this reason is counted under.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Refused => "refused",
            Self::Unresolved => "unresolved",
            Self::Tls => "tls",
            Self::Timeout => "timeout",
            Self::Unauthorized => "unauthorized",
            Self::RateLimit => "rate_limit",
            Self::Rejected => "rejected",
        }
    }
}

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
    /// reconnect counter mean two things, this carries a taxonomy of its own —
    /// see [`ConnectFailureReason`] — which
    /// `dz_publisher_ingress_connect_failures_total{reason}` counts by. The
    /// connection-state gauge staying at 0 is still the signal that the
    /// upstream never came up; what was missing was any way to say *why*.
    #[error("upstream connection could not be established: {detail}")]
    Connect {
        reason: ConnectFailureReason,
        detail: String,
    },

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
    /// a scheme this transport does not speak. A process that exits loudly at
    /// startup is diagnosable, and a driver that hides the same fault behind a
    /// backoff is not.
    ///
    /// # What happens next depends on whose connection it was
    ///
    /// **A primary source's:** the process ends, and the supervisor's restart
    /// policy applies. That is the right layer, and it is also what retries the
    /// fault — several of the causes above are only fatal *for this attempt*,
    /// and a credential path that does not exist yet is the plain example: under
    /// late secret injection the same configuration succeeds on the next start.
    ///
    /// **A non-primary source's:** the driver is dropped, the publisher carries
    /// on, and that connection's `connection_state` stays at 0. Nothing retries
    /// it — **a restart is what retries it**, and until one happens the source
    /// is down for the life of the process. That is the deliberate trade: a
    /// source that by design must not reach the wire must not be able to take
    /// the wire down with it, and the cost is that a fault which used to clear
    /// on a restart the process took itself now needs one somebody takes. The
    /// gauge at 0 is the signal.
    #[error("upstream connection is not usable as configured: {detail}")]
    Fatal { detail: String },
}

impl IngressError {
    /// A connection that was never established.
    pub fn connect(reason: ConnectFailureReason, detail: impl Into<String>) -> Self {
        Self::Connect {
            reason,
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
    /// No transport was named at all.
    ///
    /// Its own error rather than an unknown kind of `""`, because the two are
    /// different mistakes: one is a value to correct, this is a decision nobody
    /// made. There is a default for neither — a transport is the one thing
    /// about an upstream that cannot be guessed — so the message names both
    /// places it can be stated and leaves the choice where it belongs.
    #[error(
        "no transport is named: state `[ingress] kind` for a publisher with one source, or          `[[source]] ingress` once per source; the built-in set is {}",
        Kind::TOKEN_LIST
    )]
    NoKind,

    /// A transport named in two places at once.
    ///
    /// Refused rather than resolved in favour of one. A key that is read only
    /// when another is absent is a key an operator cannot reason about from the
    /// file in front of them, and the audit's own failure was a value somebody
    /// believed was in force while another one was.
    #[error(
        "the transport is named twice: `[ingress] kind = \"{document}\"` and `[[source]] \
         ingress` on {sources} source(s); name it in one place"
    )]
    KindNamedTwice { document: String, sources: usize },

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

    /// A send rate finer than the clock that paces it.
    ///
    /// The limiter spaces sends by a whole number of nanoseconds, so a rate
    /// above one per nanosecond has no interval to be paced at: the division
    /// gives zero and every send goes immediately. Refused rather than
    /// silently unpaced, because the number was written by somebody who wanted
    /// pacing — a rate this size is a keystroke, not a decision, and the
    /// failure it would otherwise produce is a publisher hammering a venue
    /// while its configuration says it is being polite.
    #[error(
        "`[ingress] {key}` is {stated} per second, which is finer than the \
         nanosecond the limiter paces by; the most it can express is \
         {most} per second, and `0` disables pacing"
    )]
    RateTooFine {
        key: &'static str,
        stated: u32,
        most: u32,
    },
}
