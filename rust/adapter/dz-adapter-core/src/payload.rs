//! What the transport hands the adapter, and how it names a connection.

/// One payload from the upstream source, borrowed from the receive buffer.
///
/// What it deliberately does not carry is a statement of how `recv_ts_ns` was
/// obtained. A transport stamps every payload the same way — the kernel does it
/// or the transport does — so the kind is a property of the connection and is
/// recorded once where the ingress metrics live, not repeated on every payload
/// for an adapter that has no use for it. The archive format carries the same
/// distinction for the same reason, and duplicating that taxonomy a third time
/// here would be the drift this family of crates exists to stop.
#[derive(Debug, Clone, Copy)]
pub struct Payload<'a> {
    /// The upstream's bytes, exactly as they arrived.
    pub bytes: &'a [u8],
    /// When this payload was received, in nanoseconds. Our own clock, never the
    /// venue's — the venue's timestamp is a field inside the payload and
    /// reaches the wire through the event.
    pub recv_ts_ns: u64,
    /// Which connection delivered it.
    pub connection: ConnectionId,
}

/// The name of one upstream connection.
///
/// A `&'static str` because it is the publisher author's own label, declared at
/// startup and used as a metric label: `dz_publisher_ingress_connection_state`
/// is pre-created at 0 for each declared name so that the `== 0` alert can fire
/// on a publisher whose upstream never came up at all — the case the metric most
/// exists for. A name allocated at runtime could not be declared, so the series
/// would not exist until the connection first succeeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConnectionId(&'static str);

impl ConnectionId {
    /// Name a connection.
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    /// The name, as the metric label carries it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl core::fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.0)
    }
}

/// Why an upstream connection ended.
///
/// **These four variants are a metric label**, the same arrangement as
/// [`ParseError`](crate::ParseError): `dz_publisher_ingress_reconnects_total`
/// counts by exactly this taxonomy, so an adapter told why a connection went
/// away is told in the vocabulary the dashboard groups by. The label enum is
/// declared a second time in the metrics crate, because a venue must not
/// inherit a Prometheus client to be told a socket closed, and a test there
/// holds the two to each other by arity and token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DisconnectReason {
    /// No traffic within the transport's timeout.
    Timeout,
    /// The far side closed it.
    RemoteClose,
    /// We were rate limited off.
    RateLimit,
    /// Credentials expired and the session could not continue.
    AuthExpired,
}

impl DisconnectReason {
    /// Every variant, in the order the metrics crate declares them.
    pub const ALL: [Self; 4] = [
        Self::Timeout,
        Self::RemoteClose,
        Self::RateLimit,
        Self::AuthExpired,
    ];

    /// The label value this reason is counted under.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_reason_has_a_distinct_token() {
        let mut tokens: Vec<&str> = DisconnectReason::ALL.iter().map(|r| r.as_str()).collect();
        tokens.sort_unstable();
        let count = tokens.len();
        tokens.dedup();
        assert_eq!(tokens.len(), count, "two reasons share a label value");
    }

    #[test]
    fn a_connection_displays_as_its_label() {
        assert_eq!(ConnectionId::new("mktdata").to_string(), "mktdata");
    }
}
