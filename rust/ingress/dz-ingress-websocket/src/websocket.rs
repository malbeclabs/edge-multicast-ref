//! The transport.

use std::sync::Arc;
use std::time::Duration;

use dz_adapter_core::{ConnectionId, DisconnectReason};
use dz_ingress_core::{BoxFuture, IngressError, Input, Received, UpstreamMessage};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Instant};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};

/// The open connection.
type Stream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// How often to ping when nothing has arrived, and therefore also how long a
/// ping may go unanswered.
///
/// Fifteen seconds gives a half-open socket a bounded lifetime of two intervals
/// while costing two messages a minute on an idle connection. It is not the
/// upstream silence guard: that is `[ingress] idle_timeout` and it is measured
/// in payloads, because a venue that has quietly dropped a subscription answers
/// pings perfectly.
const DEFAULT_PING_INTERVAL: Duration = Duration::from_secs(15);

/// The largest message this transport will assemble.
///
/// A venue's snapshot message is the large one, and eight megabytes is well
/// past any of them. It exists because the far side chooses the size and the
/// buffer is ours: the default in the library is 64 MiB, which is a memory
/// commitment made by whoever is on the other end of the socket.
const DEFAULT_MAX_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

/// How long an orderly close may take before the socket is simply dropped.
///
/// A close handshake waits for the peer, and the common reason to be closing is
/// that the peer has stopped answering. Waiting on it would put this delay in
/// front of every reconnect.
const CLOSE_GRACE: Duration = Duration::from_millis(250);

/// A websocket [`Input`].
///
/// One instance is one connection. A publisher taking first-copy-wins from two
/// upstreams holds two of these with two [`ConnectionId`]s, which is what makes
/// `dz_publisher_ingress_connection_state{connection}` say which one is down.
pub struct WebSocketInput {
    connection: ConnectionId,
    endpoint: String,
    /// The scheme, host and port of `endpoint`, and nothing else. See the
    /// `Debug` implementation for why the rest is dropped.
    authority: String,
    ping_interval: Duration,
    max_message_bytes: usize,
    stream: Option<Stream>,
    /// The message the last [`Input::recv`] handed out, kept alive because the
    /// payload borrows its bytes. Overwritten by the next receive, which is
    /// sound because the borrow checker will not let the driver ask for one
    /// while it still holds the last.
    held: Option<Message>,
    /// Set when a ping has gone out and no pong has come back. A second ping
    /// falling due while this is set is the socket being gone.
    awaiting_pong: bool,
    /// When the next ping falls due.
    next_ping_at: Option<Instant>,
}

impl WebSocketInput {
    /// A transport for one endpoint.
    ///
    /// # Errors
    ///
    /// [`IngressError::Fatal`] when the endpoint is not a websocket URL. Raised
    /// here rather than at the first connect on purpose: a publisher whose
    /// endpoint is misspelled should fail at startup, where it is diagnosable,
    /// instead of retrying against it under a backoff for as long as nobody
    /// looks at a dashboard. The library's own parser is what decides, so this
    /// accepts exactly what a connect would.
    pub fn new(
        connection: ConnectionId,
        endpoint: impl Into<String>,
    ) -> Result<Self, IngressError> {
        let endpoint = endpoint.into();
        let request = endpoint
            .as_str()
            .into_client_request()
            .map_err(|error| IngressError::fatal(format!("`{endpoint}` is not usable: {error}")))?;
        let scheme = match request.uri().scheme_str() {
            Some(scheme @ ("ws" | "wss")) => scheme,
            other => {
                return Err(IngressError::fatal(format!(
                    "`{endpoint}` has scheme {other:?}; a websocket endpoint is ws:// or wss://"
                )))
            }
        };
        let authority = match (request.uri().host(), request.uri().port_u16()) {
            (Some(host), Some(port)) => format!("{scheme}://{host}:{port}"),
            (Some(host), None) => format!("{scheme}://{host}"),
            (None, _) => format!("{scheme}://?"),
        };
        Ok(Self {
            connection,
            endpoint,
            authority,
            ping_interval: DEFAULT_PING_INTERVAL,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            stream: None,
            held: None,
            awaiting_pong: false,
            next_ping_at: None,
        })
    }

    /// How often to ping an otherwise silent connection, which is also the
    /// grace a pong is given. See [`DEFAULT_PING_INTERVAL`].
    #[must_use]
    pub const fn with_ping_interval(mut self, interval: Duration) -> Self {
        self.ping_interval = interval;
        self
    }

    /// The largest message to assemble. See [`DEFAULT_MAX_MESSAGE_BYTES`].
    #[must_use]
    pub const fn with_max_message_bytes(mut self, bytes: usize) -> Self {
        self.max_message_bytes = bytes;
        self
    }

    /// The TLS connector, with the provider named rather than discovered.
    ///
    /// `rustls::ClientConfig::builder()` takes the process-wide default
    /// provider, or the one a feature installed, or panics. All three are ways
    /// of deciding this somewhere other than here, and the panic arrives on the
    /// first `wss://` connect — which is the one moment a suite that cannot use
    /// the network never reaches. So the provider is constructed explicitly.
    fn tls_connector(&self) -> Result<Connector, IngressError> {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|error| {
                // A build whose TLS provider cannot offer a protocol version is
                // not one a retry improves.
                IngressError::fatal(format!("the TLS provider is unusable: {error}"))
            })?
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Connector::Rustls(Arc::new(config)))
    }

    /// The open stream, or the error that says we are not connected.
    ///
    /// A driver cannot reach this state — it connects before it receives — so
    /// this is the case where something else drove the transport. Reported as
    /// an ended connection rather than a panic: a publisher is a long-running
    /// process and reconnecting is a better answer than exiting.
    fn stream(&mut self) -> Result<&mut Stream, IngressError> {
        self.stream
            .as_mut()
            .ok_or_else(|| IngressError::ended(DisconnectReason::RemoteClose, "not connected"))
    }
}

/// Prints the connection, the host and whether it is up — and **not the
/// endpoint**.
///
/// A venue endpoint carrying a key in its query string is a shape several APIs
/// use, and configuration keeps credentials in files precisely so that they do
/// not end up in a log line. A derived implementation would put one there the
/// first time somebody logged this struct.
impl core::fmt::Debug for WebSocketInput {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WebSocketInput")
            .field("connection", &self.connection)
            .field("authority", &self.authority)
            .field("connected", &self.stream.is_some())
            .finish()
    }
}

impl Input for WebSocketInput {
    fn connection(&self) -> ConnectionId {
        self.connection
    }

    fn connect(&mut self, budget: Duration) -> BoxFuture<'_, Result<(), IngressError>> {
        Box::pin(async move {
            let request = self
                .endpoint
                .as_str()
                .into_client_request()
                .map_err(|error| IngressError::fatal(format!("endpoint unusable: {error}")))?;
            let connector = self.tls_connector()?;
            let config = WebSocketConfig::default().max_message_size(Some(self.max_message_bytes));
            // Nagle off. A subscription message that sits in a kernel buffer
            // waiting for company delays the whole feed behind it, and the
            // messages this transport sends are small and few.
            let disable_nagle = true;
            let attempt = tokio_tungstenite::connect_async_tls_with_config(
                request,
                Some(config),
                disable_nagle,
                Some(connector),
            );
            let (stream, _response) = match timeout(budget, attempt).await {
                Err(_elapsed) => {
                    return Err(IngressError::connect(format!(
                        "no handshake with {} within {budget:?}",
                        self.endpoint
                    )))
                }
                Ok(Err(error)) => return Err(classify_handshake(&self.endpoint, error)),
                Ok(Ok(established)) => established,
            };
            self.stream = Some(stream);
            self.held = None;
            self.awaiting_pong = false;
            self.next_ping_at = Some(Instant::now() + self.ping_interval);
            Ok(())
        })
    }

    fn send<'a>(
        &'a mut self,
        message: UpstreamMessage<'a>,
    ) -> BoxFuture<'a, Result<(), IngressError>> {
        Box::pin(async move {
            let outgoing = match message {
                UpstreamMessage::Text(text) => Message::text(text),
                UpstreamMessage::Binary(bytes) => Message::binary(bytes.to_vec()),
            };
            self.stream()?.send(outgoing).await.map_err(|error| {
                // Every send failure ends the connection: there is no partial
                // success to recover from, and the adapter's subscriptions are
                // re-issued on the next connect anyway.
                IngressError::ended(
                    DisconnectReason::RemoteClose,
                    format!("send failed: {error}"),
                )
            })
        })
    }

    fn recv<'a>(
        &'a mut self,
        budget: Option<Duration>,
    ) -> BoxFuture<'a, Result<Received<'a>, IngressError>> {
        Box::pin(async move {
            let deadline = budget.map(|budget| Instant::now() + budget);
            loop {
                let now = Instant::now();
                // The next thing to happen is whichever comes first: the
                // driver's budget running out, or a ping falling due.
                let ping_at = self
                    .next_ping_at
                    .unwrap_or_else(|| now + self.ping_interval);
                let wake_at = deadline.map_or(ping_at, |deadline| deadline.min(ping_at));

                // Abandoning this read at the timeout is safe because the
                // partial-message state is in the stream, not in the future -
                // which is the property that decided this client. See the
                // crate docs.
                match timeout(
                    wake_at.saturating_duration_since(now),
                    self.stream()?.next(),
                )
                .await
                {
                    Ok(Some(Ok(message))) => match message {
                        Message::Text(_) | Message::Binary(_) => {
                            self.held = Some(message);
                            let bytes = held_bytes(self.held.as_ref());
                            return Ok(Received::Payload { bytes, ts_ns: None });
                        }
                        Message::Pong(_) => {
                            // The socket is alive. Deliberately not a payload:
                            // the driver's silence guard counts payloads,
                            // because a venue that dropped a subscription
                            // answers pings forever.
                            self.awaiting_pong = false;
                            self.next_ping_at = Some(Instant::now() + self.ping_interval);
                            return Ok(Received::Liveness);
                        }
                        Message::Ping(_) => {
                            // Answered by the library while the stream is
                            // polled, so there is nothing to do but say the
                            // socket is alive.
                            return Ok(Received::Liveness);
                        }
                        Message::Close(closed) => {
                            let reason = closed
                                .as_ref()
                                .map_or(DisconnectReason::RemoteClose, |closed| {
                                    close_reason(u16::from(closed.code))
                                });
                            return Err(IngressError::ended(
                                reason,
                                format!("the venue closed the connection: {closed:?}"),
                            ));
                        }
                        // Not produced when reading, per the library's own
                        // documentation. Treated as an ended connection rather
                        // than ignored, because reading one would mean this
                        // transport no longer understands its own stream.
                        Message::Frame(_) => {
                            return Err(IngressError::ended(
                                DisconnectReason::RemoteClose,
                                "the stream produced a raw protocol unit",
                            ))
                        }
                    },
                    Ok(Some(Err(error))) => return Err(classify_stream(error)),
                    // The stream ended without a close: the peer went away, or
                    // something in the middle did.
                    Ok(None) => {
                        return Err(IngressError::ended(
                            DisconnectReason::RemoteClose,
                            "the stream ended without a close",
                        ))
                    }
                    Err(_elapsed) => {
                        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                            return Ok(Received::Idle);
                        }
                        if self.awaiting_pong {
                            // A ping fell due while the last one was still
                            // unanswered. Nothing has come back for two
                            // intervals, so the socket is open only as far as
                            // this host can tell - which is the failure a
                            // read timeout alone cannot see, because a
                            // half-open socket produces no error and no data.
                            return Err(IngressError::ended(
                                DisconnectReason::Timeout,
                                format!("no pong within {:?}", self.ping_interval),
                            ));
                        }
                        self.stream()?
                            .send(Message::Ping(Default::default()))
                            .await
                            .map_err(|error| {
                                IngressError::ended(
                                    DisconnectReason::RemoteClose,
                                    format!("ping failed: {error}"),
                                )
                            })?;
                        self.awaiting_pong = true;
                        self.next_ping_at = Some(Instant::now() + self.ping_interval);
                    }
                }
            }
        })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if let Some(mut stream) = self.stream.take() {
                // Bounded, and the result discarded. The usual reason to be
                // closing is that the peer has stopped answering, and waiting
                // for its half of the handshake would put this delay in front
                // of every reconnect.
                let _ = timeout(CLOSE_GRACE, stream.close(None)).await;
            }
            self.held = None;
            self.awaiting_pong = false;
            self.next_ping_at = None;
        })
    }
}

/// The bytes of the held message.
///
/// Only ever called with a text or binary message just stored by
/// [`Input::recv`], so the empty fallback is unreachable. It is an empty slice
/// rather than a panic because a transport is not a place to end a publisher's
/// process from.
fn held_bytes(held: Option<&Message>) -> &[u8] {
    match held {
        Some(Message::Text(text)) => text.as_str().as_bytes(),
        Some(Message::Binary(bytes)) => bytes,
        _ => &[],
    }
}

/// What a close code means in the four words the reconnect metric counts by.
///
/// Only three of the four are reachable from a close code — see the note on
/// `AuthExpired` — and the mapping is deliberately conservative where it is
/// unsure, because two of the four affect the reconnect delay and the safer
/// mistake is the one that waits longer.
fn close_reason(code: u16) -> DisconnectReason {
    match code {
        // 1013 *Try Again Later* and 1008 *Policy Violation* are the two codes
        // a venue reaches for when we have exceeded something. 1008 is the
        // vaguer of them, and reading it as a rate limit is the cautious
        // reading: a rate-limited connection never resets the delay sequence,
        // so the cost of being wrong is a longer wait, while the cost of the
        // other mistake is being reconnected against a venue that has just
        // told us to stop.
        1008 | 1013 => DisconnectReason::RateLimit,
        // Codes above 4000 are the venue's own, so nothing general can be said
        // about them; and a normal close, a going-away and a protocol error are
        // all just the far side ending it.
        _ => DisconnectReason::RemoteClose,
    }
}

/// A handshake that did not produce a connection.
///
/// Every one of these is [`IngressError::Connect`] or
/// [`IngressError::Fatal`], and never `Ended`: nothing was established, so
/// there is no session for the four reasons to describe. **The status the venue
/// gave is therefore in the detail and not in a label** — a rejection for bad
/// credentials and one for too many connections are the two most
/// operationally significant startup failures a venue produces, and the closed
/// ingress family has nowhere to count either. That is a gap in the family,
/// not something to paper over by folding a refused handshake into
/// `remote_close`.
fn classify_handshake(endpoint: &str, error: WsError) -> IngressError {
    match error {
        // A URL that does not parse, or a scheme this client does not speak.
        // Retrying cannot change either.
        WsError::Url(inner) => IngressError::fatal(format!("`{endpoint}` is not usable: {inner}")),
        WsError::Http(response) => IngressError::connect(format!(
            "{endpoint} refused the handshake with status {}",
            response.status()
        )),
        other => IngressError::connect(format!("{endpoint}: {other}")),
    }
}

/// An error from an established stream.
///
/// All of them are `remote_close`, and the flatness is the finding rather than
/// laziness. The four reasons are a metric label, and nothing a websocket
/// stream reports distinguishes among them: a socket error, a protocol
/// violation and the library's own end-of-close signal are all the far side or
/// the path ending it. `timeout` is reached only by this transport's own ping
/// grace, and `auth_expired` and `rate_limit` only by a close code the venue
/// chose to send. A venue that drops us for an expired session without a close
/// code is therefore counted as `remote_close`, which is true but not useful -
/// see `classify_handshake` for the same gap at the other end.
fn classify_stream(error: WsError) -> IngressError {
    IngressError::ended(DisconnectReason::RemoteClose, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_tls_connector_is_constructible_without_touching_a_network() {
        // The one part of the TLS setup that can be checked with no endpoint,
        // and the part most likely to be wrong: `rustls` panics when it has to
        // choose a crypto provider and cannot, and that panic would otherwise
        // arrive on a production `wss://` connect, which no test that can run
        // here reaches.
        let input = WebSocketInput::new(ConnectionId::new("mktdata"), "wss://example.com/stream")
            .expect("a well-formed endpoint");
        assert!(
            matches!(input.tls_connector(), Ok(Connector::Rustls(_))),
            "the connector must be the rustls one, with our own roots"
        );
    }

    #[test]
    fn the_close_codes_that_mean_slow_down_are_the_only_ones_read_as_a_rate_limit() {
        // Transcribed as literals rather than derived from the library's enum,
        // for the reason the codec's own vocabulary tests give: a table checked
        // only against itself is a table that agrees with its own mistake.
        assert_eq!(close_reason(1008), DisconnectReason::RateLimit);
        assert_eq!(close_reason(1013), DisconnectReason::RateLimit);
        for code in [1000, 1001, 1002, 1006, 1011, 1012, 4000, 4999] {
            assert_eq!(
                close_reason(code),
                DisconnectReason::RemoteClose,
                "{code} is not a statement about our rate"
            );
        }
    }

    #[test]
    fn a_debug_line_does_not_carry_the_endpoint() {
        // An endpoint carrying a key in its query string is a shape several
        // venue APIs use, and configuration keeps credentials out of the
        // document precisely so that they stay out of a log line.
        let input = WebSocketInput::new(
            ConnectionId::new("mktdata"),
            "wss://example.com/stream?api_key=not-a-real-secret",
        )
        .expect("a well-formed endpoint");
        let rendered = format!("{input:?}");
        assert!(!rendered.contains("api_key"), "{rendered}");
        assert!(rendered.contains("example.com"), "{rendered}");
    }
}
