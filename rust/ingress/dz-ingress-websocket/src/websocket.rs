//! The transport.

use std::sync::Arc;
use std::time::Duration;

use dz_adapter_core::{ConnectionId, DisconnectReason};
use dz_ingress_core::{
    BoxFuture, ConnectFailureReason, IngressError, Input, Received, UpstreamMessage,
};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Instant};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};

/// The open connection.
type Stream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// What computes the upgrade request's headers for one connect attempt.
///
/// `Send` because [`Input`] is: a transport is held by a driver that a binary
/// is free to run on a multi-threaded runtime, so a provider that could not
/// cross a thread would make this struct the one `Input` implementation that
/// cannot be one. It is the venue's own signing routine either way, and a
/// signing routine that is not `Send` is a design to fix there.
type HeaderProvider = Box<dyn Fn() -> Result<Vec<(String, String)>, String> + Send>;

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
    /// Computes the headers for each connect attempt, when the venue
    /// authenticates its upgrade. See [`WebSocketInput::with_headers`] for why
    /// this is a closure and not a list of headers.
    headers: Option<HeaderProvider>,
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
            headers: None,
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

    /// Headers for the upgrade request, computed again for **every** connect
    /// attempt.
    ///
    /// A closure and not a list of headers because the recomputation is the
    /// point. A venue that authenticates the upgrade signs a fresh millisecond
    /// timestamp — a key, a timestamp and a signature over it is the common
    /// shape — and rejects a handshake carrying an older one. Headers computed
    /// once at construction therefore give a publisher that connects and then
    /// can never reconnect: the fault appears the first time the venue drops
    /// the connection, which is hours after anyone was watching.
    ///
    /// Optional. A venue that needs no credential leaves it unset and the
    /// request goes out exactly as the library built it.
    ///
    /// Each name is set rather than appended, so the last of a repeated name
    /// wins. Two values for one credential header is a mistake either way, and
    /// sending both is the worse of the two ways to be wrong.
    ///
    /// **The provider must not name a key in the error it returns.** That text
    /// reaches an [`IngressError`] detail and from there a log line, and it is
    /// the one string on this path that this crate cannot keep credentials out
    /// of, because the provider wrote it.
    ///
    /// # What a connect makes of it
    ///
    /// Nothing runs here, so this call cannot fail. [`Input::connect`] is what
    /// reports the two ways the provider can be unusable, and they are not
    /// reported the same way:
    ///
    /// - **An error from the provider is [`IngressError::Connect`] with
    ///   [`ConnectFailureReason::Unauthorized`], and so retried.** The case
    ///   that decides it is a signing key whose file the mounting agent has not
    ///   written yet: a publisher that stopped for that would have stopped for
    ///   a condition which clears in seconds. `Unauthorized` rather than
    ///   `Rejected` because a reason is an operator's next action and not a
    ///   record of who said no — *look at the credential* is what a 401 asks
    ///   for as well, and one series is what puts the two in front of the same
    ///   person. Not `Ended`, because nothing was established.
    /// - **A name or a value that is not valid HTTP is
    ///   [`IngressError::Fatal`].** A name is a literal in the venue's code and
    ///   is the same string on the next attempt, so retrying it under a backoff
    ///   only hides it — the reason [`new`](Self::new) refuses a misspelled
    ///   endpoint at startup rather than at the first connect. A value *is*
    ///   recomputed, and it is still fatal: a signing routine that puts a
    ///   control character in a header value puts one there every time, and
    ///   nothing here can tell that from a value which would have been fine a
    ///   second later. Of the two mistakes available, the loud one is the
    ///   recoverable one — the runtime's restart policy acts on it, and a
    ///   dashboard nobody reads does not.
    ///
    /// A rejected pair is reported by its **position** in the provider's list
    /// and never by its content. The way a name comes to be invalid is a value
    /// written into the name slot — a base64 signature is not a valid header
    /// name — so printing the name would put the signature in the log line
    /// this type's [`Debug`] exists to keep it out of. A value's *name* is
    /// printed once it has parsed: it is part of the venue's published API, and
    /// it is what says which of three headers to go and look at.
    #[must_use]
    pub fn with_headers(
        mut self,
        provider: impl Fn() -> Result<Vec<(String, String)>, String> + Send + 'static,
    ) -> Self {
        self.headers = Some(Box::new(provider));
        self
    }

    /// The provider's headers, on the request the handshake is about to send.
    ///
    /// Takes the request by value and hands it back, so that there is no
    /// half-populated request to connect with by mistake.
    fn authenticate(&self, mut request: Request) -> Result<Request, IngressError> {
        let Some(provider) = self.headers.as_ref() else {
            return Ok(request);
        };
        let headers = provider().map_err(|error| {
            IngressError::connect(
                ConnectFailureReason::Unauthorized,
                format!(
                    "{}: the upgrade could not be signed: {error}",
                    self.authority
                ),
            )
        })?;
        for (position, (name, value)) in headers.iter().enumerate() {
            let name = HeaderName::try_from(name.as_str()).map_err(|_| {
                IngressError::fatal(format!(
                    "the name of the header at position {position} is not a valid \
                     HTTP header name (its text is withheld: see `with_headers`)"
                ))
            })?;
            let value = HeaderValue::try_from(value.as_str()).map_err(|_| {
                IngressError::fatal(format!(
                    "the value computed for `{name}` is not a valid HTTP header value"
                ))
            })?;
            request.headers_mut().insert(name, value);
        }
        Ok(request)
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

/// Prints the connection, the host and whether it is up — and **neither the
/// endpoint nor anything a header provider computed**.
///
/// A venue endpoint carrying a key in its query string is a shape several APIs
/// use, and configuration keeps credentials in files precisely so that they do
/// not end up in a log line. A derived implementation would put one there the
/// first time somebody logged this struct.
///
/// The same standard covers [`WebSocketInput::with_headers`], where a signature
/// is one of the values: whether the upgrade is signed is printed, because that
/// is what an operator reading a 401 wants to know, and what it is signed with
/// is not.
impl core::fmt::Debug for WebSocketInput {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WebSocketInput")
            .field("connection", &self.connection)
            .field("authority", &self.authority)
            .field("connected", &self.stream.is_some())
            // Whether the upgrade is signed, which is a real question when a
            // venue answers with a 401 - and the only part of a credential
            // that is safe to print.
            .field("authenticated", &self.headers.is_some())
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
            // Signed here and not at construction: the signature covers a
            // timestamp, and a reconnect an hour later must not carry the one
            // the process started with. See `with_headers`.
            let request = self.authenticate(request)?;
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
                    return Err(IngressError::connect(
                        ConnectFailureReason::Timeout,
                        format!("no handshake with {} within {budget:?}", self.endpoint),
                    ))
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
        // **The status is classified rather than left in the detail.** A
        // handshake rejected for credentials is a secret to rotate, one
        // rejected for too many connections is a limit to respect, and the two
        // want different people woken up — a distinction that a string nobody
        // groups by cannot make.
        WsError::Http(response) => {
            let status = response.status();
            let reason = match status.as_u16() {
                401 | 403 => ConnectFailureReason::Unauthorized,
                429 => ConnectFailureReason::RateLimit,
                _ => ConnectFailureReason::Rejected,
            };
            IngressError::connect(
                reason,
                format!("{endpoint} refused the handshake with status {status}"),
            )
        }
        // Everything else the library reports at connect time: a refused
        // socket, a name that would not resolve, a TLS negotiation that
        // failed. `tokio-tungstenite` flattens all three into `Io` and `Tls`
        // variants whose inner kinds are the only thing that separates them.
        WsError::Io(inner) => {
            let reason = match inner.kind() {
                std::io::ErrorKind::ConnectionRefused => ConnectFailureReason::Refused,
                std::io::ErrorKind::TimedOut => ConnectFailureReason::Timeout,
                // A name that would not resolve arrives here on every platform
                // this runs on, and there is no stable `ErrorKind` for it —
                // `HostUnreachable` and friends are unstable, so the string is
                // the only signal. Matched loosely and documented, because the
                // alternative is counting every socket error as a refusal.
                _ if inner.to_string().contains("resolve")
                    || inner.to_string().contains("name") =>
                {
                    ConnectFailureReason::Unresolved
                }
                _ => ConnectFailureReason::Refused,
            };
            IngressError::connect(reason, format!("{endpoint}: {inner}"))
        }
        WsError::Tls(inner) => {
            IngressError::connect(ConnectFailureReason::Tls, format!("{endpoint}: {inner}"))
        }
        other => IngressError::connect(
            ConnectFailureReason::Refused,
            format!("{endpoint}: {other}"),
        ),
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

    #[test]
    fn a_debug_line_says_that_the_upgrade_is_signed_and_not_what_with() {
        // The signature is the credential, so the same rule as the endpoint's
        // query string applies to it. Whether there is one at all is worth
        // printing: it is the first question a 401 raises.
        let input = WebSocketInput::new(ConnectionId::new("mktdata"), "wss://example.com/stream")
            .expect("a well-formed endpoint")
            .with_headers(|| {
                Ok(vec![(
                    "x-venue-access-signature".to_string(),
                    "not-a-real-signature".to_string(),
                )])
            });
        let rendered = format!("{input:?}");
        assert!(!rendered.contains("not-a-real-signature"), "{rendered}");
        assert!(rendered.contains("authenticated: true"), "{rendered}");
    }
}
