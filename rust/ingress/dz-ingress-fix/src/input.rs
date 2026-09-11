//! The transport: the socket, TLS, and every failure classified.
//!
//! # Where the logon happens, and why not in `connect`
//!
//! `connect` opens the socket and performs the negotiation, and it stops there.
//! It does **not** drive the logon, because at that moment there is no logon to
//! drive: the driver connects, *then* asks the adapter what to send through
//! [`Adapter::on_connected`](dz_adapter_core::Adapter::on_connected), then
//! sends it. The logon body does not exist until after `connect` has returned.
//!
//! So the first [`Input::send`] is what carries the session to *established*:
//! it reads the cadence out of the logon body, frames it, numbers it 1, writes
//! it and waits for the answer. Nothing else may be sent before it, and a
//! receive on a session at which the adapter wrote nothing is a refusal naming
//! the adapter's method rather than a session that waits. That ordering is the
//! driver's contract, not this crate's preference, and building the logon into
//! `connect` would mean composing one here — which is this repository signing a
//! logon on a venue's behalf.
//!
//! # TLS
//!
//! `rustls` with the compiled-in webpki trust anchors and `ring` as the
//! provider, pinned exactly as `dz-ingress-websocket` pins it and for the same
//! three reasons: no system TLS library, so no build that differs by host; the
//! trust anchors in the binary, so a host with a stale CA bundle connects like
//! every other host; and `ring` rather than `aws-lc-rs`, which wants cmake and
//! a C compiler at build time.
//!
//! The provider is named in code, in `SocketConnector`'s own constructor, rather
//! than left to whichever one a feature happened to install process-wide.
//! `rustls` panics when it has to choose and cannot, and that panic would
//! arrive on the first negotiated connect — the one moment no test that can run
//! without a network reaches.
//!
//! # A session message is `Liveness` and never a payload
//!
//! The driver's idle guard counts time since the last *payload*, so a session
//! that heartbeats forever and delivers nothing has to trip it. Every one of
//! the seven session message types is therefore [`Received::Liveness`] or an
//! error, and never [`Received::Payload`].

use std::sync::Arc;
use std::time::Duration;

use dz_adapter_core::{ConnectionId, DisconnectReason};
use dz_ingress_core::{
    BoxFuture, Clock, ConnectFailureReason, IngressError, Input, Received, TokioClock,
    UpstreamMessage,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, timeout_at, Instant};
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

use crate::config::{Endpoint, SessionConfig};
use crate::session::{ByteStream, Incoming, Session, SessionError, StreamError};

/// How many bytes one read may take off the socket.
///
/// A page-sized read, because the messages this protocol carries are small and
/// the buffer is on the stack. What the decoder does with a partial message is
/// the decoder's business, so this bounds nothing but the syscall.
const READ_CHUNK: usize = 4_096;

/// What opens the byte stream a session runs over.
///
/// The seam between the session and the socket. [`SocketConnector`] is the
/// implementation a publisher runs; a test or an exercise supplies its own, and
/// that is what makes the classification below assertable without a network.
pub trait Connector: Send {
    /// Open a stream, giving up after `budget`.
    ///
    /// **`budget` is the total for everything opening a stream takes**, which
    /// for [`SocketConnector`] is the TCP connect and the TLS handshake
    /// together. So `[ingress] connect_timeout = "5s"` buys five seconds from
    /// the first packet to a stream a logon can be written on, and not five
    /// seconds each: a budget applied twice is twice the number, which is the
    /// reason the teardown's own halves share
    /// [`LOGOUT_GRACE`](crate::session::LOGOUT_GRACE). An implementation that
    /// spends it in stages carries one deadline and hands each stage what is
    /// left.
    ///
    /// # Errors
    ///
    /// [`IngressError::Connect`] with the reason an operator acts on, or
    /// [`IngressError::Fatal`] for a configuration no retry improves.
    fn open(
        &mut self,
        budget: Duration,
    ) -> BoxFuture<'_, Result<Box<dyn ByteStream>, IngressError>>;

    /// The endpoint, for a log line and an error detail.
    ///
    /// The address and nothing else: a credential lives in a file and a
    /// signature lives in the adapter, so there is nothing else here to
    /// withhold — and stating that is what keeps a future key out of it.
    fn authority(&self) -> &str;
}

/// A socket, negotiated or not, over TCP.
pub struct SocketConnector {
    endpoint: Endpoint,
    tls: Option<Arc<ClientConfig>>,
}

impl SocketConnector {
    /// A connector for a checked endpoint.
    ///
    /// # Errors
    ///
    /// [`IngressError::Fatal`] when the TLS provider in this build cannot offer
    /// a protocol version, which is not a fault a retry improves.
    pub fn new(endpoint: Endpoint) -> Result<Self, IngressError> {
        let tls = if endpoint.tls {
            Some(Self::tls_config()?)
        } else {
            None
        };
        Ok(Self { endpoint, tls })
    }

    /// The client configuration, with the provider named rather than
    /// discovered.
    ///
    /// `rustls::ClientConfig::builder()` takes the process-wide default
    /// provider, or the one a feature installed, or panics. All three decide
    /// this somewhere other than here, and the panic arrives on the first
    /// negotiated connect — which a suite that cannot use the network never
    /// reaches. So the provider is constructed explicitly.
    fn tls_config() -> Result<Arc<ClientConfig>, IngressError> {
        let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
        let config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|error| IngressError::fatal(format!("the TLS provider is unusable: {error}")))?
            .with_root_certificates(Self::trust_anchors())
            .with_no_client_auth();
        Ok(Arc::new(config))
    }

    /// The anchors a venue's certificate chain is verified against.
    ///
    /// Its own function so that "there is something in here" is a value a test
    /// reads back. An empty store is a configuration that negotiates with
    /// nothing and looks exactly like a client with verification switched off
    /// until the first real venue, which is a fault no refusal test can see:
    /// both accept nothing and refuse everything, and only one of them is
    /// correct.
    fn trust_anchors() -> RootCertStore {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        roots
    }

    /// The negotiation, on a socket that is already open, against `deadline`.
    ///
    /// **`deadline` and not a budget of its own**, because a connect attempt is
    /// one budget in two stages: the handshake is given what the socket left of
    /// it, and nothing when the socket spent all of it. `budget` is here to be
    /// named in the timeout's own detail — the number an operator configured is
    /// the one they should read back.
    ///
    /// Its own function so that exactly that is a property a test can state,
    /// which a single expression inside [`open`](Connector::open) is not: called
    /// with a deadline that leaves a quarter of a second, this returns in a
    /// quarter of a second, and a second full budget here would sit on a silent
    /// socket for the whole of it.
    async fn negotiate(
        &self,
        socket: TcpStream,
        config: Arc<ClientConfig>,
        budget: Duration,
        deadline: Instant,
    ) -> Result<Box<dyn ByteStream>, IngressError> {
        let address = self.endpoint.address.clone();
        let name = ServerName::try_from(self.endpoint.server_name.clone()).map_err(|_| {
            // A name in the document is the same string on the next attempt, so
            // retrying it under a backoff only hides it.
            IngressError::fatal(format!(
                "`server_name = \"{}\"` is not a name a certificate can be verified against",
                self.endpoint.server_name
            ))
        })?;
        let negotiated = TlsConnector::from(config).connect(name, socket);
        match timeout_at(deadline, negotiated).await {
            Err(_elapsed) => Err(IngressError::connect(
                ConnectFailureReason::Timeout,
                format!(
                    "no negotiated session with {address} within the {budget:?} the connect and \
                     the negotiation share"
                ),
            )),
            // Every negotiation failure is `tls`: a certificate, a chain, a
            // protocol version. The taxonomy has a value for exactly this
            // because it is a different operator action from a refusal.
            Ok(Err(error)) => Err(IngressError::connect(
                ConnectFailureReason::Tls,
                format!("{address}: {error}"),
            )),
            Ok(Ok(stream)) => Ok(Box::new(Socket::new(stream, address)) as Box<dyn ByteStream>),
        }
    }
}

impl Connector for SocketConnector {
    fn open(
        &mut self,
        budget: Duration,
    ) -> BoxFuture<'_, Result<Box<dyn ByteStream>, IngressError>> {
        Box::pin(async move {
            let address = self.endpoint.address.clone();
            // **One deadline for the socket and the negotiation together.** What
            // `connect_timeout` states is how long a connect attempt may take,
            // and opening a negotiated stream is one attempt in two stages: a
            // budget started again for the second stage is twice the number an
            // operator wrote, so a slow connect that spends all five seconds
            // could take five more and still not be late. The same reasoning
            // gives the teardown's two halves one `LOGOUT_GRACE`, and it is the
            // whole reason that constant is one and not two.
            //
            // A deadline rather than the remainder recomputed, because the
            // remainder has to be measured somewhere and an `Instant` is that
            // measurement. A second stage reached with nothing left elapses at
            // once, which is the right answer: the attempt is already over
            // budget.
            let deadline = Instant::now() + budget;
            let socket = match timeout_at(deadline, TcpStream::connect(&address)).await {
                Err(_elapsed) => {
                    return Err(IngressError::connect(
                        ConnectFailureReason::Timeout,
                        format!(
                            "no socket to {address} within the {budget:?} the connect and the \
                             negotiation share"
                        ),
                    ))
                }
                Ok(Err(error)) => return Err(classify_connect(&address, &error)),
                Ok(Ok(socket)) => socket,
            };
            // Nagle off. A logon that sits in a kernel buffer waiting for
            // company delays the whole session behind it, and the messages
            // this transport sends are small and few.
            if let Err(error) = socket.set_nodelay(true) {
                return Err(IngressError::connect(
                    ConnectFailureReason::Refused,
                    format!("{address}: the socket would not take TCP_NODELAY: {error}"),
                ));
            }
            let Some(config) = self.tls.clone() else {
                return Ok(Box::new(Socket::new(socket, address)) as Box<dyn ByteStream>);
            };
            self.negotiate(socket, config, budget, deadline).await
        })
    }

    fn authority(&self) -> &str {
        &self.endpoint.address
    }
}

/// A [`ByteStream`] over anything that reads and writes bytes.
///
/// One implementation for the plaintext socket and the negotiated one, because
/// the difference between them is entirely in the connect.
struct Socket<S> {
    inner: S,
    authority: String,
}

impl<S> Socket<S> {
    const fn new(inner: S, authority: String) -> Self {
        Self { inner, authority }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> ByteStream for Socket<S> {
    fn write<'a>(&'a mut self, bytes: &'a [u8]) -> BoxFuture<'a, Result<(), StreamError>> {
        Box::pin(async move {
            // `write_all` and not `write`: a partially written message is half
            // a message on a numbered session, which is the one failure shape
            // the session layer cannot recover from.
            self.inner
                .write_all(bytes)
                .await
                .map_err(|error| StreamError::Failed {
                    detail: format!("{}: {error}", self.authority),
                })?;
            self.inner
                .flush()
                .await
                .map_err(|error| StreamError::Failed {
                    detail: format!("{}: {error}", self.authority),
                })
        })
    }

    fn read<'a>(
        &'a mut self,
        out: &'a mut Vec<u8>,
        budget: Duration,
    ) -> BoxFuture<'a, Result<usize, StreamError>> {
        Box::pin(async move {
            let mut chunk = [0u8; READ_CHUNK];
            // Abandoning this read at the budget is safe because the
            // partial-message state is in the decoder and the partial *record*
            // state is in the stream, not in the future this drops — the same
            // property that decided the websocket transport's client.
            match timeout(budget, self.inner.read(&mut chunk)).await {
                // The budget elapsed. Not an error: the caller asked for at
                // most this long.
                Err(_elapsed) => Ok(0),
                Ok(Ok(0)) => Err(StreamError::Closed {
                    detail: format!("{}: the stream ended", self.authority),
                }),
                Ok(Ok(read)) => {
                    out.extend_from_slice(&chunk[..read]);
                    Ok(read)
                }
                Ok(Err(error)) => Err(StreamError::Failed {
                    detail: format!("{}: {error}", self.authority),
                }),
            }
        })
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            // Discarded, and bounded by the caller: `Session::close` runs the
            // logout write and this together under `LOGOUT_GRACE`, which is one
            // number for what a teardown may cost rather than a second one
            // here to keep in agreement with it. The usual reason to be closing
            // is that the peer has stopped answering, and waiting for its half
            // of a shutdown would put that delay in front of every reconnect.
            let _ = self.inner.shutdown().await;
        })
    }
}

/// A session transport, as an [`Input`].
///
/// One instance is one session. A publisher with two `[[source]]` blocks holds
/// two of these with two [`ConnectionId`]s, which is what makes
/// `dz_publisher_ingress_connection_state{connection}` say which one is down —
/// and it is also why the session count is the operator's statement: **one
/// driver per enabled `[[source]]`**, not per feed, not per shard, not per
/// channel instance.
pub struct FixInput {
    connector: Box<dyn Connector>,
    session: Session,
}

impl FixInput {
    /// A transport for one checked configuration, on this host's clock.
    ///
    /// # Errors
    ///
    /// [`IngressError::Fatal`] for a document this transport cannot run: a
    /// sequence continuity it does not serve, an endpoint that is not
    /// `host:port`, a plaintext endpoint that is not loopback. Raised here
    /// rather than at the first connect on purpose — a publisher whose document
    /// asks for something impossible should fail at startup, where it is
    /// diagnosable, instead of retrying against it under a backoff.
    pub fn new(connection: ConnectionId, config: &SessionConfig) -> Result<Self, IngressError> {
        Self::with_clock(connection, config, Arc::new(TokioClock::new()))
    }

    /// A transport on a stated clock.
    ///
    /// The clock is a parameter for the reason it is one throughout this
    /// family: the cadence and the sending time are the two things here that
    /// are hard to get right, and both become values a test reads back.
    ///
    /// # Errors
    ///
    /// As [`new`](Self::new).
    pub fn with_clock(
        connection: ConnectionId,
        config: &SessionConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, IngressError> {
        let endpoint = config
            .resolve()
            .map_err(|error| IngressError::fatal(error.to_string()))?;
        Ok(Self {
            connector: Box::new(SocketConnector::new(endpoint)?),
            session: Session::new(connection, clock),
        })
    }

    /// A transport over a stated connector.
    ///
    /// What the loopback exercise and the driver tests use, and what a venue
    /// would use if it ever had a stream this crate does not open.
    #[must_use]
    pub fn with_connector(
        connection: ConnectionId,
        connector: Box<dyn Connector>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            connector,
            session: Session::new(connection, clock),
        }
    }

    /// The session, for an exercise that wants to read its state back.
    #[must_use]
    pub const fn session(&self) -> &Session {
        &self.session
    }
}

/// Prints the connection, the endpoint and the session's state — and nothing a
/// logon body carried.
///
/// The standard `dz-ingress-websocket` holds its own endpoint to: a credential
/// is kept in a file precisely so that it does not end up in a log line, and a
/// derived implementation would put a venue's signature in one the first time
/// somebody logged this struct.
impl core::fmt::Debug for FixInput {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("FixInput")
            .field("authority", &self.connector.authority())
            .field("session", &self.session)
            .finish()
    }
}

impl Input for FixInput {
    fn connection(&self) -> ConnectionId {
        self.session.connection()
    }

    fn connect(&mut self, budget: Duration) -> BoxFuture<'_, Result<(), IngressError>> {
        Box::pin(async move {
            let stream = self.connector.open(budget).await?;
            // The stream is open and the session is not: the logon is the
            // adapter's and has not been written yet. See the module docs.
            self.session.open(stream);
            Ok(())
        })
    }

    fn send<'a>(
        &'a mut self,
        message: UpstreamMessage<'a>,
    ) -> BoxFuture<'a, Result<(), IngressError>> {
        Box::pin(async move {
            let bytes = match message {
                UpstreamMessage::Text(text) => text.as_bytes(),
                UpstreamMessage::Binary(bytes) => bytes,
            };
            self.session.send(bytes).await.map_err(classify)
        })
    }

    fn recv<'a>(
        &'a mut self,
        budget: Option<Duration>,
    ) -> BoxFuture<'a, Result<Received<'a>, IngressError>> {
        Box::pin(async move {
            match self.session.receive(budget).await {
                Ok(Incoming::Message(bytes)) => Ok(Received::Payload { bytes, ts_ns: None }),
                // Never a payload. The driver's idle guard counts time since
                // the last payload, so a session that heartbeats forever and
                // delivers nothing must still trip it.
                Ok(Incoming::Liveness) => Ok(Received::Liveness),
                Ok(Incoming::Idle) => Ok(Received::Idle),
                Err(error) => Err(classify(error)),
            }
        })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.session.close().await;
        })
    }
}

/// What the driver should do about a session that could not carry on.
///
/// **A total match, and the load-bearing part of this crate.** The reason a
/// connection ended is a metric label with four values and this is the only
/// layer that can see which applies: a rejected logon, a session-level reject
/// and a silence are three different things to an operator and are
/// indistinguishable from the socket.
///
/// # The three groups, and why each is that group
///
/// **Fatal** is everything that is a mistake in venue code or in the document:
/// an adapter that wrote no logon, one that wrote a subscription first, a logon
/// body with no cadence, a body stating a tag this transport owns. Each is the
/// same on the next attempt, so retrying it under a backoff only hides it —
/// and a process that exits loudly at startup is diagnosable.
///
/// **Connect** is the logon that was written and not accepted. Nothing was
/// established, so none of the four disconnect reasons describes it: they all
/// describe a session that existed and then stopped. A refused logon is
/// `unauthorized`, which is the reason an operator acts on — *look at the
/// credential* — and a logon nothing answered is `timeout`.
///
/// **Ended** is a session that existed. `timeout` for a silence, and
/// `remote_close` for everything the venue or the path did.
///
/// # `auth_expired` is unreachable from here, and that is stated rather than
/// papered over
///
/// A logon this venue refuses is a *connect* failure and not a disconnect, and
/// a venue that ends an established session for an expired credential does so
/// with a logout that says so only in free text. Reading that text for a reason
/// code would be inventing a taxonomy out of a venue's prose. So a mid-session
/// logout is counted `remote_close`, which is true and not useful — the same
/// gap `dz-ingress-websocket` states at its own end, and a gap in the closed
/// family rather than something to fold into a label that would then mean two
/// things.
fn classify(error: SessionError) -> IngressError {
    let detail = error.to_string();
    match error {
        SessionError::NoLogon
        | SessionError::SentBeforeEstablished { .. }
        | SessionError::NoHeartbeatInterval
        | SessionError::UnusableHeartbeatInterval { .. }
        | SessionError::Body(_) => IngressError::fatal(detail),

        SessionError::LogonRejected { .. } => {
            IngressError::connect(ConnectFailureReason::Unauthorized, detail)
        }
        SessionError::LogonNotAnswered { .. } => {
            IngressError::connect(ConnectFailureReason::Timeout, detail)
        }

        SessionError::Silent { .. } => IngressError::ended(DisconnectReason::Timeout, detail),
        // `LogonNotEstablished` is here and not with the fatal group: venue
        // code did its job, and a receive on a session whose logon failed is a
        // driver that ignored what its own send returned — which is the case
        // `NotConnected` is, and it takes the same group for the same reason.
        SessionError::LoggedOut { .. }
        | SessionError::Rejected { .. }
        | SessionError::ResendRequested { .. }
        | SessionError::Framing(_)
        | SessionError::Stream(_)
        | SessionError::LogonNotEstablished
        | SessionError::NotConnected => IngressError::ended(DisconnectReason::RemoteClose, detail),
    }
}

/// A socket that never opened.
///
/// The three kinds an operator acts differently on: a refusal, a name that
/// would not resolve, and a budget that elapsed. There is no stable
/// `ErrorKind` for a resolution failure on every platform this runs on —
/// `HostUnreachable` and friends are unstable — so the string is the only
/// signal, matched loosely and documented, because the alternative is counting
/// every socket error as a refusal.
fn classify_connect(address: &str, error: &std::io::Error) -> IngressError {
    let rendered = error.to_string().to_ascii_lowercase();
    let looks_unresolved = ["resolve", "lookup address", "name or service", "dns"]
        .iter()
        .any(|phrase| rendered.contains(phrase));
    let reason = match error.kind() {
        std::io::ErrorKind::ConnectionRefused => ConnectFailureReason::Refused,
        std::io::ErrorKind::TimedOut => ConnectFailureReason::Timeout,
        _ if looks_unresolved => ConnectFailureReason::Unresolved,
        _ => ConnectFailureReason::Refused,
    };
    IngressError::connect(reason, format!("{address}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::LOGON_GRACE;

    fn config(document: &str) -> SessionConfig {
        toml::from_str(document).expect("a document")
    }

    #[test]
    fn the_tls_configuration_is_constructible_without_touching_a_network() {
        // The one part of the TLS setup checkable with no endpoint, and the
        // part most likely to be wrong: `rustls` panics when it has to choose a
        // crypto provider and cannot, and that panic would otherwise arrive on
        // a production connect, which no test that can run here reaches.
        let connector = SocketConnector::new(Endpoint {
            address: "session.example.com:9443".to_owned(),
            server_name: "session.example.com".to_owned(),
            tls: true,
        })
        .expect("a constructible client configuration");
        assert!(connector.tls.is_some());
    }

    #[test]
    fn the_chain_is_verified_against_the_anchors_compiled_into_this_binary() {
        // The trust anchors are in the binary precisely so that a host with a
        // stale CA bundle negotiates like every other host. An empty store is
        // the failure this asserts against: it verifies against nothing, so it
        // refuses every certificate — which a test that asserts a *refusal*
        // cannot tell apart from verification working.
        let anchors = SocketConnector::trust_anchors();
        assert!(
            !anchors.is_empty(),
            "a client that trusts no anchor negotiates with no venue"
        );
        assert_eq!(
            anchors.len(),
            webpki_roots::TLS_SERVER_ROOTS.len(),
            "every compiled-in anchor reaches the store"
        );
    }

    #[tokio::test]
    async fn the_handshake_is_given_what_the_socket_left_of_the_budget() {
        // The revert this test exists for: `timeout(budget, negotiated)` in
        // place of `timeout_at(deadline, negotiated)`. A connect attempt is one
        // budget in two stages, and a budget applied twice is twice the number
        // an operator configured — the same reasoning that makes the teardown's
        // two halves share one `LOGOUT_GRACE`.
        //
        // A listener that accepts and then says nothing is a socket that opens
        // and never negotiates, so what ends this call is the budget and
        // nothing else. The deadline handed in leaves a quarter of a second of
        // ten seconds: with the revert the handshake takes the ten.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback listener");
        let address = listener
            .local_addr()
            .expect("the listener's own address")
            .to_string();
        let accepted = tokio::spawn(async move {
            let socket = listener.accept().await.expect("a connection");
            // Held, so the far side sees an open socket rather than a reset.
            std::future::pending::<()>().await;
            drop(socket);
        });

        let connector = SocketConnector::new(Endpoint {
            address: address.clone(),
            server_name: "session.example.com".to_owned(),
            tls: true,
        })
        .expect("a connector");
        let config = connector.tls.clone().expect("a client configuration");
        let socket = TcpStream::connect(&address).await.expect("a socket");

        let budget = Duration::from_secs(10);
        let left = Duration::from_millis(250);
        let started = std::time::Instant::now();
        let Err(error) = connector
            .negotiate(socket, config, budget, Instant::now() + left)
            .await
        else {
            panic!("a silent socket never negotiates");
        };
        let elapsed = started.elapsed();

        assert!(
            elapsed < Duration::from_secs(2),
            "the handshake was given a budget of its own: {elapsed:?} of a {left:?} remainder"
        );
        assert!(
            elapsed >= left,
            "the handshake gave up before the deadline it was handed: {elapsed:?}"
        );
        assert!(
            matches!(
                error,
                IngressError::Connect {
                    reason: ConnectFailureReason::Timeout,
                    ..
                }
            ),
            "{error}"
        );
        // And the number an operator configured is the one they read back.
        assert!(error.to_string().contains("10s"), "{error}");
        accepted.abort();
    }

    #[test]
    fn a_plaintext_connector_holds_no_tls_configuration() {
        let connector = SocketConnector::new(Endpoint {
            address: "127.0.0.1:9443".to_owned(),
            server_name: "127.0.0.1".to_owned(),
            tls: false,
        })
        .expect("a loopback endpoint");
        assert!(connector.tls.is_none());
    }

    #[test]
    fn a_document_this_transport_cannot_run_fails_at_construction() {
        // At construction rather than at the first connect, so that the
        // publisher fails where it is diagnosable instead of retrying against
        // an impossible document under a backoff.
        for document in [
            "endpoint = \"203.0.113.10:9443\"\npersist_sequence = true\n",
            "endpoint = \"203.0.113.10\"\n",
            "endpoint = \"203.0.113.10:9443\"\ntls = false\n",
        ] {
            let error = FixInput::new(ConnectionId::new("mktdata"), &config(document))
                .expect_err("an unusable document");
            assert!(error.is_fatal(), "{document}: {error}");
        }
    }

    #[test]
    fn every_session_failure_says_what_the_driver_should_do_about_it() {
        // The classification is what makes a disconnect reason a metric label
        // with four values rather than a string nobody groups by, so every
        // variant is stated here — and the three groups are asserted by the
        // action each implies rather than by re-listing the match.
        let fatal = [
            SessionError::NoLogon,
            SessionError::SentBeforeEstablished {
                msg_type: "V".to_owned(),
            },
            SessionError::NoHeartbeatInterval,
            SessionError::UnusableHeartbeatInterval {
                stated: "0".to_owned(),
                min: Duration::from_secs(1),
                max: Duration::from_secs(600),
            },
            SessionError::Body(crate::framing::BodyError::Empty),
        ];
        for error in fatal {
            let classified = classify(error.clone());
            assert!(classified.is_fatal(), "{error} must stop the driver");
            assert_eq!(
                classified.disconnect_reason(),
                Some(DisconnectReason::RemoteClose),
                "a fatal fault still ends a connection the adapter was told about"
            );
        }

        // A logon written and not accepted. Nothing was established, so none of
        // the four disconnect reasons describes it.
        let rejected = classify(SessionError::LogonRejected {
            detail: "invalid credentials".to_owned(),
        });
        assert!(matches!(
            rejected,
            IngressError::Connect {
                reason: ConnectFailureReason::Unauthorized,
                ..
            }
        ));
        assert_eq!(rejected.disconnect_reason(), None);

        let unanswered = classify(SessionError::LogonNotAnswered { grace: LOGON_GRACE });
        assert!(matches!(
            unanswered,
            IngressError::Connect {
                reason: ConnectFailureReason::Timeout,
                ..
            }
        ));

        // A silence is the one disconnect this layer can call a timeout: the
        // socket produced no error and no data, and a test request went
        // unanswered.
        let silent = classify(SessionError::Silent {
            interval: Duration::from_secs(30),
            silence: Duration::from_secs(72),
        });
        assert_eq!(silent.disconnect_reason(), Some(DisconnectReason::Timeout));
        // Both numbers reach the log line, and the one an operator counts
        // against is the graced pair rather than two bare cadences.
        assert!(silent.to_string().contains("72s"), "{silent}");
        assert!(silent.to_string().contains("30s"), "{silent}");

        for ended in [
            SessionError::LoggedOut {
                detail: String::new(),
            },
            SessionError::Rejected {
                detail: String::new(),
            },
            SessionError::ResendRequested {
                detail: String::new(),
            },
            SessionError::Framing(crate::framing::FramingError::NotAMessage),
            SessionError::Stream(StreamError::Closed {
                detail: String::new(),
            }),
            SessionError::LogonNotEstablished,
            SessionError::NotConnected,
        ] {
            let classified = classify(ended.clone());
            assert!(!classified.is_fatal(), "{ended}");
            assert_eq!(
                classified.disconnect_reason(),
                Some(DisconnectReason::RemoteClose),
                "{ended}"
            );
        }
    }

    #[test]
    fn the_reason_a_socket_never_opened_is_the_operators_next_action() {
        use std::io::{Error, ErrorKind};
        let refused = classify_connect(
            "203.0.113.10:9443",
            &Error::new(ErrorKind::ConnectionRefused, "refused"),
        );
        assert!(matches!(
            refused,
            IngressError::Connect {
                reason: ConnectFailureReason::Refused,
                ..
            }
        ));
        let unresolved = classify_connect(
            "session.example.com:9443",
            &Error::other("failed to lookup address information: Name or service not known"),
        );
        assert!(matches!(
            unresolved,
            IngressError::Connect {
                reason: ConnectFailureReason::Unresolved,
                ..
            }
        ));
        // And the address is in the detail either way, because an operator
        // reading a refusal wants to know what was refused.
        assert!(refused.to_string().contains("203.0.113.10:9443"));
    }
}
