//! The transport against a websocket server on loopback.
//!
//! # What is tested here, and what cannot be
//!
//! Every test in this file binds `127.0.0.1:0` and talks to itself. No name is
//! resolved, no route is taken, and nothing needs a privilege — so the suite
//! runs the same on a build host with no route to the internet as on a
//! developer's machine, which is the constraint that decided the shape.
//!
//! **TLS is not tested and deliberately not faked.** Verifying the compiled-in
//! trust anchors against a real certificate chain needs a real endpoint, and a
//! self-signed certificate with a root of our own would exercise a
//! configuration this crate does not build — it would assert that a test
//! harness works. What can be checked without a network is checked in the unit
//! tests: that the connector is constructible at all, which is where the
//! provider-selection panic would land.
//!
//! What each test proves is in its own comment. Three matter more than the
//! rest, and each for the same reason — the failure it covers is invisible to
//! every other test here. The ping grace, because a half-open socket produces
//! no error and no data. The driver test, because it is the only place the two
//! halves are exercised over a real socket. And the second of the two signed
//! upgrades, because headers computed once at construction pass every test that
//! connects only the once.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dz_adapter_core::{
    Adapter, AdapterError, ConnectionId, DisconnectReason, Event, EventSink, ListingSink,
    ParseError, Payload, UpstreamSink,
};
use dz_ingress_core::{
    BackoffPolicy, BoxFuture, Clock, ConnectFailureReason, Driver, IngressError, IngressObserver,
    Input, Policy, Received, TokioClock, UpstreamMessage,
};
use dz_ingress_websocket::WebSocketInput;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message};

const CONNECTION: ConnectionId = ConnectionId::new("mktdata");

/// A ping cadence short enough for a test and long enough not to race the
/// handshake.
const FAST_PING: Duration = Duration::from_millis(60);

// ---------------------------------------------------------------------------
// A websocket server on loopback
// ---------------------------------------------------------------------------

/// One thing the test server does on an accepted connection.
enum Serve {
    /// Read one message from the client and record it.
    Expect,
    /// Send a text message.
    Text(&'static str),
    /// Send a binary message.
    Binary(&'static [u8]),
    /// Close, with a code.
    Close(CloseCode),
    /// Hold the connection open and read nothing, which is what a half-open
    /// socket looks like from this end: our pings arrive and nothing answers.
    Hold(Duration),
}

/// The upgrade request's headers, per accepted connection, in accept order.
///
/// One entry per handshake rather than one set for the server, because what a
/// reconnect carries is only visible by comparing the second entry with the
/// first.
type Upgrades = Arc<Mutex<Vec<Vec<(String, String)>>>>;

/// Binds loopback and serves each script on one accepted connection, in order.
///
/// Returns the address and the messages the client sent, so a test can assert
/// what the adapter's subscriptions actually put on the wire. When the scripts
/// run out the listener is dropped, so the next connect is refused rather than
/// hanging in a backlog — which is what lets a test say "and then no more".
async fn serve(scripts: Vec<Vec<Serve>>) -> (SocketAddr, Arc<Mutex<Vec<String>>>) {
    let (address, received, _upgrades) = serve_recording_upgrades(scripts).await;
    (address, received)
}

/// [`serve`], and also what each handshake's HTTP request carried.
///
/// The headers are read in the handshake callback and not from the socket,
/// because that is the only place they exist: `tungstenite` consumes the
/// upgrade request and hands back a stream.
async fn serve_recording_upgrades(
    scripts: Vec<Vec<Serve>>,
) -> (SocketAddr, Arc<Mutex<Vec<String>>>, Upgrades) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback is bindable without a privilege");
    let address = listener.local_addr().expect("a bound address");
    let received = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&received);
    let upgrades: Upgrades = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&upgrades);

    tokio::spawn(async move {
        for script in scripts {
            let Ok((socket, _peer)) = listener.accept().await else {
                return;
            };
            let record = Arc::clone(&seen);
            // The callback's error type is the library's rejection response,
            // which is large and never built here: this closure only reads.
            #[allow(clippy::result_large_err)]
            let observe = move |request: &Request, response: Response| {
                record.lock().expect("the recorder").push(
                    request
                        .headers()
                        .iter()
                        .map(|(name, value)| {
                            (
                                name.as_str().to_string(),
                                value.to_str().unwrap_or("<not text>").to_string(),
                            )
                        })
                        .collect(),
                );
                Ok(response)
            };
            let mut stream = match tokio_tungstenite::accept_hdr_async(socket, observe).await {
                Ok(stream) => stream,
                Err(_) => continue,
            };
            for action in script {
                match action {
                    Serve::Expect => {
                        if let Some(Ok(message)) = stream.next().await {
                            sink.lock()
                                .expect("the recorder")
                                .push(message.to_text().unwrap_or("<binary>").to_string());
                        }
                    }
                    Serve::Text(text) => {
                        let _ = stream.send(Message::text(text)).await;
                    }
                    Serve::Binary(bytes) => {
                        let _ = stream.send(Message::binary(bytes.to_vec())).await;
                    }
                    Serve::Close(code) => {
                        let _ = stream
                            .close(Some(CloseFrame {
                                code,
                                reason: "scripted".into(),
                            }))
                            .await;
                    }
                    Serve::Hold(duration) => tokio::time::sleep(duration).await,
                }
            }
        }
        drop(listener);
    });

    (address, received, upgrades)
}

/// What the server saw for one header on the nth handshake, if anything.
///
/// Case-insensitive because HTTP is, and because `http` lowercases every name
/// it parses — a test that asserted on the venue's own capitalisation would be
/// asserting about that normalisation and not about this transport.
fn upgrade_header(upgrades: &Upgrades, handshake: usize, name: &str) -> Option<String> {
    upgrades
        .lock()
        .expect("the recorder")
        .get(handshake)?
        .iter()
        .find(|(seen, _)| seen.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.clone())
}

fn endpoint(address: SocketAddr) -> String {
    format!("ws://{address}")
}

/// An address nothing is listening on.
///
/// Bound to learn a free port and then dropped. Some of the tests below use it
/// as a discriminator rather than as a server: a connect that gets as far as
/// the socket fails as `Refused`, so a test asserting some other outcome has
/// proved the request was refused before any of it went out.
async fn dead_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("loopback");
    let address = listener.local_addr().expect("a bound address");
    drop(listener);
    address
}

/// Receives one payload and copies it out, so that the borrow of the transport
/// ends before the next call.
async fn payload(input: &mut WebSocketInput) -> Vec<u8> {
    match input.recv(None).await {
        Ok(Received::Payload { bytes, .. }) => bytes.to_vec(),
        Ok(other) => panic!("expected a payload, got {other:?}"),
        Err(error) => panic!("expected a payload, got {error}"),
    }
}

// ---------------------------------------------------------------------------
// Construction, without a socket at all
// ---------------------------------------------------------------------------

#[test]
fn it_can_be_held_as_the_trait_object_the_transport_resolver_needs() {
    // `[ingress] kind` picks one transport out of a closed set, so whatever it
    // picks is a `Box<dyn Input>`. This is the compile-time half of that claim:
    // if a method on `Input` ever stops being dyn-compatible, this line is
    // where it is noticed.
    let input = WebSocketInput::new(CONNECTION, "wss://example.com/stream")
        .expect("a well-formed endpoint");
    let boxed: Box<dyn Input> = Box::new(input);
    assert_eq!(boxed.connection(), CONNECTION);
}

#[test]
fn an_endpoint_that_is_not_a_websocket_one_is_refused_at_construction() {
    // At construction and not at the first connect: a misspelled endpoint should
    // stop a publisher where it is diagnosable, rather than be retried under a
    // backoff for as long as nobody looks at a dashboard.
    for wrong in [
        "https://example.com/stream",
        "example.com/stream",
        "not a url",
    ] {
        let error = WebSocketInput::new(CONNECTION, wrong)
            .expect_err("a non-websocket endpoint must be refused");
        assert!(
            error.is_fatal(),
            "a bad endpoint must stop the driver, not be retried: {error}"
        );
    }
}

#[test]
fn both_websocket_schemes_are_accepted() {
    for right in ["ws://example.com/stream", "wss://example.com:9443/stream"] {
        assert!(WebSocketInput::new(CONNECTION, right).is_ok(), "{right}");
    }
}

// ---------------------------------------------------------------------------
// Against the server
// ---------------------------------------------------------------------------

#[tokio::test]
async fn what_the_adapter_asked_to_be_sent_reaches_the_server_and_the_reply_is_a_payload() {
    let (address, received) = serve(vec![vec![
        Serve::Expect,
        Serve::Text("{\"quote\":1}"),
        Serve::Binary(b"\x01\x02\x03"),
        Serve::Close(CloseCode::Normal),
    ]])
    .await;

    let mut input = WebSocketInput::new(CONNECTION, endpoint(address)).expect("an endpoint");
    input
        .connect(Duration::from_secs(5))
        .await
        .expect("loopback accepts");
    input
        .send(UpstreamMessage::Text("subscribe:quotes"))
        .await
        .expect("the send goes out");

    // Both message kinds reach the adapter as bytes. The transport does not
    // care which, because the venue's own protocol decides and the adapter is
    // what reads it.
    assert_eq!(payload(&mut input).await, b"{\"quote\":1}".to_vec());
    assert_eq!(payload(&mut input).await, vec![1, 2, 3]);
    assert_eq!(
        received.lock().expect("the recorder").as_slice(),
        ["subscribe:quotes"]
    );
}

#[tokio::test]
async fn a_close_from_the_venue_ends_the_connection_as_a_remote_close() {
    let (address, _) = serve(vec![vec![Serve::Close(CloseCode::Normal)]]).await;
    let mut input = WebSocketInput::new(CONNECTION, endpoint(address)).expect("an endpoint");
    input
        .connect(Duration::from_secs(5))
        .await
        .expect("loopback accepts");

    let error = input.recv(None).await.expect_err("the venue closed");
    assert_eq!(
        error.disconnect_reason(),
        Some(DisconnectReason::RemoteClose)
    );
}

#[tokio::test]
async fn a_try_again_later_close_is_reported_as_a_rate_limit() {
    // The reason matters beyond the label: a connection that ended in
    // `rate_limit` never resets the driver's delay sequence, so reading this
    // code correctly is the difference between backing off and being banned.
    let (address, _) = serve(vec![vec![Serve::Close(CloseCode::Again)]]).await;
    let mut input = WebSocketInput::new(CONNECTION, endpoint(address)).expect("an endpoint");
    input
        .connect(Duration::from_secs(5))
        .await
        .expect("loopback accepts");

    let error = input.recv(None).await.expect_err("the venue closed");
    assert_eq!(error.disconnect_reason(), Some(DisconnectReason::RateLimit));
}

#[tokio::test]
async fn a_budget_that_elapses_with_nothing_arriving_reports_idle_and_not_an_error() {
    // The transport's half of the upstream silence guard. It is not an error:
    // the transport did what it was asked, and what silence *means* is the
    // driver's decision.
    let (address, _) = serve(vec![vec![Serve::Hold(Duration::from_secs(5))]]).await;
    let mut input = WebSocketInput::new(CONNECTION, endpoint(address))
        .expect("an endpoint")
        // Well beyond the budget, so this test is about the budget and not
        // about the ping cadence.
        .with_ping_interval(Duration::from_secs(30));
    input
        .connect(Duration::from_secs(5))
        .await
        .expect("loopback accepts");

    let received = input.recv(Some(Duration::from_millis(150))).await;
    assert!(
        matches!(received, Ok(Received::Idle)),
        "a budget that elapsed is not a failure"
    );
}

#[tokio::test]
async fn a_ping_that_is_never_answered_ends_the_connection_as_a_timeout() {
    // The failure a read timeout alone cannot see. A half-open socket - one
    // side rebooted, a stateful device forgot the flow - produces no error and
    // no data, so the only way to find it is to ask and to notice that nothing
    // came back. The server here completes the handshake and then reads
    // nothing, so our ping arrives and no pong ever does.
    let (address, _) = serve(vec![vec![Serve::Hold(Duration::from_secs(5))]]).await;
    let mut input = WebSocketInput::new(CONNECTION, endpoint(address))
        .expect("an endpoint")
        .with_ping_interval(FAST_PING);
    input
        .connect(Duration::from_secs(5))
        .await
        .expect("loopback accepts");

    // No budget at all: the connection is ended by the ping grace and by
    // nothing else, which is what makes this test about the ping.
    let error = input.recv(None).await.expect_err("nothing answered");
    assert_eq!(error.disconnect_reason(), Some(DisconnectReason::Timeout));
}

#[tokio::test]
async fn a_refused_connect_is_retryable_and_not_fatal() {
    // A venue that is down must not stop the driver: it should keep trying at
    // the ceiling and leave the alerting to the connection-state gauge, which
    // is pre-created at 0 for exactly this case.
    let address = dead_address().await;
    let mut input = WebSocketInput::new(CONNECTION, endpoint(address)).expect("an endpoint");
    let error = input
        .connect(Duration::from_secs(1))
        .await
        .expect_err("nothing is listening");
    assert!(!error.is_fatal(), "{error}");
    assert_eq!(
        error.disconnect_reason(),
        None,
        "nothing was established, so nothing ended"
    );
}

#[tokio::test]
async fn a_handshake_the_far_side_answers_with_http_is_a_retryable_connect_error() {
    // A venue that answers the upgrade with a status - 401 for a credential,
    // 429 for too many connections - has refused a connection rather than
    // ended one. The status is in the message, and the report on this crate
    // says why it cannot be in a label.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("loopback");
    let address = listener.local_addr().expect("a bound address");
    tokio::spawn(async move {
        if let Ok((mut socket, _)) = listener.accept().await {
            use tokio::io::AsyncWriteExt as _;
            let _ = socket
                .write_all(b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\n\r\n")
                .await;
            let _ = socket.flush().await;
        }
    });

    let mut input = WebSocketInput::new(CONNECTION, endpoint(address)).expect("an endpoint");
    let error = input
        .connect(Duration::from_secs(5))
        .await
        .expect_err("the upgrade was refused");
    assert!(!error.is_fatal(), "{error}");
    assert!(error.to_string().contains("401"), "{error}");
}

// ---------------------------------------------------------------------------
// The signed upgrade
// ---------------------------------------------------------------------------
//
// A venue that authenticates its websocket does it on the HTTP upgrade, with a
// key, a timestamp and a signature over that timestamp. The names below are
// stand-ins for that triple: nothing venue-specific lives in this crate, and
// the property being tested is the same for any of them.

#[tokio::test]
async fn the_headers_the_provider_computed_reach_the_venue_on_the_upgrade() {
    let (address, _sent, upgrades) =
        serve_recording_upgrades(vec![vec![Serve::Close(CloseCode::Normal)]]).await;

    let mut input = WebSocketInput::new(CONNECTION, endpoint(address))
        .expect("an endpoint")
        .with_headers(|| {
            Ok(vec![
                (
                    "x-venue-access-key".to_string(),
                    "not-a-real-key".to_string(),
                ),
                (
                    "x-venue-access-timestamp".to_string(),
                    "1700000000000".to_string(),
                ),
                (
                    "x-venue-access-signature".to_string(),
                    "not-a-real-signature".to_string(),
                ),
            ])
        });
    input
        .connect(Duration::from_secs(5))
        .await
        .expect("loopback accepts");

    // Asserted from the server's side of the handshake, because that is the
    // only place that says the headers were actually sent rather than merely
    // stored.
    for (name, expected) in [
        ("x-venue-access-key", "not-a-real-key"),
        ("x-venue-access-timestamp", "1700000000000"),
        ("x-venue-access-signature", "not-a-real-signature"),
    ] {
        assert_eq!(
            upgrade_header(&upgrades, 0, name).as_deref(),
            Some(expected),
            "the venue did not see {name}"
        );
    }
}

#[tokio::test]
async fn the_provider_runs_again_on_a_reconnect_and_the_second_upgrade_is_the_new_one() {
    // The reason the provider is a closure and not a list. A venue signs a
    // fresh millisecond timestamp, so a reconnect that replayed the first
    // connect's signature would be rejected - and it is the *second* connect
    // that fails, which is hours after anybody was watching the publisher
    // start. The counter here stands in for that timestamp: a signature
    // computed once would arrive twice as `1`.
    let (address, _sent, upgrades) = serve_recording_upgrades(vec![
        vec![Serve::Close(CloseCode::Normal)],
        vec![Serve::Close(CloseCode::Normal)],
    ])
    .await;

    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let mut input = WebSocketInput::new(CONNECTION, endpoint(address))
        .expect("an endpoint")
        .with_headers(move || {
            let nth = counted.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(vec![(
                "x-venue-access-timestamp".to_string(),
                nth.to_string(),
            )])
        });

    input
        .connect(Duration::from_secs(5))
        .await
        .expect("loopback accepts");
    // The same object reconnecting, which is what a driver does: `Input` is
    // documented as having to connect again after a shutdown.
    input.shutdown().await;
    input
        .connect(Duration::from_secs(5))
        .await
        .expect("loopback accepts again");

    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the provider ran once and its output was reused"
    );
    assert_eq!(
        upgrade_header(&upgrades, 0, "x-venue-access-timestamp").as_deref(),
        Some("1")
    );
    assert_eq!(
        upgrade_header(&upgrades, 1, "x-venue-access-timestamp").as_deref(),
        Some("2"),
        "the reconnect replayed the first connect's headers"
    );
}

#[tokio::test]
async fn a_provider_that_cannot_sign_yet_is_a_retryable_unauthorized() {
    // The case this classification is for: a signing key whose file the
    // mounting agent has not written yet. Retryable, because a publisher that
    // stopped for it would stop for a condition that clears in seconds - and
    // `unauthorized`, because the operator's next action is the one a 401 asks
    // for as well.
    let mut input = WebSocketInput::new(CONNECTION, endpoint(dead_address().await))
        .expect("an endpoint")
        .with_headers(|| Err("the key file is not readable yet".to_string()));

    let error = input
        .connect(Duration::from_secs(5))
        .await
        .expect_err("nothing can be signed");
    assert!(!error.is_fatal(), "{error}");
    assert_eq!(
        error.disconnect_reason(),
        None,
        "nothing was established, so nothing ended"
    );
    // Nothing is listening on that address, so a `Refused` here would mean the
    // handshake was attempted unsigned.
    assert!(
        matches!(
            error,
            IngressError::Connect {
                reason: ConnectFailureReason::Unauthorized,
                ..
            }
        ),
        "{error}"
    );
    assert!(
        error.to_string().contains("not readable yet"),
        "the provider's own account of the failure is the only one there is: {error}"
    );
}

#[tokio::test]
async fn a_header_name_that_is_not_valid_http_stops_the_driver_and_is_not_printed() {
    // Retrying cannot make a name valid, so this is the endpoint case again:
    // fail where it is diagnosable rather than under a backoff nobody reads.
    // And the way a name comes to be invalid is a value written into the name
    // slot - a base64 signature is not a valid header name - so the message
    // must not repeat it.
    let leaked = "AAAA/not-a-real-signature=";
    let mut input = WebSocketInput::new(CONNECTION, endpoint(dead_address().await))
        .expect("an endpoint")
        .with_headers(move || Ok(vec![(leaked.to_string(), "1".to_string())]));

    let error = input
        .connect(Duration::from_secs(5))
        .await
        .expect_err("that is not a header name");
    // Nothing is listening, so anything retryable would mean the request went
    // out with the bad pair dropped or mangled.
    assert!(error.is_fatal(), "{error}");
    assert!(
        !error.to_string().contains(leaked),
        "a credential reached a log line: {error}"
    );
    assert!(
        error.to_string().contains("position 0"),
        "the entry has to be identifiable without printing it: {error}"
    );
}

#[tokio::test]
async fn a_header_value_that_is_not_valid_http_stops_the_driver_and_names_only_the_header() {
    // A value is recomputed every attempt, so in principle the next one could
    // differ. Still fatal: a signing routine that emits a control character
    // emits one every time, and of the two possible mistakes the loud one is
    // the recoverable one. The name is printed once it has parsed - it is what
    // says which of three headers to go and look at - and the value never is.
    let mut input = WebSocketInput::new(CONNECTION, endpoint(dead_address().await))
        .expect("an endpoint")
        .with_headers(|| {
            Ok(vec![(
                "x-venue-access-signature".to_string(),
                "not-a-real\nsignature".to_string(),
            )])
        });

    let error = input
        .connect(Duration::from_secs(5))
        .await
        .expect_err("that is not a header value");
    assert!(error.is_fatal(), "{error}");
    assert!(
        error.to_string().contains("x-venue-access-signature"),
        "{error}"
    );
    assert!(
        !error.to_string().contains("not-a-real"),
        "a credential reached a log line: {error}"
    );
}

// ---------------------------------------------------------------------------
// The driver over a real socket
// ---------------------------------------------------------------------------

/// The real transport, with an end.
///
/// The driver stops for one reason — a fault retrying cannot fix — which is
/// correct for a publisher and inconvenient for a test. This wraps the actual
/// [`WebSocketInput`], delegating everything, and reports a fatal fault on the
/// connect after the last one the server was scripted for. So the test
/// exercises the real socket, the real handshake and the real close, and still
/// terminates without a timeout.
struct StopAfter {
    inner: WebSocketInput,
    connects: usize,
    limit: usize,
}

impl Input for StopAfter {
    fn connection(&self) -> ConnectionId {
        self.inner.connection()
    }

    fn connect(&mut self, timeout: Duration) -> BoxFuture<'_, Result<(), IngressError>> {
        Box::pin(async move {
            if self.connects >= self.limit {
                return Err(IngressError::fatal("the server was scripted for no more"));
            }
            self.connects += 1;
            self.inner.connect(timeout).await
        })
    }

    fn send<'a>(
        &'a mut self,
        message: UpstreamMessage<'a>,
    ) -> BoxFuture<'a, Result<(), IngressError>> {
        self.inner.send(message)
    }

    fn recv<'a>(
        &'a mut self,
        budget: Option<Duration>,
    ) -> BoxFuture<'a, Result<Received<'a>, IngressError>> {
        self.inner.recv(budget)
    }

    fn shutdown(&mut self) -> BoxFuture<'_, ()> {
        self.inner.shutdown()
    }
}

#[derive(Default)]
struct CountingAdapter {
    connects: usize,
    disconnects: Vec<DisconnectReason>,
    payloads: Vec<Vec<u8>>,
    /// The receive stamp of every payload, as the adapter was handed it.
    recv_stamps: Vec<u64>,
}

impl Adapter for CountingAdapter {
    fn message_types(&self) -> &[&'static str] {
        &["quote"]
    }

    fn poll_listings(&mut self, _out: &mut dyn ListingSink) {}

    fn on_connected(
        &mut self,
        _conn: ConnectionId,
        out: &mut dyn UpstreamSink,
    ) -> Result<(), AdapterError> {
        self.connects += 1;
        out.send_text("subscribe:quotes");
        Ok(())
    }

    fn on_disconnected(&mut self, _conn: ConnectionId, reason: DisconnectReason) {
        self.disconnects.push(reason);
    }

    fn on_payload(
        &mut self,
        payload: &Payload<'_>,
        out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        self.payloads.push(payload.bytes.to_vec());
        self.recv_stamps.push(payload.recv_ts_ns);
        out.upstream_message("quote");
        Ok(())
    }
}

#[derive(Default)]
struct Discard {
    reconnects: Mutex<Vec<DisconnectReason>>,
}

impl IngressObserver for Discard {
    fn message(&self, _message_type: &'static str, _connection: &'static str) {}
    fn bytes(&self, _count: u64) {}
    fn duplicate(&self) {}
    fn parse_error(&self, _error: ParseError) {}
    fn connection_state(&self, _connection: &'static str, _connected: bool) {}
    fn reconnect(&self, reason: DisconnectReason) {
        self.reconnects.lock().expect("the recorder").push(reason);
    }
    fn connect_failure(&self, _reason: ConnectFailureReason) {}
    fn rate_limited(&self) {}
    fn adapter_error(&self, _error: AdapterError) {}
}

#[derive(Default)]
struct CountingEvents {
    messages: usize,
    /// Every payload scope the driver stated, in order: the stamp it opened
    /// with, then the `None` that closed it.
    scopes: Vec<Option<u64>>,
}

impl EventSink for CountingEvents {
    fn upstream_message(&mut self, _message_type: &'static str) {
        self.messages += 1;
    }

    fn event(&mut self, _event: Event<'_>) {}

    fn payload_scope(&mut self, recv_ts_ns: Option<u64>) {
        self.scopes.push(recv_ts_ns);
    }
}

#[tokio::test]
async fn the_driver_subscribes_again_on_every_reconnect_over_a_real_socket() {
    // The claim the whole `on_connected`-per-connect design rests on, made
    // against an actual handshake rather than a script: a venue's subscriptions
    // live on its session, so each of the two connections here has to be
    // subscribed separately, and the second payload only arrives because it
    // was.
    let (address, received) = serve(vec![
        vec![
            Serve::Expect,
            Serve::Text("first"),
            Serve::Close(CloseCode::Normal),
        ],
        vec![
            Serve::Expect,
            Serve::Text("second"),
            Serve::Close(CloseCode::Away),
        ],
    ])
    .await;

    let mut input = StopAfter {
        inner: WebSocketInput::new(CONNECTION, endpoint(address)).expect("an endpoint"),
        connects: 0,
        limit: 2,
    };
    let mut adapter = CountingAdapter::default();
    let observer = Discard::default();
    let clock = TokioClock::new();
    let mut events = CountingEvents::default();
    let policy = Policy {
        connect_timeout: Duration::from_secs(5),
        // A millisecond, because what the delay sequence does is asserted in
        // the core's own suite against a clock it controls. Here it only has to
        // not slow the test down.
        backoff: BackoffPolicy::new(Duration::from_millis(1), Duration::from_millis(2))
            .expect("a valid policy"),
        rate_limit_per_second: 0,
        idle_timeout: None,
    };

    let exit = {
        let mut driver = Driver::new(&mut input, &mut adapter, &clock, &observer, policy);
        driver.run(&mut events).await
    };

    assert!(exit.is_fatal(), "{exit}");
    assert_eq!(adapter.connects, 2, "each connection was subscribed");
    assert_eq!(
        received.lock().expect("the recorder").as_slice(),
        ["subscribe:quotes", "subscribe:quotes"],
        "and the subscription reached the server both times"
    );
    assert_eq!(
        adapter.payloads,
        vec![b"first".to_vec(), b"second".to_vec()]
    );
    assert_eq!(events.messages, 2);
    // The receive stamp reached the sink over a real socket, and it is the same
    // reading the adapter was handed: the two latency families that measure
    // from a payload's arrival need it here, and nothing about it is passed
    // through the adapter. One scope per payload, each closed before the next
    // opened - the transport has no kernel timestamp, so these are the driver's
    // own wall-clock readings.
    assert_eq!(
        events.scopes,
        vec![
            Some(adapter.recv_stamps[0]),
            None,
            Some(adapter.recv_stamps[1]),
            None
        ]
    );
    assert!(
        adapter.recv_stamps.iter().all(|stamp| *stamp > 0),
        "a payload arrived with no receive stamp at all"
    );
    // Both connections ended in a close from the far side, and each was
    // counted once.
    assert_eq!(
        observer.reconnects.lock().expect("the recorder").as_slice(),
        [DisconnectReason::RemoteClose, DisconnectReason::RemoteClose]
    );
    assert_eq!(
        adapter.disconnects.len(),
        2,
        "every connect is paired with one disconnect"
    );
}

/// The clock is the real one here, so this asserts it is a clock and not that
/// time passes: a `steady_ns` that went backwards or a `wall_ns` of zero would
/// break the idle guard and every payload timestamp respectively.
#[tokio::test]
async fn the_production_clock_reads_forwards_and_from_the_right_origin() {
    let clock = TokioClock::new();
    let first = clock.steady_ns();
    clock.sleep(Duration::from_millis(2)).await;
    assert!(clock.steady_ns() >= first);
    // Nanoseconds since 1970 for any plausible host clock, which is what a
    // payload timestamp has to be to be comparable with a venue's own.
    assert!(clock.wall_ns() > 1_600_000_000_000_000_000);
}
