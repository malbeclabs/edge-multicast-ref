//! The transport against a session on loopback: the real socket, the real
//! driver.
//!
//! # What is tested here, and what cannot be
//!
//! Every test in this file binds `127.0.0.1:0` and talks to itself. No name is
//! resolved, no route is taken, and nothing needs a privilege — so the suite
//! runs the same on a build host with no route to the internet as on a
//! developer's machine.
//!
//! # TLS: the refusal is tested and the acceptance is not
//!
//! **A negotiation this crate should refuse is asserted here.**
//! `a_certificate_no_compiled_in_anchor_signed_is_refused` puts a TLS listener
//! on `127.0.0.1` with a self-signed certificate and asserts that
//! `SocketConnector::open` fails with `ConnectFailureReason::Tls`. That fakes
//! nothing and needs no root of our own — and it is the half that would
//! otherwise go unnoticed: swapping the root store for a verifier that accepts
//! anything, or leaving `RootCertStore::empty()` unpopulated, is one line that
//! passes every other test in this workspace and surfaces at a venue's
//! security review.
//!
//! **A negotiation this crate should accept is not**, which is the standard
//! `dz-ingress-websocket` set for this family and the reason is the same:
//! verifying the compiled-in trust anchors against a chain that leads to one of
//! them needs a real endpoint, and trusting a root of our own instead would
//! exercise a configuration this crate does not build — it would assert that a
//! test harness works. What can be checked without a network is also checked in
//! the unit tests: that the client configuration is constructible at all, which
//! is where the provider-selection panic would land.
//!
//! Every other test here uses `tls = false` on a loopback endpoint, and that
//! value is accepted nowhere else.
//!
//! # What each test proves
//!
//! `a_session_over_a_real_socket_logs_on_subscribes_and_delivers` is the one
//! that no fake proves: the framing this crate composes is read by a decoder
//! on the other side of a socket, in order, with the length and the checksum
//! holding over bytes that made a round trip.
//!
//! `the_idle_guard_fires_on_a_session_that_only_heartbeats` is the failure the
//! whole `Liveness` case exists for. The venue heartbeats forever and delivers
//! nothing; the guard counts time since the last *payload*, so it has to fire.
//! Report a heartbeat as a payload and the guard never fires — the driver runs
//! against a dead subscription for the life of the process, which is why this
//! test bounds the run rather than only checking a reason.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dz_adapter_core::{
    Adapter, AdapterError, ConnectionId, DisconnectReason, EventSink, ListingSink, ParseError,
    Payload, UpstreamSink,
};
use dz_ingress_core::{
    BackoffPolicy, BoxFuture, ConnectFailureReason, Driver, IngressError, IngressObserver, Input,
    Policy, Received, TokioClock, UpstreamMessage,
};
use dz_ingress_fix::framing::{self, msg_type, Body, Decoder, SOH};
use dz_ingress_fix::{Connector, Endpoint, FixInput, SessionConfig, SocketConnector};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const CONNECTION: ConnectionId = ConnectionId::new("mktdata");

/// The cadence the logon states, short enough for a test.
const CADENCE_SECONDS: u64 = 1;

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

/// A body written with `|` for the separator.
fn wire(rendered: &str) -> Vec<u8> {
    rendered
        .replace('|', &(SOH as char).to_string())
        .into_bytes()
}

/// A framed message the venue sends, on the venue's own numbering.
fn from_venue(body: &str, sequence: u64) -> Vec<u8> {
    let bytes = wire(body);
    let body = Body::parse(&bytes).expect("a body the venue could have sent");
    let mut out = Vec::new();
    framing::frame(&mut out, &body, sequence, "20260909-11:56:50.123", false);
    out
}

/// The logon body an adapter composes: identity, signature, cadence.
fn adapter_logon() -> String {
    String::from_utf8(wire(&format!(
        "35=A|49=A-PUBLISHER|56=A-VENUE|98=0|108={CADENCE_SECONDS}|\
         553=an-account|554=not-a-real-signature|"
    )))
    .expect("a body of text")
}

/// A subscription body an adapter composes.
fn adapter_subscription(request: &str) -> String {
    String::from_utf8(wire(&format!(
        "35=V|262={request}|263=1|264=1|267=2|269=0|269=1|146=1|55=A-SYMBOL|"
    )))
    .expect("a body of text")
}

// ---------------------------------------------------------------------------
// A session on loopback
// ---------------------------------------------------------------------------

/// One thing the test server does on an accepted connection.
enum Act {
    /// Read one whole message from the client and record it.
    Expect,
    /// Send these bytes.
    Send(Vec<u8>),
    /// Send a heartbeat on this cadence until the connection goes away.
    ///
    /// What a venue that keeps a socket alive and delivers nothing looks like,
    /// which is the failure the `Liveness` case exists for.
    HeartbeatsForever { interval: Duration },
    /// Hold the connection open and send nothing.
    Hold(Duration),
}

/// Every message the client sent, rendered with `|` for the separator.
type ClientMessages = Arc<Mutex<Vec<String>>>;

/// Bind loopback and serve each script on one accepted connection, in order.
///
/// Returns the address and the messages the client sent, so a test can assert
/// what actually went out framed and numbered. When the scripts run out the
/// listener is dropped, so the next connect is refused rather than waiting in a
/// backlog — which is what lets a test say "and then no more".
async fn serve(scripts: Vec<Vec<Act>>) -> (SocketAddr, ClientMessages) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback is bindable without a privilege");
    let address = listener.local_addr().expect("a bound address");
    let received: ClientMessages = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&received);

    tokio::spawn(async move {
        for script in scripts {
            let Ok((socket, _peer)) = listener.accept().await else {
                return;
            };
            // The halves are split so that reading and sending are
            // independent: a venue heartbeating on its own cadence still reads
            // what the publisher writes, and a script that only sends does not
            // stop recording.
            let (mut reader, mut writer) = socket.into_split();
            let record = Arc::clone(&sink);
            let before = record.lock().expect("the recorder").len();
            tokio::spawn(async move {
                let mut decoder = Decoder::new();
                let mut held = Vec::new();
                let mut chunk = [0u8; 4_096];
                loop {
                    while decoder.take(&mut held).expect("the client's framing holds") {
                        record
                            .lock()
                            .expect("the recorder")
                            .push(framing::rendered(&held));
                    }
                    match reader.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(read) => decoder.feed(&chunk[..read]),
                    }
                }
            });

            let mut expected = before;
            for act in script {
                match act {
                    Act::Expect => {
                        expected += 1;
                        if !awaited(&sink, expected).await {
                            return;
                        }
                    }
                    Act::Send(bytes) => {
                        if writer.write_all(&bytes).await.is_err() {
                            return;
                        }
                    }
                    Act::HeartbeatsForever { interval } => {
                        let mut sequence = 2;
                        loop {
                            tokio::time::sleep(interval).await;
                            if writer
                                .write_all(&from_venue("35=0|", sequence))
                                .await
                                .is_err()
                            {
                                return;
                            }
                            sequence += 1;
                        }
                    }
                    Act::Hold(how_long) => tokio::time::sleep(how_long).await,
                }
            }
        }
    });

    (address, received)
}

/// Wait until the recorder holds `count` messages, or give up.
///
/// Polled rather than signalled: this is a test server, and a condition
/// variable here would be more machinery than the thing it waits for.
async fn awaited(received: &ClientMessages, count: usize) -> bool {
    for _ in 0..600 {
        if received.lock().expect("the recorder").len() >= count {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    false
}

/// A transport pointed at a loopback endpoint, negotiating nothing.
fn input(address: SocketAddr) -> FixInput {
    let document = format!("endpoint = \"{address}\"\ntls = false\n");
    let config: SessionConfig = toml::from_str(&document).expect("a document");
    FixInput::new(CONNECTION, &config).expect("a loopback endpoint")
}

// ---------------------------------------------------------------------------
// The real socket
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_session_over_a_real_socket_logs_on_subscribes_and_delivers() {
    let payload = from_venue("35=W|55=A-SYMBOL|268=1|269=0|270=100|271=5|", 2);
    let (address, received) = serve(vec![vec![
        Act::Expect,
        Act::Send(from_venue(&format!("35=A|98=0|108={CADENCE_SECONDS}|"), 1)),
        Act::Expect,
        Act::Send(payload.clone()),
        Act::Hold(Duration::from_millis(500)),
    ]])
    .await;

    let mut input = input(address);
    input
        .connect(Duration::from_secs(5))
        .await
        .expect("a socket on loopback");
    input
        .send(UpstreamMessage::Text(&adapter_logon()))
        .await
        .expect("a logon the venue accepts");
    input
        .send(UpstreamMessage::Text(&adapter_subscription(
            "over-a-socket",
        )))
        .await
        .expect("a subscription on an established session");

    match input.recv(Some(Duration::from_secs(2))).await {
        Ok(Received::Payload { bytes, ts_ns }) => {
            assert_eq!(bytes, payload, "the message made the round trip whole");
            assert!(
                ts_ns.is_none(),
                "this transport has no timestamp better than the driver's"
            );
        }
        other => panic!("{other:?}"),
    }
    input.shutdown().await;

    // What the venue actually read: the logon and the subscription, in that
    // order, each framed with the length and the checksum holding over bytes
    // that crossed a socket, and numbered on the session.
    let seen = received.lock().expect("the recorder").clone();
    assert_eq!(seen.len(), 2, "{seen:?}");
    assert!(seen[0].contains("35=A"), "{:?}", seen[0]);
    assert!(seen[0].contains("34=1"), "{:?}", seen[0]);
    assert!(
        seen[0].contains("141=Y"),
        "the reset flag has to be on the logon: {:?}",
        seen[0]
    );
    assert!(
        seen[0].contains("554=not-a-real-signature"),
        "the venue's own logon fields are the adapter's and reach the wire: {:?}",
        seen[0]
    );
    assert!(seen[1].contains("35=V"), "{:?}", seen[1]);
    assert!(seen[1].contains("34=2"), "{:?}", seen[1]);
}

#[tokio::test]
async fn a_socket_nothing_is_listening_on_is_a_connect_failure_with_its_reason() {
    // The listener is dropped before anything connects, so loopback refuses
    // rather than a name failing to resolve — which is the one connect failure
    // this suite can produce deterministically.
    let (address, _received) = serve(vec![]).await;
    // Give the spawned task a moment to find no scripts and drop the listener.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut input = input(address);
    let error = input
        .connect(Duration::from_secs(1))
        .await
        .expect_err("nothing is listening");
    match &error {
        IngressError::Connect { reason, detail } => {
            assert_eq!(*reason, ConnectFailureReason::Refused);
            assert!(
                detail.contains(&address.to_string()),
                "an operator reading a refusal wants to know what was refused: {detail}"
            );
        }
        other => panic!("{other}"),
    }
    assert!(
        !error.is_fatal(),
        "a refused socket is retried, not a reason to stop"
    );
}

#[tokio::test]
async fn the_transport_is_usable_as_the_boxed_input_a_configuration_resolves_to() {
    // `[ingress] kind` resolves one of a closed set at startup and the picked
    // one is a `Box<dyn Input>`. This is the compile-time half of that claim.
    let (address, _received) = serve(vec![]).await;
    let boxed: Box<dyn Input> = Box::new(input(address));
    assert_eq!(boxed.connection().as_str(), "mktdata");
}

// ---------------------------------------------------------------------------
// The driver, over a real socket
// ---------------------------------------------------------------------------

/// An adapter that logs on, subscribes, and records what it was handed.
#[derive(Default)]
struct RecordingAdapter {
    connects: usize,
    payloads: Vec<Vec<u8>>,
    disconnects: Vec<DisconnectReason>,
}

impl Adapter for RecordingAdapter {
    fn message_types(&self) -> &[&'static str] {
        &["W"]
    }

    fn poll_listings(&mut self, _out: &mut dyn ListingSink) {}

    fn on_connected(
        &mut self,
        _conn: ConnectionId,
        out: &mut dyn UpstreamSink,
    ) -> Result<(), AdapterError> {
        self.connects += 1;
        // The logon first and the subscription after it, which is the order
        // this transport requires and the only way an adapter can express it.
        out.send_text(&adapter_logon());
        out.send_text(&adapter_subscription("driven"));
        Ok(())
    }

    fn on_disconnected(&mut self, _conn: ConnectionId, reason: DisconnectReason) {
        self.disconnects.push(reason);
    }

    fn on_payload(
        &mut self,
        payload: &Payload<'_>,
        _out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        self.payloads.push(payload.bytes.to_vec());
        Ok(())
    }
}

/// A transport that stops the driver after a stated number of connects.
///
/// The driver returns only on a fatal error, deliberately, so a test that wants
/// it to return has to produce one.
struct StopAfter {
    inner: FixInput,
    connects: AtomicUsize,
    limit: usize,
}

impl Input for StopAfter {
    fn connection(&self) -> ConnectionId {
        self.inner.connection()
    }

    fn connect(&mut self, budget: Duration) -> BoxFuture<'_, Result<(), IngressError>> {
        Box::pin(async move {
            if self.connects.fetch_add(1, Ordering::SeqCst) >= self.limit {
                return Err(IngressError::fatal("the test has seen enough connects"));
            }
            self.inner.connect(budget).await
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

/// An observer that records the reconnect reasons and discards the rest.
#[derive(Default)]
struct Reasons {
    reconnects: Mutex<Vec<DisconnectReason>>,
}

impl IngressObserver for Reasons {
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

/// An event sink that counts and holds nothing.
#[derive(Default)]
struct Discard;

impl EventSink for Discard {
    fn upstream_message(&mut self, _message_type: &'static str) {}
    fn event(&mut self, _event: dz_adapter_core::Event<'_>) {}
    fn payload_scope(&mut self, _recv_ts_ns: Option<u64>) {}
}

/// A policy whose delays cost a test nothing. What the delay sequence does is
/// asserted in the core's own suite against a clock it controls.
fn policy(idle_timeout: Option<Duration>) -> Policy {
    Policy {
        connect_timeout: Duration::from_secs(5),
        backoff: BackoffPolicy::new(Duration::from_millis(1), Duration::from_millis(2))
            .expect("a valid policy"),
        rate_limit_per_second: 0,
        idle_timeout,
    }
}

#[tokio::test]
async fn the_idle_guard_fires_on_a_session_that_only_heartbeats() {
    // The revert this test exists for: report a heartbeat as a payload. The
    // guard counts time since the last *payload*, so a session that heartbeats
    // forever and delivers nothing must still trip it — and with a heartbeat
    // counted as a payload the guard never fires at all. That is why the run is
    // bounded: the failure is not a wrong reason, it is a driver that never
    // comes back.
    let logon_accepted = from_venue(&format!("35=A|98=0|108={CADENCE_SECONDS}|"), 1);
    let (address, _received) = serve(vec![
        vec![
            Act::Expect,
            Act::Send(logon_accepted.clone()),
            Act::Expect,
            // Never a payload, forever.
            Act::HeartbeatsForever {
                interval: Duration::from_millis(40),
            },
        ],
        // The second connect exists so the driver has somewhere to go after
        // the guard fires; `StopAfter` ends the run there.
        vec![Act::Hold(Duration::from_millis(50))],
    ])
    .await;

    let mut input = StopAfter {
        inner: input(address),
        connects: AtomicUsize::new(0),
        limit: 1,
    };
    let mut adapter = RecordingAdapter::default();
    let observer = Reasons::default();
    let clock = TokioClock::new();
    let mut events = Discard;

    let run = {
        let mut driver = Driver::new(
            &mut input,
            &mut adapter,
            &clock,
            &observer,
            policy(Some(Duration::from_millis(250))),
        );
        tokio::time::timeout(Duration::from_secs(5), driver.run(&mut events)).await
    };
    let exit = run.expect(
        "the idle guard never fired: a session that only heartbeats kept the \
         driver alive, which is a heartbeat being counted as a payload",
    );
    assert!(exit.is_fatal(), "{exit}");

    assert_eq!(
        observer.reconnects.lock().expect("the recorder").as_slice(),
        [DisconnectReason::Timeout],
        "the guard is what ended it, and it is counted as a timeout"
    );
    assert!(
        adapter.payloads.is_empty(),
        "a heartbeat reached the adapter as a payload: {:?}",
        adapter.payloads
    );
    assert_eq!(
        adapter.disconnects,
        vec![DisconnectReason::Timeout],
        "the adapter is told why, and by the same taxonomy"
    );
}

#[tokio::test]
async fn a_driver_logs_on_and_re_subscribes_on_every_connect() {
    // A subscription lives on the session, so a reconnect has to re-issue it —
    // and the logon has to precede it every time, not only the first.
    let logon_accepted = from_venue(&format!("35=A|98=0|108={CADENCE_SECONDS}|"), 1);
    let first = from_venue("35=W|55=FIRST|", 2);
    let second = from_venue("35=W|55=SECOND|", 2);
    let (address, received) = serve(vec![
        vec![
            Act::Expect,
            Act::Send(logon_accepted.clone()),
            Act::Expect,
            Act::Send(first.clone()),
        ],
        vec![
            Act::Expect,
            Act::Send(logon_accepted),
            Act::Expect,
            Act::Send(second.clone()),
            Act::Hold(Duration::from_millis(200)),
        ],
    ])
    .await;

    let mut input = StopAfter {
        inner: input(address),
        connects: AtomicUsize::new(0),
        limit: 2,
    };
    let mut adapter = RecordingAdapter::default();
    let observer = Reasons::default();
    let clock = TokioClock::new();
    let mut events = Discard;

    let exit = {
        let mut driver = Driver::new(&mut input, &mut adapter, &clock, &observer, policy(None));
        tokio::time::timeout(Duration::from_secs(10), driver.run(&mut events))
            .await
            .expect("the driver came back")
    };
    assert!(exit.is_fatal(), "{exit}");
    assert_eq!(adapter.connects, 2, "each connection was subscribed");
    assert_eq!(adapter.payloads, vec![first, second]);

    // Two logons and two subscriptions, in that order, each session numbering
    // from one — and a logout on each teardown, which is the orderly close.
    let seen = received.lock().expect("the recorder").clone();
    let written: Vec<(String, String)> = seen
        .iter()
        .map(|message| {
            let field = |prefix: &str| {
                message
                    .split('|')
                    .find_map(|field| field.strip_prefix(prefix))
                    .unwrap_or_else(|| panic!("`{message}` states no `{prefix}`"))
                    .to_owned()
            };
            (field("35="), field("34="))
        })
        .collect();
    assert_eq!(
        written,
        vec![
            (msg_type::LOGON.to_owned(), "1".to_owned()),
            ("V".to_owned(), "2".to_owned()),
            (msg_type::LOGOUT.to_owned(), "3".to_owned()),
            // And the second session numbers from one, which is what makes the
            // reset a reset rather than a coincidence.
            (msg_type::LOGON.to_owned(), "1".to_owned()),
            ("V".to_owned(), "2".to_owned()),
            (msg_type::LOGOUT.to_owned(), "3".to_owned()),
        ],
        "{seen:?}"
    );
    assert!(
        seen[3].contains("141=Y"),
        "the reset flag is on every logon and not only the first: {:?}",
        seen[3]
    );
}

#[tokio::test]
async fn a_venue_that_refuses_the_logon_is_a_connect_failure_and_not_a_disconnect() {
    // Nothing was established, so none of the four disconnect reasons
    // describes it — and the reason an operator acts on is that the credential
    // was refused.
    let (address, _received) = serve(vec![vec![
        Act::Expect,
        Act::Send(from_venue("35=5|58=invalid credentials|", 1)),
        Act::Hold(Duration::from_millis(200)),
    ]])
    .await;

    let mut input = input(address);
    input
        .connect(Duration::from_secs(5))
        .await
        .expect("a socket on loopback");
    let error = input
        .send(UpstreamMessage::Text(&adapter_logon()))
        .await
        .expect_err("the venue refused the logon");
    match &error {
        IngressError::Connect { reason, detail } => {
            assert_eq!(*reason, ConnectFailureReason::Unauthorized);
            assert!(detail.contains("invalid credentials"), "{detail}");
        }
        other => panic!("{other}"),
    }
    assert_eq!(
        error.disconnect_reason(),
        None,
        "there is no session for a disconnect reason to describe"
    );
}

/// A socket that goes away mid-session, which is the ordinary disconnect.
#[tokio::test]
async fn a_socket_that_goes_away_mid_session_is_a_remote_close() {
    let (address, _received) = serve(vec![vec![
        Act::Expect,
        Act::Send(from_venue(&format!("35=A|98=0|108={CADENCE_SECONDS}|"), 1)),
        // The script ends, the task drops the socket, and the client sees the
        // stream end with no logout.
    ]])
    .await;

    let mut input = input(address);
    input
        .connect(Duration::from_secs(5))
        .await
        .expect("a socket on loopback");
    input
        .send(UpstreamMessage::Text(&adapter_logon()))
        .await
        .expect("a logon the venue accepts");
    let error = input
        .recv(Some(Duration::from_secs(2)))
        .await
        .expect_err("the socket went away");
    assert_eq!(
        error.disconnect_reason(),
        Some(DisconnectReason::RemoteClose)
    );
}

/// The socket is real, so a message can arrive in pieces the way one does.
#[tokio::test]
async fn a_message_written_in_two_pieces_over_a_socket_is_one_message() {
    let payload = from_venue("35=W|55=A-SYMBOL|268=1|269=0|270=100|271=5|", 2);
    let (head, tail) = payload.split_at(payload.len() / 2);
    let (address, _received) = serve(vec![vec![
        Act::Expect,
        Act::Send(from_venue(&format!("35=A|98=0|108={CADENCE_SECONDS}|"), 1)),
        Act::Send(head.to_vec()),
        Act::Hold(Duration::from_millis(30)),
        Act::Send(tail.to_vec()),
        Act::Hold(Duration::from_millis(200)),
    ]])
    .await;

    let mut input = input(address);
    input
        .connect(Duration::from_secs(5))
        .await
        .expect("a socket on loopback");
    input
        .send(UpstreamMessage::Text(&adapter_logon()))
        .await
        .expect("a logon");
    match input.recv(Some(Duration::from_secs(2))).await {
        Ok(Received::Payload { bytes, .. }) => assert_eq!(bytes, payload),
        other => panic!("{other:?}"),
    }
}

// ---------------------------------------------------------------------------
// The boundary's half: the logon is the adapter's, and so is the mid-session
// write
// ---------------------------------------------------------------------------

/// An adapter that writes nothing at all when a connection comes up.
#[derive(Default)]
struct SilentAdapter {
    connects: usize,
}

impl Adapter for SilentAdapter {
    fn message_types(&self) -> &[&'static str] {
        &[]
    }

    fn poll_listings(&mut self, _out: &mut dyn ListingSink) {}

    fn on_connected(
        &mut self,
        _conn: ConnectionId,
        _out: &mut dyn UpstreamSink,
    ) -> Result<(), AdapterError> {
        // The default `on_connected` writes nothing, and an adapter reading a
        // local directory is a shape one publisher already runs — so this is
        // not a contrived mistake. On a session transport it is a session that
        // cannot exist.
        self.connects += 1;
        Ok(())
    }

    fn on_payload(
        &mut self,
        _payload: &Payload<'_>,
        _out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        Ok(())
    }
}

#[tokio::test]
async fn a_connect_with_no_logon_from_the_adapter_is_refused() {
    // The revert this test exists for: let the transport compose a logon when
    // the adapter wrote none. The failure that revert produces is not a crash
    // — it is this repository signing a logon on a venue's behalf, with an
    // identity it invented, which is the one thing the whole boundary is
    // arranged to prevent.
    //
    // The refusal is at the first *receive* and not at connect, because a
    // transport cannot know at connect what the adapter is about to queue: the
    // driver connects, then asks.
    let (address, received) = serve(vec![vec![Act::Hold(Duration::from_millis(500))]]).await;

    let mut input = input(address);
    let mut adapter = SilentAdapter::default();
    let observer = Reasons::default();
    let clock = TokioClock::new();
    let mut events = Discard;

    let exit = {
        let mut driver = Driver::new(&mut input, &mut adapter, &clock, &observer, policy(None));
        tokio::time::timeout(Duration::from_secs(5), driver.run(&mut events))
            .await
            .expect("a session with no logon must be refused, not waited on")
    };

    assert!(
        exit.is_fatal(),
        "an adapter that writes no logon is a defect to fix and not a fault to retry: {exit}"
    );
    let message = exit.to_string();
    assert!(
        message.contains("on_connected"),
        "the refusal must name the method the logon belongs in: {message}"
    );
    assert!(
        message.contains("signature") || message.contains("identity"),
        "and say why this transport will not compose one: {message}"
    );
    assert_eq!(adapter.connects, 1, "the adapter was asked, once");
    assert!(
        received.lock().expect("the recorder").is_empty(),
        "nothing at all reached the venue: {:?}",
        received.lock().expect("the recorder")
    );
}

/// An adapter that subscribes at logon and once more, mid-session.
struct AdmittingAdapter {
    /// What `poll_upstream` still has to write, drained on the first ask.
    ///
    /// **Not re-queued**, which is the contract: the runtime cannot tell a
    /// subscription it has already sent from a new one, so an adapter that
    /// queued its whole set every time it was asked would send that set to the
    /// venue on every cadence.
    outstanding: Option<String>,
}

impl Adapter for AdmittingAdapter {
    fn message_types(&self) -> &[&'static str] {
        &["W"]
    }

    fn poll_listings(&mut self, _out: &mut dyn ListingSink) {}

    fn on_connected(
        &mut self,
        _conn: ConnectionId,
        out: &mut dyn UpstreamSink,
    ) -> Result<(), AdapterError> {
        out.send_text(&adapter_logon());
        out.send_text(&adapter_subscription("at-logon"));
        Ok(())
    }

    fn poll_upstream(
        &mut self,
        _conn: ConnectionId,
        out: &mut dyn UpstreamSink,
    ) -> Result<(), AdapterError> {
        if let Some(outstanding) = self.outstanding.take() {
            out.send_text(&outstanding);
        }
        Ok(())
    }

    fn on_payload(
        &mut self,
        _payload: &Payload<'_>,
        _out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        Ok(())
    }
}

/// A clock that runs fast, so that a cadence measured in seconds costs a test
/// milliseconds.
///
/// The driver asks an adapter what is outstanding every `UPSTREAM_POLL`, which
/// is five seconds. This is the driver's clock and **not** the session's: the
/// two are separate parameters, so the session below runs on this host's real
/// clock while the driver's cadence arrives a hundred times sooner.
struct Hurrying {
    origin: std::time::Instant,
    factor: u64,
}

impl dz_ingress_core::Clock for Hurrying {
    fn wall_ns(&self) -> u64 {
        u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("a clock set after 1970")
                .as_nanos(),
        )
        .expect("a wall reading that fits")
    }

    fn steady_ns(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_nanos())
            .expect("a steady reading that fits")
            .saturating_mul(self.factor)
    }

    fn sleep(&self, duration: Duration) -> BoxFuture<'_, ()> {
        Box::pin(tokio::time::sleep(
            duration / u32::try_from(self.factor).expect("a factor"),
        ))
    }
}

#[tokio::test]
async fn a_mid_session_write_is_framed_and_numbered_on_the_same_session() {
    // What makes an instrument admitted mid-session reach a subscription
    // without a reconnect. A session transport is the one that cannot
    // re-subscribe for reasons of its own — its subscriptions live on the
    // session — so without this the instrument is minted, defined, counted in
    // the manifest, and never subscribed, with nothing reporting it.
    let logon_accepted = from_venue(&format!("35=A|98=0|108={CADENCE_SECONDS}|"), 1);
    let (address, received) = serve(vec![vec![
        Act::Expect,
        Act::Send(logon_accepted),
        Act::Expect,
        // The driver asks an adapter what is outstanding only after a receive
        // returns, so the venue has to be saying something. Heartbeats are
        // what a venue with nothing to deliver says.
        Act::HeartbeatsForever {
            interval: Duration::from_millis(10),
        },
    ]])
    .await;

    let mut input = StopAfter {
        inner: input(address),
        connects: AtomicUsize::new(0),
        limit: 1,
    };
    let mut adapter = AdmittingAdapter {
        outstanding: Some(adapter_subscription("admitted-mid-session")),
    };
    let observer = Reasons::default();
    let clock = Hurrying {
        origin: std::time::Instant::now(),
        factor: 100,
    };
    let mut events = Discard;

    let run = {
        let mut driver = Driver::new(
            &mut input,
            &mut adapter,
            &clock,
            &observer,
            // A guard long enough on the driver's own fast clock that it is the
            // adapter's cadence which fires first, and short enough that the
            // run ends.
            policy(Some(Duration::from_secs(60))),
        );
        tokio::time::timeout(Duration::from_secs(10), driver.run(&mut events)).await
    };
    assert!(run.is_ok(), "the driver came back");
    assert!(adapter.outstanding.is_none(), "the adapter was asked");

    let seen = received.lock().expect("the recorder").clone();
    let written: Vec<(String, String)> = seen
        .iter()
        .map(|message| {
            let field = |prefix: &str| {
                message
                    .split('|')
                    .find_map(|field| field.strip_prefix(prefix))
                    .unwrap_or_else(|| panic!("`{message}` states no `{prefix}`"))
                    .to_owned()
            };
            (field("35="), field("34="))
        })
        .collect();
    // The logon, the subscription written at logon, and then the one written
    // mid-session — on the same session, numbered where the last one left off
    // and not from one.
    assert_eq!(
        written.first(),
        Some(&(msg_type::LOGON.to_owned(), "1".to_owned())),
        "{seen:?}"
    );
    assert_eq!(
        written.get(1),
        Some(&("V".to_owned(), "2".to_owned())),
        "{seen:?}"
    );
    assert_eq!(
        written.get(2),
        Some(&("V".to_owned(), "3".to_owned())),
        "the mid-session write is framed and numbered on the established \
         session: {seen:?}"
    );
    assert!(
        seen[2].contains("admitted-mid-session"),
        "and it is the body the adapter wrote, not one composed here: {:?}",
        seen[2]
    );
    assert!(
        !seen[2].contains("141="),
        "a mid-session write is not a logon and states no reset: {:?}",
        seen[2]
    );
}

// ---------------------------------------------------------------------------
// TLS: the refusal
// ---------------------------------------------------------------------------

/// A TLS listener on loopback presenting a certificate nothing signed but
/// itself.
///
/// The certificate is generated here rather than committed, so there is no key
/// material in this repository and nothing to expire. The listener negotiates
/// and drops whatever it gets: what the test is about is the client's answer to
/// the chain, which is decided before a byte of session traffic.
async fn a_listener_no_anchor_vouches_for() -> SocketAddr {
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    use tokio_rustls::rustls::ServerConfig;
    use tokio_rustls::TlsAcceptor;

    let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("a self-signed certificate for loopback");
    // Round-tripped through `Vec<u8>` so that the types the server
    // configuration holds are the ones this crate's own `rustls` defines,
    // whatever the generator was built against.
    let chain = vec![CertificateDer::from(generated.cert.der().to_vec())];
    let key = PrivatePkcs8KeyDer::from(generated.signing_key.serialize_der());

    // The provider named, for the reason `SocketConnector` names it: the
    // process-wide default is decided somewhere other than here.
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("the provider offers a protocol version")
        .with_no_client_auth()
        .with_single_cert(chain, key.into())
        .expect("a server configuration");
    let acceptor = TlsAcceptor::from(Arc::new(config));

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback is bindable without a privilege");
    let address = listener.local_addr().expect("a bound address");
    tokio::spawn(async move {
        while let Ok((socket, _peer)) = listener.accept().await {
            let acceptor = acceptor.clone();
            // Whatever the handshake does is the client's business: a refusal
            // here is the expected outcome and not a failure to report.
            tokio::spawn(async move {
                let _ = acceptor.accept(socket).await;
            });
        }
    });
    address
}

#[tokio::test]
async fn a_certificate_no_compiled_in_anchor_signed_is_refused() {
    // The half of TLS that is testable without a network and without faking
    // anything: whether verification is on at all. With only `webpki-roots`
    // compiled in, a certificate signed by nobody must be refused — and the
    // refusal must be `tls` rather than a refusal or a timeout, because those
    // are different operator actions.
    //
    // The revert: swap `with_root_certificates(roots)` for a verifier that
    // accepts anything, or leave the root store empty. One line, and it passes
    // every other test in this workspace.
    let address = a_listener_no_anchor_vouches_for().await;
    let mut connector = SocketConnector::new(Endpoint {
        address: address.to_string(),
        server_name: "localhost".to_owned(),
        tls: true,
    })
    .expect("a constructible client configuration");

    let error = connector
        .open(Duration::from_secs(5))
        .await
        .err()
        .expect("a certificate no compiled-in anchor signed must not be accepted");
    assert!(
        matches!(
            error,
            IngressError::Connect {
                reason: ConnectFailureReason::Tls,
                ..
            }
        ),
        "{error}"
    );
    // And the detail names the endpoint, because an operator reading a
    // negotiation failure wants to know which one failed.
    assert!(error.to_string().contains(&address.to_string()), "{error}");
}
