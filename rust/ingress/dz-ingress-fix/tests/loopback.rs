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
//! **TLS is not tested and deliberately not faked**, which is the standard
//! `dz-ingress-websocket` set for this family and the reason is the same:
//! verifying the compiled-in trust anchors against a real certificate chain
//! needs a real endpoint, and a self-signed certificate with a root of our own
//! would exercise a configuration this crate does not build — it would assert
//! that a test harness works. What can be checked without a network is checked
//! in the unit tests: that the client configuration is constructible at all,
//! which is where the provider-selection panic would land. So `tls = false` on
//! a loopback endpoint is what these tests use, and that value is accepted
//! nowhere else.
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
use dz_ingress_fix::{FixInput, SessionConfig};
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
            let Ok((mut socket, _peer)) = listener.accept().await else {
                return;
            };
            let record = Arc::clone(&sink);
            let mut decoder = Decoder::new();
            let mut held = Vec::new();
            for act in script {
                match act {
                    Act::Expect => loop {
                        if decoder.take(&mut held).expect("the client's framing holds") {
                            record
                                .lock()
                                .expect("the recorder")
                                .push(framing::rendered(&held));
                            break;
                        }
                        let mut chunk = [0u8; 4_096];
                        match socket.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(read) => decoder.feed(&chunk[..read]),
                        }
                    },
                    Act::Send(bytes) => {
                        if socket.write_all(&bytes).await.is_err() {
                            return;
                        }
                    }
                    Act::HeartbeatsForever { interval } => {
                        let mut sequence = 2;
                        loop {
                            tokio::time::sleep(interval).await;
                            if socket
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

    // Two logons and two subscriptions, and each session numbers from one.
    let seen = received.lock().expect("the recorder").clone();
    let types: Vec<String> = seen
        .iter()
        .map(|message| {
            message
                .split('|')
                .find_map(|field| field.strip_prefix("35="))
                .expect("every message states a message type")
                .to_owned()
        })
        .collect();
    assert_eq!(
        types,
        vec![
            msg_type::LOGON.to_owned(),
            "V".to_owned(),
            msg_type::LOGON.to_owned(),
            "V".to_owned()
        ],
        "{seen:?}"
    );
    assert!(
        seen[0].contains("34=1") && seen[1].contains("34=2"),
        "{seen:?}"
    );
    assert!(
        seen[2].contains("34=1") && seen[3].contains("34=2"),
        "the second session numbers from one: {seen:?}"
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
