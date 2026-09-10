//! The polled transport, against a scripted endpoint and a clock the test owns.
//!
//! # How this suite stays network-free, and how you can tell
//!
//! Nothing here opens a socket, and nothing here sleeps. Both are structural
//! rather than a convention someone has to remember:
//!
//! - The endpoint is a [`ScriptedEndpoint`], which is a
//!   [`PollClient`](dz_ingress_poll::PollClient) answering from a list. A test
//!   that wanted a real endpoint would have to write a second one, which is the
//!   visible act this arrangement is for.
//! - The clock is a [`TestClock`], which records what it was asked to wait for
//!   and advances itself instead of waiting. So `poll_interval = "50ms"` and a
//!   two-hundred-millisecond idle guard cost the suite nothing, and the cadence
//!   is a list of readings to compare against.
//! - [`block_on`] **panics on `Poll::Pending`**. Every future here is therefore
//!   proven, by running, to wait on nothing outside the process: no timer, no
//!   socket, no other task. Add an await on something real and this suite fails
//!   rather than hanging in CI.
//!
//! # How a test in this suite ends
//!
//! [`Driver::run`] returns only on [`IngressError::Fatal`], and this transport
//! calls exactly one thing fatal: **an adapter writing binary to a polled
//! endpoint**, because what a poll sends upstream is the next request's
//! parameters and bytes that are not text are not parameters. So
//! [`ScriptedAdapter`] writes text for as many connections as a test asks for
//! and binary on the one after, which stops the driver at a stated point.
//!
//! It writes binary from `poll_upstream` too, once its scripted cursors are
//! spent, and that one is not tidiness — it is what makes the central revert a
//! **failure** rather than a hang. A transport that returned every unchanged
//! response as a payload never lets the idle guard fire, so the driver never
//! leaves its receive loop; the write at the five-second upstream ask is what
//! brings it out, with the wrong reconnect reason and a hundred payloads for
//! the assertions to name. [`ANSWERS_BEFORE_THE_ENDPOINT_GIVES_UP`] is the
//! backstop behind that one.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use dz_adapter_core::{
    Adapter, AdapterError, ConnectionId, Desync, DisconnectReason, Event, EventSink, InstrumentRef,
    ListingSink, ParseError, Payload, Scalar, SideUpdate, UpstreamSink,
};
use dz_ingress_core::{
    BackoffPolicy, BoxFuture, Clock, ConnectFailureReason, Driver, IngressError, IngressObserver,
    Input, Policy, Received, UpstreamMessage,
};
use dz_ingress_poll::{Answer, PollClient, PollConfig, PollInput, Request, RequestFailure};

/// The connection every single-source test runs under.
const CONNECTION: ConnectionId = ConnectionId::new("catalogue");

/// The two connections the per-connection test runs under.
const CATALOGUE: ConnectionId = ConnectionId::new("catalogue");
const REFDATA: ConnectionId = ConnectionId::new("refdata");

/// A documentation-range endpoint. Never a real host: an address in a fixture
/// is copied into production sooner or later.
const ENDPOINT: &str = "http://192.0.2.10/catalogue";

/// The catalogue body, and a second one that differs from it.
const CATALOGUE_BODY: &[u8] = b"instrument-a,instrument-b";
const CATALOGUE_MOVED: &[u8] = b"instrument-a,instrument-b,instrument-c";

/// The entity tag the scripted endpoint serves [`CATALOGUE_BODY`] under, when a
/// test asks it to offer one at all.
const TAG: &str = "\"catalogue-1\"";

/// A wall-clock reading distinctive enough that a payload carrying it cannot
/// have got it from anywhere else.
const WALL_NS: u64 = 1_760_000_000_123_456_789;

/// How many times the scripted endpoint will answer before it decides the test
/// is not going to end on its own.
///
/// The backstop behind the adapter's own: it panics rather than answering
/// again, so that a transport which never returns anything but a payload fails
/// this suite with a message instead of hanging in CI. Well above what any test
/// here needs — the largest uses a few hundred, and only under a revert.
const ANSWERS_BEFORE_THE_ENDPOINT_GIVES_UP: usize = 4_096;

// ---------------------------------------------------------------------------
// Running a future without a runtime
// ---------------------------------------------------------------------------

/// Polls a future to completion, refusing to wait.
///
/// The refusal is the point. This transport is meant to await nothing but the
/// clock it was injected with and the client it was handed. A `Pending` here
/// means one of those two claims has stopped being true, and it is better to be
/// told that by a panic than by a suite that hangs on a machine with no
/// network.
fn block_on<F: Future>(future: F) -> F::Output {
    let mut context = Context::from_waker(Waker::noop());
    let mut future = Box::pin(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!(
            "a future in this suite waited on something outside the process; the \
             transport may only await the clock it was given and the client it was handed"
        ),
    }
}

// ---------------------------------------------------------------------------
// The clock
// ---------------------------------------------------------------------------

#[derive(Default)]
struct ClockState {
    steady_ns: u64,
    slept: Vec<Duration>,
}

/// A clock that advances when it is asked to wait, and remembers by how much.
#[derive(Default)]
struct TestClock {
    state: Mutex<ClockState>,
}

impl TestClock {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Every wait anything asked for, in order. The transport's cadence is in
    /// here beside the driver's backoff.
    fn slept(&self) -> Vec<Duration> {
        self.state.lock().expect("clock").slept.clone()
    }
}

impl Clock for TestClock {
    fn wall_ns(&self) -> u64 {
        WALL_NS
    }

    fn steady_ns(&self) -> u64 {
        self.state.lock().expect("clock").steady_ns
    }

    fn sleep(&self, duration: Duration) -> BoxFuture<'_, ()> {
        let mut state = self.state.lock().expect("clock");
        state.slept.push(duration);
        state.steady_ns = state
            .steady_ns
            .saturating_add(u64::try_from(duration.as_nanos()).expect("a test-sized duration"));
        drop(state);
        Box::pin(std::future::ready(()))
    }
}

// ---------------------------------------------------------------------------
// The endpoint
// ---------------------------------------------------------------------------

/// One thing the scripted endpoint does when it is asked.
#[derive(Debug, Clone)]
enum Answered {
    /// A body, under this entity tag when it offers one.
    Body(&'static [u8], Option<&'static str>),
    /// A status the endpoint should not have returned.
    Status(u16),
    /// No answer at all.
    Failed(RequestFailure),
}

/// What one request looked like when it arrived.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Seen {
    parameters: Option<String>,
    validator: Option<String>,
    budget: Duration,
}

struct EndpointState {
    /// The first answers, in order.
    scripted: VecDeque<Answered>,
    /// What it answers once the script is spent, forever.
    forever: Answered,
    answers: usize,
    seen: Vec<Seen>,
}

/// An endpoint that answers from a script.
///
/// **It honours a conditional request**: a request offering the entity tag the
/// endpoint would have served is answered `304` with no body. That is not
/// decoration — an endpoint which ignored `if-none-match` could not tell a
/// transport that offers a validator from one that does not, and whether the
/// connect probe's validator is discarded is exactly what one test here turns
/// on.
struct ScriptedEndpoint {
    state: Mutex<EndpointState>,
}

impl ScriptedEndpoint {
    /// An endpoint that answers `forever` with the same thing, after `scripted`.
    fn new(scripted: Vec<Answered>, forever: Answered) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(EndpointState {
                scripted: scripted.into(),
                forever,
                answers: 0,
                seen: Vec::new(),
            }),
        })
    }

    /// An endpoint that answers with the same body every time, under an entity
    /// tag when `tag` says so. The catalogue that has stopped changing.
    fn unchanging(tag: Option<&'static str>) -> Arc<Self> {
        Self::new(Vec::new(), Answered::Body(CATALOGUE_BODY, tag))
    }

    fn answers(&self) -> usize {
        self.state.lock().expect("endpoint").answers
    }

    fn seen(&self) -> Vec<Seen> {
        self.state.lock().expect("endpoint").seen.clone()
    }
}

impl PollClient for ScriptedEndpoint {
    fn fetch<'a>(&'a self, request: Request<'a>) -> BoxFuture<'a, Result<Answer, RequestFailure>> {
        let outcome = {
            let mut state = self.state.lock().expect("endpoint");
            state.seen.push(Seen {
                parameters: request.parameters.map(str::to_string),
                validator: request.validator.map(str::to_string),
                budget: request.budget,
            });
            state.answers += 1;
            assert!(
                state.answers <= ANSWERS_BEFORE_THE_ENDPOINT_GIVES_UP,
                "the endpoint has now been asked {} times. A transport that returns \
                 every unchanged response as a payload never lets the idle guard fire, \
                 so the driver never leaves its receive loop - and this suite must fail \
                 rather than hang.",
                state.answers
            );
            let answered = state
                .scripted
                .pop_front()
                .unwrap_or_else(|| state.forever.clone());
            match &answered {
                // The conditional request, honoured. See this type's own note.
                Answered::Body(_, Some(tag)) if request.validator == Some(*tag) => Ok(Answer {
                    status: 304,
                    body: Vec::new(),
                    validator: Some((*tag).to_string()),
                }),
                Answered::Body(body, tag) => Ok(Answer {
                    status: 200,
                    body: body.to_vec(),
                    validator: tag.map(|tag| tag.to_string()),
                }),
                Answered::Status(status) => Ok(Answer {
                    status: *status,
                    body: Vec::new(),
                    validator: None,
                }),
                Answered::Failed(failure) => Err(failure.clone()),
            }
        };
        Box::pin(std::future::ready(outcome))
    }
}

// ---------------------------------------------------------------------------
// The adapter
// ---------------------------------------------------------------------------

/// An adapter that writes cursors and then stops the driver.
///
/// See the suite's own note on how a test here ends: the binary write is the
/// one thing this transport calls fatal, and it is how a test states where to
/// stop.
struct ScriptedAdapter {
    /// How many connections to serve before writing binary at `on_connected`.
    connections: usize,
    /// What to write at each `on_connected`. Exhausted repeats the last.
    at_connect: Vec<&'static str>,
    /// What to write from each successive `poll_upstream`. Exhausted writes
    /// binary, which stops the driver.
    outstanding: VecDeque<&'static str>,
    connected: Vec<ConnectionId>,
    disconnected: Vec<(ConnectionId, DisconnectReason)>,
    /// The bytes of every payload, in order. The count is the assertion that
    /// says an unchanged response was not one.
    payloads: Vec<Vec<u8>>,
    /// Every `poll_upstream`, as the connection it named and how many payloads
    /// this adapter had seen by then.
    upstream_polls: Vec<(ConnectionId, usize)>,
}

impl ScriptedAdapter {
    fn new(
        connections: usize,
        at_connect: Vec<&'static str>,
        outstanding: Vec<&'static str>,
    ) -> Self {
        Self {
            connections,
            at_connect,
            outstanding: outstanding.into(),
            connected: Vec::new(),
            disconnected: Vec::new(),
            payloads: Vec::new(),
            upstream_polls: Vec::new(),
        }
    }
}

impl Adapter for ScriptedAdapter {
    fn message_types(&self) -> &[&'static str] {
        &["definition"]
    }

    fn poll_listings(&mut self, _out: &mut dyn ListingSink) {}

    fn on_connected(
        &mut self,
        conn: ConnectionId,
        out: &mut dyn UpstreamSink,
    ) -> Result<(), AdapterError> {
        self.connected.push(conn);
        if self.connected.len() > self.connections {
            // The stated end of the test. See the suite's own note.
            out.send_binary(b"stop");
            return Ok(());
        }
        let index = (self.connected.len() - 1).min(self.at_connect.len().saturating_sub(1));
        if let Some(parameters) = self.at_connect.get(index) {
            out.send_text(parameters);
        }
        Ok(())
    }

    fn poll_upstream(
        &mut self,
        conn: ConnectionId,
        out: &mut dyn UpstreamSink,
    ) -> Result<(), AdapterError> {
        self.upstream_polls.push((conn, self.payloads.len()));
        match self.outstanding.pop_front() {
            Some(parameters) => out.send_text(parameters),
            // Nothing scripted left, so this is a connection the test did not
            // expect to still be alive. Stopping here is what turns the central
            // revert into a failure instead of a hang.
            None => out.send_binary(b"stop"),
        }
        Ok(())
    }

    fn on_disconnected(&mut self, conn: ConnectionId, reason: DisconnectReason) {
        self.disconnected.push((conn, reason));
    }

    fn on_payload(
        &mut self,
        payload: &Payload<'_>,
        out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        self.payloads.push(payload.bytes.to_vec());
        out.upstream_message("definition");
        out.event(Event::Quote {
            instrument: InstrumentRef::from_admission(0),
            source_ts_ns: WALL_NS - 250_000,
            bid: SideUpdate::Present {
                px: Scalar::text("1.00"),
                qty: Scalar::text("5"),
                source_count: None,
            },
            ask: SideUpdate::Gone,
        });
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The sinks
// ---------------------------------------------------------------------------

#[derive(Default)]
struct SilentEvents;

impl EventSink for SilentEvents {
    fn upstream_message(&mut self, _message_type: &'static str) {}
    fn event(&mut self, _event: Event<'_>) {}
    fn desynchronised(&mut self, _instrument: InstrumentRef, _reason: Desync) {}
    fn payload_scope(&mut self, _recv_ts_ns: Option<u64>) {}
}

#[derive(Default)]
struct Recorded {
    states: Vec<(&'static str, bool)>,
    reconnects: Vec<DisconnectReason>,
    connect_failures: Vec<ConnectFailureReason>,
    bytes: u64,
    rate_limited: usize,
}

#[derive(Default)]
struct TestObserver {
    recorded: Mutex<Recorded>,
}

impl TestObserver {
    /// What was recorded, behind a guard that lives to the end of the statement.
    ///
    /// **Never call this twice in one assertion.** The guard the left operand
    /// takes is still held while the failure message is formatted, so a message
    /// that reads this again deadlocks — and a revert that deadlocks is a revert
    /// that hangs instead of failing, which is the one thing this suite is built
    /// not to do. Read the values into locals first; see
    /// `the_idle_guard_still_fires_on_an_endpoint_that_answers_forever`, which
    /// is where that was found by running the revert rather than by reading it.
    fn recorded(&self) -> std::sync::MutexGuard<'_, Recorded> {
        self.recorded.lock().expect("observer")
    }
}

impl IngressObserver for TestObserver {
    fn message(&self, _message_type: &'static str, _connection: &'static str) {}

    fn bytes(&self, count: u64) {
        self.recorded().bytes += count;
    }

    fn duplicate(&self) {}

    fn parse_error(&self, _error: ParseError) {}

    fn connection_state(&self, connection: &'static str, connected: bool) {
        self.recorded().states.push((connection, connected));
    }

    fn reconnect(&self, reason: DisconnectReason) {
        self.recorded().reconnects.push(reason);
    }

    fn connect_failure(&self, reason: ConnectFailureReason) {
        self.recorded().connect_failures.push(reason);
    }

    fn rate_limited(&self) {
        self.recorded().rate_limited += 1;
    }

    fn adapter_error(&self, _error: AdapterError) {}
}

// ---------------------------------------------------------------------------
// Building one
// ---------------------------------------------------------------------------

fn config(poll_interval: Duration) -> PollConfig {
    PollConfig {
        endpoint: ENDPOINT.to_string(),
        poll_interval,
    }
}

fn policy(idle_timeout: Option<Duration>) -> Policy {
    Policy {
        connect_timeout: Duration::from_secs(5),
        backoff: BackoffPolicy::new(Duration::from_millis(500), Duration::from_secs(30))
            .expect("a valid policy"),
        rate_limit_per_second: 0,
        idle_timeout,
    }
}

fn input(
    connection: ConnectionId,
    poll_interval: Duration,
    endpoint: &Arc<ScriptedEndpoint>,
    clock: &Arc<TestClock>,
) -> PollInput {
    PollInput::new(
        connection,
        &config(poll_interval),
        Arc::clone(endpoint) as Arc<dyn PollClient>,
        Arc::clone(clock) as Arc<dyn Clock>,
    )
    .expect("a usable endpoint and cadence")
}

/// Everything one run produced, for the assertions to read.
struct Run {
    adapter: ScriptedAdapter,
    observer: Arc<TestObserver>,
    clock: Arc<TestClock>,
    endpoint: Arc<ScriptedEndpoint>,
    exit: IngressError,
}

/// Drives one scripted endpoint to the adapter's stated end.
fn run(
    policy: Policy,
    poll_interval: Duration,
    adapter: ScriptedAdapter,
    endpoint: Arc<ScriptedEndpoint>,
) -> Run {
    let clock = TestClock::new();
    let observer = Arc::new(TestObserver::default());
    let mut transport = input(CONNECTION, poll_interval, &endpoint, &clock);
    let mut adapter = adapter;
    let mut events = SilentEvents;

    let exit = {
        let mut driver = Driver::new(
            &mut transport,
            &mut adapter,
            clock.as_ref(),
            observer.as_ref(),
            policy,
        );
        block_on(driver.run(&mut events))
    };

    Run {
        adapter,
        observer,
        clock,
        endpoint,
        exit,
    }
}

// ---------------------------------------------------------------------------
// The three answers a receive has to tell apart
// ---------------------------------------------------------------------------

#[test]
fn a_changed_body_is_a_payload_and_reaches_the_adapter() {
    let endpoint = ScriptedEndpoint::new(
        vec![
            // The connect probe, whose body goes nowhere.
            Answered::Body(CATALOGUE_BODY, None),
            // The first poll: the catalogue.
            Answered::Body(CATALOGUE_BODY, None),
            // The second: a catalogue that has moved.
            Answered::Body(CATALOGUE_MOVED, None),
        ],
        // And then it stops changing, so the guard ends the connection.
        Answered::Body(CATALOGUE_MOVED, None),
    );
    let run = run(
        policy(Some(Duration::from_millis(200))),
        Duration::from_millis(50),
        ScriptedAdapter::new(1, vec!["cursor=0"], Vec::new()),
        endpoint,
    );

    assert_eq!(
        run.adapter.payloads,
        vec![CATALOGUE_BODY.to_vec(), CATALOGUE_MOVED.to_vec()],
        "both bodies the endpoint changed to are payloads, in order, and the \
         probe's copy of the first is not a third"
    );
    assert_eq!(
        run.observer.recorded().bytes,
        (CATALOGUE_BODY.len() + CATALOGUE_MOVED.len()) as u64,
        "the bytes series counts what reached the adapter"
    );
    assert!(run.exit.is_fatal(), "the run ends where the adapter says");
}

/// **The plan's centre.** An endpoint that answers forever and has stopped
/// changing is the failure the `Liveness`/payload distinction exists for, and
/// the assertion is *the idle guard still firing* rather than the value this
/// transport returned — a `Liveness` that behaved like a payload would satisfy
/// any test that only read the discriminant.
///
/// Run twice, because an endpoint has two ways of saying nothing has changed
/// and this transport has to answer both the same way: a `304` to a conditional
/// request, and a body identical to the last one for an endpoint that offers no
/// entity tag at all.
#[test]
fn the_idle_guard_still_fires_on_an_endpoint_that_answers_forever() {
    for tag in [Some(TAG), None] {
        let how = if tag.is_some() {
            "a 304 to a conditional request"
        } else {
            "a body that has not moved"
        };
        let run = run(
            policy(Some(Duration::from_millis(200))),
            Duration::from_millis(50),
            ScriptedAdapter::new(1, vec!["cursor=0"], Vec::new()),
            ScriptedEndpoint::unchanging(tag),
        );

        // Read out first, and that is not style: an assertion whose failure
        // message locks the observer again would deadlock against the guard its
        // own left operand is holding - which is a revert that hangs instead of
        // failing, and this suite exists not to have one of those.
        let reconnects = run.observer.recorded().reconnects.clone();
        let answers = run.endpoint.answers();
        let payloads = run.adapter.payloads.clone();

        // The guard fired: the connection ended for upstream silence, which is
        // what `dz_publisher_ingress_reconnects_total{reason="timeout"}` counts
        // and what an operator's alert reads.
        assert_eq!(
            reconnects,
            vec![DisconnectReason::Timeout],
            "with {how}, the connection must end for upstream silence; it ended \
             {reconnects:?} after {answers} answers and {} payloads",
            payloads.len()
        );
        // And the catalogue arrived exactly once, which is the same property
        // from the other side: every later answer produced nothing for the
        // adapter.
        assert_eq!(
            payloads,
            vec![CATALOGUE_BODY.to_vec()],
            "with {how}, only the first answer is a payload; the endpoint was \
             asked {answers} times"
        );
        assert!(
            answers >= 5,
            "with {how}, the endpoint must have gone on answering - it answered \
             {answers} times"
        );
        assert_eq!(
            // The first, because the run's own deliberate end is a second
            // connection that the adapter stops - see the suite's note.
            run.adapter.disconnected.first(),
            Some(&(CONNECTION, DisconnectReason::Timeout)),
            "with {how}, the adapter is told the connection ended and why"
        );
    }
}

#[test]
fn a_budget_that_elapses_before_the_poll_is_due_is_idle_and_not_an_error() {
    // Transport-level, because what the driver does with `Idle` is the driver's
    // and what is asserted here is that this is what it is handed.
    let endpoint = ScriptedEndpoint::unchanging(None);
    let clock = TestClock::new();
    let mut transport = input(CONNECTION, Duration::from_secs(1), &endpoint, &clock);

    block_on(transport.connect(Duration::from_secs(5))).expect("the endpoint answered");
    // The first poll falls due at once, because the probe's body went nowhere.
    // It is the *next* one that is a whole interval away, and that is the one a
    // ten-millisecond budget cannot reach.
    block_on(transport.recv(None)).expect("the first poll");
    let received = block_on(transport.recv(Some(Duration::from_millis(10))))
        .expect("an elapsed budget is not an error");

    assert_eq!(received, Received::Idle);
    assert_eq!(
        endpoint.answers(),
        2,
        "the probe and the first poll, and no more: a budget that runs out \
         before the next poll is due must not bring the request forward"
    );
    assert_eq!(
        clock.slept(),
        vec![Duration::from_millis(10)],
        "the budget is spent waiting, not returned early - a transport that \
         returned `Idle` at once would have the driver spinning through its \
         whole idle window"
    );
}

/// A failed request ends the connection **with the reason it actually had**.
///
/// Four outcomes rather than one catch-all, and each of them changes what
/// happens next: `rate_limit` never resets the driver's delay sequence and
/// records the series that says the venue did it, `auth_expired` is a
/// credential to look at, `timeout` is an endpoint that stopped answering
/// mid-request, and `remote_close` is everything that is none of those.
#[test]
fn a_failed_request_ends_the_connection_with_the_reason_the_failure_had() {
    let cases = [
        (
            Answered::Failed(RequestFailure::Timeout("no response in 30s".into())),
            DisconnectReason::Timeout,
        ),
        (Answered::Status(429), DisconnectReason::RateLimit),
        (Answered::Status(401), DisconnectReason::AuthExpired),
        (Answered::Status(503), DisconnectReason::RemoteClose),
        (
            Answered::Failed(RequestFailure::Refused("connection refused".into())),
            DisconnectReason::RemoteClose,
        ),
    ];

    for (answered, expected) in cases {
        let endpoint = ScriptedEndpoint::new(
            // The probe answers, so the connection is established and the
            // failure is a disconnect rather than a connect failure.
            vec![Answered::Body(CATALOGUE_BODY, None), answered.clone()],
            Answered::Body(CATALOGUE_BODY, None),
        );
        let run = run(
            policy(None),
            Duration::from_secs(30),
            ScriptedAdapter::new(1, vec!["cursor=0"], Vec::new()),
            endpoint,
        );

        assert_eq!(
            run.observer.recorded().reconnects,
            vec![expected],
            "{answered:?} must end the connection as {expected:?}"
        );
        assert_eq!(
            run.adapter.disconnected.first(),
            Some(&(CONNECTION, expected)),
            "{answered:?}: the adapter is told the same reason the metric counts"
        );
        assert!(
            run.observer.recorded().connect_failures.is_empty(),
            "{answered:?} is a connection that ended, not one that was never \
             established: nothing belongs in the connect-failure series"
        );
        if expected == DisconnectReason::RateLimit {
            assert_eq!(
                run.observer.recorded().rate_limited,
                1,
                "a venue that answered 429 has rate-limited us, and the series \
                 that says so is recorded"
            );
        }
    }
}

#[test]
fn a_first_request_that_fails_is_a_connect_failure_with_its_own_taxonomy() {
    // The other half of the classification, and the half where the distinctions
    // survive: a refusal, an unresolvable name and a certificate that would not
    // verify are three different people's problem, and the four disconnect
    // reasons have no word for any of them.
    let cases = [
        (
            RequestFailure::Refused("connection refused".into()),
            ConnectFailureReason::Refused,
        ),
        (
            RequestFailure::Unresolved("failed to lookup address".into()),
            ConnectFailureReason::Unresolved,
        ),
        (
            RequestFailure::Tls("certificate has expired".into()),
            ConnectFailureReason::Tls,
        ),
        (
            RequestFailure::Timeout("no response in 5s".into()),
            ConnectFailureReason::Timeout,
        ),
    ];

    for (failure, expected) in cases {
        let endpoint = ScriptedEndpoint::new(
            vec![Answered::Failed(failure.clone())],
            Answered::Body(CATALOGUE_BODY, None),
        );
        let run = run(
            policy(None),
            Duration::from_secs(30),
            // No connection served: the first connect fails, and the second is
            // where the adapter stops the run.
            ScriptedAdapter::new(0, Vec::new(), Vec::new()),
            endpoint,
        );

        assert_eq!(
            run.observer.recorded().connect_failures,
            vec![expected],
            "{failure:?} must be counted as {expected:?}"
        );
        assert!(
            run.observer.recorded().reconnects.is_empty(),
            "{failure:?} established nothing, so nothing ended and no reconnect \
             is counted"
        );
    }
}

#[test]
fn a_first_request_the_endpoint_refuses_names_the_credential_or_the_limit() {
    for (status, expected) in [
        (401, ConnectFailureReason::Unauthorized),
        (429, ConnectFailureReason::RateLimit),
        (500, ConnectFailureReason::Rejected),
        // A `304` to the probe, which offers no validator. An endpoint
        // answering *not modified* to an unconditional request has answered
        // something it should not have, and reading that as health would be
        // reading a malfunction as a healthy connection.
        (304, ConnectFailureReason::Rejected),
    ] {
        let endpoint = ScriptedEndpoint::new(
            vec![Answered::Status(status)],
            Answered::Body(CATALOGUE_BODY, None),
        );
        let run = run(
            policy(None),
            Duration::from_secs(30),
            ScriptedAdapter::new(0, Vec::new(), Vec::new()),
            endpoint,
        );
        assert_eq!(
            run.observer.recorded().connect_failures,
            vec![expected],
            "status {status} on the first request must be counted as {expected:?}"
        );
    }
}

#[test]
fn a_connect_that_answers_puts_the_state_gauge_up_and_a_connect_that_does_not_leaves_it_down() {
    // What pre-creating `dz_publisher_ingress_connection_state` at 0 is for: a
    // catalogue endpoint that is not there is a series sitting at zero, and a
    // transport whose connect "succeeded" without asking anything would have
    // put it up.
    let run = run(
        policy(None),
        Duration::from_secs(30),
        ScriptedAdapter::new(0, Vec::new(), Vec::new()),
        ScriptedEndpoint::new(
            vec![Answered::Failed(RequestFailure::Refused("refused".into()))],
            Answered::Body(CATALOGUE_BODY, None),
        ),
    );
    assert!(
        !run.observer
            .recorded()
            .states
            .contains(&("catalogue", true)),
        "an endpoint that refused the first request never brings the gauge up"
    );
}

#[test]
fn the_first_poll_after_connect_delivers_the_catalogue_the_probe_discarded() {
    // The probe proves the endpoint answers and its answer goes nowhere -
    // body and entity tag both. Keeping the tag would be the worse bug: the
    // first poll would offer it, this endpoint would answer 304, and the
    // adapter would never see the catalogue at all while every series said the
    // feed was healthy.
    let endpoint = ScriptedEndpoint::unchanging(Some(TAG));
    let clock = TestClock::new();
    let mut transport = input(CONNECTION, Duration::from_secs(30), &endpoint, &clock);

    block_on(transport.connect(Duration::from_secs(5))).expect("the endpoint answered");
    let received = block_on(transport.recv(None)).expect("the first poll");

    assert_eq!(
        received,
        Received::Payload {
            bytes: CATALOGUE_BODY,
            ts_ns: None
        },
        "the first poll delivers the catalogue, and with no timestamp of its \
         own: a response body carries no receive time this transport knows \
         better than the driver's"
    );
    let seen = endpoint.seen();
    assert_eq!(
        seen[0].validator, None,
        "the probe is unconditional - it has nothing to offer back"
    );
    assert_eq!(
        seen[1].validator, None,
        "and so is the first poll, because the probe's tag was discarded with \
         its body"
    );
    assert_eq!(
        seen.len(),
        2,
        "the probe and the first poll, and nothing waited for: the first poll \
         falls due at once because the probe's body went nowhere"
    );
}

#[test]
fn an_unchanged_answer_is_the_only_thing_that_does_not_move_the_cadence_on() {
    // The cadence is the time between two requests, measured from the request
    // going out. Asserted through the clock, which is why the clock is
    // injected: `poll_interval = "50ms"` here is four readings, not four waits.
    let run = run(
        policy(Some(Duration::from_millis(200))),
        Duration::from_millis(50),
        ScriptedAdapter::new(1, vec!["cursor=0"], Vec::new()),
        ScriptedEndpoint::unchanging(None),
    );
    let cadence: Vec<Duration> = run
        .clock
        .slept()
        .into_iter()
        .take_while(|slept| *slept <= Duration::from_millis(50))
        .collect();
    assert_eq!(
        cadence,
        vec![Duration::from_millis(50); 4],
        "four waits of one interval each, and then the guard: the transport \
         holds when the next request is due and nothing else"
    );
}

// ---------------------------------------------------------------------------
// What is polled is the adapter's to change
// ---------------------------------------------------------------------------

#[test]
fn what_the_adapter_writes_at_connect_reaches_the_first_poll_and_what_it_writes_later_reaches_the_next(
) {
    // The second half is what makes this transport and `poll_upstream` one
    // mechanism rather than two: an adapter that has just been told about a new
    // instrument changes what is polled through the same call that subscribes
    // one on a websocket.
    let run = run(
        policy(None),
        Duration::from_secs(2),
        ScriptedAdapter::new(1, vec!["cursor=0"], vec!["cursor=1"]),
        ScriptedEndpoint::unchanging(None),
    );

    let parameters: Vec<Option<String>> = run
        .endpoint
        .seen()
        .into_iter()
        .map(|seen| seen.parameters)
        .collect();

    assert_eq!(
        parameters[0], None,
        "the connect probe carries nothing: the adapter has not been asked yet"
    );
    assert_eq!(
        parameters[1],
        Some("cursor=0".to_string()),
        "what the adapter wrote at connect reaches the first poll"
    );
    let after = parameters
        .iter()
        .position(|seen| seen.as_deref() == Some("cursor=1"))
        .expect("what the adapter wrote through poll_upstream must reach a request");
    assert!(
        parameters[1..after]
            .iter()
            .all(|seen| seen.as_deref() == Some("cursor=0")),
        "and nothing before it carries the new cursor: {parameters:?}"
    );
    assert!(
        !run.adapter.upstream_polls.is_empty(),
        "the mid-session ask is what wrote the second cursor"
    );
    assert!(
        run.adapter.upstream_polls[0].1 > 0,
        "and it happened after a payload rather than beside the logon write, \
         which is the difference between this mechanism and `on_connected`"
    );
}

#[test]
fn two_polled_sources_are_two_cursors() {
    // One adapter object serves every source, which is why it is handed a
    // `ConnectionId` with every write. Two polled sources are two cursors, and
    // a cursor held anywhere shared - beside the client, say, which is the
    // thing two connections do share - would have one of them serving both.
    //
    // Interleaved deliberately: two runs one after the other would pass against
    // shared state, because each would write its own cursor immediately before
    // using it.
    let endpoint = ScriptedEndpoint::unchanging(None);
    let clock = TestClock::new();
    let mut catalogue = input(CATALOGUE, Duration::from_secs(30), &endpoint, &clock);
    let mut refdata = input(REFDATA, Duration::from_secs(30), &endpoint, &clock);

    block_on(catalogue.connect(Duration::from_secs(5))).expect("the endpoint answered");
    block_on(refdata.connect(Duration::from_secs(5))).expect("the endpoint answered");
    block_on(catalogue.send(UpstreamMessage::Text("cursor=catalogue"))).expect("a text write");
    block_on(refdata.send(UpstreamMessage::Text("cursor=refdata"))).expect("a text write");
    block_on(catalogue.recv(None)).expect("the catalogue's poll");
    block_on(refdata.recv(None)).expect("the reference data's poll");

    let parameters: Vec<Option<String>> = endpoint
        .seen()
        .into_iter()
        .map(|seen| seen.parameters)
        .collect();
    assert_eq!(
        parameters,
        vec![
            // Two probes, neither of which has been given anything yet.
            None,
            None,
            Some("cursor=catalogue".to_string()),
            Some("cursor=refdata".to_string()),
        ],
        "each source's request carries its own cursor"
    );
}

#[test]
fn binary_written_to_a_polled_endpoint_is_a_fault_retrying_cannot_fix() {
    // What a poll sends upstream is the next request's parameters, and bytes
    // that are not text are not parameters. Fatal rather than an ended
    // connection because the adapter would write the same bytes on the next
    // connection: retrying under a backoff hides a mapping that has to be
    // fixed, and of the two mistakes available the loud one is the recoverable
    // one.
    let endpoint = ScriptedEndpoint::unchanging(None);
    let clock = TestClock::new();
    let mut transport = input(CONNECTION, Duration::from_secs(30), &endpoint, &clock);

    block_on(transport.connect(Duration::from_secs(5))).expect("the endpoint answered");
    let error = block_on(transport.send(UpstreamMessage::Binary(b"\x01\x02")))
        .expect_err("binary is not a request's parameters");

    assert!(error.is_fatal(), "{error}");
    assert!(
        !format!("{error}").contains("catalogue"),
        "and the message names neither the endpoint nor what was written: {error}"
    );
}

// ---------------------------------------------------------------------------
// What a log line may carry
// ---------------------------------------------------------------------------

#[test]
fn no_error_detail_carries_the_endpoints_query_string() {
    // An endpoint with a key in its query string is a shape several venue APIs
    // use, and configuration keeps credentials in files precisely so that they
    // do not reach a log line. Every detail this transport raises carries the
    // authority instead.
    let endpoint = ScriptedEndpoint::new(
        vec![Answered::Failed(RequestFailure::Refused("refused".into()))],
        Answered::Body(CATALOGUE_BODY, None),
    );
    let clock = TestClock::new();
    let mut transport = PollInput::new(
        CONNECTION,
        &PollConfig {
            endpoint: "http://192.0.2.10/catalogue?api_key=not-a-real-secret".to_string(),
            poll_interval: Duration::from_secs(30),
        },
        Arc::clone(&endpoint) as Arc<dyn PollClient>,
        Arc::clone(&clock) as Arc<dyn Clock>,
    )
    .expect("a usable endpoint");

    let error = block_on(transport.connect(Duration::from_secs(5))).expect_err("refused");
    assert!(!format!("{error}").contains("api_key"), "{error}");
    assert!(format!("{error}").contains("192.0.2.10"), "{error}");
    let rendered = format!("{transport:?}");
    assert!(!rendered.contains("api_key"), "{rendered}");
    assert!(rendered.contains("192.0.2.10"), "{rendered}");
}
