//! The driver, against a scripted transport and a clock the test owns.
//!
//! # How this suite stays network-free, and how you can tell
//!
//! Nothing here opens a socket, and nothing here sleeps. Both are structural
//! rather than a convention someone has to remember:
//!
//! - The transport is a [`ScriptedInput`], which answers from a list. A test
//!   that wanted a real socket would have to write a second `Input`, which is
//!   the visible act this arrangement is for.
//! - The clock is a [`TestClock`], which records what it was asked to wait for
//!   and advances itself instead of waiting. So a backoff sequence is a `Vec`
//!   to compare against, and a 30-second ceiling costs the suite nothing.
//! - [`block_on`] **panics on `Poll::Pending`**. Every future in this suite is
//!   therefore proven, by running, to be one that never waits on anything
//!   outside the process: no timer, no socket, no other task. Add an await on
//!   something real and this suite fails rather than hanging in CI.

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

const CONNECTION: ConnectionId = ConnectionId::new("mktdata");

/// A wall-clock reading distinctive enough that a payload carrying it cannot
/// have got it from anywhere else.
const WALL_NS: u64 = 1_760_000_000_123_456_789;

/// Two transport-supplied receive stamps, far enough apart that an event
/// attributed to the wrong one could not be mistaken for plausible.
const RECV_A: u64 = 1_760_000_000_100_000_000;
const RECV_B: u64 = 1_760_000_000_200_000_000;

/// The venue's own timestamp the scripted adapter puts on its events: 250
/// microseconds before `RECV_A`, so that the interval
/// `dz_publisher_venue_to_recv_latency_seconds` measures is a number a test can
/// state rather than a sign it can only check.
const VENUE_TS_NS: u64 = RECV_A - 250_000;

// ---------------------------------------------------------------------------
// Running a future without a runtime
// ---------------------------------------------------------------------------

/// Polls a future to completion, refusing to wait.
///
/// The refusal is the point. This crate is meant to name no runtime, and the
/// driver is meant to await nothing but the clock and the transport it was
/// handed. A `Pending` here means one of those two claims has stopped being
/// true, and it is better to be told that by a panic than by a suite that hangs
/// on a machine with no network.
fn block_on<F: Future>(future: F) -> F::Output {
    // A waker that does nothing, because nothing here has anything to wake:
    // there is no reactor and no second task.
    let mut context = Context::from_waker(Waker::noop());
    let mut future = Box::pin(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!(
            "a future in this suite waited on something outside the process; \
             the driver may only await the clock and the transport it was given"
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
struct TestClock {
    state: Mutex<ClockState>,
}

impl TestClock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(ClockState::default()),
        })
    }

    /// Move time on without a wait, which is what the transport does while it
    /// is holding a receive open.
    fn advance(&self, by: Duration) {
        let mut state = self.state.lock().expect("clock");
        state.steady_ns = state
            .steady_ns
            .saturating_add(u64::try_from(by.as_nanos()).expect("a test-sized duration"));
    }

    /// Every wait the driver asked for, in order.
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
        self.state.lock().expect("clock").slept.push(duration);
        self.advance(duration);
        Box::pin(std::future::ready(()))
    }
}

// ---------------------------------------------------------------------------
// The transport
// ---------------------------------------------------------------------------

/// One thing a scripted connection does when the driver receives.
enum Read {
    /// A payload, stamped by the driver's own clock.
    Payload(&'static [u8]),
    /// A payload the transport had a better timestamp for, as a kernel receive
    /// timestamp would be.
    Stamped(&'static [u8], u64),
    /// Traffic that keeps the socket alive and says nothing, having taken this
    /// long to arrive.
    Keepalive(Duration),
    /// Nothing arrived within the budget. The transport consumes the whole
    /// budget it was handed, as a real one would.
    Silence,
    /// The connection ended.
    Ended(DisconnectReason),
    /// Something retrying cannot fix.
    Fatal,
}

/// One scripted connection: what `connect` does, what `send` does, and what the
/// receives yield.
struct Connection {
    /// `None` connects. `Some` is what `connect` returns instead.
    connect: Option<IngressError>,
    /// `None` sends. `Some` is what the first `send` returns instead.
    send: Option<IngressError>,
    reads: VecDeque<Read>,
}

impl Connection {
    fn live(reads: Vec<Read>) -> Self {
        Self {
            connect: None,
            send: None,
            reads: reads.into(),
        }
    }

    fn refused() -> Self {
        Self {
            connect: Some(IngressError::connect(
                ConnectFailureReason::Refused,
                "refused",
            )),
            send: None,
            reads: VecDeque::new(),
        }
    }
}

/// A transport that answers from a script.
struct ScriptedInput {
    connections: VecDeque<Connection>,
    current: Option<Connection>,
    clock: Arc<TestClock>,
    /// Every receive budget the driver handed over, in order. This is how the
    /// idle guard's arithmetic is asserted.
    budgets: Vec<Option<Duration>>,
    /// Every message the driver sent upstream, in order.
    sent: Vec<String>,
    connects: usize,
    shutdowns: usize,
}

impl ScriptedInput {
    fn new(clock: Arc<TestClock>, connections: Vec<Connection>) -> Self {
        Self {
            connections: connections.into(),
            current: None,
            clock,
            budgets: Vec::new(),
            sent: Vec::new(),
            connects: 0,
            shutdowns: 0,
        }
    }
}

impl Input for ScriptedInput {
    fn connection(&self) -> ConnectionId {
        CONNECTION
    }

    fn connect(&mut self, _timeout: Duration) -> BoxFuture<'_, Result<(), IngressError>> {
        Box::pin(async move {
            let Some(mut connection) = self.connections.pop_front() else {
                // How every test in this suite terminates: the script runs out,
                // and a fault retrying cannot fix is the one thing that stops
                // the driver.
                return Err(IngressError::fatal("the script has no more connections"));
            };
            if let Some(error) = connection.connect.take() {
                return Err(error);
            }
            self.connects += 1;
            self.current = Some(connection);
            Ok(())
        })
    }

    fn send<'a>(
        &'a mut self,
        message: UpstreamMessage<'a>,
    ) -> BoxFuture<'a, Result<(), IngressError>> {
        Box::pin(async move {
            let rendered = match message {
                UpstreamMessage::Text(text) => text.to_string(),
                UpstreamMessage::Binary(bytes) => format!("binary:{}", bytes.len()),
            };
            self.sent.push(rendered);
            match self.current.as_mut().and_then(|open| open.send.take()) {
                Some(error) => Err(error),
                None => Ok(()),
            }
        })
    }

    fn recv<'a>(
        &'a mut self,
        budget: Option<Duration>,
    ) -> BoxFuture<'a, Result<Received<'a>, IngressError>> {
        Box::pin(async move {
            self.budgets.push(budget);
            let open = self
                .current
                .as_mut()
                .expect("the driver received on a connection it had not opened");
            let read = open
                .reads
                .pop_front()
                .expect("the driver received past the end of a scripted connection");
            match read {
                Read::Payload(bytes) => Ok(Received::Payload { bytes, ts_ns: None }),
                Read::Stamped(bytes, ts_ns) => Ok(Received::Payload {
                    bytes,
                    ts_ns: Some(ts_ns),
                }),
                Read::Keepalive(after) => {
                    self.clock.advance(after);
                    Ok(Received::Liveness)
                }
                Read::Silence => {
                    self.clock
                        .advance(budget.expect("silence needs a budget to consume"));
                    Ok(Received::Idle)
                }
                Read::Ended(reason) => Err(IngressError::ended(reason, "scripted")),
                Read::Fatal => Err(IngressError::fatal("scripted")),
            }
        })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.shutdowns += 1;
            self.current = None;
        })
    }
}

// ---------------------------------------------------------------------------
// The adapter
// ---------------------------------------------------------------------------

/// The bytes this adapter refuses, so that a parse error is a scripted event
/// rather than a malformed fixture.
const UNREADABLE: &[u8] = b"not-readable";

/// A payload this adapter maps to nothing at all. Ordinary rather than
/// exceptional: a heartbeat, an acknowledgement, or an update for an instrument
/// it holds no handle for.
const SILENT: &[u8] = b"heartbeat";

/// A payload carrying three upstream messages, which is three events out of one
/// receive stamp.
const BATCH: &[u8] = b"batch-of-three";

/// A payload the adapter answers by trying to state a receive stamp of its own.
const FORGED: &[u8] = b"forge-a-stamp";

/// The stamp it tries to state. Nowhere near a real reading, so a test can tell
/// at a glance whether it got anywhere.
const FORGED_STAMP: u64 = 1;

/// A payload after which the adapter no longer trusts its own book.
const DIVERGED: &[u8] = b"upstream-gap";

#[derive(Default)]
struct RecordingAdapter {
    connected: Vec<ConnectionId>,
    disconnected: Vec<(ConnectionId, DisconnectReason)>,
    /// The bytes and the receive timestamp of every payload, in order.
    payloads: Vec<(Vec<u8>, u64)>,
    /// What each successive `on_connected` returns. Exhausted means success.
    connect_results: VecDeque<Result<(), AdapterError>>,
    /// What to write upstream from `on_connected`.
    subscriptions: Vec<&'static str>,
}

impl Adapter for RecordingAdapter {
    fn message_types(&self) -> &[&'static str] {
        &["quote"]
    }

    fn poll_listings(&mut self, _out: &mut dyn ListingSink) {}

    fn on_connected(
        &mut self,
        conn: ConnectionId,
        out: &mut dyn UpstreamSink,
    ) -> Result<(), AdapterError> {
        self.connected.push(conn);
        if let Some(Err(error)) = self.connect_results.pop_front() {
            return Err(error);
        }
        for subscription in &self.subscriptions {
            out.send_text(subscription);
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
        self.payloads
            .push((payload.bytes.to_vec(), payload.recv_ts_ns));
        if payload.bytes == UNREADABLE {
            return Err(ParseError::malformed("bid_px"));
        }
        if payload.bytes == SILENT {
            return Ok(());
        }
        if payload.bytes == DIVERGED {
            // The one thing only the adapter can know. It has to reach the
            // runtime, which is the layer that pauses the instrument and
            // schedules the recovery a subscriber needs.
            out.desynchronised(InstrumentRef::from_admission(0), Desync::UpstreamGap);
            return Ok(());
        }
        if payload.bytes == FORGED {
            // The one thing an adapter must not be able to do. This is the sink
            // the driver handed over, so the call goes to the wrapper and stops
            // there - an adapter's guess at when a payload arrived is never
            // what a latency is measured from.
            out.payload_scope(Some(FORGED_STAMP));
        }
        // A payload carrying a batch is several upstream messages, and the
        // boundary's contract is one `upstream_message` per member.
        let members = if payload.bytes == BATCH { 3 } else { 1 };
        for member in 0..members {
            out.upstream_message("quote");
            out.event(Event::Quote {
                instrument: InstrumentRef::from_admission(0),
                // Each member of a batch carries its own venue timestamp, so an
                // event attributed to the wrong one is visible rather than
                // merely uncounted.
                source_ts_ns: VENUE_TS_NS + member,
                bid: SideUpdate::Present {
                    px: Scalar::text("1.00"),
                    qty: Scalar::text("5"),
                    source_count: None,
                },
                ask: SideUpdate::Gone,
            });
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The sinks
// ---------------------------------------------------------------------------

/// The venue's own timestamp on an event, whichever variant carries it.
fn source_ts_ns(event: Event<'_>) -> u64 {
    match event {
        Event::Quote { source_ts_ns, .. }
        | Event::Trade { source_ts_ns, .. }
        | Event::Level { source_ts_ns, .. }
        | Event::Clear { source_ts_ns, .. } => source_ts_ns,
        // `Event` is `#[non_exhaustive]`: a variant added later is one this
        // suite has not been taught to read, and reading it as zero here would
        // be a latency of half a century rather than a test failure.
        _ => panic!("an event variant this suite cannot read"),
    }
}

/// The runtime's own sink, holding the payload attribution the way a runtime
/// has to hold it.
#[derive(Default)]
struct RecordingEvents {
    message_types: Vec<&'static str>,
    events: usize,
    /// The payload being mapped: `Some` between the two halves of one scope,
    /// `None` outside. This is the whole of what a runtime keeps.
    in_force: Option<u64>,
    /// Every scope transition, in order, so that a test can assert the pairing
    /// and not just the value.
    scopes: Vec<Option<u64>>,
    /// Every instrument the adapter stopped trusting its book for, in order.
    desynchronised: Vec<(InstrumentRef, Desync)>,
    /// One entry per event: the payload receive stamp it was attributable to,
    /// and the venue timestamp it carried itself. Those are the two halves of
    /// `dz_publisher_venue_to_recv_latency_seconds`, and the first half is the
    /// one that could not be reached from here before.
    attributed: Vec<(Option<u64>, u64)>,
}

impl EventSink for RecordingEvents {
    fn upstream_message(&mut self, message_type: &'static str) {
        self.message_types.push(message_type);
    }

    fn event(&mut self, event: Event<'_>) {
        self.events += 1;
        self.attributed.push((self.in_force, source_ts_ns(event)));
    }

    fn desynchronised(&mut self, instrument: InstrumentRef, reason: Desync) {
        self.desynchronised.push((instrument, reason));
    }

    fn payload_scope(&mut self, recv_ts_ns: Option<u64>) {
        self.scopes.push(recv_ts_ns);
        self.in_force = recv_ts_ns;
    }
}

#[derive(Default)]
struct Recorded {
    messages: Vec<(&'static str, &'static str)>,
    bytes: u64,
    duplicates: usize,
    parse_errors: Vec<&'static str>,
    states: Vec<(&'static str, bool)>,
    reconnects: Vec<DisconnectReason>,
    connect_failures: Vec<ConnectFailureReason>,
    rate_limited: usize,
    adapter_errors: usize,
}

#[derive(Default)]
struct TestObserver {
    recorded: Mutex<Recorded>,
}

impl TestObserver {
    fn recorded(&self) -> std::sync::MutexGuard<'_, Recorded> {
        self.recorded.lock().expect("observer")
    }
}

impl IngressObserver for TestObserver {
    fn message(&self, message_type: &'static str, connection: &'static str) {
        self.recorded().messages.push((message_type, connection));
    }

    fn bytes(&self, count: u64) {
        self.recorded().bytes += count;
    }

    fn duplicate(&self) {
        self.recorded().duplicates += 1;
    }

    fn parse_error(&self, error: ParseError) {
        self.recorded().parse_errors.push(error.as_str());
    }

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

    fn adapter_error(&self, _error: AdapterError) {
        self.recorded().adapter_errors += 1;
    }
}

// ---------------------------------------------------------------------------
// Running one
// ---------------------------------------------------------------------------

fn policy() -> Policy {
    Policy {
        connect_timeout: Duration::from_secs(5),
        backoff: BackoffPolicy::new(Duration::from_millis(500), Duration::from_secs(30))
            .expect("a valid policy"),
        rate_limit_per_second: 0,
        idle_timeout: None,
    }
}

/// Everything one run produced, for the assertions to read.
struct Run {
    adapter: RecordingAdapter,
    observer: Arc<TestObserver>,
    clock: Arc<TestClock>,
    events: RecordingEvents,
    sent: Vec<String>,
    connects: usize,
    shutdowns: usize,
    budgets: Vec<Option<Duration>>,
    exit: IngressError,
}

/// Drives one script to its fatal end and hands back what happened.
fn run(policy: Policy, adapter: RecordingAdapter, connections: Vec<Connection>) -> Run {
    let clock = TestClock::new();
    let observer = Arc::new(TestObserver::default());
    let mut input = ScriptedInput::new(Arc::clone(&clock), connections);
    let mut adapter = adapter;
    let mut events = RecordingEvents::default();

    let exit = {
        let mut driver = Driver::new(
            &mut input,
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
        events,
        sent: input.sent,
        connects: input.connects,
        shutdowns: input.shutdowns,
        budgets: input.budgets,
        exit,
    }
}

// ---------------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------------

#[test]
fn on_connected_runs_on_every_successful_connect_including_reconnects() {
    // The reason that method exists. A venue's subscriptions live on its
    // session, so a reconnect that does not re-subscribe leaves a publisher
    // with an open socket, a connection gauge at 1, and no data - which is why
    // asserting the *count* is the assertion that matters here.
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        vec![
            Connection::live(vec![
                Read::Payload(b"a"),
                Read::Ended(DisconnectReason::RemoteClose),
            ]),
            Connection::live(vec![
                Read::Payload(b"b"),
                Read::Ended(DisconnectReason::RemoteClose),
            ]),
            Connection::live(vec![
                Read::Payload(b"c"),
                Read::Ended(DisconnectReason::Timeout),
            ]),
        ],
    );

    assert_eq!(outcome.connects, 3);
    assert_eq!(outcome.adapter.connected, vec![CONNECTION; 3]);
    assert!(outcome.exit.is_fatal());
}

#[test]
fn every_connect_is_paired_with_exactly_one_disconnect() {
    // What lets an adapter reset its per-connection state in one place. An
    // adapter tracking the upstream's own sequence numbering has to reset it
    // somewhere, and a missing pairing makes the first payload of the new
    // connection look like a gap.
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        vec![
            Connection::live(vec![Read::Ended(DisconnectReason::RemoteClose)]),
            Connection::live(vec![Read::Ended(DisconnectReason::AuthExpired)]),
            // Ends on a fatal read: the pairing has to hold on the way out too.
            Connection::live(vec![Read::Fatal]),
        ],
    );

    assert_eq!(outcome.adapter.connected.len(), 3);
    assert_eq!(
        outcome
            .adapter
            .disconnected
            .iter()
            .map(|(_, reason)| *reason)
            .collect::<Vec<_>>(),
        vec![
            DisconnectReason::RemoteClose,
            DisconnectReason::AuthExpired,
            // A fatal transport fault still ended a connection the adapter was
            // told about, reported as the least specific of the four rather
            // than as an invented fifth.
            DisconnectReason::RemoteClose,
        ]
    );
    assert!(outcome
        .adapter
        .disconnected
        .iter()
        .all(|(conn, _)| *conn == CONNECTION));
    assert_eq!(outcome.shutdowns, 3, "every connection is released");
}

#[test]
fn a_connect_that_never_succeeded_pairs_with_nothing_and_counts_no_reconnect() {
    // `dz_publisher_ingress_reconnects_total` has four label values and all
    // four describe a session that existed and stopped. Folding a refused
    // connect into one of them would make the counter mean two things; the
    // series for this case is the connection-state gauge staying at 0, and
    // `connect_failures_total{reason}`, which is what says why.
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        vec![Connection::refused(), Connection::refused()],
    );

    assert!(outcome.adapter.connected.is_empty());
    assert!(outcome.adapter.disconnected.is_empty());
    let recorded = outcome.observer.recorded();
    assert!(recorded.reconnects.is_empty());
    assert!(
        !recorded.states.iter().any(|(_, connected)| *connected),
        "nothing may report itself connected"
    );
    // Once per attempt, with the reason the transport classified — the pair of
    // numbers that separates a publisher which never came up from one that is
    // flapping, which no single family could say.
    assert_eq!(
        recorded.connect_failures,
        vec![ConnectFailureReason::Refused, ConnectFailureReason::Refused]
    );
}

#[test]
fn a_payload_reaches_the_adapter_with_its_bytes_and_a_receive_timestamp() {
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        vec![Connection::live(vec![
            Read::Payload(b"one"),
            // A transport with a kernel receive timestamp has a better reading
            // than the driver's own, taken before every scheduling delay
            // between the packet and the read.
            Read::Stamped(b"two", 42),
            Read::Ended(DisconnectReason::RemoteClose),
        ])],
    );

    assert_eq!(
        outcome.adapter.payloads,
        vec![(b"one".to_vec(), WALL_NS), (b"two".to_vec(), 42)]
    );
    // Bytes are counted as the adapter sees them: payload bytes, not bytes off
    // the socket.
    assert_eq!(outcome.observer.recorded().bytes, 6);
}

#[test]
fn what_the_adapter_names_is_counted_and_forwarded_with_its_events() {
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        vec![Connection::live(vec![
            Read::Payload(b"one"),
            Read::Payload(b"two"),
            Read::Ended(DisconnectReason::RemoteClose),
        ])],
    );

    // The adapter got the series by naming its message type, which is what the
    // boundary's own docstring promises, and the name reached the runtime's sink
    // as well as the counter.
    assert_eq!(
        outcome.observer.recorded().messages,
        vec![("quote", "mktdata"), ("quote", "mktdata")]
    );
    assert_eq!(outcome.events.message_types, vec!["quote", "quote"]);
    assert_eq!(outcome.events.events, 2);
}

#[test]
fn a_disconnect_reaches_the_adapter_with_each_of_the_four_reasons() {
    // The four are a metric label. An adapter told why a connection went away
    // is told in the vocabulary the dashboard groups by, and a reason that
    // never arrives is a panel that never moves.
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        DisconnectReason::ALL
            .iter()
            .map(|reason| Connection::live(vec![Read::Ended(*reason)]))
            .collect(),
    );

    assert_eq!(
        outcome
            .adapter
            .disconnected
            .iter()
            .map(|(_, reason)| *reason)
            .collect::<Vec<_>>(),
        DisconnectReason::ALL.to_vec()
    );
    assert_eq!(
        outcome.observer.recorded().reconnects,
        DisconnectReason::ALL.to_vec()
    );
}

#[test]
fn a_rate_limit_disconnect_records_the_series_that_says_the_venue_did_it() {
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        vec![Connection::live(vec![Read::Ended(
            DisconnectReason::RateLimit,
        )])],
    );

    let recorded = outcome.observer.recorded();
    assert_eq!(recorded.rate_limited, 1);
    assert_eq!(recorded.reconnects, vec![DisconnectReason::RateLimit]);
}

#[test]
fn a_parse_error_ends_the_payload_and_not_the_connection_and_is_not_retried() {
    // The contract the boundary's rustdoc states, kept here. Retrying the same
    // bytes through the same adapter can only produce the same error, and
    // dropping the connection over one unreadable message hands a venue the
    // ability to darken a feed with a typo.
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        vec![Connection::live(vec![
            Read::Payload(b"before"),
            Read::Payload(UNREADABLE),
            Read::Payload(b"after"),
            Read::Ended(DisconnectReason::RemoteClose),
        ])],
    );

    let seen: Vec<&[u8]> = outcome
        .adapter
        .payloads
        .iter()
        .map(|(bytes, _)| bytes.as_slice())
        .collect();
    // Each payload exactly once, in order: the bad one was not re-offered, and
    // the ones after it still arrived.
    assert_eq!(seen, vec![b"before".as_slice(), UNREADABLE, b"after"]);
    assert_eq!(outcome.observer.recorded().parse_errors, vec!["malformed"]);
    // One connection, and the events either side of the failure both got
    // through.
    assert_eq!(outcome.connects, 1);
    assert_eq!(outcome.events.events, 2);
}

#[test]
fn an_adapter_error_on_connect_is_counted_and_retried_under_the_backoff() {
    let mut adapter = RecordingAdapter::default();
    adapter
        .connect_results
        .push_back(Err(AdapterError::NotReady {
            detail: "credentials",
        }));
    adapter
        .connect_results
        .push_back(Err(AdapterError::NotReady {
            detail: "credentials",
        }));

    let outcome = run(
        policy(),
        adapter,
        vec![
            Connection::live(vec![]),
            Connection::live(vec![]),
            Connection::live(vec![Read::Ended(DisconnectReason::RemoteClose)]),
        ],
    );

    let recorded = outcome.observer.recorded();
    assert_eq!(recorded.adapter_errors, 2);
    // Never announced as connected: a socket that is up and subscribed to
    // nothing is not a connection an alert should call healthy.
    let announced: Vec<bool> = recorded
        .states
        .iter()
        .take(2)
        .map(|(_, connected)| *connected)
        .collect();
    assert_eq!(announced, vec![false, false]);
    // Retried, and each retry further along the sequence rather than at the
    // same delay: an adapter that cannot compose its subscription usually
    // cannot yet read a credential, and hammering does not make it readable.
    assert_eq!(
        outcome.clock.slept(),
        vec![
            Duration::from_millis(500),
            Duration::from_secs(1),
            Duration::from_secs(2),
        ]
    );
    drop(recorded);
    assert_eq!(outcome.adapter.connected.len(), 3);
}

#[test]
fn the_delay_sequence_doubles_across_failures_and_resets_for_a_proven_connection() {
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        vec![
            Connection::refused(),
            Connection::refused(),
            Connection::refused(),
            // Accepted and immediately closed, having delivered nothing. This
            // is the shape of being throttled or of an expired credential, and
            // it must not reset the sequence.
            Connection::live(vec![Read::Ended(DisconnectReason::RemoteClose)]),
            // Delivered a payload: proven, and the sequence starts over.
            Connection::live(vec![
                Read::Payload(b"a"),
                Read::Ended(DisconnectReason::RemoteClose),
            ]),
            Connection::refused(),
        ],
    );

    assert_eq!(
        outcome.clock.slept(),
        vec![
            Duration::from_millis(500),
            Duration::from_secs(1),
            Duration::from_secs(2),
            Duration::from_secs(4),
            // The reset: back to the initial delay, not to zero. A venue that
            // closes a healthy connection on purpose - a session boundary, a
            // maintenance window - must not be reconnected against instantly.
            Duration::from_millis(500),
            Duration::from_secs(1),
        ]
    );
}

#[test]
fn a_connection_that_delivered_and_was_then_rate_limited_does_not_reset_the_sequence() {
    // The venue has just said we are going too fast. Starting the sequence over
    // because the connection had been productive is how that becomes a ban
    // rather than a delay.
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        vec![
            Connection::live(vec![
                Read::Payload(b"a"),
                Read::Ended(DisconnectReason::RateLimit),
            ]),
            Connection::live(vec![
                Read::Payload(b"b"),
                Read::Ended(DisconnectReason::RateLimit),
            ]),
            Connection::refused(),
        ],
    );

    assert_eq!(
        outcome.clock.slept(),
        vec![
            Duration::from_millis(500),
            Duration::from_secs(1),
            Duration::from_secs(2),
        ]
    );
}

#[test]
fn what_the_adapter_wrote_on_connect_is_sent_upstream_in_order() {
    let adapter = RecordingAdapter {
        // The order is the difference between a session and a rejection, and
        // the adapter has no other way to express it.
        subscriptions: vec!["auth", "subscribe:quotes"],
        ..RecordingAdapter::default()
    };
    let outcome = run(
        policy(),
        adapter,
        vec![Connection::live(vec![Read::Ended(
            DisconnectReason::RemoteClose,
        )])],
    );

    assert_eq!(outcome.sent, vec!["auth", "subscribe:quotes"]);
    // And announced connected only after both went out.
    assert_eq!(
        outcome.observer.recorded().states.first().copied(),
        Some(("mktdata", true))
    );
}

#[test]
fn a_send_that_fails_reconnects_and_asks_the_adapter_to_subscribe_again() {
    let adapter = RecordingAdapter {
        subscriptions: vec!["subscribe:quotes"],
        ..RecordingAdapter::default()
    };
    let outcome = run(
        policy(),
        adapter,
        vec![
            Connection {
                connect: None,
                send: Some(IngressError::ended(
                    DisconnectReason::RemoteClose,
                    "closed under us",
                )),
                reads: VecDeque::new(),
            },
            Connection::live(vec![Read::Ended(DisconnectReason::RemoteClose)]),
        ],
    );

    assert_eq!(outcome.adapter.connected.len(), 2);
    assert_eq!(outcome.sent, vec!["subscribe:quotes", "subscribe:quotes"]);
    // The failed subscription never counted as a reconnect - there was no
    // established, subscribed connection to end - but the second one did.
    assert_eq!(
        outcome.observer.recorded().reconnects,
        vec![DisconnectReason::RemoteClose]
    );
}

#[test]
fn the_outbound_rate_limit_defers_a_send_rather_than_dropping_it() {
    let adapter = RecordingAdapter {
        subscriptions: vec!["one", "two", "three"],
        ..RecordingAdapter::default()
    };
    let outcome = run(
        Policy {
            rate_limit_per_second: 5,
            ..policy()
        },
        adapter,
        vec![Connection::live(vec![Read::Ended(
            DisconnectReason::RemoteClose,
        )])],
    );

    // All three sent, spaced at 200ms - not two dropped to stay under the
    // limit. A dropped subscription is a silently missing instrument.
    assert_eq!(outcome.sent, vec!["one", "two", "three"]);
    // The first goes at once and each of the other two waits its 200ms slot,
    // which is the spacing rather than a growing wait: the limiter measures
    // from the previous send, so three sends occupy 400ms and not 600ms.
    assert_eq!(
        outcome.clock.slept(),
        vec![
            Duration::from_millis(200),
            Duration::from_millis(200),
            // The reconnect delay after the scripted close.
            Duration::from_millis(500),
        ]
    );
    // And our own pacing is not the venue rate-limiting us: that series must
    // not move.
    assert_eq!(outcome.observer.recorded().rate_limited, 0);
}

#[test]
fn the_idle_guard_measures_time_since_the_last_payload_and_not_since_any_traffic() {
    // The failure this exists for: a venue quietly drops a subscription and
    // keeps answering keepalives. The socket is healthy, the connection gauge
    // reads 1, and no data arrives - forever, if the guard is satisfied by
    // traffic rather than by payloads.
    let outcome = run(
        Policy {
            idle_timeout: Some(Duration::from_secs(60)),
            ..policy()
        },
        RecordingAdapter::default(),
        vec![
            Connection::live(vec![
                Read::Keepalive(Duration::from_secs(20)),
                Read::Keepalive(Duration::from_secs(20)),
                Read::Keepalive(Duration::from_secs(20)),
                Read::Silence,
            ]),
            Connection::refused(),
        ],
    );

    // The budget handed to each receive is what is left of the guard, so three
    // answered keepalives spend it rather than renewing it.
    assert_eq!(
        outcome.budgets,
        vec![
            Some(Duration::from_secs(60)),
            Some(Duration::from_secs(40)),
            Some(Duration::from_secs(20)),
            Some(Duration::ZERO),
        ]
    );
    assert_eq!(
        outcome.adapter.disconnected,
        vec![(CONNECTION, DisconnectReason::Timeout)]
    );
    assert_eq!(
        outcome.observer.recorded().reconnects,
        vec![DisconnectReason::Timeout]
    );
}

#[test]
fn a_payload_renews_the_idle_budget_in_full() {
    let outcome = run(
        Policy {
            idle_timeout: Some(Duration::from_secs(60)),
            ..policy()
        },
        RecordingAdapter::default(),
        vec![
            Connection::live(vec![
                Read::Keepalive(Duration::from_secs(30)),
                Read::Payload(b"a"),
                Read::Keepalive(Duration::from_secs(30)),
                Read::Silence,
            ]),
            Connection::refused(),
        ],
    );

    assert_eq!(
        outcome.budgets,
        vec![
            Some(Duration::from_secs(60)),
            Some(Duration::from_secs(30)),
            // The payload arrived, so the whole guard is available again.
            Some(Duration::from_secs(60)),
            Some(Duration::from_secs(30)),
        ]
    );
}

#[test]
fn no_idle_timeout_means_no_budget_at_all() {
    // A transport with no guard waits indefinitely rather than being handed a
    // large number that would eventually fire and look like a venue fault.
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        vec![Connection::live(vec![
            Read::Payload(b"a"),
            Read::Ended(DisconnectReason::RemoteClose),
        ])],
    );

    assert_eq!(outcome.budgets, vec![None, None]);
}

#[test]
fn a_fatal_transport_fault_stops_the_driver_rather_than_retrying_it_forever() {
    // A publisher pointed at an endpoint that does not parse as one would
    // otherwise retry against it every thirty seconds for as long as nobody
    // looks at a dashboard.
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        vec![
            Connection {
                connect: Some(IngressError::fatal("not an endpoint")),
                send: None,
                reads: VecDeque::new(),
            },
            // Never reached.
            Connection::live(vec![Read::Payload(b"a")]),
        ],
    );

    assert!(outcome.exit.is_fatal());
    assert_eq!(outcome.connects, 0);
    assert!(
        outcome.clock.slept().is_empty(),
        "it did not back off first"
    );
}

#[test]
fn nothing_in_this_crate_can_report_a_duplicate() {
    // Not an aspiration: `dz_publisher_ingress_duplicates_total` exists and the
    // boundary gives an adapter no way to report the upstream's own sequence
    // number or to call a payload a repeat. Until it does, this series cannot
    // move from here, and a test saying so is how that stays visible rather
    // than being read as a driver that forgot.
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        vec![Connection::live(vec![
            Read::Payload(b"a"),
            Read::Payload(b"a"),
            Read::Ended(DisconnectReason::RemoteClose),
        ])],
    );

    assert_eq!(outcome.observer.recorded().duplicates, 0);
}

// ---------------------------------------------------------------------------
// Attributing an event to the payload that produced it
// ---------------------------------------------------------------------------
//
// The two latency families that measure from a payload's arrival —
// `dz_publisher_venue_to_recv_latency_seconds` and
// `dz_publisher_recv_to_send_latency_seconds` — need the payload's receive
// stamp at the moment an event reaches the runtime's sink. `EventSink` had no
// way to carry it, and asking the adapter to pass it through would have been a
// convention every venue had to remember, whose failure is a silent zero.
//
// So the driver states it, and every test below drives a real
// `Adapter::on_payload` through `Driver::run` rather than constructing the
// driver's wrapper: the wrapper is private, which is itself the guarantee that
// nothing but the driver can open a scope.

#[test]
fn an_event_reaches_the_sink_attributable_to_the_payload_that_produced_it() {
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        vec![Connection::live(vec![
            Read::Stamped(b"one", RECV_A),
            Read::Ended(DisconnectReason::RemoteClose),
        ])],
    );

    // The scope opened with the payload's own stamp and closed after it, and
    // the one event in between carries both halves of the venue-to-receive
    // interval.
    assert_eq!(outcome.events.scopes, vec![Some(RECV_A), None]);
    assert_eq!(outcome.events.attributed, vec![(Some(RECV_A), VENUE_TS_NS)]);

    // The observation itself, computed the way the runtime will compute it.
    let (recv, source) = outcome.events.attributed[0];
    assert_eq!(
        recv.expect("an event inside a payload scope") - source,
        250_000,
        "the venue-to-receive interval is not derivable from what the sink was given"
    );
}

#[test]
fn several_events_from_one_payload_are_every_one_attributed_to_it() {
    // A payload carrying a batch is one receive stamp and several events. The
    // scope is opened once, not once per event, so an adapter that emits ten
    // events from one payload does not pay ten times for the attribution and
    // cannot attribute the tenth to anything else.
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        vec![Connection::live(vec![
            Read::Stamped(BATCH, RECV_A),
            Read::Ended(DisconnectReason::RemoteClose),
        ])],
    );

    assert_eq!(outcome.events.events, 3);
    assert_eq!(
        outcome.events.attributed,
        vec![
            (Some(RECV_A), VENUE_TS_NS),
            (Some(RECV_A), VENUE_TS_NS + 1),
            (Some(RECV_A), VENUE_TS_NS + 2),
        ]
    );
    assert_eq!(outcome.events.scopes, vec![Some(RECV_A), None]);
}

#[test]
fn a_payload_that_produces_nothing_attributes_nothing() {
    // Emitting nothing is ordinary — a heartbeat, an acknowledgement — and the
    // scope still opens and closes around it. What must not happen is an
    // observation: no event means no latency to record, and a family that moved
    // on a keepalive would be measuring our own idle time.
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        vec![Connection::live(vec![
            Read::Stamped(SILENT, RECV_A),
            Read::Ended(DisconnectReason::RemoteClose),
        ])],
    );

    assert_eq!(outcome.events.events, 0);
    assert!(outcome.events.attributed.is_empty());
    assert_eq!(outcome.events.scopes, vec![Some(RECV_A), None]);
}

#[test]
fn two_payloads_in_a_row_do_not_cross_attribute() {
    // The failure this shape exists to make impossible: a stamp left in force
    // after its payload is over, so that the next event is measured against an
    // arrival that is not its own. Every scope is closed before the next opens.
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        vec![Connection::live(vec![
            Read::Stamped(b"one", RECV_A),
            Read::Stamped(b"two", RECV_B),
            Read::Ended(DisconnectReason::RemoteClose),
        ])],
    );

    assert_eq!(
        outcome.events.attributed,
        vec![(Some(RECV_A), VENUE_TS_NS), (Some(RECV_B), VENUE_TS_NS)]
    );
    assert_eq!(
        outcome.events.scopes,
        vec![Some(RECV_A), None, Some(RECV_B), None]
    );
}

#[test]
fn a_payload_the_adapter_could_not_read_still_closes_its_scope() {
    // The scope is closed from `Drop`, so there is no way out of the mapping
    // that leaves a stamp in force — including the path where the adapter
    // returned a parse error, which is the one an adapter reaches most often.
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        vec![Connection::live(vec![
            Read::Stamped(UNREADABLE, RECV_A),
            Read::Ended(DisconnectReason::RemoteClose),
        ])],
    );

    assert_eq!(outcome.observer.recorded().parse_errors, vec!["malformed"]);
    assert_eq!(outcome.events.scopes, vec![Some(RECV_A), None]);
    assert!(outcome.events.attributed.is_empty());
}

#[test]
fn an_adapter_cannot_state_a_receive_stamp_of_its_own() {
    // Why an adapter cannot get this wrong, stated as a test rather than as a
    // docstring. The sink an adapter holds is the driver's wrapper, and the
    // wrapper does not forward the scope report: the forged stamp reaches it
    // and stops. What the runtime's sink sees is the transport's reading and
    // nothing else.
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        vec![Connection::live(vec![
            Read::Stamped(FORGED, RECV_A),
            Read::Ended(DisconnectReason::RemoteClose),
        ])],
    );

    assert_eq!(outcome.events.attributed, vec![(Some(RECV_A), VENUE_TS_NS)]);
    assert_eq!(outcome.events.scopes, vec![Some(RECV_A), None]);
    assert!(
        !outcome.events.scopes.contains(&Some(FORGED_STAMP)),
        "an adapter's own stamp reached the runtime's sink"
    );
}

#[test]
fn the_driver_stamps_a_payload_the_transport_had_no_timestamp_for() {
    // The other half of where a stamp comes from: a transport with no kernel
    // timestamp gets the driver's own wall-clock reading, and the attribution
    // is the same reading the adapter was handed. Two sources, one value —
    // which is what stops the two families disagreeing with each other.
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        vec![Connection::live(vec![
            Read::Payload(b"one"),
            Read::Ended(DisconnectReason::RemoteClose),
        ])],
    );

    assert_eq!(outcome.events.scopes, vec![Some(WALL_NS), None]);
    assert_eq!(
        outcome.adapter.payloads,
        vec![(b"one".to_vec(), WALL_NS)],
        "the adapter and the sink were told about the same arrival"
    );
}

#[test]
fn a_book_the_adapter_stopped_trusting_reaches_the_runtime_through_the_wrapper() {
    // The sink the adapter writes into is the driver's, so every report on it
    // has to be forwarded or it is lost. This one is the one thing no other
    // layer can know, and the cost of losing it is a subscriber applying deltas
    // to a book the publisher has already given up on.
    let outcome = run(
        policy(),
        RecordingAdapter::default(),
        vec![Connection::live(vec![
            Read::Stamped(DIVERGED, RECV_A),
            Read::Ended(DisconnectReason::RemoteClose),
        ])],
    );

    assert_eq!(
        outcome.events.desynchronised,
        vec![(InstrumentRef::from_admission(0), Desync::UpstreamGap)]
    );
    // Reported from inside the payload's own scope, like every other report the
    // adapter makes about it.
    assert_eq!(outcome.events.scopes, vec![Some(RECV_A), None]);
}
