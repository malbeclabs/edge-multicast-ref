//! The session state machine, over a scripted byte stream and a clock a test
//! reads back.
//!
//! # No socket, no privilege, and no sleeping
//!
//! [`ByteStream`] is the seam: every case below runs against a script of bytes
//! and a clock the script advances, so a heartbeat cadence of sixty seconds
//! costs the suite nothing and is asserted by a value rather than by a wait.
//! The real socket and TLS are one implementation of the same trait and are
//! exercised separately.
//!
//! # What each test proves, and the two that matter most
//!
//! `nothing_but_a_logon_is_sent_before_the_session_is_established` is the
//! centre. It asserts the **order of what was written**, and not that the
//! session came up, because a state machine that skips that transition still
//! connects and still receives — so a test that only checked for an established
//! session would pass with the rule gone.
//!
//! `the_cadence_is_the_one_the_logon_stated_and_no_other` is the second. A
//! transport with a fixed heartbeat timer, ignoring what the session agreed,
//! passes every test that logs on and receives; it fails against a venue,
//! hours later, by having the session dropped for a reason our own logs do not
//! carry.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dz_adapter_core::ConnectionId;
use dz_ingress_core::{BoxFuture, Clock};
use dz_ingress_fix::framing::{self, msg_type, Body, FramingError, Message, SOH};
use dz_ingress_fix::session::{
    ByteStream, Incoming, Session, SessionError, SessionState, StreamError, LOGON_GRACE,
    LOGOUT_GRACE,
};

const CONNECTION: ConnectionId = ConnectionId::new("mktdata");

// ---------------------------------------------------------------------------
// A clock a test reads back, and a byte stream a test writes
// ---------------------------------------------------------------------------

/// Time as a value.
///
/// The wall reading moves with the steady one so that a `SendingTime` in a
/// framed message is a plausible stamp, and both start at a fixed origin so
/// that the golden bytes below do not change from run to run.
#[derive(Debug)]
struct ManualClock {
    steady_ns: AtomicU64,
    wall_ns: AtomicU64,
}

/// 2026-09-09T11:56:50.123Z, as nanoseconds since 1970. See
/// `timestamp::tests` for the day count.
const ORIGIN_NS: u64 =
    ((20_705 * 86_400) + 11 * 3_600 + 56 * 60 + 50) * 1_000_000_000 + 123_000_000;

impl ManualClock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            steady_ns: AtomicU64::new(1_000_000_000),
            wall_ns: AtomicU64::new(ORIGIN_NS),
        })
    }

    fn advance(&self, by: Duration) {
        let by = u64::try_from(by.as_nanos()).expect("a duration a test states");
        self.steady_ns.fetch_add(by, Ordering::SeqCst);
        self.wall_ns.fetch_add(by, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn wall_ns(&self) -> u64 {
        self.wall_ns.load(Ordering::SeqCst)
    }

    fn steady_ns(&self) -> u64 {
        self.steady_ns.load(Ordering::SeqCst)
    }

    fn sleep(&self, duration: Duration) -> BoxFuture<'_, ()> {
        // Nothing in the session sleeps — every wait is a budget handed to a
        // read — so this advancing rather than waiting is what a test would
        // want if anything did.
        self.advance(duration);
        Box::pin(async {})
    }
}

/// One thing the scripted stream does on a read.
#[derive(Debug, Clone)]
enum Serve {
    /// Hand these bytes over.
    Bytes(Vec<u8>),
    /// Consume the whole budget and return nothing, which is what a venue with
    /// nothing to say looks like.
    Silence,
    /// End the stream.
    Closed,
}

/// Every write the session made, rendered with `|` for the separator.
type Writes = Arc<Mutex<Vec<String>>>;

/// A byte stream that serves a script and records what was written to it.
///
/// A read that finds the script exhausted is [`Serve::Silence`], so a test
/// states only the traffic it cares about and the venue is silent afterwards.
struct Script {
    clock: Arc<ManualClock>,
    serves: Arc<Mutex<std::collections::VecDeque<Serve>>>,
    writes: Writes,
}

impl Script {
    /// A stream serving `serves`, and the record of what gets written to it.
    ///
    /// Not `new`: what a test wants back is the stream *and* the recorder, and
    /// handing them over together is what stops a test holding a recorder that
    /// belongs to a different stream.
    fn serving(clock: &Arc<ManualClock>, serves: Vec<Serve>) -> (Box<dyn ByteStream>, Writes) {
        let writes: Writes = Arc::new(Mutex::new(Vec::new()));
        let stream = Self {
            clock: Arc::clone(clock),
            serves: Arc::new(Mutex::new(serves.into())),
            writes: Arc::clone(&writes),
        };
        (Box::new(stream), writes)
    }
}

/// How long a read that produced bytes takes.
///
/// Not zero: a session whose reads take no time at all would have every timer
/// fire at the same instant, which is the one arrangement a real one never
/// sees.
const READ_COST: Duration = Duration::from_millis(1);

impl ByteStream for Script {
    fn write<'a>(&'a mut self, bytes: &'a [u8]) -> BoxFuture<'a, Result<(), StreamError>> {
        self.writes
            .lock()
            .expect("the recorder")
            .push(framing::rendered(bytes));
        Box::pin(async { Ok(()) })
    }

    fn read<'a>(
        &'a mut self,
        out: &'a mut Vec<u8>,
        budget: Duration,
    ) -> BoxFuture<'a, Result<usize, StreamError>> {
        let next = self
            .serves
            .lock()
            .expect("the script")
            .pop_front()
            .unwrap_or(Serve::Silence);
        Box::pin(async move {
            match next {
                Serve::Bytes(bytes) => {
                    self.clock.advance(READ_COST.min(budget));
                    out.extend_from_slice(&bytes);
                    Ok(bytes.len())
                }
                Serve::Silence => {
                    self.clock.advance(budget);
                    Ok(0)
                }
                Serve::Closed => Err(StreamError::Closed {
                    detail: "the script ended the stream".to_owned(),
                }),
            }
        })
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// A stream that carries the logon and then never completes another write.
///
/// What a venue that has stopped *reading* looks like from this side: the
/// session establishes, the send window fills, and the next `write_all` sits
/// inside the kernel's retransmit timeout for minutes. `Script`'s write is
/// infallible and instantaneous, so no test built on it can see that — which is
/// the whole reason this one exists.
struct StallsAfterTheLogon {
    writes: usize,
    answer: Option<Vec<u8>>,
}

impl ByteStream for StallsAfterTheLogon {
    fn write<'a>(&'a mut self, _bytes: &'a [u8]) -> BoxFuture<'a, Result<(), StreamError>> {
        self.writes += 1;
        let stalled = self.writes > 1;
        Box::pin(async move {
            if stalled {
                // Never ready, and never an error either: a blocked write is
                // not a failure a caller gets told about, which is exactly
                // why an unbounded one is a hang rather than a disconnect.
                std::future::pending::<()>().await;
            }
            Ok(())
        })
    }

    fn read<'a>(
        &'a mut self,
        out: &'a mut Vec<u8>,
        _budget: Duration,
    ) -> BoxFuture<'a, Result<usize, StreamError>> {
        Box::pin(async move {
            match self.answer.take() {
                Some(bytes) => {
                    out.extend_from_slice(&bytes);
                    Ok(bytes.len())
                }
                None => std::future::pending().await,
            }
        })
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

// ---------------------------------------------------------------------------
// Messages, written the way a person reads one
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

/// The venue accepting a logon at the cadence it was asked for.
fn logon_accepted(seconds: u64) -> Serve {
    Serve::Bytes(from_venue(&format!("35=A|98=0|108={seconds}|"), 1))
}

/// A logon body the adapter composed: an identity, a signature, a cadence.
fn adapter_logon(seconds: u64) -> Vec<u8> {
    wire(&format!(
        "35=A|49=A-PUBLISHER|56=A-VENUE|98=0|108={seconds}|553=an-account|554=not-a-real-signature|"
    ))
}

/// A subscription body the adapter composed.
fn adapter_subscription(request: &str) -> Vec<u8> {
    wire(&format!(
        "35=V|262={request}|263=1|264=1|267=2|269=0|269=1|146=1|55=A-SYMBOL|"
    ))
}

/// A session with a script, ready to be opened.
fn session(clock: &Arc<ManualClock>, serves: Vec<Serve>) -> (Session, Writes) {
    let (stream, writes) = Script::serving(clock, serves);
    let mut session = Session::new(CONNECTION, Arc::clone(clock) as Arc<dyn Clock>);
    session.open(stream);
    (session, writes)
}

/// The recorded writes, as message types in order.
fn written_types(writes: &Writes) -> Vec<String> {
    writes
        .lock()
        .expect("the recorder")
        .iter()
        .map(|message| {
            message
                .split('|')
                .find_map(|field| field.strip_prefix("35="))
                .expect("every write states a message type")
                .to_owned()
        })
        .collect()
}

/// The recorded writes, as `(message type, sequence)` in order.
fn written_sequences(writes: &Writes) -> Vec<(String, u64)> {
    writes
        .lock()
        .expect("the recorder")
        .iter()
        .map(|message| {
            let field = |prefix: &str| {
                message
                    .split('|')
                    .find_map(|field| field.strip_prefix(prefix))
                    .unwrap_or_else(|| panic!("`{message}` states no `{prefix}`"))
                    .to_owned()
            };
            (field("35="), field("34=").parse().expect("a sequence"))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The plan's centre: the order of what was written
// ---------------------------------------------------------------------------

#[tokio::test]
async fn nothing_but_a_logon_is_sent_before_the_session_is_established() {
    // The revert this test exists for: allow a subscription to be sent before
    // the session is established. A state machine that skips the transition
    // still connects and still receives, so what is asserted here is the order
    // of what was *written* — and that the refused subscription reached the
    // stream not at all.
    let clock = ManualClock::new();
    let (mut session, writes) = session(&clock, vec![logon_accepted(30)]);

    let error = session
        .send(&adapter_subscription("before-the-logon"))
        .await
        .expect_err("a subscription cannot precede a logon");
    match &error {
        SessionError::SentBeforeEstablished { msg_type } => assert_eq!(msg_type, "V"),
        other => panic!("{other}"),
    }
    assert!(
        writes.lock().expect("the recorder").is_empty(),
        "the refused subscription reached the stream: {:?}",
        writes.lock().expect("the recorder")
    );
    assert_eq!(session.state(), SessionState::Connected);

    // And in the order the adapter is expected to write them.
    session
        .send(&adapter_logon(30))
        .await
        .expect("a logon the venue accepts");
    assert_eq!(session.state(), SessionState::Established);
    session
        .send(&adapter_subscription("after-the-logon"))
        .await
        .expect("a subscription on an established session");

    assert_eq!(
        written_sequences(&writes),
        vec![(msg_type::LOGON.to_owned(), 1), ("V".to_owned(), 2),],
        "the logon is first, on sequence 1, and the subscription follows it"
    );
}

#[tokio::test]
async fn a_receive_on_a_session_with_no_logon_names_the_adapters_method() {
    // Not a session that waits: a transport that logged on with a body it
    // composed itself would be signing for the venue.
    let clock = ManualClock::new();
    let (mut session, writes) = session(&clock, vec![]);
    let error = session
        .receive(Some(Duration::from_secs(1)))
        .await
        .expect_err("no logon was written");
    assert!(matches!(error, SessionError::NoLogon), "{error}");
    assert!(
        error.to_string().contains("on_connected"),
        "the refusal must name the adapter's method: {error}"
    );
    assert!(writes.lock().expect("the recorder").is_empty());
}

#[tokio::test]
async fn a_receive_after_a_refused_logon_does_not_blame_the_adapter() {
    // The adapter did its job here: it queued a logon, this transport wrote it,
    // and the venue refused it. So the refusal must not be the one that names
    // `Adapter::on_connected`, which would send an operator to read the one
    // piece of code that behaved.
    let clock = ManualClock::new();
    let (mut session, _writes) = session(
        &clock,
        vec![Serve::Bytes(from_venue("35=5|58=invalid credentials|", 1))],
    );
    session
        .send(&adapter_logon(30))
        .await
        .expect_err("the venue refused the logon");
    assert_eq!(session.state(), SessionState::LogonSent);

    let error = session
        .receive(Some(Duration::from_secs(1)))
        .await
        .expect_err("this session never came up");
    assert!(
        matches!(error, SessionError::LogonNotEstablished),
        "{error}"
    );
    let rendered = error.to_string();
    assert!(
        !rendered.contains("on_connected"),
        "the adapter queued a logon and must not be the thing named: {rendered}"
    );
    assert!(
        rendered.contains("was written"),
        "the refusal must say the logon went out: {rendered}"
    );
}

// ---------------------------------------------------------------------------
// The cadence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_cadence_is_the_one_the_logon_stated_and_no_other() {
    // The revert: run the cadence from a fixed interval, or from a
    // configuration key, instead of from the logon that was sent. Three
    // cadences, each checked one second short of itself and one second past
    // it, so no single fixed value passes.
    for stated in [10u64, 30, 60] {
        let clock = ManualClock::new();
        let (mut session, writes) = session(&clock, vec![logon_accepted(stated)]);
        session
            .send(&adapter_logon(stated))
            .await
            .expect("a logon the venue accepts");
        assert_eq!(
            session.heartbeat_interval(),
            Some(Duration::from_secs(stated)),
            "the cadence is read out of the logon body"
        );

        let short = session
            .receive(Some(Duration::from_secs(stated - 1)))
            .await
            .expect("a quiet session");
        assert_eq!(short, Incoming::Idle);
        assert_eq!(
            written_types(&writes),
            vec![msg_type::LOGON.to_owned()],
            "a heartbeat went out before the cadence of {stated}s was up"
        );

        let past = session
            .receive(Some(Duration::from_secs(2)))
            .await
            .expect("a quiet session");
        assert_eq!(past, Incoming::Idle);
        assert_eq!(
            written_types(&writes),
            vec![msg_type::LOGON.to_owned(), msg_type::HEARTBEAT.to_owned()],
            "no heartbeat went out after the cadence of {stated}s was up"
        );
    }
}

#[tokio::test]
async fn a_test_request_is_answered_with_the_identifier_it_carried() {
    let clock = ManualClock::new();
    let (mut session, writes) = session(
        &clock,
        vec![
            logon_accepted(30),
            Serve::Bytes(from_venue("35=1|112=are-you-there|", 2)),
        ],
    );
    session.send(&adapter_logon(30)).await.expect("a logon");

    let received = session
        .receive(Some(Duration::from_secs(1)))
        .await
        .expect("the venue's test request");
    assert_eq!(
        received,
        Incoming::Liveness,
        "a test request is not a payload"
    );

    let recorded = writes.lock().expect("the recorder").clone();
    let answer = recorded.last().expect("an answer was written");
    assert!(answer.contains("35=0"), "{answer}");
    assert!(
        answer.contains("112=are-you-there"),
        "the identifier has to come back or the venue does not count it: {answer}"
    );
}

#[tokio::test]
async fn silence_is_questioned_once_and_then_ends_the_session() {
    // The failure a read timeout alone cannot see: the socket is open, nothing
    // is wrong with it, and the session behind it is gone.
    let clock = ManualClock::new();
    let (mut session, writes) = session(&clock, vec![logon_accepted(10)]);
    session.send(&adapter_logon(10)).await.expect("a logon");

    let error = session
        .receive(None)
        .await
        .expect_err("a session that says nothing at all");
    match error {
        SessionError::Silent { interval, silence } => {
            assert_eq!(interval, Duration::from_secs(10));
            // The number the message states, and it is not two bare cadences:
            // silence is questioned at a cadence plus the protocol's grace on
            // it and the session is dead at two of those, so ten seconds is
            // dead at twenty-four and not at twenty.
            assert_eq!(silence, Duration::from_secs(24));
        }
        other => panic!("{other}"),
    }
    let written = written_types(&writes);
    assert!(
        written.contains(&msg_type::TEST_REQUEST.to_owned()),
        "the silence was never questioned: {written:?}"
    );
    assert!(
        written.contains(&msg_type::HEARTBEAT.to_owned()),
        "the cadence stopped when the venue went quiet: {written:?}"
    );
}

// ---------------------------------------------------------------------------
// The logon's two answers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_logon_the_venue_answers_with_a_logout_is_a_rejected_logon() {
    let clock = ManualClock::new();
    let (mut session, _writes) = session(
        &clock,
        vec![Serve::Bytes(from_venue("35=5|58=invalid credentials|", 1))],
    );
    let error = session
        .send(&adapter_logon(30))
        .await
        .expect_err("the venue refused the logon");
    match &error {
        SessionError::LogonRejected { detail } => {
            assert!(detail.contains("35=5"), "{detail}");
            assert!(detail.contains("invalid credentials"), "{detail}");
        }
        other => panic!("{other}"),
    }
    assert_ne!(
        session.state(),
        SessionState::Established,
        "a refused logon must not leave an established session"
    );
}

#[tokio::test]
async fn a_logon_the_venue_never_answers_gives_up_after_the_grace() {
    let clock = ManualClock::new();
    let (mut session, writes) = session(&clock, vec![]);
    let error = session
        .send(&adapter_logon(30))
        .await
        .expect_err("nothing came back");
    match error {
        SessionError::LogonNotAnswered { grace } => assert_eq!(grace, LOGON_GRACE),
        other => panic!("{other}"),
    }
    assert_eq!(
        written_types(&writes),
        vec![msg_type::LOGON.to_owned()],
        "the logon was written and nothing else was"
    );
}

#[tokio::test]
async fn a_logon_body_with_no_cadence_never_reaches_the_stream() {
    // The cadence is read out of the logon, so a logon without one is a body
    // this transport cannot run a session from — and refusing it after writing
    // it would be a session logged on with no cadence.
    let clock = ManualClock::new();
    let (mut session, writes) = session(&clock, vec![logon_accepted(30)]);
    let error = session
        .send(&wire("35=A|49=A-PUBLISHER|"))
        .await
        .expect_err("no cadence");
    assert!(
        matches!(error, SessionError::NoHeartbeatInterval),
        "{error}"
    );
    assert!(writes.lock().expect("the recorder").is_empty(), "{error}");
}

// ---------------------------------------------------------------------------
// The outbound sequence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_outbound_sequence_numbers_every_message_including_the_sessions_own() {
    let clock = ManualClock::new();
    let (mut session, writes) = session(
        &clock,
        vec![
            logon_accepted(10),
            Serve::Bytes(from_venue("35=1|112=probe|", 2)),
        ],
    );
    session.send(&adapter_logon(10)).await.expect("a logon");
    session
        .receive(Some(Duration::from_secs(1)))
        .await
        .expect("the venue's test request");
    session
        .send(&adapter_subscription("after"))
        .await
        .expect("a subscription");

    assert_eq!(
        written_sequences(&writes),
        vec![
            (msg_type::LOGON.to_owned(), 1),
            // The heartbeat answering the test request is the session's own
            // message and is numbered like every other.
            (msg_type::HEARTBEAT.to_owned(), 2),
            ("V".to_owned(), 3),
        ]
    );
    assert_eq!(session.next_sequence(), 4);
}

#[tokio::test]
async fn a_second_logon_numbers_from_one() {
    // The revert: carry the sequence across a reconnect. This transport resets
    // at every logon and states the flag that says so, because a resend
    // delivers deltas whose value has expired and the publisher's own snapshot
    // recovery is the better repair.
    let clock = ManualClock::new();
    let (mut session, first) = session(&clock, vec![logon_accepted(30)]);
    session.send(&adapter_logon(30)).await.expect("a logon");
    session
        .send(&adapter_subscription("first-session"))
        .await
        .expect("a subscription");
    assert_eq!(session.next_sequence(), 3);
    session.close().await;
    assert_eq!(session.state(), SessionState::Closed);

    let (stream, second) = Script::serving(&clock, vec![logon_accepted(30)]);
    session.open(stream);
    assert_eq!(
        session.next_sequence(),
        1,
        "the sequence did not reset at the second logon"
    );
    session
        .send(&adapter_logon(30))
        .await
        .expect("a second logon");

    assert_eq!(
        written_sequences(&second),
        vec![(msg_type::LOGON.to_owned(), 1)],
        "the second logon numbers from one"
    );
    let logon = second.lock().expect("the recorder")[0].clone();
    assert!(
        logon.contains("141=Y"),
        "a reset without the flag that says so is a session the venue numbers differently: {logon}"
    );
    // And the first session's last message was numbered 3, which is what makes
    // the reset a reset rather than a coincidence.
    let first = written_sequences(&first);
    assert_eq!(first.last().map(|(_, sequence)| *sequence), Some(3));
}

#[tokio::test]
async fn a_close_writes_a_logout_and_does_not_wait_for_its_answer() {
    let clock = ManualClock::new();
    let (mut session, writes) = session(&clock, vec![logon_accepted(30)]);
    session.send(&adapter_logon(30)).await.expect("a logon");
    session.close().await;
    assert_eq!(
        written_types(&writes),
        vec![msg_type::LOGON.to_owned(), msg_type::LOGOUT.to_owned()]
    );
    assert_eq!(session.state(), SessionState::Closed);
}

#[tokio::test(start_paused = true)]
async fn a_logout_the_venue_has_stopped_reading_is_bounded_and_not_a_hang() {
    // `close` already declines to wait for the venue's logout back. What this
    // asserts is the other half, which declining to wait does not give: the
    // write of *our own* logout is bounded, so a venue that has stopped reading
    // cannot hold the teardown path inside `write_all` for the kernel's
    // retransmit timeout with nothing reporting it.
    //
    // The clock is the runtime's, paused: tokio advances virtual time to the
    // next timer once every task is idle, so the assertion below is the budget
    // the close applied and not a wait this suite paid for.
    let clock = ManualClock::new();
    let mut session = Session::new(CONNECTION, Arc::clone(&clock) as Arc<dyn Clock>);
    session.open(Box::new(StallsAfterTheLogon {
        writes: 0,
        answer: Some(from_venue("35=A|98=0|108=30|", 1)),
    }));
    session.send(&adapter_logon(30)).await.expect("a logon");
    assert_eq!(session.state(), SessionState::Established);

    let started = tokio::time::Instant::now();
    // The outer bound is this suite's and not the transport's: with the
    // transport's grace gone, it makes the fault a failure that names itself
    // rather than a job that hangs.
    tokio::time::timeout(Duration::from_secs(30), session.close())
        .await
        .expect("`close` must return: an unbounded logout write is a hung teardown");
    assert_eq!(
        started.elapsed(),
        LOGOUT_GRACE,
        "the teardown is bounded by the grace the constant states"
    );
    assert_eq!(
        session.state(),
        SessionState::Closed,
        "a logout that could not be written still releases the session"
    );
}

#[tokio::test]
async fn a_close_on_a_session_that_never_logged_on_writes_no_logout() {
    // A logout on a session the venue never established is a message about a
    // session that does not exist.
    let clock = ManualClock::new();
    let (mut session, writes) = session(&clock, vec![]);
    session.close().await;
    assert!(writes.lock().expect("the recorder").is_empty());
}

// ---------------------------------------------------------------------------
// What arrives, and the refusals that end the session
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_application_message_is_handed_over_whole() {
    let clock = ManualClock::new();
    let expected = from_venue("35=W|55=A-SYMBOL|268=1|269=0|270=100|271=5|", 2);
    let (mut session, _writes) = session(
        &clock,
        vec![logon_accepted(30), Serve::Bytes(expected.clone())],
    );
    session.send(&adapter_logon(30)).await.expect("a logon");

    let received = session
        .receive(Some(Duration::from_secs(1)))
        .await
        .expect("a market-data message");
    match received {
        Incoming::Message(bytes) => assert_eq!(
            bytes, expected,
            "the header and the checksum go to the adapter too: a venue's own \
             message identity may be computed over them"
        ),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn a_message_split_across_two_reads_is_one_message() {
    let clock = ManualClock::new();
    let expected = from_venue("35=W|55=A-SYMBOL|", 2);
    let (head, tail) = expected.split_at(expected.len() / 2);
    let (mut session, _writes) = session(
        &clock,
        vec![
            logon_accepted(30),
            Serve::Bytes(head.to_vec()),
            Serve::Bytes(tail.to_vec()),
        ],
    );
    session.send(&adapter_logon(30)).await.expect("a logon");
    let received = session
        .receive(Some(Duration::from_secs(1)))
        .await
        .expect("the whole message");
    assert_eq!(received, Incoming::Message(&expected));
}

#[tokio::test]
async fn two_messages_in_one_read_are_two_messages() {
    let clock = ManualClock::new();
    let first = from_venue("35=W|55=FIRST|", 2);
    let second = from_venue("35=X|55=SECOND|", 3);
    let mut both = first.clone();
    both.extend_from_slice(&second);
    let (mut session, _writes) = session(&clock, vec![logon_accepted(30), Serve::Bytes(both)]);
    session.send(&adapter_logon(30)).await.expect("a logon");

    assert_eq!(
        session
            .receive(Some(Duration::from_secs(1)))
            .await
            .expect("the first"),
        Incoming::Message(&first)
    );
    assert_eq!(
        session
            .receive(Some(Duration::from_secs(1)))
            .await
            .expect("the second"),
        Incoming::Message(&second),
        "the second message in one read is a payload nobody sees when the \
         buffer is discarded"
    );
}

#[tokio::test]
async fn a_corrupt_message_ends_the_session() {
    // The revert: skip a message whose checksum does not hold instead of
    // ending the session. What that would cost is a session that keeps reading
    // against numbering it can no longer trust — the message after the corrupt
    // one is read as if nothing had happened, and the sequence it was numbered
    // on is unaccounted for.
    let clock = ManualClock::new();
    let mut corrupt = from_venue("35=W|55=A-SYMBOL|", 2);
    let digit = corrupt.len() - 2;
    corrupt[digit] = if corrupt[digit] == b'9' { b'8' } else { b'9' };
    let after = from_venue("35=W|55=AFTER|", 3);
    let (mut session, _writes) = session(
        &clock,
        vec![
            logon_accepted(30),
            Serve::Bytes(corrupt),
            Serve::Bytes(after),
        ],
    );
    session.send(&adapter_logon(30)).await.expect("a logon");

    let error = session
        .receive(Some(Duration::from_secs(1)))
        .await
        .expect_err("a checksum that does not hold");
    match error {
        SessionError::Framing(FramingError::ChecksumMismatch { stated, computed }) => {
            assert_ne!(stated, computed);
        }
        other => panic!("{other}"),
    }
}

#[tokio::test]
async fn the_venues_logout_ends_the_session_and_says_so() {
    let clock = ManualClock::new();
    let (mut session, _writes) = session(
        &clock,
        vec![
            logon_accepted(30),
            Serve::Bytes(from_venue("35=5|58=session ended by the venue|", 2)),
        ],
    );
    session.send(&adapter_logon(30)).await.expect("a logon");
    let error = session
        .receive(Some(Duration::from_secs(1)))
        .await
        .expect_err("the venue logged us out");
    match &error {
        SessionError::LoggedOut { detail } => {
            assert!(detail.contains("session ended by the venue"), "{detail}");
        }
        other => panic!("{other}"),
    }
}

#[tokio::test]
async fn a_session_level_reject_is_the_failure_only_this_layer_can_see() {
    let clock = ManualClock::new();
    let (mut session, _writes) = session(
        &clock,
        vec![
            logon_accepted(30),
            Serve::Bytes(from_venue(
                "35=3|45=2|371=108|373=5|58=value is incorrect|",
                2,
            )),
        ],
    );
    session.send(&adapter_logon(30)).await.expect("a logon");
    let error = session
        .receive(Some(Duration::from_secs(1)))
        .await
        .expect_err("a session-level reject");
    match &error {
        SessionError::Rejected { detail } => {
            assert!(detail.contains("373=5"), "{detail}");
            assert!(detail.contains("371=108"), "{detail}");
        }
        other => panic!("{other}"),
    }
}

#[tokio::test]
async fn a_resend_request_ends_the_session_rather_than_being_answered() {
    // This transport has no resend path by design. The honest answer to a
    // request it cannot serve is to reconnect — which resets the numbering and
    // re-subscribes — rather than to acknowledge a gap-fill that never comes.
    let clock = ManualClock::new();
    let (mut session, writes) = session(
        &clock,
        vec![
            logon_accepted(30),
            Serve::Bytes(from_venue("35=2|7=1|16=0|", 2)),
        ],
    );
    session.send(&adapter_logon(30)).await.expect("a logon");
    let error = session
        .receive(Some(Duration::from_secs(1)))
        .await
        .expect_err("a resend this transport does not do");
    assert!(
        matches!(error, SessionError::ResendRequested { .. }),
        "{error}"
    );
    assert_eq!(
        written_types(&writes),
        vec![msg_type::LOGON.to_owned()],
        "nothing was composed in answer to the resend request"
    );
}

#[tokio::test]
async fn a_sequence_reset_is_nothing_to_act_on() {
    // No inbound numbering is tracked, because tracking one would only be
    // worth it to ask for a resend this transport does not do.
    let clock = ManualClock::new();
    let (mut session, _writes) = session(
        &clock,
        vec![
            logon_accepted(30),
            Serve::Bytes(from_venue("35=4|36=9|123=Y|", 2)),
        ],
    );
    session.send(&adapter_logon(30)).await.expect("a logon");
    assert_eq!(
        session
            .receive(Some(Duration::from_secs(1)))
            .await
            .expect("a sequence reset"),
        Incoming::Liveness
    );
}

#[tokio::test]
async fn a_stream_that_ends_ends_the_session() {
    let clock = ManualClock::new();
    let (mut session, _writes) = session(&clock, vec![logon_accepted(30), Serve::Closed]);
    session.send(&adapter_logon(30)).await.expect("a logon");
    let error = session
        .receive(Some(Duration::from_secs(1)))
        .await
        .expect_err("the stream ended");
    assert!(
        matches!(error, SessionError::Stream(StreamError::Closed { .. })),
        "{error}"
    );
}

#[tokio::test]
async fn a_debug_line_does_not_carry_a_logon_body() {
    // A logon body is where a venue's signature is, and a `Debug` on a session
    // holding the framed copy of one is how it reaches a log line.
    let clock = ManualClock::new();
    let (mut session, _writes) = session(&clock, vec![logon_accepted(30)]);
    session.send(&adapter_logon(30)).await.expect("a logon");
    let rendered = format!("{session:?}");
    assert!(!rendered.contains("not-a-real-signature"), "{rendered}");
    assert!(!rendered.contains("an-account"), "{rendered}");
    assert!(rendered.contains("mktdata"), "{rendered}");
    assert!(rendered.contains("Established"), "{rendered}");
}

#[tokio::test]
async fn a_venues_own_sequence_reaches_the_adapter_on_the_message() {
    // Nothing here reads the venue's numbering — no gap detection, no resend —
    // but the field is on the message the adapter is handed, which is where a
    // venue's own message identity is computed from.
    let clock = ManualClock::new();
    let (mut session, _writes) = session(
        &clock,
        vec![
            logon_accepted(30),
            Serve::Bytes(from_venue("35=W|55=A-SYMBOL|", 4_096)),
        ],
    );
    session.send(&adapter_logon(30)).await.expect("a logon");
    let received = session
        .receive(Some(Duration::from_secs(1)))
        .await
        .expect("a message");
    match received {
        Incoming::Message(bytes) => {
            assert_eq!(
                Message::new(bytes).field_u64(framing::TAG_MSG_SEQ_NUM),
                Some(4_096)
            );
        }
        other => panic!("{other:?}"),
    }
}
