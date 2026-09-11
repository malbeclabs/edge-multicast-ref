//! The session: states, transitions, the cadence, and the outbound sequence.
//!
//! # The byte stream is behind a trait, and that is what makes this testable
//!
//! A session layer is a state machine over a byte stream. With the stream
//! behind [`ByteStream`] — the move `RouteLookup` makes for the routing table
//! and [`Clock`] makes for time — every case that matters becomes a test that
//! runs unprivileged with no network: a logon answered, a logon rejected, a
//! heartbeat due, a test request answered, a message whose checksum does not
//! hold, a message split across two reads, two messages in one read, a logout
//! from the venue, and a session that goes silent.
//!
//! The socket and TLS are then one implementation of one trait, exercised
//! against a loopback endpoint, rather than a precondition for testing the
//! protocol at all.
//!
//! # The cadence comes out of the logon and never from a key
//!
//! [`Session::send`] reads [`TAG_HEART_BT_INT`] out of the logon body the
//! adapter wrote and runs the cadence from that value. There is deliberately no
//! configuration key for it. A key could be set to disagree with what was
//! logged on with, and the venue believes the logon: a publisher heartbeating
//! every 30 seconds on a session it agreed 10 for is one the venue disconnects
//! for a reason our own logs would not carry. The other shape — a fixed timer
//! that ignores what the session agreed — is a deviation nobody notices until a
//! venue drops the session.
//!
//! # Nothing but the logon is sent before the session is established
//!
//! A subscription cannot precede a logon on a session that has not been
//! established, and this is the transition that enforces it: in
//! [`SessionState::Connected`] the only body [`Session::send`] accepts is a
//! logon, and anything else is [`SessionError::SentBeforeEstablished`] with
//! **nothing written to the stream**. That last clause is why the test for it
//! asserts the *order of what was written* rather than that the session came
//! up: a state machine that skips this transition still connects and still
//! receives.
//!
//! # What this layer does not do
//!
//! No resend, no gap-fill, and no inbound gap detection. A resend delivers
//! deltas whose value has expired, and the publisher's own recovery path — a
//! reset announced, the instrument paused, a snapshot republished — is the
//! better repair. So a `ResendRequest` from the venue **ends the session**
//! rather than being answered with a gap-fill this crate does not have, and a
//! `SequenceReset` is nothing to act on because no inbound numbering is being
//! tracked to reset.

use std::sync::Arc;
use std::time::Duration;

use dz_adapter_core::ConnectionId;
use dz_ingress_core::{BoxFuture, Clock};
use tokio::time::timeout;
use zeroize::Zeroize;

use crate::framing::{
    self, msg_type, Body, BodyError, Decoder, FramingError, Message, TAG_HEART_BT_INT,
    TAG_MSG_SEQ_NUM, TAG_REF_TAG_ID, TAG_SESSION_REJECT_REASON, TAG_TEST_REQ_ID, TAG_TEXT,
};
use crate::timestamp::sending_time;

/// How long the venue is given to answer a logon.
///
/// Its own constant rather than `[ingress] connect_timeout`, because that
/// budget was spent on the socket and the negotiation before a logon could be
/// written at all — the driver connects, asks the adapter what to send, and
/// sends it. What this bounds is a venue that accepts a socket and then answers
/// nothing, which without it would leave the driver inside one send for the
/// life of the process.
///
/// **It does not bound the write of the logon**, which is [`WRITE_GRACE`]'s. A
/// venue that never reads the logon and a venue that reads it and answers
/// nothing are two different holds on one connect, and one number for both
/// would be a number that is wrong for whichever half it was not chosen for.
pub const LOGON_GRACE: Duration = Duration::from_secs(10);

/// How long an orderly logout may take before the stream is simply dropped.
///
/// The usual reason to be closing is that the peer has stopped answering, so
/// the logout is written and its answer is not waited for. Waiting would put
/// this delay in front of every reconnect.
///
/// **It bounds the whole teardown, which is the write and the stream's own
/// close.** Declining to wait for the venue's logout back does not bound
/// anything the venue can hold: a venue that has stopped *reading* fills the
/// send window, and an unbounded `write_all` then sits inside the kernel's
/// retransmit timeout — minutes — with the teardown path hung and nothing
/// reporting it. One constant for both halves, because a second one would be
/// two numbers to keep in agreement about one thing. See [`Session::close`].
pub const LOGOUT_GRACE: Duration = Duration::from_millis(250);

/// How long one write on a live session may take before the session is over.
///
/// **A venue that has stopped reading is a venue that is gone, and no layer
/// above this one can see it.** The socket stays open and reports nothing, the
/// send window fills, and an unbounded `write_all` then sits inside the
/// kernel's retransmit timeout, which is minutes. A heartbeat that falls due
/// inside [`Session::receive`] enters that write, so the driver is parked in
/// `Input::recv` — which has no outer timeout of its own — the idle guard never
/// runs, no disconnect is recorded, and `connection_state` stays at 1 on a
/// session that is dead. That is the failure [`LOGOUT_GRACE`] bounds on the
/// teardown path, and the teardown is not where it is most likely to happen:
/// the hot path writes a heartbeat every cadence for the life of the session.
///
/// Not [`LOGOUT_GRACE`]'s 250ms, because the two paths pay differently for
/// being wrong. There the session is already over and cutting early costs an
/// unsent logout; here cutting early ends a session that is alive, and this
/// transport's reconnect is a whole one — the sequence resets and every
/// subscription is re-issued — so a send window that is briefly full during a
/// burst must not buy that.
///
/// Ten seconds is [`LOGON_GRACE`]'s number and it is the same judgement: how
/// long this transport lets a venue owe it one thing before treating the venue
/// as gone. A write asks less of a venue than an answer does — no round trip,
/// and no work but draining a socket — so ten seconds has room to spare in the
/// direction that matters, and it leaves one order of magnitude in this file
/// for "too long" rather than two.
///
/// Its own constant rather than a share of [`LOGON_GRACE`] for the reason
/// stated there, so the worst case on a logon is the two of them in turn. That
/// is the honest number and it is bounded, which is what the alternative was
/// not.
pub const WRITE_GRACE: Duration = Duration::from_secs(10);

/// The heartbeat interval a logon may not state.
///
/// Zero disables the protocol's own liveness, which would leave this transport
/// with no way to notice a half-open socket: nothing to send on a silent
/// connection, and no cadence against which the venue's silence means anything.
/// A venue that genuinely wants no heartbeat is one where the idle guard is the
/// only liveness there is, and that is a decision to take deliberately rather
/// than to inherit from a `0` in a logon body.
pub const MIN_HEARTBEAT: Duration = Duration::from_secs(1);

/// The largest heartbeat interval a logon may state.
///
/// A cadence is also the bound on how long a dead session goes unnoticed —
/// silence is questioned at one interval and the session is dead at two — so an
/// hour's cadence is an hour of a publisher believing in a connection that is
/// gone. Ten minutes is well past every real one.
pub const MAX_HEARTBEAT: Duration = Duration::from_secs(600);

/// Where the session is.
///
/// Five states, and the one that carries the rule is
/// [`Connected`](Self::Connected): a stream is open, no logon has been written,
/// and the only thing that may be written is one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionState {
    /// No stream. Before the first connect and after every close.
    Closed,
    /// The stream is open and no logon has been written.
    ///
    /// **Nothing but a logon may be written from here.** A subscription sent in
    /// this state is refused with nothing written.
    Connected,
    /// The logon has been written and its answer has not arrived.
    ///
    /// Held only while [`Session::send`] is awaiting the answer, which is why a
    /// caller never observes it: the send that writes the logon returns either
    /// established or failed.
    LogonSent,
    /// The venue answered the logon. Anything the adapter writes is framed and
    /// numbered on the session.
    Established,
    /// A logout has been written.
    LoggingOut,
}

/// Why the byte stream could not carry a message.
///
/// Two cases, because a transport above this one takes the same action for
/// both — end the connection and let the driver reconnect — and a taxonomy
/// finer than the actions taken is a taxonomy nobody reads.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StreamError {
    /// The far side, or something in the path, ended it.
    #[error("the stream ended: {detail}")]
    Closed { detail: String },
    /// A read or a write failed.
    #[error("the stream failed: {detail}")]
    Failed { detail: String },
}

/// The byte stream a session runs over.
///
/// Owned by this crate rather than taken from a runtime, which is the whole
/// point: a scripted implementation makes every state and every transition a
/// test with no socket and no privileges, and the real socket becomes one
/// implementation of one trait.
///
/// `Send` because [`Input`](dz_ingress_core::Input) is, and a session is held
/// by a driver a binary may run on a multi-threaded runtime.
pub trait ByteStream: Send {
    /// Write every byte, or fail.
    ///
    /// A partial write is not a case a caller can act on: the message is
    /// numbered on the session, so half of one on the wire means the session's
    /// numbering and the venue's no longer agree. An implementation must write
    /// all of it or report a failure.
    fn write<'a>(&'a mut self, bytes: &'a [u8]) -> BoxFuture<'a, Result<(), StreamError>>;

    /// Append whatever is available to `out`, for at most `budget`.
    ///
    /// `Ok(0)` is the budget elapsing with nothing to read, which is not an
    /// error. The stream ending is [`StreamError::Closed`].
    ///
    /// The budget is handed in rather than the caller racing the read against a
    /// timer it holds, for the reason [`Input::recv`](dz_ingress_core::Input)
    /// gives: a read abandoned mid-message by dropping its future leaves a
    /// partial message somewhere, and the symptom is one corrupt payload after
    /// a busy period.
    fn read<'a>(
        &'a mut self,
        out: &'a mut Vec<u8>,
        budget: Duration,
    ) -> BoxFuture<'a, Result<usize, StreamError>>;

    /// Release the stream. Infallible: this is called on a path that has
    /// already decided the session is over.
    fn close(&mut self) -> BoxFuture<'_, ()>;
}

/// What one call to [`Session::receive`] produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Incoming<'a> {
    /// An application message, whole, header and checksum included.
    ///
    /// Handed over entire rather than stripped to a body: a venue's own message
    /// identity may be computed over the sequence and the sending time, and an
    /// adapter that was handed only the fields this crate does not understand
    /// could not compute one.
    Message(&'a [u8]),
    /// A session message. Nothing for the adapter, and the session is alive.
    ///
    /// **Not a payload, and the difference is the point.** A session that
    /// heartbeats forever and delivers nothing must still trip the driver's
    /// idle guard, which counts time since the last *payload*.
    Liveness,
    /// The budget elapsed with nothing received.
    Idle,
}

/// Why a session could not carry on.
///
/// The session layer's own taxonomy, mapped onto an
/// [`IngressError`](dz_ingress_core::IngressError) by the transport above it.
/// Separate because the two answer different questions: this says what the
/// protocol did, and that says what the driver should do about it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SessionError {
    /// A receive on a session at which the adapter wrote no logon.
    ///
    /// **A refusal naming the adapter's method, and not a session that waits.**
    /// A transport that logged on with a body it composed itself would be
    /// signing for the venue, so the alternative to this error is not a working
    /// session — it is this repository putting its own name on a venue's logon.
    #[error(
        "no logon was written on this connection: `Adapter::on_connected` queued nothing, and \
         this transport composes no logon of its own — the identity and the signature are the \
         venue's and belong in the adapter"
    )]
    NoLogon,

    /// A receive on a session whose logon was written and not established.
    ///
    /// **Its own case, and not [`NoLogon`](Self::NoLogon).** In this state the
    /// adapter did exactly its job: it queued a logon, this transport wrote it,
    /// and the `send` that wrote it reported why the session did not come up.
    /// Naming `Adapter::on_connected` here would send an operator to read the
    /// one piece of code that behaved.
    ///
    /// A driver cannot reach it — the send it ignored is the send that returned
    /// the real reason — so, like [`NotConnected`](Self::NotConnected), it is
    /// the case where something else drove the session.
    #[error(
        "the logon was written and this session was not established: the `send` that wrote it \
         is what reported why, and there is nothing to receive on a session that never came up"
    )]
    LogonNotEstablished,

    /// Something other than a logon was written before the session was
    /// established.
    ///
    /// **Nothing was written to the stream.** A subscription cannot precede a
    /// logon on a session that has not been established, and the refusal
    /// happens before any byte reaches the socket rather than after it.
    #[error(
        "`{msg_type}` was written before the session was established: the first message on a \
         session is the logon, and nothing else may precede it"
    )]
    SentBeforeEstablished { msg_type: String },

    /// A logon body that does not state the cadence the session will run at.
    #[error(
        "the logon body states no `{TAG_HEART_BT_INT}=`: the heartbeat cadence is read out of \
         the logon and never from a configuration key, so a logon without one leaves the \
         session with no cadence to run"
    )]
    NoHeartbeatInterval,

    /// A heartbeat interval this transport will not run a session at.
    #[error(
        "the logon states `{TAG_HEART_BT_INT}={stated}`, which is not a cadence between \
         {min:?} and {max:?}"
    )]
    UnusableHeartbeatInterval {
        stated: String,
        min: Duration,
        max: Duration,
    },

    /// The body could not be framed.
    #[error("the body could not be framed: {0}")]
    Body(#[from] BodyError),

    /// The stream could not be read as messages.
    #[error("the stream could not be read: {0}")]
    Framing(#[from] FramingError),

    /// The venue answered the logon with something that was not one.
    #[error("the venue refused the logon: {detail}")]
    LogonRejected { detail: String },

    /// The venue answered nothing at all.
    #[error("the venue answered no logon within {grace:?}")]
    LogonNotAnswered { grace: Duration },

    /// A session-level reject.
    ///
    /// The failure only this layer can see: an application message is the
    /// adapter's to reject and a socket error is the stream's, and this is
    /// neither.
    #[error("the venue rejected a session message: {detail}")]
    Rejected { detail: String },

    /// The venue logged us out.
    #[error("the venue logged us out: {detail}")]
    LoggedOut { detail: String },

    /// The venue asked for messages to be resent.
    ///
    /// Ends the session rather than being answered. This transport has no
    /// resend path by design, and the honest answer to a request it cannot
    /// serve is to reconnect — which resets the numbering and re-subscribes —
    /// rather than to acknowledge a gap-fill that will not arrive.
    #[error(
        "the venue asked for a resend, which this transport does not do: {detail}. \
         Reconnecting resets the sequence and re-subscribes, which is this feed's repair"
    )]
    ResendRequested { detail: String },

    /// Nothing arrived for two graced cadences, with a test request unanswered
    /// in between.
    ///
    /// Both numbers, because `silence` is the one that reaches a log line and
    /// two cadences is not what it is: silence is questioned at one cadence
    /// *plus the protocol's grace on it* — a fifth — and the session is dead at
    /// two of those, so a thirty-second cadence is dead at seventy-two seconds
    /// and not at sixty.
    #[error(
        "nothing arrived for {silence:?} — two cadences of {interval:?} with the protocol's \
         grace on each — and a test request went unanswered: the session is gone whatever the \
         socket says"
    )]
    Silent {
        interval: Duration,
        silence: Duration,
    },

    /// The byte stream failed.
    #[error("{0}")]
    Stream(#[from] StreamError),

    /// A send or a receive on a session with no stream.
    ///
    /// A driver cannot reach this — it connects before it sends — so it is the
    /// case where something else drove the session.
    #[error("there is no stream: this session is not connected")]
    NotConnected,
}

/// The message types the session layer composes, as a closed set.
///
/// **The session's own `compose` takes one of these and not a `&str`**, which is
/// what makes *this transport composes no order entry* a property of the code
/// rather than a statement about a list. A `compose("D", …)` on some future
/// path does not compile; a list of tokens checked against itself would have
/// agreed with it.
///
/// Three, and the logon is not among them: a logon's body is the adapter's,
/// composed in venue code and only framed here. See the crate documentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Composed {
    /// The cadence's own message, and the answer to the venue's test request.
    Heartbeat,
    /// The question a suspicion of silence asks.
    TestRequest,
    /// The orderly close.
    Logout,
}

impl Composed {
    /// The three of them, for the assertion that none is order entry.
    pub const ALL: [Self; 3] = [Self::Heartbeat, Self::TestRequest, Self::Logout];

    /// The `MsgType` this composes, which is one of [`msg_type::SESSION`].
    #[must_use]
    pub const fn as_msg_type(self) -> &'static str {
        match self {
            Self::Heartbeat => msg_type::HEARTBEAT,
            Self::TestRequest => msg_type::TEST_REQUEST,
            Self::Logout => msg_type::LOGOUT,
        }
    }
}

/// What the message currently held is, as a value that borrows nothing.
///
/// The classification is separated from the handling so that a message can be
/// read while it is borrowed and acted on once it is not — which is what lets
/// an answered test request write on the same stream the message came from.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Classified {
    /// For the adapter.
    Payload,
    /// A heartbeat, or a sequence reset there is no numbering to reset.
    Liveness,
    /// A test request, whose `TestReqID` has to come back.
    TestRequest { id: String },
}

/// What a message arriving before the logon has been answered is.
///
/// The same separation [`Classified`] is for, and for a second reason here:
/// the four answers are a closed set, so a message type that is none of them
/// cannot quietly fall into the refusal that names a credential. See
/// [`Session::await_logon`], which argues which type is which.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Answer {
    /// The venue accepted it.
    Established,
    /// Neither the answer nor a refusal: keep waiting, within the same grace.
    KeepWaiting,
    /// The venue refused the credential, however it spelled it.
    Refused(String),
    /// Something that is not an answer to a logon at all.
    Unrelated(String),
}

/// One session over one byte stream.
///
/// One instance is one session, reused across reconnects: [`open`](Self::open)
/// resets every per-connection value, because a sequence, a cadence and a
/// half-read message all belong to the connection that produced them.
pub struct Session {
    connection: ConnectionId,
    clock: Arc<dyn Clock>,
    stream: Option<Box<dyn ByteStream>>,
    state: SessionState,
    /// The sequence the next message goes out on. **1 after every
    /// [`open`](Self::open)**, which is the decision this transport takes and
    /// states on the logon.
    next_sequence: u64,
    /// Read out of the logon the adapter wrote. `None` until one is.
    heartbeat: Option<Duration>,
    decoder: Decoder,
    /// The message the last [`receive`](Self::receive) handed out, kept alive
    /// because the payload borrows it.
    held: Vec<u8>,
    /// What the last read appended, before it reached the decoder.
    scratch: Vec<u8>,
    /// The framed bytes of the message being written.
    ///
    /// **This is where a venue's credential sits.** The logon body is the
    /// adapter's and the framing is this crate's, so from the moment
    /// [`send`](Self::send) frames it until the next `frame` overwrites the
    /// buffer, the signature and whatever password the venue's scheme carries
    /// are in this allocation. On an idle session the next `frame` is the next
    /// heartbeat; across a close and a reconnect it is whatever the next
    /// connection writes, which is minutes later and a different session.
    ///
    /// Nothing reads it — the `Debug` below prints no bytes and [`detail`]
    /// renders a whitelist of tags — so this is hygiene and not the closing of
    /// a disclosure path. It is done because the standard this crate holds
    /// itself to elsewhere is exactly that: four hand-written redacting
    /// `Debug` impls and a `detail` that cannot grow a credential by accident.
    ///
    /// [`open`](Self::open) and [`close`](Self::close) therefore
    /// [`zeroize`](Zeroize::zeroize) this buffer, which is the one thing
    /// `Vec::clear` is not. `clear` moves the length to `0` and leaves every
    /// byte where it was, so a core dump taken an hour later still has the
    /// logon in it. What zeroizing guarantees is worth stating exactly,
    /// because a `clear` documented as erasure would be worse than neither:
    /// **the allocation this field currently holds carries no credential byte
    /// once a session has been opened or closed**, which bounds the credential
    /// to the session that used it.
    ///
    /// What it does not guarantee is that no copy of those bytes exists. A
    /// `frame` that grows this buffer past its capacity copies it and frees
    /// the old allocation unzeroed, and the same bytes also pass through the
    /// adapter's own queue, rustls's write buffer and the kernel's socket
    /// buffer — none of which this crate owns. Nor is the erasure something a
    /// test in this crate can see: the spare capacity of a `Vec` is readable
    /// only through `unsafe`, which the crate forbids, so
    /// `tests::the_write_buffers_do_not_outlive_the_session_that_filled_them`
    /// pins the length and says so.
    outbound: Vec<u8>,
    /// A body this session composed, so that a session message goes through the
    /// same [`Body::parse`] every adapter body does.
    ///
    /// **Never a credential**, which is why [`open`](Self::open) and
    /// [`close`](Self::close) `clear` this one where they zeroize `outbound`:
    /// what it can hold is a [`Composed`] — a heartbeat, a test request, a
    /// logout — and the logon is deliberately not in that set. It is reset
    /// because it is per-connection scratch and `open` states that it resets
    /// those, not because of what it carries; zeroizing it would read as a
    /// claim that it holds something.
    composed: Vec<u8>,
    last_write_ns: u64,
    last_read_ns: u64,
    /// Set when a test request has gone out and nothing has come back since.
    test_request_outstanding: bool,
}

/// Prints the connection, the state and the sequence — and no message bytes.
///
/// A logon body carries a venue's signature, and the framed copy of it is in
/// `outbound` until the next message overwrites it or the session ends. A
/// derived implementation would put that in a log line the first time somebody
/// logged this struct, which is the standard `dz-ingress-websocket` holds its
/// own endpoint to.
impl core::fmt::Debug for Session {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Session")
            .field("connection", &self.connection)
            .field("state", &self.state)
            .field("next_sequence", &self.next_sequence)
            .field("heartbeat", &self.heartbeat)
            .finish_non_exhaustive()
    }
}

impl Session {
    /// A session for one connection, with no stream yet.
    #[must_use]
    pub fn new(connection: ConnectionId, clock: Arc<dyn Clock>) -> Self {
        Self {
            connection,
            clock,
            stream: None,
            state: SessionState::Closed,
            next_sequence: 1,
            heartbeat: None,
            decoder: Decoder::new(),
            held: Vec::new(),
            scratch: Vec::new(),
            outbound: Vec::new(),
            composed: Vec::new(),
            last_write_ns: 0,
            last_read_ns: 0,
            test_request_outstanding: false,
        }
    }

    /// Take the stream a connect produced, and reset everything that belongs to
    /// a connection.
    ///
    /// **The sequence goes back to 1 here, and nothing is persisted.** A resend
    /// delivers deltas whose value has expired to a book the subscriber has
    /// already rebuilt, and the publisher's own recovery path — a reset
    /// announced, the instrument paused, a snapshot republished — is a
    /// sequence-correct repair rather than a replay of stale intent. Persisting
    /// the sequence would also put a second file under the state directory
    /// whose corruption stops a publisher starting, which is a price this
    /// transport is not paying for a resend the feed does not want.
    pub fn open(&mut self, stream: Box<dyn ByteStream>) {
        self.stream = Some(stream);
        self.state = SessionState::Connected;
        self.next_sequence = 1;
        self.heartbeat = None;
        self.decoder.clear();
        self.held.clear();
        self.scratch.clear();
        // The write buffers, which this reset had been leaving alone. The
        // previous connection's framed logon was still in `outbound` — see
        // that field for what zeroizing it does and does not guarantee, and
        // why the buffer beside it gets a `clear` instead.
        self.outbound.zeroize();
        self.composed.clear();
        self.test_request_outstanding = false;
        let now = self.clock.steady_ns();
        self.last_write_ns = now;
        self.last_read_ns = now;
    }

    /// Which connection this is, as every metric label carries it.
    #[must_use]
    pub const fn connection(&self) -> ConnectionId {
        self.connection
    }

    /// Where the session is.
    #[must_use]
    pub const fn state(&self) -> SessionState {
        self.state
    }

    /// The cadence the logon stated, once one has been written.
    #[must_use]
    pub const fn heartbeat_interval(&self) -> Option<Duration> {
        self.heartbeat
    }

    /// The sequence the next message will go out on.
    #[must_use]
    pub const fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Send one body the adapter wrote, framed and numbered on this session.
    ///
    /// In [`SessionState::Connected`] the body must be a logon: its cadence is
    /// read out of it, the reset flag is stated on it, it goes out on sequence
    /// 1, and the answer is awaited before this returns. In
    /// [`SessionState::Established`] it is framed and numbered as it stands,
    /// which is what makes an instrument admitted mid-session reach a
    /// subscription without a reconnect.
    ///
    /// # Errors
    ///
    /// [`SessionError::SentBeforeEstablished`] for anything but a logon on a
    /// session that has not been established, **with nothing written**.
    /// [`SessionError::Body`] for a body stating a tag this transport owns.
    /// [`SessionError::LogonRejected`] and
    /// [`SessionError::LogonNotAnswered`] for a logon the venue did not accept.
    pub async fn send(&mut self, body: &[u8]) -> Result<(), SessionError> {
        let body = Body::parse(body)?;
        match self.state {
            SessionState::Closed => Err(SessionError::NotConnected),
            SessionState::Connected => {
                if body.msg_type() != msg_type::LOGON {
                    // Refused before anything reaches the stream. The order of
                    // what was written is the assertion, and a refusal after a
                    // partial write would make that assertion unprovable.
                    return Err(SessionError::SentBeforeEstablished {
                        msg_type: body.msg_type().to_owned(),
                    });
                }
                let interval = heartbeat_from(&body)?;
                self.heartbeat = Some(interval);
                self.write(&body, true).await?;
                self.state = SessionState::LogonSent;
                self.await_logon().await
            }
            SessionState::LogonSent | SessionState::LoggingOut => {
                Err(SessionError::SentBeforeEstablished {
                    msg_type: body.msg_type().to_owned(),
                })
            }
            SessionState::Established => self.write(&body, false).await,
        }
    }

    /// Wait for the next thing from the venue, for at most `budget`.
    ///
    /// `None` waits indefinitely, which is what a connection with no idle guard
    /// configured gets — and what makes the cadence below the only thing that
    /// bounds the wait.
    ///
    /// # Errors
    ///
    /// [`SessionError::NoLogon`] on a session at which the adapter wrote none.
    /// [`SessionError::LoggedOut`], [`SessionError::Rejected`],
    /// [`SessionError::Silent`], [`SessionError::Framing`] and
    /// [`SessionError::Stream`] for a session that is over.
    pub async fn receive(
        &mut self,
        budget: Option<Duration>,
    ) -> Result<Incoming<'_>, SessionError> {
        match self.state {
            SessionState::Closed => return Err(SessionError::NotConnected),
            // The refusal that names the adapter's method. Reached the first
            // time a driver receives on a connection whose `on_connected`
            // wrote nothing, which is the whole of how it is detected: a
            // transport cannot know at connect what the adapter is about to
            // queue.
            SessionState::Connected => return Err(SessionError::NoLogon),
            // And the state where the adapter did queue one: naming its method
            // here would tell an operator to fix the code that behaved.
            SessionState::LogonSent => return Err(SessionError::LogonNotEstablished),
            SessionState::Established | SessionState::LoggingOut => {}
        }
        let deadline = budget.map(|budget| self.clock.steady_ns() + nanos(budget));
        // `heartbeat` is set by the logon that established the session, so this
        // fallback is unreachable; it is the minimum rather than a longer value
        // so that a session which somehow reached here without one questions
        // silence rather than sitting in a read.
        let interval = self.heartbeat.unwrap_or(MIN_HEARTBEAT);
        let questioned = with_grace(interval);
        // The number the refusal below states, and the number its arithmetic
        // uses: one expression, so that a log line saying "nothing arrived for
        // N" cannot name a different N from the one that was waited.
        let silence = 2 * questioned;

        loop {
            if self.take()? {
                self.last_read_ns = self.clock.steady_ns();
                self.test_request_outstanding = false;
                match self.classify()? {
                    Classified::Payload => return Ok(Incoming::Message(&self.held)),
                    Classified::Liveness => return Ok(Incoming::Liveness),
                    Classified::TestRequest { id } => {
                        self.answer_test_request(&id).await?;
                        return Ok(Incoming::Liveness);
                    }
                }
            }

            let now = self.clock.steady_ns();
            let heartbeat_at = self.last_write_ns + nanos(interval);
            let question_at = self.last_read_ns + nanos(questioned);
            let dead_at = self.last_read_ns + nanos(silence);

            if now >= heartbeat_at {
                self.heartbeat().await?;
                continue;
            }
            if self.test_request_outstanding && now >= dead_at {
                // A test request went out a cadence ago and nothing has come
                // back. The socket may well still be open: that is exactly the
                // failure a read timeout alone cannot see.
                return Err(SessionError::Silent { interval, silence });
            }
            if !self.test_request_outstanding && now >= question_at {
                self.test_request().await?;
                continue;
            }
            if deadline.is_some_and(|deadline| now >= deadline) {
                return Ok(Incoming::Idle);
            }

            let mut wake_at = heartbeat_at.min(if self.test_request_outstanding {
                dead_at
            } else {
                question_at
            });
            if let Some(deadline) = deadline {
                wake_at = wake_at.min(deadline);
            }
            // Positive by construction: every one of the three instants above
            // was found to be in the future.
            let wait = Duration::from_nanos(wake_at.saturating_sub(now));
            self.read(wait).await?;
        }
    }

    /// Attempt an orderly logout and release the stream.
    ///
    /// The logout is written and its answer is **not** waited for. The usual
    /// reason to be closing is that the peer has stopped answering, and waiting
    /// for its half would put [`LOGOUT_GRACE`] in front of every reconnect.
    /// Every failure here is discarded: this is called on a path that has
    /// already decided the session is over.
    ///
    /// **The attempt is bounded by [`LOGOUT_GRACE`], and that bound is what
    /// declining to wait does not give.** A venue that has stopped reading
    /// fills the send window, and the write of our own logout then sits inside
    /// the kernel's retransmit timeout — minutes — with the teardown path hung
    /// and nothing reporting it. So the write and the stream's own close share
    /// one budget, and whatever it leaves unfinished is dropped.
    ///
    /// Abandoning that write can leave part of a logout on the wire. That is
    /// safe here and nowhere else in this crate: the stream is released on the
    /// next line and the session is over, so there is no following message for
    /// the numbering to be wrong for.
    pub async fn close(&mut self) {
        let _ = timeout(LOGOUT_GRACE, self.logout()).await;
        self.stream = None;
        self.state = SessionState::Closed;
        self.held.clear();
        self.scratch.clear();
        self.decoder.clear();
        // Here rather than only in `open`, because the gap this closes is the
        // one between the two: a publisher that logs on, runs for a day and
        // then sits in backoff would otherwise hold the logon for the whole of
        // the wait. See [`Self::outbound`].
        self.outbound.zeroize();
        self.composed.clear();
        self.test_request_outstanding = false;
    }

    /// The logout and the stream's own half of the close, as one future.
    ///
    /// Split out so that [`LOGOUT_GRACE`] bounds both together rather than each
    /// separately: what the teardown is allowed to cost is one number, and a
    /// budget applied twice is twice that number.
    async fn logout(&mut self) {
        if self.state == SessionState::Established {
            self.state = SessionState::LoggingOut;
            self.compose(Composed::Logout, &[]);
            if let Ok(body) = Body::parse(&self.composed) {
                let now = self.clock.wall_ns();
                framing::frame(
                    &mut self.outbound,
                    &body,
                    self.next_sequence,
                    &sending_time(now),
                    false,
                );
                self.next_sequence += 1;
                if let Some(stream) = self.stream.as_mut() {
                    let _ = stream.write(&self.outbound).await;
                }
            }
        }
        if let Some(stream) = self.stream.as_mut() {
            stream.close().await;
        }
    }

    /// Frame a body onto the session's own numbering and write it.
    async fn write(&mut self, body: &Body<'_>, reset_sequence: bool) -> Result<(), SessionError> {
        let now = self.clock.wall_ns();
        framing::frame(
            &mut self.outbound,
            body,
            self.next_sequence,
            &sending_time(now),
            reset_sequence,
        );
        self.write_outbound().await
    }

    /// Read the venue's answer to the logon.
    ///
    /// # Only a logout and a reject are a refused credential
    ///
    /// **`LogonRejected` is what sends an operator to look at a credential**,
    /// because that is the action
    /// `connect_failures_total{reason="unauthorized"}` names. So the first
    /// message after the socket comes up must not be read as one when it is
    /// an acceptor probes with a `TestRequest` before it answers anything, and
    /// a `Heartbeat` can already be in flight on the acceptor's own cadence
    /// when our logon arrives. Reporting either as a refusal sends somebody to
    /// audit a credential that is fine while the session it was waiting for
    /// would have come up on the next read.
    ///
    /// So `0`, `1` and `4` are **neither the answer nor a refusal**: they keep
    /// the wait going, within the same [`LOGON_GRACE`], and a venue that only
    /// ever sends those ends as [`SessionError::LogonNotAnswered`] — which is
    /// the same distinction the crate docs draw between a session with no logon
    /// and one whose logon was refused, held here at the one place the
    /// difference can still be seen.
    ///
    /// Nothing is written back at this point, the venue's test request
    /// included. Only the logon may go out before the session is established
    /// (see [`Session::send`]), and answering a probe here would put a
    /// heartbeat on sequence 2 on a session the venue has not accepted.
    ///
    /// Everything else — a resend request this transport does not serve, an
    /// application message before there is a session to carry one, a message
    /// with no type at all — is [`SessionError::Rejected`], carrying what
    /// arrived. It ends the connection exactly as the same message would
    /// mid-session, and it is deliberately not the credential's error: the one
    /// thing an operator should not be told is to go and look at a credential
    /// the venue never mentioned.
    async fn await_logon(&mut self) -> Result<(), SessionError> {
        let deadline = self.clock.steady_ns() + nanos(LOGON_GRACE);
        loop {
            if self.take()? {
                let answer = {
                    let message = Message::new(&self.held);
                    match message.msg_type() {
                        Some(msg_type::LOGON) => Answer::Established,
                        // The acceptor's own cadence and its probes. See above:
                        // not an answer, and not the credential either.
                        Some(
                            msg_type::HEARTBEAT | msg_type::TEST_REQUEST | msg_type::SEQUENCE_RESET,
                        ) => Answer::KeepWaiting,
                        // A logout or a reject answering a logon is the venue
                        // refusing the credential, which is one thing however
                        // it is spelled — and it is a connect failure rather
                        // than a session that ended, because no session was
                        // established.
                        Some(msg_type::LOGOUT | msg_type::REJECT) => {
                            Answer::Refused(detail(&message))
                        }
                        _ => Answer::Unrelated(detail(&message)),
                    }
                };
                match answer {
                    Answer::Established => {
                        self.state = SessionState::Established;
                        self.last_read_ns = self.clock.steady_ns();
                        return Ok(());
                    }
                    // Back to the top, where the next whole message is taken
                    // if one has already arrived and a read is waited for
                    // otherwise — so the grace bounds the whole wait and not
                    // each message in it.
                    Answer::KeepWaiting => continue,
                    Answer::Refused(detail) => return Err(SessionError::LogonRejected { detail }),
                    Answer::Unrelated(detail) => {
                        return Err(SessionError::Rejected {
                            detail: format!(
                                "the venue answered the logon with a message that is neither a \
                                 logon nor a refusal: {detail}"
                            ),
                        })
                    }
                }
            }
            let now = self.clock.steady_ns();
            if now >= deadline {
                return Err(SessionError::LogonNotAnswered { grace: LOGON_GRACE });
            }
            let wait = Duration::from_nanos(deadline - now);
            self.read(wait).await?;
        }
    }

    /// One read, appended to the decoder.
    async fn read(&mut self, budget: Duration) -> Result<(), SessionError> {
        self.scratch.clear();
        let stream = self.stream.as_mut().ok_or(SessionError::NotConnected)?;
        let read = stream.read(&mut self.scratch, budget).await?;
        if read > 0 {
            let bytes = &self.scratch[self.scratch.len() - read..];
            self.decoder.feed(bytes);
        }
        Ok(())
    }

    /// Move the next whole message into [`Self::held`].
    fn take(&mut self) -> Result<bool, SessionError> {
        Ok(self.decoder.take(&mut self.held)?)
    }

    /// What the held message is, and what the session makes of it.
    ///
    /// # Errors
    ///
    /// The three session messages that end a session: a logout, a session-level
    /// reject, and a resend request this transport cannot serve.
    fn classify(&self) -> Result<Classified, SessionError> {
        let message = Message::new(&self.held);
        let Some(msg_type) = message.msg_type() else {
            // A message with no `35=` reached here through a checksum that
            // held, so the bytes are intact and the message is not one this
            // protocol defines.
            return Err(SessionError::Rejected {
                detail: format!("a message with no message type: {}", detail(&message)),
            });
        };
        match msg_type {
            msg_type::HEARTBEAT => Ok(Classified::Liveness),
            msg_type::TEST_REQUEST => Ok(Classified::TestRequest {
                id: message
                    .field(TAG_TEST_REQ_ID)
                    .map(|id| String::from_utf8_lossy(id).into_owned())
                    .unwrap_or_default(),
            }),
            // Nothing to reset: no inbound numbering is tracked, because
            // tracking one would only be worth it to ask for a resend this
            // transport does not do.
            msg_type::SEQUENCE_RESET => Ok(Classified::Liveness),
            msg_type::LOGOUT => Err(SessionError::LoggedOut {
                detail: detail(&message),
            }),
            msg_type::REJECT => Err(SessionError::Rejected {
                detail: detail(&message),
            }),
            msg_type::RESEND_REQUEST => Err(SessionError::ResendRequested {
                detail: detail(&message),
            }),
            // A second logon on an established session is not a transition this
            // state machine has, and guessing at one would be guessing about
            // whose numbering is in force.
            msg_type::LOGON => Err(SessionError::Rejected {
                detail: format!(
                    "a second logon on an established session: {}",
                    detail(&message)
                ),
            }),
            _ => Ok(Classified::Payload),
        }
    }

    /// The cadence's own message.
    async fn heartbeat(&mut self) -> Result<(), SessionError> {
        self.compose(Composed::Heartbeat, &[]);
        self.write_composed().await
    }

    /// Ask the venue whether it is there.
    ///
    /// The `TestReqID` is the sequence this message goes out on, which makes it
    /// unique per session without a random-number generator, and makes the answer
    /// readable against the request in a capture.
    async fn test_request(&mut self) -> Result<(), SessionError> {
        let id = format!("{}", self.next_sequence);
        self.compose(Composed::TestRequest, &[(TAG_TEST_REQ_ID, id.as_bytes())]);
        self.write_composed().await?;
        self.test_request_outstanding = true;
        Ok(())
    }

    /// Answer the venue's own test request, echoing its identifier.
    async fn answer_test_request(&mut self, id: &str) -> Result<(), SessionError> {
        self.compose(Composed::Heartbeat, &[(TAG_TEST_REQ_ID, id.as_bytes())]);
        self.write_composed().await
    }

    /// One of the session layer's own messages, as a body.
    ///
    /// Composed as a body and then framed through the same path an adapter's
    /// body takes, rather than assembled directly: one encoder means a session
    /// message and a subscription cannot disagree about the header.
    ///
    /// The type is a [`Composed`] and not a `&str`, which is what closes the
    /// set: what this transport composes is three session messages, and a
    /// fourth cannot arrive here as a literal.
    fn compose(&mut self, msg_type: Composed, fields: &[(u32, &[u8])]) {
        self.composed.clear();
        self.composed.extend_from_slice(
            format!("{}={}", framing::TAG_MSG_TYPE, msg_type.as_msg_type()).as_bytes(),
        );
        self.composed.push(framing::SOH);
        for (tag, value) in fields {
            self.composed.extend_from_slice(tag.to_string().as_bytes());
            self.composed.push(b'=');
            self.composed.extend_from_slice(value);
            self.composed.push(framing::SOH);
        }
    }

    /// Frame and write whatever [`Self::compose`] last built.
    async fn write_composed(&mut self) -> Result<(), SessionError> {
        let now = self.clock.wall_ns();
        let sending_time = sending_time(now);
        // Scoped so the borrow of `composed` ends before the write: the bound
        // on that write belongs to the session and not to one call site, so
        // the write is a method on `self` rather than a line here.
        {
            let body = Body::parse(&self.composed)?;
            framing::frame(
                &mut self.outbound,
                &body,
                self.next_sequence,
                &sending_time,
                false,
            );
        }
        self.write_outbound().await
    }

    /// Hand the framed message to the stream under [`WRITE_GRACE`], and move
    /// the session past it.
    ///
    /// **One place, because a write that does not come through here is an
    /// unbounded one.** Both paths that write on a live session end at this
    /// method — the body an adapter wrote, including its logon, and the session
    /// layer's own composed message — so the grace is applied by construction
    /// rather than remembered at each call site. The teardown's write is the
    /// exception and bounds itself: [`Session::close`] runs it and the stream's
    /// own close together under [`LOGOUT_GRACE`], which is the shorter number
    /// and the one a path that has already decided the session is over wants.
    ///
    /// A write the grace cut short is [`StreamError::Failed`] and not a case of
    /// its own, for the reason that type states: the taxonomy is deliberately
    /// coarser than the failure, because every transport above this one ends
    /// the connection and lets the driver reconnect — which is the right answer
    /// for a venue that has stopped reading too. The part an operator acts on
    /// goes in the detail instead.
    ///
    /// The sequence advances only on a write that went out whole. That is what
    /// abandoning one costs and it is why this ends the session rather than
    /// being retried: what reached the wire may be half a message, and a
    /// session cannot number the next one against a venue that may have read
    /// half of this one.
    async fn write_outbound(&mut self) -> Result<(), SessionError> {
        let stream = self.stream.as_mut().ok_or(SessionError::NotConnected)?;
        match timeout(WRITE_GRACE, stream.write(&self.outbound)).await {
            Ok(written) => written?,
            Err(_elapsed) => {
                return Err(StreamError::Failed {
                    detail: format!(
                        "{} bytes did not leave within {WRITE_GRACE:?}: the venue has stopped \
                         reading and the send window is full, which a socket that is still \
                         open reports as nothing at all",
                        self.outbound.len()
                    ),
                }
                .into())
            }
        }
        self.next_sequence += 1;
        self.last_write_ns = self.clock.steady_ns();
        Ok(())
    }
}

/// The cadence a logon body states, checked.
///
/// # Errors
///
/// [`SessionError::NoHeartbeatInterval`] when the body states none, and
/// [`SessionError::UnusableHeartbeatInterval`] for a value this transport will
/// not run a session at.
fn heartbeat_from(body: &Body<'_>) -> Result<Duration, SessionError> {
    let stated = body
        .field(TAG_HEART_BT_INT)
        .ok_or(SessionError::NoHeartbeatInterval)?;
    let rendered = String::from_utf8_lossy(stated).into_owned();
    let seconds: u64 = core::str::from_utf8(stated)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| SessionError::UnusableHeartbeatInterval {
            stated: rendered.clone(),
            min: MIN_HEARTBEAT,
            max: MAX_HEARTBEAT,
        })?;
    let interval = Duration::from_secs(seconds);
    if interval < MIN_HEARTBEAT || interval > MAX_HEARTBEAT {
        return Err(SessionError::UnusableHeartbeatInterval {
            stated: rendered,
            min: MIN_HEARTBEAT,
            max: MAX_HEARTBEAT,
        });
    }
    Ok(interval)
}

/// The cadence plus the protocol's own grace on it.
///
/// A fifth, which is the tolerance the protocol names for a peer's heartbeat:
/// silence is questioned at one of these and the session is dead at two.
fn with_grace(interval: Duration) -> Duration {
    interval + interval / 5
}

/// A duration as nanoseconds, saturating.
///
/// Every duration here is a cadence or a grace, so the saturation is
/// unreachable; it exists so that the arithmetic has no panic in it.
fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// What a session message said, for an error an operator reads.
///
/// The sequence, the venue's own text and the coded reject reason, and **not
/// the whole message**: a logon's fields are the one place a credential
/// appears, and an error detail becomes a log line.
fn detail(message: &Message<'_>) -> String {
    let mut parts = Vec::new();
    if let Some(msg_type) = message.msg_type() {
        parts.push(format!("{}={msg_type}", framing::TAG_MSG_TYPE));
    }
    if let Some(sequence) = message.field_u64(TAG_MSG_SEQ_NUM) {
        parts.push(format!("{TAG_MSG_SEQ_NUM}={sequence}"));
    }
    for tag in [TAG_SESSION_REJECT_REASON, TAG_REF_TAG_ID, TAG_TEXT] {
        if let Some(value) = message.field(tag) {
            parts.push(format!("{tag}={}", String::from_utf8_lossy(value)));
        }
    }
    parts.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A body written with `|`, for the cadence checks below.
    fn body(rendered: &str) -> Body<'static> {
        let bytes = rendered
            .replace('|', &(framing::SOH as char).to_string())
            .into_bytes();
        Body::parse(Box::leak(bytes.into_boxed_slice())).expect("a body")
    }

    #[test]
    fn the_cadence_is_read_out_of_the_logon_body() {
        assert_eq!(
            heartbeat_from(&body("35=A|108=30|")),
            Ok(Duration::from_secs(30))
        );
        assert_eq!(
            heartbeat_from(&body("35=A|108=5|")),
            Ok(Duration::from_secs(5))
        );
    }

    #[test]
    fn a_logon_with_no_cadence_has_no_session_to_run() {
        assert_eq!(
            heartbeat_from(&body("35=A|98=0|")),
            Err(SessionError::NoHeartbeatInterval)
        );
    }

    #[test]
    fn a_cadence_outside_the_stated_bounds_is_refused_naming_them() {
        for stated in ["0", "601", "not-a-number", "-5"] {
            let error = heartbeat_from(&body(&format!("35=A|108={stated}|")))
                .expect_err("outside the bounds");
            match error {
                SessionError::UnusableHeartbeatInterval { min, max, .. } => {
                    assert_eq!(min, MIN_HEARTBEAT);
                    assert_eq!(max, MAX_HEARTBEAT);
                }
                other => panic!("`{stated}` was refused as {other}"),
            }
        }
    }

    #[test]
    fn no_message_this_crate_composes_is_an_order_entry_message() {
        // The protocol defines an order-entry path and this transport has no
        // reason to reach it: what leaves here is what the adapter wrote plus
        // the session layer's own messages. `compose` takes a `Composed` and
        // not a `&str`, so *the session layer's own* half of that is closed by
        // the type — a `compose("D", …)` on a future path does not compile.
        // What is left to assert is that the set is what it claims to be.
        //
        // The order-entry tokens are transcribed as literals rather than
        // derived, because a list checked against itself agrees with its own
        // mistake. `D` NewOrderSingle, `F` OrderCancelRequest,
        // `G` OrderCancelReplaceRequest, `E` NewOrderList,
        // `AB` NewOrderMultileg, `8` ExecutionReport, `9` OrderCancelReject,
        // `H` OrderStatusRequest.
        let composed: Vec<&str> = Composed::ALL
            .iter()
            .map(|composed| composed.as_msg_type())
            .collect();
        for order_entry in ["D", "F", "G", "E", "AB", "8", "9", "H"] {
            assert!(
                !composed.contains(&order_entry),
                "`{order_entry}` is an order-entry message type and this crate composes none"
            );
        }
        // And every one of them is the session layer's own, which is the other
        // half of the claim: a type this crate composes that the decoder would
        // hand to the adapter as a payload would be this crate writing
        // application messages.
        for one in &composed {
            assert!(
                msg_type::is_session(one),
                "`{one}` is composed here and must be one of the session layer's own types"
            );
        }
        let mut distinct = composed.clone();
        distinct.sort_unstable();
        distinct.dedup();
        assert_eq!(
            distinct.len(),
            composed.len(),
            "two of the composed messages share a token"
        );
    }

    #[test]
    fn the_grace_on_a_cadence_is_a_fifth_of_it() {
        assert_eq!(with_grace(Duration::from_secs(30)), Duration::from_secs(36));
        assert_eq!(with_grace(Duration::from_secs(10)), Duration::from_secs(12));
    }

    /// A stream `open` and `close` can be handed, which nothing here reads,
    /// writes or asserts against. The scripted stream every behavioural test
    /// uses lives in `tests/session.rs`; what this module needs is only a
    /// value of the type.
    struct Unused;

    impl ByteStream for Unused {
        fn write<'a>(&'a mut self, _bytes: &'a [u8]) -> BoxFuture<'a, Result<(), StreamError>> {
            Box::pin(async { Ok(()) })
        }

        fn read<'a>(
            &'a mut self,
            _out: &'a mut Vec<u8>,
            _budget: Duration,
        ) -> BoxFuture<'a, Result<usize, StreamError>> {
            Box::pin(async { Ok(0) })
        }

        fn close(&mut self) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }

    /// Neither write buffer survives an `open` or a `close`.
    ///
    /// The gap this is about: `open` and `close` reset the decoder and the read
    /// buffers, and both left `outbound` and `composed` exactly as the last
    /// message found them — so the framed logon, signature and password
    /// included, stayed in the heap across the whole idle-and-reconnect window.
    ///
    /// **This pins the length and not the erasure, and the difference is the
    /// whole point of the fix.** `outbound` is zeroized rather than cleared,
    /// because `Vec::clear` leaves every byte in the allocation; swapping the
    /// `zeroize` back for a `clear` keeps this test green, since a `Vec`'s
    /// spare capacity is readable only through `unsafe` and this crate forbids
    /// it. Nothing in this crate can tell the two apart, which is stated here
    /// rather than left for a reader to assume the stronger claim. What the
    /// test does catch is the reset being dropped altogether — a logon, or a
    /// session message, still reachable through the buffer after the session
    /// that wrote it is over — which is the state both buffers were in.
    #[tokio::test]
    async fn the_write_buffers_do_not_outlive_the_session_that_filled_them() {
        let mut session = Session::new(
            ConnectionId::new("mktdata"),
            Arc::new(dz_ingress_core::TokioClock::new()),
        );
        // Framed the way `send` frames a logon, `554` standing in for the
        // signature a venue's scheme puts there — the same value
        // `an_error_detail_carries_the_reject_reason_and_not_the_message`
        // looks for, because it is the same class of leak in a second place.
        let logon = body("35=A|108=30|554=not-a-real-secret|");
        let fill = |session: &mut Session| {
            framing::frame(&mut session.outbound, &logon, 1, &sending_time(0), true);
            session.compose(Composed::Heartbeat, &[]);
        };

        fill(&mut session);
        assert!(!session.outbound.is_empty(), "the fixture framed nothing");
        assert!(!session.composed.is_empty(), "the fixture composed nothing");
        session.open(Box::new(Unused));
        assert!(
            session.outbound.is_empty(),
            "`open` carried a framed logon into the next session"
        );
        assert!(
            session.composed.is_empty(),
            "`open` carried a composed session message into the next session"
        );

        // And the other end of the same session, which is the longer window:
        // a publisher in backoff sits here, not in `open`.
        fill(&mut session);
        session.close().await;
        assert!(
            session.outbound.is_empty(),
            "`close` left a framed logon in the outbound buffer"
        );
        assert!(
            session.composed.is_empty(),
            "`close` left a composed session message in the buffer"
        );
    }

    #[test]
    fn an_error_detail_carries_the_reject_reason_and_not_the_message() {
        // A logon's fields are the one place a credential appears and a detail
        // becomes a log line, so the detail is a stated list of tags rather
        // than the bytes.
        let bytes =
            "8=FIX.4.4|9=0|35=3|34=4|373=5|58=required tag missing|554=not-a-real-secret|10=000|"
                .replace('|', &(framing::SOH as char).to_string())
                .into_bytes();
        let rendered = detail(&Message::new(&bytes));
        assert!(rendered.contains("35=3"), "{rendered}");
        assert!(rendered.contains("34=4"), "{rendered}");
        assert!(rendered.contains("373=5"), "{rendered}");
        assert!(rendered.contains("required tag missing"), "{rendered}");
        assert!(!rendered.contains("not-a-real-secret"), "{rendered}");
    }
}
