//! A session transport, and the one thing in it that is the venue's.
//!
//! `[ingress] kind = "fix"` resolves to this. The protocol is session-oriented
//! and tag-value encoded, it carries market data and it also defines an
//! order-entry path — **this transport reads market data and composes no order
//! entry at all**. What leaves here is what the adapter wrote plus the session
//! layer's own messages, and the session layer composes nothing but session
//! messages.
//!
//! # Where a venue's signature lives, and why nothing is injected
//!
//! **The seam exists and is declined on purpose.** A venue's own `main`
//! constructs its transports and hands them back to the runtime, so a
//! constructor here taking a logon signer would have been possible — this is
//! not a case of there being nowhere to put one.
//!
//! It is declined because of what it would carry. A logon is not one field: it
//! is an identity, a credential, whatever ordering or canonical form a venue's
//! scheme signs over, and whatever else that scheme asks for. A signer
//! parameter would put half of one logon in the adapter and the other half
//! behind a callback this crate held, which is two places to look when a venue
//! refuses a credential — and it would be this repository holding the shape of
//! a venue's authentication, which is the thing that changes on the venue's
//! schedule and not on ours.
//!
//! **The logon body is the adapter's and everything around it is this crate's.**
//! `Adapter::on_connected` already exists for authentication frames and already
//! says so, so a venue composes the logon's venue-specific fields — its
//! identity, its signature, whatever its scheme requires — there, and this
//! crate:
//!
//! - frames it: the declared length over the span the protocol mandates, the
//!   checksum, and the four positions the protocol fixes — `8`, `9` and `35`
//!   leading, in that order, and `10` last. What sits between them is this
//!   crate's own order, and [`framing::frame`] says what it is and why;
//! - numbers it, and every message after it, on the session's own outbound
//!   sequence;
//! - reads the heartbeat interval **out of it** and runs the cadence from that
//!   value, rather than from a key an operator could set to disagree with what
//!   was logged on with;
//! - refuses to send anything else before it, so a subscription cannot precede
//!   a logon on a session that has not been established.
//!
//! The signature therefore stays in venue code, in the method that already
//! writes at logon, and this crate signs nothing on a venue's behalf.
//!
//! ## What that does not reach: a signature over tags this crate owns
//!
//! **The common FIX logon scheme signs a canonical string that includes
//! `SendingTime` and `MsgSeqNum`, and an adapter here can state neither.**
//! A body is refused both — tag `52` because the sending time is stamped when
//! the message is framed, tag `34` because the outbound sequence belongs to
//! the session — and the session takes both *after* `Adapter::on_connected`
//! has returned. Nothing on the `UpstreamSink` path tells an adapter what
//! either value will be, so an adapter whose scheme signs over them composes a
//! signature the venue rejects, and the publisher loops on an authentication
//! refusal with nothing in it that looks like a defect.
//!
//! So the seam above holds for a venue whose logon signature covers only what
//! it writes itself — its identity, its target, a password — and not for one
//! whose signature covers `52` or `34`. The second shape is the common one:
//! `SendingTime | MsgType | MsgSeqNum | SenderCompID | TargetCompID` signed
//! into `RawData` is what several venues ask for, and no test here would
//! notice, because every logon fixture carries an opaque `554` and signs
//! nothing.
//!
//! **This is a limitation and not a position.** What the paragraphs above
//! decline is a *signer parameter*, and that argument stands. Handing the
//! adapter the stamp and the sequence it is about to be framed under is a
//! narrower change they do not cover, because the scheme, the key and the
//! canonical form would all stay in venue code and this crate would still hold
//! no credential. It is not made here because it is a change to the adapter
//! seam every transport sits on, and it has ordering to settle rather than a
//! parameter to add: the session would have to commit to both values before
//! calling the adapter and then frame under exactly those, including when a
//! delayed write leaves the stamp stale.
//!
//! The same argument is already conceded inbound, which is worth reading
//! beside this: [`session::Incoming::Message`] is handed
//! over entire, and says it is because a venue's own message identity may be
//! computed over the sequence and the sending time.
//!
//! # Sequence numbers reset at logon, and that is a decision
//!
//! The outbound sequence starts at 1 on every logon, with the flag that says
//! so, and nothing is persisted. The reason is what the feed is for: a resend
//! delivers the book updates that were missed, and a market-data consumer that
//! receives them minutes later is applying deltas whose value has expired to a
//! book it has already rebuilt. The publisher's own recovery path is better in
//! every respect — it announces a reset, pauses the instrument and republishes
//! from a snapshot, which is a subscriber-visible, sequence-correct repair
//! rather than a replay of stale intent.
//!
//! Persisting session state would also put a second thing under the state
//! directory whose corruption stops a publisher from starting, and the era
//! store's own design argues at length about how expensive that file is to get
//! wrong. Paying that for a resend the feed does not want is the wrong trade.
//!
//! **A venue that requires sequence continuity is a venue this transport does
//! not serve**, and [`SessionConfig`] says so at load rather than logging on
//! and misbehaving. See [`SessionConfigError::SequenceContinuity`].
//!
//! # What the transport owns, and what it must not
//!
//! **Owns:** the socket and TLS; the framing, in both directions, including
//! the refusal of a message whose declared length or checksum does not hold;
//! the timestamp format; the session lifecycle — logon, the heartbeat cadence,
//! the test request that answers a suspicion of silence, the logout and the
//! orderly close; the outbound sequence; and the classification of every
//! failure into an [`IngressError`](dz_ingress_core::IngressError), which is
//! the load-bearing part because a disconnect reason is a metric label with
//! four values and this is the only layer that can see a session-level reject.
//!
//! **Must not:** decide when to connect, how long to wait before retrying,
//! what a payload means, or whether silence means anything. Those are
//! [`Driver`](dz_ingress_core::Driver)'s, so that they are one implementation
//! for every transport rather than one per publisher.
//!
//! **Does not build an order-entry path.** The protocol has one and this
//! repository has no reason to reach it: what leaves this transport is what the
//! adapter wrote plus the session layer's own messages, and the session layer
//! composes nothing but session messages.
//!
//! # A session message is `Liveness` and never a payload
//!
//! The driver's idle guard counts time since the last *payload*, because a
//! venue that has quietly dropped a subscription heartbeats perfectly. So a
//! session that heartbeats forever and delivers nothing must still trip the
//! guard, and every session message this transport receives is
//! [`Received::Liveness`](dz_ingress_core::Received) or an error — never a
//! payload.
//!
//! # The test surface, and why it needs no socket
//!
//! A session layer is a state machine over a byte stream, so the byte stream is
//! behind [`ByteStream`], a trait this crate owns — the move `RouteLookup`
//! makes for the routing table and [`Clock`](dz_ingress_core::Clock) makes for
//! time. Every case that matters is then a test that runs unprivileged with no
//! network: a logon answered, a logon rejected, a heartbeat due, a test request
//! answered, a message whose checksum does not hold, a message split across two
//! reads, two messages in one read, a logout from the venue, and a session that
//! goes silent.
//!
//! The real socket is exercised against a loopback endpoint, which is the half
//! no fake proves. So is the half of TLS a loopback endpoint can settle: a
//! certificate no compiled-in anchor signed **is refused**, and the refusal is
//! `tls` rather than a plain failure, which together is how this crate states
//! that verification is on at all. Proving the accepting half would mean
//! trusting a root of our own, which is a configuration this crate does not
//! build — see [`input`] and the loopback suite's own note.
//!
//! # Vocabulary
//!
//! A unit on this transport is a *message*. The wire's unit is a datagram and
//! it is nowhere near this crate: nothing here has been encoded yet. The
//! session's own numbering is a *sequence*; the publisher's era is an era and
//! this crate has none.

#![forbid(unsafe_code)]

pub mod config;
pub mod framing;
pub mod input;
pub mod session;
pub mod timestamp;

pub use config::{Endpoint, SessionConfig, SessionConfigError};
pub use framing::{
    checksum, frame, rendered, Body, BodyError, Decoder, Field, Fields, FramingError, Message,
    BEGIN_STRING,
};
pub use input::{Connector, FixInput, SocketConnector};
pub use session::{
    ByteStream, Composed, Incoming, Session, SessionError, SessionState, StreamError,
};
pub use timestamp::sending_time;
