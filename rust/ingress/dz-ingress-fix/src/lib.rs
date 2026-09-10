//! A session transport, and the one thing in it that is the venue's.
//!
//! `[ingress] kind = "fix"` resolves to this. The protocol is session-oriented
//! and tag-value encoded, it carries market data and it also defines an
//! order-entry path — **this transport reads market data and composes no order
//! entry at all**. What leaves here is what the adapter wrote plus the session
//! layer's own messages, and the session layer composes nothing but session
//! messages.
//!
//! # Where a venue's signature lives, and why it is not injected
//!
//! Transports in this family are constructed by the runtime from a closed
//! [`Kind`](dz_ingress_core::Kind) match, deliberately: the family is fixed and
//! lives in this repository. Adapters are the opposite — a registry the venue's
//! own `main` populates. So there is no seam through which a venue could hand a
//! logon signer to a transport this repository constructs, and inventing one
//! would be inventing an injection point for a single field.
//!
//! **The logon body is the adapter's and everything around it is this crate's.**
//! `Adapter::on_connected` already exists for authentication frames and already
//! says so, so a venue composes the logon's venue-specific fields — its
//! identity, its signature, whatever its scheme requires — there, and this
//! crate:
//!
//! - frames it: the declared length over the span the protocol mandates, the
//!   checksum, and the header fields in the positions they belong in;
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
//! # Vocabulary
//!
//! A unit on this transport is a *message*. The wire's unit is a datagram and
//! it is nowhere near this crate: nothing here has been encoded yet. The
//! session's own numbering is a *sequence*; the publisher's era is an era and
//! this crate has none.

#![forbid(unsafe_code)]

pub mod config;
pub mod framing;
pub mod session;
pub mod timestamp;

pub use config::{Endpoint, SessionConfig, SessionConfigError};
pub use framing::{
    checksum, frame, rendered, Body, BodyError, Decoder, Field, Fields, FramingError, Message,
    BEGIN_STRING,
};
pub use session::{ByteStream, Incoming, Session, SessionError, SessionState, StreamError};
pub use timestamp::sending_time;
