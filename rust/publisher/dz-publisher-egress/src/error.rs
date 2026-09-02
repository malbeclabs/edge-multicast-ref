//! Why a send failed, in the taxonomy the metric label is drawn from.
//!
//! Every failure below carries the `reason` label value it is counted under, so
//! that the label a dashboard groups by is decided once, here, rather than at
//! each call site that catches the error. There is no exception: two of the
//! label values are additions the governing playbook does not yet carry, and
//! [`EgressError::reason`] states which and why.

use std::io;

use dz_edge_core::{EncodeError, MAX_DATAGRAM_SIZE};
use dz_publisher_metrics::EgressErrorReason;

use crate::instance::ChannelInstance;

/// Why a [`DatagramSink`](crate::DatagramSink) could not take a datagram.
///
/// Separate from [`EgressError`] because a sink is the boundary: a sink knows
/// what the socket said and nothing about the message that was being composed,
/// and the composer knows the opposite. Folding them would give every
/// implementor of the trait variants it cannot produce.
#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    /// The send buffer is full. Transient by construction, and the reason the
    /// socket is non-blocking: the alternative is a publish loop parked in
    /// `sendmsg` while every other channel instance it serves goes quiet.
    #[error("the send buffer is full and the socket would have blocked")]
    WouldBlock,

    /// The socket refused the datagram for any other reason.
    ///
    /// Treated as **not** transient. The failure this crate exists to survive
    /// is a tunnel interface re-provisioned underneath a live socket, which
    /// returns the same error forever; recovering from it means re-deriving the
    /// source address and opening a new socket, not retrying this one. See
    /// [`Self::is_transient`].
    #[error("send failed: {0}")]
    Socket(#[from] io::Error),

    /// The datagram is longer than the mandated cap.
    ///
    /// Checked again where the bytes meet the socket, even though
    /// [`dz_edge_core::DatagramBuilder`] cannot produce one: a builder is not
    /// the only thing that can hand a sink bytes, and an over-cap datagram
    /// reaching the wire is a defect that has already shipped once — it is
    /// fragmented by the GRE encapsulation the cap exists to leave room for,
    /// and the loss is charged to the network rather than to the publisher.
    #[error("a datagram of {len} bytes exceeds the mandated cap of {MAX_DATAGRAM_SIZE}")]
    TooLarge { len: usize },

    /// There is nowhere left to send. A fan-out every member of which has been
    /// dropped, or a port role no transmitter was registered for.
    #[error("no live transmitter is registered to send on")]
    NotRegistered,
}

impl SinkError {
    /// The `reason` label value this failure is counted under.
    #[must_use]
    pub const fn reason(&self) -> EgressErrorReason {
        match self {
            Self::WouldBlock => EgressErrorReason::SendWouldBlock,
            Self::Socket(_) => EgressErrorReason::SocketError,
            Self::TooLarge { .. } => EgressErrorReason::MtuExceeded,
            Self::NotRegistered => EgressErrorReason::NotRegistered,
        }
    }

    /// Whether the same socket may reasonably be tried with the next datagram.
    ///
    /// This decides whether a fan-out drops the member that produced it (see
    /// [`Tee`](crate::Tee)). A full send buffer drains; a socket whose route
    /// has gone does not, and a per-datagram syscall that has failed the same
    /// way for an hour is a cost paid to learn nothing.
    #[must_use]
    pub const fn is_transient(&self) -> bool {
        matches!(self, Self::WouldBlock)
    }
}

/// Why a message did not reach the wire.
#[derive(Debug, thiserror::Error)]
pub enum EgressError {
    /// The codec refused the message.
    ///
    /// Carried verbatim rather than re-described, because the codec's own
    /// enumeration already separates the four cases for exactly the reasons a
    /// reader of a log line needs them separated, and a second vocabulary here
    /// would drift from it. [`Self::reason`] maps it to the metric label.
    ///
    /// Every case is per-message and recoverable: the message is dropped, the
    /// refusal is counted, and the next message is taken. A publisher that
    /// aborts on one malformed message goes dark on every instrument it serves.
    #[error(transparent)]
    Refused {
        #[from]
        source: EncodeError,
    },

    /// The datagram would have been numbered under a channel instance the
    /// sequencer does not hold.
    ///
    /// Refused rather than registered on demand. A `Channel ID` nobody
    /// registered is a configuration that does not match the code that reads
    /// it, and starting a fresh sequence series for it mid-run publishes a
    /// channel instance no subscriber was told to expect — beginning at
    /// sequence 0 in whatever era the process happens to be in, which every
    /// subscriber that *is* listening reads as a publisher restart.
    #[error("{instance} is not registered with the sequencer")]
    NotRegistered { instance: ChannelInstance },

    /// The sink refused the composed datagram.
    #[error(transparent)]
    Sink {
        #[from]
        source: SinkError,
    },
}

impl EgressError {
    /// The `reason` label value this failure is counted under.
    ///
    /// Total, and it did not used to be: this returned `Option` while
    /// [`EncodeError::NotCarriedByFeed`] and [`EncodeError::MalformedMessage`]
    /// had no label value between them, and a `None` here meant a failure that
    /// reached the wire's own error type and then reached no series at all.
    /// `EgressErrorReason` now carries a value for each, so every way a message
    /// can fail to reach the wire is counted, and the `Option` that existed
    /// only to hold the gap open is gone.
    ///
    /// Neither new value is folded into `wrong_port_role`, which is the nearest
    /// existing one — both are the specification refusing a placement — because
    /// they are three different mistakes and an operator acts differently on
    /// each: a wrong role is a send path wired to the wrong socket, a message
    /// the feed does not carry is a publisher composing for a feed it is not
    /// emitting, and a malformed message is a field combination its own
    /// specification forbids. They are label values on
    /// `dz_publisher_egress_errors_total` rather than families of their own,
    /// because both are per-message send failures carrying the same `port_role`
    /// as the other five: a separate family would split "a message did not
    /// reach the wire" in two and make every panel sum both.
    ///
    /// Both are proposed additions the governing playbook does not yet carry;
    /// see [`EgressErrorReason`].
    ///
    /// [`EncodeError::MessageCountExhausted`] maps to `mtu_exceeded` as the
    /// nearest existing value, and is unreachable through
    /// [`ChannelEgress`](crate::ChannelEgress): a datagram already holding 255
    /// messages is flushed and the message retried on a fresh one, so the only
    /// way to observe it is to drive a [`dz_edge_core::DatagramBuilder`]
    /// directly.
    #[must_use]
    pub const fn reason(&self) -> EgressErrorReason {
        match self {
            Self::Refused { source } => match source {
                EncodeError::DatagramFull { .. } | EncodeError::MessageCountExhausted { .. } => {
                    EgressErrorReason::MtuExceeded
                }
                EncodeError::WrongPortRole { .. } => EgressErrorReason::WrongPortRole,
                EncodeError::NotCarriedByFeed { .. } => EgressErrorReason::NotCarriedByFeed,
                EncodeError::MalformedMessage { .. } => EgressErrorReason::MalformedMessage,
            },
            Self::NotRegistered { .. } => EgressErrorReason::NotRegistered,
            Self::Sink { source } => source.reason(),
        }
    }
}
