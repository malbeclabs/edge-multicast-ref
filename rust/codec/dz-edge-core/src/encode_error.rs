/// Errors produced when building a datagram.
///
/// Separate from `DecodeError` so a publisher's send path does not name a decode
/// type, and so neither enum carries variants the other direction cannot reach.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum EncodeError {
    #[error("datagram builder full: {attempted} bytes would exceed capacity {capacity}")]
    DatagramFull { attempted: usize, capacity: usize },

    /// The datagram already holds 255 messages, the most the u8 Message Count
    /// field can express. Distinct from `DatagramFull`, which is a byte-capacity
    /// limit - conflating them sends a reader chasing an MTU problem.
    #[error("datagram already holds the maximum {max} messages")]
    MessageCountExhausted { max: u8 },

    /// The message's specification does not permit it on this datagram's port
    /// role. Recoverable: the send path counts it and drops the message rather
    /// than aborting, because a publisher that panics goes dark.
    #[error("{message} may not be carried on the {role} port role")]
    WrongPortRole {
        message: &'static str,
        role: &'static str,
    },

    /// The feed this datagram carries does not define this message.
    ///
    /// The magic would have been right — a builder is generic over its feed —
    /// so the datagram would have looked like the feed it claimed to be, with a
    /// message inside that the feed's specification does not list. A subscriber
    /// reading it has no defined behaviour to fall back on.
    ///
    /// Recoverable, like the others: the send path counts it and drops the
    /// message rather than aborting, because a publisher that panics goes dark.
    #[error("the {feed} feed does not carry message type {type_id:#04x}")]
    NotCarriedByFeed { feed: &'static str, type_id: u8 },

    /// The message's fields are individually representable and their
    /// combination is one its own specification forbids.
    ///
    /// Refused at the push for the same reason a wrong port role is: a
    /// publisher that emits it produces a message every conformant subscriber
    /// discards, and the effect it meant to have — a level removed, a side
    /// cleared — silently does not happen. Failing here costs one message;
    /// failing on a capture after a deploy costs however long it takes somebody
    /// to notice a book that never changed.
    ///
    /// Recoverable, like the others: the send path counts it and drops the
    /// message rather than aborting, because a publisher that panics goes dark.
    #[error("{message} is malformed: {what}")]
    MalformedMessage {
        message: &'static str,
        what: &'static str,
    },
}
