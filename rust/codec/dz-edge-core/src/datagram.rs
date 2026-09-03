//! The 24-byte datagram header and the builder that packs messages into one
//! UDP payload.

use core::marker::PhantomData;

use crate::channel::ChannelSequence;
use crate::constants::{
    DATAGRAM_HEADER_SIZE, FLAG_SNAPSHOT, MAX_DATAGRAM_SIZE, MSG_HEADER_SIZE, SCHEMA_VERSION,
    SUPPORTED_SCHEMA_VERSIONS,
};
use crate::encode_error::EncodeError;
use crate::error::DecodeError;
use crate::feed::Feed;
use crate::message::AppMessage;
use crate::port_role::PortRole;

/// The decoded datagram header.
///
/// Field names follow the spec's table. The field at offset 22 is named
/// `Frame Length` there; the identifier here is `datagram_len`, because the
/// glossary requires `datagram` for our own traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatagramHeader {
    pub magic: u16,
    pub schema_version: u8,
    pub channel_id: u8,
    pub sequence_number: u64,
    pub send_timestamp_ns: u64,
    pub msg_count: u8,
    /// Decoded straight from a byte offset, not a positional constructor
    /// argument next to another `u8`, so it carries no transposition risk;
    /// left as a bare `u8` rather than wrapped in `ResetCount`.
    pub reset_count: u8,
    pub datagram_len: u16,
}

impl DatagramHeader {
    /// Decode a header from the front of `buf`.
    ///
    /// Validates the buffer length, the schema version, and the declared
    /// datagram length's range. Rejects any schema version this build does
    /// not implement, per the spec's "a subscriber MUST discard [datagrams]
    /// whose version it does not implement".
    ///
    /// This deliberately does not judge two things:
    ///
    /// - `Magic`, because only the caller knows which feed it expects. A
    ///   caller must compare `header.magic` itself to reject a datagram
    ///   misrouted from another feed.
    ///   [`Datagram::decode`](crate::walk::Datagram::decode) is the entry
    ///   point for received traffic, precisely because it takes the expected
    ///   magic as a required argument and performs that comparison for the
    ///   caller.
    /// - The message count, because whether a zero count makes the datagram
    ///   malformed is a property of the datagram, not the header. A caller
    ///   doing sequence-gap or reset accounting on a malformed datagram still
    ///   needs `sequence_number`, `channel_id`, and `send_timestamp_ns` from
    ///   here; that rule is enforced at the datagram level instead.
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < DATAGRAM_HEADER_SIZE {
            return Err(DecodeError::ShortBuffer {
                need: DATAGRAM_HEADER_SIZE,
                got: buf.len(),
            });
        }
        let schema_version = buf[2];
        if !SUPPORTED_SCHEMA_VERSIONS.contains(&schema_version) {
            return Err(DecodeError::UnsupportedSchema(schema_version));
        }
        let datagram_len = u16::from_le_bytes([buf[22], buf[23]]) as usize;
        if !(DATAGRAM_HEADER_SIZE..=MAX_DATAGRAM_SIZE).contains(&datagram_len) {
            return Err(DecodeError::DeclaredLengthOutOfRange {
                declared: datagram_len as u16,
                min: DATAGRAM_HEADER_SIZE,
                max: MAX_DATAGRAM_SIZE,
            });
        }
        if datagram_len > buf.len() {
            return Err(DecodeError::ShortBuffer {
                need: datagram_len,
                got: buf.len(),
            });
        }
        Ok(Self {
            magic: u16::from_le_bytes([buf[0], buf[1]]),
            schema_version,
            channel_id: buf[3],
            sequence_number: u64::from_le_bytes(
                buf[4..12]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            send_timestamp_ns: u64::from_le_bytes(
                buf[12..20]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            msg_count: buf[20],
            reset_count: buf[21],
            datagram_len: datagram_len as u16,
        })
    }

    /// Read the header without judging anything but the buffer's length.
    ///
    /// [`decode`](Self::decode) refuses an unsupported schema version and a
    /// declared length outside the mandated range, which is correct for a
    /// consumer: the spec says a subscriber must discard a datagram whose
    /// version it does not implement. It is wrong for anything whose job is to
    /// *count* those datagrams. A health tier is required to report magic and
    /// schema version by value rather than judged, and to check the declared
    /// length against the cap — and through `decode` both of those datagrams
    /// are simply undecodable, so the tier learns nothing about exactly the
    /// traffic most worth knowing about.
    ///
    /// The returned header therefore carries **no validation whatever** beyond
    /// its own presence. Nothing here may be treated as a decoded header:
    /// `schema_version` may name a version this build cannot parse,
    /// `datagram_len` may be absurd, and `magic` may belong to another feed.
    /// [`schema_is_supported`](Self::schema_is_supported) and
    /// [`declared_len_is_in_range`](Self::declared_len_is_in_range) are the two
    /// judgements `decode` makes, exposed so that a caller counting them states
    /// the same rule rather than reinventing it.
    ///
    /// This does not walk messages and does not look past the 24th byte.
    pub fn peek(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < DATAGRAM_HEADER_SIZE {
            return Err(DecodeError::ShortBuffer {
                need: DATAGRAM_HEADER_SIZE,
                got: buf.len(),
            });
        }
        Ok(Self {
            magic: u16::from_le_bytes([buf[0], buf[1]]),
            schema_version: buf[2],
            channel_id: buf[3],
            sequence_number: u64::from_le_bytes(
                buf[4..12]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            send_timestamp_ns: u64::from_le_bytes(
                buf[12..20]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            msg_count: buf[20],
            reset_count: buf[21],
            datagram_len: u16::from_le_bytes([buf[22], buf[23]]),
        })
    }

    /// Whether this build implements the schema version in the header.
    ///
    /// A [`peek`](Self::peek)ed header may say no; a [`decode`](Self::decode)d
    /// one always says yes.
    #[must_use]
    pub fn schema_is_supported(&self) -> bool {
        SUPPORTED_SCHEMA_VERSIONS.contains(&self.schema_version)
    }

    /// Whether the declared datagram length is within the mandated range.
    ///
    /// Below the header size or above the cap is a publisher violation worth
    /// counting, which is why it is a question and not a refusal.
    #[must_use]
    pub fn declared_len_is_in_range(&self) -> bool {
        let declared = self.datagram_len as usize;
        (DATAGRAM_HEADER_SIZE..=MAX_DATAGRAM_SIZE).contains(&declared)
    }
}

/// Accumulates application messages into one datagram.
///
/// Capacity is `mtu` clamped to at least the datagram header and at most
/// `MAX_DATAGRAM_SIZE`. The clamp is the point: the cap is mandated by every
/// feed spec, so no configuration key and no operator can raise it. A
/// deployment default above the cap is representable, which is why the limit
/// lives here and not in a documentation note.
pub struct DatagramBuilder<F: Feed> {
    buf: Vec<u8>,
    port_role: PortRole,
    channel_id: u8,
    sequence_number: u64,
    reset_count: u8,
    capacity: usize,
    msg_count: u8,
    _feed: PhantomData<F>,
}

impl<F: Feed> DatagramBuilder<F> {
    #[must_use]
    pub fn new(sequence: ChannelSequence, port_role: PortRole, mtu: u16) -> Self {
        let capacity = (mtu as usize).clamp(DATAGRAM_HEADER_SIZE, MAX_DATAGRAM_SIZE);
        let mut buf = Vec::with_capacity(capacity);
        buf.resize(DATAGRAM_HEADER_SIZE, 0);
        Self {
            buf,
            port_role,
            channel_id: sequence.channel_id(),
            sequence_number: sequence.sequence_number(),
            reset_count: sequence.reset_count().get(),
            capacity,
            msg_count: 0,
            _feed: PhantomData,
        }
    }

    /// The port role this builder was constructed for.
    #[must_use]
    pub const fn port_role(&self) -> PortRole {
        self.port_role
    }

    /// Bytes still available for messages.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.capacity.saturating_sub(self.buf.len())
    }

    /// The clamped capacity this builder was constructed with: `mtu`, clamped
    /// to at least the datagram header and at most `MAX_DATAGRAM_SIZE`.
    ///
    /// An operator who configured a larger value than the mandated cap can log
    /// what actually took effect, without reproducing the header arithmetic
    /// themselves.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Whether the datagram holds no messages yet. This is not "no bytes": the
    /// underlying buffer always holds at least the 24-byte header, even when
    /// empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.msg_count == 0
    }

    /// Append a message, with the snapshot flag set or cleared according to
    /// this builder's port role: set when the role is `Snapshot`, cleared
    /// otherwise. The builder owns this choice - a caller cannot get it wrong
    /// by calling the wrong method, because there is no wrong method.
    ///
    /// On `Err` the builder is unchanged, so a caller may finish the current
    /// datagram and retry the same message on a fresh one.
    pub fn push<M: AppMessage>(&mut self, msg: &M) -> Result<(), EncodeError> {
        // First of all, because it is the broadest refusal: a message this feed
        // does not define is not made carriable by a correct port role, a
        // valid body or a bigger datagram. The magic would have been right,
        // which is exactly what makes it worth checking here — nothing further
        // down the send path can tell.
        if !F::carries(M::TYPE_ID) {
            return Err(EncodeError::NotCarriedByFeed {
                feed: F::NAME,
                type_id: M::TYPE_ID,
            });
        }
        // Before the port role and before the capacity check: a message that
        // may not be sent at all is not made sendable by a bigger datagram.
        msg.validate()?;
        let flags = if self.port_role == PortRole::Snapshot {
            FLAG_SNAPSHOT
        } else {
            0
        };
        self.push_with_flags(msg, flags)
    }

    fn push_with_flags<M: AppMessage>(&mut self, msg: &M, flags: u16) -> Result<(), EncodeError> {
        // `SIZE` includes the 4-byte message header, so anything smaller is a broken
        // `AppMessage` impl rather than a runtime condition. `M::SIZE` is an associated
        // const, so for any concrete type this folds away at compile time.
        assert!(
            M::SIZE >= MSG_HEADER_SIZE,
            "AppMessage::SIZE must include the 4-byte message header"
        );
        // The message header's Length field is a u8, so a larger message cannot be
        // represented on the wire at all. Truncating would write a Length that
        // frames the next message in the wrong place.
        assert!(
            M::SIZE <= u8::MAX as usize,
            "AppMessage::SIZE must fit the u8 message-header Length field"
        );
        // A message not documented for this builder's port role is a spec
        // violation, but a recoverable one: the send path counts it and drops
        // the message rather than aborting, because a publisher that panics
        // goes dark.
        if !M::PORT_ROLES.contains(&self.port_role) {
            return Err(EncodeError::WrongPortRole {
                message: core::any::type_name::<M>(),
                role: self.port_role.as_str(),
            });
        }
        // Message Count is a u8; a 256th message would wrap it to 0 and every
        // subscriber would mis-parse the rest of the datagram.
        if self.msg_count == u8::MAX {
            return Err(EncodeError::MessageCountExhausted { max: u8::MAX });
        }
        if M::SIZE > self.remaining() {
            return Err(EncodeError::DatagramFull {
                attempted: self.buf.len() + M::SIZE,
                capacity: self.capacity,
            });
        }
        let start = self.buf.len();
        self.buf.resize(start + M::SIZE, 0);
        msg.encode_into(&mut self.buf[start..start + M::SIZE]);
        // The builder owns Flags so a caller cannot set the snapshot bit on a
        // mktdata message by accident.
        self.buf[start + 2..start + 4].copy_from_slice(&flags.to_le_bytes());
        // The builder owns the message header, exactly as it owns Flags: a message's
        // own bytes cannot disagree with the associated consts the builder framed it by.
        self.buf[start] = M::TYPE_ID;
        self.buf[start + 1] = M::SIZE as u8;
        // The builder owns a message's redundant Channel ID copy too, for the
        // same reason: a message's own bytes cannot disagree with the header
        // that frames it.
        M::stamp_channel_id(&mut self.buf[start..start + M::SIZE], self.channel_id);
        self.msg_count += 1;
        Ok(())
    }

    /// Stamp the header and return the datagram.
    ///
    /// An empty datagram is not emittable: the header's Message Count field
    /// has range 1-255, so a tick with nothing pushed cannot be represented as
    /// a valid datagram. Rather than hand back the 24-byte header with
    /// `msg_count = 0` that every conformant subscriber discards, `finish`
    /// returns `None`. `None` is a normal outcome here, not an error - that is
    /// why this returns `Option` rather than `Result`.
    ///
    /// `send_timestamp_ns` is the instant the datagram left the host. Read the
    /// clock as late as possible, immediately before transmitting: this is the
    /// value a latency measurement is built on, and a datagram carrying several
    /// messages can take a while to pack.
    #[must_use]
    pub fn finish(mut self, send_timestamp_ns: u64) -> Option<Vec<u8>> {
        if self.msg_count == 0 {
            return None;
        }
        let len = self.buf.len() as u16;
        self.buf[0..2].copy_from_slice(&F::MAGIC.to_le_bytes());
        self.buf[2] = SCHEMA_VERSION;
        self.buf[3] = self.channel_id;
        self.buf[4..12].copy_from_slice(&self.sequence_number.to_le_bytes());
        self.buf[12..20].copy_from_slice(&send_timestamp_ns.to_le_bytes());
        self.buf[20] = self.msg_count;
        self.buf[21] = self.reset_count;
        self.buf[22..24].copy_from_slice(&len.to_le_bytes());
        Some(self.buf)
    }
}
