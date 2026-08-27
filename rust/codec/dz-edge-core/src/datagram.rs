//! The 24-byte datagram header and the builder that packs messages into one
//! UDP payload.

use crate::channel::ChannelSequence;
use crate::constants::{
    DATAGRAM_HEADER_SIZE, FLAG_SNAPSHOT, MAX_DATAGRAM_SIZE, MSG_HEADER_SIZE, SCHEMA_VERSION,
    SUPPORTED_SCHEMA_VERSIONS,
};
use crate::encode_error::EncodeError;
use crate::error::DecodeError;
use crate::message::AppMessage;

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
}

/// Accumulates application messages into one datagram.
///
/// Capacity is `mtu` clamped to at least the datagram header and at most
/// `MAX_DATAGRAM_SIZE`. The clamp is the point: the cap is mandated by every
/// feed spec, so no configuration key and no operator can raise it. A
/// deployment default above the cap is representable, which is why the limit
/// lives here and not in a documentation note.
pub struct DatagramBuilder {
    buf: Vec<u8>,
    magic: u16,
    channel_id: u8,
    sequence_number: u64,
    reset_count: u8,
    capacity: usize,
    msg_count: u8,
}

impl DatagramBuilder {
    #[must_use]
    pub fn new(magic: u16, sequence: ChannelSequence, mtu: u16) -> Self {
        let capacity = (mtu as usize).clamp(DATAGRAM_HEADER_SIZE, MAX_DATAGRAM_SIZE);
        let mut buf = Vec::with_capacity(capacity);
        buf.resize(DATAGRAM_HEADER_SIZE, 0);
        Self {
            buf,
            magic,
            channel_id: sequence.channel_id(),
            sequence_number: sequence.sequence_number(),
            reset_count: sequence.reset_count(),
            capacity,
            msg_count: 0,
        }
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

    /// Append a message with the snapshot flag cleared.
    ///
    /// On `Err` the builder is unchanged, so a caller may finish the current
    /// datagram and retry the same message on a fresh one.
    pub fn push<M: AppMessage>(&mut self, msg: &M) -> Result<(), EncodeError> {
        self.push_with_flags(msg, 0)
    }

    /// Append a message with the snapshot flag set. Correct only on the
    /// `snapshot` port role.
    ///
    /// On `Err` the builder is unchanged, so a caller may finish the current
    /// datagram and retry the same message on a fresh one.
    pub fn push_snapshot<M: AppMessage>(&mut self, msg: &M) -> Result<(), EncodeError> {
        self.push_with_flags(msg, FLAG_SNAPSHOT)
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
        self.buf[0..2].copy_from_slice(&self.magic.to_le_bytes());
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
