//! Walking the application messages inside a received datagram.
//!
//! This is the subscriber-side counterpart to `DatagramBuilder`: it decodes
//! and validates a received datagram and hands back an iterator over the
//! messages it contains.

use crate::constants::{DATAGRAM_HEADER_SIZE, MSG_HEADER_SIZE};
use crate::datagram::DatagramHeader;
use crate::error::DecodeError;

/// A received datagram whose structure has been fully validated.
///
/// `Datagram::decode` performs every structural check up front: the header,
/// the magic, the zero-message rule, and a pre-walk of exactly Message Count
/// message boundaries. Because of that, `messages()` returns a plain
/// iterator that cannot fail. This is deliberate: a market-data subscriber
/// must not apply three of five messages and then discover corruption, and
/// it keeps the caller's loop free of per-item error handling.
pub struct Datagram<'a> {
    header: DatagramHeader,
    /// The validated message region: `buf[DATAGRAM_HEADER_SIZE..datagram_len]`.
    messages: &'a [u8],
}

impl<'a> Datagram<'a> {
    /// Decode and fully validate a datagram believed to belong to the feed
    /// identified by `expected_magic`.
    ///
    /// Magic is the only thing that stops a datagram misrouted from one feed
    /// from being decoded as another, so the caller must state which feed it
    /// believes it is holding: there is no default and no inference. A
    /// mismatch is refused rather than parsed at the wrong layout.
    ///
    /// Checks, in order:
    /// 1. The 24-byte header decodes and its schema version is supported,
    ///    which also validates the declared length bounds (at least the
    ///    header size and does not exceed `buf.len()`).
    /// 2. `magic` matches `expected_magic`.
    /// 3. The header's Message Count is at least 1. The field's range is
    ///    1-255, so a datagram declaring zero messages is malformed rather
    ///    than merely empty.
    /// 4. Exactly Message Count messages, starting at offset 24, each fit
    ///    within the declared datagram length; finding fewer than that is
    ///    rejected as a count mismatch. Bytes left over between the last of
    ///    those messages and the declared length are never inspected and
    ///    never rejected - the reference parser reads exactly Message Count
    ///    messages the same way, and the specification does not forbid
    ///    intra-datagram padding. A `buf` longer than the declared length is
    ///    likewise fine; anything past `datagram_len` is ignored.
    pub fn decode(buf: &'a [u8], expected_magic: u16) -> Result<Self, DecodeError> {
        let header = DatagramHeader::decode(buf)?;
        if header.magic != expected_magic {
            return Err(DecodeError::MagicMismatch {
                expected: expected_magic,
                found: header.magic,
            });
        }
        if header.msg_count == 0 {
            return Err(DecodeError::EmptyDatagram);
        }

        let datagram_len = header.datagram_len as usize;
        // `DatagramHeader::decode` has already guaranteed
        // `DATAGRAM_HEADER_SIZE <= datagram_len <= buf.len()`, with the same two
        // `ShortBuffer` errors. The slice below depends on that, so relaxing
        // those checks there breaks this.
        let messages = &buf[DATAGRAM_HEADER_SIZE..datagram_len];
        let found = validate_messages(messages, header.msg_count as usize)?;
        if found != header.msg_count as usize {
            return Err(DecodeError::MessageCountMismatch {
                declared: header.msg_count,
                found,
            });
        }

        Ok(Self { header, messages })
    }

    /// The decoded and validated datagram header.
    #[must_use]
    pub fn header(&self) -> &DatagramHeader {
        &self.header
    }

    /// An iterator over the application messages inside this datagram, in
    /// wire order.
    #[must_use]
    pub fn messages(&self) -> Messages<'a> {
        Messages {
            messages: self.messages,
            offset: 0,
            remaining_count: self.header.msg_count as usize,
        }
    }
}

/// Walk `messages` from the front, reading up to `msg_count` messages and
/// checking that each one fits, then return how many were actually found
/// (at most `msg_count`).
///
/// This does not walk to the end of `messages`: it stops the moment
/// `msg_count` messages have been read, and it stops early - without error -
/// if the bytes run out first. Either way, `Datagram::decode` is the one
/// that judges the result: a short count becomes `MessageCountMismatch`, and
/// any bytes left over after a full count is reached are treated as
/// ignored padding, matching the reference parser. That split keeps this
/// function from having to fabricate anything it did not actually read.
///
/// Never panics: every slice access is bounds-checked against `remaining`
/// before it is made.
fn validate_messages(messages: &[u8], msg_count: usize) -> Result<usize, DecodeError> {
    let mut offset = 0usize;
    let mut found = 0usize;
    while found < msg_count {
        let remaining = messages.len() - offset;
        if remaining == 0 {
            // Ran out of bytes before reaching the declared count. Reported
            // by the caller comparing `found` against Message Count, so
            // nothing is fabricated here.
            break;
        }
        if remaining < MSG_HEADER_SIZE {
            // Some bytes remain, but not enough to hold even the 4-byte
            // message header, so no Length field can be honestly read here.
            return Err(DecodeError::MessageHeaderTruncated {
                offset: DATAGRAM_HEADER_SIZE + offset,
                remaining,
            });
        }
        let declared = messages[offset + 1];
        if (declared as usize) < MSG_HEADER_SIZE {
            return Err(DecodeError::MessageTooShort {
                offset: DATAGRAM_HEADER_SIZE + offset,
                declared,
            });
        }
        if declared as usize > remaining {
            return Err(DecodeError::MessageOverrunsDatagram {
                offset: DATAGRAM_HEADER_SIZE + offset,
                declared,
                remaining,
            });
        }
        offset += declared as usize;
        found += 1;
    }
    Ok(found)
}

/// One application message inside a datagram.
pub struct MessageRef<'a> {
    pub type_id: u8,
    pub flags: u16,
    /// The complete message, including its own 4-byte header, so it can be
    /// passed straight to a message type's `decode`.
    pub bytes: &'a [u8],
}

/// An iterator over the application messages inside a validated [`Datagram`].
///
/// Unknown and reserved type ids are yielded like any other message, never
/// rejected. The top-of-book specification requires it: "A decoder
/// encountering an unknown type MUST skip the message using its Message
/// Length field and continue parsing [the datagram]." That includes the reserved
/// `0x05`. This iterator only walks message boundaries; deciding what to do
/// with a `type_id` — skip it, dispatch it, reject it — is the caller's job.
/// Do not add a `BadTypeId` or `ReservedTypeId` check here: a subscriber
/// built on this walk would then fail exactly where the specification
/// requires it to skip.
pub struct Messages<'a> {
    messages: &'a [u8],
    offset: usize,
    remaining_count: usize,
}

impl<'a> Iterator for Messages<'a> {
    type Item = MessageRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_count == 0 {
            return None;
        }
        // `Datagram::decode` already validated exactly this many message
        // boundaries in this slice, so these accesses cannot go out of
        // bounds. Stopping on `remaining_count` rather than on
        // `self.offset < self.messages.len()` matters here: bytes past the
        // declared Message Count may still be inside `self.messages` (they
        // are the ignored padding `validate_messages` leaves unexamined),
        // and must not be walked as though they were another message.
        let declared = self.messages[self.offset + 1] as usize;
        let bytes = &self.messages[self.offset..self.offset + declared];
        let type_id = bytes[0];
        let flags = u16::from_le_bytes([bytes[2], bytes[3]]);
        self.offset += declared;
        self.remaining_count -= 1;
        Some(MessageRef {
            type_id,
            flags,
            bytes,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining_count, Some(self.remaining_count))
    }
}

impl ExactSizeIterator for Messages<'_> {}
