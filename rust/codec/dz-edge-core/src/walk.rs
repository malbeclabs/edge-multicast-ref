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
/// the magic, the declared datagram length, and a full pre-walk of every
/// message boundary. Because of that, `messages()` returns a plain iterator
/// that cannot fail. This is deliberate: a market-data subscriber must not
/// apply three of five messages and then discover corruption, and it keeps
/// the caller's loop free of per-item error handling.
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
    /// 1. The 24-byte header decodes and its schema version is supported.
    /// 2. `magic` matches `expected_magic`.
    /// 3. The declared datagram length is at least the header size.
    /// 4. The declared datagram length does not exceed `buf.len()` (a `buf`
    ///    longer than the declared length is fine; trailing bytes are
    ///    ignored).
    /// 5. Every message from offset 24 to the declared length fits, and the
    ///    number found agrees with the header's Message Count.
    pub fn decode(buf: &'a [u8], expected_magic: u16) -> Result<Self, DecodeError> {
        let header = DatagramHeader::decode(buf)?;
        if header.magic != expected_magic {
            return Err(DecodeError::MagicMismatch {
                expected: expected_magic,
                found: header.magic,
            });
        }

        let datagram_len = header.datagram_len as usize;
        if datagram_len < DATAGRAM_HEADER_SIZE {
            return Err(DecodeError::ShortBuffer {
                need: DATAGRAM_HEADER_SIZE,
                got: datagram_len,
            });
        }
        if datagram_len > buf.len() {
            return Err(DecodeError::ShortBuffer {
                need: datagram_len,
                got: buf.len(),
            });
        }

        let messages = &buf[DATAGRAM_HEADER_SIZE..datagram_len];
        let found = validate_messages(messages)?;
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

/// Walk `messages` from front to back, checking that every message fits
/// within it, and return how many were found. Never panics: every slice
/// access is bounds-checked against `remaining` before it is made.
fn validate_messages(messages: &[u8]) -> Result<usize, DecodeError> {
    let mut offset = 0usize;
    let mut found = 0usize;
    while offset < messages.len() {
        let remaining = messages.len() - offset;
        if remaining < MSG_HEADER_SIZE {
            // Fewer bytes remain than even the smallest possible message
            // header, so whatever length byte might be present cannot be
            // trusted as a real declaration.
            let declared = messages.get(offset + 1).copied().unwrap_or(0);
            return Err(DecodeError::MessageOverrunsDatagram {
                offset: DATAGRAM_HEADER_SIZE + offset,
                declared,
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
/// Length field and continue parsing the frame." That includes the reserved
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
        if self.offset >= self.messages.len() {
            return None;
        }
        // `Datagram::decode` already validated every message boundary in
        // this slice, so these accesses cannot go out of bounds.
        let declared = self.messages[self.offset + 1] as usize;
        let bytes = &self.messages[self.offset..self.offset + declared];
        let type_id = bytes[0];
        let flags = u16::from_le_bytes([bytes[2], bytes[3]]);
        self.offset += declared;
        self.remaining_count = self.remaining_count.saturating_sub(1);
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
