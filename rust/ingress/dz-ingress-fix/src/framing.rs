//! Tag-value framing, in both directions, and the two refusals.
//!
//! Nothing here connects, waits or holds a session. It is the encoding alone:
//! the field separator, the standard header this transport owns, the declared
//! length, the checksum, and a decoder that survives a message split across two
//! reads and two messages in one read.
//!
//! # The tags this transport owns, and why the list is a refusal
//!
//! A venue composes the *body* of a logon or a subscription — its identity, its
//! signature, the instruments it wants — and this crate composes everything
//! around it. So six tags belong to the transport and to nothing else:
//! [`TAG_BEGIN_STRING`], [`TAG_BODY_LENGTH`], [`TAG_MSG_SEQ_NUM`],
//! [`TAG_SENDING_TIME`], [`TAG_RESET_SEQ_NUM_FLAG`] and [`TAG_CHECKSUM`]. A
//! body carrying one of them is refused by [`Body::parse`] rather than
//! overridden, because the two ways of resolving it are both wrong: taking the
//! caller's value puts a sequence the session does not believe on the wire, and
//! silently dropping it discards a field somebody wrote on purpose.

use core::fmt;

/// The field separator, and the only byte that terminates a field.
pub const SOH: u8 = 0x01;

/// `BeginString`. The protocol version, which is this crate's to state.
pub const TAG_BEGIN_STRING: u32 = 8;
/// `BodyLength`, computed over the span this crate's own encoder measures.
pub const TAG_BODY_LENGTH: u32 = 9;
/// `MsgSeqNum`, on the session's own outbound numbering.
pub const TAG_MSG_SEQ_NUM: u32 = 34;
/// `MsgType`, which the caller states as the first field of its body.
pub const TAG_MSG_TYPE: u32 = 35;
/// `SendingTime`, in the format [`crate::timestamp::sending_time`] writes.
pub const TAG_SENDING_TIME: u32 = 52;
/// `TestReqID`, echoed back in the heartbeat that answers a test request.
pub const TAG_TEST_REQ_ID: u32 = 112;
/// `HeartBtInt`, the cadence the session runs at — read out of the logon the
/// adapter wrote and never from a configuration key.
pub const TAG_HEART_BT_INT: u32 = 108;
/// `ResetSeqNumFlag`, which this transport always states on a logon because it
/// always resets, which is `session`'s own decision and not an operator's.
pub const TAG_RESET_SEQ_NUM_FLAG: u32 = 141;
/// `Text`, which carries a venue's own words on a logout or a reject.
pub const TAG_TEXT: u32 = 58;
/// `SessionRejectReason`, the coded half of a session-level reject.
pub const TAG_SESSION_REJECT_REASON: u32 = 373;
/// `RefTagID`, which says which field a session-level reject was about.
pub const TAG_REF_TAG_ID: u32 = 371;
/// `CheckSum`, always the last field, always three digits.
pub const TAG_CHECKSUM: u32 = 10;

/// The `MsgType` values the session layer itself understands.
///
/// Every other value is an application message, which this transport hands to
/// the adapter as a payload without reading a single field of it. That split is
/// the whole of what this crate knows about a venue's market data.
pub mod msg_type {
    /// `Heartbeat`.
    pub const HEARTBEAT: &str = "0";
    /// `TestRequest`.
    pub const TEST_REQUEST: &str = "1";
    /// `ResendRequest`. Answered by ending the session: this transport has no
    /// resend path, deliberately.
    pub const RESEND_REQUEST: &str = "2";
    /// `Reject`: the session-level one, which is the failure only this layer
    /// can see.
    pub const REJECT: &str = "3";
    /// `SequenceReset`.
    pub const SEQUENCE_RESET: &str = "4";
    /// `Logout`.
    pub const LOGOUT: &str = "5";
    /// `Logon`.
    pub const LOGON: &str = "A";

    /// The seven of them, which is what makes "is this a session message?" a
    /// total question rather than a chain of comparisons that can be one short.
    pub const SESSION: [&str; 7] = [
        HEARTBEAT,
        TEST_REQUEST,
        RESEND_REQUEST,
        REJECT,
        SEQUENCE_RESET,
        LOGOUT,
        LOGON,
    ];

    /// Whether the session layer owns this message type.
    #[must_use]
    pub fn is_session(msg_type: &str) -> bool {
        SESSION.contains(&msg_type)
    }
}

/// The protocol version this crate composes.
///
/// Stated as a constant rather than taken from a key, because it is not an
/// operator's decision: the session layer's own messages are version-sensitive
/// and this crate composes them at one version. A venue on a different version
/// carries the difference in *its adapter's* tags, which is where a version
/// difference belongs — the tag sets are what move between versions, and the
/// session layer's field semantics are what do not.
pub const BEGIN_STRING: &str = "FIX.4.4";

/// The `CheckSum` field's own width, including its tag, its `=`, its three
/// digits and its separator: `10=123\x01`.
///
/// Load-bearing in the decoder rather than decorative: the declared length
/// covers everything up to this field, so a complete message is the declared
/// span plus exactly this many bytes.
pub const CHECKSUM_FIELD_LEN: usize = 7;

/// Why a message could not be read off the stream.
///
/// **Three of these end the session and are not skipped**, which is the
/// decision this type exists to carry. A message whose declared length or
/// checksum does not hold, or whose header never ends, means the byte stream
/// and this decoder no longer agree about where one message ends, and the
/// numbering is agreed on that stream: carrying on reads the next message
/// against a sequence that has moved for a reason nobody recorded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FramingError {
    /// The stream did not begin with `8=`.
    ///
    /// Its own case rather than a length mismatch: a stream that never starts
    /// with a `BeginString` is a socket connected to something that is not this
    /// protocol, which is an endpoint to correct rather than a corrupt message.
    #[error(
        "the stream does not begin with `{TAG_BEGIN_STRING}=`: this is not a tag-value session"
    )]
    NotAMessage,

    /// A field with no `=`, an unparseable tag, or a `BodyLength` that is not a
    /// number.
    #[error("a field could not be read: {detail}")]
    Malformed { detail: String },

    /// The declared length did not land on the checksum field.
    ///
    /// Ends the session. What it means is that the length says one thing and
    /// the bytes say another, so nothing after it can be located.
    #[error(
        "the declared `{TAG_BODY_LENGTH}={declared}` does not end at a \
         `{TAG_CHECKSUM}=` field; the numbering on this stream can no longer be trusted"
    )]
    LengthMismatch { declared: usize },

    /// The checksum did not hold.
    ///
    /// Ends the session, for the same reason as [`LengthMismatch`](Self::LengthMismatch).
    #[error(
        "the checksum is {stated} and the bytes sum to {computed}; the numbering \
         on this stream can no longer be trusted"
    )]
    ChecksumMismatch { stated: u8, computed: u8 },

    /// The far side declared a message larger than this transport will
    /// assemble.
    ///
    /// The size is chosen by whoever is on the other end of the socket and the
    /// buffer is ours, so there is a ceiling and it is stated.
    #[error("a message declaring {declared} body bytes exceeds the {limit}-byte ceiling")]
    TooLarge { declared: usize, limit: usize },

    /// The bytes ran past the ceiling with no separator to end the header.
    ///
    /// The other half of the bound, and the case
    /// [`TooLarge`](Self::TooLarge) cannot cover: a peer that writes
    /// `8=FIX.4.4\x01` and then `9=` followed by megabytes with no separator
    /// has declared nothing, so there is no length to compare against a limit —
    /// and every byte of it stays buffered waiting for a separator that is not
    /// coming. A venue bug, a truncated frame and a garbled stream all arrive
    /// this way, and the reason the ceiling exists is that the far side chooses
    /// the size and the buffer is ours.
    ///
    /// **Its ceiling is [`MAX_HEADER_BYTES`] and not the body ceiling.** A
    /// buffer still short of the header's own two separators holds nothing but
    /// header bytes — no `35=`, no body byte, because those begin past the
    /// second separator and by then a declared length bounds the wait — so the
    /// body ceiling has nothing to say about how much of this to hold, and
    /// adding it would let a peer that connects and streams junk buffer
    /// megabytes per connection for a header of some tens of bytes.
    ///
    /// Ends the session, for the reason
    /// [`LengthMismatch`](Self::LengthMismatch) does: nothing on this stream
    /// can be located any more.
    #[error(
        "{buffered} bytes arrived with no separator to end the header, past the \
         {limit}-byte ceiling; nothing on this stream can be located any more"
    )]
    HeaderNotTerminated { buffered: usize, limit: usize },
}

/// Why a body the caller composed cannot be framed.
///
/// Every one of these is a mistake in venue code — a literal, or a builder that
/// writes a tag this transport owns — so each is the same on the next attempt
/// and none of them is worth retrying under a backoff.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BodyError {
    /// The body was empty.
    #[error("nothing was written: a body must state its `{TAG_MSG_TYPE}=` and at least that")]
    Empty,

    /// The body's first field was not `MsgType`.
    ///
    /// Required rather than searched for, because the protocol mandates
    /// `MsgType` as the third field of every message and this crate writes the
    /// first two. A body whose first field is something else is one whose
    /// author had a different framing in mind.
    #[error("the first field of a body must be `{TAG_MSG_TYPE}=`, and this one is `{found}=`")]
    MsgTypeNotFirst { found: u32 },

    /// A field this transport owns.
    #[error("the body states `{tag}=`, which is this transport's own field: {why}")]
    OwnedTag { tag: u32, why: &'static str },

    /// A field with no `=`, an unterminated field, or a tag that is not a
    /// number.
    #[error("a field could not be read: {detail}")]
    Malformed { detail: String },

    /// A `MsgType` whose value is not UTF-8, which no message type is.
    #[error("the `{TAG_MSG_TYPE}=` value is not text")]
    MsgTypeNotText,
}

/// One field of a message, as it appeared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Field<'a> {
    /// The tag.
    pub tag: u32,
    /// The value, between the `=` and the separator.
    pub value: &'a [u8],
}

/// Every field of a slice of tag-value fields, in order.
///
/// Stops at the first field it cannot read, which is sound here because every
/// caller has already had the whole message's framing checked: a decoded
/// message reached [`Decoder::take`] and a body reached [`Body::parse`].
#[derive(Clone)]
pub struct Fields<'a> {
    rest: &'a [u8],
}

/// Prints how much is left to read, and not what is in it.
///
/// [`Message::fields`] hands one of these out over a whole message and
/// [`Body::field`] runs one over a whole body, so a derived implementation
/// prints every field a logon states — as a list of byte values, which is still
/// a venue's signature in a log line. [`Field`] keeps its derived
/// implementation, because one field the caller named by tag is the value a
/// diagnostic is *about*; the iterator over all of them is not.
impl fmt::Debug for Fields<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Fields")
            .field("remaining_bytes", &self.rest.len())
            .finish_non_exhaustive()
    }
}

impl<'a> Iterator for Fields<'a> {
    type Item = Field<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rest.is_empty() {
            return None;
        }
        let end = self.rest.iter().position(|byte| *byte == SOH)?;
        let (field, rest) = self.rest.split_at(end);
        self.rest = &rest[1..];
        let equals = field.iter().position(|byte| *byte == b'=')?;
        let tag = core::str::from_utf8(&field[..equals]).ok()?.parse().ok()?;
        Some(Field {
            tag,
            value: &field[equals + 1..],
        })
    }
}

/// A complete message, framing already checked.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Message<'a> {
    bytes: &'a [u8],
}

impl<'a> Message<'a> {
    /// A message over bytes whose framing has been checked.
    ///
    /// Constructed by [`Decoder::take`] and by this crate's own encoder. Not a
    /// parser: the checks live where the bytes come from, so that a `Message`
    /// in hand is one whose length and checksum held.
    #[must_use]
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// The whole message, separators and all.
    #[must_use]
    pub const fn bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// Every field, in the order the message states them.
    #[must_use]
    pub const fn fields(&self) -> Fields<'a> {
        Fields { rest: self.bytes }
    }

    /// The first value stated for `tag`.
    ///
    /// First and not last, which matters for a repeating group: a venue's
    /// market-data message states `269` once per entry, and the session layer
    /// never reads one — this is for the header fields and the session
    /// messages' own bodies, where a tag appears once.
    #[must_use]
    pub fn field(&self, tag: u32) -> Option<&'a [u8]> {
        self.fields()
            .find(|field| field.tag == tag)
            .map(|field| field.value)
    }

    /// The `MsgType`, as text.
    #[must_use]
    pub fn msg_type(&self) -> Option<&'a str> {
        core::str::from_utf8(self.field(TAG_MSG_TYPE)?).ok()
    }

    /// Whether the session layer owns this message.
    #[must_use]
    pub fn is_session(&self) -> bool {
        self.msg_type().is_some_and(msg_type::is_session)
    }

    /// A field read as an unsigned number.
    #[must_use]
    pub fn field_u64(&self, tag: u32) -> Option<u64> {
        core::str::from_utf8(self.field(tag)?).ok()?.parse().ok()
    }
}

/// Prints the message type, the sequence and how many bytes there are — and
/// none of them.
///
/// A logon's fields are the one place a credential appears, and a `Message` is
/// held over one on both sides: the framed copy this crate wrote, and the
/// venue's answer echoing what it was sent. A `{:?}` that rendered the bytes
/// would therefore put a venue's signature in a log line, which is the standard
/// [`Session`](crate::Session) and [`FixInput`](crate::FixInput) hand-write
/// their own implementations to hold, and the reason a session error's detail is
/// a stated list of tags rather than the message.
///
/// [`rendered`] is still public for whoever has decided to look at the bytes.
/// Having to ask is the difference between reading them and logging them.
impl fmt::Debug for Message<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Message")
            .field("msg_type", &self.msg_type())
            .field("sequence", &self.field_u64(TAG_MSG_SEQ_NUM))
            .field("bytes", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

/// A message's bytes with the separator shown as `|`, because a `\x01` in a log
/// line is invisible.
///
/// **For the fields this crate itself composes around a body, and never for a
/// body's own.** The decoder's details use it on the three it reads — the
/// checksum's digits, the declared length's digits, and the header prefix that
/// failed to be `{TAG_BODY_LENGTH}=` — each of them a fixed-shape field written
/// and parsed here, none of them able to hold a field somebody else wrote, and
/// without them a garbled stream is undiagnosable.
///
/// A logon's fields are the one place a credential appears, so nothing that can
/// see a body's fields renders them: [`Body::parse`] refuses by field index,
/// offset and length, and [`Body`] and [`Message`] hand-write their
/// [`Debug`](fmt::Debug) to say what they are and how much of them there is.
/// This function stays public for whoever has decided to look at the bytes —
/// having to ask is the difference between reading them and logging them.
///
/// A function rather than `Display` on [`Message`], because it is used on
/// partial and refused bytes too, where there is no message to display.
#[must_use]
pub fn rendered(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).replace(SOH as char, "|")
}

/// A body the caller composed: its `MsgType`, and its remaining fields.
///
/// What the adapter writes through the boundary's `UpstreamSink` — a logon's
/// identity and signature, or a subscription's instruments — with the header
/// this transport owns absent, because framing it is this transport's job.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Body<'a> {
    msg_type: &'a str,
    rest: &'a [u8],
}

/// Prints the message type and how many body bytes there are — and none of
/// them.
///
/// This is the type a venue's identity and its signature arrive in: a derived
/// implementation prints `rest` verbatim, so the first `{:?}` on a logon body
/// puts both in a startup log. Written out for the reason
/// [`Session`](crate::Session) and [`FixInput`](crate::FixInput) are, and saying
/// the same two things they say — what it is, and how much of it there is.
///
/// The message type is stated because it is this crate's own field: a body's
/// first field is the one thing about it that is not the caller's secret, and it
/// is what a diagnostic is asking for.
impl fmt::Debug for Body<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Body")
            .field("msg_type", &self.msg_type)
            .field("body_bytes", &self.rest.len())
            .finish_non_exhaustive()
    }
}

impl<'a> Body<'a> {
    /// Read a body, refusing every tag this transport owns.
    ///
    /// # Errors
    ///
    /// [`BodyError`], every variant of which is a mistake in the code that
    /// composed the body rather than a condition that clears.
    pub fn parse(bytes: &'a [u8]) -> Result<Self, BodyError> {
        if bytes.is_empty() {
            return Err(BodyError::Empty);
        }
        // **Every detail below locates the fault and none of them carries the
        // bytes.** This is the one type a venue's identity and its signature
        // arrive in, and a malformed body is refused on the startup and the
        // reconnect path, where an error's detail becomes a log line — so a
        // missing separator or a tag that is not a number must not be the way a
        // logon reaches one. What a detail says instead is which field, how far
        // into the body it begins, how long it is, and what was expected there,
        // which is everything the venue code that composed it needs. Fields are
        // counted from one, the `MsgType` being the first.
        if bytes.last() != Some(&SOH) {
            return Err(BodyError::Malformed {
                detail: format!(
                    "the last of {} bytes is not a separator, so the last field never ends",
                    bytes.len()
                ),
            });
        }
        let first_end = bytes
            .iter()
            .position(|byte| *byte == SOH)
            .expect("the last byte is a separator");
        let first = &bytes[..first_end];
        let equals =
            first
                .iter()
                .position(|byte| *byte == b'=')
                .ok_or_else(|| BodyError::Malformed {
                    detail: format!(
                        "field 1 is {first_end} bytes with no `=`, so it states no tag"
                    ),
                })?;
        let tag: u32 = core::str::from_utf8(&first[..equals])
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| BodyError::Malformed {
                detail: format!(
                    "field 1 does not begin with a tag: the {equals} bytes before its `=` are \
                     not a number"
                ),
            })?;
        if tag != TAG_MSG_TYPE {
            return Err(BodyError::MsgTypeNotFirst { found: tag });
        }
        let msg_type =
            core::str::from_utf8(&first[equals + 1..]).map_err(|_| BodyError::MsgTypeNotText)?;
        if msg_type.is_empty() {
            return Err(BodyError::Malformed {
                detail: format!("`{TAG_MSG_TYPE}=` states no message type"),
            });
        }

        // Every remaining field is read here rather than lazily, so that a body
        // stating one of this transport's tags is refused before anything
        // reaches a socket. A refusal after a partial write would be the one
        // failure shape this crate must not have.
        let rest = &bytes[first_end + 1..];
        let mut cursor = rest;
        let mut index = 2usize;
        let mut offset = first_end + 1;
        while !cursor.is_empty() {
            let remaining = cursor.len();
            let end = cursor.iter().position(|byte| *byte == SOH).ok_or_else(|| {
                // Not reachable while the terminator check above stands: the
                // last byte is a separator, so every tail of the body has one.
                // Kept as a refusal rather than an `expect`, because what makes
                // it unreachable is a check twenty lines away.
                BodyError::Malformed {
                    detail: format!(
                        "field {index}, at byte {offset}, is {remaining} bytes with no separator \
                         to end it"
                    ),
                }
            })?;
            let field = &cursor[..end];
            let equals = field.iter().position(|byte| *byte == b'=').ok_or_else(|| {
                BodyError::Malformed {
                    detail: format!(
                        "field {index}, at byte {offset}, is {end} bytes with no `=`, so it \
                         states no tag"
                    ),
                }
            })?;
            let tag: u32 = core::str::from_utf8(&field[..equals])
                .ok()
                .and_then(|text| text.parse().ok())
                .ok_or_else(|| BodyError::Malformed {
                    detail: format!(
                        "field {index}, at byte {offset}, does not begin with a tag: the \
                         {equals} bytes before its `=` are not a number"
                    ),
                })?;
            if let Some(why) = owned_tag_reason(tag) {
                return Err(BodyError::OwnedTag { tag, why });
            }
            cursor = &cursor[end + 1..];
            offset += end + 1;
            index += 1;
        }
        Ok(Self { msg_type, rest })
    }

    /// The message type this body states.
    #[must_use]
    pub const fn msg_type(&self) -> &'a str {
        self.msg_type
    }

    /// Every field after the `MsgType`, as the caller wrote them.
    #[must_use]
    pub const fn rest(&self) -> &'a [u8] {
        self.rest
    }

    /// A field of the body, by tag.
    #[must_use]
    pub fn field(&self, tag: u32) -> Option<&'a [u8]> {
        Fields { rest: self.rest }
            .find(|field| field.tag == tag)
            .map(|field| field.value)
    }

    /// A field of the body, read as an unsigned number.
    #[must_use]
    pub fn field_u64(&self, tag: u32) -> Option<u64> {
        core::str::from_utf8(self.field(tag)?).ok()?.parse().ok()
    }
}

/// Why a tag is this transport's and not a caller's.
///
/// A total function over the owned set rather than a `matches!`, so that a tag
/// added to the set without a reason does not compile.
const fn owned_tag_reason(tag: u32) -> Option<&'static str> {
    match tag {
        TAG_BEGIN_STRING => Some("the protocol version is this crate's to state"),
        TAG_BODY_LENGTH => Some("the declared length is computed over the framed message"),
        TAG_MSG_SEQ_NUM => Some("the outbound sequence belongs to the session"),
        TAG_SENDING_TIME => Some("the sending time is stamped when the message is framed"),
        TAG_RESET_SEQ_NUM_FLAG => {
            Some("this transport always resets at logon and always states the flag")
        }
        TAG_CHECKSUM => Some("the checksum is computed over the framed message"),
        _ => None,
    }
}

/// Frame one body: the header this transport owns, the body, the checksum.
///
/// `reset_sequence` states [`TAG_RESET_SEQ_NUM_FLAG`], and is what a logon
/// carries.
///
/// `out` is cleared first, so a caller reusing one buffer cannot append one
/// message to the tail of another.
///
/// # What of the field order the protocol fixes, and what this crate chose
///
/// Four positions are the protocol's own: `8`, `9` and `35` lead, in that
/// order, and `10` is last. What sits between them is written here in an order
/// **this crate chose** — the sequence, the sending time, the reset flag when
/// there is one, then the body as the caller wrote it.
///
/// The standard header sequences a venue's `SenderCompID` and `TargetCompID`
/// ahead of `MsgSeqNum`, and both of those are the *adapter's* fields: they say
/// who the two sides are, which is the same thing the logon body carries and
/// the reason this crate holds no key for either. Writing them before `34`
/// would mean this crate knowing them, from a configuration key or an injected
/// value, for an identity the body already states. Engines validate the four
/// fixed positions and read the rest by tag, so what that order costs is
/// nothing and what the alternative costs is a venue's identity moving into
/// this repository.
///
/// # `BodyLength` is not the message length
///
/// It counts the bytes from the field *after* `9=…\x01` through the separator
/// before `10=`, and getting that span wrong is the classic mistake: a length
/// measured over the whole message parses locally and is refused by every
/// engine on the other side, because the length is how the far side finds the
/// end of the message.
pub fn frame(
    out: &mut Vec<u8>,
    body: &Body<'_>,
    seq: u64,
    sending_time: &str,
    reset_sequence: bool,
) {
    out.clear();
    // The two fields before the measured span. `9=` is written with the value
    // once it is known, so the span is built first and the prefix after it.
    let mut measured = Vec::with_capacity(body.rest().len() + 64);
    push_field(&mut measured, TAG_MSG_TYPE, body.msg_type().as_bytes());
    push_field(&mut measured, TAG_MSG_SEQ_NUM, seq.to_string().as_bytes());
    push_field(&mut measured, TAG_SENDING_TIME, sending_time.as_bytes());
    if reset_sequence {
        push_field(&mut measured, TAG_RESET_SEQ_NUM_FLAG, b"Y");
    }
    measured.extend_from_slice(body.rest());

    push_field(out, TAG_BEGIN_STRING, BEGIN_STRING.as_bytes());
    push_field(out, TAG_BODY_LENGTH, measured.len().to_string().as_bytes());
    out.extend_from_slice(&measured);
    let sum = checksum(out);
    push_field(out, TAG_CHECKSUM, format!("{sum:03}").as_bytes());
}

/// One `tag=value\x01`.
fn push_field(out: &mut Vec<u8>, tag: u32, value: &[u8]) {
    out.extend_from_slice(tag.to_string().as_bytes());
    out.push(b'=');
    out.extend_from_slice(value);
    out.push(SOH);
}

/// The sum of every byte, modulo 256.
///
/// Over the bytes before the checksum field, which is every byte of the message
/// when this is called from [`frame`] and the declared span plus its two
/// leading fields when it is called from the decoder.
#[must_use]
pub fn checksum(bytes: &[u8]) -> u8 {
    bytes.iter().fold(0u8, |sum, byte| sum.wrapping_add(*byte))
}

/// The largest message this transport will assemble, in body bytes.
///
/// A venue's snapshot message is the large one and eight megabytes is well past
/// any of them. It exists because the far side chooses the size and the buffer
/// is ours.
///
/// **It bounds a declared length, and that is the whole of what it bounds.** A
/// ceiling checked against a parsed length alone would leave the case that
/// grows a buffer without any bound: a peer that writes `9=` and then megabytes
/// with no separator has declared nothing to check. That case is refused too,
/// and the value it is refused at is [`MAX_HEADER_BYTES`] rather than anything
/// derived from this one — a buffer still hunting for the header's own
/// separators holds only header bytes, so this ceiling is not a statement about
/// it. See [`FramingError::HeaderNotTerminated`].
pub const DEFAULT_MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

/// How many bytes of header may precede the measured span.
///
/// `8=` with its version and `9=` with its digits, which is some tens of bytes
/// at most; stated with room, because what this bounds is the search for the
/// header's two separators and not a field width to be exact about.
///
/// It is the whole ceiling for that search rather than a margin added to a body
/// ceiling, because the buffer being searched holds nothing but these bytes.
/// See [`FramingError::HeaderNotTerminated`].
pub const MAX_HEADER_BYTES: usize = 128;

/// A reader that turns a byte stream into whole messages.
///
/// Holds whatever the last read left over, so a message split across two reads
/// and two messages in one read are both ordinary. Those two are the cases a
/// hand-written reader gets wrong, and they are the reason this is a buffer
/// with a `take` rather than a function over a slice.
#[derive(Debug)]
pub struct Decoder {
    buf: Vec<u8>,
    max_body_bytes: usize,
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder {
    /// A decoder with the default ceiling. See [`DEFAULT_MAX_BODY_BYTES`].
    #[must_use]
    pub const fn new() -> Self {
        Self {
            buf: Vec::new(),
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
        }
    }

    /// A decoder with a stated ceiling on a declared body length.
    #[must_use]
    pub const fn with_max_body_bytes(max_body_bytes: usize) -> Self {
        Self {
            buf: Vec::new(),
            max_body_bytes,
        }
    }

    /// Add what a read returned.
    pub fn feed(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Forget everything buffered.
    ///
    /// Called on every connect, because a partial message from a connection
    /// that has ended is bytes belonging to a numbering that no longer exists.
    pub fn clear(&mut self) {
        self.buf.clear();
    }

    /// How many bytes are held, waiting for the rest of their message.
    #[must_use]
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// Move the next complete message into `out`, or say there is not one yet.
    ///
    /// `Ok(false)` is the ordinary case of a partial read: the bytes stay
    /// buffered and the next `feed` may complete them. `out` is cleared
    /// whenever a message is produced and left alone otherwise.
    ///
    /// # Errors
    ///
    /// [`FramingError`]. Every variant **ends the session** rather than
    /// skipping the message: the numbering is agreed on this stream, so a
    /// message whose length or checksum does not hold leaves the next message's
    /// sequence unaccounted for.
    pub fn take(&mut self, out: &mut Vec<u8>) -> Result<bool, FramingError> {
        let Some(total) = self.complete_len()? else {
            return Ok(false);
        };
        let stated = self.buf[total - 4..total - 1].to_vec();
        let computed = checksum(&self.buf[..total - CHECKSUM_FIELD_LEN]);
        let stated: u8 = core::str::from_utf8(&stated)
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| FramingError::Malformed {
                detail: format!(
                    "the `{TAG_CHECKSUM}=` value is not three digits: `{}`",
                    rendered(&stated)
                ),
            })?;
        if stated != computed {
            return Err(FramingError::ChecksumMismatch { stated, computed });
        }
        out.clear();
        out.extend_from_slice(&self.buf[..total]);
        self.buf.drain(..total);
        Ok(true)
    }

    /// The length of the message at the front of the buffer, when all of it is
    /// there.
    ///
    /// The declared length is what locates the end, which is the whole reason
    /// the field exists — and the reason a length that does not land on a
    /// `10=` field is a refusal rather than a value to search past.
    fn complete_len(&self) -> Result<Option<usize>, FramingError> {
        if self.buf.is_empty() {
            return Ok(None);
        }
        let prefix = format!("{TAG_BEGIN_STRING}=");
        if !self.buf.starts_with(prefix.as_bytes()) {
            // Only decidable once there are enough bytes to disagree with.
            if self.buf.len() < prefix.len() && prefix.as_bytes().starts_with(&self.buf) {
                return Ok(None);
            }
            return Err(FramingError::NotAMessage);
        }
        let Some(first_end) = self.buf.iter().position(|byte| *byte == SOH) else {
            return self.awaiting_a_separator();
        };
        let after_begin = first_end + 1;
        let length_prefix = format!("{TAG_BODY_LENGTH}=");
        let tail = &self.buf[after_begin..];
        if !tail.starts_with(length_prefix.as_bytes()) {
            if tail.len() < length_prefix.len() && length_prefix.as_bytes().starts_with(tail) {
                return Ok(None);
            }
            return Err(FramingError::Malformed {
                detail: format!(
                    "`{TAG_BEGIN_STRING}=` is not followed by `{TAG_BODY_LENGTH}=`: `{}`",
                    rendered(&self.buf[..self.buf.len().min(32)])
                ),
            });
        }
        let Some(second_end) = tail.iter().position(|byte| *byte == SOH) else {
            return self.awaiting_a_separator();
        };
        let declared: usize = core::str::from_utf8(&tail[length_prefix.len()..second_end])
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| FramingError::Malformed {
                detail: format!(
                    "`{TAG_BODY_LENGTH}=` is not a number: `{}`",
                    rendered(&tail[length_prefix.len()..second_end])
                ),
            })?;
        if declared > self.max_body_bytes {
            return Err(FramingError::TooLarge {
                declared,
                limit: self.max_body_bytes,
            });
        }
        let measured_at = after_begin + second_end + 1;
        let total = measured_at + declared + CHECKSUM_FIELD_LEN;
        if self.buf.len() < total {
            return Ok(None);
        }
        // The declared span has to end exactly where the checksum field begins.
        // A length that lands anywhere else is a length the far side and this
        // decoder disagree about, which is the end of the session.
        let checksum_prefix = format!("{TAG_CHECKSUM}=");
        if !self.buf[measured_at + declared..].starts_with(checksum_prefix.as_bytes())
            || self.buf[total - 1] != SOH
        {
            return Err(FramingError::LengthMismatch { declared });
        }
        Ok(Some(total))
    }

    /// "Not yet" — unless the buffer has already run past anything a header
    /// could be, in which case the separator is not coming.
    ///
    /// **The half of the ceiling a declared length cannot carry.** Every other
    /// wait in [`complete_len`](Self::complete_len) is bounded by a value that
    /// has been parsed and checked: once `9=…` is read, `declared` is compared
    /// against the ceiling and only that many more bytes are ever held. Before
    /// it, there is no number — so a peer sending `9=` and then megabytes with
    /// no separator is a buffer that grows for as long as it keeps sending, and
    /// the process is killed for memory rather than told what happened.
    ///
    /// Refused rather than searched past: the two separators this is waiting for
    /// are the header's own, so bytes that do not contain them are not a
    /// message this decoder has lost its place in — they are a stream it never
    /// had one on.
    ///
    /// **Bounded at [`MAX_HEADER_BYTES`], and not at that plus the body
    /// ceiling.** Both callers reach here with a buffer that is header and
    /// nothing else, which is what makes the smaller bound the honest one. The
    /// first is reached with no separator anywhere in the buffer, so every byte
    /// held is inside `8=`'s value; the second with the first separator found
    /// and none after it, so every byte held is `8=FIX.4.4\x01` plus what has
    /// arrived of `9=`'s digits. Neither can be holding a `35=` or a body byte:
    /// those begin past the second separator, and once it is there `declared`
    /// is parsed, checked against the body ceiling, and bounds the wait by
    /// itself. So the body ceiling adds nothing here except room — and in
    /// production it is eight megabytes of it, which a peer that opens a
    /// connection and streams junk with no `\x01` collects once per connection
    /// before being told anything.
    fn awaiting_a_separator(&self) -> Result<Option<usize>, FramingError> {
        if self.buf.len() > MAX_HEADER_BYTES {
            return Err(FramingError::HeaderNotTerminated {
                buffered: self.buf.len(),
                limit: MAX_HEADER_BYTES,
            });
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_owned_tag_set_and_the_reasons_for_it_are_one_list() {
        // A tag in the set with no reason would be refused with an empty
        // explanation, and a reason for a tag not in the set would never be
        // reached. Both are caught by asking the question the other way round.
        for tag in [
            TAG_BEGIN_STRING,
            TAG_BODY_LENGTH,
            TAG_MSG_SEQ_NUM,
            TAG_SENDING_TIME,
            TAG_RESET_SEQ_NUM_FLAG,
            TAG_CHECKSUM,
        ] {
            let why = owned_tag_reason(tag).unwrap_or_else(|| panic!("{tag} has no reason"));
            assert!(!why.is_empty(), "{tag}");
        }
        for tag in [TAG_MSG_TYPE, TAG_HEART_BT_INT, TAG_TEST_REQ_ID, TAG_TEXT] {
            assert!(
                owned_tag_reason(tag).is_none(),
                "{tag} is the caller's and must not be refused"
            );
        }
    }

    #[test]
    fn the_session_message_types_are_distinct() {
        let mut types = msg_type::SESSION.to_vec();
        types.sort_unstable();
        let count = types.len();
        types.dedup();
        assert_eq!(
            types.len(),
            count,
            "two session message types share a token"
        );
    }
}
