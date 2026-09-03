use dz_edge_core::{
    AppMessage, ChannelSequence, DatagramBuilder, DatagramHeader, DecodeError, Feed, PortRole,
    ResetCount,
};
use dz_edge_core::{EncodeError, DATAGRAM_HEADER_SIZE, MAX_DATAGRAM_SIZE, SCHEMA_VERSION};

// Core's tests cannot name `TopOfBook` - that would make core depend on
// dz-edge-tob, which is backwards. A test-local feed stands in for it.
struct TestFeed;
impl Feed for TestFeed {
    const MAGIC: u16 = 0x445A;
    const NAME: &'static str = "test";
    /// A test feed stands in for a specification it does not have, so its table
    /// is every Type ID this file pushes rather than a transcription of
    /// anything. A real feed's is the specification's own — see
    /// [`dz_edge_core::Feed::CARRIES`].
    const CARRIES: &'static [u8] = &[
        0x01, 0x02, 0x03, 0x04, 0x06, 0x07, 0x08, 0x13, 0x14, 0x20, 0x22, 0x40, 0x41, 0x42, 0x7F,
    ];
}

// A feed distinct from `TestFeed`, so `finish` stamping `F::MAGIC` can be
// proven to come from the type parameter rather than a stored field.
struct OtherFeed;
impl Feed for OtherFeed {
    const MAGIC: u16 = 0x1234;
    const NAME: &'static str = "other";
    /// A test feed stands in for a specification it does not have, so its table
    /// is every Type ID this file pushes rather than a transcription of
    /// anything. A real feed's is the specification's own — see
    /// [`dz_edge_core::Feed::CARRIES`].
    const CARRIES: &'static [u8] = &[
        0x01, 0x02, 0x03, 0x04, 0x06, 0x07, 0x08, 0x13, 0x14, 0x20, 0x22, 0x40, 0x41, 0x42, 0x7F,
    ];
}

struct Sixteen;
impl AppMessage for Sixteen {
    const TYPE_ID: u8 = 0x01;
    const SIZE: usize = 16;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Mktdata, PortRole::Snapshot];
    fn encode_into(&self, dst: &mut [u8]) {
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4..].fill(0);
    }

    // Sixteen carries no redundant Channel ID.
    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}

struct SelfFlagged;
impl AppMessage for SelfFlagged {
    const TYPE_ID: u8 = 0x01;
    const SIZE: usize = 16;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Mktdata];
    fn encode_into(&self, dst: &mut [u8]) {
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        // Deliberately sets the snapshot bit itself. The builder must clear it.
        dst[2..4].copy_from_slice(&0x0001u16.to_le_bytes());
        dst[4..].fill(0);
    }

    // SelfFlagged carries no redundant Channel ID.
    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}

struct HeaderOnly;
impl AppMessage for HeaderOnly {
    const TYPE_ID: u8 = 0x01;
    const SIZE: usize = 4;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Mktdata];
    fn encode_into(&self, dst: &mut [u8]) {
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
    }

    // HeaderOnly carries no redundant Channel ID.
    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}

#[test]
fn header_fields_land_at_their_spec_offsets() {
    let channel = ChannelSequence::resume(7, ResetCount(3), 42);
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    b.push(&Sixteen).unwrap();
    let out = b
        .finish(1_700_000_000_000_000_000)
        .expect("datagram has messages");

    assert_eq!(
        &out[0..2],
        &TestFeed::MAGIC.to_le_bytes(),
        "offset 0: Magic"
    );
    assert_eq!(out[2], SCHEMA_VERSION, "offset 2: Schema Version");
    assert_eq!(out[3], 7, "offset 3: Channel ID");
    assert_eq!(
        &out[4..12],
        &42u64.to_le_bytes(),
        "offset 4: Sequence Number"
    );
    assert_eq!(
        &out[12..20],
        &1_700_000_000_000_000_000u64.to_le_bytes(),
        "offset 12: Send Timestamp"
    );
    assert_eq!(out[20], 1, "offset 20: Message Count");
    assert_eq!(out[21], 3, "offset 21: Reset Count");
    // The spec's field table names offset 22 `Frame Length`. The identifier is
    // datagram_len; the wire meaning is the total datagram length.
    assert_eq!(
        &out[22..24],
        &(DATAGRAM_HEADER_SIZE as u16 + 16).to_le_bytes()
    );
    assert_eq!(out.len(), DATAGRAM_HEADER_SIZE + 16);
}

#[test]
fn the_send_timestamp_comes_from_finish_not_construction() {
    // `finish` takes the send timestamp; the builder's constructor has no
    // timestamp parameter left to leak from. This test guards against one
    // being reintroduced and silently winning over `finish`'s argument.
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    b.push(&Sixteen).unwrap();
    let out = b.finish(123_456_789).expect("datagram has messages");

    assert_eq!(
        &out[12..20],
        &123_456_789u64.to_le_bytes(),
        "offset 12: Send Timestamp must come from finish's argument"
    );
}

#[test]
fn an_mtu_above_the_mandated_cap_is_clamped() {
    // 1448 is a plausible deployment default and exceeds the mandated cap.
    let channel = ChannelSequence::new(0, ResetCount(0));
    let b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1448);
    assert_eq!(
        b.remaining(),
        MAX_DATAGRAM_SIZE - DATAGRAM_HEADER_SIZE,
        "capacity must clamp to the mandated maximum, not the requested MTU"
    );
}

#[test]
fn capacity_reports_the_clamped_value() {
    // 9000 is a plausible jumbo-frame MTU and exceeds the mandated cap.
    let channel = ChannelSequence::new(0, ResetCount(0));
    let b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 9000);
    assert_eq!(
        b.capacity(),
        MAX_DATAGRAM_SIZE,
        "capacity() must report what actually took effect, not the requested mtu"
    );
}

#[test]
fn a_finished_datagram_never_exceeds_the_cap() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1448);
    while b.push(&Sixteen).is_ok() {}
    let out = b.finish(0).expect("datagram has messages");
    assert!(
        out.len() <= MAX_DATAGRAM_SIZE,
        "finished datagram {} exceeds {MAX_DATAGRAM_SIZE}",
        out.len()
    );
}

#[test]
fn a_message_that_does_not_fit_is_refused_rather_than_truncated() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    while b.push(&Sixteen).is_ok() {}
    assert!(matches!(
        b.push(&Sixteen),
        Err(EncodeError::DatagramFull { .. })
    ));
}

#[test]
fn message_count_stops_at_255() {
    // Message Count is a u8. A 256th message would wrap it to 0 and every
    // subscriber would mis-parse the rest of the datagram. A 4-byte message
    // makes the cap reachable: (1232 - 24) / 4 = 302 slots, well past 255.
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    let mut pushed = 0usize;
    while b.push(&HeaderOnly).is_ok() {
        pushed += 1;
    }
    assert_eq!(
        pushed, 255,
        "the u8 Message Count must stop the builder at 255"
    );
    assert!(matches!(
        b.push(&HeaderOnly),
        Err(EncodeError::MessageCountExhausted { max: 255 })
    ));
    let out = b.finish(0).expect("datagram has messages");
    assert_eq!(out[20], 255, "offset 20: Message Count");
    assert!(out.len() <= MAX_DATAGRAM_SIZE);
}

#[test]
fn a_failed_push_leaves_the_builder_unchanged() {
    // On Err the builder must be unchanged, so a caller can finish the
    // current datagram and retry the same message on a fresh one. Two
    // builders filled identically, where only one also has an extra failing
    // push attempted on it, must finish to the same bytes.
    let build_full = || {
        let channel = ChannelSequence::new(5, ResetCount(1));
        let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
        while b.push(&Sixteen).is_ok() {}
        b
    };

    let mut with_extra_attempt = build_full();
    assert!(matches!(
        with_extra_attempt.push(&Sixteen),
        Err(EncodeError::DatagramFull { .. })
    ));
    let with_extra_attempt_bytes = with_extra_attempt.finish(999);

    let baseline = build_full();
    let baseline_bytes = baseline.finish(999);

    assert_eq!(
        with_extra_attempt_bytes, baseline_bytes,
        "a failed push must not mutate the builder"
    );
}

#[test]
fn the_snapshot_flag_follows_the_port_role() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut plain = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    plain.push(&Sixteen).unwrap();
    let out = plain.finish(0).expect("datagram has messages");
    let flags = u16::from_le_bytes([out[DATAGRAM_HEADER_SIZE + 2], out[DATAGRAM_HEADER_SIZE + 3]]);
    assert_eq!(
        flags & 0x0001,
        0,
        "mktdata and refdata messages clear bit 0"
    );

    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut snap = DatagramBuilder::<TestFeed>::new(channel, PortRole::Snapshot, 1232);
    snap.push(&Sixteen).unwrap();
    let out = snap.finish(0).expect("datagram has messages");
    let flags = u16::from_le_bytes([out[DATAGRAM_HEADER_SIZE + 2], out[DATAGRAM_HEADER_SIZE + 3]]);
    assert_eq!(flags & 0x0001, 1, "snapshot-port messages set bit 0");

    // A message that sets the snapshot bit itself must still have it cleared
    // by push() on a non-snapshot role: the builder owns Flags, not the message.
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut fights_back = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    fights_back.push(&SelfFlagged).unwrap();
    let out = fights_back.finish(0).expect("datagram has messages");
    let flags = u16::from_le_bytes([out[DATAGRAM_HEADER_SIZE + 2], out[DATAGRAM_HEADER_SIZE + 3]]);
    assert_eq!(
        flags & 0x0001,
        0,
        "push() must clear a self-set snapshot bit"
    );
}

#[test]
fn decode_rejects_a_schema_version_it_does_not_implement() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    b.push(&Sixteen).unwrap();
    let mut out = b.finish(0).expect("datagram has messages");
    out[2] = 2; // the generation that never reached the wire
    assert_eq!(
        DatagramHeader::decode(&out),
        Err(DecodeError::UnsupportedSchema(2))
    );
}

#[test]
fn decode_round_trips_a_built_datagram() {
    let channel = ChannelSequence::resume(9, ResetCount(2), 1234);
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    b.push(&Sixteen).unwrap();
    let out = b.finish(5678).expect("datagram has messages");
    let h = DatagramHeader::decode(&out).unwrap();
    assert_eq!(h.channel_id, 9);
    assert_eq!(h.sequence_number, 1234);
    assert_eq!(h.send_timestamp_ns, 5678);
    assert_eq!(h.reset_count, 2);
    assert_eq!(h.msg_count, 1);
    assert_eq!(h.datagram_len as usize, out.len());
}

#[test]
fn decode_refuses_a_short_buffer() {
    assert_eq!(
        DatagramHeader::decode(&[0u8; 10]),
        Err(DecodeError::ShortBuffer { need: 24, got: 10 })
    );
}

#[test]
fn decode_rejects_a_datagram_len_that_claims_more_than_the_buffer_holds() {
    // 24-byte buffer, but the Frame Length field claims the full 1232-byte cap:
    // a truncated datagram that still decodes clean if datagram_len is trusted.
    let mut buf = [0u8; DATAGRAM_HEADER_SIZE];
    buf[2] = SCHEMA_VERSION;
    buf[22..24].copy_from_slice(&(MAX_DATAGRAM_SIZE as u16).to_le_bytes());
    assert!(matches!(
        DatagramHeader::decode(&buf),
        Err(DecodeError::ShortBuffer { need, got })
            if need == MAX_DATAGRAM_SIZE && got == DATAGRAM_HEADER_SIZE
    ));
}

#[test]
fn decode_rejects_a_datagram_len_below_the_header_size() {
    let mut buf = [0u8; DATAGRAM_HEADER_SIZE];
    buf[2] = SCHEMA_VERSION;
    buf[22..24].copy_from_slice(&20u16.to_le_bytes());
    assert!(matches!(
        DatagramHeader::decode(&buf),
        Err(DecodeError::DeclaredLengthOutOfRange { declared, min, max })
            if declared == 20 && min == DATAGRAM_HEADER_SIZE && max == MAX_DATAGRAM_SIZE
    ));
}

#[test]
fn decode_rejects_a_datagram_len_above_the_mandated_cap() {
    // 40000 is representable in the u16 Frame Length field but far beyond the
    // 1,232-byte mandated cap: an untrusted peer's header, not anything
    // DatagramBuilder can produce (its capacity is clamped).
    let mut buf = [0u8; DATAGRAM_HEADER_SIZE];
    buf[2] = SCHEMA_VERSION;
    buf[22..24].copy_from_slice(&40000u16.to_le_bytes());
    assert!(matches!(
        DatagramHeader::decode(&buf),
        Err(DecodeError::DeclaredLengthOutOfRange { declared, min, max })
            if declared == 40000 && min == DATAGRAM_HEADER_SIZE && max == MAX_DATAGRAM_SIZE
    ));
}

#[test]
fn decode_accepts_a_zero_message_count() {
    // Otherwise well-formed 24-byte header: valid schema, datagram_len equal
    // to the header size, and Message Count left at 0. Whether a zero count
    // makes the datagram malformed is a datagram-level rule, not a header
    // one, so the header alone still decodes - a subscriber doing
    // sequence-gap or reset accounting needs `sequence_number`, `channel_id`,
    // and `send_timestamp_ns` even from a malformed datagram.
    let mut buf = [0u8; DATAGRAM_HEADER_SIZE];
    buf[2] = SCHEMA_VERSION;
    buf[22..24].copy_from_slice(&(DATAGRAM_HEADER_SIZE as u16).to_le_bytes());
    let h = DatagramHeader::decode(&buf).expect("header alone does not judge message count");
    assert_eq!(h.msg_count, 0);
}

struct TooSmallForMessageHeader;
impl AppMessage for TooSmallForMessageHeader {
    const TYPE_ID: u8 = 0x01;
    const SIZE: usize = 2;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Mktdata];
    fn encode_into(&self, dst: &mut [u8]) {
        dst.fill(0);
    }

    // TooSmallForMessageHeader carries no redundant Channel ID.
    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}

#[test]
#[should_panic(expected = "AppMessage::SIZE must include the 4-byte message header")]
fn push_panics_when_size_excludes_the_message_header() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    let _ = b.push(&TooSmallForMessageHeader);
}

struct TooLargeForLengthField;
impl AppMessage for TooLargeForLengthField {
    const TYPE_ID: u8 = 0x01;
    const SIZE: usize = 300;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Mktdata];
    fn encode_into(&self, dst: &mut [u8]) {
        dst.fill(0);
    }

    // TooLargeForLengthField carries no redundant Channel ID.
    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}

#[test]
#[should_panic(expected = "AppMessage::SIZE must fit the u8 message-header Length field")]
fn push_panics_when_size_cannot_fit_the_u8_length_field() {
    // mtu of 1232 gives the builder plenty of capacity; the assert must fire
    // before any capacity check, which it does since it is the first statement.
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    let _ = b.push(&TooLargeForLengthField);
}

#[test]
fn a_tiny_mtu_clamps_capacity_up_to_the_header() {
    // Capacity clamps UP to the header as well as down to the cap, so a
    // degenerate mtu cannot make buf longer than capacity.
    let channel = ChannelSequence::new(0, ResetCount(0));
    let b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 4);
    assert_eq!(b.remaining(), 0);
    // Nothing was pushed, so finish() must not hand back an emittable
    // datagram: the Message Count range is 1-255, and a 0-message datagram
    // is exactly what every conformant subscriber discards.
    assert!(b.finish(0).is_none());
}

#[test]
fn a_heartbeat_on_mktdata_succeeds() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    b.push(&dz_edge_core::Heartbeat {
        channel_id: 0,
        timestamp_ns: 0,
    })
    .unwrap();
}

#[test]
fn a_message_pushed_on_the_wrong_port_role_is_a_countable_error() {
    // `SelfFlagged` lists only `Mktdata`; pushing it on a `Refdata` builder
    // must be recoverable - a publisher counts and drops it rather than the
    // process aborting.
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Refdata, 1232);
    let err = b.push(&SelfFlagged).unwrap_err();
    assert_eq!(
        err,
        EncodeError::WrongPortRole {
            message: core::any::type_name::<SelfFlagged>(),
            role: "refdata",
        }
    );
}

#[test]
fn finish_stamps_the_feeds_magic_not_a_stored_field() {
    // `OtherFeed`'s magic differs from `TestFeed`'s, so a byte match at
    // offset 0 proves the stamp comes from the type parameter, not from a
    // field the constructor happened to be given.
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<OtherFeed>::new(channel, PortRole::Mktdata, 1232);
    b.push(&Sixteen).unwrap();
    let out = b.finish(0).expect("datagram has messages");
    assert_eq!(
        &out[0..2],
        &OtherFeed::MAGIC.to_le_bytes(),
        "offset 0: Magic"
    );
}

#[test]
fn port_role_returns_what_was_passed() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Refdata, 1232);
    assert_eq!(b.port_role(), PortRole::Refdata);
}
