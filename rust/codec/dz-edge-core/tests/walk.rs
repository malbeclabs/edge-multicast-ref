use dz_edge_core::{
    AppMessage, ChannelSequence, Datagram, DatagramBuilder, DecodeError, EndOfSession, Feed,
    Heartbeat, PortRole, ResetCount, DATAGRAM_HEADER_SIZE, SCHEMA_VERSION,
};

// Core's tests cannot name `TopOfBook` - that would make core depend on
// dz-edge-tob, which is backwards. A test-local feed stands in for it.
struct TestFeed;
impl Feed for TestFeed {
    const MAGIC: u16 = 0x445A;
    const NAME: &'static str = "test";
}

const TEST_MAGIC: u16 = TestFeed::MAGIC;

/// A message declared eligible for the snapshot port role, so a
/// `Snapshot`-role builder can be exercised without changing `Heartbeat`'s
/// real, spec-declared roles (`mktdata` only).
struct SnapshotEligible;
impl AppMessage for SnapshotEligible {
    const TYPE_ID: u8 = 0x01;
    const SIZE: usize = 16;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Snapshot];
    fn encode_into(&self, dst: &mut [u8]) {
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4..].fill(0);
    }

    // SnapshotEligible carries no redundant Channel ID.
    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}

/// Hand-craft a 24-byte header, independent of `DatagramBuilder`, so
/// malformations the builder cannot produce (a bad message count, a
/// truncated message) can still be expressed.
fn header_bytes(magic: u16, schema_version: u8, msg_count: u8, datagram_len: u16) -> Vec<u8> {
    let mut h = vec![0u8; DATAGRAM_HEADER_SIZE];
    h[0..2].copy_from_slice(&magic.to_le_bytes());
    h[2] = schema_version;
    h[20] = msg_count;
    h[22..24].copy_from_slice(&datagram_len.to_le_bytes());
    h
}

#[test]
fn walks_heartbeats_and_end_of_session_in_order() {
    let channel = ChannelSequence::resume(3, ResetCount(0), 10);
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    let hb1 = Heartbeat {
        channel_id: 3,
        timestamp_ns: 111,
    };
    let hb2 = Heartbeat {
        channel_id: 3,
        timestamp_ns: 222,
    };
    let eos = EndOfSession { timestamp_ns: 333 };
    b.push(&hb1).unwrap();
    b.push(&hb2).unwrap();
    b.push(&eos).unwrap();
    let out = b.finish(999).expect("datagram has messages");

    let dg = Datagram::decode(&out, TEST_MAGIC).unwrap();
    let msgs: Vec<_> = dg.messages().collect();
    assert_eq!(msgs.len(), 3);

    assert_eq!(msgs[0].type_id, Heartbeat::TYPE_ID);
    assert_eq!(msgs[0].bytes.len(), Heartbeat::SIZE);
    // The builder stamps Channel ID from the datagram, so the decoded value is
    // 3 (the channel this datagram was built for), overwriting whatever
    // encode_into wrote from the message's own channel_id field.
    assert_eq!(
        Heartbeat::decode(msgs[0].bytes).unwrap(),
        Heartbeat {
            channel_id: 3,
            timestamp_ns: hb1.timestamp_ns
        }
    );

    assert_eq!(msgs[1].type_id, Heartbeat::TYPE_ID);
    assert_eq!(msgs[1].bytes.len(), Heartbeat::SIZE);
    assert_eq!(
        Heartbeat::decode(msgs[1].bytes).unwrap(),
        Heartbeat {
            channel_id: 3,
            timestamp_ns: hb2.timestamp_ns
        }
    );

    assert_eq!(msgs[2].type_id, EndOfSession::TYPE_ID);
    assert_eq!(msgs[2].bytes.len(), EndOfSession::SIZE);
    assert_eq!(EndOfSession::decode(msgs[2].bytes).unwrap(), eos);
}

#[test]
fn flags_reflect_the_builders_port_role() {
    let hb = Heartbeat {
        channel_id: 0,
        timestamp_ns: 0,
    };

    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut plain = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    plain.push(&hb).unwrap();
    let out = plain.finish(0).expect("datagram has messages");
    let dg = Datagram::decode(&out, TEST_MAGIC).unwrap();
    let msg = dg.messages().next().unwrap();
    assert_eq!(
        msg.flags, 0,
        "a mktdata-role builder must clear the snapshot bit"
    );

    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut snap = DatagramBuilder::<TestFeed>::new(channel, PortRole::Snapshot, 1232);
    snap.push(&SnapshotEligible).unwrap();
    let out = snap.finish(0).expect("datagram has messages");
    let dg = Datagram::decode(&out, TEST_MAGIC).unwrap();
    let msg = dg.messages().next().unwrap();
    assert_eq!(
        msg.flags, 1,
        "a snapshot-role builder must set the snapshot bit"
    );
}

#[test]
fn an_empty_builder_yields_no_datagram() {
    // Message Count is 1-255, so a tick with nothing to send produces no
    // datagram at all rather than one every conformant subscriber discards.
    let channel = ChannelSequence::new(0, ResetCount(0));
    let b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    assert!(b.finish(0).is_none());
}

#[test]
fn a_hand_built_zero_message_datagram_is_refused() {
    // The decode half of the same 1-255 range: a header claiming zero messages
    // is malformed, not empty.
    let mut buf = vec![0u8; DATAGRAM_HEADER_SIZE];
    buf[0..2].copy_from_slice(&TEST_MAGIC.to_le_bytes());
    buf[2] = SCHEMA_VERSION; // schema version
    buf[20] = 0; // Message Count
    buf[22..24].copy_from_slice(&(DATAGRAM_HEADER_SIZE as u16).to_le_bytes());
    assert!(matches!(
        Datagram::decode(&buf, TEST_MAGIC),
        Err(DecodeError::EmptyDatagram)
    ));
}

#[test]
fn a_magic_mismatch_is_rejected() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    b.push(&Heartbeat {
        channel_id: 0,
        timestamp_ns: 0,
    })
    .unwrap();
    let out = b.finish(0).expect("datagram has messages");

    assert!(matches!(
        Datagram::decode(&out, 0x1234),
        Err(DecodeError::MagicMismatch {
            expected: 0x1234,
            found: 0x445A
        })
    ));
}

#[test]
fn a_truncated_buffer_is_rejected() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    b.push(&Heartbeat {
        channel_id: 0,
        timestamp_ns: 0,
    })
    .unwrap();
    let out = b.finish(0).expect("datagram has messages");
    let full_len = out.len();
    // Chop bytes off the end without correcting the header's declared length,
    // so datagram_len ends up larger than the buffer actually holds.
    let truncated = &out[..full_len - 4];

    assert!(matches!(
        Datagram::decode(truncated, TEST_MAGIC),
        Err(DecodeError::ShortBuffer { need, got })
            if need == full_len && got == full_len - 4
    ));
}

#[test]
fn trailing_bytes_past_datagram_len_are_ignored() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    b.push(&Heartbeat {
        channel_id: 0,
        timestamp_ns: 42,
    })
    .unwrap();
    let mut out = b.finish(0).expect("datagram has messages");
    // Garbage past datagram_len; a real datagram, or a buffer reused across
    // receives, could easily carry stale trailing bytes like this.
    out.extend_from_slice(&[0xAA; 16]);

    let dg = Datagram::decode(&out, TEST_MAGIC).unwrap();
    let msgs: Vec<_> = dg.messages().collect();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].type_id, Heartbeat::TYPE_ID);
}

#[test]
fn an_unsupported_schema_version_is_rejected() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    b.push(&Heartbeat {
        channel_id: 0,
        timestamp_ns: 0,
    })
    .unwrap();
    let mut out = b.finish(0).expect("datagram has messages");
    out[2] = 2; // the generation that never reached the wire

    assert!(matches!(
        Datagram::decode(&out, TEST_MAGIC),
        Err(DecodeError::UnsupportedSchema(2))
    ));
}

#[test]
fn a_declared_length_of_zero_is_message_too_short() {
    let mut buf = header_bytes(
        TEST_MAGIC,
        SCHEMA_VERSION,
        1,
        (DATAGRAM_HEADER_SIZE + 4) as u16,
    );
    buf.extend_from_slice(&[0x7F, 0, 0, 0]);

    assert!(matches!(
        Datagram::decode(&buf, TEST_MAGIC),
        Err(DecodeError::MessageTooShort {
            offset: DATAGRAM_HEADER_SIZE,
            declared: 0
        })
    ));
}

#[test]
fn a_declared_length_of_three_is_message_too_short() {
    let mut buf = header_bytes(
        TEST_MAGIC,
        SCHEMA_VERSION,
        1,
        (DATAGRAM_HEADER_SIZE + 4) as u16,
    );
    buf.extend_from_slice(&[0x7F, 3, 0, 0]);

    assert!(matches!(
        Datagram::decode(&buf, TEST_MAGIC),
        Err(DecodeError::MessageTooShort {
            offset: DATAGRAM_HEADER_SIZE,
            declared: 3
        })
    ));
}

#[test]
fn a_declared_length_past_the_datagram_end_overruns() {
    // datagram_len covers only the 4-byte message header, but the message
    // declares a total length of 16.
    let mut buf = header_bytes(
        TEST_MAGIC,
        SCHEMA_VERSION,
        1,
        (DATAGRAM_HEADER_SIZE + 4) as u16,
    );
    buf.extend_from_slice(&[0x7F, 16, 0, 0]);

    assert!(matches!(
        Datagram::decode(&buf, TEST_MAGIC),
        Err(DecodeError::MessageOverrunsDatagram {
            offset: DATAGRAM_HEADER_SIZE,
            declared: 16,
            remaining: 4
        })
    ));
}

#[test]
fn header_claims_more_messages_than_are_present() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    b.push(&Heartbeat {
        channel_id: 0,
        timestamp_ns: 0,
    })
    .unwrap();
    let mut out = b.finish(0).expect("datagram has messages");
    out[20] = 2; // claims two messages though only one is present

    assert!(matches!(
        Datagram::decode(&out, TEST_MAGIC),
        Err(DecodeError::MessageCountMismatch {
            declared: 2,
            found: 1
        })
    ));
}

#[test]
fn header_claims_fewer_messages_than_are_present_and_the_rest_is_ignored() {
    // Message Count governs the walk, not the declared datagram length: a
    // second, otherwise well-formed message sitting after the declared
    // count is exactly as unreachable as any other filler byte would be.
    // The reference parser reads only MsgCount messages and has no way to
    // notice, let alone reject, what looks like an extra message after that.
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    b.push(&Heartbeat {
        channel_id: 0,
        timestamp_ns: 0,
    })
    .unwrap();
    b.push(&Heartbeat {
        channel_id: 0,
        timestamp_ns: 0,
    })
    .unwrap();
    let mut out = b.finish(0).expect("datagram has messages");
    out[20] = 1; // claims one message though two are present

    let dg = Datagram::decode(&out, TEST_MAGIC).unwrap();
    let msgs: Vec<_> = dg.messages().collect();
    assert_eq!(msgs.len(), 1);
}

#[test]
fn a_few_filler_bytes_inside_the_declared_length_are_ignored() {
    // The specification does not forbid intra-datagram padding, and the Go
    // reference parser (`topofbook_wire.go`'s `decodeTopOfBookFrame`) reads
    // exactly `MsgCount` messages and ignores whatever remains inside the
    // declared length. A publisher that pads must not have its traffic
    // dropped here while the reference parser accepts it.
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    b.push(&Heartbeat {
        channel_id: 0,
        timestamp_ns: 7,
    })
    .unwrap();
    let mut out = b.finish(0).expect("datagram has messages");
    // Filler *inside* the declared length, unlike
    // `trailing_bytes_past_datagram_len_are_ignored`, which appends bytes
    // the header's declared length does not cover at all.
    out.extend_from_slice(&[0xEE; 5]);
    let new_len = out.len() as u16;
    out[22..24].copy_from_slice(&new_len.to_le_bytes());

    let dg = Datagram::decode(&out, TEST_MAGIC).unwrap();
    let msgs: Vec<_> = dg.messages().collect();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].type_id, Heartbeat::TYPE_ID);
}

#[test]
fn a_truncated_message_header_is_refused_without_inventing_a_length() {
    // Only 2 bytes remain where a message is still required - not enough to
    // hold the 4-byte message header, so no Length field can be read at all.
    // This must not be confused with a genuine zero-length declaration
    // (`MessageTooShort`), which does read a Length field off the wire.
    let mut buf = header_bytes(
        TEST_MAGIC,
        SCHEMA_VERSION,
        1,
        (DATAGRAM_HEADER_SIZE + 2) as u16,
    );
    buf.extend_from_slice(&[0x7F, 0x00]);

    assert!(matches!(
        Datagram::decode(&buf, TEST_MAGIC),
        Err(DecodeError::MessageHeaderTruncated {
            offset: DATAGRAM_HEADER_SIZE,
            remaining: 2
        })
    ));
}

#[test]
fn a_declared_length_beyond_the_mandated_cap_is_rejected() {
    // `DeclaredLengthOutOfRange` now lives in `DatagramHeader::decode`;
    // confirm `Datagram::decode` still surfaces it for a declared length far
    // beyond the mandated cap, not just a merely-too-small one.
    let buf = header_bytes(TEST_MAGIC, SCHEMA_VERSION, 1, 4104);

    assert!(matches!(
        Datagram::decode(&buf, TEST_MAGIC),
        Err(DecodeError::DeclaredLengthOutOfRange { declared: 4104, .. })
    ));
}

#[test]
fn an_unknown_type_id_is_yielded_not_rejected() {
    let mut buf = header_bytes(
        TEST_MAGIC,
        SCHEMA_VERSION,
        1,
        (DATAGRAM_HEADER_SIZE + 16) as u16,
    );
    let mut msg = vec![0u8; 16];
    msg[0] = 0x7F; // a type id this build does not recognise
    msg[1] = 16;
    buf.extend_from_slice(&msg);

    let dg = Datagram::decode(&buf, TEST_MAGIC).unwrap();
    let msgs: Vec<_> = dg.messages().collect();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].type_id, 0x7F);
    assert_eq!(msgs[0].bytes.len(), 16);
}

#[test]
fn the_builder_stamps_heartbeats_channel_id_from_the_datagram() {
    // A Heartbeat claiming channel 9 inside a datagram built for channel 3
    // must encode with 3 at the message's Channel ID offset and decode as 3:
    // the builder owns this redundant copy, not the caller.
    let hb = Heartbeat {
        channel_id: 9,
        timestamp_ns: 555,
    };
    let channel = ChannelSequence::new(3, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    b.push(&hb).unwrap();
    let out = b.finish(0).expect("datagram has messages");

    let msg = &out[DATAGRAM_HEADER_SIZE..];
    assert_eq!(
        msg[4], 3,
        "message offset 4: Channel ID stamped by the builder"
    );

    let decoded = Heartbeat::decode(msg).unwrap();
    assert_eq!(decoded.channel_id, 3);
}

#[test]
fn the_reserved_type_id_is_yielded_not_rejected() {
    let mut buf = header_bytes(
        TEST_MAGIC,
        SCHEMA_VERSION,
        1,
        (DATAGRAM_HEADER_SIZE + 16) as u16,
    );
    let mut msg = vec![0u8; 16];
    msg[0] = 0x05; // reserved by the wire specification
    msg[1] = 16;
    buf.extend_from_slice(&msg);

    let dg = Datagram::decode(&buf, TEST_MAGIC).unwrap();
    let msgs: Vec<_> = dg.messages().collect();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].type_id, 0x05);
}
