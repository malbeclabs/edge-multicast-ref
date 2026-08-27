use dz_edge_core::{
    AppMessage, Datagram, DatagramBuilder, DecodeError, EndOfSession, Heartbeat,
    DATAGRAM_HEADER_SIZE, SCHEMA_VERSION,
};

// Core's tests only need *a* magic value; they must not depend on
// dz-edge-tob, which owns the real MAGIC_TOB.
const TEST_MAGIC: u16 = 0x445A;

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
    let mut b = DatagramBuilder::new(TEST_MAGIC, 3, 10, 999, 0, 1232);
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
    let out = b.finish();

    let dg = Datagram::decode(&out, TEST_MAGIC).unwrap();
    let msgs: Vec<_> = dg.messages().collect();
    assert_eq!(msgs.len(), 3);

    assert_eq!(msgs[0].type_id, Heartbeat::TYPE_ID);
    assert_eq!(msgs[0].bytes.len(), Heartbeat::SIZE);
    assert_eq!(Heartbeat::decode(msgs[0].bytes).unwrap(), hb1);

    assert_eq!(msgs[1].type_id, Heartbeat::TYPE_ID);
    assert_eq!(msgs[1].bytes.len(), Heartbeat::SIZE);
    assert_eq!(Heartbeat::decode(msgs[1].bytes).unwrap(), hb2);

    assert_eq!(msgs[2].type_id, EndOfSession::TYPE_ID);
    assert_eq!(msgs[2].bytes.len(), EndOfSession::SIZE);
    assert_eq!(EndOfSession::decode(msgs[2].bytes).unwrap(), eos);
}

#[test]
fn flags_reflect_push_vs_push_snapshot() {
    let hb = Heartbeat {
        channel_id: 0,
        timestamp_ns: 0,
    };

    let mut plain = DatagramBuilder::new(TEST_MAGIC, 0, 0, 0, 0, 1232);
    plain.push(&hb).unwrap();
    let out = plain.finish();
    let dg = Datagram::decode(&out, TEST_MAGIC).unwrap();
    let msg = dg.messages().next().unwrap();
    assert_eq!(msg.flags, 0, "push() must clear the snapshot bit");

    let mut snap = DatagramBuilder::new(TEST_MAGIC, 0, 0, 0, 0, 1232);
    snap.push_snapshot(&hb).unwrap();
    let out = snap.finish();
    let dg = Datagram::decode(&out, TEST_MAGIC).unwrap();
    let msg = dg.messages().next().unwrap();
    assert_eq!(msg.flags, 1, "push_snapshot() must set the snapshot bit");
}

#[test]
fn an_empty_datagram_yields_no_messages_and_is_not_an_error() {
    let b = DatagramBuilder::new(TEST_MAGIC, 0, 0, 0, 0, 1232);
    let out = b.finish();
    let dg = Datagram::decode(&out, TEST_MAGIC).unwrap();
    assert_eq!(dg.header().msg_count, 0);
    assert_eq!(dg.messages().count(), 0);
}

#[test]
fn a_magic_mismatch_is_rejected() {
    let mut b = DatagramBuilder::new(TEST_MAGIC, 0, 0, 0, 0, 1232);
    b.push(&Heartbeat {
        channel_id: 0,
        timestamp_ns: 0,
    })
    .unwrap();
    let out = b.finish();

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
    let mut b = DatagramBuilder::new(TEST_MAGIC, 0, 0, 0, 0, 1232);
    b.push(&Heartbeat {
        channel_id: 0,
        timestamp_ns: 0,
    })
    .unwrap();
    let out = b.finish();
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
    let mut b = DatagramBuilder::new(TEST_MAGIC, 0, 0, 0, 0, 1232);
    b.push(&Heartbeat {
        channel_id: 0,
        timestamp_ns: 42,
    })
    .unwrap();
    let mut out = b.finish();
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
    let mut b = DatagramBuilder::new(TEST_MAGIC, 0, 0, 0, 0, 1232);
    b.push(&Heartbeat {
        channel_id: 0,
        timestamp_ns: 0,
    })
    .unwrap();
    let mut out = b.finish();
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
    let mut b = DatagramBuilder::new(TEST_MAGIC, 0, 0, 0, 0, 1232);
    b.push(&Heartbeat {
        channel_id: 0,
        timestamp_ns: 0,
    })
    .unwrap();
    let mut out = b.finish();
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
fn header_claims_fewer_messages_than_are_present() {
    let mut b = DatagramBuilder::new(TEST_MAGIC, 0, 0, 0, 0, 1232);
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
    let mut out = b.finish();
    out[20] = 1; // claims one message though two are present

    assert!(matches!(
        Datagram::decode(&out, TEST_MAGIC),
        Err(DecodeError::MessageCountMismatch {
            declared: 1,
            found: 2
        })
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
