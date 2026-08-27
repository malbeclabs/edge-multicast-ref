use dz_edge_core::{AppMessage, DecodeError, EndOfSession, Heartbeat};

#[test]
fn heartbeat_matches_its_spec_layout() {
    let hb = Heartbeat {
        channel_id: 7,
        timestamp_ns: 0x0102_0304_0506_0708,
    };
    let mut buf = [0u8; Heartbeat::SIZE];
    hb.encode_into(&mut buf);

    assert_eq!(buf.len(), 16);
    assert_eq!(buf[0], 0x01, "offset 0: Type");
    assert_eq!(buf[1], 16, "offset 1: Length");
    assert_eq!(buf[4], 7, "offset 4: Channel ID");
    assert_eq!(&buf[5..8], &[0, 0, 0], "offset 5: Reserved, 3 bytes");
    assert_eq!(
        &buf[8..16],
        &0x0102_0304_0506_0708u64.to_le_bytes(),
        "offset 8: Timestamp"
    );

    assert_eq!(Heartbeat::decode(&buf).unwrap(), hb);
}

#[test]
fn end_of_session_matches_its_spec_layout() {
    let eos = EndOfSession { timestamp_ns: 99 };
    let mut buf = [0u8; EndOfSession::SIZE];
    eos.encode_into(&mut buf);

    assert_eq!(buf.len(), 12);
    assert_eq!(buf[0], 0x06, "offset 0: Type");
    assert_eq!(buf[1], 12, "offset 1: Length");
    assert_eq!(&buf[4..12], &99u64.to_le_bytes(), "offset 4: Timestamp");

    assert_eq!(EndOfSession::decode(&buf).unwrap(), eos);
}

#[test]
fn heartbeat_decode_rejects_a_declared_length_that_is_not_the_fixed_size() {
    let mut buf = [0u8; Heartbeat::SIZE];
    Heartbeat {
        channel_id: 0,
        timestamp_ns: 0,
    }
    .encode_into(&mut buf);
    buf[1] = 20; // lie about the length
    assert!(matches!(
        Heartbeat::decode(&buf),
        Err(DecodeError::LengthMismatch {
            type_id: 0x01,
            declared: 20,
            expected: 16
        })
    ));
}

#[test]
fn end_of_session_decode_rejects_a_declared_length_that_is_not_the_fixed_size() {
    let mut buf = [0u8; EndOfSession::SIZE];
    EndOfSession { timestamp_ns: 0 }.encode_into(&mut buf);
    buf[1] = 20; // lie about the length
    assert!(matches!(
        EndOfSession::decode(&buf),
        Err(DecodeError::LengthMismatch {
            type_id: 0x06,
            declared: 20,
            expected: 12
        })
    ));
}
