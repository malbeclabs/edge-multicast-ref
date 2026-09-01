//! `peek` exists for the tier whose job is to count what `decode` refuses.

use dz_edge_core::{
    DatagramHeader, DecodeError, DATAGRAM_HEADER_SIZE, MAX_DATAGRAM_SIZE, SCHEMA_VERSION,
};

/// A header with every field set to something recognisable, so a transposed
/// offset shows up as a wrong value rather than as a plausible one.
fn header_bytes(schema_version: u8, declared_len: u16) -> [u8; DATAGRAM_HEADER_SIZE] {
    let mut buf = [0u8; DATAGRAM_HEADER_SIZE];
    buf[0..2].copy_from_slice(&0xABCDu16.to_le_bytes());
    buf[2] = schema_version;
    buf[3] = 7;
    buf[4..12].copy_from_slice(&1_234_567_890u64.to_le_bytes());
    buf[12..20].copy_from_slice(&9_876_543_210u64.to_le_bytes());
    buf[20] = 3;
    buf[21] = 5;
    buf[22..24].copy_from_slice(&declared_len.to_le_bytes());
    buf
}

#[test]
fn an_unsupported_schema_version_is_returned_by_value_rather_than_refused() {
    // The whole reason peek exists: through decode this datagram is simply
    // undecodable, so a tier required to count schema versions by value learns
    // nothing about the traffic most worth knowing about.
    let buf = header_bytes(0xFE, 200);
    assert!(matches!(
        DatagramHeader::decode(&buf),
        Err(DecodeError::UnsupportedSchema(0xFE))
    ));

    let header = DatagramHeader::peek(&buf).expect("a full-length buffer peeks");
    assert_eq!(header.schema_version, 0xFE);
    assert!(!header.schema_is_supported());
}

#[test]
fn a_declared_length_past_the_cap_is_returned_and_reported_out_of_range() {
    let declared = u16::try_from(MAX_DATAGRAM_SIZE + 1).unwrap();
    let buf = header_bytes(SCHEMA_VERSION, declared);
    assert!(DatagramHeader::decode(&buf).is_err());

    let header = DatagramHeader::peek(&buf).expect("a full-length buffer peeks");
    assert_eq!(header.datagram_len, declared);
    assert!(!header.declared_len_is_in_range());
}

#[test]
fn a_declared_length_below_the_header_is_also_out_of_range() {
    let header = DatagramHeader::peek(&header_bytes(SCHEMA_VERSION, 4)).unwrap();
    assert_eq!(header.datagram_len, 4);
    assert!(!header.declared_len_is_in_range());
}

#[test]
fn a_short_buffer_is_the_one_thing_peek_still_refuses() {
    // There is nothing to count in bytes that are not there.
    let buf = header_bytes(SCHEMA_VERSION, 200);
    let err = DatagramHeader::peek(&buf[..DATAGRAM_HEADER_SIZE - 1]).unwrap_err();
    assert!(matches!(
        err,
        DecodeError::ShortBuffer {
            need: DATAGRAM_HEADER_SIZE,
            got: 23
        }
    ));
}

#[test]
fn peek_and_decode_agree_on_every_field_of_a_valid_header() {
    // Two readers of one layout are two chances to transpose an offset.
    //
    // The buffer has to cover the declared length here, because that is a
    // condition decode enforces and peek does not: peek reads the header of a
    // datagram whose body never arrived, which is the case the health tier sees.
    let mut buf = vec![0u8; 200];
    buf[..DATAGRAM_HEADER_SIZE].copy_from_slice(&header_bytes(SCHEMA_VERSION, 200));
    let decoded = DatagramHeader::decode(&buf).expect("valid");
    let peeked = DatagramHeader::peek(&buf).expect("valid");
    assert_eq!(decoded, peeked);
    assert!(peeked.schema_is_supported());
    assert!(peeked.declared_len_is_in_range());
}

#[test]
fn peek_does_not_read_past_the_header() {
    // A datagram whose body is a lie must still yield its header, because the
    // sequence number in it is what a gap is measured with.
    let mut buf = vec![0u8; DATAGRAM_HEADER_SIZE + 8];
    buf[..DATAGRAM_HEADER_SIZE].copy_from_slice(&header_bytes(SCHEMA_VERSION, 1232));
    let peeked = DatagramHeader::peek(&buf).expect("the header is present");
    assert_eq!(peeked.sequence_number, 1_234_567_890);
    assert_eq!(peeked.channel_id, 7);
    assert_eq!(peeked.reset_count, 5);
}

#[test]
fn a_header_whose_body_never_arrived_still_peeks() {
    // decode refuses this: the declared length exceeds the bytes present. But a
    // truncated datagram is exactly what a capture length cut short, and its
    // sequence number is what keeps the gap arithmetic honest.
    let buf = header_bytes(SCHEMA_VERSION, 1232);
    assert!(matches!(
        DatagramHeader::decode(&buf),
        Err(DecodeError::ShortBuffer { .. })
    ));
    let peeked = DatagramHeader::peek(&buf).expect("the header is all of it");
    assert_eq!(peeked.datagram_len, 1232);
    assert!(peeked.declared_len_is_in_range());
    assert_eq!(peeked.sequence_number, 1_234_567_890);
}
