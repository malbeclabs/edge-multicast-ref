use dz_edge_core::{AppMessage, DecodeError, Heartbeat};

struct Fake;
impl AppMessage for Fake {
    const TYPE_ID: u8 = 0x01;
    const SIZE: usize = 16;
    fn encode_into(&self, dst: &mut [u8]) {
        debug_assert_eq!(dst.len(), Self::SIZE);
        dst.fill(0xAB);
    }
}

#[test]
fn a_message_encodes_into_exactly_its_size() {
    let mut buf = [0u8; 16];
    Fake.encode_into(&mut buf);
    assert_eq!(buf, [0xAB; 16]);
}

#[test]
#[should_panic(expected = "assertion `left == right` failed")]
fn encode_into_panics_on_a_slice_longer_than_size() {
    let msg = Heartbeat {
        channel_id: 0,
        timestamp_ns: 0,
    };
    let mut buf = vec![0u8; Heartbeat::SIZE + 1];
    msg.encode_into(&mut buf);
}

#[test]
fn decode_errors_render_the_numbers_a_reader_needs() {
    let e = DecodeError::ShortBuffer { need: 60, got: 12 };
    assert_eq!(e.to_string(), "short buffer: need 60 bytes, got 12");

    let e = DecodeError::UnsupportedSchema(2);
    assert_eq!(e.to_string(), "unsupported schema version 2");

    let e = DecodeError::ReservedTypeId(0x05);
    assert_eq!(
        e.to_string(),
        "type id 0x05 is reserved and carries no message"
    );
}
