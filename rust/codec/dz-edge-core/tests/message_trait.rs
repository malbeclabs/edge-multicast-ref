use dz_edge_core::{AppMessage, DecodeError, PortRole};

// `Fake` writes only `dst[..SIZE]`, matching the trait's documented contract
// exactly (see `message.rs`): a `dst` longer than `SIZE` has its first `SIZE`
// bytes written and the remainder left untouched. Slicing to `Self::SIZE`
// relies on Rust's ordinary bounds check (always enforced, in every profile)
// rather than a `debug_assert!`, so a too-short `dst` still panics
// regardless of build profile.
struct Fake;
impl AppMessage for Fake {
    const TYPE_ID: u8 = 0x01;
    const SIZE: usize = 16;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Mktdata];
    fn encode_into(&self, dst: &mut [u8]) {
        dst[..Self::SIZE].fill(0xAB);
    }

    // Fake carries no redundant Channel ID.
    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}

#[test]
fn a_message_encodes_into_exactly_its_size() {
    let mut buf = [0u8; 16];
    Fake.encode_into(&mut buf);
    assert_eq!(buf, [0xAB; 16]);
}

// Asserts the documented contract directly - a `dst` longer than `SIZE` has
// its first `SIZE` bytes written and the remainder left untouched - and is
// therefore deliberately profile-independent. This replaces a prior test
// that used `#[should_panic]` on a real message type's `encode_into`, which
// relied on a `debug_assert_eq!` in that impl: it passed under the dev
// profile (debug assertions on) and failed under `--release` (debug
// assertions compiled out), the exact opposite of what `message.rs`
// documents. Production message types (`Heartbeat` and friends) additionally
// guard the exact-size precondition with `debug_assert_eq!` as a dev-time
// bug-catching aid, which is unrelated to this test and is left untouched;
// `Fake` isolates the trait-level contract so the over-long-`dst` case can be
// exercised without depending on debug-assertion state.
#[test]
fn encode_into_writes_only_the_first_size_bytes() {
    let mut buf = [0xAAu8; Fake::SIZE + 4];
    Fake.encode_into(&mut buf);

    assert_eq!(&buf[..Fake::SIZE], [0xAB; Fake::SIZE]);
    assert!(buf[Fake::SIZE..].iter().all(|&b| b == 0xAA));
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
