use dz_edge_core::{AppMessage, DatagramBuilder, DatagramHeader, DecodeError};
use dz_edge_core::{DATAGRAM_HEADER_SIZE, MAX_DATAGRAM_SIZE, SCHEMA_VERSION};

// Core's tests only need *a* magic value; they must not depend on
// dz-edge-tob, which owns the real MAGIC_TOB.
const TEST_MAGIC: u16 = 0x445A;

struct Sixteen;
impl AppMessage for Sixteen {
    const TYPE_ID: u8 = 0x01;
    const SIZE: usize = 16;
    fn encode_into(&self, dst: &mut [u8]) {
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4..].fill(0);
    }
}

struct SelfFlagged;
impl AppMessage for SelfFlagged {
    const TYPE_ID: u8 = 0x01;
    const SIZE: usize = 16;
    fn encode_into(&self, dst: &mut [u8]) {
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        // Deliberately sets the snapshot bit itself. The builder must clear it.
        dst[2..4].copy_from_slice(&0x0001u16.to_le_bytes());
        dst[4..].fill(0);
    }
}

struct HeaderOnly;
impl AppMessage for HeaderOnly {
    const TYPE_ID: u8 = 0x01;
    const SIZE: usize = 4;
    fn encode_into(&self, dst: &mut [u8]) {
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
    }
}

#[test]
fn header_fields_land_at_their_spec_offsets() {
    let mut b = DatagramBuilder::new(TEST_MAGIC, 7, 42, 1_700_000_000_000_000_000, 3, 1232);
    b.push(&Sixteen).unwrap();
    let out = b.finish();

    assert_eq!(&out[0..2], &TEST_MAGIC.to_le_bytes(), "offset 0: Magic");
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
fn an_mtu_above_the_mandated_cap_is_clamped() {
    // 1448 is a plausible deployment default and exceeds the mandated cap.
    let b = DatagramBuilder::new(TEST_MAGIC, 0, 0, 0, 0, 1448);
    assert_eq!(
        b.remaining(),
        MAX_DATAGRAM_SIZE - DATAGRAM_HEADER_SIZE,
        "capacity must clamp to the mandated maximum, not the requested MTU"
    );
}

#[test]
fn a_finished_datagram_never_exceeds_the_cap() {
    let mut b = DatagramBuilder::new(TEST_MAGIC, 0, 0, 0, 0, 1448);
    while b.push(&Sixteen).is_ok() {}
    let out = b.finish();
    assert!(
        out.len() <= MAX_DATAGRAM_SIZE,
        "finished datagram {} exceeds {MAX_DATAGRAM_SIZE}",
        out.len()
    );
}

#[test]
fn a_message_that_does_not_fit_is_refused_rather_than_truncated() {
    let mut b = DatagramBuilder::new(TEST_MAGIC, 0, 0, 0, 0, 1232);
    while b.push(&Sixteen).is_ok() {}
    assert!(matches!(
        b.push(&Sixteen),
        Err(DecodeError::DatagramFull { .. })
    ));
}

#[test]
fn message_count_stops_at_255() {
    // Message Count is a u8. A 256th message would wrap it to 0 and every
    // subscriber would mis-parse the rest of the datagram. A 4-byte message
    // makes the cap reachable: (1232 - 24) / 4 = 302 slots, well past 255.
    let mut b = DatagramBuilder::new(TEST_MAGIC, 0, 0, 0, 0, 1232);
    let mut pushed = 0usize;
    while b.push(&HeaderOnly).is_ok() {
        pushed += 1;
    }
    assert_eq!(
        pushed, 255,
        "the u8 Message Count must stop the builder at 255"
    );
    let out = b.finish();
    assert_eq!(out[20], 255, "offset 20: Message Count");
    assert!(out.len() <= MAX_DATAGRAM_SIZE);
}

#[test]
fn the_snapshot_flag_is_set_only_by_push_snapshot() {
    let mut plain = DatagramBuilder::new(TEST_MAGIC, 0, 0, 0, 0, 1232);
    plain.push(&Sixteen).unwrap();
    let out = plain.finish();
    let flags = u16::from_le_bytes([out[DATAGRAM_HEADER_SIZE + 2], out[DATAGRAM_HEADER_SIZE + 3]]);
    assert_eq!(
        flags & 0x0001,
        0,
        "mktdata and refdata messages clear bit 0"
    );

    let mut snap = DatagramBuilder::new(TEST_MAGIC, 0, 0, 0, 0, 1232);
    snap.push_snapshot(&Sixteen).unwrap();
    let out = snap.finish();
    let flags = u16::from_le_bytes([out[DATAGRAM_HEADER_SIZE + 2], out[DATAGRAM_HEADER_SIZE + 3]]);
    assert_eq!(flags & 0x0001, 1, "snapshot-port messages set bit 0");

    // A message that sets the snapshot bit itself must still have it cleared
    // by plain push(): the builder owns Flags, not the message.
    let mut fights_back = DatagramBuilder::new(TEST_MAGIC, 0, 0, 0, 0, 1232);
    fights_back.push(&SelfFlagged).unwrap();
    let out = fights_back.finish();
    let flags = u16::from_le_bytes([out[DATAGRAM_HEADER_SIZE + 2], out[DATAGRAM_HEADER_SIZE + 3]]);
    assert_eq!(
        flags & 0x0001,
        0,
        "push() must clear a self-set snapshot bit"
    );
}

#[test]
fn decode_rejects_a_schema_version_it_does_not_implement() {
    let mut b = DatagramBuilder::new(TEST_MAGIC, 0, 0, 0, 0, 1232);
    b.push(&Sixteen).unwrap();
    let mut out = b.finish();
    out[2] = 2; // the generation that never reached the wire
    assert_eq!(
        DatagramHeader::decode(&out),
        Err(DecodeError::UnsupportedSchema(2))
    );
}

#[test]
fn decode_round_trips_a_built_datagram() {
    let mut b = DatagramBuilder::new(TEST_MAGIC, 9, 1234, 5678, 2, 1232);
    b.push(&Sixteen).unwrap();
    let out = b.finish();
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
fn a_tiny_mtu_still_yields_a_well_formed_empty_datagram() {
    // Capacity clamps UP to the header as well as down to the cap, so a
    // degenerate mtu cannot make buf longer than capacity.
    let b = DatagramBuilder::new(TEST_MAGIC, 0, 0, 0, 0, 4);
    assert_eq!(b.remaining(), 0);
    let out = b.finish();
    assert_eq!(out.len(), DATAGRAM_HEADER_SIZE);
}
