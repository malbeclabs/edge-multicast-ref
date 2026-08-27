use dz_edge_core::{AppMessage, ChannelSequence, DatagramBuilder, DatagramHeader};

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

#[test]
fn new_starts_at_sequence_zero() {
    let ch = ChannelSequence::new(3, 1);
    assert_eq!(ch.channel_id(), 3);
    assert_eq!(ch.reset_count(), 1);
    assert_eq!(ch.sequence_number(), 0);
}

#[test]
fn advance_increments_the_sequence_number() {
    let mut ch = ChannelSequence::new(3, 1);
    ch.advance();
    ch.advance();
    assert_eq!(ch.sequence_number(), 2);
    assert_eq!(ch.channel_id(), 3, "advance must not touch Channel ID");
    assert_eq!(ch.reset_count(), 1, "advance must not touch Reset Count");
}

#[test]
fn begin_era_bumps_reset_count_and_zeroes_the_sequence() {
    let mut ch = ChannelSequence::new(3, 1);
    ch.advance();
    ch.advance();
    ch.advance();
    assert_eq!(ch.sequence_number(), 3);

    ch.begin_era();

    assert_eq!(ch.reset_count(), 2, "begin_era must bump Reset Count");
    assert_eq!(
        ch.sequence_number(),
        0,
        "begin_era must restart the sequence at 0"
    );
    assert_eq!(ch.channel_id(), 3, "begin_era must not touch Channel ID");
}

#[test]
fn resume_round_trips() {
    let ch = ChannelSequence::resume(5, 7, 12_345);
    assert_eq!(ch.channel_id(), 5);
    assert_eq!(ch.reset_count(), 7);
    assert_eq!(ch.sequence_number(), 12_345);
}

#[test]
fn the_builders_header_sequence_and_reset_count_come_from_the_channel_instance() {
    let ch = ChannelSequence::resume(4, 6, 777);
    let mut b = DatagramBuilder::new(TEST_MAGIC, ch, 1232);
    b.push(&Sixteen).unwrap();
    let out = b.finish(0).expect("datagram has messages");

    let header = DatagramHeader::decode(&out).unwrap();
    assert_eq!(header.channel_id, 4);
    assert_eq!(
        header.sequence_number, 777,
        "sequence number must come from the ChannelSequence, not be transposable with reset_count"
    );
    assert_eq!(
        header.reset_count, 6,
        "reset count must come from the ChannelSequence, not be transposable with sequence_number"
    );
}
