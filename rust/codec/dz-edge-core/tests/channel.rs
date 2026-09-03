use dz_edge_core::{
    AppMessage, ChannelSequence, DatagramBuilder, DatagramHeader, Feed, PortRole, ResetCount,
};

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

struct Sixteen;
impl AppMessage for Sixteen {
    const TYPE_ID: u8 = 0x01;
    const SIZE: usize = 16;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Mktdata];
    fn encode_into(&self, dst: &mut [u8]) {
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4..].fill(0);
    }

    // Sixteen carries no redundant Channel ID.
    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}

#[test]
fn new_starts_at_sequence_zero() {
    let ch = ChannelSequence::new(3, ResetCount(1));
    assert_eq!(ch.channel_id(), 3);
    assert_eq!(ch.reset_count(), ResetCount(1));
    assert_eq!(ch.sequence_number(), 0);
}

#[test]
fn advance_increments_the_sequence_number() {
    let mut ch = ChannelSequence::new(3, ResetCount(1));
    ch.advance();
    ch.advance();
    assert_eq!(ch.sequence_number(), 2);
    assert_eq!(ch.channel_id(), 3, "advance must not touch Channel ID");
    assert_eq!(
        ch.reset_count(),
        ResetCount(1),
        "advance must not touch Reset Count"
    );
}

#[test]
fn begin_era_bumps_reset_count_and_zeroes_the_sequence() {
    let mut ch = ChannelSequence::new(3, ResetCount(1));
    ch.advance();
    ch.advance();
    ch.advance();
    assert_eq!(ch.sequence_number(), 3);

    ch.begin_era();

    assert_eq!(
        ch.reset_count(),
        ResetCount(2),
        "begin_era must bump Reset Count"
    );
    assert_eq!(
        ch.sequence_number(),
        0,
        "begin_era must restart the sequence at 0"
    );
    assert_eq!(ch.channel_id(), 3, "begin_era must not touch Channel ID");
}

#[test]
fn resume_round_trips() {
    let ch = ChannelSequence::resume(5, ResetCount(7), 12_345);
    assert_eq!(ch.channel_id(), 5);
    assert_eq!(ch.reset_count(), ResetCount(7));
    assert_eq!(ch.sequence_number(), 12_345);
}

#[test]
fn the_builders_header_sequence_and_reset_count_come_from_the_channel_instance() {
    let ch = ChannelSequence::resume(4, ResetCount(6), 777);
    let mut b = DatagramBuilder::<TestFeed>::new(ch, PortRole::Mktdata, 1232);
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
