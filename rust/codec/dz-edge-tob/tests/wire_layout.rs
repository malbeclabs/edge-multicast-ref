use dz_edge_core::{AppMessage, DecodeError};
use dz_edge_tob::{Quote, Trade};

fn sample_quote() -> Quote {
    Quote {
        instrument_id: 0x1112_1314,
        source_id: 0x2122,
        update_flags: 0x03,
        source_timestamp_ns: 0x3132_3334_3536_3738,
        bid_price: -12_345,
        bid_qty: 6789,
        ask_price: 54_321,
        ask_qty: 9876,
        bid_source_count: 4,
        ask_source_count: 5,
    }
}

#[test]
fn quote_fields_land_at_their_spec_offsets() {
    let q = sample_quote();
    let mut b = [0u8; Quote::SIZE];
    q.encode_into(&mut b);

    assert_eq!(b.len(), 60);
    assert_eq!(b[0], 0x03, "offset 0: Type");
    assert_eq!(b[1], 60, "offset 1: Length");
    assert_eq!(&b[4..8], &0x1112_1314u32.to_le_bytes(), "offset 4: Instrument ID");
    assert_eq!(&b[8..10], &0x2122u16.to_le_bytes(), "offset 8: Source ID");
    assert_eq!(b[10], 0x03, "offset 10: Update Flags");
    assert_eq!(b[11], 0, "offset 11: Reserved");
    assert_eq!(&b[12..20], &0x3132_3334_3536_3738u64.to_le_bytes(), "offset 12: Source Timestamp");
    assert_eq!(&b[20..28], &(-12_345i64).to_le_bytes(), "offset 20: Bid Price");
    assert_eq!(&b[28..36], &6789u64.to_le_bytes(), "offset 28: Bid Quantity");
    assert_eq!(&b[36..44], &54_321i64.to_le_bytes(), "offset 36: Ask Price");
    assert_eq!(&b[44..52], &9876u64.to_le_bytes(), "offset 44: Ask Quantity");
    assert_eq!(&b[52..54], &4u16.to_le_bytes(), "offset 52: Bid Source Count");
    assert_eq!(&b[54..56], &5u16.to_le_bytes(), "offset 54: Ask Source Count");
    assert_eq!(&b[56..60], &[0, 0, 0, 0], "offset 56: Reserved, 4 bytes");
}

#[test]
fn quote_round_trips() {
    let q = sample_quote();
    let mut b = [0u8; Quote::SIZE];
    q.encode_into(&mut b);
    assert_eq!(Quote::decode(&b).unwrap(), q);
}

#[test]
fn a_negative_price_survives_the_round_trip() {
    // Price is i64. A negative price must not wrap to a huge
    // positive quantity on the far side.
    let mut q = sample_quote();
    q.bid_price = i64::MIN + 1;
    let mut b = [0u8; Quote::SIZE];
    q.encode_into(&mut b);
    assert_eq!(Quote::decode(&b).unwrap().bid_price, i64::MIN + 1);
}

#[test]
fn quote_decode_rejects_a_declared_length_that_is_not_the_fixed_size() {
    let mut b = [0u8; Quote::SIZE];
    sample_quote().encode_into(&mut b);
    b[1] = 61; // lie about the length
    assert!(matches!(
        Quote::decode(&b),
        Err(DecodeError::LengthMismatch { type_id: 0x03, declared: 61, expected: 60 })
    ));
}

#[test]
fn quote_decode_rejects_a_buffer_shorter_than_the_fixed_size() {
    let mut b = [0u8; Quote::SIZE];
    sample_quote().encode_into(&mut b);
    assert!(matches!(
        Quote::decode(&b[..59]),
        Err(DecodeError::ShortBuffer { need: 60, got: 59 })
    ));
}

#[test]
fn trade_fields_land_at_their_spec_offsets() {
    let t = Trade {
        instrument_id: 7,
        source_id: 9,
        aggressor_side: 1,
        trade_flags: 0x02,
        source_timestamp_ns: 0x4142_4344_4546_4748,
        trade_price: 100,
        trade_qty: 200,
        trade_id: 300,
        cumulative_volume: 400,
    };
    let mut b = [0u8; Trade::SIZE];
    t.encode_into(&mut b);

    assert_eq!(b.len(), 52);
    assert_eq!(b[0], 0x04, "offset 0: Type");
    assert_eq!(b[1], 52, "offset 1: Length");
    assert_eq!(&b[4..8], &7u32.to_le_bytes(), "offset 4: Instrument ID");
    assert_eq!(&b[8..10], &9u16.to_le_bytes(), "offset 8: Source ID");
    assert_eq!(b[10], 1, "offset 10: Aggressor Side");
    assert_eq!(b[11], 0x02, "offset 11: Trade Flags");
    assert_eq!(&b[12..20], &0x4142_4344_4546_4748u64.to_le_bytes(), "offset 12: Source Timestamp");
    assert_eq!(&b[20..28], &100i64.to_le_bytes(), "offset 20: Trade Price");
    assert_eq!(&b[28..36], &200u64.to_le_bytes(), "offset 28: Trade Quantity");
    assert_eq!(&b[36..44], &300u64.to_le_bytes(), "offset 36: Trade ID");
    assert_eq!(&b[44..52], &400u64.to_le_bytes(), "offset 44: Cumulative Volume");

    assert_eq!(Trade::decode(&b).unwrap(), t);
}

#[test]
fn trade_decode_rejects_a_declared_length_that_is_not_the_fixed_size() {
    let t = Trade {
        instrument_id: 7,
        source_id: 9,
        aggressor_side: 1,
        trade_flags: 0x02,
        source_timestamp_ns: 0,
        trade_price: 100,
        trade_qty: 200,
        trade_id: 300,
        cumulative_volume: 400,
    };
    let mut b = [0u8; Trade::SIZE];
    t.encode_into(&mut b);
    b[1] = 53; // lie about the length
    assert!(matches!(
        Trade::decode(&b),
        Err(DecodeError::LengthMismatch { type_id: 0x04, declared: 53, expected: 52 })
    ));
}

#[test]
fn trade_decode_rejects_a_buffer_shorter_than_the_fixed_size() {
    let t = Trade {
        instrument_id: 7,
        source_id: 9,
        aggressor_side: 1,
        trade_flags: 0x02,
        source_timestamp_ns: 0,
        trade_price: 100,
        trade_qty: 200,
        trade_id: 300,
        cumulative_volume: 400,
    };
    let mut b = [0u8; Trade::SIZE];
    t.encode_into(&mut b);
    assert!(matches!(
        Trade::decode(&b[..51]),
        Err(DecodeError::ShortBuffer { need: 52, got: 51 })
    ));
}
