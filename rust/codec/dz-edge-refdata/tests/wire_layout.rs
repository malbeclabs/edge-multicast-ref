use dz_edge_core::{
    AppMessage, ChannelSequence, DatagramBuilder, DecodeError, EncodeError, Feed, Fit, PortRole,
    ResetCount, SCHEMA_VERSION, SCHEMA_VERSION_V1,
};
use dz_edge_refdata::{InstrumentDefinition, ManifestSummary, LEG_LEN, SIZE_V1, SYMBOL_LEN};

// Refdata's tests cannot name a particular feed - reference data is a
// supplement adopted by every feed in the family, not tied to any one of
// them. A test-local feed stands in for whichever feed would carry it.
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

fn sample() -> InstrumentDefinition {
    let mut symbol = [0u8; SYMBOL_LEN];
    symbol[..8].copy_from_slice(b"BTC-USDT");
    let mut leg1 = [0u8; LEG_LEN];
    leg1[..3].copy_from_slice(b"BTC");
    let mut leg2 = [0u8; LEG_LEN];
    leg2[..4].copy_from_slice(b"USDT");
    InstrumentDefinition {
        instrument_id: 42,
        source_id: 7,
        symbol,
        leg1,
        leg2,
        asset_class: 1,
        price_exponent: -2,
        qty_exponent: -8,
        market_model: 1,
        tick_size: 1,
        lot_size: 1000,
        contract_value: 0,
        expiry_ns: 0,
        settle_type: 0,
        price_bound: 0,
        manifest_seq: 9,
    }
}

#[test]
fn definition_fields_land_at_their_spec_offsets() {
    let d = sample();
    let mut b = [0u8; InstrumentDefinition::SIZE];
    d.encode_into(&mut b);

    assert_eq!(b.len(), 130);
    assert_eq!(b[0], 0x02, "offset 0: Type");
    assert_eq!(b[1], 130, "offset 1: Length");
    assert_eq!(&b[4..8], &42u32.to_le_bytes(), "offset 4: Instrument ID");
    assert_eq!(&b[8..10], &7u16.to_le_bytes(), "offset 8: Source ID");
    assert_eq!(&b[10..74], &d.symbol[..], "offset 10: Symbol, char[64]");
    assert_eq!(&b[74..82], &d.leg1[..], "offset 74: Leg1, char[8]");
    assert_eq!(&b[82..90], &d.leg2[..], "offset 82: Leg2, char[8]");
    assert_eq!(b[90], 1, "offset 90: Asset Class");
    assert_eq!(b[91] as i8, -2, "offset 91: Price Exponent");
    assert_eq!(b[92] as i8, -8, "offset 92: Qty Exponent");
    assert_eq!(b[93], 1, "offset 93: Market Model");
    assert_eq!(&b[94..102], &1i64.to_le_bytes(), "offset 94: Tick Size");
    assert_eq!(&b[102..110], &1000u64.to_le_bytes(), "offset 102: Lot Size");
    assert_eq!(
        &b[110..118],
        &0u64.to_le_bytes(),
        "offset 110: Contract Value"
    );
    assert_eq!(&b[118..126], &0u64.to_le_bytes(), "offset 118: Expiry");
    assert_eq!(b[126], 0, "offset 126: Settle Type");
    assert_eq!(b[127], 0, "offset 127: Price Bound");
    assert_eq!(
        &b[128..130],
        &9u16.to_le_bytes(),
        "offset 128: Manifest Seq"
    );
}

#[test]
fn definition_round_trips_at_v3() {
    let d = sample();
    let mut b = [0u8; InstrumentDefinition::SIZE];
    d.encode_into(&mut b);
    assert_eq!(InstrumentDefinition::decode(&b, SCHEMA_VERSION).unwrap(), d);
}

#[test]
fn a_v1_definition_decodes_at_its_own_offsets() {
    // v1: 80 bytes, no Source ID, Symbol is char[16]. Everything after Symbol
    // sits 50 bytes earlier than in v3. A subscriber meets this while one
    // publisher is still on schema 1.
    let mut b = [0u8; SIZE_V1];
    b[0] = 0x02;
    b[1] = SIZE_V1 as u8;
    b[4..8].copy_from_slice(&42u32.to_le_bytes()); // Instrument ID
    b[8..24].copy_from_slice(b"BTC-USDT\0\0\0\0\0\0\0\0"); // Symbol, char[16]
    b[24..32].copy_from_slice(b"BTC\0\0\0\0\0"); // Leg1
    b[32..40].copy_from_slice(b"USDT\0\0\0\0"); // Leg2
    b[40] = 1; // Asset Class
    b[41] = (-2i8) as u8; // Price Exponent
    b[42] = (-8i8) as u8; // Qty Exponent
    b[43] = 1; // Market Model
    b[44..52].copy_from_slice(&1i64.to_le_bytes()); // Tick Size
    b[52..60].copy_from_slice(&1000u64.to_le_bytes()); // Lot Size
    b[60..68].copy_from_slice(&0u64.to_le_bytes()); // Contract Value
    b[68..76].copy_from_slice(&0u64.to_le_bytes()); // Expiry
    b[76] = 0; // Settle Type
    b[77] = 0; // Price Bound
    b[78..80].copy_from_slice(&9u16.to_le_bytes()); // Manifest Seq

    let d = InstrumentDefinition::decode(&b, SCHEMA_VERSION_V1).unwrap();
    assert_eq!(d.instrument_id, 42);
    assert_eq!(&d.symbol[..8], b"BTC-USDT");
    assert_eq!(
        &d.symbol[8..],
        &[0u8; SYMBOL_LEN - 8][..],
        "widened symbol is null-padded"
    );
    assert_eq!(d.source_id, 0, "v1 carries no Source ID; it reads as 0");
    assert_eq!(d.price_exponent, -2);
    assert_eq!(d.qty_exponent, -8);
    assert_eq!(d.lot_size, 1000);
    assert_eq!(d.manifest_seq, 9);
}

#[test]
fn a_negative_tick_size_survives_the_round_trip() {
    // Tick Size is i64 and carries the instrument's Price Exponent, so a
    // sign error corrupts every price a subscriber derives from it.
    let mut d = sample();
    d.tick_size = i64::MIN + 1;
    let mut b = [0u8; InstrumentDefinition::SIZE];
    d.encode_into(&mut b);
    assert_eq!(
        InstrumentDefinition::decode(&b, SCHEMA_VERSION)
            .unwrap()
            .tick_size,
        i64::MIN + 1
    );
}

#[test]
fn schema_two_is_refused() {
    let d = sample();
    let mut b = [0u8; InstrumentDefinition::SIZE];
    d.encode_into(&mut b);
    assert!(
        matches!(
            InstrumentDefinition::decode(&b, 2),
            Err(DecodeError::UnsupportedSchema(2))
        ),
        "the 128-byte layout never reached the wire and must not be decodable"
    );
}

#[test]
fn definition_decode_at_v3_rejects_a_buffer_shorter_than_the_fixed_size() {
    let d = sample();
    let mut b = [0u8; InstrumentDefinition::SIZE];
    d.encode_into(&mut b);
    assert!(matches!(
        InstrumentDefinition::decode(&b[..129], SCHEMA_VERSION),
        Err(DecodeError::ShortBuffer {
            need: 130,
            got: 129
        })
    ));
}

#[test]
fn definition_decode_at_v1_rejects_a_buffer_shorter_than_the_v1_size() {
    let mut b = [0u8; SIZE_V1];
    b[0] = 0x02;
    b[1] = SIZE_V1 as u8;
    assert!(matches!(
        InstrumentDefinition::decode(&b[..79], SCHEMA_VERSION_V1),
        Err(DecodeError::ShortBuffer { need: 80, got: 79 })
    ));
}

#[test]
fn definition_decode_at_v3_rejects_a_corrupted_declared_length() {
    let d = sample();
    let mut b = [0u8; InstrumentDefinition::SIZE];
    d.encode_into(&mut b);
    b[1] = 80; // lie about the length: a valid v1 length, not v3's
    assert!(matches!(
        InstrumentDefinition::decode(&b, SCHEMA_VERSION),
        Err(DecodeError::LengthMismatch {
            type_id: 0x02,
            declared: 80,
            expected: 130
        })
    ));
}

#[test]
fn definition_decode_at_v1_rejects_a_corrupted_declared_length() {
    let mut b = [0u8; SIZE_V1];
    b[0] = 0x02;
    b[1] = 130; // lie about the length: a valid v3 length, not v1's
    assert!(matches!(
        InstrumentDefinition::decode(&b, SCHEMA_VERSION_V1),
        Err(DecodeError::LengthMismatch {
            type_id: 0x02,
            declared: 130,
            expected: 80
        })
    ));
}

#[test]
fn definition_decode_rejects_a_type_id_that_is_not_its_own() {
    let d = sample();
    let mut b = [0u8; InstrumentDefinition::SIZE];
    d.encode_into(&mut b);
    b[0] = 0xFF;
    assert!(matches!(
        InstrumentDefinition::decode(&b, SCHEMA_VERSION),
        Err(DecodeError::BadTypeId(0xFF))
    ));
}

#[test]
fn manifest_summary_carries_count_and_seq() {
    // encode_into honours Channel ID; constructed as 0 here so the plain
    // decode round-trip below holds without a framing datagram involved.
    let m = ManifestSummary {
        channel_id: 0,
        valid: 1,
        manifest_seq: 9,
        instrument_count: 1234,
        timestamp_ns: 88,
    };
    let mut b = [0u8; ManifestSummary::SIZE];
    m.encode_into(&mut b);

    assert_eq!(b.len(), 24);
    assert_eq!(b[0], 0x07, "offset 0: Type");
    assert_eq!(b[1], 24, "offset 1: Length");
    assert_eq!(b[4], 0, "offset 4: Channel ID, honoured by encode_into");
    assert_eq!(b[5], 1, "offset 5: Valid");
    assert_eq!(&b[6..8], &[0, 0], "offset 6: Reserved, 2 bytes");
    assert_eq!(&b[8..10], &9u16.to_le_bytes(), "offset 8: Manifest Seq");
    assert_eq!(&b[10..12], &[0, 0], "offset 10: Reserved, 2 bytes");
    assert_eq!(
        &b[12..16],
        &1234u32.to_le_bytes(),
        "offset 12: Instrument Count"
    );
    assert_eq!(&b[16..24], &88u64.to_le_bytes(), "offset 16: Timestamp");
    assert_eq!(ManifestSummary::decode(&b).unwrap(), m);
}

#[test]
fn manifest_summary_encode_into_round_trips_channel_id() {
    // A standalone encode/decode round trip (no DatagramBuilder involved)
    // must honour the public channel_id field honestly.
    let m = ManifestSummary {
        channel_id: 7,
        valid: 1,
        manifest_seq: 9,
        instrument_count: 1234,
        timestamp_ns: 88,
    };
    let mut b = [0u8; ManifestSummary::SIZE];
    m.encode_into(&mut b);
    assert_eq!(ManifestSummary::decode(&b).unwrap().channel_id, 7);
}

#[test]
fn manifest_summary_decode_rejects_a_declared_length_that_is_not_the_fixed_size() {
    let m = ManifestSummary {
        channel_id: 7,
        valid: 1,
        manifest_seq: 9,
        instrument_count: 1234,
        timestamp_ns: 88,
    };
    let mut b = [0u8; ManifestSummary::SIZE];
    m.encode_into(&mut b);
    b[1] = 25; // lie about the length
    assert!(matches!(
        ManifestSummary::decode(&b),
        Err(DecodeError::LengthMismatch {
            type_id: 0x07,
            declared: 25,
            expected: 24
        })
    ));
}

#[test]
fn manifest_summary_decode_rejects_a_buffer_shorter_than_the_fixed_size() {
    let m = ManifestSummary {
        channel_id: 7,
        valid: 1,
        manifest_seq: 9,
        instrument_count: 1234,
        timestamp_ns: 88,
    };
    let mut b = [0u8; ManifestSummary::SIZE];
    m.encode_into(&mut b);
    assert!(matches!(
        ManifestSummary::decode(&b[..23]),
        Err(DecodeError::ShortBuffer { need: 24, got: 23 })
    ));
}

#[test]
fn the_builder_stamps_manifest_summarys_channel_id_from_the_datagram() {
    // A ManifestSummary claiming channel 9 inside a datagram built for
    // channel 3 must encode with 3 at the message's Channel ID offset and
    // decode as 3: the builder owns this redundant copy, not the caller.
    let m = ManifestSummary {
        channel_id: 9,
        valid: 1,
        manifest_seq: 9,
        instrument_count: 1234,
        timestamp_ns: 88,
    };
    let channel = ChannelSequence::new(3, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Refdata, 1232);
    b.push(&m).unwrap();
    let out = b.finish(0).expect("datagram has messages");

    let msg = &out[dz_edge_core::DATAGRAM_HEADER_SIZE..];
    assert_eq!(
        msg[4], 3,
        "message offset 4: Channel ID stamped by the builder"
    );

    let decoded = ManifestSummary::decode(msg).unwrap();
    assert_eq!(decoded.channel_id, 3);
}

#[test]
fn set_symbol_fits_a_short_ascii_value() {
    let mut d = sample();
    let fit = d.set_symbol("ETH-USDT");
    assert_eq!(fit, Fit::Fitted);
    assert_eq!(&d.symbol[..8], b"ETH-USDT");
    assert_eq!(&d.symbol[8..], &[0u8; SYMBOL_LEN - 8][..]);
}

#[test]
fn set_symbol_reports_truncated_for_an_over_long_ascii_value() {
    let mut d = sample();
    let long = "A".repeat(SYMBOL_LEN + 10);
    let fit = d.set_symbol(&long);
    assert_eq!(fit, Fit::Truncated);
    assert_eq!(&d.symbol, &[b'A'; SYMBOL_LEN]);
}

#[test]
fn set_symbol_reports_unrepresentable_for_a_non_ascii_value() {
    let mut d = sample();
    let fit = d.set_symbol("BTC-USDT\u{e9}");
    assert_eq!(fit, Fit::Unrepresentable);
}

#[test]
fn set_leg1_fits_a_short_ascii_value() {
    let mut d = sample();
    let fit = d.set_leg1("BTC");
    assert_eq!(fit, Fit::Fitted);
    assert_eq!(&d.leg1[..3], b"BTC");
    assert_eq!(&d.leg1[3..], &[0u8; LEG_LEN - 3][..]);
}

#[test]
fn set_leg1_reports_truncated_for_an_over_long_value() {
    let mut d = sample();
    let fit = d.set_leg1("TOOLONGLEG");
    assert_eq!(fit, Fit::Truncated);
    assert_eq!(&d.leg1, b"TOOLONGL");
}

#[test]
fn set_leg2_fits_a_short_ascii_value() {
    let mut d = sample();
    let fit = d.set_leg2("USDT");
    assert_eq!(fit, Fit::Fitted);
    assert_eq!(&d.leg2[..4], b"USDT");
    assert_eq!(&d.leg2[4..], &[0u8; LEG_LEN - 4][..]);
}

#[test]
fn set_leg2_reports_unrepresentable_for_an_interior_nul() {
    let mut d = sample();
    let fit = d.set_leg2("US\0DT");
    assert_eq!(fit, Fit::Unrepresentable);
    assert_eq!(&d.leg2[..5], b"US\0DT");
}

#[test]
fn an_instrument_definition_on_refdata_succeeds() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Refdata, 1232);
    b.push(&sample()).unwrap();
}

#[test]
fn an_instrument_definition_pushed_into_a_mktdata_builder_is_a_countable_error() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    let err = b.push(&sample()).unwrap_err();
    assert_eq!(
        err,
        EncodeError::WrongPortRole {
            message: core::any::type_name::<InstrumentDefinition>(),
            role: "mktdata",
        }
    );
}
