use dz_edge_core::{
    AppMessage, ChannelSequence, DatagramBuilder, DecodeError, EncodeError, Feed, Fit, PortRole,
    ResetCount, SCHEMA_VERSION, SCHEMA_VERSION_V1,
};
use dz_edge_refdata::{
    InstrumentDefinition, ManifestSummary, StrikeInterval, BOUND_LOWER_INCLUSIVE,
    BOUND_LOWER_PRESENT, BOUND_UPPER_INCLUSIVE, BOUND_UPPER_PRESENT, LEG_LEN, SIZE_V1, SYMBOL_LEN,
};

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
        0x01, 0x02, 0x03, 0x04, 0x06, 0x07, 0x08, 0x09, 0x13, 0x14, 0x20, 0x22, 0x40, 0x41, 0x42,
        0x7F,
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
fn set_symbol_fits_a_non_ascii_value_as_utf8() {
    let mut d = sample();
    let fit = d.set_symbol("BTC-USDT\u{e9}");
    assert_eq!(fit, Fit::Fitted);
    assert_eq!(&d.symbol[..10], "BTC-USDT\u{e9}".as_bytes());
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

// ---------------------------------------------------------------------------
// StrikeInterval
// ---------------------------------------------------------------------------

const LOWER: u8 = BOUND_LOWER_PRESENT;
const LOWER_INCL: u8 = BOUND_LOWER_PRESENT | BOUND_LOWER_INCLUSIVE;
const UPPER: u8 = BOUND_UPPER_PRESENT;
const UPPER_INCL: u8 = BOUND_UPPER_PRESENT | BOUND_UPPER_INCLUSIVE;

/// "At or above 92,700.00 and below 92,799.99", fixed at a time the venue gave.
fn strike() -> StrikeInterval {
    StrikeInterval {
        instrument_id: 42,
        source_id: 7,
        strike_exponent: -2,
        bound_flags: LOWER_INCL | UPPER,
        lower_bound: 9_270_000,
        upper_bound: 9_279_999,
        fixing_time_ns: 1_700_000_000_000_000_000,
    }
}

/// A strike with these flags and bounds, and nothing else about it in question.
fn with(bound_flags: u8, lower_bound: i64, upper_bound: i64) -> StrikeInterval {
    StrikeInterval {
        bound_flags,
        lower_bound,
        upper_bound,
        fixing_time_ns: 0,
        ..strike()
    }
}

#[test]
fn strike_interval_fields_land_at_their_spec_offsets() {
    let mut b = [0xFFu8; StrikeInterval::SIZE];
    strike().encode_into(&mut b);

    assert_eq!(b.len(), 40);
    assert_eq!(b[0], 0x09, "offset 0: Type");
    assert_eq!(b[1], 40, "offset 1: Length");
    assert_eq!(&b[2..4], &[0, 0], "offset 2: Flags");
    assert_eq!(&b[4..8], &42u32.to_le_bytes(), "offset 4: Instrument ID");
    assert_eq!(&b[8..10], &7u16.to_le_bytes(), "offset 8: Source ID");
    assert_eq!(b[10], (-2i8) as u8, "offset 10: Strike Exponent");
    assert_eq!(b[11], 0b0111, "offset 11: Bound Flags");
    assert_eq!(
        &b[12..20],
        &9_270_000i64.to_le_bytes(),
        "offset 12: Lower Bound"
    );
    assert_eq!(
        &b[20..28],
        &9_279_999i64.to_le_bytes(),
        "offset 20: Upper Bound"
    );
    assert_eq!(
        &b[28..36],
        &1_700_000_000_000_000_000u64.to_le_bytes(),
        "offset 28: Fixing Time"
    );
    assert_eq!(
        &b[36..40],
        &[0; 4],
        "offset 36: Reserved, zeroed over whatever the buffer held"
    );
}

#[test]
fn strike_interval_round_trips() {
    let mut b = [0u8; StrikeInterval::SIZE];
    strike().encode_into(&mut b);
    assert_eq!(StrikeInterval::decode(&b).unwrap(), strike());
}

#[test]
fn a_negative_bound_and_a_positive_exponent_survive_the_round_trip() {
    // A settlement value can be below zero, a temperature for one, and a venue
    // can state a strike in thousands.
    let s = StrikeInterval {
        strike_exponent: 3,
        ..with(LOWER_INCL | UPPER_INCL, -15, -2)
    };
    let mut b = [0u8; StrikeInterval::SIZE];
    s.encode_into(&mut b);
    assert_eq!(StrikeInterval::decode(&b).unwrap(), s);
}

/// Every row of the specification's `Bound Flags` table, and the three other
/// values it names as valid.
#[test]
fn every_bound_flags_value_the_specification_lists_is_sendable() {
    for (flags, lower, upper, pays) in [
        (0b0001, 100, 0, "v > Lower Bound"),
        (0b0011, 100, 0, "v >= Lower Bound"),
        (0b0100, 0, 200, "v < Upper Bound"),
        (0b1100, 0, 200, "v <= Upper Bound"),
        (0b1111, 100, 200, "Lower Bound <= v <= Upper Bound"),
        (0b0101, 100, 200, "Lower Bound < v < Upper Bound"),
        (0b0111, 100, 200, "Lower Bound <= v < Upper Bound"),
        (0b1101, 100, 200, "Lower Bound < v <= Upper Bound"),
    ] {
        let s = with(flags, lower, upper);
        assert_eq!(s.defect(), None, "{flags:#06b}: {pays}");
        assert_eq!(s.validate(), Ok(()), "{flags:#06b}: {pays}");
        assert!(!s.is_pending(), "{flags:#06b}: {pays}");
    }
}

#[test]
fn a_pending_strike_has_no_bound_and_no_fixing_time() {
    let s = StrikeInterval::pending(42, 7, -2);
    assert!(s.is_pending());
    assert_eq!(s.validate(), Ok(()));
    assert_eq!(
        (
            s.bound_flags,
            s.lower_bound,
            s.upper_bound,
            s.fixing_time_ns
        ),
        (0, 0, 0, 0)
    );
    assert_eq!(
        (s.instrument_id, s.source_id, s.strike_exponent),
        (42, 7, -2)
    );
}

#[test]
fn a_point_is_an_interval_with_equal_inclusive_bounds() {
    assert_eq!(with(LOWER_INCL | UPPER_INCL, 9, 9).validate(), Ok(()));
}

/// `STRIKE.FIELDS`: the fields agree with `Bound Flags`.
#[test]
fn fields_that_disagree_with_bound_flags_are_refused() {
    let pending_with_a_time = StrikeInterval {
        fixing_time_ns: 1,
        ..with(0, 0, 0)
    };
    for (what, s) in [
        ("a reserved bit", with(LOWER | 0b0001_0000, 100, 0)),
        ("the top reserved bit", with(LOWER | 0b1000_0000, 100, 0)),
        (
            "lower inclusive, not present",
            with(BOUND_LOWER_INCLUSIVE | UPPER, 0, 200),
        ),
        (
            "upper inclusive, not present",
            with(LOWER | BOUND_UPPER_INCLUSIVE, 100, 0),
        ),
        ("a lower bound that is not present", with(UPPER, 100, 200)),
        ("an upper bound that is not present", with(LOWER, 100, 200)),
        ("a bound on a pending strike", with(0, 100, 0)),
        ("a fixing time on a pending strike", pending_with_a_time),
    ] {
        assert!(s.defect().is_some(), "{what}");
        assert!(
            matches!(s.validate(), Err(EncodeError::MalformedMessage { .. })),
            "{what}: {:?}",
            s.validate()
        );
    }
}

/// `STRIKE.INTERVAL_NOT_EMPTY`: with both bounds present, some value is inside.
#[test]
fn an_empty_interval_is_refused() {
    for (what, s) in [
        ("lower above upper", with(LOWER_INCL | UPPER_INCL, 201, 200)),
        ("equal, both exclusive", with(LOWER | UPPER, 200, 200)),
        ("equal, lower exclusive", with(LOWER | UPPER_INCL, 200, 200)),
        ("equal, upper exclusive", with(LOWER_INCL | UPPER, 200, 200)),
    ] {
        assert!(
            matches!(s.validate(), Err(EncodeError::MalformedMessage { .. })),
            "{what}: {:?}",
            s.validate()
        );
    }
}

#[test]
fn the_builder_refuses_a_strike_whose_fields_disagree() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Refdata, 1232);
    let err = b.push(&with(UPPER, 100, 200)).unwrap_err();
    assert_eq!(
        err,
        EncodeError::MalformedMessage {
            message: core::any::type_name::<StrikeInterval>(),
            what: "Lower Bound must be 0 when it is not present",
        }
    );
    assert!(b.is_empty(), "a refused message leaves nothing behind");
}

/// A reader meets bytes somebody else encoded. The specification gives it one
/// rule for every `Bound Flags` value and tells it to ignore the reserved bits,
/// so a body that breaks a publisher rule decodes, and `defect` says what is
/// wrong with it.
#[test]
fn a_strike_that_breaks_a_publisher_rule_still_decodes() {
    let sent = with(LOWER | 0b1000_0000, 100, 0);
    let mut b = [0u8; StrikeInterval::SIZE];
    sent.encode_into(&mut b);

    let read = StrikeInterval::decode(&b).unwrap();
    assert_eq!(read, sent);
    assert!(read.defect().is_some());
    assert!(
        !read.is_pending(),
        "the presence bits are read on their own"
    );
}

#[test]
fn a_reserved_bit_does_not_make_a_pending_strike_fixed() {
    assert!(with(0b1111_0000, 0, 0).is_pending());
}

#[test]
fn strike_interval_decode_rejects_a_buffer_shorter_than_the_fixed_size() {
    let mut b = [0u8; StrikeInterval::SIZE];
    strike().encode_into(&mut b);
    assert_eq!(
        StrikeInterval::decode(&b[..39]),
        Err(DecodeError::ShortBuffer { need: 40, got: 39 })
    );
}

#[test]
fn strike_interval_decode_rejects_a_declared_length_that_is_not_the_fixed_size() {
    let mut b = [0u8; StrikeInterval::SIZE];
    strike().encode_into(&mut b);
    b[1] = 24;
    assert_eq!(
        StrikeInterval::decode(&b),
        Err(DecodeError::LengthMismatch {
            type_id: 0x09,
            declared: 24,
            expected: 40,
        })
    );
}

#[test]
fn strike_interval_decode_rejects_a_type_id_that_is_not_its_own() {
    let mut b = [0u8; StrikeInterval::SIZE];
    strike().encode_into(&mut b);
    b[0] = 0x07;
    assert_eq!(
        StrikeInterval::decode(&b),
        Err(DecodeError::BadTypeId(0x07))
    );
}

#[test]
fn a_strike_interval_pushed_into_a_mktdata_builder_is_a_countable_error() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    assert_eq!(
        b.push(&strike()).unwrap_err(),
        EncodeError::WrongPortRole {
            message: core::any::type_name::<StrikeInterval>(),
            role: "mktdata",
        }
    );
}

/// The pair the specification describes: a definition and its strike are 170
/// bytes, and a datagram of the maximum size holds seven of them.
#[test]
fn seven_pairs_fill_a_datagram_and_an_eighth_does_not_fit() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Refdata, 1232);
    for _ in 0..7 {
        b.push(&sample()).unwrap();
        b.push(&strike()).unwrap();
    }
    assert_eq!(b.remaining(), 1232 - 24 - 7 * 170);
    assert!(
        matches!(b.push(&sample()), Err(EncodeError::DatagramFull { .. })),
        "18 bytes remain, and a definition is 130"
    );

    let out = b.finish(0).expect("datagram has messages");
    let datagram = dz_edge_core::Datagram::decode(&out, TestFeed::MAGIC).unwrap();
    let types: Vec<u8> = datagram.messages().map(|m| m.type_id).collect();
    assert_eq!(types, [0x02, 0x09].repeat(7));
}

/// What two separate pushes get wrong: after three bare definitions and four
/// pairs a definition still fits and its strike does not. The pair is refused
/// whole, and the builder is as it was.
#[test]
fn a_pair_whose_strike_does_not_fit_is_refused_whole() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Refdata, 1232);
    for _ in 0..3 {
        b.push(&sample()).unwrap();
    }
    for _ in 0..4 {
        b.push_pair(&sample(), &strike()).unwrap();
    }
    assert_eq!(
        b.remaining(),
        138,
        "room for a definition, not for the pair"
    );

    assert_eq!(
        b.push_pair(&sample(), &strike()),
        Err(EncodeError::DatagramFull {
            attempted: 1094 + 170,
            capacity: 1232,
        })
    );
    assert_eq!(b.remaining(), 138, "the definition was not written either");

    let out = b.finish(0).expect("datagram has messages");
    let datagram = dz_edge_core::Datagram::decode(&out, TestFeed::MAGIC).unwrap();
    assert_eq!(datagram.messages().len(), 11);
}

#[test]
fn a_pair_whose_strike_is_malformed_writes_neither_message() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Refdata, 1232);
    assert!(matches!(
        b.push_pair(&sample(), &with(UPPER, 100, 200)),
        Err(EncodeError::MalformedMessage { .. })
    ));
    assert!(b.is_empty());
}

#[test]
fn a_pair_on_a_port_role_one_of_them_may_not_use_writes_neither_message() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<TestFeed>::new(channel, PortRole::Mktdata, 1232);
    assert!(matches!(
        b.push_pair(&sample(), &strike()),
        Err(EncodeError::WrongPortRole { .. })
    ));
    assert!(b.is_empty());
}
