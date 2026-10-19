use dz_edge_core::{
    AppMessage, ChannelSequence, DatagramBuilder, DecodeError, EncodeError, Feed, PortRole,
    ResetCount,
};
use dz_edge_midpoint::{
    is_book_method, is_trade_method, Midpoint, MidpointFeed, MidpointRuleError, MAGIC_MIDPOINT,
    METHOD_BBO_MID, METHOD_CUSTOM, METHOD_LAST_TRADE_FALLBACK, METHOD_MICRO_PRICE, METHOD_UNKNOWN,
    METHOD_VWAP, METHOD_WEIGHTED_MID, QUALITY_CROSSED_OR_LOCKED, QUALITY_ONE_SIDED, QUALITY_STALE,
    QUALITY_SYNTHETIC,
};

#[test]
fn magic_midpoint_matches_the_spec() {
    assert_eq!(
        MAGIC_MIDPOINT, 0x4D44,
        "\"DM\", midpoint datagram delimiter"
    );
}

#[test]
fn the_feed_carries_the_spec_message_table() {
    assert_eq!(MidpointFeed::CARRIES, &[0x01, 0x02, 0x03, 0x06, 0x07]);
    assert!(
        !MidpointFeed::carries(0x04),
        "0x04 is Trade in top-of-book and reserved here"
    );
}

#[test]
fn method_and_quality_values_match_the_spec() {
    assert_eq!(
        [
            METHOD_UNKNOWN,
            METHOD_BBO_MID,
            METHOD_MICRO_PRICE,
            METHOD_WEIGHTED_MID,
            METHOD_LAST_TRADE_FALLBACK,
            METHOD_VWAP,
            METHOD_CUSTOM,
        ],
        [0, 1, 2, 3, 4, 5, 255]
    );
    assert_eq!(
        [
            QUALITY_STALE,
            QUALITY_ONE_SIDED,
            QUALITY_CROSSED_OR_LOCKED,
            QUALITY_SYNTHETIC,
        ],
        [0x01, 0x02, 0x04, 0x08]
    );
}

#[test]
fn every_method_value_has_one_classification_at_most() {
    for m in 0..=u8::MAX {
        assert!(!(is_book_method(m) && is_trade_method(m)), "method {m}");
    }
    assert_eq!((0..=u8::MAX).filter(|m| is_book_method(*m)).count(), 3);
    assert_eq!((0..=u8::MAX).filter(|m| is_trade_method(*m)).count(), 2);
}

fn sample() -> Midpoint {
    Midpoint {
        instrument_id: 0x1112_1314,
        source_id: 0x2122,
        method: METHOD_VWAP,
        quality_flags: QUALITY_STALE,
        input_timestamp_ns: 0x3132_3334_3536_3738,
        compute_timestamp_ns: 0x4142_4344_4546_4748,
        price: -12_345,
        window_ms: 0x5152_5354,
        volume: 0x6162_6364_6566_6768,
    }
}

#[test]
fn fields_land_at_their_spec_offsets() {
    let m = sample();
    let mut b = [0u8; Midpoint::SIZE];
    m.encode_into(&mut b);

    assert_eq!(b.len(), 48);
    assert_eq!(b[0], 0x03, "offset 0: Type");
    assert_eq!(b[1], 48, "offset 1: Length");
    assert_eq!(&b[2..4], &[0, 0], "offset 2: Flags");
    assert_eq!(
        &b[4..8],
        &0x1112_1314u32.to_le_bytes(),
        "offset 4: Instrument ID"
    );
    assert_eq!(&b[8..10], &0x2122u16.to_le_bytes(), "offset 8: Source ID");
    assert_eq!(b[10], METHOD_VWAP, "offset 10: Method");
    assert_eq!(b[11], QUALITY_STALE, "offset 11: Quality Flags");
    assert_eq!(
        &b[12..20],
        &0x3132_3334_3536_3738u64.to_le_bytes(),
        "offset 12: Input Timestamp"
    );
    assert_eq!(
        &b[20..28],
        &0x4142_4344_4546_4748u64.to_le_bytes(),
        "offset 20: Compute Timestamp"
    );
    assert_eq!(&b[28..36], &(-12_345i64).to_le_bytes(), "offset 28: Price");
    assert_eq!(
        &b[36..40],
        &0x5152_5354u32.to_le_bytes(),
        "offset 36: Window"
    );
    assert_eq!(
        &b[40..48],
        &0x6162_6364_6566_6768u64.to_le_bytes(),
        "offset 40: Volume"
    );
}

#[test]
fn round_trips() {
    let m = sample();
    let mut b = [0u8; Midpoint::SIZE];
    m.encode_into(&mut b);
    assert_eq!(Midpoint::decode(&b).unwrap(), m);
}

#[test]
fn a_negative_price_survives_the_round_trip() {
    let mut m = sample();
    m.price = i64::MIN + 1;
    let mut b = [0u8; Midpoint::SIZE];
    m.encode_into(&mut b);
    assert_eq!(Midpoint::decode(&b).unwrap().price, i64::MIN + 1);
}

#[test]
fn decode_rejects_a_declared_length_that_is_not_the_fixed_size() {
    let mut b = [0u8; Midpoint::SIZE];
    sample().encode_into(&mut b);
    // 40 is the 1.x length. A 1.x message must not decode at the 3.x layout.
    b[1] = 40;
    assert!(matches!(
        Midpoint::decode(&b),
        Err(DecodeError::LengthMismatch {
            type_id: 0x03,
            declared: 40,
            expected: 48
        })
    ));
}

#[test]
fn decode_rejects_a_buffer_shorter_than_the_fixed_size() {
    let mut b = [0u8; Midpoint::SIZE];
    sample().encode_into(&mut b);
    assert!(matches!(
        Midpoint::decode(&b[..47]),
        Err(DecodeError::ShortBuffer { need: 48, got: 47 })
    ));
}

#[test]
fn decode_rejects_a_type_id_that_is_not_its_own() {
    let mut b = [0u8; Midpoint::SIZE];
    sample().encode_into(&mut b);
    b[0] = 0x04;
    assert!(matches!(
        Midpoint::decode(&b),
        Err(DecodeError::BadTypeId(0x04))
    ));
}

#[test]
fn decode_keeps_a_message_that_breaks_a_field_rule() {
    let mut m = sample();
    m.method = METHOD_UNKNOWN;
    let mut b = [0u8; Midpoint::SIZE];
    m.encode_into(&mut b);
    let decoded = Midpoint::decode(&b).unwrap();
    assert_eq!(decoded.check(), Err(MidpointRuleError::MethodUnknown));
}

fn vwap() -> Midpoint {
    Midpoint {
        instrument_id: 1,
        source_id: 2,
        method: METHOD_VWAP,
        quality_flags: 0,
        input_timestamp_ns: 10_000_000_000,
        compute_timestamp_ns: 10_000_100_000,
        price: 100,
        window_ms: 1_000,
        volume: 7,
    }
}

fn bbo_mid() -> Midpoint {
    Midpoint {
        method: METHOD_BBO_MID,
        window_ms: 0,
        volume: 0,
        ..vwap()
    }
}

#[test]
fn check_accepts_a_conformant_message_of_each_kind() {
    assert_eq!(vwap().check(), Ok(()));
    assert_eq!(bbo_mid().check(), Ok(()));
    let crossed = Midpoint {
        quality_flags: QUALITY_ONE_SIDED | QUALITY_CROSSED_OR_LOCKED,
        ..bbo_mid()
    };
    assert_eq!(crossed.check(), Ok(()), "book flags on a book method");
    let empty = Midpoint {
        volume: 0,
        quality_flags: QUALITY_SYNTHETIC,
        ..vwap()
    };
    assert_eq!(empty.check(), Ok(()), "an empty window, carried over");
}

#[test]
fn check_refuses_method_unknown() {
    let m = Midpoint {
        method: METHOD_UNKNOWN,
        ..bbo_mid()
    };
    assert_eq!(m.check(), Err(MidpointRuleError::MethodUnknown));
}

#[test]
fn check_refuses_a_vwap_with_no_window() {
    let m = Midpoint {
        window_ms: 0,
        ..vwap()
    };
    assert_eq!(m.check(), Err(MidpointRuleError::VwapWindowZero));
}

#[test]
fn check_refuses_an_empty_vwap_window_without_the_synthetic_bit() {
    let m = Midpoint {
        volume: 0,
        quality_flags: QUALITY_STALE,
        ..vwap()
    };
    assert_eq!(
        m.check(),
        Err(MidpointRuleError::VwapEmptyWindowNotSynthetic)
    );
}

#[test]
fn check_refuses_window_or_volume_on_every_other_method() {
    for method in [
        METHOD_BBO_MID,
        METHOD_MICRO_PRICE,
        METHOD_WEIGHTED_MID,
        METHOD_LAST_TRADE_FALLBACK,
        METHOD_CUSTOM,
    ] {
        let m = Midpoint {
            method,
            window_ms: 1,
            ..bbo_mid()
        };
        assert_eq!(
            m.check(),
            Err(MidpointRuleError::WindowNotZero {
                method,
                window_ms: 1
            })
        );
        let m = Midpoint {
            method,
            volume: 1,
            ..bbo_mid()
        };
        assert_eq!(
            m.check(),
            Err(MidpointRuleError::VolumeNotZero { method, volume: 1 })
        );
    }
}

#[test]
fn check_refuses_a_book_flag_on_a_trade_method() {
    for flag in [QUALITY_ONE_SIDED, QUALITY_CROSSED_OR_LOCKED] {
        let m = Midpoint {
            quality_flags: flag,
            ..vwap()
        };
        assert_eq!(
            m.check(),
            Err(MidpointRuleError::BookFlagOnTradeMethod {
                method: METHOD_VWAP,
                quality_flags: flag
            })
        );
        let m = Midpoint {
            method: METHOD_LAST_TRADE_FALLBACK,
            quality_flags: flag,
            ..bbo_mid()
        };
        assert!(matches!(
            m.check(),
            Err(MidpointRuleError::BookFlagOnTradeMethod { .. })
        ));
    }
}

#[test]
fn the_vwap_window_ends_at_the_input_timestamp() {
    // (Input Timestamp − Window, Input Timestamp], Window in milliseconds.
    assert_eq!(vwap().window_ns(), Some((9_000_000_000, 10_000_000_000)));
    assert_eq!(bbo_mid().window_ns(), None);
    let early = Midpoint {
        input_timestamp_ns: 5,
        ..vwap()
    };
    assert_eq!(early.window_ns(), Some((0, 5)), "the start saturates");
}

#[test]
fn a_midpoint_on_mktdata_succeeds() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<MidpointFeed>::new(channel, PortRole::Mktdata, 1232);
    b.push(&sample()).unwrap();
}

#[test]
fn a_midpoint_pushed_into_a_refdata_builder_is_a_countable_error() {
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<MidpointFeed>::new(channel, PortRole::Refdata, 1232);
    let err = b.push(&sample()).unwrap_err();
    assert_eq!(
        err,
        EncodeError::WrongPortRole {
            message: core::any::type_name::<Midpoint>(),
            role: "refdata",
        }
    );
}

#[test]
fn twenty_five_midpoints_fill_one_datagram() {
    // The spec's Wire Efficiency example: 25 × 48 + 24 = 1,224 bytes.
    let channel = ChannelSequence::new(0, ResetCount(0));
    let mut b = DatagramBuilder::<MidpointFeed>::new(channel, PortRole::Mktdata, 1232);
    for _ in 0..25 {
        b.push(&sample()).unwrap();
    }
    assert!(
        matches!(b.push(&sample()), Err(EncodeError::DatagramFull { .. })),
        "a 26th does not fit"
    );
}
