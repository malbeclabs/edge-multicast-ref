use dz_edge_core::{pad_ascii, pad_text, Fit};

#[test]
fn a_short_value_pads_with_nul_and_reports_fitted() {
    let (field, fit) = pad_text::<8>("BTC");
    assert_eq!(fit, Fit::Fitted);
    assert_eq!(&field, b"BTC\0\0\0\0\0");
}

#[test]
fn an_exact_width_value_reports_fitted() {
    let (field, fit) = pad_text::<8>("BTC-USDT");
    assert_eq!(fit, Fit::Fitted);
    assert_eq!(&field, b"BTC-USDT");
}

#[test]
fn an_over_long_ascii_value_truncates_to_n_and_reports_truncated() {
    let (field, fit) = pad_text::<8>("BTC-USDT-PERPETUAL");
    assert_eq!(fit, Fit::Truncated);
    assert_eq!(&field, b"BTC-USDT");
}

#[test]
fn a_multibyte_value_cut_mid_character_backs_up_to_the_boundary() {
    // 'é' (0xC3 0xA9) straddles the 8-byte cut: "1234567" is 7 bytes, so the
    // continuation byte of 'é' would land at index 8, right at N. The cut
    // must back up to the character boundary at 7, not split the code point.
    let value = "1234567\u{e9}\u{e9}";
    let (field, fit) = pad_text::<8>(value);
    assert_eq!(fit, Fit::Truncated);
    assert_eq!(&field[..7], b"1234567");
    assert_eq!(
        field[7], 0,
        "the half code point must not appear; the byte is zeroed"
    );
}

#[test]
fn a_non_ascii_value_that_fits_reports_fitted() {
    // "caf\u{e9}" ('caf' + 'é') is 5 bytes total: 'é' is the 2-byte UTF-8
    // sequence 0xC3 0xA9. `char[N]` is UTF-8 text, so it fits as written.
    let (field, fit) = pad_text::<8>("caf\u{e9}");
    assert_eq!(fit, Fit::Fitted);
    assert_eq!(&field[..3], b"caf");
    assert_eq!(field[3], 0xc3);
    assert_eq!(field[4], 0xa9);
    assert_eq!(&field[5..], &[0, 0, 0]);
}

#[test]
fn an_interior_nul_reports_unrepresentable() {
    // A NUL inside the value, not just past its end, ends the text for every
    // reader.
    let (field, fit) = pad_text::<8>("AB\0CD");
    assert_eq!(fit, Fit::Unrepresentable);
    assert_eq!(&field[..5], b"AB\0CD");
    assert_eq!(&field[5..], &[0, 0, 0]);
}

#[test]
fn a_non_ascii_value_that_is_too_long_reports_truncated() {
    let value = "1234567890\u{e9}";
    let (field, fit) = pad_text::<8>(value);
    assert_eq!(fit, Fit::Truncated);
    assert_eq!(&field, b"12345678");
}

#[test]
fn an_interior_nul_that_is_also_too_long_reports_unrepresentable_not_truncated() {
    // Unrepresentable takes precedence over Truncated when a value is both.
    let (field, fit) = pad_text::<8>("AB\0CDEFGHIJ");
    assert_eq!(fit, Fit::Unrepresentable);
    assert_eq!(&field, b"AB\0CDEFG");
}

#[test]
fn a_multibyte_value_cut_inside_a_three_byte_character_backs_up_to_the_boundary() {
    // Two three-byte CJK characters and a third that straddles the cut: 6
    // bytes fit, the 7th and 8th would be the first two bytes of the third.
    let (field, fit) = pad_text::<8>("\u{4e00}\u{4e8c}\u{4e09}");
    assert_eq!(fit, Fit::Truncated);
    assert_eq!(&field[..6], "\u{4e00}\u{4e8c}".as_bytes());
    assert_eq!(&field[6..], &[0, 0]);
    assert!(core::str::from_utf8(&field[..6]).is_ok());
}

#[test]
fn pad_ascii_is_pad_text() {
    for value in ["BTC", "caf\u{e9}", "AB\0CD", "1234567890\u{e9}"] {
        assert_eq!(pad_ascii::<8>(value), pad_text::<8>(value), "{value:?}");
    }
}

#[test]
fn the_remainder_is_always_zeroed() {
    let (field, fit) = pad_text::<16>("BTC");
    assert_eq!(fit, Fit::Fitted);
    assert_eq!(
        &field[3..],
        &[0u8; 13][..],
        "bytes past a fitted value must be NUL"
    );

    let (field, fit) = pad_text::<8>("AB");
    assert_eq!(fit, Fit::Fitted);
    assert_eq!(
        &field[2..],
        &[0u8; 6][..],
        "bytes past a fitted value must be NUL"
    );
}
