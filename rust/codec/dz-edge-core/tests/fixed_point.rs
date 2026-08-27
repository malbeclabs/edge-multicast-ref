use dz_edge_core::{parse_signed, parse_unsigned, ScaleError};

#[test]
fn basic_negative_exponent_conversions() {
    assert_eq!(parse_signed("79360.40", -2), Ok(7_936_040));
    assert_eq!(parse_signed("1.50", -2), Ok(150));
    assert_eq!(parse_signed("0", -2), Ok(0));
}

#[test]
fn trailing_zeros_beyond_the_exponent_discard_nothing() {
    assert_eq!(parse_signed("1.500", -2), Ok(150));
    assert_eq!(parse_signed("1.5000000", -2), Ok(150));
}

#[test]
fn excess_nonzero_digits_are_too_precise() {
    assert!(matches!(
        parse_signed("1.505", -2),
        Err(ScaleError::TooPrecise { beyond: 1 })
    ));
    assert!(matches!(
        parse_signed("1.5051", -2),
        Err(ScaleError::TooPrecise { beyond: 2 })
    ));
}

#[test]
fn exponent_zero_is_the_identity() {
    assert_eq!(parse_signed("1234", 0), Ok(1234));
    assert_eq!(parse_signed("1.0", 0), Ok(1));
    assert!(matches!(
        parse_signed("1.1", 0),
        Err(ScaleError::TooPrecise { beyond: 1 })
    ));
}

#[test]
fn positive_exponent_divides() {
    assert_eq!(parse_signed("1500", 2), Ok(15));
    // Discarded digits are "01": the last non-zero one sits 2 places past
    // the cut - the same shape as the "501" case in the table test above.
    assert!(matches!(
        parse_signed("1501", 2),
        Err(ScaleError::TooPrecise { beyond: 2 })
    ));
}

#[test]
fn signed_values_carry_the_sign() {
    assert_eq!(parse_signed("-1.50", -2), Ok(-150));
    assert_eq!(parse_signed("-0.01", -2), Ok(-1));
}

#[test]
fn parse_unsigned_refuses_any_sign() {
    assert!(matches!(
        parse_unsigned("-1", -2),
        Err(ScaleError::Malformed)
    ));
    assert!(matches!(
        parse_unsigned("-0", -2),
        Err(ScaleError::Malformed)
    ));
}

#[test]
fn grammar_refusals() {
    let bad = [
        "",
        " 1",
        "1 ",
        "1 2",
        "+1",
        "1e5",
        "1E5",
        ".",
        ".5",
        "5.",
        "1.2.3",
        "abc",
        "1a",
        "1\u{ff11}", // a digit-looking string containing a non-ASCII digit
    ];
    for text in bad {
        assert!(
            matches!(parse_signed(text, -2), Err(ScaleError::Malformed)),
            "expected Malformed for signed {text:?}"
        );
        // Only cases without a leading '-' are meaningful to re-check on the
        // unsigned path with the same expectation; all of the above qualify.
        assert!(
            matches!(parse_unsigned(text, -2), Err(ScaleError::Malformed)),
            "expected Malformed for unsigned {text:?}"
        );
    }
}

#[test]
fn a_bare_non_ascii_digit_is_malformed() {
    assert!(matches!(
        parse_signed("\u{ff11}", -2),
        Err(ScaleError::Malformed)
    ));
}

#[test]
fn overflow_past_i64_max() {
    // i64::MAX is 9223372036854775807; one more than that overflows.
    assert!(matches!(
        parse_signed("9223372036854775808", 0),
        Err(ScaleError::Overflow)
    ));
}

#[test]
fn overflow_past_u64_max() {
    // u64::MAX is 18446744073709551615; one more than that overflows.
    assert!(matches!(
        parse_unsigned("18446744073709551616", 0),
        Err(ScaleError::Overflow)
    ));
}

#[test]
fn a_very_long_digit_string_overflows_without_panicking() {
    let text = "9".repeat(4096);
    assert!(matches!(parse_signed(&text, 0), Err(ScaleError::Overflow)));
    assert!(matches!(
        parse_unsigned(&text, 0),
        Err(ScaleError::Overflow)
    ));
}

#[test]
fn a_very_long_fractional_string_does_not_panic() {
    let text = format!("1.{}", "9".repeat(4096));
    assert!(matches!(
        parse_signed(&text, 0),
        Err(ScaleError::TooPrecise { .. })
    ));
}

#[test]
fn i64_max_round_trips_at_exponent_zero() {
    assert_eq!(parse_signed("9223372036854775807", 0), Ok(i64::MAX));
}

#[test]
fn i64_min_round_trips_at_exponent_zero() {
    assert_eq!(parse_signed("-9223372036854775808", 0), Ok(i64::MIN));
}

#[test]
fn u64_max_round_trips_at_exponent_zero() {
    assert_eq!(parse_unsigned("18446744073709551615", 0), Ok(u64::MAX));
}

#[test]
fn deterministic_table_of_exponent_and_precision_combinations() {
    // A fixed, deterministic sequence of (text, exponent, expected) cases
    // spanning a range of exponents and precisions rather than a single
    // spot check.
    let exact: &[(&str, i8, i64)] = &[
        ("100", -2, 10_000),
        ("100", 2, 1),
        ("1", -1, 10),
        ("10", 1, 1),
        ("0.001", -3, 1),
        ("123.456", -3, 123_456),
        ("-123.456", -3, -123_456),
        ("5", 0, 5),
        ("500", 2, 5),
        ("5000", 3, 5),
    ];
    for &(text, exponent, expected) in exact {
        assert_eq!(
            parse_signed(text, exponent),
            Ok(expected),
            "text={text:?} exponent={exponent}"
        );
    }

    let too_precise: &[(&str, i8, u32)] = &[
        ("123.4567", -3, 1),
        // The discarded digits are "01": the last non-zero one sits 2
        // places past the cut, which is how far off the exponent is. Do
        // not "correct" this back to 1 - that would be counting discarded
        // non-zero digits again instead of measuring the distance, and the
        // two definitions disagree exactly here because a zero sits between
        // the cut and the non-zero digit.
        ("501", 2, 2),
        // Same shape: discarded digits are "001", so the last non-zero
        // digit sits 3 places past the cut.
        ("5001", 3, 3),
        ("0.0011", -3, 1),
        ("1.0005", -2, 2),
        ("0.000001", -2, 4),
        ("5", 3, 3),
        ("100.0000001", -3, 4),
    ];
    for &(text, exponent, beyond) in too_precise {
        assert!(
            matches!(
                parse_signed(text, exponent),
                Err(ScaleError::TooPrecise { beyond: b }) if b == beyond
            ),
            "text={text:?} exponent={exponent} expected beyond={beyond}"
        );
    }
}
