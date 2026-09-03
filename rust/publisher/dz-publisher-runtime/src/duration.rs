//! Durations in the configuration document, and a note about where this belongs.
//!
//! A duration is written with its unit — `"30s"`, `"500ms"` — and the unit is
//! not optional. One existing publisher suffixes its duration keys `_seconds`
//! and takes integers while others parse strings, so `30` is thirty of
//! something and picking a unit for it is how a heartbeat interval becomes
//! thirty milliseconds.
//!
//! # This is the third copy in this repository, and the second one said so
//!
//! `dz-ingress-core`'s own duration parser carries the note: *"This is the
//! second implementation of it in this repository, and it should be the last:
//! the third is the point at which it needs one home rather than a copy per
//! crate that parses a section."* This is that third. It is written here anyway
//! because the alternative was to leave `[[feed]] heartbeat_interval` unparsed,
//! and because the function it would reuse — `dz_ingress_core::config`'s
//! `parse_duration` — is private and this crate may not reach into another.
//!
//! **What is owed:** one home for it. The smallest change that pays the debt is
//! `dz_ingress_core::config::parse_duration` becoming `pub` (it already carries
//! the tests that pin the syntax), and this module becoming a re-export. Until
//! then the two copies are held to each other only by the syntax test below
//! being a transcription of the ingress crate's, which is a control a reader has
//! to notice rather than one a build enforces.

use std::time::Duration;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer};

/// Parse `"<whole number><unit>"`, where the unit is one of `ns`, `us`, `ms`,
/// `s`, `m`, `h`.
pub(crate) fn parse_duration(raw: &str) -> Result<Duration, String> {
    let digits = raw
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| format!("`{raw}` has no unit (ns, us, ms, s, m, h)"))?;
    if digits == 0 {
        return Err(format!("`{raw}` does not start with a number"));
    }
    let (value, unit) = raw.split_at(digits);
    let value: u64 = value
        .parse()
        .map_err(|_| format!("`{value}` is not a whole number"))?;
    let nanos = match unit {
        "ns" => 1,
        "us" => 1_000,
        "ms" => 1_000_000,
        "s" => 1_000_000_000,
        "m" => 60 * 1_000_000_000,
        "h" => 3_600 * 1_000_000_000,
        _ => {
            return Err(format!(
                "`{unit}` is not a duration unit (ns, us, ms, s, m, h)"
            ))
        }
    };
    value
        .checked_mul(nanos)
        .map(Duration::from_nanos)
        .ok_or_else(|| format!("`{raw}` does not fit in a 64-bit nanosecond count"))
}

pub(crate) fn de_duration<'de, D: Deserializer<'de>>(de: D) -> Result<Duration, D::Error> {
    let raw = String::deserialize(de)?;
    parse_duration(&raw).map_err(D::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Transcribed from `dz-ingress-core`'s own test of the same syntax. It is
    /// the only thing holding the two copies to each other; see the module
    /// note.
    #[test]
    fn the_units_convert_as_the_ingress_crate_spells_them() {
        assert_eq!(parse_duration("500ms"), Ok(Duration::from_millis(500)));
        assert_eq!(parse_duration("30s"), Ok(Duration::from_secs(30)));
        assert_eq!(parse_duration("2m"), Ok(Duration::from_secs(120)));
        assert_eq!(parse_duration("1h"), Ok(Duration::from_secs(3_600)));
        assert_eq!(parse_duration("250us"), Ok(Duration::from_micros(250)));
        assert_eq!(parse_duration("40ns"), Ok(Duration::from_nanos(40)));
    }

    #[test]
    fn a_bare_number_is_refused_rather_than_guessed_at() {
        let error = parse_duration("60").expect_err("a bare number has no unit");
        assert!(error.contains("no unit"), "{error}");
    }

    #[test]
    fn every_unit_the_error_message_offers_is_a_unit_the_parser_takes() {
        for unit in ["ns", "us", "ms", "s", "m", "h"] {
            assert!(
                parse_duration(&format!("1{unit}")).is_ok(),
                "`1{unit}` was offered by the error message and refused"
            );
        }
    }
}
