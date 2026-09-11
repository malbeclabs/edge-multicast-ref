//! `SendingTime`, in the one format this crate writes.
//!
//! # Why this is thirty lines of arithmetic and not a date library
//!
//! One field, one direction, one format, UTC only, and it is on the connection
//! path rather than the payload path — so what a date library would buy is a
//! dependency every venue linking this crate inherits, for a conversion whose
//! whole content is two divisions and a table of month lengths. The conversion
//! itself is Howard Hinnant's `civil_from_days`, which is the standard one and
//! is exact for every day this protocol will see.
//!
//! # Milliseconds, and what a venue expecting more would see
//!
//! The protocol's `UTCTimestamp` admits second, millisecond, microsecond and
//! nanosecond precision, and this crate writes milliseconds. That is the
//! precision the field is defined at in the version this crate composes, and it
//! is what every engine on the other side parses. A venue that requires finer
//! precision on a *session* message is one to find out about here rather than
//! from a reject, so the format is stated in one function with one test over
//! known values rather than assembled at each call site.

/// The number of days from 1970-01-01 to 0000-03-01, which is the origin the
/// civil conversion counts from.
///
/// Named rather than inline because it is the one constant in the algorithm
/// that is not derivable by looking at it.
const DAYS_FROM_CIVIL_ORIGIN: i64 = 719_468;

/// Nanoseconds in a second.
const NANOS_PER_SEC: u64 = 1_000_000_000;

/// Seconds in a day.
const SECS_PER_DAY: u64 = 86_400;

/// `YYYYMMDD-HH:MM:SS.sss` in UTC, for a wall-clock reading in nanoseconds
/// since 1970.
///
/// `wall_ns` is [`Clock::wall_ns`](dz_ingress_core::Clock::wall_ns), which is
/// the wall clock and not the steady one: this value is compared by the venue
/// against its own clock, and a monotonic reading has an arbitrary origin that
/// makes the comparison meaningless.
#[must_use]
pub fn sending_time(wall_ns: u64) -> String {
    let secs = wall_ns / NANOS_PER_SEC;
    let millis = (wall_ns % NANOS_PER_SEC) / 1_000_000;
    let days = secs / SECS_PER_DAY;
    let within = secs % SECS_PER_DAY;
    let (year, month, day) = civil_from_days(days);
    let hour = within / 3_600;
    let minute = (within % 3_600) / 60;
    let second = within % 60;
    format!("{year:04}{month:02}{day:02}-{hour:02}:{minute:02}:{second:02}.{millis:03}")
}

/// The civil date for a count of days since 1970-01-01.
///
/// Hinnant's algorithm, which shifts the year to start in March so that the
/// leap day falls at the end of it and the month-length pattern becomes an
/// arithmetic progression. Exact for every date this protocol reaches.
fn civil_from_days(days: u64) -> (u64, u64, u64) {
    // The shift is what lets the rest be arithmetic rather than a table, and it
    // is the reason this is signed for one line.
    let shifted = i64::try_from(days).unwrap_or(i64::MAX) + DAYS_FROM_CIVIL_ORIGIN;
    // The 400-year cycle, over which the calendar repeats exactly. Spelled
    // `cycle` rather than by the algorithm's own name for it, which is a word
    // this repository's glossary has already given to the publisher's own.
    let cycle = shifted / 146_097;
    let day_of_cycle = shifted - cycle * 146_097;
    let year_of_cycle = (day_of_cycle - day_of_cycle / 1_460 + day_of_cycle / 36_524
        - day_of_cycle / 146_096)
        / 365;
    let year = year_of_cycle + cycle * 400;
    let day_of_year =
        day_of_cycle - (365 * year_of_cycle + year_of_cycle / 4 - year_of_cycle / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = if month <= 2 { year + 1 } else { year };
    // Every value is a calendar component of a date at or after 1970, so none
    // of them is negative; the conversion is infallible in fact and the
    // fallbacks exist so that it is infallible in the types.
    (
        u64::try_from(year).unwrap_or(0),
        u64::try_from(month).unwrap_or(1),
        u64::try_from(day).unwrap_or(1),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nanoseconds for a UTC date and time, computed the long way round.
    ///
    /// Deliberately not by calling [`civil_from_days`] backwards: a conversion
    /// checked against its own inverse agrees with its own mistake, so the
    /// expected values below are day counts somebody can verify against a
    /// calendar.
    fn at(day_count: u64, hour: u64, minute: u64, second: u64, millis: u64) -> u64 {
        ((day_count * SECS_PER_DAY) + hour * 3_600 + minute * 60 + second) * NANOS_PER_SEC
            + millis * 1_000_000
    }

    #[test]
    fn the_origin_is_the_first_day_of_1970() {
        assert_eq!(sending_time(0), "19700101-00:00:00.000");
    }

    #[test]
    fn the_format_is_the_one_the_protocol_defines() {
        // 2026-09-09 is day 20705 since 1970-01-01: 56 years of 365 days plus
        // 14 leap days to 2026-01-01 (day 20454), plus 251 days to September 9.
        assert_eq!(
            sending_time(at(20_705, 11, 56, 50, 123)),
            "20260909-11:56:50.123"
        );
    }

    #[test]
    fn a_leap_day_is_the_day_it_is_and_not_the_first_of_march() {
        // Day 789 is 1972-02-29: two years of 365 days is 730 to 1972-01-01,
        // plus 31 days of January, plus 28 more to reach the 29th of February.
        assert_eq!(sending_time(at(789, 0, 0, 0, 0)), "19720229-00:00:00.000");
        assert_eq!(sending_time(at(790, 0, 0, 0, 0)), "19720301-00:00:00.000");
    }

    #[test]
    fn a_century_that_is_not_a_leap_year_does_not_gain_a_day() {
        // 2100 is divisible by 100 and not by 400, so it has no February 29th.
        // The naive rule would put this date one day earlier, which is the
        // failure the 400-year cycle arithmetic exists to avoid.
        //
        // Day 47_482 is 2100-01-01: 130 years from 1970, of which 32 are leap
        // (1972 through 2096 inclusive, every fourth year, none of them a
        // non-leap century).
        assert_eq!(
            sending_time(at(47_482, 0, 0, 0, 0)),
            "21000101-00:00:00.000"
        );
        assert_eq!(
            sending_time(at(47_541, 0, 0, 0, 0)),
            "21000301-00:00:00.000"
        );
    }

    #[test]
    fn the_milliseconds_are_truncated_and_never_rounded() {
        // A rounded stamp can name a millisecond that has not arrived, which is
        // a `SendingTime` in the future — the one value a venue's own clock
        // check refuses.
        assert_eq!(sending_time(1_999_999), "19700101-00:00:00.001");
        assert_eq!(sending_time(999_999), "19700101-00:00:00.000");
    }

    #[test]
    fn every_field_is_fixed_width() {
        // The width is what a fixed-format parser on the other side depends on,
        // and a single-digit month is the way it breaks.
        for day in [0, 1, 59, 789, 20_705, 47_541] {
            let rendered = sending_time(at(day, 1, 2, 3, 4));
            assert_eq!(rendered.len(), 21, "{rendered}");
            assert_eq!(&rendered[8..9], "-", "{rendered}");
            assert_eq!(&rendered[17..18], ".", "{rendered}");
        }
    }
}
