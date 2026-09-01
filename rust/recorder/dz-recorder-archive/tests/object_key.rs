use dz_recorder_archive::object_key::{object_key, utc_parts};

/// Seconds since the epoch for a UTC civil date, computed the other way round
/// from the code under test so the two are not the same arithmetic twice.
fn epoch_ns(year: i64, month: u32, day: u32, hour: u32) -> u64 {
    let mut days: i64 = 0;
    for y in 1970..year {
        days += if leap(y) { 366 } else { 365 };
    }
    let lengths = [
        31,
        if leap(year) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    for m in 1..month {
        days += lengths[(m - 1) as usize];
    }
    days += i64::from(day) - 1;
    ((days * 86_400 + i64::from(hour) * 3_600) as u64) * 1_000_000_000
}

fn leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

#[test]
fn the_key_carries_the_partitions_a_cross_site_query_prunes_on() {
    let key = object_key(
        "top-of-book",
        "prod",
        "site-a",
        "recorder-01",
        epoch_ns(2026, 8, 31, 18),
        "1000-2000-5.pcapng.zst",
    );
    assert_eq!(
        key,
        "feed=top-of-book/env=prod/site=site-a/recorder=recorder-01/\
         date=2026-08-31/hour=18/1000-2000-5.pcapng.zst"
    );
}

#[test]
fn two_recorders_rotating_the_same_segment_do_not_collide() {
    // A bare filename is not an object key. Reprocessing is idempotent on
    // (object key, sha256), so two sites naming one segment the same way would
    // make one of the two archives invisible to a re-run.
    let ns = epoch_ns(2026, 8, 31, 18);
    let a = object_key(
        "top-of-book",
        "prod",
        "site-a",
        "recorder-01",
        ns,
        "1-2-5.pcapng.zst",
    );
    let b = object_key(
        "top-of-book",
        "prod",
        "site-b",
        "recorder-02",
        ns,
        "1-2-5.pcapng.zst",
    );
    assert_ne!(a, b);
}

#[test]
fn the_date_parts_are_utc_and_correct_across_the_awkward_cases() {
    // Leap day, the day after it, a century that is not a leap year, one that
    // is, and both ends of a day.
    for (y, m, d, h) in [
        (1970, 1, 1, 0),
        (2000, 2, 29, 23),
        (2024, 2, 29, 12),
        (2100, 3, 1, 0),
        (2026, 12, 31, 23),
        (2026, 1, 1, 0),
    ] {
        assert_eq!(
            utc_parts(epoch_ns(y, m, d, h)),
            (y, m, d, h),
            "{y}-{m}-{d} {h}h"
        );
    }
}

#[test]
fn an_hour_boundary_lands_in_the_hour_that_starts_it() {
    let ns = epoch_ns(2026, 8, 31, 18);
    assert_eq!(utc_parts(ns).3, 18);
    assert_eq!(
        utc_parts(ns + 3_599_999_999_999).3,
        18,
        "the last nanosecond of the hour"
    );
    assert_eq!(utc_parts(ns + 3_600_000_000_000).3, 19);
}
