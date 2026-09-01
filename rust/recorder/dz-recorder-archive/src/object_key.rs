//! Where an object lands, decided here rather than by whatever ships it.
//!
//! The recorder does not upload — `completed_dir` is the whole interface to a
//! shipper — but it does decide the layout, and the manifest states it. That
//! split keeps a shipper as dumb as possible, which was the argument for not
//! writing one: moving immutable hashed files is a solved problem, and a
//! solved problem stays solved only if nobody has to teach it a partitioning
//! scheme.
//!
//! It also fixes the key the analysis tier reprocesses on. Reprocessing is
//! idempotent on `(object key, sha256)`, and a bare filename is not an object
//! key: two recorders at two sites rotate segment 5 at the same nanosecond and
//! produce the same name for different bytes. The partitioned key cannot
//! collide, because the site and the recorder are in it.

/// The Hive-partitioned key an object is to land under, relative to a bucket.
///
/// Hive partitioning is what lets an object store be queried as a table without
/// a separate catalogue, and `site` and `recorder` are what make a cross-site
/// comparison a partition prune rather than a full scan.
#[must_use]
pub fn object_key(
    feed: &str,
    env: &str,
    site: &str,
    recorder: &str,
    start_ns: u64,
    file_name: &str,
) -> String {
    let (year, month, day, hour) = utc_parts(start_ns);
    format!(
        "feed={feed}/env={env}/site={site}/recorder={recorder}/\
         date={year:04}-{month:02}-{day:02}/hour={hour:02}/{file_name}"
    )
}

/// Year, month, day and hour in UTC for a nanosecond epoch.
///
/// Hand-rolled because a partition prefix is not worth a date dependency in a
/// crate that has none, and because the arithmetic is fixed: no zones, no
/// leap seconds, no locale.
#[must_use]
pub fn utc_parts(ns: u64) -> (i64, u32, u32, u32) {
    let secs = (ns / 1_000_000_000) as i64;
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    (year, month, day, (secs_of_day / 3_600) as u32)
}

/// Days since 1970-01-01 to a civil date.
///
/// Hinnant's algorithm. Its 400-year period is called an *era* in the original;
/// it is `cycle` here, because in this project `era` means the sequence space a
/// `Reset Count` opens and one word cannot mean both.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let cycle = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doc = z - cycle * 146_097; // day of cycle, [0, 146096]
    let yoc = (doc - doc / 1_460 + doc / 36_524 - doc / 146_096) / 365; // [0, 399]
    let y = yoc + cycle * 400;
    let doy = doc - (365 * yoc + yoc / 4 - yoc / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11], March-based
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (y + i64::from(m <= 2), m as u32, d as u32)
}
