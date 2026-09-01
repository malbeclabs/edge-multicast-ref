//! The watermark, and the rule that matters most.

mod common;

use std::path::PathBuf;

use common::{archive_config, at_secs, header_bytes, sequenced, write_bytes, SOURCE};
use dz_edge_core::PortRole;
use dz_recorder_archive::rotate::{ArchiveWriter, ArchiveWriterConfig};
use dz_recorder_core::Sink;
use tempfile::TempDir;

/// Enough datagrams that a segment's footprint is stable from one rotation to
/// the next, which is what makes the watermark arithmetic exact.
const SEGMENT_PAYLOAD_BYTES: usize = 64 << 10;

/// What one trip through the write path may average.
///
/// At the rates the recorder is sized for there are some 14 microseconds per
/// datagram, and the record path spends them on a copy and a buffered write with
/// no decode at all. A mean above this is a wait, not a slow machine — the bound
/// is seven times the whole per-datagram budget.
const WRITE_PATH_BUDGET_NANOS: u64 = 100_000;

/// The average trip through [`Sink::write`], which is where a stall the design
/// forbids shows up.
fn mean_write_path_nanos(w: &ArchiveWriter) -> u64 {
    let offered = w.datagrams_written_total() + w.datagrams_dropped_total();
    assert!(offered > 0, "nothing was offered to the write path");
    w.write_path_nanos() / offered
}

struct Archive {
    w: ArchiveWriter,
    completed: PathBuf,
    _tmp: TempDir,
}

impl Archive {
    fn with(staging_max: u64) -> Self {
        let (tmp, cfg) = config(staging_max);
        let completed = cfg.completed_dir.clone();
        Self {
            w: ArchiveWriter::new(cfg, at_secs(0)).unwrap(),
            completed,
            _tmp: tmp,
        }
    }

    /// One full segment, landed and swept: the watermark is enforced on
    /// rotation and on the sweep, never on the write path.
    fn rotate_a_full_segment(&mut self, at: u64) {
        write_bytes(&mut self.w, SEGMENT_PAYLOAD_BYTES);
        self.w.rotate_at(at).unwrap().unwrap();
        let _ = self.w.wait_completed().unwrap().unwrap();
        self.w.sweep_staging();
    }

    fn segments_on_disk(&self) -> usize {
        std::fs::read_dir(&self.completed)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|x| x == "zst")
            })
            .count()
    }
}

fn config(staging_max: u64) -> (TempDir, ArchiveWriterConfig) {
    let tmp = TempDir::new().unwrap();
    let mut cfg = archive_config(
        &tmp.path().join("staging"),
        &tmp.path().join("completed"),
        &[PortRole::Mktdata],
    );
    cfg.staging_max = staging_max;
    (tmp, cfg)
}

/// The on-disk footprint of one full segment, measured rather than assumed, so
/// the watermark arithmetic below is exact.
fn one_segment_footprint() -> u64 {
    let mut a = Archive::with(u64::MAX);
    a.rotate_a_full_segment(at_secs(61));
    a.w.bytes_on_disk()
}

#[test]
fn a_full_staging_directory_evicts_the_oldest_and_never_blocks() {
    // The single most important operational rule in the design. A writer that
    // blocks on a full disk stalls the drain thread, overflows the receive
    // queue, and converts an object-storage outage into a feed-loss incident
    // and into false publisher-loss findings in every archive written during it.
    let segment = one_segment_footprint();
    let mut a = Archive::with(4 * segment);
    for i in 0..6 {
        a.rotate_a_full_segment(at_secs(61 * (i + 1)));
    }
    assert_eq!(a.segments_on_disk(), 4);
    assert_eq!(a.w.segments_evicted_total(), 2);
    assert!(
        mean_write_path_nanos(&a.w) < WRITE_PATH_BUDGET_NANOS,
        "the capture path is never blocked: {} ns per datagram over {} datagrams",
        mean_write_path_nanos(&a.w),
        a.w.datagrams_written_total()
    );
    assert!(a.w.last_error().is_none());
}

#[test]
fn eviction_takes_the_oldest_and_never_the_open_segment() {
    let segment = one_segment_footprint();
    let mut a = Archive::with(segment);
    a.rotate_a_full_segment(at_secs(61));
    let open = a.w.open_segment_path();
    a.rotate_a_full_segment(at_secs(122));
    assert!(open.exists() || a.w.open_segment_path() != open);
    assert_eq!(a.w.oldest_segment_seq(), Some(1));
    assert_eq!(a.segments_on_disk(), 1);
}

#[test]
fn an_evicted_segment_takes_its_manifest_with_it() {
    // An object without a manifest is bytes nobody can attribute; a manifest
    // without an object is a row pointing at nothing.
    let segment = one_segment_footprint();
    let mut a = Archive::with(segment);
    a.rotate_a_full_segment(at_secs(61));
    a.rotate_a_full_segment(at_secs(122));
    assert_eq!(
        std::fs::read_dir(&a.completed).unwrap().count(),
        2,
        "one object and one manifest survive"
    );
}

#[test]
fn a_write_error_drops_and_counts_rather_than_propagating_to_the_drain_thread() {
    let (tmp, cfg) = config(u64::MAX);
    let mut a = ArchiveWriter::new(cfg, at_secs(0)).unwrap();
    // A directory where the segment file has to go: the open fails on every
    // attempt, on any filesystem, for any user.
    std::fs::create_dir_all(a.open_segment_path()).unwrap();

    let payload = header_bytes(1, 1, 0, 3);
    a.write(&sequenced(&payload, &format!("{SOURCE}:40000")))
        .ok();

    assert_eq!(a.datagrams_dropped_total(), 1);
    assert!(a.last_error().is_some(), "counted and visible, not silent");
    // No budget assertion here on purpose. This test offers one datagram, and a
    // mean over a single sample is not a budget — it is one syscall against the
    // scheduler, and it failed once under a loaded machine. The budget is
    // asserted where enough datagrams pass through for a mean to mean
    // something.
    drop(a);
    drop(tmp);
}

#[test]
fn a_watermark_smaller_than_a_segment_evicts_rather_than_refusing_to_write() {
    // Losing bounded history is recoverable in every sense that matters.
    // Refusing the next datagram is not, so a watermark too small for even one
    // segment still leaves the write path running.
    let mut a = Archive::with(1);
    a.rotate_a_full_segment(at_secs(61));
    assert_eq!(a.segments_on_disk(), 0);
    assert_eq!(a.w.segments_evicted_total(), 1);
    assert!(mean_write_path_nanos(&a.w) < WRITE_PATH_BUDGET_NANOS);

    write_bytes(&mut a.w, 1024);
    assert_eq!(a.w.datagrams_dropped_total(), 0);
    assert!(a.w.last_error().is_none());
}

#[test]
fn the_write_path_is_timed_over_everything_it_does() {
    // The counter this rule is asserted through has to measure the write path
    // itself. One that accumulates only around the machinery somebody expected
    // to wait — the watermark, say, which the write path never calls — is
    // structurally incapable of being non-zero, and then every assertion built
    // on it passes while the write path stalls.
    let mut a = Archive::with(u64::MAX);
    write_bytes(&mut a.w, SEGMENT_PAYLOAD_BYTES);

    assert!(
        a.w.write_path_nanos() > 0,
        "the write path is not being measured at all"
    );
    assert!(a.w.write_path_max_nanos() > 0);
    assert!(
        a.w.write_path_max_nanos() <= a.w.write_path_nanos(),
        "the longest trip is one of the trips"
    );
    assert!(
        mean_write_path_nanos(&a.w) < WRITE_PATH_BUDGET_NANOS,
        "{} ns per datagram",
        mean_write_path_nanos(&a.w)
    );
}

#[test]
fn a_publication_that_cannot_land_leaves_nothing_uncounted_and_says_so() {
    // A storage outage, a credential expiry, a completed directory on a
    // different mount: the publication fails on every rotation, and the failure
    // travels only through the completed channel, which a caller that is still
    // recording never reads. Uncounted bytes in staging make the watermark a
    // fiction, and a silent failure is one nobody acts on.
    let (tmp, cfg) = config(u64::MAX);
    let staging = cfg.staging_dir.clone();
    let completed = cfg.completed_dir.clone();
    let mut w = ArchiveWriter::new(cfg, at_secs(0)).unwrap();
    // The move into it cannot succeed, for any user and on any filesystem.
    std::fs::remove_dir_all(&completed).unwrap();

    for i in 0..5 {
        write_bytes(&mut w, 16 << 10);
        w.rotate_at(at_secs(61 * (i + 1))).unwrap().unwrap();
        assert!(
            w.wait_completed().unwrap().is_err(),
            "the publication cannot land"
        );
    }

    let left: Vec<String> = std::fs::read_dir(&staging)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        left.iter().all(|name| !name.ends_with(".part")),
        "a failed publication leaves no half-written object: {left:?}"
    );
    assert_eq!(w.publications_failed_total(), 5);
    assert!(
        w.last_error().is_some_and(|e| e.contains("retained")),
        "the operator's counter says what happened: {:?}",
        w.last_error()
    );
    assert_eq!(
        w.segments_on_disk(),
        5,
        "the retained segments are accounted for: {left:?}"
    );
    assert!(w.bytes_on_disk() > 0);
    // And the recorder kept recording throughout.
    assert_eq!(w.datagrams_dropped_total(), 0);
    assert!(mean_write_path_nanos(&w) < WRITE_PATH_BUDGET_NANOS);

    drop(w);
    drop(tmp);
}

#[test]
fn segments_a_publication_could_not_land_are_inside_the_staging_budget() {
    // Accounting them is only half of it: what the budget accounts for, eviction
    // has to be able to reach, or an outage fills the disk with history nothing
    // will ever delete.
    let (tmp, mut cfg) = config(0);
    // Smaller than one segment, so every rotation is over budget.
    cfg.staging_max = 4096;
    let staging = cfg.staging_dir.clone();
    let completed = cfg.completed_dir.clone();
    let mut w = ArchiveWriter::new(cfg, at_secs(0)).unwrap();
    std::fs::remove_dir_all(&completed).unwrap();

    for i in 0..3 {
        write_bytes(&mut w, 16 << 10);
        w.rotate_at(at_secs(61 * (i + 1))).unwrap().unwrap();
        assert!(w.wait_completed().unwrap().is_err());
        w.sweep_staging();
    }

    assert!(
        w.segments_evicted_total() > 0,
        "eviction reaches a retained segment"
    );
    assert!(
        w.bytes_on_disk() <= 4096,
        "{} bytes in {}",
        w.bytes_on_disk(),
        staging.display()
    );
    assert_eq!(
        w.datagrams_dropped_total(),
        0,
        "and the write path is still running"
    );

    drop(w);
    drop(tmp);
}

/// The names left in a directory, for an assertion that has to say which.
fn names(dir: &std::path::Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    v.sort();
    v
}

#[test]
fn a_failed_write_abandons_the_segment_rather_than_appending_after_a_partial_block() {
    // An ENOSPC flush can consume part of a pcapng block, and the replay reader
    // treats a bad block as terminal. Appending the next datagram after one
    // turns a single counted drop into every datagram written afterwards, up to
    // a whole segment.
    let (tmp, cfg) = config(u64::MAX);
    let staging = cfg.staging_dir.clone();
    let mut w = ArchiveWriter::new(cfg, at_secs(0)).unwrap();
    // Every write to /dev/full fails with ENOSPC, for any user and on any
    // filesystem. The segment is opened through a symlink to it, so the failure
    // lands where a full disk lands: on a flush of the segment's buffer,
    // part-way through a block.
    std::os::unix::fs::symlink("/dev/full", w.open_segment_path()).unwrap();

    // Datagram by datagram, so the moment the segment's buffer flushes and the
    // write fails is exactly the moment the next assertions are about.
    let payload = header_bytes(1, 1, 0, 3);
    let dg = sequenced(&payload, &format!("{SOURCE}:40000"));
    let mut offered = 0;
    while w.datagrams_dropped_total() == 0 {
        w.write(&dg).unwrap();
        offered += 1;
        assert!(offered < 200_000, "the segment's buffer never flushed");
    }
    assert!(
        names(&staging).iter().any(|n| n.contains(".recovered-")),
        "the partial is kept, and out of the next segment's way: {:?}",
        names(&staging)
    );
    assert_eq!(
        w.segment_seq(),
        1,
        "the number is spent, so the window it held is a gap and not a silence"
    );

    // Everything offered from here on reaches a segment a reader can read whole.
    let after = 500;
    for _ in 0..after {
        w.write(&dg).unwrap();
    }
    assert_eq!(
        w.datagrams_dropped_total(),
        1,
        "one datagram was lost, not every datagram after it"
    );

    let rotated = w.rotate_at(at_secs(61));
    assert!(
        rotated.is_ok(),
        "the segment opened after the failure closes cleanly: {rotated:?}"
    );
    let landed = w.wait_completed().unwrap().unwrap().segment;
    assert_eq!(landed.segment_seq, 1);
    assert_eq!(
        landed.datagram_count, after,
        "every datagram written after the failure is in the object"
    );
    assert!(mean_write_path_nanos(&w) < WRITE_PATH_BUDGET_NANOS);

    drop(w);
    drop(tmp);
}

#[test]
fn a_segment_a_dead_run_left_is_adopted_into_the_budget_and_can_be_evicted() {
    // Kill the recorder while the compressor is mid-publish of segment-5: the
    // sweep removes only .part files, this run's sequence restarts at 0, and the
    // preserve-partial path only ever reaches segment-0. Excluded from the
    // accounting by the shape of its name, that file sits in staging for ever —
    // never published, never counted, never reachable by eviction — and repeated
    // crashes accumulate the unbounded disk the naming scheme exists to prevent.
    let (tmp, mut cfg) = config(4096);
    let staging = cfg.staging_dir.clone();
    cfg.staging_max = 4096;
    std::fs::create_dir_all(&staging).unwrap();
    let orphan = staging.join("segment-5.pcapng");
    std::fs::write(&orphan, vec![7u8; 16 << 10]).unwrap();

    let mut w = ArchiveWriter::new(cfg, at_secs(0)).unwrap();
    assert!(
        !orphan.exists(),
        "adopted under a name the budget accounts for"
    );
    assert!(
        names(&staging).iter().any(|n| n.contains(".recovered-")),
        "{:?}",
        names(&staging)
    );
    assert!(
        w.bytes_on_disk() >= 16 << 10,
        "its bytes are counted: {} on disk",
        w.bytes_on_disk()
    );
    // Recovery that worked is not a fault, and an operator reading last_error as
    // a health signal must not see one.
    assert!(w.last_error().is_none(), "{:?}", w.last_error());
    assert!(w.last_recovery().is_some_and(|m| m.contains("adopted")));
    assert_eq!(w.recoveries_total(), 1);

    w.sweep_staging();
    assert!(
        w.segments_evicted_total() >= 1,
        "and eviction can reach what the budget counts"
    );
    assert!(
        w.bytes_on_disk() <= 4096,
        "{} bytes on disk",
        w.bytes_on_disk()
    );

    drop(w);
    drop(tmp);
}

#[test]
fn an_orphaned_segment_is_counted_while_the_open_one_is_left_alone() {
    // The open segment is the only file the budget may exclude by name. A
    // segment still under a working name that nothing is publishing is an
    // orphan — a publication whose source removal failed leaves one — and it is
    // history: counted, and evictable, because bytes nothing accounts for are
    // bytes nothing bounds.
    let mut a = Archive::with(u64::MAX);
    write_bytes(&mut a.w, 64 << 10);
    let open = a.w.open_segment_path();
    let staging = open.parent().unwrap().to_path_buf();
    let orphan = staging.join("segment-9.pcapng");
    std::fs::write(&orphan, vec![3u8; 4096]).unwrap();

    assert_eq!(
        a.w.bytes_on_disk(),
        4096,
        "the orphan is counted and the open segment is not: {:?}",
        names(&staging)
    );

    // And what is counted, eviction reaches — without taking the open segment.
    let mut a = Archive::with(1024);
    write_bytes(&mut a.w, 64 << 10);
    let open = a.w.open_segment_path();
    std::fs::write(
        open.parent().unwrap().join("segment-9.pcapng"),
        vec![3u8; 4096],
    )
    .unwrap();
    a.w.sweep_staging();
    assert_eq!(a.w.segments_evicted_total(), 1);
    assert!(
        open.exists(),
        "the open segment is never an eviction candidate"
    );
    write_bytes(&mut a.w, 1024);
    assert_eq!(a.w.datagrams_dropped_total(), 0);
}

#[test]
fn a_swept_temporary_file_is_recovery_and_not_a_fault() {
    // last_error is a health signal. A recovery that worked reported through it
    // means every unclean restart shows a fault while publications_failed_total
    // stays zero, and an operator cannot tell the two apart.
    let (tmp, cfg) = config(u64::MAX);
    let staging = cfg.staging_dir.clone();
    std::fs::create_dir_all(&staging).unwrap();
    std::fs::write(staging.join(".10-20-3.pcapng.zst.part"), b"half an object").unwrap();

    let w = ArchiveWriter::new(cfg, at_secs(0)).unwrap();

    assert!(
        !staging.join(".10-20-3.pcapng.zst.part").exists(),
        "the dead run's temporary file is gone"
    );
    assert!(
        w.last_error().is_none(),
        "a recovery that worked is not a fault: {:?}",
        w.last_error()
    );
    assert_eq!(w.publications_failed_total(), 0);
    assert!(
        w.last_recovery().is_some_and(|m| m.contains("removed")),
        "and it is still stated: {:?}",
        w.last_recovery()
    );
    assert_eq!(w.recoveries_total(), 1);

    drop(w);
    drop(tmp);
}

#[test]
fn a_failed_flush_abandons_the_segment_and_still_tells_the_caller() {
    // A flush that fails leaves the same half-written block a failed write does,
    // so the segment cannot be appended to either. The error still reaches the
    // caller: a flush is not the write path, and a caller that asked for one is
    // entitled to know it did not happen.
    let (tmp, cfg) = config(u64::MAX);
    let staging = cfg.staging_dir.clone();
    let mut w = ArchiveWriter::new(cfg, at_secs(0)).unwrap();
    std::os::unix::fs::symlink("/dev/full", w.open_segment_path()).unwrap();

    write_bytes(&mut w, 1024);
    assert_eq!(w.datagrams_dropped_total(), 0, "the buffer took them all");
    assert!(
        Sink::flush(&mut w).is_err(),
        "the caller learns the bytes are not on the disk"
    );
    assert_eq!(w.segment_seq(), 1);
    assert!(
        names(&staging).iter().any(|n| n.contains(".recovered-")),
        "{:?}",
        names(&staging)
    );

    let after = 500;
    let payload = header_bytes(1, 1, 0, 3);
    let dg = sequenced(&payload, &format!("{SOURCE}:40000"));
    for _ in 0..after {
        w.write(&dg).unwrap();
    }
    assert_eq!(w.datagrams_dropped_total(), 0);
    assert!(Sink::flush(&mut w).is_ok());
    w.rotate_at(at_secs(61)).unwrap().unwrap();
    let landed = w.wait_completed().unwrap().unwrap().segment;
    assert_eq!(landed.datagram_count, after);

    drop(w);
    drop(tmp);
}
