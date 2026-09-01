//! Rotation, the handoff to the compressor, and the hash of what lands.

mod common;

use std::path::PathBuf;
use std::time::Duration;

use common::{archive_config, at_secs, header_bytes, sequenced, write_bytes, SOURCE};
use dz_edge_core::PortRole;
use dz_recorder_archive::rotate::ArchiveWriter;
use dz_recorder_core::{CompletedSegment, Sink};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

/// The writer is declared before the directory it writes into, so the
/// compressor thread is joined while its output directory still exists.
struct Fixture {
    w: ArchiveWriter,
    staging: PathBuf,
    completed: PathBuf,
    _tmp: TempDir,
}

impl Fixture {
    fn new(rotate_bytes: u64, rotate_interval: Duration) -> Self {
        let tmp = TempDir::new().unwrap();
        let staging = tmp.path().join("staging");
        let completed = tmp.path().join("completed");
        let mut cfg = archive_config(&staging, &completed, &[PortRole::Mktdata]);
        cfg.rotate_bytes = rotate_bytes;
        cfg.rotate_interval = rotate_interval;
        let w = ArchiveWriter::new(cfg, at_secs(0)).unwrap();
        Self {
            w,
            staging,
            completed,
            _tmp: tmp,
        }
    }

    fn defaults() -> Self {
        Self::new(1 << 30, Duration::from_secs(60))
    }

    fn completed_entries(&self) -> usize {
        std::fs::read_dir(&self.completed).unwrap().count()
    }

    fn objects(&self) -> Vec<PathBuf> {
        let mut v: Vec<_> = std::fs::read_dir(&self.completed)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "zst"))
            .collect();
        v.sort();
        v
    }

    /// Rotates and waits for the object to land, which is the only thing in the
    /// archive that is allowed to wait.
    fn rotate_and_wait(&mut self, now_ns: u64) -> Option<CompletedSegment> {
        let seq = self.w.rotate_at(now_ns).unwrap()?;
        let landed = self.w.wait_completed().unwrap().unwrap().segment;
        assert_eq!(landed.segment_seq, seq);
        Some(landed)
    }
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[test]
fn rotation_fires_on_size_or_age_whichever_comes_first() {
    // A size bound keeps objects uniform for the analysis tier; an age bound
    // keeps a low-volume feed's data off a local disk for hours.
    let mut f = Fixture::new(1024, Duration::from_secs(60));
    write_bytes(&mut f.w, 2048);
    assert!(f.w.rotate_due(at_secs(1)));

    let mut f = Fixture::new(1 << 30, Duration::from_secs(60));
    write_bytes(&mut f.w, 8);
    assert!(!f.w.rotate_due(at_secs(1)));
    assert!(f.w.rotate_due(at_secs(61)));
}

#[test]
fn an_empty_segment_rotates_to_nothing() {
    let mut f = Fixture::defaults();
    assert!(
        f.w.rotate_at(at_secs(61)).unwrap().is_none(),
        "no object, and no error either"
    );
    assert_eq!(f.completed_entries(), 0);
}

#[test]
fn an_empty_rotation_does_not_consume_a_segment_sequence_number() {
    // A gap in the sequence of objects is a gap in the archive, so a rotation
    // that produced no object must not spend a number either.
    let mut f = Fixture::defaults();
    assert!(f.w.rotate_at(at_secs(61)).unwrap().is_none());
    write_bytes(&mut f.w, 64);
    assert_eq!(f.rotate_and_wait(at_secs(122)).unwrap().segment_seq, 0);
}

#[test]
fn the_segment_sequence_is_monotonic_and_gapless_within_a_run() {
    // A gap in the sequence of objects is a gap in the archive. Without it a
    // recorder that was down for an hour is indistinguishable from a feed that
    // was quiet for an hour.
    let mut f = Fixture::defaults();
    let mut seqs = Vec::new();
    for i in 0..5 {
        write_bytes(&mut f.w, 64);
        seqs.push(
            f.rotate_and_wait(at_secs(61 * (i + 1)))
                .unwrap()
                .segment_seq,
        );
    }
    assert_eq!(seqs, [0, 1, 2, 3, 4]);
    assert_eq!(f.objects().len(), 5);
}

#[test]
fn compression_never_runs_on_the_write_path() {
    // Rotation hands the file to a compressor thread and returns. A writer that
    // compresses inline stalls the drain thread for the length of a 256 MiB
    // zstd.
    //
    // Asserted structurally rather than against a clock: a bound generous enough
    // not to fail on a loaded machine is one an inline compression of a test-sized
    // segment passes anyway, which is how this test used to hold while the
    // compression ran on the caller's thread. That the object does not exist yet
    // when rotation returns cannot be true of an inline compression at all.
    let mut f = Fixture::defaults();
    write_bytes(&mut f.w, 4 << 20);

    let seq = f.w.rotate_at(at_secs(61)).unwrap();

    assert_eq!(seq, Some(0));
    assert_eq!(
        f.completed_entries(),
        0,
        "rotation returned before the object it rotated existed"
    );
    let _ = f.w.wait_completed().unwrap().unwrap();
    assert_eq!(f.objects().len(), 1, "and the object still lands");
}

#[test]
fn a_close_that_fails_spends_the_sequence_number_and_keeps_the_partial() {
    // An ENOSPC or an EIO at rotation loses a whole segment. The number is spent
    // anyway, so the gap in the sequence of objects says so — and the partial
    // file is moved aside, because the next segment would otherwise be created
    // on its path and truncate the only copy of that window.
    let mut f = Fixture::defaults();
    let open = f.w.open_segment_path();
    // Every write to /dev/full fails with ENOSPC, for any user and on any
    // filesystem. The segment is opened through a symlink to it, so the failure
    // lands where a full disk lands: on the flush at close.
    std::fs::create_dir_all(&f.staging).unwrap();
    std::os::unix::fs::symlink("/dev/full", &open).unwrap();

    write_bytes(&mut f.w, 4096);
    let failed = f.w.rotate_at(at_secs(61));
    assert!(failed.is_err(), "the close failed and rotation says so");
    assert_eq!(
        f.w.segment_seq(),
        1,
        "the number is spent, so the lost segment is a gap and not a silence"
    );
    assert!(
        f.w.last_error()
            .is_some_and(|e| e.contains("closing segment 0")),
        "{:?}",
        f.w.last_error()
    );
    let kept: Vec<_> = std::fs::read_dir(&f.staging)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains(".recovered-"))
        .collect();
    assert_eq!(
        kept.len(),
        1,
        "the partial is kept, not truncated: {kept:?}"
    );
    assert!(!open.exists(), "and not on the path the next segment takes");

    // The next segment lands under the next number, which is what makes the loss
    // visible to a reader counting objects.
    write_bytes(&mut f.w, 4096);
    let landed = f.rotate_and_wait(at_secs(122)).unwrap();
    assert_eq!(landed.segment_seq, 1);
}

#[test]
fn a_restart_does_not_truncate_the_partial_segment_the_previous_run_left() {
    // A recorder killed mid-write leaves a partial block — the replay side has a
    // whole verdict for it — and `segment_seq` restarts at 0 on every run, so
    // creating the open segment would truncate the previous run's evidence.
    let tmp = TempDir::new().unwrap();
    let staging = tmp.path().join("staging");
    let completed = tmp.path().join("completed");
    let config = || archive_config(&staging, &completed, &[PortRole::Mktdata]);

    let mut first = ArchiveWriter::new(config(), at_secs(0)).unwrap();
    write_bytes(&mut first, 256 << 10);
    let partial = first.open_segment_path();
    // Dropped without rotating, which is what a killed recorder leaves behind.
    drop(first);
    let left_behind = std::fs::metadata(&partial).unwrap().len();
    assert!(left_behind > 0);

    let mut second = ArchiveWriter::new(config(), at_secs(0)).unwrap();
    write_bytes(&mut second, 1024);
    assert_eq!(
        second.open_segment_path(),
        partial,
        "the second run opens the same path, which is the hazard"
    );

    let kept: Vec<u64> = std::fs::read_dir(&staging)
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| e.file_name().to_string_lossy().contains(".recovered-"))
        .map(|e| e.metadata().unwrap().len())
        .collect();
    assert_eq!(
        kept,
        vec![left_behind],
        "the previous run's bytes survive, whole"
    );
}

#[test]
fn the_hash_is_of_the_object_that_lands() {
    // Integrity and idempotent reprocessing key on (object key, sha256), so the
    // hash must cover the compressed bytes a consumer will actually fetch.
    let mut f = Fixture::defaults();
    write_bytes(&mut f.w, 4096);
    let seg = f.rotate_and_wait(at_secs(61)).unwrap();

    let on_disk = std::fs::read(&seg.path).unwrap();
    assert_eq!(seg.sha256, sha256(&on_disk));
    assert_eq!(seg.byte_count, on_disk.len() as u64);
    assert_eq!(seg.path.extension().unwrap(), "zst");
    assert!(seg.path.starts_with(&f.completed));
}

#[test]
fn an_object_never_lands_without_its_manifest() {
    // The move into the completed directory is the publication. A shipper that
    // finds an object must find its manifest too, or it ships bytes nobody can
    // attribute.
    let mut f = Fixture::defaults();
    write_bytes(&mut f.w, 4096);
    let seg = f.rotate_and_wait(at_secs(61)).unwrap();

    let manifest = seg.path.with_extension("").with_extension("manifest.json");
    assert!(manifest.exists(), "{manifest:?} is missing");
    assert_eq!(f.completed_entries(), 2);
    assert_eq!(
        std::fs::read_dir(&f.staging).unwrap().count(),
        0,
        "nothing is left behind in staging"
    );
}

#[test]
fn the_object_name_carries_the_receive_window_and_the_segment_sequence() {
    // The key is the only thing an object store can be queried on without
    // opening an object.
    let mut f = Fixture::defaults();
    let payload = header_bytes(1, 1, 0, 3);
    let mut first = sequenced(&payload, &format!("{SOURCE}:40000"));
    first.recv_ts_ns = 10;
    f.w.write(&first).unwrap();
    let mut last = sequenced(&payload, &format!("{SOURCE}:40000"));
    last.recv_ts_ns = 20;
    f.w.write(&last).unwrap();

    let seg = f.rotate_and_wait(at_secs(61)).unwrap();
    assert_eq!(seg.path.file_name().unwrap(), "10-20-0.pcapng.zst");
    assert_eq!((seg.start_ns, seg.end_ns), (10, 20));
    assert_eq!(seg.datagram_count, 2);
}

#[test]
fn an_uncompressed_archive_is_written_when_compression_is_off() {
    // The environments that need the older guarantee read the same blocks.
    let tmp = TempDir::new().unwrap();
    let staging = tmp.path().join("staging");
    let completed = tmp.path().join("completed");
    let mut cfg = archive_config(&staging, &completed, &[PortRole::Mktdata]);
    cfg.compression = dz_recorder_archive::Compression::None;
    let mut w = ArchiveWriter::new(cfg, at_secs(0)).unwrap();

    write_bytes(&mut w, 4096);
    w.rotate_at(at_secs(61)).unwrap().unwrap();
    let seg = w.wait_completed().unwrap().unwrap().segment;

    assert_eq!(seg.path.extension().unwrap(), "pcapng");
    assert_eq!(seg.sha256, sha256(&std::fs::read(&seg.path).unwrap()));
}
