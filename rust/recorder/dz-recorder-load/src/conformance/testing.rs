//! Recording a synthetic feed into a real archive, for this module's tests.
//!
//! Written by the real `ArchiveWriter` and read back by the real
//! `ArchiveSource`, so a hole a test finds is a hole the record path produced
//! rather than one the test agreed with itself about.

use std::path::{Path, PathBuf};
use std::time::Duration;

use dz_edge_core::PortRole;
use dz_recorder_archive::rotate::{ArchiveWriter, ArchiveWriterConfig};
use dz_recorder_archive::writer::{LinkHeaders, RoleJoin};
use dz_recorder_archive::{Compression, SegmentManifest};
use dz_recorder_core::{CaptureDropScope, OwnedDatagram, RecorderIdentity, Sink as _};
use dz_recorder_replay::synthetic::{port_for, SyntheticPublisher, GROUP};
use dz_recorder_replay::ArchiveSource;

use super::findings::{Carried, HoleFinder, Holes};

pub const SITE: &str = "site-1";
pub const RECORDER: &str = "recorder-1";
pub const FEED: &str = "top-of-book";

pub fn identity() -> RecorderIdentity {
    RecorderIdentity {
        site: SITE.to_owned(),
        recorder: RECORDER.to_owned(),
        env: "test".to_owned(),
        build_version: "0.1.0".to_owned(),
        build_commit: "0000000".to_owned(),
        config_hash: "a".repeat(64),
    }
}

/// One object and its manifest.
pub struct Recorded {
    pub object: PathBuf,
    pub manifest: SegmentManifest,
}

impl Recorded {
    /// The object's own holes, found the way the runner finds them.
    pub fn holes(&self) -> Holes {
        let mut archive = ArchiveSource::open(&self.object).expect("the object opens");
        let scope = archive
            .capture_drop_scope()
            .expect("an archive this recorder wrote states its scope");
        let mut finder = HoleFinder::new(scope);
        for dg in &mut archive {
            finder.observe(&dg.as_recorded());
        }
        finder.finish()
    }

    /// What arrived in the object, per group, found the way the runner finds
    /// it.
    pub fn carried(&self) -> Carried {
        let mut archive = ArchiveSource::open(&self.object).expect("the object opens");
        let mut carried = Carried::default();
        for dg in &mut archive {
            carried.observe(&dg.as_recorded());
        }
        carried
    }
}

/// Records `publisher` into `completed`, joined on mktdata and snapshot while
/// the feed uses mktdata alone — so the snapshot port is joined and silent.
pub fn record_into(
    root: &Path,
    completed: &Path,
    publisher: &SyntheticPublisher,
    segment_start_ns: u64,
) -> Recorded {
    let cfg = ArchiveWriterConfig {
        staging_dir: root.join(format!("staging-{segment_start_ns}")),
        completed_dir: completed.to_path_buf(),
        rotate_bytes: 1 << 30,
        rotate_interval: Duration::from_secs(3600),
        staging_max: 1 << 40,
        compression: Compression::Zstd { level: 1 },
        identity: identity(),
        feed: FEED.to_owned(),
        roles_joined: vec![
            RoleJoin::on(PortRole::Mktdata, GROUP, port_for(PortRole::Mktdata)),
            RoleJoin::on(PortRole::Snapshot, GROUP, port_for(PortRole::Snapshot)),
        ],
        link_headers: LinkHeaders::Synthesised,
        capture_drop_scope: CaptureDropScope::PortRole,
    };
    let mut writer = ArchiveWriter::new(cfg, segment_start_ns).expect("the archive opens");
    publisher
        .publish_into(&mut writer)
        .expect("the write path never fails the caller");
    assert_eq!(
        writer.datagrams_dropped_total(),
        0,
        "{:?}",
        writer.last_error()
    );
    writer
        .rotate_at(segment_start_ns + 1_000_000_000)
        .expect("rotation")
        .expect("a segment that held datagrams produces an object");
    let landed = writer
        .wait_completed()
        .expect("the compressor publishes exactly one object")
        .expect("publication");
    Recorded {
        object: landed.segment.path.clone(),
        manifest: landed.manifest,
    }
}

/// `segments` objects from one writer, each holding `publisher`'s feed. One
/// writer and not several: an object is named for its receive window and its
/// place in the run, and two writers fed the same feed would name two
/// objects alike.
pub fn record_segments(
    root: &Path,
    completed: &Path,
    feed: &str,
    publisher: &SyntheticPublisher,
    segments: u64,
) {
    let cfg = ArchiveWriterConfig {
        staging_dir: root.join("staging").join(feed),
        completed_dir: completed.to_path_buf(),
        rotate_bytes: 1 << 30,
        rotate_interval: Duration::from_secs(3600),
        staging_max: 1 << 40,
        compression: Compression::Zstd { level: 1 },
        identity: identity(),
        feed: feed.to_owned(),
        roles_joined: vec![
            RoleJoin::on(PortRole::Mktdata, GROUP, port_for(PortRole::Mktdata)),
            RoleJoin::on(PortRole::Snapshot, GROUP, port_for(PortRole::Snapshot)),
        ],
        link_headers: LinkHeaders::Synthesised,
        capture_drop_scope: CaptureDropScope::PortRole,
    };
    let mut writer = ArchiveWriter::new(cfg, 0).expect("the archive opens");
    for segment in 0..segments {
        publisher
            .publish_into(&mut writer)
            .expect("the write path never fails the caller");
        writer
            .rotate_at(1_000_000_000 * (segment + 1))
            .expect("rotation")
            .expect("a segment that held datagrams produces an object");
        writer
            .wait_completed()
            .expect("the compressor publishes exactly one object")
            .expect("publication");
    }
}

/// Records `datagrams` as they are, under `roles_joined`, into a directory of
/// its own — for a fixture the synthetic publisher cannot produce, such as one
/// feed delivered to two groups.
pub fn record_datagrams(
    datagrams: &[OwnedDatagram],
    roles_joined: Vec<RoleJoin>,
) -> (tempfile::TempDir, Recorded) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let cfg = ArchiveWriterConfig {
        staging_dir: dir.path().join("staging"),
        completed_dir: dir.path().join("completed"),
        rotate_bytes: 1 << 30,
        rotate_interval: Duration::from_secs(3600),
        staging_max: 1 << 40,
        compression: Compression::Zstd { level: 1 },
        identity: identity(),
        feed: FEED.to_owned(),
        roles_joined,
        link_headers: LinkHeaders::Synthesised,
        capture_drop_scope: CaptureDropScope::PortRole,
    };
    let mut writer = ArchiveWriter::new(cfg, 0).expect("the archive opens");
    for dg in datagrams {
        writer
            .write(&dg.as_recorded())
            .expect("the write path never fails the caller");
    }
    writer.flush().expect("flushed");
    assert_eq!(
        writer.datagrams_dropped_total(),
        0,
        "{:?}",
        writer.last_error()
    );
    writer
        .rotate_at(1_000_000_000)
        .expect("rotation")
        .expect("a segment that held datagrams produces an object");
    let landed = writer
        .wait_completed()
        .expect("the compressor publishes exactly one object")
        .expect("publication");
    (
        dir,
        Recorded {
            object: landed.segment.path.clone(),
            manifest: landed.manifest,
        },
    )
}

/// The same, into a directory of its own.
pub fn record(publisher: &SyntheticPublisher) -> (tempfile::TempDir, Recorded) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let recorded = record_into(dir.path(), &dir.path().join("completed"), publisher, 0);
    (dir, recorded)
}
