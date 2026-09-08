//! Recording a synthetic stream into a real archive, for the golden tests.
//!
//! The archive is written by the real `ArchiveWriter` and read back by the real
//! `ArchiveSource`, so a golden row set is a statement about what the record
//! path produces. A fixture assembled for the test would prove only that the
//! test agrees with itself.
//!
//! No socket, no privileges, no server: the synthetic publisher writes straight
//! into the `Sink`, which is what makes every test here a CI test.
#![allow(dead_code)]
#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::time::Duration;

use dz_edge_core::PortRole;
use dz_recorder_archive::rotate::{ArchiveWriter, ArchiveWriterConfig};
use dz_recorder_archive::writer::{LinkHeaders, RoleJoin};
use dz_recorder_archive::{Compression, SegmentManifest};
use dz_recorder_core::{CaptureDropScope, RecorderIdentity};
use dz_recorder_replay::synthetic::{port_for, SyntheticPublisher, GROUP};
use dz_recorder_replay::OwnedDatagram;
use dz_recorder_rows::{derive_object, Derived, SegmentTrailer};
use tempfile::TempDir;

pub const FEED: &str = "top-of-book";
pub const SITE: &str = "site-1";
pub const RECORDER: &str = "recorder-1";

/// One object, its manifest, and the datagrams that went in.
pub struct Recorded {
    /// Held so the archive outlives the test that reads it.
    _dir: TempDir,
    pub object: PathBuf,
    pub manifest: SegmentManifest,
    pub written: Vec<OwnedDatagram>,
}

impl Recorded {
    /// Derives every row, with no predecessor: the case a loader meets on the
    /// oldest object it can still see.
    pub fn rows(&self) -> Derived {
        derive_object(&self.object, &self.manifest, None).expect("the object derives")
    }

    /// The same, with the predecessor's trailer in hand.
    pub fn rows_after(&self, preceding: &SegmentTrailer) -> Derived {
        derive_object(&self.object, &self.manifest, Some(preceding)).expect("the object derives")
    }
}

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

/// Records a stream at `port-role` scope — socket mode's, where a per-instance
/// subtraction is valid.
pub fn record(publisher: &SyntheticPublisher) -> Recorded {
    record_at(publisher, CaptureDropScope::PortRole, 0, 0)
}

/// The same, stating the scope and the segment's place in its recorder run.
///
/// `interface_drop_total` is a parameter because it is cumulative: the quantity
/// a verdict rests on is the delta between two consecutive segments, and a
/// fixture that could not set both ends of a subtraction could not exercise it.
pub fn record_at(
    publisher: &SyntheticPublisher,
    scope: CaptureDropScope,
    segment_seq: u64,
    interface_drop_total: u64,
) -> Recorded {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let completed = dir.path().join("completed");
    let cfg = ArchiveWriterConfig {
        staging_dir: dir.path().join("staging"),
        completed_dir: completed,
        rotate_bytes: 1 << 30,
        rotate_interval: Duration::from_secs(3600),
        staging_max: 1 << 40,
        compression: Compression::Zstd { level: 1 },
        identity: identity(),
        feed: FEED.to_owned(),
        // Both `mktdata` and `snapshot` are declared joined while the stream
        // uses one of them: a port nobody sent on is what makes the difference
        // between `na` and `pass` visible in a coverage row.
        roles_joined: vec![
            RoleJoin::on(PortRole::Mktdata, GROUP, port_for(PortRole::Mktdata)),
            RoleJoin::on(PortRole::Snapshot, GROUP, port_for(PortRole::Snapshot)),
        ],
        link_headers: LinkHeaders::Synthesised,
        capture_drop_scope: scope,
    };

    let mut writer = ArchiveWriter::new(cfg, 0).expect("the archive opens");
    let written = publisher
        .publish_into(&mut writer)
        .expect("the write path never fails the caller");
    assert_eq!(
        writer.datagrams_dropped_total(),
        0,
        "the archive dropped a datagram: {:?}",
        writer.last_error()
    );
    // Loss upstream of the capture point, which the analysis tier keeps as its
    // own category. Recorded on the role the stream uses, because that is the
    // grain the writer accumulates it at.
    writer.record_interface_drops(PortRole::Mktdata, interface_drop_total);
    writer
        .rotate_at(1_000_000_000)
        .expect("rotation")
        .expect("a segment that held datagrams produces an object");
    let landed = writer
        .wait_completed()
        .expect("the compressor publishes exactly one object")
        .expect("publication");

    // `segment_seq` restarts at 0 on every recorder run, and a writer opened
    // once produces segment 0 — so the number is placed here rather than made
    // real, because what these tests exercise is the *adjacency* check, and
    // adjacency needs a segment that has a predecessor. The digest is over the
    // object's bytes and is unaffected: nothing in the object states its own
    // place in the run, which is why the manifest is what states it.
    let mut manifest = landed.manifest;
    manifest.segment_seq = segment_seq;

    Recorded {
        _dir: dir,
        object: landed.segment.path.clone(),
        manifest,
        written,
    }
}
