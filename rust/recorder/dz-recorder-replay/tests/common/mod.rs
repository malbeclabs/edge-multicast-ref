//! Recording and replaying, for the replay crate's test binaries.
//!
//! The datagrams come from `dz_recorder_replay::synthetic` and the archive is
//! written by the real `ArchiveWriter`: a round trip against a second writer
//! written for the test would prove only that the test agrees with itself.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use dz_edge_core::PortRole;
use dz_recorder_archive::rotate::{ArchiveWriter, ArchiveWriterConfig};
use dz_recorder_archive::writer::{LinkHeaders, RoleJoin};
use dz_recorder_archive::Compression;
use dz_recorder_core::{CaptureDropScope, CompletedSegment, RecorderIdentity};
use dz_recorder_replay::synthetic::{port_for, SyntheticPublisher, GROUP};
use dz_recorder_replay::{ArchiveSource, OwnedDatagram, Termination};
use tempfile::TempDir;

/// The object, its manifest, and the directory both live in.
pub struct Recorded {
    /// Held so the archive outlives the test that reads it.
    _dir: TempDir,
    pub object: PathBuf,
    pub segment: CompletedSegment,
    /// The manifest the shipper would find beside the object.
    pub manifest_json: String,
}

pub fn identity() -> RecorderIdentity {
    RecorderIdentity {
        site: "site-1".to_owned(),
        recorder: "recorder-1".to_owned(),
        env: "test".to_owned(),
        build_version: "0.1.0".to_owned(),
        build_commit: "0000000".to_owned(),
        config_hash: "a".repeat(64),
    }
}

/// Publishes the stream into a real archive and returns both halves of the
/// comparison: what went in, and where it landed.
pub fn record(
    publisher: &SyntheticPublisher,
    compression: Compression,
    roles_joined: &[PortRole],
) -> (Vec<OwnedDatagram>, Recorded) {
    // Socket mode's provenance, which is the case a reader can get wrong: a
    // synthesised zero must not come back as an observed TTL. Its drop scope
    // goes with it: socket mode really does have one accumulator per port role,
    // so a per-role subtraction is valid on these archives.
    record_at_scope(
        publisher,
        compression,
        roles_joined,
        CaptureDropScope::PortRole,
    )
}

/// The same, at a stated capture-drop scope, for the tests about what the
/// section declares.
pub fn record_at_scope(
    publisher: &SyntheticPublisher,
    compression: Compression,
    roles_joined: &[PortRole],
    capture_drop_scope: CaptureDropScope,
) -> (Vec<OwnedDatagram>, Recorded) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let staging = dir.path().join("staging");
    let completed = dir.path().join("completed");
    let cfg = ArchiveWriterConfig {
        staging_dir: staging,
        completed_dir: completed.clone(),
        rotate_bytes: 1 << 30,
        rotate_interval: Duration::from_secs(3600),
        staging_max: 1 << 40,
        compression,
        identity: identity(),
        feed: "top-of-book".to_owned(),
        roles_joined: roles_joined
            .iter()
            .map(|&role| RoleJoin::on(role, GROUP, port_for(role)))
            .collect(),
        link_headers: LinkHeaders::Synthesised,
        capture_drop_scope,
    };

    let mut writer = ArchiveWriter::new(cfg, 0).expect("the archive opens");
    let published = publisher
        .publish_into(&mut writer)
        .expect("the write path never fails the caller");
    // The write path counts rather than propagating, so a dropped datagram is
    // silent here unless it is asserted.
    assert_eq!(
        writer.datagrams_dropped_total(),
        0,
        "the archive dropped a datagram: {:?}",
        writer.last_error()
    );
    assert_eq!(writer.datagrams_written_total(), published.len() as u64);

    writer
        .rotate_at(1_000_000_000)
        .expect("rotation")
        .expect("a segment that held datagrams produces an object");
    let landed = writer
        .wait_completed()
        .expect("the compressor thread publishes exactly one object")
        .expect("publication");
    let manifest_json = read_manifest(&completed);

    (
        published,
        Recorded {
            _dir: dir,
            object: landed.segment.path.clone(),
            segment: landed.segment,
            manifest_json,
        },
    )
}

/// Replays a whole archive, asserting that it was whole.
///
/// A helper that accepted a tear in silence would hide exactly the failure the
/// truncation test exists to detect.
pub fn replay(path: &Path) -> Vec<OwnedDatagram> {
    let mut source = ArchiveSource::open(path).expect("the archive opens");
    let datagrams: Vec<OwnedDatagram> = (&mut source).collect();
    assert_eq!(
        source.terminated_by(),
        Termination::Eof,
        "the archive did not end cleanly: {:?}",
        source.last_error()
    );
    datagrams
}

/// Copies `path` to a new file cut inside a block, the way a killed recorder
/// leaves one.
///
/// The cut is computed by walking the block lengths, so it is inside a block by
/// construction rather than by luck: a cut that happened to land on a boundary
/// would leave a whole archive and the test would assert nothing.
pub fn truncate_mid_block(path: &Path, keep_blocks: usize) -> PathBuf {
    let bytes = std::fs::read(path).expect("the object is readable");
    let mut offset = 0usize;
    let mut boundaries = Vec::new();
    while offset + 12 <= bytes.len() {
        let len = u32::from_le_bytes(
            bytes[offset + 4..offset + 8]
                .try_into()
                .expect("four bytes"),
        ) as usize;
        assert!(
            len >= 12 && offset + len <= bytes.len(),
            "not a whole block"
        );
        offset += len;
        boundaries.push(offset);
    }
    assert!(
        boundaries.len() > keep_blocks + 1,
        "the archive has too few blocks to cut inside one"
    );
    // Past the length field of the next block, so the reader learns how long a
    // block it will never see the end of.
    let cut = boundaries[keep_blocks] + 8;

    let out = path.with_extension("truncated.pcapng");
    std::fs::write(&out, &bytes[..cut]).expect("the truncated copy is writable");
    out
}

/// One number out of the manifest JSON.
///
/// Read by hand rather than deserialised: the replay crate does not depend on a
/// JSON library, and a manifest a consumer cannot read with a text search is not
/// the interface the index table wants anyway.
pub fn manifest_number(json: &str, field: &str) -> u64 {
    let key = format!("\"{field}\":");
    let at = json
        .find(&key)
        .unwrap_or_else(|| panic!("the manifest states no {field}: {json}"));
    json[at + key.len()..]
        .trim_start()
        .split(|c: char| !c.is_ascii_digit())
        .find(|s| !s.is_empty())
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("the manifest's {field} is not a number: {json}"))
}

/// The `sequence_number` at its offset in the datagram header.
///
/// Read here at the offset the spec's table states rather than through anything
/// in the crate under test, so the assertion cannot be satisfied by an
/// implementation that agrees with itself.
pub fn sequence_number(payload: &[u8]) -> u64 {
    u64::from_le_bytes(payload[4..12].try_into().expect("eight bytes"))
}

pub fn channel_id(payload: &[u8]) -> u8 {
    payload[3]
}

pub fn schema_version(payload: &[u8]) -> u8 {
    payload[2]
}

/// The declared datagram length, which a fault may state above the cap.
pub fn declared_len(payload: &[u8]) -> u16 {
    u16::from_le_bytes(payload[22..24].try_into().expect("two bytes"))
}

fn read_manifest(completed: &Path) -> String {
    let entry = std::fs::read_dir(completed)
        .expect("the completed directory exists")
        .filter_map(Result::ok)
        .find(|e| e.file_name().to_string_lossy().ends_with(".manifest.json"))
        .expect("every object lands with a manifest beside it");
    std::fs::read_to_string(entry.path()).expect("the manifest is readable")
}

/// Cuts an object's *compressed* bytes, the way an interrupted copy or upload
/// out of the completed directory leaves them.
pub fn truncate_compressed(path: &Path, keep: usize) -> PathBuf {
    let bytes = std::fs::read(path).expect("the object is readable");
    assert!(keep < bytes.len(), "that is not a truncation");
    let out = sibling(path, "torn");
    std::fs::write(&out, &bytes[..keep]).expect("the torn copy is writable");
    out
}

/// A name beside the object that keeps its suffix, so a reader still
/// decompresses it by extension.
fn sibling(path: &Path, tag: &str) -> PathBuf {
    let name = path
        .file_name()
        .expect("an object has a name")
        .to_string_lossy();
    path.with_file_name(format!("{tag}-{name}"))
}
