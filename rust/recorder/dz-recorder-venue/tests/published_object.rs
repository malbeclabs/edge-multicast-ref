//! The object as it landed, opened and derived.
//!
//! Every other test in this crate hands the reader bytes it wrote itself, which
//! is the right shape for testing the format and the wrong shape for testing
//! the path a recorder actually walks: `publish` compresses by default, so the
//! object that lands in the store is a `.dzus.zst` and the bytes a caller
//! fetches are a zstd frame. A constructor that only ever saw a segment written
//! beside it cannot tell the two apart.
//!
//! So this file goes through the publication: a segment on the disk, `publish`,
//! and then the object opened under its own key and driven through the
//! derivation.
#![forbid(unsafe_code)]

mod common;

use common::{object_bytes, FixtureAdapter, Listing, CONNECTION};
use dz_recorder_archive::upstream::{
    publish, UpstreamConnection, UpstreamFormatError, UpstreamManifest,
};
use dz_recorder_archive::Compression;
use dz_recorder_core::RecvTsKind;
use dz_recorder_venue::{
    derive_venue_object, ArchivedVenueObject, CollectingSink, VenueObject, VenueObjectId,
};

const BASE: u64 = 1_700_000_000_000_000_000;

const LINES: &[&str] = &[
    "chan=113 pubseq=990001 seq=5001 sid=7 quote AAA 100.50 3 100.60 4",
    "chan=113 pubseq=990002 seq=5002 sid=7 quote AAA 100.51 3 100.60 4",
];

/// Writes the segment, publishes it under `compression`, and hands back the
/// identity the manifest states along with the bytes that landed.
fn published(dir: &std::path::Path, compression: Compression) -> (VenueObjectId, Vec<u8>) {
    let segment = dir.join("segment.dzus");
    std::fs::write(&segment, object_bytes(BASE, LINES)).expect("the segment is written");

    let landed = publish(
        &segment,
        &dir.join("completed"),
        UpstreamManifest {
            format_version: 1,
            site: "site-1".to_owned(),
            recorder: "recorder-1".to_owned(),
            env: "test".to_owned(),
            feed: "top-of-book".to_owned(),
            observation: "site-1/recorder-1".to_owned(),
            connections: vec![UpstreamConnection::new(
                CONNECTION,
                RecvTsKind::KernelSoftware,
            )],
            segment_seq: 4,
            start_ns: BASE,
            end_ns: BASE + 1_000_000,
            message_count: LINES.len() as u64,
            object_key: String::new(),
            sha256: String::new(),
            byte_count: 0,
        },
        compression,
    )
    .expect("the object publishes");

    let id = VenueObjectId {
        object_key: landed.manifest.object_key.clone(),
        object_sha256: landed.manifest.sha256.clone(),
        observation: landed.manifest.observation.clone(),
        env: landed.manifest.env.clone(),
        feed: landed.manifest.feed.clone(),
        connections: vec![dz_adapter_core::ConnectionId::new(CONNECTION)],
    };
    let bytes = std::fs::read(&landed.path).expect("the object landed");
    (id, bytes)
}

/// **A published object is readable, compressed or not.**
///
/// `Compression::Zstd` is the archive tier's default and the suffix goes on the
/// key, so `.dzus.zst` is the ordinary case and `.dzus` is the exception. The
/// mutant this kills is the constructor handing the raw bytes to the reader: the
/// first eight bytes of a compressed object are a zstd frame magic, so every
/// ordinary publication would be refused as *not an upstream object* — a reader
/// that cannot read what the writer produces, on the default setting.
#[test]
fn a_published_object_is_read_back_through_the_derivation() {
    for compression in [Compression::None, Compression::Zstd { level: 1 }] {
        let dir = tempfile::tempdir().expect("a scratch directory");
        let (id, bytes) = published(dir.path(), compression);

        let mut object = ArchivedVenueObject::open_published(id.clone(), &bytes[..])
            .expect("the published object opens");
        assert_eq!(object.declared_connections(), vec![CONNECTION.to_owned()]);

        let mut sink = CollectingSink::new();
        let mut adapter = FixtureAdapter::new(vec![Listing::new("AAA", -2, 0)]);
        let derived =
            derive_venue_object(&mut adapter, &mut object, &mut sink).expect("the object derives");

        assert_eq!(derived.message_count, 2, "{compression:?}");
        assert_eq!(derived.book_top_count, 2, "{compression:?}");
        let rows = sink.book_tops();
        assert_eq!(rows[0].object_key, id.object_key);
        assert_eq!(rows[0].object_sha256, id.object_sha256);
        assert_eq!(rows[0].bid_px_raw, Some(10_050));
        assert_eq!(rows[1].bid_px_raw, Some(10_051));
    }
}

/// The key is what decides, and it is the object's own.
///
/// One half of the pair is the failure this closes and the other is what makes
/// it a real decision rather than an unconditional decoder: the compressed
/// object handed to the constructor that takes bytes *already* an upstream
/// object is refused by the magic, and an uncompressed object is not passed
/// through a decoder that would refuse it for having no frame.
#[test]
fn the_key_decides_whether_the_object_is_decoded() {
    let dir = tempfile::tempdir().expect("a scratch directory");
    let (id, compressed) = published(dir.path(), Compression::Zstd { level: 1 });
    assert!(id.object_key.ends_with(".dzus.zst"), "{}", id.object_key);

    match ArchivedVenueObject::open(id.clone(), &compressed[..]) {
        Err(UpstreamFormatError::NotAnUpstreamObject { object_key }) => {
            assert_eq!(object_key, id.object_key);
        }
        other => panic!("a compressed object was read as a raw one: {other:?}"),
    }

    // And the same bytes under a key that does not claim compression: the
    // decision is the name's, so this is the frame reaching a reader that was
    // told not to decode it.
    let mistaken = VenueObjectId {
        object_key: id.object_key.trim_end_matches(".zst").to_owned(),
        ..id
    };
    // `.err()` because the reader itself borrows the bytes, and only the
    // refusal is wanted here.
    match ArchivedVenueObject::open_published(mistaken, &compressed[..]).err() {
        Some(UpstreamFormatError::NotAnUpstreamObject { object_key }) => {
            assert!(object_key.ends_with(".dzus"), "{object_key}");
        }
        Some(other) => panic!("a name that lied about compression: {other:?}"),
        None => panic!("a zstd frame was read as an upstream object"),
    }
}
