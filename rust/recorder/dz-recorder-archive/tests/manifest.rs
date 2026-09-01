//! The manifest: what a reader can answer without opening the object.

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;

use common::{
    archive_config, at_secs, header_bytes, port_of, sequenced, GROUP, JOIN_INTERFACE, JOIN_SOURCE,
    SECOND_SOURCE, SOURCE,
};
use dz_edge_core::PortRole;
use dz_recorder_archive::manifest::SegmentManifest;
use dz_recorder_archive::rotate::ArchiveWriter;
use dz_recorder_archive::writer::{CaptureDropScope, LinkHeaders};
use dz_recorder_core::{ChannelInstance, RecordedDatagram, Sink};
use tempfile::TempDir;

struct Recorded {
    manifest: SegmentManifest,
    _tmp: TempDir,
}

/// Records one segment and reads back the manifest that landed beside it.
fn record(roles_joined: &[PortRole], datagrams: &[RecordedDatagram<'_>]) -> Recorded {
    let tmp = TempDir::new().unwrap();
    let staging = tmp.path().join("staging");
    let completed = tmp.path().join("completed");
    let mut w = ArchiveWriter::new(
        archive_config(&staging, &completed, roles_joined),
        at_secs(0),
    )
    .unwrap();
    for dg in datagrams {
        w.write(dg).unwrap();
    }
    w.rotate_at(at_secs(61)).unwrap().unwrap();
    let published = w.wait_completed().unwrap().unwrap();
    drop(w);

    // The value the publication hands back, and the file a shipper reads, are
    // asserted to be the same manifest here rather than in a test of their own:
    // every assertion below then holds for both, and a drift between the two
    // could not be an assertion nobody wrote.
    let path: PathBuf = published
        .segment
        .path
        .with_extension("")
        .with_extension("manifest.json");
    let from_file: SegmentManifest =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(
        from_file, published.manifest,
        "the manifest beside the object is the manifest the caller was given"
    );

    Recorded {
        manifest: published.manifest,
        _tmp: tmp,
    }
}

fn ip(s: &str) -> Ipv4Addr {
    s.parse().unwrap()
}

#[test]
fn the_manifest_states_the_ports_the_recorder_was_asked_to_join() {
    // A port that was never joined produces no data, and no data looks exactly
    // like a clean feed. Without the intent, the analysis tier reports a pass
    // over rules it never ran — and without the group and the port beside the
    // role, it cannot tell a silent port from one joined on the wrong port, or
    // map a coverage row's port back to what was asked for.
    let payload = header_bytes(1, 1, 0, 3);
    let m = record(
        &[PortRole::Mktdata],
        &[sequenced(&payload, &format!("{SOURCE}:40000"))],
    )
    .manifest;
    let roles: Vec<&str> = m.roles_joined.iter().map(|r| r.role.as_str()).collect();
    assert_eq!(roles, vec!["mktdata"]);
    assert!(
        !roles.contains(&"snapshot"),
        "so a silent snapshot port reports na, not pass"
    );

    let joined = &m.roles_joined[0];
    assert_eq!(joined.group, ip(GROUP));
    assert_eq!(joined.port, port_of(PortRole::Mktdata));
    assert_eq!(joined.interface.as_deref(), Some(JOIN_INTERFACE));
    assert_eq!(joined.source, Some(ip(JOIN_SOURCE)));
    // The coverage row a reader compares it against.
    let cov = m
        .instances
        .keys()
        .find(|key| key.dst_port == joined.port)
        .expect("the coverage row maps back to the port that was joined");
    assert_eq!(cov.source, ip(SOURCE));
}

#[test]
fn a_link_header_claim_the_datagrams_contradict_is_counted_in_the_manifest() {
    // The section header states the mode before any datagram arrives. When the
    // datagrams disagree, the count is the operator-visible half of the marks in
    // the object: a claim nothing contradicts out loud is a claim a reader will
    // trust.
    let payload = header_bytes(1, 1, 0, 3);
    let tmp = TempDir::new().unwrap();
    let staging = tmp.path().join("staging");
    let completed = tmp.path().join("completed");
    let mut cfg = archive_config(&staging, &completed, &[PortRole::Mktdata]);
    // AF_PACKET mode configured, and datagrams that carry no headers.
    cfg.link_headers = LinkHeaders::Captured;
    let mut w = ArchiveWriter::new(cfg, at_secs(0)).unwrap();
    for _ in 0..3 {
        w.write(&sequenced(&payload, &format!("{SOURCE}:40000")))
            .unwrap();
    }
    w.rotate_at(at_secs(61)).unwrap().unwrap();
    let seg = w.wait_completed().unwrap().unwrap().segment;
    assert_eq!(w.link_header_exceptions_total(), 3);
    drop(w);

    let path = seg.path.with_extension("").with_extension("manifest.json");
    let m: SegmentManifest = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(m.link_headers, "captured");
    assert_eq!(m.link_header_exceptions, 3);
}

#[test]
fn per_channel_instance_coverage_answers_without_opening_the_object() {
    let mut payloads = Vec::new();
    for seq in 100u64..200 {
        payloads.push(header_bytes(1, seq, 0, 3));
    }
    for seq in 4000u64..4010 {
        payloads.push(header_bytes(1, seq, 2, 3));
    }
    let mut datagrams = Vec::new();
    for (i, p) in payloads.iter().enumerate() {
        // Two publishers serving the same Channel ID to the same group and
        // port, each advancing its own sequence space.
        let host = if i < 100 {
            &format!("{SOURCE}:40000")
        } else {
            &format!("{SECOND_SOURCE}:40000")
        };
        datagrams.push(sequenced(p, host));
    }

    let m = record(&[PortRole::Mktdata], &datagrams).manifest;
    let cov = m
        .instances
        .get(&ChannelInstance::new(ip(SOURCE), 1, 40000))
        .unwrap();
    assert_eq!((cov.first_seq, cov.last_seq, cov.count), (100, 199, 100));
    assert_eq!(cov.reset_counts_seen, vec![0]);

    let other = m
        .instances
        .get(&ChannelInstance::new(ip(SECOND_SOURCE), 1, 40000))
        .unwrap();
    assert_eq!(
        (other.first_seq, other.last_seq, other.count),
        (4000, 4009, 10)
    );
    assert_eq!(other.reset_counts_seen, vec![2]);
    assert_eq!(m.datagram_count, 110);
}

#[test]
fn a_reset_count_advance_is_recorded_rather_than_read_as_backward_motion() {
    // The sequence space restarts on a reset, so both values have to survive
    // into the manifest for the analysis tier to plan around it.
    let payloads = [
        header_bytes(1, 90, 0, 3),
        header_bytes(1, 91, 0, 3),
        header_bytes(1, 0, 1, 3),
        header_bytes(1, 1, 1, 3),
    ];
    let datagrams: Vec<_> = payloads
        .iter()
        .map(|p| sequenced(p, &format!("{SOURCE}:40000")))
        .collect();

    let m = record(&[PortRole::Mktdata], &datagrams).manifest;
    let cov = m
        .instances
        .get(&ChannelInstance::new(ip(SOURCE), 1, 40000))
        .unwrap();
    assert_eq!((cov.first_seq, cov.last_seq, cov.count), (90, 1, 4));
    assert_eq!(cov.reset_counts_seen, vec![0, 1]);
}

#[test]
fn drop_totals_are_visible_before_the_archive_is_trusted() {
    let payload = header_bytes(1, 1, 0, 3);
    let tmp = TempDir::new().unwrap();
    let staging = tmp.path().join("staging");
    let completed = tmp.path().join("completed");
    let mut w = ArchiveWriter::new(
        archive_config(&staging, &completed, &[PortRole::Mktdata]),
        at_secs(0),
    )
    .unwrap();
    let mut dg = sequenced(&payload, &format!("{SOURCE}:40000"));
    dg.drop_delta = 9;
    w.write(&dg).unwrap();
    w.record_interface_drops(PortRole::Mktdata, 4);
    w.rotate_at(at_secs(61)).unwrap().unwrap();
    let seg = w.wait_completed().unwrap().unwrap().segment;
    drop(w);

    let path = seg.path.with_extension("").with_extension("manifest.json");
    let m: SegmentManifest = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(m.capture_drop_total, 9);
    assert_eq!(m.interface_drop_total, 4);
}

#[test]
fn an_unknown_schema_version_still_gets_a_coverage_row() {
    // The coverage read is deliberately not a decode: a decoder rejects the
    // unknown version and drops the row for exactly the datagram most worth
    // knowing about.
    let payload = header_bytes(7, 42, 0, 250);
    let m = record(
        &[PortRole::Mktdata],
        &[sequenced(&payload, &format!("{SOURCE}:40000"))],
    )
    .manifest;
    let cov = m
        .instances
        .get(&ChannelInstance::new(ip(SOURCE), 7, 40000))
        .unwrap();
    assert_eq!((cov.first_seq, cov.last_seq, cov.count), (42, 42, 1));
    assert_eq!(m.short_datagrams, 0);
}

#[test]
fn a_short_datagram_is_counted_rather_than_skipped_silently() {
    // The bytes are archived either way; this only decides whether the manifest
    // can describe them, and a silent skip makes the count disagree with the
    // object for no visible reason.
    let short = [0u8; 8];
    let full = header_bytes(1, 1, 0, 3);
    let m = record(
        &[PortRole::Mktdata],
        &[
            sequenced(&short, &format!("{SOURCE}:40000")),
            sequenced(&full, &format!("{SOURCE}:40000")),
        ],
    )
    .manifest;
    assert_eq!(m.short_datagrams, 1);
    assert_eq!(m.datagram_count, 2, "both are in the object");
    assert_eq!(m.instances.len(), 1);
}

#[test]
fn the_manifest_carries_the_provenance_of_the_build_that_wrote_it() {
    // A finding is attributable to a build and to a configuration, or it is
    // attributable to nothing.
    let payload = header_bytes(1, 1, 0, 3);
    let m = record(
        &[PortRole::Mktdata],
        &[sequenced(&payload, &format!("{SOURCE}:40000"))],
    )
    .manifest;
    assert_eq!(m.site, "site-1");
    assert_eq!(m.recorder, "recorder-1");
    assert_eq!(m.env, "test");
    assert_eq!(m.build_version, "0.1.0");
    assert_eq!(m.build_commit, "0000000");
    assert_eq!(m.config_hash, "a".repeat(64));
    assert_eq!(m.segment_seq, 0);
    assert_eq!(m.feed, "top-of-book");
    // The key an object lands under, not the name it carries locally. A bare
    // file name cannot be the key the analysis tier reprocesses on: two
    // recorders at two sites rotate segment 0 at the same nanosecond and
    // produce the same name for different bytes, so one of the two archives
    // would be invisible to a re-run.
    assert_eq!(
        m.object_key,
        "feed=top-of-book/env=test/site=site-1/recorder=recorder-1/\
         date=2023-11-14/hour=22/\
         1700000000123456789-1700000000123456789-0.pcapng.zst"
    );
    assert!(
        m.object_key.ends_with(".pcapng.zst"),
        "the file name is still the last segment of the key"
    );
    assert_eq!(
        m.sha256.len(),
        64,
        "hex, so a row in an index table is a string"
    );
    assert!(m.byte_count > 0);
}

#[test]
fn the_manifest_states_the_scope_the_drop_totals_may_be_subtracted_at() {
    // A gap is subtracted against our own admitted losses before it is reported
    // as publisher loss, so a reader has to know at what scope that subtraction
    // is valid. At capture-handle scope it is the handle's total and not one
    // role's, and a reader that subtracts it per role is subtracting a guess.
    let payload = header_bytes(1, 1, 0, 3);
    let tmp = TempDir::new().unwrap();
    let staging = tmp.path().join("staging");
    let completed = tmp.path().join("completed");
    let mut cfg = archive_config(
        &staging,
        &completed,
        &[PortRole::Mktdata, PortRole::Refdata],
    );
    // AF_PACKET mode: one ring, one loss accumulator for both roles.
    cfg.capture_drop_scope = CaptureDropScope::CaptureHandle;
    let mut w = ArchiveWriter::new(cfg, at_secs(0)).unwrap();
    let mut refdata = sequenced(&payload, &format!("{SOURCE}:40000"));
    refdata.role = PortRole::Refdata;
    refdata.drop_delta = 40;
    w.write(&refdata).unwrap();
    w.rotate_at(at_secs(61)).unwrap().unwrap();
    let seg = w.wait_completed().unwrap().unwrap().segment;
    drop(w);

    let path = seg.path.with_extension("").with_extension("manifest.json");
    let m: SegmentManifest = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(m.capture_drop_scope, "capture-handle");
    assert_eq!(m.capture_drop_total, 40);
}
