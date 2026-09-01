//! The injected faults, and what the archive says about them.
//!
//! Every fault here is a thing a publisher, a network or this recorder actually
//! does. None of them may be repaired, normalised or dropped on the way into the
//! archive: the bug class a parsing recorder creates is the worst one available,
//! because the evidence needed to diagnose the bug is what the bug destroyed.

mod common;

use std::collections::BTreeSet;
use std::net::Ipv4Addr;

use common::{channel_id, declared_len, record, replay, schema_version, sequence_number};
use dz_edge_core::{PortRole, MAX_DATAGRAM_SIZE, SUPPORTED_SCHEMA_VERSIONS};
use dz_recorder_archive::Compression;
use dz_recorder_core::Source;
use dz_recorder_replay::synthetic::{
    StarvationWindow, SyntheticPublisher, OVERSIZED_DECLARED_LEN, SECOND_SOURCE, SILENT_CHANNEL_ID,
    UNKNOWN_SCHEMA_VERSION,
};
use dz_recorder_replay::{ArchiveSource, Fault, OwnedDatagram, Termination};

const MKTDATA_ONLY: &[PortRole] = &[PortRole::Mktdata];
const ZSTD: Compression = Compression::Zstd { level: 1 };
const STREAM: usize = 100;

const EVERY_FAULT: [Fault; 10] = [
    Fault::SequenceGap,
    Fault::BackwardMotion,
    Fault::ResetCountAdvance,
    Fault::NewSourceAddress,
    Fault::SourceAddressDisappears,
    Fault::Duplicate,
    Fault::ReorderedPair,
    Fault::OversizedDeclaredLength,
    Fault::UnknownSchemaVersion,
    Fault::SilentChannel,
];

#[test]
fn every_injected_fault_survives_the_round_trip_intact() {
    // A recorder that decoded would drop several of these, and each one it
    // dropped would be the datagram most worth having.
    for fault in EVERY_FAULT {
        let publisher = SyntheticPublisher::with_fault(STREAM, fault);
        let (original, archive) = record(&publisher, ZSTD, MKTDATA_ONLY);
        let replayed = replay(&archive.object);

        assert_eq!(
            payloads(&original),
            payloads(&replayed),
            "{fault:?} was not archived verbatim"
        );
        // Not only the bytes: an arrival whose source address or stamp was
        // normalised on the way through is the same class of loss.
        assert_eq!(original, replayed, "{fault:?} did not round trip whole");
    }
}

#[test]
fn every_fault_is_visible_in_the_replayed_stream() {
    // A fault the publisher failed to inject makes the test above pass over a
    // clean stream, which is the quietest way for a fault suite to stop
    // testing anything. Each fault is therefore asserted to be present.
    for fault in EVERY_FAULT {
        let publisher = SyntheticPublisher::with_fault(STREAM, fault);
        let (_, archive) = record(&publisher, ZSTD, MKTDATA_ONLY);
        let dgs = replay(&archive.object);
        let seqs: Vec<u64> = dgs.iter().map(|d| sequence_number(&d.payload)).collect();

        match fault {
            Fault::SequenceGap => assert!(
                forward_gaps(&seqs).iter().any(|g| *g > 1),
                "no gap in the sequence space"
            ),
            Fault::BackwardMotion => {
                assert!(descends_somewhere(&seqs), "nothing moved backward");
                assert_eq!(reset_counts(&dgs), BTreeSet::from([0]), "and no reset");
            }
            Fault::ResetCountAdvance => {
                assert_eq!(reset_counts(&dgs), BTreeSet::from([0, 1]));
                assert!(descends_somewhere(&seqs), "the sequence space restarted");
            }
            Fault::NewSourceAddress => {
                assert_eq!(sources(&dgs).len(), 2);
                assert!(
                    !dgs[..STREAM / 4]
                        .iter()
                        .any(|d| *d.src.ip() == SECOND_SOURCE),
                    "the second source appeared, rather than always having been there"
                );
            }
            Fault::SourceAddressDisappears => {
                assert_eq!(sources(&dgs).len(), 2);
                assert!(
                    !dgs[dgs.len() - STREAM / 4..]
                        .iter()
                        .any(|d| *d.src.ip() == SECOND_SOURCE),
                    "the second source is still sending"
                );
            }
            Fault::Duplicate => {
                let mut seen = payloads(&dgs);
                seen.sort();
                let unique = seen.iter().collect::<BTreeSet<_>>().len();
                assert_eq!(unique, seen.len() - 1, "nothing was delivered twice");
            }
            Fault::ReorderedPair => assert!(
                descends_somewhere(&seqs),
                "no pair arrived in the wrong order"
            ),
            Fault::OversizedDeclaredLength => assert!(
                dgs.iter().any(|d| {
                    let declared = declared_len(&d.payload);
                    declared == OVERSIZED_DECLARED_LEN && usize::from(declared) > MAX_DATAGRAM_SIZE
                }),
                "no datagram declared a length above the cap"
            ),
            Fault::UnknownSchemaVersion => assert!(
                dgs.iter().any(|d| {
                    let version = schema_version(&d.payload);
                    version == UNKNOWN_SCHEMA_VERSION
                        && !SUPPORTED_SCHEMA_VERSIONS.contains(&version)
                }),
                "every datagram carried a schema version this build implements"
            ),
            Fault::SilentChannel => {
                // The absence is the fault, and only the manifest's statement of
                // what was configured can distinguish it from a channel nobody
                // asked for. Here: the archive holds not one datagram for it.
                assert!(!dgs
                    .iter()
                    .any(|d| channel_id(&d.payload) == SILENT_CHANNEL_ID));
                assert_eq!(
                    archive.manifest_json.matches("\"first_seq\"").count(),
                    1,
                    "the silent channel contributes no coverage row"
                );
            }
            Fault::None => unreachable!("the clean stream is not an injected fault"),
        }
    }
}

#[test]
fn an_unknown_schema_version_is_archived_and_appears_in_the_manifest() {
    // The bug class a parsing recorder creates is the worst one available: the
    // evidence needed to diagnose the bug is what the bug destroyed. The
    // manifest's coverage row is read at fixed offsets for the same reason —
    // a decode would reject this datagram and lose the row for exactly the
    // traffic most worth describing.
    let publisher = SyntheticPublisher::with_fault(STREAM, Fault::UnknownSchemaVersion);
    let (_, archive) = record(&publisher, ZSTD, MKTDATA_ONLY);

    assert_eq!(
        common::manifest_number(&archive.manifest_json, "datagram_count"),
        100
    );
    assert_eq!(
        common::manifest_number(&archive.manifest_json, "short_datagrams"),
        0
    );
    // And the coverage row describes the whole stream, unknown versions and all.
    assert_eq!(
        common::manifest_number(&archive.manifest_json, "count"),
        100
    );

    let dgs = replay(&archive.object);
    assert_eq!(dgs.len(), 100);
    assert_eq!(
        dgs.iter()
            .filter(|d| schema_version(&d.payload) == UNKNOWN_SCHEMA_VERSION)
            .count(),
        10
    );
}

#[test]
fn a_starved_recorder_accounts_for_its_own_gap_in_the_archive() {
    // A gap covered by our own overflow is not a finding. A gap not covered by
    // it is a much stronger one, because the obvious alternative explanation
    // has been excluded by evidence rather than by assumption.
    let publisher = SyntheticPublisher::clean(1000).starved(&[
        StarvationWindow {
            first: 250,
            count: 40,
        },
        StarvationWindow {
            first: 600,
            count: 3,
        },
    ]);
    let (written, archive) = record(&publisher, ZSTD, MKTDATA_ONLY);
    assert_eq!(written.len(), 1000 - 43);

    let dgs = replay(&archive.object);
    let seqs: Vec<u64> = dgs.iter().map(|d| sequence_number(&d.payload)).collect();
    let span = seqs.last().expect("a datagram") - seqs.first().expect("a datagram") + 1;
    let missing = span - seqs.len() as u64;
    let admitted: u64 = dgs.iter().map(|d| u64::from(d.drop_delta)).sum();

    assert!(
        missing > 0,
        "the starvation must actually have starved something"
    );
    assert_eq!(
        missing, admitted,
        "every gap is attributed to us, none left to blame the publisher for"
    );
    // And the same total is answerable without opening the object, because
    // whether an archive is trustworthy is a question asked before it is used.
    assert_eq!(
        common::manifest_number(&archive.manifest_json, "capture_drop_total"),
        admitted
    );
}

#[test]
fn the_clean_stream_carries_none_of_the_faults() {
    // The control. Without it, a publisher that injected a fault into every
    // stream it built would satisfy every assertion above.
    let (_, archive) = record(&SyntheticPublisher::clean(STREAM), ZSTD, MKTDATA_ONLY);
    let dgs = replay(&archive.object);
    let seqs: Vec<u64> = dgs.iter().map(|d| sequence_number(&d.payload)).collect();

    assert_eq!(dgs.len(), STREAM);
    assert!(forward_gaps(&seqs).iter().all(|g| *g == 1));
    assert!(!descends_somewhere(&seqs));
    assert_eq!(sources(&dgs).len(), 1);
    assert_eq!(reset_counts(&dgs), BTreeSet::from([0]));
    assert!(dgs
        .iter()
        .all(|d| SUPPORTED_SCHEMA_VERSIONS.contains(&schema_version(&d.payload))));
    assert!(dgs
        .iter()
        .all(|d| usize::from(declared_len(&d.payload)) <= MAX_DATAGRAM_SIZE));
    assert_eq!(
        common::manifest_number(&archive.manifest_json, "capture_drop_total"),
        0
    );
}

fn payloads(dgs: &[OwnedDatagram]) -> Vec<Vec<u8>> {
    dgs.iter().map(|d| d.payload.clone()).collect()
}

fn sources(dgs: &[OwnedDatagram]) -> BTreeSet<Ipv4Addr> {
    dgs.iter().map(|d| *d.src.ip()).collect()
}

fn reset_counts(dgs: &[OwnedDatagram]) -> BTreeSet<u8> {
    dgs.iter().map(|d| d.payload[21]).collect()
}

/// The forward steps between consecutive sequence numbers of one source, so a
/// skipped run shows up as a step greater than one.
fn forward_gaps(seqs: &[u64]) -> Vec<u64> {
    seqs.windows(2)
        .filter(|w| w[1] > w[0])
        .map(|w| w[1] - w[0])
        .collect()
}

fn descends_somewhere(seqs: &[u64]) -> bool {
    seqs.windows(2).any(|w| w[1] < w[0])
}

#[test]
fn an_archive_of_nothing_but_interface_blocks_is_refused_rather_than_grown() {
    // The one set in this reader an archive can make grow. Interface
    // description blocks are tiny and identical, so they compress thousands to
    // one: a file small enough to be shipped without comment expands into as
    // much memory as whatever is replaying it will give up. The bound is
    // generous — a recorder writes three — and what matters is that passing it
    // ends the replay as Rejected, so a stream that stopped early can never be
    // read as a complete one.
    use pcap_file::pcapng::blocks::interface_description::InterfaceDescriptionBlock;
    use pcap_file::pcapng::PcapNgWriter;
    use pcap_file::DataLink;

    let tmp = tempfile::tempdir().expect("a temporary directory");
    let path = tmp.path().join("interfaces-only.pcapng");
    {
        let mut writer =
            PcapNgWriter::new(std::fs::File::create(&path).expect("create")).expect("section");
        for _ in 0..5_000 {
            writer
                .write_pcapng_block(InterfaceDescriptionBlock {
                    linktype: DataLink::ETHERNET,
                    snaplen: 1314,
                    options: vec![],
                })
                .expect("interface description");
        }
    }

    let mut source = ArchiveSource::open(&path).expect("the file is well-formed pcapng");
    let mut refused = None;
    loop {
        match Source::next(&mut source) {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(e) => {
                refused = Some(e.to_string());
                break;
            }
        }
    }
    assert!(
        refused.is_some_and(|e| e.contains("interfaces")),
        "an archive of nothing but interface blocks was accepted"
    );
    assert_eq!(source.terminated_by(), Termination::Rejected);
}
