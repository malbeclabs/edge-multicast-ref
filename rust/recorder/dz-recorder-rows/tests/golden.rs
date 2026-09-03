//! What each injected fault derives into, asserted row by row.
//!
//! The faults are the design's own list — a gap, backward motion, a reset, a new
//! source, a duplicate, a reordered pair, an oversized declared length, an
//! unknown schema version, a silent channel — and each one is a thing a
//! publisher, a network or this recorder actually does. The replay crate already
//! asserts that the archive holds them verbatim; these assert what the *rows*
//! say about them, which is the other half: an archive that keeps a fault and a
//! row set that cannot express it are together no better than a recorder that
//! dropped it.
//!
//! Nothing here needs a socket, a privilege or a server.
#![forbid(unsafe_code)]

mod common;

use std::collections::BTreeSet;

use common::{record, record_at, FEED, RECORDER, SITE};
use dz_edge_core::{PortRole, SUPPORTED_SCHEMA_VERSIONS};
use dz_recorder_core::CaptureDropScope;
use dz_recorder_replay::synthetic::{
    StarvationWindow, SyntheticPublisher, PRIMARY_SOURCE, SECOND_SOURCE, UNKNOWN_SCHEMA_VERSION,
};
use dz_recorder_replay::Fault;
use dz_recorder_rows::{
    DropScope, Grain, PortRoleLabel, RecvTsKindLabel, SegmentTrailer, SequenceGap, Verdict,
};

const STREAM: usize = 100;

/// A clean segment of heartbeat-shaped datagrams: span minus count is zero.
///
/// **This is the fixture that guards the one piece of arithmetic that is valid
/// at this grain and invalid one grain up.** One row per datagram makes
/// `max(sequence) − min(sequence) + 1 − count()` the loss. Against a decoded
/// per-message table it is not, because a datagram carrying no quote still
/// consumes a sequence number — and that subtraction then reports a fixed
/// fraction of every feed as missing, at every site, at once. The stream here is
/// heartbeats precisely so that a table which only counted quotes could not pass.
#[test]
fn a_clean_segment_of_heartbeats_has_span_minus_count_of_zero() {
    let recorded = record(&SyntheticPublisher::clean(STREAM));
    let derived = recorded.rows();
    let rows = &derived.rows;

    assert_eq!(rows.datagram.len(), STREAM, "one row per archived datagram");
    assert_eq!(derived.short_datagrams, 0);

    let seqs: BTreeSet<u64> = rows.datagram.iter().map(|d| d.sequence_number).collect();
    let span = seqs.last().expect("a datagram") - seqs.first().expect("a datagram") + 1;
    assert_eq!(
        span - rows.datagram.len() as u64,
        0,
        "a clean segment has no missing sequence value"
    );
    assert!(
        rows.sequence_gap.is_empty(),
        "and therefore no gap row: {:?}",
        rows.sequence_gap
    );
    // Every datagram is a heartbeat, so a per-message table over this segment
    // would hold no quote rows at all and the same subtraction would report the
    // whole segment as missing.
    assert!(
        rows.datagram.iter().all(|d| d.payload_len > 24),
        "the fixture carries a message body, not bare headers"
    );

    // The provenance every row carries, from the manifest and never from a
    // default: a finding is only attributable if this travels with it.
    for row in &rows.datagram {
        assert_eq!(row.site, SITE);
        assert_eq!(row.recorder, RECORDER);
        assert_eq!(row.feed, FEED);
        assert_eq!(row.object_key, recorded.manifest.object_key);
        assert_eq!(row.object_sha256, recorded.manifest.sha256);
        assert_eq!(row.drop_scope, DropScope::PortRole);
        assert_eq!(row.port_role, PortRoleLabel::Mktdata);
        assert_eq!(row.segment_seq, recorded.manifest.segment_seq);
    }
}

/// A datagram row is the arrival, recovered whole: the stamp to the nanosecond,
/// its kind, the addresses, the wire length and our own admitted loss.
#[test]
fn a_datagram_row_recovers_the_arrival_it_was_derived_from() {
    let recorded = record(&SyntheticPublisher::clean(STREAM));
    let derived = recorded.rows();

    assert_eq!(derived.rows.datagram.len(), recorded.written.len());
    for (row, dg) in derived.rows.datagram.iter().zip(&recorded.written) {
        assert_eq!(row.recv_ts.0, dg.recv_ts_ns, "to the nanosecond");
        assert_eq!(row.recv_ts_kind, RecvTsKindLabel::from(dg.recv_ts_kind));
        assert_eq!(row.source_addr, *dg.src.ip());
        assert_eq!(row.group_addr, *dg.dst.ip());
        assert_eq!(row.dst_port, dg.dst.port());
        assert_eq!(row.wire_payload_len, dg.wire_payload_len);
        assert_eq!(u64::from(row.payload_len), dg.payload.len() as u64);
        assert_eq!(row.drop_delta, dg.drop_delta);
        // The send stamp comes out of the header this crate peeked at, so a
        // reader can subtract the two without opening the object.
        assert_eq!(
            row.send_ts.0,
            u64::from_le_bytes(dg.payload[12..20].try_into().expect("eight bytes"))
        );
    }
    // Both stamp kinds are present, or the column proves nothing: a latency
    // computed from an application fallback measures our own scheduler, and the
    // panel that excludes it needs something to exclude.
    let kinds: BTreeSet<RecvTsKindLabel> = derived
        .rows
        .datagram
        .iter()
        .map(|d| d.recv_ts_kind)
        .collect();
    assert_eq!(kinds.len(), 2, "the fixture carries one kind only");
}

/// The publisher skipped a run, nothing was admitted, and the row says so.
#[test]
fn a_publisher_gap_is_one_row_with_the_run_and_no_admitted_loss() {
    let recorded = record(&SyntheticPublisher::with_fault(STREAM, Fault::SequenceGap));
    let rows = recorded.rows().rows;

    assert_eq!(rows.sequence_gap.len(), 1, "{:?}", rows.sequence_gap);
    let gap = &rows.sequence_gap[0];
    assert_eq!(gap.missing_count, 7, "the fixture skips seven values");
    assert_eq!(gap.missing_to - gap.missing_from + 1, gap.missing_count);
    assert_eq!(gap.admitted_recorder, 0, "we lost nothing");
    assert_eq!(
        gap.unexplained_count,
        Some(7),
        "so the whole run is unexplained"
    );
    assert_eq!(gap.era_index, 1);
    assert_eq!(gap.reset_count, 0);
    assert_eq!(gap.source_addr, PRIMARY_SOURCE);
    assert_eq!(gap.port_role, PortRoleLabel::Mktdata);
    assert!(gap.before_ts < gap.after_ts, "placement, never the measure");
    assert_eq!(gap.reference_seqs, 107, "the window spans 0..=106");

    // The strongest available finding, and this loader must not make it: it
    // needs the datagram absent from *every* site, which one object cannot say.
    assert_eq!(gap.verdict, Verdict::Unverifiable);
    assert_eq!(gap.seen_elsewhere, None);
    assert_eq!(gap.sent_from_ts, None);
    assert_eq!(gap.sent_to_ts, None);
    assert_eq!(
        gap.anchor_certain, 0,
        "no predecessor was offered, so the era's anchor is not settled"
    );
}

/// A gap our own overflow covers is ours, and it never leaves our alerting.
#[test]
fn a_gap_our_own_overflow_covers_is_the_recorders() {
    let publisher = SyntheticPublisher::clean(1000).starved(&[StarvationWindow {
        first: 250,
        count: 40,
    }]);
    let recorded = record(&publisher);
    let rows = recorded.rows().rows;

    assert_eq!(rows.sequence_gap.len(), 1, "{:?}", rows.sequence_gap);
    let gap = &rows.sequence_gap[0];
    assert_eq!(gap.missing_count, 40);
    assert_eq!(gap.admitted_recorder, 40, "the delta rode on the next one");
    assert_eq!(gap.unexplained_count, Some(0));
    assert_eq!(
        gap.verdict,
        Verdict::Recorder,
        "a counter and an alert on us, never a publisher finding"
    );
    assert_eq!(gap.admitted_scope, DropScope::PortRole);
}

/// The same starvation at capture-handle scope admits nothing per instance.
///
/// A ring counts frames dropped *before* demultiplexing, so its total belongs to
/// the handle and to no port role in particular: subtracting it per instance
/// would credit one role with another's losses. The archive can only exonerate
/// itself at this scope, and only when its own total is zero.
#[test]
fn at_capture_handle_scope_a_covered_gap_has_no_residue_to_report() {
    let publisher = SyntheticPublisher::clean(1000).starved(&[StarvationWindow {
        first: 250,
        count: 40,
    }]);
    let recorded = record_at(&publisher, CaptureDropScope::CaptureHandle, 0, 0);
    let rows = recorded.rows().rows;

    assert_eq!(rows.sequence_gap.len(), 1, "{:?}", rows.sequence_gap);
    let gap = &rows.sequence_gap[0];
    assert_eq!(gap.admitted_scope, DropScope::CaptureHandle);
    assert_eq!(
        gap.unexplained_count, None,
        "zero would exonerate the publisher and forty would accuse it"
    );
    assert_eq!(gap.verdict, Verdict::Unverifiable);
}

/// Interface drops that rose over the window are a switch question, and the
/// column carries the delta and never the cumulative total.
#[test]
fn a_gap_beside_rising_interface_drops_is_upstream_and_the_column_is_a_delta() {
    // The host has a long history of upstream loss, and none of it in this
    // window: a panel showing the total shows history.
    let quiet = record_at(
        &SyntheticPublisher::with_fault(STREAM, Fault::SequenceGap),
        CaptureDropScope::PortRole,
        6,
        900,
    );
    let preceding = SegmentTrailer {
        segment_seq: 5,
        interface_drop_total: 900,
        instances: Vec::new(),
    };
    let gap = &quiet.rows_after(&preceding).rows.sequence_gap[0];
    assert_eq!(gap.interface_drops, Some(0), "nothing rose in this window");
    assert_ne!(
        gap.verdict,
        Verdict::Upstream,
        "a total nobody moved says nothing about now"
    );

    // The same object against a predecessor with a lower total: the delta is
    // what rose, and the verdict follows it.
    let preceding = SegmentTrailer {
        segment_seq: 5,
        interface_drop_total: 880,
        instances: Vec::new(),
    };
    let gap = &quiet.rows_after(&preceding).rows.sequence_gap[0];
    assert_eq!(gap.interface_drops, Some(20));
    assert_eq!(gap.verdict, Verdict::Upstream);
}

/// A reset opens a new era, and a sequence number going backwards across one is
/// not backward motion.
#[test]
fn a_reset_opens_an_era_and_no_gap_spans_it() {
    let recorded = record(&SyntheticPublisher::with_fault(
        STREAM,
        Fault::ResetCountAdvance,
    ));
    let derived = recorded.rows();
    let rows = &derived.rows;

    let eras: Vec<_> = rows
        .era
        .iter()
        .filter(|e| e.source_addr == PRIMARY_SOURCE)
        .collect();
    assert_eq!(eras.len(), 2, "the instance's first era and the reset's");
    assert_eq!(eras[0].reset_count, 0);
    assert_eq!(eras[1].reset_count, 1);
    assert_eq!(
        eras[1].anchor_seq, 0,
        "the second era's sequence space restarted"
    );
    assert!(
        eras[0].anchor_ts < eras[1].anchor_ts,
        "the rank is over the openings, in the order they opened"
    );
    assert_eq!(
        eras[1].anchor_certain, 1,
        "the transition is in this object"
    );
    assert_eq!(eras[1].continuation, 0);
    assert_eq!(
        eras[0].anchor_certain, 0,
        "the instance's first era is the one the predecessor decides"
    );
    assert!(
        rows.sequence_gap.is_empty(),
        "a comparison across a reset is an artefact, not a gap: {:?}",
        rows.sequence_gap
    );

    // The wire value is kept as a fact on every datagram row and used as a key
    // nowhere: it is a `u8` and it wraps.
    let wire: BTreeSet<u8> = rows.datagram.iter().map(|d| d.reset_count).collect();
    assert_eq!(wire, BTreeSet::from([0, 1]));
}

/// The adjacency check, and the three answers it can give.
#[test]
fn a_boundary_era_is_settled_by_the_predecessor_or_by_nothing_at_all() {
    let recorded = record_at(
        &SyntheticPublisher::clean(STREAM),
        CaptureDropScope::PortRole,
        4,
        0,
    );
    let key = |trailer: &SegmentTrailer| recorded.rows_after(trailer).rows.era[0].clone();

    // Settled as a continuation: the predecessor's last `Reset Count` for this
    // instance is the one this era carries, so the boundary opens no era.
    let continues = key(&SegmentTrailer {
        segment_seq: 3,
        interface_drop_total: 0,
        instances: vec![dz_recorder_rows::InstanceReset {
            source_addr: PRIMARY_SOURCE,
            channel_id: 1,
            dst_port: 40_000,
            reset_count: 0,
        }],
    });
    assert_eq!((continues.anchor_certain, continues.continuation), (1, 1));

    // Settled as new: the predecessor was there and ended in another era.
    let opened = key(&SegmentTrailer {
        segment_seq: 3,
        interface_drop_total: 0,
        instances: vec![dz_recorder_rows::InstanceReset {
            source_addr: PRIMARY_SOURCE,
            channel_id: 1,
            dst_port: 40_000,
            reset_count: 9,
        }],
    });
    assert_eq!((opened.anchor_certain, opened.continuation), (1, 0));

    // Not settled at all: a trailer that is not the immediately preceding
    // segment is no evidence, and `segment_seq` restarts at 0 on every recorder
    // run — so a hole here is a recorder that was down, which is exactly a case
    // where continuity is genuinely unknown rather than merely unrecorded.
    let unsettled = key(&SegmentTrailer {
        segment_seq: 1,
        interface_drop_total: 0,
        instances: Vec::new(),
    });
    assert_eq!((unsettled.anchor_certain, unsettled.continuation), (0, 0));
    assert_eq!(
        recorded.rows().rows.era[0].anchor_certain,
        0,
        "and neither is one nobody offered"
    );
}

/// Two publishers on one channel and port are two instances, and a value absent
/// from one and present in the other is the redundancy earning its cost.
#[test]
fn a_second_source_is_a_second_instance_and_never_backward_motion() {
    let recorded = record(&SyntheticPublisher::with_fault(
        STREAM,
        Fault::NewSourceAddress,
    ));
    let rows = recorded.rows().rows;

    let sources: BTreeSet<_> = rows.datagram.iter().map(|d| d.source_addr).collect();
    assert_eq!(sources, BTreeSet::from([PRIMARY_SOURCE, SECOND_SOURCE]));
    // One era row per instance, and never one instance's alternation read as
    // the other's reset.
    let eras: BTreeSet<_> = rows.era.iter().map(|e| e.source_addr).collect();
    assert_eq!(eras.len(), 2);
    for era in &rows.era {
        assert_eq!(era.reset_count, 0, "neither publisher reset");
    }

    // The second source appears halfway through, so the first instance's own
    // space is untouched by it: the gap rows below belong to whoever skipped.
    for gap in &rows.sequence_gap {
        assert!(
            gap.on_redundant_path.is_some(),
            "this channel and port carried a second source, so the question was \
             asked: {gap:?}"
        );
    }
}

/// A duplicate delivers no new sequence value, and an oversized or unknown
/// datagram still contributes its own.
#[test]
fn the_faults_a_decoder_would_refuse_still_carry_their_sequence_numbers() {
    for fault in [
        Fault::Duplicate,
        Fault::ReorderedPair,
        Fault::OversizedDeclaredLength,
        Fault::UnknownSchemaVersion,
        Fault::BackwardMotion,
    ] {
        let recorded = record(&SyntheticPublisher::with_fault(STREAM, fault));
        let derived = recorded.rows();
        let rows = &derived.rows;

        assert_eq!(
            rows.datagram.len(),
            recorded.written.len(),
            "{fault:?}: every archived datagram has a row"
        );
        assert_eq!(derived.short_datagrams, 0, "{fault:?}");
        assert!(
            rows.sequence_gap.is_empty(),
            "{fault:?} skipped no sequence value, so it is not loss: {:?}",
            rows.sequence_gap
        );
    }

    // And the two a subscriber must discard are visible as themselves, because
    // the row set reads the header at fixed offsets rather than decoding it.
    let recorded = record(&SyntheticPublisher::with_fault(
        STREAM,
        Fault::UnknownSchemaVersion,
    ));
    assert_eq!(recorded.rows().rows.datagram.len(), STREAM);
    assert!(
        !SUPPORTED_SCHEMA_VERSIONS.contains(&UNKNOWN_SCHEMA_VERSION),
        "the fixture's version is one this build really does not implement"
    );

    let recorded = record(&SyntheticPublisher::with_fault(
        STREAM,
        Fault::OversizedDeclaredLength,
    ));
    let rows = recorded.rows().rows;
    assert!(
        rows.datagram
            .iter()
            .all(|d| d.wire_payload_len == u32::from(d.payload_len)),
        "nothing here was truncated by the capture length"
    );
}

/// A configured port nobody sent on produces no coverage row, and that absence
/// is what `roles_joined` turns from `pass` into `na`.
#[test]
fn a_silent_port_is_declared_joined_and_has_no_coverage_row() {
    let recorded = record(&SyntheticPublisher::with_fault(
        STREAM,
        Fault::SilentChannel,
    ));
    let rows = recorded.rows().rows;

    assert_eq!(
        rows.segment_coverage.len(),
        1,
        "one instance sent: {:?}",
        rows.segment_coverage
    );
    let coverage = &rows.segment_coverage[0];
    assert_eq!(coverage.datagram_count, STREAM as u64);
    assert_eq!(coverage.first_seq, 0);
    assert_eq!(coverage.last_seq, STREAM as u64 - 1);
    assert_eq!(coverage.reset_counts_seen, vec![0]);
    assert_eq!(coverage.segment_seq, recorded.manifest.segment_seq);
    assert_eq!(coverage.start_ts.0, recorded.manifest.start_ns);
    assert_eq!(coverage.end_ts.0, recorded.manifest.end_ns);

    // Two roles were joined and one of them was silent. Without the intent, a
    // port joined on the wrong port is silent in exactly the way a port nobody
    // joined is — and no data looks exactly like a clean feed.
    let joined: Vec<&str> = coverage.roles_joined.iter().map(|r| r.0.as_str()).collect();
    assert_eq!(joined, vec!["mktdata", "snapshot"]);
    assert!(
        !rows.segment_coverage.iter().any(|c| c.dst_port == 40_002),
        "the snapshot port was joined and produced nothing"
    );
}

/// The coverage rows come off the manifest, so a coverage question opens no
/// object at all.
#[test]
fn coverage_is_the_manifest_and_needs_no_object_opened() {
    let recorded = record_at(
        &SyntheticPublisher::clean(STREAM),
        CaptureDropScope::CaptureHandle,
        11,
        7,
    );
    let rows = recorded.rows().rows;
    let coverage = &rows.segment_coverage[0];

    assert_eq!(coverage.interface_drop_total, 7);
    assert_eq!(coverage.capture_drop_total, 0);
    assert_eq!(coverage.drop_scope, DropScope::CaptureHandle);
    assert_eq!(coverage.build_version, "0.1.0");
    assert_eq!(coverage.build_commit, "0000000");
    assert_eq!(coverage.config_hash, "a".repeat(64));
    assert_eq!(coverage.object_sha256, recorded.manifest.sha256);

    // Every value above is in the manifest the recorder wrote beside the
    // object, and every one of them is asserted against it rather than against
    // a constant this test chose.
    let (key, from_manifest) = recorded
        .manifest
        .instances
        .iter()
        .next()
        .expect("one instance");
    assert_eq!(coverage.source_addr, key.source);
    assert_eq!(coverage.channel_id, key.channel_id);
    assert_eq!(coverage.dst_port, key.dst_port);
    assert_eq!(coverage.first_seq, from_manifest.first_seq);
    assert_eq!(coverage.last_seq, from_manifest.last_seq);
    assert_eq!(coverage.datagram_count, from_manifest.count);
}

/// No runner ran, so the table is empty — and empty is the honest answer.
///
/// A `pass` row here would be a pass over a rule that never ran, which is the
/// one thing the design says a rule set must never report.
#[test]
fn nothing_here_judges_conformance() {
    let rows = record(&SyntheticPublisher::clean(STREAM)).rows().rows;
    assert!(rows.conformance_finding.is_empty());
    assert_eq!(rows.rows(Grain::ConformanceFinding), 0);
    assert_eq!(rows.rows(Grain::Datagram), STREAM);
    assert_eq!(rows.len(), rows.datagram.len() + rows.era.len() + 1);
}

/// Every fault derives, and the batch names the object it came from.
#[test]
fn every_fault_derives_and_names_its_object() {
    for fault in [
        Fault::None,
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
    ] {
        let recorded = record(&SyntheticPublisher::with_fault(STREAM, fault));
        let rows = recorded.rows().rows;
        assert_eq!(rows.object_key, recorded.manifest.object_key, "{fault:?}");
        assert_eq!(rows.object_sha256, recorded.manifest.sha256, "{fault:?}");
        assert!(!rows.is_empty(), "{fault:?} derived nothing");
        for gap in &rows.sequence_gap {
            assert_eq!(
                gap.object_key, recorded.manifest.object_key,
                "{fault:?}: a gap row has to say where the evidence is"
            );
        }
    }
}

/// The trailer this load hands the next one: the instance's last `Reset Count`
/// in *arrival* order, which the manifest's sorted set cannot recover.
#[test]
fn the_trailer_carries_the_last_reset_count_in_arrival_order() {
    let recorded = record(&SyntheticPublisher::with_fault(
        STREAM,
        Fault::ResetCountAdvance,
    ));
    let derived = recorded.rows();

    assert_eq!(derived.trailer.segment_seq, recorded.manifest.segment_seq);
    assert_eq!(
        derived.trailer.interface_drop_total,
        recorded.manifest.interface_drop_total
    );
    let key = dz_recorder_core::ChannelInstance::new(PRIMARY_SOURCE, 1, 40_000);
    assert_eq!(
        derived.trailer.last_reset_count(&key),
        Some(1),
        "the segment ended in the era the reset opened"
    );

    // The manifest's own set holds both, in value order, and therefore cannot
    // say which came last. That is the limit this trailer exists to lift.
    let (_, coverage) = recorded
        .manifest
        .instances
        .iter()
        .next()
        .expect("one instance");
    assert_eq!(coverage.reset_counts_seen, vec![0, 1]);
}

/// A gap row's era resolves to an era row, which is how the global rank is
/// reachable without storing it.
#[test]
fn a_gap_row_joins_to_its_era_by_the_anchor() {
    let recorded = record(&SyntheticPublisher::with_fault(STREAM, Fault::SequenceGap));
    let rows = recorded.rows().rows;
    let gap: &SequenceGap = &rows.sequence_gap[0];

    let era = rows
        .era
        .iter()
        .find(|e| {
            e.source_addr == gap.source_addr
                && e.channel_id == gap.channel_id
                && e.dst_port == gap.dst_port
                && e.anchor_ts == gap.era_anchor_ts
        })
        .expect("the gap's era is in the same batch");
    assert_eq!(era.reset_count, gap.reset_count);
    assert_eq!(era.anchor_certain, gap.anchor_certain);
    assert!(
        era.anchor_ts.0 <= gap.before_ts.0,
        "an era opens before anything can be missing inside it"
    );
}

/// One port role's datagrams, on the role the archive says they arrived on.
#[test]
fn the_port_role_on_a_row_is_the_one_the_archive_recorded() {
    for role in [PortRole::Mktdata, PortRole::Refdata, PortRole::Snapshot] {
        let recorded = record(&SyntheticPublisher::clean(20).on_role(role));
        let rows = recorded.rows().rows;
        assert!(
            rows.datagram
                .iter()
                .all(|d| d.port_role == PortRoleLabel::from(role)),
            "{role:?}"
        );
    }
}
