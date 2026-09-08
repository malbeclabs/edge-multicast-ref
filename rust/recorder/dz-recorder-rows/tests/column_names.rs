//! Every row, against a literal JSON object with the column names the DDL uses.
//!
//! The point of holding these against literals rather than against a round trip
//! is that a round trip agrees with itself: a field renamed in the struct
//! serialises and deserialises perfectly and lands in a column that does not
//! exist, where `JSONEachRow` either refuses the row or — worse, with
//! `input_format_skip_unknown_fields` on somewhere — accepts it and drops the
//! value. A rename has to fail here.
//!
//! The literals are also the contract for how a value reaches a column: a
//! nanosecond count is a bare integer for a `DateTime64(9)`, an address is a
//! dotted string for an `IPv4`, an unnamed tuple is an array, and `unknown` is
//! `null` and never a zero.
#![forbid(unsafe_code)]

use std::net::Ipv4Addr;

use dz_recorder_rows::{
    ConformanceFinding, Datagram, DropScope, Era, FindingVerdict, Grain, Nanos, PortRoleLabel,
    RecvTsKindLabel, RoleJoinRow, SegmentCoverage, SequenceGap, Verdict,
};
use serde_json::{json, Value};

const SOURCE: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 10);
const GROUP: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 10);
const SHA: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const KEY: &str = "feed=top-of-book/env=test/site=site-1/recorder=recorder-1/\
                   date=2026-09-03/hour=12/1-2-3.pcapng.zst";

fn as_json<T: serde::Serialize>(row: &T) -> Value {
    serde_json::to_value(row).expect("a row serialises")
}

#[test]
fn a_datagram_row_carries_exactly_the_datagram_columns() {
    let row = Datagram {
        recv_ts: Nanos(1_700_000_000_123_456_789),
        send_ts: Nanos(1_700_000_000_000_000_001),
        recv_ts_kind: RecvTsKindLabel::KernelSoftware,
        source_addr: SOURCE,
        channel_id: 1,
        dst_port: 40_000,
        feed: "top-of-book".to_owned(),
        port_role: PortRoleLabel::Mktdata,
        group_addr: GROUP,
        sequence_number: 42,
        reset_count: 3,
        segment_seq: 7,
        payload_len: 48,
        wire_payload_len: 48,
        drop_delta: 0,
        site: "site-1".to_owned(),
        recorder: "recorder-1".to_owned(),
        env: "test".to_owned(),
        drop_scope: DropScope::PortRole,
        object_key: KEY.to_owned(),
        object_sha256: SHA.to_owned(),
    };

    assert_eq!(
        as_json(&row),
        json!({
            "recv_ts": 1_700_000_000_123_456_789u64,
            "send_ts": 1_700_000_000_000_000_001u64,
            "recv_ts_kind": "kernel-software",
            "source_addr": "192.0.2.10",
            "channel_id": 1,
            "dst_port": 40_000,
            "feed": "top-of-book",
            "port_role": "mktdata",
            "group_addr": "233.252.0.10",
            "sequence_number": 42,
            "reset_count": 3,
            "segment_seq": 7,
            "payload_len": 48,
            "wire_payload_len": 48,
            "drop_delta": 0,
            "site": "site-1",
            "recorder": "recorder-1",
            "env": "test",
            "drop_scope": "port-role",
            "object_key": KEY,
            "object_sha256": SHA,
        })
    );
}

/// The design's own DDL listed an `era_index` on this table and put it in the
/// sort key. Its own principle forbids it, and this is where that decision is
/// enforced rather than restated: a stored rank is renumbered by any
/// later-arriving *earlier* object, which is what a backfill is, and renumbering
/// a column inside the sort key of the largest table rewrites that table.
#[test]
fn a_datagram_row_carries_no_era_index_and_no_materialised_latency() {
    let row = as_json(&datagram());
    let object = row.as_object().expect("a row is an object");
    assert!(
        !object.contains_key("era_index"),
        "the era is resolved by range join to `era`, never stored here"
    );
    assert!(
        object.contains_key("reset_count") && object.contains_key("segment_seq"),
        "what the object itself states is what the row carries"
    );
    assert!(
        !object.contains_key("send_recv_ms"),
        "`send_recv_ms` is MATERIALIZED: sending it would insert into a column \
         the engine computes"
    );
}

#[test]
fn an_era_row_carries_exactly_the_era_columns() {
    let row = Era {
        site: "site-1".to_owned(),
        recorder: "recorder-1".to_owned(),
        feed: "top-of-book".to_owned(),
        source_addr: SOURCE,
        channel_id: 1,
        dst_port: 40_000,
        anchor_ts: Nanos(1_700_000_000_123_456_789),
        anchor_seq: 0,
        reset_count: 3,
        segment_seq: 7,
        anchor_certain: 0,
        continuation: 0,
        object_key: KEY.to_owned(),
        object_sha256: SHA.to_owned(),
    };

    assert_eq!(
        as_json(&row),
        json!({
            "site": "site-1",
            "recorder": "recorder-1",
            "feed": "top-of-book",
            "source_addr": "192.0.2.10",
            "channel_id": 1,
            "dst_port": 40_000,
            "anchor_ts": 1_700_000_000_123_456_789u64,
            "anchor_seq": 0,
            "reset_count": 3,
            "segment_seq": 7,
            "anchor_certain": 0,
            "continuation": 0,
            "object_key": KEY,
            "object_sha256": SHA,
        })
    );
}

#[test]
fn a_segment_coverage_row_carries_exactly_the_manifest_columns() {
    let row = SegmentCoverage {
        site: "site-1".to_owned(),
        recorder: "recorder-1".to_owned(),
        env: "test".to_owned(),
        feed: "top-of-book".to_owned(),
        source_addr: SOURCE,
        channel_id: 1,
        dst_port: 40_000,
        segment_seq: 7,
        start_ts: Nanos(1_700_000_000_000_000_000),
        end_ts: Nanos(1_700_000_060_000_000_000),
        first_seq: 0,
        last_seq: 99,
        datagram_count: 100,
        reset_counts_seen: vec![0, 1],
        capture_drop_total: 0,
        interface_drop_total: 12,
        drop_scope: DropScope::CaptureHandle,
        roles_joined: vec![RoleJoinRow("mktdata".to_owned(), GROUP, 40_000)],
        object_key: KEY.to_owned(),
        object_sha256: SHA.to_owned(),
        build_version: "0.1.0".to_owned(),
        build_commit: "unknown".to_owned(),
        config_hash: "a".repeat(64),
    };

    assert_eq!(
        as_json(&row),
        json!({
            "site": "site-1",
            "recorder": "recorder-1",
            "env": "test",
            "feed": "top-of-book",
            "source_addr": "192.0.2.10",
            "channel_id": 1,
            "dst_port": 40_000,
            "segment_seq": 7,
            "start_ts": 1_700_000_000_000_000_000u64,
            "end_ts": 1_700_000_060_000_000_000u64,
            "first_seq": 0,
            "last_seq": 99,
            "datagram_count": 100,
            "reset_counts_seen": [0, 1],
            "capture_drop_total": 0,
            "interface_drop_total": 12,
            "drop_scope": "capture-handle",
            // Array(Tuple(String, IPv4, UInt16)): an unnamed tuple is an array,
            // and the intent is carried and not only the role — a port joined
            // on the wrong port is silent exactly as a port nobody joined is.
            "roles_joined": [["mktdata", "233.252.0.10", 40_000]],
            "object_key": KEY,
            "object_sha256": SHA,
            "build_version": "0.1.0",
            "build_commit": "unknown",
            "config_hash": "a".repeat(64),
        })
    );
}

#[test]
fn a_sequence_gap_row_carries_exactly_the_gap_columns() {
    let row = SequenceGap {
        site: "site-1".to_owned(),
        recorder: "recorder-1".to_owned(),
        env: "test".to_owned(),
        feed: "top-of-book".to_owned(),
        port_role: PortRoleLabel::Snapshot,
        group_addr: GROUP,
        source_addr: SOURCE,
        channel_id: 1,
        dst_port: 40_002,
        reset_count: 3,
        era_index: 2,
        era_anchor_ts: Nanos(1_700_000_000_000_000_000),
        anchor_certain: 1,
        missing_from: 10,
        missing_to: 14,
        missing_count: 5,
        reference_seqs: 100,
        before_ts: Nanos(1_700_000_000_100_000_000),
        after_ts: Nanos(1_700_000_000_200_000_000),
        sent_from_ts: None,
        sent_to_ts: None,
        admitted_recorder: 3,
        admitted_scope: DropScope::PortRole,
        unexplained_count: Some(2),
        interface_drops: Some(0),
        seen_elsewhere: None,
        on_redundant_path: Some(0),
        verdict: Verdict::Unverifiable,
        object_key: KEY.to_owned(),
    };

    assert_eq!(
        as_json(&row),
        json!({
            "site": "site-1",
            "recorder": "recorder-1",
            "env": "test",
            "feed": "top-of-book",
            "port_role": "snapshot",
            "group_addr": "233.252.0.10",
            "source_addr": "192.0.2.10",
            "channel_id": 1,
            "dst_port": 40_002,
            "reset_count": 3,
            "era_index": 2,
            "era_anchor_ts": 1_700_000_000_000_000_000u64,
            "anchor_certain": 1,
            "missing_from": 10,
            "missing_to": 14,
            "missing_count": 5,
            "reference_seqs": 100,
            "before_ts": 1_700_000_000_100_000_000u64,
            "after_ts": 1_700_000_000_200_000_000u64,
            // A site has no clock reading for a datagram it never received, so
            // its own bracket above is the weaker answer and these stay absent
            // until a site that recorded them says otherwise.
            "sent_from_ts": Value::Null,
            "sent_to_ts": Value::Null,
            "admitted_recorder": 3,
            "admitted_scope": "port-role",
            "unexplained_count": 2,
            "interface_drops": 0,
            "seen_elsewhere": Value::Null,
            "on_redundant_path": 0,
            "verdict": "unverifiable",
            "object_key": KEY,
        })
    );
}

/// Unknown is `null`, and a zero would be a measurement.
///
/// Each of these four is a place where both plausible defaults are wrong: a zero
/// residue exonerates the publisher, a residue equal to the missing count
/// accuses it, a zero interface delta says the switch was fine, and a zero
/// `seen_elsewhere` says we looked at every site and the datagram was nowhere —
/// which is the strongest finding this system makes.
#[test]
fn what_is_not_known_is_null_and_never_zero() {
    let row = SequenceGap {
        unexplained_count: None,
        interface_drops: None,
        seen_elsewhere: None,
        on_redundant_path: None,
        ..gap()
    };
    let json = as_json(&row);
    for column in [
        "unexplained_count",
        "interface_drops",
        "seen_elsewhere",
        "on_redundant_path",
    ] {
        assert_eq!(
            json.get(column),
            Some(&Value::Null),
            "{column} must reach the column as null"
        );
    }
}

#[test]
fn a_conformance_finding_row_carries_exactly_the_finding_columns() {
    let row = ConformanceFinding {
        run_ts: Nanos(1_700_000_100_000_000_000),
        rule_id: "seq-continuity".to_owned(),
        rule_set_version: "1.4.0".to_owned(),
        site: "site-1".to_owned(),
        recorder: "recorder-1".to_owned(),
        env: "test".to_owned(),
        feed: "top-of-book".to_owned(),
        port_role: PortRoleLabel::Refdata,
        source_addr: SOURCE,
        channel_id: 1,
        dst_port: 40_001,
        window_start: Nanos(1_700_000_000_000_000_000),
        window_end: Nanos(1_700_000_060_000_000_000),
        verdict: FindingVerdict::Na,
        detail: "no port role joined".to_owned(),
        object_key: KEY.to_owned(),
        first_seq: 0,
        last_seq: 99,
    };

    assert_eq!(
        as_json(&row),
        json!({
            "run_ts": 1_700_000_100_000_000_000u64,
            "rule_id": "seq-continuity",
            "rule_set_version": "1.4.0",
            "site": "site-1",
            "recorder": "recorder-1",
            "env": "test",
            "feed": "top-of-book",
            "port_role": "refdata",
            "source_addr": "192.0.2.10",
            "channel_id": 1,
            "dst_port": 40_001,
            "window_start": 1_700_000_000_000_000_000u64,
            "window_end": 1_700_000_060_000_000_000u64,
            "verdict": "na",
            "detail": "no port role joined",
            "object_key": KEY,
            "first_seq": 0,
            "last_seq": 99,
        })
    );
}

/// Every token a `LowCardinality(String)` column can hold, spelled once.
///
/// `GLOSSARY.md` governs these: the port roles are `mktdata`/`refdata`/
/// `snapshot` and no alias, and the two scope tokens are the ones the archive's
/// section header and its manifest already write, so a subtraction reads the
/// same word wherever it looks for it.
#[test]
fn every_label_token_is_spelled_as_the_specification_states_it() {
    let of = |v: Value| v.as_str().expect("a token is a string").to_owned();
    assert_eq!(of(as_json(&PortRoleLabel::Mktdata)), "mktdata");
    assert_eq!(of(as_json(&PortRoleLabel::Refdata)), "refdata");
    assert_eq!(of(as_json(&PortRoleLabel::Snapshot)), "snapshot");
    assert_eq!(of(as_json(&DropScope::PortRole)), "port-role");
    assert_eq!(of(as_json(&DropScope::CaptureHandle)), "capture-handle");
    assert_eq!(
        of(as_json(&RecvTsKindLabel::KernelSoftware)),
        "kernel-software"
    );
    assert_eq!(
        of(as_json(&RecvTsKindLabel::ApplicationFallback)),
        "application-fallback"
    );
    for (verdict, token) in [
        (Verdict::Recorder, "recorder"),
        (Verdict::Upstream, "upstream"),
        (Verdict::Path, "path"),
        (Verdict::Unverifiable, "unverifiable"),
        (Verdict::Publisher, "publisher"),
    ] {
        assert_eq!(of(as_json(&verdict)), token);
    }
    for (verdict, token) in [
        (FindingVerdict::Pass, "pass"),
        (FindingVerdict::Violation, "violation"),
        (FindingVerdict::Unverifiable, "unverifiable"),
        (FindingVerdict::Na, "na"),
    ] {
        assert_eq!(of(as_json(&verdict)), token);
    }
}

/// The table name is the metric label and the file name, so one spelling has to
/// serve all three.
#[test]
fn a_grain_names_its_table_once() {
    let tables: Vec<&str> = Grain::ALL.iter().map(|g| g.table()).collect();
    assert_eq!(
        tables,
        vec![
            "datagram",
            "era",
            "segment_coverage",
            "sequence_gap",
            "conformance_finding"
        ]
    );
    for grain in Grain::ALL {
        assert_eq!(grain.to_string(), grain.table());
    }
}

/// Every row deserialises from what it serialised to, because the loader's
/// ledger and the golden fixtures read rows back.
#[test]
fn every_row_reads_back_as_itself() {
    let datagram = datagram();
    let round: Datagram =
        serde_json::from_value(as_json(&datagram)).expect("a datagram row reads back");
    assert_eq!(round, datagram);

    let gap = gap();
    let round: SequenceGap = serde_json::from_value(as_json(&gap)).expect("a gap row reads back");
    assert_eq!(round, gap);
}

fn datagram() -> Datagram {
    Datagram {
        recv_ts: Nanos(1_700_000_000_123_456_789),
        send_ts: Nanos(1_700_000_000_000_000_001),
        recv_ts_kind: RecvTsKindLabel::ApplicationFallback,
        source_addr: SOURCE,
        channel_id: 1,
        dst_port: 40_000,
        feed: "top-of-book".to_owned(),
        port_role: PortRoleLabel::Mktdata,
        group_addr: GROUP,
        sequence_number: 42,
        reset_count: 0,
        segment_seq: 7,
        payload_len: 48,
        wire_payload_len: 1300,
        drop_delta: 2,
        site: "site-1".to_owned(),
        recorder: "recorder-1".to_owned(),
        env: "test".to_owned(),
        drop_scope: DropScope::CaptureHandle,
        object_key: KEY.to_owned(),
        object_sha256: SHA.to_owned(),
    }
}

fn gap() -> SequenceGap {
    SequenceGap {
        site: "site-1".to_owned(),
        recorder: "recorder-1".to_owned(),
        env: "test".to_owned(),
        feed: "top-of-book".to_owned(),
        port_role: PortRoleLabel::Mktdata,
        group_addr: GROUP,
        source_addr: SOURCE,
        channel_id: 1,
        dst_port: 40_000,
        reset_count: 0,
        era_index: 1,
        era_anchor_ts: Nanos(1_700_000_000_000_000_000),
        anchor_certain: 0,
        missing_from: 3,
        missing_to: 5,
        missing_count: 3,
        reference_seqs: 8,
        before_ts: Nanos(1_700_000_000_100_000_000),
        after_ts: Nanos(1_700_000_000_200_000_000),
        sent_from_ts: None,
        sent_to_ts: None,
        admitted_recorder: 0,
        admitted_scope: DropScope::PortRole,
        unexplained_count: Some(3),
        interface_drops: None,
        seen_elsewhere: None,
        on_redundant_path: None,
        verdict: Verdict::Unverifiable,
        object_key: KEY.to_owned(),
    }
}
