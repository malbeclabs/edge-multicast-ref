//! Golden vectors: the cross-language contract.
//!
//! These bytes are the specification's meaning made concrete. Every
//! implementation in every language must reproduce them — the Go decoder in
//! this repository reads the same five files and asserts the same field values,
//! so a layout change that only one side made fails on the other. A change here
//! is a wire change and must be justified against edge-feed-spec, never
//! adjusted to match code that started failing.

use dz_edge_core::{AppMessage, PortRole, FLAG_SNAPSHOT};
use dz_edge_mbp::{
    BookClear, LevelUpdate, SnapshotBegin, SnapshotEnd, SnapshotLevel, CLEAR_ASK, SCOPE_FROM_PRICE,
    SIDE_ASK, SIDE_BID,
};
use std::path::PathBuf;

fn golden(name: &str) -> Vec<u8> {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/golden")
        .join(name);
    std::fs::read(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

/// Values are deliberately asymmetric so a transposed field pair cannot pass,
/// and they are the ones the manifest records.
pub fn canonical_level_update() -> LevelUpdate {
    LevelUpdate {
        instrument_id: 1,
        source_id: 2,
        side: SIDE_ASK,
        action: 1,
        per_instrument_seq: 4242,
        price_raw: 10_000_500,
        qty_raw: 7_250,
        timestamp_ns: 1_700_000_000_000_000_003,
        order_count: 5,
        level_index: 6,
        update_reason: 2,
        level_flags: 8,
    }
}

pub fn canonical_book_clear() -> BookClear {
    BookClear {
        instrument_id: 1,
        source_id: 2,
        clear_side: CLEAR_ASK,
        scope: SCOPE_FROM_PRICE,
        per_instrument_seq: 4243,
        from_price_raw: 10_000_500,
        timestamp_ns: 1_700_000_000_000_000_004,
        clear_reason: 3,
    }
}

pub fn canonical_snapshot_begin() -> SnapshotBegin {
    SnapshotBegin {
        instrument_id: 1,
        anchor_seq: 918_273_645,
        total_levels: 2,
        snapshot_id: 77,
        last_instrument_seq: 4241,
        timestamp_ns: 1_700_000_000_000_000_005,
        depth_bound: 50,
    }
}

pub fn canonical_snapshot_level() -> SnapshotLevel {
    SnapshotLevel {
        snapshot_id: 77,
        price_raw: 9_999_500,
        qty_raw: 12_500,
        order_count: 3,
        side: SIDE_BID,
        level_flags: 4,
    }
}

pub fn canonical_snapshot_end() -> SnapshotEnd {
    SnapshotEnd {
        instrument_id: 1,
        anchor_seq: 918_273_645,
        snapshot_id: 77,
    }
}

/// The message as it appears **on the wire**, which for a snapshot-port message
/// is not what `encode_into` alone produces.
///
/// `Flags` bit 0 is set on every message travelling the snapshot port, and the
/// builder stamps it at `push` — after `encode_into` has run. A golden vector is
/// the specification's meaning made concrete, and the meaning includes the port
/// the message travels on, so a vector transcribed from `encode_into` alone
/// ships a per-message defect: the Go parser counts it as
/// `SnapshotFlagMismatch`, and an implementation copying these bytes would
/// inherit it.
fn on_the_wire<M: AppMessage>(message: &M) -> Vec<u8> {
    let mut buf = vec![0u8; M::SIZE];
    message.encode_into(&mut buf);
    buf[2..4].copy_from_slice(&flags_on_wire::<M>().to_le_bytes());
    buf
}

/// The `Flags` a message of this type carries once it is on the wire, which is
/// also what the manifest records as `flags_on_wire`. Derived from the message's
/// port roles rather than stated, so a message moved to another port has one
/// place to disagree with either.
fn flags_on_wire<M: AppMessage>() -> u16 {
    assert_eq!(
        M::PORT_ROLES.len(),
        1,
        "a vector for a message carried on several roles would need one per role"
    );
    if M::PORT_ROLES[0] == PortRole::Snapshot {
        FLAG_SNAPSHOT
    } else {
        0
    }
}

#[test]
fn the_encoder_reproduces_every_golden_vector() {
    assert_eq!(
        on_the_wire(&canonical_level_update()),
        golden("level-update-v3.bin")
    );
    assert_eq!(
        on_the_wire(&canonical_book_clear()),
        golden("book-clear-v3.bin")
    );
    assert_eq!(
        on_the_wire(&canonical_snapshot_begin()),
        golden("snapshot-begin-v3.bin")
    );
    assert_eq!(
        on_the_wire(&canonical_snapshot_level()),
        golden("snapshot-level-v3.bin")
    );
    assert_eq!(
        on_the_wire(&canonical_snapshot_end()),
        golden("snapshot-end-v3.bin")
    );
}

#[test]
fn the_decoder_reads_every_golden_vector_back() {
    // The other direction, because an encoder and a decoder that agree with
    // each other but not with the file would both pass the test above alone.
    assert_eq!(
        LevelUpdate::decode(&golden("level-update-v3.bin")).expect("decodes"),
        canonical_level_update()
    );
    assert_eq!(
        BookClear::decode(&golden("book-clear-v3.bin")).expect("decodes"),
        canonical_book_clear()
    );
    assert_eq!(
        SnapshotBegin::decode(&golden("snapshot-begin-v3.bin")).expect("decodes"),
        canonical_snapshot_begin()
    );
    assert_eq!(
        SnapshotLevel::decode(&golden("snapshot-level-v3.bin")).expect("decodes"),
        canonical_snapshot_level()
    );
    assert_eq!(
        SnapshotEnd::decode(&golden("snapshot-end-v3.bin")).expect("decodes"),
        canonical_snapshot_end()
    );
}

#[test]
fn every_snapshot_vector_carries_the_flag_its_port_requires() {
    // Asserted on the bytes rather than on the encoder, because the encoder is
    // not where the flag comes from: an implementation reading these files has
    // only the bytes, and the bit is the difference between a conformant
    // snapshot message and one every subscriber counts as a defect.
    for name in [
        "snapshot-begin-v3.bin",
        "snapshot-level-v3.bin",
        "snapshot-end-v3.bin",
    ] {
        let buf = golden(name);
        assert_eq!(
            u16::from_le_bytes([buf[2], buf[3]]),
            FLAG_SNAPSHOT,
            "{name} must carry Flags bit 0"
        );
    }
    for name in ["level-update-v3.bin", "book-clear-v3.bin"] {
        let buf = golden(name);
        assert_eq!(
            u16::from_le_bytes([buf[2], buf[3]]),
            0,
            "{name} travels the mktdata port and must not claim otherwise"
        );
    }
}

#[test]
fn every_golden_vector_is_exactly_its_messages_size() {
    // A file longer than the message would let a decoder that ignores trailing
    // bytes pass while writing something else on the wire.
    assert_eq!(golden("level-update-v3.bin").len(), LevelUpdate::SIZE);
    assert_eq!(golden("book-clear-v3.bin").len(), BookClear::SIZE);
    assert_eq!(golden("snapshot-begin-v3.bin").len(), SnapshotBegin::SIZE);
    assert_eq!(golden("snapshot-level-v3.bin").len(), SnapshotLevel::SIZE);
    assert_eq!(golden("snapshot-end-v3.bin").len(), SnapshotEnd::SIZE);
}

// ---------------------------------------------------------------------------
// The manifest, made load-bearing
// ---------------------------------------------------------------------------
//
// `testdata/golden/README.md` names `manifest.json` as where an implementation
// in any language reads a vector's field values from. The canonical values above
// are struct literals, as Go's assertions are, and a suite that only compares
// them with the bytes leaves that sentence a claim rather than a binding: the
// manifest could then drift from the bytes and from both sets of literals, in
// either direction, with every suite green.
//
// The test below closes it. Each canonical value is turned into the rows the
// manifest names, and those rows are compared with the manifest's own `fields`
// block in both directions: a value the manifest states differently fails, and
// so does a field named on one side and not the other. The reverse drift needs
// no new test — a literal edited above stops reproducing the bytes and fails
// the cases there. Between the two, the only arrangement that passes is one
// where the manifest, the bytes and these values all say the same thing.
//
// The five `-from-event-` vectors of these same message types are bound the
// same way by `dz-publisher-lowering`, where the values come from the lowering
// rather than from a literal.

use dz_edge_core::SCHEMA_VERSION;
use serde_json::Value;

/// One vector's entry in `testdata/golden/manifest.json`.
///
/// A missing entry panics rather than passing quietly, for the reason the
/// vectors themselves are read with `unwrap_or_else`: a check that asserts
/// nothing because it found nothing reports the same success as one that
/// compared every row.
fn manifest_entry(file: &str) -> Value {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../testdata/golden/manifest.json");
    let text =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let doc: Value = serde_json::from_str(&text).expect("manifest.json is JSON");
    let vectors = doc["vectors"]
        .as_array()
        .expect("manifest.json has a `vectors` array");
    // Every `file` is rejected for being stated twice before anything is looked
    // up, which is what the Go readers do (`readGoldenManifest` builds a map and
    // fails on a repeated `file`). A bare `find` answers with the first of two
    // entries for one vector and never reads the second, so a manifest carrying
    // a correct entry followed by a contradicting one would leave this suite
    // green while stating two different things about the same bytes.
    let mut seen = std::collections::BTreeSet::new();
    for vector in vectors {
        let name = vector["file"]
            .as_str()
            .expect("a vector's `file` is a string");
        assert!(
            seen.insert(name),
            "manifest.json lists {name} twice: the first entry would answer for both and the \
             second binds nothing"
        );
    }
    vectors
        .iter()
        .find(|v| v["file"] == file)
        .unwrap_or_else(|| panic!("manifest.json carries no entry for {file}"))
        .clone()
}

/// Asserts that the manifest states exactly what this suite asserts about one
/// vector: the same header values, and the same `fields` block down to the set
/// of names.
fn manifest_states<M: AppMessage>(file: &str, rows: &[(&str, i64)]) {
    let entry = manifest_entry(file);
    assert_eq!(entry["size"].as_u64(), Some(M::SIZE as u64), "{file}: size");
    assert_eq!(
        entry["type_id"].as_str(),
        Some(format!("{:#04x}", M::TYPE_ID).as_str()),
        "{file}: type_id"
    );
    assert_eq!(
        entry["flags_on_wire"].as_u64(),
        Some(u64::from(flags_on_wire::<M>())),
        "{file}: flags_on_wire"
    );
    assert_eq!(
        entry["schema_version"].as_u64(),
        Some(u64::from(SCHEMA_VERSION)),
        "{file}: schema_version"
    );

    let fields = entry["fields"]
        .as_object()
        .unwrap_or_else(|| panic!("{file}: the manifest entry has no `fields` block"));
    for (name, want) in rows {
        let stated = fields.get(*name).unwrap_or_else(|| {
            panic!("{file}: the manifest states no `{name}`, which this suite asserts as {want}")
        });
        assert_eq!(
            stated.as_i64(),
            Some(*want),
            "{file}: fields.{name} is {stated}, but this suite asserts {want}"
        );
    }

    // The other direction. A field added to the manifest and asserted nowhere
    // is a value nothing holds the codec to, which is exactly what this
    // test refuses to let the manifest carry.
    let mut asserted: Vec<&str> = rows.iter().map(|(n, _)| *n).collect();
    asserted.sort_unstable();
    let mut stated: Vec<&str> = fields.keys().map(String::as_str).collect();
    stated.sort_unstable();
    assert_eq!(
        stated, asserted,
        "{file}: the manifest's `fields` block and this suite's rows must name the same fields"
    );
}

/// Each message as the rows the manifest names for it. Built from the value
/// rather than restated, so the two cannot hold different numbers.
pub fn level_update_rows(m: &LevelUpdate) -> Vec<(&'static str, i64)> {
    vec![
        ("instrument_id", i64::from(m.instrument_id)),
        ("source_id", i64::from(m.source_id)),
        ("side", i64::from(m.side)),
        ("action", i64::from(m.action)),
        ("per_instrument_seq", i64::from(m.per_instrument_seq)),
        ("price_raw", m.price_raw),
        ("qty_raw", m.qty_raw as i64),
        ("timestamp_ns", m.timestamp_ns as i64),
        ("order_count", i64::from(m.order_count)),
        ("level_index", i64::from(m.level_index)),
        ("update_reason", i64::from(m.update_reason)),
        ("level_flags", i64::from(m.level_flags)),
    ]
}

pub fn book_clear_rows(m: &BookClear) -> Vec<(&'static str, i64)> {
    vec![
        ("instrument_id", i64::from(m.instrument_id)),
        ("source_id", i64::from(m.source_id)),
        ("clear_side", i64::from(m.clear_side)),
        ("scope", i64::from(m.scope)),
        ("per_instrument_seq", i64::from(m.per_instrument_seq)),
        ("from_price_raw", m.from_price_raw),
        ("timestamp_ns", m.timestamp_ns as i64),
        ("clear_reason", i64::from(m.clear_reason)),
    ]
}

pub fn snapshot_begin_rows(m: &SnapshotBegin) -> Vec<(&'static str, i64)> {
    vec![
        ("instrument_id", i64::from(m.instrument_id)),
        ("anchor_seq", m.anchor_seq as i64),
        ("total_levels", i64::from(m.total_levels)),
        ("snapshot_id", i64::from(m.snapshot_id)),
        ("last_instrument_seq", i64::from(m.last_instrument_seq)),
        ("timestamp_ns", m.timestamp_ns as i64),
        ("depth_bound", i64::from(m.depth_bound)),
    ]
}

pub fn snapshot_level_rows(m: &SnapshotLevel) -> Vec<(&'static str, i64)> {
    vec![
        ("snapshot_id", i64::from(m.snapshot_id)),
        ("price_raw", m.price_raw),
        ("qty_raw", m.qty_raw as i64),
        ("order_count", i64::from(m.order_count)),
        ("side", i64::from(m.side)),
        ("level_flags", i64::from(m.level_flags)),
    ]
}

pub fn snapshot_end_rows(m: &SnapshotEnd) -> Vec<(&'static str, i64)> {
    vec![
        ("instrument_id", i64::from(m.instrument_id)),
        ("anchor_seq", m.anchor_seq as i64),
        ("snapshot_id", i64::from(m.snapshot_id)),
    ]
}

#[test]
fn the_manifest_states_every_canonical_value() {
    manifest_states::<LevelUpdate>(
        "level-update-v3.bin",
        &level_update_rows(&canonical_level_update()),
    );
    manifest_states::<BookClear>(
        "book-clear-v3.bin",
        &book_clear_rows(&canonical_book_clear()),
    );
    manifest_states::<SnapshotBegin>(
        "snapshot-begin-v3.bin",
        &snapshot_begin_rows(&canonical_snapshot_begin()),
    );
    manifest_states::<SnapshotLevel>(
        "snapshot-level-v3.bin",
        &snapshot_level_rows(&canonical_snapshot_level()),
    );
    manifest_states::<SnapshotEnd>(
        "snapshot-end-v3.bin",
        &snapshot_end_rows(&canonical_snapshot_end()),
    );
}
