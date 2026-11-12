//! Golden vectors: the cross-language contract.
//!
//! `midpoint-v3.bin` was transcribed from the field table in
//! `midpoint/spec.md` 3.0.0, not captured from this encoder. A change here is a
//! wire change and must be justified against edge-feed-spec.

use dz_edge_core::{AppMessage, PortRole, FLAG_SNAPSHOT, SCHEMA_VERSION};
use dz_edge_midpoint::Midpoint;
use serde_json::Value;
use std::path::PathBuf;

fn golden(name: &str) -> Vec<u8> {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/golden")
        .join(name);
    std::fs::read(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

/// The canonical Midpoint. Values are asymmetric so a transposed field pair
/// cannot pass. It is a conformant VWAP.
fn canonical_midpoint() -> Midpoint {
    Midpoint {
        instrument_id: 1,
        source_id: 2,
        method: 5,
        quality_flags: 0x01,
        input_timestamp_ns: 1_700_000_000_000_000_000,
        compute_timestamp_ns: 1_700_000_000_000_250_000,
        price: 10_000_123,
        window_ms: 60_000,
        volume: 1_234_500,
    }
}

#[test]
fn midpoint_matches_its_golden_vector() {
    let mut b = [0u8; Midpoint::SIZE];
    canonical_midpoint().encode_into(&mut b);
    assert_eq!(b.to_vec(), golden("midpoint-v3.bin"));
}

#[test]
fn the_golden_vector_decodes_back_to_its_values() {
    assert_eq!(
        Midpoint::decode(&golden("midpoint-v3.bin")).unwrap(),
        canonical_midpoint()
    );
}

#[test]
fn the_canonical_midpoint_is_conformant() {
    assert_eq!(canonical_midpoint().check(), Ok(()));
}

// ---------------------------------------------------------------------------
// The manifest, held to the same values in both directions. See
// `dz-edge-tob/tests/golden.rs` for the reasoning.
// ---------------------------------------------------------------------------

fn manifest_entry(file: &str) -> Value {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../testdata/golden/manifest.json");
    let text =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let doc: Value = serde_json::from_str(&text).expect("manifest.json is JSON");
    let vectors = doc["vectors"]
        .as_array()
        .expect("manifest.json has a `vectors` array");
    let mut seen = std::collections::BTreeSet::new();
    for vector in vectors {
        let name = vector["file"]
            .as_str()
            .expect("a vector's `file` is a string");
        assert!(seen.insert(name), "manifest.json lists {name} twice");
    }
    vectors
        .iter()
        .find(|v| v["file"] == file)
        .unwrap_or_else(|| panic!("manifest.json carries no entry for {file}"))
        .clone()
}

fn flags_on_wire<M: AppMessage>() -> u16 {
    assert_eq!(M::PORT_ROLES.len(), 1);
    if M::PORT_ROLES[0] == PortRole::Snapshot {
        FLAG_SNAPSHOT
    } else {
        0
    }
}

fn manifest_states<M: AppMessage>(file: &str, schema_version: u8, rows: &[(&str, i64)]) {
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
        Some(u64::from(schema_version)),
        "{file}: schema_version"
    );

    let fields = entry["fields"]
        .as_object()
        .unwrap_or_else(|| panic!("{file}: the manifest entry has no `fields` block"));
    for (name, want) in rows {
        let stated = fields
            .get(*name)
            .unwrap_or_else(|| panic!("{file}: the manifest states no `{name}`"));
        assert_eq!(stated.as_i64(), Some(*want), "{file}: fields.{name}");
    }
    let mut asserted: Vec<&str> = rows.iter().map(|(n, _)| *n).collect();
    asserted.sort_unstable();
    let mut stated: Vec<&str> = fields.keys().map(String::as_str).collect();
    stated.sort_unstable();
    assert_eq!(
        stated, asserted,
        "{file}: the manifest's `fields` block and this suite's rows must name the same fields"
    );
}

fn midpoint_rows(m: &Midpoint) -> Vec<(&'static str, i64)> {
    vec![
        ("instrument_id", i64::from(m.instrument_id)),
        ("source_id", i64::from(m.source_id)),
        ("method", i64::from(m.method)),
        ("quality_flags", i64::from(m.quality_flags)),
        ("input_timestamp_ns", m.input_timestamp_ns as i64),
        ("compute_timestamp_ns", m.compute_timestamp_ns as i64),
        ("price", m.price),
        ("window_ms", i64::from(m.window_ms)),
        ("volume", m.volume as i64),
    ]
}

#[test]
fn the_manifest_states_the_canonical_midpoint() {
    manifest_states::<Midpoint>(
        "midpoint-v3.bin",
        SCHEMA_VERSION,
        &midpoint_rows(&canonical_midpoint()),
    );
}
