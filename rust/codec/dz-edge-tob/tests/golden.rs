//! Golden vectors: the cross-language contract.
//!
//! These bytes are the specification's meaning made concrete. Every
//! implementation in every language must reproduce them. A change here is a
//! wire change and must be justified against edge-feed-spec, never adjusted to
//! match code that started failing.

use dz_edge_core::AppMessage;
use dz_edge_tob::{Quote, Trade};
use std::path::PathBuf;

fn golden(name: &str) -> Vec<u8> {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/golden")
        .join(name);
    std::fs::read(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

/// The canonical Quote. Values are deliberately asymmetric so a transposed
/// field pair cannot pass.
fn canonical_quote() -> Quote {
    Quote {
        instrument_id: 1,
        source_id: 2,
        update_flags: 0x03,
        source_timestamp_ns: 1_700_000_000_000_000_000,
        bid_price: 9_999_500,
        bid_qty: 12_500,
        ask_price: 10_000_500,
        ask_qty: 7_250,
        bid_source_count: 3,
        ask_source_count: 4,
    }
}

fn canonical_trade() -> Trade {
    Trade {
        instrument_id: 1,
        source_id: 2,
        aggressor_side: 1,
        trade_flags: 0x02,
        source_timestamp_ns: 1_700_000_000_000_000_001,
        trade_price: 10_000_000,
        trade_qty: 500,
        trade_id: 987_654_321,
        cumulative_volume: 1_000_000,
    }
}

#[test]
fn quote_matches_its_golden_vector() {
    let mut b = [0u8; Quote::SIZE];
    canonical_quote().encode_into(&mut b);
    assert_eq!(b.to_vec(), golden("quote-v3.bin"));
}

#[test]
fn trade_matches_its_golden_vector() {
    let mut b = [0u8; Trade::SIZE];
    canonical_trade().encode_into(&mut b);
    assert_eq!(b.to_vec(), golden("trade-v3.bin"));
}

#[test]
fn golden_vectors_decode_back_to_their_values() {
    assert_eq!(
        Quote::decode(&golden("quote-v3.bin")).unwrap(),
        canonical_quote()
    );
    assert_eq!(
        Trade::decode(&golden("trade-v3.bin")).unwrap(),
        canonical_trade()
    );
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
// The tests below close it. Each canonical value is turned into the rows the
// manifest names, and those rows are compared with the manifest's own `fields`
// block in both directions: a value the manifest states differently fails, and
// so does a field named on one side and not the other. The reverse drift needs
// no new test — a literal edited above stops reproducing the bytes and fails
// the cases there. Between the two, the only arrangement that passes is one
// where the manifest, the bytes and these values all say the same thing.

use dz_edge_core::{PortRole, FLAG_SNAPSHOT, SCHEMA_VERSION};
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

/// The `Flags` a message of this type carries once it is on the wire, which is
/// what the manifest records as `flags_on_wire`. Derived from the message's port
/// roles rather than stated, so a message moved to another port has one place
/// to disagree with the manifest.
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

/// Asserts that the manifest states exactly what this suite asserts about one
/// vector: the same header values, and the same `fields` block down to the set
/// of names.
fn manifest_states<M: AppMessage>(
    file: &str,
    schema_version: u8,
    rows: &[(&str, i64)],
    text: &[(&str, &str)],
) {
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
        let stated = fields.get(*name).unwrap_or_else(|| {
            panic!("{file}: the manifest states no `{name}`, which this suite asserts as {want}")
        });
        assert_eq!(
            stated.as_i64(),
            Some(*want),
            "{file}: fields.{name} is {stated}, but this suite asserts {want}"
        );
    }
    for (name, want) in text {
        let stated = fields.get(*name).unwrap_or_else(|| {
            panic!("{file}: the manifest states no `{name}`, which this suite asserts as {want:?}")
        });
        assert_eq!(
            stated.as_str(),
            Some(*want),
            "{file}: fields.{name} is {stated}, but this suite asserts {want:?}"
        );
    }

    // The other direction. A field added to the manifest and asserted nowhere
    // is a value nothing holds the codec to, which is exactly what this
    // test refuses to let the manifest carry.
    let mut asserted: Vec<&str> = rows
        .iter()
        .map(|(n, _)| *n)
        .chain(text.iter().map(|(n, _)| *n))
        .collect();
    asserted.sort_unstable();
    let mut stated: Vec<&str> = fields.keys().map(String::as_str).collect();
    stated.sort_unstable();
    assert_eq!(
        stated, asserted,
        "{file}: the manifest's `fields` block and this suite's rows must name the same fields"
    );
}

/// The canonical Quote as the rows the manifest names for it. Built from the
/// value rather than restated, so the two cannot hold different numbers.
fn quote_rows(q: &Quote) -> Vec<(&'static str, i64)> {
    vec![
        ("instrument_id", i64::from(q.instrument_id)),
        ("source_id", i64::from(q.source_id)),
        ("update_flags", i64::from(q.update_flags)),
        ("source_timestamp_ns", q.source_timestamp_ns as i64),
        ("bid_price", q.bid_price),
        ("bid_qty", q.bid_qty as i64),
        ("ask_price", q.ask_price),
        ("ask_qty", q.ask_qty as i64),
        ("bid_source_count", i64::from(q.bid_source_count)),
        ("ask_source_count", i64::from(q.ask_source_count)),
    ]
}

fn trade_rows(t: &Trade) -> Vec<(&'static str, i64)> {
    vec![
        ("instrument_id", i64::from(t.instrument_id)),
        ("source_id", i64::from(t.source_id)),
        ("aggressor_side", i64::from(t.aggressor_side)),
        ("trade_flags", i64::from(t.trade_flags)),
        ("source_timestamp_ns", t.source_timestamp_ns as i64),
        ("trade_price", t.trade_price),
        ("trade_qty", t.trade_qty as i64),
        ("trade_id", t.trade_id as i64),
        ("cumulative_volume", t.cumulative_volume as i64),
    ]
}

#[test]
fn the_manifest_states_the_canonical_quote() {
    manifest_states::<Quote>(
        "quote-v3.bin",
        SCHEMA_VERSION,
        &quote_rows(&canonical_quote()),
        &[],
    );
}

#[test]
fn the_manifest_states_the_canonical_trade() {
    manifest_states::<Trade>(
        "trade-v3.bin",
        SCHEMA_VERSION,
        &trade_rows(&canonical_trade()),
        &[],
    );
}
