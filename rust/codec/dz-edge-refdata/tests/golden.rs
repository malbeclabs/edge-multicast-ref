//! Golden vectors: the cross-language contract.
//!
//! These bytes are the specification's meaning made concrete. Every
//! implementation in every language must reproduce them. A change here is a
//! wire change and must be justified against edge-feed-spec, never adjusted to
//! match code that started failing.

use dz_edge_core::{AppMessage, SCHEMA_VERSION, SCHEMA_VERSION_V1};
use dz_edge_refdata::{InstrumentDefinition, ManifestSummary, LEG_LEN, SIZE_V1, SYMBOL_LEN};
use std::path::PathBuf;

fn golden(name: &str) -> Vec<u8> {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/golden")
        .join(name);
    std::fs::read(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

/// The canonical InstrumentDefinition at schema 3. Values are deliberately
/// asymmetric so a transposed field pair cannot pass. Schema 1 carries the
/// same logical values apart from `source_id`, which it has no field for.
fn canonical_definition_v3() -> InstrumentDefinition {
    let mut symbol = [0u8; SYMBOL_LEN];
    symbol[..8].copy_from_slice(b"BTC-USDT");
    let mut leg1 = [0u8; LEG_LEN];
    leg1[..3].copy_from_slice(b"BTC");
    let mut leg2 = [0u8; LEG_LEN];
    leg2[..4].copy_from_slice(b"USDT");
    InstrumentDefinition {
        instrument_id: 1,
        source_id: 2,
        symbol,
        leg1,
        leg2,
        asset_class: 1,
        price_exponent: -2,
        qty_exponent: -8,
        market_model: 1,
        tick_size: 1,
        lot_size: 1000,
        contract_value: 0,
        expiry_ns: 0,
        settle_type: 0,
        price_bound: 0,
        manifest_seq: 9,
    }
}

fn canonical_manifest_summary() -> ManifestSummary {
    ManifestSummary {
        channel_id: 7,
        valid: 1,
        manifest_seq: 9,
        instrument_count: 1234,
        timestamp_ns: 1_700_000_000_000_000_002,
    }
}

#[test]
fn instrument_definition_v3_matches_its_golden_vector() {
    let mut b = [0u8; InstrumentDefinition::SIZE];
    canonical_definition_v3().encode_into(&mut b);
    assert_eq!(b.to_vec(), golden("instrument-definition-v3.bin"));
}

#[test]
fn instrument_definition_v3_golden_vector_decodes_to_canonical_values() {
    let d = InstrumentDefinition::decode(&golden("instrument-definition-v3.bin"), SCHEMA_VERSION)
        .unwrap();
    assert_eq!(d, canonical_definition_v3());
}

#[test]
fn instrument_definition_v1_golden_vector_decodes_to_canonical_values() {
    // Schema 1 is decode-only: there is no encoder for it, so only the decode
    // direction is asserted here.
    let d =
        InstrumentDefinition::decode(&golden("instrument-definition-v1.bin"), SCHEMA_VERSION_V1)
            .unwrap();

    let v3 = canonical_definition_v3();
    assert_eq!(d.instrument_id, v3.instrument_id);
    assert_eq!(d.leg1, v3.leg1);
    assert_eq!(d.leg2, v3.leg2);
    assert_eq!(d.asset_class, v3.asset_class);
    assert_eq!(d.price_exponent, v3.price_exponent);
    assert_eq!(d.qty_exponent, v3.qty_exponent);
    assert_eq!(d.market_model, v3.market_model);
    assert_eq!(d.tick_size, v3.tick_size);
    assert_eq!(d.lot_size, v3.lot_size);
    assert_eq!(d.contract_value, v3.contract_value);
    assert_eq!(d.expiry_ns, v3.expiry_ns);
    assert_eq!(d.settle_type, v3.settle_type);
    assert_eq!(d.price_bound, v3.price_bound);
    assert_eq!(d.manifest_seq, v3.manifest_seq);

    // Schema 1 has no Source ID field; it must decode as 0.
    assert_eq!(d.source_id, 0, "v1 carries no Source ID; it reads as 0");

    // The symbol is "BTC-USDT" left-justified with the remaining 56 bytes
    // zeroed (the widened schema-3 width, since InstrumentDefinition always
    // stores Symbol at SYMBOL_LEN regardless of the schema it was read from).
    assert_eq!(&d.symbol[..8], b"BTC-USDT");
    assert_eq!(&d.symbol[8..], &[0u8; SYMBOL_LEN - 8][..]);
}

#[test]
fn manifest_summary_matches_its_golden_vector() {
    let mut b = [0u8; ManifestSummary::SIZE];
    canonical_manifest_summary().encode_into(&mut b);
    // encode_into honours Channel ID directly now, so the encoder's
    // unmodified output is compared against the vector.
    assert_eq!(b.to_vec(), golden("manifest-summary-v3.bin"));
}

#[test]
fn manifest_summary_golden_vector_decodes_to_canonical_values() {
    assert_eq!(
        ManifestSummary::decode(&golden("manifest-summary-v3.bin")).unwrap(),
        canonical_manifest_summary()
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

use dz_edge_core::{PortRole, FLAG_SNAPSHOT};
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
    doc["vectors"]
        .as_array()
        .expect("manifest.json has a `vectors` array")
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
///
/// `size` is a parameter rather than `M::SIZE` because
/// `instrument-definition-v1.bin` is a decode-only layout of the same type at
/// 80 bytes against schema 3's 130, so the constant cannot state both.
fn manifest_states<M: AppMessage>(
    file: &str,
    size: usize,
    schema_version: u8,
    rows: &[(&str, i64)],
    text: &[(&str, &str)],
) {
    let entry = manifest_entry(file);
    assert_eq!(entry["size"].as_u64(), Some(size as u64), "{file}: size");
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

/// A fixed-width ASCII field as the manifest writes it: left-justified, the
/// null padding dropped.
fn ascii(field: &[u8]) -> &str {
    let end = field.iter().position(|b| *b == 0).unwrap_or(field.len());
    std::str::from_utf8(&field[..end]).expect("the canonical values are ASCII")
}

/// The canonical InstrumentDefinition as the rows the manifest names for it.
/// Built from the value rather than restated, so the two cannot hold different
/// numbers.
fn definition_rows(d: &InstrumentDefinition) -> Vec<(&'static str, i64)> {
    vec![
        ("instrument_id", i64::from(d.instrument_id)),
        ("source_id", i64::from(d.source_id)),
        ("asset_class", i64::from(d.asset_class)),
        ("price_exponent", i64::from(d.price_exponent)),
        ("qty_exponent", i64::from(d.qty_exponent)),
        ("market_model", i64::from(d.market_model)),
        ("tick_size", d.tick_size),
        ("lot_size", d.lot_size as i64),
        ("contract_value", d.contract_value as i64),
        ("expiry_ns", d.expiry_ns as i64),
        ("settle_type", i64::from(d.settle_type)),
        ("price_bound", i64::from(d.price_bound)),
        ("manifest_seq", i64::from(d.manifest_seq)),
    ]
}

fn definition_text(d: &InstrumentDefinition) -> Vec<(&'static str, &str)> {
    vec![
        ("symbol", ascii(&d.symbol)),
        ("leg1", ascii(&d.leg1)),
        ("leg2", ascii(&d.leg2)),
    ]
}

fn manifest_summary_rows(m: &ManifestSummary) -> Vec<(&'static str, i64)> {
    vec![
        ("channel_id", i64::from(m.channel_id)),
        ("valid", i64::from(m.valid)),
        ("manifest_seq", i64::from(m.manifest_seq)),
        ("instrument_count", i64::from(m.instrument_count)),
        ("timestamp_ns", m.timestamp_ns as i64),
    ]
}

#[test]
fn the_manifest_states_the_canonical_definition_at_schema_3() {
    let d = canonical_definition_v3();
    manifest_states::<InstrumentDefinition>(
        "instrument-definition-v3.bin",
        InstrumentDefinition::SIZE,
        SCHEMA_VERSION,
        &definition_rows(&d),
        &definition_text(&d),
    );
}

#[test]
fn the_manifest_states_the_canonical_definition_at_schema_1() {
    // The same logical values apart from `source_id`, which schema 1 has no
    // field for and which the manifest states decodes as 0. Written as an
    // override of the schema 3 value rather than a second literal, so the pair
    // cannot drift apart in anything else.
    let d = InstrumentDefinition {
        source_id: 0,
        ..canonical_definition_v3()
    };
    manifest_states::<InstrumentDefinition>(
        "instrument-definition-v1.bin",
        SIZE_V1,
        SCHEMA_VERSION_V1,
        &definition_rows(&d),
        &definition_text(&d),
    );
}

#[test]
fn the_manifest_states_the_canonical_manifest_summary() {
    manifest_states::<ManifestSummary>(
        "manifest-summary-v3.bin",
        ManifestSummary::SIZE,
        SCHEMA_VERSION,
        &manifest_summary_rows(&canonical_manifest_summary()),
        &[],
    );
}
