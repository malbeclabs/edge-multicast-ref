//! Golden vectors: the cross-language contract.
//!
//! These bytes are the specification's meaning made concrete. Every
//! implementation in every language must reproduce them. A change here is a
//! wire change and must be justified against edge-feed-spec, never adjusted to
//! match code that started failing.

use dz_edge_core::{AppMessage, SCHEMA_VERSION, SCHEMA_VERSION_V1};
use dz_edge_refdata::{InstrumentDefinition, ManifestSummary, LEG_LEN, SYMBOL_LEN};
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
    assert_eq!(b.to_vec(), golden("manifest-summary-v3.bin"));
}

#[test]
fn manifest_summary_golden_vector_decodes_to_canonical_values() {
    assert_eq!(
        ManifestSummary::decode(&golden("manifest-summary-v3.bin")).unwrap(),
        canonical_manifest_summary()
    );
}
