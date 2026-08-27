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
