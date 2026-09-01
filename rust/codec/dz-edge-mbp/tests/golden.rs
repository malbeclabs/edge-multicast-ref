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
    assert_eq!(
        M::PORT_ROLES.len(),
        1,
        "a vector for a message carried on several roles would need one per role"
    );
    let flags = if M::PORT_ROLES[0] == PortRole::Snapshot {
        FLAG_SNAPSHOT
    } else {
        0
    };
    buf[2..4].copy_from_slice(&flags.to_le_bytes());
    buf
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
