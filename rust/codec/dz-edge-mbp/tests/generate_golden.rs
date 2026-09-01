//! Writes the golden files. Ignored by default: it is a tool, not a check.
//!
//! `cargo test -p dz-edge-mbp --test generate_golden -- --ignored --exact
//! write_the_golden_vectors` regenerates them, and the diff is then the wire
//! change to justify.
#[path = "golden.rs"]
mod golden;

use dz_edge_core::AppMessage;
use std::path::PathBuf;

fn write<M: AppMessage>(message: &M, name: &str) {
    let mut buf = vec![0u8; M::SIZE];
    message.encode_into(&mut buf);
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/golden")
        .join(name);
    std::fs::write(&p, &buf).unwrap_or_else(|e| panic!("write {}: {e}", p.display()));
}

#[test]
#[ignore = "a generator, run by hand when the wire format changes"]
fn write_the_golden_vectors() {
    write(&golden::canonical_level_update(), "level-update-v3.bin");
    write(&golden::canonical_book_clear(), "book-clear-v3.bin");
    write(&golden::canonical_snapshot_begin(), "snapshot-begin-v3.bin");
    write(&golden::canonical_snapshot_level(), "snapshot-level-v3.bin");
    write(&golden::canonical_snapshot_end(), "snapshot-end-v3.bin");
}
