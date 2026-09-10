//! Write a directory of recorded payloads, one record each.
//!
//! What a source process in another language would send, written by this one so
//! that an offline run has something to replay. The values are the
//! cross-language golden vector's, so a subscriber's output can be read against
//! `testdata/golden/manifest.json`.

use std::path::PathBuf;

use dz_adapter_core::{Aggressor, Event, InstrumentRef, Scalar, SideUpdate, TradeFlags};
use dz_adapter_uds::RecordWriter;

fn arg(name: &str) -> Option<String> {
    args(name).into_iter().next()
}

/// Every value given for a flag, in the order they were written.
///
/// `--symbol` is repeatable because a publisher carrying several shards needs a
/// recording that names more than one instrument, and one file per symbol would
/// make the replay's name order — which is its receive order — depend on how a
/// script happened to interleave two invocations.
fn args(name: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        if a == name {
            if let Some(value) = args.next() {
                found.push(value);
            }
        }
    }
    found
}

fn main() -> std::io::Result<()> {
    let mut symbols = args("--symbol");
    if symbols.is_empty() {
        symbols.push("REPLAY-1".to_string());
    }
    let dir = PathBuf::from(arg("--dir").unwrap_or_else(|| ".".to_string()));
    std::fs::create_dir_all(&dir)?;

    // The handle is a placeholder: a record names the instrument by symbol, and
    // the reader resolves it against what its own runtime admitted. This value
    // is never encoded.
    let instrument = InstrumentRef::from_admission(0);

    let events = [
        Event::Quote {
            instrument,
            source_ts_ns: 1_700_000_000_000_000_000,
            bid: SideUpdate::Present {
                px: Scalar::text("999.95"),
                qty: Scalar::text("125.00"),
                source_count: Some(3),
            },
            ask: SideUpdate::Present {
                px: Scalar::text("1000.05"),
                qty: Scalar::text("72.50"),
                source_count: Some(4),
            },
        },
        Event::Trade {
            instrument,
            source_ts_ns: 1_700_000_000_000_000_001,
            px: Scalar::text("1000.00"),
            qty: Scalar::text("5.00"),
            aggressor: Aggressor::Buy,
            trade_id: Some(987_654_321),
            cumulative_volume: Some(Scalar::text("10000.00")),
            flags: TradeFlags {
                sweep: true,
                ..TradeFlags::NONE
            },
        },
        Event::Quote {
            instrument,
            source_ts_ns: 1_700_000_000_000_000_002,
            bid: SideUpdate::Gone,
            ask: SideUpdate::Present {
                px: Scalar::text("1000.05"),
                qty: Scalar::text("72.50"),
                source_count: None,
            },
        },
    ];

    // Every symbol carries the same three events, so that what a subscriber
    // reads on one channel instance can be read against what another read on
    // its own: a difference between two outputs is then the publisher's
    // partitioning and not the recording's.
    let mut writer = RecordWriter::new();
    let mut n = 0;
    for symbol in &symbols {
        for event in &events {
            let mut bytes = Vec::new();
            // A refusal names the event and costs that record, not the whole
            // recording: what a recorder does with one is count it and keep
            // going. **And it writes no file.** `write` appends nothing when it
            // refuses, so writing the buffer anyway would leave a zero-length
            // `.record` that a reader cannot decode — a refusal that cost the
            // replay rather than the record.
            if let Err(refused) = writer.write(symbol, event, &mut bytes) {
                eprintln!("dz-adapter-uds: {refused}");
                continue;
            }
            // Zero-padded, because a replay reads its directory in name order
            // and `10` sorts before `9`. A refused event leaves a gap in the
            // numbering, which costs nothing: the order is what the names carry
            // and a reader takes the files that exist.
            let path = dir.join(format!("{n:04}.record"));
            std::fs::write(&path, &bytes)?;
            println!("{} ({} bytes) {symbol}", path.display(), bytes.len());
            n += 1;
        }
    }
    Ok(())
}
