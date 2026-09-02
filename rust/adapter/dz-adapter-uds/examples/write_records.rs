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
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        if a == name {
            return args.next();
        }
    }
    None
}

fn main() -> std::io::Result<()> {
    let symbol = arg("--symbol").unwrap_or_else(|| "REPLAY-1".to_string());
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

    let mut writer = RecordWriter::new();
    for (n, event) in events.iter().enumerate() {
        let mut bytes = Vec::new();
        writer.write(&symbol, event, &mut bytes);
        // Zero-padded, because a replay reads its directory in name order and
        // `10` sorts before `9`.
        let path = dir.join(format!("{n:04}.record"));
        std::fs::write(&path, &bytes)?;
        println!("{} ({} bytes)", path.display(), bytes.len());
    }
    Ok(())
}
