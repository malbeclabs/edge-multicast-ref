# Rust Kernel-Socket Multicast Shred Receiver

Minimal Rust implementation that receives Solana shreds from a DoubleZero edge multicast feed via standard kernel UDP sockets, parses shred headers, and displays live statistics.

## Prerequisites

- Rust toolchain (stable, 1.75+)
- Linux with a configured `doublezero1` GRE tunnel interface
- `libclang` (required by the `solana-ledger` dependency for RocksDB)

On Ubuntu/Debian:

```bash
apt install libclang-dev
```

On macOS (for development/cross-compilation only — the tool targets Linux):

```bash
# libclang ships with Xcode Command Line Tools
# You may need to set:
export DYLD_FALLBACK_LIBRARY_PATH=/Library/Developer/CommandLineTools/usr/lib
```

## Build

```bash
cd rust/kernel-receiver
cargo build --release
```

The binary is at `target/release/edge-multicast-receiver`.

## Usage

### Quick Start

With defaults (interface `doublezero1`, multicast group `233.84.178.1`, shred port `7733`):

```bash
./edge-multicast-receiver
```

### With CLI Options

```bash
./edge-multicast-receiver \
    --interface doublezero1 \
    --multicast-group 233.84.178.1 \
    --shred-port 7733 \
    --mode tui
```

### With Config File

```bash
cp config.example.toml config.toml
# edit config.toml as needed
./edge-multicast-receiver --config config.toml
```

CLI arguments override config file values.

### Display Modes

**TUI mode** (default) — live dashboard with slot table and aggregate stats:

```bash
./edge-multicast-receiver --mode tui
```

Press `q` or `Esc` to quit.

**Log mode** — machine-parseable streaming output:

```bash
./edge-multicast-receiver --mode log
```

Output format:

```
slot=312345678 sig=d88e..202a data=67 coding=34 fec_sets=3 age_ms=412
[stats] shreds/sec=8432 data=5621 coding=2811 errors=0 heartbeats=47 (last: 230ms ago)
```

## Configuration

See [config.example.toml](config.example.toml) for all options:

| Section | Key | Default | Description |
|---------|-----|---------|-------------|
| `network` | `interface` | `doublezero1` | Network interface to bind to |
| `network` | `multicast_group` | `233.84.178.1` | Multicast group address |
| `network` | `shred_port` | `7733` | UDP port for shred packets |
| `network` | `heartbeat_port` | `5765` | UDP port for heartbeat packets |
| `network` | `recv_buffer_size` | `8388608` | Socket receive buffer (bytes) |
| `display` | `mode` | `tui` | Display mode: `tui` or `log` |
| `display` | `refresh_hz` | `4` | TUI refresh rate |
| `display` | `log_interval_secs` | `5` | Log mode print interval |
| `stats` | `max_slots` | `32` | Number of recent slots to track |

## Architecture

```
                    ┌─────────────────────────┐
                    │     doublezero1 (GRE)    │
                    │  multicast 233.84.178.1  │
                    └──────┬──────────┬────────┘
                           │          │
                    port 7733    port 5765
                     shreds     heartbeats
                           │          │
                    ┌──────┴──────────┴────────┐
                    │    Receiver Thread        │
                    │  poll() on 2 UDP sockets  │
                    │  parse shreds via         │
                    │  solana-ledger            │
                    └──────────┬───────────────┘
                               │
                    Arc<RwLock<Stats>>
                               │
                    ┌──────────┴───────────────┐
                    │    Main Thread            │
                    │  TUI (ratatui) or         │
                    │  Log (stdout)             │
                    └──────────────────────────┘
```

Two threads:
1. **Receiver thread** — tight `poll()` loop reading from two UDP sockets, parsing shred headers, updating shared stats
2. **Main thread** — runs the display (TUI or log mode), handles shutdown signals

## What It Tracks

- Per-slot: data/coding shred counts, FEC set count, highest shred index, signature prefix, arrival timestamps
- Global: total shreds (data + coding), heartbeat count + recency, parse errors, shreds/sec rate
- Ring buffer of 32 most recent slots (configurable)

## Limitations

- **Receive-only** — does not send heartbeats (the DoubleZero client handles this)
- **No FEC recovery** — tracks shred inventory only, does not reconstruct missing data shreds
- **No deshredding** — does not reassemble shreds into Solana entries/transactions
- **Leader identity** — uses signature prefix as a proxy identifier (full leader schedule lookup is out of scope)
