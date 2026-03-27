# Rust Kernel-Socket Multicast Shred Receiver — Design Spec

**Date:** 2026-03-26
**Status:** Draft
**Scope:** Rust implementation, kernel socket receive path only

## Overview

A minimal Rust reference implementation for consuming Solana shred multicast feeds from DoubleZero edge infrastructure. The tool receives shreds via a kernel GRE tunnel interface (`doublezero1`), parses shred headers using upstream Agave `solana-ledger` crates, tracks per-slot and aggregate statistics in memory, and displays them via a ratatui TUI or streaming stdout log.

**Target audience:** Traders and operators already familiar with tools like the [jito shredstream-proxy](https://github.com/jito-labs/shredstream-proxy), who want to consume DoubleZero edge multicast feeds directly.

**Non-goals for this iteration:**
- FEC recovery / deshredding
- Heartbeat sending (the DoubleZero client handles this)
- XDP receive path (separate future implementation)
- Go / C implementations (separate future implementations)

## Repo Context

This implementation lives within a polyglot reference design repo, organized language-first:

```
rust/kernel-receiver/    <-- this spec
go/                      <-- future
c/                       <-- future
pcaps/                   <-- shared sample captures
```

## Network Architecture

### Feed Structure (from pcap analysis)

DoubleZero edge delivers Solana shreds as GRE-encapsulated UDP multicast:

```
Physical NIC:  Eth → Outer IP (4.42.212.122 → 64.130.37.175) → GRE → Inner IP → UDP → Shred
GRE interface: Inner IP (148.51.x.x → 233.84.178.1) → UDP → Shred payload
```

The Linux kernel handles GRE de-encapsulation. Packets arriving on the `doublezero1` interface are clean UDP — identical to what shredstream-proxy's recv path sees.

**Two packet types on the feed:**
- **Shred packets** — dst port 7733, ~1247-1272 bytes, multicast to `233.84.178.1`
- **Heartbeat packets** — dst port 5765, 4 bytes (`44 5a 00 01` / "DZ" + version), same multicast group

### Receive Path

1. Create two `UdpSocket`s via `socket2`:
   - Shred socket bound to `0.0.0.0:7733`
   - Heartbeat socket bound to `0.0.0.0:5765`
2. Join multicast group `233.84.178.1` on the `doublezero1` interface via `IP_ADD_MEMBERSHIP` (using interface IP or index)
3. Set `SO_RCVBUF` to 8MB on both sockets to absorb bursts
4. Multiplex both sockets via `libc::poll` in a single recv thread

## Project Structure

```
rust/kernel-receiver/
├── Cargo.toml
├── config.example.toml
├── src/
│   ├── main.rs          # CLI parsing, config loading, thread spawning
│   ├── config.rs        # TOML config + CLI override structs
│   ├── receiver.rs      # UDP socket setup, multicast join, recv loop
│   ├── shred_parser.rs  # Wraps solana-ledger shred deserialization
│   ├── stats.rs         # Shared stats: per-slot shred counts, rates, leader tracking
│   ├── display/
│   │   ├── mod.rs       # Display mode enum + trait
│   │   ├── tui.rs       # ratatui dashboard
│   │   └── log.rs       # Streaming stdout logger
```

## Shred Parsing

Uses `solana_ledger::shred::Shred::new_from_serialized_shred()` from upstream Agave crates to parse each UDP payload. Extracts:

- **Shred variant** — data vs coding (discriminator byte)
- **Slot** — which slot this shred belongs to
- **Shred index** — position within the slot
- **FEC set index** — erasure coding group identifier
- **Version** — shred version field

The shred signature is the leader's Ed25519 signature, but extracting the leader pubkey requires a leader schedule (not available in this receive-only tool). Instead, we track the **signing pubkey** by verifying/recovering it from the signature if feasible, or simply group shreds by slot and label slots by their observed signature prefix as a proxy identifier. Full leader identity mapping is out of scope.

No FEC recovery. No deshredding. Parse errors increment an error counter and the packet is skipped.

## Stats Tracking

Shared between recv thread and display thread via `Arc<RwLock<Stats>>`.

### Global Counters

- Total data shreds received
- Total coding shreds received
- Total heartbeats received
- Parse errors
- Shreds/sec (rolling 1-second window)

### Per-Slot Tracking (Ring Buffer, 32 Slots)

- Slot number
- Data shred count + highest index seen
- Coding shred count
- FEC set indices seen
- Signature prefix (from first shred parsed for that slot, used as proxy leader ID)
- First/last shred arrival timestamps

Old slots are evicted as new ones arrive. Memory is bounded.

### Per-Leader Summary

- Shred count per leader over a recent window

## Display Modes

Selectable via `--mode tui` (default) or `--mode log`, or `display.mode` in config file.

### TUI Mode (ratatui)

- **Top bar:** interface name, multicast group, uptime, heartbeat count + last-seen timestamp
- **Middle panel:** table of recent slots — slot number, leader (truncated pubkey), data/coding shred counts, FEC sets seen, age
- **Bottom panel:** aggregate stats — shreds/sec, data/coding ratio, error count
- Refreshes at ~4Hz
- `q` to quit

### Log Mode

- Per-slot line as slots complete or on timeout: `slot=312345678 leader=Ab3x..7f2q data=67 coding=34 fec_sets=3 elapsed_ms=412`
- Periodic summary every N seconds: `[stats] shreds/sec=8432 data=5621 coding=2811 errors=0 heartbeats=47`
- Machine-parseable, grep-friendly

Both modes handle `Ctrl+C` cleanly.

## Configuration

### Config File (`config.example.toml`)

```toml
[network]
interface = "doublezero1"
multicast_group = "233.84.178.1"
shred_port = 7733
heartbeat_port = 5765
recv_buffer_size = 8388608  # 8MB

[display]
mode = "tui"  # "tui" or "log"
refresh_hz = 4
log_interval_secs = 5

[stats]
max_slots = 32  # ring buffer size for recent slots
```

### CLI Overrides

CLI args override any config file value: `--interface`, `--multicast-group`, `--shred-port`, `--heartbeat-port`, `--mode`, `--config` (path to config file, defaults to `./config.toml`).

Uses `clap` (derive) for CLI, `serde` + `toml` for config deserialization, with CLI values merged over file values.

## Threading Model

Two threads:

### 1. Recv Thread (spawned)

Tight loop multiplexing two UDP sockets via `libc::poll`:
- On shred socket readable: `recv_from()` → `Shred::new_from_serialized_shred()` → update stats
- On heartbeat socket readable: `recv_from()` → increment heartbeat counter + update last-seen timestamp
- Checks `AtomicBool` shutdown flag each iteration

### 2. Display Thread (main thread)

Runs either:
- **TUI:** ratatui event loop with crossterm backend. Reads `RwLock<Stats>` on tick. Handles keyboard input (`q` to quit).
- **Log:** periodic `println!` on a timer. Reads stats each interval.

Handles `Ctrl+C` via signal handler that sets the shared `AtomicBool` shutdown flag.

### Startup Sequence

1. Load config file (if present) + CLI overrides
2. Create and configure sockets (bind, multicast join, `SO_RCVBUF`)
3. Spawn recv thread
4. Run display on main thread
5. On shutdown: set flag → join recv thread → restore terminal (if TUI) → exit

## Dependencies

```toml
[dependencies]
# Solana - upstream Agave, latest stable
solana-ledger = "2.2"
solana-sdk = "2.2"

# CLI + Config
clap = { version = "4", features = ["derive"] }
toml = "0.8"
serde = { version = "1", features = ["derive"] }

# TUI
ratatui = "0.29"
crossterm = "0.28"

# Networking
socket2 = "0.5"

# Misc
anyhow = "1"
```

No async runtime. No crossbeam. Deliberately minimal.

## Future Considerations (Out of Scope)

- **XDP receive path:** Will attach to physical NIC, parse Eth → outer IP → GRE → inner IP → UDP → shred in eBPF. Shares the same stats/display layer.
- **FEC recovery:** Add `solana-ledger` Reed-Solomon recovery to reconstruct missing data shreds from coding shreds.
- **Adaptation to shredstream-proxy:** The recv + parse path here could be integrated as an alternative input source for shredstream-proxy's forwarding pipeline.
- **Go and C implementations:** Same functionality, different languages, same repo.
