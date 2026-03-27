# Rust XDP Multicast Shred Receiver — Design Spec

**Date:** 2026-03-26
**Status:** Draft
**Scope:** Rust implementation, XDP receive path on physical NIC

## Overview

An XDP-based Rust implementation for consuming Solana shred multicast feeds from DoubleZero edge infrastructure. An eBPF program (written in Rust via aya) attaches to the physical NIC, parses through GRE encapsulation, filters for shred and heartbeat packets, and redirects them to an AF_XDP socket. Rust userspace reads packets from the AF_XDP RX ring, parses shred headers with `solana-ledger`, and displays live statistics via ratatui TUI or streaming log — same output interface as the kernel-socket receiver.

**Target audience:** Traders and operators familiar with the [kernel-socket receiver](../../rust/kernel-receiver/) who want lower-latency packet processing by bypassing the kernel network stack.

**Non-goals for this iteration:**
- FEC recovery / deshredding
- Heartbeat sending
- Zero-copy AF_XDP mode (documented as optimization, not implemented)
- Multi-queue / multi-socket binding
- Go / C XDP implementations

## Relationship to Kernel Receiver

The XDP receiver is a standalone binary at `rust/xdp-receiver/`. It copies and adapts several modules from `rust/kernel-receiver/`:

| Module | Reuse | Changes |
|--------|-------|---------|
| `config.rs` | Copy + adapt | Remove `interface`, add `physical_interface`, `[xdp]` section |
| `stats.rs` | Copy + adapt | Add XDP-specific counters (redirect/pass counts, ring fill, starvation) |
| `shred_parser.rs` | Copy verbatim | No changes |
| `display/tui.rs` | Copy + adapt | Add XDP stats panel |
| `display/log.rs` | Copy + adapt | Add XDP stats to summary line |
| `display/mod.rs` | Copy verbatim | No changes |
| `receiver.rs` | New | AF_XDP socket, UMEM, RX ring polling (replaces UDP socket recv) |
| `xdp.rs` | New | eBPF program loading and attaching via aya |
| `ebpf/` | New | Separate crate for the XDP eBPF program |

## Network Architecture

### Packet Path (Physical NIC)

The XDP program sees raw Ethernet frames before the kernel processes them:

```
Physical NIC (e.g. eth0, Mellanox mlx5)
  │
  ▼
XDP eBPF program (attached to NIC)
  │  Parse: Eth → Outer IP → GRE → Inner IP → UDP
  │  Filter: dst port 7733/5765, dst IP = multicast group
  │
  ├─ Match → XDP_REDIRECT → AF_XDP socket → Rust userspace
  │
  └─ No match → XDP_PASS → kernel network stack (normal processing)
```

### Header Layout (from pcap analysis)

```
Offset  0: Ethernet (14 bytes) — dst MAC, src MAC, EtherType 0x0800
Offset 14: Outer IPv4 (20 bytes) — src 4.42.212.122, dst 64.130.37.175, proto 47 (GRE)
Offset 34: GRE (4 bytes minimum) — flags 0x0000, protocol 0x0800
Offset 38: Inner IPv4 (20 bytes) — src 148.51.x.x, dst 233.84.178.1, proto 17 (UDP)
Offset 58: UDP (8 bytes) — dst port 7733 (shreds) or 5765 (heartbeats)
Offset 66: Payload — shred data (~1247-1272 bytes) or heartbeat (4 bytes)
```

Total header overhead before payload: 66 bytes (minimum, GRE flags may add 4-12 more bytes).

## Project Structure

```
rust/xdp-receiver/
├── Cargo.toml
├── config.example.toml
├── src/
│   ├── main.rs           # CLI, config, XDP setup, AF_XDP setup, thread spawning
│   ├── config.rs          # Config with XDP-specific fields
│   ├── stats.rs           # Stats with XDP-specific counters
│   ├── shred_parser.rs    # Wraps solana-ledger shred deserialization (same as kernel)
│   ├── receiver.rs        # AF_XDP socket setup, UMEM, RX ring polling, header stripping
│   ├── xdp.rs             # eBPF program loading/attaching via aya
│   ├── display/
│   │   ├── mod.rs         # Display mode dispatch
│   │   ├── tui.rs         # ratatui dashboard with XDP stats panel
│   │   └── log.rs         # Streaming logger with XDP stats
rust/xdp-receiver/ebpf/
├── Cargo.toml             # Separate crate, target bpfel-unknown-none
├── src/
│   └── main.rs            # XDP eBPF program: parse GRE, filter, redirect to AF_XDP
```

## eBPF Program

### Compilation

The eBPF program is a separate Cargo crate compiled to the `bpfel-unknown-none` target using `aya-ebpf`. It produces an ELF binary that the userspace loads at runtime via `aya::Ebpf::load()`.

### Logic

```
fn xdp_filter(ctx: XdpContext) -> u32:
    1. Read filter config from BPF Array map (multicast IP, shred port, heartbeat port)
    2. Parse Ethernet header
       - Bounds check: ctx.data + 14 <= ctx.data_end
       - Check EtherType == 0x0800 (IPv4), else XDP_PASS
    3. Parse outer IPv4 header
       - Bounds check: + 20 bytes
       - Check protocol == 47 (GRE), else XDP_PASS
       - Read IHL for variable header length
    4. Parse GRE header
       - Bounds check: + 4 bytes minimum
       - Check protocol == 0x0800 (IPv4 inner), else XDP_PASS
       - Check flags for C/K/S bits → advance offset by 4/8/12 additional bytes
    5. Parse inner IPv4 header
       - Bounds check: + 20 bytes
       - Check protocol == 17 (UDP), else XDP_PASS
       - Check dst IP == configured multicast group, else XDP_PASS
    6. Parse UDP header
       - Bounds check: + 8 bytes
       - Check dst port == shred_port OR heartbeat_port, else XDP_PASS
    7. Return XDP_REDIRECT (to AF_XDP socket)
```

### BPF Maps

- **Config map** (`Array<FilterConfig>`, 1 entry): holds multicast group IP (u32), shred port (u16), heartbeat port (u16). Written by userspace at startup.
- **XSK map** (`XskMap`): AF_XDP socket map for `XDP_REDIRECT`. Populated by userspace when creating the AF_XDP socket.
- **Stats map** (`PerCpuArray<XdpStats>`, 1 entry): counters for packets redirected, passed, and dropped (parse errors). Read by userspace for display.

## AF_XDP Receive Path

### Setup

1. Allocate UMEM: contiguous memory region (default 4MB), divided into fixed-size frames (default 2048 bytes). Registered with the AF_XDP socket.
2. Create AF_XDP socket bound to the physical NIC's specified RX queue (default queue 0), copy mode.
3. Populate the fill ring with frame addresses — tells the kernel where to write incoming packets.
4. Insert socket file descriptor into the XSK BPF map so `XDP_REDIRECT` knows where to send packets.

### RX Loop

1. `poll()` on the AF_XDP socket (with 100ms timeout for shutdown checks).
2. Read completed RX descriptors — each gives a UMEM offset and packet length.
3. For each packet:
   - Read UDP dst port at offset 60 (inner UDP header, after Eth+outerIP+GRE+innerIP). Account for variable GRE header length by re-checking GRE flags byte at offset 34.
   - If port == 7733: extract payload starting at offset 66+, feed to `shred_parser::parse_shred()`, update stats.
   - If port == 5765: increment heartbeat counter.
4. Return consumed frame addresses to the fill ring.
5. Periodically read BPF stats map for XDP program counters.

### UMEM Configuration

| Parameter | Default | Notes |
|-----------|---------|-------|
| UMEM size | 4MB | `umem_size` in config |
| Frame size | 2048 | `frame_size` in config, must be >= max packet size |
| Number of frames | 2048 | `umem_size / frame_size` |
| Fill ring size | 2048 | Same as frame count |
| Completion ring size | 2048 | Same as frame count |
| RX ring size | 2048 | Same as frame count |
| TX ring size | 0 | Not used (RX only) |

## Stats Tracking

Extends the kernel receiver's `Stats` struct with:

### XDP-Specific Counters

- `xdp_attach_mode: String` — "native" or "skb" (set once at startup)
- `xdp_redirected: u64` — packets matched filter and redirected to AF_XDP (from BPF stats map)
- `xdp_passed: u64` — packets that didn't match and went to kernel (from BPF stats map)
- `xdp_errors: u64` — packets that failed eBPF parsing (from BPF stats map)
- `afxdp_rx_fill_level: usize` — current fill ring occupancy
- `afxdp_fill_starvation: u64` — times the fill ring was empty when kernel needed a frame

All other stats (per-slot tracking, shreds/sec, heartbeats, parse errors) are identical to the kernel receiver.

## Display Modes

### TUI Mode

Same layout as kernel receiver plus an additional panel:

- **Top bar:** physical interface, multicast group, XDP mode, uptime, heartbeat count
- **Second panel (new):** XDP stats — redirected/passed/error counts, AF_XDP ring fill level, starvation count
- **Middle panel:** slot table (same as kernel receiver)
- **Bottom panel:** aggregate shred stats (same as kernel receiver)

### Log Mode

Same per-slot lines. Summary line extended:

```
[stats] shreds/sec=8432 data=5621 coding=2811 errors=0 heartbeats=47 (last: 230ms ago) xdp_mode=native redirected=94521 passed=12034 ring_fill=1847/2048
```

## Configuration

### Config File (`config.example.toml`)

```toml
[network]
physical_interface = "eth0"
multicast_group = "233.84.178.1"
shred_port = 7733
heartbeat_port = 5765

[xdp]
xdp_mode = "auto"          # "auto", "native", "skb"
umem_size = 4194304         # 4MB
frame_size = 2048
rx_queue = 0

[display]
mode = "tui"
refresh_hz = 4
log_interval_secs = 5

[stats]
max_slots = 32
```

### CLI Overrides

`--physical-interface`, `--multicast-group`, `--shred-port`, `--heartbeat-port`, `--xdp-mode`, `--rx-queue`, `--mode`, `--config`

## Threading Model

Two threads (same as kernel receiver):

### 1. Receiver Thread (spawned)

Polls AF_XDP RX ring via `poll()` with 100ms timeout. Strips GRE/IP/UDP headers, parses shred payloads, updates stats. Periodically reads BPF stats map. Checks `AtomicBool` shutdown flag.

### 2. Display Thread (main thread)

Runs TUI or log display. Reads `Arc<RwLock<Stats>>`. Handles `Ctrl+C` / `q`.

### Startup Sequence

1. Load config file + CLI overrides
2. Load eBPF ELF, attach XDP program to physical NIC (try native → fallback SKB if auto)
3. Write filter config to BPF config map
4. Create AF_XDP socket, allocate UMEM, bind to NIC RX queue
5. Populate fill ring with frame addresses
6. Insert AF_XDP socket fd into XSK map
7. Spawn receiver thread
8. Run display on main thread
9. On shutdown: set flag → join receiver → **detach XDP program** → close AF_XDP socket → restore terminal → exit

### Cleanup

The XDP program **must** be detached on exit. If the process crashes or is killed without cleanup, the XDP program remains attached to the NIC and keeps redirecting packets (which now go nowhere). The implementation should:

- Use `ctrlc` handler to trigger clean shutdown
- Implement cleanup in a `Drop` trait or explicit finally block
- Document how to manually detach a stuck XDP program: `ip link set dev eth0 xdp off`

## Linux Capabilities

The binary requires these capabilities instead of running as root:

```bash
sudo setcap cap_net_raw,cap_net_admin,cap_bpf,cap_perfmon=ep ./xdp-receiver
```

| Capability | Needed For |
|------------|-----------|
| `CAP_NET_RAW` | AF_XDP socket creation |
| `CAP_NET_ADMIN` | XDP program attach, NIC queue binding |
| `CAP_BPF` | Loading eBPF programs |
| `CAP_PERFMON` | BPF map access |

## Dependencies

### Userspace (`rust/xdp-receiver/Cargo.toml`)

```toml
[dependencies]
# Solana
solana-ledger = "2.2"
solana-sdk = "2.2"

# eBPF / XDP
aya = "0.13"
aya-log = "0.2"

# CLI + Config
clap = { version = "4", features = ["derive"] }
toml = "0.8"
serde = { version = "1", features = ["derive"] }

# TUI
ratatui = "0.29"
crossterm = "0.28"

# Networking / System
libc = "0.2"

# Signal handling
ctrlc = { version = "3", features = ["termination"] }

# Misc
anyhow = "1"
```

### eBPF (`rust/xdp-receiver/ebpf/Cargo.toml`)

```toml
[package]
name = "xdp-filter"
version = "0.1.0"
edition = "2021"

[dependencies]
aya-ebpf = "0.1"
aya-log-ebpf = "0.1"

[[bin]]
name = "xdp-filter"
path = "src/main.rs"
```

Compiled with: `cargo +nightly build -Z build-std=core --target bpfel-unknown-none --release`

## Future Considerations (Out of Scope)

- **Zero-copy AF_XDP mode** — requires hugepage UMEM allocation and driver support. Significant performance improvement for high packet rates. Document as an optimization path.
- **Multi-queue binding** — bind AF_XDP sockets to multiple NIC RX queues with per-CPU receive threads. Needed only if single-queue throughput is insufficient.
- **RSS steering** — configure NIC RSS (Receive Side Scaling) to direct multicast traffic to a specific RX queue, ensuring it hits our AF_XDP socket.
- **Forwarding to shredstream-proxy** — after receiving via XDP, forward packets to a local shredstream-proxy instance for FEC recovery and entry streaming.
