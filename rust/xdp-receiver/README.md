# Rust XDP Multicast Shred Receiver

High-performance Rust implementation that receives Solana shreds from a DoubleZero edge multicast feed using XDP (eXpress Data Path) and AF_XDP sockets, bypassing the kernel network stack for minimal latency. Parses shred headers and displays live statistics via TUI or log output.

## How It Works

An eBPF XDP program attaches to the physical NIC and parses incoming packets through the full encapsulation stack (Eth → outer IP → GRE → inner IP → UDP). Packets matching the configured multicast group and ports are redirected to an AF_XDP socket. Userspace reads packets from the AF_XDP RX ring, strips encapsulation headers, and parses shred payloads with `solana-ledger`.

This approach processes packets before they enter the kernel network stack, avoiding socket buffer overhead and context switches.

## Prerequisites

- **Linux only** (kernel 5.4+ recommended for XDP support)
- Rust stable toolchain (1.75+)
- Rust nightly toolchain with `rust-src` (for eBPF compilation)
- `bpf-linker`
- `clang` and `llvm` (required by `libxdp-sys` build)
- `libclang-dev` (required by `solana-ledger` for RocksDB)
- `libelf-dev` and `zlib1g-dev` (required by `libbpf-sys`)
- `libpcap-dev` (required by `libxdp-sys`)
- `m4` (required by `libxdp-sys` configure)

```bash
# Install nightly toolchain for eBPF
rustup toolchain install nightly --component rust-src

# Install bpf-linker
cargo install bpf-linker

# Install system deps (Ubuntu/Debian)
apt install clang llvm libclang-dev libelf-dev zlib1g-dev libpcap-dev m4
```

## Build

```bash
cd rust/xdp-receiver
cargo build --release
```

This compiles both the eBPF XDP program (via `build.rs`) and the userspace binary. The eBPF ELF is embedded in the binary at compile time.

The binary is at `target/release/edge-multicast-xdp-receiver`.

## Capabilities

The binary needs elevated privileges to load eBPF programs and create AF_XDP sockets. Either run as root or set capabilities:

```bash
sudo setcap cap_net_raw,cap_net_admin,cap_bpf,cap_perfmon=ep ./target/release/edge-multicast-xdp-receiver
```

## Usage

### Quick Start

```bash
./edge-multicast-xdp-receiver --physical-interface eth0
```

### With CLI Options

```bash
./edge-multicast-xdp-receiver \
    --physical-interface eth0 \
    --multicast-group 233.84.178.1 \
    --shred-port 7733 \
    --xdp-mode auto \
    --rx-queue 0 \
    --mode tui
```

### With Config File

```bash
cp config.example.toml config.toml
# edit config.toml as needed
./edge-multicast-xdp-receiver --config config.toml
```

CLI arguments override config file values.

### Display Modes

**TUI mode** (default) — live dashboard with slot table, XDP stats panel, and aggregate stats:

```bash
./edge-multicast-xdp-receiver --mode tui
```

Press `q` or `Esc` to quit.

**Log mode** — machine-parseable streaming output with XDP counters:

```bash
./edge-multicast-xdp-receiver --mode log
```

Output format:

```
slot=312345678 sig=d88e..202a data=67 coding=34 fec_sets=3 age_ms=412
[stats] shreds/sec=8432 data=5621 coding=2811 errors=0 heartbeats=47 (last: 230ms ago) xdp_mode=native redirected=94521 passed=12034 ring_fill=2048/2048
```

## Configuration

See [config.example.toml](config.example.toml) for all options:

| Section | Key | Default | Description |
|---------|-----|---------|-------------|
| `network` | `physical_interface` | `eth0` | Physical NIC to attach XDP program to |
| `network` | `multicast_group` | `233.84.178.1` | Multicast group address to filter |
| `network` | `shred_port` | `7733` | UDP port for shred packets |
| `network` | `heartbeat_port` | `5765` | UDP port for heartbeat packets |
| `xdp` | `xdp_mode` | `auto` | XDP attach mode: `auto`, `native`, `skb` |
| `xdp` | `umem_size` | `4194304` | AF_XDP UMEM size in bytes (4MB) |
| `xdp` | `frame_size` | `2048` | UMEM frame size in bytes |
| `xdp` | `rx_queue` | `0` | NIC RX queue to bind AF_XDP socket |
| `display` | `mode` | `tui` | Display mode: `tui` or `log` |
| `display` | `refresh_hz` | `4` | TUI refresh rate |
| `display` | `log_interval_secs` | `5` | Log mode print interval |
| `stats` | `max_slots` | `32` | Number of recent slots to track |

## Architecture

```
             Physical NIC (eth0)
                    │
         ┌──────────┴──────────┐
         │  XDP eBPF Program   │
         │  Parse: Eth→IP→GRE  │
         │  →IP→UDP            │
         │  Filter: mcast+port │
         └───┬────────────┬────┘
          matched       unmatched
             │              │
        XDP_REDIRECT    XDP_PASS
             │          (to kernel)
    ┌────────┴────────┐
    │  AF_XDP Socket  │
    │  UMEM RX Ring   │
    └────────┬────────┘
             │
    ┌────────┴────────────────┐
    │   Receiver Thread       │
    │  Strip GRE headers      │
    │  Parse shreds via       │
    │  solana-ledger           │
    └────────┬────────────────┘
             │
      Arc<RwLock<Stats>>
             │
    ┌────────┴────────────────┐
    │   Main Thread           │
    │  TUI (ratatui) or       │
    │  Log (stdout)           │
    └─────────────────────────┘
```

Two threads:
1. **Receiver thread** — polls AF_XDP RX ring, strips GRE encapsulation, parses shred headers, updates shared stats, periodically reads eBPF per-CPU counters
2. **Main thread** — runs the display (TUI or log mode), handles shutdown signals

## XDP Modes

| Mode | Flag | Description |
|------|------|-------------|
| `auto` | — | Try native first, fall back to SKB |
| `native` | `XDP_FLAGS_DRV_MODE` | Driver-level XDP (requires NIC driver support) |
| `skb` | `XDP_FLAGS_SKB_MODE` | Generic/SKB mode (works on any NIC, lower performance) |

## What It Tracks

- Per-slot: data/coding shred counts, FEC set count, highest shred index, signature prefix, arrival timestamps
- Global: total shreds (data + coding), heartbeat count + recency, parse errors, shreds/sec rate
- XDP: attach mode, packets redirected/passed/errored (from eBPF per-CPU counters), fill ring level, fill starvation count
- Ring buffer of 32 most recent slots (configurable)

## Manual XDP Detach

If the process crashes without cleaning up, detach XDP manually:

```bash
ip link set dev eth0 xdp off
```

## Limitations

- **Receive-only** — does not send heartbeats (the DoubleZero client handles this)
- **No FEC recovery** — tracks shred inventory only, does not reconstruct missing data shreds
- **No deshredding** — does not reassemble shreds into Solana entries/transactions
- **Single queue** — binds to one NIC RX queue (multi-queue support is possible but not implemented)
- **Leader identity** — uses signature prefix as a proxy identifier (full leader schedule lookup is out of scope)

## Design Spec

See [docs/2026-03-26-rust-xdp-receiver-design.md](../../docs/2026-03-26-rust-xdp-receiver-design.md) for the full design specification.
