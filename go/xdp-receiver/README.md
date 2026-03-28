# Go XDP Multicast Shred Receiver

High-performance Go implementation that receives Solana shreds from a DoubleZero edge multicast feed using XDP (eXpress Data Path) and AF_XDP sockets, bypassing the kernel network stack for minimal latency. Parses shred headers and displays live statistics via TUI or log output.

## How It Works

An eBPF XDP program (written in C, compiled at build time via `cilium/ebpf` `bpf2go`) attaches to the physical NIC and parses incoming packets through the full encapsulation stack (Eth -> outer IP -> GRE -> inner IP -> UDP). Packets matching the configured multicast group and ports are redirected to an AF_XDP socket. Userspace reads packets from the AF_XDP RX ring, strips encapsulation headers, and parses shred payloads with the native Go shred parser.

This approach processes packets before they enter the kernel network stack, avoiding socket buffer overhead and context switches.

## Prerequisites

- **Linux only** (kernel 5.4+ recommended for XDP support)
- Go 1.23+
- `clang` (for eBPF compilation via `bpf2go`)
- `libelf-dev` and `zlib1g-dev`

```bash
# Install system deps (Ubuntu/Debian)
apt install clang libelf-dev zlib1g-dev
```

## Build

```bash
cd go/xdp-receiver
go generate ./...
go build -o xdp-receiver .
```

`go generate` invokes `bpf2go` to compile the C eBPF program into Go-embedded ELF objects. The resulting `.o` files and Go bindings are checked in, so `go generate` is only needed when modifying the eBPF C source.

## Capabilities

The binary needs elevated privileges to load eBPF programs and create AF_XDP sockets. Either run as root or set capabilities:

```bash
sudo setcap cap_net_raw,cap_net_admin,cap_bpf,cap_perfmon=ep ./xdp-receiver
```

## Usage

### Quick Start

```bash
sudo ./xdp-receiver --physical-interface eth0
```

### With CLI Options

```bash
sudo ./xdp-receiver \
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
sudo ./xdp-receiver --config config.toml
```

CLI flags override config file values.

### Display Modes

**TUI mode** (default) — live dashboard with slot table, XDP stats panel, and aggregate stats:

```bash
sudo ./xdp-receiver --mode tui
```

Press `q` or `Esc` to quit.

**Log mode** — machine-parseable streaming output with XDP counters:

```bash
sudo ./xdp-receiver --mode log
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
                    |
         +----------+----------+
         |  XDP eBPF Program   |
         |  (C, via bpf2go)    |
         |  Parse: Eth->IP->GRE|
         |  ->IP->UDP          |
         |  Filter: mcast+port |
         +---+------------+----+
          matched       unmatched
             |              |
        XDP_REDIRECT    XDP_PASS
             |          (to kernel)
    +--------+--------+
    |  AF_XDP Socket  |
    |  UMEM RX Ring   |
    +--------+--------+
             |
    +--------+----------------+
    |   Receiver Goroutine    |
    |  Strip GRE headers      |
    |  Parse shreds via       |
    |  native Go parser       |
    +--------+----------------+
             |
       sync.RWMutex shared Stats
             |
    +--------+----------------+
    |   Main Goroutine        |
    |  TUI (bubbletea) or     |
    |  Log (stdout)           |
    +-------------------------+
```

Two goroutines:
1. **Receiver goroutine** — polls AF_XDP RX ring, strips GRE encapsulation, parses shred headers, updates shared stats, periodically reads eBPF per-CPU counters
2. **Main goroutine** — runs the display (TUI or log mode), handles shutdown signals

## XDP Modes

| Mode | Flag | Description |
|------|------|-------------|
| `auto` | -- | Try native first, fall back to SKB |
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
