# Go Kernel-Socket Multicast Shred Receiver

Minimal Go implementation that receives Solana shreds from a DoubleZero edge multicast feed via standard kernel UDP sockets, parses shred headers, and displays live statistics. Uses a native Go shred parser (no cgo) that handles the binary format directly, supporting both legacy and Merkle shred variants.

## Prerequisites

- Go 1.23+
- Linux with a configured `doublezero1` GRE tunnel interface

Dependencies are managed via `go.mod` (notably `golang.org/x/net` for multicast socket support).

## Build

```bash
cd go/kernel-receiver
go build -o kernel-receiver .
```

## Usage

### Quick Start

With defaults (interface `doublezero1`, multicast group `233.84.178.1`, shred port `7733`):

```bash
sudo ./kernel-receiver
```

### With CLI Options

```bash
sudo ./kernel-receiver \
    --interface doublezero1 \
    --multicast-group 233.84.178.1 \
    --shred-port 7733 \
    --mode tui
```

### With Config File

```bash
cp config.example.toml config.toml
# edit config.toml as needed
sudo ./kernel-receiver --config config.toml
```

CLI flags override config file values.

### Display Modes

**TUI mode** (default) — live dashboard with slot table and aggregate stats (powered by bubbletea):

```bash
sudo ./kernel-receiver --mode tui
```

Press `q` or `Esc` to quit.

**Log mode** — machine-parseable streaming output:

```bash
sudo ./kernel-receiver --mode log
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
                    +---------------------------+
                    |     doublezero1 (GRE)     |
                    |  multicast 233.84.178.1   |
                    +------+----------+---------+
                           |          |
                    port 7733    port 5765
                     shreds     heartbeats
                           |          |
                    +------+----------+---------+
                    |  Shred Goroutine  |  HB   |
                    |  UDP recv loop    |  recv  |
                    |  native Go shred  |  loop  |
                    |  parser (no cgo)  |        |
                    +------+----------+---------+
                           |          |
                      sync.RWMutex shared Stats
                               |
                    +----------+----------------+
                    |    Main Goroutine          |
                    |  TUI (bubbletea) or        |
                    |  Log (stdout)              |
                    +---------------------------+
```

Goroutine-per-socket architecture:
1. **Shred goroutine** — reads from the shred UDP socket, parses shred headers with the native Go parser, updates shared stats
2. **Heartbeat goroutine** — reads from the heartbeat UDP socket, updates heartbeat stats
3. **Main goroutine** — runs the display (TUI or log mode), handles shutdown signals

## What It Tracks

- Per-slot: data/coding shred counts, FEC set count, highest shred index, signature prefix, arrival timestamps
- Global: total shreds (data + coding), heartbeat count + recency, parse errors, shreds/sec rate
- Ring buffer of 32 most recent slots (configurable)

## Limitations

- **Receive-only** — does not send heartbeats (the DoubleZero client handles this)
- **No FEC recovery** — tracks shred inventory only, does not reconstruct missing data shreds
- **No deshredding** — does not reassemble shreds into Solana entries/transactions
- **Leader identity** — uses signature prefix as a proxy identifier (full leader schedule lookup is out of scope)
