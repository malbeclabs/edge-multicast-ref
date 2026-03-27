# Go Kernel-Socket and XDP Multicast Shred Receivers

## Context

The edge-multicast-ref repository contains Rust reference implementations for consuming Solana shred multicast feeds from DoubleZero edge infrastructure. The root README shows Go kernel-socket and XDP receivers as "planned". This plan covers both implementations, which mirror the Rust versions functionally but use idiomatic Go patterns.

Both programs receive GRE-encapsulated Solana shreds on a multicast feed, parse shred headers, and display live statistics (TUI or log mode).

---

## Repository Layout

```
go/
├── go.work                          # Go workspace linking modules
├── internal/
│   ├── go.mod
│   ├── shred/
│   │   ├── shred.go                 # Shred binary parser (NO Go solana-ledger equivalent)
│   │   └── shred_test.go
│   ├── stats/
│   │   ├── stats.go                 # SlotStats + Stats (shared between both receivers)
│   │   └── stats_test.go
│   ├── display/
│   │   ├── log.go                   # Log display mode
│   │   ├── tui.go                   # Bubbletea TUI base model
│   │   └── format.go               # Signature prefix, duration formatting
│   └── config/
│       ├── config.go                # Shared config types (DisplayConfig, StatsConfig)
│       └── config_test.go
├── kernel-receiver/
│   ├── go.mod
│   ├── main.go                      # Entry point, goroutine orchestration
│   ├── config.go                    # Kernel-specific config + CLI flags
│   ├── receiver.go                  # UDP multicast sockets, recv goroutines
│   ├── display.go                   # Dispatch to log/tui
│   ├── config.example.toml
│   └── README.md
└── xdp-receiver/
    ├── go.mod
    ├── main.go                      # Entry point, XDP attach, AF_XDP setup
    ├── config.go                    # XDP-specific config (mode, UMEM, queue)
    ├── xdp.go                       # eBPF loading via cilium/ebpf, map management
    ├── receiver.go                  # AF_XDP socket, UMEM, GRE header stripping
    ├── stats.go                     # Extends base stats with XDP counters
    ├── display.go                   # TUI with XDP stats panel, log with XDP counters
    ├── generate.go                  # //go:generate bpf2go directive
    ├── bpf/
    │   └── xdp_filter.c            # eBPF XDP program in C (port from Rust eBPF)
    ├── config.example.toml
    └── README.md
```

Shared code in `go/internal/` avoids duplication between the two receivers. Go workspace (`go.work`) links them.

---

## Key Design Decisions

### 1. Shred parsing: native Go binary parser

No Go equivalent of `solana-ledger::Shred`. We parse the binary format directly — the fields we need are at fixed offsets in the common header:

```
Offset  0-63:  Ed25519 signature (64 bytes)
Offset  64:    ShredVariant byte — bits 5-6 encode type: 1=Data, 2=Coding
Offset  65-72: Slot (uint64 LE)
Offset  73-76: Index (uint32 LE)
Offset  77-78: Version (uint16 LE)
Offset  79-82: FEC set index (uint32 LE)
```

Minimum payload: 83 bytes. Uses `encoding/binary.LittleEndian`. Test against pcap fixtures from `pcaps/`.

### 2. Concurrency: goroutines + sync.RWMutex

- Receiver goroutine(s) + display on main goroutine
- `sync.RWMutex` wrapping `Stats` (read-heavy, same pattern as Rust's `Arc<RwLock<Stats>>`)
- `context.WithCancel` + `os/signal.NotifyContext` for shutdown

### 3. TUI: bubbletea + lipgloss

The dominant Go TUI framework. Elm architecture fits the tick-based stats rendering.

### 4. XDP/eBPF: cilium/ebpf + C eBPF program

- eBPF XDP filter written in C (port from Rust eBPF — same parsing logic)
- `bpf2go` code generator embeds compiled eBPF in the Go binary
- `cilium/ebpf` handles program loading, map access, XDP attachment

### 5. AF_XDP: raw syscalls via golang.org/x/sys/unix

Go lacks a mature AF_XDP library. Use `unix.Socket(AF_XDP, ...)`, `SockaddrXDP`, `XDPUmemReg` directly. The API surface is small (~200-300 lines). If `github.com/asavie/xdp` is actively maintained at implementation time, prefer it.

### 6. Kernel receiver networking: goroutine-per-socket

More idiomatic than Rust's `poll()` approach. Two goroutines doing blocking `ReadFromUDP()`, context cancellation via `SetReadDeadline`.

---

## Implementation Phases

### Phase 1: Shared packages (`go/internal/`)

| Task | Description | Reference file |
|------|-------------|----------------|
| 1.1 | Directory structure, `go.work`, `go.mod` files | — |
| 1.2 | `internal/shred/shred.go` — binary shred parser + tests | `rust/kernel-receiver/src/shred_parser.rs` |
| 1.3 | `internal/stats/stats.go` — SlotStats, Stats, ring buffer eviction, rate calc + tests | `rust/kernel-receiver/src/stats.rs` |
| 1.4 | `internal/config/config.go` — shared DisplayConfig, StatsConfig types | `rust/kernel-receiver/src/config.rs` |
| 1.5 | `internal/display/` — format helpers, log mode, bubbletea TUI base | `rust/kernel-receiver/src/display/` |

### Phase 2: Kernel receiver (`go/kernel-receiver/`)

| Task | Description | Reference file |
|------|-------------|----------------|
| 2.1 | `config.go` — NetworkConfig, TOML loading, CLI flags | `rust/kernel-receiver/src/config.rs` |
| 2.2 | `receiver.go` — multicast socket setup, two recv goroutines | `rust/kernel-receiver/src/receiver.rs` |
| 2.3 | `main.go` — orchestration, signal handling | `rust/kernel-receiver/src/main.rs` |
| 2.4 | `display.go` — dispatch, kernel-specific TUI model | `rust/kernel-receiver/src/display/` |
| 2.5 | README, config.example.toml, integration test on doublezero1 | `rust/kernel-receiver/README.md` |

### Phase 3: XDP receiver (`go/xdp-receiver/`)

| Task | Description | Reference file |
|------|-------------|----------------|
| 3.1 | `bpf/xdp_filter.c` — port eBPF XDP filter from Rust to C | `rust/xdp-receiver/ebpf/src/main.rs` |
| 3.2 | `generate.go` + `bpf2go` setup, verify compilation | `rust/xdp-receiver/build.rs` |
| 3.3 | `xdp.go` — load eBPF, attach XDP (auto/native/skb), map ops | `rust/xdp-receiver/src/xdp.rs` |
| 3.4 | `receiver.go` — AF_XDP socket, UMEM, rings, `findUDPPayload` header stripping | `rust/xdp-receiver/src/receiver.rs` |
| 3.5 | `config.go` — XdpConfig (mode, UMEM size, frame size, queue) | `rust/xdp-receiver/src/config.rs` |
| 3.6 | `stats.go` — extend base with XDP counters; `display.go` — XDP stats panel | `rust/xdp-receiver/src/stats.rs`, `src/display/` |
| 3.7 | `main.go` — full orchestration: XDP → AF_XDP → XSKMAP → fill ring → recv → display | `rust/xdp-receiver/src/main.rs` |
| 3.8 | README, config.example.toml, integration test on bond0 with `--xdp-mode skb` | `rust/xdp-receiver/README.md` |

---

## Dependencies

**Kernel receiver:**
- `github.com/BurntSushi/toml` — TOML config
- `github.com/charmbracelet/bubbletea` — TUI
- `github.com/charmbracelet/lipgloss` — TUI styling
- `golang.org/x/net` — `ipv4` multicast socket ops

**XDP receiver (additional):**
- `github.com/cilium/ebpf` — eBPF program loading, map access, XDP attach
- `github.com/cilium/ebpf/cmd/bpf2go` — build-time code generation
- `golang.org/x/sys` — AF_XDP syscalls, mmap

**Build-time (XDP):** `clang` for eBPF C compilation

---

## Risks

| Risk | Mitigation |
|------|-----------|
| Shred variant byte decoding may miss edge cases | Test against real pcap data from `pcaps/`; only need data-vs-coding, not full deserialization |
| AF_XDP raw syscalls are complex | Start with kernel receiver (Phase 2) to validate shared code first; evaluate `github.com/asavie/xdp` freshness |
| eBPF C program may need kernel headers | Use `bpf2go` with minimal headers; XDP helpers are stable on kernel 5.x+ |

---

## Verification

**Kernel receiver:**
```bash
cd go/kernel-receiver && go build -o kernel-receiver .
sudo ./kernel-receiver --interface doublezero1 --mode log
# Expect: slot lines + [stats] summary matching Rust output
```

**XDP receiver:**
```bash
cd go/xdp-receiver && go generate ./... && go build -o xdp-receiver .
sudo ./xdp-receiver --physical-interface bond0 --xdp-mode skb --mode log
# Expect: slot lines + [stats] summary with XDP counters
```

Both should produce output equivalent to the Rust implementations running on the same machine.
