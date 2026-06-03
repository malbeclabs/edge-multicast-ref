# DoubleZero Edge Multicast Reference Designs

Reference implementations for consuming Solana shred multicast feeds from [DoubleZero](https://doublezero.xyz) edge infrastructure.

## What This Is

DoubleZero delivers Solana shreds via GRE-encapsulated UDP multicast. These reference designs show how to receive, parse, and monitor that feed using kernel sockets (simple) and XDP (high performance).

The feed arrives on a GRE tunnel interface (e.g. `doublezero1`) as clean UDP packets — the kernel handles GRE de-encapsulation. Two packet types are present on the feed:

- **Shred packets** (port 7733) — Solana shreds (~1247-1272 bytes each)
- **Heartbeat packets** (port 5765) — 4-byte DoubleZero liveness probes

## Implementations

| Language | Kernel Sockets | XDP |
|----------|---------------|-----|
| **Rust** | [rust/kernel-receiver](rust/kernel-receiver/) | [rust/xdp-receiver](rust/xdp-receiver/) |
| **Go** | [go/kernel-receiver](go/kernel-receiver/) | [go/xdp-receiver](go/xdp-receiver/) |
| **C** | planned | planned |

### GRE Decapsulator

[gre-decap](gre-decap/) is a standalone XDP program that strips GRE encapsulation inline on the physical NIC. After decap, the kernel sees plain multicast UDP — no tunnel interface or application changes needed. Useful when you want existing socket-based applications to receive the feed without a GRE tunnel.

### Top-of-Book Feeds

The repo also includes components for a second feed type — binary market data frames over multicast (DZ-TOB v0.1.0):

| Component | Description |
|---|---|
| [go/topofbook-parser](go/topofbook-parser/) | Multicast subscriber. Decodes frames, writes JSON/CSV to a file or Unix socket, exposes Prometheus metrics |
| [go/topofbook-bot](go/topofbook-bot/) | Reference subscriber that reads the parser's Unix socket, filters by symbol, exposes per-symbol TOB state as Prometheus metrics, and optionally writes every tick to ClickHouse |
| [demo/](demo/) | One-command Docker Compose stack: parser + bot + ClickHouse + Grafana, pre-provisioned dashboard |

### Depth-of-Book Demo

A sibling pipeline to top-of-book, consuming the DZ-DOB v0.1.0 feed:

- **[`go/depthofbook-parser`](go/depthofbook-parser/)** — three-port multicast subscriber + binary wire decoder, broadcasts decoded JSONL on a Unix socket
- **[`go/depthofbook-bot`](go/depthofbook-bot/)** — book builder + persistor, maintains in-memory MBO order books and writes per-event rows + coalesced top-N level snapshots to ClickHouse
- **[`demo`](demo/)** — extended docker-compose stack with a new "DZ Depth-of-Book" Grafana dashboard featuring book ladder, depth heatmap, spread, trade tape, and event-rate panels

## Target Audience

Traders and operators already familiar with tools like the [jito shredstream-proxy](https://github.com/jito-labs/shredstream-proxy) who want to consume DoubleZero edge multicast feeds directly.

## Sample Captures

The `pcaps/` directory contains sample packet captures from a live DoubleZero edge feed. These can be inspected with Wireshark using the `solana.shreds` dissector (decode as UDP port 7733 -> `solana.shreds`).

## Network Setup

These tools expect a working DoubleZero GRE tunnel interface. The DoubleZero client handles tunnel setup and heartbeat responses — the reference designs are receive-only.

```
Physical NIC:  Eth → Outer IP → GRE → Inner IP → UDP → Shred
GRE interface: Inner IP (148.51.x.x → 233.84.178.1) → UDP → Shred payload
```

Example interface:

```
$ ip a s doublezero1
26: doublezero1@NONE: <POINTOPOINT,NOARP,UP,LOWER_UP> mtu 1476 qdisc noqueue state UNKNOWN
    link/gre 64.130.37.175 peer 4.42.212.122
    inet 169.254.10.233/31 scope link doublezero1
```

## License

Licensed under the **Apache License 2.0**.

See [LICENSE](./LICENSE) for details.
