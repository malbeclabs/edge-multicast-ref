# DoubleZero Edge Multicast Reference Designs

Reference implementations for consuming [DoubleZero](https://doublezero.xyz) edge multicast feeds.

DoubleZero delivers data as GRE-encapsulated UDP multicast — the kernel handles GRE de-encapsulation, so applications see clean UDP. This repo covers two kinds of feed that arrive over that transport, each with its own reference code:

- **[Solana shreds](#shred-receivers)** — raw shred packets received directly off the wire. High-throughput, receive-only receivers in Rust and Go, over both kernel sockets and XDP. *This is the repo's original focus.*
- **[Market data](#market-data-pipelines)** — DoubleZero's binary market-data feeds (see [edge-feed-spec](https://github.com/malbeclabs/edge-feed-spec)), decoded and monitored through a parse → build → store → visualize demo stack.

## Transport

The feed arrives on a GRE tunnel interface (e.g. `doublezero1`) as UDP multicast. The DoubleZero client handles tunnel setup and heartbeat responses — these reference designs are receive-only.

```
Physical NIC:  Eth → Outer IP → GRE → Inner IP → UDP → payload
GRE interface: Inner IP (148.51.x.x → 233.84.178.1) → UDP → payload
```

Example interface:

```
$ ip a s doublezero1
26: doublezero1@NONE: <POINTOPOINT,NOARP,UP,LOWER_UP> mtu 1476 qdisc noqueue state UNKNOWN
    link/gre 64.130.37.175 peer 4.42.212.122
    inet 169.254.10.233/31 scope link doublezero1
```

**GRE decapsulator (optional).** [gre-decap](gre-decap/) is a standalone XDP program that strips GRE encapsulation inline on the physical NIC. After decap, the kernel sees plain multicast UDP — no tunnel interface or application changes needed. Useful when you want existing socket-based applications to receive the feed without a GRE tunnel.

## Shred Receivers

Receive Solana shreds directly off the multicast group. Two packet types are present on the shred feed:

- **Shred packets** (port 7733) — Solana shreds (~1247–1272 bytes each)
- **Heartbeat packets** (port 5765) — 4-byte DoubleZero liveness probes

| Language | Kernel Sockets | XDP |
|----------|----------------|-----|
| **Rust** | [rust/kernel-receiver](rust/kernel-receiver/) | [rust/xdp-receiver](rust/xdp-receiver/) |
| **Go**   | [go/kernel-receiver](go/kernel-receiver/)     | [go/xdp-receiver](go/xdp-receiver/) |
| **C**    | planned | planned |

Kernel-socket receivers are the simple path; XDP receivers are the high-performance path.

## Market Data Pipelines

Decode DoubleZero's binary market-data feeds (defined in [edge-feed-spec](https://github.com/malbeclabs/edge-feed-spec)) and monitor them live. Each feed has a multicast subscriber (**parser**) that decodes the wire format and republishes it on a Unix socket, and a reference **bot** that builds state and persists to ClickHouse.

| Feed | Spec | Implementation |
|------|------|----------------|
| Top-of-Book & Trades | [spec](https://github.com/malbeclabs/edge-feed-spec/blob/main/top-of-book/spec.md) | `go/topofbook-parser`, `go/topofbook-bot` |
| Market-by-Order | [spec](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-order/spec.md) | `go/marketbyorder-parser`, `go/marketbyorder-bot` |
| Market-by-Price | [spec](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-price/spec.md) | `go/marketbyprice-parser`, `go/marketbyprice-bot` |

### Top-of-Book & Trades

Consumes the [Top-of-Book & Trades feed](https://github.com/malbeclabs/edge-feed-spec/blob/main/top-of-book/spec.md).

| Component | Description |
|---|---|
| [go/topofbook-parser](go/topofbook-parser/) | Multicast subscriber. Decodes frames, writes JSON/CSV to a file or Unix socket, exposes Prometheus metrics |
| [go/topofbook-bot](go/topofbook-bot/) | Reads the parser's Unix socket, filters by symbol, exposes per-symbol top-of-book state as Prometheus metrics, and optionally writes every tick to ClickHouse |

### Market-by-Order

Consumes the [Market-by-Order feed](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-order/spec.md).

| Component | Description |
|---|---|
| [go/marketbyorder-parser](go/marketbyorder-parser/) | Three-port multicast subscriber + binary wire decoder, broadcasts decoded JSONL on a Unix socket |
| [go/marketbyorder-bot](go/marketbyorder-bot/) | Book builder + persistor. Maintains in-memory market-by-order books and writes per-event rows + coalesced top-N level snapshots to ClickHouse |

### Market-by-Price

Consumes the [Market-by-Price feed](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-price/spec.md) — full-depth aggregated (L2) levels rather than individual orders.

| Component | Description |
|---|---|
| [go/marketbyprice-parser](go/marketbyprice-parser/) | Three-port multicast subscriber + binary wire decoder, broadcasts decoded JSONL on a Unix socket |
| [go/marketbyprice-bot](go/marketbyprice-bot/) | Book builder + persistor. Maintains price-keyed L2 books across shards and writes per-event rows, raw snapshot levels, and coalesced top-N level snapshots to ClickHouse |

### Demo Stack

[demo/](demo/) is a one-command Docker Compose stack — parsers + bots + ClickHouse + Grafana — with pre-provisioned dashboards for all three feeds: per-symbol top-of-book state, an order-book view with ladder, depth heatmap, spread, trade tape and event-rate panels, and an aggregated-depth view adding crossed-book, delta-buffer and persistence panels.

## Sample Captures

The `pcaps/` directory contains sample packet captures from a live DoubleZero edge feed. These can be inspected with Wireshark using the `solana.shreds` dissector (decode as UDP port 7733 → `solana.shreds`).

## Target Audience

Traders and operators already familiar with tools like the [jito shredstream-proxy](https://github.com/jito-labs/shredstream-proxy) who want to consume DoubleZero edge multicast feeds directly.

## License

Licensed under the **Apache License 2.0**.

See [LICENSE](./LICENSE) for details.
