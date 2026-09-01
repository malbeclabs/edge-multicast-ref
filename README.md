# DoubleZero Edge Multicast Reference Designs

Reference code for both sides of a [DoubleZero](https://doublezero.xyz) edge multicast feed: libraries for publishing one, reference implementations for subscribing to one.

DoubleZero delivers data as GRE-encapsulated UDP multicast. The kernel handles GRE de-encapsulation, so applications see clean UDP.

| | | Where |
|---|---|---|
| **[Publishing a feed](#publishing-a-feed)** | Rust crates a venue publisher is built from | [`rust/codec`](rust/codec/), [`rust/publisher`](rust/publisher/) |
| **[Subscribing to market data](#subscribing-to-market-data)** | Parsers, book-builders, demo stack | [`go/`](go/), [`demo/`](demo/) |
| **[Subscribing to Solana shreds](#subscribing-to-solana-shreds)** | Receive-only receivers, kernel sockets and XDP | [`rust/`](rust/), [`go/`](go/) |

Wire formats are specified in [edge-feed-spec](https://github.com/malbeclabs/edge-feed-spec), which is also the authority for vocabulary here.

## Transport

The feed arrives on a GRE tunnel interface (e.g. `doublezero1`) as UDP multicast. The DoubleZero client handles tunnel setup and heartbeat responses; the subscribing designs here are receive-only.

```
Physical NIC:  Eth → Outer IP → GRE → Inner IP → UDP → payload
GRE interface: Inner IP (148.51.x.x → 233.84.178.1) → UDP → payload
```

```
$ ip a s doublezero1
26: doublezero1@NONE: <POINTOPOINT,NOARP,UP,LOWER_UP> mtu 1476 qdisc noqueue state UNKNOWN
    link/gre 64.130.37.175 peer 4.42.212.122
    inet 169.254.10.233/31 scope link doublezero1
```

[gre-decap](gre-decap/) is an optional XDP program that strips GRE inline on the physical NIC, so socket-based applications can receive the feed without a tunnel interface.

## Publishing a feed

Crates a publisher depends on from its own repository. See [rust/README.md](rust/README.md).

| Crate | Owns |
|---|---|
| [`dz-edge-core`](rust/codec/dz-edge-core/) | Datagram and message headers, sequencing, receive-side walk, decimal conversion |
| [`dz-edge-tob`](rust/codec/dz-edge-tob/) | Top-of-Book: `Quote`, `Trade` |
| [`dz-edge-refdata`](rust/codec/dz-edge-refdata/) | Reference data: `InstrumentDefinition`, `ManifestSummary` |
| [`dz-publisher-metrics`](rust/publisher/dz-publisher-metrics/) | The normative `dz_publisher_*` Prometheus set |

## Subscribing to market data

Each feed has a **parser** that subscribes, decodes and republishes records on a Unix socket, and a **book-builder** that maintains book state and persists to ClickHouse.

| Feed | Spec | Parser | Book-builder |
|---|---|---|---|
| Top-of-Book & Trades | [spec](https://github.com/malbeclabs/edge-feed-spec/blob/main/top-of-book/spec.md) | [topofbook-parser](go/topofbook-parser/) | [topofbook-bot](go/topofbook-bot/) |
| Market-by-Order | [spec](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-order/spec.md) | [marketbyorder-parser](go/marketbyorder-parser/) | [marketbyorder-bot](go/marketbyorder-bot/) |
| Market-by-Price | [spec](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-price/spec.md) | [marketbyprice-parser](go/marketbyprice-parser/) | [marketbyprice-bot](go/marketbyprice-bot/) |

| Component | |
|---|---|
| [topofbook-parser](go/topofbook-parser/) | Decodes datagrams, writes JSON/CSV to a file or Unix socket |
| [topofbook-bot](go/topofbook-bot/) | Per-symbol top-of-book state as Prometheus metrics, optional ClickHouse writes |
| [marketbyorder-parser](go/marketbyorder-parser/) | Three-port subscriber and wire decoder, fans decoded JSONL out on a Unix socket |
| [marketbyorder-bot](go/marketbyorder-bot/) | Order-keyed books; per-event rows and top-N snapshots to ClickHouse |
| [marketbyprice-parser](go/marketbyprice-parser/) | Three-port subscriber and wire decoder, fans decoded JSONL out on a Unix socket |
| [marketbyprice-bot](go/marketbyprice-bot/) | Price-keyed L2 books across channels; per-event rows, snapshot levels and top-N snapshots to ClickHouse |

[demo/](demo/) runs all of it — parsers, book-builders, ClickHouse, Grafana — with one command and pre-provisioned dashboards for all three feeds.

## Subscribing to Solana shreds

Two packet types on the shred feed: shred packets on port 7733 (~1247–1272 bytes), and 4-byte DoubleZero liveness probes on port 5765.

| Language | Kernel sockets | XDP |
|----------|----------------|-----|
| **Rust** | [rust/kernel-receiver](rust/kernel-receiver/) | [rust/xdp-receiver](rust/xdp-receiver/) |
| **Go**   | [go/kernel-receiver](go/kernel-receiver/)     | [go/xdp-receiver](go/xdp-receiver/) |
| **C**    | planned | planned |

Kernel sockets are the simple path, XDP the fast one.

## Repository layout

| Path | |
|---|---|
| [`rust/codec`](rust/codec/), [`rust/publisher`](rust/publisher/) | Publisher crates, one Cargo workspace ([README](rust/README.md)) |
| `rust/kernel-receiver`, `rust/xdp-receiver` | Shred receivers, excluded from that workspace |
| [`go/`](go/) | Parsers, book-builders, Go shred receivers |
| [`gre-decap/`](gre-decap/) | XDP GRE decapsulator |
| [`demo/`](demo/) | Docker Compose stack and Grafana dashboards |
| [`testdata/golden/`](testdata/golden/) | Wire vectors the Go and Rust decoders both assert against |
| [`docs/`](docs/) | Design documents and plans ([index](docs/README.md)) |
| `pcaps/` | Sample captures; read with Wireshark's `solana.shreds` dissector on UDP 7733 |

## Vocabulary

Prose follows [GLOSSARY.md](https://github.com/malbeclabs/edge-feed-spec/blob/main/GLOSSARY.md): a **datagram** is one UDP payload, a header plus N **messages**; a **channel** is a shard named by `Channel ID`; a **parser** decodes and republishes; a **book-builder** consumes parser output.

The `-bot` paths and `dz_bot_*` metric names predate that glossary and are kept because CI jobs, binaries and dashboards reference them. `Frame Length` is the spec's own field name at offset 22, so `parse_errors_total{reason="frame_length"}` matches it deliberately.

## License

Apache 2.0. See [LICENSE](./LICENSE).
