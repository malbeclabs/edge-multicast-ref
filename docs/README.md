# Documents

Design documents and implementation plans, kept as a record. Each was written against the code as it stood at its date and is not updated afterwards — for current behaviour, read the component's README. On wire format, [edge-feed-spec](https://github.com/malbeclabs/edge-feed-spec) wins over anything here.

`superpowers/specs/` and `superpowers/plans/` are the current convention: a spec argues a design and is reviewed before code, a plan turns it into ordered tasks, and they pair by name and date. The dated files at this level predate that split.

Older documents also predate [GLOSSARY.md](https://github.com/malbeclabs/edge-feed-spec/blob/main/GLOSSARY.md) and say `frame` for `datagram` and `bot` for `book-builder`. They are left as written.

## Publisher crates

| | |
|---|---|
| [Shared publisher crates](superpowers/specs/2026-08-26-edge-publisher-crates-design.md) | The design behind [`rust/codec`](../rust/codec/) and [`rust/publisher`](../rust/publisher/) |
| [Codec crates: Top-of-Book path](superpowers/plans/2026-08-26-codec-crates-top-of-book.md) | Plan for the first three codec crates |
| [The venue adapter interface](superpowers/specs/2026-09-02-venue-adapter-interface-design.md) · [plan](superpowers/plans/2026-09-02-venue-adapter-interface.md) | The trait a venue repository implements to turn its own source into our messages, and how the recorder re-lowers it to compare against multicast |

## Feeds

| Feed | Design | Plan |
|---|---|---|
| Market-by-Order | [demo stack](2026-04-23-marketbyorder-design.md), [rename from depth-of-book](superpowers/specs/2026-06-05-marketbyorder-rename-design.md), [snapshot resilience](superpowers/specs/2026-06-06-marketbyorder-bot-snapshot-resilience-design.md), [shard dispatcher](2026-05-19-marketbyorder-bot-shard-dispatcher-design.md) | [demo stack](2026-04-23-marketbyorder-plan.md), [rename](superpowers/plans/2026-06-05-marketbyorder-rename.md), [snapshot resilience](superpowers/plans/2026-06-06-marketbyorder-bot-snapshot-resilience.md), [shard dispatcher](2026-05-19-marketbyorder-bot-shard-dispatcher-plan.md) |
| Market-by-Price | [parser, book-builder, demo stack](superpowers/specs/2026-08-02-marketbyprice-design.md), [persistence](superpowers/specs/2026-08-07-marketbyprice-bot-persistence-design.md) | [parser](superpowers/plans/2026-08-02-marketbyprice-parser.md), [book engine](superpowers/plans/2026-08-02-marketbyprice-bot-engine.md), [persistence](superpowers/plans/2026-08-07-marketbyprice-bot-persistence.md) |

## Cross-cutting

| | |
|---|---|
| [Dual-version refdata](superpowers/specs/2026-08-08-refdata-v3-dual-version-design.md) · [plan](superpowers/plans/2026-08-08-refdata-v3-dual-version.md) | Decoding `InstrumentDefinition` at schema versions 1 and 3, and watching a cutover |
| [Per-publisher sequence tracking](superpowers/specs/2026-08-10-per-channel-seq-tracking-design.md) · [plan](superpowers/plans/2026-08-10-per-publisher-seq-tracking.md) | Why gap detection keys on `(source IP address, Channel ID, destination port)` |
| [Cross-feed latency normalization](superpowers/specs/2026-06-06-cross-feed-latency-normalization-design.md) · [plan](superpowers/plans/2026-06-06-cross-feed-latency-normalization.md) | Comparing latency across feeds that timestamp differently |

## Shred receivers

| | |
|---|---|
| [Rust kernel-socket receiver](2026-03-26-rust-kernel-receiver-design.md) · [plan](2026-03-26-rust-kernel-receiver-plan.md) | |
| [Rust XDP receiver](2026-03-26-rust-xdp-receiver-design.md) · [plan](2026-03-26-rust-xdp-receiver-plan.md) | |
| [Go receivers](2026-03-27-go-receivers-plan.md) | Kernel-socket and XDP |
| [XDP GRE decapsulator](2026-03-28-xdp-gre-decap-design.md) | The [gre-decap](../gre-decap/) program |

## Other

| | |
|---|---|
| [Receiving the Hyperliquid feed](hyperliquid.md) | Consuming one venue's feed over DoubleZero Edge |
