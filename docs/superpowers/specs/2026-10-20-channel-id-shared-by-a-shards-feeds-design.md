# One Channel ID for a shard's two feeds

**Status:** design. Read against `main` at `5adfd82` (`v0.6.0`).

## The failure

`Config::resolve` refuses two enabled `[[feed]]` blocks with one `channel_id`, whatever their specification (`dz-publisher-runtime/src/config.rs:985-1001`, `StartupError::DuplicateChannelId`, tested by `two_blocks_claiming_one_channel_id_are_refused_naming_both`).

The glossary defines a **Channel** as a logical shard of the instrument set, named by `Channel ID`. Nothing in it ties a `Channel ID` to one feed spec. A product that publishes one instrument set on Top-of-Book and on Market-by-Price under one `Channel ID` uses the word as defined, and one does today: Kalshi's shipped publisher sends perps on Channel ID `1` (and `101` from the second host) on both feeds, and each sports league under one ID on both. On perps it refuses to start if the two differ, and its subscribers cross-reference the two feeds by that ID (`malbeclabs/kalshi`, `docs/subscriber-notes.md` §5.3).

Under this runtime, such a product needs **one process per feed**. Each process then has its own upstream connection and subscription set, catalogue read, books, and `instruments.state`. Two sequential records agree on an `Instrument ID` only while both processes see every listing in the same order, so one instrument can carry two IDs across the product's two feeds. One process carrying both feeds is a shape the runtime already supports: a shard owns both send paths, `Quote` goes to one, `Level` and `Clear` to the other, and a `Trade` is lowered once for both (`publisher.rs`, `ShardFeeds`). This check is the only thing in the way.

## What the check protects, verified

The feed-routes design gives its reason as "two channel instances writing one metric series" (`2026-09-09-publisher-feed-routes-design.md`, the check table). Here is everything keyed on `Channel ID` inside one process, and what happens when a shard's two feeds share one:

| What | Keyed on | Top-of-Book and Market-by-Price on one `Channel ID` |
|---|---|---|
| `Sequence Number`, `Reset Count`, the snapshot cycle | `ChannelInstance`, `(source IP address, Channel ID, destination port)`, in each `FeedPipeline`'s own egress per port role (`dz-publisher-egress/src/egress.rs:51`, `instance.rs:30`) | Separate, as long as no destination port is shared |
| Era | `<spec>.era`, or `<spec>.<shard>.era` | Separate |
| Published set, `Manifest Seq`, `Instrument Count` | The shard (`publisher.rs:1667-1685`) | One value, if both blocks are on one shard |
| Recorder, book-builders, conformance | The channel instance (`dz-recorder-core/src/datagram.rs:190`), with each feed on its own group | Separate |
| `dz_publisher_egress_sequence_current{port_role,channel_id}` | `port_role` × `channel_id` | **Collides.** Both feeds' `mktdata`, and both feeds' `refdata`, write `{port_role, N}` |
| `dz_publisher_egress_heartbeat_last_sent_timestamp_seconds{port_role,channel_id}` | Same | **Collides.** One feed's heartbeat hides the other's silence |
| `dz_publisher_channel_last_published_timestamp_seconds{channel_id}` | `channel_id` | **Collides.** Top-of-Book's quotes keep a channel whose level updates stopped looking fresh |
| `dz_publisher_refdata_{manifest_seq,manifest_valid,instruments_current}{channel_id}` | `channel_id`, written from the shard | Written twice with the same value, if one shard |
| `Config::channel_ids`, `Feeds::channel_ids` | Sorted and deduplicated | The deduplication now removes a real channel instance from pre-creation |

So the hazard is narrower than "one `Channel ID` per process". It is these three cases:

1. **Three metric families** that cannot tell the two feeds apart.
2. **One `Channel ID` on two shards.** One ID would name two instrument sets: the wire contract breaks, and the reference-data gauges collide with different values.
3. **One `Channel ID` and one destination port in two blocks.** The glossary keys a channel instance on `(source IP address, Channel ID, destination port)`, with no group in the key, so that is one channel instance carrying two `Sequence Number` series.

## The rule

Two enabled blocks may share a `Channel ID` only as one shard's two feeds:

- **Different `spec`.** Two blocks of one specification on one `Channel ID` stay refused, as `DuplicateChannelId`. On different shards, that is two instrument sets under one name. On one shard, `DuplicateFeedShard` already refuses it.
- **One `shard`.** Two shards on one ID is refused, naming both blocks: `StartupError::ChannelIdOnTwoShards { channel_id, first_spec, first_shard, second_spec, second_shard }`.
- **No destination port in common** across the two blocks' `mktdata_port`, `refdata_port` and `snapshot_port`. Refused, naming both keys: `StartupError::ChannelInstancePortShared { channel_id, port, first_spec, first_key, second_spec, second_key }`. `PortsCollide` already covers the same rule within one block. Blocks with different `Channel ID`s are not checked against each other, because their channel instances are distinct whatever the port.

With two specifications and `DuplicateFeedShard`, at most two blocks can share a `Channel ID`. A shard's two feeds may also keep different IDs, which is today's only shape. Nothing requires them to share one.

## Metrics: a `feed` label on the three families that collide

The three families gain a `feed` label. Its value is the `[[feed]] spec` token (`top-of-book`, `market-by-price`), which is the codec's `Feed::NAME` and the glossary's **Feed**:

- `dz_publisher_egress_sequence_current{feed,port_role,channel_id}`
- `dz_publisher_egress_heartbeat_last_sent_timestamp_seconds{feed,port_role,channel_id}`
- `dz_publisher_channel_last_published_timestamp_seconds{feed,channel_id}`

The three reference-data gauges stay keyed on `channel_id` alone. Their value is the shard's, and the rule makes it the same for both blocks.

**On every publisher, not only one sharing an ID.** A label that appeared only for some documents would make every dashboard query depend on the document. A publisher whose `Channel ID`s are all distinct gains no series, because each ID already belongs to one feed. A shared ID adds one series per family.

**What a query sees:**
- A query that selects or aggregates `by (channel_id)` now sums the two feeds' series. On sequence numbers that sum means nothing, and the crate `README` says so.
- The README's staleness rule, `max without(channel_id)`, now compares channels within one feed. That is the comparison it should make: its own note says a trade refreshes both feeds' gauges.

**Normative status.** `sequence_current` and `heartbeat_last_sent` are normative families (Feed Publisher Playbook, Phase 6.5), so the label is a change to the normative set and is listed with the four existing proposals until the playbook carries it. `last_published` is already a proposal.

**API.**
- `PublisherMetricsConfig::channel_ids: &[u8]` becomes `channels: &[(&str, u8)]`, one `(feed, Channel ID)` pair per block.
- `set_sequence`, `set_heartbeat_last_sent` and `set_last_published` take the feed. The egress has it as `F::NAME`.
- `Config::channel_ids` and `Feeds::channel_ids` return the pairs. Nothing is deduplicated, because a pair is a block.

A struct literal or a call outside this repository stops compiling, so it ships in a minor release.

## Compatibility

- **Documents.** Every document valid today stays valid and means the same thing. The error a document sharing an ID across shards gets changes from `DuplicateChannelId` to `ChannelIdOnTwoShards`.
- **Rollback.** A `v0.6.0` publisher given a document that shares an ID refuses it at startup, naming both blocks. A rollback therefore fails loudly, not silently.

## What this does not do

- It changes nothing on the wire, and nothing in `Source ID` or `Instrument ID` allocation, the recorder or the book-builders.
- It does not let an adapter state a `Channel ID`.
- It does not make a shard's two feeds share a `Channel ID`, by default or by requirement.
