# One Channel ID for a shard's two feeds

**Status:** design. Read against `main` at `a189b13`, after `v0.6.0`.

## The failure

`Config::resolve` refuses two enabled `[[feed]]` blocks with one `channel_id`, whatever their specification (`dz-publisher-runtime/src/config.rs:989-1005`, `StartupError::DuplicateChannelId`, tested by `two_blocks_claiming_one_channel_id_are_refused_naming_both`).

The glossary defines a **Channel** as a logical shard of the instrument set, named by `Channel ID`. Nothing in it ties a `Channel ID` to one feed spec. A product that publishes one instrument set on Top-of-Book and on Market-by-Price under one `Channel ID` uses the word as defined, and one does today: Kalshi's shipped publisher sends perps on Channel ID `1` (and `101` from the second host) on both feeds, and each sports league under one ID on both. On perps it refuses to start if the two differ, and its subscribers cross-reference the two feeds by that ID (`malbeclabs/kalshi`, `docs/subscriber-notes.md` §5.3).

Under this runtime, such a product needs **one process per feed**. Each process then has its own upstream connection and subscription set, catalogue read, books, and `instruments.state`. Two sequential records agree on an `Instrument ID` only while both processes see every listing in the same order, so one instrument can carry two IDs across the product's two feeds. One process carrying both feeds is a shape the runtime already supports: a shard owns both send paths, `Quote` goes to one, `Level` and `Clear` to the other, and a `Trade` is lowered once for both (`publisher.rs`, `ShardFeeds`). This check is the only thing in the way.

## What the check protects, verified

The feed-routes design gives its reason as "two channel instances writing one metric series" (`2026-09-09-publisher-feed-routes-design.md`, the check table). Here is everything keyed on `Channel ID` inside one process and in the recorder's rows downstream of it, and what happens when a shard's two feeds share one:

| What | Keyed on | Top-of-Book and Market-by-Price on one `Channel ID` |
|---|---|---|
| `Sequence Number`, `Reset Count`, the snapshot cycle | `ChannelInstance`, `(source IP address, Channel ID, destination port)`, in each `FeedPipeline`'s own egress per port role (`dz-publisher-egress/src/egress.rs:51`, `instance.rs:30`) | Separate, as long as no destination port is shared |
| Era | `<spec>.era`, or `<spec>.<shard>.era` | Separate |
| Published set, `Manifest Seq`, `Instrument Count` | The shard (`publisher.rs:1709-1727`) | One value, if both blocks are on one shard |
| Recorder transport rows, book-builders, conformance | The channel instance (`dz-recorder-core/src/datagram.rs:190`), with each feed on its own group | Separate |
| Recorder `event`, `instrument` | Sort keys holding `source_addr` and `dst_port` (`005_recorder_market_data.sql:163-164`, `:214-215`) | Separate |
| Recorder `book_top` | `(channel_id, instrument_id, recv_ts, sequence_number, message_index, observation)`, with no port and no feed (`005_recorder_market_data.sql:322-341`) | **Folds**, where both feeds are derived. Each feed's tops for one instrument read as one book's history to a query that does not filter on `feed` |
| `state_key`, and the views `book_top_occurrence` and `book_top_race` | `state_key(channel_id, instrument_id, top)` (`010_recorder_book_key.sql:14`). The views partition and group on `channel_id, instrument_id, state_key` (`006_recorder_book_top_pairing.sql:160-161`, `:227`) | **Collides** on a top both feeds key alike. The race can pair one feed's top at one site with the other's at another |
| `publisher_book_top_occurrence`, `feed_race` | `feed` and `book_key` (`010_recorder_book_key.sql:415`, `:552`) | Separate |
| `dz_publisher_egress_sequence_current{port_role,channel_id}` | `port_role` × `channel_id` | **Collides.** Both feeds' `mktdata`, and both feeds' `refdata`, write `{port_role, N}` |
| `dz_publisher_egress_heartbeat_last_sent_timestamp_seconds{port_role,channel_id}` | Same | **Collides.** One feed's heartbeat hides the other's silence |
| `dz_publisher_channel_last_published_timestamp_seconds{channel_id}` | `channel_id` | **Collides.** Top-of-Book's quotes keep a channel whose level updates stopped looking fresh |
| `dz_publisher_refdata_{manifest_seq,manifest_valid,instruments_current}{channel_id}` | `channel_id`, written from the shard | Written twice with the same value, if one shard |
| `Config::channel_ids`, `Feeds::channel_ids` | Sorted and deduplicated | The deduplication now removes a real channel instance from pre-creation |

So the hazard is narrower than "one `Channel ID` per process". It is these three cases:

1. **Three metric families** that cannot tell the two feeds apart.
2. **One `Channel ID` on two shards.** One ID would name two instrument sets: the wire contract breaks, and the reference-data gauges collide with different values.
3. **One `Channel ID` and one destination port in two blocks.** The glossary keys a channel instance on `(source IP address, Channel ID, destination port)`, with no group in the key, so that is one channel instance carrying two `Sequence Number` series.

The recorder's book rows are a fourth place, outside the process, and the rule cannot refuse it. See "The recorder's market data" below.

## The rule

Two enabled blocks may share a `Channel ID` only as one shard's two feeds:

- **Different `spec`.** Two blocks of one specification on one `Channel ID` stay refused, as `DuplicateChannelId`. On different shards, that is two instrument sets under one name. On one shard, `DuplicateFeedShard` already refuses it.
- **One `shard`.** Two shards on one ID is refused, naming both blocks: `StartupError::ChannelIdOnTwoShards { channel_id, first_spec, first_shard, second_spec, second_shard }`.
- **No destination port in common** across the two blocks' `mktdata_port`, `refdata_port` and `snapshot_port`. Refused, naming both keys: `StartupError::ChannelInstancePortShared { channel_id, port, first_spec, first_key, second_spec, second_key }`. `PortsCollide` already covers the same rule within one block. Blocks with different `Channel ID`s are not checked against each other, because their channel instances are distinct whatever the port.

A block is checked against **every** earlier block on its `Channel ID`, not only the first. With two specifications and `DuplicateFeedShard`, at most two blocks can share an ID, so today the first is the only one. The family already has a third specification, Market-by-Order, and the rule must not depend on how many reach the runtime: with three, one shard could carry three blocks on one ID, one per specification, with no port in common across any two of them. A shard's two feeds may also keep different IDs, which is today's only shape. Nothing requires them to share one.

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
- The never-published rule joins `dz_publisher_uptime_seconds`, which has no `feed`, so its join becomes `ignoring(channel_id, feed)`.

**Normative status.** `sequence_current` and `heartbeat_last_sent` are normative families (Feed Publisher Playbook, Phase 6.5), so the label is a change to the normative set and is listed with the four existing proposals until the playbook carries it. `last_published` is already a proposal.

**API.**
- `PublisherMetricsConfig::channel_ids: &[u8]` becomes `channels: &[(&str, u8)]`, one `(feed, Channel ID)` pair per block.
- `set_sequence`, `set_heartbeat_last_sent` and `set_last_published` take the feed. The egress has it as `F::NAME`.
- `Config::channel_ids` and `Feeds::channel_ids` return the pairs. Nothing is deduplicated, because a pair is a block.

A struct literal or a call outside this repository stops compiling, so it ships in a minor release.

## The recorder's market data

The recorder keeps its transport rows per channel instance, so the two feeds stay apart there. Its market-data rows are written only for a feed named in the loader's `[[market_data]]`, and three of them do not key on the instance:

- **`book_top`** has no port and no feed in its sort key, on purpose: "a book is one book whichever redundant path delivered the message that moved it" (`005_recorder_market_data.sql:335-339`). With both feeds derived, an as-of lookup of "the top for channel N, instrument I" mixes the two feeds.
- **`book_top_occurrence` and `book_top_race`** number and pair on `state_key`. How often the two feeds' keys meet is narrow, but not zero. The fold hashes an absent `source_count` and a stated one differently (`dz-recorder-events/src/book.rs:771-774`). A Top-of-Book quote states one on every side it carries (`:586`), and a top recomputed from levels never does (`:631`, `:640`). So the one top both feeds key alike is a top with both sides absent, an empty book, and an empty book is a common state.

**Shared IDs did not start this.** A `Channel ID` is unique within one publisher's document, not within a recorder. The mainnet recorders already derive market data for `tob_edge_binance_spot` and `tob_edge_phoenix`, both on Channel ID `1` (`malbeclabs/infra`, `group_vars/feed_recorders.yml`, `dz_recorder_feeds` and `dz_recorder_load_market_data`). Kalshi's two perps feeds share Channel ID `1` too. `malbeclabs/infra#3235` adds the Kalshi feeds to `dz_recorder_load_feeds` only, with no `dz_recorder_load_market_data` entry, so the loader writes their transport rows (`datagram`, `era`, `segment_coverage`, `sequence_gap`) and no `book_top`. An entry naming both perps feeds later would start the mixing, with or without this change.

**What changes: the two views key on `feed`.** A new migration, `013_recorder_book_top_feed.sql`, re-creates `book_top_occurrence` with `feed` in its window partition, and `book_top_race` with `feed` in its grouping and its output. It is `CREATE OR REPLACE VIEW` only. No table changes and no stored value moves. Wherever a `Channel ID` carries one feed at an observation, `feed` is constant within every partition the views already had, so they return the same rows. Where two feeds share one, the views stop crossing them.

**What stays: the table's key and `state_key`.** `state_key` is a stored value that may not move (`book.rs:711`). Re-keying `book_top` is a rebuild of the table: `MODIFY ORDER BY` appends only a column the same `ALTER` adds, and `feed` exists already. What the unchanged key leaves exposed is a `ReplacingMergeTree` merge of two genuine rows. That needs two datagrams kernel-stamped at the same nanosecond at one recorder, carrying one sequence number from two independent counters, at one message index, for one instrument. Binance spot and Phoenix carry the same exposure on Channel ID `1` today.

**The rule for readers.** A query over `book_top` or `book_top_settled` about one instrument filters on `feed`. The `013` header and the loader `README`'s `[[market_data]]` section say so.

## Compatibility

- **Documents.** Every document valid today stays valid and means the same thing. The error a document sharing an ID across shards gets changes from `DuplicateChannelId` to `ChannelIdOnTwoShards`.
- **Recorder.** `013` is applied as every `schema()` migration is. A deployment where no `Channel ID` carries two feeds at one observation reads the same rows from both views before and after it. Where one does, as Binance spot and Phoenix do on mainnet, the views stop numbering and pairing one feed's states with the other's.
- **Rollback.** A `v0.6.0` publisher given a document that shares an ID refuses it at startup, naming both blocks. A rollback therefore fails loudly, not silently.

## What this does not do

- It changes nothing on the wire, and nothing in `Source ID` or `Instrument ID` allocation, the recorder's capture, its tables, or the book-builders. In the recorder's schema it re-creates two views, keyed on `feed`.
- It does not let an adapter state a `Channel ID`.
- It does not make a shard's two feeds share a `Channel ID`, by default or by requirement.
