# One Channel ID for a shard's two feeds: plan

Breaks [the design](../specs/2026-10-20-channel-id-shared-by-a-shards-feeds-design.md) into ordered tasks.

**Base:** `main`.

## Global constraints

- **Vocabulary:** `GLOSSARY.md` governs every identifier, comment, test name, error message and commit message. The new label is `feed`. Its values are the codec's `Feed::NAME` (`top-of-book`, `market-by-price`).
- **Every new test must fail under a mutation of the code it covers.** Each task names the revert for its central test. The mutation for each of the other tests is found and run during the task, and listed in the PR.
- **A document valid today keeps its meaning.** Every existing test stays green without changing what it asserts. The two exceptions are named in task 4: the test whose case is now accepted, and the one whose error changes.
- **Order.** Tasks 1 and 2 (the pairs and the `feed` label) land **before** task 4 relaxes the rule, in the same pull request. A commit that accepts a shared ID while the series still collide must not exist on `main`. Task 3 (the recorder's views) also lands before task 4, in the same pull request or an earlier one: it fixes a crossing mainnet already has, and needs nothing from the publisher. A test that needs a document sharing an ID waits for task 4.

---

## Tasks

### 1. Pairs, not IDs

- [ ] `Config::channel_ids` and `Feeds::channel_ids` become `channels()`, returning `(feed, Channel ID)` pairs in the document's order, without deduplication. Remove the comments that say the deduplication removes nothing a document can produce.
- [ ] Every `PublisherMetricsConfig` literal passes the pairs: `run.rs:533`, `threaded.rs:1018`, the loopback example, and the tests that build one (`tests/harness/mod.rs`, `composition.rs`, `ingress_observer.rs`, `threaded.rs`, and the metrics crate's `precreated_at_startup.rs`).

**Test:** a document with two shards' blocks yields one pair per block, in document order. The shared-ID case cannot be stated yet (task 4 accepts it) and is added there.

---

### 2. The `feed` label

- [ ] `dz-publisher-metrics`:
  - `PublisherMetricsConfig::channel_ids: &[u8]` becomes `channels: &[(&str, u8)]`;
  - `sequence_current`, `heartbeat_last_sent_timestamp_seconds` and `last_published_timestamp_seconds` gain `feed`, and pre-create per pair;
  - `set_sequence`, `set_heartbeat_last_sent` and `set_last_published` take the feed;
  - the three reference-data gauges keep `channel_id` alone, and pre-create once per distinct ID.
- [ ] `dz-publisher-egress/src/egress.rs:388-396`: pass `F::NAME`. `publisher.rs:1787`: pass the feed of the pipeline that sent.
- [ ] `README.md`:
  - the config section explains the pairs;
  - the label is listed with the proposals;
  - the relative-staleness rule keeps matching on `feed` (`max without(channel_id)` and `ignoring(channel_id)`), and the text says it now compares channels within one feed;
  - the never-published rule joins `dz_publisher_uptime_seconds`, which carries no `feed`, with `ignoring(channel_id, feed)`. `ignoring(channel_id)` alone would leave it matching nothing;
  - a note says that `by (channel_id)` sums the two feeds' sequence series.
- [ ] `tests/precreated_at_startup.rs`, `tests/normative_names.rs`: the label set.

**Test** (metrics crate, configured directly with pairs, so it needs no document the runtime accepts yet): given `("top-of-book", 1)` and `("market-by-price", 1)`, the exposition holds two `sequence_current{port_role="mktdata",channel_id="1"}` series, one per feed. Setting one leaves the other at its value. A test evaluates both README rules against an exposition with the label and asserts that each returns a series.

**The revert:** drop `feed` from `sequence_current`'s label names, and the test fails.

---

### 3. The recorder's book views key on `feed`

- [ ] `rust/recorder/dz-recorder-clickhouse/db/clickhouse/013_recorder_book_top_feed.sql`, `CREATE OR REPLACE VIEW` only:
  - `book_top_occurrence`: `feed` in the window's `PARTITION BY`, after `observation`;
  - `book_top_race`: `feed` first in the select list and in the `GROUP BY`.

  No `ALTER`, no table, no grant. `book_top_settled` stays as `010` states it, and it already selects `feed`.
- [ ] The `013` header says why the views change and the table does not: the rebuild a new sort key needs, the merge the unchanged key leaves exposed, and `state_key`'s value, which may not move. It states the rule for readers: a query over `book_top` or `book_top_settled` about one instrument filters on `feed`.
- [ ] `src/ddl.rs`: the migration in `migrations()`, which becomes `[Migration; 13]`. It is not in `ACCESS_MANAGEMENT`, so `schema()` applies it.
- [ ] `dz-recorder-load/README.md`, the `[[market_data]]` section: two named feeds may share a `Channel ID`, as `tob_edge_binance_spot` and `tob_edge_phoenix` do, and a `book_top` reader filters on `feed`.

**Migration notes:**
- `CREATE OR REPLACE VIEW` is idempotent. Applying `013` twice, or during a rolling upgrade, leaves the same two views. A view holds no rows, so nothing is rewritten or backfilled.
- `book_top_race` gains an output column. A reader that selects by name is unaffected. One that selects `*` sees `feed` first.
- `012` grants the dashboards' reader neither view, so nothing is re-granted.
- Rows change only where two feeds share a `Channel ID` at one observation, which mainnet already has on Channel ID `1`. There the views stop numbering and pairing one feed's states with the other's.
- Rollback is re-creating the two views as `006` states them.

**Test** (`tests/container.rs`, rows inserted directly as the pairing tests insert them):
- One observation, two feeds on one `Channel ID` and one `Instrument ID`, a row each with one `state_key` (the key an empty top carries on both feeds): `book_top_occurrence` numbers each feed's row `1`, not `1` and `2`.
- Two observations, one feed's row at each, with one `state_key`: `book_top_race` returns two races of one observation each, with `lead_ms` null, not one race across both feeds.
- The existing pairing tests, with one feed per `Channel ID`, return what they return today.
- `tests/ddl.rs`: `013`'s two view bodies name `feed` in the partition and the grouping.

**The revert:** drop `feed` from `book_top_race`'s `GROUP BY`, and the two-observation test fails. Drop it from `book_top_occurrence`'s partition, and the one-observation test fails.

---

### 4. The rule

- [ ] `config.rs`: in `Config::resolve`'s loop, check a block against **every** earlier block on its `channel_id`, not `feeds.iter().find(...)`, which stops at the first. For each such pair, in this order:
  1. the same `spec`: `DuplicateChannelId`, as now;
  2. different shards: `ChannelIdOnTwoShards`;
  3. a destination port the two blocks have in common: `ChannelInstancePortShared`.

  Accept anything else.
- [ ] The pairwise check is a function over each block's spec token, shard, `Channel ID` and three port keys, so its test can state a specification this build has no codec for.
- [ ] `error.rs`: the two new variants, each naming both blocks. Each doc comment states the failure it prevents, and `DuplicateChannelId`'s doc comment is rewritten to cover one specification only.
- [ ] `two_blocks_claiming_one_channel_id_are_refused_naming_both` becomes `a_shards_two_feeds_on_one_channel_id_resolve`, because its case is now valid. `two_shards_of_one_specification_sharing_a_channel_id_are_refused_naming_both_shards` keeps asserting `DuplicateChannelId`.

**Test** (`config.rs`, on the pairwise check): three blocks on one `Channel ID` and one shard, with the specification tokens `top-of-book`, `market-by-price` and `market-by-order`. With nine distinct ports, they pass. With one port of the third block equal to one of the **second** block's, and none of the first's, they are refused as `ChannelInstancePortShared`, naming the second block's key. This is a unit test because no document can state it today. With two specifications, a third block on an ID shares a specification with one of the first two. On the first's specification, the comparison with the first refuses it. On the second's, `DuplicateFeedShard` refuses it on the shared shard, and the comparison with the first refuses it on another. So no document separates `find` from the loop yet.

**Test** (`tests/config_document.rs`):
- Top-of-Book and Market-by-Price on one shard, on one `Channel ID`, with five distinct ports, resolve.
- The same two blocks on two shards are refused as `ChannelIdOnTwoShards`, naming both shards.
- The same two blocks with Top-of-Book's `refdata_port` equal to Market-by-Price's `snapshot_port` are refused as `ChannelInstancePortShared`, naming both keys. Repeat for each pair of keys.
- Two blocks with different `Channel ID`s and one shared port still resolve.

- With a shared ID: `channels()` yields two pairs (task 1), and the runtime's exposition holds one `sequence_current` series per feed (task 2).

**The revert:** drop check 3 and the shared-port test fails. Drop check 2 and the two-shards test fails. Put `find` back in place of the loop, and the three-block test fails.

---

### 5. One process, both feeds, one Channel ID, on the wire

- [ ] `dz-recorder-e2e`: a variant of `strike_conformance.rs`'s composition, with both feeds on one `Channel ID`. The archive is replayed per group, and `dz-conformance` passes on `tob` and on `mbp`.
- [ ] Assert:
  - each channel instance's `Sequence Number` series is contiguous on its own;
  - the two feeds' `Instrument Count` and `Manifest Seq` agree per manifest;
  - an instrument has one `Instrument ID` on both feeds.

- The composition interleaves the two feeds' datagrams on the shared ID: a quote, a level update, a quote, and so on, so the two `mktdata` instances alternate.

**The revert:** key the sequencer's counter on `(source IP address, Channel ID)`, without the destination port, so the two feeds' instances on that ID share one `ChannelSequence`. Sharing a `Sequencer` is not a mutation, because it already keeps one counter per `ChannelInstance` (`sequencer.rs:38`, `:91-109`). With interleaved datagrams, each instance's series then shows a gap at every alternation, and contiguity fails.

---

### 6. Documents and release

- [ ] `BRINGING-UP-A-FEED.md`: the `channel_id` bullet states the rule.
- [ ] `docs/README.md`: the row for this design.
- [ ] The release notes for the next minor list:
  - the API change (`channels`, the three setters);
  - the new label;
  - the two new refusals;
  - the one error that changes;
  - the recorder migration `013`, and that a `book_top` reader filters on `feed`.
