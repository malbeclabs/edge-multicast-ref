# One Channel ID for a shard's two feeds: plan

Breaks [the design](../specs/2026-10-20-channel-id-shared-by-a-shards-feeds-design.md) into ordered tasks.

**Base:** `main`.

## Global constraints

- **Vocabulary:** `GLOSSARY.md` governs every identifier, comment, test name, error message and commit message. The new label is `feed`. Its values are the codec's `Feed::NAME` (`top-of-book`, `market-by-price`).
- **Every new test must fail under a mutation of the code it covers.** Each task names the revert for its central test. The mutation for each of the other tests is found and run during the task, and listed in the PR.
- **A document valid today keeps its meaning.** Every existing test stays green without changing what it asserts. The two exceptions are named in task 3: the test whose case is now accepted, and the one whose error changes.
- **Order.** Tasks 1 and 2 (the pairs and the `feed` label) land **before** task 3 relaxes the rule, in the same pull request. A commit that accepts a shared ID while the series still collide must not exist on `main`. A test that needs a document sharing an ID waits for task 3.

---

## Tasks

### 1. Pairs, not IDs

- [ ] `Config::channel_ids` and `Feeds::channel_ids` become `channels()`, returning `(feed, Channel ID)` pairs in the document's order, without deduplication. Remove the comments that say the deduplication removes nothing a document can produce.
- [ ] `run.rs:515`, the loopback example and the harness pass the pairs.

**Test:** a document with two shards' blocks yields one pair per block, in document order. The shared-ID case cannot be stated yet (task 3 accepts it) and is added there.

---

### 2. The `feed` label

- [ ] `dz-publisher-metrics`:
  - `PublisherMetricsConfig::channel_ids: &[u8]` becomes `channels: &[(&str, u8)]`;
  - `sequence_current`, `heartbeat_last_sent_timestamp_seconds` and `last_published_timestamp_seconds` gain `feed`, and pre-create per pair;
  - `set_sequence`, `set_heartbeat_last_sent` and `set_last_published` take the feed;
  - the three reference-data gauges keep `channel_id` alone, and pre-create once per distinct ID.
- [ ] `dz-publisher-egress/src/egress.rs:381-389`: pass `F::NAME`. `publisher.rs:1746`: pass the feed of the pipeline that sent.
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

### 3. The rule

- [ ] `config.rs`: in `Config::resolve`'s loop, check two blocks sharing a `channel_id` in this order:
  1. the same `spec`: `DuplicateChannelId`, as now;
  2. different shards: `ChannelIdOnTwoShards`;
  3. a destination port the two blocks have in common: `ChannelInstancePortShared`.

  Accept anything else.
- [ ] `error.rs`: the two new variants, each naming both blocks. Each doc comment states the failure it prevents, and `DuplicateChannelId`'s doc comment is rewritten to cover one specification only.
- [ ] `two_blocks_claiming_one_channel_id_are_refused_naming_both` becomes `a_shards_two_feeds_on_one_channel_id_resolve`, because its case is now valid. `two_shards_of_one_specification_sharing_a_channel_id_are_refused_naming_both_shards` keeps asserting `DuplicateChannelId`.

**Test** (`tests/config_document.rs`):
- Top-of-Book and Market-by-Price on one shard, on one `Channel ID`, with five distinct ports, resolve.
- The same two blocks on two shards are refused as `ChannelIdOnTwoShards`, naming both shards.
- The same two blocks with Top-of-Book's `refdata_port` equal to Market-by-Price's `snapshot_port` are refused as `ChannelInstancePortShared`, naming both keys. Repeat for each pair of keys.
- Two blocks with different `Channel ID`s and one shared port still resolve.

- With a shared ID: `channels()` yields two pairs (task 1), and the runtime's exposition holds one `sequence_current` series per feed (task 2).

**The revert:** drop check 3 and the shared-port test fails. Drop check 2 and the two-shards test fails.

---

### 4. One process, both feeds, one Channel ID, on the wire

- [ ] `dz-recorder-e2e`: a variant of `strike_conformance.rs`'s composition, with both feeds on one `Channel ID`. The archive is replayed per group, and `dz-conformance` passes on `tob` and on `mbp`.
- [ ] Assert:
  - each channel instance's `Sequence Number` series is contiguous on its own;
  - the two feeds' `Instrument Count` and `Manifest Seq` agree per manifest;
  - an instrument has one `Instrument ID` on both feeds.

- The composition interleaves the two feeds' datagrams on the shared ID: a quote, a level update, a quote, and so on, so the two `mktdata` instances alternate.

**The revert:** key the sequencer's counter on `(source IP address, Channel ID)`, without the destination port, so the two feeds' instances on that ID share one `ChannelSequence`. Sharing a `Sequencer` is not a mutation, because it already keeps one counter per `ChannelInstance` (`sequencer.rs:38`, `:91-109`). With interleaved datagrams, each instance's series then shows a gap at every alternation, and contiguity fails.

---

### 5. Documents and release

- [ ] `BRINGING-UP-A-FEED.md`: the `channel_id` bullet states the rule.
- [ ] `docs/README.md`: the row for this design.
- [ ] The release notes for the next minor list:
  - the API change (`channels`, the three setters);
  - the new label;
  - the two new refusals;
  - the one error that changes.
