# Several channel instances of one feed specification — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** One publisher process operating many channel instances of one feed
specification, with reference data, sequencing, eras and snapshot cycles all
keyed on the unit the specification and the glossary say owns them — and with a
venue able to say which shard an instrument belongs to without being able to say
which `Channel ID` that is.

**Spec:** `docs/superpowers/specs/2026-09-09-publisher-feed-routes-design.md`

**Tech stack:** Rust 2021, workspace MSRV. No new dependency anywhere, and none
at all in `dz-adapter-core` — see *Global constraints*. No async below
`dz-ingress-*`; nothing in this plan touches a transport.

---

## Scope

One plan, sixteen tasks, all in this repository. No venue repository changes at
any point: an adapter that never names a shard compiles unchanged against every
task, and the last task is the one that lets a document ask for a second block
of one specification.

The worked size throughout is the design's: **62 channel instances from one
process, 31 of each of two specifications**, against the 2 a process operates
today.

---

## The ordering constraint, which is the whole shape of this plan

`Config::resolve` refusing a second `[[feed]]` of one specification —
`StartupError::DuplicateFeedSpec` — is not input validation. It is the gate that
makes three other pieces of the runtime correct, and the design says why at
length. Relaxing it early does not produce a publisher that is approximately
right:

| Relaxed without | The publisher | What a subscriber sees |
|---|---|---|
| the per-shard drain | starts, runs, emits well-formed datagrams | `Manifest Seq` that increments for an admission on a channel it is not bound to, `Valid` that describes the process, `Instrument Count` 31× what it will ever get a message for |
| the re-keyed era store | starts, runs, emits well-formed datagrams | an era decided by the block's position in the document, drawn from one counter that advances by 31 a start — and eventually an era this channel has published under before, which is a restart nothing tells it about |
| the vectored `Feeds` | does not compile | — |

Only the third protects itself. So **task 9 is the gate, and tasks 1–8 all leave
`DuplicateFeedSpec` in force.** Each of them is a merge that changes no observed
behaviour, because with one shard configured every partition is a partition of
one. That is what makes them independently gateable: the suite that passes today
must still pass, unchanged, at the end of every task before 9.

A task that finds itself needing to relax the check early has hit the boundary
this plan is ordered around, and the answer is to stop and re-order rather than
to relax it.

---

## Global constraints

- **Vocabulary:** `GLOSSARY.md` governs every identifier, comment, test name,
  config key and commit message. `channel` only for the `Channel ID` shard;
  `datagram` never `frame`; `era` never `epoch`; `feed` or `path` never `lane`;
  `fan-out` for the reference copy; `source` never bare; and the word the
  glossary bans outright in every sense stays out of prose, identifiers, test
  names and comments alike. The new key is `shard` and the design's Naming
  section says why it
  is not `route`, `category` or `lane` — a task that renames it has to re-argue
  that section.
- **No venue names.** This repository is public. No commit message, comment,
  test name, fixture or configuration example names a venue, a venue repository,
  a venue crate or an issue tracker, or gives a count of publisher hosts.
- **`dz-adapter-core` takes no new dependency.** `thiserror`, and nothing else.
  Its manifest test enforces this; a task that needs a second stops and asks.
- **Nothing spec-decided becomes expressible.** A shard name is a venue word. No
  `Channel ID`, group, port, sequence number, `Reset Count` or era reaches any
  parameter an adapter touches, at any task.
- **Lints:** `#![forbid(unsafe_code)]` and the workspace clippy set.
  `cargo clippy --all-targets -- -D warnings` and `cargo fmt --all --check` pass
  at every task boundary, on CI's toolchain rather than on a local stable that
  may be older.
- **Tests run in both profiles.** CI runs `cargo test --all` and
  `cargo test --all --release`; `debug_assert!` differs.
- **No network, no privilege, no venue, in any test in this plan.** Every one is
  reachable with the fakes the suite already has: recording sinks behind
  `DatagramSink`, injected clocks, `MemoryStore`, and a temporary directory for
  the two tasks that touch the filesystem. Nothing here needs a socket, and
  `scripts/check-public-repo-rules.sh` runs before the toolchain does.
- **Every test must be shown to kill its mutant.** Revert the change, watch the
  new test fail, restore it. A test that passes against the unfixed tree is a
  test that asserts nothing, and this plan adds several whose subject is an
  absence — a series that is *not* shared, a file that is *not* renamed.
- **Commit before touching the tree with anything that discards work.** Two of
  these tasks rewrite files under a state directory in tests.

---

## The pieces where the obvious implementation is the wrong one

Stated up front, because each was found by reading the code rather than by
reasoning about it, and each is a task below that would otherwise be written
wrong.

| Piece | Why the obvious version is wrong |
|---|---|
| `list_on` defaulted to `list` | an implementor who did not update admits every instrument to the default shard — 31 shards collapsed onto one channel, no error, no counter, no log. The default goes the other way: `list_on` is required, `list` is defaulted, and the crate takes a major version |
| the era file for the default shard | a renamed file reads as *no file*, which resolves to `FIRST_ERA`. A publisher on era 7 would restart on era 1 and announce nothing — the exact corruption the corrupt-file refusal exists to prevent, delivered by the upgrade meant to be safe. The default shard keeps `<spec>.era` |
| partitioning the definition drain | per **feed** asks each pacer for the whole lap's debt once per feed, which is fence 4's burst. Per **shard** is the unit: one pacer, asked once, packed onto every feed of that shard |
| partitioning the registry | one registry per shard is N `Instrument ID` spaces and N writers on one state directory — and the single-writer guard would refuse the second, so it fails loudly at startup rather than subtly. One registry, N published sets |
| `SnapshotRotation::tick` | it divides the cycle by the published set size, read fresh every tick. Divided by the *process's* count, every shard is paced 31× slow while the key still reads as honoured |
| the `Anchor Seq` | `desynchronised` and `snapshot` read it off `feeds.market_by_price`. Off the wrong shard's pipeline it is a number from another channel instance's sequence series, which the subscriber compares against its own. A wrong answer, not a slow one |
| where the shard is recorded | on `InstrumentTable` it drags `dz-publisher-lowering` — and therefore the recorder's Mode C re-lowering, which links the lowering without the runtime — into a change that buys them nothing. It goes on the registry's own published entry |
| `Config::channel_ids()` | it sorts and **dedups**, so two blocks sharing a `Channel ID` silently pre-create one series set and both write to it. Nothing checks uniqueness today; 62 blocks make the collision likely rather than theoretical |
| `Config::feed_specs()` | one entry per block. At the worked size an adapter is handed `market-by-price` thirty-one times through `AdapterContext::feeds`, and it is answering "which feeds does this publisher emit" |
| `RegistryConfig.channel_id` | taken from `config.feeds.first()`. The builder overwrites the redundant copy per push, so it is invisible on the wire — but the field exists for the caller that encodes without a builder, which is exactly the caller it would lie to |
| the three port roles of one block | they share one era on purpose — "a restart is one event for the whole feed". That is a statement about one block, one block is one channel instance, and splitting it would be re-keying the wrong thing |
| relaxing `DuplicateFeedSpec` first | see *The ordering constraint*. It is task 9, not task 1 |

---

## Tasks

### 1. `shard` in the document: parsed, checked, and still singular

- [x] `FeedSection::shard: Option<String>`, `#[serde(default)]`, under the
      existing `deny_unknown_fields`.
- [x] A checked `ShardName` on the resolved `Feed`, produced by a constructor
      that owns the invariant — the pattern `SourceId` and `SelectionPolicy`
      already follow.
- [x] `DEFAULT_SHARD`, one token, in `dz-adapter-core` so that the boundary and
      the configuration cannot spell it differently. An absent key resolves to
      it.
- [x] `StartupError::UnsafeShardName` — one lowercase path component, at most 64
      bytes of `[a-z0-9-]`, checked at load. The same rule `EraStore::path_for`
      already applies to a feed name, checked in the one place the value enters
      the process, because it becomes a path component in two places later.
- [x] `StartupError::ReservedShardName` for a block spelling the default token
      explicitly: two spellings of one shard would be two era files.
- [x] `StartupError::DuplicateChannelId`, naming both blocks. New, and it closes
      a hole that exists today.
- [x] `Config::shards()` — the distinct shard names, in document order.
- [x] `Config::feed_specs()` returns the **distinct** specification set;
      `AdapterContext::shards()` added beside `feeds()`.
- [x] `DuplicateFeedSpec` unchanged and still in force.

**Test** (`dz-publisher-runtime/tests/config_document.rs`, no filesystem — every
case goes through `Document::parse` on a string): a name with an underscore, a
slash, an upper-case letter and a 65-byte name are each refused, and the message
names what would have been accepted; the default token spelled explicitly is
refused; two enabled blocks sharing a `channel_id` are refused naming both; a
document with no `shard` key resolves both blocks to the default shard; and the
control — `two_feed_blocks_naming_one_specification_are_refused` passes
unchanged, asserted by name so that a later task cannot delete it by accident.

> The `DuplicateChannelId` test is the one to check the mutant on: remove the
> check and it must fail. `channel_ids()` dedups, so without the check the
> document loads, one series set is created, and nothing anywhere says so.

---

### 2. `ListingSink::list_on`, and the shard set the registry checks it against

- [x] `fn list_on(&mut self, shard: &str, spec: &InstrumentSpec<'_>) -> Option<InstrumentRef>`
      on `ListingSink`, **required**; `list` becomes the defaulted method,
      forwarding with `DEFAULT_SHARD`.
- [x] The break is signalled where `RELEASING.md` says a consumer looks: **the
      tag message and the release notes**, in the shared-version bump. The
      earlier wording here asked for two things this repository does not have —
      a per-crate major version and a changelog. Every crate is
      `version.workspace = true` at one shared version, which `RELEASING.md`
      makes deliberate ("These crates are one workspace with one shared version
      for exactly this reason"), pre-1.0 a minor release may break an
      implementor, and its step 5 is *say what breaks* in the tag message. There
      is no changelog in the tree.
- [x] The reason that signal carries is corrected too. A venue *calls*
      `ListingSink` and needs no change; an implementor of it does, and
      **implementors outside this workspace exist** — `dz-adapter-core`'s own
      `tests/adapter_is_usable.rs` calls exercising the trait end to end "the
      property a venue's own mapping tests depend on", which is a venue's test
      doubles implementing it. What makes the break worth asking for is where it
      lands: a compile error in a test double is found by the next `cargo test`,
      and a published set silently collapsed onto one channel is found by a
      subscriber.
- [x] `Registry` holds the configured shard set, handed to it through
      `RegistryConfig`.
- [x] `Refusal::UnknownShard` and `Refusal::ShardRestated`, and their `Counts`
      entries. No new metric family — `dz-publisher-refdata` constructs none,
      and the alertable signal is task 10's
      `dz_publisher_refdata_instruments_current{channel_id}` sitting at 0.
- [x] The unknown name is *remembered* once per distinct value, not once per
      poll: an adapter may re-offer its whole set every second, and a line per
      offer would bury the first one. `Registry::take_unknown_shards` hands a
      caller each distinct name once and nothing twice.
- [x] **The caller.** This crate writes no line — it constructs no metric and it
      logs nothing — so the item above is only half of what this bullet
      promised. It was checked off with the mechanism built and nothing calling
      it, which is a venue's misnamed shard dropping instruments with neither a
      line nor a series to show it. Task 14 wires it, and the checkbox above is
      split in two so that the same claim cannot cover both halves again.

**Test** (`dz-publisher-refdata/tests/identity.rs` and a new
`tests/shards.rs`, `MemoryStore` and an injected clock): `list` and
`list_on(DEFAULT_SHARD, ..)` admit the same instrument to the same handle; an
unknown shard name refuses with `UnknownShard` and mints no `Instrument ID` —
asserted by restarting the registry over the same store and finding `next_id`
unmoved, because a refusal that still consumed an id is the failure worth
naming; a re-offer naming a different shard is `ShardRestated` and leaves the
instrument where it was. Plus a compile-level control: a fake `ListingSink` in
the test module that implements only `list_on` compiles, and one that implements
only `list` does not — the second as a `trybuild`-style doc assertion or, if
that is too much machinery for one property, as a comment on the trait pointing
at the design's paragraph.

---

### 3. The registry partitions its published set

- [x] Per shard: the published membership, `Instrument Count`, `Manifest Seq`,
      `Valid`, and one `DefinitionPacer`.
- [x] Process-wide and untouched: the `Instrument ID` minting table, `next_id`,
      the persisted record, the state-directory claim, `SelectionPolicy`, the
      `InstrumentTable` the lowerings borrow.
- [x] The shard is recorded on the registry's published entry. `InstrumentTable`
      and `dz-publisher-lowering` are not touched.
- [x] `definition_tick` takes a shard and drains that shard's pacer;
      `manifest()` takes a shard and composes that shard's summary with that
      shard's `channel_id`.
- [x] `RegistryConfig` carries the per-shard `Channel ID` rather than the first
      block's.
- [x] `seeding_complete` stays process-wide: one poll establishes the set.

**Test** (`dz-publisher-refdata/tests/cycle.rs`, extended): with two shards
configured, an admission on one bumps that shard's `Manifest Seq` and **not** the
other's, and the other's `Instrument Count` is unmoved — the assertion the
design's four specification citations reduce to; one lap of one shard's pacer
covers that shard's instruments exactly once and none of the other's; and the
existing `no_tick_can_be_made_to_emit_the_whole_published_set` is re-run per
shard so the anti-burst property is asserted at its new scope rather than
retired. With one shard configured, every existing test in the file passes
byte-for-byte unchanged, which is the task's real gate.

---

### 4. The era store keyed on the channel instance

- [x] `begin_era` and `persisted_era` take the shard alongside the feed.
- [x] The path is `<spec>.era` for the default shard and `<spec>.<shard>.era`
      for a named one. The shard component is already known safe from task 1;
      `path_for`'s own check stays as the second line of defence, because it is
      the function that builds the path.
- [x] `run.rs`'s two call sites pass the block's shard.

**Test** (`dz-publisher-egress/tests/era_persistence.rs`, a temporary
directory): a store holding `top-of-book.era` at 7 — written as the file, not
through the API, so the test describes an existing installation rather than one
this code just made — returns 8 for the default shard and **1** for a named one;
two named shards of one specification advance independently, and advancing one
leaves the other's file byte-identical; `persisted_era` answers for one shard
rather than for the specification; and the property fence 3 actually loses,
asserted directly: across two starts, each shard's era advances by exactly one,
and the values are the same whichever order the shards are begun in — which is
the order-dependence test, and it needs the two starts because a single start
cannot tell a stride of one from a stride of two. The existing `OtherFeed` test,
which already asserts that two feeds do not share an era, is kept and joined by
its shard twin.

> The mutant to kill is the rename. Change the default shard's path to
> `<spec>.default.era` and the first assertion must fail with 1 where 8 was
> expected. If it does not, the test is reading a store it created itself.

---

### 5. The reference-copy fan-out socket keyed on the shard

- [x] `TeeConfig::destination` takes the shard:
      `<path>.<spec>.<shard>.<port role>` for a named shard,
      `<path>.<spec>.<port role>` unchanged for the default one.
- [x] The doc comment gains the third noun. Its existing argument — a Unix
      datagram carries neither a destination port nor a group, so a recorder
      handed two things on one socket cannot attribute a datagram without
      decoding it — is the same argument for shards and should be extended, not
      rewritten.

**Test** (`dz-publisher-runtime/tests/config_document.rs`, pure path
arithmetic, no socket): the default shard's destinations are exactly the five
strings the current doc comment lists; a named shard's are distinct from them
and from each other across two shards and three port roles; and the suffix is
appended to the last component rather than becoming a child directory, which is
the property the existing `OsString` construction exists for.

---

### 6. `Feeds` as two vectors, and routing by index

- [x] `Feeds { top_of_book: Vec<FeedPipeline<TopOfBook>>, market_by_price: Vec<FeedPipeline<MarketByPrice>> }`.
      Two typed fields, no dynamic dispatch on the datagram path — fence 2's
      reason, kept.
- [x] A shard index resolved once per event beside the instrument lookup. No
      string comparison on the hot path.
- [x] `Event::Quote`, `Level`, `Clear` route to the shard's pipeline for the
      specification that carries them. `Event::Trade` goes to both of the
      shard's pipelines **from one lowered value** — one lowering, no second
      call site, unchanged.
- [x] `channel_ids`, `dark_transmitter` and `dropped_sinks` iterate.
- [x] `EventSink` unchanged. Not "unchanged in spirit" — the trait, its
      methods and its parameters are untouched, and a task that changes one has
      moved routing to the adapter.

**Test** (`dz-publisher-runtime/tests/depth_end_to_end.rs`, recording sinks):
with one shard, `one_trade_reaches_both_feeds_as_the_same_bytes` and
`a_publisher_emitting_both_feeds_routes_each_event_to_the_feed_that_carries_it`
pass unchanged — this task is a refactor and the existing suite is its gate. One
new test asserts the negative that will matter at task 9: a quote for an
instrument admitted to shard A appears on shard A's top-of-book mktdata sink and
on **no other sink**, asserted as an emptiness across every sink in the harness
rather than as a presence on one.

---

### 7. The snapshot rotation and the anchors

- [x] One `SnapshotRotation` per shard carrying a snapshot port role.
- [x] The per-instrument tick divides the cycle by **that shard's** published
      count, from the membership task 3 holds.
- [x] `desynchronised` reads the `Anchor Seq` from the instrument's own shard's
      market-by-price pipeline.
- [x] `snapshot` and `capture_and_send` likewise, and `SnapshotError::NoDepthFeed`
      keeps its meaning per shard: an instrument on a shard whose blocks carry no
      snapshot port role.
- [x] **The ceiling is a process ceiling, and dividing per shard stopped
      detecting it.** The rotation's own doc comment states the ceiling as a
      cycle divided by *one shard's* count falling below the runtime's tick.
      That was the whole of it when there was one rotation; with N of them the
      process still serves at most one snapshot per tick, so N shards owe N×
      what one publisher can send and each one's arithmetic still reads as
      comfortable. Task 16 restates the ceiling as the sum it actually is and
      counts the ticks on which it is breached.

**Test** (`rotation.rs`'s unit tests and `depth_end_to_end.rs`): the existing
`the_tick_is_the_cycle_divided_by_the_published_set` gains a sibling asserting
it is divided by the shard's count, with two shards of unequal size, so the
31×-slow bug is the thing the test fails on; and a reset on an instrument in
shard B produces an `InstrumentReset` whose anchor equals **shard B's**
mktdata sequence, with shard A's sequence deliberately set to a different value
first — a test that cannot pass by accident.

---

### 8. Teardown and the metric bridge, per channel instance

- [x] `shut_down` sends one final manifest with `Valid = 0` and one
      `EndOfSession` **per channel instance**, in the existing order within each:
      admissions closed, manifest, `EndOfSession`, flush. The manifest precedes
      `EndOfSession` because a subscriber that stops at the terminal statement
      would otherwise never see it, and that is a per-instance ordering.
- [x] `forward_counts` writes `manifest_seq` and `manifest_valid` from the
      shard that owns each `Channel ID`, not from one registry-wide value.

**Test** (`depth_end_to_end.rs`): with two shards,
`shutting_down_a_depth_publisher_ends_every_feeds_mktdata_channel` extends to
every channel instance, and each final manifest carries **its own** shard's
`Manifest Seq` — asserted with the two shards holding different published sets,
because equal sets would let a process-wide value pass.

---

### 9. The gate: a second `[[feed]]` of one specification

- [x] `DuplicateFeedSpec` becomes `DuplicateFeedShard { spec, shard }`, keyed on
      the pair.
- [x] `StartupError::ShardSpecsDisagree`, naming the shard and the specification
      it has no block for. This is the check that makes `list_on` total: an
      instrument admitted to a shard with no top-of-book block would have quotes
      that reach no wire and are counted only as unroutable.
- [x] `run.rs` composes one pipeline per block instead of assigning into an
      `Option`.
- [x] The design's *Why this cannot land incrementally* section is the review
      note on this task: it is the change that makes tasks 3, 4 and 7 load-bearing
      rather than latent.

**Test** (`config_document.rs` and a new
`dz-publisher-runtime/tests/shards_end_to_end.rs`): two blocks of one
specification with distinct shards resolve; two with the same shard are refused;
a shard with a market-by-price block and no top-of-book block is refused naming
both; and end to end, with two shards and both specifications — four channel
instances — an admission on one shard bumps one `Manifest Seq` and leaves the
other three unmoved, each channel instance starts in its own era, and each
refdata port carries only its own shard's definitions.

> That last assertion is the plan's centre. Written as *shard A's refdata port
> carries A's definitions*, it passes against a publisher that packs everything
> everywhere. It has to be written as *and none of B's*.

---

### 10. Metrics: sizing, and the one label that moves

- [x] `dz_publisher_refdata_instruments_current` gains a `channel_id` label and
      is pre-created per declared `Channel ID`. It is the gauge that mirrors the
      wire's `Instrument Count`, and that is per channel.
- [x] `definitions_emitted_total`, `new_listings_total` and `delistings_total`
      stay process-wide, and the reason goes in their doc comments so the next
      reader does not undo it.
- [x] No family is renamed and none is added, so `NORMATIVE_NAMES` is unchanged
      — which is itself the assertion that this task added nothing to a set
      somebody else owns.

**Test** (`dz-publisher-metrics/tests/precreated_at_startup.rs`): 62 declared
`Channel ID`s pre-create exactly 372 channel-keyed series across the four
families plus 62 for `instruments_current`, counted from the gathered
exposition rather than from arithmetic in the test; and 2 declared ids give 12
plus 2, so the ratio is asserted rather than asserted-about. The existing
`declared_channel_ids_render_at_zero_from_startup` extends to the new family.

---

### 11. The documents that have to stay true

- [x] `BRINGING-UP-A-FEED.md` gains `shard` in its configuration block, marked
      optional, with one line on when an operator needs it. That guide is the
      one document in this repository that must stay true rather than be a
      record of a date, and a key it does not mention is a key an operator will
      not know exists.
- [x] The publisher crate READMEs describe the shard as the unit of reference
      data and the channel instance as the unit of sequencing, in the design's
      own words, because those two being different is what everything above
      turns on.
- [x] `docs/README.md` carries the row for this pair. Landed with the spec and
      the plan, not here.

**Test:** `scripts/check-public-repo-rules.sh`, which runs before the toolchain
in CI, plus a read of the new prose against the glossary's banned-word table.
The words most likely to slip in here are the three the Naming section rejected.

---

### 12. It has to run

- [x] `examples/replay.sh` extended to a document with **four shards of one
      specification** — four channel instances — over the built-in
      normalized-event encoding, read by **this repository's Go subscriber**.
      Two shards and both specifications is what this bullet asked for and it
      is refused at startup: the built-in record adapter holds no book, so it
      answers no snapshot and `AdapterRegistry` refuses it beside a
      `market-by-price` block rather than publish deltas nothing can
      resynchronise. Four blocks of one specification is the arrangement the
      duplicate-specification gate refused, which is the half of the change
      this run exists to exercise; the depth half has no venue in this
      repository to run it. See *It ran*.
- [x] One block names no shard, so the run covers the upgrade as well as the
      feature: the default shard's era file has to keep the name it has always
      had while the other three take one of their own.
- [x] The subscriber's own output is the assertion: four channel instances,
      each with its own sequence series starting at 0, its own `Reset Count`,
      its own manifest, and only its own instruments' definitions.
- [x] Run twice, because an era is only observable across a restart: every
      channel instance's era advances by exactly one, independently.

**Test:** the script itself, run by hand and recorded here in the plan's own
"It ran" section, as the venue adapter plan does. It is deliberately not a CI
gate: it needs a multicast group and two processes, and a test that can only run
by hand must not be able to fail the build. Everything it exercises is already
covered by task 9's end-to-end test against recording sinks — which is exactly
the split that made the last plan's real run worth doing, because what it found
was two things no fake could have.

---

### 13. The composition, under test — a hole the acceptance rule left open

**Found after the fact, by perturbation.** Reversing the shard order in `run.rs`'s composition loop — the edit that publishes each shard's instruments under another channel instance's sequence series — leaves **all 1408 tests passing**. Nothing in the suite reaches `compose_and_run`. The only things that do are `examples/replay_publisher.rs` and the by-hand `examples/replay.sh`.

That leaves this plan's own acceptance rule satisfied in every part but one. "Reverting task 3, 4, 7 or 9 kills a named test" holds for all four — but task 9's *composition* half, `run.rs composes one pipeline per block instead of assigning into an `Option``, has no test that dies. It is the half whose failure is invisible on the wire.

**And the by-hand run does not catch it either**, which the perturbation also showed and which is a correction to the finding as first stated. Two reasons, both worth recording:

- The definition path is **name-keyed**, not index-keyed: `ShardFeeds` derives its name from one of its own send paths, and the tick asks the registry by that name. So reversing the order leaves every reference-data port carrying exactly its own shard's definitions — which is what `replay.sh` asserts.
- The *event* path is index-keyed. A quote resolves the registry's shard index and uses it to index `Feeds`, so under the reversal it reaches another shard's pipeline, whose lowering does not hold that instrument, and the message is dropped before any wire. Every channel loses exactly its own market data and no channel gains any — 38 messages became 32 — and `replay.sh` asserted the count in prose and never asserted that a **quote** arrived at all.

So the mutant is killed by nothing, including the run this plan credits. Three things close it.

- [x] **A port opener behind a trait.** `open_ports` takes `route: &KernelRoute`, the concrete type, although `RouteLookup` exists and its own doc comment says it exists because "a test that needs a route to a multicast group is a test that does not run in CI". The route is not the missing seam: `MulticastTransmitter::open` binds and connects a real socket, so what has to go behind a trait is the thing that produces a feed's `Ports`. The real implementation is backed by `KernelRoute` and is what `run()` passes.
- [x] **The composition extracted and called with it.** One function taking the shard list, the feed list, the era store, the metrics and the opener, returning `Feeds`. A test with two shards then asserts that `Feeds`' order is `Config::shards()`' order and that each shard carries its own `Channel ID`s — which covers the order, the era file per shard, and the registry's shard list in one place, because all three are built from that one list.
- [x] **`ShardFeeds::new(None, None)` tested directly**, because no document can produce the empty pair: `Config::shards()` is the distinct shards *of the enabled blocks*. An invariant no document can violate is one a later refactor can, which is what the refusal is for.
- [x] **`StartupError::ShardWithNoFeed` exercised**, and this is where the decision about it lands. It cannot be reached through a document *or* through a hand-built `Config`, for the same reason — so it is reached through the extracted composition, handed a shard list that names a shard the feed list does not. A variant nothing exercises is worse than no variant; extracting the composition is what makes this one real rather than decorative.
- [x] **`replay.sh` gains the floor it was missing**: each channel carried its own instruments' **quotes and trades**, not only their definitions. Asserting a definition passes against a publisher that publishes no market data at all — the same shape of hole this plan found twice in its own unit tests and fixed both times.

**Test:** `dz-publisher-runtime/tests/composition.rs`, over the existing harness's port builders — the fake opener hands back the same recording `Ports` the end-to-end suites already use, so nothing new has to be invented to build one.

**The reverts, run.**

| Reverted | Test that failed |
|---|---|
| The shard order reversed in `compose_feeds` — the perturbation that started this task | `the_composition_orders_the_shards_as_the_document_states_them`, `a_shards_blocks_are_opened_together_before_the_next_shards` |
| `ShardFeeds::new` defaults the name instead of refusing the empty pair | `a_shard_with_neither_specification_is_not_a_shard` |
| A shard with no block is skipped instead of refused | `a_shard_with_no_block_is_refused_rather_than_skipped` |
| The shard order reversed, against `replay.sh` | the run now **fails**, on the floor: `channel 0 on (the default shard) carried definitions and no quote: {'manifest_summary': 19, 'heartbeat': 3, 'instrument_definition': 9, 'end_of_session': 1}` |

Before this task the first of those failed **nothing at all**, in the suite or by hand. The last row is what the missing floor cost: the script's own diagnostic now names the shape of the failure — every definition intact, no market data anywhere.

**One thing beyond what was asked, recorded because it is why the perturbation was invisible.** The end-to-end harness composes its own `Feeds`, shard-outer and block-inner, duplicating the runtime's composition. So those suites assert the harness's ordering, and the two can drift. Rewiring the harness onto `compose_feeds` would put every end-to-end test on the real composition, and it is not done here: `compose_feeds` takes an `EraStore`, which writes files, and the harness hands each pipeline a literal `ResetCount` and touches no disk. Doing it properly means a second seam for the era, which is its own change.

---

## The three tasks a review added

Tasks 14 to 16 were not in the plan as written. They are here because a review
of everything above found three things, two of them blocking, and because a task
added after the fact belongs in the plan rather than only in a commit message:
the first is an item task 2 checked off without delivering, the second is this
change reaching a tier the plan never mentioned, and the third is a stated
ceiling that stopped being true when the divisor became per shard.

---

### 14. The unknown shard name reaches an operator

- [x] The runtime drains `Registry::take_unknown_shards` on the tick that
      polled, and writes one line per distinct name, beside the tick lines it
      already writes for a dropped fan-out member and a refused snapshot.
- [x] The line names the shard the venue asked for **and the shards this
      publisher is configured with**, because that pair is what identifies a
      misspelling and neither half alone does.
- [x] The exit report carries `declined_unknown_shard` and
      `declined_shard_restated`, beside the five numbers no series carries that
      it already prints.

**The two counts stay mapped to no metric family.** The normative
`dz_publisher_*` set is closed by the playbook, this repository does not own it,
and a series added here would be one nobody declared — the same reasoning that
leaves `declined_at_cap` unmapped, and it is unchanged.

What does change is the claim used to justify it. `Counts` and the design both
say the alertable signal already exists, because
`refdata_instruments_current{channel_id}` sits at 0 for a shard nothing was
admitted to. That is true only of the total case. A venue that misnames *some*
of its offers — one instrument, or every instrument of one product line — leaves
the gauge non-zero and every one of those instruments unpublished, and no series
in the closed set separates that from a channel that simply holds fewer
instruments. So the line is not a convenience beside a gauge; for the partial
case it is the only signal there is, which is exactly why promising it and not
writing it was the defect rather than a missing nicety.

**Test** (`shards_end_to_end.rs`): a venue offers one instrument on a shard the
document does not name; the drain returns that name once, and a second poll
re-offering the same instrument returns nothing. The property under test is that
the runtime drains the registry at all, so the mutant is a runtime that does
not.

**And one part of this has no test that can fail**, said plainly rather than
covered by a test that would look like a gate. The `eprintln!` in `tick_loop` is
unreachable from the suite — no test in this workspace calls that function, and
none captures stderr — so what is asserted is the drain the line is written from
and not the writing. That is the same shape of gap `report` has had since it was
written, and it is why the drain was put on `Publisher`, where a test reaches
it, rather than inlined at the call site.

---

### 15. The recorder's completeness check becomes per channel

This is the one that reaches another tier, and the plan had nothing about the
recorder in it.

`ManifestSummary`'s `Instrument Count` and `Manifest Seq` are per channel — that
is the specification's own definition and it is the whole point of task 3 — but
`dz-recorder-relower` keeps **one** manifest for a whole archive, picked by
highest `Manifest Seq`, and `ArchivedRefdata::finalise` compares it against the
union of every channel's definitions. Over an archive covering one channel that
is correct. Over an archive covering two it compares a union against one
channel's count, and it reports `ReferenceDataIncomplete` on complete reference
data, or stays silent on incomplete data, depending on nothing but which channel
happened to carry the higher `Manifest Seq`.

- [x] `ArchivedRefdata` holds a manifest **per `Channel ID`**, and the symbols
      each channel defined, and `finalise` compares each channel's declared
      count against that channel's own definitions.
- [x] `Caveat::ReferenceDataIncomplete` names the channel. Two caveats that
      differ only in which channel was short are otherwise one line printed
      twice, and `push_once` would collapse them into one.
- [x] `observe_definition` and `observe_manifest` take the `Channel ID` the
      message was carried on. Both call sites already hold the provenance, so
      nothing new has to be threaded to reach them.
- [x] `declared_instrument_count` takes a `Channel ID`. There is no process-wide
      answer to compose from several channels' counts, and their sum is not one
      either: it is the sum of disjoint published sets, which is a number no
      manifest states and no subscriber sees.

**Keyed on the channel and not on the channel instance**, which is the decision
here worth arguing. `by_symbol` is already a union across the redundant paths of
a channel, and it raises `ScaleRestated` when two paths disagree, so the
reconstruction a completeness check guards resolves from that union. Keyed on
the instance the check would report a caveat against a path whose refdata window
was shorter even where the union covers the set and the re-lowering declines
nothing — a caveat about capture coverage dressed as one about the archive.
`Instrument Count` is defined per channel, `GLOSSARY.md` has an instrument
unique *within a channel*, and `dz-recorder-events`' own accumulator keys on
`(source IP address, Channel ID)` for a related reason it states at length. The
channel is the scope the count is stated at and the scope the reconstruction
resolves at, so it is the scope the comparison belongs at.

**Not deferred, and here is why that was the choice.** The honest alternative
was to call this its own piece of work and meanwhile make the current behaviour
loud — refuse to run over an archive covering more than one channel rather than
compare the wrong two numbers. That is a real option and it is the right one
whenever the fix is large. This fix is not: both call sites hold the `Channel
ID` already, the map is a `BTreeMap` keyed on a `u8`, and `finalise` becomes a
loop over it. Refusing would have cost a public error variant, a runner that
stops on captures it can read perfectly well, and the same work again later.

**Test** (`dz-recorder-relower/tests/reference_data.rs`): one archive covering
two channels, one complete and one short, yields exactly one
`ReferenceDataIncomplete` naming the short channel. Reverted to one manifest per
archive, the union of both channels' definitions is compared against the
higher-`Manifest Seq` channel's count and the caveat is wrong in whichever
direction the fixture is arranged.

---

### 16. The snapshot ceiling is a sum over shards

- [x] `rotation.rs`'s module note states the ceiling as the sum it is: N
      rotations against one process's serving rate of one snapshot per tick,
      rather than each rotation against its own derived tick.
- [x] The arithmetic is a function, so it can be asserted directly the way
      `tick` is.
- [x] The publisher counts the ticks on which the configured cycles ask for more
      snapshots than one process can send. The tick loop names it on the decade
      schedule `worth_a_line` already states — a document that asks for too much
      asks for it on every tick thereafter, so an unfiltered line would be a
      hundred a second — and the exit report names the total.

**It counts, and it does not refuse.** A refusal would have to happen at load,
and at load the number that decides it does not exist: the divisor is the
published count, and nothing is published until the venue's first poll has
returned. Deferring the refusal to the first tick that *can* compute it means
darkening a publisher that is already sending — over a pacing shortfall, which
degrades into a slower lap and never into a wrong answer, and whose remedy is a
configuration edit an operator has to be told about rather than one the process
can make. A count and a line say the thing; a refusal would trade a slow feed
for no feed.

**Test** (`rotation.rs`'s unit tests and `snapshot.rs`): the share arithmetic
asserted directly, including the case the per-shard statement misses — several
shards each of whose derived tick is comfortably above the process tick, whose
sum is not — and a publisher over such a configuration counting the tick.
Reverting the sum to the per-shard comparison leaves the count at 0.

---

## It ran

Everything above is tested against fakes — sockets behind traits, injected
clocks, recording sinks — which is what makes the suite run unprivileged with no
network, and which means all of it was a hypothesis until four channels left
four sockets.

`rust/publisher/dz-publisher-runtime/examples/replay.sh` is that run. It writes
a document with four `[[feed]]` blocks of one specification, records a directory
of normalized-event payloads for eight instruments, runs `run()` over them —
the real config, the real registry, the real built-in adapter, the real
lowering, real multicast sockets, the real teardown — and reads the other end
with four instances of **this repository's Go subscriber**, one per channel
instance. Then it does the whole thing again, because an era is only observable
across a restart.

```text
  channel  0 (the default shard)  38 messages, era 2, seq from 0, definitions REPLAY-D1,REPLAY-D2
  channel  1 alpha                38 messages, era 2, seq from 0, definitions REPLAY-A1,REPLAY-A2
  channel  2 beta                 38 messages, era 2, seq from 0, definitions REPLAY-B1,REPLAY-B2
  channel  3 gamma                38 messages, era 2, seq from 0, definitions REPLAY-G1,REPLAY-G2

  four eras, one per channel instance: [2, 2, 2, 2]
  each advanced by exactly one across the restart

  fan-out.top-of-book.mktdata       20 datagrams, longest 84 bytes, first magic 0x5a44
  fan-out.top-of-book.alpha.mktdata 20 datagrams, longest 84 bytes, first magic 0x5a44
```

Each subscriber read its own `Channel ID` and no other, its own sequence series
from 0, a `Reset Count` equal to its own era file, and **exactly its own
shard's two definitions** — stated as an equality, because *carries mine*
passes against a publisher that packs everything onto everything. Four era
files sit under the state directory, the default shard's still
`top-of-book.era`, and each advanced by one rather than by four, which is what
a shared counter would have done.

Two instruments per shard rather than one, for the end-to-end test's reason: a
packing publisher owes one definition per lap and would still put exactly one
symbol on each port, so with one each the failure reads as *carries none of its
own* and the exclusion is never reached.

**What could not be run, and why the plan asked for it anyway.** Task 12 asked
for two shards and *both* specifications. The built-in record adapter holds no
book, so it answers no snapshot, and `AdapterRegistry` refuses it beside a
`market-by-price` block — a depth feed publishing deltas with no snapshot leaves
a mid-session joiner wrong at every price it never saw an update for,
indefinitely. That refusal is right and predates this plan; what it means is
that a real depth run needs a venue with a book, and this repository has none.
Four shards of one specification exercises everything the gate at task 9
lifted; the depth path's shard routing is covered where it can be observed, by
`depth_end_to_end.rs` and `shards_end_to_end.rs` against recording sinks.

**Two things only the real run could find.**

`GROUPS` is bash's own array of the invoking user's group ids, and assignments to
it are silently ignored. The generated document therefore carried
`multicast_group = "1000"`, and startup refused it by name — the failure this
publisher is built to produce instead of starting on something that cannot be a
group. A script that had defaulted the key, or a publisher that had parsed it
loosely, would have joined nothing and looked healthy.

And the reference-copy fan-out was still keyed on the feed and the port role
alone. Four channel instances fanned out to five sockets, two shards' copies of
one role arriving on one — which a recorder cannot attribute without decoding,
the one thing a record path does not do. That is task 5, and the tasks-1-to-8
commit had missed it: `fan-out.top-of-book.alpha.mktdata` did not exist until
it landed, and the run's second reference copy is the assertion that it does.

## Acceptance

The plan is done when:

1. a document with 62 `[[feed]]` blocks — 31 shards × 2 specifications — starts,
   and one with two blocks naming one `(spec, shard)` pair, or a duplicate
   `Channel ID`, or a shard missing a specification's block, is refused at load
   with a message naming both offenders;
2. an adapter admits an instrument with `list_on("<a shard>", spec)` without
   naming a `Channel ID`, a group, a port, a sequence number or an era anywhere
   in its own code, and an adapter that never names a shard compiles and runs
   unchanged;
3. an admission on one shard changes one shard's `Manifest Seq`, `Valid` and
   `Instrument Count`, and no other shard's;
4. a restart advances every channel instance's era by exactly one, independently,
   with one era file each under the state directory, and reordering the
   `[[feed]]` blocks changes none of them — with an existing default-shard era
   file carried across the upgrade rather than restarted;
5. each channel instance's refdata port carries its own shard's definitions and
   none of any other's;
6. the pre-created exposition is 372 channel-keyed series plus 62, counted, not
   estimated;

and when reverting any one of tasks 3, 4, 7 or 9's changes makes at least one
named test fail — because a plan whose tests all pass against the tree it was
written for has documented the tree rather than changed it.

**That rule was satisfied in every part but one, and task 13 is the part.** Task
9's composition half had no test that died: nothing in the suite reached
`compose_and_run`, and the by-hand run could not tell the difference either.
The rule found it — one revert short of the four it asks for — which is the
argument for the rule rather than against it.

**Each of those reverts was run, and this is what died.**

| Reverted | What was put back | Tests that failed |
|---|---|---|
| Task 3 | `definition_tick` emits every slot it walks, not only its shard's | `each_reference_data_port_carries_its_own_shards_definitions_and_none_of_the_others` |
| Task 3 | `published_on` answers the process's published count | `an_admitted_instrument_joins_one_published_set_and_no_other`, `a_re_offer_naming_another_shard_leaves_the_instrument_where_it_is` |
| Task 4 | the era file is `<spec>.era` for every shard | seven in `era_persistence.rs`, including `a_newly_named_shard_does_not_inherit_another_shards_era` and `adding_or_removing_a_shard_does_not_change_the_era_the_others_see_next` |
| Task 7 | the rotation divides the cycle by the process's published count | `a_shards_snapshot_rotation_serves_its_own_instruments_at_its_own_cycle` |
| Task 9 | the duplicate gate is keyed on the specification alone | `two_blocks_of_one_specification_on_different_shards_resolve`, `thirty_one_shards_of_both_specifications_resolve_as_sixty_two_channel_instances` |
| Task 14 | `Publisher::take_unknown_shards` hands back nothing, which is the runtime as the review found it | `a_shard_the_document_has_no_channel_for_is_named_once_however_often_it_is_offered` |
| Task 14 | the line drops the configured shard names and keeps the offered one | `the_unknown_shard_line_names_the_offer_and_the_configured_shards`, `a_publisher_with_no_named_shard_still_names_what_it_has` |
| Task 15 | `finalise` compares each channel's count against the union of every channel's definitions | `each_channels_manifest_is_checked_against_its_own_definitions`, `a_channel_with_no_manifest_of_its_own_borrows_no_other_channels_count` |
| Task 15 | one manifest for the archive, highest `Manifest Seq` across channels | the two above plus `the_reconstructed_table_matches_what_the_definitions_said` and `a_manifest_declaring_more_instruments_than_the_archive_carries_is_reported` |
| Task 16 | the per-shard comparison the module note used to state, instead of the sum | `cycles_that_are_achievable_per_shard_and_not_together_are_counted` |
| Task 16 | an empty published set is charged for the pass `tick`'s clamp implies | `a_shard_with_nothing_published_asks_for_nothing` |

The two task-15 rows are worth reading together, because between them they show
the shape of the defect rather than just its presence. Reverted to the union,
the archive whose complete channel holds the higher `Manifest Seq` reports
`ReferenceDataIncomplete { channel_id: 1, declared: 2, reconstructed: 3 }` — a
caveat against the channel that is complete, carrying a count belonging to
neither channel, with the channel that actually fell short unmentioned.

**One part of task 14 has no revert, and it is named rather than covered.** The
`eprintln!` in `tick_loop` is unreachable from the suite: no test calls that
function and none captures stderr. What is killed is the drain it writes from
and the line it writes; the call between them is not. That is the same gap
`report` has always had, and the drain sits on `Publisher` precisely because
that is the furthest along the path a test reaches.

Task 5 was reverted twice, in both directions, because its two halves fail
differently: always appending the shard fails the default's five literal
destinations, and never appending it fails the distinctness of the fifteen
sockets three shards of both feeds open. A revert that fails nothing is the
finding, and there was one — the reference-copy socket had never been keyed on
the shard at all. See *It ran*.
