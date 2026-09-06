# Market data as rows, derived from the archive rather than captured beside it

**Status:** draft, pending review
**Date:** 2026-09-05
**Applies to:** the recorder's analysis tier, and the column store a dashboard reads
**Authority:** [`edge-feed-spec`](https://github.com/malbeclabs/edge-feed-spec) and its [`GLOSSARY.md`](https://github.com/malbeclabs/edge-feed-spec/blob/main/GLOSSARY.md); `2026-08-31-sequence-loss-and-conformance-rows-design.md`, whose row tier this extends; `2026-09-02-venue-adapter-interface-design.md`, which is why this can be written once rather than once per product line

---

## Naming

This repository is public. This document names no venue, venue repository,
product line, host, bucket, dashboard or issue tracker, and gives no count of
publishers or of recorder sites. It states only what is to be built.

`GLOSSARY.md` governs the vocabulary: `datagram` never `frame`, **`era` never
`epoch`**, `channel` only for the `Channel ID` shard and `port role` for
`mktdata`/`refdata`/`snapshot`, `feed` never `lane` or `stream`, `decode` never
`normalization`, `published set` never `roster`, and **`source` never bare** —
every use below is `source address`, `Source ID`, or `upstream`.

Two glossary definitions are load-bearing here rather than decorative, and the
schema is different because of them:

> **Instrument** — one tradable entity, keyed by `Instrument ID` (`u32`), unique
> within a channel.
>
> **Symbol** — the `char[64]` human-readable name in `InstrumentDefinition`.
> Display and filtering only.

---

## The question

The rows that exist answer **transport** questions: how many datagrams are
missing per channel instance, whose they are, and which spec rules passed. They
are keyed for that and they are good at it.

Not one of them can answer a **market data** question:

- What was the top of book for this instrument at this instant?
- How many instruments were quoting, and which stopped?
- Two observation points saw the same book state — which saw it first, and by
  how much?
- A `LevelUpdate` arrived at this sequence number. What did the book look like
  before it, and after?

`recorder.datagram` holds `payload_len` and `wire_payload_len`. It records how
large a message was, never what it said. That is a deliberate property of the
record path — the recorder decodes nothing while recording, which is what makes
it cheap enough to run beside a publisher — but it is a property of the record
path, and it has been allowed to become a property of the rows.

**Those questions are answered today by a second process per venue**: one that
subscribes to the same feed independently, decodes live, maintains its own book,
and writes its own tables under its own column names. Every product line that
wants the answers gets its own copy of that process, its own schema, its own
metric names, and its own decoder — a decoder this repository already ships, and
a book this repository already specifies.

**This document specifies the same information, derived from the archive.** Once,
generically, so that a feed becomes rows by being recorded rather than by
someone writing a capture for it.

---

## Why this is a derivation and not a port

The pieces are in the tree already, and the load-bearing one is not obvious.

**Decoding an archive into messages exists.** `dz-recorder-relower`'s
`WireCapture::absorb` walks an archive to exhaustion against a declared `Magic`
and yields, per message, a `MessageBody` — `Quote`, `Trade`, `LevelUpdate`,
`BookClear` — with a `WireProvenance` carrying `channel_id`, `sequence_number`,
`reset_count`, `send_timestamp_ns`, `recv_ts_ns`, the port role, and the index
of the message inside its datagram. It was built to compare a publisher's output
against a re-lowering of the upstream payloads, but the walk is what a row
deriver needs, and it is already written, already tested, and already strict
about the three ways a walk goes wrong: foreign `Magic`, undecodable datagram,
unknown message type.

**Reference data comes off the wire, not from a registry.**
`InstrumentDefinition` and `ManifestSummary` are in the same archive, so the
exponents that decode a price are the ones that were on the wire — not the ones a
registry holds today. A live capture that reads a registry at startup runs
today's mapping over yesterday's bytes and has no way to notice. That property is
the reason to derive offline at all.

**`ArchivedRefdata` is not the accumulator this needs, and how it differs is
instructive.** It keys `by_symbol`, and on a restatement it **keeps the first
definition** and raises `ScaleRestated`. That is deliberate and the reasoning is
sound for what it was built for: a re-lowering holds *two* archives whose clocks
belong to a subscriber and to a publisher, with no key that orders one against
the other, so there is no defensible instant at which to switch exponents.
Keeping the first is the honest half of an unanswerable question.

**A row deriver is not in that position.** It holds one archive, in which every
message carries a sequence number and every era carries an anchor, so it *can*
place a restatement exactly: at the sequence number of the definition that made
it. The accumulator this needs is therefore a different one — keyed
`(channel_id, instrument_id)` rather than by symbol, and **scoped to an era**,
restating forward from the definition that changed it rather than pinning the
first. Reusing `ArchivedRefdata` unchanged would apply a pre-restatement exponent
to post-restatement prices, and would key the whole join on the one field this
document argues is not a key.

**The row machinery exists.** `dz-recorder-rows` defines the row model,
`dz-recorder-clickhouse` owns the checked-in migrations and the insert sink, and
`dz-recorder-load` is the loader: read a host's own completed objects read-only,
keep a ledger keyed on `(object key, sha256)`, insert idempotently, and expose
how far behind it is.

What is missing is a **row model, a book, and a retention decision**. No new
decoder, no venue client, no credential, and no change to the record path.

---

## The rows

Three tables, in `recorder`, beside the five that exist — `datagram`, `era`,
`segment_coverage`, `sequence_gap` and `conformance_finding`. Every one carries
the identity block those rows carry — `site`, `recorder`, `env`, `feed`,
`port_role`, `source_addr`, `channel_id`, `dst_port` — so that a market data
question and a loss question can be asked in one join. That is the whole reason
for putting them in the same database rather than in a second one.

### 1. `event` — one row per decoded message

The base fact, and the expensive one.

```sql
CREATE TABLE IF NOT EXISTS recorder.event (
    recv_ts            DateTime64(9),
    send_ts            DateTime64(9),
    upstream_ts        Nullable(DateTime64(9)),  -- the venue's own event time
    send_recv_ms       Float64 MATERIALIZED
                         (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(send_ts)) / 1e6,
    recv_ts_kind       LowCardinality(String),

    site               LowCardinality(String),
    recorder           LowCardinality(String),
    env                LowCardinality(String),
    feed               LowCardinality(String),
    port_role          LowCardinality(String),
    source_addr        IPv4,
    channel_id         UInt8,
    dst_port           UInt16,

    sequence_number    UInt64,
    reset_count        UInt8,
    era_anchor_ts      DateTime64(9),
    message_index      UInt8,

    source_id          UInt16,
    instrument_id      UInt32,
    symbol             LowCardinality(String),   -- display only, resolved at this era
    price_exp          Int8,
    qty_exp            Int8,
    per_instrument_seq Nullable(UInt32),

    message_type       LowCardinality(String),
    side_raw           Nullable(UInt8),
    action_raw         Nullable(UInt8),
    reason_raw         Nullable(UInt8),
    flags_raw          Nullable(UInt8),
    price_raw          Nullable(Int64),
    qty_raw            Nullable(UInt64),
    order_count        Nullable(UInt16),
    level_index        Nullable(UInt16),

    bid_px_raw         Nullable(Int64),          -- Quote carries both sides at once
    bid_qty_raw        Nullable(UInt64),
    bid_source_count   Nullable(UInt16),
    ask_px_raw         Nullable(Int64),
    ask_qty_raw        Nullable(UInt64),
    ask_source_count   Nullable(UInt16),

    trade_id           Nullable(UInt64),
    cumulative_volume  Nullable(UInt64),

    snapshot_id        Nullable(UInt32),
    anchor_seq         Nullable(UInt64),
    total_levels       Nullable(UInt32),
    levels_seen        Nullable(UInt32),
    depth_bound        Nullable(UInt32),

    object_key         String,
    object_sha256      String,
    datagram_index     UInt64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (channel_id, instrument_id, era_anchor_ts, sequence_number, message_index,
          source_addr, dst_port, site, recv_ts);
```

**One table with nullable per-type columns, not one table per message type.** The
bodies share every column above `message_type` — identity, sequencing, instrument
and timing — and differ in at most six. The dominant question is "everything that
happened to this instrument over this window", which separate tables turn into a
union that must be rewritten whenever a message type is added to the family. The
cost is a column store's cheapest case: a column of one message type's fields is
not read by a query filtered to another.

**The mapping from message to columns is stated here, once, so that a reader
never has to infer it from the deriver.** Anything not listed for a type is
`NULL` in that type's rows.

| Message | Carries on the wire | Mapping |
|---|---|---|
| `Quote` | `instrument_id`, `source_id`, `source_timestamp_ns` | `bid_px_raw`, `bid_qty_raw`, `bid_source_count`, `ask_px_raw`, `ask_qty_raw`, `ask_source_count`, `flags_raw` ← `update_flags`, `upstream_ts` ← `source_timestamp_ns` |
| `Trade` | `instrument_id`, `source_id`, `source_timestamp_ns` | `price_raw` ← `trade_price`, `qty_raw` ← `trade_qty`, `side_raw` ← `aggressor_side`, `flags_raw` ← `trade_flags`, `trade_id`, `cumulative_volume`, `upstream_ts` ← `source_timestamp_ns` |
| `LevelUpdate` | `instrument_id`, `source_id`, `per_instrument_seq`, `timestamp_ns` | `side_raw` ← `side`, `action_raw` ← `action`, `reason_raw` ← `update_reason`, `flags_raw` ← `level_flags`, `price_raw`, `qty_raw`, `order_count`†, `level_index`†, `upstream_ts` ← `timestamp_ns` |
| `BookClear` | `instrument_id`, `source_id`, `per_instrument_seq`, `timestamp_ns` | `side_raw` ← `clear_side`, `action_raw` ← `scope`, `reason_raw` ← `clear_reason`, `price_raw` ← `from_price_raw`, `upstream_ts` ← `timestamp_ns` |
| `InstrumentReset` | `instrument_id`, `timestamp_ns` — **no `Source ID`** | `reason_raw` ← `reason`, `anchor_seq` ← **`new_anchor_seq`**, `upstream_ts` ← `timestamp_ns`, `source_id` ‡ |
| `SnapshotBegin` | `instrument_id`, `timestamp_ns` — **no `Source ID`** | `snapshot_id`, `anchor_seq`, `total_levels`, `depth_bound`, `per_instrument_seq` ← `last_instrument_seq`, `upstream_ts` ← `timestamp_ns`, `source_id` ‡ |
| `SnapshotLevel` | **`snapshot_id` only** — no instrument, no timestamp, no level index | `side_raw` ← `side`, `flags_raw` ← `level_flags`, `price_raw`, `qty_raw`, `order_count`†; `instrument_id`, `upstream_ts` and `level_index` ⁂ |
| `SnapshotEnd` | `instrument_id`, `anchor_seq`, `snapshot_id` | `snapshot_id`, `anchor_seq`, `levels_seen` ⁑ |

**† The wire's absent-value sentinel is `NULL`, not a number.** `order_count` and
`level_index` carry `U16_UNAVAILABLE` (`0xFFFF`) when the venue exposes neither,
and the specification is explicit that it is not a count and not a rank. Written
through as `65535` it becomes an instrument with sixty-five thousand orders at a
level, which is not a subtle wrongness but it is a silent one. The deriver
translates the sentinel to `NULL` on both columns.

**‡ Where a message omits `Source ID`, it is resolved from era-qualified
reference data** for that `(channel_id, instrument_id)`, never invented and never
carried over from an adjacent message of another type. The same rule supplies
`price_exp` and `qty_exp`, which no market data message carries at all.

**⁑ `levels_seen` is counted by the deriver, not read from the wire**, and it
exists because persisting every `SnapshotLevel` is optional. A cycle is
`total_levels` messages per instrument per cycle, on the runtime's cadence rather
than the market's, so it is the largest row count in the system attached to the
port role with the least analytical value per row. **The book consumes every
level; `event` persists them behind a per-feed switch that is off by default**,
while `SnapshotBegin` and `SnapshotEnd` are always written. `total_levels` on the
begin row against `levels_seen` on the end row then answers *was the snapshot
complete* from rows alone — which is the question persisting the levels would
otherwise have been the only way to ask.

**⁂ A `SnapshotLevel` inherits its instrument and its time from the
`SnapshotBegin` that `snapshot_id` ties it to**, which is precisely why
`snapshot_id` is on the level at all — the codec says so, and a level whose
`snapshot_id` matches no open begin in the window is a row the deriver refuses
rather than guesses at. `level_index` is the level's ordinal within the snapshot,
assigned by the deriver from arrival order within the cycle, and is marked as
derived rather than read so that nobody later compares it against a wire field
that does not exist.

Note what this table already tells you: **the deriver decodes more message types
than `WireCapture` does, and needs more of each datagram than it keeps.**
`MessageBody` has four variants because a re-lowering compares only the messages
a venue event produces; a book needs the three snapshot messages and
`InstrumentReset` as well, and none of them is in that enum.

And `WireProvenance` is narrower than this schema. It keeps `channel_id`,
`sequence_number`, `reset_count`, the two timestamps, the port role and the two
indices — it does **not** keep the source address, the destination port or the
receive-timestamp kind, all three of which `RecordedDatagram` carries and the
walk discards. Every one is in the identity block above, so provenance must be
widened before a single `event` row can be written. That is the least
interesting item on the work list and the one most likely to be discovered late.

**Ordered by instrument first, and by the channel instance as well.** Two things
are being served and they pull opposite ways. `ReplacingMergeTree` deduplicates on
the whole sort key, so the key must carry everything that distinguishes two
genuine rows: without `source_addr` and `dst_port`, two paths publishing one
`Channel ID` collapse into one row; without `recv_ts`, a duplicated datagram —
same sequence number, same message index, different arrival — deletes the
original rather than sitting beside it. `datagram` carries all three for exactly
this reason and this table must too.

They sit *after* `instrument_id` rather than before it, which is the one place
this key departs from `datagram`'s. Every question asked of `datagram` is per
channel instance; the dominant question here is per instrument over a window, and
a leading instance prefix makes that a full scan. The instance columns are here
for identity and for deduplication, not as the leading filter.

**Never ordered by symbol.** See *Symbol is not a key*.

**Prices and quantities stay raw, with their exponents beside them.** Converting
to a decimal at load time bakes in a scale a later era can change and loses the
exact integer the wire carried — the only value a conformance question can be
asked against. A reader who wants a number writes `price_raw * pow(10,
price_exp)`; a reader who wants to know what was sent still has it.

### 2. `instrument` — the archived reference data, kept

```sql
CREATE TABLE IF NOT EXISTS recorder.instrument (
    site           LowCardinality(String),
    recorder       LowCardinality(String),
    env            LowCardinality(String),
    feed           LowCardinality(String),
    port_role      LowCardinality(String),
    source_addr    IPv4,
    channel_id     UInt8,
    dst_port       UInt16,
    source_id      UInt16,
    instrument_id  UInt32,
    era_anchor_ts  DateTime64(9),
    reset_count    UInt8,
    symbol         String,
    price_exp      Int8,
    qty_exp        Int8,
    contract_value UInt64,
    first_seen_ts  DateTime64(9),
    last_seen_ts   DateTime64(9),
    manifest_seq   Nullable(UInt16),
    declared_count Nullable(UInt32),   -- what ManifestSummary said the published set held
    object_key     String
)
ENGINE = ReplacingMergeTree(last_seen_ts)
PARTITION BY toYYYYMMDD(era_anchor_ts)
ORDER BY (channel_id, instrument_id, era_anchor_ts, source_addr, dst_port, site, recorder);
```

The era-scoped accumulator, as a table. It exists for three reasons, each of
which is a defect in its absence: the mapping is **per era**, so any join to it
must be era-qualified; `declared_count` against the count of distinct
`instrument_id` values actually observed is the only statement of published-set
coverage the archive can make; and without it the `symbol` column on `event`
would be a lie across an era boundary.

**It carries the full identity block, including the channel instance**, for a
reason that is easy to skip: an `era_anchor_ts` is only meaningful *for one
channel instance*, because a `Reset Count` is that instance's. Two paths
publishing one `Channel ID` open their eras independently, so a table keyed
without `source_addr` and `dst_port` merges two eras that are not the same era
and produces a join that silently picks one path's exponents for the other
path's prices. `port_role` is on it because reference data arrives on the
`refdata` role and a reader joining from a `mktdata` event must be able to see
that the roles differ rather than discover it.

### 3. `book_top` — one row per change in top of book

The derived table, and the one most questions are actually asked against.

```sql
CREATE TABLE IF NOT EXISTS recorder.book_top (
    recv_ts           DateTime64(9),
    send_ts           DateTime64(9),
    site              LowCardinality(String),
    recorder          LowCardinality(String),
    env               LowCardinality(String),
    feed              LowCardinality(String),
    observation       LowCardinality(String),   -- see The race is a query
    source_addr       IPv4,
    channel_id        UInt8,
    dst_port          UInt16,
    source_id         UInt16,
    instrument_id     UInt32,
    symbol            LowCardinality(String),
    sequence_number   UInt64,
    message_index     UInt8,
    reset_count       UInt8,
    era_anchor_ts     DateTime64(9),
    bid_px_raw        Nullable(Int64),
    bid_qty_raw       Nullable(UInt64),
    bid_source_count  Nullable(UInt16),
    ask_px_raw        Nullable(Int64),
    ask_qty_raw       Nullable(UInt64),
    ask_source_count  Nullable(UInt16),
    price_exp         Int8,
    qty_exp           Int8,
    state_key         UInt64,                   -- the equivalence key
    book_certain      UInt8,                    -- 0 once the book is unknowable
    uncertain_since   Nullable(UInt64),         -- the sequence number that made it so
    uncertain_reason  LowCardinality(String),   -- gap | instrument_reset | no_anchor
    object_key        String
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (channel_id, instrument_id, era_anchor_ts, recv_ts, sequence_number,
          message_index, observation);
```

One row per *change*, where a change is a change in **either** the visible top
**or** the certainty of it. A message that moves neither produces no row: a feed
whose depth updates rarely reach the top would otherwise pay per-event volume for
a per-change question.

**Certainty is part of the state, not an annotation on it.** Emitting rows only
when prices or quantities move loses exactly the transition that matters most: a
gap or an `InstrumentReset` arrives, no later message happens to move the top,
and every `ASOF` lookup from then on keeps returning the last row — which says
`book_certain = 1` and is now false. So a transition of `book_certain` emits a
row on its own, carrying the same top as the row before it and a different
verdict on whether that top can be believed.

**`message_index` is in the row and in the sort key** because a publisher may
pack several top-changing messages for one instrument into one datagram. They
share `recv_ts`, `sequence_number` and `observation`, so without the index
`ReplacingMergeTree` collapses a run of genuine changes into whichever one merged
last.

### There is no fourth table

A window in which the book is unknown is not a separate fact. It is
`book_certain = 0` on the rows that follow, and the gap that caused it already
has a row in `sequence_gap` with a verdict. A second table would be a second
place for one interval to disagree with itself.

---

## Symbol is not a key

The glossary is explicit that `Symbol` is display and filtering only. Per-venue
tables key on it anyway — ordering by symbol, joining on symbol, comparing two
observation points by symbol. That works until it does not, and it fails in three
ways that all present as data quality problems rather than as a key mistake:

- **A symbol is unique within a channel at an instant, not across eras.** An
  operator retiring an instrument and later publishing another under the same
  human-readable name produces two instruments that a symbol join silently
  merges. `Instrument ID` does not.
- **A symbol is `char[64]` of venue-chosen text.** Trailing padding, case, and
  any upstream renaming all change it while no market data changes.
- **Two channels may legitimately carry the same symbol**, and a symbol join
  across them is a cross join.

So every table above keys on `(channel_id, instrument_id)` within an era and
carries `symbol` as a column resolved from `recorder.instrument` **at that era**,
for display and for the `WHERE` clause a human types. Nothing joins on it.

This is the one place where these tables are deliberately not column-compatible
with what a per-venue capture writes today. **A migration off one is a re-key,
not a rename**, and any plan that treats it as a rename will produce two rows
where there is one instrument.

---

## The equivalence key

Two observation points saw the same top of book. Establishing *that* is the whole
of a race measurement, and it is where an implementation usually picks something
convenient and wrong.

`state_key` is a 64-bit hash over exactly this tuple:

```
(channel_id, instrument_id,
 bid_px_raw, bid_qty_raw, bid_source_count,
 ask_px_raw, ask_qty_raw, ask_source_count)
```

with an absent side encoded as a distinguished value rather than as zero — an
empty side and a zero-priced side are different books — and with `price_exp` and
`qty_exp` **asserted equal** within the era rather than hashed, which is what the
era means.

Three things are deliberately absent, and each has been a real mistake:

- **No timestamp.** A timestamp is the quantity being measured; a key containing
  one measures whether two observation points agree about time, which they do
  not. A key containing a *venue-supplied* timestamp is worse: its resolution and
  its meaning differ between transports, so one state carried over two transports
  hashes two ways and no pair is ever found. The key must be a function of the
  state, and time is not part of the state.
- **No sequence number, no `Reset Count`, no datagram index.** These belong to
  the publisher, and two observation points on two transports do not share them.
  Between two recorders of one multicast feed they are shared only because the
  path is shared — which is the assumption a race exists to test, not to depend
  on.
- **No bytes.** Hashing the payload makes the key a function of the schema
  version, the batching and any padding, so a publisher upgrade repartitions the
  key space and the race silently reports nothing.

**The key is not unique, and must not be.** A book returning to a previous state
produces the same `state_key` again, which is correct. It also means a join on
the key alone produces a cross product on any instrument that oscillates between
two states, which is most of them.

**`ASOF JOIN` does not fix that, and it is worth being precise about why**, since
reaching for it is the obvious move. `ASOF` selects the nearest right-hand row
*independently for each left-hand row*: it has no notion of consuming a match, so
when a state repeats quickly, several occurrences at one observation point all
pair with the same occurrence at the other. The lead times that come out are not
wrong in a way anyone notices — they are plausible, biased, and derived from
counting one arrival several times.

**Number the occurrences instead.** Within one `(observation, channel_id,
instrument_id, era, state_key)`, assign each row its ordinal by `recv_ts`; pair
ordinal *n* at one observation point with ordinal *n* at the other. That is
one-to-one by construction, it needs no window function beyond `row_number`, and
it fails visibly rather than quietly: an occurrence with no counterpart at the
same ordinal is an unpaired row, which is a fact worth seeing — it usually means
one observation point missed a state the other saw. A bounded `|Δt|` then
discards pairs whose ordinals happen to align across an outage.

---

## The book, and the honest part

`Quote` carries both sides and needs no state. `LevelUpdate` is a delta and needs
a book. So `book_top` has two derivations, not one, and only the second needs an
anchor at all.

**A `Quote` is self-anchoring.** It states a complete two-sided top, so it
establishes a certain top by itself, with no prior state and no snapshot — and
after a gap it *restores* certainty the moment the next one arrives, because
nothing about a missed `Quote` makes the next one less true. A quote-only feed
therefore produces `book_top` rows from its first message, and a rule requiring a
snapshot cycle would have produced none at all for it. Snapshot anchoring belongs
to stateful depth reconstruction and nowhere else.

**For a book built from deltas there is exactly one anchor: a complete snapshot
cycle.** `SnapshotBegin` states `anchor_seq` — the channel sequence number the
book state is true as of — along with `total_levels` and a `snapshot_id`; the
`SnapshotLevel` messages follow; `SnapshotEnd` repeats both so that a reader who
lost either end knows it. A cycle with fewer levels than `total_levels`, or with
no `SnapshotEnd`, is incomplete and must not be applied.

Two messages that look like anchors and are not:

- **`BookClear` is a delta.** It asserts that named levels are gone —
  `clear_side` with a scope of an entire side or from a price — and a subscriber
  applying it stays ready. It does not say the book is empty and it is not a
  starting point.
- **`InstrumentReset` is the opposite of an anchor.** It is the message a
  publisher owes when it has lost confidence in its own book for one instrument.
  It *destroys* certainty rather than establishing it: after it the book is
  unknown, and `uncertain_reason` records it as `instrument_reset`.

  It also carries the terms of its own recovery, which is why `new_anchor_seq` is
  mapped rather than dropped: the cycle that re-establishes the book is one whose
  `anchor_seq` is at or after it. A deriver that ignores that field will accept a
  snapshot already in flight when the reset was published — a book state the
  publisher had already disowned — and will rebuild from it with
  `book_certain = 1`. That is the worst outcome available here, because it is
  confidently wrong rather than honestly unknown.

**Before the first anchor, a delta-built book emits one row and then nothing.**
The obvious rule — emit nothing until anchored — is the wrong contract, because
absence is indistinguishable from a feed that was silent, and an `ASOF` lookup
into a pre-anchor window returns whatever preceded it, which may be another era.
So the first `LevelUpdate` for an instrument in an unanchored window emits a
single row with `book_certain = 0` and `uncertain_reason = 'no_anchor'`, no
prices, and nothing further is emitted until a cycle completes. A consumer can
then distinguish "not yet knowable" from "no data" by looking, which is the whole
job of the column.

**A live book cannot say that it does not know.** A capture that missed datagrams
applies the deltas that did arrive and keeps quoting a top of book that has
silently diverged from the publisher's. It cannot notice, because the thing it
would need in order to notice is the datagram it did not receive. The derived
book has `sequence_gap` in the same database: a gap in the channel instance's
sequence space between two `LevelUpdate` messages means every later top of book
is unknowable until the next anchor, and `book_certain` says so with
`uncertain_since` naming the sequence number.

That one column is the strongest argument in this document. It turns "our book
might be wrong" from an unfalsifiable worry into a `WHERE` clause.

**A snapshot anchors a book and never times one.** The runtime pulls it on its
own cadence from the adapter's book, and the archive records when it was
published rather than when it was asked for. So a snapshot is a starting state
and is never an observation in a race — `book_top` rows derived from applying a
snapshot carry the anchor's sequence number and are excluded from pairing.

**`Per-Instrument Seq` is the depth join key and it is deterministic**, stamped by
the runtime from a counter keyed on the instrument and reset with the era. It is
on `event` for that reason: it is the one sequencing value that survives a
comparison against anything derived from the same upstream event.

---

## The race is a query, not a table

The predecessor established that a cross-site loss comparison needs no new table,
because both sites' rows land in one table and the comparison is a query. The
same holds here, and for a stronger reason: a race between two transports and a
race between two sites are one question asked of different values in one column.

`book_top.observation` names **where this view of the book came from**, as `site`
names a recorder. Two recorders of one multicast feed are two observations. A
multicast feed and some other transport carrying the same instruments are two
observations. Nothing in the schema knows which is which, and nothing should: a
race is one `state_key` seen at more than one `observation`, paired `ASOF` within
a bounded window, with the lead time the difference of `recv_ts`.

Ship it as a view over `book_top`. **Materialize it only when measured to need
it**, and state the measurement in advance: a materialized view is warranted when
the `ASOF` pairing over one day of `book_top` exceeds the query budget of the
thing that asks it, and not before. A materialized view over a table whose rows
arrive out of order — which these do, because observation points load
independently — is a correctness problem before it is a performance solution.

---

## What makes this generic

Nothing above is venue-specific, and that is a property of where the boundary was
already drawn rather than an achievement of this document.

- **The codec decides the layout.** A feed declares its `Magic` and its port
  roles — the same declaration the recorder already takes in order to record it —
  and the codec crates decode it. The deriver never sees venue-specific bytes.
- **The adapter interface decides what an event means upstream**, and the
  publisher's lowering has already turned it into the family's messages. By the
  time a message is in the archive it is a `Quote`, a `Trade`, a `LevelUpdate`, a
  `BookClear`, an `InstrumentReset` or part of a snapshot cycle, and the row is a
  function of that.
- **Reference data is on the wire**, so instrument identity, symbols and
  exponents need no per-venue configuration.
- **No product line is named in the schema.** `feed` and `observation` are
  declared strings, opaque to every query above.

**A new feed in the price-level message set becomes rows by being recorded.** For
that feed the list of things someone must write is empty: no adapter code, no
schema change, no migration, no configuration beyond the declaration that already
makes it recordable.

**That boundary is the message set, and it is worth stating rather than
overselling.** `event` has a closed set of columns covering eight message types,
and `WireCapture` classifies anything else as an unknown type. A feed carrying
order-level messages — an add, a cancel, an execution — is not
configuration-only: it needs enum variants, a mapping, columns and a migration,
which is the same work this document does for price levels and is why *No
order-level reconstruction* is a non-goal below rather than a footnote.

The extension mechanism is therefore ordinary and is named here so nobody expects
a different one: **a new message set is a new migration and a widened mapping
table**, done once for the family rather than once per venue. What is generic is
that the second and third feed *of a given set* cost nothing — not that every
conceivable feed does.

---

## Cost, stated before it is discovered

`event` is not `datagram` with more columns. It is `datagram` multiplied by the
number of messages a datagram carries — a publisher's batching decision, neither
small nor constant. A feed that batches an update burst into one datagram
produces one transport row and hundreds of market data rows, and the burst is
exactly when someone wants the rows.

Three consequences, all of which belong in the plan rather than in a later
incident:

- **The multiplier is measured per feed before that feed's derivation is
  enabled**, from the archive itself: messages walked over datagrams walked, over
  a window that includes a burst. It is a property of the feed and its publisher,
  not of the recorder, so it cannot be assumed from another feed.
- **The retention split from the predecessor applies one table further down.**
  `event` is the expensive table and expires on a short whole number of days;
  `book_top` is per change and is worth keeping far longer; `instrument` is tens
  of bytes per instrument per era and has no TTL. As before, a TTL that is not in
  a migration file is a TTL nobody can find.
- **Derivation is per feed, and off by default.** The datagram loader is a replay
  plus a header read — the cheap half. The event deriver is a full message walk
  plus a book. The configuration must make enabling it a per-feed decision.

---

## What this needs that does not exist yet

- **An event deriver.** A crate that walks an archive object with the existing
  `WireCapture`, joins each message to `ArchivedRefdata`, and emits `event` and
  `instrument` rows. The walk, the reference data and the row-sink machinery
  exist; the join and the row shapes do not.
- **Decode coverage for four more message types.** `MessageBody` has four
  variants; the book needs `InstrumentReset`, `SnapshotBegin`, `SnapshotLevel`
  and `SnapshotEnd`, which `WireCapture` counts as skipped today. The codec
  already decodes them — this is a widening of the walk's output, not new
  parsing.
- **A wider `WireProvenance`.** The source address, the destination port and the
  receive-timestamp kind are on `RecordedDatagram` and are dropped by the walk,
  and all three are in the identity block. Nothing can be written until they
  survive it.
- **An era-scoped reference-data accumulator**, keyed `(channel_id,
  instrument_id)`, restating exponents forward from the definition that changed
  them. `ArchivedRefdata` keys by symbol and pins the first statement, which is
  right for a re-lowering and wrong here — see *Why this is a derivation*.
- **A book with an unknown state.** Two derivations: `Quote` self-anchoring, and
  a delta book anchored only on a complete snapshot cycle, fed by `LevelUpdate`
  and `BookClear`, and driven to `book_certain = 0` by a gap in the channel
  instance's sequence space or by an `InstrumentReset`. A certainty transition
  emits its own row.
- **`state_key`, and a test that it is stable.** Specifically: unchanged across a
  schema version bump, across a change in batching, and across two observation
  points that decode the same state — the three ways a race key fails.
- **The occurrence-ordinal pairing**, as a view, with the unpaired rows visible
  rather than dropped.
- **Migration `005`**, with the three tables and their TTLs, applied the way the
  existing five are: checked in, embedded, and applied by the deploy rather than
  by a process at startup.
- **A per-feed derivation switch** in the loader's configuration, and a lag
  metric for the derivation distinct from the lag of the datagram load.
- **The messages-per-datagram measurement**, per feed, before any of it is turned
  on.

---

## Non-goals

**No venue clients, credentials, or subscription of any kind.** Every row here
comes from an archive this fleet already wrote. A second transport becomes a
second `observation` by being recorded, not by this tier connecting to anything.

**No repoint or widening of the transport rows.** `datagram`, `era`,
`segment_coverage`, `sequence_gap` and `conformance_finding` are keyed for the
channel instance and stay as they are. These tables sit beside them and join on
the identity block.

**No change to the record path.** The recorder still decodes nothing while
recording. All of this happens in a separate process reading completed objects
read-only — the property that lets it be turned off, run late, or re-run over the
same objects without touching a live capture.

**No decimal prices, no cross-feed unit conversion, and no currency.** Raw
integers with their exponents. A tier that converts is a tier that has to be right
about every venue's conventions forever.

**No conformance rules over decoded messages.** The rule set is proposed upstream
and its verdicts already have a table. If a market data row makes a new rule
expressible, that rule is a change to `edge-feed-spec`, not a query here.

**No order-level reconstruction.** This is a price-level book. Order-level state
is a different family member with a different message set, and deriving it is a
different document.
