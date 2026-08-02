# Market-by-Price (L2) feed: parser, bot, and demo stack — design

Spec: [market-by-price/spec.md](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-price/spec.md)
(Schema Version 1, magic `0x4442`)

Related prior work: `2026-04-23-marketbyorder-design.md` (the sibling feed this
mirrors), `2026-06-06-marketbyorder-bot-snapshot-resilience-design.md` (the
shadow-commit snapshot model this adopts and the field evidence behind it).

## Problem

DoubleZero publishes a new price-aggregated (L2) market-data feed. The repo has
reference consumers for Top-of-Book and Market-by-Order but nothing that decodes
market-by-price, so there is no reference implementation to point subscribers at
and no way to observe the feed in the demo stack.

Market-by-price is structurally close to market-by-order — same three-port
channel model, same 24-byte frame header, four byte-identical message payloads —
but the addressing model differs in a way that changes the consumer materially.
A level is keyed by `(Side, Price)`, quantities are absolute rather than
incremental, and the feed carries three obligations the sibling has no concept
of: a declared depth bound, a crossed-book defect counter, and a bounded delta
buffer with a stated overflow policy.

## Scope

Full parity with what market-by-order has today:

- `go/marketbyprice-parser` — multicast subscriber, stateless wire decode,
  JSONL on a Unix socket or file, Prometheus metrics.
- `go/marketbyprice-bot` — book state machine, ClickHouse persistence.
- `demo/clickhouse/init/03_schema_mbp.sql` — `marketbyprice` database.
- Demo stack wiring: compose services, `.env.example` keys, Prometheus scrape
  jobs, a Grafana dashboard.
- Docs: root `README.md` feed table, `demo/README.md`, and the port table in
  `docs/hyperliquid.md`.

### Non-goals

- No refactor of the shared-by-copy parser scaffolding. `runner.go`,
  `sink*.go`, `seqtracker`, `timestamp_*.go`, and `metrics.go` are duplicated
  near-verbatim between the two existing parsers; market-by-price becomes the
  third copy. Each parser stays a standalone reference implementation, which is
  the point of this repo, and the two shipping feeds are not touched.
- No publisher. Validation is against the live feed plus byte-exact unit tests.
- No positional-index addressing. The spec reserves `0x50`–`0x5F` for a future
  mode and defines nothing there; unknown types are skipped by length.

## Architecture

```
multicast (3 ports)  →  marketbyprice-parser  →  unix socket (JSONL)  →  marketbyprice-bot  →  ClickHouse  →  Grafana
                        stateless frame decode                           book state machine     marketbyprice db
```

The parser is stateless: decode a frame, emit one `Record` per application
message, count per-port frame-sequence gaps, write to the sink. All book state
lives in the bot. This is the existing split for both shipping feeds and it does
not change.

---

## Component 1: `go/marketbyprice-parser`

Package `main`, files mirroring `go/marketbyorder-parser`:

| File | Contents |
|------|----------|
| `marketbyprice_wire.go` | Frame header, message header, one body struct + parse func per type |
| `marketbyprice.go` | Parser registration, `ParseFrame`, `decodeMessage`, enum stringers |
| `parser.go` | `Record`, `Parser` interface, registry (copy) |
| `sink.go`, `sink_json.go`, `sink_socket.go` | Output sinks (copy) |
| `runner.go` | Per-port receive loop, `seqTracker`, latency observation (copy) |
| `timestamp_linux.go`, `timestamp_other.go` | `SO_TIMESTAMPNS` support (copy) |
| `metrics.go` | Prometheus metrics, `dz_mbp_parser_*` namespace |
| `main.go` | Flags: `--group`, `--refdata-port`, `--mktdata-port`, `--snapshot-port`, `--interface`, `--output`, `--format`, `--metrics-addr` |
| `Dockerfile`, `README.md`, `.gitignore`, `go.mod` | Plus a `go/go.work` entry |

### Wire layout

Magic `0x4442`, Schema Version 1, 24-byte frame header and 4-byte application
message header identical to the sibling feeds. Maximum frame 1,232 bytes.

Thirteen message types. Body sizes below are the message size minus the 4-byte
header, since that is what the parse functions receive:

| Type | Name | Msg | Body | Port | Provenance |
|------|------|-----|------|------|------------|
| `0x01` | Heartbeat | 16 | 12 | mktdata | identical to both siblings |
| `0x02` | InstrumentDefinition | 80 | 76 | refdata | identical to market-by-order |
| `0x04` | Trade | 52 | 48 | mktdata | identical to both siblings |
| `0x06` | EndOfSession | 12 | 8 | mktdata | identical to both siblings |
| `0x07` | ManifestSummary | 24 | 20 | refdata | identical to both siblings |
| `0x08` | Liquidation | 48 | 44 | mktdata | identical to top-of-book on the wire, but **neither existing parser decodes it — new code here** |
| `0x13` | BatchBoundary | 16 | 12 | mktdata | identical to market-by-order |
| `0x14` | InstrumentReset | 28 | 24 | mktdata | identical to market-by-order |
| `0x20` | SnapshotBegin | 40 | 36 | snapshot | market-by-order's 32-byte body + `Depth Bound` at body offset 32 |
| `0x22` | SnapshotEnd | 20 | 16 | snapshot | identical to market-by-order |
| `0x40` | LevelUpdate | 48 | 44 | mktdata | **new** |
| `0x41` | BookClear | 36 | 32 | mktdata | **new** |
| `0x42` | SnapshotLevel | 32 | 28 | snapshot | **new** |

`0x03` and `0x05` are reserved and intentionally unused, so a misrouted
top-of-book or midpoint frame cannot cross-decode. `Magic` is the primary
rejection.

The three new bodies, at body-relative offsets:

**`0x40 LevelUpdate`** (44 bytes) — Instrument ID `u32` @0, Source ID `u16` @4,
Side `u8` @6, Action `u8` @7, Per-Instrument Seq `u32` @8, Price `i64` @12,
Quantity `u64` @20, Timestamp `ts_ns` @28, Order Count `u16` @36, Level Index
`u16` @38, Update Reason `u8` @40, Level Flags `u8` @41, 2 bytes reserved.

**`0x41 BookClear`** (32 bytes) — Instrument ID `u32` @0, Source ID `u16` @4,
Clear Side `u8` @6, Scope `u8` @7, Per-Instrument Seq `u32` @8, From Price
`i64` @12, Timestamp `ts_ns` @20, Clear Reason `u8` @28, 3 bytes reserved.

**`0x42 SnapshotLevel`** (28 bytes) — Snapshot ID `u32` @0, Price `i64` @4,
Quantity `u64` @12, Order Count `u16` @20, Side `u8` @22, Level Flags `u8` @23,
4 bytes reserved. No Instrument ID; the containing `SnapshotBegin` implies it.

### Strict body lengths

Body length checks are exact equality (`len(buf) != N`), matching the sibling
parsers, not `>=`. The spec's forward-compatibility rule that a decoder should
ignore trailing bytes only applies across a Schema Version bump, and the frame
header rejects unimplemented versions before any body is parsed. Within v1, a
body of unexpected length is malformed.

The one place this could mislead a reader is `0x20 SnapshotBegin`, which the
spec describes as a prefix-superset of market-by-order's shorter layout. That
rule exists so a market-by-order decoder can read a market-by-price frame; it
does not license a market-by-price decoder to accept a 32-byte body. A comment
in the code says so.

### u16 sentinels

`Order Count` and `Level Index` both use `0xFFFF` to mean *not provided, or
beyond what the field can express*. The emitted JSON **omits those keys** rather
than carrying 65535, so nothing downstream can read a sentinel as a magnitude.
`Order Count = 0` is a real value on a `LevelUpdate` and is emitted as `0`.

### Enum stringers

`side` (bid/ask), `clear_side` (bid/ask/both), `action`
(unknown/new/change/delete/other), `update_reason`
(unknown/trade/cancel/new_order/amend/venue_action/other), `clear_reason`
(unspecified/halt/session_end/venue_reset/settled/other), `reset_reason`
(unspecified/publisher_inconsistency/venue_resync/upstream_gap/other),
`aggressor_side` (buy/sell/unknown). Unknown values render as `"unknown"`; the
spec requires receivers to accept any `u8` and permits new values without a
version bump, so an unrecognized value is never an error.

### Metrics

The market-by-order set (`ingress_packets`, `ingress_bytes`, `frame_seq_gaps`,
`frames_missing`, `parse_errors`, `records_total`, `send_latency`,
`source_latency`, `sink_write_errors`, socket client gauges), plus two the spec
motivates directly:

- `dz_mbp_parser_snapshot_flag_mismatch_total{port}` — application-header
  `Flags` bit 0 disagreeing with the port the message arrived on. The spec makes
  bit 0 normative for the first time on this feed specifically so it is
  verifiable from a capture, and asks subscribers to count disagreement as a
  publisher defect. Routing still uses Type ID and port, never the bit.
- `dz_mbp_parser_malformed_total{reason}` — `reason` in
  `{message_length_underflow, bookclear_scope_side}`. The second is `Scope = 1`
  with `Clear Side = 2`, which the spec declares malformed because one price
  cannot bound both sides, and requires the subscriber to discard and count.

  The first is the `Message Length < 4` case. The feed spec motivates that floor
  by noting a length of `0` advances the walk by zero bytes and spins forever,
  which is true of a walk driven by remaining bytes (`for len(body) > 0`). This
  parser's walk is bounded by the frame header's `Message Count` instead, so the
  floor is not what prevents a hang here — a hang is not reachable. What it
  prevents is a slice-bounds panic on `body[4:mh.Length]` when `Message Length`
  is below the header size. Both are reasons to keep the check; only the second
  describes this implementation. Stating it precisely matters because the wrong
  rationale invites a test that guards nothing: a timeout-based test passes
  whether or not the check exists.

As with the sibling parsers, per-port frame-sequence gap tracking excludes
`refdata`, which is a low-rate periodic-retransmit stream where gaps are not a
loss signal.

---

## Component 2: `go/marketbyprice-bot`

Mirrors the market-by-order bot's structure: socket reader (`bot.go`) →
coordinator (`coordinator.go`, channel-scoped messages, reset fencing, dispatch
by instrument) → per-shard state machine (`shard.go`, `instrument.go`) → events
writer and coalesced snapshot writer → ClickHouse batcher. Shard count defaults
to `GOMAXPROCS-2`, clamped to `[1,8]`.

### Book representation

```go
type LevelState struct {
    QtyRaw     uint64
    OrderCount uint16   // 0xFFFF = unavailable
    Flags      uint8
}

type Instrument struct {
    ID            uint32
    Symbol        string
    PriceExponent int8
    QtyExponent   int8
    Status        InstrumentStatus // awaiting-snapshot | ready | gap
    Bids, Asks    map[int64]*LevelState // keyed by RAW price
    DepthBound    *uint32               // nil = unknown, 0 = complete, N = bounded per side
    LastAppliedMktdataSeq    uint64
    LastAppliedInstrumentSeq uint32
    RequiredAnchorSeq        *uint64
    OpenSnapshot             *PendingSnapshot
    Pending                  map[uint32]Record // reorder window
}
```

The spec's five-state machine collapses to three statuses because two of its
states are represented orthogonally, following the market-by-order bot:
`awaiting-refdata` is absence from the shard's instrument map (deltas for an
unknown instrument buffer until its definition arrives), and
`building-snapshot` is `OpenSnapshot != nil`, which is deliberately independent
of serving status so that building a snapshot never affects whether the current
book is usable.

Rank is derived by sorting keys at read time, as `levels.go` does today. The
aggregation step the market-by-order bot needs disappears, because the wire is
already price-aggregated: a `level_snapshots` row is a direct read of the map.

Prices are held raw and scaled by `PriceExponent` only at persistence time.

### Apply rules

`LevelUpdate` is the spec's two-line rule, and `Action` never gates it:

```
if Quantity == 0:  delete (Side, Price)
else:              set (Side, Price) = {qty, order_count, flags}
```

`BookClear`: `Scope = 0` clears the named side(s) entirely; `Scope = 1` clears
from `From Price` outward — for bids every level at or below it, for asks every
level at or above it. `Scope = 1` with `Clear Side = 2` is malformed, discarded,
and counted. A `BookClear` is not a resynchronization signal: an instrument that
applies one stays `ready`.

Sequencing follows the spec's steady state, per `(channel_id, instrument_id)`:
apply when `Per-Instrument Seq == last_applied + 1`, discard silently when
`<= last_applied`, and on a forward gap buffer within a small reorder window
before declaring `gap`. The reorder window is carried over from the
market-by-order bot, where it was needed because the snapshot stream reorders on
the live path.

### Five behaviors the market-by-order bot does not have

**1. Snapshot-while-ready discriminator.** The market-by-order bot ignores the
snapshot stream entirely while an instrument is `ready` — a deliberate choice
from the June snapshot-resilience work, because on the Tokyo feed mktdata was
effectively lossless while ~3.7% of snapshots arrived short, so processing
periodic re-snapshots was pure self-inflicted churn.

The market-by-price spec does not permit the blanket shortcut. It defines a
discriminator on `Last Instrument Seq` (`K`):

- `K > last_applied_instrument_seq[I]` — the subscriber is genuinely behind; the
  snapshot was captured after deltas it never applied. Re-bootstrap `I`.
- `K <= last_applied_instrument_seq[I]` — the ordinary case; ignore the snapshot.

The spec is explicit that `Anchor Seq` must **not** be used for this comparison:
it is a channel-wide mktdata sequence, so it advances on every other
instrument's deltas and on every heartbeat, which would make "subscriber is
behind" true for nearly every instrument on nearly every cycle and rebuild every
good book every rotation.

This preserves the June result. On a healthy channel `K <= tracker` holds and
the bot ignores snapshots exactly as it does today; it only re-bootstraps on
evidence of having actually missed deltas.

**2. Shadow commit is retained, against the spec's literal wording.** Spec
§Cold Start step 6 says that on a snapshot validation failure the subscriber
discards the partial book and reverts to `awaiting-snapshot`. Applied literally
to an instrument that reached step 6 via the re-bootstrap branch above, that is
precisely the regression the June work removed: one lost `SnapshotLevel` frame
evicts a live, correct book for a full round-robin cycle.

Resolution: snapshots build into a shadow `PendingSnapshot` and commit
atomically only on validation success. On failure the shadow alone is discarded.

- Instrument was `awaiting-snapshot` or `gap` → it stays there. Behavior is
  identical to the spec.
- Instrument was `ready` → it keeps its existing book and its trackers, and
  waits for the next snapshot.

The second case is a deliberate, documented deviation. It is strictly safer than
the literal text: the spec's own §Gap Recovery says an instrument holding bad
state is repaired by the next round-robin snapshot on exactly the schedule it
would have been repaired anyway, so dropping a book that deltas are keeping
correct buys nothing and costs a cycle of availability. Snapshot loss is
amplified by book width — a wide-book snapshot spans many frames, and this feed
has wider books than its sibling — so the failure rate this guards against is
higher here, not lower.

**3. Depth bound.** `DepthBound *uint32`: nil is unknown, 0 is a publisher
claim of completeness, N is bounded at N levels per side. It defaults to
**unknown and never to 0** — a never-snapshotted instrument must not assert
completeness through the subscriber's own initialization. Levels at or beyond
rank N are unknown rather than empty, so the value is persisted with every
`level_snapshots` row and any panel computing cumulative depth is qualified by
it. A bot bound only to mktdata and refdata would never learn a bound; this one
binds all three ports, so it learns one per instrument on that instrument's
first snapshot.

**4. Crossed-book counter.** Compared at consistency points and counted, never
acted on:

```
if bids and asks are both non-empty and best_bid > best_ask: count
```

Strict `>`, so a locked book is not counted as crossed. The consistency point is
the `BatchBoundary` on a channel that emits them, evaluated across the
instruments touched since the previous boundary; on a channel with no boundaries
every applied delta is a consistency point. The bot tracks whether it has ever
seen a `BatchBoundary` on the channel to decide which mode it is in, since the
message is legitimately absent on non-batching channels.

This is observability. It must not change status, discard a book, or trigger a
re-bootstrap. Surfaced as `dz_mbp_bot_crossed_book_events_total` (unlabeled, for
cardinality) plus a gauge of currently-crossed instruments and a `crossed` flag
column on the persisted rows.

**5. Bounded delta buffer with a declared overflow policy.** The spec requires
both, and sizes the cold-start worst case at roughly 1.4 GB for a 60 s cycle —
noting that the cycle-period knob and the subscriber-memory knob are the same
knob. The market-by-order bot caps at 10,000 deltas per instrument and drops the
oldest silently, which loses the tail of a recovery without recording that it
happened.

Here: a per-shard budget by message count, and on overflow the spec's
recommended policy — drop the buffered deltas for the instrument holding the
most buffered data, mark that instrument `gap`, continue, and count the event as
`dz_mbp_bot_delta_buffer_overflow_total`. Sustained overflow means the cycle
period is too long for the memory budget, which is a tuning signal an operator
needs to see.

### Divergence counters

From §Absolute Apply Semantics, counted without altering the applied result, as
`dz_mbp_bot_book_divergence_total{kind}`:

| kind | condition |
|------|-----------|
| `new_on_present` | `Action = New` for a `(Side, Price)` already in the book |
| `change_on_absent` | `Action = Change` for a `(Side, Price)` not in the book |
| `delete_nonzero_qty` | `Action = Delete` carrying non-zero `Quantity` |
| `zero_qty_wrong_action` | `Quantity = 0` with any `Action` other than `Delete` |

Each is a publisher defect or undetected loss. None changes the code path — an
`Action` byte that is wrong must never be able to corrupt a book.

### Reset handling

`InstrumentReset(I, new_anchor_seq=S')`: discard `I`'s levels and any open
snapshot, drop buffered deltas with `mktdata_seq <= S'`, set
`RequiredAnchorSeq = S'`, status `awaiting-snapshot`.

While a required anchor is set, any `SnapshotBegin` for `I` with
`Anchor Seq < S'` is discarded. Without this, a snapshot captured before the
reset but delivered after it — the two travel on different ports, so the skew is
ordinary — passes every other check, replays sequence-continuously, and leaves
the instrument `ready` holding exactly the diverged book the reset existed to
discard, with no gap and no counter.

The required anchor clears when **any** accepted snapshot at or after `S'`
completes, not only one matching `S'` exactly. The publisher must emit one at
`S'`, but that snapshot can itself be lost, and the next round-robin snapshot
carries a newer anchor and is a perfectly good recovery. Clearing only on exact
match would leave the anchor set permanently in that case.

`Reset Count` change on any port wipes all channel state and restarts from cold
start, using the coordinator's existing fence-and-wipe path.

`Manifest Seq` change: instruments no longer in the manifest are discarded, new
ones enter `awaiting-snapshot`, and existing `ready` instruments that remain
keep their state.

---

## Component 3: ClickHouse schema

`demo/clickhouse/init/03_schema_mbp.sql`, database `marketbyprice`. Five tables
paralleling `marketbyorder`, all `MergeTree` partitioned by day with a 30-day
TTL except `instruments`.

**`instruments`** — `ReplacingMergeTree(recv_ts) ORDER BY (channel_id,
instrument_id)`. Same columns as the market-by-order table: symbol, leg1, leg2,
asset_class, market_model, price/qty exponents, tick_size, lot_size,
contract_value, expiry_ts, settle_type, price_bound, manifest_seq.

**`events`** — per-message log, `ORDER BY (symbol, recv_ts, kind)`. Shared
columns (recv_ts, publisher_send_ts, source_ts, recv_ts_kind, materialized
`send_latency_ms` / `source_latency_ms`, channel_id, mktdata_seq, reset_count,
kind, instrument_id, symbol, source_id, per_instrument_seq) plus, by kind:

| kind | columns |
|------|---------|
| `level_update` | side, price, qty, order_count `Nullable(UInt32)`, level_index `Nullable(UInt16)`, action, update_reason, level_flags |
| `book_clear` | clear_side, clear_scope, from_price, clear_reason |
| `trade` | trade_id, aggressor_side, price, qty, cumulative_volume, trade_flags |
| `liquidation` | trade_id, liquidation_flags, method, mark_price, liquidated_user |
| `batch_boundary` | batch_id, batch_ts |
| `instrument_reset` | reset_reason, new_anchor_seq |

`order_count` and `level_index` are nullable because the wire sentinel means
absent, and null is how that is spelled in SQL.

**`level_snapshots`** — coalesced top-N book, `ORDER BY (symbol, recv_ts, side,
level_idx)`. The market-by-order columns (recv_ts, publisher_send_ts,
materialized wire_latency_ms, channel_id, instrument_id, symbol,
last_applied_seq, side, level_idx, price, qty, order_count, cumulative_qty,
stale) plus:

- `crossed UInt8` — the book was crossed at the last consistency point.
- `depth_bound Nullable(UInt32)` — null unknown, 0 complete, N bounded.
  `cumulative_qty` is only exhaustive when this is 0.

**`wire_levels`** — raw `SnapshotLevel` capture for replay, the analogue of
market-by-order's `wire_snapshots`. Group identity denormalized onto every row:
snapshot_id, anchor_seq, total_levels, last_instrument_seq, depth_bound, then
side, price, qty, order_count, level_flags. `ORDER BY (channel_id,
instrument_id, snapshot_id, side, price)`.

**`channel_health`** — heartbeats, manifest summaries, end-of-session. Identical
to the market-by-order table.

---

## Component 4: Demo stack

**`docker-compose.yml`** — two services following the market-by-order pair:

- `marketbyprice-parser`: `network_mode: host` for the multicast join, output to
  `unix:///var/run/dz/mbp.sock` on the shared `dz-sockets` volume, metrics on
  `DZ_MBP_PARSER_METRICS_PORT` (default 9095).
- `marketbyprice-bot`: reads the socket, `--clickhouse-database=marketbyprice`,
  metrics on 9094 exposed as `MBP_BOT_METRICS_PORT`.

Prometheus gains both as scrape targets — the bot by service name, the parser
via `host.docker.internal` because host networking puts it outside the bridge
network — and both are added to its `depends_on`.

**`.env.example`** — `DZ_MBP_MULTICAST_GROUP`, `DZ_MBP_REFDATA_PORT`,
`DZ_MBP_MKTDATA_PORT`, `DZ_MBP_SNAPSHOT_PORT`, `DZ_MBP_SYMBOLS`,
`DZ_MBP_DEPTH` (default 20), `DZ_MBP_COALESCE_MS` (default 50),
`DZ_MBP_PARSER_METRICS_PORT`, `MBP_BOT_METRICS_PORT`.

**`grafana/dashboards/marketbyprice.json`** — mirrors the market-by-order
dashboard's structure: book depth table and heatmap from `level_snapshots`,
spread and mid, level-update rate broken out by `update_reason`, datagram loss
and per-instrument gap panels, send and source latency histograms, plus panels
for the three feed-specific signals — crossed-book events, delta-buffer
overflow, and instruments whose depth bound is non-zero or unknown.

**Docs** — a market-by-price row in the root `README.md` feed table, a demo
walkthrough section in `demo/README.md`, and the live port table in
`docs/hyperliquid.md`.

> **Open item:** the `docs/hyperliquid.md` port table and the `.env.example`
> defaults need the live feed's multicast group, market-by-price port sets, and
> channel ID. Everything else is unblocked; this lands in the final PR.

---

## Error handling

Malformed frames are counted and dropped, never fatal, and never reset a
channel. The frame walk bounds-checks `Message Length` against both the 4-byte
floor and the bytes remaining before using it to advance. Unknown Type IDs are
skipped by length. Parse errors are classified into `bad_magic`,
`schema_version`, `frame_length`, `truncated`, and `other` for the metric label,
as in the sibling parsers.

On the bot side, a ClickHouse outage drops rows with a counter rather than
blocking the book state machine — the existing batcher behavior. Socket
disconnect reconnects with backoff.

## Testing

**Wire tests** build every message byte-by-byte and assert each field lands at
its spec offset, then cover the rejection paths: bad magic, unimplemented schema
version, frame-length mismatch, truncated body, unknown type skipped by length,
and `Message Length = 0` failing without hanging the walk. Sentinel handling
gets explicit cases: `Order Count = 0xFFFF` and `Level Index = 0xFFFF` omitted
from JSON, `Order Count = 0` emitted.

**State machine tests**, each traceable to a spec rule:

- Cold start: buffer deltas, apply snapshot, replay only `mktdata_seq > anchor`.
- Duplicate delta (`<= last_applied`) discarded silently during replay — a
  duplicated frame during bootstrap must not cost a re-bootstrap.
- Forward per-instrument gap beyond the reorder window demotes to `gap`.
- Snapshot-while-ready, both branches: `K > tracker` re-bootstraps,
  `K <= tracker` is ignored.
- Short snapshot for a `ready` instrument leaves the book and trackers intact
  (the shadow-commit deviation).
- `InstrumentReset` followed by a stale `SnapshotBegin` with an older anchor:
  discarded, instrument stays `awaiting-snapshot`.
- Required anchor cleared by a newer snapshot, not only an exact match.
- `BookClear` for every scope/side combination, including the malformed
  `Scope = 1` + `Clear Side = 2`.
- Absolute apply, including `Quantity = 0` deleting a level.
- Crossed-book counted at batch boundaries on a batching channel and per delta
  otherwise; locked book not counted.
- Delta buffer overflow evicts the largest buffer, marks that instrument `gap`,
  and counts.
- `Reset Count` change wipes all state.

## Sequencing

Five stacked PRs. Each is independently reviewable and lands before the next
starts.

| PR | Contents | New non-test lines |
|----|----------|--------------------|
| 1 | `marketbyprice_wire.go`, `marketbyprice.go`, `parser.go`, `go.mod`, `go.work` entry, wire tests | ~450 |
| 2 | Parser binary: `runner.go`, sinks, `timestamp_*`, `metrics.go`, `main.go`, `Dockerfile`, `README.md` | ~500 |
| 3 | Bot book engine: `instrument.go`, `levels.go`, `shard.go`, `coordinator.go`, `bot.go`, `record.go` + tests. No persistence | ~600 (over guideline; flagged) |
| 4 | Bot persistence: `clickhouse.go`, `events_writer.go`, `snapshot_writer.go`, `metrics.go`, `main.go`, `Dockerfile`, `03_schema_mbp.sql` | ~550 (over guideline; flagged) |
| 5 | Demo stack: compose, `.env.example`, Prometheus, Grafana dashboard, READMEs, `docs/hyperliquid.md` ports | ~50 + dashboard JSON |

PRs 3 and 4 exceed the 500-line guideline. Splitting 3 further would separate
the state machine from the `Instrument` type it operates on, and splitting 4
would land writers without the schema they write into; both would produce PRs
that cannot be reviewed or tested on their own.
