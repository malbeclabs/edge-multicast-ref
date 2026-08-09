# Dual-version refdata: decoding InstrumentDefinition v1 and v3

**Status:** implemented
**Upstream:** [`edge-feed-spec`](https://github.com/malbeclabs/edge-feed-spec) tags `<feed>/v3.0.0`
**Applies to:** `topofbook-parser`, `marketbyorder-parser`, `marketbyprice-parser`, their bots, and the demo ClickHouse schema

## The change upstream

The feed specs bumped to `3.0.0`. Across all five tagged feeds the wire change is
confined to one message: `InstrumentDefinition` grows from 80 to 130 bytes.
`Schema Version` in the frame header goes `1` to `3`.

Two independent changes stack inside that message:

- `Source ID` (`u16`) is inserted immediately after `Instrument ID`, matching the
  field ordering already used by `Quote` and `Trade`. Everything after it shifts
  by 2 bytes.
- `Symbol` widens from `char[16]` to `char[64]`. Everything after it shifts by a
  further 48 bytes.

No other message type changed size or layout in any feed. The Midpoint feed is
excluded upstream and keeps its independent 64-byte definition at schema `1`;
this repo has no Midpoint parser, so that exclusion costs us nothing.

**There is no version 2 on the wire.** A 128-byte `InstrumentDefinition` carrying
the widened `Symbol` without `Source ID` was specified and then superseded before
any publisher emitted it. It is not a layout this repo needs to decode, and the
accepted-version check below is deliberately built so it cannot be mistaken for
one.

Body-relative offsets, as the parsers see them after the 4-byte message header:

| Field | v1 | v3 |
|---|---|---|
| Instrument ID | `0:4` | `0:4` |
| Source ID | — | `4:6` |
| Symbol | `4:20` | `6:70` |
| Leg1 | `20:28` | `70:78` |
| Leg2 | `28:36` | `78:86` |
| Asset Class | `36` | `86` |
| Price Exponent | `37` | `87` |
| Qty Exponent | `38` | `88` |
| Market Model | `39` | `89` |
| Tick Size | `40:48` | `90:98` |
| Lot Size | `48:56` | `98:106` |
| Contract Value | `56:64` | `106:114` |
| Expiry | `64:72` | `114:122` |
| Settle Type | `72` | `122` |
| Price Bound | `73` | `123` |
| Manifest Seq | `74:76` | `124:126` |
| **Body length** | **76** | **126** |

The motivating case for the symbol widening: this feed's symbols are Kalshi
tickers. At 16 bytes the publisher was emitting only the last 16 characters, so
`KXNFLGAME-26SEP13NYJTEN-NYJ` arrived as `6SEP13NYJTEN-NYJ`, the market-family
prefix silently gone, and two families on the same game truncating toward
colliding tails. 64 bytes carries the whole ticker.

`Source ID` closes a different gap. Every `Quote` and `Trade` already names its
originating venue, but the instrument dimension those events join against did
not, so an instrument could not be attributed to a venue without going back to
the event stream.

## Decisions

### Version is read per frame, and both are accepted

The frame header already carries `Schema Version`; each parser currently rejects
anything but `1`. That widens to accept `1` and `3`, and the value is passed down
to `InstrumentDefinition` decoding.

Per frame rather than locking to the first version seen, because a publisher can
then cut over mid-run without restarting every parser, and a capture spanning the
cutover replays correctly. The cost is one branch on a cold path; refdata is the
lowest-rate port.

### The accepted set is `{1, 3}`, not a range

This is the one structural difference from a plain renumbering, and it is worth
stating explicitly because the natural implementation is wrong.

`topofbook-parser` validates its version with a ceiling: `SchemaVersion == 0 ||
SchemaVersion > maxSchemaVersion`. Raising that ceiling to `3` would admit
version 2 frames into the decoder, where they would fail later on a length
mismatch, in a different error bucket, for a reason that reads as corruption
rather than as a version that does not exist. The ceiling check becomes explicit
set membership.

`marketbyorder-parser` and `marketbyprice-parser` previously rejected on a
single equality check against version 1 (`SchemaVersion != 1`). For them the
fix is to widen that into an explicit membership test, `SchemaVersion != v1 &&
SchemaVersion != v3`, rather than raising a ceiling.

Version 2 therefore lands in the same bucket as version 0 and version 4: an
unsupported version, rejected at the header, counted, channel kept.

### Message length is the cross-check

A v1 frame must declare an 80-byte `InstrumentDefinition` (76-byte body); a v3
frame must declare 130 (126-byte body). A frame whose header version and message
length disagree is malformed: count it, skip the message, keep the channel, the
same handling every other malformed message already gets.

This is what catches a publisher that bumps the header but not the payload, or
the reverse. Without it, a v3-declared frame carrying a v1 body would decode
`Source ID` and `Symbol` across 66 bytes of adjacent fields and produce
plausible-looking garbage rather than an error.

### Source ID is plumbed through to ClickHouse

Unlike the symbol widening, which was invisible below the parser because `Symbol`
was already a Go `string` end to end, `Source ID` is new data. It is decoded,
carried on the record, written by each bot, and stored.

Nothing here needs an envelope schema change. The parsers already emit a
free-form `Fields` map on the `instrument_definition` record, so `source_id`
joins `leg1`, `manifest_seq`, and the rest without touching the record type.

1. **Parsers.** `SourceID uint16` on `InstrumentDefinitionBody` (marketbyorder,
   marketbyprice) and `topOfBookInstrumentDef` (topofbook); `"source_id"` added
   to the `Fields` map. At v1 the field is absent from the wire and the value is
   `0`, which is the Source ID Registry's Unknown.
2. **Bots.** One line each: `getUint16(rec.Fields, "source_id")` in the
   marketbyorder and marketbyprice `events_writer.go` instrument branch, and
   `uintOrZero(rec, "source_id")` in `topofbook-bot`'s `EnqueueInstrument`.
   Not `intOrZero`, which sits beside it and returns `int64` for the signed
   exponents; an unsigned venue ID must not be able to arrive sign-extended.

   Records cross the parser/bot boundary as JSON, so every numeric field
   arrives at a bot as `float64`, never as the `uint16` the parser wrote. Both
   accessors already handle that (`toUint16` switches on `float64`;
   `uintOrZero` reads through `floatField`), which is why no new accessor is
   needed on either side. A type assertion to `uint16` in a bot would compile,
   pass a hand-built unit test, and return `0` for every instrument in
   production.
3. **ClickHouse.** `source_id UInt16 DEFAULT 0` on `topofbook.instruments`,
   `marketbyorder.instruments`, and `marketbyprice.instruments`. `DEFAULT 0`
   matches what the marketbyorder and marketbyprice `events` tables already do
   for their own `source_id` column.

The `ORDER BY` keys are untouched, so unlike the symbol change there is no data
split at cutover. These are `ReplacingMergeTree` tables keyed on
`(channel_id, instrument_id)`, so a v1 row carrying `source_id = 0` is replaced
by the v3 row carrying the real venue on the next merge. The dimension
self-heals.

An earlier version of this design deferred all downstream plumbing on the
grounds that a ClickHouse migration is not worth forensic value nobody asked
for. That reasoning held for a schema-version field, which is observability.
It does not hold for `Source ID`, which is instrument metadata with a registry
behind it and an existing `source_id` column on the sibling event tables to
join against.

### The branch lives in each parser, not in a shared package

Each parser is its own Go module and already owns a full copy of its wire
decoder; that duplication is existing structure. The change is roughly 25 lines
in each.

Extracting a shared `instrumentdef` package was considered and rejected for now.
It would add an `internal` module dependency to three more modules, each needing
a `require`, a `replace`, and a `COPY go/internal/` in its Dockerfile, the exact
step that silently broke the container build during the persistence work. That
is a poor trade for 25 lines, especially while v1's lifetime is undecided.
Revisit if v1 outlives expectations or a fourth parser appears.

### The decoded version is exposed as a metric label, not in the record

What ships is a `frames_total{port,schema_version}` counter in each parser,
incremented once per successfully parsed frame. `records_total` is untouched and
carries no version label.

A label on `records_total` was the original plan, but it does not fit: that
counter is incremented from a decoded *record*, and a record deliberately
carries no version, so there is nothing to key the label off at that point.
Worse, the three parsers' `ParseFrame`/`Parse` return different shapes (defect
structs, record slices) with no uniform place to thread a version value through
to per-record instrumentation. A frame-level counter sidesteps both problems:
the runner already reads the frame header once per datagram, so it can read the
Schema Version byte and label a purpose-built counter without touching record
decoding at all.

The version is read from byte 2 of the frame, the frame header's Schema Version
field, directly in each parser's `runner.go` after `Parse`/`ParseFrame`
succeeds, rather than threaded through the parser's return value. This is
deliberate, for the same reason as above: it is observability, not data the
parser needs, and reading it independently means all three runners can do it
identically despite returning different shapes.

To watch a cutover: `frames_total{schema_version="3"}` climbing while
`frames_total{schema_version="1"}` goes flat, then to zero, is when the v1
decode path can be retired. Do not look for this on `records_total`; it was
never built there, for the reasons above.

`frames_total{schema_version="2"}` should never be observed. The counter is
incremented only after a frame parses successfully, and version 2 is rejected at
the header, so a nonzero count there means a publisher is emitting a version this
repo believes does not exist. That is worth alerting on.

## Per-parser notes

`marketbyorder-parser` and `marketbyprice-parser` decode this message
byte-identically today, with explicit offsets and a `len(buf) != 76` guard. Their
changes are the same edit twice.

`topofbook-parser` uses a sequential byte reader in `tob/topofbook_wire.go`
(`br.bytes(16)`) rather than explicit offsets, so its change is the read width,
the inserted `Source ID` read, and the version branch. Its symbol trimming
happens a layer later, in `topofbook.go` via `trimNull`, where the other two use
`fixedString` at decode time. Both are correct; no change needed. It is also the
only parser whose header validation needs restructuring rather than
renumbering, per the accepted-set decision above.

## Downstream exposure

Beyond the `source_id` column, this is an audit rather than a change.

Symbol is a Go `string` end to end, JSON-encoded, landing in a ClickHouse
`LowCardinality(String)`. Nothing in the parsers or bots hardcodes 16 bytes.

The real exposure is Prometheus cardinality, and it is a widening rather than a
multiplication: the per-symbol book gauges already produce roughly 27,000 series
against ~8,000 instruments on the market-by-price feed, and longer symbol values
make each series heavier without adding any. `LowCardinality` absorbs the
ClickHouse side. Confirm the dashboards render a 64-character symbol without
breaking their table and legend layouts.

## Testing

- Golden byte fixtures for both layouts in each parser, asserting every field
  lands at the right offset. The v3 fixture must use a symbol longer than 16
  bytes **and** a nonzero `Source ID`. Without both, it does not distinguish v3
  from either v1 or the v2 layout that never shipped.
- The length cross-check in both directions: version 3 with a 76-byte body, and
  version 1 with a 126-byte body. Both must be counted malformed and skipped
  without dropping the channel.
- Schema version `2` is rejected. This is now the interesting negative case
  rather than an arbitrary unknown version, because a ceiling check would let it
  through.
- Schema versions `0` and `4` are also rejected, covering below and above the
  accepted set.
- A capture or synthetic stream that switches version mid-run (v1 to v3 and back),
  asserting the parser follows it without restarting.
- `source_id` reaches the record's `Fields` map as `0` at v1 and as the decoded
  value at v3, and each bot writes it.
- End to end against the live feed once a publisher is emitting v3, confirming
  full-length symbols and real source IDs reach ClickHouse.

## Open items

- No publisher is known to be emitting v3 yet. Until one is, the v3 path is
  covered by fixtures only, and the end-to-end confirmation above stays
  outstanding.
- `order-intent` and `perp-stats` have v3 specs but no parser in this repo, so
  they are out of scope.
- Source ID Registry membership is not validated. A definition carrying an
  unregistered venue is stored as-is. Upstream made the same call for the
  conformance tool, deliberately keeping registry validation separate from the
  layout migration.
- Whether v1 support is permanent is undecided. The v1 path is kept as one
  clearly marked branch per parser so it can be deleted in a single commit.
