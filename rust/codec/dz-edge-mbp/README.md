# dz-edge-mbp

The Market-by-Price feed's wire format: the five messages only this feed has.

`Heartbeat`, `ManifestSummary`, `BatchBoundary` and the rest belong to the
family and live in [`dz-edge-core`](../dz-edge-core); `Trade` and
`InstrumentDefinition` are byte-identical to their siblings' and live in
[`dz-edge-tob`](../dz-edge-tob) and [`dz-edge-refdata`](../dz-edge-refdata).
Duplicating them here would create two definitions of one layout, which is the
drift these crates exist to prevent.

| Message | Type ID | Size | Port role |
| --- | --- | --- | --- |
| `LevelUpdate` | `0x40` | 48 | mktdata |
| `BookClear` | `0x41` | 36 | mktdata |
| `SnapshotBegin` | `0x20` | 40 | snapshot |
| `SnapshotLevel` | `0x42` | 32 | snapshot |
| `SnapshotEnd` | `0x22` | 20 | snapshot |

This is the first feed in these crates to carry the **snapshot** port role, and
the first whose messages only mean something in sequence: a subscriber's book
exists because it applied every one of them in order.

## The three rules that are easy to get wrong

**Quantity is absolute, never a delta.** A `LevelUpdate` carries the aggregate
resting quantity at the price *after* the change, and zero removes the level. A
subscriber that added it to what it held would drift silently; one that missed a
message is wrong at that price and correct everywhere else, which is what makes
the loss bounded and detectable.

**`Action`, `Level Index` and `Update Reason` are informational.** They must not
gate the apply. Two subscribers receiving the same message must reach the same
book, and one branching on a field the other ignored would not.

**A `BookClear` is not a resynchronisation signal.** A subscriber that applies
one stays ready. Reading it as a reset is how a subscriber throws away a book it
could have kept and asks for a snapshot nobody needed to send.

## What this crate refuses

One combination, and only one: a `BookClear` with `scope = from price` and
`clear_side = both`. One price cannot bound two sides running in opposite
directions — *outward* means down on the bids and up on the asks — so there is
no reading of it that two implementations would agree on. That is exactly when a
decoder must refuse rather than pick one.

Everything else is framing, and framing is `dz-edge-core`'s.

## Golden vectors

The five files in [`testdata/golden`](../../../testdata/golden) are the
cross-language contract: this crate asserts its encoder and decoder against
them, and `go/marketbyprice-parser`'s `golden_test.go` reads the same bytes with
the Go decoder and asserts the same field values. A layout change made on one
side alone fails on the other.

Regenerate them with

```sh
cargo test -p dz-edge-mbp --test generate_golden -- --ignored
```

and treat the diff as what it is: a wire change, to be justified against
`edge-feed-spec` rather than committed to make a test pass.
