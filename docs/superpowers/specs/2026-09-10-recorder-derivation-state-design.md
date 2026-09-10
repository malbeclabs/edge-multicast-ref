# Derivation state that outlives one call, so a window is not a fresh recorder

**Status:** draft, pending review
**Date:** 2026-09-10
**Applies to:** `dz-recorder-events` — the fold that turns recorded datagrams into market data rows
**Authority:** [`edge-feed-spec`](https://github.com/malbeclabs/edge-feed-spec) and its [`GLOSSARY.md`](https://github.com/malbeclabs/edge-feed-spec/blob/main/GLOSSARY.md); `2026-09-05-recorder-market-data-rows-design.md`, whose fold this extends and whose row types, refusals and ordering it does not touch

---

## Naming

This repository is public. This document names no venue, venue repository,
product line, host, bucket, dashboard or issue tracker, and gives no count of
publishers or of recorder sites. It states only what is to be built.

`GLOSSARY.md` governs the vocabulary: `datagram` never `frame`, `era` never
`epoch`, `channel` only for the `Channel ID` shard and `port role` for
`mktdata`/`refdata`/`snapshot`, `feed` never `lane` or `stream`, and `source`
never bare — every use below is `source address`, `Source ID`, or `upstream`.

---

## The question

`derive_events` is an object-at-a-time entry point. It builds the whole of its
per-object state per call — `InstrumentTable::new()`, the snapshot-id
attribution map, `Book::new()` — and it ends the object itself, with
`close_object()` and a copy of the book's refusal counters out to the result.

For an archive object that is right, and
`2026-09-06-recorder-market-data-rows.md`'s decision 4 argues why: an object
spans minutes and contains whole definition cycles, so a fold that begins empty
and ends complete is a fold over a unit that is actually a unit.

**A caller reading a socket has no such unit.** It has to cut arrivals into
windows, because the fold sorts a whole input before folding any of it — the
merge of the three outputs *is* the ordering the derivation depends on — so one
datagram at a time is not on offer. Every window then begins with empty state,
and the derivation is a fresh recorder once per window.

The failure is silent. Nothing refuses a caller that does this, no counter
distinguishes it from a quiet feed, and the rows it produces are individually
well formed.

That deferral was explicit. Decision 4 of the market-data-rows plan says
cross-object state "is task 6 of a follow-up plan rather than a late amendment
to this one", and lists what the full version costs: the entry point, a loader
holding state between objects, a ledger column with its migration, and a guard
against an object arriving out of order. **This document takes up the first of
those four and leaves the other three where they are.** The archive path is
correct as it stands and is changed by nothing here.

---

## What was measured

One top-of-book feed, socket mode, windows cut by time, nothing else changed.

| window | datagrams | rows derived | refused `unresolved_instrument` |
|---|---|---|---|
| 200 ms | 7 168 | 12 | 6 936 |
| 60 s, 20 minutes, 19 windows | 772 901 | 564 812 | 191 054 (25.3% of resolvable) |

Zero capture drops and zero malformed datagrams in both, so the loss is entirely
the fold refusing what it cannot resolve.

**The per-window share is not stationary**, which is the part a single reading
hides:

| window | Δdatagrams | Δrows | Δrefused | refused % |
|---|---|---|---|---|
| 4 | 39 280 | 26 820 | 11 542 | 30.1% |
| 6 | 41 113 | 26 196 | 14 052 | 34.9% |
| 9 | 48 652 | 34 454 | 13 170 | 27.7% |
| 12 | 40 152 | 27 772 | 11 607 | 29.5% |
| 14 | 31 906 | 25 487 | 5 787 | 18.5% |
| 15 | 35 145 | 30 308 | 3 885 | 11.4% |
| 16 | 35 947 | 33 065 | 1 821 | 5.2% |
| 17 | 33 450 | 31 130 | 1 360 | 4.2% |
| 18 | 28 404 | 26 279 | 1 248 | 4.5% |
| 19 | 28 358 | 14 793 | 12 997 | 46.8% |

It decays from about 30% to 4.2% over seventeen windows and then jumps to 46.8%
in one.

**A hypothesis, and it is labelled one because it is not yet measured.** A window
period of 60 s plus derivation time drifts against the cadence on which
definitions are restated, so the share would be a phase relationship rather than
a number a caller can tune. `InstrumentTable::defined_count` per window is what
would confirm or kill it, and killing it matters before anyone tunes a window
length: a tuned window that happens to sit in phase reads as a fix and is a
coincidence.

---

## Three consequences, in order of severity

### 1. `unresolved_instrument` is a per-window floor, not a rate

Every message preceding a window's first definition for its instrument resolves
to nothing and is refused. `source_id`, `price_exp` and `qty_exp` are not
nullable on the row and are exactly the values that decide what a price means,
so refusing is right — the fold is behaving correctly and the input is wrong.

Because it is per window, a longer window shrinks the fraction and never removes
it. There is no window length at which the first message after the cut has a
definition in force.

### 2. A depth feed loses its anchor once per window — it does not lie

This is the one place where the reading that prompted this document was too
strong, and the correction matters because it changes what the fix buys.

A first delta into an empty book does **not** apply and does not mark itself
certain. `Book::level` on a book that is not established emits one row with no
prices and `Certainty::unanchored(NoAnchor)`, then returns nothing for every
delta after it until a cycle anchors the book. The design is honest here
already, deliberately: one row rather than absence, because absence cannot be
told from a silent feed.

So `book_certain` never describes a book the publisher never sent. What is lost
is the anchoring: a delta feed becomes certain only when a whole snapshot cycle
falls inside one window, and a window short relative to the cycle cadence sits
at `book_certain = 0` with `no_anchor` for its whole length. The cost is
coverage, not correctness — which is why it is the second consequence and not
the first.

### 3. Two boundary blindnesses the crate's own doc comments already name

- **Gaps across a boundary are invisible.** `Book::observe_sequence` keeps its
  high-water mark in `Book::last_sequence`, and correctly so — a last-seen mark
  would let a reordered datagram move it backwards and invent a gap. With a
  fresh book per window the first datagram of each window only *establishes* the
  mark and reports nothing, so a gap straddling a boundary is not counted
  anywhere. In the 19-window run above that is eighteen boundaries, none of them
  checkable.
- **`unclosed_cycle` changes meaning under a short window.**
  `BookRefused::unclosed_cycle` already documents a cycle that straddles an
  object boundary anchoring neither side. Its doc comment argues that non-zero
  and persistent means "the anchoring is losing a race against object rotation".
  Under a 60 s window that reading is wrong: the counter goes up once per
  boundary per open cycle as a matter of course, and the levels after the cut
  land as `orphan_snapshot_level` because the snapshot-id attribution map went
  with the call.

Inline mode has the same shape at a longer period. Deriving per rotation
interval contains whole definition cycles, so the same loss is present at a
ratio small enough to read as nothing — and a rotation on bytes shrinks its own
windows exactly during a burst, which is when the loss is largest.

---

## Why a caller cannot hold the state itself

`InstrumentTable` and `Book` are public, so the two obvious pieces are in reach.
Everything that drives them in the fold is not: `Decoded`, `instrument_of_market`,
`instrument_of_state`, `market_row`, `state_row` and `book_row` are private to
the crate, and `instance_of` is `pub(crate)`.

`WireCapture` and `WireProvenance` are **not** part of that argument — they are
public in `dz-recorder-relower` and a caller can absorb an archive itself. The
argument stands on the row builders and the attribution map: reproducing them
outside the crate means a second decoder of the same bytes, which is the one
thing a recorder must not have, because the two would disagree eventually and
nothing would say which was right.

---

## The change

An added entry point that takes the state by reference.

```rust
/// The state one derivation hands to the next.
#[derive(Debug, Default)]
pub struct Derivation { /* the table, the book, the attribution map, two counters */ }

impl Derivation {
    pub fn new() -> Self;
    /// The derivation ended: close open cycles and report what they refused.
    pub fn close_object(&mut self) -> BookRefused;
}

pub fn derive_events_into<S: Source + ?Sized>(
    state: &mut Derivation,
    source: &mut S,
    input: &EventInput<'_>,
) -> Result<DerivedEvents, RelowerError>;
```

`derive_events` keeps its exact signature and becomes `Derivation::new()`, one
call into the new entry point, and `close_object()`. The archive path's
behaviour is then unchanged **by construction rather than by review**, which is
the property worth having: nobody has to read the new fold to know the old one
still holds.

### What crosses a call, and what must not

Three pieces cross: the `InstrumentTable`, the `Book`, and the snapshot-id
attribution map that `instrument_of_state` maintains. The book carries its own
open cycles and its own sequence high-water marks with it, so consequence 3
resolves as a side effect of the book persisting rather than by anything new.

Two locals stay per call, and stating why is the point:

- **`seen`** is the instrument grain for definitions observed *in this call*.
  Persisting it would re-emit every instrument row in every later window.
- **`at_datagram`** is a within-call dedup for calling `observe_sequence` once
  per datagram rather than once per message. It must reset, because the first
  datagram of a new window is exactly the one whose sequence number has to be
  tested against the previous window's mark — the boundary gap this document
  exists to make visible.

---

## Three semantics, decided here because silence on any of them is a caller's bug

### 1. `book_refused` reports the per-call delta

`Book::refused` is cumulative: `close_object` does `+=` and the field is a
running total for the life of the book. `DerivedEvents::book_refused` is that
total copied out. On a persisted `Book` a caller that sums the field per window
double-counts, and nothing tells it so.

Everything else on `DerivedEvents` is already per call — `refused` is a fresh
`Refused` every time — so leaving one field cumulative and the rest per call is
precisely the asymmetry that produces a silent wrong answer. **The new entry
point reports the delta since the previous call.** `Book::refused` stays
cumulative and unchanged, as the book's own running total; `Derivation`
remembers the previous total and subtracts.

For `derive_events` the delta and the total are the same number, because the
state is fresh, so the archive path reads identically.

### 2. `close_object` is for the end of a derivation, and a live caller calls it once

For an archive object, `close_object` is the boundary: a cycle still open when
the object ends anchored nothing, and counting it is the only way that shows up.

For a live caller, calling it per window would count every cycle that merely
straddles a window boundary — which is the failure this document is fixing,
re-introduced through the counter. So a live caller calls it **at shutdown, or
when it abandons the state, and nowhere else.** A cycle open at that moment
genuinely anchored nothing; a cycle open at a window boundary is simply still
open.

That makes `unclosed_cycle` mean two things depending on the caller, and its doc
comment currently states only one. **The comment says both**, or a reader takes
a per-window count as a rotation-rate fault and looks for a race that is not
there.

### 3. `datagram_index` continues across a call, so that splitting is a no-op in every column

`WireProvenance::datagram_index` is documented as position "counting from 0 over
everything the source yielded", and `absorb` numbers from a private counter on a
`WireCapture` that `derive_events` builds fresh per call. So the same bytes,
split at a datagram boundary, produce rows whose `datagram_index` restarts —
verified: quotes at 1 and 3 over the whole input, and at 1 in the second half
alone.

Left there, the acceptance criterion below would need an exception clause for
one column, and a caller comparing two window cuttings would find rows that
differ for no reason it could see. So **`Derivation` carries the base** and the
new entry point advances it by `WireCapture::datagrams()`, which counts exactly
what `datagram_index` is an index into, including the foreign and undecodable
datagrams that yield no message.

The field is safe to make run-relative rather than call-relative: in `005` it is
a plain column, absent from the `recorder.event` sort key, so
`ReplacingMergeTree` neither dedups nor joins on it. `derive_events` starts a
fresh `Derivation`, so its base is 0 and archive rows are unchanged.

---

## The acceptance criterion: splitting is a no-op

For any input and any datagram boundary, one `derive_events` over the whole
input and two `derive_events_into` calls over the two halves produce the same
`event` and `book_top` rows, in the same order, with per-call counters summing
to the whole's.

`instrument` rows carry one qualification, and it is the row type's own design
rather than a concession. `seen` is per call by decision above, so a definition
restated in both halves yields one row per call where the whole yields one row.
`recorder.instrument` is `ReplacingMergeTree(last_seen_ts)` keyed on
`(channel_id, instrument_id, from_sequence, source address, dst_port, …)`, so
those rows collapse to the one with the greatest `last_seen_ts` — which is the
row the whole-input run produced. **The criterion for `instrument` is therefore
equality after that reduction**, stated in the test rather than left for a
reader to infer.

A test that passes both ways has documented the tree rather than changed it, so
each case below must fail against today's `derive_events` when the same bytes
are split, and pass through the new entry point:

- a definition in the first half and its quotes in the second — today every
  quote is refused as `unresolved_instrument`, measured: two of two;
- a snapshot cycle straddling the split — today `orphan_snapshot_level` for the
  levels after the cut and `unclosed_cycle` for the cycle;
- a `mktdata` sequence gap straddling the split — today counted nowhere at all;
- a restatement in the second half applying only to the prices after it, which
  `a_restatement_applies_to_the_prices_that_came_after_it` already asserts
  within one call and which must survive the cut.

---

## Non-goals

- **The fold's ordering and its merge.** Unchanged, and the reason the entry
  point takes a whole input rather than a datagram.
- **Every refusal the fold counts, and their names.** No refusal is added,
  removed or renamed. Two doc comments are corrected because persisting the
  state makes them false, which is not the same thing.
- **The row types and the DDL.** No column is added or changed.
- **The archive path.** Correct as it stands for objects.
- **The loader holding state between objects, the ledger column, and the
  out-of-order guard.** The other three quarters of the deferred decision. This
  document is the entry point they would be built on, and none of them is
  reachable without it.
- **Any caller.** Nothing here cuts a window, and no window length is
  recommended — the hypothesis above says a recommendation would be a
  coincidence until `defined_count` per window says otherwise.
