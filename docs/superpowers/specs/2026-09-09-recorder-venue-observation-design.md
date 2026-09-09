# The venue half of a feed race

**Status:** design. Answers a request to let a recorder read a venue's own upstream alongside its capture inputs, and derive both into the same rows.

A feed race compares what a venue said with what a publisher sent. This repository covers the publisher half completely — a capture with `SO_RXQ_OVFL`, an archive, an offline re-lowering, six row grains, a column-store writer, a loss derivation and a conformance runner. The venue half has nowhere to live: no recorder crate links any `dz-ingress-*` crate, so an integration that wants the comparison re-implements a receive layer, a decode tier, a writer and a book rebuild to get it.

Both halves exist. The request is to join them, and the request's own shape is wrong. This document says where the seam is instead, and what the row tier would have had to give up to put it where the request asked.

## Naming

`observation` is the column that already names *where a view of the book came from*, and this document uses it for the venue side rather than inventing a second word. A **venue-side observation** is a recording made where the venue's own bytes arrive; a **publisher-side observation** is a recording made where a channel's datagrams arrive. `GLOSSARY.md` governs the rest: a datagram is a UDP payload and a venue's upstream message is not one, an era is a `Reset Count` span and belongs to a publisher, and `source` never appears bare.

The word this document rejects is **capture**. A capture is a receive path over a socket that observes datagrams, counts what the handle dropped, and records link headers; a venue-side recording is none of those, and calling both a capture is what makes the request's shape look like a small change.

## The request, and the two sentences that decide it

> let a recorder take a `Box<dyn Input>` alongside its capture inputs and relower venue payloads into the same rows through that venue's own `Adapter`.

Refused as stated. Two facts settle it, and both are in the code the request cites.

**`Source` yields a datagram, not a payload.** `dz_recorder_core::Source::next` hands back a `RecordedDatagram`, whose fields are `payload`, `src: SocketAddrV4`, `dst: SocketAddrV4`, `role: PortRole`, `recv_ts_ns`, `recv_ts_kind`, `drop_delta`, `ttl: Option<u8>`, `link_headers` and `wire_payload_len`. A venue's upstream message — a websocket text message, a tag-value message on a session, a response body — has no source address, no destination group, no port role, no capture-handle drop count and no wire length distinct from its own. An `Input` handed to that tier has to invent all of it.

**The derivation reads a 24-byte header off the payload.** The row tier peeks the datagram header to place every row: `sequence_number`, `channel_id`, `reset_count`, `message_index`. A venue payload carries no such header, so the peek either refuses — and the payload is dropped and counted as malformed, which is a recorder reporting the venue's feed as broken — or the header is synthesised, which is worse.

The tier is explicit about why synthesising is worse, in the one place it already faced this question. `RecordedDatagram::ttl` is an `Option` and its doc says why: "`None` when the capture mode did not observe it — never zero for *not observed*, because zero is a TTL a datagram can actually carry." Every field a venue-side observation would have to fill has that same property and no `Option` to fill it with. Channel `0` is a channel. Port `0` is a port a document can state. Sequence `0` is the first sequence of an era. `0.0.0.0` is a source address that reads as *unset* to a person and as a value to a query.

## What the row tier would have given up

The request asks for "the same rows". Six guarantees stand in the way, and they are the guarantees the tier exists for.

**Provenance is not decoration; it is identity.** `event` carries `source_addr`, `channel_id`, `dst_port`, `source_id`, `sequence_number`, `reset_count`, `segment_seq` and `message_index`, none of them nullable, and they are in the sort key. `book_top` carries the same set. That is what makes a row traceable to bytes in an object a reader can fetch and re-derive. A venue-side row can fill none of the eight truthfully, and a row that cannot be traced to bytes is the one thing this tier does not produce.

**Idempotence is `(object key, sha256)`.** Reprocessing replaces rather than duplicates, and the batch is the unit that either lands or does not. A live `Input` produces no object and no digest, so there is no key to replace on and no boundary to batch at. Re-running a venue-side recording would accumulate rows, and the `book_top` pairing counts occurrences — so a duplicate does not inflate a count there, it manufactures evidence of loss. The pairing view says so itself.

**A `sequence_gap` is a run of *this* sequence space.** A venue's own counter — a session sequence number, an update id — is a different series with a different owner and different loss semantics. Writing one into that column makes the cross-site views compare two unrelated counters and report a venue's session resend as a publisher's gap.

**An `era` is a publisher's `Reset Count` span.** A venue has none. The pairing view's own header states the consequence: "two transports do not share a sequence space at all, so they share no era in any form."

**`drop_delta` belongs to a capture handle.** It is the quantity `epb_dropcount` is defined as, charged to the handle rather than to a port role. A venue transport's loss is its session's, measured by the venue's own resend mechanism, and the two must not land in one column.

**`book_certain` has two definitions.** On the publisher side, certainty falls when a gap in the publisher's sequence means the book cannot be believed. On the venue side, certainty falls when the venue's own resynchronisation says so. Same column, two meanings, and a `min(book_certain)` over a pair silently mixes them.

## The finding the request did not make, and should have

The schema **invites** the venue side into a table it cannot honestly write.

`book_top.observation`'s doc comment says: "Two recorders of one multicast feed are two observations; a multicast feed and some other transport carrying the same instruments are two observations. Nothing in the schema knows which is which, and nothing should." The pairing migration repeats it in its header and builds on it: no new table, "both sides' rows land in one table and the comparison is a query."

That promise is not keepable as the tables stand, for the eight-column reason above, and there is a second reason that is worse because it is not about nullability at all. **The race is keyed on publisher identity.** The pairing groups by `(channel_id, instrument_id, state_key, occurrence)`, and a venue-side observation can compute neither of the first two:

- `instrument_id` is minted by the publisher's reference-data registry, is unique only within an era, and is not derivable from anything a venue sends. A venue knows the symbol.
- `channel_id` is the operator's mapping from a shard to a channel. A venue **may not** know it — that is the constraint the whole adapter boundary is built on, restated in the feed-routes design: a venue names a shard and cannot name a `Channel ID`.

So a venue-side observation could only pair by being handed the publisher's configuration and its live registry state, which breaks the boundary in the one direction it exists to prevent, and would still be wrong across an era boundary.

The comparison a venue side can compute is on `(feed, symbol, state_key)`. `state_key` is already transport-independent by construction — "a hash over the instrument and both sides, and over nothing else. No timestamp, no sequence number, no bytes" — and `event.upstream_ts` is already documented as excluded from every equivalence key because "one book state carried over two of them would hash two ways and no pair would ever be found." The equivalence key was designed for this. The *provenance* and the *grouping* were not.

**Whichever way this design goes, those two doc comments and that migration header are wrong today and should be corrected.** They promise a shape the columns refuse. That is a documentation defect in the current tree, independent of the venue side ever being built.

## The shape that is right

Three moves, and each one reuses a mechanism this repository already has rather than widening one.

### 1. The venue side archives before it derives

A venue-side recording writes the venue's own upstream bytes into archive objects — keyed, compressed, digested, rotated on the same policy — and derives from the objects, not from the socket. That restores every guarantee the live-`Input` shape gave up: `(object key, sha256)` is back, the batch boundary is the object, a re-run replaces, and the bytes a row came from can be fetched and re-derived by somebody who doubts the row.

It also keeps the evidence. Deriving from the socket and keeping nothing means a mapping defect found next month cannot be re-examined; deriving from raw venue bytes means it can, with a corrected adapter, which is the reason the offline re-lowering exists on the publisher side.

The archive is a **second archive shape**, not the pcapng one: there are no link headers to record and no datagram boundaries to preserve, so what is recorded is a length-delimited sequence of upstream messages with the connection each arrived on and a receive stamp. The record encoding that already exists for the offline comparison is the obvious candidate and is deliberately *not* reused for this: that encoding carries **normalized events**, which are downstream of the venue's decode, and the whole point of keeping raw bytes is to keep what the venue actually sent. The two coexist — raw bytes as evidence, normalized events as the reference for a re-lowering diff — and the design says which is which rather than letting one drift into the other's job.

### 2. The derivation is a library the venue's own binary composes

Decoding a venue's bytes requires the venue's `Adapter`, and this repository must not link one. The publisher side already solved exactly this: `run(AdapterRegistry)` is composed by the venue's `main`, and the adapter arrives through a registry this repository never populates.

So the venue-side derivation is a crate that takes an `&mut dyn Adapter`, drives it over an archived object's messages, collects the normalized events it emits, and produces rows. This repository's recorder binary gains **no** venue mode and links **no** venue crate. A venue's own recorder binary is three lines, the way its publisher's `main` is.

That answers the request's "through that venue's own `Adapter`" exactly, and puts it where the adapter can be handed over without this repository knowing the venue exists.

### 3. The venue's rows are their own grains, and the race is a view

Venue-side rows go to their own tables with venue provenance — the observation, the upstream connection, the object key, the venue's own message identity where it has one, the symbol — and **never** `channel_id`, `instrument_id`, `sequence_number`, `reset_count`, `segment_seq` or `drop_delta`.

The comparison is a view joining venue-side occurrences to publisher-side occurrences on `(feed, symbol, state_key, occurrence)`, numbered per observation exactly as the existing pairing numbers its own, with `lead_ms` as a column and the bound on it left to the caller. Every argument the existing pairing migration makes carries over unchanged and is cited rather than restated: why this is not an `ASOF JOIN`, why an unpaired occurrence is a row rather than an absence, why a snapshot-anchored row consumes no ordinal, and why the bound is the caller's predicate.

That the join is a view and not a step in the derivation is the same decision the cross-site verdict already took: "a verdict decided while an object is loading is decided against whatever else had arrived by then."

## What this refuses

- **No venue code in this repository**, in the recorder or anywhere else. The derivation takes an adapter; it does not contain one.
- **No new column on `event` or `book_top`**, and no existing one made nullable. A nullable provenance column weakens every publisher-side row to accommodate a row of a different kind — and `UncertainReason::None` already states the rule: a column is not nullable when "a NULL invites a join that drops the row."
- **No venue rows in the publisher-side tables.** The `observation` column stays what it is: which publisher-side observation point a row came from. Two recorders of one channel are two observations; a venue-side recording is a different table.
- **No sentinel provenance.** Not `channel_id = 0`, not `dst_port = 0`, not `source_addr = 0.0.0.0`, not `sequence_number = 0`.
- **No live `Input` in the capture path**, which is the request as stated.
- **No claim about attribution.** Whether a state the venue published and the publisher never sent is the publisher's fault stays the loss derivation's question and the cross-site views'. A venue-side observation adds one more thing that can be missing, not an answer about whose fault it is.

## What it costs

- **A second archive shape**, with its own format document, its own reader and its own retention. Two archive shapes in one repository is the real cost of this design and the honest alternative to inventing datagram provenance.
- **A new crate**, plus a migration for the venue-side tables and the race view. The derivation cannot live in the rows crate: that crate decodes no payload by design, and this one must link the adapter boundary and the lowering.
- **A venue's recorder binary is now a binary it assembles**, not one it runs. That is the same trade the publisher side already made, and the same argument answers it: a venue that must not be handed a `Channel ID` also must not be handed a row writer that fills one in.
- **The comparison is coarser than the publisher-side race.** Keyed on the symbol rather than the `Instrument ID`, it cannot separate two instruments that shared a symbol across an era boundary, and it depends on the venue's symbol and the published symbol agreeing — which is a reference-data assertion, and worth being a column rather than an assumption, the way `exponents_agree` is.
- **A venue-side observation cannot report loss.** It has no sequence space of its own that this repository defines, so a state the venue produced and nobody recorded is invisible on that side. The tier says what it does not know rather than filling it with a zero.

## Decisions

| Decision | Why |
|---|---|
| The venue side archives raw upstream bytes and derives from objects | Restores `(object key, sha256)` idempotence, the batch boundary, and the ability to re-derive with a corrected adapter |
| The derivation is a library, driven by a venue's own binary | Decoding venue bytes needs the venue's adapter, and this repository links no venue — the publisher side's own arrangement |
| Venue rows are their own grains | Eight non-nullable provenance columns on `event` and `book_top` are statements about a datagram, and a venue message is not one |
| The race is a view on `(feed, symbol, state_key, occurrence)` | The only key both sides can compute: `instrument_id` is minted by the publisher and `channel_id` is the operator's, which a venue may not name |
| Raw bytes, not the normalized-event record encoding, are what is archived | That encoding is downstream of the venue's decode; the evidence has to be what the venue sent |
| The `observation` doc comments and the pairing migration header are corrected | They promise venue rows in a table whose columns and grouping key refuse them |

## Non-goals

Attribution across sites. A second archive format for the publisher side. Any change to the four publisher-only grains. A venue-side conformance rule set — the rules are the feed spec's and a venue's upstream is not that feed.
