# The venue half of a feed race — plan

Turns [the design](../specs/2026-09-09-recorder-venue-observation-design.md) into ordered tasks.

**Base:** `main`. Nothing here touches the publisher runtime, so this is independent of `jo/publisher-feed-routes`.

**Read the design's refusals first.** Three of the tasks below exist to make something impossible rather than to make something work, and each is a place where the obvious implementation is the one the request asked for.

## Scope

A venue-side observation: an archive of a venue's own upstream bytes, a derivation that drives a venue's `Adapter` over it, venue-side row grains with venue provenance, and a race view that pairs them against the publisher side on the one key both can compute.

**Not in scope:** any venue code in this repository, any change to the publisher-side grains' columns, attribution, and a venue-side conformance rule set.

## The ordering constraint

Task 1 is a correction to documents that are wrong **today**, and it lands first because it is true whether or not anything else here is ever built. Tasks 2 through 6 are the venue side, and each is behaviour-neutral for the publisher side by construction: nothing in them is reachable from the recorder binary this repository ships.

## Global constraints

- **Vocabulary:** `GLOSSARY.md` governs every identifier, comment, test name, config key and commit message. A venue's upstream message is never a `datagram`; the publisher's `Reset Count` span is an `era` and the venue side has none; `source` never appears bare; and the word `capture` is reserved for the receive path that observes datagrams — a venue-side recording is not one.
- **Every test must be shown to kill its mutant.** Revert the change, watch the named test fail, restore it. Several tests here assert an absence — a column that is *not* written, a table a row does *not* reach — which is exactly the shape that passes against an unfixed tree.
- **Nothing in this repository links a venue.** A task that adds a venue dependency to any crate here has misread the design.

---

## Tasks

### 1. The two doc comments and the migration header that are wrong today

- [ ] `book_top.observation`'s doc comment stops promising that "a multicast feed and some other transport carrying the same instruments are two observations" land in one table. It says what the column is — which publisher-side observation point a row came from — and names the two reasons a venue-side row cannot join it: eight non-nullable provenance columns that are statements about a datagram, and a pairing grouped on `channel_id` and `instrument_id`, neither of which a venue may compute.
- [ ] The same correction in `006_recorder_book_top_pairing.sql`'s header, which repeats the promise and builds "no new table" on it. The rest of that header is untouched: every argument it makes about the ordinal, the unpaired occurrence, the anchored row and the caller's bound is right and is what the venue-side view will cite.
- [ ] `event.upstream_ts`'s doc comment is left exactly as it is. "One book state carried over two of them would hash two ways and no pair would ever be found" is true and is the reason the equivalence key works across transports.

**Test:** `dz-recorder-clickhouse/tests/ddl.rs` already holds the migration set against literals; extend whatever it asserts about `006` to cover the corrected header only if it asserts header text at all — otherwise this task's gate is `scripts/check-public-repo-rules.sh` plus a read against the glossary's banned-word table.

**The revert:** there is no test to kill here, and the plan says so rather than inventing one. A documentation correction whose test is a string comparison against the corrected string documents the string.

---

### 2. The venue-side archive format

- [ ] A length-delimited object format for upstream messages: a header naming the format version, the connection each message arrived on, the receive stamp and its kind, and the message bytes exactly as they arrived. No link headers, no destination group, no port role, no capture-handle drop count — the design's whole point is that none of those exist here.
- [ ] Rotation, compression and digesting reuse the archive tier's own policy rather than a second one. The object key and its `sha256` are what the derivation is idempotent on, so they are not optional and not derived at read time.
- [ ] The format document states plainly that this is **not** the normalized-event record encoding and why: that encoding is downstream of a venue's decode, and the evidence has to be what the venue sent.

**Test:** a round trip over a synthetic set of messages, including a message of zero length, a message at the largest size the format admits, and a truncated tail — which must be a refusal naming the object rather than a short read that returns fewer messages than were written.

**The revert:** make the truncated tail a silent stop. `a_truncated_object_is_refused_rather_than_read_short` fails. That is the mutant that matters, because a silent stop turns a half-written object into a venue that went quiet.

---

### 3. The derivation, driven by an adapter the caller owns

- [ ] `fn derive_venue_object(&mut dyn Adapter, object, &mut dyn RowSink)` in a new crate, taking the adapter as an argument for the reason the publisher's runtime takes one: the adapter is the venue's, and this repository never constructs it.
- [ ] The adapter is driven over the object's messages in recorded order, and the normalized events it emits are collected through the same `EventSink` the publisher uses — no second sink shape, so a venue's adapter cannot behave differently under a recorder than under a publisher.
- [ ] An adapter that refuses a message costs that message and is counted, not the object. A recorder that stops at the first message a venue's own adapter cannot parse reports the venue's feed as ended.

**Test:** a fixture adapter over a fixture object produces the expected venue-side rows; an adapter that refuses one message in the middle produces the rows either side of it and a count of one refusal.

**The revert:** make a refusal end the object. `one_refused_message_costs_that_message_and_not_the_object` fails.

---

### 4. The venue-side grains, and the columns they do not have

- [ ] Venue-side row types carrying venue provenance: the observation, the upstream connection, the object key, the venue's own message identity where it has one, the symbol, the instrument's exponents as the venue states them, both sides of the top, and `book_key` computed by **the same function** the publisher side uses — not a second implementation, because two hashes of one book state pair with nothing.
- [ ] No `channel_id`, no `instrument_id`, no `sequence_number`, no `reset_count`, no `segment_seq`, no `drop_delta`, and no `era` column. Asserted as an absence: `tests/column_names.rs`'s pattern extended, holding the venue-side column set against a literal so that adding one of the six is a test failure rather than a review comment.
- [ ] A migration for the venue-side tables, numbered after the highest existing one, with the same `ReplacingMergeTree` and the same day partitioning, so that a re-load replaces.

**Test:** the column-name literal, and a derivation over a fixture that would have had a channel and a sequence number available to it — a fixture whose object metadata carries them — asserting that neither reaches a row.

**The revert:** add `channel_id` to the venue-side row and fill it from the object's metadata. `the_venue_side_rows_carry_no_publisher_provenance` fails. This is the plan's centre: the request asked for exactly that column to be filled in.

---

### 5. The race, as a view

- [ ] A view numbering venue-side occurrences per observation on `(observation, feed, symbol, book_key)` ordered by receive stamp, and a pairing grouping on `(feed, symbol, book_key, occurrence)` with `uniqExact(observation)`, `lead_ms` as a nullable column, and the bound on it left to the caller. **`book_key` and not `state_key`**: that one eats a `channel_id` and an `Instrument ID`, and a venue side can compute neither.
- [ ] `symbols_agree` and `exponents_agree` as columns rather than assumptions, for the reason the publisher-side pairing carries `exponents_agree`: the key covers the raw prices and leaves the exponents out, and a pair whose exponents disagree is two different prices wearing one key.
- [ ] Every argument the existing pairing makes is **cited, not restated**: why this is not an `ASOF JOIN`, why an unpaired occurrence is a row, why an anchored row consumes no ordinal, and why the bound is the caller's.

**Test:** the golden-query pattern the existing migrations use — a fixture load and an expected result set — including a state that repeats quickly (which is what `ASOF` gets wrong), a state only one side saw, and two sides whose exponents disagree.

**The revert:** replace the ordinal pairing with an `ASOF JOIN`. `a_state_that_repeats_pairs_one_to_one` fails with plausible, biased lead times — which is the failure mode the existing migration's header describes and the reason this test exists at all.

---

### 6. The documents that have to stay true

- [ ] The recorder README gains the venue-side arrangement, and says in one line that a venue's recorder is a binary the venue assembles, as its publisher is.
- [ ] `docs/README.md` carries the row for this pair.
- [ ] The archive format document from task 2 is linked from both.

**Test:** `scripts/check-public-repo-rules.sh`, plus a read of the new prose against the glossary's banned-word table. The words most likely to slip in here are `capture` for a venue-side recording, `stream` for a venue's feed, and `frame` for an upstream message.

---

## Acceptance

The plan is done when:

1. a venue's own binary can compose a recorder from this repository's crates plus its own adapter, and no crate here names a venue;
2. a venue-side object re-derived twice produces one set of rows, not two;
3. a venue-side row carries no publisher provenance, asserted against a column-name literal;
4. a state both sides saw appears as one pair with a measured `lead_ms`, a state one side saw appears as a row with `observations = 1` and a null `lead_ms`, and a state that repeats four times pairs four times one-to-one;
5. a truncated object is a refusal naming the object;

and when reverting task 4's absence or task 5's ordinal makes a named test fail — because those two are where the request's own shape would have been accepted.

## What this plan does not do

It does not build a venue-side receive path. A venue-side observation needs an `Input`, and the two transports a venue would use for one are declared and unbuilt: a session transport and a polled one, each of which is its own design. This plan takes archived objects as its starting point precisely so that it does not depend on either.
