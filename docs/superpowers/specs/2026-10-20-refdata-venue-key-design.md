# An instrument recalled by its venue key

**Status:** design. Answers ask 1 of [#181](https://github.com/malbeclabs/edge-multicast-ref/issues/181), the one [the derived-ID design](2026-10-09-refdata-derived-instrument-ids-design.md) left out.

## The failure

The registry recalls an instrument by its padded wire `Symbol`: `offer` takes `symbol_field(spec.symbol)` and looks it up in `handles`, and `admit` looks it up in `minted` (`registry.rs`, `offer` and `admit`). `symbol_field` truncates to the field's 64 bytes. Two venue instruments whose names agree on their first 64 bytes are therefore one instrument to the registry. The second is handed the first one's handle, its quotes update the first one's book, and it never gets an `Instrument ID` of its own.

The glossary defines `Symbol` as "display and filtering only", and "a state key" is in its `Not` column. The feed specs allow truncation: "Truncate only if the venue's symbol exceeds 64 bytes". Nothing on the wire requires two instruments to have different `Symbol`s. Only the registry makes it a requirement.

It happens at scale. On Kalshi sports, 3,086 groups of 64-byte prefixes cover 7,828 instruments: about 19% of instruments share a `Symbol` with another (2026-08-07 enumeration). Kalshi's shipped publisher keys on the full ticker and gives each its own ID. A publisher on this runtime cannot reproduce that, and cannot import those IDs: two tickers that land on one `Symbol` are two entries the record refuses (`RecordError::DuplicateSymbol`).

## The key

**A venue key is the venue's own name for an instrument, as bytes, between 1 and 256 bytes long.** It is never put on the wire. It is what the registry recalls an instrument by, and it is what the record stores beside the `Symbol`. Two instruments may share a `Symbol`. They never share a venue key or an `Instrument ID`.

The bound is a refusal, not a truncation, because truncating it would bring the failure back. 256 bytes is four times the field, and more than any venue in this workspace uses. Raising it later only widens what is accepted.

**`InstrumentSpec` gains `venue_key: Option<&'a str>`.** `None` means the key is the `symbol` the adapter stated, **untruncated**. So an adapter whose ticker is both its display name and its key, as Kalshi's is, changes nothing but the struct literal. A venue whose key is not its display name states both.

**Identity is a configuration choice, and the default is today's.** `[refdata] identity` takes:

- **`"symbol"`**, the default: the registry recalls by the padded wire `Symbol` exactly as it does now and ignores `venue_key`. Records, refusals and tests are unchanged.
- **`"venue_key"`**: the registry recalls by the venue key. Two keys that share a `Symbol` are two instruments with two IDs and two definitions, both carrying that `Symbol`.

`RegistryConfig` gains `identity: Identity { Symbol, VenueKey }`, `#[non_exhaustive]` for the reason `IdAllocation` is.

## Recall and forgetting

Under `"venue_key"`:

- **`handles` and `minted` key on the venue key.** A re-offer is one hash lookup on the key's bytes, with no allocation: the maps hold `Box<[u8]>` and are looked up by `&[u8]`.
- **`in_use` stays keyed by `Instrument ID`,** and only its value changes, from the `Symbol` to the venue key. The derived-ID check needs exactly that direction: is this ID held, and by whom (`registry.rs`, `admit` and `remember_unavailable`).
- **`symbol_field` is still applied,** but only to compose the definition, and `Fit::Truncated` is still counted. Truncation is now display-only, which is the meaning the spec gives it.
- **Forgetting keys on the venue key.** `forgettable` and the compaction that drops forgotten entries are unchanged except for the key type. A forgotten key that is listed again is a new instrument with a new ID, as a forgotten `Symbol` is today.
- **`take_unavailable_ids` names the venue key,** and the `Symbol` beside it.

## The record: version 4

```
dz-refdata-state 4 <source_id> <next_id> <base entries> <allocation>
<instrument_id> <symbol, 128 hex digits> <venue key, hex | ->
<instrument_id> <symbol, 128 hex digits> <venue key, hex | -> <unix seconds>
...
```

- **The header** names the allocation: `sequential`, `derived` or `derived_key` (below). The second number means what it means in versions 2 and 3: `next_id` under `sequential`, the floor under the other two. Version 4 carries both allocations, so there is one keyed layout rather than two.
- **The venue key is a third field, always present:** the key in lower-case hexadecimal, or `-`. `-` means "the key is this `Symbol` with its NUL padding removed", and **it is allowed only where the `Symbol` has padding**, so it ends before byte 64. There the key is known to be complete: nothing was truncated. A key of 64 bytes or more is always written in hex, even one equal to its `Symbol`, so the record never has to guess whether a full-width `Symbol` is a whole key or a truncated prefix. An ordinary entry costs two bytes more than in version 2. A key and a timestamp can never be confused, because the key field is always third.
- **The loader refuses** a key outside 1–256 bytes (`RecordError::BadVenueKey`), odd or non-hex digits (the same error), `-` on a full-width `Symbol` (`RecordError::UnresolvedVenueKey`), two entries with one key (`RecordError::DuplicateVenueKey`), and two entries with one ID (unchanged). Two entries with one `Symbol` are allowed. That is the change.
- **The sequential and derived line rules are unchanged:** `IdNotBelowNext` and `AppendedOutOfOrder` apply under `sequential`. Under `derived` and `derived_key`, a mint must be the CRC-32 of its input, as in version 3. A torn final line is dropped.
- **Only `"venue_key"` writes version 4.** `"symbol"` keeps writing version 2 or 3, so a publisher that never sets `identity` sees no change on disk.

### Opening an unkeyed record under `"venue_key"`

A version 1, 2 or 3 record is read, and every entry whose `Symbol` has padding gets the key `-`: its key is complete. The record is rewritten as version 4 at open through `Loaded::needs_rewrite`, as the derived design did for version 3. A rewrite that fails is a startup error.

**An entry whose `Symbol` fills all 64 bytes is not resolved at open.** It may stand for that exact 64-byte key, or for a longer ticker that was truncated, and the record cannot say which. Nothing in one process can decide it either. Giving the ID to whichever key is offered first depends on what each host saw first, so two redundant paths could give one ID to two instruments, which is the outcome the derived design rules out. The open refuses such a record (`RefdataError::UnresolvedVenueKeys`, naming each entry's ID and `Symbol`). It opens only after the operator resolves those entries with an explicit mapping through `seed` (below):

- **mapped:** a `(venue_key, instrument_id)` pair whose `symbol_field` is the entry's `Symbol` and whose ID is the entry's ID makes it an ordinary keyed entry;
- **retired:** with `Seed::retire_unmapped`, a full-width entry the mapping does not name is forgotten. Its ID stays below `next_id` and is never issued again, and the instrument, if listed, gets a new ID.

The resolution is made once and written into a version-4 record. **Redundant paths resume only from that record:** copied to every path that shares the record, or, for paths with their own records, each resolved from its own mapping before its first start under `"venue_key"`. A path never resolves an entry by itself while it runs.

For Kalshi the mapping exists already: the shipped publisher's `instruments.json` maps every full ticker to the ID it published under. The handover imports it with `seed` before the first start, so no full-width entry is left unresolved.

## Derived allocation

`"derived"` keeps hashing the `Symbol`, as its design states. Under `"venue_key"`, two keys that share a `Symbol` derive the same ID, and the second is declined as `Refusal::IdUnavailable`. That is consistent, but it puts the merged instruments back on one side of a refusal.

So **`[refdata] id_allocation = "derived_key"`**: the CRC-32/ISO-HDLC of the venue key's bytes, with the same floor, the same decline-never-move rule, the same refusal of `forget_delisted_after`, and the same seeding rule as `"derived"`. A different input derives different IDs, so it is a new name, not a reinterpretation of the old one. It is refused under `identity = "symbol"`, where there is no key to hash (`RefdataError::DerivedKeyWithoutVenueKey`). Moving a record between `"derived"` and `"derived_key"` is refused like any other change of allocation, because every derived ID above the floor would change.

## Seeding a record from another publisher's IDs

Ask 3 was answered by a file copy, which works only between hosts running this runtime. A publisher moving onto it from its own registry, such as Kalshi's shipped publisher with `instruments.json` (`{ "next_id": N, "tickers": { ticker: id } }`, keyed on the full ticker), needs to write a record in a format it does not own. Today it does that by hand through `StateRecord::encode`.

**`dz_publisher_refdata::seed(store, Seed { source_id, next_id, entries })`**, where `entries` are `(venue_key, instrument_id)` pairs. It writes a version-4 `sequential` record. Each entry's `Symbol` is `symbol_field(venue_key)`. Nothing the caller writes depends on the layout.

It merges into a record that already exists rather than replacing it. That is what a handover in both directions needs: import before the first start, export on a rollback, import again. The merge:

- refuses a `Source ID` that differs from the record's (`SeedError::SourceIdMismatch`);
- refuses a key the record holds under another ID, and an ID the record holds under another key (`SeedError::Conflict`, naming both sides);
- refuses an ID of `0` or at or above the seed's own `next_id` (`SeedError::IdNotBelowNext`);
- refuses an ID the record does not hold but that is below the record's `next_id` (`SeedError::IdMayBeForgotten`). The record may have issued and forgotten it (`forget_delisted_after`), and `next_id` is the only thing that keeps an ID from being issued twice (`state.rs`, `StateRecord`). Below `next_id`, "not held" does not mean "never issued". A destination with no record has a `next_id` of `FIRST_INSTRUMENT_ID`, so a first import passes. So does a re-import after a rollback, where every ID the other side minted is at or above the `next_id` it was exported with;
- resolves the full-width entries of an unkeyed record, as described above (`SeedError::Unresolved` when a full-width entry is neither mapped nor retired);
- refuses an invalid venue key (`SeedError::BadVenueKey`);
- refuses a derived record (`SeedError::RecordIsDerived`). A seed's IDs belong below a floor, so seeding comes before the switch to `"derived_key"`, not after;
- sets `next_id` to the larger of the record's and the seed's;
- writes the result as one base through `StateStore::store`.

**The outcome of a failed write is not always known.** `StateError::NotReplaced` means the old record is still in place. `StateError::Write` is the contract's "not known" (`store.rs`, `StateStore::store`): `FileStore::store` returns it when the directory sync fails after the rename, and then the record on disk may be either the old one or the merged one. So `seed` returns `SeedError::Store(StateError)` unchanged, and its documentation says: after `NotReplaced`, nothing changed; after `Write`, reload before acting. Because the merge is a union that refuses every disagreement, running the same seed again over either record gives the same result, so retrying after reloading is safe.

The reverse needs no new API: `StateRecord::load` returns the entries, and in version 4 each entry carries its key. `Entry` gains `venue_key: Option<Box<[u8]>>`, where `None` is `-`.

## Compatibility

- **Upgrade, `identity` unset:** no change on disk, on the wire, or in behaviour.
- **Upgrade to `"venue_key"`:** the record is rewritten as version 4 at open. A record with full-width entries is refused until a mapping resolves them.
- **Switching back to `"symbol"`:** a version-4 record is refused at startup (`RefdataError::StateIsKeyed`). Its entries may share a `Symbol`, and a symbol-keyed registry cannot hold them.
- **Rollback to an older build:** that build refuses version 4 as `UnsupportedVersion`, which is what the format tag is for.
- **Wire:** no change. Two published instruments may now carry one `Symbol`, which the spec has always allowed.
- **Consumers in this repository that key on `Symbol`:** `dz-recorder-relower` matches the multicast archive to upstream payloads by `Symbol` (`dz-recorder-events/src/instruments.rs` explains why). For two instruments that share one, that join is ambiguous. That is a limit of the relower to state in its own design, not something this change can fix. Every other consumer keys on `Instrument ID`.
- **Metrics:** no new family. The venue-key refusals are in `Counts` and the exit report, beside `declined_id_unavailable`.

## Refusals

- `Refusal::BadVenueKey`: an offer under `"venue_key"` whose key is empty or over 256 bytes. It is not ordinary, so it is counted like `compose`'s refusals.
- `RecordError::BadVenueKey`, `RecordError::DuplicateVenueKey`, `RecordError::UnresolvedVenueKey`, `RefdataError::UnresolvedVenueKeys`, `RefdataError::StateIsKeyed`, `RefdataError::DerivedKeyWithoutVenueKey`, and the `SeedError`s.

## Tests

- **The failure, fixed:** under `"venue_key"`, two 65-byte keys that differ only in their last byte are two instruments, two IDs and two definitions with one `Symbol`, and each one's quotes reach only its own book. The existing `a_symbol_is_persisted_as_the_wire_field_and_not_as_the_venues_string` stays as written under `"symbol"`.
- **`None` is the untruncated `symbol`:** an adapter that states no key gets the same separation.
- **Round trip:** a version-4 record with `-` entries, explicit keys, a shared `Symbol` and a delisted timestamp encodes and decodes to itself. A record with every key `-` and no shared `Symbol` decodes to the same entries as its version-2 form. A 64-byte key equal to its `Symbol` is written in hex, never `-`.
- **The loader refuses** each malformed key, `-` on a full-width `Symbol`, a duplicate key and a duplicate ID, and accepts a duplicate `Symbol`.
- **Upgrade:** a version-2 record with only padded `Symbol`s, opened under `"venue_key"`, is rewritten as version 4, and every ID is recalled. One with a full-width entry is refused, and opens after a mapping resolves it.
- **The 64/65-byte pair:** a 64-byte key and a 65-byte key that share a `Symbol`, mapped to IDs A and B. Offered in either order, each gets its own ID, and the same IDs after a restart. Unmapped and retired, the full-width entry's ID is issued to neither, and both get new IDs.
- **Switching back** to `"symbol"` over a version-4 record is refused.
- **`"derived_key"`:** two keys with one `Symbol` derive two IDs. It is refused under `"symbol"`. Moving between `"derived"` and `"derived_key"` is refused.
- **Seed:** a real `instruments.json` with tickers that collide at 64 bytes seeds a record in which every ticker has its own ID and `next_id` is carried. Each `SeedError` is tested, including a merge whose incoming ID is absent from the destination and below its `next_id`, after the destination forgot it. A merge into an existing record is the union. A seed, a mint, an export and a re-seed leave no ID naming two keys. The same seed run twice gives the same record. A write that fails after its rename leaves a record that a reload and a second seed bring to the merged result.
- **Forgetting** under `"venue_key"` forgets by key: a forgotten key that is listed again gets a new ID, and a key that shares its `Symbol` with it is unaffected.

## What this does not do

- **It does not change the wire.** The `Symbol` is still truncated to 64 bytes, and a subscriber tells two instruments apart by `Instrument ID`, as it always has to.
- **It does not change the default.** A publisher that never sets `identity` writes and reads what it does today.
- **It does not resolve the relower's join** for instruments that share a `Symbol`.
