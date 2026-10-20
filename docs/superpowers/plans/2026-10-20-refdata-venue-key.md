# An instrument recalled by its venue key: plan

Breaks [the design](../specs/2026-10-20-refdata-venue-key-design.md) into ordered tasks. Answers ask 1 of [#181](https://github.com/malbeclabs/edge-multicast-ref/issues/181).

**Base:** `main`.

## Global constraints

- **Vocabulary:** `GLOSSARY.md` governs every identifier, comment, test name, config key and commit message. The new terms are `venue_key`, `[refdata] identity` with the values `symbol` and `venue_key`, and `id_allocation = "derived_key"`. `Symbol` is never called a key in new text.
- **Every new test must be shown to fail under a mutation of the code it covers.** Each task names the revert for its central test. The others are found during the task and listed in the PR.
- **`identity = "symbol"` sees no change in behaviour.** It writes version 2 or 3, recalls by the padded `Symbol`, and every existing test stays green without changing what it asserts. The API changes: `InstrumentSpec`, `RegistryConfig` and `Entry` each gain a public field, so every struct literal gains it too. Those are the 15 files that build `InstrumentSpec` (including `dz-adapter-uds/src/lib.rs` and both recorder test harnesses) and the 22 `RegistryConfig` literals. The PR lists out-of-tree callers that must add the field.
- **The invariant is not up for trade.** No `Instrument ID` names two venue keys, none is re-issued, and a published ID always resolves to a published definition.

---

## Tasks

### 1. The boundary and the configuration

- [ ] `dz-adapter-core`: `InstrumentSpec::venue_key: Option<&'a str>`. The doc comment says `None` is the stated `symbol`, untruncated.
- [ ] `dz-publisher-refdata`: `Identity { Symbol, VenueKey }` (`#[non_exhaustive]`, default `Symbol`), and `RegistryConfig::identity`.
- [ ] `dz-publisher-runtime`: `[refdata] identity`, with the values spelled `symbol` and `venue_key` (`rename_all = "snake_case"`). It is passed through to `RegistryConfig`.

**Test:** the configuration document accepts both values and refuses a third. Absent means `symbol`.

### 2. The record: version 4

- [ ] `state.rs`: `FORMAT_VERSION_KEYED = 4`, the header's allocation field, and the third line field (hex or `-`). `Entry::venue_key: Option<Box<[u8]>>`.
- [ ] Loader rules: `BadVenueKey`, `DuplicateVenueKey`, and a duplicate `Symbol` allowed in version 4 only. The sequential and derived line rules are carried over unchanged.
- [ ] `Loaded::needs_rewrite` takes `identity` into account.

**Test:** the round trips and refusals in the design's Tests section. **The revert:** keep `DuplicateSymbol` for version 4, and the shared-`Symbol` round trip fails.

### 3. Recall on the key

- [ ] `Registry`: `handles`, `minted` and `in_use` keyed on `Box<[u8]>` and looked up by `&[u8]`. Under `Symbol`, the key is the 64 padded bytes, so recall and on-disk behaviour are what they are today. Under `VenueKey`, the key is `venue_key.unwrap_or(symbol)`.
- [ ] `offer` validates the key (`Refusal::BadVenueKey`) before the lookup. `symbol_field` is used only for composing the definition.
- [ ] `forgettable`, compaction and `take_unavailable_ids` work on the key, and `take_unavailable_ids` also reports the `Symbol`.
- [ ] Opening a version 1, 2 or 3 record under `VenueKey` rewrites it as version 4. Full-width `-` entries are claimable. A claim appends the explicit key and counts `claimed_by_venue_key`.
- [ ] Opening a version-4 record under `Symbol` is `RefdataError::StateIsKeyed`.

**Test:** the 65-byte pair, `None` as the untruncated symbol, the upgrade and claim cases, switching back, and forgetting by key. **The revert:** look up `handles` by `symbol_field(spec.symbol)` under `VenueKey`, and the 65-byte test fails.

### 4. `"derived_key"`

- [ ] `IdAllocation::DerivedKey`: the CRC-32 of the key bytes, through the existing table. The floor, decline and seeding rules are shared with `Derived`.
- [ ] Refusals: `DerivedKeyWithoutVenueKey` under `Symbol`; a change between `Derived` and `DerivedKey`; `forget_delisted_after` under `DerivedKey`.

**Test:** two keys with one `Symbol` derive two IDs, and each refusal is tested. **The revert:** hash the `Symbol` under `DerivedKey`, and the first test fails.

### 5. `seed`

- [ ] `pub fn seed(store, Seed { source_id, next_id, entries })`, where `entries` are `(venue_key, instrument_id)` pairs. It merges as the design states, writes a single base through `replace`, and returns `SeedError` for each refusal.
- [ ] `BRINGING-UP-A-FEED.md`: how a publisher moving from its own registry seeds the record, and that seeding comes before `"derived_key"`.

**Test:** a fixture `instruments.json` with tickers that collide at 64 bytes; each `SeedError`; merge as union; and seed, mint, export and re-seed with no ID naming two keys. **The revert:** drop the existing-key check from the merge, and the conflict test fails.

### 6. Release

- [ ] `RELEASING.md` conventions: a minor version, and notes naming the three new struct fields and `[refdata] identity`.
