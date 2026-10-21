# An instrument recalled by its venue key: plan

Breaks [the design](../specs/2026-10-20-refdata-venue-key-design.md) into ordered tasks. Answers ask 1 of [#181](https://github.com/malbeclabs/edge-multicast-ref/issues/181).

**Base:** `main`.

## Global constraints

- **Vocabulary:** `GLOSSARY.md` governs every identifier, comment, test name, config key and commit message. The new terms are `venue_key`, `[refdata] identity` with the values `symbol` and `venue_key`, `[refdata] venue_keys` with the values `symbol` and `stated`, and `id_allocation = "derived_key"`. `Symbol` is never called a key in new text.
- **Every new test must be shown to fail under a mutation of the code it covers.** Each task names the revert for its central test. The others are found during the task and listed in the PR.
- **`identity = "symbol"` sees no change in behaviour.** It writes version 2 or 3, recalls by the padded `Symbol`, and every existing test stays green without changing what it asserts. The API changes: `InstrumentSpec`, `RegistryConfig` and `Entry` each gain a public field, so every struct literal gains it too. Those are the 15 files that build `InstrumentSpec` (including `dz-adapter-uds/src/lib.rs` and both recorder test harnesses) and the 22 `RegistryConfig` literals. The PR lists out-of-tree callers that must add the field.
- **The invariant is not up for trade.** No `Instrument ID` names two venue keys, none is re-issued, and a published ID always resolves to a published definition.

---

## Tasks

### 1. The boundary and the configuration

- [ ] `dz-adapter-core`: `InstrumentSpec::venue_key: Option<&'a str>`. The doc comment says `None` is the stated `symbol`, untruncated, and that a key holds no NUL.
- [ ] `dz-publisher-refdata`: `Identity { Symbol, VenueKey }` (`#[non_exhaustive]`, default `Symbol`), and `RegistryConfig::identity`. `VenueKeys { Symbol, Stated }` (`#[non_exhaustive]`, default `Symbol`), and `RegistryConfig::venue_keys`; `Stated` under `Identity::Symbol` is `RefdataError::VenueKeysWithoutVenueKey`.
- [ ] `dz-publisher-runtime`: `[refdata] identity`, with the values spelled `symbol` and `venue_key`, and `[refdata] venue_keys`, with `symbol` and `stated` (`rename_all = "snake_case"`). Both are passed through to `RegistryConfig`.

**Test:** the configuration document accepts both values of each key and refuses a third. Absent means `symbol`. `venue_keys = "stated"` with `identity = "symbol"` is refused.

### 2. The record: version 4

- [ ] `state.rs`: `FORMAT_VERSION_KEYED = 4`, the header's allocation and `venue_keys` fields, and the third line field (hex or `-`). `Entry::venue_key: Option<Box<[u8]>>`.
- [ ] Loader rules: `BadVenueKey` (including a key holding a NUL), `DuplicateVenueKey`, `UnresolvedVenueKey` (`-` on a `Symbol` with fewer than four bytes of padding, which includes a full-width one, or with a NUL before its padding), and a duplicate `Symbol` allowed in version 4 only. A key of 61 bytes or more is always encoded in hex. The sequential and derived line rules are carried over unchanged.
- [ ] `Loaded::needs_rewrite` takes `identity` into account.

**Test:** the round trips and refusals in the design's Tests section. **The revert:** keep `DuplicateSymbol` for version 4, and the shared-`Symbol` round trip fails. A second revert: allow `-` on any padded `Symbol`, and the loader test for 63 ASCII bytes and one NUL fails.

### 3. Recall on the key

- [ ] `Registry`: `handles` and `minted` keyed on `Box<[u8]>` and looked up by `&[u8]`. `in_use` stays `HashMap<u32, _>`, from ID to holder, and only its value type becomes the key, so the derived check in `admit` (is the ID held, and by whom) and `remember_unavailable` keep their direction. Under `Symbol`, the key is the 64 padded bytes, so recall and on-disk behaviour are what they are today. Under `VenueKey`, the key is `venue_key.unwrap_or(symbol)`.
- [ ] `offer` validates the key (`Refusal::BadVenueKey`, and `Refusal::VenueKeyNotDeclared` for a key differing from its `symbol` under `VenueKeys::Symbol`) before the lookup. `symbol_field` is used only for composing the definition.
- [ ] `forgettable`, compaction and `take_unavailable_ids` work on the key, and `take_unavailable_ids` also reports the `Symbol`.
- [ ] Opening a version 1, 2 or 3 record under `VenueKey` rewrites it as version 4 when no entry is unresolved. Under `VenueKeys::Symbol`, an entry is unresolved when `-` is not allowed on it: fewer than four bytes of padding, or a NUL before them. Under `VenueKeys::Stated`, every entry is. An unresolved entry refuses the open as `UnresolvedVenueKeys` until `seed` resolves it. Nothing is claimed by offer order.
- [ ] Opening a version-4 record under `Symbol` is `RefdataError::StateIsKeyed`, and under the other `venue_keys` than its header names, `RefdataError::VenueKeysChanged`.

**Test:** the 65-byte pair; the 64/65-byte pair mapped, in both offer orders and across a restart; `None` as the untruncated symbol; the upgrade refused until it is resolved; a multi-byte character across byte 64 (63 ASCII bytes and `é`, and a 4-byte character from byte 61) refused at open and recalled only once mapped; under `Stated`, a record of short padded `Symbol`s refused until every entry is mapped; `VenueKeyNotDeclared`; `VenueKeysChanged` both ways; switching back; and forgetting by key. **The revert:** look up `handles` by `symbol_field(spec.symbol)` under `VenueKey`, and the 65-byte test fails. A second revert: give `-` to every short padded entry under `Stated`, and the stated-keys upgrade test opens when it must not.

### 4. `"derived_key"`

- [ ] `IdAllocation::DerivedKey`: the CRC-32 of the key bytes, through the existing table. The floor, decline and seeding rules are shared with `Derived`.
- [ ] Refusals: `DerivedKeyWithoutVenueKey` under `Symbol`; a change between `Derived` and `DerivedKey`; `forget_delisted_after` under `DerivedKey`.

**Test:** two keys with one `Symbol` derive two IDs, and each refusal is tested. **The revert:** hash the `Symbol` under `DerivedKey`, and the first test fails.

### 5. `seed`

- [ ] `pub fn seed(store, Seed { source_id, next_id, entries, retire_unmapped, venue_keys })`, where `entries` are `(venue_key, instrument_id)` pairs. It merges as the design states, including `IdMayBeForgotten` below the record's `next_id` and the resolution of unresolved entries under the `venue_keys` it is given (by ID alone under `Stated`). It writes a single base through `StateStore::store`, and returns `SeedError` for each refusal.
- [ ] On a derived record, `seed` accepts a resolve-only seed (mapped unresolved entries the record holds, no new entries, no `retire_unmapped`, `next_id` equal to the floor) and writes the record back with its allocation and floor unchanged. Every other seed on a derived record is `SeedError::RecordIsDerived`.
- [ ] Its documentation states the outcome of a failed write: `NotReplaced` leaves the old record; `Write` may follow the rename, so reload before acting. A repeated seed is a no-op.
- [ ] `BRINGING-UP-A-FEED.md`: how a publisher moving from its own registry seeds the record, and that seeding comes before `"derived_key"`.

**Test:** a fixture `instruments.json` with tickers that collide at 64 bytes; each `SeedError`, including an absent ID below the destination's `next_id` after it was forgotten; merge as union; idempotence; a write that fails after its rename (`MemoryStore::break_flushes`), followed by a reload and a re-seed; seed, mint, export and re-seed with no ID naming two keys; and on a derived record with a full-width entry, a resolve-only seed accepted and the open succeeding after it, beside a seed with a new entry, one with `retire_unmapped`, one naming an ID the record does not hold and one moving the floor, each refused with the record unchanged. **The revert:** drop the existing-key check from the merge, and the conflict test fails. A second revert: refuse every seed on a derived record, and the resolve-only test fails.

### 6. Release

- [ ] `RELEASING.md` conventions: a minor version, and notes naming the three new struct fields and `[refdata] identity`.
