# Instrument IDs two hosts agree on without talking: plan

Breaks [the design](../specs/2026-10-09-refdata-derived-instrument-ids-design.md) into ordered tasks. Answers asks 2 and 3 of [#181](https://github.com/malbeclabs/edge-multicast-ref/issues/181).

**Base:** `main`.

## Global constraints

- **Vocabulary:** `GLOSSARY.md` governs every identifier, comment, test name, config key and commit message. The new key is `id_allocation`, and its values are `sequential` and `derived`.
- **Every new test must be shown to fail under a mutation of the code it covers.** Each task names the revert for its central test. The mutation for each of the others is found and run during the task, and listed in the PR. Commit before each round of mutants, and restore from a copy taken for that mutant alone.
- **A sequential publisher sees no change in behaviour.** It writes version 2, mints from `next_id`, and keeps every existing test green without changing what that test asserts. The API does change: `StateRecord` and `RegistryConfig` each gain a public field, so every struct literal of them gains it too. That covers `tests/identity.rs` at lines 276, 306, 380 and 385, and `examples/loopback_publisher.rs:281`. An out-of-tree caller that builds either, such as the seeder #181 describes, must add the field. The PR says so.
- **The invariant is not up for trade.** A published `Instrument ID` always resolves to a published definition, and no ID is ever re-issued. A task that moves a colliding ID, lowers the floor, or derives below it has misread the design.

---

## Tasks

### 1. The derivation

- [x] `state.rs`: a `const` CRC-32/ISO-HDLC table, stated in full in the source as the design asks, and `pub fn derive_instrument_id(symbol: &[u8; SYMBOL_LEN]) -> u32` over the 64 bytes with trailing NULs removed. An interior NUL is part of the input.
- [x] Export it from `lib.rs`.

**Test** (`tests/derived.rs`):
- the standard check value, `crc32("123456789") == 0xCBF43926`;
- a symbol's derivation ignores its trailing NUL padding;
- two symbols that differ only past an interior NUL derive differently.

**The revert:** drop the final XOR, and the check-value test fails. Stop at the first NUL, and the interior-NUL test fails.

---

### 2. The record: version 3

- [x] `IdAllocation { Sequential, Derived }`, and `StateRecord` gains `allocation`. Under `Derived`, `next_id` is read as the floor.
- [x] `decode` reads version 3 under the design's rules:
      - base entries below the floor, or equal to their own derivation at or above it;
      - appended lines that restate, or mint their own derivation, at or above the floor and held by no other symbol;
      - each failure is its own `RecordError`. `NotDerived` names the line.
- [x] `encode` writes version 3 for `Derived`, and version 2 for `Sequential`. `encode_line` writes no version and is unchanged, since a line has the same layout in both.
- [x] Versions 1 and 2 still decode as `Sequential`.
- [x] A torn final appended line in version 3 is dropped as in version 2.

**Test** (`tests/derived.rs`):
- a version-3 round trip;
- a seeded base below the floor, with derived entries above it, decodes;
- an appended mint whose ID is not its symbol's derivation is `NotDerived`;
- a mint below the floor is refused;
- a mint held by another symbol is refused;
- a torn version-3 tail is dropped;
- a version-2 record still decodes as `Sequential`, with every existing persistence test unchanged.

**The revert:** skip the recomputation in `decode`, and the `NotDerived` test fails.

---

### 3. The registry under `Derived`

- [x] `RegistryConfig::id_allocation`.
- [x] `open` refuses `Derived` with `forget_delisted_after` as `RefdataError::ForgettingUnderDerivedIds`.
- [x] `open` refuses a version-3 record under `Sequential` as `RefdataError::StateIsDerived`.
- [x] `open` under `Derived`:
      - takes an older record's `next_id` as the floor;
      - holds one `HashMap<u32, SymbolKey>` of IDs in use, which answers both whether an ID is held and by whom;
      - rewrites an older record as version 3 through the mandatory `needs_rewrite` path, not `compact`. `compact` gives up on `StateError::NotReplaced`, which would leave a version-2 record to take a derived mint and be refused as `AppendedOutOfOrder` at the next start. `Loaded::needs_rewrite` becomes allocation-aware: version 1 or 2 under `Derived` needs a rewrite, version 3 under `Derived` does not, and a failed rewrite is a startup error.

      A cold directory has a floor of `FIRST_INSTRUMENT_ID`.
- [x] `admit` under `Derived`:
      - a recalled symbol keeps its ID;
      - a new one gets `derive_instrument_id`;
      - `0`, an ID below the floor, or an ID in use is declined as `Refusal::IdUnavailable`, and nothing is persisted;
      - otherwise one minting line is appended, and `next_id` does not move.
- [x] `Refusal::IdUnavailable`, not ordinary, counted as `Counts::declined_id_unavailable`, with its own arm in `count_refusal`.
- [x] `Registry::take_unavailable_ids() -> Vec<UnavailableId>`, the shape of `take_unknown_shards`. The registry remembers each declined symbol once, with its derived ID and the holder if there is one (an ID that is `0` or below the floor has none), and hands each out once. Adapters reach the registry through `ListingSink::list_on`, which answers nothing, and `Refusal` is `Copy` and carries no symbol, so the runtime has no other way to learn which symbol was declined.

**Test** (`tests/derived.rs`, over `MemoryStore` and `ManualClock`):
- **Two hosts, one seed, opposite orders.** Two registries opened on copies of one version-2 seed record under `Derived` are offered the seeded symbols and then new ones, one in offer order and the other in reverse. Every symbol gets the same ID in both.
- **The seed survives.** Every seeded symbol keeps its sequential ID after the upgrade to version 3, and the floor equals the seed's `next_id`.
- **A cold derived start** mints the derivation for the very first symbol.
- **Collisions.** A real CRC-32 collision, `plumless` and `buckeroo`, both `0x4DDB0C25`, declines the newcomer as `IdUnavailable`. The holder keeps its ID, nothing is appended, `declined_id_unavailable` is 1 after a single offer and 2 after a second, and `take_unavailable_ids` reports the newcomer once, naming the holder.
- **The collision divergence is pinned, not hidden.** Two cold hosts offered a real colliding pair in opposite orders each keep the symbol they saw first. The test states the divergence the design accepts, so a change that claims to remove it has to change the test.
- **A seed on a full disk.** A version-2 seed opened under `Derived` over a store that refuses replacement fails to open, rather than starting on version 2.
- A derivation below the floor is declined.
- `Derived` with a horizon is refused at `open`.
- A version-3 record under `Sequential` is refused at `open`.
- A restart under `Derived` recalls every derived ID.

**The revert:** let `admit` fall back to `next_id` on a collision, and the collision test fails. Ignore the floor, and the below-floor test fails. Rewrite through `compact` instead of `needs_rewrite`, and the full-disk test fails. Advance `next_id` on a derived mint, and the floor-equals-seed test fails after the second mint.

---

### 4. The runtime key

- [x] `[refdata] id_allocation = "sequential" | "derived"`, default `sequential`, carried through `Refdata` into `RegistryConfig`.
- [x] Load refuses `"derived"` with `forget_delisted_after`, as `StartupError::ForgettingUnderDerivedIds`, beside the existing horizon check. An unknown value is refused by the deserialiser.
- [x] One stderr line per declined symbol, from `take_unavailable_ids`, naming the symbol, its derived ID, and the symbol that holds the ID when one does. The registry deduplicates, so a re-offer does not repeat it.
- [x] The key's doc comment states the design's rule: every path of a channel starts from the same seed, or all from none.

**Test** (`dz-publisher-runtime/tests/config_document.rs`): the default is `sequential`; `"derived"` loads; `"derived"` with a horizon is refused with the key named; an unknown value is refused.

**The revert:** drop the horizon check, and its test fails.

---

### 5. Documents

- [x] `docs/README.md` lists the design and plan.
- [x] `BRINGING-UP-A-FEED.md`, where `[refdata]` keys are described, names `id_allocation`, the seed procedure (copy, then open under `derived`), and the one-seed-or-none rule.
