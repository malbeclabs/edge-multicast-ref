# Instrument IDs two hosts agree on without talking: plan

Breaks [the design](../specs/2026-10-09-refdata-derived-instrument-ids-design.md) into ordered tasks. Answers asks 2 and 3 of [#181](https://github.com/malbeclabs/edge-multicast-ref/issues/181).

**Base:** `main`.

## Global constraints

- **Vocabulary:** `GLOSSARY.md` governs every identifier, comment, test name, config key and commit message. The new key is `id_allocation`, and its values are `sequential` and `derived`.
- **Every test must be shown to kill its mutant.** Each task names the revert that must turn a named test red. Commit before each round of mutants, and restore from a copy taken for that mutant alone.
- **A sequential publisher sees no change.** It writes version 2, mints from `next_id`, and keeps every existing test green without editing that test's expectations.
- **The invariant is not up for trade.** A published `Instrument ID` always resolves to a published definition, and no ID is ever re-issued. A task that moves a colliding ID, lowers the floor, or derives below it has misread the design.

---

## Tasks

### 1. The derivation

- [ ] `state.rs`: a `const` CRC-32/ISO-HDLC table, stated in full in the source as the design asks, and `pub fn derive_instrument_id(symbol: &[u8; SYMBOL_LEN]) -> u32` over the bytes before the first NUL.
- [ ] Export it from `lib.rs`.

**Test** (`tests/identity.rs`):
- the standard check value, `crc32("123456789") == 0xCBF43926`;
- a symbol's derivation ignores its NUL padding;
- two symbols that differ only past the first NUL derive alike.

**The revert:** drop the final XOR, and the check-value test fails.

---

### 2. The record: version 3

- [ ] `IdAllocation { Sequential, Derived }`, and `StateRecord` gains `allocation`. Under `Derived`, `next_id` is read as the floor.
- [ ] `decode` reads version 3 under the design's rules:
      - base entries below the floor, or equal to their own derivation at or above it;
      - appended lines that restate, or mint their own derivation, at or above the floor and held by no other symbol;
      - each failure is its own `RecordError`. `NotDerived` names the line.
- [ ] `encode` and `encode_line` write version 3 for `Derived`, and version 2 for `Sequential`.
- [ ] Versions 1 and 2 still decode as `Sequential`.
- [ ] A torn final appended line in version 3 is dropped as in version 2.

**Test** (`tests/persistence.rs`):
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

- [ ] `RegistryConfig::id_allocation`.
- [ ] `open` refuses `Derived` with `forget_delisted_after` as `RefdataError::ForgettingUnderDerivedIds`.
- [ ] `open` refuses a version-3 record under `Sequential` as `RefdataError::StateIsDerived`.
- [ ] `open` under `Derived`:
      - takes an older record's `next_id` as the floor;
      - holds a set of IDs in use;
      - compacts at open to rewrite an older record as version 3.

      A cold directory has a floor of `FIRST_INSTRUMENT_ID`.
- [ ] `admit` under `Derived`:
      - a recalled symbol keeps its ID;
      - a new one gets `derive_instrument_id`;
      - `0`, an ID below the floor, or an ID in use is declined as `Refusal::IdUnavailable`, and nothing is persisted;
      - otherwise one minting line is appended, and `next_id` does not move.
- [ ] `Refusal::IdUnavailable`, not ordinary, counted as `Counts::declined_id_unavailable`, with its own arm in `count_refusal`.
- [ ] `Registry::holder_of(id) -> Option<&[u8; SYMBOL_LEN]>`, so the runtime can name the symbol that holds a declined ID.

**Test** (`tests/identity.rs`, `tests/persistence.rs`, over `MemoryStore` and `ManualClock`):
- **Two hosts, one seed, opposite orders.** Two registries opened on copies of one version-2 seed record under `Derived` are offered the seeded symbols and then new ones, one in offer order and the other in reverse. Every symbol gets the same ID in both.
- **The seed survives.** Every seeded symbol keeps its sequential ID after the upgrade to version 3, and the floor equals the seed's `next_id`.
- **A cold derived start** mints the derivation for the very first symbol.
- **Collisions.** A forced collision, made by seeding a record that holds the derivation's value under another symbol, declines the newcomer as `IdUnavailable`. The holder keeps its ID, nothing is appended, and `declined_id_unavailable` is 1.
- A derivation below the floor is declined.
- `Derived` with a horizon is refused at `open`.
- A version-3 record under `Sequential` is refused at `open`.
- A restart under `Derived` recalls every derived ID.

**The revert:** let `admit` fall back to `next_id` on a collision, and the collision test fails. Ignore the floor, and the below-floor test fails. Advance `next_id` on a derived mint, and the floor-equals-seed test fails after the second mint.

---

### 4. The runtime key

- [ ] `[refdata] id_allocation = "sequential" | "derived"`, default `sequential`, carried through `Refdata` into `RegistryConfig`.
- [ ] Load refuses `"derived"` with `forget_delisted_after`, as `StartupError::ForgettingUnderDerivedIds`, beside the existing horizon check. An unknown value is refused by the deserialiser.
- [ ] One `warn!` per declined symbol, naming the symbol and the one that holds the ID. The runtime holds a set so a re-offer does not repeat it.
- [ ] The key's doc comment states the design's rule: every path of a channel starts from the same seed, or all from none.

**Test** (`dz-publisher-runtime` config tests): the default is `sequential`; `"derived"` loads; `"derived"` with a horizon is refused with the key named; an unknown value is refused.

**The revert:** drop the horizon check, and its test fails.

---

### 5. Documents

- [ ] `docs/README.md` lists the design and plan.
- [ ] `BRINGING-UP-A-FEED.md`, where `[refdata]` keys are described, names `id_allocation`, the seed procedure (copy, then open under `derived`), and the one-seed-or-none rule.
