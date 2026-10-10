# Instrument IDs two hosts agree on without talking

**Status:** design. Answers asks 2 and 3 of [#181](https://github.com/malbeclabs/edge-multicast-ref/issues/181), in the shape set out in [its first comment](https://github.com/malbeclabs/edge-multicast-ref/issues/181#issuecomment-6088414754). Ask 1, a venue instrument key, is not part of this.

## The failure

The registry mints an `Instrument ID` from `next_id`, in the order the venue first offers each instrument, and persists the map in its own `instruments.state` (`registry.rs`, `admit`). A minted ID is a fact about one process's history, not about the instrument.

Two hosts carrying one channel for redundancy publish under one `Source ID` and one `Channel ID`, and are told apart only by source IP address. A subscriber that fails over between them, or anything that keys instrument state on `(Channel ID, Instrument ID)`, needs both hosts to map every symbol to the same ID. Two sequential records agree only while both hosts see every listing in the same order and forget on the same second. A second host started later starts from `FIRST_INSTRUMENT_ID`, in whatever order the venue offers on the day it starts.

The state directory takes one writer (`registry.rs:265`), so the hosts cannot share a record. They have to agree without one.

## Derived allocation

`[refdata] id_allocation` takes `"sequential"`, which is the default and today's behaviour, or `"derived"`.

Under `"derived"`:

- **A symbol the record holds keeps its ID,** as it does now. That is what makes a seed work (below).
- **A new symbol is minted `crc32(symbol)`.** That is CRC-32/ISO-HDLC (IEEE 802.3, reflected, polynomial `0xEDB88320`, initial and final XOR `0xFFFFFFFF`) over the 64-byte wire `Symbol` with its trailing NULs removed. The record keys on all 64 bytes, and a ticker with an interior NUL is still admitted (`Fit::Unrepresentable`), so stopping at the first NUL would derive one ID for two distinct record keys and decline the second for ever. Trimming only the padding gives every distinct key its own input, and for an ordinary symbol it is the same bytes.
- **A consumer cannot recompute IDs from symbols.** Seeded symbols keep their sequential IDs below the floor, a declined symbol has no ID, and the floor is not on the wire, so nothing tells a consumer which symbols are derived. A consumer takes every ID from the definitions, as it does now. The table is stated in full in the source so the derivation can be audited, not so it can be relied on downstream.
- **The ID is refused, never moved,** when it is `0`, when it is below the record's **floor**, or when another symbol holds it. The new instrument is declined as `Refusal::IdUnavailable` and counted in `declined_id_unavailable`. Like the other declined counts, that counts offers: a declined symbol is never admitted, so every re-offer is declined and counted again. Each distinct declined symbol is reported once, by `Registry::take_unavailable_ids` (below). The instrument that holds the ID keeps it. Nothing probes to a neighbouring ID, because a probe's answer depends on what that host admitted first, and that is how two hosts diverge.
- **Nothing is forgotten.** `forget_delisted_after` together with `"derived"` is a configuration refusal. A relisting near the horizon would come back under the record's ID on a host that had not yet forgotten it, and under a fresh derivation on one that had. Two hosts forget about one poll apart.

### The floor

`next_id` is the only guard against re-issue (`state.rs`, `StateRecord`). A sequential record may have forgotten entries, so "not held by another symbol" is not enough: a derived ID could land on an ID that was published and later forgotten.

So a record that switches to `"derived"` keeps its `next_id` as a **floor**. No derived ID below it is ever minted. The floor is persisted, and it never moves. For a floor in the low thousands, the chance that a symbol derives below it is about 1 in 3 million.

A directory with no record starts at a floor of `FIRST_INSTRUMENT_ID`. Every ID it mints is derived.

### Collisions

Only derived IDs can collide, since seeded entries sit below the floor. For `n` derived symbols over `2^32` values, the chance of any collision is about `n² / 2^33`: about 1 in 30,000 for five hundred listings, and about 1 in 340 for five thousand.

**A declined collision is still order-dependent.** The holder is whichever of the two symbols a host admitted first. If two hosts first admit colliding symbols A and B in opposite orders, or one host declines A for an unrelated reason (`Capped`, a `compose` refusal, `Unpersistable`) and then admits B, the first host publishes the ID as A and the second as B. One `(Channel ID, Instrument ID)` then names two instruments across the paths, which is worse than an ID that differs.

No rule that keeps the invariant avoids this. A host that has seen only A must publish A or withhold it, and withholding every symbol until no collider could appear withholds everything. Any tie-break that both hosts would compute, such as the lower symbol bytes winning, needs the host holding the losing symbol to retract it, and a published `Instrument ID` always resolves to a published definition. So the design states the divergence rather than preventing it:

- It needs a collision, about `n² / 2^33` over the derived symbols, as above.
- It needs the two colliding symbols first admitted in different orders. Hosts that run together see listings made at different times in the same order, so for them it also needs the two listed within one poll of each other, or one host down or declining at the time.
- Hosts started cold together on an existing catalogue admit it in whatever order each is offered it. For a catalogue of a thousand, the chance it holds any colliding pair is about 1 in 8,600.

When it happens, each host reports the declined symbol and the symbol that holds the ID, so the split is visible on both paths. It is not detected across them.

## Seeding

A second host is seeded in three steps:

1. **Switch the source host to `"derived"` first,** with a restart. Its open takes its own `next_id` as the floor and rewrites its record as version 3.
2. **Copy the source's `instruments.state` into the new host's state directory,** at any time after that and before the new host's first start. No quiesced cutover is needed. Between compactions the record only grows by appended lines, and a compaction replaces it by rename. So a copy taken while the source runs is the record at some moment, and at worst ends in a torn line, which is dropped. Anything the source mints after the copy, the new host derives to the same ID.
3. **Open the new host under `"derived"`.** Every instrument the source had published keeps its ID, and every new one is derived the same way on both hosts.

The copy can be in any layout the source host wrote. Version 1, version 2 and the derived layout are all read. That is what makes a file copy an import: ask 3 needs no API beyond the open.

**Every path of a channel starts from the same seed, or all of them start from none.** A host started cold under `"derived"` derives IDs for symbols that seeded hosts hold below the floor, and disagrees on all of them. This is stated on the key, because nothing in one process can see it.

**The same seed means the same record at the moment `"derived"` takes over.** That is why step 1 comes before step 2. A copy of a host still minting sequentially goes stale with its next mint. A host that will be seeded later takes its copy from a host that already runs `"derived"`, not an old copy of a sequential one. The floor is the record's `next_id` when it is first opened under `"derived"`, so two hosts seeded from different points in a sequential history differ in their floors as well as in their entries.

**A source that cannot switch,** because it is not to be redeployed, can still seed a host. But if it keeps minting sequentially, it agrees with the seeded hosts on everything up to the seed, and **disagrees on every listing after it, by construction**. It mints the next sequential ID and they mint the CRC-32. This is the milder kind of disagreement. Every seeded ID sits below the floor, so no host gives a seeded ID to a different instrument. A derived ID matches one the source minted for another symbol only if its CRC-32 lands among the few IDs the source mints above the floor. But the disagreement on new listings is certain, not possible, and it lasts as long as the source keeps minting sequentially. That is a transition state, ended by retiring or switching the source, not a supported steady state.

## The record

Version 3 is the version-2 layout, with the allocation named in the header:

```
dz-refdata-state 3 <source_id> <floor> <base entries> derived
<instrument_id> <symbol, 128 hex digits>
<instrument_id> <symbol, 128 hex digits> <unix seconds>
...
```

- **The header** carries the floor where version 2 carries `next_id`. Under `"derived"` they are the same number, and it never advances.
- **A base entry** is either below the floor, or equal to `crc32` of its own symbol and at or above it. Anything else is a refusal.
- **An appended line** either restates a known pair, as now, or mints. A mint must carry `crc32` of its own symbol, at or above the floor, held by no other symbol. The loader recomputes the CRC. A mismatch is `RecordError::NotDerived`, a stronger check than the sequential one, which can only confirm that a line names the next number.
- **`IdNotBelowNext` and `AppendedOutOfOrder`** are sequential rules and do not apply to version 3.
- **A torn final line** is dropped exactly as in version 2.

Only `"derived"` writes version 3. `"sequential"` keeps writing version 2, so a publisher that never sets the key sees no change and can still roll back to a build that reads version 2.

## Compatibility

- **Upgrade, sequential:** no change.
- **Upgrade, derived:** a version-1 or version-2 record is read, takes its `next_id` as the floor, and is rewritten as version 3 at open. The rewrite is the mandatory one, through `Loaded::needs_rewrite`, not the best-effort compaction: `compact` leaves the record alone on `StateError::NotReplaced`, and a version-2 record with a derived mint appended to it is refused as `AppendedOutOfOrder` at the next start. So `needs_rewrite` compares the record's version with the allocation's: version 1 or 2 under `"derived"` needs a rewrite, version 3 under `"derived"` does not, and a rewrite that fails is a startup error.
- **Switching back:** a version-3 record opened under `"sequential"` is a startup refusal, `RefdataError::StateIsDerived`. A version-2 record requires every entry below `next_id` (`IdNotBelowNext`). Derived IDs are spread over `2^32`, so `next_id` would have to sit above the highest of them, which leaves almost no ID space to mint into.
- **Rollback:** a build that reads only version 2 refuses a version-3 record as `UnsupportedVersion`, the refusal the format tag exists for. Converting one back means writing a version-2 base of the same entries, with `next_id` above the highest ID. The design does not provide that, because switching back is refused anyway.
- **Wire:** no change.
- **Metrics:** no new family. The normative label set is closed, and an ID collision is not a `schema` load error. `declined_id_unavailable` is in `Counts`, and the runtime logs one line per distinct declined symbol, naming the symbol and its derived ID, and the symbol that holds the ID when one does. An ID that is `0` or below the floor has no holder. A metric is a follow-up for the metric specification, not this crate's to invent.

## What changes

- `RegistryConfig` gains `id_allocation: IdAllocation`, `Sequential` or `Derived`. `Registry::open` refuses `Derived` with a horizon as `RefdataError::ForgettingUnderDerivedIds`, for a caller composing the configuration itself.
- `[refdata] id_allocation`. The runtime refuses `"derived"` with `forget_delisted_after` at load, beside the existing horizon check.
- `state.rs`:
  - version 3, with `StateRecord::allocation` and the floor;
  - `decode` and `encode` handle the layout and its rules;
  - a `crc32` stated with its table, and `pub fn derive_instrument_id(symbol: &[u8; SYMBOL_LEN]) -> u32`.
- `Registry`:
  - under `Derived`, `admit` mints the derivation and declines an unavailable one. One `HashMap<u32, SymbolKey>` of IDs in use answers both whether an ID is held and by whom;
  - each declined symbol is remembered once, with its ID and its holder if any, and handed out by `take_unavailable_ids`, the shape of `take_unknown_shards`. Adapters reach the registry through `ListingSink::list_on`, which answers nothing, and `Refusal` carries no symbol, so the runtime cannot learn this any other way;
  - `open` sets the floor and rewrites an older record as version 3 through `needs_rewrite`;
  - `next_id` does not advance.
- `Refusal::IdUnavailable`, which is not ordinary, and `Counts::declined_id_unavailable`.

## What this does not do

- **No coordination and no shared state.** Each host keeps one writer.
- **No venue instrument key.** The record already keys on the wire `Symbol` today, which the glossary keeps for display and filtering and would not choose as a key. This design leaves that key alone and only changes how a new ID is minted for it. The derivation reads the same key. When ask 1 lands, the input can move to the key, under a new allocation name, since a different input derives different IDs.
- **No change for a sequential publisher,** on disk or on the wire.
