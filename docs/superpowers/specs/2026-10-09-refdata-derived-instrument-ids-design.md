# Instrument IDs two hosts agree on without talking

**Status:** design. Answers asks 2 and 3 of [#181](https://github.com/malbeclabs/edge-multicast-ref/issues/181), in the shape set out in [its first comment](https://github.com/malbeclabs/edge-multicast-ref/issues/181#issuecomment-6088414754). Ask 1, a venue instrument key, is not part of this.

## The failure

The registry mints an `Instrument ID` from `next_id`, in the order the venue first offers each instrument, and persists the map in its own `instruments.state` (`registry.rs`, `admit`). A minted ID is a fact about one process's history, not about the instrument.

Two hosts carrying one channel for redundancy publish under one `Source ID` and one `Channel ID`, and are told apart only by source IP address. A subscriber that fails over between them, or anything that keys instrument state on `(Channel ID, Instrument ID)`, needs both hosts to map every symbol to the same ID. Two sequential records agree only while both hosts see every listing in the same order and forget on the same second. A second host started later starts from `FIRST_INSTRUMENT_ID`, in whatever order the venue offers on the day it starts.

The state directory takes one writer (`registry.rs:262`), so the hosts cannot share a record. They have to agree without one.

## Derived allocation

`[refdata] id_allocation` takes `"sequential"`, which is the default and today's behaviour, or `"derived"`.

Under `"derived"`:

- **A symbol the record holds keeps its ID,** as it does now. That is what makes a seed work (below).
- **A new symbol is minted `crc32(symbol)`.** That is CRC-32/ISO-HDLC (IEEE 802.3, reflected, polynomial `0xEDB88320`, initial and final XOR `0xFFFFFFFF`) over the wire `Symbol` up to its first NUL. It uses the same 64 bytes the record keys on, so two venue tickers that are one symbol on the wire are one input here too. The table is stated in full in the source, so a consumer can recompute an ID from a symbol without asking the publisher.
- **The ID is refused, never moved,** when it is `0`, when it is below the record's **floor**, or when another symbol holds it. The new instrument is declined as `Refusal::IdUnavailable` and counted as `declined_id_unavailable`. The instrument that holds the ID keeps it. Nothing probes to a neighbouring ID, because a probe's answer depends on what that host admitted first, and that is how two hosts diverge.
- **Nothing is forgotten.** `forget_delisted_after` together with `"derived"` is a configuration refusal. A relisting near the horizon would come back under the record's ID on a host that had not yet forgotten it, and under a fresh derivation on one that had. Two hosts forget about one poll apart.

### The floor

`next_id` is the only guard against re-issue (`state.rs`, `StateRecord`). A sequential record may have forgotten entries, so "not held by another symbol" is not enough: a derived ID could land on an ID that was published and later forgotten.

So a record that switches to `"derived"` keeps its `next_id` as a **floor**. No derived ID below it is ever minted. The floor is persisted, and it never moves. For a floor in the low thousands, the chance that a symbol derives below it is about 1 in 3 million.

A directory with no record starts at a floor of `FIRST_INSTRUMENT_ID`. Every ID it mints is derived.

### Collisions

Only derived IDs can collide, since seeded entries sit below the floor. For `n` derived symbols over `2^32` values, the chance of any collision is about `n² / 2^33`: about 1 in 30,000 for five hundred listings, and about 1 in 340 for five thousand.

A collision is declined, not resolved, for the reason above. Two hosts with the same seed then still agree, unless one of the colliding symbols was seen by only one host, for example a listing that came and went while the other was down. That is the one divergence this design leaves. It is counted when it happens. It is not prevented.

## Seeding

A second host is seeded by **copying the first host's `instruments.state` into its state directory before its first start**, and opening it under `"derived"`. The open takes the record's `next_id` as the floor and rewrites the record in the derived layout. From then on every instrument the first host had published keeps its ID, and every new one is derived the same way on both hosts.

The copy can be in any layout the source host wrote. Version 1, version 2 and the derived layout are all read. That is what makes a file copy an import: ask 3 needs no API beyond the open.

**Every path of a channel starts from the same seed, or all of them start from none.** A host started cold under `"derived"` derives IDs for symbols that seeded hosts hold below the floor, and disagrees on all of them. This is stated on the key, because nothing in one process can see it.

**The same seed means the same record at the moment `"derived"` takes over.** A copy goes stale with the source's next sequential mint. A host that will be seeded later takes its copy from a host that already runs `"derived"`, not an old copy of a host still minting sequentially. The floor is the record's `next_id` when it is first opened under `"derived"`, so two hosts seeded from different points in a sequential history differ in their floors as well as in their entries.

The source host need not switch. If it keeps minting sequentially, it agrees with the seeded hosts on everything up to the seed, and **disagrees on every listing after it, by construction**. It mints the next sequential ID and they mint the CRC-32. This is the milder kind of disagreement. Every seeded ID sits below the floor, so no host gives a seeded ID to a different instrument. A derived ID matches one the source minted for another symbol only if its CRC-32 lands among the few IDs the source mints above the floor. But the disagreement on new listings is certain, not possible, and it lasts as long as the source keeps minting sequentially.

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
- **Upgrade, derived:** a version-1 or version-2 record is read, takes its `next_id` as the floor, and is rewritten as version 3 by the compaction at open.
- **Switching back:** a version-3 record opened under `"sequential"` is a startup refusal, `RefdataError::StateIsDerived`. Sequential minting would continue from the floor, straight into IDs the derived mints already hold.
- **Rollback:** a build that reads only version 2 refuses a version-3 record as `UnsupportedVersion`, the refusal the format tag exists for. Converting one back means writing a version-2 base of the same entries, with `next_id` above the highest ID. The design does not provide that, because switching back is refused anyway.
- **Wire:** no change.
- **Metrics:** no new family. The normative label set is closed, and an ID collision is not a `schema` load error. `declined_id_unavailable` is in `Counts`, and the runtime logs one warning per declined symbol, naming the symbol that holds the ID. A metric is a follow-up for the metric specification, not this crate's to invent.

## What changes

- `RegistryConfig` gains `id_allocation: IdAllocation`, `Sequential` or `Derived`. `Registry::open` refuses `Derived` with a horizon as `RefdataError::ForgettingUnderDerivedIds`, for a caller composing the configuration itself.
- `[refdata] id_allocation`. The runtime refuses `"derived"` with `forget_delisted_after` at load, beside the existing horizon check.
- `state.rs`:
  - version 3, with `StateRecord::allocation` and the floor;
  - `decode` and `encode` handle the layout and its rules;
  - a `crc32` stated with its table, and `pub fn derive_instrument_id(symbol: &[u8; SYMBOL_LEN]) -> u32`.
- `Registry`:
  - under `Derived`, `admit` mints the derivation, holds a set of IDs in use, and declines an unavailable one;
  - `open` sets the floor and rewrites an older record as version 3;
  - `next_id` does not advance.
- `Refusal::IdUnavailable`, which is not ordinary, and `Counts::declined_id_unavailable`.

## What this does not do

- **No coordination and no shared state.** Each host keeps one writer.
- **No venue instrument key.** The derivation reads the wire `Symbol`. When ask 1 lands, the input can move to the key, under a new allocation name, since a different input derives different IDs.
- **No change for a sequential publisher,** on disk or on the wire.
