//! Seeding a record from another publisher's `Instrument ID`s.
//!
//! What a publisher moving onto this runtime from its own registry needs: its
//! `(ticker, id)` pairs written as a keyed record, merged with whatever is in
//! the directory, and every disagreement refused rather than resolved by a
//! guess. No `Instrument ID` may come to name two keys, and none may be issued
//! twice.

use std::collections::HashMap;
use std::time::Duration;

use dz_adapter_core::{
    AssetClass, InstrumentSpec, ListingSink, MarketModel, PriceBound, Scalar, SettleType,
};
use dz_publisher_lowering::SourceId;
use dz_publisher_refdata::{
    derive_instrument_id, seed, symbol_field, CycleSchedule, IdAllocation, Identity, ManualClock,
    MemoryStore, RefdataError, Registry, RegistryConfig, Seed, SeedError, SelectionPolicy,
    ShardConfig, StateError, StateRecord, StateStore, VenueKeys, COMPACTION_FLOOR,
};

const SOURCE_ID: u16 = 7;

fn spec<'a>(symbol: &'a str, venue_key: Option<&'a str>) -> InstrumentSpec<'a> {
    InstrumentSpec {
        symbol,
        venue_key,
        leg1: None,
        leg2: None,
        asset_class: AssetClass::PredictionBinary,
        price_exponent: -4,
        qty_exponent: 0,
        market_model: MarketModel::Clob,
        tick_size: Scalar::text("0.0001"),
        lot_size: Scalar::text("1"),
        contract_value: None,
        quoted_per_contract: None,
        expiry_ns: None,
        settle_type: SettleType::Cash,
        price_bound: PriceBound::UnitInterval,
    }
}

fn config(
    identity: Identity,
    venue_keys: VenueKeys,
    id_allocation: IdAllocation,
) -> RegistryConfig {
    RegistryConfig {
        source_id: SourceId::new(SOURCE_ID).expect("7 is an assigned production id"),
        shards: vec![ShardConfig::default_shard(3)],
        selection: SelectionPolicy::from_seed(4096).expect("4096 is a seed"),
        schedule: CycleSchedule::new(Duration::from_secs(30), 1232, 8),
        forget_delisted_after: None,
        id_allocation,
        identity,
        venue_keys,
    }
}

fn keyed() -> RegistryConfig {
    config(
        Identity::VenueKey,
        VenueKeys::Symbol,
        IdAllocation::Sequential,
    )
}

fn stated() -> RegistryConfig {
    config(
        Identity::VenueKey,
        VenueKeys::Stated,
        IdAllocation::Sequential,
    )
}

fn opened<S: StateStore>(store: S, config: RegistryConfig) -> Registry<S, ManualClock> {
    let mut registry =
        Registry::open(config, store, ManualClock::new()).expect("the directory is usable");
    registry.seeding_complete();
    registry
}

fn id_of<S: StateStore>(
    registry: &mut Registry<S, ManualClock>,
    symbol: &str,
    venue_key: Option<&str>,
) -> Option<u32> {
    let handle = registry.list(&spec(symbol, venue_key))?;
    Some(
        registry
            .definition(handle)
            .expect("published")
            .instrument_id,
    )
}

/// A ticker of `len` bytes: `prefix`, then `x` up to the length.
fn ticker(prefix: &str, len: usize) -> String {
    let mut out = prefix.to_owned();
    while out.len() < len {
        out.push('x');
    }
    out
}

fn pairs(entries: &[(&str, u32)]) -> Vec<(String, u32)> {
    entries
        .iter()
        .map(|(key, id)| ((*key).to_owned(), *id))
        .collect()
}

fn a_seed(next_id: u32, entries: &[(&str, u32)]) -> Seed {
    Seed {
        source_id: SOURCE_ID,
        next_id,
        entries: pairs(entries),
        retire_unmapped: false,
        venue_keys: VenueKeys::Symbol,
    }
}

/// An unkeyed record holding `symbols` as IDs 1, 2, 3 ..., written by a
/// publisher under the symbol identity and `allocation`.
fn unkeyed_record(symbols: &[&str], allocation: IdAllocation) -> Vec<u8> {
    let store = MemoryStore::new();
    let mut first = opened(
        store.clone(),
        config(Identity::Symbol, VenueKeys::Symbol, allocation),
    );
    for symbol in symbols {
        first.list(&spec(symbol, None)).expect("admitted");
    }
    drop(first);
    store.record().expect("a record")
}

fn holding(bytes: &[u8]) -> MemoryStore {
    let store = MemoryStore::new();
    store.set_record(bytes.to_vec());
    store
}

fn record_of(store: &MemoryStore) -> StateRecord {
    StateRecord::decode(&store.record().expect("a record")).expect("it reads back")
}

/// Every `(key, id)` the record holds, checking on the way that no ID names
/// two keys and no key two IDs.
fn mapping(record: &StateRecord) -> HashMap<Vec<u8>, u32> {
    let mut by_key = HashMap::new();
    let mut by_id = HashMap::new();
    for entry in &record.entries {
        assert!(
            by_key
                .insert(entry.key().to_vec(), entry.instrument_id)
                .is_none(),
            "a key held twice"
        );
        assert!(
            by_id
                .insert(entry.instrument_id, entry.key().to_vec())
                .is_none(),
            "an ID naming two keys"
        );
    }
    by_key
}

/// Kalshi's shipped `instruments.json`, as a [`Seed`].
fn instruments_json() -> Seed {
    let text = include_str!("fixtures/instruments.json");
    let value: serde_json::Value = serde_json::from_str(text).expect("the fixture is JSON");
    let next_id = u32::try_from(value["next_id"].as_u64().expect("next_id")).expect("a u32");
    let entries = value["tickers"]
        .as_object()
        .expect("tickers")
        .iter()
        .map(|(ticker, id)| {
            (
                ticker.clone(),
                u32::try_from(id.as_u64().expect("an id")).expect("a u32"),
            )
        })
        .collect();
    Seed {
        source_id: SOURCE_ID,
        next_id,
        entries,
        retire_unmapped: false,
        venue_keys: VenueKeys::Symbol,
    }
}

// ---------------------------------------------------------------------------
// A shipped registry, imported
// ---------------------------------------------------------------------------

#[test]
fn an_instruments_json_whose_tickers_collide_at_64_bytes_seeds_one_id_per_ticker() {
    let imported = instruments_json();
    let symbols: Vec<_> = imported
        .entries
        .iter()
        .map(|(ticker, _)| symbol_field(ticker).0)
        .collect();
    let shared = symbols
        .iter()
        .filter(|symbol| symbols.iter().filter(|other| other == symbol).count() > 1)
        .count();
    assert_eq!(shared, 3, "the fixture has three tickers on one Symbol");

    let store = MemoryStore::new();
    seed(store.clone(), &imported).expect("seeded");
    let record = record_of(&store);
    assert_eq!(record.next_id, 11, "next_id carried");
    assert_eq!(record.keyed, Some(VenueKeys::Symbol));
    let held = mapping(&record);
    for (ticker, id) in &imported.entries {
        assert_eq!(held.get(ticker.as_bytes()), Some(id), "{ticker}");
    }

    let mut registry = opened(store, keyed());
    for (ticker, id) in &imported.entries {
        assert_eq!(id_of(&mut registry, ticker, None), Some(*id), "{ticker}");
    }
    assert_eq!(id_of(&mut registry, "KXNEW", None), Some(11));
}

// ---------------------------------------------------------------------------
// Resolving an unkeyed record
// ---------------------------------------------------------------------------

#[test]
fn a_full_width_entry_opens_once_a_mapping_resolves_it() {
    let full = ticker("F", 64);
    let longer = format!("{full}-RESOLVED");
    let store = holding(&unkeyed_record(&["AAA", &full], IdAllocation::Sequential));
    assert!(matches!(
        Registry::open(keyed(), store.clone(), ManualClock::new()),
        Err(RefdataError::UnresolvedVenueKeys { .. })
    ));

    seed(store.clone(), &a_seed(3, &[(&longer, 2)])).expect("resolved");
    let mut registry = opened(store, keyed());
    assert_eq!(id_of(&mut registry, &longer, None), Some(2));
    assert_eq!(id_of(&mut registry, "AAA", None), Some(1));
    // The 64-byte ticker itself was not what ID 2 named.
    assert_eq!(id_of(&mut registry, &full, None), Some(3));
}

#[test]
fn a_multi_byte_character_cut_at_byte_64_is_recalled_only_once_mapped() {
    for (prefix_len, character) in [(63, "é"), (61, "🦀")] {
        let short = ticker("C", prefix_len);
        let long = format!("{short}{character}");
        assert_eq!(symbol_field(&long).0, symbol_field(&short).0);
        let store = holding(&unkeyed_record(&[&long], IdAllocation::Sequential));
        assert!(matches!(
            Registry::open(keyed(), store.clone(), ManualClock::new()),
            Err(RefdataError::UnresolvedVenueKeys { .. })
        ));

        seed(store.clone(), &a_seed(2, &[(&long, 1)])).expect("resolved");
        let mut registry = opened(store, keyed());
        assert_eq!(id_of(&mut registry, &long, None), Some(1), "{character}");
        assert_eq!(
            id_of(&mut registry, &short, None),
            Some(2),
            "the cut ticker offered alone is another instrument"
        );
    }
}

#[test]
fn stated_keys_open_an_unkeyed_record_once_every_entry_is_mapped() {
    let store = holding(&unkeyed_record(&["AAA", "BBB"], IdAllocation::Sequential));
    let stated_seed = |entries: &[(&str, u32)]| Seed {
        venue_keys: VenueKeys::Stated,
        ..a_seed(3, entries)
    };
    let partial = seed(store.clone(), &stated_seed(&[("venue-a", 1)])).unwrap_err();
    assert!(
        matches!(&partial, SeedError::Unresolved { entries } if entries.len() == 1 && entries[0].0 == 2),
        "{partial}"
    );

    seed(
        store.clone(),
        &stated_seed(&[("venue-a", 1), ("venue-b", 2)]),
    )
    .expect("every entry mapped");
    let record = record_of(&store);
    assert_eq!(record.keyed, Some(VenueKeys::Stated));
    assert_eq!(
        record.entries[0].symbol,
        symbol_field("AAA").0,
        "an entry keeps its Symbol"
    );

    let mut registry = opened(store, stated());
    assert_eq!(id_of(&mut registry, "AAA", Some("venue-a")), Some(1));
    assert_eq!(id_of(&mut registry, "BBB", Some("venue-b")), Some(2));
}

#[test]
fn the_64_and_65_byte_pair_mapped_keep_their_ids_in_either_order_and_across_a_restart() {
    let sixty_four = ticker("KXNFLGAME-", 64);
    let sixty_five = format!("{sixty_four}Z");
    for order in [[&sixty_four, &sixty_five], [&sixty_five, &sixty_four]] {
        let store = holding(&unkeyed_record(&[&sixty_four], IdAllocation::Sequential));
        seed(
            store.clone(),
            &a_seed(3, &[(&sixty_four, 1), (&sixty_five, 2)]),
        )
        .expect("mapped");
        let mut registry = opened(store.clone(), keyed());
        let first = id_of(&mut registry, order[0], None).expect("admitted");
        let second = id_of(&mut registry, order[1], None).expect("admitted");
        assert_eq!(
            [first, second],
            if order[0] == &sixty_four {
                [1, 2]
            } else {
                [2, 1]
            }
        );
        drop(registry);
        let mut reopened = opened(store, keyed());
        assert_eq!(id_of(&mut reopened, &sixty_four, None), Some(1));
        assert_eq!(id_of(&mut reopened, &sixty_five, None), Some(2));
    }
}

#[test]
fn a_retired_full_width_entry_gives_its_id_to_neither_key() {
    let sixty_four = ticker("KXNFLGAME-", 64);
    let sixty_five = format!("{sixty_four}Z");
    let store = holding(&unkeyed_record(&[&sixty_four], IdAllocation::Sequential));
    seed(
        store.clone(),
        &Seed {
            retire_unmapped: true,
            ..a_seed(1, &[])
        },
    )
    .expect("retired");
    assert!(record_of(&store).entries.is_empty());
    assert_eq!(record_of(&store).next_id, 2, "the retired ID stays spent");

    let mut registry = opened(store, keyed());
    let a = id_of(&mut registry, &sixty_four, None).expect("admitted");
    let b = id_of(&mut registry, &sixty_five, None).expect("admitted");
    assert!(a != 1 && b != 1 && a != b, "{a} {b}");
}

// ---------------------------------------------------------------------------
// Every refusal
// ---------------------------------------------------------------------------

/// What a refused seed is expected to have been refused as.
type Expected = fn(&SeedError) -> bool;

/// A keyed record holding `A` as 1 and `B` as 2, with `next_id` 3.
fn a_keyed_store() -> MemoryStore {
    let store = MemoryStore::new();
    seed(store.clone(), &a_seed(3, &[("A", 1), ("B", 2)])).expect("seeded");
    store
}

#[test]
fn a_seed_refuses_every_disagreement_and_writes_nothing() {
    let cases: Vec<(Seed, Expected)> = vec![
        (
            Seed {
                source_id: 8,
                ..a_seed(3, &[])
            },
            |error| {
                matches!(
                    error,
                    SeedError::SourceIdMismatch {
                        persisted: 7,
                        seeded: 8
                    }
                )
            },
        ),
        // A key the record holds under another ID.
        (a_seed(4, &[("A", 3)]), |error| {
            matches!(
                error,
                SeedError::Conflict {
                    seeded_id: 3,
                    held_id: 1,
                    ..
                }
            )
        }),
        // An ID the record holds under another key.
        (a_seed(4, &[("C", 2)]), |error| {
            matches!(
                error,
                SeedError::Conflict {
                    seeded_id: 2,
                    held_id: 2,
                    ..
                }
            )
        }),
        // Two pairs of one seed disagreeing between themselves.
        (a_seed(6, &[("C", 4), ("C", 5)]), |error| {
            matches!(
                error,
                SeedError::Conflict {
                    seeded_id: 5,
                    held_id: 4,
                    ..
                }
            )
        }),
        (a_seed(4, &[("C", 0)]), |error| {
            matches!(
                error,
                SeedError::IdNotBelowNext {
                    instrument_id: 0,
                    ..
                }
            )
        }),
        (a_seed(4, &[("C", 4)]), |error| {
            matches!(
                error,
                SeedError::IdNotBelowNext {
                    instrument_id: 4,
                    next_id: 4
                }
            )
        }),
        (a_seed(4, &[("with\0nul", 3)]), |error| {
            matches!(error, SeedError::BadVenueKey { .. })
        }),
        (a_seed(4, &[("", 3)]), |error| {
            matches!(error, SeedError::BadVenueKey { .. })
        }),
        (
            Seed {
                venue_keys: VenueKeys::Stated,
                ..a_seed(3, &[])
            },
            |error| matches!(error, SeedError::VenueKeysChanged { .. }),
        ),
    ];
    for (bad, expected) in cases {
        let store = a_keyed_store();
        let before = store.record();
        let error = seed(store.clone(), &bad).unwrap_err();
        assert!(expected(&error), "{bad:?}: {error}");
        assert_eq!(store.record(), before, "{bad:?} wrote nothing");
    }

    let damaged = holding(b"dz-refdata-state 4 7 1 1 sequential symbol\n");
    assert!(matches!(
        seed(damaged, &a_seed(3, &[])),
        Err(SeedError::Record(_))
    ));
}

#[test]
fn a_seed_refuses_an_id_the_destination_may_have_issued_and_forgotten() {
    // The destination minted OLD as 1, delisted it, and forgot it: it holds no
    // entry for 1, and its next_id is what keeps 1 from being issued again.
    let store = MemoryStore::new();
    let clock = ManualClock::new();
    let forgetting = RegistryConfig {
        forget_delisted_after: Some(Duration::from_secs(60)),
        ..keyed()
    };
    let mut registry = Registry::open(forgetting, store.clone(), clock.clone()).expect("opens");
    registry.seeding_complete();
    let old = registry.list(&spec("OLD", None)).expect("admitted");
    registry.delist(old);
    clock.advance(Duration::from_secs(120));
    for index in 0..=COMPACTION_FLOOR {
        registry
            .list(&spec(&format!("W{index}"), None))
            .expect("admitted");
    }
    drop(registry);
    let record = record_of(&store);
    assert!(record.entries.iter().all(|entry| entry.instrument_id != 1));
    assert!(record.next_id > 1);

    let error = seed(store.clone(), &a_seed(record.next_id + 1, &[("NEW", 1)])).unwrap_err();
    assert!(
        matches!(error, SeedError::IdMayBeForgotten { instrument_id: 1, next_id } if next_id == record.next_id),
        "{error}"
    );
}

#[test]
fn a_seed_into_a_record_is_the_union_of_the_two() {
    let store = a_keyed_store();
    seed(store.clone(), &a_seed(6, &[("A", 1), ("C", 5)])).expect("merged");
    let record = record_of(&store);
    assert_eq!(record.next_id, 6);
    let held = mapping(&record);
    assert_eq!(held.len(), 3);
    assert_eq!(held.get(b"B".as_slice()), Some(&2));
    assert_eq!(held.get(b"C".as_slice()), Some(&5));

    // The record's next_id stands when it is the larger.
    seed(store.clone(), &a_seed(2, &[("A", 1)])).expect("merged");
    assert_eq!(record_of(&store).next_id, 6);
}

#[test]
fn the_same_seed_twice_gives_the_same_record() {
    let store = MemoryStore::new();
    let imported = instruments_json();
    seed(store.clone(), &imported).expect("seeded");
    let once = store.record();
    seed(store.clone(), &imported).expect("seeded again");
    assert_eq!(store.record(), once);
}

#[test]
fn a_write_that_fails_after_its_rename_is_finished_by_a_reload_and_a_second_seed() {
    let store = a_keyed_store();
    let merge = a_seed(6, &[("C", 5)]);
    store.break_flushes("the directory sync failed");
    let error = seed(store.clone(), &merge).unwrap_err();
    assert!(
        matches!(error, SeedError::Store(StateError::Write(_))),
        "{error}"
    );
    // The outcome is not known; here the merged record landed.
    assert_eq!(record_of(&store).next_id, 6);

    store.repair_writes();
    seed(store.clone(), &merge).expect("the retry is safe");
    let held = mapping(&record_of(&store));
    assert_eq!(held.len(), 3);
    assert_eq!(held.get(b"C".as_slice()), Some(&5));
}

#[test]
fn a_seed_a_mint_an_export_and_a_reseed_leave_no_id_naming_two_keys() {
    // The import.
    let runtime = MemoryStore::new();
    let imported = instruments_json();
    seed(runtime.clone(), &imported).expect("imported");
    let mut registry = opened(runtime.clone(), keyed());
    let minted = id_of(&mut registry, "KXMINTED-BY-THE-RUNTIME", None).expect("minted");
    assert_eq!(minted, imported.next_id);
    drop(registry);

    // The export, on a rollback: the record's own entries, with their keys.
    let record = record_of(&runtime);
    let exported = Seed {
        source_id: record.source_id,
        next_id: record.next_id,
        entries: record
            .entries
            .iter()
            .map(|entry| {
                (
                    String::from_utf8(entry.key().to_vec()).expect("a ticker"),
                    entry.instrument_id,
                )
            })
            .collect(),
        retire_unmapped: false,
        venue_keys: VenueKeys::Symbol,
    };
    assert_eq!(exported.entries.len(), imported.entries.len() + 1);

    // The shipped side mints one of its own above the exported next_id, and
    // the runtime imports again.
    let mut shipped_side: Vec<(String, u32)> = exported.entries.clone();
    shipped_side.push(("KXMINTED-BY-THE-SHIPPED-SIDE".to_owned(), exported.next_id));
    let reimport = Seed {
        next_id: exported.next_id + 1,
        entries: shipped_side,
        ..exported
    };
    seed(runtime.clone(), &reimport).expect("imported again");
    let held = mapping(&record_of(&runtime));
    assert_eq!(held.len(), imported.entries.len() + 2);
    assert_eq!(
        held.get(b"KXMINTED-BY-THE-RUNTIME".as_slice()),
        Some(&minted)
    );
    assert_eq!(
        held.get(b"KXMINTED-BY-THE-SHIPPED-SIDE".as_slice()),
        Some(&(minted + 1))
    );
}

// ---------------------------------------------------------------------------
// A derived record
// ---------------------------------------------------------------------------

#[test]
fn a_derived_record_takes_a_resolve_only_seed_and_nothing_else() {
    let full = ticker("D", 64);
    let longer = format!("{full}-RESOLVED");
    let bytes = unkeyed_record(&["AAA", &full], IdAllocation::Derived);
    let before = StateRecord::decode(&bytes).expect("a derived record");
    assert_eq!(before.allocation, IdAllocation::Derived);
    let full_id = derive_instrument_id(&symbol_field(&full).0);
    let derived = config(Identity::VenueKey, VenueKeys::Symbol, IdAllocation::Derived);
    assert!(matches!(
        Registry::open(derived.clone(), holding(&bytes), ManualClock::new()),
        Err(RefdataError::UnresolvedVenueKeys { .. })
    ));

    let floor = before.next_id;
    let refused = [
        a_seed(floor, &[(&longer, full_id), ("NEW", 9)]),
        Seed {
            retire_unmapped: true,
            ..a_seed(floor, &[(&longer, full_id)])
        },
        a_seed(floor, &[(&longer, full_id ^ 1)]),
        a_seed(floor + 1, &[(&longer, full_id)]),
    ];
    for bad in &refused {
        let store = holding(&bytes);
        let error = seed(store.clone(), bad).unwrap_err();
        assert!(
            matches!(error, SeedError::RecordIsDerived),
            "{bad:?}: {error}"
        );
        assert_eq!(
            store.record().as_deref(),
            Some(bytes.as_slice()),
            "unchanged"
        );
    }

    let store = holding(&bytes);
    seed(store.clone(), &a_seed(floor, &[(&longer, full_id)])).expect("resolve-only");
    let after = record_of(&store);
    assert_eq!(after.allocation, IdAllocation::Derived);
    assert_eq!(after.next_id, floor, "the floor does not move");
    assert_eq!(after.entries.len(), before.entries.len());
    let mut registry = opened(store, derived);
    assert_eq!(id_of(&mut registry, &longer, None), Some(full_id));
    assert_eq!(
        id_of(&mut registry, "AAA", None),
        Some(derive_instrument_id(&symbol_field("AAA").0))
    );
}
