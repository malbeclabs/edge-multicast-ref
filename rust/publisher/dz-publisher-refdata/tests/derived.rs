//! Derived `Instrument ID` allocation: two publishers of one channel that
//! agree on every ID without exchanging anything.
//!
//! The promise is narrow and these tests hold it to that: from the same seed,
//! the same symbol gets the same ID whatever order a host saw the venue's
//! listings in. A collision is declined, never moved, because moving it is how
//! two hosts would come to disagree.

use dz_adapter_core::{
    AssetClass, InstrumentSpec, ListingSink, MarketModel, PriceBound, Scalar, SettleType,
};
use dz_edge_refdata::SYMBOL_LEN;
use dz_publisher_lowering::SourceId;
use dz_publisher_refdata::{
    derive_instrument_id, encode_line, symbol_field, CycleSchedule, Entry, IdAllocation,
    ManualClock, MemoryStore, RecordError, RefdataError, Refusal, Registry, RegistryConfig,
    SelectionPolicy, ShardConfig, StateRecord, StateStore,
};

const SOURCE_ID: u16 = 7;

/// A real CRC-32 collision, not a forced one: both are `0x4DDB0C25`.
const COLLIDES: (&str, &str) = ("plumless", "buckeroo");

fn spec(symbol: &str) -> InstrumentSpec<'_> {
    InstrumentSpec {
        symbol,
        leg1: None,
        leg2: None,
        asset_class: AssetClass::CryptoSpot,
        price_exponent: -4,
        qty_exponent: -2,
        market_model: MarketModel::Clob,
        tick_size: Scalar::text("0.0001"),
        lot_size: Scalar::text("0.01"),
        contract_value: None,
        quoted_per_contract: None,
        expiry_ns: None,
        settle_type: SettleType::NotApplicable,
        price_bound: PriceBound::Unbounded,
    }
}

fn config(id_allocation: IdAllocation) -> RegistryConfig {
    RegistryConfig {
        source_id: SourceId::new(SOURCE_ID).expect("7 is an assigned production id"),
        shards: vec![ShardConfig::default_shard(3)],
        selection: SelectionPolicy::from_seed(32).expect("32 is a seed"),
        schedule: CycleSchedule::new(std::time::Duration::from_secs(30), 1232, 8),
        forget_delisted_after: None,
        id_allocation,
    }
}

fn opened<S: StateStore>(store: S, id_allocation: IdAllocation) -> Registry<S, ManualClock> {
    let mut registry = Registry::open(config(id_allocation), store, ManualClock::new())
        .expect("the directory is usable");
    registry.seeding_complete();
    registry
}

fn id_of<S: StateStore>(registry: &mut Registry<S, ManualClock>, symbol: &str) -> Option<u32> {
    let handle = registry.list(&spec(symbol))?;
    Some(
        registry
            .definition(handle)
            .expect("published")
            .instrument_id,
    )
}

fn key(symbol: &str) -> [u8; SYMBOL_LEN] {
    symbol_field(symbol).0
}

/// A sequential record holding `symbols` as IDs 1, 2, 3 ..., as the publisher
/// a second host is seeded from would have written it.
fn seed(symbols: &[&str]) -> Vec<u8> {
    let store = MemoryStore::new();
    let mut first = opened(store.clone(), IdAllocation::Sequential);
    for symbol in symbols {
        first.list(&spec(symbol)).expect("admitted");
    }
    drop(first);
    store.record().expect("the first host wrote a record")
}

fn seeded(bytes: &[u8]) -> MemoryStore {
    let store = MemoryStore::new();
    store.set_record(bytes.to_vec());
    store
}

fn record_of(store: &MemoryStore) -> StateRecord {
    StateRecord::decode(&store.record().expect("a record")).expect("it reads back")
}

// ---------------------------------------------------------------------------
// The derivation
// ---------------------------------------------------------------------------

#[test]
fn the_derivation_is_crc32_iso_hdlc() {
    // The catalogue's check value for CRC-32/ISO-HDLC. A table or a final XOR
    // that is off by anything moves it.
    assert_eq!(derive_instrument_id(&key("123456789")), 0xCBF4_3926);
}

#[test]
fn the_derivation_ignores_the_trailing_nul_padding() {
    assert_eq!(
        derive_instrument_id(&key("ABC")),
        crc32_of(b"ABC"),
        "the padding is not part of the input"
    );
}

#[test]
fn symbols_that_differ_only_past_an_interior_nul_derive_differently() {
    // Two distinct record keys, as a ticker with an interior NUL is admitted.
    // Stopping at the first NUL would give both one ID and decline the second
    // for ever.
    let mut one = [0u8; SYMBOL_LEN];
    let mut other = [0u8; SYMBOL_LEN];
    one[..5].copy_from_slice(b"AB\0CX");
    other[..5].copy_from_slice(b"AB\0CY");
    assert_ne!(derive_instrument_id(&one), derive_instrument_id(&other));
    assert_eq!(derive_instrument_id(&one), crc32_of(b"AB\0CX"));
}

/// CRC-32/ISO-HDLC bit by bit, independent of the table under test.
fn crc32_of(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

// ---------------------------------------------------------------------------
// The record
// ---------------------------------------------------------------------------

fn derived_record(floor: u32, seeded: &[(u32, &str)], derived: &[&str]) -> StateRecord {
    let mut entries: Vec<Entry> = seeded
        .iter()
        .map(|&(instrument_id, symbol)| Entry {
            instrument_id,
            symbol: key(symbol),
            delisted_at: None,
        })
        .collect();
    entries.extend(derived.iter().map(|symbol| Entry {
        instrument_id: derive_instrument_id(&key(symbol)),
        symbol: key(symbol),
        delisted_at: None,
    }));
    StateRecord {
        source_id: SOURCE_ID,
        next_id: floor,
        entries,
        allocation: IdAllocation::Derived,
    }
}

#[test]
fn a_derived_record_round_trips_with_seeded_entries_below_its_floor() {
    let record = derived_record(3, &[(1, "OLD-A"), (2, "OLD-B")], &["NEW-A", "NEW-B"]);
    let bytes = record.encode();
    assert!(
        bytes.starts_with(b"dz-refdata-state 3 7 3 4 derived\n"),
        "{}",
        String::from_utf8_lossy(&bytes[..40])
    );
    let mut back = StateRecord::decode(&bytes).expect("it reads back");
    let mut expected = record.clone();
    back.entries
        .sort_unstable_by_key(|entry| entry.instrument_id);
    expected
        .entries
        .sort_unstable_by_key(|entry| entry.instrument_id);
    assert_eq!(back, expected);
}

#[test]
fn a_derived_line_whose_id_is_not_its_symbols_derivation_is_refused() {
    let mut bytes = derived_record(3, &[(1, "OLD-A"), (2, "OLD-B")], &[]).encode();
    bytes.extend(encode_line(4_000_000, &key("NEW-A")));
    assert!(matches!(
        StateRecord::decode(&bytes),
        Err(RecordError::NotDerived {
            instrument_id: 4_000_000,
            ..
        })
    ));

    // In the base too: an entry at or above the floor must be derived.
    let mut forged = derived_record(3, &[(1, "OLD-A")], &[]);
    forged.entries.push(Entry {
        instrument_id: 4_000_000,
        symbol: key("NEW-A"),
        delisted_at: None,
    });
    assert!(matches!(
        StateRecord::decode(&forged.encode()),
        Err(RecordError::NotDerived {
            instrument_id: 4_000_000,
            ..
        })
    ));
}

#[test]
fn a_derived_line_below_the_floor_is_refused() {
    // A floor above every derivation but `u32::MAX` itself.
    let mut bytes = derived_record(u32::MAX, &[], &[]).encode();
    let derived = derive_instrument_id(&key("NEW-A"));
    bytes.extend(encode_line(derived, &key("NEW-A")));
    assert!(matches!(
        StateRecord::decode(&bytes),
        Err(RecordError::MintedBelowFloor {
            floor: u32::MAX,
            ..
        })
    ));
}

#[test]
fn a_derived_line_claiming_an_id_another_symbol_holds_is_refused() {
    let (held, newcomer) = COLLIDES;
    let mut bytes = derived_record(1, &[], &[held]).encode();
    bytes.extend(encode_line(
        derive_instrument_id(&key(newcomer)),
        &key(newcomer),
    ));
    assert!(matches!(
        StateRecord::decode(&bytes),
        Err(RecordError::RestatedUnderAnotherSymbol { .. })
    ));
}

#[test]
fn a_torn_final_line_of_a_derived_record_is_dropped() {
    let mut bytes = derived_record(1, &[], &["NEW-A"]).encode();
    let line = encode_line(derive_instrument_id(&key("NEW-B")), &key("NEW-B"));
    bytes.extend(&line[..line.len() / 2]);
    let loaded = StateRecord::load(&bytes).expect("a torn tail is not damage");
    assert!(loaded.torn);
    assert_eq!(loaded.record.entries.len(), 1);
}

// ---------------------------------------------------------------------------
// The registry
// ---------------------------------------------------------------------------

#[test]
fn two_hosts_seeded_alike_agree_on_every_id_whatever_order_they_see() {
    let seed = seed(&["S-1", "S-2", "S-3"]);
    let new = ["N-1", "N-2", "N-3", "N-4", "N-5"];
    let mut offered: Vec<&str> = vec!["S-1", "S-2", "S-3"];
    offered.extend(new);

    let mut one = opened(seeded(&seed), IdAllocation::Derived);
    let mut other = opened(seeded(&seed), IdAllocation::Derived);
    let mut reversed = offered.clone();
    reversed.reverse();

    let mut first = Vec::new();
    for symbol in &offered {
        first.push((*symbol, id_of(&mut one, symbol).expect("admitted")));
    }
    let mut second = std::collections::HashMap::new();
    for symbol in &reversed {
        second.insert(*symbol, id_of(&mut other, symbol).expect("admitted"));
    }
    for (symbol, id) in first {
        assert_eq!(second[symbol], id, "{symbol} differs between the two hosts");
    }
}

#[test]
fn a_seed_keeps_its_ids_and_its_next_id_becomes_the_floor() {
    let store = seeded(&seed(&["S-1", "S-2", "S-3"]));
    let mut host = opened(store.clone(), IdAllocation::Derived);
    assert_eq!(id_of(&mut host, "S-1"), Some(1));
    assert_eq!(id_of(&mut host, "S-3"), Some(3));
    assert_eq!(
        id_of(&mut host, "N-1"),
        Some(derive_instrument_id(&key("N-1")))
    );
    assert_eq!(
        id_of(&mut host, "N-2"),
        Some(derive_instrument_id(&key("N-2")))
    );

    // Rewritten in the derived layout at open, and the floor never moves.
    let record = record_of(&store);
    assert_eq!(record.allocation, IdAllocation::Derived);
    assert_eq!(record.next_id, 4, "the floor is the seed's next_id");
    assert_eq!(record.entries.len(), 5);
}

#[test]
fn a_cold_derived_start_derives_the_very_first_id() {
    let store = MemoryStore::new();
    let mut host = opened(store.clone(), IdAllocation::Derived);
    assert_eq!(
        id_of(&mut host, "N-1"),
        Some(derive_instrument_id(&key("N-1")))
    );
    assert_eq!(
        id_of(&mut host, "N-2"),
        Some(derive_instrument_id(&key("N-2")))
    );
    // The first mint wrote the base, and its floor is the cold start's: a
    // derived mint never advances it.
    let record = record_of(&store);
    assert_eq!(record.allocation, IdAllocation::Derived);
    assert_eq!(record.next_id, dz_publisher_refdata::FIRST_INSTRUMENT_ID);
}

#[test]
fn a_collision_is_declined_and_the_holder_keeps_its_id() {
    let (held, newcomer) = COLLIDES;
    let store = MemoryStore::new();
    let mut host = opened(store.clone(), IdAllocation::Derived);
    let id = id_of(&mut host, held).expect("admitted");
    assert_eq!(id, 0x4DDB_0C25);
    let appends = store.appends();
    let stores = store.stores();

    assert_eq!(id_of(&mut host, newcomer), None);
    assert_eq!(host.last_refusal(), Some(Refusal::IdUnavailable));
    assert_eq!(host.counts().declined_id_unavailable, 1);
    assert_eq!(
        (store.appends(), store.stores()),
        (appends, stores),
        "nothing is persisted for a declined offer"
    );
    // The holder is untouched.
    assert_eq!(id_of(&mut host, held), Some(id));

    let reported = host.take_unavailable_ids();
    assert_eq!(reported.len(), 1);
    assert_eq!(reported[0].symbol, key(newcomer));
    assert_eq!(reported[0].instrument_id, id);
    assert_eq!(reported[0].holder, Some(key(held)));

    // A re-offer is counted again and named once.
    assert_eq!(id_of(&mut host, newcomer), None);
    assert_eq!(host.counts().declined_id_unavailable, 2);
    assert!(host.take_unavailable_ids().is_empty());
}

#[test]
fn colliding_symbols_seen_in_opposite_orders_split_the_two_hosts() {
    // The divergence the design accepts rather than prevents: each host keeps
    // the colliding symbol it admitted first, so one ID names a different
    // instrument on each path. A change that claims to remove this has to
    // change this test.
    let (a, b) = COLLIDES;
    let mut one = opened(MemoryStore::new(), IdAllocation::Derived);
    let mut other = opened(MemoryStore::new(), IdAllocation::Derived);

    assert_eq!(id_of(&mut one, a), Some(0x4DDB_0C25));
    assert_eq!(id_of(&mut one, b), None);
    assert_eq!(id_of(&mut other, b), Some(0x4DDB_0C25));
    assert_eq!(id_of(&mut other, a), None);

    assert_eq!(one.take_unavailable_ids()[0].holder, Some(key(a)));
    assert_eq!(other.take_unavailable_ids()[0].holder, Some(key(b)));
}

#[test]
fn a_seed_that_cannot_be_rewritten_does_not_open() {
    // A disk with room for a line and not for the record. Starting on the
    // version-2 seed would append a derived mint to it, and the next start
    // would refuse the record as `AppendedOutOfOrder`.
    let bytes = seed(&["S-1", "S-2"]);
    let store = seeded(&bytes);
    store.break_stores("no room for the record");
    assert!(matches!(
        Registry::open(
            config(IdAllocation::Derived),
            store.clone(),
            ManualClock::new()
        ),
        Err(RefdataError::State(_))
    ));
    assert_eq!(store.record(), Some(bytes), "the seed is left as it was");
}

#[test]
fn a_derivation_below_the_floor_is_declined() {
    // A sequential record whose next_id is past every derivation but one.
    let store = seeded(format!("dz-refdata-state 2 {SOURCE_ID} {} 0\n", u32::MAX).as_bytes());
    let mut host = opened(store, IdAllocation::Derived);
    assert_eq!(id_of(&mut host, "N-1"), None);
    assert_eq!(host.last_refusal(), Some(Refusal::IdUnavailable));
    let reported = host.take_unavailable_ids();
    assert_eq!(reported[0].holder, None);
}

#[test]
fn derived_allocation_with_a_horizon_is_refused() {
    let mut config = config(IdAllocation::Derived);
    config.forget_delisted_after = Some(std::time::Duration::from_secs(3_600));
    assert!(matches!(
        Registry::open(config, MemoryStore::new(), ManualClock::new()),
        Err(RefdataError::ForgettingUnderDerivedIds)
    ));
}

#[test]
fn a_derived_record_cannot_be_continued_sequentially() {
    let store = MemoryStore::new();
    let mut host = opened(store.clone(), IdAllocation::Derived);
    host.list(&spec("N-1")).expect("admitted");
    drop(host);
    assert!(matches!(
        Registry::open(config(IdAllocation::Sequential), store, ManualClock::new()),
        Err(RefdataError::StateIsDerived)
    ));
}

#[test]
fn a_restart_under_derived_allocation_recalls_every_id() {
    let store = seeded(&seed(&["S-1"]));
    let mut host = opened(store.clone(), IdAllocation::Derived);
    let before = [
        id_of(&mut host, "S-1"),
        id_of(&mut host, "N-1"),
        id_of(&mut host, "N-2"),
    ];
    drop(host);
    let mut again = opened(store.clone(), IdAllocation::Derived);
    let after = [
        id_of(&mut again, "S-1"),
        id_of(&mut again, "N-1"),
        id_of(&mut again, "N-2"),
    ];
    assert_eq!(before, after);
    assert_eq!(record_of(&store).next_id, 2, "the floor did not move");
}
