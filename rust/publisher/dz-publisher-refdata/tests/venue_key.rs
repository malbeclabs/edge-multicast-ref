//! An instrument recalled by its venue key, so a truncated `Symbol` names no
//! two instruments.
//!
//! Under `identity = "venue_key"` the registry keys on the venue's own name for
//! an instrument and keeps the wire `Symbol` for display. These tests hold the
//! one invariant that matters to a subscriber: no `Instrument ID` names two
//! keys, none is issued twice, and a key keeps its ID across a restart.

use dz_adapter_core::{
    AssetClass, InstrumentSpec, ListingSink, MarketModel, PriceBound, Scalar, SettleType,
};
use dz_edge_refdata::SYMBOL_LEN;
use dz_publisher_lowering::SourceId;
use dz_publisher_refdata::{
    derive_instrument_id, derive_instrument_id_from_key, encode_keyed_line, symbol_field,
    CycleSchedule, Entry, IdAllocation, Identity, ManualClock, MemoryStore, RecordError,
    RefdataError, Refusal, Registry, RegistryConfig, SelectionPolicy, ShardConfig, StateRecord,
    StateStore, VenueKeys, COMPACTION_FLOOR,
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
        selection: SelectionPolicy::from_seed(64).expect("64 is a seed"),
        schedule: CycleSchedule::new(std::time::Duration::from_secs(30), 1232, 8),
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

// ---------------------------------------------------------------------------
// The configuration
// ---------------------------------------------------------------------------

#[test]
fn stated_venue_keys_under_the_symbol_identity_are_refused_at_open() {
    let error = Registry::open(
        config(
            Identity::Symbol,
            VenueKeys::Stated,
            IdAllocation::Sequential,
        ),
        MemoryStore::new(),
        ManualClock::new(),
    )
    .unwrap_err();
    assert!(
        matches!(error, RefdataError::VenueKeysWithoutVenueKey),
        "{error}"
    );
}

#[test]
fn the_symbol_identity_ignores_a_stated_venue_key() {
    // The default changes nothing: the registry still recalls by the padded
    // Symbol, so two keys that differ under one Symbol are one instrument.
    let mut registry = opened(
        MemoryStore::new(),
        config(
            Identity::Symbol,
            VenueKeys::Symbol,
            IdAllocation::Sequential,
        ),
    );
    let first = id_of(&mut registry, "SAME", Some("key-one")).expect("admitted");
    let second = id_of(&mut registry, "SAME", Some("key-two")).expect("recalled");
    assert_eq!(first, second);
}

// ---------------------------------------------------------------------------
// The record: version 4
// ---------------------------------------------------------------------------

/// A ticker of `len` bytes: `prefix`, then `x` up to the length.
fn ticker(prefix: &str, len: usize) -> String {
    let mut out = prefix.to_owned();
    while out.len() < len {
        out.push('x');
    }
    out
}

fn symbol_of(text: &str) -> [u8; SYMBOL_LEN] {
    symbol_field(text).0
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// One version-4 line with the venue key field given as written.
fn line(instrument_id: u32, symbol: &[u8; SYMBOL_LEN], key_field: &str) -> String {
    format!("{instrument_id} {} {key_field}\n", hex(symbol))
}

fn keyed_record(next_id: u32, lines: &[String]) -> Vec<u8> {
    let mut out = format!(
        "dz-refdata-state 4 {SOURCE_ID} {next_id} {} sequential symbol\n",
        lines.len()
    );
    for one in lines {
        out.push_str(one);
    }
    out.into_bytes()
}

fn loaded(bytes: &[u8]) -> Result<StateRecord, RecordError> {
    StateRecord::decode(bytes)
}

#[test]
fn a_keyed_record_round_trips_with_a_shared_symbol() {
    let long = ticker("KXSPORTS-", 64);
    let longer_a = format!("{long}A");
    let longer_b = format!("{long}B");
    let record = StateRecord {
        source_id: SOURCE_ID,
        next_id: 5,
        entries: vec![
            Entry::keyed(1, "SHORT", None),
            Entry::keyed(2, &longer_a, None),
            // The same Symbol as ID 2: allowed in a keyed record, and the
            // case the whole version exists for.
            Entry::keyed(3, &longer_b, Some(1_700_000_000)),
            Entry {
                instrument_id: 4,
                symbol: symbol_of("DISPLAY"),
                venue_key: Some(b"venue-4".to_vec().into_boxed_slice()),
                delisted_at: None,
            },
        ],
        allocation: IdAllocation::Sequential,
        keyed: Some(VenueKeys::Stated),
    };
    assert_eq!(record.entries[1].symbol, record.entries[2].symbol);
    assert_eq!(record.entries[0].venue_key, None, "a whole Symbol is `-`");

    let decoded = loaded(&record.encode()).expect("a keyed record reads back");
    assert_eq!(decoded, record);
    assert_eq!(decoded.entries[2].key(), longer_b.as_bytes());
}

#[test]
fn a_keyed_record_of_whole_symbols_holds_what_its_version_2_form_holds() {
    let entries = vec![
        Entry::keyed(1, "AAA", None),
        Entry::keyed(2, "BBB", Some(1_700_000_000)),
    ];
    let unkeyed = StateRecord {
        source_id: SOURCE_ID,
        next_id: 3,
        entries: entries.clone(),
        allocation: IdAllocation::Sequential,
        keyed: None,
    };
    let keyed = StateRecord {
        keyed: Some(VenueKeys::Symbol),
        ..unkeyed.clone()
    };
    let two = StateRecord::load(&unkeyed.encode()).expect("version 2");
    let four = StateRecord::load(&keyed.encode()).expect("version 4");
    assert_eq!((two.version, four.version), (2, 4));
    assert_eq!(two.record.entries, four.record.entries);
    assert!(four
        .record
        .entries
        .iter()
        .all(|entry| entry.venue_key.is_none()));
}

#[test]
fn a_key_of_61_bytes_or_more_is_written_out_even_when_it_is_its_symbol() {
    for len in 61..=64 {
        let key = ticker("K", len);
        let entry = Entry::keyed(1, &key, None);
        assert_eq!(
            entry.venue_key.as_deref(),
            Some(key.as_bytes()),
            "{len} bytes leave fewer than four of padding"
        );
        let text = String::from_utf8(
            StateRecord {
                source_id: SOURCE_ID,
                next_id: 2,
                entries: vec![entry],
                allocation: IdAllocation::Sequential,
                keyed: Some(VenueKeys::Symbol),
            }
            .encode(),
        )
        .expect("text");
        assert!(
            text.ends_with(&format!(" {}\n", hex(key.as_bytes()))),
            "{text}"
        );
    }
    assert_eq!(Entry::keyed(1, &ticker("K", 60), None).venue_key, None);
}

#[test]
fn the_loader_refuses_a_malformed_venue_key() {
    let symbol = symbol_of("ABC");
    for field in ["616", "4A", "zz", "", "6100"] {
        let error = loaded(&keyed_record(2, &[line(1, &symbol, field)])).unwrap_err();
        assert!(
            matches!(error, RecordError::BadVenueKey { line: 2 }),
            "{field:?}: {error}"
        );
    }
    let too_long = "61".repeat(257);
    assert!(matches!(
        loaded(&keyed_record(2, &[line(1, &symbol, &too_long)])),
        Err(RecordError::BadVenueKey { .. })
    ));
    let longest = "61".repeat(256);
    assert!(loaded(&keyed_record(2, &[line(1, &symbol, &longest)])).is_ok());
}

#[test]
fn the_loader_refuses_a_dash_on_a_symbol_that_may_have_been_cut() {
    // Full width, three bytes of padding, and one: each could be a prefix.
    let full = symbol_of(&ticker("F", 64));
    let three = symbol_of(&ticker("T", 61));
    // 63 ASCII bytes and `é`: the Symbol is the 63 bytes and one NUL, which
    // is also the Symbol of the 63-byte ticker alone.
    let cut_character = symbol_of(&format!("{}é", ticker("C", 63)));
    assert_eq!(cut_character, symbol_of(&ticker("C", 63)));
    let mut interior = [0u8; SYMBOL_LEN];
    interior[..5].copy_from_slice(b"AB\0CD");
    for symbol in [full, three, cut_character, interior] {
        let error = loaded(&keyed_record(2, &[line(1, &symbol, "-")])).unwrap_err();
        assert!(
            matches!(
                error,
                RecordError::UnresolvedVenueKey {
                    line: 2,
                    instrument_id: 1
                }
            ),
            "{error}"
        );
    }
    let whole = symbol_of(&ticker("W", 60));
    assert!(loaded(&keyed_record(2, &[line(1, &whole, "-")])).is_ok());
}

#[test]
fn the_loader_refuses_a_shared_key_or_id_and_accepts_a_shared_symbol() {
    let symbol = symbol_of(&ticker("S", 64));
    let shared_symbol = keyed_record(
        3,
        &[
            line(1, &symbol, &hex(format!("{}1", ticker("S", 64)).as_bytes())),
            line(2, &symbol, &hex(format!("{}2", ticker("S", 64)).as_bytes())),
        ],
    );
    assert_eq!(loaded(&shared_symbol).expect("allowed").entries.len(), 2);

    let shared_key = keyed_record(
        3,
        &[
            line(1, &symbol_of("A"), &hex(b"same")),
            line(2, &symbol_of("B"), &hex(b"same")),
        ],
    );
    assert!(matches!(
        loaded(&shared_key),
        Err(RecordError::DuplicateVenueKey {
            first: 1,
            second: 2
        })
    ));
    // A `-` and a stated key that name the same bytes are one key.
    let dash_and_hex = keyed_record(
        3,
        &[
            line(1, &symbol_of("SAME"), "-"),
            line(2, &symbol_of("OTHER"), &hex(b"SAME")),
        ],
    );
    assert!(matches!(
        loaded(&dash_and_hex),
        Err(RecordError::DuplicateVenueKey { .. })
    ));

    let shared_id = keyed_record(
        3,
        &[line(1, &symbol_of("A"), "-"), line(1, &symbol_of("B"), "-")],
    );
    assert!(matches!(
        loaded(&shared_id),
        Err(RecordError::DuplicateId { instrument_id: 1 })
    ));
}

#[test]
fn a_keyed_line_appends_and_a_torn_one_is_dropped() {
    let long = ticker("L", 70);
    let mut bytes = keyed_record(2, &[line(1, &symbol_of("A"), "-")]);
    bytes.extend(encode_keyed_line(
        2,
        &symbol_of(&long),
        Some(long.as_bytes()),
    ));
    let whole = StateRecord::load(&bytes).expect("an appended keyed line");
    assert_eq!(whole.appended, 1);
    assert_eq!(whole.record.next_id, 3);
    assert_eq!(whole.record.entries[1].key(), long.as_bytes());

    // Cut inside the venue key: an append that never completed.
    let mut torn = bytes.clone();
    let cut = encode_keyed_line(3, &symbol_of("C"), Some(b"cee"));
    torn.extend(&cut[..cut.len() - 3]);
    let read = StateRecord::load(&torn).expect("the torn line is dropped");
    assert!(read.torn);
    assert_eq!(read.record.entries.len(), 2);

    // Restating ID 2 under another key is not something a relisting writes.
    let mut restated = bytes;
    restated.extend(encode_keyed_line(2, &symbol_of(&long), Some(b"another")));
    assert!(matches!(
        StateRecord::load(&restated),
        Err(RecordError::RestatedUnderAnotherVenueKey {
            instrument_id: 2,
            ..
        })
    ));
}

#[test]
fn a_version_4_header_names_its_allocation_and_its_venue_keys() {
    for header in [
        "dz-refdata-state 4 7 1 0\n",
        "dz-refdata-state 4 7 1 0 sequential\n",
        "dz-refdata-state 4 7 1 0 random symbol\n",
        "dz-refdata-state 4 7 1 0 sequential own\n",
    ] {
        assert!(
            matches!(
                loaded(header.as_bytes()),
                Err(RecordError::Malformed { line: 1, .. })
            ),
            "{header}"
        );
    }
    let read = loaded(b"dz-refdata-state 4 7 1 0 sequential stated\n").expect("empty");
    assert_eq!(read.keyed, Some(VenueKeys::Stated));
}

// ---------------------------------------------------------------------------
// Recall on the key
// ---------------------------------------------------------------------------

/// A sequential, unkeyed record holding `symbols` as IDs 1, 2, 3 ..., written
/// by a publisher under the symbol identity.
fn unkeyed_record(symbols: &[&str]) -> Vec<u8> {
    let store = MemoryStore::new();
    let mut first = opened(
        store.clone(),
        config(
            Identity::Symbol,
            VenueKeys::Symbol,
            IdAllocation::Sequential,
        ),
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

#[test]
fn two_tickers_that_share_a_symbol_are_two_instruments() {
    let long = ticker("KXNCAAFGAME-26NOV28-OHIO-STATE-MICHIGAN-TOTAL-POINTS-", 64);
    let (one, other) = (format!("{long}1"), format!("{long}2"));
    let mut registry = opened(MemoryStore::new(), keyed());

    // The key stated, and equal to the symbol, which the symbol declaration
    // allows.
    let first = registry.list(&spec(&one, Some(&one))).expect("admitted");
    let second = registry
        .list(&spec(&other, Some(&other)))
        .expect("admitted");
    assert_ne!(first, second, "each gets its own handle, and its own book");
    let (a, b) = (
        registry.definition(first).expect("published"),
        registry.definition(second).expect("published"),
    );
    assert_ne!(a.instrument_id, b.instrument_id);
    assert_eq!(a.symbol, b.symbol, "both carry one truncated Symbol");
    assert_eq!(registry.published(), 2);

    // A re-offer finds each again.
    assert_eq!(registry.list(&spec(&one, Some(&one))), Some(first));
    assert_eq!(registry.list(&spec(&other, Some(&other))), Some(second));
}

#[test]
fn a_spec_stating_no_key_is_keyed_on_its_untruncated_symbol() {
    let long = ticker("KXMLBGAME-", 64);
    let (one, other) = (format!("{long}A"), format!("{long}B"));
    let mut registry = opened(MemoryStore::new(), keyed());
    let a = id_of(&mut registry, &one, None).expect("admitted");
    let b = id_of(&mut registry, &other, None).expect("admitted");
    assert_ne!(a, b);
}

#[test]
fn a_64_and_a_65_byte_key_keep_their_own_ids_in_either_order_and_across_a_restart() {
    let sixty_four = ticker("KXNFLGAME-", 64);
    let sixty_five = format!("{sixty_four}Z");
    for order in [[&sixty_four, &sixty_five], [&sixty_five, &sixty_four]] {
        let store = MemoryStore::new();
        let mut registry = opened(store.clone(), keyed());
        let first = id_of(&mut registry, order[0], None).expect("admitted");
        let second = id_of(&mut registry, order[1], None).expect("admitted");
        assert_ne!(first, second);
        drop(registry);

        let mut reopened = opened(store, keyed());
        assert_eq!(id_of(&mut reopened, order[1], None), Some(second));
        assert_eq!(id_of(&mut reopened, order[0], None), Some(first));
    }
}

#[test]
fn a_venue_key_that_is_not_one_is_refused() {
    let mut registry = opened(
        MemoryStore::new(),
        config(
            Identity::VenueKey,
            VenueKeys::Stated,
            IdAllocation::Sequential,
        ),
    );
    let too_long = ticker("K", 257);
    for key in ["", "with\0nul", too_long.as_str()] {
        assert_eq!(
            registry.offer("default", &spec("DISPLAY", Some(key))),
            Err(Refusal::BadVenueKey),
            "{key:?}"
        );
    }
    let longest = ticker("K", 256);
    assert!(registry
        .offer("default", &spec("DISPLAY", Some(&longest)))
        .is_ok());
}

#[test]
fn a_key_the_configuration_did_not_declare_is_refused() {
    let mut registry = opened(MemoryStore::new(), keyed());
    assert_eq!(
        registry.offer("default", &spec("DISPLAY", Some("venue-1"))),
        Err(Refusal::VenueKeyNotDeclared)
    );
    assert_eq!(registry.counts().declined_unrepresentable, 1);
    // Stating the symbol itself is no declaration of its own keys.
    assert!(registry
        .offer("default", &spec("DISPLAY", Some("DISPLAY")))
        .is_ok());

    let mut stated = opened(
        MemoryStore::new(),
        config(
            Identity::VenueKey,
            VenueKeys::Stated,
            IdAllocation::Sequential,
        ),
    );
    assert!(stated
        .offer("default", &spec("DISPLAY", Some("venue-1")))
        .is_ok());
}

#[test]
fn an_unkeyed_record_of_whole_symbols_is_rewritten_keyed_and_keeps_every_id() {
    let short = ticker("S", 60);
    let bytes = unkeyed_record(&["AAA", "BBB", &short]);
    assert!(bytes.starts_with(b"dz-refdata-state 2 "));
    let store = holding(&bytes);
    let mut registry = opened(store.clone(), keyed());
    assert!(
        store
            .record()
            .expect("a record")
            .starts_with(b"dz-refdata-state 4 "),
        "rewritten at open"
    );
    assert_eq!(id_of(&mut registry, "BBB", None), Some(2));
    assert_eq!(id_of(&mut registry, &short, None), Some(3));
    assert_eq!(id_of(&mut registry, "AAA", None), Some(1));
    assert_eq!(
        id_of(&mut registry, "CCC", None),
        Some(4),
        "next_id carried"
    );
}

fn unresolved(error: &RefdataError) -> Vec<u32> {
    match error {
        RefdataError::UnresolvedVenueKeys { entries } => {
            entries.iter().map(|(id, _)| *id).collect()
        }
        other => panic!("not unresolved: {other}"),
    }
}

#[test]
fn an_unkeyed_record_with_a_full_width_entry_is_refused_at_open() {
    let full = ticker("F", 64);
    let bytes = unkeyed_record(&["AAA", &full]);
    let store = holding(&bytes);
    let error = Registry::open(keyed(), store.clone(), ManualClock::new()).unwrap_err();
    assert_eq!(unresolved(&error), vec![2]);
    assert_eq!(store.record(), Some(bytes), "nothing is rewritten");
}

#[test]
fn a_multi_byte_character_across_byte_64_leaves_its_entry_unresolved() {
    // 63 ASCII bytes and `é`: one byte of padding. A 4-byte character from
    // byte 61: three.
    let two_byte = format!("{}é", ticker("C", 63));
    let four_byte = format!("{}🦀", ticker("D", 61));
    assert_eq!(two_byte.len(), 65);
    assert_eq!(four_byte.len(), 65);
    let bytes = unkeyed_record(&["AAA", &two_byte, &four_byte]);
    let error = Registry::open(keyed(), holding(&bytes), ManualClock::new()).unwrap_err();
    assert_eq!(unresolved(&error), vec![2, 3]);
}

#[test]
fn under_stated_keys_every_entry_of_an_unkeyed_record_is_unresolved() {
    let bytes = unkeyed_record(&["AAA", "BBB"]);
    let error = Registry::open(
        config(
            Identity::VenueKey,
            VenueKeys::Stated,
            IdAllocation::Sequential,
        ),
        holding(&bytes),
        ManualClock::new(),
    )
    .unwrap_err();
    assert_eq!(unresolved(&error), vec![1, 2]);
}

/// A keyed record, written by a keyed publisher under `venue_keys`.
fn keyed_record_under(venue_keys: VenueKeys) -> Vec<u8> {
    let store = MemoryStore::new();
    let mut registry = opened(
        store.clone(),
        config(Identity::VenueKey, venue_keys, IdAllocation::Sequential),
    );
    registry.list(&spec("AAA", None)).expect("admitted");
    drop(registry);
    store.record().expect("a record")
}

#[test]
fn a_keyed_record_is_refused_under_the_other_venue_keys() {
    for (written, opened_under) in [
        (VenueKeys::Symbol, VenueKeys::Stated),
        (VenueKeys::Stated, VenueKeys::Symbol),
    ] {
        let error = Registry::open(
            config(Identity::VenueKey, opened_under, IdAllocation::Sequential),
            holding(&keyed_record_under(written)),
            ManualClock::new(),
        )
        .unwrap_err();
        assert!(
            matches!(
                error,
                RefdataError::VenueKeysChanged { persisted, configured }
                    if persisted == written && configured == opened_under
            ),
            "{error}"
        );
    }
}

#[test]
fn a_keyed_record_is_refused_under_the_symbol_identity() {
    let error = Registry::open(
        config(
            Identity::Symbol,
            VenueKeys::Symbol,
            IdAllocation::Sequential,
        ),
        holding(&keyed_record_under(VenueKeys::Symbol)),
        ManualClock::new(),
    )
    .unwrap_err();
    assert!(matches!(error, RefdataError::StateIsKeyed), "{error}");
}

#[test]
fn forgetting_forgets_a_key_and_not_the_key_beside_it_on_one_symbol() {
    let long = ticker("KXFORGET-", 64);
    let (gone, kept) = (format!("{long}1"), format!("{long}2"));
    let forgetting = RegistryConfig {
        forget_delisted_after: Some(std::time::Duration::from_secs(60)),
        selection: SelectionPolicy::from_seed(4096).expect("4096 is a seed"),
        ..keyed()
    };
    let store = MemoryStore::new();
    let clock = ManualClock::new();
    let mut registry =
        Registry::open(forgetting.clone(), store.clone(), clock.clone()).expect("opens");
    registry.seeding_complete();
    let gone_handle = registry.list(&spec(&gone, None)).expect("admitted");
    let gone_id = registry
        .definition(gone_handle)
        .expect("published")
        .instrument_id;
    let kept_id = id_of(&mut registry, &kept, None).expect("admitted");
    registry.delist(gone_handle);

    // Past the horizon, a compaction forgets the delisted key. The churn is
    // what sets the compaction off, as the venue's own listings would.
    clock.advance(std::time::Duration::from_secs(120));
    for index in 0..=COMPACTION_FLOOR {
        registry
            .list(&spec(&format!("W{index}"), None))
            .expect("admitted");
    }
    let record = StateRecord::decode(&store.record().expect("a record")).expect("it reads back");
    assert!(record
        .entries
        .iter()
        .all(|entry| entry.key() != gone.as_bytes()));
    assert!(record
        .entries
        .iter()
        .any(|entry| entry.key() == kept.as_bytes()));

    let relisted = id_of(&mut registry, &gone, None).expect("admitted again");
    assert_ne!(relisted, gone_id, "a forgotten key is a new instrument");
    assert_eq!(id_of(&mut registry, &kept, None), Some(kept_id));
    drop(registry);

    let mut reopened = Registry::open(forgetting, store, clock).expect("opens");
    reopened.seeding_complete();
    assert_eq!(id_of(&mut reopened, &gone, None), Some(relisted));
    assert_eq!(id_of(&mut reopened, &kept, None), Some(kept_id));
}

// ---------------------------------------------------------------------------
// "derived_key"
// ---------------------------------------------------------------------------

fn derived_key() -> RegistryConfig {
    config(
        Identity::VenueKey,
        VenueKeys::Symbol,
        IdAllocation::DerivedKey,
    )
}

#[test]
fn under_derived_key_two_keys_on_one_symbol_derive_two_ids() {
    let long = ticker("KXDERIVE-", 64);
    let (one, other) = (format!("{long}1"), format!("{long}2"));
    let mut registry = opened(MemoryStore::new(), derived_key());
    let a = id_of(&mut registry, &one, None).expect("admitted");
    let b = id_of(&mut registry, &other, None).expect("admitted");
    assert_eq!(a, derive_instrument_id_from_key(one.as_bytes()));
    assert_eq!(b, derive_instrument_id_from_key(other.as_bytes()));
    assert_ne!(a, b);
    // A second host from the same (empty) seed derives the same, in the
    // other order.
    let mut second = opened(MemoryStore::new(), derived_key());
    assert_eq!(id_of(&mut second, &other, None), Some(b));
    assert_eq!(id_of(&mut second, &one, None), Some(a));
}

#[test]
fn under_derived_two_keys_on_one_symbol_collide_and_the_decline_names_both_keys() {
    let long = ticker("KXDERIVE-", 64);
    let (one, other) = (format!("{long}1"), format!("{long}2"));
    let mut registry = opened(
        MemoryStore::new(),
        config(Identity::VenueKey, VenueKeys::Symbol, IdAllocation::Derived),
    );
    let a = id_of(&mut registry, &one, None).expect("admitted");
    assert_eq!(a, derive_instrument_id(&symbol_of(&one)));
    assert_eq!(
        registry.offer("default", &spec(&other, None)),
        Err(Refusal::IdUnavailable)
    );
    let declined = registry.take_unavailable_ids();
    assert_eq!(declined.len(), 1);
    assert_eq!(declined[0].venue_key.as_deref(), Some(other.as_bytes()));
    assert_eq!(
        declined[0].holder_venue_key.as_deref(),
        Some(one.as_bytes())
    );
    assert_eq!(declined[0].holder, Some(symbol_of(&one)));
}

/// `"derived"` takes a new key's ID from its display `Symbol`, which a stated
/// key may be renamed under, so two hosts either side of a rename would derive
/// two IDs for one instrument. `"derived_key"` is what takes stated keys.
#[test]
fn derived_ids_are_refused_with_stated_venue_keys() {
    let error = Registry::open(
        config(Identity::VenueKey, VenueKeys::Stated, IdAllocation::Derived),
        MemoryStore::new(),
        ManualClock::new(),
    )
    .unwrap_err();
    assert!(
        matches!(error, RefdataError::DerivedWithStatedVenueKeys),
        "{error}"
    );

    opened(
        MemoryStore::new(),
        config(
            Identity::VenueKey,
            VenueKeys::Stated,
            IdAllocation::DerivedKey,
        ),
    );
}

#[test]
fn derived_key_is_refused_under_the_symbol_identity_and_with_forgetting() {
    let error = Registry::open(
        config(
            Identity::Symbol,
            VenueKeys::Symbol,
            IdAllocation::DerivedKey,
        ),
        MemoryStore::new(),
        ManualClock::new(),
    )
    .unwrap_err();
    assert!(
        matches!(error, RefdataError::DerivedKeyWithoutVenueKey),
        "{error}"
    );

    let error = Registry::open(
        RegistryConfig {
            forget_delisted_after: Some(std::time::Duration::from_secs(60)),
            ..derived_key()
        },
        MemoryStore::new(),
        ManualClock::new(),
    )
    .unwrap_err();
    assert!(
        matches!(error, RefdataError::ForgettingUnderDerivedIds),
        "{error}"
    );
}

#[test]
fn a_record_is_never_moved_between_the_two_derivations() {
    for (written, opened_under) in [
        (IdAllocation::Derived, IdAllocation::DerivedKey),
        (IdAllocation::DerivedKey, IdAllocation::Derived),
    ] {
        let store = MemoryStore::new();
        let mut registry = opened(
            store.clone(),
            config(Identity::VenueKey, VenueKeys::Symbol, written),
        );
        registry.list(&spec("AAA", None)).expect("admitted");
        drop(registry);
        let error = Registry::open(
            config(Identity::VenueKey, VenueKeys::Symbol, opened_under),
            store,
            ManualClock::new(),
        )
        .unwrap_err();
        assert!(
            matches!(
                error,
                RefdataError::DerivationChanged { persisted, configured }
                    if persisted == written && configured == opened_under
            ),
            "{error}"
        );
    }
}

#[test]
fn a_derived_key_record_checks_each_mint_against_its_key() {
    let key = ticker("KXCHECK-", 70);
    let symbol = symbol_of(&key);
    let id = derive_instrument_id_from_key(key.as_bytes());
    let record = |instrument_id: u32| {
        format!(
            "dz-refdata-state 4 {SOURCE_ID} 1 1 derived_key symbol\n{}",
            line(instrument_id, &symbol, &hex(key.as_bytes()))
        )
        .into_bytes()
    };
    assert!(loaded(&record(id)).is_ok());
    // The Symbol's derivation is not the key's.
    let by_symbol = derive_instrument_id(&symbol);
    assert_ne!(by_symbol, id);
    assert!(matches!(
        loaded(&record(by_symbol)),
        Err(RecordError::NotDerived { .. })
    ));
}
