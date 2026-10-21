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
    encode_keyed_line, symbol_field, CycleSchedule, Entry, IdAllocation, Identity, ManualClock,
    MemoryStore, RecordError, RefdataError, Registry, RegistryConfig, SelectionPolicy, ShardConfig,
    StateRecord, StateStore, VenueKeys,
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
    let _ = keyed();
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
