//! `Instrument ID` minting, what survives a restart, and the writer that is
//! refused.
//!
//! One sentence is behind all of it: a published `Instrument ID` always
//! resolves to a published definition. A subscriber keys a book on one, so an
//! ID that means something else tomorrow is worse than an ID that was never
//! published.

use dz_adapter_core::{
    AssetClass, InstrumentSpec, ListingSink, MarketModel, PriceBound, Scalar, SettleType,
};
use dz_publisher_lowering::SourceId;
use dz_publisher_refdata::{
    symbol_field, CycleSchedule, Entry, ManualClock, MemoryStore, RecordError, RefdataError,
    Refusal, Registry, RegistryConfig, SelectionPolicy, StateError, StateRecord,
    FIRST_INSTRUMENT_ID,
};

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

/// This publisher's configured identity. `7` is in the source registry's
/// assigned production range.
const SOURCE_ID: u16 = 7;

fn config() -> RegistryConfig {
    RegistryConfig {
        source_id: SourceId::new(SOURCE_ID).expect("7 is an assigned production id"),
        channel_id: 3,
        selection: SelectionPolicy::from_seed(8).expect("8 is a seed"),
        schedule: CycleSchedule::new(std::time::Duration::from_secs(30), 1232, 8),
    }
}

fn open(store: MemoryStore) -> Result<Registry<MemoryStore, ManualClock>, RefdataError> {
    Registry::open(config(), store, ManualClock::new())
}

fn opened(store: MemoryStore) -> Registry<MemoryStore, ManualClock> {
    let mut registry = open(store).expect("the directory is usable");
    registry.seeding_complete();
    registry
}

#[test]
fn the_first_instrument_id_minted_is_one() {
    // Not zero. A zero-filled buffer, a short read and a decode that gave up
    // part way through all present as an `Instrument ID` of 0, so the ID that
    // means "nothing was set" must not also mean "the first thing the venue
    // listed".
    assert_eq!(FIRST_INSTRUMENT_ID, 1);

    let mut registry = opened(MemoryStore::new());
    let handle = registry.list(&spec("AAA")).expect("admitted");
    assert_eq!(
        registry
            .definition(handle)
            .expect("published")
            .instrument_id,
        1
    );
}

#[test]
fn a_minted_instrument_id_survives_a_restart_and_resolves_to_the_same_symbol() {
    let store = MemoryStore::new();
    {
        let mut registry = opened(store.clone());
        let first = registry.list(&spec("AAA")).expect("admitted");
        let second = registry.list(&spec("BBB")).expect("admitted");
        assert_eq!(
            registry.definition(first).expect("published").instrument_id,
            1
        );
        assert_eq!(
            registry
                .definition(second)
                .expect("published")
                .instrument_id,
            2
        );
    }

    // The restart. Offered in the opposite order this time, which is the point:
    // the ID belongs to the symbol and not to the order the venue happened to
    // poll in.
    let mut restarted = opened(store.clone());
    let second = restarted.list(&spec("BBB")).expect("admitted");
    let first = restarted.list(&spec("AAA")).expect("admitted");
    assert_eq!(
        restarted
            .definition(second)
            .expect("published")
            .instrument_id,
        2
    );
    assert_eq!(
        restarted
            .definition(first)
            .expect("published")
            .instrument_id,
        1
    );

    // And a symbol that has never been offered takes the next ID rather than
    // one that is already published.
    let third = restarted.list(&spec("CCC")).expect("admitted");
    assert_eq!(
        restarted
            .definition(third)
            .expect("published")
            .instrument_id,
        3
    );
}

#[test]
fn a_second_writer_is_refused_and_the_incumbent_keeps_running() {
    // Which one wins: the incumbent, and this test is what says so. The
    // running publisher has already put `Instrument ID`s on the wire that
    // subscribers hold, so refusing the newcomer costs one failed start.
    // Admitting it costs both: each mints from its own copy of `next_id` and
    // each flush overwrites the other's, so after the next restart whichever
    // IDs lost the last flush resolve to nothing.
    let store = MemoryStore::new();
    let mut incumbent = opened(store.clone());
    incumbent.list(&spec("AAA")).expect("admitted");

    let refused = open(store.clone());
    assert!(matches!(
        refused,
        Err(RefdataError::StateHeldByAnotherWriter)
    ));

    // The incumbent is unaffected by the refusal: it did not lose its claim,
    // and it goes on minting.
    let next = incumbent.list(&spec("BBB")).expect("admitted");
    assert_eq!(
        incumbent.definition(next).expect("published").instrument_id,
        2
    );

    // The claim is released when the writer holding it goes away - which is
    // what happens to a real claim when the process holding it dies, however it
    // dies - so a restart after a crash is not locked out.
    drop(incumbent);
    let successor = open(store).expect("the claim was released");
    drop(successor);
}

#[test]
fn a_missing_record_is_a_cold_start_and_not_an_error() {
    // Indistinguishable from a state directory somebody has emptied, which is
    // why the directory is durable state rather than a cache. What it must not
    // be is a startup failure: a publisher that has never minted an ID has
    // nothing to lose by starting.
    let store = MemoryStore::new();
    assert!(store.record().is_none());

    let mut registry = opened(store.clone());
    assert_eq!(registry.published(), 0);
    let handle = registry.list(&spec("AAA")).expect("admitted");
    assert_eq!(
        registry
            .definition(handle)
            .expect("published")
            .instrument_id,
        FIRST_INSTRUMENT_ID
    );
    assert!(store.record().is_some(), "the mint was persisted");
}

#[test]
fn an_unreadable_record_stops_the_publisher_starting() {
    // The alternative is minting from the start of the ID space while
    // subscribers still hold yesterday's IDs, which is the failure the
    // persistence exists to prevent - reached by carrying on.
    let store = MemoryStore::new();
    store.break_reads("the device is not answering");

    assert!(matches!(
        open(store),
        Err(RefdataError::State(StateError::Read(_)))
    ));
}

#[test]
fn a_damaged_record_stops_the_publisher_starting() {
    let not_ours = MemoryStore::new();
    not_ours.set_record(b"# some other tool's file\n".to_vec());
    assert!(matches!(
        open(not_ours),
        Err(RefdataError::CorruptState(RecordError::NotOurFormat))
    ));

    // A later format is refused by name rather than read on a best-effort
    // basis: a layout this build does not know may hold a field that changes
    // what the fields it does know mean.
    let newer = MemoryStore::new();
    newer.set_record(b"dz-refdata-state 2 7 1\n".to_vec());
    assert!(matches!(
        open(newer),
        Err(RefdataError::CorruptState(
            RecordError::UnsupportedVersion { found: 2 }
        ))
    ));

    let truncated_entry = MemoryStore::new();
    truncated_entry.set_record(b"dz-refdata-state 1 7 3\n2 not-hexadecimal\n".to_vec());
    assert!(matches!(
        open(truncated_entry),
        Err(RefdataError::CorruptState(RecordError::Malformed { .. }))
    ));

    // An ID that minting would hand out again. `next_id` is the whole guarantee
    // against re-issue, so a record where it does not exceed every ID already
    // minted is one that would collide on the next admission.
    let colliding = MemoryStore::new();
    colliding.set_record(
        StateRecord {
            source_id: SOURCE_ID,
            next_id: 1,
            entries: vec![Entry {
                instrument_id: 1,
                symbol: symbol_field("AAA").0,
            }],
        }
        .encode(),
    );
    assert!(matches!(
        open(colliding),
        Err(RefdataError::CorruptState(RecordError::IdNotBelowNext {
            instrument_id: 1,
            next_id: 1
        }))
    ));
}

#[test]
fn a_record_minted_under_another_source_id_stops_the_publisher_starting() {
    // Two feeds configured to share one state directory. The live-writer guard
    // cannot see this, because they need never run at the same time; what
    // catches it is the record saying whose IDs these are. Continuing would
    // publish the other publisher's `Instrument ID`s under this one's `Source
    // ID`, and a subscriber resolving the pair would find two publishers
    // claiming one instrument identity.
    let store = MemoryStore::new();
    store.set_record(
        StateRecord {
            source_id: 9,
            next_id: 2,
            entries: vec![Entry {
                instrument_id: 1,
                symbol: symbol_field("AAA").0,
            }],
        }
        .encode(),
    );

    assert!(matches!(
        open(store),
        Err(RefdataError::StateBelongsToAnotherSource {
            persisted: 9,
            configured: 7
        })
    ));
}

#[test]
fn a_mint_that_cannot_be_persisted_admits_nothing() {
    // The order is the guarantee: persisted, then admitted. An `Instrument ID`
    // published from memory and absent from the record resolves to nothing
    // after a restart, and the next run hands it to a different instrument.
    let store = MemoryStore::new();
    let mut registry = opened(store.clone());
    store.break_writes("no space left on device");

    assert!(registry.list(&spec("AAA")).is_none());
    assert_eq!(registry.published(), 0);
    assert_eq!(registry.instruments().len(), 0);
    assert_eq!(registry.last_refusal(), Some(Refusal::Unpersistable));
    assert!(registry.fault().is_some());
    assert!(store.record().is_none());

    // The fault is terminal for this registry even once writes work again: it
    // stops minting and reports, and whether the process ends is the runtime's
    // decision. What it must not do is resume minting IDs whose predecessors
    // were lost.
    store.repair_writes();
    assert!(registry.list(&spec("AAA")).is_none());
    assert_eq!(registry.last_refusal(), Some(Refusal::Unpersistable));

    // Nothing was persisted, so a fresh start mints from the beginning again -
    // which is only correct because nothing was published either.
    drop(registry);
    let mut restarted = opened(store);
    let handle = restarted.list(&spec("AAA")).expect("admitted");
    assert_eq!(
        restarted
            .definition(handle)
            .expect("published")
            .instrument_id,
        FIRST_INSTRUMENT_ID
    );
}

#[test]
fn a_record_round_trips_and_encodes_the_same_bytes_whatever_order_it_was_built_in() {
    // The state file is compared between hosts and between runs, so the same
    // set of admissions has to produce the same bytes. Entry order is the venue
    // poll order, and that is not stable.
    let aaa = Entry {
        instrument_id: 1,
        symbol: symbol_field("AAA").0,
    };
    let bbb = Entry {
        instrument_id: 2,
        symbol: symbol_field("BBB").0,
    };
    let one_way = StateRecord {
        source_id: SOURCE_ID,
        next_id: 3,
        entries: vec![aaa, bbb],
    };
    let other_way = StateRecord {
        source_id: SOURCE_ID,
        next_id: 3,
        entries: vec![bbb, aaa],
    };

    assert_eq!(one_way.encode(), other_way.encode());
    assert_eq!(
        StateRecord::decode(&one_way.encode()).expect("our own bytes"),
        one_way
    );

    // The header, transcribed by hand. A reader that accepted a different tag
    // or a different version would be accepting somebody else's format.
    let text = String::from_utf8(one_way.encode()).expect("ASCII");
    assert!(text.starts_with("dz-refdata-state 1 7 3\n"), "{text}");
}

#[test]
fn a_symbol_is_persisted_as_the_wire_field_and_not_as_the_venues_string() {
    // The field is 64 bytes on the wire, so two venue tickers that differ only
    // past that width are one symbol to every subscriber. Keying on the venue's
    // string would mint them two `Instrument ID`s that publish as the same
    // instrument.
    let sixty_four = "A".repeat(64);
    let sixty_five = "A".repeat(65);

    let store = MemoryStore::new();
    let mut registry = opened(store);
    let first = registry.list(&spec(&sixty_four)).expect("admitted");
    let second = registry.list(&spec(&sixty_five)).expect("the same symbol");

    assert_eq!(first, second);
    assert_eq!(registry.published(), 1);
}
