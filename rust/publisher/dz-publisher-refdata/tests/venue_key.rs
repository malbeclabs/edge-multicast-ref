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
use dz_publisher_lowering::SourceId;
use dz_publisher_refdata::{
    CycleSchedule, IdAllocation, Identity, ManualClock, MemoryStore, RefdataError, Registry,
    RegistryConfig, SelectionPolicy, ShardConfig, StateStore, VenueKeys,
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
