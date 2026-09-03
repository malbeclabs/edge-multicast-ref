//! What a venue gets back from offering an instrument, and what the table holds
//! afterwards.
//!
//! The boundary promises an adapter it may offer its whole set on every poll
//! without tracking what it has already offered. That promise is only worth
//! having if a re-offer is the same handle and not a second instrument, so
//! these are the assertions behind it.

use dz_adapter_core::{
    AssetClass, InstrumentSpec, ListingSink, MarketModel, PriceBound, Scalar, SettleType,
};
use dz_publisher_lowering::SourceId;
use dz_publisher_refdata::{
    CycleSchedule, ManualClock, MemoryStore, Phase, Refusal, Registry, RegistryConfig,
    SelectionPolicy,
};

/// An instrument whose every scalar converts exactly at its own exponents:
/// `0.0001` at `-4` is 1, and `0.01` at `-2` is 1.
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

fn config(selection: SelectionPolicy) -> RegistryConfig {
    RegistryConfig {
        source_id: SourceId::new(7).expect("7 is an assigned production id"),
        channel_id: 3,
        selection,
        schedule: CycleSchedule::new(std::time::Duration::from_secs(30), 1232, 8),
    }
}

fn registry(selection: SelectionPolicy) -> Registry<MemoryStore, ManualClock> {
    Registry::open(config(selection), MemoryStore::new(), ManualClock::new())
        .expect("an empty directory is a cold start")
}

fn seeded(selection: SelectionPolicy) -> Registry<MemoryStore, ManualClock> {
    let mut registry = registry(selection);
    registry.seeding_complete();
    registry
}

#[test]
fn re_offering_an_admitted_instrument_returns_the_same_handle_and_one_entry() {
    let mut registry = seeded(SelectionPolicy::from_seed(4).expect("4 is a seed"));

    let first = registry.list(&spec("BTC-USD")).expect("admitted");
    let second = registry.list(&spec("BTC-USD")).expect("still admitted");
    let third = registry.list(&spec("BTC-USD")).expect("still admitted");

    assert_eq!(first, second);
    assert_eq!(first, third);
    assert_eq!(registry.instruments().len(), 1);
    assert_eq!(registry.published(), 1);
    // Three offers, one listing. A count that moved per offer is the shape a
    // dashboard would read as a venue relisting its whole universe every poll.
    assert_eq!(registry.counts().admitted, 1);
    // And one `Instrument ID`, not three.
    assert_eq!(
        registry.definition(first).expect("published").instrument_id,
        1
    );
}

#[test]
fn an_instrument_the_policy_declines_is_none_and_is_not_in_the_table() {
    // Seed of one, so the second offer of the first poll is over the seed
    // limit. Declining is the policy working: admission is sticky, so nothing
    // is evicted to make room, and the boundary documents this `None` as
    // ordinary rather than as an error.
    let mut registry = registry(SelectionPolicy::from_seed(1).expect("1 is a seed"));

    let admitted = registry.list(&spec("BTC-USD")).expect("admitted");
    assert!(registry.list(&spec("ETH-USD")).is_none());

    assert_eq!(registry.instruments().len(), 1);
    assert_eq!(registry.published(), 1);
    assert_eq!(registry.last_refusal(), Some(Refusal::Capped));
    assert!(registry.last_refusal().expect("declined").is_ordinary());
    assert_eq!(registry.counts().declined_at_cap, 1);
    // The declined instrument consumed no `Instrument ID`, so the next
    // admission takes the one it would have had.
    assert_eq!(
        registry
            .definition(admitted)
            .expect("published")
            .instrument_id,
        1
    );
}

#[test]
fn the_seed_limit_gives_way_to_the_cap_only_when_seeding_is_complete() {
    // Seed 2, cap 4: the playbook's shape, from one number. The headroom above
    // the seed is what a listing that appears later is admitted into, and
    // spending it on the venue's opening offer is what the two limits exist to
    // prevent.
    let policy = SelectionPolicy::from_seed(2).expect("2 is a seed");
    assert_eq!(policy.bootstrap_top_n(), 2);
    assert_eq!(policy.max_published(), 4);
    assert_eq!(policy.warn_published_above(), 2);

    let mut registry = registry(policy);
    assert_eq!(registry.phase(), Phase::Seeding);
    assert!(registry.list(&spec("AAA")).is_some());
    assert!(registry.list(&spec("BBB")).is_some());
    assert!(
        registry.list(&spec("CCC")).is_none(),
        "the seed limit binds until the first poll has returned"
    );

    registry.seeding_complete();
    assert_eq!(registry.phase(), Phase::Established);
    assert!(registry.list(&spec("CCC")).is_some());
    assert!(registry.list(&spec("DDD")).is_some());
    assert!(
        registry.list(&spec("EEE")).is_none(),
        "the cap binds afterwards"
    );
    assert_eq!(registry.published(), 4);
    // Above the seed, which is where the warning sits so that an operator hears
    // about the headroom while there is still headroom.
    assert!(registry.warns());
}

#[test]
fn a_delisting_withdraws_the_instrument_and_keeps_its_instrument_id_to_itself() {
    let mut registry = seeded(SelectionPolicy::from_seed(4).expect("4 is a seed"));

    let first = registry.list(&spec("AAA")).expect("admitted");
    let second = registry.list(&spec("BBB")).expect("admitted");
    registry.delist(first);

    assert_eq!(registry.published(), 1);
    assert_eq!(registry.instruments().len(), 1);
    assert!(registry.definition(first).is_none());
    assert!(registry.instruments().get(first).is_err());
    assert_eq!(registry.counts().delisted, 1);
    // The neighbour is untouched: slots are never reused and never shift, so a
    // handle an adapter is still carrying cannot come to mean something else.
    assert_eq!(
        registry
            .definition(second)
            .expect("published")
            .instrument_id,
        2
    );

    // A third instrument takes the next ID and not the withdrawn one. A
    // subscriber holding a book keyed on 1 must never find it pointing at
    // something else.
    let third = registry.list(&spec("CCC")).expect("admitted");
    assert_eq!(
        registry.definition(third).expect("published").instrument_id,
        3
    );

    // And the symbol that was withdrawn comes back as itself, because a symbol
    // is the identity and its ID was never given away.
    let relisted = registry.list(&spec("AAA")).expect("admitted again");
    assert_eq!(
        registry
            .definition(relisted)
            .expect("published")
            .instrument_id,
        1
    );
}

#[test]
fn delisting_a_handle_the_registry_does_not_hold_is_silent() {
    // Withdrawing something that is already gone is the state the caller asked
    // for, and an adapter that delists twice is not a fault to report.
    let mut registry = seeded(SelectionPolicy::from_seed(4).expect("4 is a seed"));
    let handle = registry.list(&spec("AAA")).expect("admitted");

    registry.delist(handle);
    registry.delist(handle);
    registry.delist(dz_adapter_core::InstrumentRef::from_admission(9_999));

    assert_eq!(registry.published(), 0);
    assert_eq!(registry.counts().delisted, 1);
}

#[test]
fn nothing_is_admitted_once_shutdown_has_begun() {
    // An `Instrument ID` minted during shutdown is persisted and never
    // published, which is the one way this crate can create the unresolvable ID
    // it exists to prevent.
    let mut registry = seeded(SelectionPolicy::from_seed(4).expect("4 is a seed"));
    let admitted = registry.list(&spec("AAA")).expect("admitted");

    registry.begin_shutdown();

    assert!(registry.list(&spec("BBB")).is_none());
    assert_eq!(registry.last_refusal(), Some(Refusal::ShuttingDown));
    // What was already published stays published: it is still what the last
    // manifest described.
    assert_eq!(registry.published(), 1);
    assert!(registry.definition(admitted).is_some());
}
