//! One listings poll, and what it does to `Manifest Seq`.
//!
//! A poll that changes a shard's set advances that shard's `Manifest Seq` by
//! one, however many instruments it admitted or withdrew. The field says the
//! set changed; how many changes went into it is not what it carries. An
//! advance for each of them moves the seq by `k` between two manifests, which
//! is what the specification's `REFDATA.SEQ_BUMP_NOT_BY_ONE` reports, and each
//! advance costs a subscriber the definitions it has collected.
//!
//! Every count below is of more than one instrument, because one admission in
//! one poll is one advance under either rule.

use dz_adapter_core::{
    AssetClass, InstrumentRef, InstrumentSpec, ListingSink, MarketModel, PriceBound, Scalar,
    SettleType,
};
use dz_publisher_lowering::SourceId;
use dz_publisher_refdata::{
    CycleSchedule, ManualClock, MemoryStore, Registry, RegistryConfig, SelectionPolicy, ShardConfig,
};

const ALPHA: &str = "alpha";
const BETA: &str = "beta";

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

/// A registry with two shards and room for 64 instruments, still seeding.
fn registry() -> Registry<MemoryStore, ManualClock> {
    let config = RegistryConfig {
        source_id: SourceId::new(7).expect("7 is an assigned production id"),
        shards: vec![
            ShardConfig {
                name: ALPHA.to_string(),
                channel_id: 3,
            },
            ShardConfig {
                name: BETA.to_string(),
                channel_id: 4,
            },
        ],
        selection: SelectionPolicy::from_seed(64).expect("a seed"),
        schedule: CycleSchedule::new(std::time::Duration::from_secs(30), 1232, 8),
        forget_delisted_after: None,
    };
    Registry::open(config, MemoryStore::new(), ManualClock::new())
        .expect("an empty directory is a cold start")
}

/// One poll that offers `symbols` on `shard`, and the handles it was given.
fn poll_offering(
    registry: &mut Registry<MemoryStore, ManualClock>,
    shard: &str,
    symbols: &[&str],
) -> Vec<InstrumentRef> {
    registry.poll(|listings| {
        symbols
            .iter()
            .map(|symbol| listings.list_on(shard, &spec(symbol)).expect("admitted"))
            .collect()
    })
}

fn seq(registry: &Registry<MemoryStore, ManualClock>, shard: &str) -> u16 {
    registry.manifest_seq(shard).expect("configured")
}

#[test]
fn a_poll_that_admits_several_instruments_advances_the_seq_by_one() {
    let mut registry = registry();
    poll_offering(&mut registry, ALPHA, &["A0"]);
    registry.seeding_complete();
    let before = seq(&registry, ALPHA);

    poll_offering(&mut registry, ALPHA, &["A0", "A1", "A2", "A3", "A4"]);

    assert_eq!(registry.published_on(ALPHA), Some(5));
    assert_eq!(seq(&registry, ALPHA), before + 1);
}

#[test]
fn a_poll_that_withdraws_several_instruments_advances_the_seq_by_one() {
    let mut registry = registry();
    let handles = poll_offering(&mut registry, ALPHA, &["A0", "A1", "A2", "A3"]);
    registry.seeding_complete();
    let before = seq(&registry, ALPHA);

    registry.poll(|listings| {
        for &handle in &handles[..3] {
            listings.delist(handle);
        }
    });

    assert_eq!(registry.published_on(ALPHA), Some(1));
    assert_eq!(seq(&registry, ALPHA), before + 1);
}

#[test]
fn a_poll_that_admits_and_withdraws_advances_the_seq_by_one() {
    // What a roll is: one window's instruments leave and the next window's
    // arrive, in one answer from the venue.
    let mut registry = registry();
    let handles = poll_offering(&mut registry, ALPHA, &["A0", "A1", "A2"]);
    registry.seeding_complete();
    let before = seq(&registry, ALPHA);

    registry.poll(|listings| {
        listings.delist(handles[0]);
        listings.delist(handles[1]);
        for symbol in ["A3", "A4", "A5"] {
            listings.list_on(ALPHA, &spec(symbol)).expect("admitted");
        }
    });

    assert_eq!(registry.published_on(ALPHA), Some(4));
    assert_eq!(seq(&registry, ALPHA), before + 1);
}

#[test]
fn a_poll_that_changes_nothing_does_not_advance_the_seq() {
    let mut registry = registry();
    poll_offering(&mut registry, ALPHA, &["A0", "A1"]);
    poll_offering(&mut registry, BETA, &["B0"]);
    registry.seeding_complete();
    let alpha = seq(&registry, ALPHA);
    let beta = seq(&registry, BETA);

    // An adapter offers its whole set on every poll. A seq that advanced for
    // that would advance every second and mean nothing.
    for _ in 0..3 {
        poll_offering(&mut registry, ALPHA, &["A0", "A1"]);
        poll_offering(&mut registry, BETA, &["B0"]);
    }
    // And a poll that offers nothing at all.
    registry.poll(|_listings| {});

    assert_eq!(seq(&registry, ALPHA), alpha);
    assert_eq!(seq(&registry, BETA), beta);
}

#[test]
fn a_poll_whose_offers_are_all_declined_does_not_advance_the_seq() {
    let mut registry = registry();
    let handles = poll_offering(&mut registry, ALPHA, &["A0", "A1"]);
    registry.seeding_complete();
    registry.poll(|listings| listings.delist(handles[1]));
    let before = seq(&registry, ALPHA);

    registry.poll(|listings| {
        // A shard this publisher has none of, and a delisting of a handle
        // already withdrawn: neither changes a set.
        assert!(listings.list_on("gamma", &spec("G0")).is_none());
        listings.delist(handles[1]);
    });

    assert_eq!(seq(&registry, ALPHA), before);
    assert_eq!(seq(&registry, BETA), 0);
}

#[test]
fn each_shard_advances_for_its_own_changes_and_by_one() {
    let mut registry = registry();
    registry.poll(|listings| {
        for symbol in ["A0", "A1", "A2"] {
            listings.list_on(ALPHA, &spec(symbol)).expect("admitted");
        }
        for symbol in ["B0", "B1"] {
            listings.list_on(BETA, &spec(symbol)).expect("admitted");
        }
    });
    registry.seeding_complete();
    // One poll changed both sets, and each advanced once: not once for the
    // process, and not once for each of the five admissions.
    assert_eq!(seq(&registry, ALPHA), 1);
    assert_eq!(seq(&registry, BETA), 1);

    // A poll that changes only one of them leaves the other where it was.
    poll_offering(&mut registry, BETA, &["B0", "B1", "B2", "B3"]);
    assert_eq!(seq(&registry, ALPHA), 1);
    assert_eq!(seq(&registry, BETA), 2);

    poll_offering(&mut registry, ALPHA, &["A0", "A1", "A2", "A3", "A4"]);
    assert_eq!(seq(&registry, ALPHA), 2);
    assert_eq!(seq(&registry, BETA), 2);
}

#[test]
fn the_first_valid_manifest_carries_one_whatever_the_size_of_the_seed() {
    // The seed is one poll, so it is one change: from no set to the first one.
    // Before, the first `Valid` manifest of a process stated the size of the
    // set in a field that counts changes.
    let mut registry = registry();
    let before = registry.manifest(ALPHA).expect("configured");
    assert_eq!((before.valid, before.manifest_seq), (0, 0));

    let symbols: Vec<String> = (0..40).map(|index| format!("A{index}")).collect();
    let symbols: Vec<&str> = symbols.iter().map(String::as_str).collect();
    poll_offering(&mut registry, ALPHA, &symbols);
    registry.seeding_complete();

    let first = registry.manifest(ALPHA).expect("configured");
    assert_eq!(first.valid, 1);
    assert_eq!(first.instrument_count, 40);
    assert_eq!(first.manifest_seq, 1);
}

#[test]
fn a_definition_emitted_after_a_poll_carries_the_seq_the_poll_ended_on() {
    let mut registry = registry();
    let handles = poll_offering(&mut registry, ALPHA, &["A0", "A1", "A2"]);
    registry.seeding_complete();

    for &handle in &handles {
        assert_eq!(
            registry.definition(handle).expect("published").manifest_seq,
            seq(&registry, ALPHA)
        );
    }
    assert_eq!(seq(&registry, ALPHA), 1);
}

#[test]
fn a_restated_definition_shares_the_polls_one_advance() {
    // A definition whose content changed is a change to what the manifest
    // describes, and it is one of the poll's changes rather than its own.
    let mut registry = registry();
    poll_offering(&mut registry, ALPHA, &["A0", "A1"]);
    registry.seeding_complete();
    let before = seq(&registry, ALPHA);

    registry.poll(|listings| {
        let restated = InstrumentSpec {
            tick_size: Scalar::text("0.0005"),
            ..spec("A0")
        };
        listings.list_on(ALPHA, &restated).expect("still admitted");
        listings
            .list_on(ALPHA, &spec("A1"))
            .expect("still admitted");
        listings.list_on(ALPHA, &spec("A2")).expect("admitted");
    });

    assert_eq!(seq(&registry, ALPHA), before + 1);
}

#[test]
fn a_change_made_outside_a_poll_advances_the_seq_at_once() {
    // The runtime makes no such call. A caller that does has no poll whose end
    // could carry the advance, and a set that changed under a seq that did not
    // is what the field exists to prevent.
    let mut registry = registry();
    let first = registry.list_on(ALPHA, &spec("A0")).expect("admitted");
    assert_eq!(seq(&registry, ALPHA), 1);
    registry.list_on(ALPHA, &spec("A1")).expect("admitted");
    assert_eq!(seq(&registry, ALPHA), 2);
    registry.delist(first);
    assert_eq!(seq(&registry, ALPHA), 3);

    // And the next poll starts clean: nothing is left over to advance for.
    registry.poll(|_listings| {});
    assert_eq!(seq(&registry, ALPHA), 3);
}

#[test]
fn the_seq_wraps_to_one_and_a_poll_still_moves_it_by_one() {
    let mut registry = registry();
    // 65,535 single changes outside a poll, which is the short way to the top.
    let mut handle = registry.list_on(ALPHA, &spec("A0")).expect("admitted");
    for _ in 0..(u16::MAX as usize - 1) / 2 {
        registry.delist(handle);
        handle = registry.list_on(ALPHA, &spec("A0")).expect("admitted");
    }
    assert_eq!(seq(&registry, ALPHA), u16::MAX);

    poll_offering(&mut registry, ALPHA, &["A0", "A1", "A2"]);
    assert_eq!(seq(&registry, ALPHA), 1);
}
