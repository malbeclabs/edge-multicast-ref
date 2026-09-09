//! One registry, several published sets, and the two refusals that keep them
//! apart.
//!
//! A shard is the partition a venue names at admission. It says which published
//! set an instrument joins and therefore which channel instance carries it; it
//! says nothing about the `Channel ID`, the group, the ports or the sequence
//! series, and there is no parameter here through which it could. What this
//! file asserts is the part that is invisible from the wire until it is wrong:
//! that the sets do not leak into each other, and that the `Instrument ID`
//! space they draw from is still one.
//!
//! Nothing here touches a filesystem, a socket or a clock that moves on its
//! own: `MemoryStore` and `ManualClock` are the whole environment.

use std::time::Duration;

use dz_adapter_core::{
    AssetClass, InstrumentSpec, ListingSink, MarketModel, PriceBound, Scalar, SettleType,
};
use dz_publisher_lowering::SourceId;
use dz_publisher_refdata::{
    CycleSchedule, ManualClock, MemoryStore, RefdataError, Refusal, Registry, RegistryConfig,
    SelectionPolicy, ShardConfig, StateRecord,
};

/// Two shards of one publisher, named the way a venue names a partition it
/// computes rather than numbered the way a channel is.
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

fn config(shards: Vec<ShardConfig>) -> RegistryConfig {
    RegistryConfig {
        source_id: SourceId::new(7).expect("7 is an assigned production id"),
        shards,
        selection: SelectionPolicy::from_seed(8).expect("8 is a seed"),
        schedule: CycleSchedule::new(Duration::from_secs(30), 1232, 8),
    }
}

fn two_shards() -> Vec<ShardConfig> {
    vec![
        ShardConfig {
            name: ALPHA.to_string(),
            channel_id: 3,
        },
        ShardConfig {
            name: BETA.to_string(),
            channel_id: 4,
        },
    ]
}

/// A registry over `store`, seeded, with the shards it is given.
fn opened(store: MemoryStore, shards: Vec<ShardConfig>) -> Registry<MemoryStore, ManualClock> {
    let mut registry = Registry::open(config(shards), store, ManualClock::new())
        .expect("an empty directory is a cold start");
    registry.seeding_complete();
    registry
}

/// Which instruments one shard's cycle emits, over more than a whole lap.
///
/// Walked to the end rather than sampled, because the property under test is
/// what a subscriber on that channel collects — and a definition that reached
/// the wrong port would arrive somewhere in the lap rather than in its first
/// tick. Distinct, because a cycle retransmits: what is asserted is which
/// instruments appear on the channel, not how often.
fn lapped(
    registry: &mut Registry<MemoryStore, ManualClock>,
    shard: &str,
    clock: &ManualClock,
) -> Vec<u32> {
    let mut out = Vec::new();
    let mut seen = Vec::new();
    registry.definition_tick(shard, &mut out);
    for _ in 0..30 {
        clock.advance(Duration::from_secs(1));
        registry.definition_tick(shard, &mut out);
        seen.extend(out.iter().map(|definition| definition.instrument_id));
    }
    seen.sort_unstable();
    seen.dedup();
    seen
}

#[test]
fn an_admitted_instrument_joins_one_published_set_and_no_other() {
    // The whole point of partitioning the set. An instrument that appeared on a
    // second shard would be counted by a manifest on a channel its quotes never
    // reach, and a subscriber there would collect a definition for an
    // instrument that never updates.
    let clock = ManualClock::new();
    let mut registry = Registry::open(config(two_shards()), MemoryStore::new(), clock.clone())
        .expect("an empty directory is a cold start");
    registry.seeding_complete();

    let here = registry
        .list_on(ALPHA, &spec("AAA"))
        .expect("within the seed");
    let elsewhere = registry
        .list_on(BETA, &spec("BBB"))
        .expect("within the seed");

    assert_eq!(registry.published_on(ALPHA), Some(1));
    assert_eq!(registry.published_on(BETA), Some(1));
    assert_eq!(
        registry
            .manifest(ALPHA)
            .expect("configured")
            .instrument_count,
        1,
        "the count on the wire is the channel's and not the process's"
    );
    assert_eq!(registry.published(), 2, "and the cap sees both");

    // Both shards hold something, so a cycle that packed the process's set onto
    // every port shows up as a definition that is present rather than as a
    // pacer that had nothing to do. Asserted as the whole of what each lap
    // emits: written as "shard A carries A's definition" it would pass against
    // a publisher that carries everything everywhere.
    let mine = registry.definition(here).expect("published").instrument_id;
    let theirs = registry
        .definition(elsewhere)
        .expect("published")
        .instrument_id;
    assert_ne!(mine, theirs);
    assert_eq!(lapped(&mut registry, ALPHA, &clock), vec![mine]);
    assert_eq!(lapped(&mut registry, BETA, &clock), vec![theirs]);
}

#[test]
fn a_shard_nothing_was_admitted_to_is_not_a_shard_everything_left() {
    // Two ways to hold nothing, and an operator has to be able to tell them
    // apart: a shard the venue has never named is a mapping that does not work,
    // while a shard whose instruments have all expired is a market that has
    // ended. Both report an `Instrument Count` of 0, so the answer is
    // `Manifest Seq`, which counts changes rather than members.
    //
    // The instrument is admitted to and withdrawn from the *second* shard, so
    // that a withdrawal booked against the first is a count going below zero
    // rather than a number that happens to look right.
    let mut registry = opened(MemoryStore::new(), two_shards());

    let handle = registry
        .list_on(BETA, &spec("AAA"))
        .expect("within the seed");
    registry.delist(handle);

    assert_eq!(registry.published_on(ALPHA), Some(0));
    assert_eq!(registry.published_on(BETA), Some(0));
    assert_eq!(
        registry.manifest_seq(BETA),
        Some(2),
        "an admission and a withdrawal are two changes"
    );
    assert_eq!(
        registry.manifest_seq(ALPHA),
        Some(0),
        "nothing has ever changed on this channel"
    );

    // Both are `Valid`: the flag says the set is established, not that it has
    // members. A publisher that reported an empty shard as invalid would be
    // reporting a venue with nothing listed as a publisher that had not
    // started.
    assert!(registry.is_valid(ALPHA));
    assert_eq!(registry.manifest(ALPHA).expect("configured").valid, 1);

    // And a shard that was never configured is not a published set at all,
    // which is a different answer again from either of the two above.
    assert_eq!(registry.published_on("gamma"), None);
    assert_eq!(registry.manifest_seq("gamma"), None);
    assert!(registry.manifest("gamma").is_none());
    assert!(!registry.is_valid("gamma"));
}

#[test]
fn one_registry_mints_one_instrument_id_space_across_every_shard() {
    // One registry rather than one per shard, and this is what that buys. Two
    // registries would be two `Instrument ID` spaces over one `Source ID`, and
    // two writers on one state directory - which the single-writer guard would
    // refuse at startup, so the alternative fails loudly rather than subtly and
    // is still a failure.
    let store = MemoryStore::new();
    let mut registry = opened(store.clone(), two_shards());

    let first = registry
        .list_on(ALPHA, &spec("AAA"))
        .expect("within the seed");
    let second = registry
        .list_on(BETA, &spec("BBB"))
        .expect("within the seed");
    let third = registry
        .list_on(ALPHA, &spec("CCC"))
        .expect("within the seed");

    let ids = [first, second, third].map(|handle| {
        registry
            .definition(handle)
            .expect("published")
            .instrument_id
    });
    assert_eq!(ids, [1, 2, 3], "one space, in offer order, across shards");

    let record = StateRecord::decode(&store.record().expect("persisted")).expect("our own bytes");
    assert_eq!(record.next_id, 4);
    assert_eq!(
        record.entries.len(),
        3,
        "one persisted set, not one per shard"
    );
}

#[test]
fn an_unknown_shard_is_refused_and_mints_no_instrument_id() {
    // Refused rather than defaulted. A fallback to some other shard would
    // publish the instrument on a channel nobody chose, and everything about
    // that reads as a working publisher.
    //
    // The `Instrument ID` is the part worth asserting: a refusal that had
    // already minted one would leave a gap in the space that the record
    // carries across every restart from here on.
    let store = MemoryStore::new();
    let mut registry = opened(store.clone(), two_shards());
    registry
        .list_on(ALPHA, &spec("AAA"))
        .expect("within the seed");

    assert!(registry.list_on("gamma", &spec("BBB")).is_none());
    assert_eq!(registry.last_refusal(), Some(Refusal::UnknownShard));
    assert_eq!(registry.counts().declined_unknown_shard, 1);
    assert_eq!(
        registry.counts().declined_unrepresentable,
        0,
        "nothing about the instrument was unstateable"
    );
    assert_eq!(registry.published(), 1);

    // The mint is checked through the store rather than through this registry,
    // because what the refusal must not have cost is the ID the *next* start
    // hands out.
    drop(registry);
    let mut restarted = opened(store, two_shards());
    let recovered = restarted
        .list_on(BETA, &spec("BBB"))
        .expect("within the seed");
    assert_eq!(
        restarted
            .definition(recovered)
            .expect("published")
            .instrument_id,
        2,
        "the refused offer consumed no Instrument ID"
    );
}

#[test]
fn a_re_offer_naming_another_shard_leaves_the_instrument_where_it_is() {
    // The shard is fixed at admission, exactly as an exponent is. Honouring the
    // restatement would move a live instrument between channel instances, and
    // no message in the family says that an instrument moved: a subscriber on
    // the channel it left would see it stop updating, which is what a market
    // going quiet looks like.
    let mut registry = opened(MemoryStore::new(), two_shards());
    let handle = registry
        .list_on(ALPHA, &spec("AAA"))
        .expect("within the seed");
    let seq = registry.manifest_seq(ALPHA).expect("configured");

    assert_eq!(
        registry.list_on(BETA, &spec("AAA")),
        Some(handle),
        "the handle the adapter is carrying is still its instrument's"
    );

    assert_eq!(registry.last_refusal(), Some(Refusal::ShardRestated));
    assert_eq!(registry.counts().declined_shard_restated, 1);
    assert_eq!(registry.published_on(ALPHA), Some(1));
    assert_eq!(registry.published_on(BETA), Some(0));
    assert_eq!(
        registry.manifest_seq(ALPHA),
        Some(seq),
        "nothing about the published set changed"
    );
    assert_eq!(registry.manifest_seq(BETA), Some(0));
}

#[test]
fn an_unknown_shard_name_is_reported_once_however_often_it_is_offered() {
    // An adapter may re-offer its whole set every second, so a report per offer
    // would bury the first one under thousands of copies of itself and the log
    // would cost more than the mistake. Once per distinct value is what a
    // caller needs to name the thing an operator has to fix.
    let mut registry = opened(MemoryStore::new(), two_shards());

    for _ in 0..3 {
        assert!(registry.list_on("gamma", &spec("AAA")).is_none());
    }
    assert!(registry.list_on("delta", &spec("BBB")).is_none());
    assert!(registry.list_on("delta", &spec("CCC")).is_none());

    assert_eq!(
        registry.take_unknown_shards(),
        vec!["gamma".to_string(), "delta".to_string()],
        "each name once, in the order it was first seen"
    );
    assert_eq!(registry.counts().declined_unknown_shard, 5);

    // Taken means taken: a caller that logs whatever it is handed cannot repeat
    // itself on the next poll.
    assert!(registry.take_unknown_shards().is_empty());
    assert!(registry.list_on("gamma", &spec("AAA")).is_none());
    assert!(
        registry.take_unknown_shards().is_empty(),
        "a name already reported is not news"
    );
}

#[test]
fn a_configuration_with_no_shard_does_not_start() {
    // Every offer would be refused as an unknown shard and the publisher would
    // run with an empty set on every channel, which is a feed that is silent
    // for a reason no datagram carries.
    let refused = Registry::open(config(Vec::new()), MemoryStore::new(), ManualClock::new());
    assert!(matches!(refused, Err(RefdataError::NoShardConfigured)));
}

#[test]
fn a_shard_configured_twice_does_not_start() {
    // A name resolves to one published set, so the second block's `Channel ID`
    // would carry a manifest that stayed empty for the life of the process
    // while the venue went on admitting instruments it believed were there.
    let twice = vec![
        ShardConfig {
            name: ALPHA.to_string(),
            channel_id: 3,
        },
        ShardConfig {
            name: ALPHA.to_string(),
            channel_id: 4,
        },
    ];
    let refused = Registry::open(config(twice), MemoryStore::new(), ManualClock::new());
    assert!(matches!(
        refused,
        Err(RefdataError::ShardConfiguredTwice { shard }) if shard == ALPHA
    ));

    // The claim is the thing this must not have taken on its way out: a
    // publisher refused for its own configuration must not lock the state
    // directory against the one that replaces it.
    let store = MemoryStore::new();
    let _ = Registry::open(config(Vec::new()), store.clone(), ManualClock::new());
    assert!(Registry::open(config(two_shards()), store, ManualClock::new()).is_ok());
}
