//! The strike of an instrument: what is published, when, and what is refused.
//!
//! Every rule here is one of the publisher rules for `StrikeInterval` in the
//! reference-data supplement, held at the registry because the registry is
//! what decides what a definition is followed by.

use std::time::Duration;

use dz_adapter_core::{
    AssetClass, InstrumentRef, InstrumentSpec, ListingSink, MarketModel, PriceBound, Scalar,
    SettleType, Strike, StrikeBound, DEFAULT_SHARD,
};
use dz_edge_refdata::StrikeInterval;
use dz_publisher_lowering::SourceId;
use dz_publisher_refdata::{
    CycleSchedule, ManualClock, MemoryStore, PublishedDefinition, Refusal, Registry,
    RegistryConfig, SelectionPolicy, ShardConfig,
};

const MTU: u16 = 1232;
const CYCLE: Duration = Duration::from_secs(30);
/// 80% of the cycle: the lap the pacer spreads the set across.
const LAP: Duration = Duration::from_secs(24);
const ALPHA: &str = "alpha";
const BETA: &str = "beta";

type TestRegistry = Registry<MemoryStore, ManualClock>;

fn spec(symbol: &str) -> InstrumentSpec<'_> {
    InstrumentSpec {
        symbol,
        leg1: None,
        leg2: None,
        asset_class: AssetClass::PredictionBinary,
        price_exponent: -4,
        qty_exponent: -2,
        market_model: MarketModel::Clob,
        tick_size: Scalar::text("0.0001"),
        lot_size: Scalar::text("0.01"),
        contract_value: None,
        quoted_per_contract: None,
        expiry_ns: None,
        settle_type: SettleType::Cash,
        price_bound: PriceBound::UnitInterval,
    }
}

fn registry(shards: Vec<ShardConfig>, cap: usize) -> (TestRegistry, ManualClock) {
    let clock = ManualClock::new();
    let config = RegistryConfig {
        source_id: SourceId::new(7).expect("7 is an assigned production id"),
        shards,
        selection: SelectionPolicy::from_seed(cap).expect("a seed"),
        schedule: CycleSchedule::new(CYCLE, MTU, 1),
        forget_delisted_after: None,
    };
    let registry = Registry::open(config, MemoryStore::new(), clock.clone())
        .expect("an empty directory is a cold start");
    (registry, clock)
}

fn one_shard(cap: usize) -> (TestRegistry, ManualClock) {
    registry(vec![ShardConfig::default_shard(3)], cap)
}

fn at_or_above(value: &str) -> Strike<'_> {
    Strike::Fixed {
        lower: Some(StrikeBound::inclusive(Scalar::text(value))),
        upper: None,
        fixing_time_ns: None,
    }
}

/// One whole lap of the cycle, a second at a time, and everything it emitted.
fn lap(registry: &mut TestRegistry, clock: &ManualClock, shard: &str) -> Vec<PublishedDefinition> {
    let mut out = Vec::new();
    let mut seen = Vec::new();
    // The first tick with a published set starts the lap and owes nothing.
    registry.definition_tick(shard, &mut out);
    seen.extend(out.iter().copied());
    for _ in 0..LAP.as_secs() {
        clock.advance(Duration::from_secs(1));
        registry.definition_tick(shard, &mut out);
        seen.extend(out.iter().copied());
    }
    seen
}

fn changes(registry: &mut TestRegistry, shard: &str) -> Vec<PublishedDefinition> {
    let mut out = Vec::new();
    registry.strike_changes(shard, &mut out);
    out
}

fn strike_of(entries: &[PublishedDefinition], instrument_id: u32) -> Option<StrikeInterval> {
    entries
        .iter()
        .rev()
        .find(|entry| entry.definition.instrument_id == instrument_id)
        .unwrap_or_else(|| panic!("instrument {instrument_id} was not defined"))
        .strike
}

fn id_of(registry: &TestRegistry, handle: InstrumentRef) -> u32 {
    registry
        .definition(handle)
        .expect("published")
        .instrument_id
}

#[test]
fn an_instrument_no_strike_is_stated_for_is_defined_alone() {
    let (mut registry, clock) = one_shard(4);
    let handle = registry.list(&spec("PAYS-ON-NO-STRIKE")).unwrap();
    let id = id_of(&registry, handle);

    let seen = lap(&mut registry, &clock, DEFAULT_SHARD);
    assert_eq!(strike_of(&seen, id), None);
    assert_eq!(registry.strikes_pending_on(DEFAULT_SHARD), Some(0));
}

/// Rule 1: from the first definition, pending until the venue fixes it.
#[test]
fn a_strike_stated_pending_at_listing_follows_the_first_definition() {
    let (mut registry, clock) = one_shard(4);
    let handle = registry.list(&spec("WINDOW")).unwrap();
    registry.strike(handle, &Strike::Pending);
    let id = id_of(&registry, handle);
    assert_eq!(registry.strikes_pending_on(DEFAULT_SHARD), Some(1));

    let seen = lap(&mut registry, &clock, DEFAULT_SHARD);
    let strike = strike_of(&seen, id).expect("a strike follows the definition");
    assert!(strike.is_pending());
    assert_eq!((strike.instrument_id, strike.source_id), (id, 7));
    assert!(
        changes(&mut registry, DEFAULT_SHARD).is_empty(),
        "nothing changed after the first definition"
    );
}

/// Rules 3 and 4: sent when the venue fixes it, and `Manifest Seq` does not
/// move.
#[test]
fn a_pending_strike_the_venue_fixes_is_owed_at_once_and_the_manifest_stands() {
    let (mut registry, clock) = one_shard(4);
    let handle = registry.list(&spec("WINDOW")).unwrap();
    registry.strike(handle, &Strike::Pending);
    let id = id_of(&registry, handle);
    lap(&mut registry, &clock, DEFAULT_SHARD);
    let seq = registry.manifest_seq(DEFAULT_SHARD);
    let count = registry.manifest(DEFAULT_SHARD).unwrap().instrument_count;

    assert_eq!(
        registry.state_strike(handle, &at_or_above("82506.23")),
        Ok(())
    );

    assert_eq!(registry.manifest_seq(DEFAULT_SHARD), seq);
    assert_eq!(
        registry.manifest(DEFAULT_SHARD).unwrap().instrument_count,
        count
    );
    assert_eq!(registry.strikes_pending_on(DEFAULT_SHARD), Some(0));

    // No time has passed, so the cycle owes nothing; the change is owed anyway.
    let owed = changes(&mut registry, DEFAULT_SHARD);
    assert_eq!(owed.len(), 1);
    assert_eq!(owed[0].definition.instrument_id, id);
    assert_eq!(owed[0].definition.manifest_seq, seq.unwrap());
    let strike = owed[0].strike.expect("the pair");
    assert_eq!(
        (
            strike.strike_exponent,
            strike.bound_flags,
            strike.lower_bound
        ),
        (-2, 0b0011, 8_250_623)
    );
    assert!(
        changes(&mut registry, DEFAULT_SHARD).is_empty(),
        "owed once"
    );
    assert_eq!(registry.counts().strikes_changed, 1);

    // And the cycle carries the fixed strike from then on.
    let seen = lap(&mut registry, &clock, DEFAULT_SHARD);
    assert_eq!(strike_of(&seen, id), Some(strike));
}

#[test]
fn a_strike_fixed_at_listing_goes_out_with_the_first_definition_and_not_before() {
    let (mut registry, clock) = one_shard(4);
    let handle = registry.list(&spec("FIXED-AT-LISTING")).unwrap();
    registry.strike(handle, &at_or_above("100"));
    let id = id_of(&registry, handle);

    assert!(
        changes(&mut registry, DEFAULT_SHARD).is_empty(),
        "no definition has gone out, so there is nothing to send again"
    );
    assert_eq!(registry.counts().strikes_changed, 0);
    let seen = lap(&mut registry, &clock, DEFAULT_SHARD);
    assert!(!strike_of(&seen, id).expect("a strike").is_pending());
}

/// An adapter re-offers its whole set each poll and states each strike with it.
#[test]
fn restating_the_same_strike_changes_nothing_however_it_is_written() {
    let (mut registry, clock) = one_shard(4);
    let handle = registry.list(&spec("WINDOW")).unwrap();
    registry.strike(handle, &at_or_above("0.0926679"));
    lap(&mut registry, &clock, DEFAULT_SHARD);
    let seq = registry.manifest_seq(DEFAULT_SHARD);

    for _ in 0..3 {
        registry.list(&spec("WINDOW")).unwrap();
        registry.strike(handle, &at_or_above("0.0926679"));
    }
    // The same value with other digits, as two of a venue's own readers give it.
    registry.strike(handle, &at_or_above("0.09266790"));
    registry.strike(
        handle,
        &Strike::Fixed {
            lower: Some(StrikeBound::inclusive(Scalar::fixed(926_679, -7))),
            upper: None,
            fixing_time_ns: None,
        },
    );

    assert!(changes(&mut registry, DEFAULT_SHARD).is_empty());
    assert_eq!(registry.counts().strikes_changed, 0);
    assert_eq!(registry.manifest_seq(DEFAULT_SHARD), seq);
    assert_eq!(registry.last_refusal(), None);
}

/// Rule 6: the venue corrects a fixed strike.
#[test]
fn a_corrected_strike_replaces_the_fixed_one_and_is_owed_at_once() {
    let (mut registry, clock) = one_shard(4);
    let handle = registry.list(&spec("WINDOW")).unwrap();
    registry.strike(handle, &at_or_above("82506.23"));
    lap(&mut registry, &clock, DEFAULT_SHARD);
    let seq = registry.manifest_seq(DEFAULT_SHARD);

    registry.strike(handle, &at_or_above("82506.32"));

    let owed = changes(&mut registry, DEFAULT_SHARD);
    assert_eq!(owed.len(), 1);
    assert_eq!(owed[0].strike.unwrap().lower_bound, 8_250_632);
    assert_eq!(registry.manifest_seq(DEFAULT_SHARD), seq);
}

/// Rule 5: a fixed strike does not return to pending.
#[test]
fn a_fixed_strike_restated_as_pending_is_refused_and_the_fixed_one_stands() {
    let (mut registry, clock) = one_shard(4);
    let handle = registry.list(&spec("WINDOW")).unwrap();
    registry.strike(handle, &Strike::Pending);
    registry.strike(handle, &at_or_above("82506.23"));
    let id = id_of(&registry, handle);
    lap(&mut registry, &clock, DEFAULT_SHARD);

    assert_eq!(
        registry.state_strike(handle, &Strike::Pending),
        Err(Refusal::StrikeUnfixed)
    );

    assert_eq!(registry.counts().declined_strike_unfixed, 1);
    assert_eq!(registry.last_refusal(), Some(Refusal::StrikeUnfixed));
    assert_eq!(registry.strikes_pending_on(DEFAULT_SHARD), Some(0));
    assert!(changes(&mut registry, DEFAULT_SHARD).is_empty());
    let seen = lap(&mut registry, &clock, DEFAULT_SHARD);
    assert_eq!(strike_of(&seen, id).unwrap().lower_bound, 8_250_623);
}

/// `STRIKE.PRESENCE_STABLE`: what the first definition carried is settled.
#[test]
fn a_strike_stated_after_the_instrument_was_defined_without_one_is_refused() {
    let (mut registry, clock) = one_shard(4);
    let handle = registry.list(&spec("LATE")).unwrap();
    let id = id_of(&registry, handle);
    lap(&mut registry, &clock, DEFAULT_SHARD);

    for stated in [Strike::Pending, at_or_above("100")] {
        assert_eq!(
            registry.state_strike(handle, &stated),
            Err(Refusal::StrikePresence)
        );
    }

    assert_eq!(registry.counts().declined_strike_presence, 2);
    assert!(changes(&mut registry, DEFAULT_SHARD).is_empty());
    let seen = lap(&mut registry, &clock, DEFAULT_SHARD);
    assert_eq!(strike_of(&seen, id), None, "still published without one");
}

#[test]
fn a_strike_the_wire_cannot_carry_is_refused_and_counted_with_the_unrepresentable() {
    let (mut registry, _clock) = one_shard(4);
    let handle = registry.list(&spec("WINDOW")).unwrap();
    registry.strike(handle, &Strike::Pending);
    let before = registry.counts().declined_unrepresentable;

    assert_eq!(
        registry.state_strike(handle, &at_or_above("TBD")),
        Err(Refusal::StrikeUnrepresentable)
    );
    let empty = Strike::Fixed {
        lower: Some(StrikeBound::inclusive(Scalar::text("201"))),
        upper: Some(StrikeBound::inclusive(Scalar::text("200"))),
        fixing_time_ns: None,
    };
    assert_eq!(
        registry.state_strike(handle, &empty),
        Err(Refusal::StrikeEmpty)
    );

    assert_eq!(registry.counts().declined_unrepresentable, before + 2);
    assert_eq!(
        registry.strikes_pending_on(DEFAULT_SHARD),
        Some(1),
        "the pending strike stands"
    );
}

#[test]
fn a_withdrawn_instrument_owes_nothing_and_is_no_longer_pending() {
    let (mut registry, clock) = one_shard(4);
    let pending = registry.list(&spec("PENDING")).unwrap();
    registry.strike(pending, &Strike::Pending);
    let fixed = registry.list(&spec("FIXED")).unwrap();
    registry.strike(fixed, &Strike::Pending);
    lap(&mut registry, &clock, DEFAULT_SHARD);
    registry.strike(fixed, &at_or_above("1"));
    assert_eq!(registry.strikes_pending_on(DEFAULT_SHARD), Some(1));

    registry.delist(pending);
    registry.delist(fixed);

    assert_eq!(registry.strikes_pending_on(DEFAULT_SHARD), Some(0));
    assert!(changes(&mut registry, DEFAULT_SHARD).is_empty());
    // A handle that no longer names anything is ignored, as `delist` ignores it.
    assert_eq!(registry.state_strike(fixed, &at_or_above("2")), Ok(()));
    assert_eq!(registry.strikes_pending_on("no-such-shard"), None);
}

/// A relisted symbol is a new publication: it may have a strike or not, afresh.
#[test]
fn a_relisted_instrument_states_its_strike_again() {
    let (mut registry, clock) = one_shard(4);
    let handle = registry.list(&spec("WINDOW")).unwrap();
    registry.strike(handle, &at_or_above("1"));
    lap(&mut registry, &clock, DEFAULT_SHARD);
    registry.strike(handle, &at_or_above("2"));
    registry.delist(handle);

    let again = registry.list(&spec("WINDOW")).unwrap();
    assert!(
        changes(&mut registry, DEFAULT_SHARD).is_empty(),
        "the change owed by the withdrawn publication is not owed by this one"
    );
    assert_eq!(
        registry.state_strike(again, &Strike::Pending),
        Ok(()),
        "not defined yet, so it may still state one"
    );
    let id = id_of(&registry, again);
    let seen = lap(&mut registry, &clock, DEFAULT_SHARD);
    assert!(strike_of(&seen, id).unwrap().is_pending());
}

#[test]
fn a_changed_strike_is_owed_on_its_own_shard_only() {
    let shards = vec![
        ShardConfig {
            name: ALPHA.to_string(),
            channel_id: 3,
        },
        ShardConfig {
            name: BETA.to_string(),
            channel_id: 4,
        },
    ];
    let (mut registry, clock) = registry(shards, 4);
    let on_alpha = registry.list_on(ALPHA, &spec("A")).unwrap();
    let on_beta = registry.list_on(BETA, &spec("B")).unwrap();
    registry.strike(on_alpha, &Strike::Pending);
    registry.strike(on_beta, &Strike::Pending);
    lap(&mut registry, &clock, ALPHA);
    lap(&mut registry, &clock, BETA);
    let beta_seq = registry.manifest_seq(BETA);

    registry.strike(on_alpha, &at_or_above("1"));

    assert!(changes(&mut registry, BETA).is_empty());
    assert_eq!(registry.strikes_pending_on(BETA), Some(1));
    assert_eq!(registry.manifest_seq(BETA), beta_seq);
    let owed = changes(&mut registry, ALPHA);
    assert_eq!(owed.len(), 1);
    assert_eq!(owed[0].definition.instrument_id, id_of(&registry, on_alpha));
    assert_eq!(registry.strikes_pending_on(ALPHA), Some(0));
}

/// A definition with a strike is 170 bytes and seven fit one datagram, where
/// nine bare definitions do: 24 + 7 x 170 is 1,214 of 1,232 bytes, and an eighth
/// would be 1,384. One tick is one datagram here, so it holds seven.
#[test]
fn a_tick_of_a_set_with_strikes_is_what_one_datagram_holds() {
    let (mut registry, clock) = one_shard(64);
    for index in 0..64 {
        let handle = registry.list(&spec(&format!("W{index}"))).unwrap();
        registry.strike(handle, &Strike::Pending);
    }
    let mut out = Vec::new();
    registry.definition_tick(DEFAULT_SHARD, &mut out);
    assert!(out.is_empty(), "the lap starts here");

    // Far enough into the lap that more is owed than a tick may carry.
    clock.advance(Duration::from_secs(12));
    registry.definition_tick(DEFAULT_SHARD, &mut out);
    assert_eq!(out.len(), 7);
    assert_eq!(
        out.iter()
            .map(PublishedDefinition::wire_size)
            .sum::<usize>(),
        7 * 170
    );
}

#[test]
fn a_mixed_tick_is_filled_as_the_datagram_is() {
    // Bare, then with a strike, alternating: 130 + 170 a pair of entries. Four
    // such pairs are 1,200 of the 1,208 bytes, so a tick holds eight entries,
    // between the seven of a set that all carry strikes and the nine of one
    // that carries none.
    let (mut registry, clock) = one_shard(64);
    for index in 0..64 {
        let handle = registry.list(&spec(&format!("W{index}"))).unwrap();
        if index % 2 == 1 {
            registry.strike(handle, &Strike::Pending);
        }
    }
    let mut out = Vec::new();
    registry.definition_tick(DEFAULT_SHARD, &mut out);
    clock.advance(Duration::from_secs(12));
    registry.definition_tick(DEFAULT_SHARD, &mut out);
    assert_eq!(out.len(), 8);
    assert_eq!(
        out.iter()
            .map(PublishedDefinition::wire_size)
            .sum::<usize>(),
        1200
    );
}

/// What a tick holds back is still owed, so the lap still covers the set.
#[test]
fn a_lap_of_a_set_with_strikes_still_defines_every_instrument_once() {
    let (mut registry, clock) = one_shard(40);
    let mut ids = Vec::new();
    for index in 0..40 {
        let handle = registry.list(&spec(&format!("W{index}"))).unwrap();
        registry.strike(handle, &Strike::Pending);
        ids.push(id_of(&registry, handle));
    }
    let seen = lap(&mut registry, &clock, DEFAULT_SHARD);
    let mut defined: Vec<u32> = seen
        .iter()
        .map(|entry| entry.definition.instrument_id)
        .collect();
    defined.sort_unstable();
    ids.sort_unstable();
    assert_eq!(defined, ids);
    assert!(seen.iter().all(|entry| entry.strike.is_some()));
}

/// A venue that fixes every strike in one poll is spread over the ticks that
/// follow, oldest first, and is not a burst.
///
/// Six to a tick, not the seven a datagram holds: the caller sends a 24-byte
/// `ManifestSummary` ahead of them, and 24 + 7 x 170 is 1,214 of the 1,208
/// bytes a datagram has for messages. A seventh would leave in a second
/// datagram, without the summary.
#[test]
fn many_strikes_fixed_at_once_are_owed_a_datagram_at_a_time() {
    let (mut registry, clock) = one_shard(20);
    let mut handles = Vec::new();
    for index in 0..20 {
        let handle = registry.list(&spec(&format!("W{index}"))).unwrap();
        registry.strike(handle, &Strike::Pending);
        handles.push(handle);
    }
    lap(&mut registry, &clock, DEFAULT_SHARD);
    for handle in &handles {
        registry.strike(*handle, &at_or_above("82506.23"));
    }
    assert_eq!(registry.strikes_pending_on(DEFAULT_SHARD), Some(0));

    let mut order = Vec::new();
    let mut sizes = Vec::new();
    loop {
        let owed = changes(&mut registry, DEFAULT_SHARD);
        if owed.is_empty() {
            break;
        }
        sizes.push(owed.len());
        order.extend(owed.iter().map(|entry| entry.definition.instrument_id));
    }
    assert_eq!(sizes, [6, 6, 6, 2]);
    let expected: Vec<u32> = handles.iter().map(|h| id_of(&registry, *h)).collect();
    assert_eq!(order, expected, "oldest first, each once");
}

/// A strike fixed and then corrected before it was sent is owed once, as it is
/// now.
#[test]
fn two_changes_before_a_tick_are_one_definition_carrying_the_last() {
    let (mut registry, clock) = one_shard(4);
    let handle = registry.list(&spec("WINDOW")).unwrap();
    registry.strike(handle, &Strike::Pending);
    lap(&mut registry, &clock, DEFAULT_SHARD);

    registry.strike(handle, &at_or_above("1"));
    registry.strike(handle, &at_or_above("2"));

    let owed = changes(&mut registry, DEFAULT_SHARD);
    assert_eq!(owed.len(), 1);
    assert_eq!(owed[0].strike.unwrap().lower_bound, 2);
    assert_eq!(registry.counts().strikes_changed, 2);
}

/// Rule 1 is a MUST: a contract that pays on a strike has one from its first
/// definition. A venue that says so in a way the wire cannot carry has still
/// said so.
#[test]
fn a_first_statement_the_wire_cannot_carry_leaves_the_strike_pending() {
    let neither_bound = Strike::Fixed {
        lower: None,
        upper: None,
        fixing_time_ns: None,
    };
    for (what, stated) in [
        ("not a decimal", at_or_above("TBD")),
        ("neither bound", neither_bound),
    ] {
        let (mut registry, clock) = one_shard(4);
        let handle = registry.list(&spec("WINDOW")).unwrap();
        let id = id_of(&registry, handle);

        assert_eq!(
            registry.state_strike(handle, &stated),
            Err(Refusal::StrikeUnrepresentable),
            "{what}"
        );
        assert_eq!(registry.counts().declined_unrepresentable, 1, "{what}");
        assert_eq!(
            registry.strikes_pending_on(DEFAULT_SHARD),
            Some(1),
            "{what}"
        );

        let seen = lap(&mut registry, &clock, DEFAULT_SHARD);
        assert!(
            strike_of(&seen, id)
                .unwrap_or_else(|| panic!("{what}: defined without a strike"))
                .is_pending(),
            "{what}"
        );

        // And the strike the venue states properly a poll later is taken.
        assert_eq!(
            registry.state_strike(handle, &at_or_above("82506.23")),
            Ok(()),
            "{what}"
        );
        assert_eq!(
            registry.strikes_pending_on(DEFAULT_SHARD),
            Some(0),
            "{what}"
        );
        let owed = changes(&mut registry, DEFAULT_SHARD);
        assert_eq!(owed.len(), 1, "{what}");
        assert_eq!(owed[0].strike.unwrap().lower_bound, 8_250_623, "{what}");
    }
}

/// Only the first statement: a strike already held is not replaced by a
/// statement that is refused.
#[test]
fn a_later_statement_the_wire_cannot_carry_leaves_the_held_strike() {
    let (mut registry, clock) = one_shard(4);
    let handle = registry.list(&spec("WINDOW")).unwrap();
    registry.strike(handle, &at_or_above("82506.23"));
    let id = id_of(&registry, handle);

    assert_eq!(
        registry.state_strike(handle, &at_or_above("TBD")),
        Err(Refusal::StrikeUnrepresentable)
    );

    assert_eq!(registry.strikes_pending_on(DEFAULT_SHARD), Some(0));
    let seen = lap(&mut registry, &clock, DEFAULT_SHARD);
    assert_eq!(strike_of(&seen, id).unwrap().lower_bound, 8_250_623);
}

/// What the cycle hands out on a shard no strike was stated on is what it
/// handed out before there were strikes: nine to a tick.
#[test]
fn a_shard_with_no_strike_is_paced_as_it_was() {
    let (mut registry, clock) = one_shard(64);
    for index in 0..64 {
        registry.list(&spec(&format!("W{index}"))).unwrap();
    }
    let mut out = Vec::new();
    registry.definition_tick(DEFAULT_SHARD, &mut out);
    clock.advance(Duration::from_secs(12));
    registry.definition_tick(DEFAULT_SHARD, &mut out);
    assert_eq!(out.len(), 9);
}

/// One instrument with a strike among many without is enough to change what a
/// tick holds, and only on the tick that carries it.
#[test]
fn one_strike_on_a_shard_costs_the_tick_that_carries_it_and_no_other() {
    let (mut registry, clock) = one_shard(64);
    for index in 0..64 {
        let handle = registry.list(&spec(&format!("W{index}"))).unwrap();
        if index == 4 {
            registry.strike(handle, &Strike::Pending);
        }
    }
    let mut out = Vec::new();
    registry.definition_tick(DEFAULT_SHARD, &mut out);
    clock.advance(Duration::from_secs(12));

    // Nine bare definitions are 1,170 bytes; with one of them 40 bytes larger
    // the ninth no longer fits the 1,208.
    registry.definition_tick(DEFAULT_SHARD, &mut out);
    assert_eq!(out.len(), 8);
    assert_eq!(
        out.iter()
            .map(PublishedDefinition::wire_size)
            .sum::<usize>(),
        7 * 130 + 170
    );
    registry.definition_tick(DEFAULT_SHARD, &mut out);
    assert_eq!(out.len(), 9, "the next nine are all bare");
}

fn at_or_above_fixed_at(value: &str, fixing_time_ns: Option<u64>) -> Strike<'_> {
    Strike::Fixed {
        lower: Some(StrikeBound::inclusive(Scalar::text(value))),
        upper: None,
        fixing_time_ns,
    }
}

/// Rule 6: a different fixed strike goes out only when the venue corrects it.
/// A venue that gives the fixing time on one of its paths and not on another
/// has corrected nothing.
#[test]
fn a_fixing_time_that_comes_and_goes_is_not_a_correction() {
    const FIXED_AT: u64 = 1_700_000_000_000_000_000;
    let (mut registry, clock) = one_shard(4);
    let handle = registry.list(&spec("WINDOW")).unwrap();
    registry.strike(handle, &at_or_above_fixed_at("92700", None));
    let id = id_of(&registry, handle);
    lap(&mut registry, &clock, DEFAULT_SHARD);

    for fixing_time_ns in [Some(FIXED_AT), None, Some(FIXED_AT), None] {
        assert_eq!(
            registry.state_strike(handle, &at_or_above_fixed_at("92700", fixing_time_ns)),
            Ok(())
        );
        assert!(
            changes(&mut registry, DEFAULT_SHARD).is_empty(),
            "the same bounds were sent ahead of the cycle"
        );
    }
    assert_eq!(registry.counts().strikes_changed, 0);

    // The time is kept from when it was first stated, and rides the cycle.
    let seen = lap(&mut registry, &clock, DEFAULT_SHARD);
    let strike = strike_of(&seen, id).unwrap();
    assert_eq!(
        (strike.lower_bound, strike.fixing_time_ns),
        (92_700, FIXED_AT)
    );
}

/// A later time for the same bounds does not replace the one first stated, so
/// two paths that disagree about it do not rewrite the strike each poll.
#[test]
fn the_fixing_time_first_stated_is_the_one_kept() {
    let (mut registry, clock) = one_shard(4);
    let handle = registry.list(&spec("WINDOW")).unwrap();
    registry.strike(handle, &at_or_above_fixed_at("92700", Some(111)));
    let id = id_of(&registry, handle);
    lap(&mut registry, &clock, DEFAULT_SHARD);

    registry.strike(handle, &at_or_above_fixed_at("92700", Some(222)));

    assert!(changes(&mut registry, DEFAULT_SHARD).is_empty());
    let seen = lap(&mut registry, &clock, DEFAULT_SHARD);
    assert_eq!(strike_of(&seen, id).unwrap().fixing_time_ns, 111);
}

/// A correction is a change of the bounds, and it brings its own fixing time.
#[test]
fn a_corrected_strike_takes_the_fixing_time_stated_with_it() {
    let (mut registry, clock) = one_shard(4);
    let handle = registry.list(&spec("WINDOW")).unwrap();
    registry.strike(handle, &at_or_above_fixed_at("92700", Some(111)));
    lap(&mut registry, &clock, DEFAULT_SHARD);

    registry.strike(handle, &at_or_above_fixed_at("92800", None));

    let owed = changes(&mut registry, DEFAULT_SHARD);
    assert_eq!(owed.len(), 1);
    let strike = owed[0].strike.unwrap();
    assert_eq!((strike.lower_bound, strike.fixing_time_ns), (92_800, 0));
    assert_eq!(registry.counts().strikes_changed, 1);
}
