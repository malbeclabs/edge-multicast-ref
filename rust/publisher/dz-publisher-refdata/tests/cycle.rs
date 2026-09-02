//! The definition cycle's pacing, and the manifest that describes what it is
//! emitting.
//!
//! `reference-data/spec.md` rule 2: publishers MUST NOT emit the entire
//! published set as a single burst. One existing publisher does — its own
//! comment calls the emission a synchronized burst — and this file is the
//! difference. Every timing below is stated to an injected clock and asserted
//! on the definitions handed back; nothing here sleeps, so nothing here is
//! occasionally wrong.

use std::time::Duration;

use dz_adapter_core::{
    AssetClass, InstrumentSpec, ListingSink, MarketModel, PriceBound, Scalar, SettleType,
};
use dz_edge_core::AppMessage;
use dz_edge_refdata::{InstrumentDefinition, ManifestSummary};
use dz_publisher_lowering::SourceId;
use dz_publisher_refdata::{
    definitions_per_datagram, CycleSchedule, ManualClock, MemoryStore, Registry, RegistryConfig,
    SelectionPolicy, LAP_PERCENT,
};

/// The mandated datagram size. Not derived here: every feed specification
/// states 1,232 bytes to leave room for the GRE headers the last mile adds.
const MTU: u16 = 1232;

/// The `definition_cycle` a feed is configured with.
const CYCLE: Duration = Duration::from_secs(30);

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

/// A registry holding `published` instruments, with a cycle capped at
/// `datagrams_per_tick`, and the clock it is reading.
fn seeded(
    published: usize,
    datagrams_per_tick: usize,
) -> (Registry<MemoryStore, ManualClock>, ManualClock) {
    let clock = ManualClock::new();
    let config = RegistryConfig {
        source_id: SourceId::new(7).expect("7 is an assigned production id"),
        channel_id: 3,
        selection: SelectionPolicy::from_seed(published.max(1)).expect("a seed"),
        schedule: CycleSchedule::new(CYCLE, MTU, datagrams_per_tick),
    };
    let mut registry = Registry::open(config, MemoryStore::new(), clock.clone())
        .expect("an empty directory is a cold start");
    for index in 0..published {
        registry
            .list(&spec(&format!("SYM{index}")))
            .expect("within the seed");
    }
    registry.seeding_complete();
    assert_eq!(registry.published(), published);
    (registry, clock)
}

#[test]
fn a_datagram_holds_nine_definitions_and_the_cap_is_derived_from_that() {
    // 1,232 bytes, less the 24-byte datagram header, is 1,208 for messages. An
    // `InstrumentDefinition` is 130 bytes including its own 4-byte message
    // header, and 9 x 130 = 1,170 fits where 10 x 130 = 1,300 does not.
    assert_eq!(InstrumentDefinition::SIZE, 130);
    assert_eq!(definitions_per_datagram(MTU), 9);

    // Configuration cannot raise it. A publisher that shipped a 1,448-byte
    // default to production is why the clamp is in the builder and why this is
    // derived from the same clamp rather than from the number an operator set.
    assert_eq!(definitions_per_datagram(1448), 9);
    assert_eq!(definitions_per_datagram(9000), 9);

    let schedule = CycleSchedule::new(CYCLE, MTU, 8);
    assert_eq!(schedule.definitions_per_datagram(), 9);
    assert_eq!(schedule.max_definitions_per_tick(), 72);

    // 80% of 30s is 24s. The period is a maximum on the interval between
    // retransmissions of any single definition, not a lap target, and the fifth
    // of the period that is not used is what absorbs the difference.
    assert_eq!(LAP_PERCENT, 80);
    assert_eq!(schedule.lap_ns(), 24_000_000_000);
}

#[test]
fn a_lap_is_spread_across_the_lap_and_not_emitted_at_the_start_of_it() {
    // 24 instruments and a 24-second lap is one instrument a second, and the
    // per-tick ceiling is 72, so nothing here is capped: what paces it is the
    // debt the clock says is owed. A burst would be 24 definitions in the first
    // tick.
    let (mut registry, clock) = seeded(24, 8);
    let mut out = Vec::new();

    registry.definition_tick(&mut out);
    assert!(
        out.is_empty(),
        "no time has passed, so no definition is owed yet"
    );

    clock.advance(Duration::from_secs(1));
    registry.definition_tick(&mut out);
    assert_eq!(out.len(), 1, "one twenty-fourth of the set, not the set");

    let mut emitted = out.len();
    for _ in 1..24 {
        clock.advance(Duration::from_secs(1));
        registry.definition_tick(&mut out);
        assert_eq!(out.len(), 1);
        emitted += out.len();
    }
    assert_eq!(emitted, 24, "the whole set, one lap, twenty-four seconds");
    assert_eq!(registry.counts().definitions_emitted, 24);
}

#[test]
fn one_lap_covers_every_published_instrument_exactly_once() {
    // What the pacing is for: every definition is retransmitted inside the
    // cycle period. A rotation that skipped an instrument would leave a
    // subscriber that joined after its last emission unable to resolve it.
    let (mut registry, clock) = seeded(24, 8);
    let mut out = Vec::new();
    let mut seen = Vec::new();

    // The first tick is where the lap starts, and it owes nothing: a lap cannot
    // be owed before there is one.
    registry.definition_tick(&mut out);
    assert!(out.is_empty());

    for _ in 0..24 {
        clock.advance(Duration::from_secs(1));
        registry.definition_tick(&mut out);
        seen.extend(out.iter().map(|definition| definition.instrument_id));
    }

    seen.sort_unstable();
    assert_eq!(seen, (1..=24).collect::<Vec<u32>>());
}

#[test]
fn a_stall_is_capped_at_the_tick_ceiling_and_degrades_into_a_denser_lap() {
    // One datagram a tick, so nine definitions. An unstalled tick a second into
    // the lap owes ceil(100 / 24) = 5 of the hundred.
    let (mut paced, clock) = seeded(100, 1);
    let mut out = Vec::new();
    paced.definition_tick(&mut out);
    clock.advance(Duration::from_secs(1));
    paced.definition_tick(&mut out);
    assert_eq!(out.len(), 5);

    // The same set, with the first tick arriving twelve seconds late: half the
    // lap has elapsed, so half the set is owed. It emits its ceiling and not
    // the fifty it owes - and the debt is not forgotten, so the ticks that
    // follow run at the ceiling rather than at the five an unstalled lap would
    // ask for. That is the lap getting denser, which is what a stall is allowed
    // to cost. A burst is not.
    let (mut stalled, clock) = seeded(100, 1);
    stalled.definition_tick(&mut out);
    clock.advance(Duration::from_secs(12));
    stalled.definition_tick(&mut out);
    assert_eq!(out.len(), 9);

    clock.advance(Duration::from_secs(1));
    stalled.definition_tick(&mut out);
    assert_eq!(out.len(), 9);
    assert!(out.len() > 5, "denser than an unstalled lap");
}

#[test]
fn no_tick_can_be_made_to_emit_the_whole_published_set() {
    // The rule, asserted directly. A caller cannot get the burst back by
    // ticking rarely, by ticking often, or by ticking after the period has
    // elapsed entirely - the ceiling is the pacer's and the caller has no
    // access to it.
    let (mut registry, clock) = seeded(100, 1);
    let ceiling = CycleSchedule::new(CYCLE, MTU, 1).max_definitions_per_tick();
    let mut out = Vec::new();

    for advance in [
        Duration::from_millis(1),
        Duration::from_secs(1),
        Duration::from_secs(30),
        Duration::from_secs(600),
    ] {
        clock.advance(advance);
        registry.definition_tick(&mut out);
        assert!(
            out.len() <= ceiling,
            "{} definitions in one tick",
            out.len()
        );
        assert!(out.len() < registry.published());
    }
}

#[test]
fn an_empty_published_set_emits_nothing_and_starts_its_lap_when_it_is_not_empty() {
    // A publisher whose venue has listed nothing is silent rather than looping,
    // and the lap begins when there is something to lap: inheriting a partly
    // elapsed lap would owe most of the set on the first admission, which is
    // the burst again.
    let (mut registry, clock) = seeded(0, 1);
    let mut out = Vec::new();

    clock.advance(Duration::from_secs(30));
    registry.definition_tick(&mut out);
    assert!(out.is_empty());

    registry.list(&spec("AAA")).expect("admitted");
    registry.definition_tick(&mut out);
    assert!(out.is_empty(), "the lap starts here, and owes nothing yet");

    clock.advance(Duration::from_secs(24));
    registry.definition_tick(&mut out);
    assert_eq!(out.len(), 1);
}

#[test]
fn an_emitted_definition_carries_the_manifest_it_belongs_to() {
    // Stamped on the way out rather than at composition. A definition emitted
    // before the last change to the published set would otherwise claim a
    // manifest that no longer exists, and a subscriber reconciling the two
    // would see a definition it cannot place.
    let (mut registry, clock) = seeded(2, 8);
    let mut out = Vec::new();

    registry.definition_tick(&mut out);
    clock.advance(Duration::from_secs(24));
    registry.definition_tick(&mut out);
    assert_eq!(out.len(), 2);
    for definition in &out {
        assert_eq!(definition.manifest_seq, registry.manifest_seq());
    }

    registry.list(&spec("LATE")).expect("within the cap");
    let after = registry.manifest_seq();
    clock.advance(Duration::from_secs(24));
    registry.definition_tick(&mut out);
    assert!(!out.is_empty());
    for definition in &out {
        assert_eq!(definition.manifest_seq, after);
    }
}

#[test]
fn manifest_seq_advances_when_the_published_set_changes_and_not_otherwise() {
    let (mut registry, _clock) = seeded(0, 8);

    // Zero before anything is published, which is the value a subscriber only
    // ever sees paired with `Valid` at 0.
    assert_eq!(registry.manifest_seq(), 0);

    let first = registry.list(&spec("AAA")).expect("admitted");
    assert_eq!(registry.manifest_seq(), 1);
    registry.list(&spec("BBB")).expect("admitted");
    assert_eq!(registry.manifest_seq(), 2);

    // A re-offer of exactly what the venue already offered is not a change. An
    // adapter offers its whole set on every poll, so a manifest that advanced
    // per offer would advance forever and mean nothing.
    registry.list(&spec("AAA")).expect("still admitted");
    registry.list(&spec("BBB")).expect("still admitted");
    assert_eq!(registry.manifest_seq(), 2);

    // A withdrawal is a change.
    registry.delist(first);
    assert_eq!(registry.manifest_seq(), 3);

    // And the cycle itself is not: emitting a definition changes nothing about
    // the set it belongs to.
    let mut out = Vec::new();
    registry.definition_tick(&mut out);
    assert_eq!(registry.manifest_seq(), 3);
}

#[test]
fn the_valid_flag_means_the_published_set_is_established() {
    // The codec's own definition of the field: 1 once the published set is
    // established, 0 while uninitialized or shutting down. Not a health signal
    // - a channel whose instruments are all dormant is silent and valid.
    let clock = ManualClock::new();
    let config = RegistryConfig {
        source_id: SourceId::new(7).expect("7 is an assigned production id"),
        channel_id: 3,
        selection: SelectionPolicy::from_seed(4).expect("4 is a seed"),
        schedule: CycleSchedule::new(CYCLE, MTU, 8),
    };
    let mut registry = Registry::open(config, MemoryStore::new(), clock.clone())
        .expect("an empty directory is a cold start");

    assert!(!registry.is_valid());
    assert_eq!(registry.manifest().valid, 0);

    // Admitting an instrument does not establish the set: the first poll has
    // not returned yet, so what the venue has offered so far is not what it is
    // going to offer.
    registry.list(&spec("AAA")).expect("admitted");
    assert_eq!(registry.manifest().valid, 0);

    registry.seeding_complete();
    assert!(registry.is_valid());

    clock.set_unix_ns(1_700_000_000_000_000_000);
    let manifest: ManifestSummary = registry.manifest();
    assert_eq!(manifest.valid, 1);
    assert_eq!(manifest.manifest_seq, 1);
    assert_eq!(manifest.instrument_count, 1);
    assert_eq!(manifest.channel_id, 3);
    assert_eq!(manifest.timestamp_ns, 1_700_000_000_000_000_000);

    // And 0 again from the start of shutdown, before `EndOfSession` rather than
    // after it: a subscriber joining during the shutdown must not be told the
    // set it is collecting is final.
    registry.begin_shutdown();
    assert!(!registry.is_valid());
    assert_eq!(registry.manifest().valid, 0);
    assert_eq!(
        registry.manifest().instrument_count,
        1,
        "what was published stays published"
    );
}
