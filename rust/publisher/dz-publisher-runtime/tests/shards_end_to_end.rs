//! Four channel instances from one process, and what each of them carries.
//!
//! Two shards, both specifications: the arrangement the whole change exists for,
//! and the one the duplicate-specification gate refused until it lifted.
//!
//! # Every assertion here is written as an exclusion
//!
//! *Shard A's reference-data port carries A's definitions* passes against a
//! publisher that packs everything onto everything. So does *A's manifest
//! advanced*. The only form that fails against that publisher is **and none of
//! B's** — which is why every test below asserts what a channel did **not**
//! carry beside what it did, and why the two shards are given different
//! instruments rather than the same ones.
//!
//! # The era is not asserted here, deliberately
//!
//! Each channel instance owning its own `Reset Count` is the property, and it is
//! covered where it can actually be observed: the era store's own suite, which
//! advances two shards independently and shows that adding or removing one moves
//! nobody else's. This harness hands each pipeline a literal era rather than
//! resolving one, so an assertion here would be about the harness. Coverage that
//! looks like coverage and tests a fixture is worse than none.
//!
//! Nothing here needs a socket, a privilege or a venue.
#![forbid(unsafe_code)]

mod harness;

use dz_adapter_core::EventSink as _;
use dz_edge_refdata::InstrumentDefinition;
use harness::{harness_two_shards, quote, FakeAdapter, SHARD_A, SHARD_B};

/// `InstrumentDefinition`, on the reference-data port.
const DEFINITION: u8 = 0x02;

/// The one schema generation this build emits, as `end_to_end.rs` states it: a
/// publisher speaks one, and a mixture would make the version byte meaningless.
const SCHEMA: u8 = 3;

/// The symbol a definition names, with the NUL padding of the fixed field cut.
fn symbol_of(bytes: &[u8]) -> String {
    let definition =
        InstrumentDefinition::decode(bytes, SCHEMA).expect("this publisher composed it");
    String::from_utf8_lossy(&definition.symbol)
        .trim_end_matches('\0')
        .to_owned()
}

/// The distinct symbols a reference-data port published a definition for,
/// sorted.
///
/// Distinct because a full lap of a two-instrument set repeats, and the
/// question here is *which* instruments a channel described, not how often.
fn symbols_on(recorders: &harness::FeedRecorders) -> Vec<String> {
    let mut symbols: Vec<String> = recorders
        .refdata
        .messages()
        .iter()
        .filter(|(type_id, _)| *type_id == DEFINITION)
        .map(|(_, bytes)| symbol_of(bytes))
        .collect();
    symbols.sort();
    symbols.dedup();
    symbols
}

/// An admission on one shard moves that shard's manifest and no other.
///
/// `Manifest Seq` is defined per channel — "increments every time the published
/// instrument set changes **on this channel**" — so a subscriber on a quiet
/// shard must not see its manifest advance for an admission it can never
/// receive a message for. A process-wide sequence would advance all four.
#[test]
fn an_admission_on_one_shard_moves_one_manifest_and_leaves_the_others() {
    let mut h = harness_two_shards();
    let mut adapter = FakeAdapter::on_shards(&[("A-B", SHARD_A)]);
    assert!(h.publisher.poll_listings(&mut adapter));

    let before: Vec<Option<u16>> = [SHARD_A, SHARD_B]
        .iter()
        .map(|shard| h.publisher.refdata().manifest_seq(shard))
        .collect();

    // The poll is due-gated on `LISTING_POLL`, so a second call on the same
    // instant is a no-op that returns `false`. Asserting the gate rather than
    // ignoring it is what stops this test from reading a manifest that never
    // saw the admission and calling the result a per-shard property.
    let mut second = FakeAdapter::on_shards(&[("A-B", SHARD_A), ("C-D", SHARD_A)]);
    h.clock.advance(dz_publisher_runtime::LISTING_POLL);
    assert!(h.publisher.poll_listings(&mut second));

    let after: Vec<Option<u16>> = [SHARD_A, SHARD_B]
        .iter()
        .map(|shard| h.publisher.refdata().manifest_seq(shard))
        .collect();

    assert!(
        after[0] > before[0],
        "the admitting shard's manifest did not advance: {before:?} -> {after:?}"
    );
    assert_eq!(
        after[1], before[1],
        "the other shard's manifest advanced for an admission it cannot see: \
         {before:?} -> {after:?}"
    );
}

/// **The plan's centre.** Each reference-data port carries its own shard's
/// definitions and none of the other's.
///
/// Written as an exclusion on purpose. A publisher that packed the whole
/// published set onto every reference-data port would satisfy *A carries A*, and
/// that is exactly the publisher this change exists to stop being: it would make
/// `Instrument Count` describe the process, and a subscriber would collect
/// definitions for instruments no message will ever arrive for on the channel it
/// is bound to.
#[test]
fn each_reference_data_port_carries_its_own_shards_definitions_and_none_of_the_others() {
    let mut h = harness_two_shards();
    // Two instruments per shard, not one. With a single instrument each, the
    // pacer owes one definition per lap and a publisher that packed everything
    // onto everything would still put exactly one symbol on each port — so the
    // failure would surface as "carries none of its own" and the exclusion
    // below would never be reached. Two per shard is what makes a packing
    // publisher show up as four symbols where two belong.
    let mut adapter = FakeAdapter::on_shards(&[
        ("A-B", SHARD_A),
        ("A-C", SHARD_A),
        ("B-D", SHARD_B),
        ("B-E", SHARD_B),
    ]);
    assert!(h.publisher.poll_listings(&mut adapter));

    // Two ticks with time between them, for the pacer's reason: the first tick
    // with a published set starts the lap and owes nothing, so a single tick
    // would read as "no definitions" for a publisher that is working. The gap
    // is wide enough for a whole lap of a two-instrument set.
    let _ = h.publisher.tick();
    h.clock.advance(std::time::Duration::from_secs(20));
    let _ = h.publisher.tick();

    // Stated as equality, which is the inclusion and the exclusion in one
    // assertion: A's port carries A's two and nothing else, B's carries B's.
    let expected: [Vec<String>; 2] = [
        vec!["A-B".to_owned(), "A-C".to_owned()],
        vec!["B-D".to_owned(), "B-E".to_owned()],
    ];
    let mut ports = 0;
    for (index, shard) in h.shards.iter().enumerate() {
        for recorders in [shard.tob.as_ref(), shard.mbp.as_ref()]
            .into_iter()
            .flatten()
        {
            ports += 1;
            assert_eq!(
                symbols_on(recorders),
                expected[index],
                "shard {index}'s reference-data port does not carry exactly its \
                 own published set"
            );
        }
    }
    assert_eq!(
        ports, 4,
        "four channel instances is the arrangement under test; {ports} ports \
         means the harness changed and every assertion above narrowed with it"
    );
}

/// A quote reaches its own shard's channel and no other.
///
/// The routing half of the same property. Off the wrong shard's pipeline a
/// message leaves by a channel whose sequence series belongs to somebody else,
/// which a subscriber reads as its own.
#[test]
fn a_quote_reaches_its_own_shards_channel_and_no_other() {
    let mut h = harness_two_shards();
    let mut adapter = FakeAdapter::on_shards(&[("A-B", SHARD_A), ("C-D", SHARD_B)]);
    assert!(h.publisher.poll_listings(&mut adapter));
    // The second shard deliberately: a routing bug that always reaches for the
    // first would be invisible against the first.
    let on_b = adapter.handles()[1];

    h.publisher.event(quote(on_b, 1));

    let mut reached: Vec<usize> = Vec::new();
    for (index, shard) in h.shards.iter().enumerate() {
        for recorders in [shard.tob.as_ref(), shard.mbp.as_ref()]
            .into_iter()
            .flatten()
        {
            if recorders.mktdata.type_ids().contains(&0x03) {
                reached.push(index);
            }
        }
    }
    assert_eq!(
        reached,
        vec![1],
        "a quote for an instrument on the second shard reached {reached:?}"
    );
}

/// A manifest names the count of its own shard, not of the process.
#[test]
fn a_manifest_states_its_own_shards_instrument_count() {
    let mut h = harness_two_shards();
    // Unequal sets, or a process-wide count passes.
    let mut adapter =
        FakeAdapter::on_shards(&[("A-B", SHARD_A), ("C-D", SHARD_B), ("E-F", SHARD_B)]);
    assert!(h.publisher.poll_listings(&mut adapter));

    assert_eq!(h.publisher.refdata().published_on(SHARD_A), Some(1));
    assert_eq!(h.publisher.refdata().published_on(SHARD_B), Some(2));
    assert_eq!(
        h.publisher.refdata().published(),
        3,
        "the process-wide count is still the cap's number, and still three"
    );
}

/// A shard name this document has no channel for reaches the runtime, once.
///
/// The refusal itself was already right: the instrument is declined, the count
/// climbs, and nothing is published on a channel nobody chose. What was missing
/// was anybody seeing it. `dz-publisher-refdata` collects the distinct names and
/// writes no line — it constructs no metric and it logs nothing — so those names
/// are a signal only if the runtime drains them, and until this test nothing
/// did.
///
/// **What this asserts, and what it does not.** It asserts the drain: the
/// publisher hands the name up, once, and never again for the same name. It does
/// **not** assert the `eprintln!` in `tick_loop` that writes it, because no test
/// in this workspace runs that function and none captures stderr. That gap is
/// named here rather than papered over with an assertion that would pass either
/// way; the line's own composition is asserted in `run.rs`'s unit tests.
#[test]
fn a_shard_the_document_has_no_channel_for_is_named_once_however_often_it_is_offered() {
    let mut h = harness_two_shards();
    // Two instruments under one misspelling, so a drain reporting per offer
    // rather than per distinct name would hand back two.
    let mut adapter = FakeAdapter::on_shards(&[("A-B", "gamma"), ("C-D", "gamma")]);
    assert!(h.publisher.poll_listings(&mut adapter));

    assert_eq!(
        h.publisher.take_unknown_shards(),
        vec!["gamma".to_owned()],
        "the runtime did not drain the name the venue offered"
    );
    assert_eq!(
        h.publisher.refdata().published(),
        0,
        "an instrument on a shard this publisher has no channel for was published anyway"
    );
    assert_eq!(
        h.publisher.refdata().counts().declined_unknown_shard,
        2,
        "both offers are counted even though one name is reported"
    );

    // The whole set again, which is what the boundary promises an adapter may
    // do. A name already reported must not be reported twice: a venue
    // re-offering every second would otherwise be a line a second.
    h.clock.advance(dz_publisher_runtime::LISTING_POLL);
    let mut again = FakeAdapter::on_shards(&[("A-B", "gamma"), ("C-D", "gamma")]);
    assert!(h.publisher.poll_listings(&mut again));
    assert!(
        h.publisher.take_unknown_shards().is_empty(),
        "a name already reported was reported again"
    );
}
