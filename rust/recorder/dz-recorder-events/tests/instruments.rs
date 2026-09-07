//! Reference data placed in the sequence space, and ended by an era.
//!
//! No archive here, and none needed: the accumulator's whole job is to answer
//! *what was in force at this sequence number*, and that is a question about
//! statements and positions rather than about bytes.

use std::net::Ipv4Addr;

use dz_edge_refdata::{InstrumentDefinition, ManifestSummary, LEG_LEN, SYMBOL_LEN};
use dz_recorder_core::ChannelInstance;
use dz_recorder_events::{At, Channel, InstrumentTable, Observed};

const CHANNEL: u8 = 1;
const PORT: u16 = 31_000;
const SOURCE_ID: u16 = 1_000;
const AAA: u32 = 11;

fn instance() -> ChannelInstance {
    ChannelInstance::new(Ipv4Addr::new(198, 51, 100, 1), CHANNEL, PORT)
}

/// The same publisher's `refdata` port: a different channel instance, the same
/// channel. Definitions arrive here and prices arrive on `instance()`, which is
/// why the table is keyed on the channel and resolved by arrival time.
fn refdata_instance() -> ChannelInstance {
    ChannelInstance::new(Ipv4Addr::new(198, 51, 100, 1), CHANNEL, PORT + 1)
}

fn channel() -> Channel {
    Channel::of(instance())
}

fn other_channel() -> Channel {
    Channel::of(other_path())
}

/// The receive stamp `at(n)` produces, so a test can ask "in force then?".
const fn ts(sequence_number: u64) -> u64 {
    1_700_000_000_000_000_000 + sequence_number
}

/// A second path serving the same channel — a different instance, and nothing
/// it says belongs to the first one's table.
fn other_path() -> ChannelInstance {
    ChannelInstance::new(Ipv4Addr::new(198, 51, 100, 2), CHANNEL, PORT)
}

fn at(sequence_number: u64, reset_count: u8) -> At {
    At {
        instance: instance(),
        sequence_number,
        reset_count,
        recv_ts_ns: 1_700_000_000_000_000_000 + sequence_number,
    }
}

fn symbol(text: &str) -> [u8; SYMBOL_LEN] {
    let mut out = [0_u8; SYMBOL_LEN];
    out[..text.len()].copy_from_slice(text.as_bytes());
    out
}

fn definition(instrument_id: u32, name: &str, price_exponent: i8) -> InstrumentDefinition {
    InstrumentDefinition {
        instrument_id,
        source_id: SOURCE_ID,
        symbol: symbol(name),
        leg1: [0_u8; LEG_LEN],
        leg2: [0_u8; LEG_LEN],
        asset_class: 0,
        price_exponent,
        qty_exponent: 0,
        market_model: 0,
        tick_size: 1,
        lot_size: 1,
        contract_value: 100,
        expiry_ns: 0,
        settle_type: 0,
        price_bound: 0,
        manifest_seq: 3,
    }
}

#[test]
fn a_restatement_applies_from_its_own_sequence_number() {
    let mut table = InstrumentTable::new();
    assert_eq!(
        table.observe_definition(&definition(AAA, "AAA", -2), at(100, 0)),
        Observed::First
    );
    assert_eq!(
        table.observe_definition(&definition(AAA, "AAA", -4), at(500, 0)),
        Observed::Restated
    );

    // The case the re-lowering's accumulator deliberately cannot answer: it
    // holds two archives with no key ordering them, so it pins the first
    // statement and says so. This one has the position, so the prices either
    // side of the restatement decode at different scales — which is what
    // actually happened on the wire.
    assert_eq!(
        table
            .resolve(channel(), AAA, ts(499))
            .unwrap()
            .price_exponent,
        -2
    );
    assert_eq!(
        table
            .resolve(channel(), AAA, ts(500))
            .unwrap()
            .price_exponent,
        -4
    );
    assert_eq!(
        table
            .resolve(channel(), AAA, ts(100_000))
            .unwrap()
            .price_exponent,
        -4
    );
}

#[test]
fn a_price_before_the_first_definition_resolves_to_nothing() {
    let mut table = InstrumentTable::new();
    table.observe_definition(&definition(AAA, "AAA", -2), at(100, 0));

    // The exponent that decodes it was not on the wire yet. Inventing one
    // produces a number rather than an answer.
    assert!(table.resolve(channel(), AAA, ts(99)).is_none());
}

#[test]
fn the_definition_cycle_coming_round_again_is_not_a_change() {
    let mut table = InstrumentTable::new();
    table.observe_definition(&definition(AAA, "AAA", -2), at(100, 0));

    // The runtime republishes every instrument forever. A deriver writing a row
    // per definition observed would be recording the publisher's pacing, not the
    // venue's changes.
    for sequence in [200, 300, 400] {
        assert_eq!(
            table.observe_definition(&definition(AAA, "AAA", -2), at(sequence, 0)),
            Observed::Repeated
        );
    }
}

#[test]
fn a_statement_positioned_behind_the_one_in_force_is_refused() {
    let mut table = InstrumentTable::new();
    table.observe_definition(&definition(AAA, "AAA", -2), at(500, 0));

    assert_eq!(
        table.observe_definition(&definition(AAA, "AAA", -4), at(400, 0)),
        Observed::OutOfOrder
    );
    // Refused, not inserted: accepting it would put the statements out of the
    // order every lookup depends on.
    assert_eq!(
        table
            .resolve(channel(), AAA, ts(100_000))
            .unwrap()
            .price_exponent,
        -2
    );
}

#[test]
fn an_era_ends_a_table_rather_than_extending_it() {
    let mut table = InstrumentTable::new();
    table.observe_definition(&definition(AAA, "AAA", -2), at(9_000, 0));

    // A `Reset Count` restarts the sequence space, so statements from the old
    // era would be ordered against numbers from a different one.
    assert_eq!(
        table.observe_definition(&definition(AAA, "AAA", -4), at(10, 1)),
        Observed::First
    );
    assert_eq!(table.era(channel()), Some(1));
    assert_eq!(
        table
            .resolve(channel(), AAA, ts(20))
            .unwrap()
            .price_exponent,
        -4
    );
    // And the old era's statement is gone rather than shadowing the new one at a
    // sequence number the new era has not reached.
    assert_eq!(table.defined_count(channel()), 1);
}

#[test]
fn a_symbol_reused_across_eras_is_two_instruments() {
    let mut table = InstrumentTable::new();
    table.observe_definition(&definition(AAA, "AAA", -2), at(100, 0));
    // Same human-readable name, a new `Instrument ID`, a new era. A table keyed
    // by symbol merges these two; keyed by `Instrument ID` it cannot.
    table.observe_definition(&definition(99, "AAA", -2), at(100, 1));

    assert!(table.resolve(channel(), AAA, ts(100)).is_none());
    assert_eq!(
        table.resolve(channel(), 99, ts(100)).unwrap().instrument_id,
        99
    );
}

#[test]
fn two_paths_serving_one_channel_keep_separate_tables() {
    let mut table = InstrumentTable::new();
    table.observe_definition(&definition(AAA, "AAA", -2), at(100, 0));
    table.observe_definition(
        &definition(AAA, "AAA", -6),
        At {
            instance: other_path(),
            sequence_number: 100,
            reset_count: 0,
            recv_ts_ns: 1_700_000_000_000_000_100,
        },
    );

    // Each instance advances its own sequence space and opens its own eras, so
    // one path's restatement is not the other's.
    assert_eq!(
        table
            .resolve(channel(), AAA, ts(100))
            .unwrap()
            .price_exponent,
        -2
    );
    assert_eq!(
        table
            .resolve(other_channel(), AAA, ts(100))
            .unwrap()
            .price_exponent,
        -6
    );
}

#[test]
fn a_manifest_that_is_not_valid_yet_declares_nothing() {
    let mut table = InstrumentTable::new();
    let summary = ManifestSummary {
        channel_id: CHANNEL,
        valid: 0,
        manifest_seq: 3,
        instrument_count: 40,
        timestamp_ns: 1,
    };
    table.observe_manifest(&summary, at(100, 0));

    // Absent, not zero. A zero would read as a feed publishing nothing.
    assert_eq!(table.declared_count(channel()), None);

    table.observe_manifest(
        &ManifestSummary {
            valid: 1,
            ..summary
        },
        at(200, 0),
    );
    assert_eq!(table.declared_count(channel()), Some(40));
}

#[test]
fn the_declared_count_is_what_coverage_is_measured_against() {
    let mut table = InstrumentTable::new();
    table.observe_manifest(
        &ManifestSummary {
            channel_id: CHANNEL,
            valid: 1,
            manifest_seq: 3,
            instrument_count: 3,
            timestamp_ns: 1,
        },
        at(100, 0),
    );
    table.observe_definition(&definition(AAA, "AAA", -2), at(101, 0));
    table.observe_definition(&definition(12, "BBB", -2), at(102, 0));

    // Two of the three the publisher said it had. That subtraction is the only
    // statement of published-set coverage an archive can make.
    assert_eq!(table.declared_count(channel()), Some(3));
    assert_eq!(table.defined_count(channel()), 2);
}

/// Definitions arrive on one port role and prices on another, and the table has
/// to serve both.
///
/// This is the case that made the sequence-number key untenable: `refdata` and
/// `mktdata` are two channel instances with two sequence spaces, so a statement
/// at refdata sequence 40 orders against a price at mktdata sequence 40 not at
/// all. One archive is written in arrival order by one host, so the receive
/// clock is what both can be read against.
#[test]
fn a_definition_on_refdata_resolves_for_a_price_on_mktdata() {
    let mut table = InstrumentTable::new();
    table.observe_definition(
        &definition(AAA, "AAA", -2),
        At {
            instance: refdata_instance(),
            // A sequence number from the refdata space, deliberately far from
            // anything mktdata would carry.
            sequence_number: 40,
            reset_count: 0,
            recv_ts_ns: ts(1_000),
        },
    );

    // A price that arrived later, on the other port role, at a sequence number
    // that means nothing in the refdata space.
    assert_eq!(
        table
            .resolve(channel(), AAA, ts(2_000))
            .unwrap()
            .price_exponent,
        -2
    );
    // And one that arrived before the definition still resolves to nothing.
    assert!(table.resolve(channel(), AAA, ts(500)).is_none());
}

/// The port is not in the key, but the address is.
#[test]
fn the_channel_is_the_key_and_the_port_is_not() {
    assert_eq!(channel(), Channel::of(refdata_instance()));
    assert_ne!(channel(), other_channel());
}
