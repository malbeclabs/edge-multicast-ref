//! Reference data comes from the archive, and says so when the archive does not
//! carry enough.

mod common;

use common::{
    pack, payloads, refdata_datagrams, DatagramLog, Framing, LineAdapter, Listed, Msg, SOURCE_ID,
};
use dz_edge_core::PortRole;
use dz_edge_mbp::{LevelUpdate, MarketByPrice, MAGIC_MBP};
use dz_edge_refdata::ManifestSummary;
use dz_recorder_relower::{compare_archives, Caveat, Finding, RelowerError, WireCapture};

/// Prices at two decimal places, quantities at none.
const AAA: Listed = Listed::new("AAA", 11, -2, 0);
/// A second instrument at entirely different exponents, so a table that held one
/// pair for the whole feed would fail here.
const BBB: Listed = Listed::new("BBB", 12, -6, -3);

const ABSENT_U16: u16 = 0xFFFF;
const SIDE_BID: u8 = 0;
const ACTION_NEW: u8 = 1;

#[test]
fn the_reconstructed_table_matches_what_the_definitions_said() {
    let mut archive = DatagramLog::new(refdata_datagrams::<MarketByPrice>(&[AAA, BBB], 7));
    let mut capture = WireCapture::new();
    capture
        .absorb(&mut archive, MAGIC_MBP)
        .expect("the archive is complete");

    let refdata = capture.refdata();
    assert_eq!(refdata.len(), 2);

    let first = refdata.by_symbol("AAA").expect("AAA was defined");
    assert_eq!(first.instrument_id, 11);
    assert_eq!(first.price_exponent, -2);
    assert_eq!(first.qty_exponent, 0);
    assert_eq!(first.source_id, SOURCE_ID);
    assert_eq!(first.manifest_seq, 7);
    assert_eq!(first.symbol_text(), "AAA");

    let second = refdata.by_symbol("BBB").expect("BBB was defined");
    assert_eq!(second.instrument_id, 12);
    assert_eq!(second.price_exponent, -6);
    assert_eq!(second.qty_exponent, -3);

    // And what the lowering will be handed: the archive's id and exponents, and
    // no contract factor, because the wire does not carry one.
    let instrument = second.as_instrument();
    assert_eq!(instrument.instrument_id, 12);
    assert_eq!(instrument.price_exponent, -6);
    assert_eq!(instrument.qty_exponent, -3);
    assert_eq!(instrument.quoted_per_contract, None);

    // The manifest is read, not just the definitions.
    assert_eq!(refdata.declared_instrument_count(), Some(2));
    assert!(
        refdata.caveats().is_empty(),
        "a complete set owes no caveats: {:?}",
        refdata.caveats()
    );
    // Every reference-data message was consumed and none of them joined.
    assert_eq!(capture.skipped().reference_data, 3);
    assert!(capture.messages().is_empty());
}

#[test]
fn the_exponents_the_archive_states_are_the_ones_the_re_lowering_uses() {
    // The venue's listing states a scale of its own — four places for prices,
    // three for quantities — and the archive says the publisher published two
    // and none. A re-lowering that took the venue's word for it, or a live
    // registry's, would report a field difference on every message; the archive
    // wins, so the window is clean.
    let listing_says = [("AAA", -4i8, -3i8)];
    let mut adapter = LineAdapter::over_stating(&listing_says);

    let wire = vec![Msg::Level(LevelUpdate {
        instrument_id: 11,
        source_id: SOURCE_ID,
        side: SIDE_BID,
        action: ACTION_NEW,
        per_instrument_seq: 1,
        // 99.50 at the archive's exponent of -2, by hand.
        price_raw: 9_950,
        qty_raw: 12,
        timestamp_ns: 1_000_000_003,
        order_count: ABSENT_U16,
        level_index: ABSENT_U16,
        update_reason: 0,
        level_flags: 0,
    })];
    let mut archive = DatagramLog::new(refdata_datagrams::<MarketByPrice>(&[AAA], 1));
    archive.extend(pack::<MarketByPrice>(
        &wire,
        PortRole::Mktdata,
        Framing::tight(),
    ));
    let mut upstream = payloads(&["l AAA 1000000003 bid 99.50 12 new"]);

    let report = compare_archives(&mut adapter, &mut upstream, &mut archive, MAGIC_MBP)
        .expect("both archives are complete");

    assert!(report.is_clean(), "{:?}", report.findings);
    assert_eq!(report.summary.identical, 1);
}

#[test]
fn an_instrument_with_no_definition_in_the_archive_is_reported_rather_than_guessed() {
    // The archive defines one of the two instruments the venue streams. The
    // other is declined at admission rather than admitted under an invented
    // `Instrument ID`: every message it produced would join against nothing and
    // be reported as one the publisher dropped, which is a false accusation
    // drawn from our own missing reference data.
    let mut archive = DatagramLog::new(refdata_datagrams::<MarketByPrice>(&[AAA], 1));
    archive.extend(pack::<MarketByPrice>(
        &[Msg::Level(LevelUpdate {
            instrument_id: 12,
            source_id: SOURCE_ID,
            side: SIDE_BID,
            action: ACTION_NEW,
            per_instrument_seq: 1,
            price_raw: 1,
            qty_raw: 1,
            timestamp_ns: 1_000_000_009,
            order_count: ABSENT_U16,
            level_index: ABSENT_U16,
            update_reason: 0,
            level_flags: 0,
        })],
        PortRole::Mktdata,
        Framing::tight(),
    ));

    let mut adapter = LineAdapter::over(&[AAA, BBB]);
    let mut upstream = payloads(&["l BBB 1000000009 bid 0.000001 0.001 new"]);

    let report = compare_archives(&mut adapter, &mut upstream, &mut archive, MAGIC_MBP)
        .expect("both archives are complete");

    assert_eq!(report.missing_definitions.len(), 1);
    assert_eq!(report.missing_definitions[0].symbol, "BBB");
    // Offered on every poll, and counted rather than repeated.
    assert!(report.missing_definitions[0].offers >= 1);

    // Nothing was lowered for it, so its message on the wire is reported as one
    // the re-lowering did not produce — and the missing definition beside it is
    // what says the absence is ours.
    assert_eq!(report.summary.re_lowered, 0);
    assert_eq!(report.findings.len(), 1);
    assert!(matches!(
        report.findings[0],
        Finding::OnWireNotReLowered { .. }
    ));
}

#[test]
fn a_manifest_declaring_more_instruments_than_the_archive_carries_is_reported() {
    // The manifest is what makes an incomplete refdata window detectable before
    // a single message is compared. Without it, a missing definition is only
    // found when an adapter happens to offer that symbol.
    let mut definitions = refdata_datagrams::<MarketByPrice>(&[AAA], 3);
    // Replace the manifest with one declaring a set of five.
    definitions.pop();
    definitions.extend(pack::<MarketByPrice>(
        &[Msg::Manifest(ManifestSummary {
            channel_id: 1,
            valid: 1,
            manifest_seq: 3,
            instrument_count: 5,
            timestamp_ns: 1_700_000_000_000_000_000,
        })],
        PortRole::Refdata,
        Framing::tight(),
    ));

    let mut archive = DatagramLog::new(definitions);
    let mut capture = WireCapture::new();
    capture
        .absorb(&mut archive, MAGIC_MBP)
        .expect("the archive is complete");

    assert_eq!(
        capture.refdata().caveats(),
        [Caveat::ReferenceDataIncomplete {
            manifest_seq: 3,
            declared: 5,
            reconstructed: 1,
        }]
    );
    // And it renders as something an operator can act on.
    assert!(capture.refdata().caveats()[0]
        .to_string()
        .contains("declares 5 instruments"));
}

#[test]
fn a_manifest_that_is_not_valid_yet_declares_nothing() {
    // `Valid = 0` is what a publisher sends while its set is not yet
    // established and while it is shutting down, and the count beside it
    // describes neither state.
    let mut archive = DatagramLog::new(pack::<MarketByPrice>(
        &[
            Msg::Definition(AAA.definition(1)),
            Msg::Manifest(ManifestSummary {
                channel_id: 1,
                valid: 0,
                manifest_seq: 9,
                instrument_count: 400,
                timestamp_ns: 1_700_000_000_000_000_000,
            }),
        ],
        PortRole::Refdata,
        Framing::tight(),
    ));
    let mut capture = WireCapture::new();
    capture
        .absorb(&mut archive, MAGIC_MBP)
        .expect("the archive is complete");

    assert_eq!(capture.refdata().declared_instrument_count(), None);
    assert!(capture.refdata().caveats().is_empty());
}

#[test]
fn a_restated_exponent_keeps_the_first_statement_and_says_so() {
    // The publisher republished one symbol at a different scale inside the
    // window. There is no key that orders the multicast archive against the
    // payload archive, so the instant it took effect cannot be placed in the
    // payload stream: the first statement is used for the whole window and the
    // caveat is what tells a reader that the messages after the restatement are
    // compared at the wrong exponent.
    let restated = Listed {
        price_exponent: -4,
        ..AAA
    };
    let mut archive = DatagramLog::new(pack::<MarketByPrice>(
        &[
            Msg::Definition(AAA.definition(1)),
            Msg::Definition(restated.definition(2)),
        ],
        PortRole::Refdata,
        Framing::tight(),
    ));
    let mut capture = WireCapture::new();
    capture
        .absorb(&mut archive, MAGIC_MBP)
        .expect("the archive is complete");

    assert_eq!(
        capture.refdata().by_symbol("AAA").map(|i| i.price_exponent),
        Some(-2)
    );
    assert!(capture
        .refdata()
        .caveats()
        .contains(&Caveat::ScaleRestated {
            instrument_id: 11,
            kept: (-2, 0),
            later: (-4, 0),
        }));
}

#[test]
fn a_definition_that_declares_a_contract_says_the_factor_is_not_on_the_wire() {
    // `Contract Value` states what one contract is worth, at `Price Exponent`.
    // The lowering's factor states how much of the underlying one contract is,
    // which a price is divided by and a quantity multiplied by. The second is
    // not derivable from the first and is on no wire message, so the
    // re-lowering applies none — and if the publisher applied one, every price
    // and quantity for this instrument will differ. This caveat is what sends
    // the reader to the venue's listing instead of to the publisher's scaling.
    let per_contract = Listed {
        contract_value: 250_000,
        ..AAA
    };
    let mut archive = DatagramLog::new(refdata_datagrams::<MarketByPrice>(&[per_contract], 1));
    let mut capture = WireCapture::new();
    capture
        .absorb(&mut archive, MAGIC_MBP)
        .expect("the archive is complete");

    assert!(capture
        .refdata()
        .caveats()
        .contains(&Caveat::ContractFactorNotOnTheWire { instrument_id: 11 }));
}

#[test]
fn an_archive_with_no_publisher_identity_cannot_be_compared() {
    // The `Source ID` is on the wire, so it is reconstructed like everything
    // else. A window that carries none cannot be re-lowered: every message
    // would differ in `source_id`, and a comparison that reported that on every
    // message would report nothing.
    let mut archive = DatagramLog::new(Vec::new());
    let mut capture = WireCapture::new();
    capture
        .absorb(&mut archive, MAGIC_MBP)
        .expect("an empty archive is complete");

    match capture.source_id() {
        Err(RelowerError::NoSourceIdInArchive { found }) => assert!(found.is_empty()),
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[test]
fn two_publisher_identities_in_one_archive_are_refused() {
    // Two publishers on one channel is a finding for the health tier and not
    // something to pick from: a comparison run against the mixture would
    // attribute one publisher's messages to the other's mapping.
    let mut second = BBB.definition(1);
    second.source_id = 1001;
    let mut archive = DatagramLog::new(pack::<MarketByPrice>(
        &[Msg::Definition(AAA.definition(1)), Msg::Definition(second)],
        PortRole::Refdata,
        Framing::tight(),
    ));
    let mut capture = WireCapture::new();
    capture
        .absorb(&mut archive, MAGIC_MBP)
        .expect("the archive is complete");

    match capture.source_id() {
        Err(RelowerError::AmbiguousSourceId { first, second }) => {
            assert_eq!((first, second), (1000, 1001));
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[test]
fn a_definition_with_no_source_id_falls_back_to_the_messages() {
    // The schema-1 layout has no `Source ID` and decodes as `0`, which the
    // registry does not admit. The identity is still on every `LevelUpdate`, so
    // that is where it comes from.
    let mut definition = AAA.definition(1);
    definition.source_id = 0;
    let mut archive = DatagramLog::new(pack::<MarketByPrice>(
        &[Msg::Definition(definition)],
        PortRole::Refdata,
        Framing::tight(),
    ));
    archive.extend(pack::<MarketByPrice>(
        &[Msg::Level(LevelUpdate {
            instrument_id: 11,
            source_id: SOURCE_ID,
            side: SIDE_BID,
            action: ACTION_NEW,
            per_instrument_seq: 1,
            price_raw: 9_950,
            qty_raw: 12,
            timestamp_ns: 1_000_000_003,
            order_count: ABSENT_U16,
            level_index: ABSENT_U16,
            update_reason: 0,
            level_flags: 0,
        })],
        PortRole::Mktdata,
        Framing::tight(),
    ));

    let mut capture = WireCapture::new();
    capture
        .absorb(&mut archive, MAGIC_MBP)
        .expect("the archive is complete");
    let identity = capture
        .source_id()
        .expect("the messages state one the registry admits");
    assert_eq!(identity.get(), SOURCE_ID);
}
