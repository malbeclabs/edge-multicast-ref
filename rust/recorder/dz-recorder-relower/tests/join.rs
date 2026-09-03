//! The join is a join: a key that appears twice, or not at all, has a stated
//! behaviour rather than a heuristic.

mod common;

use common::{
    pack, payloads, refdata_datagrams, DatagramLog, Framing, LineAdapter, Listed, Msg, SOURCE_ID,
};
use dz_edge_core::PortRole;
use dz_edge_mbp::{LevelUpdate, MarketByPrice, MAGIC_MBP};
use dz_edge_tob::{TopOfBook, Trade, MAGIC_TOB};
use dz_recorder_relower::{
    compare, compare_archives, key_overlap, relower, Caveat, Finding, JoinKey, Outcome,
    TopOfBookTie, WireCapture,
};

const AAA: Listed = Listed::new("AAA", 11, -2, 0);

const ABSENT_U16: u16 = 0xFFFF;
const SIDE_BID: u8 = 0;
const ACTION_NEW: u8 = 1;
const BOTH_UPDATED: u8 = 0x03;
const AGGRESSOR_BUY: u8 = 1;

fn level(seq: u32, price_raw: i64, ts: u64) -> LevelUpdate {
    LevelUpdate {
        instrument_id: AAA.instrument_id,
        source_id: SOURCE_ID,
        side: SIDE_BID,
        action: ACTION_NEW,
        per_instrument_seq: seq,
        price_raw,
        qty_raw: 12,
        timestamp_ns: ts,
        order_count: ABSENT_U16,
        level_index: ABSENT_U16,
        update_reason: 0,
        level_flags: 0,
    }
}

fn depth_archive(messages: &[Msg]) -> DatagramLog {
    let mut archive = DatagramLog::new(refdata_datagrams::<MarketByPrice>(&[AAA], 1));
    archive.extend(pack::<MarketByPrice>(
        messages,
        PortRole::Mktdata,
        Framing::tight(),
    ));
    archive
}

#[test]
fn a_key_absent_from_the_wire_is_one_finding_and_never_a_pairing_with_its_neighbour() {
    // The property that makes this a join. The wire holds seq 1 and seq 3; the
    // re-lowering produces 1, 2 and 3. A comparison that aligned by position
    // would pair the re-lowered 2 with the wire's 3 and report two field
    // differences — a wrong price and a wrong timestamp — instead of one
    // missing message, and every subsequent message would be reported wrong
    // too. Nothing here aligns: the absent key is absent, and its neighbours
    // are untouched.
    let wire = vec![
        Msg::Level(level(1, 9_950, 1_000_000_001)),
        Msg::Level(level(3, 9_970, 1_000_000_003)),
    ];
    let mut archive = depth_archive(&wire);
    let mut adapter = LineAdapter::over(&[AAA]);
    let mut upstream = payloads(&[
        "l AAA 1000000001 bid 99.50 12 new",
        "l AAA 1000000002 bid 99.60 12 new",
        "l AAA 1000000003 bid 99.70 12 new",
    ]);

    let report = compare_archives(&mut adapter, &mut upstream, &mut archive, MAGIC_MBP)
        .expect("both archives are complete");

    assert_eq!(report.findings.len(), 1, "{:?}", report.findings);
    assert_eq!(
        *report.findings[0].key(),
        JoinKey::Depth {
            instrument_id: 11,
            per_instrument_seq: 2,
        }
    );
    assert_eq!(report.findings[0].outcome(), Outcome::ReLoweredNotOnWire);
    // The two that were on the wire joined, and neither is a field difference.
    assert_eq!(report.summary.identical, 2);
    assert_eq!(report.summary.fields_differ, 0);
}

#[test]
fn a_key_the_wire_carries_twice_pairs_one_copy_and_declares_the_ambiguity() {
    // A datagram delivered twice — which a recorder archives as it arrives,
    // because de-duplicating would destroy the evidence that the network
    // duplicates. The join pairs one copy and reports the surplus, and the
    // caveat says the key was not unique. The two possible readings, a network
    // duplicate and a publisher that sent the same message twice, are told
    // apart by the loss tier's own duplicate rows for the same window: this
    // comparison holds no datagram identity and must not guess between them.
    let duplicated = Msg::Level(level(2, 9_960, 1_000_000_002));
    let wire = vec![
        Msg::Level(level(1, 9_950, 1_000_000_001)),
        duplicated,
        duplicated,
    ];
    let mut archive = depth_archive(&wire);
    let mut adapter = LineAdapter::over(&[AAA]);
    let mut upstream = payloads(&[
        "l AAA 1000000001 bid 99.50 12 new",
        "l AAA 1000000002 bid 99.60 12 new",
    ]);

    let report = compare_archives(&mut adapter, &mut upstream, &mut archive, MAGIC_MBP)
        .expect("both archives are complete");

    assert_eq!(report.summary.identical, 2);
    assert_eq!(report.findings.len(), 1, "{:?}", report.findings);
    assert!(matches!(
        report.findings[0],
        Finding::OnWireNotReLowered { .. }
    ));
    assert!(report.caveats.contains(&Caveat::AmbiguousJoinKey {
        key: JoinKey::Depth {
            instrument_id: 11,
            per_instrument_seq: 2,
        },
        on_wire: 2,
        re_lowered: 1,
    }));
}

#[test]
fn a_key_both_sides_carry_twice_is_paired_in_order_and_stays_clean() {
    // Two trades on one instrument at one nanosecond with no venue trade
    // identifier — `Trade ID` is `0` for a venue that publishes none, so both
    // carry the same key. Both sides hold two, they are paired in arrival
    // order, and the window is clean; the caveat is what says the pairing was
    // arbitrary.
    let trade = Trade {
        instrument_id: AAA.instrument_id,
        source_id: SOURCE_ID,
        aggressor_side: AGGRESSOR_BUY,
        trade_flags: 0,
        source_timestamp_ns: 1_000_000_005,
        trade_price: 10_025,
        trade_qty: 2,
        trade_id: 0,
        cumulative_volume: 0,
    };
    let mut archive = DatagramLog::new(refdata_datagrams::<TopOfBook>(&[AAA], 1));
    archive.extend(pack::<TopOfBook>(
        &[Msg::Trade(trade), Msg::Trade(trade)],
        PortRole::Mktdata,
        Framing::batched(2),
    ));
    let mut adapter = LineAdapter::over(&[AAA]);
    let mut upstream = payloads(&[
        "t AAA 1000000005 100.25 2 buy 0",
        "t AAA 1000000005 100.25 2 buy 0",
    ]);

    let report = compare_archives(&mut adapter, &mut upstream, &mut archive, MAGIC_TOB)
        .expect("both archives are complete");

    assert!(report.is_clean(), "{:?}", report.findings);
    assert_eq!(report.summary.identical, 2);
    assert!(report.caveats.contains(&Caveat::AmbiguousJoinKey {
        key: JoinKey::TopOfBook {
            instrument_id: 11,
            source_timestamp_ns: 1_000_000_005,
            tie: TopOfBookTie::TradeId(0),
        },
        on_wire: 2,
        re_lowered: 2,
    }));
}

#[test]
fn a_key_neither_side_carries_is_not_in_the_report_at_all() {
    // "Or not at all": a key absent from both sides is not a key. Asserted
    // through the count of keys the join considered, because the alternative —
    // enumerating a key space and reporting the holes — would report every
    // sequence number an instrument has not reached yet.
    let wire = vec![Msg::Level(level(1, 9_950, 1_000_000_001))];
    let mut archive = depth_archive(&wire);
    let mut adapter = LineAdapter::over(&[AAA]);
    let mut upstream = payloads(&["l AAA 1000000001 bid 99.50 12 new"]);

    let report = compare_archives(&mut adapter, &mut upstream, &mut archive, MAGIC_MBP)
        .expect("both archives are complete");

    assert!(report.is_clean());
    assert_eq!(report.summary.keys, 1);
    assert_eq!(report.summary.identical, 1);
}

#[test]
fn a_quote_and_a_trade_the_venue_stamped_at_one_instant_do_not_join_to_each_other() {
    // The tie in the top-of-book key, end to end. Both messages are for one
    // instrument at one nanosecond; a join on `(Instrument ID, source
    // timestamp)` alone would pair them and report the difference between a
    // quote and a trade as a lowering defect.
    let quote = dz_edge_tob::Quote {
        instrument_id: AAA.instrument_id,
        source_id: SOURCE_ID,
        update_flags: BOTH_UPDATED,
        source_timestamp_ns: 1_000_000_005,
        bid_price: 9_950,
        bid_qty: 12,
        ask_price: 10_050,
        ask_qty: 7,
        bid_source_count: 0,
        ask_source_count: 0,
    };
    let trade = Trade {
        instrument_id: AAA.instrument_id,
        source_id: SOURCE_ID,
        aggressor_side: AGGRESSOR_BUY,
        trade_flags: 0,
        source_timestamp_ns: 1_000_000_005,
        trade_price: 10_025,
        trade_qty: 2,
        trade_id: 7788,
        cumulative_volume: 0,
    };
    let mut archive = DatagramLog::new(refdata_datagrams::<TopOfBook>(&[AAA], 1));
    archive.extend(pack::<TopOfBook>(
        &[Msg::Quote(quote), Msg::Trade(trade)],
        PortRole::Mktdata,
        Framing::batched(2),
    ));
    let mut adapter = LineAdapter::over(&[AAA]);
    let mut upstream = payloads(&[
        "q AAA 1000000005 99.50 12 100.50 7",
        "t AAA 1000000005 100.25 2 buy 7788",
    ]);

    let report = compare_archives(&mut adapter, &mut upstream, &mut archive, MAGIC_TOB)
        .expect("both archives are complete");

    assert!(report.is_clean(), "{:?}", report.findings);
    assert_eq!(report.summary.keys, 2);
    assert_eq!(report.summary.identical, 2);
    // And no ambiguity was declared, because the keys really are distinct.
    assert!(!report
        .caveats
        .iter()
        .any(|caveat| matches!(caveat, Caveat::AmbiguousJoinKey { .. })));
}

#[test]
fn the_two_windows_can_be_read_from_separate_archives_and_their_overlap_reported() {
    // The general path, for a recorder whose port roles are separate archives:
    // one `absorb` per source into one capture, then the re-lowering, then the
    // join. `compare_archives` is the convenience over the ordinary case and
    // not the only way in.
    //
    // `key_overlap` is the number that says whether two archives describe the
    // same window at all: a comparison whose key sets barely intersect is one
    // whose windows do not line up, and its findings are noise.
    let mut refdata = DatagramLog::new(refdata_datagrams::<MarketByPrice>(&[AAA], 1));
    let mut mktdata = DatagramLog::new(pack::<MarketByPrice>(
        &[
            Msg::Level(level(1, 9_950, 1_000_000_001)),
            Msg::Level(level(2, 9_960, 1_000_000_002)),
        ],
        PortRole::Mktdata,
        Framing::batched(2),
    ));

    let mut capture = WireCapture::new();
    capture
        .absorb(&mut refdata, MAGIC_MBP)
        .expect("the reference-data archive is complete");
    capture
        .absorb(&mut mktdata, MAGIC_MBP)
        .expect("the market-data archive is complete");
    let source_id = capture.source_id().expect("the definitions state one");

    let mut adapter = LineAdapter::over(&[AAA]);
    let mut upstream = payloads(&[
        "l AAA 1000000001 bid 99.50 12 new",
        "l AAA 1000000002 bid 99.60 12 new",
        "l AAA 1000000003 bid 99.70 12 new",
    ]);
    let re_lowered = relower(&mut adapter, &mut upstream, capture.refdata(), source_id)
        .expect("the payload archive is complete");

    let (on_wire, produced, shared) = key_overlap(&capture, &re_lowered);
    assert_eq!((on_wire, produced, shared), (2, 3, 2));

    let report = compare(&capture, &re_lowered);
    assert_eq!(report.summary.identical, 2);
    assert_eq!(report.summary.re_lowered_not_on_wire, 1);
}
