//! Where the evidence runs out, and what the tool says instead of guessing.
//!
//! Each of these is a case where a comparison *could* produce a pile of
//! findings and where every one of them would be about the archive rather than
//! about the publisher. A tool that made those accusations would be closed by
//! its first reader.

mod common;

use common::{
    pack, payloads, refdata_datagrams, DatagramLog, FailingPayloads, FailingSource, Framing,
    LineAdapter, Listed, Msg, SOURCE_ID,
};
use dz_edge_core::PortRole;
use dz_edge_mbp::{LevelUpdate, MarketByPrice, MAGIC_MBP};
use dz_recorder_relower::{compare_archives, relower, Caveat, RelowerError, WireCapture};
use dz_recorder_replay::SyntheticPublisher;

const AAA: Listed = Listed::new("AAA", 11, -2, 0);

const ABSENT_U16: u16 = 0xFFFF;
const SIDE_BID: u8 = 0;
const ACTION_NEW: u8 = 1;

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

fn depth_archive(messages: &[Msg], framing: Framing) -> DatagramLog {
    let mut archive = DatagramLog::new(refdata_datagrams::<MarketByPrice>(&[AAA], 1));
    archive.extend(pack::<MarketByPrice>(messages, PortRole::Mktdata, framing));
    archive
}

#[test]
fn datagrams_from_another_stream_are_counted_and_never_decoded() {
    // The archive also holds the synthetic publisher's traffic, whose delimiter
    // is deliberately not any real feed's. `Magic` is the only thing that stops
    // a datagram misrouted from a sibling feed being parsed at the wrong
    // layout, so the foreign datagrams are counted as foreign — not as
    // undecodable, which would say something quite different about the archive,
    // and not as messages.
    let mut archive = depth_archive(
        &[Msg::Level(level(1, 9_950, 1_000_000_001))],
        Framing::tight(),
    );
    let foreign = SyntheticPublisher::clean(4).datagrams();
    let foreign_count = foreign.len() as u64;
    archive.extend(foreign);

    let mut adapter = LineAdapter::over(&[AAA]);
    let mut upstream = payloads(&["l AAA 1000000001 bid 99.50 12 new"]);
    let report = compare_archives(&mut adapter, &mut upstream, &mut archive, MAGIC_MBP)
        .expect("both archives are complete");

    assert!(report.is_clean(), "{:?}", report.findings);
    assert_eq!(report.skipped.foreign_magic, foreign_count);
    assert_eq!(report.skipped.undecodable, 0);
    assert_eq!(report.summary.identical, 1);
}

#[test]
fn a_heartbeat_is_skipped_rather_than_reported_as_an_invented_message() {
    // The publisher's own cadence produced it and no upstream payload did. Its
    // absence from the re-lowering means nothing, so it is counted as control
    // traffic rather than joined.
    let archive_messages = vec![
        Msg::Heartbeat(dz_edge_core::Heartbeat {
            channel_id: common::CHANNEL_ID,
            timestamp_ns: 1_700_000_000_000_000_000,
        }),
        Msg::Level(level(1, 9_950, 1_000_000_001)),
    ];
    let mut archive = depth_archive(&archive_messages, Framing::batched(2));
    let mut adapter = LineAdapter::over(&[AAA]);
    let mut upstream = payloads(&["l AAA 1000000001 bid 99.50 12 new"]);

    let report = compare_archives(&mut adapter, &mut upstream, &mut archive, MAGIC_MBP)
        .expect("both archives are complete");

    assert!(report.is_clean(), "{:?}", report.findings);
    assert_eq!(report.skipped.control, 1);
}

#[test]
fn a_window_whose_depth_numbering_does_not_start_at_one_is_reported() {
    // The single most dangerous thing about the depth join. The re-lowering
    // starts every instrument's series at 1, because that is what an era does;
    // a multicast window that opened after the era began carries a numbering
    // offset from it by however many deltas preceded the window, and *every*
    // key for that instrument is then wrong in both directions. The caveat is
    // worth more than the findings it explains.
    let wire = vec![
        Msg::Level(level(5, 9_950, 1_000_000_001)),
        Msg::Level(level(6, 9_960, 1_000_000_002)),
    ];
    let mut archive = depth_archive(&wire, Framing::tight());
    let mut adapter = LineAdapter::over(&[AAA]);
    let mut upstream = payloads(&[
        "l AAA 1000000001 bid 99.50 12 new",
        "l AAA 1000000002 bid 99.60 12 new",
    ]);

    let report = compare_archives(&mut adapter, &mut upstream, &mut archive, MAGIC_MBP)
        .expect("both archives are complete");

    assert!(report
        .caveats
        .contains(&Caveat::WindowMayNotStartAtEraBoundary {
            instrument_id: 11,
            first_seq: 5,
        }));
    // And the findings are exactly what an offset numbering produces: every
    // message unmatched on both sides. Asserted so that the shape of the
    // failure is on the record next to the caveat that explains it.
    assert_eq!(report.findings.len(), 4);
    assert_eq!(report.summary.identical, 0);
}

#[test]
fn an_era_change_inside_the_window_is_reported() {
    // `Per-Instrument Seq` restarts at 1 with the era, so a window containing a
    // `Reset Count` change contains one key space used twice — and the payload
    // archive carries nothing that says which payload the reset fell between.
    let mut archive = depth_archive(
        &[Msg::Level(level(1, 9_950, 1_000_000_001))],
        Framing::tight(),
    );
    archive.extend(pack::<MarketByPrice>(
        &[Msg::Level(level(1, 9_960, 1_000_000_002))],
        PortRole::Mktdata,
        Framing {
            reset_count: 1,
            first_sequence: 0,
            ..Framing::tight()
        },
    ));

    let mut capture = WireCapture::new();
    capture
        .absorb(&mut archive, MAGIC_MBP)
        .expect("the archive is complete");

    assert!(capture
        .caveats()
        .contains(&Caveat::EraChangeInsideWindow { channel_id: 1 }));
}

#[test]
fn a_torn_multicast_archive_is_an_error_and_not_a_clean_report() {
    // A short read taken for a complete window turns our own truncation into
    // the strongest accusation this tool can make: every message after the tear
    // is one the publisher never sent.
    let mut torn = FailingSource {
        after: 2,
        inner: depth_archive(
            &[
                Msg::Level(level(1, 9_950, 1_000_000_001)),
                Msg::Level(level(2, 9_960, 1_000_000_002)),
            ],
            Framing::tight(),
        ),
    };
    let mut capture = WireCapture::new();
    let error = capture
        .absorb(&mut torn, MAGIC_MBP)
        .expect_err("a torn archive is refused");
    assert!(matches!(error, RelowerError::MulticastArchive(_)));
}

#[test]
fn a_torn_payload_archive_is_an_error_and_not_a_clean_report() {
    // The mirror image, and the mirror accusation: every message after the tear
    // is on the wire and absent from the re-lowering, which reads as a
    // publisher inventing traffic.
    let mut archive = depth_archive(
        &[Msg::Level(level(1, 9_950, 1_000_000_001))],
        Framing::tight(),
    );
    let mut capture = WireCapture::new();
    capture
        .absorb(&mut archive, MAGIC_MBP)
        .expect("the archive is complete");
    let source_id = capture.source_id().expect("the definitions state one");

    let mut torn = FailingPayloads::new(
        1,
        payloads(&[
            "l AAA 1000000001 bid 99.50 12 new",
            "l AAA 1000000002 bid 99.60 12 new",
        ]),
    );
    let mut adapter = LineAdapter::over(&[AAA]);
    let error = relower(&mut adapter, &mut torn, capture.refdata(), source_id)
        .expect_err("a torn payload archive is refused");
    assert!(matches!(error, RelowerError::PayloadArchive(_)));
}

#[test]
fn a_payload_the_adapter_cannot_parse_is_counted_and_is_not_a_finding() {
    // The publisher's own adapter refused the identical bytes, under the
    // identical reason, and produced nothing from them. Neither side holds a
    // message, so there is nothing to join and nothing to report — but the
    // count is in the report, because a window full of them compared very
    // little.
    let mut archive = depth_archive(
        &[Msg::Level(level(1, 9_950, 1_000_000_001))],
        Framing::tight(),
    );
    let mut adapter = LineAdapter::over(&[AAA]);
    let mut upstream = payloads(&[
        "l AAA 1000000001 bid 99.50 12 new",
        "z AAA 1000000002 nonsense",
    ]);

    let report = compare_archives(&mut adapter, &mut upstream, &mut archive, MAGIC_MBP)
        .expect("both archives are complete");

    assert!(report.is_clean(), "{:?}", report.findings);
    assert_eq!(report.parse_failures.len(), 1);
    assert_eq!(report.parse_failures[0].payload_index, 1);
    // The taxonomy's own token, which is the label a live publisher would have
    // counted it under.
    assert_eq!(report.parse_failures[0].reason, "schema");
}

#[test]
fn a_value_the_archived_exponent_cannot_state_is_a_refusal_and_the_report_says_so() {
    // The venue quoted three decimal places for an instrument the publisher
    // published at two. The lowering refuses rather than rounds — which is the
    // whole argument for it — so the re-lowering produces nothing, the wire copy
    // has nothing to join against, and the report carries the refusal beside
    // the finding so that nobody reads it as the publisher inventing a message.
    let wire = vec![Msg::Level(level(1, 9_950, 1_000_000_001))];
    let mut archive = depth_archive(&wire, Framing::tight());
    let mut adapter = LineAdapter::over(&[AAA]);
    let mut upstream = payloads(&["l AAA 1000000001 bid 99.505 12 new"]);

    let report = compare_archives(&mut adapter, &mut upstream, &mut archive, MAGIC_MBP)
        .expect("both archives are complete");

    assert_eq!(report.refusals.len(), 1);
    assert_eq!(report.refusals[0].message_type, "LevelUpdate");
    assert_eq!(report.refusals[0].instrument_id, Some(11));
    assert_eq!(report.refusals[0].reason, "too_precise");
    assert_eq!(report.refusals[0].field, Some("price"));
    assert_eq!(report.findings.len(), 1);
    assert_eq!(report.summary.on_wire_not_re_lowered, 1);
}

#[test]
fn a_snapshot_is_skipped_because_the_archive_cannot_say_when_it_was_asked_for() {
    // The snapshot is pulled by the runtime on its own cadence, from the
    // adapter's own book, and the payload archive records nothing about when it
    // asked. Re-lowering one offline would compare a book state taken at one
    // instant against one taken at another, which is a different comparison
    // altogether.
    let mut archive = depth_archive(
        &[Msg::Level(level(1, 9_950, 1_000_000_001))],
        Framing::tight(),
    );
    archive.extend(pack::<MarketByPrice>(
        &[Msg::SnapshotEnd(dz_edge_mbp::SnapshotEnd {
            instrument_id: 11,
            anchor_seq: 3,
            snapshot_id: 1,
        })],
        PortRole::Snapshot,
        Framing::tight(),
    ));

    let mut adapter = LineAdapter::over(&[AAA]);
    let mut upstream = payloads(&["l AAA 1000000001 bid 99.50 12 new"]);
    let report = compare_archives(&mut adapter, &mut upstream, &mut archive, MAGIC_MBP)
        .expect("both archives are complete");

    assert!(report.is_clean(), "{:?}", report.findings);
    assert_eq!(report.skipped.snapshot, 1);
}
