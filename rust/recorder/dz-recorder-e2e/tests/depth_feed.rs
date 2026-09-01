//! A depth feed across all three port roles, from the encoder to the archive
//! and back.
//!
//! The suite beside this one covers top-of-book, which uses two ports. This one
//! exists for the third. Until `dz-edge-mbp` landed, **no message type in these
//! crates declared `PortRole::Snapshot`** — the role was wired through
//! configuration, bindings, the archive's three interface blocks and the
//! manifest, and no test had ever put a datagram through it that a real encoder
//! produced. It was plausible that it worked. It was not demonstrated.
//!
//! What is proved here: a snapshot arrives as the shape a snapshot has (begin,
//! its levels, end, tied by one snapshot id), the record path carries all three
//! roles of one feed without touching any of them, and the manifest attributes
//! each channel instance to the role it arrived on.
#![forbid(unsafe_code)]

mod common;

use common::{
    port_of, record, replay, Recorded, Wire, ALL_ROLES, JOIN_INTERFACE, JOIN_SOURCE, PUBLISHER_A,
    PUBLISHER_MTU,
};
use dz_edge_core::{
    AppMessage, ChannelSequence, Datagram, Feed, PortRole, ResetCount, DATAGRAM_HEADER_SIZE,
    SCHEMA_VERSION,
};
use dz_edge_mbp::{
    BookClear, LevelUpdate, MarketByPrice, SnapshotBegin, SnapshotEnd, SnapshotLevel,
    ACTION_CHANGE, ACTION_DELETE, CLEAR_BID, SCOPE_ENTIRE_SIDE, SIDE_ASK, SIDE_BID,
};
use dz_edge_refdata::{InstrumentDefinition, ManifestSummary};
use dz_recorder_replay::OwnedDatagram;

const MKTDATA_CHANNEL: u8 = 11;
const REFDATA_CHANNEL: u8 = 12;
const SNAPSHOT_CHANNEL: u8 = 13;
const INSTRUMENT: u32 = 4_242;
const SNAPSHOT_ID: u32 = 77;
const ANCHOR_SEQ: u64 = 918_273_645;

fn fresh(channel_id: u8) -> ChannelSequence {
    ChannelSequence::new(channel_id, ResetCount::NEVER_RESET)
}

/// One datagram of this feed, framed by the real builder for `role`.
fn datagram(sequence: ChannelSequence, role: PortRole, push: impl FnOnce(&mut Builder)) -> Vec<u8> {
    let mut builder = Builder::new(sequence, role, PUBLISHER_MTU);
    push(&mut builder);
    builder
        .finish(sequence.sequence_number() * 1_000 + 1_772_000_000_000_000_000)
        .expect("a datagram with at least one message is emittable")
}

type Builder = dz_edge_core::DatagramBuilder<MarketByPrice>;

fn level_update(seq: u32, price_raw: i64, qty_raw: u64, side: u8) -> LevelUpdate {
    LevelUpdate {
        instrument_id: INSTRUMENT,
        source_id: 2,
        side,
        // The specification pairs these in both directions: a zero quantity is
        // a removal and must say so, and a Delete must carry nothing else. The
        // conformance tool caught this fixture writing Unknown on a deletion —
        // self-consistent, so every round-trip test here passed it.
        action: if qty_raw == 0 {
            ACTION_DELETE
        } else {
            ACTION_CHANGE
        },
        per_instrument_seq: seq,
        price_raw,
        qty_raw,
        timestamp_ns: 1_772_000_000_000_000_000 + u64::from(seq),
        order_count: 3,
        level_index: 0,
        update_reason: 0,
        level_flags: 0,
    }
}

fn snapshot_level(price_raw: i64, qty_raw: u64, side: u8) -> SnapshotLevel {
    SnapshotLevel {
        snapshot_id: SNAPSHOT_ID,
        price_raw,
        qty_raw,
        order_count: 1,
        side,
        level_flags: 0,
    }
}

/// The three ports of one depth feed, as a publisher drives them: live level
/// changes on `mktdata`, the instrument's definition on `refdata`, and one
/// instrument's book state on `snapshot`.
fn depth_stream() -> Vec<OwnedDatagram> {
    let mut wire = Wire::new();

    let mut mktdata = fresh(MKTDATA_CHANNEL);
    for (price, qty, side) in [
        (10_000_500_i64, 7_250_u64, SIDE_ASK),
        (9_999_500, 12_500, SIDE_BID),
        // Zero is a deletion, and it has to survive the round trip as one.
        (9_999_500, 0, SIDE_BID),
    ] {
        let seq = u32::try_from(mktdata.sequence_number()).expect("a small test sequence");
        let payload = datagram(mktdata, PortRole::Mktdata, |b| {
            b.push(&level_update(seq, price, qty, side))
                .expect("mktdata carries a level update");
        });
        wire.arrive(payload, PUBLISHER_A, PortRole::Mktdata);
        mktdata.advance();
    }
    // A bulk removal beside the individual ones, in a datagram of two messages.
    let seq = u32::try_from(mktdata.sequence_number()).expect("a small test sequence");
    let payload = datagram(mktdata, PortRole::Mktdata, |b| {
        b.push(&BookClear {
            instrument_id: INSTRUMENT,
            source_id: 2,
            clear_side: CLEAR_BID,
            scope: SCOPE_ENTIRE_SIDE,
            per_instrument_seq: seq,
            from_price_raw: 0,
            timestamp_ns: 1_772_000_000_000_000_100,
            clear_reason: 1,
        })
        .expect("mktdata carries a book clear");
        b.push(&level_update(seq + 1, 10_001_000, 500, SIDE_ASK))
            .expect("and another level update behind it");
    });
    wire.arrive(payload, PUBLISHER_A, PortRole::Mktdata);

    let mut refdata = fresh(REFDATA_CHANNEL);
    let payload = datagram(refdata, PortRole::Refdata, |b| {
        b.push(&instrument_definition())
            .expect("refdata carries an instrument definition");
        b.push(&ManifestSummary {
            channel_id: 0,
            valid: 1,
            manifest_seq: 9,
            instrument_count: 1,
            timestamp_ns: 1_772_000_000_000_000_200,
        })
        .expect("and the manifest summary that closes it");
    });
    wire.arrive(payload, PUBLISHER_A, PortRole::Refdata);
    refdata.advance();

    // The shape a snapshot has: one begin, its levels, one end. Split across
    // two datagrams so the record path carries a snapshot that does not fit in
    // one, which is the ordinary case for a real book.
    let mut snapshot = fresh(SNAPSHOT_CHANNEL);
    let payload = datagram(snapshot, PortRole::Snapshot, |b| {
        b.push(&SnapshotBegin {
            instrument_id: INSTRUMENT,
            anchor_seq: ANCHOR_SEQ,
            total_levels: 2,
            snapshot_id: SNAPSHOT_ID,
            last_instrument_seq: 4,
            timestamp_ns: 1_772_000_000_000_000_300,
            depth_bound: 50,
        })
        .expect("snapshot carries a begin");
        b.push(&snapshot_level(9_999_500, 12_500, SIDE_BID))
            .expect("and a level");
    });
    wire.arrive(payload, PUBLISHER_A, PortRole::Snapshot);
    snapshot.advance();

    let payload = datagram(snapshot, PortRole::Snapshot, |b| {
        b.push(&snapshot_level(10_000_500, 7_250, SIDE_ASK))
            .expect("the second level");
        b.push(&SnapshotEnd {
            instrument_id: INSTRUMENT,
            anchor_seq: ANCHOR_SEQ,
            snapshot_id: SNAPSHOT_ID,
        })
        .expect("and the end that closes it");
    });
    wire.arrive(payload, PUBLISHER_A, PortRole::Snapshot);

    wire.sent
}

fn instrument_definition() -> InstrumentDefinition {
    let mut symbol = [0u8; 64];
    symbol[..8].copy_from_slice(b"BTC-USDT");
    let mut leg1 = [0u8; 8];
    leg1[..3].copy_from_slice(b"BTC");
    let mut leg2 = [0u8; 8];
    leg2[..4].copy_from_slice(b"USDT");
    InstrumentDefinition {
        instrument_id: INSTRUMENT,
        source_id: 2,
        symbol,
        leg1,
        leg2,
        asset_class: 1,
        price_exponent: -2,
        qty_exponent: -8,
        market_model: 1,
        tick_size: 1,
        lot_size: 1_000,
        contract_value: 0,
        expiry_ns: 0,
        settle_type: 0,
        price_bound: 0,
        manifest_seq: 9,
    }
}

fn recorded() -> (Vec<OwnedDatagram>, Recorded) {
    let sent = depth_stream();
    let archive = record(&sent, ALL_ROLES);
    (sent, archive)
}

#[test]
fn every_depth_datagram_comes_back_byte_for_byte() {
    // The recorder's whole promise, on a feed whose messages only mean
    // something in sequence: what the encoder emitted is what the archive
    // holds, in the order it arrived, across all three ports.
    let (sent, archive) = recorded();
    let replayed = replay(&archive.object);

    assert_eq!(replayed.len(), sent.len());
    for (back, out) in replayed.iter().zip(&sent) {
        assert_eq!(back.payload, out.payload, "a datagram changed on the way");
        assert_eq!(back.role, out.role, "and it must come back on its own role");
        assert_eq!(back.dst.port(), port_of(out.role));
        assert_eq!(back.recv_ts_ns, out.recv_ts_ns);
    }
}

#[test]
fn the_snapshot_comes_back_as_the_shape_a_snapshot_has() {
    // Decoded from the replayed bytes rather than from the fixtures: this is
    // the analysis tier's own path, and the first time a snapshot-port datagram
    // has been through the record path at all.
    let (_, archive) = recorded();
    let replayed = replay(&archive.object);

    let mut begins = 0;
    let mut levels = Vec::new();
    let mut ends = 0;
    for dg in replayed.iter().filter(|dg| dg.role == PortRole::Snapshot) {
        let decoded = Datagram::decode(&dg.payload, MarketByPrice::MAGIC).expect("a datagram");
        for msg in decoded.messages() {
            match msg.type_id {
                SnapshotBegin::TYPE_ID => {
                    let begin = SnapshotBegin::decode(msg.bytes).expect("a begin");
                    assert_eq!(begin.snapshot_id, SNAPSHOT_ID);
                    assert_eq!(begin.anchor_seq, ANCHOR_SEQ);
                    begins += 1;
                }
                SnapshotLevel::TYPE_ID => {
                    let level = SnapshotLevel::decode(msg.bytes).expect("a level");
                    assert_eq!(
                        level.snapshot_id, SNAPSHOT_ID,
                        "levels carry their snapshot"
                    );
                    levels.push(level);
                }
                SnapshotEnd::TYPE_ID => {
                    let end = SnapshotEnd::decode(msg.bytes).expect("an end");
                    assert_eq!(end.snapshot_id, SNAPSHOT_ID);
                    assert_eq!(end.anchor_seq, ANCHOR_SEQ, "the end repeats the anchor");
                    ends += 1;
                }
                other => panic!("a snapshot-port datagram carried type {other:#04x}"),
            }
        }
    }

    assert_eq!((begins, ends), (1, 1), "one snapshot, opened and closed");
    assert_eq!(levels.len(), 2, "and as many levels as the begin promised");
    assert!(levels.iter().any(|l| l.side == SIDE_BID));
    assert!(levels.iter().any(|l| l.side == SIDE_ASK));
}

#[test]
fn a_deletion_survives_the_round_trip_as_a_deletion() {
    // The one value in this feed whose meaning is not its magnitude. An archive
    // that lost it would leave the level in every book rebuilt from these bytes.
    let (_, archive) = recorded();
    let deletions = replay(&archive.object)
        .iter()
        .filter(|dg| dg.role == PortRole::Mktdata)
        .flat_map(|dg| {
            let decoded = Datagram::decode(&dg.payload, MarketByPrice::MAGIC).expect("a datagram");
            decoded
                .messages()
                .filter(|m| m.type_id == LevelUpdate::TYPE_ID)
                .map(|m| LevelUpdate::decode(m.bytes).expect("a level update"))
                .collect::<Vec<_>>()
        })
        .filter(|u| u.qty_raw == 0)
        .count();
    assert_eq!(deletions, 1);
}

#[test]
fn each_channel_instance_is_described_under_the_role_it_arrived_on() {
    // Three roles of one feed, three instances, and no folding between them:
    // the manifest is what an index table reads, and a role attributed wrongly
    // there sends a query looking down the wrong port.
    let (_, archive) = recorded();

    let mktdata = archive.expect_coverage(PUBLISHER_A, MKTDATA_CHANNEL, PortRole::Mktdata);
    assert_eq!(
        mktdata.count, 4,
        "three level updates and the clear datagram"
    );
    assert_eq!((mktdata.first_seq, mktdata.last_seq), (0, 3));

    let refdata = archive.expect_coverage(PUBLISHER_A, REFDATA_CHANNEL, PortRole::Refdata);
    assert_eq!(refdata.count, 1);

    let snapshot = archive.expect_coverage(PUBLISHER_A, SNAPSHOT_CHANNEL, PortRole::Snapshot);
    assert_eq!(snapshot.count, 2, "the snapshot spans two datagrams");
    assert_eq!((snapshot.first_seq, snapshot.last_seq), (0, 1));

    assert_eq!(
        archive.manifest.instances.len(),
        3,
        "one per channel instance, and never one per port"
    );
}

#[test]
fn the_archive_declares_this_feeds_own_magic_and_schema() {
    // A datagram of this feed archived under a sibling's magic would decode at
    // the wrong layout for anyone reading the object later.
    let (_, archive) = recorded();
    for dg in replay(&archive.object) {
        assert_eq!(
            u16::from_le_bytes([dg.payload[0], dg.payload[1]]),
            MarketByPrice::MAGIC
        );
        assert_eq!(dg.payload[2], SCHEMA_VERSION);
        assert!(dg.payload.len() >= DATAGRAM_HEADER_SIZE);
    }
    assert_eq!(archive.manifest.roles_joined.len(), ALL_ROLES.len());
    for join in &archive.manifest.roles_joined {
        assert_eq!(join.interface.as_deref(), Some(JOIN_INTERFACE));
        assert_eq!(join.source, Some(JOIN_SOURCE));
    }
}

#[cfg(feature = "conformance")]
#[test]
fn what_the_publisher_wrote_and_the_recorder_kept_is_valid_by_the_spec() {
    // Every other test here compares the chain against itself, which cannot
    // catch the two halves agreeing on something the specification forbids. The
    // tool is the third party: 88 rules this repository has never encoded,
    // applied to the bytes that came back out of the archive.
    let (_, archive) = recorded();
    common::conformance::conformance_of(&archive, "mbp").assert_clean();
}

#[cfg(feature = "conformance")]
#[test]
fn the_conformance_gate_fails_a_stream_the_spec_forbids() {
    // The negative control, and the reason the test above is worth anything: an
    // exit code of 0 has to mean *validated and clean* rather than *saw
    // nothing*. A tool pointed at the wrong ports, or one that could not parse
    // the pcap at all, would find no violations either — and would report a
    // clean feed for a stream nobody checked.
    //
    // The violation is the one the specification warns about by name: a removal
    // carrying an Action other than Delete. It is self-consistent, so every
    // other test in this file passes it.
    let mut wire = Wire::new();
    let mut mktdata = fresh(MKTDATA_CHANNEL);
    for (price, qty) in [(10_000_500_i64, 7_250_u64), (10_000_500, 0)] {
        let seq = u32::try_from(mktdata.sequence_number()).expect("a small test sequence");
        let mut update = level_update(seq, price, qty, SIDE_ASK);
        update.action = ACTION_CHANGE; // a Change carrying zero
        let payload = datagram(mktdata, PortRole::Mktdata, |b| {
            b.push(&update).expect("the encoder does not judge Action");
        });
        wire.arrive(payload, PUBLISHER_A, PortRole::Mktdata);
        mktdata.advance();
    }

    let archive = record(&wire.sent, ALL_ROLES);
    let verdict = common::conformance::conformance_of(&archive, "mbp");
    assert_eq!(
        verdict.code, 1,
        "the rule set passed a stream it defines as a violation, so it is \
         validating nothing:\n{}",
        verdict.stderr
    );
    assert!(
        verdict.stderr.contains("MBP.DELTA.ABSOLUTE_APPLY"),
        "and it has to be the rule this stream breaks:\n{}",
        verdict.stderr
    );
}

#[cfg(feature = "conformance")]
#[test]
fn the_conformance_gate_fails_a_snapshot_the_spec_forbids() {
    // The control above exercises `mktdata` alone, so it cannot rule out the
    // vacuity it exists for on the other two ports: a `-snapshot-port` that is
    // set but wrong evaluates zero snapshot frames and still exits 0, because
    // the tool warns about a starved rule only when the flag is unset. This one
    // proves the snapshot port is reaching the rule set.
    //
    // The violation is structural and loss cannot explain it: the Begin is
    // present in the same stream, and a level claims to belong to a different
    // snapshot than the one it opened.
    let mut wire = Wire::new();
    let snapshot = fresh(SNAPSHOT_CHANNEL);
    let payload = datagram(snapshot, PortRole::Snapshot, |b| {
        b.push(&SnapshotBegin {
            instrument_id: INSTRUMENT,
            anchor_seq: ANCHOR_SEQ,
            total_levels: 1,
            snapshot_id: SNAPSHOT_ID,
            last_instrument_seq: 4,
            timestamp_ns: 1_772_000_000_000_000_300,
            depth_bound: 50,
        })
        .expect("the encoder does not judge a group");
        let mut level = snapshot_level(9_999_500, 12_500, SIDE_BID);
        level.snapshot_id = SNAPSHOT_ID + 1; // belongs to no open group
        b.push(&level).expect("nor does it judge a level's id");
        b.push(&SnapshotEnd {
            instrument_id: INSTRUMENT,
            anchor_seq: ANCHOR_SEQ,
            snapshot_id: SNAPSHOT_ID,
        })
        .expect("and the end closes it");
    });
    wire.arrive(payload, PUBLISHER_A, PortRole::Snapshot);

    let archive = record(&wire.sent, ALL_ROLES);
    let verdict = common::conformance::conformance_of(&archive, "mbp");
    assert_eq!(
        verdict.code, 1,
        "the snapshot port reached no rule, so a clean exit there means nothing:\n{}",
        verdict.stderr
    );
    assert!(
        verdict.stderr.contains("MBP.SNAP.GROUP_STRUCTURE"),
        "and it has to be the rule this snapshot breaks:\n{}",
        verdict.stderr
    );
}
