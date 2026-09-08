//! The whole chain, in one process: the publisher's encoder, the recorder's
//! archive, and the loader's rows.
//!
//! Every other suite in this crate stops at the archive. This one carries on to
//! the rows a dashboard reads, so that a disagreement between what a publisher
//! encoded and what a panel would show is a test failure rather than a discovery
//! in a dashboard.
//!
//! **The comparison is against the datagrams that were encoded**, not against
//! what the derivation expects: the sequence numbers, the send timestamps and
//! the `Reset Count`s asserted below are read out of the payloads the real
//! `DatagramBuilder` produced, at the offsets the specification's own table
//! states.
//!
//! No socket, no privileges and no server: the rows go into a `FileSink`.
#![forbid(unsafe_code)]

mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::{
    encode, fresh, port_of, record, Msg, Wire, ALL_ROLES, GROUP, PUBLISHER_A, PUBLISHER_B,
};
use dz_edge_core::{ChannelSequence, PortRole, ResetCount};
use dz_recorder_replay::OwnedDatagram;
use dz_recorder_rows::{
    derive_object, DropScope, FileSink, Grain, PortRoleLabel, RowSink, SegmentTrailer, Verdict,
};
/// One instant for every sink call in this file.
///
/// The sinks take the clock as a parameter, so a test states it rather than
/// sleeping: what is under test here is what a sink writes, never when it
/// decides to.
const NOW: u64 = 1_700_000_000_000_000_000;

const MKTDATA_CHANNEL: u8 = 7;
const REFDATA_CHANNEL: u8 = 9;
/// The recorder lost this many before one of `PUBLISHER_B`'s datagrams, so the
/// gap it leaves is one the archive admits.
const LOST_BEFORE: u32 = 3;
/// Sequence numbers `PUBLISHER_A` skips, which nothing admits.
const SKIPPED: u64 = 4;

/// The sequence number, at the offset the specification's table states.
///
/// Read here rather than through anything under test, so an assertion cannot be
/// satisfied by an implementation that agrees with itself.
fn sequence_number(payload: &[u8]) -> u64 {
    u64::from_le_bytes(payload[4..12].try_into().expect("eight bytes"))
}

fn send_timestamp_ns(payload: &[u8]) -> u64 {
    u64::from_le_bytes(payload[12..20].try_into().expect("eight bytes"))
}

fn channel_id(payload: &[u8]) -> u8 {
    payload[3]
}

fn reset_count(payload: &[u8]) -> u8 {
    payload[21]
}

/// A stream with everything the rows have to be able to express: two publishers
/// on one channel and port, an era change, a gap nobody admits, a gap the
/// recorder admits, and a second port role.
fn stream() -> Vec<OwnedDatagram> {
    let mut wire = Wire::new();

    let mut a = fresh(MKTDATA_CHANNEL);
    for msgs in [
        &[Msg::Quote(1), Msg::Trade(1)][..],
        &[Msg::Quote(1), Msg::Quote(2), Msg::Heartbeat][..],
        &[Msg::Trade(2), Msg::Heartbeat][..],
    ] {
        wire.arrive(
            encode(a, PortRole::Mktdata, msgs),
            PUBLISHER_A,
            PortRole::Mktdata,
        );
        a.advance();
    }
    // The publisher skips a run: nothing admits it, so it is the gap the whole
    // tier exists to attribute.
    for _ in 0..SKIPPED {
        a.advance();
    }
    for msgs in [&[Msg::Quote(2), Msg::Heartbeat][..], &[Msg::Trade(1)][..]] {
        wire.arrive(
            encode(a, PortRole::Mktdata, msgs),
            PUBLISHER_A,
            PortRole::Mktdata,
        );
        a.advance();
    }
    // A reset restarts the sequence space, so what follows is a second era and
    // not backward motion in the first.
    a.begin_era();
    for msgs in [
        &[Msg::Quote(1), Msg::Trade(1)][..],
        &[Msg::Heartbeat, Msg::Quote(2)][..],
    ] {
        wire.arrive(
            encode(a, PortRole::Mktdata, msgs),
            PUBLISHER_A,
            PortRole::Mktdata,
        );
        a.advance();
    }

    // A second publisher serving the same `Channel ID` to the same group and
    // port: a separate channel instance with its own space. Resumed high, so a
    // tracker folding the two together would read a 97-datagram gap.
    let mut b = ChannelSequence::resume(MKTDATA_CHANNEL, ResetCount::NEVER_RESET, 100);
    wire.arrive(
        encode(b, PortRole::Mktdata, &[Msg::Quote(3), Msg::Trade(3)]),
        PUBLISHER_B,
        PortRole::Mktdata,
    );
    b.advance();
    // The recorder loses the next `LOST_BEFORE` datagrams: they are encoded and
    // never reach the sink, and their count rides on the next one that does.
    // That is the order `drop_delta` is defined in — what the handle lost
    // *between the previous datagram and this one* — so the admission belongs to
    // the run this datagram closes, and a fixture that put the delta in front of
    // the loss would be testing the opposite of what the archive means.
    for _ in 0..LOST_BEFORE {
        let _lost = encode(b, PortRole::Mktdata, &[Msg::Heartbeat]);
        b.advance();
    }
    wire.arrive_after_loss(
        encode(b, PortRole::Mktdata, &[Msg::Quote(3), Msg::Heartbeat]),
        PUBLISHER_B,
        PortRole::Mktdata,
        LOST_BEFORE,
    );
    b.advance();
    wire.arrive(
        encode(b, PortRole::Mktdata, &[Msg::Trade(3), Msg::Quote(4)]),
        PUBLISHER_B,
        PortRole::Mktdata,
    );
    b.advance();

    let mut r = fresh(REFDATA_CHANNEL);
    for msgs in [
        &[Msg::ManifestSummary(1), Msg::InstrumentDefinition(1)][..],
        &[Msg::InstrumentDefinition(2), Msg::InstrumentDefinition(3)][..],
    ] {
        wire.arrive(
            encode(r, PortRole::Refdata, msgs),
            PUBLISHER_A,
            PortRole::Refdata,
        );
        r.advance();
    }

    wire.sent
}

/// One datagram row per encoded datagram, recovering what the encoder wrote.
#[test]
fn every_encoded_datagram_becomes_one_row_that_recovers_it() {
    let sent = stream();
    let recorded = record(&sent, ALL_ROLES);
    let derived = derive_object(&recorded.object, &recorded.manifest, None)
        .expect("the object the writer just published derives");
    let rows = &derived.rows;

    assert_eq!(rows.datagram.len(), sent.len());
    assert_eq!(derived.short_datagrams, 0);

    for (row, dg) in rows.datagram.iter().zip(&sent) {
        // The header fields, out of the payload the real builder produced.
        assert_eq!(row.sequence_number, sequence_number(&dg.payload));
        assert_eq!(row.send_ts.0, send_timestamp_ns(&dg.payload));
        assert_eq!(row.channel_id, channel_id(&dg.payload));
        assert_eq!(row.reset_count, reset_count(&dg.payload));
        // The arrival, out of what the archive recorded.
        assert_eq!(row.recv_ts.0, dg.recv_ts_ns, "to the nanosecond");
        assert_eq!(row.source_addr, *dg.src.ip());
        assert_eq!(row.group_addr, GROUP);
        assert_eq!(row.dst_port, dg.dst.port());
        assert_eq!(row.port_role, PortRoleLabel::from(dg.role));
        assert_eq!(row.drop_delta, dg.drop_delta);
        assert_eq!(u64::from(row.payload_len), dg.payload.len() as u64);
        assert_eq!(row.wire_payload_len, dg.wire_payload_len);
        // The provenance, out of the manifest.
        assert_eq!(row.feed, recorded.manifest.feed);
        assert_eq!(row.site, recorded.manifest.site);
        assert_eq!(row.object_sha256, recorded.manifest.sha256);
        assert_eq!(row.drop_scope, DropScope::PortRole);
    }

    // The send stamps are the publisher's own and the receive stamps are the
    // recorder's, so the materialised latency column has something to subtract.
    assert!(
        rows.datagram.iter().all(|r| r.send_ts != r.recv_ts),
        "two clocks, and a column that subtracts one from the other"
    );
}

/// The gap the publisher left and the gap the recorder admits, told apart per
/// run — and the scope that stops either from being attributed.
///
/// **This is the case the design says is the easiest to get wrong by accident.**
/// The archive declares its drop scope as `port-role`, and socket mode really
/// does hold one accumulator per role — but this role carries *two* channel
/// instances, so the accumulator is the socket's and no instance's: its delta
/// rides on whichever datagram next gets through, from either publisher.
/// Subtracting one instance's share would exonerate whichever arrived next and
/// charge the other for loss the recorder caused.
///
/// So the rows carry the number and refuse the subtraction: `admitted_recorder`
/// is the delta on the datagram that closed each run, a fact, and
/// `unexplained_count` is absent, which is the archive declining to say whose
/// the loss was. Precision we do not have is worse than scope we declare.
#[test]
fn the_gap_the_publisher_left_and_the_gap_the_recorder_admits_are_told_apart() {
    let sent = stream();
    let recorded = record(&sent, ALL_ROLES);
    let rows = derive_object(&recorded.object, &recorded.manifest, None)
        .expect("the object derives")
        .rows;

    let by_source: BTreeMap<_, Vec<_>> =
        rows.sequence_gap
            .iter()
            .fold(BTreeMap::new(), |mut acc, gap| {
                acc.entry(gap.source_addr).or_default().push(gap);
                acc
            });

    // The publisher's own skip, on the first instance. Nothing rode on the
    // datagram that closed it, so we admit nothing against it.
    let a = by_source
        .get(&PUBLISHER_A)
        .expect("the first publisher skipped a run");
    assert_eq!(a.len(), 1, "{a:?}");
    assert_eq!(a[0].missing_count, SKIPPED);
    assert_eq!(a[0].admitted_recorder, 0, "we lost nothing here");
    assert_eq!(a[0].era_index, 1, "the skip is in the first era");
    assert_eq!(a[0].channel_id, MKTDATA_CHANNEL);
    assert_eq!(a[0].dst_port, port_of(PortRole::Mktdata));
    assert!(
        a[0].before_ts < a[0].after_ts,
        "placement, never the measure"
    );

    // Our own overflow, on the second instance. The delta rode on the datagram
    // that closed this run, and that is the run it is attributed to — per run,
    // because the consuming report sums this column over a window.
    let b = by_source
        .get(&PUBLISHER_B)
        .expect("the recorder lost datagrams on the second instance");
    assert_eq!(b.len(), 1, "{b:?}");
    assert_eq!(b[0].missing_count, u64::from(LOST_BEFORE));
    assert_eq!(b[0].admitted_recorder, u64::from(LOST_BEFORE));

    // And neither may be subtracted, because the role carries two instances.
    for gap in &rows.sequence_gap {
        assert_eq!(
            gap.unexplained_count, None,
            "the accumulator is the socket's and no instance's: {gap:?}"
        );
        assert_ne!(
            gap.verdict,
            Verdict::Recorder,
            "the archive must not claim a loss it cannot attribute: {gap:?}"
        );
        assert_ne!(
            gap.verdict,
            Verdict::Publisher,
            "and never the accusation from one vantage: {gap:?}"
        );
        assert_eq!(gap.seen_elsewhere, None);
        assert_eq!(gap.sent_from_ts, None, "no site here recorded them");
        assert_eq!(gap.admitted_scope, DropScope::PortRole);
        // Two publishers on one channel and port were asked about each other,
        // and neither carried the other's numbers: each advances its own space,
        // resumed a hundred apart precisely so that folding them would show.
        assert_eq!(
            gap.on_redundant_path,
            Some(0),
            "the question was asked and the answer was no: {gap:?}"
        );
    }
}

/// The two publishers are two instances, and the era change is two eras.
///
/// A tracker keyed any coarser reads the alternation between the two publishers
/// as backward motion in one direction, and lets one publisher's heartbeats
/// cover the other's outage in the other.
#[test]
fn two_publishers_on_one_channel_are_two_instances_with_two_sequence_spaces() {
    let sent = stream();
    let recorded = record(&sent, ALL_ROLES);
    let rows = derive_object(&recorded.object, &recorded.manifest, None)
        .expect("the object derives")
        .rows;

    // Three instances: two publishers on `mktdata` and one on `refdata`.
    let instances: BTreeSet<_> = rows
        .datagram
        .iter()
        .map(|r| (r.source_addr, r.channel_id, r.dst_port))
        .collect();
    assert_eq!(instances.len(), 3, "{instances:?}");
    assert_eq!(rows.segment_coverage.len(), 3);

    // Four eras: the first publisher's two, the second publisher's one, and the
    // refdata instance's one. A gap never spans one, and the wire value is a
    // fact on every row and a key nowhere.
    let mktdata_a: Vec<_> = rows
        .era
        .iter()
        .filter(|e| e.source_addr == PUBLISHER_A && e.dst_port == port_of(PortRole::Mktdata))
        .collect();
    assert_eq!(
        mktdata_a.len(),
        2,
        "the reset opened a second era: {mktdata_a:?}"
    );
    assert!(mktdata_a[0].anchor_ts < mktdata_a[1].anchor_ts);
    assert_ne!(mktdata_a[0].reset_count, mktdata_a[1].reset_count);
    assert_eq!(
        mktdata_a[1].anchor_certain, 1,
        "the transition is in this object"
    );
    assert_eq!(mktdata_a[1].continuation, 0);
    assert_eq!(rows.era.len(), 4, "{:?}", rows.era);

    // And the second era's anchor is where the sequence space restarted, which
    // is what the encoder did.
    let restarted = sent
        .iter()
        .filter(|d| *d.src.ip() == PUBLISHER_A && d.role == PortRole::Mktdata)
        .find(|d| reset_count(&d.payload) != ResetCount::NEVER_RESET.get())
        .expect("the stream carries a reset");
    assert_eq!(mktdata_a[1].anchor_seq, sequence_number(&restarted.payload));
    assert_eq!(mktdata_a[1].anchor_ts.0, restarted.recv_ts_ns);
}

/// Coverage comes off the manifest, so every value in it is the recorder's own
/// statement about the segment.
#[test]
fn the_coverage_rows_are_the_manifest_the_recorder_wrote() {
    let sent = stream();
    let recorded = record(&sent, ALL_ROLES);
    let rows = derive_object(&recorded.object, &recorded.manifest, None)
        .expect("the object derives")
        .rows;

    for row in &rows.segment_coverage {
        let coverage = recorded
            .manifest
            .instances
            .get(&dz_recorder_core::ChannelInstance::new(
                row.source_addr,
                row.channel_id,
                row.dst_port,
            ))
            .expect("every coverage row names an instance the manifest describes");
        assert_eq!(row.first_seq, coverage.first_seq);
        assert_eq!(row.last_seq, coverage.last_seq);
        assert_eq!(row.datagram_count, coverage.count);
        assert_eq!(row.reset_counts_seen, coverage.reset_counts_seen);
        assert_eq!(row.start_ts.0, recorded.manifest.start_ns);
        assert_eq!(row.end_ts.0, recorded.manifest.end_ns);
        assert_eq!(row.capture_drop_total, recorded.manifest.capture_drop_total);
    }

    // Three roles were joined and the intent is on every row, so a role that
    // sent nothing reports `na` rather than `pass`.
    let joined: BTreeSet<&str> = rows.segment_coverage[0]
        .roles_joined
        .iter()
        .map(|r| r.0.as_str())
        .collect();
    assert_eq!(
        joined,
        BTreeSet::from(["mktdata", "refdata", "snapshot"]),
        "the archive states what it was asked to join"
    );
    // And the snapshot port really did send nothing.
    assert!(!rows
        .segment_coverage
        .iter()
        .any(|r| r.dst_port == port_of(PortRole::Snapshot)));
}

/// The rows through a real sink, and back out as the JSON a column store would
/// have been sent.
#[test]
fn the_rows_land_in_a_sink_as_one_json_object_per_line() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let sent = stream();
    let recorded = record(&sent, ALL_ROLES);
    let derived =
        derive_object(&recorded.object, &recorded.manifest, None).expect("the object derives");
    let expected: Vec<(Grain, usize)> = Grain::ALL
        .iter()
        .map(|g| (*g, derived.rows.rows(*g)))
        .collect();

    let mut sink = FileSink::create(dir.path()).expect("the directory is writable");
    let written = sink
        .write_batch(derived.rows, NOW)
        .expect("the batch lands")
        .accepted;
    sink.flush(NOW).expect("flush");

    for (grain, count) in expected {
        assert_eq!(written.rows(grain), count as u64, "{grain}");
        if count == 0 {
            continue;
        }
        let text = std::fs::read_to_string(FileSink::path_in(dir.path(), grain))
            .expect("the grain's file was written");
        let lines: Vec<serde_json::Value> = text
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str(l).expect("one JSON object per line"))
            .collect();
        assert_eq!(lines.len(), count, "{grain}");
        // The column a `DateTime64(9)` reads: a bare integer nanosecond count.
        for line in &lines {
            for (column, value) in line.as_object().expect("an object") {
                if column.ends_with("_ts") {
                    assert!(
                        value.is_u64() || value.is_null(),
                        "{grain}.{column} is not a nanosecond count: {value}"
                    );
                }
            }
        }
    }

    // The datagram rows carry every sequence number the encoder produced, in
    // the order they arrived.
    let text = std::fs::read_to_string(FileSink::path_in(dir.path(), Grain::Datagram))
        .expect("the datagram rows were written");
    let sequences: Vec<u64> = text
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).expect("an object")["sequence_number"]
                .as_u64()
                .expect("a sequence number")
        })
        .collect();
    let encoded: Vec<u64> = sent.iter().map(|d| sequence_number(&d.payload)).collect();
    assert_eq!(sequences, encoded);
}

/// A predecessor settles the boundary era, which is what in-order loading buys.
#[test]
fn a_predecessor_settles_the_boundary_era_this_object_could_not() {
    let sent = stream();
    let recorded = record(&sent, ALL_ROLES);

    let alone = derive_object(&recorded.object, &recorded.manifest, None)
        .expect("the object derives")
        .rows;
    assert!(
        alone.era.iter().filter(|e| e.anchor_certain == 0).count() >= 3,
        "with no predecessor, every instance's first era is unsettled: {:?}",
        alone.era
    );

    // The trailer a preceding segment would have left, with each instance's last
    // `Reset Count` in arrival order.
    let mut instances = Vec::new();
    let mut last: BTreeMap<(std::net::Ipv4Addr, u8, u16), u8> = BTreeMap::new();
    for dg in &sent {
        last.insert(
            (*dg.src.ip(), channel_id(&dg.payload), dg.dst.port()),
            reset_count(&dg.payload),
        );
    }
    for ((source_addr, channel_id, dst_port), reset_count) in last {
        instances.push(dz_recorder_rows::InstanceReset {
            source_addr,
            channel_id,
            dst_port,
            reset_count,
        });
    }
    // `segment_seq` restarts at 0 on every recorder run and a writer opened once
    // produces segment 0, so the object is placed inside a run here rather than
    // given a predecessor that cannot exist. Nothing in the object states its
    // own place in the run — which is why the manifest is what states it, and
    // why the digest below is unaffected.
    let mut inside_a_run = recorded.manifest.clone();
    inside_a_run.segment_seq = 5;
    let preceding = SegmentTrailer {
        segment_seq: 4,
        interface_drop_total: 0,
        instances,
    };

    let settled = derive_object(&recorded.object, &inside_a_run, Some(&preceding))
        .expect("the object derives")
        .rows;
    assert!(
        settled.era.iter().all(|e| e.anchor_certain == 1),
        "every boundary is settled once the predecessor is in hand: {:?}",
        settled.era
    );
}
