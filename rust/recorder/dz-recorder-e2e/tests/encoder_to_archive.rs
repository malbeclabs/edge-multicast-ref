//! Correct traffic, from the publisher's encoder to the archive and back.
//!
//! Every datagram here is produced by the real [`DatagramBuilder`] and kept by
//! the real [`ArchiveWriter`]. Nothing in the loop is a fixture of the other
//! side, which is the whole reason this crate exists: until now a disagreement
//! between the encoder and the archive had nothing that could fail a build.
//!
//! [`DatagramBuilder`]: dz_edge_core::DatagramBuilder
//! [`ArchiveWriter`]: dz_recorder_archive::ArchiveWriter
#![forbid(unsafe_code)]

mod common;

use common::{
    encode, fresh, port_of, record, replay, Msg, RawHeader, Recorded, Wire, ALL_ROLES, GROUP,
    JOIN_INTERFACE, JOIN_SOURCE, PUBLISHER_A, PUBLISHER_B,
};
use dz_edge_core::{
    ChannelSequence, Datagram, Feed, PortRole, ResetCount, DATAGRAM_HEADER_SIZE, FLAG_SNAPSHOT,
    SCHEMA_VERSION,
};
use dz_edge_tob::TopOfBook;
use dz_recorder_core::RecvTsKind;
use dz_recorder_replay::OwnedDatagram;

/// The `Channel ID` each instance in the correct stream stamps.
const MKTDATA_CHANNEL: u8 = 7;
const REFDATA_CHANNEL: u8 = 9;
const SNAPSHOT_CHANNEL: u8 = 3;

/// The recorder lost this many datagrams before one of `PUBLISHER_B`'s, so the
/// round trip has a non-zero `epb_dropcount` to carry.
const LOST_BEFORE: u32 = 3;

/// The stream the encoder emits: several datagrams per channel instance, more
/// than one message in each, two port roles, a sequence that advances the way a
/// publisher's does, and an era that begins again.
///
/// `mktdata` carries three instances' worth of traffic because that is where the
/// interesting shapes are: an era change on one instance, and a second publisher
/// serving the same `Channel ID` to the same group and port, which is a separate
/// channel instance with its own sequence space and must not be folded into the
/// first.
fn correct_stream() -> Vec<OwnedDatagram> {
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
    // A reset restarts the sequence space and bumps `Reset Count`, so what
    // follows is a second sequence space on the same instance and not backward
    // motion in the first.
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

    // Resumed high, so that a tracker folding the two publishers together would
    // read a 97-datagram gap and this test would say so.
    let mut b = ChannelSequence::resume(MKTDATA_CHANNEL, ResetCount::NEVER_RESET, 100);
    for (index, msgs) in [
        &[Msg::Quote(3), Msg::Trade(3)][..],
        &[Msg::Quote(3), Msg::Heartbeat][..],
        &[Msg::Trade(3), Msg::Quote(4)][..],
    ]
    .into_iter()
    .enumerate()
    {
        let payload = encode(b, PortRole::Mktdata, msgs);
        if index == 1 {
            wire.arrive_after_loss(payload, PUBLISHER_B, PortRole::Mktdata, LOST_BEFORE);
        } else {
            wire.arrive(payload, PUBLISHER_B, PortRole::Mktdata);
        }
        b.advance();
    }

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

fn recorded() -> (Vec<OwnedDatagram>, Recorded) {
    let sent = correct_stream();
    let archive = record(&sent, ALL_ROLES);
    (sent, archive)
}

#[test]
fn what_the_encoder_emitted_is_what_the_archive_replays_field_for_field() {
    let (sent, archive) = recorded();
    let replayed = replay(&archive.object);

    assert_eq!(
        replayed.len(),
        sent.len(),
        "the archive replayed {} of the {} datagrams the encoder emitted",
        replayed.len(),
        sent.len()
    );
    for (index, (out, back)) in sent.iter().zip(&replayed).enumerate() {
        assert_eq!(
            out.payload, back.payload,
            "payload bytes of datagram {index}"
        );
        assert_eq!(out.src, back.src, "source address of datagram {index}");
        assert_eq!(out.dst, back.dst, "group and port of datagram {index}");
        assert_eq!(out.role, back.role, "port role of datagram {index}");
        assert_eq!(
            out.recv_ts_ns, back.recv_ts_ns,
            "receive stamp of datagram {index}, to the nanosecond"
        );
        assert_eq!(
            out.recv_ts_kind, back.recv_ts_kind,
            "stamp kind of datagram {index}"
        );
        assert_eq!(
            out.drop_delta, back.drop_delta,
            "the recorder's own loss before datagram {index}"
        );
        assert_eq!(out.ttl, back.ttl, "TTL of datagram {index}");
        assert_eq!(
            out.wire_payload_len, back.wire_payload_len,
            "on-wire length of datagram {index}"
        );
        // Whole values as well as fields, so a field added to `OwnedDatagram`
        // is compared without anyone having to remember to add it above.
        assert_eq!(out, back, "datagram {index} as a whole value");
    }
    assert_eq!(sent, replayed);

    // Socket mode observed no headers, so the 42 bytes in front of each payload
    // were assembled and are not evidence about the wire. Replay must say so,
    // or a reconstruction becomes a claim.
    assert!(
        replayed.iter().all(|dg| dg.link_headers.is_none()),
        "a synthesised header came back as though it had been captured"
    );
    assert!(
        replayed
            .iter()
            .all(|dg| dg.recv_ts_kind == RecvTsKind::KernelSoftware),
        "the section's stamp kind did not survive the round trip"
    );
}

#[test]
fn every_replayed_datagram_still_decodes_as_the_feed_the_encoder_wrote() {
    let (sent, archive) = recorded();
    let replayed = replay(&archive.object);

    for (index, (out, back)) in sent.iter().zip(&replayed).enumerate() {
        let emitted = Datagram::decode(&out.payload, TopOfBook::MAGIC)
            .unwrap_or_else(|e| panic!("the encoder emitted datagram {index} malformed: {e}"));
        let archived = Datagram::decode(&back.payload, TopOfBook::MAGIC).unwrap_or_else(|e| {
            panic!("datagram {index} does not decode after the round trip: {e}")
        });

        assert_eq!(
            archived.header(),
            emitted.header(),
            "header of datagram {index}"
        );
        assert_eq!(
            archived.header().schema_version,
            SCHEMA_VERSION,
            "the encoder stamps the one generation it speaks"
        );
        assert_eq!(
            archived.header().datagram_len as usize,
            back.payload.len(),
            "the declared length of datagram {index} is the length archived"
        );
        assert!(
            archived.header().msg_count > 1,
            "datagram {index} carries more than one message"
        );

        let emitted_msgs: Vec<(u8, u16)> =
            emitted.messages().map(|m| (m.type_id, m.flags)).collect();
        let archived_msgs: Vec<(u8, u16)> =
            archived.messages().map(|m| (m.type_id, m.flags)).collect();
        assert_eq!(
            archived_msgs, emitted_msgs,
            "the messages inside datagram {index}"
        );
        assert_eq!(
            archived_msgs.len(),
            archived.header().msg_count as usize,
            "the walk of datagram {index} finds what its header declares"
        );
        // The builder owns the snapshot bit: set on the snapshot port role and
        // cleared everywhere else, and nothing in this stream is on that role.
        assert!(
            archived_msgs
                .iter()
                .all(|(_, flags)| flags & FLAG_SNAPSHOT == 0),
            "datagram {index} is not on the snapshot port role and must not claim the flag"
        );
    }
}

#[test]
fn the_manifest_covers_each_channel_instance_exactly_as_the_encoder_sequenced_it() {
    let (sent, archive) = recorded();

    let publisher_a = archive.expect_coverage(PUBLISHER_A, MKTDATA_CHANNEL, PortRole::Mktdata);
    assert_eq!(publisher_a.first_seq, 0, "the first sequence that arrived");
    // In arrival order, not in value order: the last datagram on this instance
    // is the second of the new era.
    assert_eq!(publisher_a.last_seq, 1, "the last sequence that arrived");
    assert_eq!(publisher_a.count, 5);
    assert_eq!(publisher_a.reset_counts_seen, vec![0, 1]);

    let publisher_b = archive.expect_coverage(PUBLISHER_B, MKTDATA_CHANNEL, PortRole::Mktdata);
    assert_eq!(
        (
            publisher_b.first_seq,
            publisher_b.last_seq,
            publisher_b.count
        ),
        (100, 102, 3),
        "the second publisher's sequence space is its own"
    );
    assert_eq!(publisher_b.reset_counts_seen, vec![0]);

    let refdata = archive.expect_coverage(PUBLISHER_A, REFDATA_CHANNEL, PortRole::Refdata);
    assert_eq!(
        (refdata.first_seq, refdata.last_seq, refdata.count),
        (0, 1, 2)
    );

    assert_eq!(
        archive.manifest.instances.len(),
        3,
        "three channel instances, and the two publishers on one channel are not one of them: {:?}",
        archive.manifest.instances.keys().collect::<Vec<_>>()
    );

    assert_eq!(archive.manifest.datagram_count, sent.len() as u64);
    assert_eq!(
        archive.manifest.payload_byte_count,
        sent.iter().map(|dg| dg.payload.len() as u64).sum::<u64>()
    );
    assert_eq!(archive.manifest.short_datagrams, 0);
    assert_eq!(archive.manifest.instances_dropped, 0);
    assert_eq!(
        archive.manifest.capture_drop_total,
        u64::from(LOST_BEFORE),
        "our own loss, which is what a gap in this archive is subtracted against"
    );
    assert_eq!(archive.manifest.link_headers, "synthesised");
    assert_eq!(archive.manifest.link_header_exceptions, 0);
    assert_eq!(archive.manifest.capture_drop_scope, "port-role");
    assert_eq!(archive.manifest.feed, "top-of-book");
    assert_eq!(archive.segment.datagram_count, sent.len() as u64);
}

#[test]
fn an_era_that_begins_again_is_a_second_sequence_space_and_not_backward_motion() {
    let (sent, archive) = recorded();

    // Read at the offsets the spec's field table states, so the claim does not
    // rest on the same code the manifest was built from.
    let eras: Vec<(u8, u64)> = sent
        .iter()
        .filter(|dg| dg.role == PortRole::Mktdata && *dg.src.ip() == PUBLISHER_A)
        .map(|dg| {
            (
                dg.payload[21],
                u64::from_le_bytes(dg.payload[4..12].try_into().expect("eight bytes")),
            )
        })
        .collect();
    assert_eq!(
        eras,
        vec![(0, 0), (0, 1), (0, 2), (1, 0), (1, 1)],
        "`begin_era` bumps `Reset Count` and restarts the sequence at 0"
    );

    let coverage = archive.expect_coverage(PUBLISHER_A, MKTDATA_CHANNEL, PortRole::Mktdata);
    assert_eq!(
        coverage.reset_counts_seen,
        vec![0, 1],
        "a segment that spans a reset holds two sequence spaces and says so"
    );
    let peak = eras
        .iter()
        .map(|(_, seq)| *seq)
        .max()
        .expect("a sequence arrived");
    assert!(
        coverage.last_seq < peak,
        "the last sequence to arrive ({}) is below the peak ({peak}), which is a finding unless \
         the reset is recorded — and it is what makes `reset_counts_seen` load-bearing",
        coverage.last_seq
    );
}

#[test]
fn a_port_role_joined_and_silent_is_stated_in_the_manifest_rather_than_absent() {
    let (_, archive) = recorded();

    let joined: Vec<(&str, u16)> = archive
        .manifest
        .roles_joined
        .iter()
        .map(|row| (row.role.as_str(), row.port))
        .collect();
    assert_eq!(
        joined,
        vec![
            ("mktdata", port_of(PortRole::Mktdata)),
            ("refdata", port_of(PortRole::Refdata)),
            ("snapshot", port_of(PortRole::Snapshot)),
        ],
        "the tokens the specification requires, in the order that fixes interface_id"
    );
    for row in &archive.manifest.roles_joined {
        assert_eq!(row.group, GROUP);
        assert_eq!(row.interface.as_deref(), Some(JOIN_INTERFACE));
        assert_eq!(row.source, Some(JOIN_SOURCE));
    }

    // The intent is stated and the port produced nothing, which is precisely
    // the pair a reader needs: a port that was never joined produces no data,
    // and no data looks exactly like a clean feed.
    assert!(
        archive
            .manifest
            .instances
            .keys()
            .all(|instance| instance.dst_port != port_of(PortRole::Snapshot)),
        "nothing was sent on the snapshot port role"
    );
}

#[test]
fn the_snapshot_port_role_round_trips_although_no_message_type_can_be_encoded_for_it() {
    // The one datagram in this suite the encoder could not supply. No message
    // type in `dz-edge-core`, `dz-edge-tob` or `dz-edge-refdata` lists
    // `PortRole::Snapshot`, because the snapshot port role belongs to the depth
    // feeds, so `DatagramBuilder::push` refuses every message currently defined
    // on it and `finish` returns `None` for the empty datagram that leaves. The
    // body here is therefore assembled at the spec's offsets: a 16-byte message
    // with the snapshot flag set, which is what a depth feed's publisher will
    // emit. It is a gap in the encoder, not in the recorder.
    let mut body = vec![0u8; 16];
    body[0] = 0x01;
    body[1] = 16;
    body[2..4].copy_from_slice(&FLAG_SNAPSHOT.to_le_bytes());

    let mut wire = Wire::new();
    for sequence in 0..3 {
        let header = RawHeader::conformant(SNAPSHOT_CHANNEL, sequence, body.len());
        wire.arrive(header.followed_by(&body), PUBLISHER_A, PortRole::Snapshot);
    }
    let sent = wire.sent;
    let archive = record(&sent, ALL_ROLES);
    let replayed = replay(&archive.object);

    assert_eq!(
        replayed, sent,
        "the snapshot port role's bytes and metadata"
    );
    assert!(
        replayed.iter().all(|dg| dg.role == PortRole::Snapshot),
        "the port role came back as something else: {:?}",
        replayed.iter().map(|dg| dg.role).collect::<Vec<_>>()
    );

    let coverage = archive.expect_coverage(PUBLISHER_A, SNAPSHOT_CHANNEL, PortRole::Snapshot);
    assert_eq!(
        (coverage.first_seq, coverage.last_seq, coverage.count),
        (0, 2, 3)
    );
    assert_eq!(
        archive.manifest.instances.len(),
        1,
        "one instance, keyed on the snapshot port"
    );

    // A conformant subscriber still reads it, snapshot flag and all: the
    // structure is the family's, and only the message type is a depth feed's.
    let decoded = Datagram::decode(&replayed[0].payload, TopOfBook::MAGIC)
        .expect("the datagram is structurally conformant");
    assert_eq!(
        decoded.header().datagram_len as usize,
        DATAGRAM_HEADER_SIZE + 16
    );
    assert_eq!(
        decoded.messages().map(|m| m.flags).collect::<Vec<_>>(),
        vec![FLAG_SNAPSHOT],
        "the snapshot bit is set on the snapshot port role"
    );
}
