//! Wrong traffic, and the half that matters.
//!
//! The recorder's central promise is that it records feeds it does not
//! understand, and that a datagram a decoder would refuse is the one most worth
//! keeping: the bug class a parsing recorder creates is the worst one available
//! to a recorder, because the evidence needed to diagnose the bug is what the
//! bug destroyed. So the assertion under every case here is the same one — every
//! byte is in the archive and replays identically — and the second assertion is
//! that a conformant subscriber really would have refused it.
//!
//! `DatagramBuilder` refuses most of these by construction, which is what a
//! builder is for. Those are assembled at the offsets the spec's field table
//! states; each case says why it is a publisher violation rather than a recorder
//! bug. The sequence faults are different: nothing in the builder can see them,
//! so they are emitted by the real encoder from a `ChannelSequence` a publisher
//! has advanced wrongly.
#![forbid(unsafe_code)]

mod common;

use common::{encode, fresh, record, replay, Msg, Recorded, Wire, GROUP, PUBLISHER_A};
use dz_edge_core::{
    AppMessage, ChannelSequence, Datagram, DecodeError, Feed, PortRole, ResetCount,
    DATAGRAM_HEADER_SIZE, MAX_DATAGRAM_SIZE, SCHEMA_VERSION,
};
use dz_edge_tob::{Quote, TopOfBook};
use dz_recorder_replay::OwnedDatagram;

/// Only `mktdata` and `refdata`, so the manifest states no snapshot join it
/// could not have honoured.
const JOINED: &[PortRole] = &[PortRole::Mktdata, PortRole::Refdata];

/// A `Channel ID` per malformed case, so the manifest's coverage row for each is
/// identifiable without opening the object.
const UNKNOWN_SCHEMA: u8 = 11;
const DECLARED_PAST_THE_CAP: u8 = 12;
const DECLARED_ABOVE_THE_BYTES: u8 = 13;
const DECLARED_BELOW_THE_BYTES: u8 = 14;
const FOREIGN_MAGIC: u8 = 15;
const MESSAGE_COUNT_MISMATCH: u8 = 16;
const SHORT_OF_THE_HEADER: u8 = 17;
const NO_MESSAGES: u8 = 18;
const DECLARED_BELOW_THE_HEADER: u8 = 19;

/// And one per sequence fault, for the same reason.
const SEQUENCE_GAP: u8 = 21;
const DUPLICATE: u8 = 22;
const REORDERED: u8 = 23;
const BACKWARD_MOTION: u8 = 24;

/// A schema version this build does not implement. `SUPPORTED_SCHEMA_VERSIONS`
/// is `[3, 1]`, and 9 is in neither.
const UNIMPLEMENTED_SCHEMA_VERSION: u8 = 9;

/// A delimiter that is not this feed's. What matters is only that it differs
/// from `TopOfBook::MAGIC`: magic is what rejects a datagram misrouted from a
/// sibling feed.
const ANOTHER_FEEDS_MAGIC: u16 = 0x445B;

/// One malformed datagram, and what a conformant subscriber does with it.
struct Malformed {
    channel_id: u8,
    payload: Vec<u8>,
    /// The error `Datagram::decode` must return. A subscriber MUST discard the
    /// datagram, which is exactly why the recorder must not.
    verdict: DecodeError,
}

/// One `Quote`, framed by the real encoder, to be spoiled in one field.
///
/// Taking the encoder's own bytes and overwriting a single header field keeps
/// each case a violation of exactly one rule: a body assembled by hand as well
/// would leave two possible reasons for a decoder's refusal.
fn one_quote(channel_id: u8) -> Vec<u8> {
    encode(fresh(channel_id), PortRole::Mktdata, &[Msg::Quote(1)])
}

fn malformed() -> Vec<Malformed> {
    let body_len = Quote::SIZE;
    let whole = DATAGRAM_HEADER_SIZE + body_len;

    // A publisher speaks one generation: there is no reader asking it to
    // downgrade, and `finish` stamps `SCHEMA_VERSION` with no way to ask for
    // another. A version byte no reader implements is therefore the publisher's
    // fault, and every conformant subscriber discards the datagram — so a
    // recorder that also discarded it would leave nothing to diagnose the
    // publisher with.
    let mut unknown_schema = one_quote(UNKNOWN_SCHEMA);
    unknown_schema[2] = UNIMPLEMENTED_SCHEMA_VERSION;

    // The 1232-byte cap is mandated by every feed spec, and `DatagramBuilder`
    // clamps its capacity to it so no configuration key and no operator can
    // raise it. A declared length above the cap can only come from something
    // that is not this builder.
    let mut past_the_cap = one_quote(DECLARED_PAST_THE_CAP);
    past_the_cap[22..24].copy_from_slice(&1300u16.to_le_bytes());

    // The header's declared length describes the datagram it heads — the spec's
    // field table calls it `Frame Length`. Declaring more bytes than were sent
    // puts the last message boundary past the end of what arrived, which is why
    // a subscriber refuses the whole datagram rather than the messages it can
    // reach.
    let mut above_the_bytes = one_quote(DECLARED_ABOVE_THE_BYTES);
    above_the_bytes[22..24].copy_from_slice(&200u16.to_le_bytes());

    // The other direction, and not symmetrical: the bytes are all present and
    // the declared length cuts the last message in half, so the walk overruns
    // the region the header framed. `finish` writes the buffer's own length and
    // cannot produce either.
    let mut below_the_bytes = one_quote(DECLARED_BELOW_THE_BYTES);
    below_the_bytes[22..24].copy_from_slice(&(whole as u16 - 20).to_le_bytes());

    // Magic belongs to the feed rather than to a call site, so a datagram
    // carrying a sibling's delimiter reached this group by misrouting or by a
    // publisher writing the wrong feed's constant. Either way it must not be
    // parsed at this feed's layout — and it must still be recorded, because the
    // misrouting is the finding.
    let mut foreign_magic = one_quote(FOREIGN_MAGIC);
    foreign_magic[0..2].copy_from_slice(&ANOTHER_FEEDS_MAGIC.to_le_bytes());

    // `Message Count` is what frames the walk, and the builder increments it per
    // `push`. A count above what the datagram holds makes a subscriber look for
    // a message that was never sent.
    let mut count_mismatch = one_quote(MESSAGE_COUNT_MISMATCH);
    count_mismatch[20] = 3;

    // `Message Count` of zero. The field's range is 1-255, so a datagram
    // declaring no messages is malformed rather than merely empty — and
    // `finish` refuses to emit a datagram with nothing pushed, so this is
    // another shape only something that is not this builder produces. It is the
    // one `DecodeError` variant the rest of this table leaves unexercised.
    let mut no_messages = one_quote(NO_MESSAGES);
    no_messages[20] = 0;

    // The other end of the declared-length range, and the branch `past_the_cap`
    // does not reach: below the 24 bytes of header that every datagram begins
    // with, so the header declares a datagram too small to contain the header
    // that declared it.
    let mut below_the_header = one_quote(DECLARED_BELOW_THE_HEADER);
    below_the_header[22..24].copy_from_slice(&12u16.to_le_bytes());

    // Shorter than the 24-byte header, so there is no header to read and no
    // field to trust — including the `Channel ID` at offset 3, which is why the
    // manifest can only count this datagram and not describe it. Twelve bytes
    // of a header, as a publisher writing a truncated datagram would leave.
    let short_of_the_header = one_quote(SHORT_OF_THE_HEADER)[..12].to_vec();

    vec![
        Malformed {
            channel_id: UNKNOWN_SCHEMA,
            payload: unknown_schema,
            verdict: DecodeError::UnsupportedSchema(UNIMPLEMENTED_SCHEMA_VERSION),
        },
        Malformed {
            channel_id: DECLARED_PAST_THE_CAP,
            payload: past_the_cap,
            verdict: DecodeError::DeclaredLengthOutOfRange {
                declared: 1300,
                min: DATAGRAM_HEADER_SIZE,
                max: MAX_DATAGRAM_SIZE,
            },
        },
        Malformed {
            channel_id: DECLARED_ABOVE_THE_BYTES,
            payload: above_the_bytes,
            verdict: DecodeError::ShortBuffer {
                need: 200,
                got: whole,
            },
        },
        Malformed {
            channel_id: DECLARED_BELOW_THE_BYTES,
            payload: below_the_bytes,
            verdict: DecodeError::MessageOverrunsDatagram {
                offset: DATAGRAM_HEADER_SIZE,
                declared: body_len as u8,
                remaining: body_len - 20,
            },
        },
        Malformed {
            channel_id: FOREIGN_MAGIC,
            payload: foreign_magic,
            verdict: DecodeError::MagicMismatch {
                expected: TopOfBook::MAGIC,
                found: ANOTHER_FEEDS_MAGIC,
            },
        },
        Malformed {
            channel_id: NO_MESSAGES,
            payload: no_messages,
            verdict: DecodeError::EmptyDatagram,
        },
        Malformed {
            channel_id: DECLARED_BELOW_THE_HEADER,
            payload: below_the_header,
            verdict: DecodeError::DeclaredLengthOutOfRange {
                declared: 12,
                min: DATAGRAM_HEADER_SIZE,
                max: MAX_DATAGRAM_SIZE,
            },
        },
        Malformed {
            channel_id: MESSAGE_COUNT_MISMATCH,
            payload: count_mismatch,
            verdict: DecodeError::MessageCountMismatch {
                declared: 3,
                found: 1,
            },
        },
        Malformed {
            channel_id: SHORT_OF_THE_HEADER,
            payload: short_of_the_header,
            verdict: DecodeError::ShortBuffer {
                need: DATAGRAM_HEADER_SIZE,
                got: 12,
            },
        },
    ]
}

/// The four faults a decoder cannot see, each on its own channel instance.
///
/// Every datagram here is structurally perfect and comes off the real encoder.
/// The violation is in the relation between datagrams, which is why it is the
/// health tier's business and why nothing but the archive can answer it after
/// the fact.
fn sequence_faults(wire: &mut Wire) {
    // A gap: the publisher advanced its sequence without sending the datagram
    // that number belonged to.
    for seq in [0, 1, 3] {
        emit(wire, SEQUENCE_GAP, seq);
    }
    // A duplicate: the same sequence number sent twice in one era, which makes
    // one datagram's messages applicable twice.
    for seq in [0, 1, 1, 2] {
        emit(wire, DUPLICATE, seq);
    }
    // A reordered pair: 3 arrives before 2, and the pair is last, so the
    // instance's last arrival is below its peak. Either the publisher sent them
    // out of order or the network delivered them so, and only a cross-site join
    // can tell those apart — which needs the bytes at both sites.
    for seq in [0, 1, 3, 2] {
        emit(wire, REORDERED, seq);
    }
    // Backward motion that is not a reset: the sequence returns to 0 with
    // `Reset Count` unchanged, so it is not the second era the specification
    // allows. This is the case a tracker keyed less finely than the channel
    // instance invents for itself out of two healthy publishers.
    for seq in [0, 1, 2, 0] {
        emit(wire, BACKWARD_MOTION, seq);
    }
}

fn emit(wire: &mut Wire, channel_id: u8, sequence_number: u64) {
    let sequence = ChannelSequence::resume(channel_id, ResetCount::NEVER_RESET, sequence_number);
    wire.arrive(
        encode(sequence, PortRole::Mktdata, &[Msg::Quote(1), Msg::Trade(1)]),
        PUBLISHER_A,
        PortRole::Mktdata,
    );
}

/// Everything above, in one archive: what a recorder sees is one stream, and a
/// case that only survives on its own would not be evidence of much.
fn recorded() -> (Vec<Malformed>, Vec<OwnedDatagram>, Recorded) {
    let cases = malformed();
    let mut wire = Wire::new();
    for case in &cases {
        wire.arrive(case.payload.clone(), PUBLISHER_A, PortRole::Mktdata);
    }
    sequence_faults(&mut wire);
    let sent = wire.sent;
    let archive = record(&sent, JOINED);
    (cases, sent, archive)
}

#[test]
fn every_byte_of_a_publisher_violation_is_in_the_archive_and_replays_identically() {
    let (cases, sent, archive) = recorded();
    let replayed = replay(&archive.object);

    assert_eq!(
        replayed.len(),
        sent.len(),
        "the archive replayed {} of the {} datagrams that arrived; a recorder that quietly drops \
         what it cannot parse destroys the evidence needed to diagnose the publisher",
        replayed.len(),
        sent.len()
    );
    for (index, (arrived, back)) in sent.iter().zip(&replayed).enumerate() {
        assert_eq!(
            arrived.payload, back.payload,
            "payload bytes of datagram {index}"
        );
        assert_eq!(arrived, back, "datagram {index} as a whole value");
    }
    assert_eq!(sent, replayed);

    // Named, so a failure says which violation went missing rather than only
    // that a count is short.
    for case in &cases {
        assert!(
            replayed.iter().any(|dg| dg.payload == case.payload),
            "the archive does not hold the datagram on channel {} whose verdict is {}",
            case.channel_id,
            case.verdict
        );
    }
}

#[test]
fn a_conformant_subscriber_refuses_each_datagram_the_archive_kept() {
    let (cases, _, archive) = recorded();
    let replayed = replay(&archive.object);

    for case in &cases {
        let back = replayed
            .iter()
            .find(|dg| dg.payload == case.payload)
            .unwrap_or_else(|| {
                panic!(
                    "the archive does not hold the datagram on channel {}",
                    case.channel_id
                )
            });
        let refusal = Datagram::decode(&back.payload, TopOfBook::MAGIC)
            .err()
            .unwrap_or_else(|| {
                panic!(
                    "the datagram on channel {} decoded cleanly, so it is not the violation this \
                     case claims",
                    case.channel_id
                )
            });
        assert_eq!(
            refusal, case.verdict,
            "the verdict on channel {}",
            case.channel_id
        );
    }
}

#[test]
fn a_datagram_shorter_than_the_header_is_counted_in_the_manifest_rather_than_skipped() {
    let (cases, _, archive) = recorded();
    let short = cases
        .iter()
        .find(|case| case.channel_id == SHORT_OF_THE_HEADER)
        .expect("the short case is in the stream");
    assert!(short.payload.len() < DATAGRAM_HEADER_SIZE);

    assert_eq!(
        archive.manifest.short_datagrams, 1,
        "a silent skip would make the manifest disagree with the object for no visible reason"
    );
    // There is no `Channel ID` to key on, so the datagram is in the object and
    // not in a coverage row. The count is what tells a reader those two numbers
    // are allowed to differ.
    assert!(
        archive
            .coverage(PUBLISHER_A, SHORT_OF_THE_HEADER, PortRole::Mktdata)
            .is_none(),
        "a datagram with no header cannot have its `Channel ID` read out of one"
    );
    assert_eq!(
        archive.manifest.datagram_count,
        archive
            .manifest
            .instances
            .values()
            .map(|c| c.count)
            .sum::<u64>()
            + archive.manifest.short_datagrams,
        "every archived datagram is either described or counted"
    );
}

#[test]
fn the_manifest_describes_a_datagram_whose_schema_version_it_cannot_read() {
    let (_, _, archive) = recorded();

    // The coverage read is the one deliberate exception to "the record path
    // never parses", and it reads three bare integers at fixed offsets rather
    // than going through `DatagramHeader::decode` — which refuses an unknown
    // schema version and would drop the row for exactly the datagram most worth
    // knowing about.
    let coverage = archive.expect_coverage(PUBLISHER_A, UNKNOWN_SCHEMA, PortRole::Mktdata);
    assert_eq!(
        (coverage.first_seq, coverage.last_seq, coverage.count),
        (0, 0, 1)
    );
    assert_eq!(coverage.reset_counts_seen, vec![0]);

    for channel_id in [
        DECLARED_PAST_THE_CAP,
        DECLARED_ABOVE_THE_BYTES,
        DECLARED_BELOW_THE_BYTES,
        FOREIGN_MAGIC,
        MESSAGE_COUNT_MISMATCH,
        NO_MESSAGES,
        DECLARED_BELOW_THE_HEADER,
    ] {
        let coverage = archive.expect_coverage(PUBLISHER_A, channel_id, PortRole::Mktdata);
        assert_eq!(
            coverage.count, 1,
            "the datagram on channel {channel_id} is described, whatever a decoder makes of it"
        );
    }
}

#[test]
fn the_sequence_faults_decode_cleanly_and_are_visible_only_in_the_coverage() {
    let (_, sent, archive) = recorded();
    let replayed = replay(&archive.object);

    let faulted: Vec<&OwnedDatagram> = replayed
        .iter()
        .filter(|dg| [SEQUENCE_GAP, DUPLICATE, REORDERED, BACKWARD_MOTION].contains(&dg.payload[3]))
        .collect();
    assert_eq!(faulted.len(), 15, "three, four, four and four datagrams");
    for dg in &faulted {
        let decoded = Datagram::decode(&dg.payload, TopOfBook::MAGIC).unwrap_or_else(|e| {
            panic!("a sequence fault is not a malformed datagram, but this one is: {e}")
        });
        assert_eq!(decoded.header().schema_version, SCHEMA_VERSION);
        assert_eq!(decoded.header().msg_count, 2);
    }

    // The window's edges are in arrival order, which is what makes each fault
    // legible: a gap widens the range without filling it, a duplicate lifts the
    // count above the range, a reorder ends above where it passed, and backward
    // motion ends below where it started.
    let gap = archive.expect_coverage(PUBLISHER_A, SEQUENCE_GAP, PortRole::Mktdata);
    assert_eq!((gap.first_seq, gap.last_seq, gap.count), (0, 3, 3));

    let duplicate = archive.expect_coverage(PUBLISHER_A, DUPLICATE, PortRole::Mktdata);
    assert_eq!(
        (duplicate.first_seq, duplicate.last_seq, duplicate.count),
        (0, 2, 4),
        "four datagrams across a three-wide window"
    );

    let reordered = archive.expect_coverage(PUBLISHER_A, REORDERED, PortRole::Mktdata);
    assert_eq!(
        (reordered.first_seq, reordered.last_seq, reordered.count),
        (0, 2, 4),
        "the window ends on the earlier half of the reordered pair, because the edges are in \
         arrival order and a coverage row that sorted them would erase the reordering"
    );
    // The order is only recoverable from the object, which is the argument for
    // keeping the bytes rather than only the coverage.
    let order: Vec<u64> = faulted
        .iter()
        .filter(|dg| dg.payload[3] == REORDERED)
        .map(|dg| u64::from_le_bytes(dg.payload[4..12].try_into().expect("eight bytes")))
        .collect();
    assert_eq!(order, vec![0, 1, 3, 2], "the pair arrived reordered");

    assert_eq!(sent.len(), replayed.len());
}

#[test]
fn backward_sequence_motion_without_a_reset_is_not_recorded_as_an_era_change() {
    let (_, _, archive) = recorded();

    let coverage = archive.expect_coverage(PUBLISHER_A, BACKWARD_MOTION, PortRole::Mktdata);
    assert_eq!(
        (coverage.first_seq, coverage.last_seq, coverage.count),
        (0, 0, 4)
    );
    assert!(
        coverage.last_seq < 2,
        "the sequence went backwards, and the coverage says so"
    );
    assert_eq!(
        coverage.reset_counts_seen,
        vec![0],
        "one era, so the backward motion is a violation and not a restart — the reset count is \
         the only thing that separates the two, and inventing one here would excuse the publisher"
    );
}

#[test]
fn the_manifest_states_only_the_ports_the_recorder_was_asked_to_join() {
    let (_, _, archive) = recorded();

    let joined: Vec<&str> = archive
        .manifest
        .roles_joined
        .iter()
        .map(|row| row.role.as_str())
        .collect();
    assert_eq!(joined, vec!["mktdata", "refdata"]);
    assert!(archive
        .manifest
        .roles_joined
        .iter()
        .all(|row| row.group == GROUP));
    assert_eq!(archive.manifest.capture_drop_total, 0);
    assert_eq!(
        archive.manifest.instances_dropped, 0,
        "ten channel instances is well inside the per-segment cap"
    );
}
