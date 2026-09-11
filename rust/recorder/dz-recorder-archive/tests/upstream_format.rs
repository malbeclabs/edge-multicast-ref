//! The venue-side archive shape, round-tripped and refused.
//!
//! Every test here writes into a `Vec<u8>` and reads back out of one, except
//! the publication test, which needs a directory to move a file into. No
//! socket, no privilege, no venue.
#![forbid(unsafe_code)]

use dz_recorder_archive::upstream::{
    publish, upstream_object_extension, RecvTsKindLabel, UpstreamConnection, UpstreamFormatError,
    UpstreamManifest, UpstreamObjectReader, UpstreamSegmentWriter, MAX_UPSTREAM_MESSAGE_BYTES,
    UPSTREAM_FORMAT_VERSION, UPSTREAM_MAGIC, UPSTREAM_OBJECT_EXTENSION,
};
use dz_recorder_archive::{Compression, RotationPolicy};
use dz_recorder_core::{RecvTsKind, SinkError};

const KEY: &str = "feed=top-of-book/env=test/site=site-1/recorder=recorder-1/\
                   date=2026-09-09/hour=12/1-2-3.dzus";

/// The digest of some bytes, in the manifest's own notation.
fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut out = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn connections() -> Vec<UpstreamConnection> {
    vec![
        UpstreamConnection::new("mktdata", RecvTsKind::KernelSoftware),
        UpstreamConnection::new("catalogue", RecvTsKind::ApplicationFallback),
    ]
}

/// One message and everything the format states about it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Read {
    connection: String,
    recv_ts_kind: RecvTsKind,
    recv_ts_ns: u64,
    bytes: Vec<u8>,
}

fn write(messages: &[(u16, u64, Vec<u8>)]) -> Vec<u8> {
    let mut writer =
        UpstreamSegmentWriter::open(Vec::new(), &connections()).expect("the header is writable");
    for (connection, recv_ts_ns, bytes) in messages {
        writer
            .write_message(*connection, *recv_ts_ns, bytes)
            .expect("the message is writable");
    }
    writer.finish().expect("the segment flushes")
}

fn read_all(object: &[u8]) -> Result<Vec<Read>, UpstreamFormatError> {
    let mut reader = UpstreamObjectReader::open(KEY, object)?;
    let mut out = Vec::new();
    while let Some(message) = reader.next_message()? {
        out.push(Read {
            connection: message.connection.to_owned(),
            recv_ts_kind: message.recv_ts_kind,
            recv_ts_ns: message.recv_ts_ns,
            bytes: message.bytes.to_vec(),
        });
    }
    Ok(out)
}

/// Every message comes back exactly as it went in, at both ends of the size
/// range the format admits.
///
/// The zero-length message is in here because it is the one a length-delimited
/// format is most likely to lose: a reader that treated a zero length as *no
/// more records* would end the object at the first heartbeat a venue sends with
/// an empty body, and report every message after it as never having arrived.
///
/// The largest admitted message is in here because the bound is the one number
/// in this format that a reader and a writer can disagree about while both
/// still work on ordinary traffic.
#[test]
fn an_object_round_trips_every_message_it_was_given() {
    let largest = vec![0xA5u8; MAX_UPSTREAM_MESSAGE_BYTES as usize];
    let object = write(&[
        (0, 1_700_000_000_000_000_001, b"{\"t\":\"quote\"}".to_vec()),
        // A message of no bytes at all: an empty upstream message on a session,
        // a keep-alive with no body.
        (0, 1_700_000_000_000_000_002, Vec::new()),
        (1, 1_700_000_000_000_000_003, b"[]".to_vec()),
        (0, 1_700_000_000_000_000_004, largest.clone()),
    ]);

    let read = read_all(&object).expect("a whole object reads");
    assert_eq!(
        read,
        vec![
            Read {
                connection: "mktdata".to_owned(),
                recv_ts_kind: RecvTsKind::KernelSoftware,
                recv_ts_ns: 1_700_000_000_000_000_001,
                bytes: b"{\"t\":\"quote\"}".to_vec(),
            },
            Read {
                connection: "mktdata".to_owned(),
                recv_ts_kind: RecvTsKind::KernelSoftware,
                recv_ts_ns: 1_700_000_000_000_000_002,
                bytes: Vec::new(),
            },
            // The stamp kind is the connection's and not the object's: the
            // second connection stamped its own payloads differently and the
            // record says so.
            Read {
                connection: "catalogue".to_owned(),
                recv_ts_kind: RecvTsKind::ApplicationFallback,
                recv_ts_ns: 1_700_000_000_000_000_003,
                bytes: b"[]".to_vec(),
            },
            Read {
                connection: "mktdata".to_owned(),
                recv_ts_kind: RecvTsKind::KernelSoftware,
                recv_ts_ns: 1_700_000_000_000_000_004,
                bytes: largest,
            },
        ]
    );
}

/// **A truncated object is a refusal that names the object, never a short
/// read.**
///
/// This is the mutant that matters. A reader that stopped quietly at a
/// half-written record would hand a derivation fewer messages than were
/// written, with nothing anywhere saying so — and the rows that came out would
/// describe a venue that went quiet at the instant the segment was cut. A
/// silent stop and a genuine outage are the same rows.
///
/// Both places a record can be cut are checked, because they are two branches:
/// inside the fixed-width record header, and inside the message body it
/// declared.
#[test]
fn a_truncated_object_is_refused_rather_than_read_short() {
    let object = write(&[
        (0, 10, b"first".to_vec()),
        (0, 20, b"second".to_vec()),
        (0, 30, b"third".to_vec()),
    ]);

    // Cut inside the last message's body.
    let cut_in_body = &object[..object.len() - 2];
    match read_all(cut_in_body) {
        Err(UpstreamFormatError::Truncated {
            object_key,
            what,
            messages_read,
            wanted,
            got,
        }) => {
            assert_eq!(object_key, KEY, "the refusal has to name the object");
            assert_eq!(what, "a message body");
            assert_eq!(messages_read, 2, "the two whole messages were read");
            assert_eq!((wanted, got), (5, 3));
        }
        other => panic!("a body cut short was not refused: {other:?}"),
    }

    // Cut inside the last message's record header.
    let cut_in_header = &object[..object.len() - b"third".len() - 3];
    match read_all(cut_in_header) {
        Err(UpstreamFormatError::Truncated {
            object_key,
            what,
            messages_read,
            wanted,
            got,
        }) => {
            assert_eq!(object_key, KEY);
            assert_eq!(what, "a record header");
            assert_eq!(messages_read, 2);
            assert_eq!((wanted, got), (14, 11));
        }
        other => panic!("a record header cut short was not refused: {other:?}"),
    }

    // And the whole object still ends cleanly, so the refusals above are about
    // truncation and not about the reader mistaking every end for one.
    assert_eq!(
        read_all(&object)
            .expect("the whole object reads")
            .iter()
            .map(|r| r.bytes.clone())
            .collect::<Vec<_>>(),
        vec![b"first".to_vec(), b"second".to_vec(), b"third".to_vec()],
    );
}

/// A reader that has refused stays refused.
///
/// A caller looping on `next_message` until it gets `Ok(None)` must not be able to walk
/// past a truncation by asking again: the second call would be reading a record
/// header out of the middle of a message body.
#[test]
fn a_reader_that_refused_does_not_resume_mid_object() {
    let object = write(&[(0, 10, b"first".to_vec()), (0, 20, b"second".to_vec())]);
    let mut reader =
        UpstreamObjectReader::open(KEY, &object[..object.len() - 3]).expect("the header reads");
    assert!(reader.next_message().expect("the first message").is_some());
    assert!(reader.next_message().is_err(), "the truncation is refused");
    assert!(
        reader.next_message().is_err(),
        "asking again resumed inside a record"
    );
}

/// A message over the bound is refused by the writer, so no object holds one.
#[test]
fn a_message_over_the_bound_is_refused_by_the_writer() {
    let mut writer = UpstreamSegmentWriter::open(Vec::new(), &connections()).expect("the header");
    let over = vec![0u8; MAX_UPSTREAM_MESSAGE_BYTES as usize + 1];
    let refused = writer
        .write_message(0, 1, &over)
        .expect_err("a message over the bound is not writable");
    assert!(
        refused
            .to_string()
            .contains(&MAX_UPSTREAM_MESSAGE_BYTES.to_string()),
        "the refusal states the bound: {refused}"
    );
    assert_eq!(writer.message_count(), 0);
}

/// A connection the segment never declared cannot be written against.
#[test]
fn a_connection_the_header_does_not_declare_is_refused() {
    let mut writer = UpstreamSegmentWriter::open(Vec::new(), &connections()).expect("the header");
    assert!(
        writer.write_message(2, 1, b"x").is_err(),
        "a third connection was accepted into a two-connection object"
    );
}

/// Something that is not one of these objects is refused by the magic.
///
/// The two archive shapes land in one object store under one shipper, so a
/// reader handed the wrong one has to say which object it was handed rather
/// than read a pcapng section header block as a record length.
#[test]
fn an_object_that_is_not_an_upstream_object_is_refused() {
    // The first bytes of a pcapng file: a Section Header Block.
    let pcapng = [
        0x0A, 0x0D, 0x0D, 0x0A, 0x1C, 0, 0, 0, 0x4D, 0x3C, 0x2B, 0x1A,
    ];
    match UpstreamObjectReader::open(KEY, &pcapng[..]) {
        Err(UpstreamFormatError::NotAnUpstreamObject { object_key }) => {
            assert_eq!(object_key, KEY);
        }
        other => panic!("a pcapng object was not refused: {other:?}"),
    }
}

/// A format version this build does not know is refused, and says both numbers.
#[test]
fn an_unsupported_format_version_is_refused_and_names_both_versions() {
    let mut object = write(&[(0, 1, b"x".to_vec())]);
    let next = UPSTREAM_FORMAT_VERSION + 1;
    object[8..10].copy_from_slice(&next.to_le_bytes());
    match UpstreamObjectReader::open(KEY, &object[..]) {
        Err(UpstreamFormatError::UnsupportedVersion {
            object_key,
            version,
            known,
        }) => {
            assert_eq!(object_key, KEY);
            assert_eq!((version, known), (next, UPSTREAM_FORMAT_VERSION));
        }
        other => panic!("a future version was not refused: {other:?}"),
    }
}

/// The header states the version and declares the connections once.
#[test]
fn the_header_states_the_format_version_and_the_connections() {
    let object = write(&[]);
    assert_eq!(&object[..8], &UPSTREAM_MAGIC);
    let reader = UpstreamObjectReader::open(KEY, &object[..]).expect("an empty object reads");
    assert_eq!(
        reader.connections(),
        &[
            UpstreamConnection {
                name: "mktdata".to_owned(),
                recv_ts_kind: RecvTsKindLabel::KernelSoftware,
            },
            UpstreamConnection {
                name: "catalogue".to_owned(),
                recv_ts_kind: RecvTsKindLabel::ApplicationFallback,
            },
        ]
    );
}

/// An object holding nothing ends cleanly and is not a truncation.
#[test]
fn an_object_with_no_messages_ends_cleanly() {
    assert_eq!(
        read_all(&write(&[])).expect("an empty object reads"),
        vec![]
    );
}

/// This shape is not the pcapng one, and its name says so.
///
/// The extension is the only cheap discriminator an object store has, so a
/// shipper that put one shape under the other's extension would hand a pcapng
/// reader bytes it cannot refuse gracefully.
#[test]
fn the_object_extension_is_not_the_pcapng_one() {
    assert_eq!(UPSTREAM_OBJECT_EXTENSION, "dzus");
    assert_eq!(upstream_object_extension(Compression::None), "dzus");
    assert_eq!(
        upstream_object_extension(Compression::Zstd { level: 3 }),
        "dzus.zst"
    );
    for compression in [Compression::None, Compression::Zstd { level: 3 }] {
        assert_ne!(
            upstream_object_extension(compression),
            compression.extension(),
            "the two archive shapes land under one name"
        );
        // And the compression suffix itself is the same word in both, so the
        // two names cannot come to disagree about what zstd is called.
        assert!(
            compression.extension().ends_with(compression.suffix()),
            "{}",
            compression.extension()
        );
    }
}

/// Rotation is the datagram archive's own rule, and this writer keeps the number
/// the rule reads.
///
/// **The writer does not rotate.** It accounts for the bytes it has put on the
/// disk and states the window it covers, and the decision is the venue's own
/// binary's — which applies [`RotationPolicy`] rather than declaring a second
/// rule. So there are two halves and this holds them meeting: the policy's own
/// bounds, size or age whichever comes first, and the writer's accounting, which
/// is the value the size bound is read against.
///
/// The mutant the second half kills is a count that has drifted from the bytes
/// on the disk. The policy would then be exactly right about the wrong number:
/// a segment rotating at a size nobody configured, and objects whose uniformity
/// the analysis tier is entitled to assume.
#[test]
fn rotation_is_the_archive_tiers_own_policy_over_this_writers_own_count() {
    let policy = RotationPolicy {
        rotate_bytes: 1_000,
        rotate_interval: std::time::Duration::from_secs(60),
    };
    assert!(!policy.due(999, 0, 59_999_999_999));
    assert!(policy.due(1_000, 0, 0), "the size bound");
    assert!(policy.due(0, 0, 60_000_000_000), "the age bound");

    let mut writer =
        UpstreamSegmentWriter::open(Vec::new(), &connections()).expect("the header is writable");
    // The header is bytes on the disk too, and a segment holding only its header
    // is not due: an empty rotation is not published, so a policy that fired on
    // one would publish a window nobody observed.
    let header_only = writer.bytes_written();
    assert!(header_only > 0, "the header was not accounted for");
    assert!(!policy.due(header_only, 0, 0));

    let mut written = 0u64;
    while !policy.due(writer.bytes_written(), 0, 0) {
        writer
            .write_message(0, 1_700_000_000_000_000_000 + written, &[0x5Au8; 100])
            .expect("the message is writable");
        written += 1;
        assert!(written < 1_000, "the writer's own count is not moving");
    }
    // The window the manifest states, from the same accounting.
    assert_eq!(writer.start_ns(), Some(1_700_000_000_000_000_000));
    assert_eq!(
        writer.end_ns(),
        Some(1_700_000_000_000_000_000 + written - 1)
    );
    assert_eq!(writer.message_count(), written);

    let accounted = writer.bytes_written();
    let object = writer.finish().expect("the segment flushes");
    assert_eq!(
        accounted,
        object.len() as u64,
        "the size bound is read against a number that is not the bytes on the disk"
    );
    // And the object the rule fired on is one the reader reads whole.
    assert_eq!(
        read_all(&object).expect("a whole object reads").len() as u64,
        written
    );
}

/// A published object carries the key and the digest a derivation is idempotent
/// on, and the digest is of the bytes that landed.
///
/// **Both compressions, because the digest is of the object that lands.** Run
/// uncompressed only, this passes over a `seal` that hashed the segment instead
/// of the object: the two are the same bytes there, so a reader checking a
/// fetched `.dzus.zst` against the manifest would be the first to find out. The
/// zstd half is also where the frame checksum is, which is the difference
/// between an archive that can tell it has been damaged and one that decodes to
/// a different buffer with no error at all.
#[test]
fn a_published_object_carries_its_key_and_its_digest() {
    for compression in [Compression::None, Compression::Zstd { level: 3 }] {
        a_published_object_carries_its_key_and_its_digest_under(compression);
    }
}

fn a_published_object_carries_its_key_and_its_digest_under(compression: Compression) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let segment = dir.path().join("open.dzus");
    // Compressible and not tiny, so that the zstd path is a real encode rather
    // than a frame around three bytes.
    let object = write(&[
        (0, 1_700_000_000_000_000_000, b"one".to_vec()),
        (1, 1_700_000_000_000_000_000, vec![0x5Au8; 64 << 10]),
    ]);
    std::fs::write(&segment, &object).expect("the segment is writable");

    let completed = dir.path().join("completed");
    let published = publish(
        &segment,
        &completed,
        UpstreamManifest {
            format_version: UPSTREAM_FORMAT_VERSION,
            site: "site-1".to_owned(),
            recorder: "recorder-1".to_owned(),
            env: "test".to_owned(),
            feed: "top-of-book".to_owned(),
            observation: "site-1/recorder-1".to_owned(),
            connections: connections(),
            segment_seq: 3,
            start_ns: 1_700_000_000_000_000_000,
            end_ns: 1_700_000_000_000_000_000,
            message_count: 2,
            object_key: String::new(),
            sha256: String::new(),
            byte_count: 0,
        },
        compression,
    )
    .expect("the object publishes");

    // The Hive-partitioned key the datagram archive already produces, with this
    // shape's own extension on the end — and the compression suffix on that,
    // because the key names the object that landed.
    assert_eq!(
        published.manifest.object_key,
        format!(
            "feed=top-of-book/env=test/site=site-1/recorder=recorder-1/\
             date=2023-11-14/hour=22/1700000000000000000-1700000000000000000-3.{}",
            upstream_object_extension(compression)
        )
    );

    let landed = std::fs::read(&published.path).expect("the object landed");
    assert_eq!(
        published.manifest.byte_count,
        landed.len() as u64,
        "the byte count is not of the object that landed"
    );
    assert_eq!(published.manifest.sha256.len(), 64);
    assert!(
        published
            .manifest
            .sha256
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
        "{}",
        published.manifest.sha256
    );
    // Of the bytes that landed, so a reader can check the object it fetched
    // without decompressing it first.
    assert_eq!(
        published.manifest.sha256,
        sha256_hex(&landed),
        "the digest is not of the object that landed"
    );

    match compression {
        Compression::None => assert_eq!(landed, object),
        Compression::Zstd { .. } => {
            assert!(
                landed.len() < object.len(),
                "the object landed uncompressed under a compression that declares zstd"
            );
            // The frame checksum, which is what lets a damaged object be
            // refused rather than decoded to a different buffer. Bit 2 of the
            // frame header descriptor, which is the byte after the magic.
            assert_eq!(&landed[..4], &[0x28, 0xB5, 0x2F, 0xFD], "a zstd frame");
            assert_eq!(
                landed[4] & 0x04,
                0x04,
                "the frame carries no content checksum"
            );
            // And it decompresses to the object the writer produced, message
            // for message.
            let decoded = zstd::stream::decode_all(&landed[..]).expect("the object decompresses");
            assert_eq!(decoded, object);
            assert_eq!(read_all(&decoded).expect("a whole object reads").len(), 2);
        }
    }

    // And the manifest is beside it, with the same key and digest, so a shipper
    // needs to know nothing about the layout.
    let manifest_path = published.path.with_file_name(format!(
        "{}.manifest.json",
        published
            .path
            .file_name()
            .expect("a file name")
            .to_string_lossy()
    ));
    let beside: UpstreamManifest = serde_json::from_str(
        &std::fs::read_to_string(&manifest_path).expect("the manifest landed"),
    )
    .expect("the manifest parses");
    assert_eq!(beside, published.manifest);
}

/// **Two objects of one window are told apart by their keys and never ordered
/// by them**, which is the property `009`'s occurrence window rests on and the
/// one it must not overstate.
///
/// `009` breaks a tie on equal receive stamps with `object_key` ahead of
/// `message_index`, because the record index restarts at zero in every object
/// and so orders nothing across two of them. What the key buys there is a
/// **total** order: it names one object and cannot collide, because the site
/// and the recorder are in it. What it does not buy is the objects' own order,
/// and this is the case that says so — the name's last component is
/// `segment_seq` written without padding, so a pair that reaches it compares
/// `1` against `9` and puts segment 10 ahead of segment 9.
///
/// The pair reaches it exactly where that tie-break is needed. `start_ns` and
/// `end_ns` are the smallest and the largest receive stamp the window saw, so
/// an object whose whole window fits inside one clock tick states one stamp for
/// both — and a rotation inside that tick hands the next object the same two,
/// leaving the sequence the only component that differs.
///
/// The mutant this kills is padding the component and leaving `009`'s paragraph
/// as it stands. The keys would then collate in the objects' order for objects
/// minted after the change and for no others, and the view would be resting on
/// which commit wrote an object — which is the dependence that paragraph exists
/// to refuse.
#[test]
fn two_objects_of_one_window_are_told_apart_by_their_keys_and_not_ordered_by_them() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let completed = dir.path().join("completed");
    // One stamp for the whole window, which is what a clock coarser than the
    // rotation produces and what makes both objects state the same two.
    let at = 1_700_000_000_000_000_000;

    let key_of = |segment_seq: u64| -> String {
        let segment = dir.path().join(format!("open-{segment_seq}.dzus"));
        std::fs::write(&segment, write(&[(0, at, b"one".to_vec())]))
            .expect("the segment is writable");
        publish(
            &segment,
            &completed,
            UpstreamManifest {
                format_version: UPSTREAM_FORMAT_VERSION,
                site: "site-1".to_owned(),
                recorder: "recorder-1".to_owned(),
                env: "test".to_owned(),
                feed: "top-of-book".to_owned(),
                observation: "site-1/recorder-1".to_owned(),
                connections: connections(),
                segment_seq,
                start_ns: at,
                end_ns: at,
                message_count: 1,
                object_key: String::new(),
                sha256: String::new(),
                byte_count: 0,
            },
            Compression::None,
        )
        .expect("the object publishes")
        .manifest
        .object_key
    };

    // In the order the recorder wrote them.
    let ninth = key_of(9);
    let tenth = key_of(10);

    // Everything ahead of the sequence agrees, because everything ahead of it
    // is the partition prefix and the window — which is what leaves the
    // comparison to the one unpadded component.
    assert_eq!(
        ninth.strip_suffix("-9.dzus"),
        tenth.strip_suffix("-10.dzus"),
        "the two keys differ in more than the sequence: {ninth} and {tenth}"
    );
    // And the object written second sorts first.
    assert!(
        tenth < ninth,
        "the sequence collates numerically after all: {tenth} then {ninth}"
    );
}

/// The stamp kind travels as a token, and it is `dz-recorder-core`'s own
/// taxonomy rather than a second one.
#[test]
fn the_receive_stamp_kind_is_the_recorders_own_taxonomy() {
    for kind in [RecvTsKind::KernelSoftware, RecvTsKind::ApplicationFallback] {
        let label = RecvTsKindLabel::of(kind);
        assert_eq!(label.kind(), kind);
        assert_eq!(RecvTsKindLabel::from_byte(label.as_byte()), Some(label));
    }
    assert_eq!(
        serde_json::to_value(RecvTsKindLabel::KernelSoftware).expect("a token"),
        serde_json::json!("kernel-software")
    );
    assert_eq!(
        serde_json::to_value(RecvTsKindLabel::ApplicationFallback).expect("a token"),
        serde_json::json!("application-fallback")
    );
    assert_eq!(RecvTsKindLabel::from_byte(2), None);
}

/// **A connection name is declared once, and the writer is where that is
/// fixed.**
///
/// The name is the whole of a connection's identity in this format. A record
/// carries an index into the header's table, a reader resolves that index to a
/// name, and everything above it resolves the name back to the caller's own
/// `ConnectionId` — `VenueObjectId::connection` is a name lookup, because a
/// `ConnectionId` is a `&'static str` and a name read out of a file cannot
/// become one.
///
/// So two entries with one name make that lookup ambiguous: every record on the
/// second entry is attributed to the first connection, and the two may declare
/// different receive-stamp kinds while they do it. The mutant this kills is the
/// check's absence — a writer that admitted the header would land an object
/// whose records cannot be attributed at all, and the object is the only copy of
/// the window it holds.
#[test]
fn a_connection_name_declared_twice_is_refused_by_the_writer() {
    let duplicated = vec![
        UpstreamConnection::new("mktdata", RecvTsKind::KernelSoftware),
        UpstreamConnection::new("catalogue", RecvTsKind::ApplicationFallback),
        // The same name again, and with the other stamp kind, which is what
        // makes the ambiguity more than cosmetic.
        UpstreamConnection::new("mktdata", RecvTsKind::ApplicationFallback),
    ];
    match UpstreamSegmentWriter::open(Vec::new(), &duplicated) {
        Err(SinkError::Encode(detail)) => {
            // Both entries, because *the name is a duplicate* sends somebody to
            // read the header to find out which two it is.
            assert!(
                detail.contains("\"mktdata\"") && detail.contains("entries 0 and 2"),
                "{detail}"
            );
        }
        other => panic!("a duplicate connection name was not refused: {other:?}"),
    }
}

/// And a header that already holds one is refused on the way in.
///
/// A derivation runs over objects a shipper moved, and the build that wrote one
/// is not the build reading it. An index that resolves to an ambiguous name is
/// worse than a refusal, because the rows it produces name a connection they did
/// not arrive on — so the reader refuses rather than resolving to the first
/// match. The bytes are composed here by hand, since the writer above will not
/// produce them.
#[test]
fn a_header_that_declares_one_name_twice_is_refused_by_the_reader() {
    let mut object = Vec::new();
    object.extend_from_slice(&UPSTREAM_MAGIC);
    object.extend_from_slice(&UPSTREAM_FORMAT_VERSION.to_le_bytes());
    object.extend_from_slice(&2u16.to_le_bytes());
    for kind in [
        RecvTsKindLabel::KernelSoftware,
        RecvTsKindLabel::ApplicationFallback,
    ] {
        object.extend_from_slice(&7u16.to_le_bytes());
        object.push(kind.as_byte());
        object.extend_from_slice(b"mktdata");
    }
    match UpstreamObjectReader::open(KEY, &object[..]) {
        Err(UpstreamFormatError::DuplicateConnectionName {
            object_key,
            name,
            first,
            second,
        }) => {
            assert_eq!(object_key, KEY);
            assert_eq!(name, "mktdata");
            assert_eq!((first, second), (0, 1));
        }
        other => panic!("an ambiguous header was not refused: {other:?}"),
    }
}

/// The reader answers with the version its **own header** states.
///
/// The same number as this build's constant today, and the wrong answer the
/// moment a build reads a version it did not write: it would stamp its own
/// version on every row derived from the older object, and `format_version` is
/// the column that exists to expose exactly that disagreement.
///
/// One version is admitted, so this cannot yet be shown by reading two. What it
/// holds is where the value comes from: the header's version field is read out
/// of the object's bytes here and compared against what the reader answers, so
/// a reader that stopped keeping it has nothing to answer with.
#[test]
fn a_reader_answers_with_the_format_version_its_header_states() {
    let object = write(&[(0, 1_700_000_000_000_000_000, b"one".to_vec())]);
    let stated = u16::from_le_bytes([object[8], object[9]]);
    assert_eq!(
        stated, UPSTREAM_FORMAT_VERSION,
        "the fixture's own header is not this build's version"
    );
    let reader = UpstreamObjectReader::open(KEY, &object[..]).expect("the object opens");
    assert_eq!(reader.format_version(), stated);
}

/// The draft a publication is handed, so a test that is about the failure path
/// does not restate twelve fields to get there.
fn draft() -> UpstreamManifest {
    UpstreamManifest {
        format_version: UPSTREAM_FORMAT_VERSION,
        site: "site-1".to_owned(),
        recorder: "recorder-1".to_owned(),
        env: "test".to_owned(),
        feed: "top-of-book".to_owned(),
        observation: "site-1/recorder-1".to_owned(),
        connections: connections(),
        segment_seq: 3,
        start_ns: 1_700_000_000_000_000_000,
        end_ns: 1_700_000_000_000_000_000,
        message_count: 1,
        object_key: String::new(),
        sha256: String::new(),
        byte_count: 0,
    }
}

/// **A publication that cannot land leaves nothing behind.**
///
/// The temporary object is a sealed, full-size copy of the segment, inside
/// `completed_dir` itself and under a name no manifest points at. One failed
/// rotation leaks one of them, so a storage outage leaks one per rotation for as
/// long as it lasts: files the watermark does not account for and eviction
/// cannot reach, which is an unbounded disk out of the outage the watermark
/// exists to survive — the failure the pcapng side's own `clean_up` says it is
/// there to prevent.
///
/// The fault is forced where a real one falls, between the seal and the moves,
/// by putting a *directory* where the manifest has to land: the rename that
/// publishes it then cannot succeed. The mutant this kills is any early return
/// between those two points — each one left the sealed object and the temporary
/// manifest in place, and the object rename was the only failure with a cleanup
/// of its own.
#[test]
fn a_publication_that_cannot_land_leaves_no_sealed_object_behind() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let segment = dir.path().join("open.dzus");
    // Not tiny, so that what a leak would cost is what a leak really costs.
    let object = write(&[(0, 1_700_000_000_000_000_000, vec![0x5Au8; 64 << 10])]);
    std::fs::write(&segment, &object).expect("the segment is writable");

    let completed = dir.path().join("completed");
    std::fs::create_dir_all(&completed).expect("the completed directory");
    let blocked = format!(
        "1700000000000000000-1700000000000000000-3.{}.manifest.json",
        upstream_object_extension(Compression::None)
    );
    std::fs::create_dir(completed.join(&blocked)).expect("the blocking directory");

    let error = publish(&segment, &completed, draft(), Compression::None)
        .expect_err("the manifest cannot be moved onto a directory");
    assert!(matches!(error, SinkError::Io(_)), "{error:?}");

    let mut left: Vec<String> = std::fs::read_dir(&completed)
        .expect("the completed directory reads")
        .map(|entry| {
            entry
                .expect("an entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    left.sort();
    assert_eq!(
        left,
        vec![blocked],
        "a failed publication left files in completed_dir"
    );

    // And the segment is still where it was, which is the one thing the failure
    // path must never take: it is the only copy of the window.
    assert!(
        segment.exists(),
        "a failed publication removed the segment, which is the only copy of the window"
    );
}

/// **A failed retry does not take the manifest of the publication that landed.**
///
/// A publication's name is `(start_ns, end_ns, segment_seq, compression)`, so a
/// second publication of the same window computes the same final manifest name
/// as the first. That is not a corner: re-publication of one window is what
/// this crate's idempotent reprocessing is built around, and a retry after a
/// transient failure reaches it directly.
///
/// The cleanup therefore cannot treat that name as its own. Removing it
/// unconditionally deleted the manifest of the publication that had already
/// succeeded, while leaving that publication's object where it was — a
/// full-size object no manifest points at, which to a reader is a window that
/// never happened and to the watermark is bytes it does not account for. The
/// failure that triggered it was transient; the loss was not.
///
/// THE MUTANT THIS KILLS is the `installed.manifest` condition removed from
/// `clean_up`, which is how the code read before: the assertions below then
/// find the first publication's manifest gone and its object still there.
/// `a_publication_that_cannot_land_leaves_no_sealed_object_behind` does not
/// notice, because it publishes into an empty `completed_dir` where the name
/// this one is about belongs to nobody.
#[test]
fn a_failed_retry_leaves_an_earlier_publications_manifest_alone() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let segment = dir.path().join("open.dzus");
    let object = write(&[(0, 1_700_000_000_000_000_000, vec![0x5Au8; 4 << 10])]);
    std::fs::write(&segment, &object).expect("the segment is writable");

    let completed = dir.path().join("completed");

    // The publication that lands, and the manifest a reader now depends on.
    let published = publish(&segment, &completed, draft(), Compression::None)
        .expect("the first publication lands");
    let manifest_path = completed.join(format!(
        "1700000000000000000-1700000000000000000-3.{}.manifest.json",
        upstream_object_extension(Compression::None)
    ));
    let manifest_before =
        std::fs::read(&manifest_path).expect("the first publication wrote its manifest");
    assert!(
        published.path.exists(),
        "the first publication's object is the thing the manifest points at"
    );

    // The retry, failing before its own manifest could be written: the segment
    // it is told to read is not there. Same draft, so the same names.
    let error = publish(
        &dir.path().join("gone.dzus"),
        &completed,
        draft(),
        Compression::None,
    )
    .expect_err("a segment that is not there cannot be sealed");
    assert!(matches!(error, SinkError::Io(_)), "{error:?}");

    assert_eq!(
        std::fs::read(&manifest_path).ok().as_deref(),
        Some(manifest_before.as_slice()),
        "a failed retry removed the manifest of the publication that had landed, \
         leaving its object unreachable"
    );
    assert!(
        published.path.exists(),
        "a failed retry removed the object of the publication that had landed"
    );
}
