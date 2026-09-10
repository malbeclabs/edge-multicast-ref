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
use dz_recorder_core::RecvTsKind;

const KEY: &str = "feed=top-of-book/env=test/site=site-1/recorder=recorder-1/\
                   date=2026-09-09/hour=12/1-2-3.dzus";

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
        // A message of no bytes at all: an empty frame on a session, a
        // keep-alive with no body.
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

/// Rotation is the datagram archive's own rule, not a second one.
///
/// Size or age, whichever comes first. Held here because the two shapes sharing
/// a policy is the property, and a copy of the rule in the upstream writer would
/// pass every test written against itself.
#[test]
fn rotation_is_the_archive_tiers_own_policy() {
    let policy = RotationPolicy {
        rotate_bytes: 1_000,
        rotate_interval: std::time::Duration::from_secs(60),
    };
    assert!(!policy.due(999, 0, 59_999_999_999));
    assert!(policy.due(1_000, 0, 0), "the size bound");
    assert!(policy.due(0, 0, 60_000_000_000), "the age bound");
}

/// A published object carries the key and the digest a derivation is idempotent
/// on, and the digest is of the bytes that landed.
#[test]
fn a_published_object_carries_its_key_and_its_digest() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let segment = dir.path().join("open.dzus");
    let object = write(&[(0, 1_700_000_000_000_000_000, b"one".to_vec())]);
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
            message_count: 1,
            object_key: String::new(),
            sha256: String::new(),
            byte_count: 0,
        },
        Compression::None,
    )
    .expect("the object publishes");

    // The Hive-partitioned key the datagram archive already produces, with this
    // shape's own extension on the end.
    assert_eq!(
        published.manifest.object_key,
        "feed=top-of-book/env=test/site=site-1/recorder=recorder-1/\
         date=2023-11-14/hour=22/1700000000000000000-1700000000000000000-3.dzus"
    );
    assert_eq!(published.manifest.byte_count, object.len() as u64);
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
    // Of the bytes that landed, so a reader can check the object it fetched.
    assert_eq!(
        std::fs::read(&published.path).expect("the object landed"),
        object
    );

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
