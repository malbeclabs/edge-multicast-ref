//! A transport that answers whatever a test chose, and a real row batch to send
//! through it.
//!
//! No server anywhere. The rows come from the real derivation over a real
//! archive written by the real writer, so what these tests assert about a
//! request body is what the loader actually sends.
#![allow(dead_code)]
#![forbid(unsafe_code)]

use std::cell::RefCell;
use std::time::Duration;

use dz_edge_core::PortRole;
use dz_recorder_archive::rotate::{ArchiveWriter, ArchiveWriterConfig};
use dz_recorder_archive::writer::{LinkHeaders, RoleJoin};
use dz_recorder_archive::Compression;
use dz_recorder_clickhouse::{ClickHouseConfig, Credentials, Response, Transport, TransportError};
use dz_recorder_core::{CaptureDropScope, RecorderIdentity};
use dz_recorder_replay::synthetic::{port_for, SyntheticPublisher, GROUP};
use dz_recorder_replay::Fault;
use dz_recorder_rows::{derive_object, BookTop, Era, Nanos, RowBatch, UncertainReason};

/// One request, as the sink issued it.
#[derive(Debug, Clone)]
pub struct Sent {
    pub url: String,
    pub body: String,
    pub user: String,
    pub has_password: bool,
}

impl Sent {
    /// The table this request inserts into, read out of the statement.
    #[must_use]
    pub fn table(&self) -> String {
        let at = self
            .url
            .find("INSERT%20INTO%20")
            .expect("every insert carries its statement");
        let rest = &self.url[at + "INSERT%20INTO%20".len()..];
        let end = rest.find("%20").expect("the statement continues");
        rest[..end].to_owned()
    }

    #[must_use]
    pub fn rows(&self) -> Vec<serde_json::Value> {
        self.body
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str(l).expect("every line is one JSON object"))
            .collect()
    }
}

/// Answers a scripted sequence, and records everything it was asked.
#[derive(Debug, Default)]
pub struct FakeTransport {
    sent: RefCell<Vec<Sent>>,
    /// Answers, consumed in order. An exhausted script answers 200, so a test
    /// only scripts the failures it is about.
    answers: RefCell<Vec<Answer>>,
}

#[derive(Debug, Clone)]
pub enum Answer {
    Ok,
    Unreachable,
    Refused(u16),
}

impl FakeTransport {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn answering(answers: Vec<Answer>) -> Self {
        Self {
            sent: RefCell::new(Vec::new()),
            answers: RefCell::new(answers),
        }
    }

    #[must_use]
    pub fn sent(&self) -> Vec<Sent> {
        self.sent.borrow().clone()
    }

    #[must_use]
    pub fn requests(&self) -> usize {
        self.sent.borrow().len()
    }
}

impl Transport for FakeTransport {
    fn post(
        &self,
        url: &str,
        credentials: &Credentials,
        body: &[u8],
    ) -> Result<Response, TransportError> {
        self.sent.borrow_mut().push(Sent {
            url: url.to_owned(),
            body: String::from_utf8_lossy(body).into_owned(),
            user: credentials.user.clone(),
            has_password: credentials.is_authenticated(),
        });
        let answer = {
            let mut answers = self.answers.borrow_mut();
            if answers.is_empty() {
                Answer::Ok
            } else {
                answers.remove(0)
            }
        };
        match answer {
            Answer::Ok => Ok(Response {
                status: 200,
                body: String::new(),
            }),
            Answer::Unreachable => Err(TransportError::Unreachable {
                url: url.to_owned(),
                message: "connection refused".to_owned(),
            }),
            Answer::Refused(status) => Err(TransportError::Refused {
                url: url.to_owned(),
                status,
                body: "Code 47. DB::Exception: Missing columns".to_owned(),
            }),
        }
    }
}

/// A configuration pointing at a documentation address, which nothing here
/// contacts.
/// A configuration that posts every batch as it arrives.
///
/// `insert_min_rows = 1`, so a test about what one request looks like is not
/// also a test of the coalescing. The tests that *are* about coalescing set the
/// bounds themselves.
#[must_use]
pub fn config() -> ClickHouseConfig {
    ClickHouseConfig {
        endpoint: "http://192.0.2.20:8123".to_owned(),
        database: "recorder".to_owned(),
        user: "loader".to_owned(),
        insert_min_rows: 1,
        ..ClickHouseConfig::default()
    }
}

/// One instant, for the tests that are not about time.
pub const NOW: u64 = 1_700_000_000_000_000_000;

/// Nanoseconds in one second, for a test stating a delay.
pub const SECOND_NS: u64 = 1_000_000_000;

/// A real batch on one port role, so two of them have disjoint sort keys.
///
/// The destination port is part of the channel instance and part of every
/// table's sort key, so the same stream on two roles is two instances. That is
/// what lets a test insert several objects without the engine collapsing their
/// rows into one another — see `an_insert_block_is_collapsed_on_the_sort_key...`
/// for the case where they do overlap, and why.
#[must_use]
pub fn batch_on_role(datagrams: usize, role: PortRole) -> RowBatch {
    build(SyntheticPublisher::clean(datagrams).on_role(role), role)
}

/// A real batch: the real writer, the real archive, the real derivation.
#[must_use]
pub fn batch(datagrams: usize, fault: Fault) -> RowBatch {
    build(
        SyntheticPublisher::with_fault(datagrams, fault),
        PortRole::Mktdata,
    )
}

fn build(publisher: SyntheticPublisher, role: PortRole) -> RowBatch {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let cfg = ArchiveWriterConfig {
        staging_dir: dir.path().join("staging"),
        completed_dir: dir.path().join("completed"),
        rotate_bytes: 1 << 30,
        rotate_interval: Duration::from_secs(3600),
        staging_max: 1 << 40,
        compression: Compression::Zstd { level: 1 },
        identity: RecorderIdentity {
            site: "site-1".to_owned(),
            recorder: "recorder-1".to_owned(),
            env: "test".to_owned(),
            build_version: "0.1.0".to_owned(),
            build_commit: "0000000".to_owned(),
            config_hash: "a".repeat(64),
        },
        feed: "top-of-book".to_owned(),
        roles_joined: vec![RoleJoin::on(role, GROUP, port_for(role))],
        link_headers: LinkHeaders::Synthesised,
        capture_drop_scope: CaptureDropScope::PortRole,
    };
    let mut writer = ArchiveWriter::new(cfg, 0).expect("the archive opens");
    publisher
        .publish_into(&mut writer)
        .expect("the write path never fails the caller");
    writer
        .rotate_at(1_000_000_000)
        .expect("rotation")
        .expect("a segment that held datagrams produces an object");
    let landed = writer
        .wait_completed()
        .expect("the compressor publishes exactly one object")
        .expect("publication");

    derive_object(&landed.segment.path, &landed.manifest, None)
        .expect("the object derives")
        .rows
}

/// A wait that does not wait, so the retry tests are instant.
pub fn no_wait(_: Duration) {}

/// The equivalence key the fixtures below pair on.
///
/// A literal, and deliberately so: what is under test here is the pairing, and
/// the pairing knows nothing about how a key was computed — only that two rows
/// carrying one value are two views of one book state. How the value is arrived
/// at, and the three ways that can fail, are `dz-recorder-events`' own tests.
pub const REPEATED: u64 = 7_777_777_777_777_777_777;
pub const ANOTHER: u64 = 1_234_567_890;

/// One top of book, as an observation point wrote it down.
///
/// The stamps are relative to *now* rather than to [`NOW`], which is the one
/// place in this file the clock is not a constant. `005` gives `book_top` a
/// thirty-day TTL and a row-level TTL is applied as the part is written, so a
/// fixture stamped years in the past — as every other fixture here deliberately
/// is — would be deleted in the same step that inserted it, with the insert
/// answered `200` and every count below coming back zero. A race is about the
/// difference between two arrivals, so the instant they are measured from is
/// free.
pub fn top(observation: &str, site: &str, base: u64, offset_ms: u64, state_key: u64) -> BookTop {
    BookTop {
        recv_ts: Nanos(base + offset_ms * 1_000_000),
        send_ts: Nanos(base + offset_ms * 1_000_000 - 1_000_000),
        site: site.to_owned(),
        recorder: format!("recorder-{site}"),
        env: "test".to_owned(),
        feed: "feed".to_owned(),
        observation: observation.to_owned(),
        source_addr: std::net::Ipv4Addr::new(192, 0, 2, 10),
        channel_id: 1,
        dst_port: 40_000,
        source_id: 1_000,
        instrument_id: 11,
        symbol: "AAA".to_owned(),
        sequence_number: 1_000 + offset_ms,
        message_index: 0,
        reset_count: 0,
        segment_seq: 3,
        bid_px_raw: Some(9_950),
        bid_qty_raw: Some(12),
        bid_source_count: Some(2),
        ask_px_raw: Some(10_050),
        ask_qty_raw: Some(7),
        ask_source_count: Some(3),
        price_exp: -2,
        qty_exp: 0,
        state_key,
        from_anchor: 0,
        book_certain: 1,
        uncertain_since: None,
        uncertain_reason: UncertainReason::None,
        object_key: "object".to_owned(),
    }
}

/// The era each observation point opened, so the ordinal has one to number
/// within.
///
/// The two points open theirs at two instants, which is not decoration: an era's
/// stored identity is its anchor and an anchor is a *receive* stamp, so two
/// recorders of one feed have two of them and two transports share none at all.
/// A fixture that gave both points one anchor would let a pairing grouped on the
/// era pass, and that pairing finds nothing in the field.
pub fn opening(site: &str, base: u64) -> Era {
    Era {
        site: site.to_owned(),
        recorder: format!("recorder-{site}"),
        feed: "feed".to_owned(),
        source_addr: std::net::Ipv4Addr::new(192, 0, 2, 10),
        channel_id: 1,
        dst_port: 40_000,
        anchor_ts: Nanos(base),
        anchor_seq: 1,
        reset_count: 0,
        segment_seq: 3,
        anchor_certain: 1,
        continuation: 0,
        object_key: "object".to_owned(),
        object_sha256: "sha".to_owned(),
    }
}

/// A state seen three times at each of two observation points, a fourth time at
/// only one of them, and a snapshot-derived row that is no observation at all.
pub fn race_fixture(base: u64) -> RowBatch {
    let mut book_top = Vec::new();
    for offset in [10, 30, 50] {
        book_top.push(top("a", "one", base, offset, REPEATED));
        // Two milliseconds behind, every time. The lead is what a race
        // measures, and a fixture where it is constant makes a wrong pairing
        // arithmetically visible rather than merely different.
        book_top.push(top("b", "two", base, offset + 2, REPEATED));
    }
    // The occurrence only one observation point saw.
    book_top.push(top("a", "one", base, 70, REPEATED));
    // A snapshot-derived top, earlier than anything else at `b`. If it took an
    // ordinal it would take the *first* one, shifting every later occurrence at
    // `b` by one — and the unpaired row above would then pair with it and
    // disappear.
    let mut anchored = top("b", "two", base, 5, REPEATED);
    anchored.from_anchor = 1;
    book_top.push(anchored);
    // A different state, once at each point, so the fixture is not one key.
    book_top.push(top("a", "one", base, 90, ANOTHER));
    book_top.push(top("b", "two", base, 93, ANOTHER));

    RowBatch {
        object_key: "object".to_owned(),
        object_sha256: "sha".to_owned(),
        era: vec![
            opening("one", base - 5_000_000),
            opening("two", base - 3_000_000),
        ],
        book_top,
        ..RowBatch::default()
    }
}

pub fn now_ns() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock after the epoch")
            .as_nanos(),
    )
    .expect("nanoseconds since the epoch fit a u64")
}
