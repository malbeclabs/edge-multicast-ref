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
use dz_recorder_rows::{
    derive_object, BookTop, Datagram, DropScope, Era, Nanos, PortRoleLabel, RecvTsKindLabel,
    RoleJoinRow, RowBatch, SegmentCoverage, SequenceGap, UncertainReason, Verdict,
};

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

/// Midday today, in nanoseconds.
///
/// Two properties the cross-site fixtures need and neither [`NOW`] nor
/// [`now_ns`] has. It is *recent*, so a row-level TTL cannot delete a fixture in
/// the same step that inserts it — the trap `002` documents and the reason
/// `Scratch` does not apply that file. And it is nowhere near a day boundary,
/// which matters because the cross-site verdict's census of which sites were
/// reporting is keyed on `toYYYYMMDD(before_ts)`, `sequence_gap`'s own partition
/// key. A fixture anchored to *now* would put its rows either side of midnight
/// for one minute in every 1,440, and a suite that fails once a day is worse
/// than no suite at all.
#[must_use]
pub fn midday_ns() -> u64 {
    const DAY: u64 = 86_400 * SECOND_NS;
    (now_ns() / DAY) * DAY + 12 * 3_600 * SECOND_NS
}

/// The cross-site case, carried in the `Channel ID`.
///
/// One channel instance per case, so the cases cannot contaminate one another
/// through a shared sort key and a failing assertion names its case rather than
/// a row number.
pub const PRESENT_AT_ANOTHER_SITE: u8 = 1;
pub const ABSENT_EVERYWHERE: u8 = 2;
pub const ABSENT_BUT_A_SITE_OVERFLOWED: u8 = 3;
pub const A_SITE_IS_UP_AND_SILENT: u8 = 4;
pub const NOBODY_ELSE_HAS_LOADED: u8 = 5;
pub const ONLY_A_CO_LOCATED_RECORDER: u8 = 6;
pub const OUR_OWN_SCOPE_CANNOT_SUBTRACT: u8 = 7;

/// The sequence numbers our site is missing, in every case.
pub const MISSING_FROM: u64 = 103;
pub const MISSING_TO: u64 = 105;

const SOURCE: std::net::Ipv4Addr = std::net::Ipv4Addr::new(192, 0, 2, 10);

/// One segment of one site, as its manifest states it.
///
/// The pair of them is what makes a cumulative counter readable: a segment and
/// the one before it, so `capture_drop_total` becomes a delta. A recorder that
/// dropped a burst an hour ago carries that burst in its total for ever, and a
/// rule reading the total would find no site admissible on any host that had
/// ever overflowed.
#[derive(Debug, Clone, Copy)]
pub struct Segment {
    pub segment_seq: u64,
    pub start_ts: u64,
    pub end_ts: u64,
    pub first_seq: u64,
    pub last_seq: u64,
    pub capture_drop_total: u64,
}

impl Segment {
    /// The segment the gap is in: sequence 100 to 110, either side of `base`.
    #[must_use]
    pub fn window(base: u64, capture_drop_total: u64) -> Self {
        Self {
            segment_seq: 5,
            start_ts: base - 60 * SECOND_NS,
            end_ts: base + 60 * SECOND_NS,
            first_seq: 100,
            last_seq: 110,
            capture_drop_total,
        }
    }

    /// The one before it, which is what turns the counter into a delta.
    #[must_use]
    pub fn preceding(base: u64, capture_drop_total: u64) -> Self {
        Self {
            segment_seq: 4,
            start_ts: base - 120 * SECOND_NS,
            end_ts: base - 60 * SECOND_NS,
            first_seq: 50,
            last_seq: 99,
            capture_drop_total,
        }
    }
}

/// A vantage, written `site` for the site's own recorder and `site/recorder`
/// for a second one beside it.
///
/// Two recorders at one site are two vantages and the site is never folded away
/// — and the difference between the two is exactly what
/// `ONLY_A_CO_LOCATED_RECORDER` is about, because a box in our own rack shares
/// our switch, our uplink and our load.
fn identity(vantage: &str) -> (String, String) {
    vantage.split_once('/').map_or_else(
        || (vantage.to_owned(), format!("recorder-{vantage}")),
        |(site, recorder)| (site.to_owned(), recorder.to_owned()),
    )
}

/// One coverage row, as a site's own manifest produced it.
#[must_use]
pub fn coverage(site: &str, channel: u8, segment: Segment) -> SegmentCoverage {
    let (site, recorder) = identity(site);
    SegmentCoverage {
        site,
        recorder,
        env: "test".to_owned(),
        feed: "feed".to_owned(),
        source_addr: SOURCE,
        channel_id: channel,
        dst_port: port_for(PortRole::Mktdata),
        segment_seq: segment.segment_seq,
        start_ts: Nanos(segment.start_ts),
        end_ts: Nanos(segment.end_ts),
        first_seq: segment.first_seq,
        last_seq: segment.last_seq,
        datagram_count: segment.last_seq - segment.first_seq + 1,
        reset_counts_seen: vec![0],
        capture_drop_total: segment.capture_drop_total,
        interface_drop_total: 0,
        drop_scope: DropScope::PortRole,
        roles_joined: vec![RoleJoinRow(
            "mktdata".to_owned(),
            GROUP,
            port_for(PortRole::Mktdata),
        )],
        object_key: format!("{}", segment.segment_seq),
        object_sha256: "sha".to_owned(),
        build_version: "0.1.0".to_owned(),
        build_commit: "0000000".to_owned(),
        config_hash: "a".repeat(64),
    }
}

/// One gap row, exactly as a site's own loader would have written it: the
/// verdict `unverifiable`, `seen_elsewhere` absent, and no send stamps.
///
/// `unexplained` is the residue, and it carries the whole of the admissibility
/// question a gap row answers on its own: `None` is the loader saying that no
/// per-instance subtraction was valid at its declared scope, which is a site
/// that cannot say what it lost and therefore cannot say what a publisher lost.
#[must_use]
pub fn gap(
    site: &str,
    channel: u8,
    base: u64,
    era_anchor: u64,
    unexplained: Option<u64>,
) -> SequenceGap {
    let (site, recorder) = identity(site);
    SequenceGap {
        site,
        recorder,
        env: "test".to_owned(),
        feed: "feed".to_owned(),
        port_role: PortRoleLabel::Mktdata,
        group_addr: GROUP,
        source_addr: SOURCE,
        channel_id: channel,
        dst_port: port_for(PortRole::Mktdata),
        reset_count: 0,
        era_index: 1,
        era_anchor_ts: Nanos(era_anchor),
        anchor_certain: 1,
        missing_from: MISSING_FROM,
        missing_to: MISSING_TO,
        missing_count: MISSING_TO - MISSING_FROM + 1,
        reference_seqs: 11,
        before_ts: Nanos(base),
        after_ts: Nanos(base + SECOND_NS),
        sent_from_ts: None,
        sent_to_ts: None,
        admitted_recorder: 0,
        // The scope follows the residue rather than being a third parameter,
        // because the two are one fact: a null residue is what the loader
        // writes when the archive declared capture-handle scope and the handle
        // admitted something, since a ring counts frames dropped before
        // demultiplexing and the number then belongs to no port role at all.
        admitted_scope: if unexplained.is_some() {
            DropScope::PortRole
        } else {
            DropScope::CaptureHandle
        },
        unexplained_count: unexplained,
        interface_drops: Some(0),
        seen_elsewhere: None,
        on_redundant_path: None,
        verdict: Verdict::Unverifiable,
        object_key: "5".to_owned(),
    }
}

/// One datagram row, at a site that received what we did not.
#[must_use]
pub fn datagram(site: &str, channel: u8, sequence_number: u64, recv_ts: u64) -> Datagram {
    let (site, recorder) = identity(site);
    Datagram {
        recv_ts: Nanos(recv_ts),
        send_ts: Nanos(recv_ts - 500_000),
        recv_ts_kind: RecvTsKindLabel::KernelSoftware,
        source_addr: SOURCE,
        channel_id: channel,
        dst_port: port_for(PortRole::Mktdata),
        feed: "feed".to_owned(),
        port_role: PortRoleLabel::Mktdata,
        group_addr: GROUP,
        sequence_number,
        reset_count: 0,
        segment_seq: 5,
        payload_len: 100,
        wire_payload_len: 100,
        drop_delta: 0,
        site,
        recorder,
        env: "test".to_owned(),
        drop_scope: DropScope::PortRole,
        object_key: "5".to_owned(),
        object_sha256: "sha".to_owned(),
    }
}

/// The five cross-site cases, as the objects several loaders would have written.
///
/// One batch per (site, segment), because that is what an object is: the
/// idempotence test re-writes these same batches, and a fixture folding a site's
/// two segments into one object could not tell a re-load of one from a re-load
/// of both.
///
/// Our site is `one`, and it has the same gap — sequence 103 to 105 — in every
/// case. What differs is what the other sites wrote:
///
/// * `PRESENT_AT_ANOTHER_SITE` — `two` covered the range and recorded no gap
///   over it, so it held the datagrams. Not a publisher gap.
/// * `ABSENT_EVERYWHERE` — `two` covered the range, missed the same three, and
///   its two segments carry the same cumulative total, so it overflowed
///   nothing. The finding.
/// * `ABSENT_BUT_A_SITE_OVERFLOWED` — the same, except that `two`'s window
///   segment admitted two drops its predecessor had not. Its absence may be its
///   own ring, and an absence that may be somebody's own ring is no evidence
///   about a publisher.
/// * `A_SITE_IS_UP_AND_SILENT` — `three` covered the range and missed it
///   admissibly, which is everything the finding above needed; and `two` has
///   coverage of this instance earlier the same day and none over the window. A
///   site that is not reporting is not a site that reported nothing.
/// * `NOBODY_ELSE_HAS_LOADED` — our rows, and nobody else's.
/// * `ONLY_A_CO_LOCATED_RECORDER` — a second recorder at our own site covered
///   the range and missed the same three, admissibly in every respect but the
///   one that matters. It is not another site.
/// * `OUR_OWN_SCOPE_CANNOT_SUBTRACT` — `two` is absent and admissible, so the
///   answer is *known*; but our own ring overflowed at capture-handle scope, so
///   we cannot say how much of our own gap is ours. Known absent elsewhere and
///   still not a finding, which is the case that says the escalation is a
///   conjunction and not a rename of `seen_elsewhere`.
#[must_use]
pub fn cross_site_fixture(base: u64) -> Vec<RowBatch> {
    let ours = base - 30 * SECOND_NS;
    let theirs = base - 25 * SECOND_NS;
    let every_case = [
        PRESENT_AT_ANOTHER_SITE,
        ABSENT_EVERYWHERE,
        ABSENT_BUT_A_SITE_OVERFLOWED,
        A_SITE_IS_UP_AND_SILENT,
        NOBODY_ELSE_HAS_LOADED,
        ONLY_A_CO_LOCATED_RECORDER,
        OUR_OWN_SCOPE_CANNOT_SUBTRACT,
    ];
    let mut batches = Vec::new();
    let mut object = |site: &str, segment: Segment, batch: RowBatch| {
        batches.push(RowBatch {
            object_key: format!("{site}/{}", segment.segment_seq),
            object_sha256: format!("sha-{site}-{}", segment.segment_seq),
            ..batch
        });
    };

    // Ours: the same gap in every case, and a predecessor segment so that our
    // own counter is a delta too.
    let preceding = Segment::preceding(base, 0);
    let window = Segment::window(base, 0);
    object(
        "one",
        preceding,
        RowBatch {
            segment_coverage: every_case
                .iter()
                .map(|case| coverage("one", *case, preceding))
                .collect(),
            ..RowBatch::default()
        },
    );
    // Our own window segment admitted five drops in the one case that is about
    // our own ring, and nothing in the others. The cross-site views never read
    // our own overflow — it is our *residue* they read — but a fixture whose
    // manifest contradicted its own gap row would describe a recorder that
    // cannot exist.
    let our_overflowing_window = Segment::window(base, 5);
    object(
        "one",
        window,
        RowBatch {
            segment_coverage: every_case
                .iter()
                .map(|case| {
                    if *case == OUR_OWN_SCOPE_CANNOT_SUBTRACT {
                        coverage("one", *case, our_overflowing_window)
                    } else {
                        coverage("one", *case, window)
                    }
                })
                .collect(),
            sequence_gap: every_case
                .iter()
                .map(|case| {
                    let residue = if *case == OUR_OWN_SCOPE_CANNOT_SUBTRACT {
                        None
                    } else {
                        Some(3)
                    };
                    gap("one", *case, base, ours, residue)
                })
                .collect(),
            ..RowBatch::default()
        },
    );

    // `two`, in the four cases it appears in. Seven admitted drops carried
    // forward from before any of this: a total that is not zero and a delta that
    // is, which is the distinction the admissibility test has to make.
    let their_preceding = Segment::preceding(base, 7);
    let their_window = Segment::window(base, 7);
    // Two drops the predecessor had not admitted, in the one case that is about
    // overflow.
    let their_overflowing_window = Segment::window(base, 9);
    object(
        "two",
        their_preceding,
        RowBatch {
            segment_coverage: [
                PRESENT_AT_ANOTHER_SITE,
                ABSENT_EVERYWHERE,
                ABSENT_BUT_A_SITE_OVERFLOWED,
                A_SITE_IS_UP_AND_SILENT,
                OUR_OWN_SCOPE_CANNOT_SUBTRACT,
            ]
            .iter()
            .map(|case| coverage("two", *case, their_preceding))
            .collect(),
            ..RowBatch::default()
        },
    );
    object(
        "two",
        their_window,
        RowBatch {
            segment_coverage: vec![
                coverage("two", PRESENT_AT_ANOTHER_SITE, their_window),
                coverage("two", ABSENT_EVERYWHERE, their_window),
                coverage(
                    "two",
                    ABSENT_BUT_A_SITE_OVERFLOWED,
                    their_overflowing_window,
                ),
                coverage("two", OUR_OWN_SCOPE_CANNOT_SUBTRACT, their_window),
            ],
            sequence_gap: vec![
                gap("two", ABSENT_EVERYWHERE, base, theirs, Some(3)),
                gap("two", ABSENT_BUT_A_SITE_OVERFLOWED, base, theirs, Some(3)),
                gap("two", OUR_OWN_SCOPE_CANNOT_SUBTRACT, base, theirs, Some(3)),
            ],
            // The datagrams `two` received and we did not, a millisecond apart,
            // which is what makes the first case not a publisher gap.
            datagram: (MISSING_FROM..=MISSING_TO)
                .map(|seq| {
                    datagram(
                        "two",
                        PRESENT_AT_ANOTHER_SITE,
                        seq,
                        base + (seq - MISSING_FROM + 1) * 1_000_000,
                    )
                })
                .collect(),
            ..RowBatch::default()
        },
    );

    // `three`, which speaks admissibly in the case `two` is silent in — so that
    // the silence is the only thing standing between that case and a finding.
    object(
        "three",
        their_preceding,
        RowBatch {
            segment_coverage: vec![coverage("three", A_SITE_IS_UP_AND_SILENT, their_preceding)],
            ..RowBatch::default()
        },
    );
    object(
        "three",
        their_window,
        RowBatch {
            segment_coverage: vec![coverage("three", A_SITE_IS_UP_AND_SILENT, their_window)],
            sequence_gap: vec![gap("three", A_SITE_IS_UP_AND_SILENT, base, theirs, Some(3))],
            ..RowBatch::default()
        },
    );

    // A second recorder in our own rack, which missed the same three. What it
    // held would have been conclusive; what it missed is not a second opinion
    // about a publisher, and treating it as one would let a rack's shared
    // uplink accuse the feed.
    let beside_us = "one/recorder-one-b";
    object(
        beside_us,
        their_preceding,
        RowBatch {
            segment_coverage: vec![coverage(
                beside_us,
                ONLY_A_CO_LOCATED_RECORDER,
                their_preceding,
            )],
            ..RowBatch::default()
        },
    );
    object(
        beside_us,
        their_window,
        RowBatch {
            segment_coverage: vec![coverage(
                beside_us,
                ONLY_A_CO_LOCATED_RECORDER,
                their_window,
            )],
            sequence_gap: vec![gap(
                beside_us,
                ONLY_A_CO_LOCATED_RECORDER,
                base,
                theirs,
                Some(3),
            )],
            ..RowBatch::default()
        },
    );

    batches
}
