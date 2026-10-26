//! A strike, from an adapter's statement to the specification's own rule set.
//!
//! `dz-publisher-runtime`'s `strikes_end_to_end.rs` holds what reaches the
//! wire against this repository's reading of the specification, and a
//! publisher and a test that share a misreading pass each other. The five
//! `STRIKE.*` rules are the specification's reading, in the specification's
//! repository, so this suite hands them what the runtime sent.
//!
//! # The feed is the runtime's, and no fixture's
//!
//! Every other suite in this crate states its datagrams message by message
//! through the codec. A `StrikeInterval` cannot be stated that way and mean
//! anything: where it stands, what it repeats and what it does to
//! `Manifest Seq` are decisions of the reference-data registry and of the
//! egress packer, so a feed assembled here would be judged for what this
//! file wrote. The publisher is therefore composed as a venue composes it,
//! over the recording sinks of the runtime's own test harness, and driven the
//! way `dz_publisher_runtime::run` drives one: a listings poll, the periodic
//! snapshot and a tick, once a second of a clock this test sets.
//!
//! One publisher emits both feeds, and each feed's datagrams go into an
//! archive of their own, as a recorder joined to that feed's ports would have
//! kept them. From there the path is the one every conformance test here
//! takes: the archive, replay, the bridge's segment, the tool.
//!
//! # A capture with no `0x09` passes all five rules
//!
//! So the capture is read before the tool is believed. The first test below
//! needs no tool and runs in the default build: it holds that a strike follows
//! every definition of each strike instrument and none follows the plain
//! one's, that the two instruments whose venue fixes the strike late show
//! pending and then fixed and nothing else, and that `Manifest Seq` is one
//! value from the first datagram to the last. The two tests behind the
//! `conformance` feature then ask the tool, and hold its own count of how
//! often each continuity rule looked.

#![forbid(unsafe_code)]

mod common;
// The runtime's own harness, included rather than copied: a second way to
// compose a publisher over recorders would be a second thing to keep in step
// with `Publisher::new`.
#[path = "../../../publisher/dz-publisher-runtime/tests/harness/mod.rs"]
mod harness;

use std::net::SocketAddrV4;
use std::time::Duration;

use common::{record_joined_as, replay, Recorded, EGRESS_PORT, JOIN_INTERFACE, JOIN_SOURCE};
use dz_adapter_core::{
    Adapter, AdapterError, DepthBound, EventSink, InstrumentRef, ListingSink, ParseError, Payload,
    Scalar, Side, SnapshotSink, Strike, StrikeBound,
};
use dz_edge_core::{Datagram, Feed, PortRole, SCHEMA_VERSION};
use dz_edge_mbp::MarketByPrice;
use dz_edge_refdata::{InstrumentDefinition, ManifestSummary, StrikeInterval};
use dz_edge_tob::TopOfBook;
use dz_recorder_archive::writer::RoleJoin;
use dz_recorder_core::{RecvTsKind, Sink};
use dz_recorder_replay::OwnedDatagram;
use harness::{Arrive, Harness, Recorder};

const DEFINITION: u8 = 0x02;
const MANIFEST: u8 = 0x07;
const STRIKE: u8 = 0x09;

const LOWER_PRESENT: u8 = 0b0001;
const LOWER_INCLUSIVE: u8 = 0b0010;
const UPPER_PRESENT: u8 = 0b0100;

/// Pays at or above a value the venue fixes after listing: a lower bound, and
/// a settlement value may equal it.
const ABOVE: &str = "ABOVE";
/// Pays inside a range the venue fixes after listing: both bounds, the lower
/// one inclusive and the upper one not.
const BETWEEN: &str = "BETWEEN";
/// Pays below a value the venue stated when it listed the contract.
const BELOW: &str = "BELOW";
/// Pays on no strike. The venue never states one.
const PLAIN: &str = "PLAIN";
const SYMBOLS: [&str; 4] = [ABOVE, BETWEEN, BELOW, PLAIN];

/// When the venue says it fixed `ABOVE`'s strike. `BETWEEN`'s venue gives no
/// time, which is the other thing a venue can say.
const FIXING_TIME_NS: u64 = 1_700_000_090_000_000_000;

/// One lap of the definition cycle the harness configures, in seconds.
const LAP: u64 = 30;
/// Whole laps on each side of the fix, so that each side is the cycle coming
/// round to an instrument several times and not one transmission of it.
const LAPS_EACH_SIDE: u64 = 3;
/// How often the venue sends a quote for every instrument. Inside the
/// harness's idle-guard window, so the publisher is never one a guard would
/// have ended.
const QUOTE_EVERY: u64 = 5;

/// A venue with four contracts, two of whose strikes it fixes after listing.
struct Venue {
    fixed: bool,
    /// The handles the runtime admitted, in [`SYMBOLS`] order.
    handles: Vec<InstrumentRef>,
    /// Whether the book has bootstrapped. Before it has, a snapshot is refused
    /// as `NotReady`, which is the documented answer and costs one slot.
    book: bool,
}

impl Venue {
    fn strike(&self, symbol: &str) -> Option<Strike<'static>> {
        match symbol {
            ABOVE if self.fixed => Some(Strike::Fixed {
                lower: Some(StrikeBound::inclusive(Scalar::text("82506.23"))),
                upper: None,
                fixing_time_ns: Some(FIXING_TIME_NS),
            }),
            BETWEEN if self.fixed => Some(Strike::Fixed {
                lower: Some(StrikeBound::inclusive(Scalar::text("82000.5"))),
                upper: Some(StrikeBound::exclusive(Scalar::text("82500"))),
                fixing_time_ns: None,
            }),
            ABOVE | BETWEEN => Some(Strike::Pending),
            BELOW => Some(Strike::Fixed {
                lower: None,
                upper: Some(StrikeBound::exclusive(Scalar::text("0.75"))),
                fixing_time_ns: None,
            }),
            _ => None,
        }
    }
}

impl Adapter for Venue {
    fn message_types(&self) -> &[&'static str] {
        &["quote", "level"]
    }

    fn poll_listings(&mut self, out: &mut dyn ListingSink) {
        self.handles.clear();
        for symbol in SYMBOLS {
            let handle = out
                .list(&harness::spec(symbol))
                .expect("four instruments are inside the selection policy");
            if let Some(strike) = self.strike(symbol) {
                out.strike(handle, &strike);
            }
            self.handles.push(handle);
        }
    }

    fn on_payload(
        &mut self,
        _payload: &Payload<'_>,
        _out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        Ok(())
    }

    fn snapshot(
        &self,
        _instrument: InstrumentRef,
        out: &mut dyn SnapshotSink,
    ) -> Result<DepthBound, AdapterError> {
        if !self.book {
            return Err(AdapterError::NotReady {
                detail: "the book has not bootstrapped",
            });
        }
        // The one level `harness::bid_level` rests, so the book a subscriber
        // builds from the live path is the book this snapshot states.
        out.level(
            Side::Bid,
            Scalar::text("100.25"),
            Scalar::text("2.500"),
            None,
        );
        Ok(DepthBound::Complete)
    }
}

/// One port role of one feed: where the runtime sends it and what it sent.
struct Tap {
    role: PortRole,
    port: u16,
    recorder: Recorder,
    /// How many of the recorder's datagrams have already been taken.
    taken: usize,
}

/// What one feed's recorder host would have received.
struct FeedCapture {
    taps: Vec<Tap>,
    sent: Vec<OwnedDatagram>,
}

impl FeedCapture {
    fn new(taps: Vec<(PortRole, u16, Recorder)>) -> Self {
        Self {
            taps: taps
                .into_iter()
                .map(|(role, port, recorder)| Tap {
                    role,
                    port,
                    recorder,
                    taken: 0,
                })
                .collect(),
            sent: Vec::new(),
        }
    }

    /// Takes what the sinks were handed since the last call, as arrivals.
    fn drain(&mut self, recv_ts_ns: &mut u64) {
        for tap in &mut self.taps {
            let all = tap.recorder.datagrams();
            for payload in &all[tap.taken..] {
                let wire_payload_len = u32::try_from(payload.len()).expect("a datagram is small");
                self.sent.push(OwnedDatagram {
                    payload: payload.clone(),
                    src: SocketAddrV4::new(harness::SOURCE, EGRESS_PORT),
                    dst: SocketAddrV4::new(harness::GROUP, tap.port),
                    role: tap.role,
                    recv_ts_ns: *recv_ts_ns,
                    recv_ts_kind: RecvTsKind::KernelSoftware,
                    drop_delta: 0,
                    ttl: Some(4),
                    link_headers: None,
                    wire_payload_len,
                });
                *recv_ts_ns += 1_000;
            }
            tap.taken = all.len();
        }
    }

    fn joins(&self) -> Vec<RoleJoin> {
        self.taps
            .iter()
            .map(|tap| RoleJoin {
                role: tap.role,
                group: harness::GROUP,
                port: tap.port,
                interface: Some(JOIN_INTERFACE.to_owned()),
                source: Some(JOIN_SOURCE),
            })
            .collect()
    }

    fn port(&self, role: PortRole) -> u16 {
        self.taps
            .iter()
            .find(|tap| tap.role == role)
            .expect("the feed carries this port role")
            .port
    }

    /// Through the real archive writer, as every suite here records.
    fn record(&self, feed: &str) -> Recorded {
        record_joined_as(self.joins(), feed, |writer| {
            for dg in &self.sent {
                Sink::write(writer, &dg.as_recorded())
                    .expect("the write path never fails the caller");
            }
            self.sent.len() as u64
        })
    }
}

/// Both feeds of one publisher, captured as it runs.
struct Capture {
    tob: FeedCapture,
    mbp: FeedCapture,
    /// The arrival stamp of the next datagram. Strictly increasing, and taken
    /// from the publisher's own clock at each second so that the capture's
    /// time is the time the cadences were paced against.
    recv_ts_ns: u64,
}

impl Capture {
    fn of(h: &Harness) -> Self {
        let tob = h.tob.as_ref().expect("a top-of-book feed");
        let mbp = h.mbp.as_ref().expect("a market-by-price feed");
        Self {
            tob: FeedCapture::new(vec![
                (
                    PortRole::Mktdata,
                    harness::MKTDATA_PORT,
                    tob.mktdata.clone(),
                ),
                (
                    PortRole::Refdata,
                    harness::REFDATA_PORT,
                    tob.refdata.clone(),
                ),
            ]),
            mbp: FeedCapture::new(vec![
                (
                    PortRole::Mktdata,
                    harness::DEPTH_MKTDATA_PORT,
                    mbp.mktdata.clone(),
                ),
                (
                    PortRole::Refdata,
                    harness::DEPTH_REFDATA_PORT,
                    mbp.refdata.clone(),
                ),
                (
                    PortRole::Snapshot,
                    harness::DEPTH_SNAPSHOT_PORT,
                    mbp.snapshot.clone().expect("a depth feed's snapshot port"),
                ),
            ]),
            recv_ts_ns: 0,
        }
    }

    /// Called after every call into the publisher, so that the order of two
    /// datagrams on two ports is the order the publisher sent them in whenever
    /// two calls sent them. Within one call the roles are taken in turn, which
    /// is an order a capture off two sockets would not promise either.
    fn drain(&mut self, h: &Harness) {
        let now = dz_publisher_refdata::Clock::unix_ns(&h.clock);
        self.recv_ts_ns = self.recv_ts_ns.max(now);
        self.tob.drain(&mut self.recv_ts_ns);
        self.mbp.drain(&mut self.recv_ts_ns);
    }
}

/// The two archives of one run.
struct Run {
    tob: Recorded,
    mbp: Recorded,
    tob_capture: FeedCapture,
    mbp_capture: FeedCapture,
}

/// Runs the publisher for three laps of the definition cycle, has the venue
/// fix two strikes, and runs it for three more.
fn run() -> Run {
    // A rotation, because a depth feed without one sends nothing on its
    // snapshot port and the rule set would be judging a port nobody used. Four
    // seconds over four instruments is one snapshot a second.
    let mut h = harness::harness_both_with_rotation(Duration::from_secs(4));
    let mut capture = Capture::of(&h);
    let mut venue = Venue {
        fixed: false,
        handles: Vec::new(),
        book: false,
    };

    h.publisher.declare_venue_timestamps(&venue);
    assert!(h.publisher.poll_listings(&mut venue));
    assert!(h.publisher.tick().is_none());
    capture.drain(&h);

    let fix_at = LAPS_EACH_SIDE * LAP;
    for second in 1..=2 * fix_at {
        h.clock.advance(Duration::from_secs(1));
        if second == fix_at + 1 {
            venue.fixed = true;
        }
        assert!(
            h.publisher.poll_listings(&mut venue),
            "the poll is due once a second"
        );
        capture.drain(&h);

        let source_ts_ns = dz_publisher_refdata::Clock::unix_ns(&h.clock);
        if second == 1 {
            // Each book bootstraps with one resting level, and from here the
            // venue has a book to state when it is asked for a snapshot.
            for handle in venue.handles.clone() {
                h.publisher.upstream_message("level");
                h.publisher.arrive(harness::bid_level(handle, source_ts_ns));
            }
            venue.book = true;
            capture.drain(&h);
        }
        if second % QUOTE_EVERY == 0 {
            for handle in venue.handles.clone() {
                h.publisher.upstream_message("quote");
                h.publisher.arrive(harness::quote(handle, source_ts_ns));
            }
            capture.drain(&h);
        }

        if let Some(Err(error)) = h.publisher.periodic_snapshot(&venue) {
            panic!("a periodic snapshot was refused: {error}");
        }
        capture.drain(&h);
        assert!(
            h.publisher.tick().is_none(),
            "a guard ended the publisher at second {second}, so what follows is not a \
             publisher's feed"
        );
        capture.drain(&h);
    }

    Run {
        tob: capture.tob.record(TopOfBook::NAME),
        mbp: capture.mbp.record(MarketByPrice::NAME),
        tob_capture: capture.tob,
        mbp_capture: capture.mbp,
    }
}

/// One thing a refdata port said, in the order it said it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Said {
    Manifest {
        manifest_seq: u16,
    },
    Definition {
        symbol: String,
        manifest_seq: u16,
        /// The `StrikeInterval` directly behind this definition in its
        /// datagram, if one stands there.
        strike: Option<StrikeInterval>,
    },
}

/// Everything the archive holds for one feed's refdata port, read back out of
/// the object and decoded against the feed's own `Magic`.
///
/// Out of the archive and not out of the harness's recorders, so that what is
/// asserted is what the tool is about to be handed.
fn refdata_said(archive: &Recorded, magic: u16, refdata_port: u16) -> Vec<Said> {
    let mut said = Vec::new();
    for dg in replay(&archive.object) {
        if dg.dst.port() != refdata_port {
            continue;
        }
        let decoded = Datagram::decode(&dg.payload, magic).expect("a datagram of this feed");
        let messages: Vec<(u8, &[u8])> = decoded.messages().map(|m| (m.type_id, m.bytes)).collect();
        for (index, (type_id, bytes)) in messages.iter().enumerate() {
            match *type_id {
                MANIFEST => {
                    let manifest = ManifestSummary::decode(bytes).expect("a manifest decodes");
                    assert_eq!(manifest.valid, 1, "the published set is established");
                    said.push(Said::Manifest {
                        manifest_seq: manifest.manifest_seq,
                    });
                }
                DEFINITION => {
                    let definition = InstrumentDefinition::decode(bytes, SCHEMA_VERSION)
                        .expect("a definition decodes");
                    let strike = messages
                        .get(index + 1)
                        .filter(|(next, _)| *next == STRIKE)
                        .map(|(_, bytes)| StrikeInterval::decode(bytes).expect("a strike decodes"));
                    if let Some(strike) = &strike {
                        assert_eq!(strike.instrument_id, definition.instrument_id);
                        assert_eq!(strike.source_id, definition.source_id);
                        assert_eq!(strike.defect(), None);
                    }
                    said.push(Said::Definition {
                        symbol: String::from_utf8_lossy(&definition.symbol)
                            .trim_end_matches(['\0', ' '])
                            .to_owned(),
                        manifest_seq: definition.manifest_seq,
                        strike,
                    });
                }
                STRIKE => assert!(
                    index > 0 && messages[index - 1].0 == DEFINITION,
                    "a StrikeInterval stands somewhere other than directly behind a definition"
                ),
                _ => {}
            }
        }
    }
    said
}

/// What followed each definition of one instrument, in order.
fn strikes_of(said: &[Said], symbol: &str) -> Vec<Option<StrikeInterval>> {
    said.iter()
        .filter_map(|s| match s {
            Said::Definition {
                symbol: named,
                strike,
                ..
            } if named == symbol => Some(*strike),
            _ => None,
        })
        .collect()
}

/// Holds the capture to everything the rule set would pass vacuously without.
fn assert_the_capture_shows_the_strikes(feed: &str, said: &[Said]) {
    let laps = usize::try_from(LAPS_EACH_SIDE).expect("a small count");

    // A pair for each strike instrument, on every definition of it.
    for symbol in [ABOVE, BETWEEN, BELOW] {
        let strikes = strikes_of(said, symbol);
        assert!(
            strikes.len() >= 2 * laps,
            "{feed}: {symbol} was defined {} times in {} laps",
            strikes.len(),
            2 * laps
        );
        assert!(
            strikes.iter().all(Option::is_some),
            "{feed}: a definition of {symbol} left without its StrikeInterval"
        );
    }

    // And none for the instrument that pays on no strike.
    let plain = strikes_of(said, PLAIN);
    assert!(
        plain.len() >= 2 * laps,
        "{feed}: {PLAIN} was barely defined"
    );
    assert!(
        plain.iter().all(Option::is_none),
        "{feed}: a StrikeInterval follows a definition of {PLAIN}, which pays on none"
    );

    // Pending, then fixed, and exactly that: one change, in one direction.
    let fixed_as = |symbol: &str| -> StrikeInterval {
        let strikes: Vec<StrikeInterval> = strikes_of(said, symbol).into_iter().flatten().collect();
        let pending = strikes.iter().take_while(|s| s.is_pending()).count();
        let fixed = &strikes[pending..];
        assert!(
            pending >= laps,
            "{feed}: {symbol} was pending on {pending} definitions before the fix"
        );
        assert!(
            fixed.len() >= laps,
            "{feed}: {symbol} was fixed on {} definitions after it",
            fixed.len()
        );
        assert!(
            fixed.iter().all(|s| !s.is_pending()),
            "{feed}: {symbol} went back to pending after it was fixed"
        );
        assert!(
            fixed.iter().all(|s| *s == fixed[0]),
            "{feed}: {symbol}'s fixed strike changed once fixed"
        );
        fixed[0]
    };

    let above = fixed_as(ABOVE);
    assert_eq!(
        (
            above.strike_exponent,
            above.bound_flags,
            above.lower_bound,
            above.upper_bound,
            above.fixing_time_ns
        ),
        (
            -2,
            LOWER_PRESENT | LOWER_INCLUSIVE,
            8_250_623,
            0,
            FIXING_TIME_NS
        ),
        "{feed}: at or above 82506.23"
    );
    let between = fixed_as(BETWEEN);
    assert_eq!(
        (
            between.strike_exponent,
            between.bound_flags,
            between.lower_bound,
            between.upper_bound,
            between.fixing_time_ns
        ),
        (
            -1,
            LOWER_PRESENT | LOWER_INCLUSIVE | UPPER_PRESENT,
            820_005,
            825_000,
            0
        ),
        "{feed}: at or above 82000.5 and below 82500"
    );

    // The one that was fixed at listing never was anything else.
    let below: Vec<StrikeInterval> = strikes_of(said, BELOW).into_iter().flatten().collect();
    assert!(
        below.iter().all(|s| *s == below[0]),
        "{feed}: {BELOW} changed"
    );
    assert_eq!(
        (
            below[0].strike_exponent,
            below[0].bound_flags,
            below[0].lower_bound,
            below[0].upper_bound,
            below[0].fixing_time_ns
        ),
        (-2, UPPER_PRESENT, 0, 75, 0),
        "{feed}: below 0.75"
    );

    // `Manifest Seq` is one value across the fix, on every summary and on
    // every definition's tag, and there are summaries on both sides of it.
    let fix = said
        .iter()
        .position(|s| {
            matches!(s, Said::Definition { symbol, strike: Some(strike), .. }
                if symbol == ABOVE && !strike.is_pending())
        })
        .expect("the fix is in the capture");
    let seqs = |said: &[Said]| -> (Vec<u16>, usize) {
        let mut distinct = Vec::new();
        let mut summaries = 0;
        for s in said {
            let seq = match s {
                Said::Manifest { manifest_seq } => {
                    summaries += 1;
                    *manifest_seq
                }
                Said::Definition { manifest_seq, .. } => *manifest_seq,
            };
            if !distinct.contains(&seq) {
                distinct.push(seq);
            }
        }
        (distinct, summaries)
    };
    let (before, summaries_before) = seqs(&said[..fix]);
    let (after, summaries_after) = seqs(&said[fix..]);
    assert!(
        summaries_before > 0 && summaries_after > 0,
        "{feed}: a ManifestSummary stands on each side of the fix"
    );
    assert_eq!(before.len(), 1, "{feed}: Manifest Seq moved before the fix");
    assert_ne!(before[0], 0, "{feed}: a serving publisher's Manifest Seq");
    assert_eq!(
        after, before,
        "{feed}: fixing a strike moved Manifest Seq, which says the published set changed"
    );
}

#[test]
fn the_capture_shows_every_strike_and_the_change_from_pending_to_fixed() {
    let run = run();
    for (feed, archive, capture, magic) in [
        ("tob", &run.tob, &run.tob_capture, TopOfBook::MAGIC),
        ("mbp", &run.mbp, &run.mbp_capture, MarketByPrice::MAGIC),
    ] {
        let said = refdata_said(archive, magic, capture.port(PortRole::Refdata));
        assert_the_capture_shows_the_strikes(feed, &said);
        // The other port roles carried traffic too, so the rule set is not
        // judging a refdata port in front of two silent ones.
        for tap in &capture.taps {
            assert!(
                tap.taken > 0,
                "{feed}: nothing was sent on the {} port",
                tap.role.as_str()
            );
        }
    }
}

#[cfg(feature = "conformance")]
mod by_the_rule_set {
    use super::*;
    use dz_recorder_conformance::tool::PortRoles;

    const STRIKE_RULES: [&str; 5] = [
        "STRIKE.FOLLOWS_DEFINITION",
        "STRIKE.FIELDS",
        "STRIKE.INTERVAL_NOT_EMPTY",
        "STRIKE.PRESENCE_STABLE",
        "STRIKE.FIXED_STAYS_FIXED",
    ];

    /// How many findings of one status a rule stated, by the tool's own report.
    fn count(report: &serde_json::Value, rule: &str, status: &str) -> u64 {
        rules(report)
            .iter()
            .find(|entry| entry["rule_id"] == rule)
            .and_then(|entry| entry["counts"][status].as_u64())
            .unwrap_or(0)
    }

    fn rules(report: &serde_json::Value) -> &Vec<serde_json::Value> {
        report["rules"]
            .as_array()
            .unwrap_or_else(|| panic!("the report has no `rules` array: {report}"))
    }

    /// Every rule that stated a violation or a suspicion, with its counts.
    fn flagged(report: &serde_json::Value) -> Vec<String> {
        rules(report)
            .iter()
            .filter(|entry| {
                ["violation", "suspected"]
                    .iter()
                    .any(|status| entry["counts"][status].as_u64().unwrap_or(0) > 0)
            })
            .map(ToString::to_string)
            .collect()
    }

    /// Runs the tool over one feed's archive, on the ports that feed was sent
    /// on.
    fn asked(
        feed: &str,
        archive: &Recorded,
        capture: &FeedCapture,
    ) -> (common::conformance::Verdict, serde_json::Value) {
        let mut ports = PortRoles::none();
        for tap in &capture.taps {
            ports = ports.with(tap.role, tap.port);
        }
        let (verdict, report) =
            common::conformance::conformance_reported(archive, feed, harness::GROUP, ports);
        assert_ne!(
            verdict.code, 2,
            "dz-conformance could not run at all:\n{}",
            verdict.stderr
        );
        (verdict, report)
    }

    fn judge(feed: &str, archive: &Recorded, capture: &FeedCapture, magic: u16) {
        // The capture first: a verdict over a capture with no strike in it is
        // five passes that say nothing.
        let refdata = capture.port(PortRole::Refdata);
        let said = refdata_said(archive, magic, refdata);
        assert_the_capture_shows_the_strikes(feed, &said);

        let (verdict, report) = asked(feed, archive, capture);
        assert_eq!(
            report["read_error"], "",
            "{feed}: the tool stopped reading before the capture ended:\n{}",
            verdict.stderr
        );
        assert_eq!(
            report["capture_drops"], 0,
            "{feed}: nothing was lost on the way to the tool, so nothing may be excused as lost"
        );

        // Everything the rule set flagged, whichever rule it was, in front of
        // any assertion that could end the test before it is seen.
        let flagged = flagged(&report);
        for entry in &flagged {
            eprintln!("{feed}: the rule set flagged {entry}");
        }

        for rule in STRIKE_RULES {
            // What the rule set itself counted, printed whether or not it is
            // about to be asserted on: `--nocapture` then shows the verdict
            // rule by rule and not only that nothing failed.
            let counts = rules(&report)
                .iter()
                .find(|entry| entry["rule_id"] == rule)
                .map_or_else(
                    || "no entry".to_owned(),
                    |entry| entry["counts"].to_string(),
                );
            eprintln!("{feed}: {rule}: {counts}");
            for status in ["violation", "suspected"] {
                assert_eq!(
                    count(&report, rule, status),
                    0,
                    "{feed}: {rule} stated a {status}:\n{}",
                    verdict.stderr
                );
            }
        }

        // The two continuity rules account for every accepted definition, so
        // their own counts say how often they looked. Each strike instrument's
        // definitions after its first are opportunities for both, and a count
        // short of that is a rule that was not shown the capture this file
        // believes it handed over.
        let after_the_first: u64 = [ABOVE, BETWEEN, BELOW]
            .iter()
            .map(|symbol| strikes_of(&said, symbol).len() as u64 - 1)
            .sum();
        assert!(
            count(&report, "STRIKE.PRESENCE_STABLE", "pass") >= after_the_first,
            "{feed}: STRIKE.PRESENCE_STABLE passed {} times over {after_the_first} repeated \
             definitions of a strike instrument: {report}",
            count(&report, "STRIKE.PRESENCE_STABLE", "pass")
        );
        // Fixed-stays-fixed has something to hold only once a strike is fixed:
        // every definition of the one fixed at listing after its first, and
        // every definition of the other two after the one that fixed them.
        let fixed_then_repeated: u64 = [ABOVE, BETWEEN, BELOW]
            .iter()
            .map(|symbol| {
                strikes_of(&said, symbol)
                    .iter()
                    .flatten()
                    .filter(|strike| !strike.is_pending())
                    .count() as u64
                    - 1
            })
            .sum();
        assert!(
            count(&report, "STRIKE.FIXED_STAYS_FIXED", "pass") >= fixed_then_repeated,
            "{feed}: STRIKE.FIXED_STAYS_FIXED passed {} times over {fixed_then_repeated} \
             repeated definitions of a fixed strike: {report}",
            count(&report, "STRIKE.FIXED_STAYS_FIXED", "pass")
        );

        // And the rest of the rule set, which this capture is as much subject
        // to as any other in this crate.
        assert!(
            flagged.is_empty(),
            "{feed}: the rule set flagged rules other than the five this suite is about:\n{}\n{}",
            flagged.join("\n"),
            verdict.stderr
        );
        verdict.assert_clean();
    }

    /// The offset of the `Bound Flags` byte in a `StrikeInterval`, and of the
    /// first byte past its `Fixing Time`: the specification's field table.
    const BOUND_FLAGS_AT: usize = 11;
    const FIXING_TIME_END: usize = 36;

    /// Where each `StrikeInterval` of one instrument stands in what was sent:
    /// the datagram's index and the message's offset in its payload.
    fn strikes_sent(capture: &FeedCapture, magic: u16, symbol: &str) -> Vec<(usize, usize)> {
        let refdata = capture.port(PortRole::Refdata);
        let mut found = Vec::new();
        for (index, dg) in capture.sent.iter().enumerate() {
            if dg.dst.port() != refdata {
                continue;
            }
            let decoded = Datagram::decode(&dg.payload, magic).expect("a datagram of this feed");
            let mut at = dz_edge_core::DATAGRAM_HEADER_SIZE;
            let mut defined = None;
            for message in decoded.messages() {
                match message.type_id {
                    DEFINITION => {
                        let definition =
                            InstrumentDefinition::decode(message.bytes, SCHEMA_VERSION)
                                .expect("a definition decodes");
                        defined = Some(
                            String::from_utf8_lossy(&definition.symbol)
                                .trim_end_matches(['\0', ' '])
                                .to_owned(),
                        );
                    }
                    STRIKE => {
                        if defined.as_deref() == Some(symbol) {
                            found.push((index, at));
                        }
                        defined = None;
                    }
                    _ => defined = None,
                }
                at += message.bytes.len();
            }
        }
        found
    }

    /// The negative controls, and the reason the two tests below are worth
    /// anything: three of the five rules state a finding only when they find a
    /// defect, so their silence over a conformant capture is the silence of a
    /// tool pointed at a port nothing was sent on. Each control spoils one
    /// `StrikeInterval` of the same run, in place, and the rule set has to name
    /// the rule that one byte breaks.
    fn spoiled<F: Fn(&mut [u8])>(symbol: &str, rule: &str, spoil: F) {
        for (feed, magic) in [("tob", TopOfBook::MAGIC), ("mbp", MarketByPrice::MAGIC)] {
            let run = run();
            let (mut capture, name) = match feed {
                "tob" => (run.tob_capture, TopOfBook::NAME),
                _ => (run.mbp_capture, MarketByPrice::NAME),
            };
            let (index, at) = *strikes_sent(&capture, magic, symbol)
                .last()
                .expect("the instrument has a strike in the capture");
            spoil(&mut capture.sent[index].payload[at..]);

            let archive = capture.record(name);
            let (verdict, report) = asked(feed, &archive, &capture);
            assert_eq!(
                verdict.code, 1,
                "{feed}: the rule set passed a strike it defines as a violation, so it is \
                 validating nothing:\n{}",
                verdict.stderr
            );
            assert!(
                count(&report, rule, "violation") > 0,
                "{feed}: and it has to be {rule} that this strike breaks: {report}"
            );
        }
    }

    #[test]
    fn the_rule_set_fails_a_fixed_strike_that_returns_to_pending() {
        // A well-formed pending strike, where the definition before it carried
        // a fixed one: every structural rule passes it and only the continuity
        // rule can object.
        spoiled(ABOVE, "STRIKE.FIXED_STAYS_FIXED", |strike| {
            strike[BOUND_FLAGS_AT..FIXING_TIME_END].fill(0);
        });
    }

    #[test]
    fn the_rule_set_fails_a_strike_with_a_reserved_flag_set() {
        spoiled(BETWEEN, "STRIKE.FIELDS", |strike| {
            strike[BOUND_FLAGS_AT] |= 0b0001_0000;
        });
    }

    #[test]
    fn the_strike_rules_pass_on_top_of_book() {
        let run = run();
        judge("tob", &run.tob, &run.tob_capture, TopOfBook::MAGIC);
    }

    #[test]
    fn the_strike_rules_pass_on_market_by_price() {
        let run = run();
        judge("mbp", &run.mbp, &run.mbp_capture, MarketByPrice::MAGIC);
    }
}
