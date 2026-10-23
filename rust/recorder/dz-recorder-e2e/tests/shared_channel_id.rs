//! One process, both feeds, one `Channel ID`, on the wire.
//!
//! A shard's two feeds may share a `Channel ID`, and then the destination port
//! is all that separates their channel instances. This suite composes the
//! runtime as `strike_conformance.rs` does, over the recording sinks of its own
//! test harness, with the depth block on the top-of-book block's ID, and holds
//! what it sent:
//!
//! - each channel instance's `Sequence Number` series is contiguous on its own,
//!   with the two feeds' `mktdata` datagrams alternating on the shared ID;
//! - the two feeds' `Instrument Count` agrees on every `Manifest Seq`;
//! - an instrument has one `Instrument ID` on both feeds.
//!
//! Behind the `conformance` feature, each feed's archive is then handed to
//! `dz-conformance` on the ports that feed was sent on, and must pass.

#![forbid(unsafe_code)]

mod common;
// The runtime's own harness, included rather than copied, as
// `strike_conformance.rs` includes it.
#[path = "../../../publisher/dz-publisher-runtime/tests/harness/mod.rs"]
mod harness;

use std::collections::BTreeMap;
use std::net::SocketAddrV4;
use std::time::Duration;

use common::{record_joined_as, Recorded, EGRESS_PORT, JOIN_INTERFACE, JOIN_SOURCE};
use dz_adapter_core::{
    Adapter, AdapterError, DepthBound, EventSink, InstrumentRef, ListingSink, ParseError, Payload,
    Scalar, Side, SnapshotSink,
};
use dz_edge_core::{Datagram, DatagramHeader, Feed, PortRole, SCHEMA_VERSION};
use dz_edge_mbp::MarketByPrice;
use dz_edge_refdata::{InstrumentDefinition, ManifestSummary};
use dz_edge_tob::TopOfBook;
use dz_recorder_archive::writer::RoleJoin;
use dz_recorder_core::{RecvTsKind, Sink};
use dz_recorder_replay::OwnedDatagram;
use harness::{Arrive, Harness, Recorder};

const DEFINITION: u8 = 0x02;
const MANIFEST: u8 = 0x07;

const SYMBOLS: [&str; 4] = ["AAA", "BBB", "CCC", "DDD"];

/// How long the publisher runs: two laps of the harness's 30-second
/// definition cycle.
const SECONDS: u64 = 60;

/// A venue with four instruments, a quote and a level update for each every
/// second.
struct Venue {
    handles: Vec<InstrumentRef>,
    book: bool,
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

    // For the rule set, behind `conformance`.
    #[cfg_attr(not(feature = "conformance"), allow(dead_code))]
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

    // For the rule set, behind `conformance`.
    #[cfg_attr(not(feature = "conformance"), allow(dead_code))]
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

struct Capture {
    tob: FeedCapture,
    mbp: FeedCapture,
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

    /// Called after every call into the publisher, so the capture's order is
    /// the order the publisher sent in.
    fn drain(&mut self, h: &Harness) {
        let now = dz_publisher_refdata::Clock::unix_ns(&h.clock);
        self.recv_ts_ns = self.recv_ts_ns.max(now);
        self.tob.drain(&mut self.recv_ts_ns);
        self.mbp.drain(&mut self.recv_ts_ns);
    }
}

struct Run {
    tob: FeedCapture,
    mbp: FeedCapture,
}

/// Runs the publisher with both feeds on one `Channel ID`, a quote and then a
/// level update for each instrument every second, each drained before the
/// next, so the two feeds' `mktdata` datagrams alternate on the shared ID.
fn run() -> Run {
    // A rotation, so the depth feed's snapshot port carries traffic too.
    let mut h = harness::harness_both_on_one_channel_with_rotation(Duration::from_secs(4));
    let mut capture = Capture::of(&h);
    let mut venue = Venue {
        handles: Vec::new(),
        book: false,
    };

    h.publisher.declare_venue_timestamps(&venue);
    assert!(h.publisher.poll_listings(&mut venue));
    assert!(h.publisher.tick().is_none());
    capture.drain(&h);

    for second in 1..=SECONDS {
        h.clock.advance(Duration::from_secs(1));
        assert!(h.publisher.poll_listings(&mut venue));
        capture.drain(&h);

        let source_ts_ns = dz_publisher_refdata::Clock::unix_ns(&h.clock);
        for handle in venue.handles.clone() {
            h.publisher.upstream_message("quote");
            h.publisher.arrive(harness::quote(handle, source_ts_ns));
            capture.drain(&h);
            h.publisher.upstream_message("level");
            h.publisher.arrive(harness::bid_level(handle, source_ts_ns));
            capture.drain(&h);
        }
        venue.book = true;

        if let Some(Err(error)) = h.publisher.periodic_snapshot(&venue) {
            panic!("a periodic snapshot was refused: {error}");
        }
        capture.drain(&h);
        assert!(
            h.publisher.tick().is_none(),
            "a guard ended the publisher at second {second}"
        );
        capture.drain(&h);
    }

    Run {
        tob: capture.tob,
        mbp: capture.mbp,
    }
}

fn header(dg: &OwnedDatagram) -> DatagramHeader {
    DatagramHeader::decode(&dg.payload).expect("a datagram the runtime sent")
}

/// Every channel instance's `Sequence Number` series is contiguous on its own,
/// and the two feeds' `mktdata` instances on the shared ID alternated.
///
/// **The revert:** give the two feeds' egress one counter per `(source IP
/// address, Channel ID)`, without the destination port, and each instance's
/// series shows a gap at every alternation.
#[test]
fn each_channel_instance_on_the_shared_id_numbers_contiguously() {
    let run = run();

    for (feed, capture) in [("tob", &run.tob), ("mbp", &run.mbp)] {
        for tap in &capture.taps {
            let sequence: Vec<u64> = capture
                .sent
                .iter()
                .filter(|dg| dg.dst.port() == tap.port)
                .map(|dg| {
                    let header = header(dg);
                    assert_eq!(
                        header.channel_id,
                        harness::CHANNEL_ID,
                        "{feed}: every datagram is on the shared Channel ID"
                    );
                    header.sequence_number
                })
                .collect();
            assert!(
                sequence.len() > 10,
                "{feed}: the {} port carried {} datagrams",
                tap.role.as_str(),
                sequence.len()
            );
            for pair in sequence.windows(2) {
                assert_eq!(
                    pair[1],
                    pair[0] + 1,
                    "{feed}: the {} port's series is not contiguous: {sequence:?}",
                    tap.role.as_str()
                );
            }
        }
    }

    // The two `mktdata` instances, in the order they were sent, alternate.
    let tob_mktdata = run.tob.port(PortRole::Mktdata);
    let mbp_mktdata = run.mbp.port(PortRole::Mktdata);
    let mut both: Vec<(u64, u16)> = run
        .tob
        .sent
        .iter()
        .chain(&run.mbp.sent)
        .filter(|dg| [tob_mktdata, mbp_mktdata].contains(&dg.dst.port()))
        .map(|dg| (dg.recv_ts_ns, dg.dst.port()))
        .collect();
    both.sort_unstable();
    let alternations = both
        .windows(2)
        .filter(|pair| pair[0].1 != pair[1].1)
        .count();
    assert!(
        alternations >= SECONDS as usize,
        "the two feeds' mktdata datagrams alternated {alternations} times, so the shared ID was \
         not interleaved"
    );
}

/// What one feed's refdata port stated: `Instrument Count` per `Manifest Seq`,
/// and the `Instrument ID` of each symbol.
fn refdata_of(capture: &FeedCapture, magic: u16) -> (BTreeMap<u16, u32>, BTreeMap<String, u32>) {
    let refdata = capture.port(PortRole::Refdata);
    let mut counts = BTreeMap::new();
    let mut ids = BTreeMap::new();
    for dg in capture.sent.iter().filter(|dg| dg.dst.port() == refdata) {
        let decoded = Datagram::decode(&dg.payload, magic).expect("a datagram of this feed");
        for message in decoded.messages() {
            match message.type_id {
                MANIFEST => {
                    let manifest =
                        ManifestSummary::decode(message.bytes).expect("a manifest decodes");
                    assert_eq!(manifest.channel_id, harness::CHANNEL_ID);
                    let previous = counts.insert(manifest.manifest_seq, manifest.instrument_count);
                    assert!(
                        previous.is_none_or(|count| count == manifest.instrument_count),
                        "one feed stated two counts under one Manifest Seq"
                    );
                }
                DEFINITION => {
                    let definition = InstrumentDefinition::decode(message.bytes, SCHEMA_VERSION)
                        .expect("a definition decodes");
                    let symbol = String::from_utf8_lossy(&definition.symbol)
                        .trim_end_matches(['\0', ' '])
                        .to_owned();
                    let previous = ids.insert(symbol.clone(), definition.instrument_id);
                    assert!(
                        previous.is_none_or(|id| id == definition.instrument_id),
                        "{symbol} was defined under two Instrument IDs on one feed"
                    );
                }
                _ => {}
            }
        }
    }
    (counts, ids)
}

/// The two feeds state one published set on the shared ID: the same
/// `Instrument Count` under every `Manifest Seq` both stated, and one
/// `Instrument ID` per instrument.
#[test]
fn the_two_feeds_agree_on_the_manifest_and_on_every_instrument_id() {
    let run = run();
    let (tob_counts, tob_ids) = refdata_of(&run.tob, TopOfBook::MAGIC);
    let (mbp_counts, mbp_ids) = refdata_of(&run.mbp, MarketByPrice::MAGIC);

    assert!(
        !tob_counts.is_empty(),
        "the top-of-book feed sent manifests"
    );
    for (manifest_seq, count) in &tob_counts {
        if let Some(depth) = mbp_counts.get(manifest_seq) {
            assert_eq!(
                count, depth,
                "Manifest Seq {manifest_seq}: the feeds state different instrument counts"
            );
        }
    }
    assert!(
        tob_counts.keys().any(|seq| mbp_counts.contains_key(seq)),
        "no Manifest Seq is stated on both feeds: tob {tob_counts:?}, mbp {mbp_counts:?}"
    );
    assert_eq!(
        tob_ids.len(),
        SYMBOLS.len(),
        "every instrument is defined on top-of-book"
    );
    assert_eq!(
        tob_ids, mbp_ids,
        "an instrument has one Instrument ID on both feeds"
    );
}

#[cfg(feature = "conformance")]
mod by_the_rule_set {
    use super::*;
    use dz_recorder_conformance::tool::PortRoles;

    /// Every rule that stated a violation or a suspicion, with its counts.
    fn flagged(report: &serde_json::Value) -> Vec<String> {
        report["rules"]
            .as_array()
            .unwrap_or_else(|| panic!("the report has no `rules` array: {report}"))
            .iter()
            .filter(|entry| {
                ["violation", "suspected"]
                    .iter()
                    .any(|status| entry["counts"][status].as_u64().unwrap_or(0) > 0)
            })
            .map(ToString::to_string)
            .collect()
    }

    fn judge(feed: &str, archive: &Recorded, capture: &FeedCapture) {
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
        assert_eq!(
            report["read_error"], "",
            "{feed}: the tool stopped reading before the capture ended:\n{}",
            verdict.stderr
        );
        let flagged = flagged(&report);
        assert!(
            flagged.is_empty(),
            "{feed}: the rule set flagged:\n{}\n{}",
            flagged.join("\n"),
            verdict.stderr
        );
        verdict.assert_clean();
    }

    #[test]
    fn the_rule_set_passes_top_of_book_on_the_shared_id() {
        let run = run();
        judge("tob", &run.tob.record(TopOfBook::NAME), &run.tob);
    }

    #[test]
    fn the_rule_set_passes_market_by_price_on_the_shared_id() {
        let run = run();
        judge("mbp", &run.mbp.record(MarketByPrice::NAME), &run.mbp);
    }
}
