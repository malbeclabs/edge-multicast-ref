//! Fixtures shared by the egress tests.
//!
//! Every test in this crate runs with **no privileges, no network and no
//! socket**. The send path is exercised through [`FakeSink`] and the socket
//! discipline through [`FakeSocket`], because a test that needs a route to a
//! multicast group, or `CAP_NET_ADMIN`, is a test that does not run in CI — and
//! sequencing that is only checked by reading a capture is sequencing that is
//! not checked.
//!
//! Every address here is documentation-range: RFC 5737 for hosts,
//! MCAST-TEST-NET for groups. Nothing here can be mistaken for a real
//! deployment.
//!
//! Each test binary compiles this module in full and uses only part of it, so
//! the unused half is not dead code — it is another binary's fixture.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::fs;
use std::io;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use dz_edge_core::{AppMessage, EncodeError, Feed, PortRole};
use dz_publisher_egress::{DatagramSink, DatagramSocket, FailureScope, RouteLookup, SinkError};
use dz_publisher_metrics::{PublisherMetrics, PublisherMetricsConfig};

// ---------------------------------------------------------------- wire offsets

// Transcribed from the datagram header field table, not read back off the
// builder: a test that asks the encoder where it put a field cannot fail.
pub const OFF_MAGIC: usize = 0;
pub const OFF_SCHEMA_VERSION: usize = 2;
pub const OFF_CHANNEL_ID: usize = 3;
pub const OFF_SEQUENCE_NUMBER: usize = 4;
pub const OFF_SEND_TIMESTAMP: usize = 12;
pub const OFF_MSG_COUNT: usize = 20;
pub const OFF_RESET_COUNT: usize = 21;
pub const OFF_DATAGRAM_LEN: usize = 22;
/// The first application message begins after the 24-byte datagram header, and
/// its own 4-byte header is type, length, then two bytes of flags.
pub const OFF_FIRST_MSG_FLAGS: usize = 24 + 2;

/// `Sequence Number`, from the bytes.
#[must_use]
pub fn sequence_number(datagram: &[u8]) -> u64 {
    u64::from_le_bytes(
        datagram[OFF_SEQUENCE_NUMBER..OFF_SEQUENCE_NUMBER + 8]
            .try_into()
            .expect("eight bytes"),
    )
}

/// The declared `Frame Length` field, which the glossary makes us call the
/// datagram length.
#[must_use]
pub fn declared_len(datagram: &[u8]) -> u16 {
    u16::from_le_bytes(
        datagram[OFF_DATAGRAM_LEN..OFF_DATAGRAM_LEN + 2]
            .try_into()
            .expect("two bytes"),
    )
}

#[must_use]
pub fn first_msg_flags(datagram: &[u8]) -> u16 {
    u16::from_le_bytes(
        datagram[OFF_FIRST_MSG_FLAGS..OFF_FIRST_MSG_FLAGS + 2]
            .try_into()
            .expect("two bytes"),
    )
}

// ---------------------------------------------------------------------- feeds

/// A feed to compose for. The egress crate cannot name a real one: that would
/// make it depend on a per-feed codec crate, which is backwards.
pub struct TestFeed;
impl Feed for TestFeed {
    const MAGIC: u16 = 0x445A;
    const NAME: &'static str = "test-feed";
    /// A test feed stands in for a specification it does not have, so its table
    /// is every Type ID this file pushes rather than a transcription of
    /// anything. A real feed's is the specification's own — see
    /// [`dz_edge_core::Feed::CARRIES`].
    const CARRIES: &'static [u8] = &[
        0x01, 0x02, 0x03, 0x04, 0x06, 0x07, 0x08, 0x11, 0x12, 0x13, 0x14, 0x20, 0x22, 0x40, 0x41,
        0x42, 0x7F,
    ];
}

/// A second feed, for the per-feed era store.
pub struct OtherFeed;
impl Feed for OtherFeed {
    const MAGIC: u16 = 0x1234;
    const NAME: &'static str = "other-feed";
    /// A test feed stands in for a specification it does not have, so its table
    /// is every Type ID this file pushes rather than a transcription of
    /// anything. A real feed's is the specification's own — see
    /// [`dz_edge_core::Feed::CARRIES`].
    const CARRIES: &'static [u8] = &[
        0x01, 0x02, 0x03, 0x04, 0x06, 0x07, 0x08, 0x11, 0x12, 0x13, 0x14, 0x20, 0x22, 0x40, 0x41,
        0x42, 0x7F,
    ];
}

/// A feed whose name is not one path component.
pub struct EscapingFeed;
impl Feed for EscapingFeed {
    const MAGIC: u16 = 0x0000;
    const NAME: &'static str = "../escape";
    /// A test feed stands in for a specification it does not have, so its table
    /// is every Type ID this file pushes rather than a transcription of
    /// anything. A real feed's is the specification's own — see
    /// [`dz_edge_core::Feed::CARRIES`].
    const CARRIES: &'static [u8] = &[
        0x01, 0x02, 0x03, 0x04, 0x06, 0x07, 0x08, 0x11, 0x12, 0x13, 0x14, 0x20, 0x22, 0x40, 0x41,
        0x42, 0x7F,
    ];
}

// ------------------------------------------------------------------- messages

/// 16 bytes, mktdata only.
pub struct Small;
impl AppMessage for Small {
    const TYPE_ID: u8 = 0x11;
    const SIZE: usize = 16;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Mktdata];
    fn encode_into(&self, dst: &mut [u8]) {
        dst.fill(0);
    }
    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}

/// 255 bytes: the largest a message can be, since the message header's Length
/// field is a `u8`. Four of these plus the 24-byte datagram header is 1,044
/// bytes and a fifth would be 1,299 — which is how the mandated cap is reached
/// without any single message being over it.
pub struct Big;
impl AppMessage for Big {
    const TYPE_ID: u8 = 0x12;
    const SIZE: usize = 255;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Mktdata];
    fn encode_into(&self, dst: &mut [u8]) {
        dst.fill(0);
    }
    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}

/// 16 bytes, snapshot only, so that a push on another role is refused.
pub struct SnapshotOnly;
impl AppMessage for SnapshotOnly {
    const TYPE_ID: u8 = 0x13;
    const SIZE: usize = 16;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Snapshot];
    fn encode_into(&self, dst: &mut [u8]) {
        dst.fill(0);
    }
    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}

/// A message whose fields are individually representable and whose combination
/// its own specification forbids.
pub struct Contradictory;
impl AppMessage for Contradictory {
    const TYPE_ID: u8 = 0x14;
    const SIZE: usize = 16;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Mktdata];
    fn validate(&self) -> Result<(), EncodeError> {
        Err(EncodeError::MalformedMessage {
            message: core::any::type_name::<Self>(),
            what: "a fixture that is always malformed",
        })
    }
    fn encode_into(&self, dst: &mut [u8]) {
        dst.fill(0);
    }
    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}

// ---------------------------------------------------------------------- fakes

/// What a fake will do with the next datagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Accept,
    /// A full send buffer: transient, so nothing may be dropped over it.
    WouldBlock,
    /// Any other socket failure: not transient.
    Broken,
}

impl Verdict {
    fn apply(self, datagram: &[u8], accepted: &Mutex<Vec<Vec<u8>>>) -> Result<(), SinkError> {
        match self {
            Self::Accept => {
                accepted
                    .lock()
                    .expect("fixture mutex")
                    .push(datagram.to_vec());
                Ok(())
            }
            Self::WouldBlock => Err(SinkError::WouldBlock),
            Self::Broken => Err(SinkError::Socket(io::Error::other("fixture"))),
        }
    }
}

struct FakeState {
    name: &'static str,
    scope: FailureScope,
    accepted: Mutex<Vec<Vec<u8>>>,
    scripted: Mutex<VecDeque<Verdict>>,
    always: Mutex<Verdict>,
}

/// A [`DatagramSink`] that records what it took and can be told to fail.
///
/// Cloning gives another handle to the same state, so a test can inspect a
/// sink it has already handed to a `Tee` or to a `ChannelEgress`.
#[derive(Clone)]
pub struct FakeSink {
    state: Arc<FakeState>,
}

impl FakeSink {
    /// Accepts everything. Failure scope `Channel`, so a test that cares about
    /// the scope states it.
    #[must_use]
    pub fn new(name: &'static str) -> Self {
        Self::with_scope(name, FailureScope::Channel)
    }

    /// A sink whose failure darkens the publisher.
    #[must_use]
    pub fn essential(name: &'static str) -> Self {
        Self::with_scope(name, FailureScope::Process)
    }

    fn with_scope(name: &'static str, scope: FailureScope) -> Self {
        Self {
            state: Arc::new(FakeState {
                name,
                scope,
                accepted: Mutex::new(Vec::new()),
                scripted: Mutex::new(VecDeque::new()),
                always: Mutex::new(Verdict::Accept),
            }),
        }
    }

    /// Fail every send from now on.
    pub fn always(&self, verdict: Verdict) {
        *self.state.always.lock().expect("fixture mutex") = verdict;
    }

    /// Verdicts for the next sends, in order. When they run out, the standing
    /// verdict from [`Self::always`] applies.
    pub fn script(&self, verdicts: impl IntoIterator<Item = Verdict>) {
        *self.state.scripted.lock().expect("fixture mutex") = verdicts.into_iter().collect();
    }

    /// Every datagram this sink accepted, in order.
    #[must_use]
    pub fn accepted(&self) -> Vec<Vec<u8>> {
        self.state.accepted.lock().expect("fixture mutex").clone()
    }

    #[must_use]
    pub fn accepted_count(&self) -> usize {
        self.state.accepted.lock().expect("fixture mutex").len()
    }

    #[must_use]
    pub fn boxed(&self) -> Box<dyn DatagramSink> {
        Box::new(self.clone())
    }
}

impl DatagramSink for FakeSink {
    fn name(&self) -> &str {
        self.state.name
    }

    fn send(&mut self, datagram: &[u8]) -> Result<(), SinkError> {
        let scripted = self
            .state
            .scripted
            .lock()
            .expect("fixture mutex")
            .pop_front();
        let verdict = match scripted {
            Some(verdict) => verdict,
            None => *self.state.always.lock().expect("fixture mutex"),
        };
        verdict.apply(datagram, &self.state.accepted)
    }

    fn failure_scope(&self) -> FailureScope {
        self.state.scope
    }
}

/// A [`DatagramSocket`], so that `MulticastTransmitter` is tested without one.
#[derive(Clone)]
pub struct FakeSocket {
    sent: Arc<Mutex<Vec<Vec<u8>>>>,
    verdict: Arc<Mutex<Verdict>>,
}

impl FakeSocket {
    #[must_use]
    pub fn new() -> Self {
        Self {
            sent: Arc::new(Mutex::new(Vec::new())),
            verdict: Arc::new(Mutex::new(Verdict::Accept)),
        }
    }

    pub fn always(&self, verdict: Verdict) {
        *self.verdict.lock().expect("fixture mutex") = verdict;
    }

    #[must_use]
    pub fn sent(&self) -> Vec<Vec<u8>> {
        self.sent.lock().expect("fixture mutex").clone()
    }
}

impl DatagramSocket for FakeSocket {
    fn send(&self, datagram: &[u8]) -> Result<(), SinkError> {
        let verdict = *self.verdict.lock().expect("fixture mutex");
        verdict.apply(datagram, &self.sent)
    }
}

/// A routing table that answers whatever the test says, or refuses.
pub struct FakeRoute {
    answer: io::Result<Ipv4Addr>,
}

impl FakeRoute {
    #[must_use]
    pub const fn resolving(source: Ipv4Addr) -> Self {
        Self { answer: Ok(source) }
    }

    #[must_use]
    pub fn unreachable() -> Self {
        Self {
            answer: Err(io::Error::other("no route")),
        }
    }
}

impl RouteLookup for FakeRoute {
    fn source_for(&self, _destination: SocketAddrV4) -> io::Result<Ipv4Addr> {
        match &self.answer {
            Ok(source) => Ok(*source),
            Err(error) => Err(io::Error::new(error.kind(), error.to_string())),
        }
    }
}

// -------------------------------------------------------------------- metrics

/// The normative metric set, as a publisher would build it.
#[must_use]
pub fn metrics(port_roles: &[PortRole], channel_ids: &[u8]) -> Arc<PublisherMetrics> {
    Arc::new(PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles,
        connections: &[],
        channel_ids,
        ingress_message_types: &[],
    }))
}

/// The value of one rendered sample, by family name and the labels that pick
/// the child series out.
///
/// Read out of the exposition rather than off a handle, so what is asserted is
/// what a scrape would see.
#[must_use]
pub fn sample(metrics: &PublisherMetrics, name: &str, labels: &[(&str, &str)]) -> u64 {
    let rendered = metrics.render();
    let prefix = format!("{name}{{");
    let line = rendered
        .lines()
        .filter(|line| line.starts_with(&prefix))
        .find(|line| {
            labels
                .iter()
                .all(|(key, value)| line.contains(&format!("{key}=\"{value}\"")))
        })
        .unwrap_or_else(|| panic!("no sample for {name} with {labels:?}:\n{rendered}"));
    line.rsplit(' ')
        .next()
        .expect("a sample line ends in a value")
        .parse()
        .expect("a counter renders as an integer")
}

// ------------------------------------------------------------------- temp dirs

static NEXT_DIR: AtomicU32 = AtomicU32::new(0);

/// A directory for an era store, removed when the test ends.
///
/// Hand-rolled rather than pulled in as a dependency: this crate's manifest is
/// three lines and a temporary directory is eight.
pub struct TempStateDir {
    path: PathBuf,
}

impl TempStateDir {
    #[must_use]
    pub fn new(tag: &str) -> Self {
        let unique = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "dz-publisher-egress-{}-{tag}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("a temporary directory");
        Self { path }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempStateDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

// ------------------------------------------------------------------ addresses

/// A source address in RFC 5737's first documentation range.
#[must_use]
pub const fn doc_source() -> Ipv4Addr {
    Ipv4Addr::new(192, 0, 2, 10)
}

/// A source address in RFC 5737's second documentation range, for the case
/// where two publishers serve one `Channel ID`.
#[must_use]
pub const fn other_doc_source() -> Ipv4Addr {
    Ipv4Addr::new(198, 51, 100, 10)
}

/// A group in MCAST-TEST-NET.
#[must_use]
pub const fn doc_group(port: u16) -> SocketAddrV4 {
    SocketAddrV4::new(Ipv4Addr::new(233, 252, 0, 1), port)
}
