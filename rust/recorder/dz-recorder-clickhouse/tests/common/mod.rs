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
use dz_recorder_rows::{derive_object, RowBatch};

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

/// A real batch: the real writer, the real archive, the real derivation.
#[must_use]
pub fn batch(datagrams: usize, fault: Fault) -> RowBatch {
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
        roles_joined: vec![RoleJoin::on(
            PortRole::Mktdata,
            GROUP,
            port_for(PortRole::Mktdata),
        )],
        link_headers: LinkHeaders::Synthesised,
        capture_drop_scope: CaptureDropScope::PortRole,
    };
    let mut writer = ArchiveWriter::new(cfg, 0).expect("the archive opens");
    SyntheticPublisher::with_fault(datagrams, fault)
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
