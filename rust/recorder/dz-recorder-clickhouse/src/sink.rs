//! The sink: rows into `JSONEachRow` bodies, bounded batches, bounded retries.
//!
//! # The batch is the retry unit, and the object is the unit of credit
//!
//! A [`RowBatch`] is split into one or more requests per grain, because a
//! request is bounded by rows and by bytes and an object is not. Each request is
//! retried on its own. But if *any* request in the batch fails after its
//! attempts are spent, the whole `write_batch` fails and the loader must treat
//! the object as unloaded — even though some rows landed.
//!
//! That looks wasteful and it is the only correct answer. The tables are
//! `ReplacingMergeTree` and the rows are a pure function of `(object key,
//! sha256)`, so loading the object again replaces what landed rather than
//! duplicating it. The alternative — reporting what got through — leaves an
//! object whose datagram rows are present and whose gap rows are not, and that
//! object reads as a clean feed for ever. Partial credit is how a gap becomes
//! invisible.

use std::time::Duration;

use dz_recorder_rows::{Grain, RowBatch, RowSink, RowSinkError, Written};
use serde::Serialize;

use crate::config::{ClickHouseConfig, Credentials};
use crate::transport::{HttpTransport, Transport, TransportError};

/// The only place a password enters this process.
pub const PASSWORD_ENV: &str = "DZ_LOADER_CLICKHOUSE_PASSWORD";

/// How long to wait before attempt *n*, doubling.
///
/// Short, and bounded by [`MAX_BACKOFF`]: this is a loader catching up against
/// an eviction clock, so a long sleep inside one object is loading progress
/// spent on one object's bad luck.
const FIRST_BACKOFF: Duration = Duration::from_millis(250);
const MAX_BACKOFF: Duration = Duration::from_secs(4);

/// Rows into a column store.
///
/// Generic over the transport so that batching and retry are testable with no
/// server; [`ClickHouseSink::over_http`] is the one a binary builds.
#[derive(Debug)]
pub struct ClickHouseSink<T: Transport> {
    config: ClickHouseConfig,
    credentials: Credentials,
    transport: T,
    /// Requests that spent their attempts, counted so that a loader can alert on
    /// it rather than infer it from a lag that stopped falling.
    batches_failed: u64,
    last_error: Option<String>,
    /// Waits between attempts, injected so the retry tests do not sleep.
    sleep: fn(Duration),
}

impl ClickHouseSink<HttpTransport> {
    /// The sink a binary builds: HTTP, with the password from the environment.
    ///
    /// The password is read here rather than passed in, so there is one place it
    /// can come from and no signature anywhere that could carry it out of a
    /// configuration file.
    #[must_use]
    pub fn over_http(config: ClickHouseConfig) -> Self {
        let credentials = Credentials::new(config.user.clone(), std::env::var(PASSWORD_ENV).ok());
        let transport = HttpTransport::new(config.timeout);
        Self::with_transport(config, credentials, transport)
    }
}

impl<T: Transport> ClickHouseSink<T> {
    #[must_use]
    pub fn with_transport(
        config: ClickHouseConfig,
        credentials: Credentials,
        transport: T,
    ) -> Self {
        Self {
            config,
            credentials,
            transport,
            batches_failed: 0,
            last_error: None,
            sleep: std::thread::sleep,
        }
    }

    /// Replaces the wait between attempts, for a test that must not sleep.
    #[must_use]
    pub fn waiting_with(mut self, sleep: fn(Duration)) -> Self {
        self.sleep = sleep;
        self
    }

    /// Requests that spent their attempts.
    #[must_use]
    pub const fn batches_failed(&self) -> u64 {
        self.batches_failed
    }

    /// The transport, for a caller that has to see what was sent.
    ///
    /// Public because the sink owns it: a caller builds one value and passes it
    /// around, and a test that needs the recording a fake transport kept would
    /// otherwise have to hold a second handle to it and keep the two in step.
    #[must_use]
    pub const fn transport(&self) -> &T {
        &self.transport
    }

    /// The last failure, verbatim.
    ///
    /// A bounded retry that discards what the destination said leaves an
    /// operator with a count and no cause, so the server's own message — which
    /// names the column it could not parse — is kept.
    #[must_use]
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    /// Posts one statement, for `--check`'s reachability probe and for the DDL.
    ///
    /// # Errors
    ///
    /// [`TransportError`], which is what a probe wants: the status and the
    /// server's own message, not a translated version of them.
    pub fn statement(&self, sql: &str) -> Result<String, TransportError> {
        let url = self.config.statement_url();
        self.transport
            .post(&url, &self.credentials, sql.as_bytes())
            .map(|r| r.body)
    }

    /// Splits one grain's rows into bodies no larger than the configured bounds.
    ///
    /// Both bounds are enforced, and the byte bound is the one that binds: a row
    /// count says nothing about a row's width, and the widest grain here carries
    /// an object key and two digests. A single row over the byte bound is sent
    /// on its own rather than refused — the bound exists to keep a request
    /// reasonable, and refusing a row for being wide would silently drop the
    /// row most worth having.
    fn bodies<R: Serialize>(&self, grain: Grain, rows: &[R]) -> Result<Vec<Body>, RowSinkError> {
        let mut bodies = Vec::new();
        let mut current = Body::default();
        for row in rows {
            let line =
                serde_json::to_vec(row).map_err(|source| RowSinkError::Encode { grain, source })?;
            let would_be = current.bytes.len() as u64 + line.len() as u64 + 1;
            if current.rows > 0
                && (current.rows >= self.config.max_rows || would_be > self.config.max_bytes)
            {
                bodies.push(std::mem::take(&mut current));
            }
            current.bytes.extend_from_slice(&line);
            current.bytes.push(b'\n');
            current.rows += 1;
        }
        if current.rows > 0 {
            bodies.push(current);
        }
        Ok(bodies)
    }

    /// Sends one body, retrying while the failure is one a retry could fix.
    fn send(&mut self, grain: Grain, body: &Body) -> Result<(), TransportError> {
        let url = self.config.insert_url(grain.table());
        let mut backoff = FIRST_BACKOFF;
        let mut attempt = 1;
        loop {
            match self.transport.post(&url, &self.credentials, &body.bytes) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    self.last_error = Some(format!("{grain}: {e}"));
                    // A statement the server rejected will be rejected again
                    // however many times it is sent, and the attempts are worth
                    // more spent on the next object.
                    if attempt >= self.config.attempts || !e.is_worth_retrying() {
                        self.batches_failed += 1;
                        return Err(e);
                    }
                    (self.sleep)(backoff);
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    attempt += 1;
                }
            }
        }
    }

    fn write_grain<R: Serialize>(
        &mut self,
        grain: Grain,
        rows: &[R],
        object_key: &str,
    ) -> Result<u64, RowSinkError> {
        let mut bytes = 0u64;
        for body in self.bodies(grain, rows)? {
            self.send(grain, &body)
                .map_err(|e| RowSinkError::Rejected {
                    object_key: object_key.to_owned(),
                    attempts: self.config.attempts,
                    last: e.to_string(),
                })?;
            bytes += body.bytes.len() as u64;
        }
        Ok(bytes)
    }
}

impl<T: Transport> RowSink for ClickHouseSink<T> {
    fn write_batch(&mut self, rows: RowBatch) -> Result<Written, RowSinkError> {
        let key = rows.object_key.clone();
        let mut bytes = 0u64;
        // Order matters only for what a half-landed object looks like while it
        // is being retried, and the derived grains go last deliberately: an
        // object whose gap rows are present and whose datagram rows are not
        // reads as a finding with no evidence, which is the more alarming of the
        // two intermediate states and the one an operator would chase.
        bytes += self.write_grain(Grain::Datagram, &rows.datagram, &key)?;
        bytes += self.write_grain(Grain::Era, &rows.era, &key)?;
        bytes += self.write_grain(Grain::SegmentCoverage, &rows.segment_coverage, &key)?;
        bytes += self.write_grain(Grain::SequenceGap, &rows.sequence_gap, &key)?;
        bytes += self.write_grain(Grain::ConformanceFinding, &rows.conformance_finding, &key)?;
        Ok(Written::of(&rows, bytes))
    }

    fn flush(&mut self) -> Result<(), RowSinkError> {
        // Nothing is held: every batch is posted before `write_batch` returns,
        // because a sink that buffered across objects would make "this object is
        // loaded" a claim about memory rather than about the store — and the
        // ledger entry written after it would then survive a crash the rows did
        // not.
        Ok(())
    }
}

/// One request's worth of rows.
#[derive(Debug, Default)]
struct Body {
    bytes: Vec<u8>,
    rows: usize,
}

/// The grains, in the order [`ClickHouseSink::write_batch`] sends them.
#[must_use]
pub const fn send_order() -> [Grain; Grain::COUNT] {
    [
        Grain::Datagram,
        Grain::Era,
        Grain::SegmentCoverage,
        Grain::SequenceGap,
        Grain::ConformanceFinding,
    ]
}
