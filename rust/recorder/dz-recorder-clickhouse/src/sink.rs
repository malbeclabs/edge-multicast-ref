//! The sink: rows into `JSONEachRow` bodies, bounded batches, bounded retries.
//!
//! # Rows are coalesced across objects, and that is the whole design
//!
//! **Merge pressure is the constraint, not row volume.** An insert is one atomic
//! block and becomes one part, so what a day of loading costs the destination is
//! set by rows per part rather than rows per day — and merge work never appears
//! in a query log, only as the gap between a provider's CPU graph and
//! query-attributed CPU. A chatty inserter raises it silently.
//!
//! A sink that posted once per object would write one part per object per lane.
//! On the busiest lane measured that is fine; on the quietest — 130 to 150
//! datagrams a minute, about 700 rows in a time-rotated object — it is a 700-row
//! part per object for ever, which is the pathological profile. So the sink holds
//! rows until [`insert_min_rows`](crate::ClickHouseConfig::insert_min_rows),
//! caps an insert at [`insert_max_rows`](crate::ClickHouseConfig::insert_max_rows),
//! and gives up holding after
//! [`insert_max_delay`](crate::ClickHouseConfig::insert_max_delay) so a quiet
//! lane is late rather than absent.
//!
//! # Accepted is not landed
//!
//! Holding rows means `write_batch` returning `Ok` no longer means the rows are
//! in the store, so it says which objects *landed* instead — see
//! [`Accepted`](dz_recorder_rows::Accepted). The loader records an object in its
//! ledger only when the insert carrying its rows has been acknowledged. An
//! entry written on acceptance would mark an object loaded whose rows are still
//! in memory, and a crash would then lose them with nothing recording that it
//! did.
//!
//! # The batch is the retry unit, and the object is the unit of credit
//!
//! A failure fails **every object whose rows are in the buffer**, not only the
//! one that was being written. That is wider than it was and still correct for
//! the same reason: the tables are `ReplacingMergeTree` and the rows are a pure
//! function of `(object key, sha256)`, so re-loading them all is a replace.
//! Reporting partial success is how a gap becomes invisible — an object whose
//! datagram rows landed and whose gap rows did not reads as a clean feed for
//! ever.
//!
//! # Nothing here may block the recorder
//!
//! The loader is a separate process that shares only a directory with the
//! recorder. A column store that is down, slow or full must cost loading
//! progress and nothing else, so every failure below is a counted refusal and
//! never a wait without a bound.
//!
//! # Credentials come from the environment, and are never logged
//!
//! [`Credentials::from_env`](crate::Credentials::from_env) is the only way one
//! enters this process.

use std::time::Duration;

use dz_recorder_rows::{
    Accepted, Grain, Landed, ObjectId, RowBatch, RowSink, RowSinkError, Written,
};
use serde::Serialize;

use crate::config::{ClickHouseConfig, Credentials};
use crate::transport::{HttpTransport, Transport, TransportError};

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
    /// The rows taken and not yet posted, and who they belong to.
    held: Held,
    /// Bytes sent, cumulatively. Per request rather than per object, for the
    /// reason `Accepted::bytes_posted` states.
    bytes_posted: u64,
}

/// Rows accepted and not yet sent.
///
/// One buffer per grain, because one insert goes to one table. `objects` is
/// every object with rows anywhere in those buffers: they land together and they
/// fail together, which is what makes a retry a replace rather than a partial
/// truth.
#[derive(Debug, Default)]
struct Held {
    rows: RowBatch,
    objects: Vec<ObjectId>,
    /// When the oldest row in the buffer was accepted, which is what the age
    /// bound is measured from. `None` when nothing is held.
    oldest_ns: Option<u64>,
    bytes: u64,
}

impl Held {
    fn take(&mut self, batch: RowBatch, now_ns: u64) {
        let id = ObjectId::of(&batch);
        if !self.objects.contains(&id) {
            self.objects.push(id);
        }
        self.oldest_ns.get_or_insert(now_ns);
        // The object key and digest live on every row, so the batch's own pair
        // is not carried forward: an insert spanning objects is ordinary, and
        // the rows say which object each came from.
        self.rows.datagram.extend(batch.datagram);
        self.rows.era.extend(batch.era);
        self.rows.segment_coverage.extend(batch.segment_coverage);
        self.rows.sequence_gap.extend(batch.sequence_gap);
        self.rows
            .conformance_finding
            .extend(batch.conformance_finding);
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    /// Whether what is held has to go now.
    ///
    /// Rows first, age second, and the order is only for legibility: either is
    /// sufficient. The age is measured from the *oldest* held row rather than
    /// from the last write, or a lane that trickles one object per interval
    /// would reset the clock on every arrival and never post at all.
    fn due(&self, now_ns: u64, min_rows: usize, max_delay: Duration) -> bool {
        if self.is_empty() {
            return false;
        }
        if self.len() >= min_rows {
            return true;
        }
        let delay_ns = u64::try_from(max_delay.as_nanos()).unwrap_or(u64::MAX);
        self.oldest_ns
            .is_some_and(|oldest| now_ns.saturating_sub(oldest) >= delay_ns)
    }

    fn clear(&mut self) -> Vec<ObjectId> {
        self.rows = RowBatch::default();
        self.oldest_ns = None;
        self.bytes = 0;
        std::mem::take(&mut self.objects)
    }
}

impl ClickHouseSink<HttpTransport> {
    /// The sink a binary builds: HTTP, with the password from the environment.
    ///
    /// The password is read here rather than passed in, so there is one place it
    /// can come from and no signature anywhere that could carry it out of a
    /// configuration file.
    #[must_use]
    pub fn over_http(config: ClickHouseConfig) -> Self {
        let credentials = Credentials::from_env(config.user.clone());
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
            held: Held::default(),
            bytes_posted: 0,
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

    /// Bytes sent, cumulatively.
    #[must_use]
    pub const fn bytes_posted(&self) -> u64 {
        self.bytes_posted
    }

    /// Rows held, waiting to be posted.
    #[must_use]
    pub fn held_rows(&self) -> usize {
        self.held.len()
    }

    /// Objects whose rows are held and therefore not yet loaded.
    #[must_use]
    pub fn held_objects(&self) -> usize {
        self.held.objects.len()
    }

    /// Splits one grain's rows into bodies no larger than the configured bounds.
    ///
    /// Both bounds are enforced. `insert_max_rows` is the one that governs merge
    /// pressure — an insert is one part — and `insert_max_bytes` is the one that
    /// keeps a request reasonable whatever the row count says, because a row
    /// count says nothing about a row's width. A single row over the byte bound
    /// is sent on its own rather than refused: the bound exists to keep a
    /// request sane, and refusing a row for being wide would silently drop the
    /// row most worth having.
    fn bodies<R: Serialize>(&self, grain: Grain, rows: &[R]) -> Result<Vec<Body>, RowSinkError> {
        let mut bodies = Vec::new();
        let mut current = Body::default();
        for row in rows {
            let line =
                serde_json::to_vec(row).map_err(|source| RowSinkError::Encode { grain, source })?;
            let would_be = current.bytes.len() as u64 + line.len() as u64 + 1;
            if current.rows > 0
                && (current.rows >= self.config.insert_max_rows
                    || would_be > self.config.insert_max_bytes)
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
        objects: &[ObjectId],
    ) -> Result<u64, RowSinkError> {
        let mut bytes = 0u64;
        for body in self.bodies(grain, rows)? {
            self.send(grain, &body)
                .map_err(|e| RowSinkError::Rejected {
                    // The insert spans objects, so the refusal names them all:
                    // every one of them stays unloaded, and an error naming one of
                    // several would send an operator after the wrong file.
                    object_key: describe(objects),
                    attempts: self.config.attempts,
                    last: e.to_string(),
                })?;
            bytes += body.bytes.len() as u64;
        }
        Ok(bytes)
    }

    /// Posts everything held, whatever it is, and says what landed.
    ///
    /// The base grain goes first and the derived grains after, and the order is
    /// deliberate: an insert set whose gap rows are present and whose datagram
    /// rows are not reads as a finding with no evidence behind it, which is the
    /// more alarming of the two intermediate states and the one an operator
    /// would chase.
    fn post(&mut self) -> Result<(Vec<ObjectId>, u64), RowSinkError> {
        if self.held.is_empty() {
            return Ok((Vec::new(), 0));
        }
        // Taken out first, so a failure below leaves nothing held: those objects
        // are unloaded and will be re-derived, and holding their rows as well
        // would send them twice on the next successful post.
        let rows = std::mem::take(&mut self.held.rows);
        let objects = self.held.clear();

        let mut bytes = 0u64;
        bytes += self.write_grain(Grain::Datagram, &rows.datagram, &objects)?;
        bytes += self.write_grain(Grain::Era, &rows.era, &objects)?;
        bytes += self.write_grain(Grain::SegmentCoverage, &rows.segment_coverage, &objects)?;
        bytes += self.write_grain(Grain::SequenceGap, &rows.sequence_gap, &objects)?;
        bytes += self.write_grain(
            Grain::ConformanceFinding,
            &rows.conformance_finding,
            &objects,
        )?;
        Ok((objects, bytes))
    }

    /// [`post`](Self::post), with the bytes it sent added to the running total.
    ///
    /// Every path out of this sink goes through here, so the accumulator cannot
    /// be missing the one that carries most of the traffic — which is what it
    /// was: `write_batch` posts whenever the batch it took made the buffer due,
    /// and that is the dominant path under any configuration where an object is
    /// bigger than `insert_min_rows`.
    fn posted(&mut self) -> Result<(Vec<ObjectId>, u64), RowSinkError> {
        let (objects, bytes) = self.post()?;
        self.bytes_posted += bytes;
        Ok((objects, bytes))
    }
}

impl<T: Transport> RowSink for ClickHouseSink<T> {
    fn write_batch(&mut self, rows: RowBatch, now_ns: u64) -> Result<Accepted, RowSinkError> {
        // Zero bytes on the row count: see `Accepted::bytes_posted` for why a
        // byte count cannot be per object once an insert spans objects.
        let accepted = Written::of(&rows, 0);
        self.held.take(rows, now_ns);
        let (landed, bytes_posted) = if self.held.due(
            now_ns,
            self.config.insert_min_rows,
            self.config.insert_max_delay,
        ) {
            self.posted()?
        } else {
            (Vec::new(), 0)
        };
        Ok(Accepted {
            accepted,
            landed,
            bytes_posted,
        })
    }

    fn post_if_due(&mut self, now_ns: u64) -> Result<Landed, RowSinkError> {
        if self.held.due(
            now_ns,
            self.config.insert_min_rows,
            self.config.insert_max_delay,
        ) {
            self.posted().map(|(objects, bytes_posted)| Landed {
                objects,
                bytes_posted,
            })
        } else {
            Ok(Landed::default())
        }
    }

    fn flush(&mut self, _now_ns: u64) -> Result<Landed, RowSinkError> {
        self.posted().map(|(objects, bytes_posted)| Landed {
            objects,
            bytes_posted,
        })
    }
}

/// The objects an insert carried, for an error to name.
///
/// All of them, and never the first: they land together and fail together, so a
/// refusal naming one of five would send an operator after the wrong file.
fn describe(objects: &[ObjectId]) -> String {
    match objects {
        [] => "no object".to_owned(),
        [one] => one.key.clone(),
        many => format!(
            "{} objects including {}",
            many.len(),
            many.iter()
                .take(3)
                .map(|o| o.key.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
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
