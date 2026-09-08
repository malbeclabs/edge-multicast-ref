//! The record path: capture in, archive and health tier out, and a shutdown
//! that keeps what the recorder is holding.
//!
//! One thread per feed, and nothing on it but the loop. Rotation hands a
//! segment over; compression, hashing and publication happen on the archive's
//! own thread, and the health tier is allocation-free per datagram — so the
//! only thing between a datagram arriving and it being in the open segment is a
//! copy and a buffered write.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dz_recorder_archive::{ArchiveWriter, Published};
use dz_recorder_capture::{CaptureStats, SocketSource, SocketSourceConfig, Waited};
use dz_recorder_core::{Observer as _, RecordedDatagram, Sink, SinkError, SourceError};
use dz_recorder_health::{
    CaptureDeltas, FeedSeries, HealthError, HealthMetrics, HealthMetricsConfig, HealthObserver,
    InstanceLimits,
};
use thiserror::Error;

use crate::endpoint::serve;
use crate::startup::{FeedPlan, Plan};

/// How long the loop waits for a datagram before doing its periodic work.
///
/// Also the granularity at which a shutdown is noticed, and the interval at
/// which a rotation on age can fire on a feed that has gone quiet — which is
/// the case an age bound exists for.
const POLL: Duration = Duration::from_millis(100);

/// How often the staging budget is swept and the capture's own counters are
/// read across into the health tier. Objects land asynchronously, so the budget
/// is enforced on a cadence as well as on rotation.
const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// How long shutdown spends taking datagrams that were already captured.
///
/// Bounded, because on a busy feed the queue is never empty and a drain that
/// waited for it to be would never end. Two seconds is far longer than the
/// queue takes to empty at any rate this recorder is sized for, and it is spent
/// only once, at exit.
const DRAIN_WINDOW: Duration = Duration::from_secs(2);

/// The wait inside that window. Short, so a queue that is already empty costs
/// one of these rather than the whole window.
const DRAIN_POLL: Duration = Duration::from_millis(50);

#[derive(Debug, Error)]
pub enum RunError {
    #[error("the metrics endpoint could not bind {addr}: {source}. A recorder nobody can scrape is the archive-and-forget recorder this design rejects, so this is refused rather than warned about")]
    Endpoint {
        addr: std::net::SocketAddr,
        source: std::io::Error,
    },
    #[error("feed `{feed}`: the capture could not start: {source}")]
    Capture { feed: String, source: SourceError },
    #[error("feed `{feed}`: the archive could not start: {source}")]
    Archive { feed: String, source: SinkError },
    #[error("feed `{feed}`: the health tier could not start: {source}")]
    Health { feed: String, source: HealthError },
    #[error("feed `{feed}`: the capture handle was lost while recording: {source}")]
    HandleLost { feed: String, source: SourceError },
    #[error(
        "feed `{feed}`: the capture ended without being asked to. The open segment was published \
         first, so nothing in hand was lost — but this process is no longer recording, and a \
         recorder that exits 0 having stopped archiving is one a supervisor never restarts."
    )]
    CaptureEnded { feed: String },
    #[error("feed `{feed}`: the recorder thread panicked")]
    Panicked { feed: String },
}

/// What one capture handle has to be, for the loop and for the shutdown.
///
/// A trait rather than the concrete sources, because the shutdown ordering is
/// the part of this file most worth testing and a socket is the part hardest to
/// have in a test. Both live sources implement it; so does the fake below.
pub trait Capturing {
    /// Waits at most `timeout`. A live source has no end, so a timeout and an
    /// ending are distinguishable outcomes and never the same one.
    fn wait(&mut self, timeout: Duration) -> Result<Waited<'_>, SourceError>;
    /// Stops the capture. After this the handle hands nothing else back.
    fn stop(&self);
}

impl Capturing for SocketSource {
    fn wait(&mut self, timeout: Duration) -> Result<Waited<'_>, SourceError> {
        self.next_within(timeout)
    }
    fn stop(&self) {
        Self::stop(self);
    }
}

#[cfg(feature = "afpacket")]
impl Capturing for dz_recorder_capture::AfPacketSource {
    fn wait(&mut self, timeout: Duration) -> Result<Waited<'_>, SourceError> {
        self.next_within(timeout)
    }
    fn stop(&self) {
        Self::stop(self);
    }
}

/// One capture handle, whichever mode the configuration asked for.
enum Capture {
    Socket(SocketSource),
    #[cfg(feature = "afpacket")]
    AfPacket(dz_recorder_capture::AfPacketSource),
}

impl Capturing for Capture {
    fn wait(&mut self, timeout: Duration) -> Result<Waited<'_>, SourceError> {
        match self {
            Self::Socket(source) => source.wait(timeout),
            #[cfg(feature = "afpacket")]
            Self::AfPacket(source) => source.wait(timeout),
        }
    }
    fn stop(&self) {
        match self {
            Self::Socket(source) => Capturing::stop(source),
            #[cfg(feature = "afpacket")]
            Self::AfPacket(source) => Capturing::stop(source),
        }
    }
}

impl Capture {
    /// Frames the interface or its driver dropped, upstream of the capture
    /// point — its own category, never folded into publisher loss. Socket mode
    /// cannot see them: it is below no interface it can read a counter from.
    /// The capture's own cumulative counters, whichever mode is running.
    fn capture_stats(&self) -> CaptureStats {
        match self {
            Self::Socket(source) => source.stats(),
            #[cfg(feature = "afpacket")]
            Self::AfPacket(source) => source.stats().capture,
        }
    }

    fn interface_drops(&self) -> Option<u64> {
        match self {
            Self::Socket(_) => None,
            #[cfg(feature = "afpacket")]
            Self::AfPacket(source) => Some(source.interface_drops()),
        }
    }

    fn drops(&self) -> u64 {
        match self {
            Self::Socket(source) => source.stats().overflow_drops,
            #[cfg(feature = "afpacket")]
            Self::AfPacket(source) => source.stats().capture.overflow_drops,
        }
    }

    fn queue_drops(&self) -> u64 {
        match self {
            Self::Socket(source) => source.stats().queue_drops,
            #[cfg(feature = "afpacket")]
            Self::AfPacket(source) => source.stats().capture.queue_drops,
        }
    }
}

/// What one wait found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    Recorded,
    /// The deadline passed with nothing received. A live feed may be quiet.
    Quiet,
    Ended,
}

/// One wait, and whatever it found handed on.
pub fn pump<C: Capturing + ?Sized>(
    capture: &mut C,
    deliver: &mut dyn FnMut(&RecordedDatagram<'_>),
    timeout: Duration,
) -> Result<Progress, SourceError> {
    match capture.wait(timeout)? {
        Waited::Datagram(dg) => {
            deliver(&dg);
            Ok(Progress::Recorded)
        }
        Waited::TimedOut => Ok(Progress::Quiet),
        Waited::Ended => Ok(Progress::Ended),
    }
}

/// Takes what the capture is already holding, then stops it.
///
/// In that order, and the order is the whole point. Both live sources report
/// `Ended` as soon as their stop flag is set, so a shutdown that stopped the
/// capture first would discard every datagram its drain threads had already
/// queued — datagrams that were received, that the publisher will never send
/// again, and that would leave a gap in the archive charged to somebody else.
///
/// Bounded by `window`, because a busy feed's queue never empties.
pub fn drain_and_stop<C: Capturing + ?Sized>(
    capture: &mut C,
    deliver: &mut dyn FnMut(&RecordedDatagram<'_>),
    window: Duration,
    poll: Duration,
) -> u64 {
    let until = Instant::now() + window;
    let mut drained = 0;
    while Instant::now() < until {
        match pump(capture, deliver, poll) {
            Ok(Progress::Recorded) => drained += 1,
            // Nothing was waiting: everything captured is now in the archive.
            Ok(Progress::Quiet) | Ok(Progress::Ended) => break,
            // A handle that is already gone has nothing left to hand over, and
            // reporting it here would lose the segment this is protecting.
            Err(_) => break,
        }
    }
    capture.stop();
    drained
}

/// Flushes, rotates and waits for every rotated segment to be published.
///
/// The last step is what makes a shutdown lossless. Rotation hands the segment
/// to the compressor and returns immediately — it must, or the write path would
/// stall behind a compression — so a process that exited after rotating would
/// exit while the object it had just closed was still being written, and the
/// window an operator is most likely to ask about would be the one that never
/// landed.
pub fn settle_archive(
    writer: &mut ArchiveWriter,
    outstanding: &mut u64,
) -> (Vec<Published>, Vec<SinkError>) {
    let mut published = Vec::new();
    let mut failures = Vec::new();

    if let Err(e) = writer.flush() {
        failures.push(e);
    }
    match writer.rotate_at(now_ns()) {
        Ok(Some(_)) => *outstanding += 1,
        // The segment held nothing. Not an error, and not a rotation either: an
        // empty segment does not spend a sequence number, because a gap in the
        // sequence of objects is how a reader learns the archive has one.
        Ok(None) => {}
        Err(e) => failures.push(e),
    }
    while *outstanding > 0 {
        match writer.wait_completed() {
            Some(Ok(object)) => {
                *outstanding = outstanding.saturating_sub(1);
                published.push(object);
            }
            Some(Err(e)) => {
                *outstanding = outstanding.saturating_sub(1);
                failures.push(e);
            }
            // The compressor is gone; nothing else will land.
            None => break,
        }
    }
    (published, failures)
}

/// What one feed's run amounted to, for the line printed at exit.
#[derive(Debug, Clone, Default)]
pub struct Summary {
    pub feed: String,
    pub datagrams_written: u64,
    pub datagrams_dropped: u64,
    pub capture_drops: u64,
    pub queue_drops: u64,
    pub segments_published: u64,
    pub segments_evicted: u64,
    pub drained_at_shutdown: u64,
    pub last_error: Option<String>,
}

impl std::fmt::Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "feed {}: {} datagrams archived, {} published segments, {} evicted, \
             {} capture drops, {} queue drops, {} write drops, {} drained at shutdown",
            self.feed,
            self.datagrams_written,
            self.segments_published,
            self.segments_evicted,
            self.capture_drops,
            self.queue_drops,
            self.datagrams_dropped,
            self.drained_at_shutdown,
        )?;
        if let Some(error) = &self.last_error {
            write!(f, "; last archive fault: {error}")?;
        }
        Ok(())
    }
}

/// One feed's capture, archive and health tier, on one thread.
struct FeedRecorder {
    feed: String,
    capture: Capture,
    writer: ArchiveWriter,
    observer: HealthObserver,
    /// Segments handed to the compressor that have not landed yet. Every
    /// submission produces exactly one completion, so this is exact and the
    /// wait at shutdown ends.
    outstanding: u64,
    segments_published: u64,
    segments_evicted_seen: u64,
    interface_drops_seen: u64,
    /// The capture's counters as of the last sweep, so what reaches the health
    /// tier is a delta and not every earlier sweep counted again.
    capture_seen: CaptureStats,
    last_sweep: Instant,
    last_error: Option<String>,
}

impl FeedRecorder {
    fn open(plan: &Plan, feed: &FeedPlan, metrics: &Arc<HealthMetrics>) -> Result<Self, RunError> {
        let capture = open_capture(plan, feed)?;
        let writer = ArchiveWriter::new(feed.archive.clone(), now_ns()).map_err(|source| {
            RunError::Archive {
                feed: feed.spec.clone(),
                source,
            }
        })?;
        let observer = HealthObserver::new(
            Arc::clone(metrics),
            &feed.spec,
            // The bounds on per-source state, which are the crate's and not a
            // configuration's: an any-source join accepts datagrams from any
            // sender, so the key space is not ours to trust, and there is no
            // key that can raise a bound the recorder's own memory rests on.
            InstanceLimits::default(),
            // The capture's own scope, never a preference: the same value the
            // archive declares in its segments, so the live metrics and the
            // object on disk cannot disagree about what a drop count covers.
            crate::startup::drop_scope(plan.mode),
        )
        .map_err(|source| RunError::Health {
            feed: feed.spec.clone(),
            source,
        })?;
        Ok(Self {
            feed: feed.spec.clone(),
            capture,
            writer,
            observer,
            outstanding: 0,
            segments_published: 0,
            segments_evicted_seen: 0,
            interface_drops_seen: 0,
            capture_seen: CaptureStats::default(),
            last_sweep: Instant::now(),
            last_error: None,
        })
    }

    /// One wait, and whatever it found written and observed.
    fn tick(&mut self, timeout: Duration) -> Result<Progress, SourceError> {
        let Self {
            capture,
            writer,
            observer,
            ..
        } = self;
        let mut deliver = |dg: &RecordedDatagram<'_>| {
            // The archive first: the bytes are the part that cannot be
            // re-derived. `write` counts its own failures and always returns
            // Ok, because a record path that stopped on a storage fault would
            // convert a storage outage into feed loss.
            let _ = writer.write(dg);
            observer.on_datagram(dg);
        };
        pump(capture, &mut deliver, timeout)
    }

    /// Rotation, publication and the staging budget. Never on the write path.
    fn maintain(&mut self) {
        let now = now_ns();
        if self.writer.rotate_due(now) {
            match self.writer.rotate_at(now) {
                Ok(Some(_)) => self.outstanding += 1,
                Ok(None) => {}
                Err(e) => self.note(&e.to_string()),
            }
        }
        while let Some(landed) = self.writer.try_completed() {
            // Saturating, though every submission produces exactly one
            // completion and this cannot reach zero with an object in hand: a
            // panic here would stop a recorder over an accounting slip, and a
            // recorder that stops is the failure every rule in this file is
            // arranged against.
            self.outstanding = self.outstanding.saturating_sub(1);
            match landed {
                Ok(_) => self.segments_published += 1,
                Err(e) => self.note(&e.to_string()),
            }
        }
        if self.last_sweep.elapsed() >= SWEEP_INTERVAL {
            self.sweep();
        }
    }

    fn sweep(&mut self) {
        self.last_sweep = Instant::now();
        self.writer.sweep_staging();

        // Eviction is bounded, counted history — and an alert. It reaches the
        // health tier as the deltas the archive counted, so that a recorder
        // whose shipper stopped draining is visible in minutes.
        let evicted = self.writer.segments_evicted_total();
        for _ in self.segments_evicted_seen..evicted {
            self.observer.record_segment_evicted();
        }
        self.segments_evicted_seen = evicted;

        // And how much history is left, which the count above cannot say. A
        // full budget evicts on every sweep for ever, so that counter rises at
        // steady state by design; this is the level somebody chasing last
        // night's loss report actually needs. Published on every sweep, evicted
        // or not, because it moves when a segment is *written* too.
        //
        // Read from the sweep above rather than measured here: that pass has
        // already scanned both directories, and asking the disk again would
        // double the reads of every sweep for an answer it just computed.
        self.observer.record_archive_oldest_segment(
            self.writer.retained_floor_ns().map(|ns| ns / 1_000_000_000),
            self.writer.retained_objects(),
        );

        // Loss upstream of the capture point. Fed to the health tier, which
        // counts it per feed, and deliberately not to the archive, whose
        // interface-drop accounting is per port role: at capture-handle scope
        // there is no role to charge it to, and a guess recorded as a number is
        // what makes a false publisher-loss finding.
        if let Some(total) = self.capture.interface_drops() {
            let delta = total.saturating_sub(self.interface_drops_seen);
            if delta != 0 {
                self.observer.record_interface_drops(delta);
                self.interface_drops_seen = total;
            }
        }

        // The capture's own counters, as deltas. The health tier pre-creates
        // every one of them, so left unfed they read as a healthy zero for the
        // life of the process — and rejoins is the diagnostic for exactly the
        // failure it exists for, on a socket-mode staleness cadence that
        // replaces memberships every thirty seconds.
        let stats = self.capture.capture_stats();
        self.observer.record_capture_deltas(CaptureDeltas {
            rejoins: stats.rejoins.saturating_sub(self.capture_seen.rejoins),
            rejoin_failures: stats
                .rejoin_failures
                .saturating_sub(self.capture_seen.rejoin_failures),
            unexpected_source_datagrams: stats
                .unexpected_source_datagrams
                .saturating_sub(self.capture_seen.unexpected_source_datagrams),
            foreign_group_datagrams: stats
                .foreign_group_datagrams
                .saturating_sub(self.capture_seen.foreign_group_datagrams),
        });
        self.capture_seen = stats;

        if let Some(error) = self.writer.last_error() {
            self.note(&error);
        }
    }

    /// Reported once per distinct fault: an unwritable destination would
    /// otherwise fill a log with one line per sweep and bury the first one.
    fn note(&mut self, error: &str) {
        if self.last_error.as_deref() == Some(error) {
            return;
        }
        eprintln!("dz-recorder: feed {}: {error}", self.feed);
        self.last_error = Some(error.to_owned());
    }

    /// Records until something asks it to stop, then shuts down losing nothing
    /// it holds.
    fn run(mut self, shutdown: &AtomicBool) -> Result<Summary, RunError> {
        let mut lost = None;
        let mut ended_unasked = false;
        while !shutdown.load(Ordering::Relaxed) {
            match self.tick(POLL) {
                Ok(Progress::Recorded | Progress::Quiet) => {}
                // The handle went away without being asked to. The archive
                // still holds a segment, so the shutdown runs first — and then
                // this is reported as the failure it is. Ending 0 here is how a
                // recorder that silently stopped archiving keeps its unit
                // `active`: `Restart=on-failure` never fires, every other feed
                // is torn down with it, and the feed reads as clean.
                Ok(Progress::Ended) => {
                    ended_unasked = true;
                    break;
                }
                Err(source) => {
                    lost = Some(source);
                    break;
                }
            }
            self.maintain();
        }
        let summary = self.finish();
        match lost {
            Some(source) => Err(RunError::HandleLost {
                feed: summary.feed,
                source,
            }),
            // A fatal reported by a drain thread can be laundered into `Ended`
            // when the channel disconnects before it is re-read, so this is the
            // same failure wearing a quieter face.
            None if ended_unasked => Err(RunError::CaptureEnded { feed: summary.feed }),
            None => Ok(summary),
        }
    }

    /// Drain, stop, flush, rotate, wait for the publication. In that order.
    fn finish(mut self) -> Summary {
        let Self {
            capture,
            writer,
            observer,
            ..
        } = &mut self;
        let mut deliver = |dg: &RecordedDatagram<'_>| {
            let _ = writer.write(dg);
            observer.on_datagram(dg);
        };
        let drained = drain_and_stop(capture, &mut deliver, DRAIN_WINDOW, DRAIN_POLL);

        let (published, failures) = settle_archive(&mut self.writer, &mut self.outstanding);
        self.segments_published += published.len() as u64;
        for failure in &failures {
            self.note(&failure.to_string());
        }
        // Last, so the eviction count and any fault the compressor recorded
        // while publishing the final segment are in the numbers reported.
        self.sweep();

        Summary {
            feed: self.feed.clone(),
            datagrams_written: self.writer.datagrams_written_total(),
            datagrams_dropped: self.writer.datagrams_dropped_total(),
            capture_drops: self.capture.drops(),
            queue_drops: self.capture.queue_drops(),
            segments_published: self.segments_published,
            segments_evicted: self.writer.segments_evicted_total(),
            drained_at_shutdown: drained,
            last_error: self.writer.last_error(),
        }
    }
}

fn open_capture(plan: &Plan, feed: &FeedPlan) -> Result<Capture, RunError> {
    let failed = |source| RunError::Capture {
        feed: feed.spec.clone(),
        source,
    };
    match &feed.device {
        None => {
            let mut config =
                SocketSourceConfig::new(feed.membership_interface, feed.bindings.clone());
            config.recv_buffer_bytes = usize::try_from(plan.buffer_bytes).unwrap_or(usize::MAX);
            config.expected_sources = feed.expected_sources.clone();
            config.read_timeout = POLL;
            SocketSource::bind(&config)
                .map(Capture::Socket)
                .map_err(failed)
        }
        #[cfg(feature = "afpacket")]
        Some(device) => {
            let mut config = dz_recorder_capture::AfPacketSourceConfig::new(
                device.clone(),
                feed.membership_interface,
                feed.bindings.clone(),
            );
            config.buffer_bytes = plan.buffer_bytes;
            config.snaplen = plan.snaplen;
            config.expected_sources = feed.expected_sources.clone();
            config.read_timeout = POLL;
            dz_recorder_capture::AfPacketSource::open(&config)
                .map(Capture::AfPacket)
                .map_err(failed)
        }
        // Unreachable: startup refuses AF_PACKET on a build without it, and a
        // device is only ever set for AF_PACKET mode. Stated as a refusal
        // rather than as an `unreachable!`, because a panic in a recorder is
        // the one failure mode that leaves no message worth reading.
        #[cfg(not(feature = "afpacket"))]
        Some(_) => Err(failed(SourceError::HandleLost(
            "this build has no AF_PACKET support compiled in".to_owned(),
        ))),
    }
}

/// Builds everything, records, and reports.
///
/// Every handle that can fail is opened here, on the calling thread, before any
/// recording thread exists: a bind that fails has to fail the process rather
/// than leave one feed silently unrecorded while the others look healthy.
pub fn run(plan: &Plan, run_for: Option<Duration>) -> Result<(), RunError> {
    let series: Vec<FeedSeries<'_>> = plan
        .feeds
        .iter()
        .map(|feed| FeedSeries {
            feed: &feed.spec,
            port_roles: &feed.port_roles,
            // What the operator declared, and nothing inferred. With sources
            // it decides which instances are declared ones — pre-created,
            // surviving eviction, admitted over a stranger — so a guessed entry
            // would assert an instance that cannot exist and a missing one
            // would make a real publisher a stranger. Empty is the common case
            // and states nothing, which is not the same as stating none.
            channel_ids: &feed.expected_channel_ids,
            expected_sources: &feed.expected_sources,
            expected_magic: None,
        })
        .collect();
    let metrics = Arc::new(HealthMetrics::new(&HealthMetricsConfig {
        site: &plan.identity.site,
        recorder: &plan.identity.recorder,
        feeds: &series,
    }));

    let endpoint =
        serve(Arc::clone(&metrics), plan.listen_addr).map_err(|source| RunError::Endpoint {
            addr: plan.listen_addr,
            source,
        })?;
    let bound = endpoint.local_addr().unwrap_or(plan.listen_addr);
    eprintln!("dz-recorder: metrics on http://{bound}/metrics");

    let mut recorders = Vec::with_capacity(plan.feeds.len());
    for feed in &plan.feeds {
        recorders.push(FeedRecorder::open(plan, feed, &metrics)?);
        eprintln!(
            "dz-recorder: feed {} recording to {}",
            feed.spec,
            feed.archive.staging_dir.display()
        );
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    // A recorder is stopped by its supervisor, and a supervisor stops things with
    // a signal. Without this, every restart abandons the open segment — which is
    // the window an operator is most likely to be asking about, lost on the one
    // event that happens on every deploy. The handler only raises the flag the
    // shutdown sequence already waits on, so a signal takes exactly the same path
    // a bounded run does: drain, rotate, wait for the publication, then exit.
    //
    // A second signal exits, and it has to be made to: `ctrlc` keeps its handler
    // installed for the life of the process, so without this a second SIGINT
    // only re-raises the same flag. That matters because the shutdown waits on
    // the publication of the open segment, and a publication into hung storage
    // waits for as long as the storage does — leaving SIGKILL as the only way
    // out, which is precisely the way that abandons the segment. An operator
    // signalling twice is saying the graceful path is taking too long, and the
    // right answer then is to die.
    //
    // The `termination` feature is what puts SIGTERM and SIGHUP on the same
    // handler as SIGINT; all three take this path.
    {
        let stop = Arc::clone(&shutdown);
        let signals = AtomicU32::new(0);
        if let Err(e) = ctrlc::set_handler(move || {
            if signals.fetch_add(1, Ordering::Relaxed) == 0 {
                stop.store(true, Ordering::Relaxed);
                return;
            }
            eprintln!(
                "dz-recorder: second signal; exiting without waiting for the open segment to be \
                 published"
            );
            std::process::exit(130);
        }) {
            // Not fatal: a recorder that cannot install a handler still records,
            // and saying so is better than refusing to start over it.
            eprintln!(
                "dz-recorder: no signal handler installed ({e}); a signal will \
                 abandon the open segment, so stop this process with --run-for"
            );
        }
    }
    let threads: Vec<(String, JoinHandle<Result<Summary, RunError>>)> = recorders
        .into_iter()
        .map(|recorder| {
            let feed = recorder.feed.clone();
            let stop = Arc::clone(&shutdown);
            let handle = std::thread::Builder::new()
                .name(format!("record-{feed}"))
                .spawn(move || recorder.run(&stop))
                .expect("a thread per configured feed");
            (feed, handle)
        })
        .collect();

    wait_for_shutdown(&shutdown, &threads, run_for);
    // Held until every recorder has finished, so a scrape taken during the
    // shutdown still sees the counters rather than a refused connection.
    let outcome = join_all(threads);
    drop(endpoint);
    outcome
}

/// Waits for the bounded run to end, or for a recorder to end on its own.
///
/// A feed whose handle is lost stops the whole process rather than leaving the
/// others recording: a recorder that is half running is a recorder whose
/// archive has a hole nothing in it explains.
fn wait_for_shutdown(
    shutdown: &AtomicBool,
    threads: &[(String, JoinHandle<Result<Summary, RunError>>)],
    run_for: Option<Duration>,
) {
    let deadline = run_for.map(|window| Instant::now() + window);
    loop {
        if threads.iter().any(|(_, handle)| handle.is_finished()) {
            break;
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            break;
        }
        std::thread::sleep(POLL);
    }
    shutdown.store(true, Ordering::Relaxed);
}

fn join_all(threads: Vec<(String, JoinHandle<Result<Summary, RunError>>)>) -> Result<(), RunError> {
    let mut failure = None;
    for (feed, handle) in threads {
        match handle.join() {
            Ok(Ok(summary)) => eprintln!("dz-recorder: {summary}"),
            Ok(Err(e)) => {
                eprintln!("dz-recorder: {e}");
                failure = failure.or(Some(e));
            }
            Err(_) => {
                let e = RunError::Panicked { feed };
                eprintln!("dz-recorder: {e}");
                failure = failure.or(Some(e));
            }
        }
    }
    match failure {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// The wall clock, which is what a receive timestamp and an object key are in.
#[must_use]
pub fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::path::Path;

    use dz_edge_core::PortRole;
    use dz_recorder_archive::{
        ArchiveWriterConfig, CaptureDropScope, Compression, LinkHeaders, RoleJoin,
    };
    use dz_recorder_core::{RecorderIdentity, RecvTsKind};

    const GROUP: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 1);
    const SOURCE: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
    const PORT: u16 = 41000;

    /// A capture that hands back a fixed number of datagrams it is already
    /// holding, then reports nothing more — and remembers when it was stopped.
    struct Queued {
        left: usize,
        payload: Vec<u8>,
        stopped: Cell<bool>,
        handed_over_after_stop: Cell<usize>,
    }

    impl Queued {
        fn holding(count: usize) -> Self {
            Self {
                left: count,
                payload: vec![7u8; 32],
                stopped: Cell::new(false),
                handed_over_after_stop: Cell::new(0),
            }
        }
    }

    impl Capturing for Queued {
        fn wait(&mut self, _timeout: Duration) -> Result<Waited<'_>, SourceError> {
            if self.left == 0 {
                return Ok(Waited::TimedOut);
            }
            self.left -= 1;
            if self.stopped.get() {
                self.handed_over_after_stop
                    .set(self.handed_over_after_stop.get() + 1);
            }
            Ok(Waited::Datagram(RecordedDatagram {
                payload: &self.payload,
                src: SocketAddrV4::new(SOURCE, 50000),
                dst: SocketAddrV4::new(GROUP, PORT),
                role: PortRole::Mktdata,
                recv_ts_ns: 1_700_000_000_000_000_000,
                recv_ts_kind: RecvTsKind::KernelSoftware,
                drop_delta: 0,
                ttl: Some(8),
                link_headers: None,
                wire_payload_len: 32,
            }))
        }

        fn stop(&self) {
            self.stopped.set(true);
        }
    }

    fn identity() -> RecorderIdentity {
        RecorderIdentity {
            site: "site-a".to_owned(),
            recorder: "recorder-1".to_owned(),
            env: "test".to_owned(),
            build_version: "0.0.0".to_owned(),
            build_commit: "unknown".to_owned(),
            config_hash: "0".repeat(64),
        }
    }

    fn writer_in(root: &Path) -> ArchiveWriter {
        ArchiveWriter::new(
            ArchiveWriterConfig {
                staging_dir: root.join("staging"),
                completed_dir: root.join("completed"),
                // Large enough that nothing rotates on its own: every rotation
                // in these tests is one the shutdown asked for.
                rotate_bytes: 1 << 30,
                rotate_interval: Duration::from_secs(3600),
                staging_max: 1 << 30,
                compression: Compression::Zstd { level: 1 },
                identity: identity(),
                feed: "top-of-book".to_owned(),
                roles_joined: vec![RoleJoin::on(PortRole::Mktdata, GROUP, PORT)],
                link_headers: LinkHeaders::Synthesised,
                capture_drop_scope: CaptureDropScope::PortRole,
            },
            now_ns(),
        )
        .expect("a writer over a temporary directory")
    }

    fn write_one(writer: &mut ArchiveWriter) {
        let payload = [3u8; 48];
        let dg = RecordedDatagram {
            payload: &payload,
            src: SocketAddrV4::new(SOURCE, 50000),
            dst: SocketAddrV4::new(GROUP, PORT),
            role: PortRole::Mktdata,
            recv_ts_ns: now_ns(),
            recv_ts_kind: RecvTsKind::KernelSoftware,
            drop_delta: 0,
            ttl: Some(8),
            link_headers: None,
            wire_payload_len: 48,
        };
        writer.write(&dg).expect("the write path never returns Err");
    }

    #[test]
    fn shutdown_takes_what_the_capture_is_holding_before_it_stops_it() {
        let mut capture = Queued::holding(5);
        let mut seen = 0;
        let mut deliver = |_: &RecordedDatagram<'_>| seen += 1;
        let drained = drain_and_stop(
            &mut capture,
            &mut deliver,
            Duration::from_secs(5),
            Duration::from_millis(1),
        );
        assert_eq!(drained, 5, "every queued datagram has to reach the archive");
        assert_eq!(seen, 5);
        assert!(capture.stopped.get(), "the capture has to end up stopped");
        assert_eq!(
            capture.handed_over_after_stop.get(),
            0,
            "stopping first would have discarded datagrams already received"
        );
    }

    #[test]
    fn a_drain_of_a_feed_that_never_goes_quiet_is_bounded() {
        // A busy feed's queue never empties, so the drain is bounded by its
        // window rather than by the queue. Without the bound this would not
        // return.
        let mut capture = Queued::holding(usize::MAX);
        let mut deliver = |_: &RecordedDatagram<'_>| {};
        let started = Instant::now();
        drain_and_stop(
            &mut capture,
            &mut deliver,
            Duration::from_millis(50),
            Duration::from_millis(1),
        );
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(capture.stopped.get());
    }

    #[test]
    fn shutdown_rotates_the_open_segment_and_waits_for_it_to_be_published() {
        let root = tempfile::tempdir().expect("a temporary directory");
        let mut writer = writer_in(root.path());
        for _ in 0..4 {
            write_one(&mut writer);
        }
        let mut outstanding = 0;
        let (published, failures) = settle_archive(&mut writer, &mut outstanding);

        assert!(failures.is_empty(), "{failures:?}");
        assert_eq!(
            published.len(),
            1,
            "the open segment has to be rotated and its publication waited for"
        );
        assert_eq!(outstanding, 0);
        let object = &published[0];
        assert_eq!(object.segment.datagram_count, 4);
        assert!(
            object.segment.path.exists(),
            "the object has landed by the time this returns: {}",
            object.segment.path.display()
        );
        assert_eq!(object.manifest.datagram_count, 4);
        assert_eq!(object.manifest.capture_drop_scope, "port-role");
    }

    #[test]
    fn a_shutdown_with_nothing_open_publishes_nothing_and_reports_no_failure() {
        // An empty segment does not spend a sequence number: a gap in the
        // sequence of objects is how a reader learns the archive has one, and a
        // recorder that rotated nothing on every restart would manufacture
        // gaps.
        let root = tempfile::tempdir().expect("a temporary directory");
        let mut writer = writer_in(root.path());
        let mut outstanding = 0;
        let (published, failures) = settle_archive(&mut writer, &mut outstanding);
        assert!(published.is_empty());
        assert!(failures.is_empty(), "{failures:?}");
    }

    #[test]
    fn a_pump_reports_a_quiet_feed_and_an_ended_one_as_different_things() {
        // A live feed may be quiet, and a recorder that read a quiet feed as a
        // dead one would stop recording exactly when a feed went silent — the
        // moment an archive is most wanted.
        let mut capture = Queued::holding(1);
        let mut deliver = |_: &RecordedDatagram<'_>| {};
        assert_eq!(
            pump(&mut capture, &mut deliver, Duration::from_millis(1)).unwrap(),
            Progress::Recorded
        );
        assert_eq!(
            pump(&mut capture, &mut deliver, Duration::from_millis(1)).unwrap(),
            Progress::Quiet
        );
    }
}
