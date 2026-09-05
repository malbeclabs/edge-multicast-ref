//! The open segment, when it rotates, and where it goes next.
//!
//! Rotation is the only place the archive does anything expensive, and even
//! there it does not compress: it fsyncs, closes, hands the file to the
//! compressor thread, enforces the watermark and returns.

use std::fs::{self, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use dz_edge_core::PortRole;
use dz_recorder_core::{CompletedSegment, RecordedDatagram, RecorderIdentity, Sink, SinkError};

use crate::compress::{Compression, Compressor, Faults, Job, Published};
use crate::manifest::SegmentManifest;
use crate::staging::{
    is_compressor_temp, open_segment_name, recovered_segment_name, unpublished_object_name,
    working_segment_seq, StagingWatermark,
};
use crate::writer::{
    role_index, CaptureDropScope, LinkHeaders, RoleJoin, SegmentStats, SegmentWriter,
    SegmentWriterConfig, ALL_ROLES,
};

/// How much of one segment is buffered before it reaches the disk.
const SEGMENT_BUFFER_BYTES: usize = 1 << 20;

#[derive(Debug, Clone)]
pub struct ArchiveWriterConfig {
    /// Where the open segment lives and where an object is assembled.
    pub staging_dir: PathBuf,
    /// What the shipper watches. A move into it is the publication.
    pub completed_dir: PathBuf,
    pub rotate_bytes: u64,
    pub rotate_interval: Duration,
    /// The outage buffer: `retention_minutes × measured_bytes_per_second`.
    pub staging_max: u64,
    pub compression: Compression,
    pub identity: RecorderIdentity,
    /// The feed specification's name — never a venue. It is the first partition
    /// of the object key, so it is the recorder's to state and not a shipper's
    /// to infer.
    pub feed: String,
    /// What the recorder was asked to join, and where.
    pub roles_joined: Vec<RoleJoin>,
    pub link_headers: LinkHeaders,
    /// The scope the capture handles report their losses at, which is a fact
    /// about them and not about the link headers.
    pub capture_drop_scope: CaptureDropScope,
}

type OpenSegment = SegmentWriter<BufWriter<File>>;

pub struct ArchiveWriter {
    cfg: ArchiveWriterConfig,
    watermark: StagingWatermark,
    compressor: Compressor,
    /// Shared with the compressor thread, because a publication that cannot land
    /// fails there and an operator looks here.
    faults: Arc<Faults>,
    /// Opened on the first datagram, so a segment that never received one leaves
    /// nothing behind and an unwritable directory is a counted drop rather than
    /// a failed startup.
    open: Option<OpenSegment>,
    segment_seq: u64,
    opened_ns: u64,
    interface_drops: [u64; ALL_ROLES.len()],
    datagrams_written_total: u64,
    datagrams_dropped_total: u64,
    link_header_exceptions_total: u64,
    write_path_nanos: u64,
    write_path_max_nanos: u64,
}

impl ArchiveWriter {
    pub fn new(cfg: ArchiveWriterConfig, opened_ns: u64) -> Result<Self, SinkError> {
        fs::create_dir_all(&cfg.staging_dir).map_err(SinkError::Io)?;
        fs::create_dir_all(&cfg.completed_dir).map_err(SinkError::Io)?;
        let compressor = Compressor::spawn();
        let faults = compressor.faults();
        let mut watermark = StagingWatermark::new(
            cfg.staging_dir.clone(),
            cfg.completed_dir.clone(),
            cfg.staging_max,
        );
        // So that the one segment eviction must not take is the one the
        // compressor says it is holding, rather than every file that shares its
        // name shape.
        watermark.track_custody(compressor.custody());
        // So a budget that cannot be met reaches last_error rather than being
        // known only to the sweep that discovered it.
        watermark.track_faults(Arc::clone(&faults));
        sweep_dead_temps(&cfg.staging_dir, &faults);
        // Before this run's sequence starts at 0 again, so that a segment a dead
        // run left under a working name is accounted and evictable rather than
        // sitting in staging for ever.
        adopt_dead_segments(&cfg.staging_dir, &faults);
        Ok(Self {
            cfg,
            watermark,
            compressor,
            faults,
            open: None,
            segment_seq: 0,
            opened_ns,
            interface_drops: [0; ALL_ROLES.len()],
            datagrams_written_total: 0,
            datagrams_dropped_total: 0,
            link_header_exceptions_total: 0,
            write_path_nanos: 0,
            write_path_max_nanos: 0,
        })
    }

    #[must_use]
    pub fn open_segment_path(&self) -> PathBuf {
        self.cfg
            .staging_dir
            .join(open_segment_name(self.segment_seq))
    }

    /// Size or age, whichever comes first: a size bound keeps objects uniform
    /// for the analysis tier, an age bound keeps a low-volume feed's data off a
    /// local disk for hours.
    #[must_use]
    pub fn rotate_due(&self, now_ns: u64) -> bool {
        let bytes = self.open.as_ref().map_or(0, SegmentWriter::bytes_written);
        let interval_ns = u64::try_from(self.cfg.rotate_interval.as_nanos()).unwrap_or(u64::MAX);
        bytes >= self.cfg.rotate_bytes || now_ns.saturating_sub(self.opened_ns) >= interval_ns
    }

    /// Closes the open segment and hands it to the compressor.
    ///
    /// `Ok(None)` means the segment held nothing: no object, and no error
    /// either. An empty rotation does not spend a `segment_seq`, because a gap
    /// in the sequence of objects is a gap in the archive.
    pub fn rotate_at(&mut self, now_ns: u64) -> Result<Option<u64>, SinkError> {
        self.opened_ns = now_ns;
        let Some(mut writer) = self.open.take() else {
            return Ok(None);
        };
        if writer.datagram_count() == 0 {
            self.open = Some(writer);
            return Ok(None);
        }

        for role in ALL_ROLES {
            let drops = std::mem::take(&mut self.interface_drops[role_index(role) as usize]);
            if drops != 0 {
                writer.record_interface_drops(role, drops);
            }
        }

        let source = self.open_segment_path();
        // The number is spent before anything that can fail, so an ENOSPC at
        // close leaves a gap in the sequence of objects — which is how a reader
        // learns the archive has one — and so the next segment is not created
        // on this one's path, truncating what survived of it.
        let segment_seq = self.segment_seq;
        self.advance_segment_seq();

        let stats = match close(writer) {
            Ok(stats) => stats,
            Err(e) => {
                // The partial file is the only copy of the window it holds.
                self.preserve_partial(&source, segment_seq, now_ns);
                self.faults
                    .record(format!("closing segment {segment_seq}: {e}"));
                return Err(e);
            }
        };
        self.link_header_exceptions_total += stats.link_header_exceptions;

        if let Err(e) = self.compressor.submit(Job {
            source: source.clone(),
            staging_dir: self.cfg.staging_dir.clone(),
            completed_dir: self.cfg.completed_dir.clone(),
            manifest: self.draft_manifest(segment_seq, &stats),
            compression: self.cfg.compression,
        }) {
            // A segment nothing will publish has to be accounted for under a
            // name the budget can see, or the disk grows without a counter.
            self.retain_unpublished(&source, &stats, segment_seq);
            self.faults
                .record(format!("handing segment {segment_seq} over: {e}"));
            return Err(e);
        }

        self.enforce_watermark();
        Ok(Some(segment_seq))
    }

    /// The periodic half of the watermark: objects land asynchronously, so the
    /// budget is checked on a cadence as well as on rotation.
    pub fn sweep_staging(&mut self) {
        self.enforce_watermark();
    }

    /// Objects that have landed since the last call, if any. Never waits.
    pub fn try_completed(&mut self) -> Option<Result<Published, SinkError>> {
        self.compressor.try_completed()
    }

    /// Waits for the next object to land. For a caller that has stopped
    /// recording — never for one that is still receiving.
    pub fn wait_completed(&mut self) -> Option<Result<Published, SinkError>> {
        self.compressor.wait_completed()
    }

    /// Loss upstream of the capture point, which the analysis tier treats as its
    /// own category rather than folding it into publisher loss.
    pub fn record_interface_drops(&mut self, role: PortRole, delta: u64) {
        self.interface_drops[role_index(role) as usize] += delta;
    }

    #[must_use]
    pub fn bytes_on_disk(&self) -> u64 {
        self.watermark.bytes_on_disk()
    }

    #[must_use]
    pub fn segments_on_disk(&self) -> usize {
        self.watermark.segments_on_disk()
    }

    #[must_use]
    pub fn oldest_segment_seq(&self) -> Option<u64> {
        self.watermark.oldest_segment_seq()
    }

    #[must_use]
    pub fn segments_evicted_total(&self) -> u64 {
        self.watermark.segments_evicted_total()
    }

    /// Where this feed's retained history starts, as of the last
    /// [`sweep_staging`](Self::sweep_staging). See
    /// [`StagingWatermark::retained_floor_ns`].
    #[must_use]
    pub const fn retained_floor_ns(&self) -> Option<u64> {
        self.watermark.retained_floor_ns()
    }

    /// Published objects evicted, a subset of
    /// [`segments_evicted_total`](Self::segments_evicted_total).
    #[must_use]
    pub fn objects_evicted_total(&self) -> u64 {
        self.watermark.objects_evicted_total()
    }

    #[must_use]
    pub fn datagrams_written_total(&self) -> u64 {
        self.datagrams_written_total
    }

    #[must_use]
    pub fn datagrams_dropped_total(&self) -> u64 {
        self.datagrams_dropped_total
    }

    /// Publications that could not land, each one leaving a segment retained in
    /// staging under the object key it was going to have.
    #[must_use]
    pub fn publications_failed_total(&self) -> u64 {
        self.faults.publications_failed()
    }

    /// Datagrams whose own link headers contradicted the configured mode.
    #[must_use]
    pub fn link_header_exceptions_total(&self) -> u64 {
        self.link_header_exceptions_total
    }

    /// Nanoseconds spent inside the write path, over every datagram offered to
    /// it.
    ///
    /// Timed around the whole of [`Sink::write`] rather than around the parts a
    /// wait was expected in: a counter that only covers the places somebody
    /// thought to instrument cannot catch a stall anywhere else, and this is the
    /// rule the design cares most about. Two clock reads per datagram, against a
    /// per-datagram budget of some microseconds at the rates the recorder is
    /// sized for.
    #[must_use]
    pub fn write_path_nanos(&self) -> u64 {
        self.write_path_nanos
    }

    /// The longest single trip through the write path. A stall averages away;
    /// this is where one is visible.
    #[must_use]
    pub fn write_path_max_nanos(&self) -> u64 {
        self.write_path_max_nanos
    }

    /// The last fault any part of the archive saw, including the compressor
    /// thread's.
    #[must_use]
    pub fn last_error(&self) -> Option<String> {
        self.faults.last()
    }

    /// Recovery work an unclean restart made necessary, and which succeeded:
    /// temporary files swept, segments a dead run left adopted into the budget.
    /// Stated separately from [`ArchiveWriter::last_error`], because a recovery
    /// that worked is not a fault and anything reading `last_error` as a health
    /// signal would report one.
    #[must_use]
    pub fn last_recovery(&self) -> Option<String> {
        self.faults.last_recovery()
    }

    #[must_use]
    pub fn recoveries_total(&self) -> u64 {
        self.faults.recoveries()
    }

    #[must_use]
    pub fn segment_seq(&self) -> u64 {
        self.segment_seq
    }

    fn draft_manifest(&self, segment_seq: u64, stats: &SegmentStats) -> SegmentManifest {
        let id = &self.cfg.identity;
        // In the order that fixes `interface_id`, and every stated join rather
        // than one per role: a role claimed twice is itself worth seeing.
        let mut joins: Vec<&RoleJoin> = self.cfg.roles_joined.iter().collect();
        joins.sort_by_key(|join| role_index(join.role));
        let roles_joined = joins.into_iter().map(RoleJoin::as_row).collect();
        SegmentManifest {
            site: id.site.clone(),
            recorder: id.recorder.clone(),
            env: id.env.clone(),
            build_version: id.build_version.clone(),
            build_commit: id.build_commit.clone(),
            config_hash: id.config_hash.clone(),
            feed: self.cfg.feed.clone(),
            segment_seq,
            start_ns: stats.start_ns,
            end_ns: stats.end_ns,
            datagram_count: stats.datagram_count,
            payload_byte_count: stats.payload_byte_count,
            object_key: String::new(),
            byte_count: 0,
            sha256: String::new(),
            instances: stats.instances.clone(),
            short_datagrams: stats.short_datagrams,
            instances_dropped: stats.instances_dropped,
            capture_drop_total: stats.capture_drop_total,
            interface_drop_total: stats.interface_drop_total,
            roles_joined,
            link_headers: self.cfg.link_headers.as_str().to_owned(),
            link_header_exceptions: stats.link_header_exceptions,
            capture_drop_scope: self.cfg.capture_drop_scope.as_str().to_owned(),
        }
    }

    fn enforce_watermark(&mut self) {
        if let Err(e) = self.watermark.enforce() {
            self.faults
                .record(format!("enforcing the staging budget: {e}"));
        }
    }

    /// Moves a partial segment aside so that opening the next one cannot
    /// truncate it.
    ///
    /// A recorder killed mid-write leaves a partial block, which the replay side
    /// has a whole verdict for; the file that carries it is the only copy of
    /// that window. If the move fails, the truncation goes ahead anyway and the
    /// fault is recorded: losing bounded history is recoverable, and refusing to
    /// record live data is not.
    fn preserve_partial(&self, path: &Path, segment_seq: u64, at_ns: u64) {
        let kept = self
            .cfg
            .staging_dir
            .join(recovered_segment_name(segment_seq, at_ns));
        if let Err(e) = fs::rename(path, &kept) {
            self.faults.record(format!(
                "keeping the partial segment at {}: {e}",
                kept.display()
            ));
        }
    }

    fn retain_unpublished(&self, path: &Path, stats: &SegmentStats, segment_seq: u64) {
        let kept = self.cfg.staging_dir.join(unpublished_object_name(
            stats.start_ns,
            stats.end_ns,
            segment_seq,
        ));
        if let Err(e) = fs::rename(path, &kept) {
            self.faults
                .record(format!("retaining a segment as {}: {e}", kept.display()));
        }
    }

    /// The only place `segment_seq` moves, so the watermark's idea of which
    /// segment is transient cannot drift from the writer's: every other
    /// `segment-{seq}.pcapng` is accounted and evictable, and one excluded by
    /// mistake is bytes nothing bounds.
    fn advance_segment_seq(&mut self) {
        self.segment_seq += 1;
        self.watermark.track_open_segment(self.segment_seq);
    }

    /// Gives up the open segment after a write into it failed.
    ///
    /// A failed write can leave a pcapng block half on the disk — an ENOSPC
    /// flush consumes part of one — and appending the next datagram after that
    /// puts every datagram written afterwards behind a bad block, which the
    /// replay reader treats as terminal. So one counted drop would cost the rest
    /// of the segment. Nothing here waits: the segment is dropped without an
    /// fsync, the partial is renamed out of the next segment's way, and the
    /// number is spent so that the window it held is a gap in the sequence of
    /// objects rather than a silence.
    fn abandon_open_segment(&mut self, at_ns: u64) {
        if self.open.take().is_none() {
            return;
        }
        let partial = self.open_segment_path();
        let segment_seq = self.segment_seq;
        self.advance_segment_seq();
        self.preserve_partial(&partial, segment_seq, at_ns);
    }

    fn open_segment(&mut self) -> Result<&mut OpenSegment, SinkError> {
        if self.open.is_none() {
            let path = self.open_segment_path();
            // `segment_seq` restarts at 0 on every run and `File::create`
            // truncates, so without this a restart destroys the partial segment
            // the previous run left. Only a file: a directory on this path is
            // the unwritable case, and it has to stay an error.
            if fs::metadata(&path).is_ok_and(|m| m.is_file()) {
                self.preserve_partial(&path, self.segment_seq, now_ns());
            }
            let file = File::create(&path).map_err(SinkError::Io)?;
            let cfg = SegmentWriterConfig {
                identity: self.cfg.identity.clone(),
                roles_joined: self.cfg.roles_joined.clone(),
                link_headers: self.cfg.link_headers,
                capture_drop_scope: self.cfg.capture_drop_scope,
            };
            self.open = Some(SegmentWriter::new(
                BufWriter::with_capacity(SEGMENT_BUFFER_BYTES, file),
                &cfg,
            )?);
        }
        Ok(self
            .open
            .as_mut()
            .expect("the segment was just opened or already was"))
    }

    /// Counts the drop and remembers why, and does not tell the caller.
    ///
    /// Nothing here may propagate into the drain thread: a drain thread that
    /// sees an error has no correct action but to stop, and stopping converts a
    /// storage fault into feed loss.
    fn drop_and_count(&mut self, e: SinkError) {
        self.datagrams_dropped_total += 1;
        self.faults.record(format!("writing a datagram: {e}"));
    }
}

impl Sink for ArchiveWriter {
    fn write(&mut self, dg: &RecordedDatagram<'_>) -> Result<(), SinkError> {
        let started = Instant::now();
        let result = match self.open_segment() {
            Ok(w) => w.write(dg),
            Err(e) => Err(e),
        };
        match result {
            Ok(()) => self.datagrams_written_total += 1,
            Err(e) => {
                self.drop_and_count(e);
                // The clock is read only here: a segment that has taken a write
                // failure is not one the next datagram may be appended to.
                self.abandon_open_segment(now_ns());
            }
        }
        // Last, so the measurement covers everything the write path does and
        // not only the part before the first early return.
        let took = elapsed_nanos(started);
        self.write_path_nanos += took;
        self.write_path_max_nanos = self.write_path_max_nanos.max(took);
        Ok(())
    }

    /// Rotates on the system clock, and returns an object that has already
    /// landed if there is one.
    ///
    /// The trait's signature is synchronous and publication is not: the object
    /// this call rotates is compressed, hashed and moved on another thread, and
    /// waiting for it here would reintroduce exactly the stall that design
    /// forbids. [`ArchiveWriter::rotate_at`] is the precise form — its
    /// `Ok(None)` means the segment held nothing — and
    /// [`ArchiveWriter::try_completed`] drains the rest.
    fn rotate(&mut self) -> Result<Option<CompletedSegment>, SinkError> {
        self.rotate_at(now_ns())?;
        match self.try_completed() {
            Some(Ok(published)) => Ok(Some(published.segment)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    /// A failed flush leaves the same half-written block a failed write does, so
    /// the segment is abandoned here too — and the error still reaches the
    /// caller, because a flush is not the write path and a caller asking for one
    /// is entitled to know it did not happen.
    fn flush(&mut self) -> Result<(), SinkError> {
        let Some(w) = self.open.as_mut() else {
            return Ok(());
        };
        match w.flush() {
            Ok(()) => Ok(()),
            Err(e) => {
                self.faults
                    .record(format!("flushing the open segment: {e}"));
                self.abandon_open_segment(now_ns());
                Err(e)
            }
        }
    }
}

impl Drop for ArchiveWriter {
    /// Flushes the open segment so a stop does not discard what it holds. The
    /// segment stays in staging: an object is published only by rotation, and a
    /// half-length segment published on shutdown would be indistinguishable
    /// from a full one.
    fn drop(&mut self) {
        if let Some(w) = self.open.as_mut() {
            let _ = w.flush();
        }
    }
}

/// Appends the statistics blocks, flushes the buffer and fsyncs.
fn close(writer: OpenSegment) -> Result<SegmentStats, SinkError> {
    let (file, stats) = writer.finish()?;
    let file = file.into_inner().map_err(|e| SinkError::Io(e.into()))?;
    file.sync_all().map_err(SinkError::Io)?;
    Ok(stats)
}

/// Removes the temporary files of a publication a previous run did not finish.
///
/// One recorder owns one staging directory — two would collide on the open
/// segment — and this runs before this run submits anything, so a `.part` file
/// here belongs to a process that is gone. Left alone it is unaccounted bytes
/// nothing will ever reach.
fn sweep_dead_temps(staging_dir: &Path, faults: &Faults) {
    let Ok(entries) = fs::read_dir(staging_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let is_temp = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(is_compressor_temp);
        if is_temp {
            match fs::remove_file(&path) {
                Ok(()) => faults.record_recovery(format!(
                    "removed {}, left by a publication a previous run did not finish",
                    path.display()
                )),
                Err(e) => faults.record(format!("removing {}: {e}", path.display())),
            }
        }
    }
}

/// Adopts the segments a previous run left under a working name.
///
/// One recorder owns one staging directory and this runs before this run opens
/// anything, so a `segment-{seq}.pcapng` here belongs to a process that is gone:
/// killed mid-write, or killed while the compressor was mid-publish. Under that
/// name it is excluded from the budget as the transient file it no longer is —
/// never published, never counted, never reachable by eviction — and this run's
/// sequence restarts at 0, so it would only ever be found again by accident.
/// Renamed, it is history the budget accounts for and eviction can take.
fn adopt_dead_segments(staging_dir: &Path, faults: &Faults) {
    let Ok(entries) = fs::read_dir(staging_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(segment_seq) = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(working_segment_seq)
        else {
            continue;
        };
        let adopted = staging_dir.join(recovered_segment_name(segment_seq, now_ns()));
        match fs::rename(&path, &adopted) {
            Ok(()) => faults.record_recovery(format!(
                "adopted {}, left by a run that ended before it rotated, as {}",
                path.display(),
                adopted.display()
            )),
            Err(e) => faults.record(format!(
                "adopting {} as {}: {e}",
                path.display(),
                adopted.display()
            )),
        }
    }
}

fn elapsed_nanos(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}
