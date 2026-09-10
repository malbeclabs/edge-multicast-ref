//! The three stages, their threads, and the rules that decide where the seams
//! are.
//!
//! ```text
//! capture ──ring──► derivation ──spool──► posting ──► column store
//!  (caller's)        (a thread)   (disk)   (a thread)
//! ```
//!
//! # Why three and not two
//!
//! [`RowSink::write_batch`] posts synchronously and retries what the destination
//! refuses. On the derivation stage, a destination that is merely slow would
//! hold up the next window, the ring behind it would fill, and the capture would
//! start dropping — a column-store problem turned into feed loss. So posting is
//! its own thread, and the spool between them is disk rather than memory.
//!
//! # The lock is never held across the network
//!
//! Both stages reach the spool, so it lives behind a mutex. The posting stage
//! takes a window under the lock, **releases it**, posts, and takes the lock
//! again to record what landed. Holding it across the insert would put the
//! derivation behind the destination's timeout, which is the same backpressure
//! chain by a route nobody would look for.
//!
//! # Two contracts the spool imposes, and they are easy to get wrong
//!
//! **[`Spool::forget_loaded`] is called once, after opening.** A crash between
//! the ledger entry and the directory delete leaves a window on disk whose rows
//! are already in the store; posting it again is a replace that costs an insert
//! for nothing.
//!
//! **[`Spool::record_landed`] is called once a pass even when nothing landed.**
//! A window whose rows are in the store but whose ledger entry could not be
//! written owes an entry, not an insert — so it is not offered for posting
//! again, and the only thing that retries it is a call to `record_landed`. A
//! pass that skipped it when the sink had nothing to say would leave that entry
//! unwritten for as long as the destination stayed quiet.
//!
//! # A stage that panics is restarted, and the capture never is one of them
//!
//! The derivation and the posting stage each run under [`catch_unwind`]. A panic
//! is counted and the stage begins again, because the alternative is a recorder
//! that keeps capturing into a ring nobody drains and reports itself healthy.
//! The capture thread belongs to the caller and is not one of these: nothing
//! here can bring it down.
//!
//! [`RowSink::write_batch`]: dz_recorder_rows::RowSink::write_batch
//! [`catch_unwind`]: std::panic::catch_unwind

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use dz_recorder_archive::JoinedRole;
use dz_recorder_core::{CaptureDropScope, RecorderIdentity};
use dz_recorder_load::{now_unix_nanos, Ledger};
use dz_recorder_rows::{RowSink, SegmentTrailer};

use crate::derivation::WindowDeriver;
use crate::manifest::WindowIdentity;
use crate::ring::{RingReceiver, RingSender};
use crate::spool::Spool;
use crate::window::{Closed, WindowBound, WindowSource};

/// How long the posting stage waits between passes when there is nothing to do.
///
/// Short enough that a window does not sit on disk for want of being asked
/// about, and long enough that an idle recorder is not spinning. The sink's own
/// coalescing decides when rows actually leave, not this.
const POST_INTERVAL: Duration = Duration::from_secs(1);

/// What the pipeline publishes about itself.
///
/// Cumulative counters and two gauges. Every counter here is a delta to alert
/// on, with one exception stated on it.
#[derive(Debug, Default)]
pub struct InlineCounters {
    windows_derived: AtomicU64,
    windows_empty: AtomicU64,
    rows_derived: AtomicU64,
    windows_stored: AtomicU64,
    windows_landed: AtomicU64,
    posts_failed: AtomicU64,
    stage_restarts: AtomicU64,
    spool_bytes: AtomicU64,
    /// **The number to alert on.** Not the eviction counter: a full budget
    /// evicts on every pass at steady state by design, so that counter rises
    /// whether or not anything is wrong, while one window older than the budget
    /// can hold is history already gone.
    oldest_unposted_age_seconds: AtomicU64,
}

macro_rules! counter {
    ($name:ident) => {
        #[must_use]
        pub fn $name(&self) -> u64 {
            self.$name.load(Ordering::Relaxed)
        }
    };
}

impl InlineCounters {
    counter!(windows_derived);
    counter!(windows_empty);
    counter!(rows_derived);
    counter!(windows_stored);
    counter!(windows_landed);
    counter!(posts_failed);
    counter!(stage_restarts);
    counter!(spool_bytes);
    counter!(oldest_unposted_age_seconds);
}

/// Everything the derivation needs that does not change between windows.
pub struct DerivationConfig {
    pub identity: RecorderIdentity,
    pub feed: String,
    pub roles_joined: Vec<JoinedRole>,
    pub drop_scope: CaptureDropScope,
    pub link_headers_captured: bool,
    pub bound: WindowBound,
}

/// The running pipeline: two threads, and the counters they publish.
///
/// Dropping this does not stop it. Call [`stop`](Self::stop), which runs the
/// shutdown in the order that keeps what is in hand.
#[must_use = "a pipeline that is dropped without being stopped abandons its open window"]
pub struct Pipeline {
    /// One flag per stage, because the two are stopped at different moments and
    /// a shared flag cannot express that. See [`Pipeline::stop`].
    derivation_stop: Arc<AtomicBool>,
    posting_stop: Arc<AtomicBool>,
    counters: Arc<InlineCounters>,
    spool: Arc<Mutex<Spool>>,
    derivation: Option<JoinHandle<()>>,
    posting: Option<JoinHandle<()>>,
}

/// Starts both stages over a ring the caller feeds.
///
/// The [`RingSender`] half stays with the caller, because the capture loop is
/// the caller's: this crate never owns a socket.
pub fn start<S: RowSink + Send + 'static>(
    receiver: RingReceiver,
    spool: Spool,
    ledger: Ledger,
    sink: S,
    config: DerivationConfig,
) -> Pipeline {
    let derivation_stop = Arc::new(AtomicBool::new(false));
    let posting_stop = Arc::new(AtomicBool::new(false));
    let counters = Arc::new(InlineCounters::default());
    let spool = Arc::new(Mutex::new(spool));

    let derivation = spawn_stage(
        "dz-recorder-derive",
        Arc::clone(&derivation_stop),
        Arc::clone(&counters),
        Deriving {
            receiver,
            config,
            spool: Arc::clone(&spool),
            // **A run starts at zero and anchors on nothing, and the ledger's
            // trailer changes neither.** These two lines are a decision rather
            // than a gap: a restart is a run boundary, the capture stopped over
            // it, and a `segment_seq` that begins again is how a reader is told
            // so. The predecessor test is `segment_seq + 1`, so a trailer left
            // by the previous run precedes nothing here and `derive` filters it
            // out — reading it back on its own changes no answer anywhere.
            //
            // Continuing the number so that it *would* is the version to
            // refuse. `007_recorder_cross_site.sql`'s `segment_overflow` takes
            // the nearest earlier segment, checks `p.segment_seq + 1 =
            // c.segment_seq`, and clamps a counter that went backwards to zero
            // — and the capture-drop counter belongs to the capture handle, so
            // the first window of the new run would report a delta of zero over
            // a handle opened seconds earlier. That reads as this host having
            // admitted nothing, which is one of the two things that make an
            // absence usable against a publisher.
            window_seq: 0,
            preceding: None,
            deriver: WindowDeriver::new(),
        },
        derivation_stage,
    );

    let posting = spawn_stage(
        "dz-recorder-post",
        Arc::clone(&posting_stop),
        Arc::clone(&counters),
        Posting {
            spool: Arc::clone(&spool),
            ledger,
            sink,
            forgotten: false,
        },
        posting_stage,
    );

    Pipeline {
        derivation_stop,
        posting_stop,
        counters,
        spool,
        derivation: Some(derivation),
        posting: Some(posting),
    }
}

impl Pipeline {
    #[must_use]
    pub fn counters(&self) -> &Arc<InlineCounters> {
        &self.counters
    }

    /// Stops both stages, in the one order that keeps what is in hand.
    ///
    /// **It takes the capture end**, and that is not a convenience. Closing the
    /// ring is what ends the open window, so the datagrams the capture has
    /// already accepted are derived and spooled instead of abandoned — and the
    /// publisher will not send those again, so abandoning them would leave a
    /// hole in the rows that nothing in them could explain. A `stop` that only
    /// set a flag would race the derivation into throwing away a window it had
    /// not opened yet, which is a bug the type system can simply prevent.
    ///
    /// So: close the ring, let the derivation run out, then stop the posting
    /// stage — which makes one last pass, so a window the destination was ready
    /// to take does not sit on disk until the next start.
    ///
    /// **Each stage's flag is set before the join that waits on it**, which is
    /// why there are two. `derivation_stage` never reads its flag — the ring
    /// closing is its ending — so setting it here costs nothing on the ordinary
    /// path. What it buys is the path where the derivation is inside
    /// [`spawn_stage`]'s restart loop: that loop gives up only while the flag
    /// is set, so a derivation panicking on every pass would be joined for ever
    /// and the shutdown would never reach the posting stage at all. A single
    /// flag could not do this, because setting it early would tell the posting
    /// stage to make its last pass while the derivation was still spooling into
    /// it — which is the ordering this method exists to get right.
    pub fn stop(mut self, capture: RingSender) -> Arc<InlineCounters> {
        self.derivation_stop.store(true, Ordering::SeqCst);
        drop(capture);
        if let Some(handle) = self.derivation.take() {
            let _ = handle.join();
        }
        self.posting_stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.posting.take() {
            let _ = handle.join();
        }
        Arc::clone(&self.counters)
    }

    /// The spool, for a caller that wants to read its counters for an exposition.
    #[must_use]
    pub fn spool(&self) -> &Arc<Mutex<Spool>> {
        &self.spool
    }
}

/// How long a stage that has just panicked waits before it is begun again, and
/// the ceiling that wait climbs to.
///
/// The wait doubles per consecutive panic, and a pass that ran for longer than
/// the ceiling resets it — so a stage that panics once an hour is never slowed,
/// and one that panics on the instruction it starts on is. The floor is short
/// enough that a single panic costs the derivation nothing an operator could
/// measure, and the ceiling low enough that a stage which recovers comes back
/// inside one posting interval.
const RESTART_DELAY: Duration = Duration::from_millis(50);
/// The ceiling [`RESTART_DELAY`] climbs to.
const RESTART_DELAY_MAX: Duration = Duration::from_secs(5);

/// Runs a stage, restarting it if it panics.
///
/// A panic in a stage is a bug, and the answer to a bug is not to stop
/// recording: the capture keeps going either way, and a derivation that has
/// stopped means a ring nobody drains and a feed nobody is deriving, reported by
/// a process that looks healthy. So it is counted and begun again. The counter
/// is what makes it visible; a restart nobody can see is worse than a crash.
///
/// **Begun again is not begun again immediately.** A stage that panics on every
/// pass would otherwise restart at the speed of the panic: a busy loop taking
/// the CPU the capture needs, and a line on stderr per pass burying the first
/// one — the only one that names the bug. See [`RESTART_DELAY`].
fn spawn_stage<T, F>(
    name: &str,
    stop: Arc<AtomicBool>,
    counters: Arc<InlineCounters>,
    mut state: T,
    body: F,
) -> JoinHandle<()>
where
    T: Send + 'static,
    F: Fn(&mut T, &AtomicBool, &InlineCounters) + Send + 'static,
{
    let name = name.to_owned();
    std::thread::Builder::new()
        .name(name.clone())
        .spawn(move || {
            // Attempts a shutting-down stage still gets. A panic in the final
            // pass would otherwise abandon a window the destination was ready
            // to take: the flag says stop, and the loop below would read that
            // as "do not come back". Bounded, because a stage that panics on
            // every attempt must not hold a shutdown open for ever — after
            // these, what it was holding stays on disk for the next start,
            // which is what the spool is for.
            const AFTER_STOP: u32 = 2;
            let mut remaining_after_stop = AFTER_STOP;
            let mut delay = RESTART_DELAY;

            loop {
                let began = Instant::now();
                // The state outlives the attempt, so a restarted stage resumes
                // where it was rather than from nothing: a derivation that
                // began again with no preceding trailer would write an
                // uncertain era anchor for no reason but a bug it recovered
                // from.
                let attempt = catch_unwind(AssertUnwindSafe(|| body(&mut state, &stop, &counters)));
                if attempt.is_ok() {
                    // A clean return means the stage decided it was done.
                    break;
                }
                counters.stage_restarts.fetch_add(1, Ordering::Relaxed);
                eprintln!("dz-recorder: the {name} stage panicked and was restarted");

                if stop.load(Ordering::SeqCst) {
                    if remaining_after_stop == 0 {
                        eprintln!(
                            "dz-recorder: the {name} stage panicked {AFTER_STOP} more times while \
                             stopping and is being left; what it held is on disk"
                        );
                        break;
                    }
                    remaining_after_stop -= 1;
                    // Not paced. A shutdown is bounded by the count above, and
                    // sleeping between those attempts would spend a supervisor's
                    // stop timeout on a stage that has already been given up on.
                    continue;
                }

                // A pass that ran longer than the ceiling did work before it
                // failed, so it is not the loop this paces: the next panic
                // starts from the floor again.
                if began.elapsed() >= RESTART_DELAY_MAX {
                    delay = RESTART_DELAY;
                }
                std::thread::sleep(delay);
                delay = (delay * 2).min(RESTART_DELAY_MAX);
            }
        })
        .expect("a thread can be spawned")
}

/// The derivation stage's state, held across a restart.
struct Deriving {
    receiver: RingReceiver,
    config: DerivationConfig,
    spool: Arc<Mutex<Spool>>,
    /// Monotonic within a run and restarting at zero across one, the same as
    /// `segment_seq`: a hole in it is a hole in the derivation, which is what
    /// distinguishes a recorder that was down from a feed that was quiet.
    ///
    /// **An empty window therefore spends none of it.** A quiet feed closes
    /// windows on age as a matter of course, so spending a number on one would
    /// write *the derivation was down* once a window bound for the whole of a
    /// silence — and would leave every window after that silence with an
    /// uncertain era anchor, the predecessor test being `segment_seq + 1`.
    window_seq: u64,
    /// The previous window's trailer, which decides one bit of the next
    /// window's first era: whether its anchor is certain. Windows here are
    /// strictly sequential and none is evicted before it is derived, so unlike
    /// an archive loader — whose predecessor is routinely gone — this is
    /// available for every window after the first **of a run**.
    ///
    /// Not for the first window of one, and not from the ledger either: the
    /// ledger's trailer describes a window of the run before, the predecessor
    /// test is `segment_seq + 1`, and a run begins at zero. See the comment at
    /// the two lines in [`start`] that hold this value and the sequence it is
    /// checked against.
    ///
    /// `None` is *unknown*, never *there was none*, which is why a window the
    /// spool refused clears it rather than handing its trailer on: the rows that
    /// window described are not in the store, and an anchor that called the next
    /// window a continuation would merge two sequence spaces over a hole nothing
    /// in the rows can explain.
    preceding: Option<SegmentTrailer>,
    /// The two passes one window needs, and the buffer they share. Held across
    /// windows and across a restart of this stage, so a window after the first
    /// costs a copy per datagram rather than an allocation.
    deriver: WindowDeriver,
}

/// The posting stage's state, held across a restart.
struct Posting<S> {
    spool: Arc<Mutex<Spool>>,
    ledger: Ledger,
    sink: S,
    /// Whether the spool has been reconciled against the ledger, which happens
    /// once and must not happen again on a restart.
    forgotten: bool,
}

/// Window after window, until the capture ends or a stop is asked for.
fn derivation_stage(state: &mut Deriving, stop: &AtomicBool, counters: &InlineCounters) {
    let Deriving {
        receiver,
        config,
        spool,
        window_seq,
        preceding,
        deriver,
    } = state;

    // No stop flag is consulted here, deliberately. The ring closing is the
    // signal, and it is the only one that cannot arrive before the datagrams
    // already accepted have been derived. A flag checked at the top of this
    // loop would let a shutdown discard a window this stage had not opened yet.
    let _ = stop;
    loop {
        let mut window = WindowSource::open(&mut *receiver, config.bound);
        let identity = WindowIdentity {
            identity: &config.identity,
            feed: &config.feed,
            roles_joined: &config.roles_joined,
            drop_scope: config.drop_scope,
            link_headers_captured: config.link_headers_captured,
        };

        // Drained, then described, then derived — in that order, because a
        // manifest describes what the window saw and `derive` stamps that
        // manifest onto every row as it reads. `WindowDeriver` owns the order
        // and the equivalence gate calls the same thing, so the shape asserted
        // is the shape that runs.
        let derived =
            match deriver.derive_window(&mut window, &identity, *window_seq, preceding.as_ref()) {
                Ok(derived) => Some(derived),
                Err(e) => {
                    eprintln!("dz-recorder: a window derived nothing: {e}");
                    None
                }
            };

        let closed = window.closed();
        let tally_start = window.tally().first_recv_ts_ns;
        let empty = window.is_empty();
        drop(window);

        if empty {
            // And it spends no window sequence number: a hole there says the
            // derivation was down, and a feed that has gone quiet closes
            // windows on age as a matter of course.
            counters.windows_empty.fetch_add(1, Ordering::Relaxed);
        } else {
            // Spent by every window that saw a datagram, whatever became of its
            // rows. A window whose derivation or whose spool failed has lost
            // rows, and a hole in the sequence is exactly what that is.
            *window_seq += 1;
            if let Some(derived) = derived {
                counters.windows_derived.fetch_add(1, Ordering::Relaxed);

                counters
                    .rows_derived
                    .fetch_add(derived.rows.len() as u64, Ordering::Relaxed);
                let trailer = derived.trailer.clone();
                let mut held = spool
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match held.store(tally_start, derived.rows, derived.trailer) {
                    Ok(()) => {
                        counters.windows_stored.fetch_add(1, Ordering::Relaxed);
                        *preceding = Some(trailer);
                    }
                    // Counted and carried on. A spool that cannot take a window
                    // costs that window's rows; stopping here would cost every
                    // window after it as well.
                    //
                    // **And the next window's anchor becomes uncertain.** The
                    // trailer is true, but the rows it describes are not in the
                    // store, so handing it on would let a reader join across a
                    // hole as one continuous sequence space.
                    Err(e) => {
                        eprintln!("dz-recorder: a window could not be spooled: {e}");
                        *preceding = None;
                    }
                }
                counters.spool_bytes.store(held.bytes(), Ordering::Relaxed);
                drop(held);
            } else {
                // A window that saw datagrams and derived nothing describes
                // them nowhere, so the next window's predecessor is unknown for
                // the same reason a window the spool refused makes it unknown.
                *preceding = None;
            }
        }

        // An ending is the capture's, and there is no next window to open.
        if closed == Closed::CaptureEnded {
            return;
        }
    }
}

/// Take, post, record — with the lock held for neither the insert nor the wait.
fn posting_stage<S: RowSink>(state: &mut Posting<S>, stop: &AtomicBool, counters: &InlineCounters) {
    let Posting {
        spool,
        ledger,
        sink,
        forgotten,
    } = state;

    // **Every time this stage is entered, including after a panic.** A window
    // taken for posting is marked in flight, and a panic between taking it and
    // recording or releasing it unwinds past both — leaving a window nobody is
    // holding that the spool will never offer again. It would sit on disk,
    // ageing the lag gauge, until the process restarted. A release here is a
    // no-op on the first entry and the recovery on every other.
    {
        let mut held = spool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        held.release();
    }

    // Once, before the first pass: a crash between a ledger entry and the
    // directory delete leaves a window whose rows are already in the store, and
    // posting it again is an insert paid for nothing. Guarded, because a
    // restarted stage must not do it a second time.
    if !*forgotten {
        let mut held = spool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dropped = held.forget_loaded(ledger);
        if dropped > 0 {
            eprintln!("dz-recorder: {dropped} spooled window(s) were already loaded");
        }
        *forgotten = true;
    }

    loop {
        pass(spool, ledger, sink, counters);
        if stop.load(Ordering::SeqCst) {
            // One last pass after the stop, so a shutdown does not leave a
            // window on disk that the destination was ready to take.
            pass(spool, ledger, sink, counters);
            return;
        }
        // Sliced, so a stop is noticed promptly rather than a whole interval
        // later — a supervisor with a stop timeout kills the difference.
        let mut waited = Duration::ZERO;
        while waited < POST_INTERVAL && !stop.load(Ordering::SeqCst) {
            let slice = Duration::from_millis(100).min(POST_INTERVAL - waited);
            std::thread::sleep(slice);
            waited += slice;
        }
    }
}

/// One pass: every window the spool will offer, then the entries it owes.
fn pass<S: RowSink>(
    spool: &Arc<Mutex<Spool>>,
    ledger: &mut Ledger,
    sink: &mut S,
    counters: &InlineCounters,
) {
    let now = now_unix_nanos();
    loop {
        // The lock is taken to get a window and released before the insert.
        let taken = {
            let mut held = spool
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            held.take_oldest()
        };
        let Some(taken) = taken else { break };
        let window = match taken {
            Ok(window) => window,
            // Already discarded and counted by the spool; ask for the next.
            Err(e) => {
                eprintln!("dz-recorder: a spooled window was discarded: {e}");
                continue;
            }
        };

        // No lock held here, and that is the point of the whole shape: this
        // call talks to the destination and retries what it refuses.
        match sink.write_batch(window.into_rows(), now) {
            Ok(accepted) => {
                let mut held = spool
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let recorded = held.record_landed(&accepted.landed, ledger);
                counters
                    .windows_landed
                    .fetch_add(recorded.recorded, Ordering::Relaxed);
                for message in recorded.failures {
                    eprintln!("dz-recorder: ledger: {message}");
                }
            }
            Err(e) => {
                counters.posts_failed.fetch_add(1, Ordering::Relaxed);
                eprintln!("dz-recorder: the destination refused a window: {e}");
                let mut held = spool
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                held.release();
                // Everything the sink was holding went with it, so there is no
                // point offering it the next window on this pass.
                break;
            }
        }
    }

    // **Once a pass, including a pass that found no window.** The sink holds
    // rows across windows deliberately — one insert is one part, and a part per
    // window per feed is merge work the destination pays for — so a feed that
    // has gone quiet would hold its last rows until something else arrived,
    // which is the opposite of what the sink's age bound is for.
    match sink.post_if_due(now) {
        Ok(landed) if !landed.objects.is_empty() => {
            let mut held = spool
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let recorded = held.record_landed(&landed.objects, ledger);
            counters
                .windows_landed
                .fetch_add(recorded.recorded, Ordering::Relaxed);
            for message in recorded.failures {
                eprintln!("dz-recorder: ledger: {message}");
            }
        }
        Ok(_) => {}
        Err(e) => {
            counters.posts_failed.fetch_add(1, Ordering::Relaxed);
            eprintln!("dz-recorder: the destination refused what the sink was holding: {e}");
            let mut held = spool
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            held.release();
        }
    }

    // **Once a pass, even when nothing landed.** A window whose rows reached the
    // store but whose ledger entry could not be written owes an entry and not an
    // insert, so it is never offered above — and this is the only thing that
    // retries it.
    let mut held = spool
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let recorded = held.record_landed(&[], ledger);
    counters
        .windows_landed
        .fetch_add(recorded.recorded, Ordering::Relaxed);
    counters.spool_bytes.store(held.bytes(), Ordering::Relaxed);
    counters
        .oldest_unposted_age_seconds
        .store(held.oldest_age_seconds(now), Ordering::Relaxed);
    for e in held.take_discarded() {
        eprintln!("dz-recorder: a spooled window was discarded: {e}");
    }
}

/// The capture half, for a caller wiring a record loop.
pub type Capture = RingSender;

#[cfg(test)]
mod tests {
    use super::*;

    /// Waits for a stage to end, rather than joining and hanging if it does not.
    ///
    /// A `join` on a stage that never ends is a test that never fails — it times
    /// the suite out, in a job whose log says nothing about which test it was.
    fn ended_within(handle: &JoinHandle<()>, limit: Duration) -> bool {
        let deadline = Instant::now() + limit;
        while Instant::now() < deadline {
            if handle.is_finished() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        false
    }

    /// A stage that panics as soon as it starts is paced, not spun.
    ///
    /// Three panics, so three waits: the floor, then twice it, then twice that.
    /// The assertion is on the elapsed time rather than on a restart count,
    /// because a count is the same under a loop that sleeps and one that does
    /// not — which is the whole of what this is about. Remove the sleep and this
    /// finishes in microseconds.
    #[test]
    fn a_stage_that_panics_immediately_is_paced_before_it_is_begun_again() {
        let counters = Arc::new(InlineCounters::default());
        let stop = Arc::new(AtomicBool::new(false));
        let began = Instant::now();

        let handle = spawn_stage(
            "paced",
            Arc::clone(&stop),
            Arc::clone(&counters),
            0_u32,
            |attempts, _stop, _counters| {
                *attempts += 1;
                assert!(*attempts > 3, "the first three passes panic");
            },
        );
        handle
            .join()
            .expect("the stage thread does not panic itself");

        assert_eq!(
            counters.stage_restarts(),
            3,
            "counted once per panic, whatever the pacing"
        );
        assert!(
            began.elapsed() >= RESTART_DELAY * 7,
            "three panics owe the floor, twice it and twice that — {:?} elapsed",
            began.elapsed()
        );
    }

    /// A stage that panics on every pass ends once its flag is set, and the
    /// count is what bounds it.
    ///
    /// This is the loop [`Pipeline::stop`] sets the derivation's flag *before*
    /// joining. With the flag never set, nothing here counts down and the join
    /// waits for ever — a shutdown that never reaches the posting stage, on a
    /// process that is otherwise fine.
    #[test]
    fn a_stage_that_panics_on_every_pass_ends_once_its_flag_is_set() {
        let counters = Arc::new(InlineCounters::default());
        let stop = Arc::new(AtomicBool::new(true));

        let handle = spawn_stage(
            "always",
            Arc::clone(&stop),
            Arc::clone(&counters),
            (),
            |(), _stop, _counters| panic!("this stage has a bug on every pass"),
        );

        assert!(
            ended_within(&handle, Duration::from_secs(5)),
            "a stage panicking under a set flag is given up on, not waited for"
        );
        handle
            .join()
            .expect("the stage thread does not panic itself");
        assert_eq!(
            counters.stage_restarts(),
            3,
            "the attempt that found the flag set, and the two it is allowed after it"
        );
    }

    /// The shutdown path is not paced.
    ///
    /// Bounded by the count above instead, because sleeping between attempts a
    /// stage has already been given up on spends a supervisor's stop timeout on
    /// a stage that is not coming back.
    #[test]
    fn giving_up_on_a_stopping_stage_costs_no_delay() {
        let counters = Arc::new(InlineCounters::default());
        let stop = Arc::new(AtomicBool::new(true));
        let began = Instant::now();

        let handle = spawn_stage(
            "stopping",
            Arc::clone(&stop),
            Arc::clone(&counters),
            (),
            |(), _stop, _counters| panic!("this stage has a bug on every pass"),
        );
        handle
            .join()
            .expect("the stage thread does not panic itself");

        assert!(
            began.elapsed() < RESTART_DELAY,
            "three passes under a set flag waited on nothing — {:?} elapsed",
            began.elapsed()
        );
    }
}
