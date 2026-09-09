//! A window of the live feed, presented as the [`Source`] the derivation reads.
//!
//! # The window is the object's replacement, and only the object's
//!
//! Archive mode's unit is an object: a rotation bound's worth of datagrams,
//! compressed, hashed and described by a manifest. Inline mode's unit is a
//! **window**: the same bound, in memory, derived and then discarded.
//!
//! What that changes is smaller than it sounds, because the object was never the
//! unit of *derivation* — it was the unit of *storage*. [`derive`] reads a
//! `Source` to exhaustion and consults exactly one thing outside the window it
//! is given: the preceding window's trailer, which decides one bit. So a window
//! with no object behind it derives correctly, and this module's whole job is to
//! make a live capture look like a source that ends.
//!
//! # Where the bound is checked, and why it is before the receive
//!
//! At the top of [`Source::next`], never after a datagram is in hand. A window
//! that received a datagram and then discovered it did not fit would have to
//! hold it over for the next one — a datagram in neither window's tally and in
//! one window's rows, or worse, in neither. Checking first leaves the datagram
//! that would cross the bound in the ring, where the next window picks it up as
//! its first. The window may therefore end slightly under the bound rather than
//! slightly over, which is what rotation on size already does.
//!
//! # A quiet feed still closes its window
//!
//! The age bound exists for exactly that case, so the wait is sliced: a receive
//! that times out re-checks the clock rather than blocking until traffic
//! arrives. Without it a channel that goes silent holds its last rows until
//! something else turns up, which is the opposite of what an age bound is for.
//!
//! [`derive`]: dz_recorder_rows::derive
//! [`Source`]: dz_recorder_core::Source

use std::time::{Duration, Instant};

use dz_recorder_archive::CoverageTracker;
use dz_recorder_core::{OwnedDatagram, RecordedDatagram, Source, SourceError};

use crate::ring::{Arrival, RingReceiver};

/// How long one receive waits before the clock is re-read.
///
/// Also the granularity at which a window's age bound can fire on a feed that
/// has gone quiet — which is the case the age bound exists for — and the same
/// value the recorder's own record loop polls at.
const POLL: Duration = Duration::from_millis(100);

/// When a window closes.
///
/// Both, whichever comes first, for the reasons rotation has both: a size bound
/// keeps windows uniform for the analysis tier, and an age bound keeps a quiet
/// feed's rows moving instead of holding them until traffic returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowBound {
    /// Payload bytes, not bytes on disk: nothing here writes a file.
    pub bytes: u64,
    pub interval: Duration,
}

/// Why a window stopped.
///
/// The difference matters to the caller and to nobody else: a window that ended
/// because the capture stopped is the last window of the run, and the pipeline
/// must not open another.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Closed {
    /// The payload bound was reached.
    Bytes,
    /// The age bound came due. A quiet feed closes this way and it is ordinary.
    Age,
    /// The capture is gone and the ring is drained.
    CaptureEnded,
    /// Still open.
    Open,
}

/// What one window observed, in the shape the manifest wants.
///
/// The per-instance half is accumulated by [`CoverageTracker`] — the archive
/// writer's own, not a second implementation of it. Two accumulators would be
/// two answers to what a window covered, and they would differ in exactly the
/// cases the coverage row exists to describe: a reset inside the window, a
/// datagram too short to attribute, an instance past the cap.
#[derive(Debug, Default)]
pub struct WindowTally {
    pub datagram_count: u64,
    pub payload_byte_count: u64,
    /// Receive timestamps, not send timestamps: this is the window the recorder
    /// can vouch for.
    pub first_recv_ts_ns: u64,
    pub last_recv_ts_ns: u64,
    /// Every `drop_delta` the window walked, whatever port role carried it.
    ///
    /// The archive writer's own arithmetic over the same field, so the two
    /// modes' `capture_drop_total` mean one thing and a reader can subtract one
    /// mode's coverage row from the other's. At `capture-handle` scope that
    /// total is the only one there is; at `port-role` scope the manifest still
    /// carries one number for the window, because the coverage grain is the
    /// window and not the role.
    pub capture_drop_total: u64,
    pub coverage: CoverageTracker,
}

/// One window of the live feed, read to exhaustion by the derivation.
///
/// Borrows the ring rather than owning it, because the next window reads the
/// same ring: the receiver outlives every window taken from it.
pub struct WindowSource<'a> {
    ring: &'a mut RingReceiver,
    bound: WindowBound,
    deadline: Instant,
    tally: WindowTally,
    closed: Closed,
}

impl<'a> WindowSource<'a> {
    /// Opens a window on the ring, closing at `bound` from now.
    pub fn open(ring: &'a mut RingReceiver, bound: WindowBound) -> Self {
        Self {
            ring,
            bound,
            deadline: Instant::now() + bound.interval,
            tally: WindowTally::default(),
            closed: Closed::Open,
        }
    }

    /// What the window saw. **Meaningful only once the window has been walked**
    /// — see [`HeldWindow`], which is what walks it, and the manifest a caller
    /// builds from this before then describes nothing.
    #[must_use]
    pub const fn tally(&self) -> &WindowTally {
        &self.tally
    }

    /// Why it closed, which the caller needs in order to know whether to open
    /// another.
    #[must_use]
    pub const fn closed(&self) -> Closed {
        self.closed
    }

    /// True when nothing arrived at all.
    ///
    /// **An empty window spends no window sequence number** and derives no rows,
    /// for the reason an empty segment spends no segment sequence number: a gap
    /// in the sequence of windows is how a reader learns the derivation has one.
    /// A quiet feed closes windows on age and that is ordinary, so a window
    /// which spent a number on nothing would state *the derivation was down*
    /// once a window bound for as long as the feed stayed silent.
    ///
    /// It is also what holds the era anchor across the silence: the
    /// predecessor test is `segment_seq + 1`, so a spent number leaves the next
    /// window's trailer two behind and an uncertain anchor after every quiet
    /// stretch.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.tally.datagram_count == 0
    }
}

impl Source for WindowSource<'_> {
    fn next(&mut self) -> Result<Option<RecordedDatagram<'_>>, SourceError> {
        // Before the receive, never after: see the module documentation. A
        // datagram taken out of the ring and then found not to fit belongs to
        // no window at all.
        if self.tally.payload_byte_count >= self.bound.bytes {
            self.closed = Closed::Bytes;
            return Ok(None);
        }

        loop {
            if Instant::now() >= self.deadline {
                self.closed = Closed::Age;
                return Ok(None);
            }
            // Never past the deadline, so a window with a short interval is not
            // held open by a long poll.
            let slice = POLL.min(self.deadline.saturating_duration_since(Instant::now()));
            match self.ring.wait(slice) {
                Arrival::Datagram => break,
                // A live feed may be quiet, and quiet is not an ending: go back
                // and let the clock decide.
                Arrival::TimedOut => {}
                Arrival::Ended => {
                    self.closed = Closed::CaptureEnded;
                    return Ok(None);
                }
            }
        }

        // Tallied here rather than by the caller, because the borrow the caller
        // gets back ends at its next call, and a count taken afterwards would be
        // a count of what it remembered rather than of what arrived.
        {
            let dg = self.ring.in_hand().expect("the wait took a slot in hand");
            self.tally.datagram_count += 1;
            self.tally.payload_byte_count += dg.payload.len() as u64;
            self.tally.capture_drop_total += u64::from(dg.drop_delta);
            if self.tally.first_recv_ts_ns == 0 {
                self.tally.first_recv_ts_ns = dg.recv_ts_ns;
            }
            self.tally.last_recv_ts_ns = dg.recv_ts_ns;
            self.tally.coverage.observe(&dg);
        }
        Ok(self.ring.in_hand())
    }
}

/// One window's datagrams, kept so that the window can be read a second time.
///
/// # Why a window is read twice
///
/// [`derive`] stamps its manifest — the window key, the window sequence, the
/// start and the end — onto every row as it reads, so the manifest has to exist
/// before the first datagram is taken. A window's manifest describes what the
/// window saw, and a window has seen nothing until it has been walked. One pass
/// cannot satisfy both, and an object never had to: the writer counted as it
/// wrote the file and the loader walks the finished file again.
///
/// A window is what replaces the object, so the window is what has to be
/// readable twice. This is that: [`fill`](Self::fill) drains the ring and
/// completes the tally, and [`replay`](Self::replay) hands the same datagrams
/// back in arrival order.
///
/// **What one pass writes instead is not a slower derivation.** It is a manifest
/// built from [`WindowTally::default`]: `start_ns` and `end_ns` at zero, so
/// every coverage row is stamped at the Unix epoch; no `instances`, so no
/// coverage row is written at all; and a window key of `live/…/0-<window_seq>`,
/// which is one key for window *k* of every run this recorder ever makes. That
/// last is the loss — the coverage grain is deduplicated on a sort key ending in
/// the start stamp, so a second run's window replaces the first's rather than
/// standing beside it.
///
/// # It is reused, window after window
///
/// The buffer belongs to the derivation stage and outlives every window taken
/// from it. Slots are refilled through the ring's own [`refill`], so a window
/// after the first costs a copy per datagram and not an allocation — the
/// discipline the ring's pooled slots follow, for the same reason, one thread
/// further along.
///
/// [`derive`]: dz_recorder_rows::derive
/// [`refill`]: crate::ring
#[derive(Debug, Default)]
pub struct HeldWindow {
    /// Every slot ever needed, in arrival order for the first `filled` of them.
    /// The rest are slots an earlier, longer window left behind, kept for the
    /// payload capacity they have already grown to.
    datagrams: Vec<OwnedDatagram>,
    filled: usize,
}

impl HeldWindow {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            datagrams: Vec::new(),
            filled: 0,
        }
    }

    /// Drains `window` to its close, keeping every datagram it handed over.
    ///
    /// After this the window's [`tally`](WindowSource::tally),
    /// [`closed`](WindowSource::closed) and [`is_empty`](WindowSource::is_empty)
    /// all describe a completed window, which is when a manifest may be built
    /// from them.
    ///
    /// # Errors
    ///
    /// Whatever the window's source failed with. Nothing is retained: a window
    /// that did not reach its close may not be derived, because the tally
    /// describing it would be short by however much was not read.
    pub fn fill(&mut self, window: &mut WindowSource<'_>) -> Result<(), SourceError> {
        self.filled = 0;
        while let Some(dg) = window.next()? {
            if self.filled == self.datagrams.len() {
                // Nothing reserved: a window of small datagrams holds many
                // slots, and reserving a datagram's worth on each would cost
                // orders of magnitude more than the window's own bound.
                self.datagrams.push(crate::ring::slot(0));
            }
            // `dg.drop_delta` verbatim: it is already what the ring rewrote it
            // to, and the debt the ring folded in is part of what this window
            // has to admit.
            crate::ring::refill(&mut self.datagrams[self.filled], &dg, dg.drop_delta);
            self.filled += 1;
        }
        Ok(())
    }

    /// The datagrams [`fill`](Self::fill) kept, as a source that ends.
    #[must_use]
    pub fn replay(&self) -> Replay<'_> {
        Replay {
            datagrams: &self.datagrams[..self.filled],
            next: 0,
        }
    }
}

/// A [`HeldWindow`]'s second pass: the same datagrams, in arrival order.
#[derive(Debug)]
pub struct Replay<'a> {
    datagrams: &'a [OwnedDatagram],
    next: usize,
}

impl Source for Replay<'_> {
    fn next(&mut self) -> Result<Option<RecordedDatagram<'_>>, SourceError> {
        // Copied out of `self` before the cursor moves: the borrow this returns
        // comes from the buffer and not from the receiver, which is what lets a
        // caller advance and read in one expression.
        let datagrams = self.datagrams;
        let Some(dg) = datagrams.get(self.next) else {
            return Ok(None);
        };
        self.next += 1;
        Ok(Some(dg.as_recorded()))
    }
}
