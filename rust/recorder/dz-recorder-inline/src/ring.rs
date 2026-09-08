//! The hand-off between the capture thread and the derivation thread, and the
//! loss it owes when it cannot make one.
//!
//! # Why this is the most delicate module in the crate
//!
//! Inline mode puts a derivation behind a live capture, which means there is now
//! a place a datagram can be lost that did not exist before. **A datagram the
//! deriver never sees is a sequence value nobody delivered, and a sequence value
//! nobody delivered with nothing admitted behind it is a `publisher` verdict on
//! a gap the recorder caused.** The whole point of this recorder is to make loss
//! attributable; a ring that dropped silently would make it attributable to the
//! wrong party, which is worse than not measuring it at all.
//!
//! So a drop here is charged, through [`PendingLoss`] — the same accumulator the
//! capture already uses one layer down, for the same reason and with the same
//! arithmetic. The debt rides on the `drop_delta` of the next datagram that gets
//! through, which is exactly what that field is defined as: datagrams lost
//! between the previous one and this one.
//!
//! # It never blocks, and that is not a performance choice
//!
//! A capture thread that waited for a slot would stop draining its receive
//! queue, the queue would overflow, and a storage or destination problem would
//! become feed loss plus a false publisher-loss finding in every window derived
//! during it. So a full ring is a drop and a counter — the same rule the
//! archive's staging watermark applies to bytes, applied here to datagrams.
//!
//! # The slots are pooled
//!
//! [`OwnedDatagram::from_recorded`] allocates, and an allocation per datagram on
//! the capture thread is a regression against a path that today costs a copy and
//! a buffered write. Every slot is allocated once, at construction, and returned
//! to a free list when the deriver is done with it: the copy stays, the
//! allocation does not.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::Arc;
use std::time::Duration;

use dz_edge_core::MAX_DATAGRAM_SIZE;
use dz_recorder_capture::PendingLoss;
use dz_recorder_core::{OwnedDatagram, RecordedDatagram, RecvTsKind};

use std::net::SocketAddrV4;

/// The link headers a capture may hand over, sized so a slot holding them
/// allocates once. Ethernet, IPv4 with options, and UDP.
const LINK_HEADER_CAP: usize = 64;

/// What one offer to the ring came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Offered {
    Accepted,
    /// The derivation is behind. The datagram is gone and its loss is owed to
    /// the next one that gets through.
    Dropped,
    /// The derivation thread is gone. Nothing will carry anything.
    Disconnected,
}

/// The ring's own counters, cumulative and never reset.
///
/// Cumulative for the reason every other counter in this recorder is: a total
/// that has not moved in a day says nothing about health now, and a small one
/// that is climbing says everything. **Alert on the delta.**
#[derive(Debug, Default)]
pub struct RingCounters {
    accepted: AtomicU64,
    dropped: AtomicU64,
}

impl RingCounters {
    /// Datagrams handed to the derivation.
    #[must_use]
    pub fn accepted(&self) -> u64 {
        self.accepted.load(Ordering::Relaxed)
    }

    /// Datagrams the derivation never saw, every one of them admitted in the
    /// `drop_delta` of a later datagram.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// The capture thread's end.
///
/// Not `Sync`, and not meant to be: one capture thread owns one of these, and
/// the debt it carries is only correct if a single writer decides the order.
pub struct RingSender {
    full: SyncSender<OwnedDatagram>,
    free: Receiver<OwnedDatagram>,
    /// The free list's own sending end, held so that the unreachable branch in
    /// [`offer`](Self::offer) can put a slot back instead of dropping it. A
    /// dropped slot is a permanent loss of ring capacity that nothing reports —
    /// the pool would shrink by one and the feed would simply tolerate a little
    /// less lag from then on, which is the kind of quiet degradation this
    /// recorder refuses everywhere else.
    free_return: SyncSender<OwnedDatagram>,
    /// Loss owed to the next datagram that reaches the derivation.
    pending: PendingLoss,
    counters: Arc<RingCounters>,
}

/// The derivation thread's end.
pub struct RingReceiver {
    full: Receiver<OwnedDatagram>,
    free: SyncSender<OwnedDatagram>,
    /// The slot the last [`recv_within`](Self::recv_within) handed out, held so
    /// the borrow it returned stayed valid. Returned to the pool on the next
    /// call.
    current: Option<OwnedDatagram>,
    counters: Arc<RingCounters>,
}

/// What one wait on the ring found.
#[derive(Debug)]
pub enum Waited<'a> {
    Datagram(RecordedDatagram<'a>),
    /// The deadline passed with nothing waiting. A live feed may be quiet, so
    /// this is never an ending.
    TimedOut,
    /// The capture is gone and the ring is drained.
    Ended,
}

/// A ring of `capacity` pooled slots.
///
/// Every slot is allocated here. `capacity` is the number of datagrams the
/// derivation may fall behind by before the next one is dropped and owed, so it
/// is a latency budget rather than a memory one — though it is both, at roughly
/// `capacity × 1.3 KiB`.
///
/// # Panics
///
/// If `capacity` is zero. A ring that can hold nothing drops every datagram and
/// reports a feed nobody is deriving, which is the failure this crate is least
/// able to notice at runtime.
#[must_use]
pub fn ring(capacity: usize) -> (RingSender, RingReceiver) {
    assert!(capacity > 0, "a ring of zero slots derives nothing");

    // Both channels hold every slot, so neither send can ever fail for want of
    // room: a slot is in the free list, in the full list, or in the deriver's
    // hand, and never in two places.
    let (full_tx, full_rx) = sync_channel(capacity);
    let (free_tx, free_rx) = sync_channel(capacity);
    for _ in 0..capacity {
        let slot = OwnedDatagram {
            payload: Vec::with_capacity(MAX_DATAGRAM_SIZE),
            src: SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 0),
            dst: SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 0),
            role: dz_edge_core::PortRole::Mktdata,
            recv_ts_ns: 0,
            recv_ts_kind: RecvTsKind::ApplicationFallback,
            drop_delta: 0,
            ttl: None,
            link_headers: None,
            wire_payload_len: 0,
        };
        free_tx
            .try_send(slot)
            .expect("the free list holds every slot it was sized for");
    }

    let counters = Arc::new(RingCounters::default());
    (
        RingSender {
            full: full_tx,
            free: free_rx,
            free_return: free_tx.clone(),
            pending: PendingLoss::new(),
            counters: Arc::clone(&counters),
        },
        RingReceiver {
            full: full_rx,
            free: free_tx,
            current: None,
            counters,
        },
    )
}

impl RingSender {
    /// Offers a datagram to the derivation, never waiting for it.
    ///
    /// **The datagram's own admitted loss is owed to the accumulator before the
    /// offer is made, not after it succeeds.** The offer may fail, and a delta
    /// that left on a datagram which did not get through is loss the rows never
    /// hear about — a gap with nothing admitted behind it, which is how a
    /// recorder's own drop becomes a publisher's finding. This is the same order
    /// `dz-recorder-capture` hands a datagram to the record loop in, for the
    /// same reason.
    pub fn offer(&mut self, dg: &RecordedDatagram<'_>) -> Offered {
        self.pending.owe(dg.drop_delta);

        let mut slot = match self.free.try_recv() {
            Ok(slot) => slot,
            // No slot free: the derivation is behind. This datagram is itself
            // one more lost between the previous one and the next, and
            // everything it declared is still owed.
            Err(TryRecvError::Empty) => {
                self.pending.undelivered();
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
                return Offered::Dropped;
            }
            Err(TryRecvError::Disconnected) => return Offered::Disconnected,
        };

        refill(&mut slot, dg, self.pending.owed());
        match self.full.try_send(slot) {
            Ok(()) => {
                // It reached the derivation, so the debt travelled with it.
                self.pending.settled();
                self.counters.accepted.fetch_add(1, Ordering::Relaxed);
                Offered::Accepted
            }
            // Unreachable while both channels hold every slot, and handled
            // rather than asserted: a panic on the capture thread over an
            // accounting slip would stop the recorder, which is the failure
            // every rule in this file is arranged against.
            Err(TrySendError::Full(slot)) => {
                let _ = self.free_return.try_send(slot);
                self.pending.undelivered();
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
                Offered::Dropped
            }
            Err(TrySendError::Disconnected(_)) => Offered::Disconnected,
        }
    }

    /// Loss owed to the next datagram that gets through.
    ///
    /// Exposed for the tests and for a shutdown that wants to know it is walking
    /// away from an admission nothing will now carry.
    #[must_use]
    pub const fn owed(&self) -> u32 {
        self.pending.owed()
    }

    #[must_use]
    pub fn counters(&self) -> &Arc<RingCounters> {
        &self.counters
    }
}

impl RingReceiver {
    /// Waits at most `timeout` for the next datagram.
    ///
    /// The slot handed out by the previous call goes back to the pool here,
    /// which is why the borrow this returns ends at the next call and not
    /// before: a caller that wants to keep a datagram must copy it.
    ///
    /// **One slot is therefore always in hand**, so a producer sees at most
    /// `capacity - 1` free while the deriver is holding a datagram. That is not
    /// an off-by-one to fix — it is what makes the borrow safe — but it does
    /// mean a ring of one accepts a datagram only when the deriver has asked for
    /// the next one, and a capacity worth configuring starts well above that.
    pub fn recv_within(&mut self, timeout: Duration) -> Waited<'_> {
        if let Some(slot) = self.current.take() {
            // The pool holds every slot, so this cannot fail for want of room;
            // a disconnected capture means nothing will ask for it again.
            let _ = self.free.try_send(slot);
        }
        match self.full.recv_timeout(timeout) {
            Ok(slot) => {
                self.current = Some(slot);
                let held = self
                    .current
                    .as_ref()
                    .expect("the slot was just placed in hand");
                Waited::Datagram(held.as_recorded())
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Waited::TimedOut,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Waited::Ended,
        }
    }

    #[must_use]
    pub fn counters(&self) -> &Arc<RingCounters> {
        &self.counters
    }
}

/// Refills a pooled slot from a borrowed datagram, keeping its buffers.
///
/// Field for field rather than by assignment from a constructed value, because
/// the whole point is that `payload` and `link_headers` keep the capacity they
/// were allocated with.
fn refill(slot: &mut OwnedDatagram, dg: &RecordedDatagram<'_>, drop_delta: u32) {
    slot.payload.clear();
    slot.payload.extend_from_slice(dg.payload);

    match dg.link_headers {
        Some(headers) => {
            let buf = slot
                .link_headers
                .get_or_insert_with(|| Vec::with_capacity(LINK_HEADER_CAP));
            buf.clear();
            buf.extend_from_slice(headers);
        }
        // `None` means the capture synthesises them, and that is a property of
        // the mode rather than of the datagram — so in a given run this branch
        // is taken every time or never, and the buffer it drops is one that was
        // never allocated.
        None => slot.link_headers = None,
    }

    slot.src = dg.src;
    slot.dst = dg.dst;
    slot.role = dg.role;
    slot.recv_ts_ns = dg.recv_ts_ns;
    slot.recv_ts_kind = dg.recv_ts_kind;
    slot.ttl = dg.ttl;
    slot.wire_payload_len = dg.wire_payload_len;
    slot.drop_delta = drop_delta;
}
