//! Framing a pulled snapshot: begin, the levels, end.

use dz_adapter_core::{InstrumentRef, Scalar, Side, SnapshotSink};
use dz_edge_mbp::{SnapshotBegin, SnapshotEnd, SnapshotLevel, SIDE_ASK, SIDE_BID, U16_UNAVAILABLE};

use crate::depth::DepthLowering;
use crate::error::LoweringError;
use crate::instrument::Instrument;
use crate::scale::{price_for, qty_for};

/// One instrument's book state, framed.
///
/// Three message types rather than one, because a snapshot is one book state
/// cut across datagrams and a subscriber has to be able to tell whether it
/// received all of it. The begin declares how many levels are coming and which
/// point in the live stream the result is true as of; the end repeats the
/// identifiers so a subscriber that lost either one knows it did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub begin: SnapshotBegin,
    pub levels: Vec<SnapshotLevel>,
    pub end: SnapshotEnd,
}

/// The [`SnapshotSink`] an adapter writes its book into, and the framing around
/// it.
///
/// **The snapshot is pulled, not pushed**, and this type is the seam. The
/// cadence, the rotation across instruments and the framing belong to the
/// runtime, because they are what a subscriber's recovery depends on; the book
/// belongs to the adapter, because it is the venue's microstructure. Neither
/// can drive the other, so the runtime asks — and what it asks with is this.
///
/// # Why a refusal is recorded rather than returned
///
/// [`SnapshotSink::level`] cannot fail: it is called from inside the adapter's
/// own loop over its book, and an adapter has nothing useful to do with a
/// scaling refusal on one level. So a refusal is kept and surfaced by
/// [`finish`](Self::finish), which is the point where the runtime can count it
/// and skip the instrument. The alternative — a sink that swallows what it
/// cannot convert — would emit a snapshot missing a level while declaring a
/// count that included it, and a subscriber applying that has a book that never
/// existed.
#[derive(Debug)]
pub struct SnapshotFramer {
    instrument_id: u32,
    /// The whole instrument rather than its two exponents, because a venue
    /// quoting per contract needs the factor applied here too - a snapshot in
    /// different units from the deltas it anchors is a book that never existed.
    instrument: Instrument,
    anchor_seq: u64,
    snapshot_id: u32,
    last_instrument_seq: u32,
    timestamp_ns: u64,
    depth_bound: u32,
    levels: Vec<SnapshotLevel>,
    /// The first refusal, kept. The first rather than the last, because it is
    /// the one whose cause the operator can still reason about.
    refused: Option<LoweringError>,
}

impl SnapshotSink for SnapshotFramer {
    fn level(&mut self, side: Side, px: Scalar<'_>, qty: Scalar<'_>, order_count: Option<u16>) {
        if self.refused.is_some() {
            return;
        }

        let price_raw = match price_for(&self.instrument, px, "snapshot_price") {
            Ok(raw) => raw,
            Err(error) => {
                self.refused = Some(error);
                return;
            }
        };
        let qty_raw = match qty_for(&self.instrument, qty, "snapshot_qty") {
            Ok(raw) => raw,
            Err(error) => {
                self.refused = Some(error);
                return;
            }
        };

        self.levels.push(SnapshotLevel {
            snapshot_id: self.snapshot_id,
            price_raw,
            qty_raw,
            // This feed's sentinel for "not exposed", and the opposite value
            // from the one top-of-book uses for the same question. See
            // `DepthLowering::lower_level`.
            order_count: order_count.unwrap_or(U16_UNAVAILABLE),
            side: match side {
                Side::Bid => SIDE_BID,
                Side::Ask => SIDE_ASK,
            },
            level_flags: 0,
        });
    }
}

impl SnapshotFramer {
    /// Close the snapshot.
    ///
    /// The level count the begin declares is what was actually written, so a
    /// subscriber counting fewer than promised has genuinely lost one rather
    /// than been told a number the publisher invented.
    ///
    /// # Errors
    ///
    /// [`LoweringError::Scale`] for the first level that could not be stated
    /// exactly at this instrument's exponents. Nothing partial is returned: an
    /// incomplete snapshot is worse than none, because a subscriber cannot tell
    /// the difference.
    pub fn finish(self) -> Result<Snapshot, LoweringError> {
        if let Some(error) = self.refused {
            return Err(error);
        }

        // `u32` is what the wire carries. A book with more than 2^32 levels is
        // past what a snapshot can declare, and saturating would declare a
        // count that does not match what follows - which is the one thing the
        // field exists to make detectable.
        let total_levels = u32::try_from(self.levels.len()).map_err(|_| LoweringError::Scale {
            field: "total_levels",
            source: dz_edge_core::fixed_point::ScaleError::Overflow,
        })?;

        Ok(Snapshot {
            begin: SnapshotBegin {
                instrument_id: self.instrument_id,
                anchor_seq: self.anchor_seq,
                total_levels,
                snapshot_id: self.snapshot_id,
                last_instrument_seq: self.last_instrument_seq,
                timestamp_ns: self.timestamp_ns,
                depth_bound: self.depth_bound,
            },
            levels: self.levels,
            end: SnapshotEnd {
                instrument_id: self.instrument_id,
                anchor_seq: self.anchor_seq,
                snapshot_id: self.snapshot_id,
            },
        })
    }
}

impl DepthLowering<'_> {
    /// Open a snapshot for one instrument, to hand to
    /// [`Adapter::snapshot`](dz_adapter_core::Adapter::snapshot).
    ///
    /// `anchor_seq` is the channel sequence number the resulting book state is
    /// true as of, and `depth_bound` how deep the publisher's book goes — both
    /// the runtime's, because both are what a subscriber's recovery depends on.
    ///
    /// `Last Instrument Seq` is filled from the sequence this lowering has been
    /// stamping, which is the whole reason the counter lives here: it is the
    /// value a subscriber initialises its own tracker to after applying the
    /// snapshot, and it is `0` when no delta has been sent for this instrument
    /// in the current era. **The counter is not reset by opening a snapshot**;
    /// see [`PerInstrumentSeq`](crate::PerInstrumentSeq) for why that would
    /// make a duplicate indistinguishable from a fresh delta.
    ///
    /// # Errors
    ///
    /// [`LoweringError::UnknownInstrument`] for a handle the table does not
    /// hold.
    pub fn open_snapshot(
        &mut self,
        instrument: InstrumentRef,
        anchor_seq: u64,
        timestamp_ns: u64,
        depth_bound: u32,
    ) -> Result<SnapshotFramer, LoweringError> {
        let inst = *self.table().get(instrument)?;
        let last_instrument_seq = self.sequence().last(instrument);
        let snapshot_id = self.take_snapshot_id();

        Ok(SnapshotFramer {
            instrument_id: inst.instrument_id,
            instrument: inst,
            anchor_seq,
            snapshot_id,
            last_instrument_seq,
            timestamp_ns,
            depth_bound,
            levels: Vec::new(),
            refused: None,
        })
    }
}
