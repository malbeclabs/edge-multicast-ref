//! Market by price: a normalized level or clear, lowered onto `0x40` and
//! `0x41`.

use dz_adapter_core::{
    Aggressor, ClearScope, Desync, InstrumentRef, Presence, Scalar, Side, TradeFlags,
};
use dz_edge_mbp::{
    BookClear, InstrumentReset, LevelUpdate, ACTION_CHANGE, ACTION_DELETE, ACTION_NEW,
    ACTION_UNKNOWN, CLEAR_ASK, CLEAR_BID, CLEAR_BOTH, RESET_UNSPECIFIED, RESET_UPSTREAM_GAP,
    RESET_VENUE_RESYNC, SCOPE_ENTIRE_SIDE, SCOPE_FROM_PRICE, SIDE_ASK, SIDE_BID, U16_UNAVAILABLE,
};

use crate::error::LoweringError;
use crate::instrument::InstrumentTable;
use crate::scale::{price_for, qty_for};
use crate::seq::PerInstrumentSeq;
use crate::source::SourceId;

/// The depth feed's lowering, and the sequence it stamps.
///
/// Stateful where [`Lowering`](crate::Lowering) is not, and the state is the
/// reason: `Per-Instrument Seq` is a counter, top-of-book has no such field,
/// and folding the two together would make the stateless path carry state for
/// nothing and stop it being `Copy`.
///
/// The instrument table arrives per call rather than being held — see
/// [`Lowering`](crate::Lowering) for why, and note that this is the type the
/// reason is really about: rebuilding it to release a borrow would restart the
/// sequence it carries.
#[derive(Debug)]
pub struct DepthLowering {
    source_id: SourceId,
    seq: PerInstrumentSeq,
    next_snapshot_id: u32,
}

impl DepthLowering {
    /// Bind the publisher's own `Source ID`.
    #[must_use]
    pub const fn new(source_id: SourceId) -> Self {
        Self {
            source_id,
            seq: PerInstrumentSeq::new(),
            // Snapshot ids tie a begin, its levels and its end together, so
            // that two overlapping snapshots for one instrument cannot be
            // interleaved into one wrong book. Starting at 1 leaves 0 meaning
            // "no snapshot", which is what an uninitialised field would read
            // as.
            next_snapshot_id: 1,
        }
    }

    /// The sequence this lowering stamps, for the runtime that owns the channel.
    ///
    /// Exposed so a `Reset Count` change can end the era — see
    /// [`PerInstrumentSeq::end_era`] for the two things that do not.
    pub fn sequence_mut(&mut self) -> &mut PerInstrumentSeq {
        &mut self.seq
    }

    /// `Event::Level` to `0x40 LevelUpdate`.
    ///
    /// # `Action` is derived here, and this is the derivation
    ///
    /// **A quantity of zero is a removal and nothing else can be.** A non-zero
    /// quantity takes the adapter's [`Presence`] hint, and `Unknown` is a
    /// conformant answer for an upstream that does not distinguish an insertion
    /// from a change. Written this way round, the two pairings the
    /// specification forbids — a removal carrying any other action, and a
    /// removal action carrying quantity — have no representation at this
    /// boundary rather than being merely refused here.
    ///
    /// That is aimed at a defect that reached live traffic: a publisher
    /// numbering the action table from `New` instead of `Unknown` emitted every
    /// removal as a change carrying zero. Self-consistent, so invisible to any
    /// test that encodes and then decodes — subscribers applied by quantity and
    /// built correct books while every consumer reading the field was quietly
    /// wrong. There is one derivation now, for every venue, and no venue can
    /// reach the byte.
    ///
    /// # `Order Count`'s sentinel is not top-of-book's
    ///
    /// This feed says "not exposed" with [`U16_UNAVAILABLE`], and `0` is a
    /// **real value** here. The top-of-book feed's `Source Count` says the
    /// opposite with the opposite value: `0` is its "unavailable". Two specs,
    /// one question, opposite answers — which is a trap for anyone normalising
    /// the two fields into one, and the reason each is written out here rather
    /// than shared through a helper that would have to pick a side.
    ///
    /// # Errors
    ///
    /// [`LoweringError::UnknownInstrument`] for a handle the table does not
    /// hold; [`LoweringError::Scale`] naming the field for a price or quantity
    /// that cannot be stated exactly at this instrument's exponent. The
    /// sequence number is taken **after** both, so a refused message consumes
    /// none: a number spent on a message that never reached the wire is a gap
    /// every subscriber reads as packet loss.
    // The parameters are exactly the fields of `Event::Level`. Grouping them
    // into a struct would be a second definition of that variant, in another
    // crate, free to drift from it.
    #[allow(clippy::too_many_arguments)]
    pub fn lower_level(
        &mut self,
        instruments: &InstrumentTable,
        instrument: InstrumentRef,
        source_ts_ns: u64,
        side: Side,
        px: Scalar<'_>,
        qty: Scalar<'_>,
        order_count: Option<u16>,
        presence: Presence,
    ) -> Result<LevelUpdate, LoweringError> {
        let inst = *instruments.get(instrument)?;

        let price_raw = price_for(&inst, px, "price")?;
        let qty_raw = qty_for(&inst, qty, "qty")?;

        Ok(LevelUpdate {
            instrument_id: inst.instrument_id,
            source_id: self.source_id.get(),
            side: side_byte(side),
            action: action_byte(qty_raw, presence),
            per_instrument_seq: self.seq.stamp(instrument),
            price_raw,
            qty_raw,
            timestamp_ns: source_ts_ns,
            order_count: order_count.unwrap_or(U16_UNAVAILABLE),
            // Informational, and nothing at the adapter boundary can state it:
            // a level's rank at emission time is a property of the publisher's
            // own book as it emits, not of the venue's event. Absent is what
            // the specification defines this value for.
            level_index: U16_UNAVAILABLE,
            // Neither is expressible at the boundary, and both are
            // informational. Zero is each one's defined default.
            update_reason: 0,
            level_flags: 0,
        })
    }

    /// `Event::Clear` to `0x41 BookClear`.
    ///
    /// **Not a resynchronisation signal**, and the runtime must not turn it
    /// into one: it says these levels are gone, not that the book a subscriber
    /// holds is untrustworthy. A subscriber reading it as a reset throws away a
    /// book it could have kept and asks for a snapshot nobody needed to send.
    ///
    /// The one pairing this feed forbids — a clear bounded by a price that
    /// applies to both sides, which has no meaning two implementations would
    /// agree on — is unreachable rather than refused: [`ClearScope::FromPrice`]
    /// names exactly one side, and there is no variant that bounds both. The
    /// codec refuses those bytes at the push as well, and the two are not
    /// redundant: that one governs what someone else sent us.
    ///
    /// # Errors
    ///
    /// As [`lower_level`](Self::lower_level).
    pub fn lower_clear(
        &mut self,
        instruments: &InstrumentTable,
        instrument: InstrumentRef,
        source_ts_ns: u64,
        scope: ClearScope<'_>,
    ) -> Result<BookClear, LoweringError> {
        let inst = *instruments.get(instrument)?;

        let (clear_side, wire_scope, from_price_raw) = match scope {
            ClearScope::EntireSide(side) => (side_clear_byte(side), SCOPE_ENTIRE_SIDE, 0),
            ClearScope::BothSides => (CLEAR_BOTH, SCOPE_ENTIRE_SIDE, 0),
            ClearScope::FromPrice { side, px } => (
                side_clear_byte(side),
                SCOPE_FROM_PRICE,
                price_for(&inst, px, "from_price")?,
            ),
        };

        Ok(BookClear {
            instrument_id: inst.instrument_id,
            source_id: self.source_id.get(),
            clear_side,
            scope: wire_scope,
            // The same series as `LevelUpdate`, because both mutate the book
            // and their relative order is significant.
            per_instrument_seq: self.seq.stamp(instrument),
            from_price_raw,
            timestamp_ns: source_ts_ns,
            // Informational, and not expressible at the boundary.
            clear_reason: 0,
        })
    }

    /// The sequence, read-only, for the framer's `Last Instrument Seq`.
    pub(crate) const fn sequence(&self) -> &PerInstrumentSeq {
        &self.seq
    }

    /// `Event::Trade` to `0x04 Trade`, on a depth channel.
    ///
    /// **The same function the top-of-book channel calls.** This feed carries
    /// `0x04` as well — a trade tape beside the book, for consumers who want
    /// one — and the wire's cross-specification policy requires the bytes to be
    /// identical to the top-of-book feed's for the same execution. So this is a
    /// call to [`crate::trade::lower`] and not a second encoder, which is the
    /// whole distinction: in one existing publisher that obligation is a doc
    /// comment holding two implementations to each other by hand.
    ///
    /// Note what it does **not** take: a `Per-Instrument Seq`. That series
    /// belongs to the messages that mutate the book, and a trade does not.
    /// Spending a number on one would put a gap in a series every subscriber
    /// reads for loss.
    ///
    /// # Errors
    ///
    /// As [`lower_level`](Self::lower_level).
    #[allow(clippy::too_many_arguments)]
    pub fn lower_trade(
        &self,
        instruments: &InstrumentTable,
        instrument: InstrumentRef,
        source_ts_ns: u64,
        px: Scalar<'_>,
        qty: Scalar<'_>,
        aggressor: Aggressor,
        trade_id: Option<u64>,
        cumulative_volume: Option<Scalar<'_>>,
        flags: TradeFlags,
    ) -> Result<dz_edge_tob::Trade, LoweringError> {
        crate::trade::lower(
            self.source_id,
            instruments,
            instrument,
            source_ts_ns,
            px,
            qty,
            aggressor,
            trade_id,
            cumulative_volume,
            flags,
        )
    }

    /// `EventSink::desynchronised` to `0x14 InstrumentReset`.
    ///
    /// **The reason is derived from what the adapter could say, plus what only
    /// the runtime knows.** A venue can report that its upstream resynchronised
    /// or that it saw a gap; it cannot report a publisher-side integrity
    /// failure, because that is a statement about us. So the two venue
    /// observations map straight through and `Unspecified` stays unspecified —
    /// this layer does not upgrade a silence into a claim about which of the
    /// five reasons it was.
    ///
    /// `sequence_number` is the number the datagram carrying this message will
    /// take, and it becomes `New Anchor Seq`. That is not a diagnostic: the
    /// reset takes effect immediately, so the anchor is where the stream is
    /// *now*, and the specification's own conformance subscriber grades a
    /// mismatch a violation. The caller passes it because the send path is the
    /// only layer that knows it.
    ///
    /// # What this obliges the caller to do next
    ///
    /// Emit a snapshot for this instrument with `Anchor Seq` equal to
    /// `sequence_number`, **before** resuming delta emission. A subscriber
    /// discards any snapshot for the instrument with an older anchor, so a
    /// reset whose promised snapshot never arrives leaves that instrument
    /// waiting forever — worse than the divergence it announced. Nothing here
    /// can enforce that; it is stated so that a caller cannot say it did not
    /// know.
    ///
    /// The per-instrument sequence is **not** touched. It restarts on a
    /// `Reset Count` change and at no other time, and an instrument reset is
    /// not one: the channel is intact and every other instrument on it is
    /// unaffected, which is the whole point of a per-instrument signal.
    ///
    /// # Errors
    ///
    /// [`LoweringError::UnknownInstrument`] for a handle the table does not
    /// hold.
    pub fn lower_instrument_reset(
        &mut self,
        instruments: &InstrumentTable,
        instrument: InstrumentRef,
        source_ts_ns: u64,
        reason: Desync,
        sequence_number: u64,
    ) -> Result<InstrumentReset, LoweringError> {
        let inst = *instruments.get(instrument)?;
        Ok(InstrumentReset::anchored_at(
            inst.instrument_id,
            reset_reason(reason),
            sequence_number,
            source_ts_ns,
        ))
    }

    /// Mint the next snapshot id, for the framer.
    pub(crate) fn take_snapshot_id(&mut self) -> u32 {
        let id = self.next_snapshot_id;
        self.next_snapshot_id = self.next_snapshot_id.wrapping_add(1);
        id
    }
}

/// The wire's `Side`. Exhaustive, so a third side fails to compile.
const fn side_byte(side: Side) -> u8 {
    match side {
        Side::Bid => SIDE_BID,
        Side::Ask => SIDE_ASK,
    }
}

/// The wire's `Clear Side` for one side. `Both` has no [`Side`] to come from,
/// which is why it is written at its own call site.
const fn side_clear_byte(side: Side) -> u8 {
    match side {
        Side::Bid => CLEAR_BID,
        Side::Ask => CLEAR_ASK,
    }
}

/// The whole `Action` derivation, in one place.
///
/// Zero first, and unconditionally, is what makes the illegal pairings
/// unreachable: no [`Presence`] can reach a removal, and a removal cannot
/// reach any other action.
const fn action_byte(qty_raw: u64, presence: Presence) -> u8 {
    if qty_raw == 0 {
        return ACTION_DELETE;
    }
    match presence {
        Presence::Unknown => ACTION_UNKNOWN,
        Presence::New => ACTION_NEW,
        Presence::Change => ACTION_CHANGE,
    }
}

/// The wire's reset reason for what a venue was able to say.
///
/// Exhaustive, so a fourth [`Desync`] fails to compile here rather than being
/// silently reported as unspecified. The two publisher-side values of the wire
/// table — an integrity failure and a venue-specific code — are absent because
/// no adapter can reach them: they are statements about the publisher, and a
/// runtime that makes one composes the message itself.
const fn reset_reason(reason: Desync) -> u8 {
    match reason {
        Desync::Unspecified => RESET_UNSPECIFIED,
        Desync::VenueResync => RESET_VENUE_RESYNC,
        Desync::UpstreamGap => RESET_UPSTREAM_GAP,
    }
}
