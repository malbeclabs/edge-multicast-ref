//! What the lowering must know about an instrument, and how it resolves a
//! handle to it.

use dz_adapter_core::InstrumentRef;

use crate::contract::ContractSize;
use crate::error::LoweringError;

/// The three things about an admitted instrument that lowering an event needs.
///
/// The `Instrument ID` is the reference-data owner's to mint, persist and
/// publish; the exponents are what it published in that instrument's
/// `InstrumentDefinition`. All three arrive here from that owner and are read
/// on the hot path.
///
/// Both existing publishers are covered by holding the exponents per
/// instrument. One carries them per instrument already; the other fixes them as
/// constants for a whole product line, which is this shape with the same pair
/// repeated, and costs it nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Instrument {
    /// As minted and published by the reference-data owner.
    pub instrument_id: u32,
    /// The exponent every price for this instrument is carried at.
    pub price_exponent: i8,
    /// The exponent every quantity for this instrument is carried at.
    pub qty_exponent: i8,
    /// How much of the underlying one contract is, for a venue that quotes per
    /// contract and states the underlying's exponents above.
    ///
    /// `None` — the ordinary case — means the venue's numbers are already in
    /// the units the exponents describe. Read off the venue's
    /// `InstrumentSpec` at admission, so the hot path applies a factor rather
    /// than parsing one.
    pub quoted_per_contract: Option<ContractSize>,
}

/// `InstrumentRef` to [`Instrument`]: the runtime's own admitted set, as the
/// lowering reads it.
///
/// A dense `Vec` indexed by the handle, so the hot path costs a bounds check
/// and a load rather than a hash of a venue symbol. That density is the reason
/// [`InstrumentRef`] is an index in the first place, and it is why this table —
/// not the adapter — is what hands one out.
///
/// # A withdrawn instrument leaves a hole
///
/// Slots are never reused and never shift. A handle an adapter is still
/// carrying must not come to mean a different instrument because something else
/// was withdrawn, so [`withdraw`](Self::withdraw) empties the slot and leaves
/// it empty. The cost is one `Option` per instrument ever admitted; the
/// alternative is a quote published under an `Instrument ID` that belongs to
/// someone else.
#[derive(Debug, Clone, Default)]
pub struct InstrumentTable {
    slots: Vec<Option<Instrument>>,
    /// How many slots are held, maintained by `admit` and `withdraw`.
    ///
    /// **Cached because [`len`](Self::len) is on a tick path.** The snapshot
    /// rotation derives its per-instrument interval from the published set on
    /// every tick, and that tick's cost being O(1) in the size of the set is the
    /// invariant a large set is sized against — counting the held slots each
    /// time would have made it O(n) and quietly turned the pacing arithmetic
    /// into the most expensive thing in the loop. One `usize`, updated at the
    /// two places that can change it.
    held: usize,
}

impl InstrumentTable {
    /// An empty table.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            slots: Vec::new(),
            held: 0,
        }
    }

    /// Admit an instrument and mint the handle the adapter will carry for it.
    ///
    /// **For the reference-data owner.** The handle is the index of the slot
    /// this instrument now owns for the lifetime of the table.
    pub fn admit(&mut self, instrument: Instrument) -> InstrumentRef {
        let index = self.slots.len();
        self.slots.push(Some(instrument));
        self.held += 1;
        // A `u32` handle bounds the table at 2^32 instruments. Reaching it
        // would take more admissions than a publisher process can hold
        // definitions for, so the cast is a documented ceiling rather than a
        // truncation: `try_from` here would add an error case with no reachable
        // cause and no action.
        debug_assert!(index <= u32::MAX as usize, "instrument table overflowed");
        InstrumentRef::from_admission(index as u32)
    }

    /// Withdraw an instrument. Its handle resolves to nothing from here on.
    ///
    /// Idempotent, and silent for a handle the table never held: withdrawing
    /// something that is already gone is the state the caller asked for.
    pub fn withdraw(&mut self, instrument: InstrumentRef) {
        // `take` rather than an assignment, because the count may only move for
        // a slot that was actually held: withdrawing twice, or withdrawing a
        // handle this table never minted, is a state the caller asked for and
        // not an instrument leaving the published set.
        if let Some(slot) = self.slots.get_mut(instrument.index() as usize) {
            if slot.take().is_some() {
                self.held -= 1;
            }
        }
    }

    /// Resolve a handle, or refuse it.
    ///
    /// # Errors
    ///
    /// [`LoweringError::UnknownInstrument`] for a handle this table does not
    /// hold — one forged by an adapter, or one whose instrument has been
    /// withdrawn. See that variant for why it is reachable at all.
    pub fn get(&self, instrument: InstrumentRef) -> Result<&Instrument, LoweringError> {
        self.slots
            .get(instrument.index() as usize)
            .and_then(Option::as_ref)
            .ok_or(LoweringError::UnknownInstrument)
    }

    /// Restate an admitted instrument in place, keeping its handle.
    ///
    /// **For a venue that changed what it says about an instrument it is still
    /// streaming** — a re-declared exponent, or a contract factor the venue
    /// revised. The handle has to survive, because the adapter is carrying it:
    /// withdrawing and re-admitting would strand every copy it holds, and
    /// leaving the table alone while republishing the definition would put a
    /// definition on the wire declaring one scale while quotes went out at
    /// another, which is the worst of the three outcomes and the only silent
    /// one.
    ///
    /// Returns whether the handle was held. `false` is not an error: an
    /// instrument that has been withdrawn has nothing to restate, and the
    /// caller wanted the state it already has.
    ///
    /// This is a decision the caller has to have made, not a convenience — a
    /// scale that changes under a subscriber is a barrier event, and whether
    /// this is the right answer or a delist-and-relist is the reference-data
    /// owner's call.
    pub fn replace(&mut self, handle: InstrumentRef, instrument: Instrument) -> bool {
        match self.slots.get_mut(handle.index() as usize) {
            Some(slot @ Some(_)) => {
                *slot = Some(instrument);
                true
            }
            _ => false,
        }
    }

    /// How many instruments the table currently holds.
    ///
    /// Withdrawn slots are not counted, so this is the published set and not
    /// the number of handles ever minted.
    ///
    /// O(1), and it has to be: the snapshot rotation reads it on every tick to
    /// derive the per-instrument interval, and a walk of the slots there would
    /// make the pacing arithmetic the most expensive thing in a tick that is
    /// documented as O(1) in the published set. Maintained by
    /// [`admit`](Self::admit) and [`withdraw`](Self::withdraw); see `held`.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.held
    }

    /// Whether the table holds nothing.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.held == 0
    }

    /// How many handles have ever been minted, withdrawn ones included.
    ///
    /// The bound a cursor walking this table counts to, which is why it is not
    /// [`len`](Self::len): a handle is the index of its slot for the lifetime of
    /// the table, so a withdrawal leaves a hole rather than shifting the ones
    /// after it. A rotation that counted to `len` would stop short of the last
    /// instrument as soon as anything was delisted.
    #[must_use]
    pub const fn slots(&self) -> usize {
        self.slots.len()
    }

    /// Whether this handle resolves to an instrument.
    ///
    /// For a caller walking the table by index — a snapshot rotation — where
    /// resolving the instrument itself is not what is wanted and
    /// [`get`](Self::get)'s refusal would have to be discarded. Costs one bounds
    /// check, which is one of the two halves of a rotation tick being O(1) in
    /// the size of the published set; the other is [`len`](Self::len), which is
    /// a cached count for exactly the same reason.
    #[must_use]
    pub fn holds(&self, instrument: InstrumentRef) -> bool {
        self.slots
            .get(instrument.index() as usize)
            .is_some_and(Option::is_some)
    }
}
