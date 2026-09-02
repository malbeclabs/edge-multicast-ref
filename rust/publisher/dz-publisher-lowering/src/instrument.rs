//! What the lowering must know about an instrument, and how it resolves a
//! handle to it.

use dz_adapter_core::InstrumentRef;

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
}

impl InstrumentTable {
    /// An empty table.
    #[must_use]
    pub const fn new() -> Self {
        Self { slots: Vec::new() }
    }

    /// Admit an instrument and mint the handle the adapter will carry for it.
    ///
    /// **For the reference-data owner.** The handle is the index of the slot
    /// this instrument now owns for the lifetime of the table.
    pub fn admit(&mut self, instrument: Instrument) -> InstrumentRef {
        let index = self.slots.len();
        self.slots.push(Some(instrument));
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
        if let Some(slot) = self.slots.get_mut(instrument.index() as usize) {
            *slot = None;
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

    /// How many instruments the table currently holds.
    ///
    /// Withdrawn slots are not counted, so this is the published set and not
    /// the number of handles ever minted.
    #[must_use]
    pub fn len(&self) -> usize {
        self.slots.iter().flatten().count()
    }

    /// Whether the table holds nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
