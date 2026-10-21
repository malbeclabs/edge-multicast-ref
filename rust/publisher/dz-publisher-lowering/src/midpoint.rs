//! Midpoint: a normalized VWAP, lowered onto `0x03 Midpoint`.

use core::num::NonZeroU32;

use dz_adapter_core::{InstrumentRef, Scalar};
use dz_edge_midpoint::{Midpoint, METHOD_VWAP, QUALITY_STALE, QUALITY_SYNTHETIC};

use crate::error::LoweringError;
use crate::instrument::InstrumentTable;
use crate::scale::{price_for, qty_for};
use crate::source::SourceId;

/// Everything the midpoint lowering needs that is not in the event.
///
/// The table is an argument for the reason [`Lowering`](crate::Lowering)
/// gives.
#[derive(Debug, Clone, Copy)]
pub struct MidpointLowering {
    source_id: SourceId,
}

impl MidpointLowering {
    /// Bind the publisher's own `Source ID`, which is also the one in every
    /// `InstrumentDefinition` it publishes, as the specification requires.
    #[must_use]
    pub const fn new(source_id: SourceId) -> Self {
        Self { source_id }
    }

    /// `Event::Vwap` to `0x03 Midpoint`, method `5`.
    ///
    /// The quality flags are derived here. `synthetic` is set when the scaled
    /// volume is zero, which is the empty window the specification requires it
    /// for. The book-only bits have no input and are never set. The result
    /// passes [`Midpoint::check`] by construction.
    ///
    /// # Errors
    ///
    /// [`LoweringError::UnknownInstrument`] for a handle the table does not
    /// hold; [`LoweringError::Scale`] or [`LoweringError::InexactContract`]
    /// naming the field for a price or volume that cannot be stated exactly.
    // The parameters are exactly the fields of `Event::Vwap`, as for
    // `crate::trade::lower`.
    #[allow(clippy::too_many_arguments)]
    pub fn lower_vwap(
        &self,
        instruments: &InstrumentTable,
        instrument: InstrumentRef,
        window_end_ns: u64,
        window_ms: NonZeroU32,
        compute_ts_ns: u64,
        px: Scalar<'_>,
        volume: Scalar<'_>,
        stale: bool,
    ) -> Result<Midpoint, LoweringError> {
        let inst = instruments.get(instrument)?;

        let price = price_for(inst, px, "price")?;
        let volume = qty_for(inst, volume, "volume")?;

        let mut quality_flags = 0;
        if stale {
            quality_flags |= QUALITY_STALE;
        }
        if volume == 0 {
            quality_flags |= QUALITY_SYNTHETIC;
        }

        Ok(Midpoint {
            instrument_id: inst.instrument_id,
            source_id: self.source_id.get(),
            method: METHOD_VWAP,
            quality_flags,
            input_timestamp_ns: window_end_ns,
            compute_timestamp_ns: compute_ts_ns,
            price,
            window_ms: window_ms.get(),
            volume,
        })
    }
}
