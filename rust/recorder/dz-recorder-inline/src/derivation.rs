//! One window, drained and then derived — the two passes, in one place.
//!
//! # Why this is a module and not four lines in the derivation stage
//!
//! The equivalence gate is the gate on the whole design: one synthetic feed
//! through both paths, and the row sets equal but for their provenance. It
//! cannot start a pipeline — it has no spool, no ledger and no destination — so
//! it builds a window of its own, and a gate that builds its own window is free
//! to build one the derivation stage does not have.
//!
//! It did. The gate walked the window, built the manifest from the completed
//! tally, and derived; the derivation stage built the manifest from a window it
//! had just opened. Both were green, and the shape under test was the one
//! nothing ran. **A fixture that supplies the correctness under test is the one
//! failure a gate cannot report, because it looks exactly like a pass** — so the
//! two passes live here, and both callers call them.
//!
//! [`HeldWindow`] is why there are two passes at all.

use dz_recorder_rows::{derive, Derivation, DeriveError, DeriveInput, Derived, SegmentTrailer};

use crate::manifest::{window_key, window_manifest, WindowIdentity};
use crate::window::{HeldWindow, WindowSource};

/// The buffer the two passes share, and the derivation over it.
///
/// Held by the derivation stage across every window of a run, and across a
/// restart of that stage, so that a window after the first costs a copy per
/// datagram rather than an allocation.
#[derive(Debug, Default)]
pub struct WindowDeriver {
    held: HeldWindow,
}

impl WindowDeriver {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            held: HeldWindow::new(),
        }
    }

    /// Drains `window`, builds its manifest, and derives from what was drained.
    ///
    /// In that order, and the order is the whole of it: a manifest describes
    /// what the window saw, and [`derive`] stamps that manifest onto every row
    /// as it reads. On return the window's tally, close reason and emptiness all
    /// describe a completed window, so a caller reads them afterwards.
    ///
    /// `preceding` is the previous window's trailer, and `None` there means
    /// *unknown* rather than *there was none*.
    ///
    /// # Errors
    ///
    /// [`DeriveError::Source`] if the window did not reach its close, and
    /// whatever `derive` refuses the drained datagrams for. A window that was
    /// not read to its end is not derived at all: its tally would be short by
    /// however much was left, and a manifest short by that much describes a
    /// window nobody captured.
    pub fn derive_window(
        &mut self,
        window: &mut WindowSource<'_>,
        identity: &WindowIdentity<'_>,
        window_seq: u64,
        preceding: Option<&SegmentTrailer>,
    ) -> Result<Derived, DeriveError> {
        self.held
            .fill(window)
            .map_err(|source| DeriveError::Source {
                // The key of the window that was not completed, from the tally
                // as far as it got: there is no manifest yet, and the error
                // still has to name which window it is about.
                object_key: window_key(identity, window.tally().first_recv_ts_ns, window_seq),
                source,
            })?;

        let manifest = window_manifest(identity, window.tally(), window_seq);
        let mut replay = self.held.replay();
        derive(
            &mut replay,
            &DeriveInput {
                manifest: &manifest,
                // The capture's own scope, carried on the identity rather than
                // passed twice: two callers with two answers would be two
                // claims about one capture.
                drop_scope: identity.drop_scope,
                preceding,
                derivation: Derivation::Live,
            },
        )
    }
}
