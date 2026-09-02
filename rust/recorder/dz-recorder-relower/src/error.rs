//! Why a comparison could not be run at all.
//!
//! Everything a comparison *finds* is a [`Finding`](crate::Finding) or a
//! [`Caveat`](crate::Caveat), not an error. These are the conditions under which
//! there is no comparison to report: an archive that failed before it was
//! exhausted, or a publisher identity the capture cannot state.

use dz_recorder_core::SourceError;
use thiserror::Error;

/// A comparison could not be run.
#[derive(Debug, Error)]
pub enum RelowerError {
    /// The multicast archive failed before it was exhausted.
    ///
    /// Returned rather than tolerated, and for the reason
    /// `dz-recorder-loss` returns its own: a short read taken for a complete
    /// window turns our own truncation into a publisher finding. Every message
    /// after the tear would be reported as *in the re-lowered stream, not on the
    /// wire*, which is the strongest accusation this tool can make.
    #[error("the multicast archive failed before it was exhausted: {0}")]
    MulticastArchive(#[source] SourceError),

    /// The upstream-payload archive failed before it was exhausted.
    ///
    /// The mirror image, and the mirror accusation: every message after the tear
    /// is on the wire and absent from the re-lowering, which reads as a
    /// publisher inventing traffic.
    #[error("the upstream-payload archive failed before it was exhausted: {0}")]
    PayloadArchive(#[source] SourceError),

    /// The capture carries no `Source ID` this build can lower with.
    ///
    /// The publisher's identity is on the wire — in every `InstrumentDefinition`
    /// at schema 3 and in every `Quote`, `Trade` and `LevelUpdate` — so it is
    /// reconstructed from the capture like everything else. A window that
    /// carries none, or carries only the reserved `0` a schema-1 definition
    /// leaves there, cannot be re-lowered: the re-lowered copy would differ from
    /// the wire copy in `source_id` on every single message.
    #[error("the archive states no Source ID the registry admits (found {found:?})")]
    NoSourceIdInArchive { found: Vec<u16> },

    /// The capture carries more than one publisher identity.
    ///
    /// Two publishers on one channel is a finding in its own right and the
    /// health tier's to make; it is not something to average over. A comparison
    /// run against the mixture would attribute one publisher's messages to the
    /// other's mapping.
    #[error("the archive carries two Source IDs, {first} and {second}: it is not one publisher's")]
    AmbiguousSourceId { first: u16, second: u16 },
}
