//! Why an offered instrument was not admitted.

use dz_publisher_lowering::LoweringError;

/// The reasons [`ListingSink::list`](dz_adapter_core::ListingSink::list)
/// answers `None`.
///
/// The boundary documents a `None` as ordinary rather than as an error, and for
/// [`Capped`](Self::Capped) that is exactly right: a venue whose universe
/// exceeds what a feed publishes is the normal case. The others are not
/// ordinary, and keeping them apart from it is the point of this type — an
/// instrument declined because a number it stated cannot be represented is a
/// misconfiguration somebody has to see, and it would be invisible if it were
/// counted next to the ones the cap declined.
///
/// # Where these are counted
///
/// This crate constructs no metric. The normative set has no family for a
/// declined listing, and inventing one is not this crate's to do; what it owes
/// is that the reasons stay distinguishable, which is what
/// [`Registry::counts`](crate::Registry::counts) and
/// [`Registry::last_refusal`](crate::Registry::last_refusal) hand the runtime.
/// A refusal that is not [`Capped`](Self::Capped) is a reference-data load that
/// did not fully load, and the runtime records it under the load-error family's
/// `schema` reason, since what failed is the venue's statement of the
/// instrument rather than the transport that carried it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    /// The published cap is reached, and admission is sticky, so nothing is
    /// evicted to make room. Ordinary.
    #[error("the published set is at its cap")]
    Capped,

    /// The venue stated a `quoted_per_contract` that is not a strictly positive
    /// value stateable at nine decimal places.
    ///
    /// Refused at admission rather than per message, which is the whole
    /// argument for the field existing above the venue boundary: an instrument
    /// whose contract size we cannot represent must not be published at all,
    /// because every price and quantity for it would be refused one at a time
    /// while the manifest went on claiming it.
    #[error("the stated contract size is not a positive value we can represent exactly")]
    ContractSize,

    /// A scalar the venue stated could not be converted exactly at the
    /// instrument's own exponent.
    ///
    /// The three [`LoweringError`] cases are carried through rather than folded
    /// together, because each is a different operator action: too precise means
    /// the exponent is wrong for this instrument, malformed means the upstream
    /// changed its format, and an inexact contract means the contract size does
    /// not divide what the venue quoted.
    #[error("{0}")]
    Field(#[source] LoweringError),

    /// The venue restated an exponent or a contract factor for an instrument
    /// that is already published.
    ///
    /// Those three numbers are the ones the lowering converts every price and
    /// quantity against, and they are also published in the definition. The
    /// admitted set holds no replacement in place, so accepting the
    /// restatement would leave the definition declaring one scale while every
    /// quote for the instrument went out at the other — self-consistent on
    /// each side, and invisible to any test that encodes and then decodes.
    /// Re-admitting instead would move the instrument to a new slot and strand
    /// the handle the adapter is carrying.
    ///
    /// So the published definition stands and the restatement is counted. An
    /// instrument whose exponent has genuinely changed is a delisting and a
    /// relisting, which is what it is to a subscriber holding its book.
    #[error("an exponent or contract factor was restated for a published instrument")]
    ScaleRestated,

    /// The `Instrument ID` space is exhausted.
    ///
    /// Unreachable in practice and refused rather than wrapped: the next ID
    /// after `u32::MAX` is one already published, and publishing a definition
    /// that re-points a live `Instrument ID` at a different instrument is worse
    /// than declining a listing.
    #[error("the Instrument ID space is exhausted")]
    IdSpaceExhausted,

    /// The persisted state could not be written, so no further ID is minted.
    ///
    /// An `Instrument ID` that was published and not persisted resolves to
    /// nothing after a restart, so a publisher that cannot persist stops
    /// minting. It does not stop publishing what it has already minted; see
    /// [`Registry::fault`](crate::Registry::fault) for who decides what happens
    /// next.
    #[error("the minted Instrument ID could not be persisted")]
    Unpersistable,

    /// The publisher is shutting down.
    ///
    /// An admission during shutdown mints and persists an ID that no definition
    /// cycle will ever publish, which is the one way this crate can create the
    /// unresolvable ID it exists to prevent.
    #[error("the publisher is shutting down")]
    ShuttingDown,
}

impl From<LoweringError> for Refusal {
    fn from(error: LoweringError) -> Self {
        Self::Field(error)
    }
}

impl Refusal {
    /// Whether this is the ordinary refusal the boundary documents.
    #[must_use]
    pub const fn is_ordinary(self) -> bool {
        matches!(self, Self::Capped)
    }
}
