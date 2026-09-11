//! The venue half of a feed race: rows derived from a venue's own upstream.
//!
//! A feed race compares what a venue said with what a publisher sent. The
//! publisher half of it is complete elsewhere in this repository — a capture, an
//! archive, an offline re-lowering, six row grains, a loss derivation and a
//! conformance runner. This crate is the other half: an archive of a venue's own
//! upstream bytes becomes rows, by driving that venue's own `Adapter` over it.
//!
//! # Nothing here links a venue, and nothing here may
//!
//! [`derive_venue_object`] takes `&mut dyn Adapter`. The adapter is the venue's
//! and arrives from the venue's own binary, exactly as `run(AdapterRegistry)`
//! takes one on the publisher side — so this repository's recorder gains no
//! venue mode and links no venue crate, and a venue's own recorder binary is
//! three lines.
//!
//! # Why this reads objects and not a socket
//!
//! The request this design answers asked for a live input in the capture path,
//! relowering venue payloads into the publisher's own rows. Both halves of that
//! were refused, and the refusals are what this crate is shaped by.
//!
//! **The rows.** `event` and `book_top` carry eight non-nullable provenance
//! columns — the source address, the `Channel ID`, the destination port, the
//! `Source ID`, the `Instrument ID`, the `Sequence Number`, the `Reset Count`
//! and the `segment_seq` — and they are in the sort key. Every one of them is a
//! statement about a datagram on a channel instance, a venue's upstream message
//! is not one, and each has a plausible value that is also a real reading. So
//! the venue's rows are their own grains, in [`rows`], and the absence of those
//! columns is asserted against a literal rather than left as a review comment.
//!
//! **The input.** A live input produces no object and no digest, so there is no
//! key to replace on and no boundary to batch at — and re-running a recording
//! would accumulate rows in a table whose pairing counts occurrences, where a
//! duplicate does not inflate a count but manufactures evidence of loss.
//! Deriving from archived objects restores `(object key, sha256)` idempotence,
//! makes the object the batch, and keeps the bytes so that a mapping defect
//! found next month can be re-examined with a corrected adapter.
//!
//! That also decouples this crate from a receive path. The two transports a
//! venue would use for one are each their own design and neither is built;
//! [`object`] takes archived objects as its starting point precisely so that
//! nothing here waits on either.
//!
//! # The race is a view, and it is keyed on `book_key`
//!
//! `009` numbers the occurrences of a book state per observation point and pairs
//! ordinal to ordinal, as `006` does for the publisher side and for the same
//! reason. It keys on [`book_key`](dz_recorder_events::book_key) — the two sides
//! of a top and nothing else — and **not** on `state_key`, which folds the
//! `Channel ID` and the `Instrument ID` in before it folds a price. A venue side
//! can compute neither: the channel is the operator's mapping, and the
//! identifier is minted by the publisher's reference-data registry. Keyed on
//! `state_key` this race would return zero pairs and read as each side missing
//! every state the other saw.
//!
//! The key is computed by that function and not by a copy of it. Two hashes of
//! one book state pair with nothing, and the predicate that decides an absent
//! side is private in that crate for exactly that reason.
//!
//! # What a venue-side observation cannot say
//!
//! It cannot report loss. It has no sequence space of its own that this
//! repository defines, so a state the venue produced and nobody recorded is
//! invisible on this side. And it makes no claim about attribution: whether a
//! state the venue published and the publisher never sent is the publisher's
//! fault stays the loss derivation's question and the cross-site views'. A
//! venue-side observation adds one more thing that can be missing, not an answer
//! about whose fault it is.
#![forbid(unsafe_code)]

pub mod derive;
pub mod object;
pub mod rows;

pub use derive::{derive_venue_object, DeriveError, Derived};
pub use object::{ArchivedVenueObject, VenueObject, VenueObjectId};
pub use rows::{
    CollectingSink, RefusalCount, VenueBookTop, VenueGrain, VenueObjectRow, VenueRowBatch,
    VenueRowSink, VenueRowSinkError,
};
