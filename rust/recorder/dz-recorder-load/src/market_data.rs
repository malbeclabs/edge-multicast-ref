//! The seam between one object and its market data rows.
//!
//! Everything the loader wrote until now is a fact about *datagrams*: how many
//! arrived, on what instance, with what missing between them. This is the other
//! derivation — the messages inside those datagrams, as rows about instruments —
//! and it is reached only for the feeds a configuration names.
//!
//! # It is off unless a feed asked, and that is the whole of the switch
//!
//! [`derivation_for`] is the only way in, and an unnamed feed gets `None`. There
//! is no global enable and no default entry: a host with no `[[market_data]]`
//! section loads exactly what it loaded before, into the same four tables, and
//! `event`, `instrument` and `book_top` stay empty. That is a property the
//! loader's own tests assert rather than a promise this comment makes, because
//! this tier is being added under a loader that is already in production shape
//! and the expensive failure is the one where turning nothing on changed
//! something.
//!
//! # The object is walked a second time, deliberately
//!
//! [`dz_recorder_rows::derive_object`] verifies the digest, walks the archive
//! for the transport tier and hands back no source. Rather than widen it, an
//! enabled feed opens the object again and walks it with the codec. The second
//! read costs a decompression, and it is paid by the feed that asked for it —
//! which is the same shape as the switch: the crate that knows what a `Quote` is
//! stays out of the crate that knows what a datagram is, and a feed that derives
//! nothing pays nothing.
//!
//! # And it is refused on the same terms
//!
//! A walk that ends anywhere but EOF derives nothing. The digest above already
//! rules out a truncated object and the first walk already refused a torn one —
//! but a check that holds only because another function ran first is a check one
//! refactor away from not holding, and what it prevents here is an object whose
//! `event` table stops mid-window while its `datagram` table does not.

use std::path::Path;

use dz_recorder_archive::SegmentManifest;
use dz_recorder_core::RecorderIdentity;
use dz_recorder_events::{derive_events, DerivedEvents, EventInput};
use dz_recorder_replay::{ArchiveSource, Termination};
use dz_recorder_rows::RowBatch;
use thiserror::Error;

use crate::config::MarketDataFeed;

/// One object's market data could not be derived, so the object stays unloaded.
///
/// Every variant leaves the whole object unloaded and not just its market data
/// rows. An object whose datagram rows landed and whose `event` rows did not is
/// an object that reads as a feed nobody published on — the same partial credit
/// the batch refuses across grains, for the same reason.
#[derive(Debug, Error)]
pub enum MarketDataError {
    #[error("{path}: {source}")]
    Open {
        path: std::path::PathBuf,
        #[source]
        source: dz_recorder_core::SourceError,
    },
    #[error("deriving market data from {object_key}: {source}")]
    Walk {
        object_key: String,
        #[source]
        source: dz_recorder_relower::RelowerError,
    },
    /// The walk stopped before the end of the archive.
    #[error(
        "the market data walk of {object_key} ended at {termination}, not at the end of the \
         archive: every message after the tear would be missing from a table that holds all of \
         them"
    )]
    Incomplete {
        object_key: String,
        termination: String,
    },
}

/// The entry in force for a feed, or `None` — which is every feed by default.
#[must_use]
pub fn derivation_for<'a>(feeds: &'a [MarketDataFeed], feed: &str) -> Option<&'a MarketDataFeed> {
    feeds.iter().find(|derived| derived.feed == feed)
}

/// Walks one object with the codec and derives its market data rows.
///
/// The identity on every row comes from the **manifest**, never from the
/// loader's own configuration: it describes the recorder that observed the
/// bytes, and a loader re-processing another host's object must not sign it.
/// The loader refuses a foreign object long before this, so the two agree — and
/// they agree because one of them is the source, not because both were checked.
///
/// # Errors
///
/// [`MarketDataError`], and then the object stays unloaded.
pub fn derive_market_data(
    object: &Path,
    manifest: &SegmentManifest,
    derived: &MarketDataFeed,
) -> Result<DerivedEvents, MarketDataError> {
    let mut source = ArchiveSource::open(object).map_err(|source| MarketDataError::Open {
        path: object.to_path_buf(),
        source,
    })?;
    let identity = identity_of(manifest);
    let events = derive_events(
        &mut source,
        &EventInput {
            identity: &identity,
            feed: &manifest.feed,
            object_key: &manifest.object_key,
            object_sha256: &manifest.sha256,
            segment_seq: manifest.segment_seq,
            magic: derived.magic,
            observation: &identity.hardware(),
            persist_snapshot_levels: derived.persist_snapshot_levels,
        },
    )
    .map_err(|source| MarketDataError::Walk {
        object_key: manifest.object_key.clone(),
        source,
    })?;

    if source.terminated_by() != Termination::Eof {
        return Err(MarketDataError::Incomplete {
            object_key: manifest.object_key.clone(),
            termination: format!("{:?}", source.terminated_by()),
        });
    }
    Ok(events)
}

/// Puts the derived rows in the batch the sink takes.
///
/// One batch and not a second one: an object is the unit that either landed or
/// did not, and market data posted separately from the datagram rows of the same
/// object would be an object half of which is durable.
pub fn extend_batch(batch: &mut RowBatch, events: DerivedEvents) {
    batch.event = events.event;
    batch.instrument = events.instrument;
    batch.book_top = events.book_top;
}

/// Which recorder observed these bytes, out of the manifest it wrote.
fn identity_of(manifest: &SegmentManifest) -> RecorderIdentity {
    RecorderIdentity {
        site: manifest.site.clone(),
        recorder: manifest.recorder.clone(),
        env: manifest.env.clone(),
        build_version: manifest.build_version.clone(),
        build_commit: manifest.build_commit.clone(),
        config_hash: manifest.config_hash.clone(),
    }
}

/// What a fold declined to attribute, as a bounded label.
///
/// Every one of these is a message that is *not* in a table, and every one of
/// them presents downstream as the same thing: fewer rows than a feed carried.
/// A derivation that resolved nothing at all — the case a wrong `Magic` or a
/// missing `refdata` join produces — writes an empty `event` table, and an empty
/// table is indistinguishable from a feed nobody published on. So the refusals
/// are counted, by cause, and the causes have different answers: reference data
/// that never arrived is a recorder that did not join `refdata`, and a stale
/// cycle is a publisher doing what the specification tells it to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusalKind {
    /// A message for an instrument with no definition in force at its position.
    UnresolvedInstrument,
    /// A snapshot level whose `snapshot_id` matches no open cycle.
    OrphanSnapshotLevel,
    /// A definition positioned behind the statement already in force.
    OutOfOrderDefinition,
    /// A cycle that carried fewer levels than its `SnapshotBegin` promised.
    IncompleteCycle,
    /// A cycle whose `anchor_seq` was behind an unrecovered reset's.
    StaleCycle,
}

impl RefusalKind {
    pub const ALL: [Self; 5] = [
        Self::UnresolvedInstrument,
        Self::OrphanSnapshotLevel,
        Self::OutOfOrderDefinition,
        Self::IncompleteCycle,
        Self::StaleCycle,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnresolvedInstrument => "unresolved_instrument",
            Self::OrphanSnapshotLevel => "orphan_snapshot_level",
            Self::OutOfOrderDefinition => "out_of_order_definition",
            Self::IncompleteCycle => "incomplete_cycle",
            Self::StaleCycle => "stale_cycle",
        }
    }
}

/// The two refusal reports one derivation produces, as one list.
#[must_use]
pub fn refusals(events: &DerivedEvents) -> [(RefusalKind, u64); RefusalKind::ALL.len()] {
    [
        (
            RefusalKind::UnresolvedInstrument,
            events.refused.unresolved_instrument,
        ),
        (
            RefusalKind::OrphanSnapshotLevel,
            events.refused.orphan_snapshot_level,
        ),
        (
            RefusalKind::OutOfOrderDefinition,
            events.refused.out_of_order_definition,
        ),
        (
            RefusalKind::IncompleteCycle,
            events.book_refused.incomplete_cycle,
        ),
        (RefusalKind::StaleCycle, events.book_refused.stale_cycle),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed(name: &str) -> MarketDataFeed {
        MarketDataFeed {
            feed: name.to_owned(),
            magic: 0x4442,
            persist_snapshot_levels: false,
        }
    }

    /// The default, and it is the absence of an entry rather than a flag.
    #[test]
    fn a_feed_nobody_named_derives_nothing() {
        assert!(derivation_for(&[], "market-by-price").is_none());
        assert!(derivation_for(&[feed("top-of-book")], "market-by-price").is_none());
    }

    /// Matched on the manifest's feed exactly, so a host carrying two feeds into
    /// one directory derives the one that was asked for and not the other.
    #[test]
    fn a_named_feed_is_matched_exactly_and_never_by_prefix() {
        let feeds = [feed("market-by-price"), feed("top-of-book")];
        assert_eq!(
            derivation_for(&feeds, "top-of-book").expect("named").feed,
            "top-of-book"
        );
        assert!(derivation_for(&feeds, "market-by-price-2").is_none());
        assert!(derivation_for(&feeds, "market-by").is_none());
    }

    /// Every refusal has a name, because every one of them is a row that is not
    /// there and they do not have the same answer.
    #[test]
    fn every_refusal_the_fold_reports_has_a_counter_of_its_own() {
        let mut events = DerivedEvents::default();
        events.refused.unresolved_instrument = 3;
        events.book_refused.stale_cycle = 1;
        let counted = refusals(&events);
        assert_eq!(counted.len(), RefusalKind::ALL.len());
        for (kind, count) in counted {
            let expected = match kind {
                RefusalKind::UnresolvedInstrument => 3,
                RefusalKind::StaleCycle => 1,
                _ => 0,
            };
            assert_eq!(count, expected, "{}", kind.as_str());
        }
        // Distinct label values, so no two causes share a series.
        let mut names: Vec<&str> = RefusalKind::ALL.iter().map(|k| k.as_str()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), RefusalKind::ALL.len());
    }
}
