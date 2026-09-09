//! The manifest of a window that has no object.
//!
//! [`derive`] takes a [`SegmentManifest`], and a window has no file to describe.
//! So one is synthesised — under the discipline socket-mode capture already
//! applies to the IP headers it synthesises, which this module inherits rather
//! than invents: **a field the recorder did not observe is never written as
//! though it had been.**
//!
//! Most of it is observed. The identity, the counts, the per-instance coverage,
//! the capture's own drop total, the declared drop scope, the roles joined and
//! whether the link headers were captured are all facts about this window, and
//! the archive path would write exactly the same values. Three fields are not,
//! and each is left empty rather than filled with something plausible:
//!
//! - **`sha256` is the empty string.** No datagram was kept, so nothing was
//!   hashed. A digest computed over anything else — the rows, the payloads in
//!   flight — would be a different claim wearing the field name of a claim about
//!   an archived object, and a reader checking it would be checking nothing.
//!   Absent is the honest answer, and [`Derivation::Live`] on every row is what
//!   makes it legible rather than merely missing.
//! - **`byte_count` is zero**, for the same reason: there is no object to have a
//!   size.
//! - **`interface_drop_total` is zero, and so is archive mode's.** Loss upstream
//!   of the capture point is read per capture handle, while this field's
//!   accounting is per port role — so the record path hands it to the health
//!   tier and never to a segment, deliberately, because at capture-handle scope
//!   there is no role to charge it to and a guess recorded as a number is how a
//!   false publisher-loss finding is made. Inline mode writing a number here
//!   would be one mode claiming a measurement the other declines to make, in a
//!   column a reader subtracts across both.
//!
//! `capture_drop_total` is on the observed side, and by the writer's own
//! arithmetic: the window sums every `drop_delta` it walked, which is what
//! `SegmentWriter` sums into the same field over the same unit. In inline mode
//! that sum includes what the ring itself dropped, because the ring folds its
//! debt into the same field before the derivation ever sees it — which is the
//! honest total for a column asking whether this host kept up, and the reason
//! the ring charges its drops there in the first place.
//!
//! `object_key` is a window key. It is not empty, because it identifies the
//! window in the ledger and in the rows, and it carries the window's start in
//! wall-clock nanoseconds so that windows order across process runs — which a
//! per-run sequence number cannot do, since it restarts at zero. But it names no
//! object anyone can fetch, and the key's own shape says so.
//!
//! [`derive`]: dz_recorder_rows::derive
//! [`Derivation::Live`]: dz_recorder_rows::Derivation::Live

use dz_recorder_archive::{JoinedRole, SegmentManifest};
use dz_recorder_core::{CaptureDropScope, RecorderIdentity};

use crate::window::WindowTally;

/// The part of a window's manifest that is the same for every window of a run.
#[derive(Debug, Clone)]
pub struct WindowIdentity<'a> {
    pub identity: &'a RecorderIdentity,
    /// The feed specification's name, never a venue.
    pub feed: &'a str,
    /// What the recorder was asked to join, and where. A port that was never
    /// joined produces no data, and no data looks exactly like a clean feed.
    pub roles_joined: &'a [JoinedRole],
    /// The scope the capture declares its drops at — the capture's own, never a
    /// preference. The same value the archive would write, so a subtraction
    /// reads the same word whichever mode produced the row.
    pub drop_scope: CaptureDropScope,
    /// `true` when the capture read the link headers off the interface. The
    /// archive writes the same distinction, and a reader must not mistake a
    /// synthesised header for an observed one.
    pub link_headers_captured: bool,
}

/// The key a window is identified by, in the ledger and on every row.
///
/// Hive-partitioned like an object key, because the rows are joined the same way
/// and a reader should not have to learn two shapes. The `live/` prefix is the
/// part that differs, and it is deliberate: a key that looked like an object key
/// would invite somebody to go and fetch the object, and there is none.
///
/// `start_ns` and not the window sequence number: the sequence restarts at zero
/// on every run, so two runs of one recorder would produce the same key for
/// different datagrams. The stamp orders windows across runs and makes the key
/// unique without a coordinator.
#[must_use]
pub fn window_key(identity: &WindowIdentity<'_>, start_ns: u64, window_seq: u64) -> String {
    let id = identity.identity;
    format!(
        "live/feed={}/env={}/site={}/recorder={}/{start_ns}-{window_seq}",
        identity.feed, id.env, id.site, id.recorder,
    )
}

/// The manifest for one closed window.
///
/// `window_seq` is monotonic within a run and restarts at zero across one, the
/// same as `segment_seq`: a hole in it is a hole in the derivation, which is
/// what distinguishes a recorder that was down from a feed that was quiet.
///
/// **`tally` must describe a window that has been walked.** Every field below
/// comes from it, so a tally taken before the walk produces a manifest that
/// describes nothing and stamps that nothing onto every row — see
/// [`HeldWindow`](crate::window::HeldWindow), which is what walks it.
///
/// There is deliberately no parameter here. Both cumulative drop totals used to
/// be arguments, and both arrived as a literal zero from the one caller for as
/// long as they existed: a builder that can be handed a number nobody observed
/// is a builder that will be.
#[must_use]
pub fn window_manifest(
    identity: &WindowIdentity<'_>,
    tally: &WindowTally,
    window_seq: u64,
) -> SegmentManifest {
    let id = identity.identity;
    SegmentManifest {
        site: id.site.clone(),
        recorder: id.recorder.clone(),
        env: id.env.clone(),
        feed: identity.feed.to_owned(),
        build_version: id.build_version.clone(),
        build_commit: id.build_commit.clone(),
        config_hash: id.config_hash.clone(),

        segment_seq: window_seq,
        start_ns: tally.first_recv_ts_ns,
        end_ns: tally.last_recv_ts_ns,

        datagram_count: tally.datagram_count,
        payload_byte_count: tally.payload_byte_count,

        object_key: window_key(identity, tally.first_recv_ts_ns, window_seq),
        // Nothing was written and nothing was hashed. See the module
        // documentation: an invented digest is a claim that something was
        // verified.
        byte_count: 0,
        sha256: String::new(),

        instances: tally.coverage.coverage(),
        short_datagrams: tally.coverage.short_datagrams(),
        instances_dropped: tally.coverage.instances_dropped(),

        // The archive writer's own arithmetic, over the same field and the same
        // unit: the sum of every `drop_delta` the window walked.
        capture_drop_total: tally.capture_drop_total,
        capture_drop_scope: scope_token(identity.drop_scope).to_owned(),
        // Zero, and the same zero archive mode writes. See the module
        // documentation: neither mode's manifest can attribute loss upstream of
        // the capture point, and one mode writing a number there would make the
        // two modes' coverage rows unsubtractable.
        interface_drop_total: 0,

        roles_joined: identity.roles_joined.to_vec(),
        link_headers: if identity.link_headers_captured {
            "captured".to_owned()
        } else {
            "synthesised".to_owned()
        },
        // Nothing synthesises a header here — the derivation reads the payload
        // and the datagram header, and never a link header — so there is no
        // datagram whose own headers could contradict the claim above.
        link_header_exceptions: 0,
    }
}

/// The two tokens the archive's section header and its manifest already write.
///
/// Spelled here rather than reached for through a `Debug`, so that a rename in
/// either place is a compile error rather than a subtraction under a scope the
/// archive never claimed.
const fn scope_token(scope: CaptureDropScope) -> &'static str {
    match scope {
        CaptureDropScope::PortRole => "port-role",
        CaptureDropScope::CaptureHandle => "capture-handle",
    }
}
