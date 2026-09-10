//! The gate the inline mode design rests on: one feed, two paths, the same rows.
//!
//! Inline mode derives rows from a live capture and keeps no datagrams. The
//! argument for that being sound is not that it is cheaper — it is that
//! **deriving in flight is the same function as deriving from an object**. If it
//! is, then a row's provenance is the only thing that changed and every query
//! written against archive-mode rows means what it meant. If it is not, then
//! inline mode is a second analysis wearing the first one's column names, and no
//! `derivation` column would make the two comparable.
//!
//! So this feeds one synthetic stream through both paths — recorded to a real
//! archive and derived with [`derive_object`], and pushed through the ring and a
//! window and derived with [`derive`] — and asserts the two row sets are equal
//! but for what must differ:
//!
//! | | Why it differs |
//! |---|---|
//! | `derivation` | the point of the column |
//! | `object_key` | one names an object, the other names a window |
//! | `object_sha256` | one is a digest, the other is empty because nothing was hashed |
//!
//! Everything else — every sequence gap, every era boundary, every coverage
//! count, every datagram row field — must match exactly.
//!
//! Nothing here needs a socket, a privilege or a server.
#![forbid(unsafe_code)]

mod common;

use common::{identity, record};
use dz_edge_core::PortRole;
use dz_recorder_core::{CaptureDropScope, OwnedDatagram};
use dz_recorder_inline::derivation::WindowDeriver;
use dz_recorder_inline::manifest::WindowIdentity;
use dz_recorder_inline::ring::{ring, Offered};
use dz_recorder_inline::window::{WindowBound, WindowSource};
use dz_recorder_replay::synthetic::SyntheticPublisher;
use dz_recorder_replay::Fault;
use dz_recorder_rows::{derive_object, Derivation, Derived, RowBatch};

/// Long enough that no window in this file ever closes on age: every one of them
/// is meant to hold the whole stream, so that the comparison is window-for-
/// object rather than window-for-part-of-one.
const NEVER: std::time::Duration = std::time::Duration::from_secs(3_600);

/// The same datagrams, derived the way inline mode derives them.
///
/// Through the real ring and a real window, not by calling `derive` over the
/// archive with a flag flipped: the point is to exercise the path a running
/// recorder takes, including the hand-off that can drop and the tally that the
/// window keeps as it goes.
fn derive_inline(
    sent: &[OwnedDatagram],
    segment_seq: u64,
    drop_scope: CaptureDropScope,
    // The same joins the archive recorded. A port that was never joined produces
    // no data and no data looks exactly like a clean feed, so the coverage row
    // carries what was joined — and the two paths have to have joined the same
    // thing for the comparison to be about the derivation.
    roles_joined: &[dz_recorder_archive::JoinedRole],
) -> Derived {
    // Room for the whole stream, because a drop here would be a drop this test
    // did not ask for — the ring's own drop accounting has its own tests.
    let (mut tx, mut rx) = ring(sent.len() + 8);
    for dg in sent {
        assert_eq!(
            tx.offer(&dg.as_recorded()),
            Offered::Accepted,
            "the ring was sized for the whole stream"
        );
    }
    drop(tx);

    let id = identity();
    let window_identity = WindowIdentity {
        identity: &id,
        feed: "top-of-book",
        roles_joined,
        drop_scope,
        link_headers_captured: false,
    };

    let mut window = WindowSource::open(
        &mut rx,
        WindowBound {
            bytes: u64::MAX,
            interval: NEVER,
        },
    );
    // **The derivation stage's own two passes, called and not arranged here.**
    // The manifest describes what the window saw and the window has seen
    // nothing until it has been walked, so a window is drained and then derived
    // from what was drained. This test used to do that itself, over a second
    // ring — and the derivation stage did not, so the gate was green over a
    // shape nothing ran, which is the one failure a gate cannot report.
    let derived = WindowDeriver::new()
        .derive_window(&mut window, &window_identity, segment_seq, None)
        .expect("the window derives");
    assert_eq!(
        window.tally().datagram_count as usize,
        sent.len(),
        "the window saw the whole stream"
    );
    derived
}

/// Erases the three fields that must differ, so `assert_eq!` can do the rest.
///
/// Erasing rather than skipping: a comparison that walked the fields it cared
/// about would silently ignore any field added later, and the whole value of
/// this test is that it covers fields nobody thought to list.
fn normalise(batch: &mut RowBatch) {
    batch.object_key.clear();
    batch.object_sha256.clear();
    batch.derivation = Derivation::Archive;
    for row in &mut batch.datagram {
        row.object_key.clear();
        row.object_sha256.clear();
        row.derivation = Derivation::Archive;
    }
    for row in &mut batch.era {
        row.object_key.clear();
        row.object_sha256.clear();
        row.derivation = Derivation::Archive;
    }
    for row in &mut batch.segment_coverage {
        row.object_key.clear();
        row.object_sha256.clear();
        row.derivation = Derivation::Archive;
    }
    for row in &mut batch.sequence_gap {
        row.object_key.clear();
        row.derivation = Derivation::Archive;
    }
    for row in &mut batch.conformance_finding {
        row.object_key.clear();
        row.derivation = Derivation::Archive;
    }
    for row in &mut batch.event {
        row.object_key.clear();
        row.object_sha256.clear();
        row.derivation = Derivation::Archive;
    }
    for row in &mut batch.instrument {
        row.object_key.clear();
        row.derivation = Derivation::Archive;
    }
    for row in &mut batch.book_top {
        row.object_key.clear();
        row.derivation = Derivation::Archive;
    }
}

/// One publisher's stream, through both paths, asserted equal.
fn both_paths_agree(publisher: &SyntheticPublisher, what: &str) {
    let sent = publisher.datagrams();
    let recorded = record(&sent, &[PortRole::Mktdata]);

    let archived =
        derive_object(&recorded.object, &recorded.manifest, None).expect("the object derives");
    // The same window sequence number, or every row would differ in a field
    // that has nothing to do with the question.
    let live = derive_inline(
        &sent,
        recorded.manifest.segment_seq,
        CaptureDropScope::PortRole,
        &recorded.manifest.roles_joined,
    );

    assert_eq!(
        live.rows.derivation,
        Derivation::Live,
        "{what}: the live batch must say so before anything else is compared"
    );
    assert_eq!(
        live.short_datagrams, archived.short_datagrams,
        "{what}: short datagrams"
    );
    assert_eq!(live.trailer, archived.trailer, "{what}: the trailer");

    let mut live_rows = live.rows;
    let mut archived_rows = archived.rows;
    normalise(&mut live_rows);
    normalise(&mut archived_rows);

    // Grain by grain and row by row rather than one comparison of two whole
    // batches. Two batches printed in full are hundreds of rows of output with
    // the difference somewhere inside them, and a gate whose failure nobody can
    // read is a gate that gets deleted.
    macro_rules! same_rows {
        ($grain:ident) => {{
            assert_eq!(
                live_rows.$grain.len(),
                archived_rows.$grain.len(),
                "{what}: the two paths produced different numbers of {} rows",
                stringify!($grain)
            );
            for (i, (live_row, archived_row)) in live_rows
                .$grain
                .iter()
                .zip(archived_rows.$grain.iter())
                .enumerate()
            {
                assert_eq!(
                    live_row,
                    archived_row,
                    "{what}: {} row {i} differs between the two paths",
                    stringify!($grain)
                );
            }
        }};
    }
    same_rows!(datagram);
    same_rows!(era);
    same_rows!(segment_coverage);
    same_rows!(sequence_gap);
    same_rows!(conformance_finding);
    same_rows!(event);
    same_rows!(instrument);
    same_rows!(book_top);
}

/// A clean feed derives identically both ways.
#[test]
fn a_clean_feed_derives_identically_through_both_paths() {
    both_paths_agree(&SyntheticPublisher::clean(200), "a clean feed");
}

/// Every fault the replay crate injects derives identically both ways.
///
/// The faults are the design's own list, and they are where the two paths would
/// diverge if they were going to: a gap, backward motion, a reset, a second
/// publisher, a duplicate, a reordered pair, an over-cap declared length, an
/// unknown schema version, and a channel that goes quiet. Each is a thing a
/// publisher, a network or this recorder actually does, and each produces rows
/// an archive-mode dashboard already reads.
///
/// # What `SilentChannel` covers here, and what it does not
///
/// It is the fault whose *production* behaviour differs most between the two
/// modes: archive mode finds a quiet channel when a segment rotates on its
/// interval, inline mode when a window closes on age, and age is inline mode's
/// own key rather than one the modes share. That difference is a difference of
/// **timing**, and this fixture cannot see it — one window holding the same
/// datagrams as one segment is the fixture's whole premise, which is what makes
/// the row comparison meaningful for every other fault.
///
/// What it does assert is the derivation half: with one channel among several
/// falling silent partway through, the coverage, era and gap rows the two paths
/// produce are the same. That is the half a dashboard reads. Asserting the
/// timing half wants a fixture with a clock over several windows, which is a
/// different test and not this one.
#[test]
fn every_injected_fault_derives_identically_through_both_paths() {
    for fault in GATED {
        both_paths_agree(
            &SyntheticPublisher::with_fault(200, fault),
            &format!("{fault:?}"),
        );
    }
}

/// The faults this gate runs.
const GATED: [Fault; 10] = [
    Fault::SequenceGap,
    Fault::BackwardMotion,
    Fault::ResetCountAdvance,
    Fault::NewSourceAddress,
    Fault::SourceAddressDisappears,
    Fault::Duplicate,
    Fault::ReorderedPair,
    Fault::OversizedDeclaredLength,
    Fault::UnknownSchemaVersion,
    Fault::SilentChannel,
];

/// Every fault the replay crate injects, whether this gate runs it or not.
///
/// Kept beside [`GATED`] rather than derived from it, because a list compared
/// against itself compares nothing. The function below is what keeps *this* one
/// honest: a new `Fault` variant is a compile error there, and a compile error
/// is the only reminder that survives a year.
const EVERY: [Fault; 11] = [
    Fault::None,
    Fault::SequenceGap,
    Fault::BackwardMotion,
    Fault::ResetCountAdvance,
    Fault::NewSourceAddress,
    Fault::SourceAddressDisappears,
    Fault::Duplicate,
    Fault::ReorderedPair,
    Fault::OversizedDeclaredLength,
    Fault::UnknownSchemaVersion,
    Fault::SilentChannel,
];

/// Nothing calls this. Its `match` is the guard on [`EVERY`].
#[allow(dead_code)]
fn a_new_fault_is_a_compile_error_here(fault: Fault) {
    match fault {
        Fault::None
        | Fault::SequenceGap
        | Fault::BackwardMotion
        | Fault::ResetCountAdvance
        | Fault::NewSourceAddress
        | Fault::SourceAddressDisappears
        | Fault::Duplicate
        | Fault::ReorderedPair
        | Fault::OversizedDeclaredLength
        | Fault::UnknownSchemaVersion
        | Fault::SilentChannel => {}
    }
}

/// The gate runs every fault there is, and this is what says so.
///
/// **A list of faults is exactly the kind of thing that quietly falls one
/// short.** `SilentChannel` was missing from this gate for the life of the
/// branch and nothing failed, because a fault absent from a loop is not a fault
/// that fails — it is a fault nobody runs, which looks identical to a pass. So
/// the completeness of the list is itself an assertion, and dropping a fault
/// from [`GATED`] fails a named test rather than quietly narrowing the gate.
///
/// `Fault::None` is excepted by name: it is the clean feed, and
/// [`a_clean_feed_derives_identically_through_both_paths`] runs it on its own.
#[test]
fn the_gate_runs_every_fault_the_replay_crate_injects() {
    for fault in EVERY {
        if fault == Fault::None {
            continue;
        }
        assert!(
            GATED.contains(&fault),
            "{fault:?} is not in the equivalence gate's list, so the two paths are compared \
             over every fault but that one"
        );
    }
    assert_eq!(
        GATED.len(),
        EVERY.len() - 1,
        "the gate's list and the full list have drifted: {GATED:?} against {EVERY:?}"
    );
}

/// A gap the ring caused is not charged to the publisher.
///
/// This is the failure inline mode could introduce that archive mode cannot: a
/// datagram the derivation never saw is a sequence value nobody delivered, and a
/// sequence value nobody delivered with nothing admitted behind it is a
/// `publisher` verdict. The ring charges its drops so that the gap is the
/// recorder's, and this asserts it at the altitude a dashboard reads — the row,
/// not the counter.
///
/// # The loss has to fall in the middle, and that is why there is a thread
///
/// A gap is only visible between two datagrams that were received. Overrunning
/// the ring and then stopping puts every drop after the last arrival, where
/// nothing reveals it, and the derivation sees a short contiguous window and is
/// right to. Interleaving the offers with reads by hand does not work either:
/// the reads have to be the derivation's own, and it does not offer a hook
/// between them.
///
/// So the capture runs where it runs in production — on its own thread —
/// offering a burst far larger than the ring, pausing, and then offering the
/// rest. The derivation drains concurrently, and the datagram that reopens the
/// feed after the pause is the one that has to admit what was lost.
///
/// Derived through the same two passes as everything above, so that what
/// drains the ring here is what drains it in production: the first pass runs
/// while the capture thread is still offering, which is the concurrency this
/// test is about. Coverage rows are not what it asks — gap rows come from the
/// loss deriver reading `drop_delta` — and the equivalence tests above are what
/// hold the coverage grain honest.
#[test]
fn a_gap_the_ring_caused_is_not_attributed_to_the_publisher() {
    const CAPACITY: usize = 4;
    let sent = SyntheticPublisher::clean(40).datagrams();

    let (mut tx, mut rx) = ring(CAPACITY);
    let producer = std::thread::spawn(move || {
        let mut dropped = 0usize;
        // Twenty into four, as fast as the loop goes: the ring cannot take them
        // and every one it refuses is owed forward.
        for dg in &sent[0..20] {
            if tx.offer(&dg.as_recorded()) == Offered::Dropped {
                dropped += 1;
            }
        }
        // Long enough for the derivation to have drained what did get in, so
        // that what follows lands rather than piling into the same overrun.
        std::thread::sleep(std::time::Duration::from_millis(200));
        for dg in &sent[20..40] {
            let _ = tx.offer(&dg.as_recorded());
        }
        dropped
    });

    let id = identity();
    let window_identity = WindowIdentity {
        identity: &id,
        feed: "top-of-book",
        roles_joined: &[],
        drop_scope: CaptureDropScope::PortRole,
        link_headers_captured: false,
    };

    let mut window = WindowSource::open(
        &mut rx,
        WindowBound {
            bytes: u64::MAX,
            interval: std::time::Duration::from_secs(5),
        },
    );
    let derived = WindowDeriver::new()
        .derive_window(&mut window, &window_identity, 0, None)
        .expect("the window derives");

    let dropped = producer.join().expect("the capture thread does not panic");
    assert!(dropped > 0, "the ring was meant to be overrun");
    assert!(
        !derived.rows.sequence_gap.is_empty(),
        "the ring dropped {dropped} datagrams in the middle of a feed and no gap row \
         describes it, so this test proves nothing"
    );

    for gap in &derived.rows.sequence_gap {
        assert_ne!(
            gap.verdict,
            dz_recorder_rows::Verdict::Publisher,
            "a gap the recorder's own ring caused was charged to the publisher: {gap:?}"
        );
    }
    // And they are explained rather than merely not accused: what the ring lost
    // is admitted, so nothing is left over to attribute to anybody.
    let unexplained: u64 = derived
        .rows
        .sequence_gap
        .iter()
        .filter_map(|g| g.unexplained_count)
        .sum();
    assert_eq!(
        unexplained, 0,
        "the ring's own losses are admitted, so no gap has an unexplained residue"
    );
}
