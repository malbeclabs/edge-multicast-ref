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
use dz_recorder_core::{CaptureDropScope, OwnedDatagram, Source as _};
use dz_recorder_inline::manifest::{window_manifest, WindowIdentity};
use dz_recorder_inline::ring::{ring, Offered};
use dz_recorder_inline::window::{WindowBound, WindowSource};
use dz_recorder_replay::synthetic::SyntheticPublisher;
use dz_recorder_replay::Fault;
use dz_recorder_rows::{derive, derive_object, Derivation, DeriveInput, Derived, RowBatch};

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
    // Read it out first: the manifest describes what the window saw, and the
    // window has not seen anything until the derivation has walked it.
    let mut walked = Vec::new();
    while let Some(dg) = window.next().expect("the ring does not fail") {
        walked.push(dg.recv_ts_ns);
    }
    assert_eq!(walked.len(), sent.len(), "the window saw the whole stream");

    let manifest = window_manifest(&window_identity, window.tally(), segment_seq, 0, 0);

    // A second window over the same datagrams, because the first was consumed
    // reading the tally. The ring is empty now, so this one is fed directly.
    let (mut tx, mut rx) = ring(sent.len() + 8);
    for dg in sent {
        assert_eq!(tx.offer(&dg.as_recorded()), Offered::Accepted);
    }
    drop(tx);
    let mut window = WindowSource::open(
        &mut rx,
        WindowBound {
            bytes: u64::MAX,
            interval: NEVER,
        },
    );
    derive(
        &mut window,
        &DeriveInput {
            manifest: &manifest,
            drop_scope,
            preceding: None,
            derivation: Derivation::Live,
        },
    )
    .expect("the window derives")
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
/// unknown schema version. Each is a thing a publisher, a network or this
/// recorder actually does, and each produces rows an archive-mode dashboard
/// already reads.
#[test]
fn every_injected_fault_derives_identically_through_both_paths() {
    for fault in [
        Fault::SequenceGap,
        Fault::BackwardMotion,
        Fault::ResetCountAdvance,
        Fault::NewSourceAddress,
        Fault::SourceAddressDisappears,
        Fault::Duplicate,
        Fault::ReorderedPair,
        Fault::OversizedDeclaredLength,
        Fault::UnknownSchemaVersion,
    ] {
        both_paths_agree(
            &SyntheticPublisher::with_fault(200, fault),
            &format!("{fault:?}"),
        );
    }
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
/// The manifest is built from an empty tally on purpose: coverage rows are not
/// what this asks about, and the equivalence tests above are what hold those
/// honest. Gap rows come from the loss deriver reading `drop_delta`, which is
/// exactly the path under test.
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
    let manifest = window_manifest(
        &WindowIdentity {
            identity: &id,
            feed: "top-of-book",
            roles_joined: &[],
            drop_scope: CaptureDropScope::PortRole,
            link_headers_captured: false,
        },
        &dz_recorder_inline::window::WindowTally::default(),
        0,
        0,
        0,
    );

    let mut window = WindowSource::open(
        &mut rx,
        WindowBound {
            bytes: u64::MAX,
            interval: std::time::Duration::from_secs(5),
        },
    );
    let derived = derive(
        &mut window,
        &DeriveInput {
            manifest: &manifest,
            drop_scope: CaptureDropScope::PortRole,
            preceding: None,
            derivation: Derivation::Live,
        },
    )
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
