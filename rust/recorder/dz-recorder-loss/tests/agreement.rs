//! One datagram stream, both halves of the recorder, and the numbers they must
//! agree about.
//!
//! The live half is [`HealthObserver`](dz_recorder_health::HealthObserver), fed
//! as the datagrams are captured and read back through its rendered exposition.
//! The offline half is a real archive, written with `dz-recorder-archive`,
//! replayed with `dz-recorder-replay` and derived here.
//!
//! This is the point of the crate. A dashboard whose live panel and historical
//! panel disagree about the same feed teaches nobody anything, so the two halves
//! are held against each other here rather than trusted.
//!
//! **What it proves, now that both halves drive one `SequenceTracker`.** The
//! classification itself is no longer two implementations, so this is not a
//! check that two copies of a rule set still agree — that duplication is gone,
//! and with it the way it could drift. What is compared is everything each half
//! builds on top of the shared tracker and cannot share: the live half turns
//! outcomes into Prometheus counters, keyed on `(feed, port_role, channel,
//! source)` and read back out of a rendered exposition; the offline half turns
//! them into per-era delivered ranges, keyed on the full channel instance, after
//! the stream has been through a real archive and back. Either half can still
//! count an outcome into the wrong counter, key an instance wrongly, lose a
//! datagram in the writer or the reader, or drop an era at a boundary, and each
//! of those is a disagreement this test catches.
//!
//! The absolute numbers are asserted alongside the agreement, because two halves
//! that are wrong in the same way agree perfectly — and a shared tracker is
//! exactly a way for both to be wrong identically, so those assertions carry
//! more of the weight here than they used to.
#![forbid(unsafe_code)]

mod common;

use common::{
    derive, instance, observe_live, through_an_archive, Stream, PUBLISHER_A, PUBLISHER_B,
};
use dz_edge_core::PortRole;
use dz_recorder_core::CaptureDropScope;
use dz_recorder_loss::Unexplained;

/// Gaps, a duplicate, a reordering, an era change, a second publisher on the
/// same channel and port, and loss the recorder admits.
fn stream() -> Stream {
    let mut stream = Stream::new();
    stream.send(PUBLISHER_A, 0, 0);
    stream.send(PUBLISHER_A, 1, 0);
    stream.send(PUBLISHER_B, 0, 0);
    stream.send(PUBLISHER_A, 2, 0);
    // Sequence numbers 3, 4 and 5 nobody delivered.
    stream.send(PUBLISHER_A, 6, 0);
    stream.send(PUBLISHER_B, 1, 0);
    stream.send(PUBLISHER_A, 7, 0);
    // The same datagram delivered twice.
    stream.send(PUBLISHER_A, 7, 0);
    // Two datagrams that arrived in the wrong order: 8 was not lost.
    stream.send(PUBLISHER_A, 9, 0);
    stream.send(PUBLISHER_A, 8, 0);
    stream.send(PUBLISHER_B, 2, 0);
    // A new era, restarting the sequence space.
    stream.send(PUBLISHER_A, 0, 1);
    stream.send(PUBLISHER_A, 1, 1);
    // Two missing, and the capture handle admits both.
    stream.send_after_loss(PUBLISHER_A, 4, 1, 2);
    stream.send(PUBLISHER_A, 5, 1);
    stream.send(PUBLISHER_B, 3, 0);
    stream
}

/// One publisher on the role, so the socket's accumulator and that publisher's
/// loss are the same subject and the subtraction means something.
fn one_publisher_stream() -> Stream {
    let mut stream = Stream::new();
    stream.send(PUBLISHER_A, 0, 0);
    stream.send(PUBLISHER_A, 1, 0);
    stream.send(PUBLISHER_A, 2, 0);
    // 3, 4 and 5 nobody delivered, and the socket admits two of the three.
    stream.send_after_loss(PUBLISHER_A, 6, 0, 2);
    stream.send(PUBLISHER_A, 7, 0);
    stream
}

#[test]
fn the_live_tier_and_the_deriver_agree_about_one_stream() {
    let stream = stream();
    let live = observe_live(&stream);
    let offline = through_an_archive(CaptureDropScope::PortRole, &stream);

    assert_eq!(
        offline.instances().len(),
        2,
        "two publishers on one channel and port are two instances"
    );

    for loss in offline.instances() {
        let source = loss.instance.source;
        let role = loss.role;
        let named = |name: &str| live.instance_value(name, source, role);

        assert_eq!(
            loss.missing_on_arrival,
            named("dz_recorder_missing_datagrams_on_arrival_total"),
            "missing datagrams disagree for {source}"
        );
        assert_eq!(
            loss.gaps_on_arrival,
            named("dz_recorder_sequence_gaps_total"),
            "gap counts disagree for {source}"
        );
        assert_eq!(
            loss.duplicates,
            named("dz_recorder_duplicate_datagrams_total"),
            "duplicate counts disagree for {source}"
        );
        assert_eq!(
            loss.era_transitions,
            named("dz_recorder_era_transitions_total"),
            "era transitions disagree for {source}"
        );
        assert_eq!(
            loss.reordered,
            named("dz_recorder_reordered_datagrams_total"),
            "reordering disagrees for {source}"
        );
        assert_eq!(
            loss.resets,
            named("dz_recorder_resets_total"),
            "reset counts disagree for {source}"
        );
        assert_eq!(
            loss.backward,
            named("dz_recorder_backward_sequence_total"),
            "backward motion disagrees for {source}"
        );

        let current = loss.eras.last().expect("an instance has at least one era");
        assert_eq!(
            current.last_seq,
            named("dz_recorder_sequence_current"),
            "the highest sequence number disagrees for {source}"
        );
        assert_eq!(
            current.ordinal,
            named("dz_recorder_era_ordinal"),
            "the era ordinal disagrees for {source}"
        );

        // The one divergence, and it is a definition rather than a fault: a
        // reordered datagram was delivered, so the archive is not missing it,
        // while a live tier had already counted it as absent at the moment it
        // looked absent. The row-grain count is the smaller one, by exactly the
        // reordering.
        assert_eq!(
            loss.missing,
            loss.missing_on_arrival - loss.reordered,
            "the row-grain missing count for {source} is not the live count less \
             the reordering"
        );
    }

    assert_eq!(
        offline.admitted_on_role(PortRole::Mktdata),
        live.role_value("dz_recorder_capture_drops_total", PortRole::Mktdata),
        "the recorder's own admitted loss disagrees"
    );

    // The absolute answers, so that two halves agreeing on a wrong number is
    // still a failure.
    let a = offline
        .instance(&instance(PUBLISHER_A, PortRole::Mktdata))
        .expect("publisher A's instance");
    assert_eq!(a.missing_on_arrival, 6);
    assert_eq!(a.gaps_on_arrival, 3);
    assert_eq!(a.duplicates, 1);
    assert_eq!(a.reordered, 1);
    assert_eq!(a.resets, 1);
    assert_eq!(a.era_transitions, 2);
    assert_eq!(a.backward, 0);
    assert_eq!(a.missing, 5, "sequence numbers 3, 4, 5 and then 2, 3");
    assert_eq!(a.runs.len(), 2);
    assert_eq!((a.runs[0].missing_from, a.runs[0].missing_to), (3, 5));
    assert_eq!(a.runs[0].era_ordinal, 1);
    assert_eq!((a.runs[1].missing_from, a.runs[1].missing_to), (2, 3));
    assert_eq!(a.runs[1].era_ordinal, 2);
    assert_eq!(
        a.reference_seqs, 16,
        "0..=9 in one era and 0..=5 in the next"
    );
    assert_eq!(a.admitted, 2);
    // Not `Count(3)`, which is what a per-instance subtraction would report and
    // what this test asserted before. Two publishers share this role, and the
    // role's accumulator is the socket: its delta rides on whichever datagram
    // next gets through, from either of them. So `a.admitted` is not a
    // statement about A's loss — it is a statement about where the socket's
    // loss happened to be noticed — and subtracting it exonerates whichever
    // publisher arrived next while charging the other for it. That is the false
    // publisher finding this crate exists to prevent, and the honest answer is
    // that this window cannot separate the two.
    assert_eq!(offline.instances_on_role(PortRole::Mktdata), 2);
    assert_eq!(
        offline.unexplained(&a.instance),
        Some(Unexplained::Unverifiable),
        "a shared socket's admitted loss belongs to no single instance"
    );

    let b = offline
        .instance(&instance(PUBLISHER_B, PortRole::Mktdata))
        .expect("publisher B's instance");
    assert_eq!(b.missing, 0);
    assert_eq!(b.missing_on_arrival, 0);
    assert_eq!(b.era_transitions, 1);
    assert_eq!(b.reference_seqs, 4);
}

/// The one arrangement where the per-instance subtraction is arithmetic on a
/// quantity that exists: one instance on the role, so the socket's accumulator
/// and the instance's loss are the same subject.
#[test]
fn a_role_carrying_one_instance_subtracts_its_admitted_loss() {
    let stream = one_publisher_stream();
    let offline = derive(CaptureDropScope::PortRole, &stream.sent);

    let a = offline
        .instance(&instance(PUBLISHER_A, PortRole::Mktdata))
        .expect("publisher A's instance");
    assert_eq!(offline.instances_on_role(PortRole::Mktdata), 1);
    assert_eq!(a.missing, 3, "sequence numbers 3, 4 and 5");
    assert_eq!(a.admitted, 2);
    assert_eq!(
        offline.unexplained(&a.instance),
        Some(Unexplained::Count(1)),
        "three missing, two of them ours"
    );
}

/// The archive is the only thing between the two halves, so what it does not
/// carry is a disagreement the comparison above would blame on the deriver.
#[test]
fn the_archive_round_trip_preserves_everything_the_deriver_reads() {
    let stream = stream();
    let live_path = derive(CaptureDropScope::PortRole, &stream.sent);
    let offline = through_an_archive(CaptureDropScope::PortRole, &stream);

    assert_eq!(offline.datagrams(), live_path.datagrams());
    assert_eq!(offline.short_datagrams(), live_path.short_datagrams());
    assert_eq!(offline.handle_admitted(), live_path.handle_admitted());
    for expected in live_path.instances() {
        let replayed = offline
            .instance(&expected.instance)
            .unwrap_or_else(|| panic!("the archive lost {:?}", expected.instance));
        // Whole values, so a field added to either side is compared without
        // anyone having to remember to add it here — the run timestamps
        // included, which is what a nanosecond-resolution archive is for.
        assert_eq!(replayed, expected);
    }
}
