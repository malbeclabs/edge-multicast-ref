//! Sequence continuity, reset accounting, and the era rule.
//!
//! Each fault below maps to exactly one counter, asserted, so a fault that moves
//! no counter fails CI rather than waiting to be discovered on a host.

mod common;

use common::{instance_sample, observer_with_sources, Arrival, Datagram, PUBLISHER_A};
use dz_recorder_core::Observer;

/// A stream of in-order datagrams, so that every test below starts from a
/// tracker that has an established position rather than from an empty one.
fn feed_in_order(observer: &mut impl Observer, sequences: &[u64]) {
    for sequence_number in sequences {
        let datagram = Datagram::seq(*sequence_number);
        let payload = datagram.payload();
        observer.on_datagram(&Arrival::default().recorded(&payload));
    }
}

#[test]
fn a_gap_counts_one_gap_and_the_datagrams_it_skipped() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    feed_in_order(&mut observer, &[1, 2, 7]);

    let rendered = metrics.render();
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_sequence_gaps_total", PUBLISHER_A),
        1.0,
        "one discontinuity is one gap"
    );
    assert_eq!(
        instance_sample(
            &rendered,
            "dz_recorder_missing_datagrams_on_arrival_total",
            PUBLISHER_A
        ),
        4.0,
        "3, 4, 5 and 6 were skipped: one gap of four is not four gaps"
    );
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_sequence_current", PUBLISHER_A),
        7.0
    );
}

#[test]
fn a_duplicate_is_counted_as_a_duplicate() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    feed_in_order(&mut observer, &[1, 2, 3, 2]);

    let rendered = metrics.render();
    assert_eq!(
        instance_sample(
            &rendered,
            "dz_recorder_duplicate_datagrams_total",
            PUBLISHER_A
        ),
        1.0
    );
    assert_eq!(
        instance_sample(
            &rendered,
            "dz_recorder_reordered_datagrams_total",
            PUBLISHER_A
        ),
        0.0,
        "a sequence number already seen is a duplicate, never a reordering"
    );
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_sequence_current", PUBLISHER_A),
        3.0,
        "a duplicate does not move the position backwards"
    );
}

#[test]
fn a_late_arrival_inside_the_window_is_counted_as_reordered() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    // 3 is skipped, then arrives after 4 — the reordering the gap was really.
    feed_in_order(&mut observer, &[1, 2, 4, 3]);

    let rendered = metrics.render();
    assert_eq!(
        instance_sample(
            &rendered,
            "dz_recorder_reordered_datagrams_total",
            PUBLISHER_A
        ),
        1.0
    );
    assert_eq!(
        instance_sample(
            &rendered,
            "dz_recorder_duplicate_datagrams_total",
            PUBLISHER_A
        ),
        0.0,
        "a sequence number not yet seen is a reordering, never a duplicate"
    );
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_sequence_gaps_total", PUBLISHER_A),
        1.0,
        "the gap stays counted: a gap count is an upper bound until reordering \
         is subtracted from it"
    );
}

#[test]
fn a_reset_count_transition_is_a_reset_and_not_a_gap() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    feed_in_order(&mut observer, &[1, 2, 3]);
    let datagram = Datagram::seq(0).with_reset(1);
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::default().recorded(&payload));

    let rendered = metrics.render();
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_resets_total", PUBLISHER_A),
        1.0
    );
    assert_eq!(
        instance_sample(
            &rendered,
            "dz_recorder_backward_sequence_total",
            PUBLISHER_A
        ),
        0.0,
        "a reset is not backward motion"
    );
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_sequence_gaps_total", PUBLISHER_A),
        0.0,
        "a reset opens a new sequence space; nothing was skipped"
    );
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_sequence_current", PUBLISHER_A),
        0.0
    );
}

#[test]
fn backward_motion_without_a_reset_is_counted_as_backward() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    // A publisher that restarted its sequence space and left Reset Count alone.
    // The jump is beyond the reordering window, so it cannot be a late arrival.
    feed_in_order(&mut observer, &[100_000, 100_001, 5]);

    let rendered = metrics.render();
    assert_eq!(
        instance_sample(
            &rendered,
            "dz_recorder_backward_sequence_total",
            PUBLISHER_A
        ),
        1.0
    );
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_resets_total", PUBLISHER_A),
        0.0,
        "Reset Count did not move, so this is not a reset"
    );
    assert_eq!(
        instance_sample(
            &rendered,
            "dz_recorder_reordered_datagrams_total",
            PUBLISHER_A
        ),
        0.0,
        "beyond the reordering window it cannot be called a reordering"
    );
}

/// `Reset Count` is a `u8` and it wraps, so an era 256 resets later carries a
/// value already seen. Comparing the wire value would merge the two eras and
/// hide the loss between them, which is the worse failure: a tier that reports
/// nothing is worse than one that reports wrongly.
#[test]
fn an_era_that_wraps_to_a_reset_count_already_seen_advances_the_ordinal() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);

    for (sequence_number, reset_count) in [(1_u64, 5_u8), (2, 5), (0, 6), (1, 6), (0, 5), (1, 5)] {
        let datagram = Datagram::seq(sequence_number).with_reset(reset_count);
        let payload = datagram.payload();
        observer.on_datagram(&Arrival::default().recorded(&payload));
    }

    let rendered = metrics.render();
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_era_ordinal", PUBLISHER_A),
        3.0,
        "the era the first datagram opened, then two transitions in receive \
         order — the third era shares a Reset Count with the first and is still \
         its own era"
    );
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_resets_total", PUBLISHER_A),
        2.0
    );
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_era_transitions_total", PUBLISHER_A),
        3.0,
        "one more than the resets: the first era was opened, not reset into"
    );
    assert_eq!(
        instance_sample(
            &rendered,
            "dz_recorder_backward_sequence_total",
            PUBLISHER_A
        ),
        0.0,
        "returning to a Reset Count already seen must not read as backward motion"
    );
}

/// Kept next to the tests above because it is the same rule seen from the other
/// side: a reset is only a reset when `Reset Count` actually changed.
#[test]
fn an_unchanged_reset_count_across_an_in_order_stream_opens_no_new_era() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    feed_in_order(&mut observer, &[1, 2, 3, 4]);

    let rendered = metrics.render();
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_era_ordinal", PUBLISHER_A),
        1.0
    );
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_resets_total", PUBLISHER_A),
        0.0
    );
}

#[test]
fn a_sequence_number_too_far_ahead_is_refused_rather_than_credited_as_loss() {
    // The asymmetry that made this exploitable: backward motion has always been
    // bounded by the reordering window, and forward motion — the direction that
    // credits a counter — was not. The sequence number is a field on the wire
    // and an any-source join accepts it from anybody.
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    feed_in_order(&mut observer, &[1, 2, 3]);

    let forged = Datagram::seq(u64::MAX);
    let payload = forged.payload();
    observer.on_datagram(&Arrival::default().recorded(&payload));

    let rendered = metrics.render();
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_forward_jump_total", PUBLISHER_A),
        1.0,
        "the datagram is counted, under its own name"
    );
    assert_eq!(
        instance_sample(
            &rendered,
            "dz_recorder_missing_datagrams_on_arrival_total",
            PUBLISHER_A
        ),
        0.0,
        "nothing was lost, and a counter this size cannot be walked back"
    );
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_sequence_gaps_total", PUBLISHER_A),
        0.0
    );
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_sequence_current", PUBLISHER_A),
        3.0,
        "the tracker did not adopt it"
    );
}

#[test]
fn genuine_loss_is_still_counted_after_a_forged_sequence_number() {
    // The half that matters more. Adopting the forged number would put every
    // genuine datagram behind the reordering window, so real loss would never
    // be counted again for the rest of the era — a permanent blinding, from one
    // datagram, on a channel anyone can send to.
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    feed_in_order(&mut observer, &[1, 2, 3]);

    let forged = Datagram::seq(u64::MAX - 1);
    let payload = forged.payload();
    observer.on_datagram(&Arrival::default().recorded(&payload));
    feed_in_order(&mut observer, &[7]);

    let rendered = metrics.render();
    assert_eq!(
        instance_sample(
            &rendered,
            "dz_recorder_missing_datagrams_on_arrival_total",
            PUBLISHER_A
        ),
        3.0,
        "4, 5 and 6 are still missing and still counted"
    );
    assert_eq!(
        instance_sample(
            &rendered,
            "dz_recorder_backward_sequence_total",
            PUBLISHER_A
        ),
        0.0,
        "the real datagrams are not behind anything"
    );
}

#[test]
fn a_sequence_number_above_i64_renders_as_itself_and_not_as_a_negative() {
    // The gauge is an i64 and the field is a u64. A cast that wraps reports -1
    // for a number that is merely large, which reads as an error condition that
    // is not there — and hides the one that is.
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    let high = Datagram::seq(u64::MAX / 2 + 1_000);
    let payload = high.payload();
    observer.on_datagram(&Arrival::default().recorded(&payload));

    let rendered = metrics.render();
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_sequence_current", PUBLISHER_A),
        i64::MAX as f64,
        "saturated at the gauge's ceiling, never wrapped past it"
    );
}
