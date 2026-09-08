//! What the deriver concludes from a stream, and what it refuses to conclude.
//!
//! Every test here drives a real [`Source`](dz_recorder_core::Source), because
//! the trait is the boundary the whole design rests on: a stream fed datagram by
//! datagram would prove something no caller does.
#![forbid(unsafe_code)]

mod common;

use std::net::Ipv4Addr;

use common::{
    derive, derive_with_limits, instance, port_of, Header, Stream, StreamSource, CHANNEL, GROUP,
    PUBLISHER_A, PUBLISHER_B,
};
use dz_edge_core::PortRole;
use dz_recorder_core::CaptureDropScope;
use dz_recorder_loss::DeriverLimits;
use dz_recorder_loss::{LossDeriver, Unexplained};

/// A gap is a run of sequence numbers, its size is how many there were, and the
/// timestamps say where to look rather than how much was lost.
#[test]
fn a_gap_is_one_run_of_missing_sequence_values() {
    let mut stream = Stream::new();
    for seq in 0..3 {
        stream.send(PUBLISHER_A, seq, 0);
    }
    stream.send(PUBLISHER_A, 6, 0);
    stream.send(PUBLISHER_A, 7, 0);

    let report = derive(CaptureDropScope::PortRole, &stream.sent);
    let loss = report
        .instance(&instance(PUBLISHER_A, PortRole::Mktdata))
        .expect("the instance sent five datagrams");

    assert_eq!(loss.runs.len(), 1, "one contiguous run: {:?}", loss.runs);
    let run = &loss.runs[0];
    assert_eq!(run.missing_from, 3);
    assert_eq!(run.missing_to, 5);
    assert_eq!(run.missing_count(), 3);
    assert_eq!(loss.missing, 3);
    assert_eq!(loss.reference_seqs, 8, "the window spans 0..=7");
    assert_eq!(loss.gaps_on_arrival, 1);
    assert_eq!(loss.missing_on_arrival, 3);
    assert_eq!(loss.era_transitions, 1);

    // Placement, and only placement: the datagrams either side of the run.
    assert_eq!(run.before_ts_ns, stream.ts_of(2));
    assert_eq!(run.after_ts_ns, stream.ts_of(3));
    assert_eq!(run.span_ns(), stream.ts_of(3) - stream.ts_of(2));

    // Everything the consuming report keys a row on.
    assert_eq!(run.instance.source, PUBLISHER_A);
    assert_eq!(run.instance.channel_id, CHANNEL);
    assert_eq!(run.instance.dst_port, port_of(PortRole::Mktdata));
    assert_eq!(run.group, GROUP);
    assert_eq!(run.role, PortRole::Mktdata);
    assert_eq!(run.era_ordinal, 1);
    assert_eq!(run.reset_count, 0);
}

/// The same datagram delivered twice is one delivered sequence value.
#[test]
fn a_duplicate_is_counted_and_delivers_nothing() {
    let mut stream = Stream::new();
    for seq in 0..3 {
        stream.send(PUBLISHER_A, seq, 0);
    }
    stream.send(PUBLISHER_A, 1, 0);

    let report = derive(CaptureDropScope::PortRole, &stream.sent);
    let loss = report
        .instance(&instance(PUBLISHER_A, PortRole::Mktdata))
        .expect("the instance sent four datagrams");

    assert_eq!(loss.duplicates, 1);
    assert_eq!(loss.datagrams, 4);
    assert!(loss.runs.is_empty(), "nothing is missing: {:?}", loss.runs);
    assert_eq!(loss.missing, 0);
    assert_eq!(loss.reference_seqs, 3, "three distinct sequence values");
}

/// A late arrival was delivered, so the run it fills is not a run.
///
/// This is where the two halves of the recorder measure different things, and
/// both numbers are here because both are wanted: `missing` is what the archive
/// does not hold, and `missing_on_arrival` is what a live tier could see at the
/// moment it looked absent. They differ by exactly the reordering.
#[test]
fn a_reordered_pair_leaves_no_run_but_is_still_a_gap_on_arrival() {
    let mut stream = Stream::new();
    stream.send(PUBLISHER_A, 0, 0);
    stream.send(PUBLISHER_A, 1, 0);
    stream.send(PUBLISHER_A, 3, 0);
    stream.send(PUBLISHER_A, 2, 0);

    let report = derive(CaptureDropScope::PortRole, &stream.sent);
    let loss = report
        .instance(&instance(PUBLISHER_A, PortRole::Mktdata))
        .expect("the instance sent four datagrams");

    assert_eq!(loss.reordered, 1);
    assert_eq!(loss.duplicates, 0);
    assert!(loss.runs.is_empty(), "nothing is missing: {:?}", loss.runs);
    assert_eq!(loss.missing, 0);
    assert_eq!(loss.gaps_on_arrival, 1);
    assert_eq!(loss.missing_on_arrival, 1);
    assert_eq!(
        loss.missing_on_arrival - loss.reordered,
        loss.missing,
        "the two measures differ by exactly the reordering"
    );
    assert_eq!(loss.reference_seqs, 4);
}

/// A reset opens a new sequence space, so the sequence number that goes
/// backwards across it is neither backward motion nor a gap.
#[test]
fn a_reset_opens_an_era_and_no_run_spans_one() {
    let mut stream = Stream::new();
    for seq in 0..3 {
        stream.send(PUBLISHER_A, seq, 0);
    }
    for seq in 0..3 {
        stream.send(PUBLISHER_A, seq, 1);
    }

    let report = derive(CaptureDropScope::PortRole, &stream.sent);
    let loss = report
        .instance(&instance(PUBLISHER_A, PortRole::Mktdata))
        .expect("the instance sent six datagrams");

    assert_eq!(loss.resets, 1);
    assert_eq!(loss.era_transitions, 2, "the opened era counts as one");
    assert_eq!(loss.backward, 0, "a reset is not backward motion");
    assert!(
        loss.runs.is_empty(),
        "no run spans the era: {:?}",
        loss.runs
    );
    assert_eq!(loss.missing, 0);
    assert_eq!(loss.eras.len(), 2);
    assert_eq!(loss.eras[0].ordinal, 1);
    assert_eq!(loss.eras[0].reset_count, 0);
    assert_eq!(loss.eras[1].ordinal, 2);
    assert_eq!(loss.eras[1].reset_count, 1);
    assert_eq!(loss.eras[1].first_seq, 0);
}

/// A publisher that restarted its sequence space without advancing
/// `Reset Count`. Nothing else in the tier would notice it, and the datagram is
/// still one we hold: it splits the run rather than being absent from it.
#[test]
fn backward_motion_that_is_not_a_reset_is_its_own_finding() {
    let mut stream = Stream::new();
    for seq in 0..4 {
        stream.send(PUBLISHER_A, seq, 0);
    }
    stream.send(PUBLISHER_A, 2_000, 0);
    // 1500 behind the highest, so beyond the reordering window: a reordering
    // and a restarted sequence space are indistinguishable there.
    stream.send(PUBLISHER_A, 500, 0);

    let report = derive(CaptureDropScope::PortRole, &stream.sent);
    let loss = report
        .instance(&instance(PUBLISHER_A, PortRole::Mktdata))
        .expect("the instance sent six datagrams");

    assert_eq!(loss.backward, 1);
    assert_eq!(loss.resets, 0, "no Reset Count advanced");
    assert_eq!(loss.era_transitions, 1, "backward motion opens no era");
    assert_eq!(loss.reordered, 0);
    assert_eq!(loss.runs.len(), 2, "the late datagram splits the run");
    assert_eq!(
        (loss.runs[0].missing_from, loss.runs[0].missing_to),
        (4, 499)
    );
    assert_eq!(
        (loss.runs[1].missing_from, loss.runs[1].missing_to),
        (501, 1_999)
    );
    assert_eq!(loss.missing, 1_995);
    assert_eq!(loss.missing_on_arrival, 1_996);
    assert_eq!(loss.reference_seqs, 2_001);
}

/// The measured case the design settles: two eras carrying the same wire
/// `Reset Count`, the later one missing five datagrams.
///
/// Partitioning by the wire value finds **zero** gaps, because the earlier era's
/// sequence numbers sit at exactly the missing values. Partitioning by the
/// ordinal finds the run and its five datagrams. Inventing a false gap would be
/// the better failure; hiding a real one is the worse.
#[test]
fn an_era_repeating_a_reset_count_already_seen_is_a_new_era() {
    let mut stream = Stream::new();
    for seq in 0..10 {
        stream.send(PUBLISHER_A, seq, 3);
    }
    for seq in 0..2 {
        stream.send(PUBLISHER_A, seq, 4);
    }
    for seq in 0..5 {
        stream.send(PUBLISHER_A, seq, 3);
    }
    stream.send(PUBLISHER_A, 10, 3);
    stream.send(PUBLISHER_A, 11, 3);

    let report = derive(CaptureDropScope::PortRole, &stream.sent);
    let loss = report
        .instance(&instance(PUBLISHER_A, PortRole::Mktdata))
        .expect("the instance sent nineteen datagrams");

    assert_eq!(loss.era_transitions, 3);
    assert_eq!(loss.resets, 2);
    assert_eq!(loss.eras[0].reset_count, loss.eras[2].reset_count);
    assert_eq!(
        loss.eras[2].ordinal, 3,
        "a repeated wire value is a new era"
    );

    assert_eq!(
        loss.runs.len(),
        1,
        "one run, in the third era: {:?}",
        loss.runs
    );
    let run = &loss.runs[0];
    assert_eq!(run.era_ordinal, 3);
    assert_eq!((run.missing_from, run.missing_to), (5, 9));
    assert_eq!(loss.missing, 5);
    assert_eq!(loss.reference_seqs, 10 + 2 + 12);
}

/// Two publishers serving one channel to one group and port are two channel
/// instances. A tracker keyed any coarser reads the alternation as backward
/// motion in one direction and lets one publisher's traffic cover the other's
/// gap in the other.
#[test]
fn two_publishers_on_one_channel_and_port_are_two_instances() {
    let mut stream = Stream::new();
    for seq in 0..3 {
        stream.send(PUBLISHER_A, seq, 0);
        stream.send(PUBLISHER_B, seq, 0);
    }
    stream.send(PUBLISHER_A, 4, 0);
    stream.send(PUBLISHER_B, 3, 0);

    let report = derive(CaptureDropScope::PortRole, &stream.sent);
    assert_eq!(report.instances().len(), 2);

    let a = report
        .instance(&instance(PUBLISHER_A, PortRole::Mktdata))
        .expect("publisher A's instance");
    let b = report
        .instance(&instance(PUBLISHER_B, PortRole::Mktdata))
        .expect("publisher B's instance");

    assert_eq!(a.missing, 1, "A skipped sequence number 3");
    assert_eq!(a.runs.len(), 1);
    assert_eq!((a.runs[0].missing_from, a.runs[0].missing_to), (3, 3));
    assert_eq!(b.missing, 0, "B skipped nothing");
    assert!(b.runs.is_empty());
    assert_eq!(a.backward, 0, "the alternation is not backward motion");
    assert_eq!(b.backward, 0);
    assert_eq!(a.duplicates, 0, "B's sequence numbers are not A's");
    assert_eq!(b.duplicates, 0);
}

/// A schema version this build does not implement is counted and still carries
/// its sequence number. A subscriber must discard such a datagram; anything
/// measuring loss must not, or the sequence number becomes a gap we invented.
#[test]
fn an_unknown_schema_version_is_counted_and_still_carries_its_sequence() {
    let mut stream = Stream::new();
    stream.send(PUBLISHER_A, 0, 0);
    stream.send(PUBLISHER_A, 1, 0);
    let mut unknown = Header::conformant(CHANNEL, 2, 0);
    unknown.schema_version = 0xFE;
    stream.send_header(PUBLISHER_A, PortRole::Mktdata, unknown);
    stream.send(PUBLISHER_A, 3, 0);

    let report = derive(CaptureDropScope::PortRole, &stream.sent);
    let loss = report
        .instance(&instance(PUBLISHER_A, PortRole::Mktdata))
        .expect("the instance sent four datagrams");

    assert_eq!(loss.unknown_schema, 1);
    assert_eq!(loss.datagrams, 4);
    assert!(
        loss.runs.is_empty(),
        "sequence number 2 was delivered: {:?}",
        loss.runs
    );
    assert_eq!(loss.missing, 0);
    assert_eq!(loss.reference_seqs, 4);
}

/// A datagram too short to hold a header is attributable to no channel
/// instance, and is counted rather than dropped on the floor: a window that
/// silently skipped it would under-report the traffic it claims to describe.
#[test]
fn a_short_datagram_is_counted_and_never_skipped() {
    let mut stream = Stream::new();
    stream.send(PUBLISHER_A, 0, 0);
    stream.send(PUBLISHER_A, 1, 0);
    stream.send_bytes(PUBLISHER_A, PortRole::Mktdata, vec![0x5A; 12]);
    stream.send(PUBLISHER_A, 2, 0);

    let report = derive(CaptureDropScope::PortRole, &stream.sent);
    assert_eq!(report.short_datagrams(), 1);
    assert_eq!(report.datagrams(), 4, "every datagram the source produced");

    let loss = report
        .instance(&instance(PUBLISHER_A, PortRole::Mktdata))
        .expect("three datagrams carried a header");
    assert_eq!(loss.datagrams, 3);
    assert_eq!(loss.missing, 0);
}

/// At port-role scope there is one loss accumulator per role because there is
/// one socket per role, so the per-instance sum is a valid subtraction and the
/// residue is what a dashboard shows.
#[test]
fn admitted_loss_is_subtracted_at_port_role_scope() {
    let mut stream = Stream::new();
    stream.send(PUBLISHER_A, 0, 0);
    stream.send(PUBLISHER_A, 1, 0);
    stream.send_after_loss(PUBLISHER_A, 7, 0, 3);

    let key = instance(PUBLISHER_A, PortRole::Mktdata);
    let report = derive(CaptureDropScope::PortRole, &stream.sent);
    let loss = report.instance(&key).expect("the instance");

    assert_eq!(loss.missing, 5, "sequence numbers 2..=6");
    assert_eq!(loss.admitted, 3);
    assert_eq!(
        report.unexplained(&key),
        Some(Unexplained::Count(2)),
        "five missing, three of them ours"
    );
    assert_eq!(report.admitted_on_role(PortRole::Mktdata), 3);
    assert_eq!(report.handle_admitted(), 3);
}

/// The measured mixed-scope case: a ring dropped forty `mktdata` datagrams and
/// the delta rode on the next `refdata` datagram that got through. Summing
/// admitted drops per instance reports forty unexplained against `mktdata` and a
/// false publisher finding, while the handle had admitted all forty.
#[test]
fn at_capture_handle_scope_an_admitting_handle_explains_nothing_per_instance() {
    let mut stream = Stream::new();
    stream.send(PUBLISHER_A, 0, 0);
    stream.send(PUBLISHER_A, 1, 0);
    stream.send(PUBLISHER_A, 42, 0);
    stream.send_header_after_loss(
        PUBLISHER_A,
        PortRole::Refdata,
        Header::conformant(CHANNEL, 0, 0),
        40,
    );

    let mktdata = instance(PUBLISHER_A, PortRole::Mktdata);
    let report = derive(CaptureDropScope::CaptureHandle, &stream.sent);
    let loss = report.instance(&mktdata).expect("the mktdata instance");

    assert_eq!(loss.missing, 40);
    assert_eq!(loss.admitted, 0, "the delta rode on another role");
    assert_eq!(report.handle_admitted(), 40);
    assert_eq!(
        report.unexplained(&mktdata),
        Some(Unexplained::Unverifiable),
        "a handle that dropped anything cannot say which role lost it"
    );

    // The same stream at the scope it is not: a scope guessed rather than taken
    // from the archive is exactly this difference.
    let as_if_per_role = derive(CaptureDropScope::PortRole, &stream.sent);
    assert_eq!(
        as_if_per_role.unexplained(&mktdata),
        Some(Unexplained::Count(40))
    );
}

/// The common case, and the interesting one: a recorder admitting nothing turns
/// every gap into someone else's, with evidence rather than by inference.
#[test]
fn at_capture_handle_scope_a_handle_admitting_nothing_exonerates_itself() {
    let mut stream = Stream::new();
    stream.send(PUBLISHER_A, 0, 0);
    stream.send(PUBLISHER_A, 1, 0);
    stream.send(PUBLISHER_A, 7, 0);

    let key = instance(PUBLISHER_A, PortRole::Mktdata);
    let report = derive(CaptureDropScope::CaptureHandle, &stream.sent);

    assert_eq!(report.handle_admitted(), 0);
    assert_eq!(report.unexplained(&key), Some(Unexplained::Count(5)));
}

/// What a missing count is a share of, summed over the eras. Neither era's span
/// alone is the answer, and the two are not one span: a reset restarts the
/// sequence space, so the values overlap.
#[test]
fn reference_seqs_spans_every_era() {
    let mut stream = Stream::new();
    for seq in 0..10 {
        stream.send(PUBLISHER_A, seq, 0);
    }
    for seq in [0, 1, 3, 4] {
        stream.send(PUBLISHER_A, seq, 1);
    }

    let report = derive(CaptureDropScope::PortRole, &stream.sent);
    let loss = report
        .instance(&instance(PUBLISHER_A, PortRole::Mktdata))
        .expect("the instance");

    assert_eq!(loss.eras[0].reference_seqs(), 10);
    assert_eq!(loss.eras[1].reference_seqs(), 5);
    assert_eq!(loss.reference_seqs, 15);
    assert_eq!(loss.missing, 1);
    assert_eq!(loss.eras[0].missing(), 0);
    assert_eq!(loss.eras[1].missing(), 1);
}

/// A source address never seen before opens a series silently: no gap, no loss.
/// A tunnel address is a lease, it can be reassigned under a live host, and a
/// reassignment must not become a publisher finding.
#[test]
fn a_source_never_seen_before_opens_a_series_silently() {
    let mut stream = Stream::new();
    stream.send(PUBLISHER_A, 5_000, 0);
    stream.send(PUBLISHER_A, 5_001, 0);
    stream.send(PUBLISHER_B, 0, 0);
    stream.send(PUBLISHER_B, 1, 0);

    let report = derive(CaptureDropScope::PortRole, &stream.sent);
    assert_eq!(report.runs().count(), 0, "no series opened with a gap");
    for loss in report.instances() {
        assert_eq!(loss.missing, 0, "{:?}", loss.instance);
        assert_eq!(loss.gaps_on_arrival, 0);
        assert_eq!(loss.era_transitions, 1);
    }
}

/// A source that failed before EOF is an error and never a window. A short
/// replay read as a complete one is a sequence gap with nothing admitted behind
/// it, and that is a publisher finding drawn from our own truncation.
#[test]
fn a_source_that_failed_before_eof_is_not_a_window() {
    let mut stream = Stream::new();
    for seq in 0..4 {
        stream.send(PUBLISHER_A, seq, 0);
    }

    let mut deriver = LossDeriver::new(CaptureDropScope::PortRole);
    let mut source = StreamSource::failing_at(&stream.sent, 2);
    let error = deriver
        .drive(&mut source)
        .expect_err("a source that failed is not exhausted");
    assert!(
        error.to_string().contains("before it was exhausted"),
        "{error}"
    );
}

#[test]
fn a_sequence_number_no_outage_explains_does_not_open_an_era_around_itself() {
    // The deriver does not validate the wire, and the sequence number is a
    // wire value. Delivering one at u64::MAX into the era's range set makes
    // that era span the distance to it: a run claiming ~1.8e19 missing values,
    // which is a fabricated finding rather than an absurd-looking one — and the
    // rows design expands runs with arrayJoin(range(...)), so a loader would
    // try to materialise it. It also broke the arithmetic: `end + 1` panics in
    // a debug build and wraps to zero in a release one, silently reordering the
    // range set and losing a genuine missing value with it.
    let mut stream = Stream::new();
    for seq in 1..=20 {
        stream.send(PUBLISHER_A, seq, 0);
    }
    stream.send(PUBLISHER_A, u64::MAX, 0);
    for seq in 25..=40 {
        stream.send(PUBLISHER_A, seq, 0);
    }

    let report = derive(CaptureDropScope::PortRole, &stream.sent);
    let loss = report
        .instance(&instance(PUBLISHER_A, PortRole::Mktdata))
        .expect("the instance");

    assert_eq!(loss.forward_jumps, 1, "counted, under its own name");
    assert_eq!(
        loss.missing, 4,
        "21, 22, 23 and 24, and nothing the forged value invented"
    );
    assert_eq!(loss.runs.len(), 1);
    assert_eq!(
        (loss.runs[0].missing_from, loss.runs[0].missing_to),
        (21, 24)
    );
    assert_eq!(loss.reference_seqs, 40, "1..=40, and not to u64::MAX");
}

#[test]
fn a_straggler_across_a_reset_is_delivered_into_the_era_it_came_from() {
    // A datagram still carrying the previous Reset Count, arriving just after a
    // reset: the network held it back across the boundary. Opening an era for
    // it splits the real one, because the next datagram of the new era opens a
    // third — and runs are derived within an era, so everything missing across
    // the split is reported by neither half. The loss disappears at exactly the
    // moment the publisher restarted, which is when it is most worth seeing.
    let mut stream = Stream::new();
    for seq in 0..=2 {
        stream.send(PUBLISHER_A, seq, 0);
    }
    for seq in 0..=2 {
        stream.send(PUBLISHER_A, seq, 1);
    }
    // Late, from the era before: it extends that space rather than restarting
    // one, which is what tells it apart from a reused Reset Count.
    stream.send(PUBLISHER_A, 5, 0);
    for seq in 5..=6 {
        stream.send(PUBLISHER_A, seq, 1);
    }

    let report = derive(CaptureDropScope::PortRole, &stream.sent);
    let loss = report
        .instance(&instance(PUBLISHER_A, PortRole::Mktdata))
        .expect("the instance");

    assert_eq!(loss.resets, 1, "one restart, not three");
    assert_eq!(loss.era_transitions, 2);
    assert_eq!(
        loss.missing, 4,
        "3 and 4 in the first era, 3 and 4 in the second — and nothing hidden by a split"
    );
    assert_eq!(loss.runs.len(), 2);
}

#[test]
fn a_role_carrying_two_instances_cannot_attribute_the_sockets_admitted_loss() {
    // The accumulator is the socket, not the channel. Its delta rides on
    // whichever datagram next gets through, from any instance on that group and
    // port — so subtracting one instance's share exonerates whoever arrived
    // next and charges the other for loss the recorder caused. That is the
    // false publisher finding this crate exists to prevent, and it is reachable
    // with two publishers or with two Channel IDs on one port.
    let mut stream = Stream::new();
    for seq in 0..=2 {
        stream.send(PUBLISHER_A, seq, 0);
    }
    // The socket loses five of A's datagrams, and the delta rides on B's next.
    stream.send_after_loss(PUBLISHER_B, 0, 0, 5);
    stream.send(PUBLISHER_A, 8, 0);

    let report = derive(CaptureDropScope::PortRole, &stream.sent);
    assert_eq!(report.instances_on_role(PortRole::Mktdata), 2);

    let a = instance(PUBLISHER_A, PortRole::Mktdata);
    let loss = report.instance(&a).expect("publisher A");
    assert_eq!(loss.missing, 5, "3 through 7");
    assert_eq!(
        loss.admitted, 0,
        "the delta arrived on B's datagram, so A's instance never saw it"
    );
    assert_eq!(
        report.unexplained(&a),
        Some(Unexplained::Unverifiable),
        "reporting Count(5) here is a publisher finding the recorder manufactured"
    );
}

#[test]
fn a_descending_stream_is_bounded_rather_than_quadratic() {
    // Strictly descending sequence numbers insert a new disjoint range at the
    // front of the vector on every datagram, which is quadratic in the number
    // of datagrams — minutes for a few hundred thousand, on a thread that may
    // be a live capture's. `observe` is public for exactly that use.
    let limits = DeriverLimits {
        max_ranges_per_era: 64,
        ..DeriverLimits::default()
    };
    let mut stream = Stream::new();
    for seq in (0..1_000).rev() {
        stream.send(PUBLISHER_A, seq * 2, 0);
    }

    let started = std::time::Instant::now();
    let report = derive_with_limits(CaptureDropScope::PortRole, &stream.sent, limits);
    let elapsed = started.elapsed();

    let loss = report
        .instance(&instance(PUBLISHER_A, PortRole::Mktdata))
        .expect("the instance");
    assert!(
        loss.ranges_refused > 0,
        "the bound was never reached, so this test is not exercising it"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "1,000 descending datagrams took {elapsed:?}"
    );
}

#[test]
fn instances_past_the_bound_are_refused_and_counted() {
    // An any-source join accepts datagrams from any sender, so the key space is
    // not this crate's to trust. The health tier bounds the same space with a
    // counted refusal; a window that quietly stopped tracking would report a
    // feed as silent.
    let limits = DeriverLimits {
        max_instances: 4,
        ..DeriverLimits::default()
    };
    let mut stream = Stream::new();
    for n in 0..20u32 {
        stream.send(Ipv4Addr::from(0xc0000200 + n), 0, 0);
    }

    let report = derive_with_limits(CaptureDropScope::PortRole, &stream.sent, limits);
    assert_eq!(report.instances().len(), 4);
    assert_eq!(report.instances_refused(), 16);
}

#[test]
fn two_groups_keyed_to_one_instance_are_counted_rather_than_merged_in_silence() {
    // ChannelInstance carries no group, and in AF_PACKET mode the filter is a
    // cross product of the joined groups and the ports — so two groups on one
    // port key to the same instance and the row is labelled with whichever
    // arrived first. Not correctable here; what matters is that a reader can
    // see the row describes two groups rather than believing the label.
    let mut stream = Stream::new();
    stream.send(PUBLISHER_A, 0, 0);
    stream.send_to_group(PUBLISHER_A, 1, 0, Ipv4Addr::new(233, 252, 0, 42));

    let report = derive(CaptureDropScope::PortRole, &stream.sent);
    let loss = report
        .instance(&instance(PUBLISHER_A, PortRole::Mktdata))
        .expect("the instance");
    assert_eq!(loss.group_mismatches, 1);
}

#[test]
fn a_forged_opening_sequence_number_does_not_fabricate_an_era_around_itself() {
    // The asymmetry the forward bound left behind. The tracker bounds a jump
    // from an established position, but the value that *opens* an era is
    // adopted with nothing to compare it against — so a forged opener sets the
    // era's far edge, every ordinary datagram after it falls behind by the
    // whole distance, and that is backward motion: no gap credited, no forward
    // jump counted, and an era spanning 1.8e19 values reported as one run.
    // `forward_jumps` stays at zero throughout, which is why the forward bound
    // could not catch it.
    let mut stream = Stream::new();
    stream.send(PUBLISHER_A, u64::MAX, 0);
    for seq in 1..=3 {
        stream.send(PUBLISHER_A, seq, 0);
    }

    let report = derive(CaptureDropScope::PortRole, &stream.sent);
    let loss = report
        .instance(&instance(PUBLISHER_A, PortRole::Mktdata))
        .expect("the instance");

    assert_eq!(loss.forward_jumps, 0, "nothing jumped forward");
    assert_eq!(
        loss.implausible_deliveries, 3,
        "the three ordinary datagrams are the ones too far from the opener"
    );
    assert_eq!(loss.missing, 0, "there is no run to fabricate");
    assert!(loss.runs.is_empty());
    assert_eq!(
        loss.reference_seqs, 1,
        "the era holds the one value it opened on"
    );
}

#[test]
fn an_era_that_opens_normally_refuses_a_value_no_outage_puts_in_it() {
    // The same bound from the other side: an ordinary era, and a datagram whose
    // number is further from it than any outage explains. Bounding the era's
    // span rather than the direction of travel is what covers both.
    let mut stream = Stream::new();
    for seq in 1..=3 {
        stream.send(PUBLISHER_A, seq, 0);
    }
    stream.send(PUBLISHER_A, u64::MAX, 0);
    for seq in 4..=6 {
        stream.send(PUBLISHER_A, seq, 0);
    }

    let report = derive(CaptureDropScope::PortRole, &stream.sent);
    let loss = report
        .instance(&instance(PUBLISHER_A, PortRole::Mktdata))
        .expect("the instance");

    assert_eq!(loss.forward_jumps, 1, "this one the tracker saw coming");
    assert_eq!(loss.missing, 0);
    assert_eq!(
        loss.reference_seqs, 6,
        "1..=6, and nothing the forged value opened"
    );
}

/// An era's anchor is where it *opened*, and a datagram arriving afterwards
/// below that value does not move it.
///
/// The distinction is load-bearing for the offline tier: an era is identified by
/// a rank over the openings, and a range join from a per-datagram row resolves
/// the era by the anchor's receive stamp. Deriving either from the delivered
/// span would make an era's identity depend on a datagram that arrived later —
/// so a reordering, or backward motion the archive holds anyway, would renumber
/// history and move a boundary that had already been recorded.
#[test]
fn an_eras_anchor_is_where_it_opened_and_not_its_lowest_delivered_value() {
    let mut stream = Stream::new();
    // The era opens at 100, and 98 arrives late — inside the reordering window,
    // so it is a late delivery into this era rather than a new space.
    stream.send(PUBLISHER_A, 100, 0);
    stream.send(PUBLISHER_A, 101, 0);
    stream.send(PUBLISHER_A, 98, 0);

    let report = derive(CaptureDropScope::PortRole, &stream.sent);
    let loss = report
        .instance(&instance(PUBLISHER_A, PortRole::Mktdata))
        .expect("the instance sent three datagrams");

    assert_eq!(loss.eras.len(), 1, "one era: {:?}", loss.eras);
    let era = &loss.eras[0];
    assert_eq!(era.anchor_seq, 100, "the opening value");
    assert_eq!(
        era.anchor_ts_ns,
        stream.ts_of(0),
        "the opening datagram's own stamp"
    );
    assert_eq!(era.first_seq, 98, "the span did widen downwards");
    assert!(
        era.anchor_seq > era.first_seq,
        "the anchor and the span are the same value here, so this test asserts nothing"
    );
    assert!(
        era.anchor_ts_ns < stream.ts_of(2),
        "the anchor took the late arrival's stamp"
    );
}
