//! The bounded, least-recently-seen channel-instance map, and the rule that a
//! source address not seen before opens a new series in silence.

mod common;

use std::net::Ipv4Addr;
use std::time::Duration;

use common::{
    has_sample, instance_sample, observer_with_limits, observer_with_sources, sample, Arrival,
    Datagram, FEED, PUBLISHER_A, PUBLISHER_B, SECOND_NS, STRANGER, T0,
};
use dz_edge_core::PortRole;
use dz_recorder_core::Observer;
use dz_recorder_health::InstanceLimits;

/// A tunnel address is a lease. It can be reassigned under a live host, and a
/// reassignment must not page: the new address's first datagram is the start of
/// its own series, whatever sequence number it carries.
#[test]
fn a_new_source_address_opens_a_series_silently() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);

    for sequence_number in [1_u64, 2, 3] {
        let datagram = Datagram::seq(sequence_number);
        let payload = datagram.payload();
        observer.on_datagram(&Arrival::default().recorded(&payload));
    }
    // A second publisher, far ahead in its own sequence space.
    let datagram = Datagram::seq(9_000_000);
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::from(PUBLISHER_B).recorded(&payload));

    let rendered = metrics.render();
    for source in [PUBLISHER_A, PUBLISHER_B] {
        assert_eq!(
            instance_sample(&rendered, "dz_recorder_sequence_gaps_total", source),
            0.0,
            "{source} must not be charged a gap for another publisher's position"
        );
        assert_eq!(
            instance_sample(
                &rendered,
                "dz_recorder_missing_datagrams_on_arrival_total",
                source
            ),
            0.0
        );
        assert_eq!(
            instance_sample(&rendered, "dz_recorder_backward_sequence_total", source),
            0.0,
            "{source} must not read as backward motion against another publisher"
        );
    }
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_sequence_current", PUBLISHER_A),
        3.0
    );
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_sequence_current", PUBLISHER_B),
        9_000_000.0,
        "each publisher advances its own sequence space"
    );
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_instances_opened_total",
            &[("feed", FEED)]
        ),
        2.0,
        "the silence is still visible here, which is the point of the counter"
    );
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_instances_tracked",
            &[("feed", FEED)]
        ),
        2.0
    );
}

/// An any-source join accepts datagrams from any sender, so the key space is not
/// ours to trust.
#[test]
fn the_instance_map_evicts_the_least_recently_seen() {
    let (metrics, mut observer) = observer_with_limits(
        &[PUBLISHER_A],
        InstanceLimits {
            max_instances: 2,
            // Anything already seen may be evicted, so the eviction under test
            // is the ordering and not the age gate.
            min_evict_age: Duration::ZERO,
            ..InstanceLimits::default()
        },
    );

    // Two strangers arrive, the first of them longest ago.
    let first = Ipv4Addr::new(203, 0, 113, 1);
    let second = Ipv4Addr::new(203, 0, 113, 2);
    for (index, source) in [first, second].into_iter().enumerate() {
        let datagram = Datagram::seq(1);
        let payload = datagram.payload();
        observer.on_datagram(
            &Arrival::from(source)
                .at(T0 + index as u64 * SECOND_NS)
                .recorded(&payload),
        );
    }
    assert_eq!(observer.instances_tracked(), 2);

    let datagram = Datagram::seq(1);
    let payload = datagram.payload();
    observer.on_datagram(
        &Arrival::from(STRANGER)
            .at(T0 + 10 * SECOND_NS)
            .recorded(&payload),
    );

    assert_eq!(
        observer.instances_tracked(),
        2,
        "the map must stay inside its bound"
    );
    let rendered = metrics.render();
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_instances_evicted_total",
            &[("feed", FEED)]
        ),
        1.0,
        "eviction is counted, never silent"
    );
    assert!(
        !has_sample(
            &rendered,
            "dz_recorder_sequence_current",
            &[("source", &first.to_string())]
        ),
        "an evicted stranger's series is dropped, or the bound just moves one \
         layer down into the label vectors"
    );
    assert!(has_sample(
        &rendered,
        "dz_recorder_sequence_current",
        &[("source", &STRANGER.to_string())]
    ));
    assert!(
        has_sample(
            &rendered,
            "dz_recorder_sequence_current",
            &[("source", &PUBLISHER_A.to_string())]
        ),
        "a declared source's series was pre-created and must survive: an \
         operator's own publisher does not vanish from a dashboard because \
         strangers filled a map"
    );
}

/// Opening an instance allocates. Without a minimum age, a sender emitting one
/// datagram per source address would put that allocation on every datagram.
#[test]
fn a_map_full_of_live_instances_refuses_a_newcomer_rather_than_evicting() {
    let (metrics, mut observer) = observer_with_limits(
        &[],
        InstanceLimits {
            max_instances: 1,
            min_evict_age: Duration::from_secs(60),
            ..InstanceLimits::default()
        },
    );

    let datagram = Datagram::seq(1);
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::from(PUBLISHER_A).at(T0).recorded(&payload));
    // One second later: the resident instance is far too fresh to evict.
    observer.on_datagram(
        &Arrival::from(STRANGER)
            .at(T0 + SECOND_NS)
            .recorded(&payload),
    );

    let rendered = metrics.render();
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_instances_refused_total",
            &[("feed", FEED)]
        ),
        1.0
    );
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_instances_evicted_total",
            &[("feed", FEED)]
        ),
        0.0,
        "a live instance must not be evicted for a newcomer"
    );
    assert!(!has_sample(
        &rendered,
        "dz_recorder_sequence_current",
        &[("source", &STRANGER.to_string())]
    ));
    // Once the resident has been quiet longer than the gate, the newcomer is
    // admitted exactly as a genuine tunnel reassignment would be.
    observer.on_datagram(
        &Arrival::from(STRANGER)
            .at(T0 + 61 * SECOND_NS)
            .recorded(&payload),
    );
    let rendered = metrics.render();
    assert!(has_sample(
        &rendered,
        "dz_recorder_sequence_current",
        &[("source", &STRANGER.to_string())]
    ));
}

/// Every other series is keyed on a role this recorder was told about, so a
/// datagram on an undeclared one is counted and nothing else is concluded.
#[test]
fn a_datagram_on_an_undeclared_port_role_is_counted_and_not_tracked() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    let datagram = Datagram::seq(1);
    let payload = datagram.payload();
    let mut arrival = Arrival::from(PUBLISHER_A);
    arrival.role = PortRole::Snapshot;
    arrival.port = common::SNAPSHOT_PORT;
    observer.on_datagram(&arrival.recorded(&payload));

    assert_eq!(observer.instances_tracked(), 0);
    let rendered = metrics.render();
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_datagrams_unexpected_role_total",
            &[("feed", FEED)]
        ),
        1.0
    );
    assert!(!has_sample(
        &rendered,
        "dz_recorder_datagrams_total",
        &[("port_role", "snapshot")]
    ));
}

#[test]
fn an_observer_on_an_undeclared_feed_is_refused() {
    let metrics = std::sync::Arc::new(common::metrics_with_sources(&[PUBLISHER_A]));
    let result = dz_recorder_health::HealthObserver::new(
        metrics,
        "a-feed-nobody-declared",
        InstanceLimits::default(),
        dz_recorder_core::CaptureDropScope::PortRole,
    );
    match result {
        Err(error) => assert_eq!(
            error,
            dz_recorder_health::HealthError::UnknownFeed {
                feed: "a-feed-nobody-declared".to_owned()
            }
        ),
        Ok(_) => panic!(
            "an undeclared feed has no pre-created series, so an observer on it \
             must be refused rather than left to emit series that first appear \
             after the traffic they describe"
        ),
    }
}

/// Fills the instance map with datagrams from addresses nobody declared.
fn flood_with_strangers(observer: &mut impl Observer, count: usize, at_ns: u64) {
    for n in 0..count {
        let octets = u32::try_from(n).expect("a test count fits a u32");
        let stranger = Ipv4Addr::from(0x0100_0000 + octets);
        let datagram = Datagram::seq(1);
        let payload = datagram.payload();
        observer.on_datagram(&Arrival::from(stranger).at(at_ns).recorded(&payload));
    }
}

#[test]
fn a_declared_publisher_is_not_the_first_thing_evicted_by_a_flood_of_strangers() {
    // Ordering victims on age alone makes the declared publisher the preferred
    // one: the default eviction age is 60 seconds and a heartbeating channel
    // can be quiet for five minutes, so the feed being watched is the feed the
    // watching sheds first. Its tracker goes with it and the next datagram
    // reopens the instance silently, which is real loss reading as zero.
    let limits = InstanceLimits {
        max_instances: 8,
        min_evict_age: Duration::from_secs(60),
        ..InstanceLimits::default()
    };
    let (metrics, mut observer) = observer_with_limits(&[PUBLISHER_A], limits);

    let datagram = Datagram::seq(1);
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::from(PUBLISHER_A).at(T0).recorded(&payload));

    // Strangers fill the map, then keep arriving long after everything in it is
    // old enough to evict.
    flood_with_strangers(&mut observer, 7, T0 + SECOND_NS);
    flood_with_strangers(&mut observer, 40, T0 + 600 * SECOND_NS);

    // The publisher's own sequence space carried on while it was quiet.
    let later = Datagram::seq(2);
    let payload = later.payload();
    observer.on_datagram(
        &Arrival::from(PUBLISHER_A)
            .at(T0 + 700 * SECOND_NS)
            .recorded(&payload),
    );

    let rendered = metrics.render();
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_sequence_current", PUBLISHER_A),
        2.0,
        "the declared publisher's tracker did not survive the flood"
    );
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_era_ordinal", PUBLISHER_A),
        1.0,
        "it was evicted and reopened, which reads as a new era"
    );
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_declared_instances_evicted_total",
            &[("feed", FEED)]
        ),
        0.0
    );
}

#[test]
fn a_declared_publisher_is_admitted_against_a_stranger_of_any_age() {
    // Admission has the same failure as eviction: refused entry, a declared
    // publisher never becomes a declared instance at all. The map stays
    // bounded — a stranger is still evicted to make the room — and between an
    // unknown sender and the publisher named in the configuration, the
    // configuration wins.
    let limits = InstanceLimits {
        max_instances: 4,
        min_evict_age: Duration::from_secs(60),
        ..InstanceLimits::default()
    };
    let (metrics, mut observer) = observer_with_limits(&[PUBLISHER_A], limits);

    // A full map of strangers, all seen just now, so none is old enough.
    flood_with_strangers(&mut observer, 4, T0);

    let datagram = Datagram::seq(1);
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::from(PUBLISHER_A).at(T0).recorded(&payload));

    let rendered = metrics.render();
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_sequence_current", PUBLISHER_A),
        1.0,
        "the declared publisher was refused entry to its own recorder"
    );
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_instances_tracked",
            &[("feed", FEED)]
        ),
        4.0,
        "and the map is still bounded"
    );
}

#[test]
fn a_full_map_refuses_a_stranger_without_scanning_itself() {
    // The refusal path is the hot one under a spoofed-source flood: every
    // datagram from an unknown key reaches it. A scan of the whole map for each
    // of them is tens of microseconds on the drain thread, which is the record
    // path shedding datagrams in order to answer a question about metrics — the
    // Observer contract forbids exactly that. The budget below is many times an
    // O(1) refusal and far under a scan of 4096 entries.
    let limits = InstanceLimits {
        max_instances: 4096,
        min_evict_age: Duration::from_secs(300),
        ..InstanceLimits::default()
    };
    let (_metrics, mut observer) = observer_with_limits(&[PUBLISHER_A], limits);
    flood_with_strangers(&mut observer, 4096, T0);

    let datagram = Datagram::seq(1);
    let payload = datagram.payload();
    let refusals = 20_000;
    let started = std::time::Instant::now();
    for octets in 0..refusals {
        let stranger = Ipv4Addr::from(0x0a00_0000 + octets);
        observer.on_datagram(&Arrival::from(stranger).at(T0).recorded(&payload));
    }
    let each = started.elapsed() / refusals;

    assert!(
        each < Duration::from_micros(2),
        "a refusal cost {each:?}, which is a scan of the map and not a comparison"
    );
}

#[test]
fn a_reopened_declared_instance_keeps_its_era_ordinal() {
    // era_ordinal is documented as monotonic and as the value to group an era
    // by, and era_transitions_total as resets + 1 for a live instance. A
    // declared source's series survive eviction, so a tracker that restarted
    // the ordinal at 1 would merge two eras under one number and break both
    // statements while the gauge that carries them stayed live.
    let limits = InstanceLimits {
        max_instances: 2,
        min_evict_age: Duration::from_secs(1),
        ..InstanceLimits::default()
    };
    let (metrics, mut observer) = observer_with_limits(&[PUBLISHER_A], limits);

    // Two eras before the eviction.
    for (sequence_number, reset_count) in [(1_u64, 0_u8), (2, 0), (1, 1)] {
        let datagram = Datagram::seq(sequence_number).with_reset(reset_count);
        let payload = datagram.payload();
        observer.on_datagram(&Arrival::from(PUBLISHER_A).at(T0).recorded(&payload));
    }
    assert_eq!(
        instance_sample(&metrics.render(), "dz_recorder_era_ordinal", PUBLISHER_A),
        2.0,
        "two eras before anything is evicted"
    );

    // A declared arrival displaces strangers, so the publisher is evicted only
    // by another declared instance: two more channels on the same declared
    // address, which is the case that fills a map of declared entries.
    for channel_id in [7_u8, 8] {
        let mut datagram = Datagram::seq(1);
        datagram.channel_id = channel_id;
        let payload = datagram.payload();
        observer.on_datagram(
            &Arrival::from(PUBLISHER_A)
                .at(T0 + 10 * SECOND_NS)
                .recorded(&payload),
        );
    }

    // The original channel returns, in a third era.
    let datagram = Datagram::seq(1).with_reset(2);
    let payload = datagram.payload();
    observer.on_datagram(
        &Arrival::from(PUBLISHER_A)
            .at(T0 + 20 * SECOND_NS)
            .recorded(&payload),
    );

    assert_eq!(
        instance_sample(&metrics.render(), "dz_recorder_era_ordinal", PUBLISHER_A),
        3.0,
        "the ordinal belongs to the instance, which outlived the entry tracking it"
    );
}

#[test]
fn the_channel_wide_cadence_histogram_goes_with_the_last_instance_on_its_channel() {
    // Every other series of an evicted stranger is removed, and this one — keyed
    // (feed, role, channel) and shared by every instance on the channel — was
    // not. Left behind, it is the bound this removal exists to hold, moved one
    // layer down: cycle channel_id and the label vector keeps a histogram per
    // channel the feed never carried.
    let limits = InstanceLimits {
        max_instances: 1,
        min_evict_age: Duration::from_secs(0),
        ..InstanceLimits::default()
    };
    let (metrics, mut observer) = observer_with_limits(&[PUBLISHER_A], limits);

    let mut datagram = Datagram::seq(1);
    datagram.channel_id = 31;
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::from(STRANGER).at(T0).recorded(&payload));
    assert!(
        has_sample(
            &metrics.render(),
            "dz_recorder_heartbeat_interval_seconds_count",
            &[("feed", FEED), ("port_role", "mktdata"), ("channel", "31")]
        ),
        "the channel's cadence series exists while an instance holds it"
    );

    // A second stranger on another channel evicts the first.
    let mut other = Datagram::seq(1);
    other.channel_id = 32;
    let payload = other.payload();
    observer.on_datagram(
        &Arrival::from(PUBLISHER_B)
            .at(T0 + SECOND_NS)
            .recorded(&payload),
    );

    assert!(
        !has_sample(
            &metrics.render(),
            "dz_recorder_heartbeat_interval_seconds_count",
            &[("feed", FEED), ("port_role", "mktdata"), ("channel", "31")]
        ),
        "an evicted instance kept the one series eviction could not reach"
    );
}

#[test]
fn an_undeclared_channel_from_a_declared_address_is_a_stranger() {
    // Eviction survival is keyed on the whole instance — role, channel and
    // source — while declared status was decided on the address alone. Spoofing
    // a declared address while cycling Channel ID then kept series alive on
    // channels the feed never carried, and each one also displaced traffic that
    // was genuinely declared: the bounded map's guarantee defeated one label at
    // a time.
    let limits = InstanceLimits {
        max_instances: 2,
        min_evict_age: Duration::from_secs(0),
        ..InstanceLimits::default()
    };
    let (metrics, mut observer) = observer_with_limits(&[PUBLISHER_A], limits);

    // Channel 0 is declared by the test's metric set; 200 is not.
    let mut forged = Datagram::seq(1);
    forged.channel_id = 200;
    let payload = forged.payload();
    observer.on_datagram(&Arrival::from(PUBLISHER_A).at(T0).recorded(&payload));

    let declared = Datagram::seq(1);
    let payload = declared.payload();
    observer.on_datagram(
        &Arrival::from(PUBLISHER_A)
            .at(T0 + SECOND_NS)
            .recorded(&payload),
    );

    // A stranger arrives into a full map. The undeclared channel is what it may
    // displace; the declared one is not.
    let other = Datagram::seq(1);
    let payload = other.payload();
    observer.on_datagram(
        &Arrival::from(STRANGER)
            .at(T0 + 2 * SECOND_NS)
            .recorded(&payload),
    );

    let rendered = metrics.render();
    assert_eq!(
        instance_sample(&rendered, "dz_recorder_sequence_current", PUBLISHER_A),
        1.0,
        "the declared channel's instance was displaced"
    );
    assert!(
        !has_sample(
            &rendered,
            "dz_recorder_sequence_current",
            &[
                ("feed", FEED),
                ("port_role", "mktdata"),
                ("channel", "200"),
                ("source", "192.0.2.10")
            ]
        ),
        "a channel the feed never declared kept its series past eviction"
    );
}

#[test]
fn a_feed_that_declares_no_channels_treats_every_channel_of_a_declared_source_as_declared() {
    // "Unstated" is not "none". Most feeds state no channel ids, and reading an
    // empty declaration as an empty set would make every publisher a stranger
    // on its own recorder — the failure this gating exists to prevent, arriving
    // by the other door.
    let limits = InstanceLimits {
        max_instances: 1,
        min_evict_age: Duration::from_secs(0),
        ..InstanceLimits::default()
    };
    let (metrics, mut observer) = common::observer_with_undeclared_channels(&[PUBLISHER_A], limits);

    let mut datagram = Datagram::seq(1);
    datagram.channel_id = 200;
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::from(PUBLISHER_A).at(T0).recorded(&payload));

    let stranger = Datagram::seq(1);
    let payload = stranger.payload();
    observer.on_datagram(
        &Arrival::from(STRANGER)
            .at(T0 + SECOND_NS)
            .recorded(&payload),
    );

    assert!(
        has_sample(
            &metrics.render(),
            "dz_recorder_sequence_current",
            &[
                ("feed", FEED),
                ("port_role", "mktdata"),
                ("channel", "200"),
                ("source", "192.0.2.10")
            ]
        ),
        "a declared publisher was evicted for a stranger because it named no channels"
    );
}
