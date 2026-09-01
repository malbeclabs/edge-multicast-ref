//! Every series must render at 0 the instant the metric set is built.
//!
//! A metric that first appears after the event it counts is a metric no
//! dashboard can chart, and a panel that is blank because nothing has happened
//! yet is indistinguishable from one that is blank because the recorder is dead.
//! An alert on `== 0` cannot fire on a series that does not exist at all.

mod common;

use common::{
    has_sample, metrics_with_sources, observer_with_sources, Arrival, Datagram, NORMATIVE_NAMES,
    PUBLISHER_A, PUBLISHER_B, STRANGER,
};
use dz_recorder_core::Observer;

/// The declared sources are what make the channel-instance families' `source`
/// label knowable before any traffic. With them declared, *every* family in the
/// normative set exists and reads zero at construction.
#[test]
fn every_family_renders_zero_before_the_first_datagram() {
    let metrics = metrics_with_sources(&[PUBLISHER_A, PUBLISHER_B]);
    let rendered = metrics.render();

    for name in NORMATIVE_NAMES {
        assert!(
            rendered.contains(&format!("# TYPE {name} ")),
            "{name} does not exist before the first datagram, so no dashboard \
             can chart it and no `== 0` alert on it can fire:\n{rendered}"
        );
    }

    for line in rendered
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
    {
        let value = line
            .rsplit(' ')
            .next()
            .expect("a sample line ends in a value");
        assert_eq!(
            value, "0",
            "nothing has been recorded, so every sample must read 0: {line}"
        );
    }
}

/// The deliberate opposite, and the rule that makes the pre-creation above
/// bounded: a source the operator did not declare gets no series until its first
/// datagram, and then opens one in silence.
#[test]
fn an_undeclared_source_has_no_series_until_it_sends() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);

    let rendered = metrics.render();
    assert!(
        has_sample(
            &rendered,
            "dz_recorder_sequence_current",
            &[("source", &PUBLISHER_A.to_string())]
        ),
        "a declared source's series exists from startup"
    );
    assert!(
        !has_sample(
            &rendered,
            "dz_recorder_sequence_current",
            &[("source", &STRANGER.to_string())]
        ),
        "an any-source join's key space is not ours to pre-create from"
    );

    let datagram = Datagram::seq(4_000);
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::from(STRANGER).recorded(&payload));

    let rendered = metrics.render();
    assert!(has_sample(
        &rendered,
        "dz_recorder_sequence_current",
        &[("source", &STRANGER.to_string())]
    ));
}

/// The constant labels are applied once, in one place, so there is no path to a
/// `dz_recorder_*` series that cannot say which capture point produced it.
#[test]
fn every_series_carries_the_site_and_recorder_labels() {
    let metrics = metrics_with_sources(&[PUBLISHER_A]);
    let rendered = metrics.render();

    for line in rendered
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
    {
        assert!(
            line.contains(&format!("site=\"{}\"", common::SITE))
                && line.contains(&format!("recorder=\"{}\"", common::RECORDER)),
            "a series with no capture point: {line}"
        );
    }
}
