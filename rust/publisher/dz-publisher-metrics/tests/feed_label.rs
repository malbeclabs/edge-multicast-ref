//! A shard's two feeds may share a `Channel ID`, and each is a channel
//! instance of its own. The three families keyed on one carry `feed`, so the
//! two instances write two series and not one.
//!
//! Configured with the pairs directly: no document the runtime accepts states
//! a shared ID yet, and none has to for this crate's half of it.

use std::collections::{BTreeMap, BTreeSet};

use dz_edge_core::PortRole;
use dz_publisher_metrics::{PublisherMetrics, PublisherMetricsConfig};

const TOP_OF_BOOK: &str = "top-of-book";
const MARKET_BY_PRICE: &str = "market-by-price";

fn metrics(channels: &[(&str, u8)]) -> PublisherMetrics {
    PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata, PortRole::Refdata],
        connections: &[],
        channels,
        ingress_message_types: &[],
    })
}

/// One sample: its family, its labels, its value.
struct Sample {
    family: String,
    labels: BTreeMap<String, String>,
    value: f64,
}

/// Every sample line of an exposition, parsed. Label values here hold no
/// comma, quote or brace, so a split is a parse.
fn samples(rendered: &str) -> Vec<Sample> {
    rendered
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .filter_map(|line| {
            let (series, value) = line.rsplit_once(' ')?;
            let (family, labels) = match series.split_once('{') {
                Some((family, rest)) => (family, rest.strip_suffix('}')?),
                None => (series, ""),
            };
            let labels = labels
                .split(',')
                .filter(|pair| !pair.is_empty())
                .filter_map(|pair| {
                    let (k, v) = pair.split_once('=')?;
                    Some((k.to_owned(), v.trim_matches('"').to_owned()))
                })
                .collect();
            Some(Sample {
                family: family.to_owned(),
                labels,
                value: value.parse().ok()?,
            })
        })
        .collect()
}

fn of<'a>(samples: &'a [Sample], family: &str) -> Vec<&'a Sample> {
    samples.iter().filter(|s| s.family == family).collect()
}

/// The sequence gauge of `feed` on mktdata, Channel ID 1.
fn sequence_on_one(rendered: &str, feed: &str) -> f64 {
    let all = samples(rendered);
    let found: Vec<&Sample> = of(&all, "dz_publisher_egress_sequence_current")
        .into_iter()
        .filter(|s| {
            s.labels["port_role"] == "mktdata"
                && s.labels["channel_id"] == "1"
                && s.labels["feed"] == feed
        })
        .collect();
    assert_eq!(found.len(), 1, "one series for {feed}:\n{rendered}");
    found[0].value
}

/// Two feeds on Channel ID 1 are two sequence series, and a write to one
/// leaves the other at its value.
///
/// **The revert:** take `feed` off `sequence_current`'s label names, and the
/// two pairs fold onto one series.
#[test]
fn two_feeds_on_one_channel_id_are_two_sequence_series() {
    let metrics = metrics(&[(TOP_OF_BOOK, 1), (MARKET_BY_PRICE, 1)]);
    let rendered = metrics.render();
    let all = samples(&rendered);
    let feeds: BTreeSet<&str> = of(&all, "dz_publisher_egress_sequence_current")
        .into_iter()
        .filter(|s| s.labels["port_role"] == "mktdata" && s.labels["channel_id"] == "1")
        .map(|s| s.labels["feed"].as_str())
        .collect();
    assert_eq!(feeds, BTreeSet::from([TOP_OF_BOOK, MARKET_BY_PRICE]));

    metrics
        .egress()
        .set_sequence(PortRole::Mktdata, TOP_OF_BOOK, 1, 41);
    let rendered = metrics.render();
    assert_eq!(sequence_on_one(&rendered, TOP_OF_BOOK), 41.0);
    assert_eq!(sequence_on_one(&rendered, MARKET_BY_PRICE), 0.0);
}

/// The heartbeat and last-published gauges are per pair as well, and the
/// reference-data gauges once per distinct ID.
#[test]
fn the_heartbeat_and_last_published_gauges_are_per_pair_and_the_manifest_per_id() {
    let rendered = metrics(&[(TOP_OF_BOOK, 1), (MARKET_BY_PRICE, 1)]).render();
    let all = samples(&rendered);
    for family in [
        "dz_publisher_egress_heartbeat_last_sent_timestamp_seconds",
        "dz_publisher_channel_last_published_timestamp_seconds",
    ] {
        let feeds: BTreeSet<&str> = of(&all, family)
            .into_iter()
            .map(|s| s.labels["feed"].as_str())
            .collect();
        assert_eq!(
            feeds,
            BTreeSet::from([TOP_OF_BOOK, MARKET_BY_PRICE]),
            "{family}:\n{rendered}"
        );
    }
    for family in [
        "dz_publisher_refdata_manifest_seq",
        "dz_publisher_refdata_manifest_valid",
        "dz_publisher_refdata_instruments_current",
    ] {
        let series = of(&all, family);
        assert_eq!(
            series.len(),
            1,
            "{family} once for Channel ID 1:\n{rendered}"
        );
        assert!(!series[0].labels.contains_key("feed"), "{family}");
    }
}

// ------------------------------------------------- the README's two rules

/// The README's PromQL blocks, in order.
fn readme_rules() -> Vec<&'static str> {
    let readme = include_str!("../README.md");
    readme
        .split("```promql")
        .skip(1)
        .filter_map(|rest| rest.split_once("```").map(|(block, _)| block))
        .collect()
}

/// The labels inside the first `<keyword>(...)` of `rule`.
fn label_list<'a>(rule: &'a str, keyword: &str) -> BTreeSet<&'a str> {
    let start = rule
        .find(&format!("{keyword}("))
        .unwrap_or_else(|| panic!("no {keyword}(...) in {rule}"))
        + keyword.len()
        + 1;
    let end = start + rule[start..].find(')').expect("closed");
    rule[start..end].split(',').map(str::trim).collect()
}

/// A sample's labels without `ignored`: the key a vector match compares.
fn key(sample: &Sample, ignored: &BTreeSet<&str>) -> BTreeMap<String, String> {
    sample
        .labels
        .iter()
        .filter(|(k, _)| !ignored.contains(k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Each README rule, evaluated for its join against an exposition that carries
/// `feed`, returns a series.
///
/// Only the matching is evaluated. The thresholds filter values, and a join
/// that matches nothing returns nothing whatever the values are, which is the
/// failure the `feed` label could introduce. The labels are read off the
/// README, so the test fails if the README's rule does.
#[test]
fn the_readme_rules_match_with_the_feed_label() {
    let channels = [(TOP_OF_BOOK, 1), (MARKET_BY_PRICE, 1), (TOP_OF_BOOK, 2)];
    let metrics = metrics(&channels);
    metrics
        .channel()
        .set_last_published(TOP_OF_BOOK, 1, 1_700_000_000.0);
    metrics
        .channel()
        .set_last_published(MARKET_BY_PRICE, 1, 1_700_000_100.0);
    let rendered = metrics.render();
    let all = samples(&rendered);
    let last_published = of(
        &all,
        "dz_publisher_channel_last_published_timestamp_seconds",
    );

    let rules = readme_rules();
    let staleness = rules
        .iter()
        .find(|rule| rule.contains("group_right"))
        .expect("the relative-staleness rule");
    let never = rules
        .iter()
        .find(|rule| rule.contains("dz_publisher_uptime_seconds"))
        .expect("the never-published rule");

    // Relative staleness: `max without(...)` groups the non-zero series, and
    // each non-zero series must find its group under `ignoring(...)`. The
    // groups are per feed, so a Top-of-Book channel is measured against
    // Top-of-Book's freshest.
    let without = label_list(staleness, "without");
    let ignoring = label_list(staleness, "ignoring");
    let live: Vec<&&Sample> = last_published.iter().filter(|s| s.value > 0.0).collect();
    let groups: BTreeSet<BTreeMap<String, String>> =
        live.iter().map(|s| key(s, &without)).collect();
    assert_eq!(groups.len(), 2, "one group per feed: {groups:?}");
    for sample in &live {
        assert!(
            groups.contains(&key(sample, &ignoring)),
            "{:?} matches no group under ignoring{ignoring:?}",
            sample.labels
        );
    }

    // Never published: each zero series must find the uptime gauge, which
    // carries no `feed`, under `ignoring(...)`.
    let ignoring = label_list(never, "ignoring");
    let uptime: Vec<BTreeMap<String, String>> = of(&all, "dz_publisher_uptime_seconds")
        .into_iter()
        .map(|s| key(s, &ignoring))
        .collect();
    assert!(!uptime.is_empty(), "the uptime gauge renders:\n{rendered}");
    let zero: Vec<&&Sample> = last_published.iter().filter(|s| s.value == 0.0).collect();
    assert_eq!(zero.len(), 1, "Channel ID 2 has published nothing");
    for sample in zero {
        assert!(
            uptime.contains(&key(sample, &ignoring)),
            "{:?} matches no uptime series under ignoring{ignoring:?}",
            sample.labels
        );
    }
}
