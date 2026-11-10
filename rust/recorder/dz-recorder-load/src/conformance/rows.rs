//! Placed verdicts become `conformance_finding` rows: one per rule, per
//! instance, per object.
//!
//! The table's grain is `(rule_id, channel instance, window)`, and its
//! `ReplacingMergeTree` keeps one row per key — so the several entries a rule
//! set states for one rule on one instance (a pass, a violation split into
//! ranges, an unverifiable stretch) are reduced to one here. Left to the engine,
//! whichever happened to be written last would survive, and that is an
//! accident rather than a verdict.

use std::collections::BTreeMap;

use dz_recorder_archive::SegmentManifest;
use dz_recorder_core::ChannelInstance;
use dz_recorder_rows::{ConformanceFinding, Derivation, FindingVerdict, Nanos, PortRoleLabel};

use super::findings::{with_note, Placed, Subject};

/// How bad a verdict is, for the reduction.
///
/// Worst wins: a violation anywhere on the instance in this window is the
/// window's verdict, then a stretch nothing could be decided over, then a pass,
/// and `na` only when nothing else was stated. A window that holds a violation
/// and a pass is not a window that passed.
const fn severity(verdict: FindingVerdict) -> u8 {
    match verdict {
        FindingVerdict::Violation => 3,
        FindingVerdict::Unverifiable => 2,
        FindingVerdict::Pass => 1,
        FindingVerdict::Na => 0,
    }
}

/// What every row from one run over one object shares.
#[derive(Debug, Clone, Copy)]
pub struct RunStamp<'a> {
    pub manifest: &'a SegmentManifest,
    /// The version the rule set answered with, never a configured one.
    pub rule_set_version: &'a str,
    /// When the rule set ran.
    pub run_ts: u64,
}

/// One row per `(rule, instance)` in `placed`.
///
/// The row's window is the object's — `window_start` and `window_end` from the
/// manifest, never from what a rule happened to evaluate, so two rules over one
/// object agree about which window they are talking about. `first_seq` and
/// `last_seq` are **the evidence range of the winning verdict**, spanning every
/// range that verdict was stated over in one era; where the winning entries cite
/// no range, the instance's own coverage in the manifest stands in, so a row is
/// always placeable in the sequence space.
///
/// Identity — `site`, `recorder`, `env`, `feed` — is the manifest's: a runner
/// re-judging another recorder's object must not sign it as its own.
#[must_use]
pub fn finding_rows(stamp: &RunStamp<'_>, placed: &[Placed<'_>]) -> Vec<ConformanceFinding> {
    let mut by_key: BTreeMap<(&str, ChannelInstance), Vec<&Placed<'_>>> = BTreeMap::new();
    for p in placed {
        by_key
            .entry((p.entry.rule_id.as_str(), p.instance))
            .or_default()
            .push(p);
    }
    by_key
        .into_values()
        .map(|group| row(stamp, &group))
        .collect()
}

fn row(stamp: &RunStamp<'_>, group: &[&Placed<'_>]) -> ConformanceFinding {
    let worst = group
        .iter()
        .map(|p| severity(p.verdict))
        .max()
        .expect("a group is never empty");
    let winners: Vec<&Placed<'_>> = group
        .iter()
        .copied()
        .filter(|p| severity(p.verdict) == worst)
        .collect();
    let first = winners[0];

    // The span of the winning verdict's evidence, in the era of the first
    // range that cites one. A span across a reset would compare two rulers.
    let era = winners
        .iter()
        .find_map(|p| p.entry.evidence.map(|e| e.reset_count));
    let ranges: Vec<(u64, u64)> = winners
        .iter()
        .filter_map(|p| p.entry.evidence)
        .filter(|e| Some(e.reset_count) == era)
        .map(|e| (e.first_seq, e.last_seq))
        .collect();
    let (first_seq, last_seq) = if first.subject == Subject::Channel {
        // A channel row spans every path's sequence space, and no one range
        // describes it. 0/0, which no instance row ever states with a window
        // that held datagrams, rather than one path's numbers presented as the
        // channel's.
        (0, 0)
    } else if let (true, Some(coverage)) = (ranges.is_empty(), first.coverage) {
        (coverage.first_seq, coverage.last_seq)
    } else {
        (
            ranges.iter().map(|r| r.0).min().unwrap_or(0),
            ranges.iter().map(|r| r.1).max().unwrap_or(0),
        )
    };

    let detail = if winners.len() > 1 {
        with_note(
            &first.detail,
            &format!(
                "{} stated over {} entries; first_seq and last_seq span the {} in era {}",
                verdict_name(first.verdict),
                winners.len(),
                if ranges.len() == 1 { "range" } else { "ranges" },
                era.map_or_else(|| "none".to_owned(), |e| e.to_string()),
            ),
        )
    } else {
        first.detail.clone()
    };

    let m = stamp.manifest;
    ConformanceFinding {
        run_ts: Nanos(stamp.run_ts),
        rule_id: first.entry.rule_id.clone(),
        rule_set_version: stamp.rule_set_version.to_owned(),
        site: m.site.clone(),
        recorder: m.recorder.clone(),
        env: m.env.clone(),
        feed: m.feed.clone(),
        port_role: PortRoleLabel::from(first.role),
        source_addr: first.instance.source,
        channel_id: first.instance.channel_id,
        dst_port: first.instance.dst_port,
        window_start: Nanos(m.start_ns),
        window_end: Nanos(m.end_ns),
        verdict: first.verdict,
        detail,
        object_key: m.object_key.clone(),
        // Judged from bytes kept and checked against their manifest.
        derivation: Derivation::Archive,
        first_seq,
        last_seq,
    }
}

const fn verdict_name(verdict: FindingVerdict) -> &'static str {
    match verdict {
        FindingVerdict::Pass => "pass",
        FindingVerdict::Violation => "violation",
        FindingVerdict::Unverifiable => "unverifiable",
        FindingVerdict::Na => "na",
    }
}

#[cfg(test)]
mod tests {
    use dz_edge_core::PortRole;
    use dz_recorder_conformance::{EvidenceRange, Outcome, RuleOutcome};

    use super::*;
    use crate::conformance::findings::tests::{
        coverage, entry, joined, manifest, report, GROUP, MKTDATA, SNAPSHOT,
    };
    use crate::conformance::findings::{place, Holes};

    fn ranged(rule: &str, outcome: Outcome, port: u16, first: u64, last: u64) -> RuleOutcome {
        let mut e = entry(rule, outcome, Some(port));
        e.evidence = Some(EvidenceRange {
            reset_count: 0,
            first_seq: first,
            last_seq: last,
        });
        e
    }

    fn rows_for(rules: Vec<RuleOutcome>) -> Vec<ConformanceFinding> {
        let m = manifest(
            vec![
                joined(PortRole::Mktdata, GROUP, MKTDATA),
                joined(PortRole::Snapshot, GROUP, SNAPSHOT),
            ],
            &[(MKTDATA, coverage(0, 99)), (SNAPSHOT, coverage(0, 9))],
        );
        let r = report(rules);
        let (placed, _) = place(&m, GROUP, &r, &Holes::default());
        finding_rows(
            &RunStamp {
                manifest: &m,
                rule_set_version: "v1+abc",
                run_ts: 42,
            },
            &placed,
        )
    }

    /// Several entries for one rule on one instance are one row, and the worst
    /// verdict is that row's.
    #[test]
    fn one_row_per_rule_and_instance_and_the_worst_verdict_wins() {
        let rows = rows_for(vec![
            ranged("TEST.A", Outcome::Pass, MKTDATA, 0, 99),
            ranged("TEST.A", Outcome::Violation, MKTDATA, 10, 12),
            ranged("TEST.A", Outcome::Violation, MKTDATA, 40, 41),
            ranged("TEST.A", Outcome::Unverifiable, MKTDATA, 50, 60),
            ranged("TEST.A", Outcome::Pass, SNAPSHOT, 0, 9),
        ]);
        assert_eq!(rows.len(), 2, "{rows:#?}");
        let mktdata = rows.iter().find(|r| r.dst_port == MKTDATA).expect("a row");
        assert_eq!(mktdata.verdict, FindingVerdict::Violation);
        assert_eq!(
            (mktdata.first_seq, mktdata.last_seq),
            (10, 41),
            "the violation's span"
        );
        assert!(
            mktdata.detail.contains("violation stated over 2 entries"),
            "{}",
            mktdata.detail
        );
        let snapshot = rows.iter().find(|r| r.dst_port == SNAPSHOT).expect("a row");
        assert_eq!(snapshot.verdict, FindingVerdict::Pass);
        assert_eq!(snapshot.port_role, PortRoleLabel::Snapshot);

        for (outcomes, expected) in [
            (
                vec![Outcome::Pass, Outcome::Unverifiable],
                FindingVerdict::Unverifiable,
            ),
            (vec![Outcome::Na, Outcome::Pass], FindingVerdict::Pass),
            (vec![Outcome::Na], FindingVerdict::Na),
        ] {
            let rows = rows_for(
                outcomes
                    .into_iter()
                    .map(|o| ranged("TEST.B", o, MKTDATA, 1, 2))
                    .collect(),
            );
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].verdict, expected);
        }
    }

    /// The window is the object's, the identity is the manifest's, the version
    /// is the one the rule set answered with.
    #[test]
    fn a_row_states_the_objects_window_and_the_manifests_identity() {
        let rows = rows_for(vec![ranged("TEST.A", Outcome::Pass, MKTDATA, 3, 4)]);
        let r = &rows[0];
        assert_eq!((r.window_start, r.window_end), (Nanos(1_000), Nanos(9_000)));
        assert_eq!(
            (
                r.site.as_str(),
                r.recorder.as_str(),
                r.env.as_str(),
                r.feed.as_str()
            ),
            ("site-1", "recorder-1", "test", "market-by-price")
        );
        assert_eq!(r.rule_set_version, "v1+abc");
        assert_eq!(r.run_ts, Nanos(42));
        assert_eq!(r.object_key, "feed=market-by-price/obj-7");
        assert_eq!(r.derivation, Derivation::Archive);
        assert_eq!((r.first_seq, r.last_seq), (3, 4));
    }

    /// A channel row states 0/0: it spans every path's sequence space, and no
    /// one range describes it.
    #[test]
    fn a_channel_row_states_no_sequence_range() {
        let mut e = entry("TEST.CHANNEL_WIDE", Outcome::Violation, None);
        e.channel_id = Some(1);
        e.port_role = Some(dz_recorder_conformance::NeededRole::Mktdata);
        let rows = rows_for(vec![
            e,
            ranged("TEST.CHANNEL_WIDE", Outcome::Pass, MKTDATA, 3, 4),
        ]);
        assert_eq!(
            rows.len(),
            2,
            "a channel row and an instance row: {rows:#?}"
        );
        let channel = rows
            .iter()
            .find(|r| r.source_addr == std::net::Ipv4Addr::UNSPECIFIED)
            .expect("the channel row");
        assert_eq!(channel.verdict, FindingVerdict::Violation);
        assert_eq!((channel.first_seq, channel.last_seq), (0, 0));
        assert_eq!(channel.dst_port, MKTDATA);
    }

    /// Where the winning verdict cites no range, the instance's coverage
    /// stands in, so the row is still placeable.
    #[test]
    fn a_verdict_with_no_range_takes_the_instances_coverage() {
        let mut e = entry("TEST.A", Outcome::Pass, Some(MKTDATA));
        e.evidence = None;
        let rows = rows_for(vec![e]);
        assert_eq!((rows[0].first_seq, rows[0].last_seq), (0, 99));
    }
}
