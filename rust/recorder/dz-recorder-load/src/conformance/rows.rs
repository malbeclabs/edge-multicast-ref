//! Placed verdicts become `conformance_finding` rows: one per rule, per
//! instance, per object; and those rows, counted, become the object's
//! `conformance_judged` rows: one per instance, whatever verdicts are kept.
//!
//! The table's grain is `(rule_id, channel instance, window)`, and its
//! `ReplacingMergeTree` keeps one row per key — so the several entries a rule
//! set states for one rule on one instance (a pass, a violation split into
//! ranges, an unverifiable stretch) are reduced to one here. Left to the engine,
//! whichever happened to be written last would survive, and that is an
//! accident rather than a verdict.

use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use dz_recorder_archive::SegmentManifest;
use dz_recorder_core::ChannelInstance;
use dz_recorder_rows::{
    ConformanceFinding, ConformanceJudged, Derivation, FindingVerdict, Nanos, PortRoleLabel,
};

use super::findings::{joined_group, stated, with_note, Placed, Subject};

/// How bad a verdict is, for the reduction.
///
/// Worst wins: a violation anywhere on the instance in this window is the
/// window's verdict, then a stretch nothing could be decided over, then a pass,
/// and `na` only when nothing else was stated. A window that holds a violation
/// and a pass is not a window that passed.
///
/// Not the rule set's severity, which says how bad a rule's violation is and
/// is carried on the row; this ranks the verdicts of one rule against each
/// other.
const fn rank(verdict: FindingVerdict) -> u8 {
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
        .map(|p| rank(p.verdict))
        .max()
        .expect("a group is never empty");
    let winners: Vec<&Placed<'_>> = group
        .iter()
        .copied()
        .filter(|p| rank(p.verdict) == worst)
        .collect();
    // The entry the row speaks for: among the winners, the one the rule set
    // stated most severely. A pass and a violation a hole both downgraded tie
    // as unverifiable, and the row's detail is then the violation's message
    // with its note, not the pass's.
    let first = winners
        .iter()
        .copied()
        .rev()
        .max_by_key(|p| rank(stated(p.entry.outcome)))
        .expect("a group is never empty");

    // The span of the winning verdict's evidence, in the era of the entry the
    // row speaks for (or, where it cites none, of the first winner that does) — its reset count, and its era ordinal where the
    // report states one. A span across a reset would compare two rulers.
    let era = first
        .entry
        .evidence
        .into_iter()
        .chain(winners.iter().filter_map(|p| p.entry.evidence))
        .map(|e| (e.reset_count, e.era_index))
        .next();
    let ranges: Vec<(u64, u64)> = winners
        .iter()
        .filter_map(|p| p.entry.evidence)
        .filter(|e| Some((e.reset_count, e.era_index)) == era)
        .map(|e| (e.first_seq, e.last_seq))
        .collect();
    let (first_seq, last_seq) = if first.subject == Subject::Channel {
        // A channel row spans every path's sequence space, and no one range
        // describes it. 0/0, which no instance row ever states with a window
        // that held datagrams, rather than one path's numbers presented as the
        // channel's.
        (0, 0)
    } else if ranges.is_empty() {
        first.coverage.map_or((0, 0), coverage_span)
    } else {
        (
            ranges.iter().map(|r| r.0).min().unwrap_or(0),
            ranges.iter().map(|r| r.1).max().unwrap_or(0),
        )
    };

    let detail = if winners.len() > 1 {
        let reduced = format!(
            "{} stated over {} entries",
            verdict_name(first.verdict),
            winners.len()
        );
        // The span is named only when it is one: a fallback to the instance's
        // coverage cites no range, and saying it spans one would be untrue.
        let note = match (ranges.len(), era) {
            (0, _) | (_, None) => reduced,
            (n, Some((reset_count, _))) => format!(
                "{reduced}; first_seq and last_seq span the {} at reset count {reset_count}",
                if n == 1 { "range" } else { "ranges" },
            ),
        };
        with_note(&first.detail, &note)
    } else {
        first.detail.clone()
    };

    let m = stamp.manifest;
    ConformanceFinding {
        run_ts: Nanos(stamp.run_ts),
        rule_id: first.entry.rule_id.clone(),
        // The entry the row speaks for, so a violation's row states the
        // violation's severity. Lower-cased as the report spells it, and
        // empty where it states none: unknown, which is not `info`.
        severity: first
            .entry
            .severity
            .as_deref()
            .map(str::to_ascii_lowercase)
            .unwrap_or_default(),
        rule_set_version: stamp.rule_set_version.to_owned(),
        site: m.site.clone(),
        recorder: m.recorder.clone(),
        env: m.env.clone(),
        feed: m.feed.clone(),
        group_addr: joined_group(m, first.role, first.instance.dst_port),
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

/// One `conformance_judged` row per instance the finding rows of a run are
/// placed on, and one at zero for a run that placed nothing.
///
/// `findings` are the reduced rows **before** the kept verdicts are chosen:
/// the counts are of what keeping every verdict would have written, so they
/// are the same under `violations` as under `all`, and a clean object judged
/// under the default still says it was judged. `refused` is the run's count of
/// verdicts it declined to place, written on the first row: those belong to no
/// instance, and spread over several rows a sum would count them once per row.
///
/// Keyed on the group and the instance, which with the run's identity and the
/// object and its window is the table's whole sort key, so no two rows of one run
/// collapse into one another under `ReplacingMergeTree`.
#[must_use]
pub fn judged_rows(
    stamp: &RunStamp<'_>,
    findings: &[ConformanceFinding],
    refused: u64,
) -> Vec<ConformanceJudged> {
    let m = stamp.manifest;
    let zero = |group_addr, instance: ChannelInstance| ConformanceJudged {
        run_ts: Nanos(stamp.run_ts),
        rule_set_version: stamp.rule_set_version.to_owned(),
        site: m.site.clone(),
        recorder: m.recorder.clone(),
        env: m.env.clone(),
        feed: m.feed.clone(),
        group_addr,
        source_addr: instance.source,
        channel_id: instance.channel_id,
        dst_port: instance.dst_port,
        window_start: Nanos(m.start_ns),
        window_end: Nanos(m.end_ns),
        object_key: m.object_key.clone(),
        derivation: Derivation::Archive,
        pass: 0,
        violation: 0,
        unverifiable: 0,
        na: 0,
        violation_must: 0,
        violation_should: 0,
        violation_info: 0,
        refused: 0,
    };

    let mut by_key: BTreeMap<(Ipv4Addr, ChannelInstance), ConformanceJudged> = BTreeMap::new();
    for f in findings {
        let instance = ChannelInstance::new(f.source_addr, f.channel_id, f.dst_port);
        let row = by_key
            .entry((f.group_addr, instance))
            .or_insert_with(|| zero(f.group_addr, instance));
        let count = match f.verdict {
            FindingVerdict::Pass => &mut row.pass,
            FindingVerdict::Violation => &mut row.violation,
            FindingVerdict::Unverifiable => &mut row.unverifiable,
            FindingVerdict::Na => &mut row.na,
        };
        *count = count.saturating_add(1);
        if f.verdict == FindingVerdict::Violation {
            // A severity this runner does not know is counted in `violation`
            // and in none of these, so a reader summing the three and finding
            // less than the total knows the split is incomplete.
            let split = match f.severity.as_str() {
                "must" => Some(&mut row.violation_must),
                "should" => Some(&mut row.violation_should),
                "info" => Some(&mut row.violation_info),
                _ => None,
            };
            if let Some(split) = split {
                *split = split.saturating_add(1);
            }
        }
    }

    let mut rows: Vec<ConformanceJudged> = by_key.into_values().collect();
    if rows.is_empty() {
        // Judged, and nothing placed: without a row the object would read
        // exactly like one nobody judged.
        rows.push(zero(
            Ipv4Addr::UNSPECIFIED,
            ChannelInstance::new(Ipv4Addr::UNSPECIFIED, 0, 0),
        ));
    }
    rows[0].refused = u32::try_from(refused).unwrap_or(u32::MAX);
    rows
}

/// The instance's coverage, as a span the row may state.
///
/// The manifest's `first_seq` and `last_seq` are the first and last numbers to
/// arrive, across every era in the object. Where the instance reset inside the
/// object they are two rulers — 900 before the reset and 50 after it — and no
/// span between them means anything, so the row states 0/0 rather than one.
fn coverage_span(coverage: &dz_recorder_archive::InstanceCoverage) -> (u64, u64) {
    if coverage.reset_counts_seen.len() > 1 {
        (0, 0)
    } else {
        (coverage.first_seq, coverage.last_seq)
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
        carried, coverage, entry, joined, manifest, report, GROUP, MKTDATA, SNAPSHOT, SOURCE_IP,
    };
    use crate::conformance::findings::{place, Holes, CHANNEL_SOURCE};

    fn ranged(rule: &str, outcome: Outcome, port: u16, first: u64, last: u64) -> RuleOutcome {
        let mut e = entry(rule, outcome, Some(port));
        e.evidence = Some(EvidenceRange {
            reset_count: 0,
            era_index: None,
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
        let (placed, _) = place(&m, GROUP, &r, &Holes::default(), &carried(&m));
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

    /// An instance that sent 900 to 1000, reset, and sent 1 to 50 has a
    /// coverage of 900 and 50 in arrival order: two rulers. A row that cites
    /// no range states 0/0 for it, not a span from one era into the next.
    #[test]
    fn a_coverage_that_spans_a_reset_is_not_stated_as_a_span() {
        let mut across = coverage(900, 1_000);
        across.last_seq = 50;
        across.count = 151;
        across.reset_counts_seen = vec![0, 1];
        let m = manifest(
            vec![joined(PortRole::Mktdata, GROUP, MKTDATA)],
            &[(MKTDATA, across)],
        );
        let mut e = entry("TEST.A", Outcome::Pass, Some(MKTDATA));
        e.evidence = None;
        let r = report(vec![e.clone(), e]);
        let (placed, _) = place(&m, GROUP, &r, &Holes::default(), &carried(&m));
        let rows = finding_rows(
            &RunStamp {
                manifest: &m,
                rule_set_version: "v1+abc",
                run_ts: 42,
            },
            &placed,
        );
        assert_eq!((rows[0].first_seq, rows[0].last_seq), (0, 0));
        assert!(
            !rows[0].detail.contains("span"),
            "no range was cited, so none is said to be spanned: {}",
            rows[0].detail
        );
        assert!(
            rows[0].detail.contains("pass stated over 2 entries"),
            "{}",
            rows[0].detail
        );
    }

    /// A pass and a violation of one rule on one instance, both over a hole,
    /// are both unverifiable. The row keeps the violation's message, so it
    /// still says what the rule set saw, and the pass's message is not
    /// mistaken for it.
    #[test]
    fn a_downgraded_violation_speaks_for_the_row_over_a_downgraded_pass() {
        use crate::conformance::findings::tests::SOURCE_IP;
        use dz_recorder_loss::SequenceRun;

        let m = manifest(
            vec![joined(PortRole::Mktdata, GROUP, MKTDATA)],
            &[(MKTDATA, coverage(0, 99))],
        );
        let mut holes = Holes::default();
        holes.missing(&SequenceRun {
            instance: ChannelInstance::new(SOURCE_IP, 1, MKTDATA),
            group: GROUP,
            role: PortRole::Mktdata,
            era_ordinal: 1,
            reset_count: 0,
            missing_from: 5,
            missing_to: 5,
            before_ts_ns: 0,
            after_ts_ns: 0,
        });
        let mut pass = ranged("TEST.A", Outcome::Pass, MKTDATA, 4, 6);
        pass.detail = "the pass".to_owned();
        let mut violation = ranged("TEST.A", Outcome::Violation, MKTDATA, 5, 7);
        violation.detail = "the violation".to_owned();
        let r = report(vec![pass, violation]);
        let (placed, _) = place(&m, GROUP, &r, &holes, &carried(&m));
        let rows = finding_rows(
            &RunStamp {
                manifest: &m,
                rule_set_version: "v1+abc",
                run_ts: 42,
            },
            &placed,
        );
        assert_eq!(rows.len(), 1, "{rows:#?}");
        assert_eq!(rows[0].verdict, FindingVerdict::Unverifiable);
        assert!(
            rows[0].detail.starts_with("the violation"),
            "{}",
            rows[0].detail
        );
        assert!(
            rows[0]
                .detail
                .contains("unverifiable stated over 2 entries"),
            "{}",
            rows[0].detail
        );
    }

    /// The span follows the entry the row speaks for. A downgraded pass in one
    /// era and a downgraded violation in the next: the row's detail is the
    /// violation's, so its sequence numbers are the violation's era too.
    #[test]
    fn the_span_is_in_the_era_of_the_entry_the_row_speaks_for() {
        use crate::conformance::findings::tests::SOURCE_IP;
        use dz_recorder_loss::SequenceRun;

        let m = manifest(
            vec![joined(PortRole::Mktdata, GROUP, MKTDATA)],
            &[(MKTDATA, coverage(0, 99))],
        );
        let mut holes = Holes::default();
        for (era_ordinal, reset_count) in [(1, 0), (2, 1)] {
            holes.missing(&SequenceRun {
                instance: ChannelInstance::new(SOURCE_IP, 1, MKTDATA),
                group: GROUP,
                role: PortRole::Mktdata,
                era_ordinal,
                reset_count,
                missing_from: 5,
                missing_to: 5,
                before_ts_ns: 0,
                after_ts_ns: 0,
            });
        }
        let mut pass = ranged("TEST.A", Outcome::Pass, MKTDATA, 4, 6);
        pass.detail = "the pass".to_owned();
        let mut violation = ranged("TEST.A", Outcome::Violation, MKTDATA, 5, 9);
        violation.detail = "the violation".to_owned();
        violation.evidence.as_mut().expect("ranged").reset_count = 1;
        let r = report(vec![pass, violation]);
        let (placed, _) = place(&m, GROUP, &r, &holes, &carried(&m));
        let rows = finding_rows(
            &RunStamp {
                manifest: &m,
                rule_set_version: "v1+abc",
                run_ts: 42,
            },
            &placed,
        );
        assert_eq!(rows.len(), 1, "{rows:#?}");
        assert_eq!(rows[0].verdict, FindingVerdict::Unverifiable);
        assert!(
            rows[0].detail.starts_with("the violation"),
            "{}",
            rows[0].detail
        );
        assert_eq!((rows[0].first_seq, rows[0].last_seq), (5, 9));
        assert!(
            rows[0].detail.contains("the range at reset count 1"),
            "{}",
            rows[0].detail
        );
    }

    fn judged_for(rules: Vec<RuleOutcome>) -> (Vec<ConformanceFinding>, Vec<ConformanceJudged>) {
        let m = manifest(
            vec![
                joined(PortRole::Mktdata, GROUP, MKTDATA),
                joined(PortRole::Snapshot, GROUP, SNAPSHOT),
            ],
            &[(MKTDATA, coverage(0, 99)), (SNAPSHOT, coverage(0, 9))],
        );
        let r = report(rules);
        let (placed, refused) = place(&m, GROUP, &r, &Holes::default(), &carried(&m));
        let stamp = RunStamp {
            manifest: &m,
            rule_set_version: "v1+abc",
            run_ts: 42,
        };
        let findings = finding_rows(&stamp, &placed);
        let judged = judged_rows(&stamp, &findings, refused.total());
        (findings, judged)
    }

    fn severe(mut e: RuleOutcome, severity: Option<&str>) -> RuleOutcome {
        e.severity = severity.map(str::to_owned);
        e
    }

    /// The counts are the reduced finding rows of each instance, channel grain
    /// included, split by verdict and the violations by severity; a severity
    /// nobody knows counts in the total and in no split, and what could not be
    /// placed is counted once, on the first row.
    #[test]
    fn a_judged_row_counts_each_instances_reduced_verdicts() {
        use dz_recorder_conformance::NeededRole;

        let mut channel = severe(
            entry("TEST.CHANNEL", Outcome::Violation, None),
            Some("must"),
        );
        channel.channel_id = Some(1);
        channel.port_role = Some(NeededRole::Mktdata);
        let (findings, judged) = judged_for(vec![
            ranged("TEST.P", Outcome::Pass, MKTDATA, 0, 99),
            severe(
                ranged("TEST.V1", Outcome::Violation, MKTDATA, 1, 1),
                Some("MUST"),
            ),
            severe(
                ranged("TEST.V2", Outcome::Violation, MKTDATA, 2, 2),
                Some("should"),
            ),
            severe(
                ranged("TEST.V3", Outcome::Violation, MKTDATA, 3, 3),
                Some("may"),
            ),
            ranged("TEST.V4", Outcome::Violation, MKTDATA, 4, 4),
            // A pass and a violation of one rule on one instance are one
            // finding row, a violation, and so one count and not two.
            ranged("TEST.BOTH", Outcome::Pass, MKTDATA, 0, 99),
            severe(
                ranged("TEST.BOTH", Outcome::Violation, MKTDATA, 5, 5),
                Some("info"),
            ),
            ranged("TEST.U", Outcome::Unverifiable, MKTDATA, 6, 6),
            ranged("TEST.N", Outcome::Na, MKTDATA, 7, 7),
            ranged("TEST.P", Outcome::Pass, SNAPSHOT, 0, 9),
            severe(
                ranged("TEST.V1", Outcome::Violation, SNAPSHOT, 1, 1),
                Some("info"),
            ),
            channel,
            // Two the runner declines to place: no instance and no channel,
            // and an instance the manifest does not describe.
            entry("TEST.NOWHERE", Outcome::Violation, None),
            ranged("TEST.UNDESCRIBED", Outcome::Violation, 40_009, 1, 1),
        ]);

        let counts: Vec<_> = judged
            .iter()
            .map(|j| {
                (
                    j.source_addr,
                    j.dst_port,
                    [j.pass, j.violation, j.unverifiable, j.na],
                    [j.violation_must, j.violation_should, j.violation_info],
                    j.refused,
                )
            })
            .collect();
        assert_eq!(
            counts,
            vec![
                (CHANNEL_SOURCE, MKTDATA, [0, 1, 0, 0], [1, 0, 0], 2),
                (SOURCE_IP, MKTDATA, [1, 5, 1, 1], [1, 1, 1], 0),
                (SOURCE_IP, SNAPSHOT, [1, 1, 0, 0], [0, 0, 1], 0),
            ],
            "{judged:#?}"
        );

        // Every row is one the run's finding rows are placed on, and its
        // total is the number of them, which is what `verdicts = "all"`
        // would have written for it.
        for j in &judged {
            let on: Vec<_> = findings
                .iter()
                .filter(|f| {
                    (f.source_addr, f.channel_id, f.dst_port)
                        == (j.source_addr, j.channel_id, j.dst_port)
                })
                .collect();
            assert_eq!(
                (j.pass + j.violation + j.unverifiable + j.na) as usize,
                on.len()
            );
            assert!(on.iter().all(|f| f.group_addr == j.group_addr));
            assert_eq!(j.group_addr, GROUP);
            assert_eq!(j.channel_id, 1);
            assert_eq!((j.window_start, j.window_end), (Nanos(1_000), Nanos(9_000)));
            assert_eq!(j.object_key, "feed=market-by-price/obj-7");
            assert_eq!(j.rule_set_version, "v1+abc");
            assert_eq!(j.run_ts, Nanos(42));
            assert_eq!(j.derivation, Derivation::Archive);
        }
        assert_eq!(judged.len(), 3, "one row per instance, and no more");
    }

    /// A judged object that placed nothing still says it was judged: one row
    /// at zero, carrying what it declined to place.
    #[test]
    fn an_object_that_placed_nothing_writes_one_row_at_zero() {
        let (findings, judged) = judged_for(vec![
            entry("TEST.NOWHERE", Outcome::Violation, None),
            entry("TEST.NOWHERE", Outcome::Pass, None),
            ranged("TEST.UNDESCRIBED", Outcome::Pass, 40_009, 1, 1),
        ]);
        assert!(findings.is_empty());
        assert_eq!(judged.len(), 1, "{judged:#?}");
        let j = &judged[0];
        assert_eq!(
            (j.group_addr, j.source_addr, j.channel_id, j.dst_port),
            (Ipv4Addr::UNSPECIFIED, Ipv4Addr::UNSPECIFIED, 0, 0)
        );
        assert_eq!(
            [
                j.pass,
                j.violation,
                j.unverifiable,
                j.na,
                j.violation_must,
                j.violation_should,
                j.violation_info
            ],
            [0; 7]
        );
        assert_eq!(j.refused, 3);
        assert_eq!(j.site, "site-1");

        // And with nothing refused either, still the one row.
        let (_, judged) = judged_for(Vec::new());
        assert_eq!(judged.len(), 1);
        assert_eq!(judged[0].refused, 0);
    }

    /// A finding row states the severity of the entry it speaks for,
    /// lower-cased, and empty where the report stated none; and the group the
    /// manifest joined its role and port in.
    #[test]
    fn a_finding_row_states_its_severity_and_its_group() {
        let rows = rows_for(vec![
            severe(
                ranged("TEST.A", Outcome::Pass, MKTDATA, 0, 99),
                Some("info"),
            ),
            severe(
                ranged("TEST.A", Outcome::Violation, MKTDATA, 3, 4),
                Some("Must"),
            ),
            ranged("TEST.B", Outcome::Violation, SNAPSHOT, 3, 4),
        ]);
        let a = rows.iter().find(|r| r.rule_id == "TEST.A").expect("a row");
        assert_eq!(a.verdict, FindingVerdict::Violation);
        assert_eq!(a.severity, "must", "the violation's, lower-cased");
        assert_eq!(a.group_addr, GROUP);
        let b = rows.iter().find(|r| r.rule_id == "TEST.B").expect("a row");
        assert_eq!(b.severity, "", "unknown, and never `info`");
        assert_eq!(b.group_addr, GROUP);
    }

    /// The group is the manifest's one answer for the role and port, and
    /// `0.0.0.0` where it has none or two: a reader is never handed a guess.
    #[test]
    fn the_group_is_the_one_the_manifest_joined_or_none() {
        use crate::conformance::findings::tests::OTHER_GROUP;

        let one = manifest(vec![joined(PortRole::Mktdata, GROUP, MKTDATA)], &[]);
        assert_eq!(joined_group(&one, PortRole::Mktdata, MKTDATA), GROUP);
        assert_eq!(
            joined_group(&one, PortRole::Snapshot, MKTDATA),
            Ipv4Addr::UNSPECIFIED,
            "not joined as that role"
        );
        assert_eq!(
            joined_group(&one, PortRole::Mktdata, SNAPSHOT),
            Ipv4Addr::UNSPECIFIED,
            "not joined on that port"
        );
        let two = manifest(
            vec![
                joined(PortRole::Mktdata, GROUP, MKTDATA),
                joined(PortRole::Mktdata, OTHER_GROUP, MKTDATA),
            ],
            &[],
        );
        assert_eq!(
            joined_group(&two, PortRole::Mktdata, MKTDATA),
            Ipv4Addr::UNSPECIFIED
        );
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
