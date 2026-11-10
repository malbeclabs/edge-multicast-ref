//! What a stated verdict becomes, over one object: placed on an instance, or
//! refused; `na` when the rule could not have run; and `unverifiable` when its
//! evidence lies where this object has a hole.
//!
//! A pure function of the manifest, the rule set's report and this object's own
//! holes. Nothing here reads configuration — the manifest describes the
//! recorder that observed the bytes, and a configuration describes what
//! somebody believes — and nothing here reads a table: the holes are derived
//! from the same object in the same pass, so a verdict never depends on the
//! order objects were loaded in.

use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use dz_edge_core::{DatagramHeader, PortRole};
use dz_recorder_archive::{InstanceCoverage, SegmentManifest};
use dz_recorder_conformance::{EvidenceRange, Outcome, RuleOutcome, RuleSetReport};
use dz_recorder_core::{CaptureDropScope, ChannelInstance, RecordedDatagram};
use dz_recorder_loss::{LossDeriver, LossReport, SequenceRun};
use dz_recorder_rows::FindingVerdict;

/// Why a stretch of one instance's sequence space is not evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoleCause {
    /// Sequence numbers this object does not hold, as the loss derivation found
    /// them. Whose loss it is — ours, the path's, the publisher's — is
    /// `sequence_gap`'s question and not this one: every reading of an absence
    /// has an explanation that is not the publisher's, and a runner that
    /// accused over one would be deciding attribution it does not have the
    /// admitted drops to decide.
    Missing,
    /// A datagram the capture cut short: its body is shorter than the wire
    /// said. A rule handed that body sees a declared length the bytes do not
    /// fill, which is a violation the recorder caused.
    Truncated,
}

impl HoleCause {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Truncated => "truncated by the capture",
        }
    }
}

/// One stretch of one instance's sequence space, within one era.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hole {
    /// The wire `Reset Count`, which is what an evidence range states its era
    /// by. A range means nothing across a reset.
    pub reset_count: u8,
    pub first: u64,
    pub last: u64,
    pub cause: HoleCause,
}

impl Hole {
    fn note(self) -> String {
        format!(
            "{}:{}-{} {} in this object",
            self.reset_count,
            self.first,
            self.last,
            self.cause.as_str()
        )
    }
}

/// This object's own holes, per instance.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Holes {
    by_instance: BTreeMap<ChannelInstance, Vec<Hole>>,
}

impl Holes {
    /// One run the loss derivation produced.
    pub fn missing(&mut self, run: &SequenceRun) {
        self.push(
            run.instance,
            Hole {
                reset_count: run.reset_count,
                first: run.missing_from,
                last: run.missing_to,
                cause: HoleCause::Missing,
            },
        );
    }

    /// One datagram the capture cut short.
    pub fn truncated(&mut self, instance: ChannelInstance, reset_count: u8, seq: u64) {
        self.push(
            instance,
            Hole {
                reset_count,
                first: seq,
                last: seq,
                cause: HoleCause::Truncated,
            },
        );
    }

    fn push(&mut self, instance: ChannelInstance, hole: Hole) {
        self.by_instance.entry(instance).or_default().push(hole);
    }

    /// The first hole on `instance` that `evidence` touches, in the same era.
    ///
    /// **Touching includes starting on the datagram right after a missing
    /// run.** A rule that reasons about an absence — a sequence that jumped, a
    /// cadence that lapsed — can only cite what it *saw*, and the first thing it
    /// saw after the hole is the datagram that follows it. Its evidence range
    /// therefore begins at `missing_to + 1` and shares no number with the run;
    /// an overlap test that required one would let every gap-type violation
    /// over our own drop through, which is the one this test exists to stop. A
    /// truncated datagram is its own evidence and touches only itself.
    #[must_use]
    pub fn overlapping(
        &self,
        instance: &ChannelInstance,
        evidence: &EvidenceRange,
    ) -> Option<Hole> {
        self.on(instance).find(|h| {
            let reach = match h.cause {
                HoleCause::Missing => h.last.saturating_add(1),
                HoleCause::Truncated => h.last,
            };
            h.reset_count == evidence.reset_count && evidence.overlaps(h.first, reach)
        })
    }

    /// Any hole at all on `instance`.
    #[must_use]
    pub fn first_on(&self, instance: &ChannelInstance) -> Option<Hole> {
        self.on(instance).next()
    }

    fn on(&self, instance: &ChannelInstance) -> impl Iterator<Item = Hole> + '_ {
        self.by_instance
            .get(instance)
            .into_iter()
            .flatten()
            .copied()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_instance.is_empty()
    }
}

/// Finds an object's holes in the walk that replays it.
///
/// The loss derivation is the one the row deriver runs, at the scope the
/// archive states, so a hole here is the same hole `sequence_gap` holds for the
/// same object — computed again rather than read back, because a derivation
/// that read the table it writes beside would depend on load order.
#[derive(Debug)]
pub struct HoleFinder {
    loss: LossDeriver,
    holes: Holes,
}

impl HoleFinder {
    #[must_use]
    pub fn new(scope: CaptureDropScope) -> Self {
        Self {
            loss: LossDeriver::new(scope),
            holes: Holes::default(),
        }
    }

    pub fn observe(&mut self, dg: &RecordedDatagram<'_>) {
        self.loss.observe(dg);
        let held = u32::try_from(dg.payload.len()).unwrap_or(u32::MAX);
        if dg.wire_payload_len > held {
            if let Ok(header) = DatagramHeader::peek(dg.payload) {
                self.holes.truncated(
                    ChannelInstance::new(*dg.src.ip(), header.channel_id, dg.dst.port()),
                    header.reset_count,
                    header.sequence_number,
                );
            }
        }
    }

    #[must_use]
    pub fn finish(self) -> Holes {
        let report: LossReport = self.loss.finish();
        let mut holes = self.holes;
        for run in report.runs() {
            holes.missing(run);
        }
        holes
    }
}

/// A role token as the manifest writes it, read back.
///
/// `None` for anything but the three the specification names: a manifest that
/// claims a role this runner cannot read is not a manifest whose joins it can
/// reason about, and no role is inferred from a misspelling.
#[must_use]
pub fn role_named(token: &str) -> Option<PortRole> {
    match token {
        "mktdata" => Some(PortRole::Mktdata),
        "refdata" => Some(PortRole::Refdata),
        "snapshot" => Some(PortRole::Snapshot),
        _ => None,
    }
}

/// The port `role` was joined on in `group`, as the manifest states it.
#[must_use]
pub fn joined_port(manifest: &SegmentManifest, group: Ipv4Addr, role: PortRole) -> Option<u16> {
    manifest
        .roles_joined
        .iter()
        .find(|j| j.group == group && role_named(&j.role) == Some(role))
        .map(|j| j.port)
}

/// The role the recorder joined `port` as in `group`, if it joined it at all.
#[must_use]
pub fn role_on(manifest: &SegmentManifest, group: Ipv4Addr, port: u16) -> Option<PortRole> {
    manifest
        .roles_joined
        .iter()
        .find(|j| j.group == group && j.port == port)
        .and_then(|j| role_named(&j.role))
}

/// Why a rule that needed `role` did not run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unrun {
    /// `roles_joined` never claimed the role in this group.
    NotJoined(PortRole),
    /// Claimed, on this port, and no instance on that port is in the
    /// manifest's `instances` map: the port carried nothing in this object.
    Silent(PortRole, u16),
}

impl Unrun {
    fn note(self) -> String {
        match self {
            Self::NotJoined(role) => format!(
                "na: the rule reads the {} role, which this recorder did not join",
                role.as_str()
            ),
            Self::Silent(role, port) => format!(
                "na: the rule reads the {} role, joined on port {port}, which carried nothing \
                 in this object",
                role.as_str()
            ),
        }
    }
}

/// The first role `entry` needs that did not carry anything here, if any.
///
/// Two cases with one verdict and two code paths, and only one of them is
/// obvious. A role nobody joined produces no data; a role joined on a port that
/// received nothing produces no data either, and it is the manifest's
/// `instances` map that tells it from a clean one. A port that carried nothing
/// looks exactly like a clean feed to the rule set, whose exit code cannot say
/// it looked at nothing.
#[must_use]
pub fn unrun(manifest: &SegmentManifest, group: Ipv4Addr, entry: &RuleOutcome) -> Option<Unrun> {
    entry.needs.iter().find_map(|needed| {
        let role = PortRole::from(*needed);
        let Some(port) = joined_port(manifest, group, role) else {
            return Some(Unrun::NotJoined(role));
        };
        let carried = manifest.instances.keys().any(|i| i.dst_port == port);
        (!carried).then_some(Unrun::Silent(role, port))
    })
}

/// Entries that could not be written, by cause. Counted, never filed under a
/// guess.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Refused {
    /// The rule set named no instance. A finding on the wrong instance sends a
    /// reader to a sequence space where the evidence is not, so none is chosen.
    pub no_instance: u64,
    /// The rule set named an instance this object's manifest does not describe,
    /// or one on a port the recorder did not join in that group — so there is
    /// no coverage to state its window by and no role to file it under.
    pub undescribed_instance: u64,
}

impl Refused {
    pub fn add(&mut self, other: Self) {
        self.no_instance += other.no_instance;
        self.undescribed_instance += other.undescribed_instance;
    }

    #[must_use]
    pub const fn total(&self) -> u64 {
        self.no_instance + self.undescribed_instance
    }
}

/// One stated verdict, placed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placed<'a> {
    pub entry: &'a RuleOutcome,
    pub instance: ChannelInstance,
    /// The role the recorder joined the instance's port as.
    pub role: PortRole,
    /// The instance's coverage in this object, which states the row's window.
    pub coverage: &'a InstanceCoverage,
    pub verdict: FindingVerdict,
    pub detail: String,
}

/// The verdict a stated outcome is, before anything about this object's holes
/// is consulted.
///
/// `suspected` is a first mismatch the rule set itself has not confirmed and
/// does not fail its own gate on, so it is not a violation here either: it is
/// `unverifiable`, with the rule's own message kept.
const fn stated(outcome: Outcome) -> FindingVerdict {
    match outcome {
        Outcome::Pass => FindingVerdict::Pass,
        Outcome::Violation => FindingVerdict::Violation,
        Outcome::Suspected | Outcome::Unverifiable => FindingVerdict::Unverifiable,
        Outcome::Na => FindingVerdict::Na,
    }
}

/// Every entry of one group's report, placed or refused.
pub fn place<'a>(
    manifest: &'a SegmentManifest,
    group: Ipv4Addr,
    report: &'a RuleSetReport,
    holes: &Holes,
) -> (Vec<Placed<'a>>, Refused) {
    let mut placed = Vec::with_capacity(report.rules.len());
    let mut refused = Refused::default();
    for entry in &report.rules {
        let Some(named) = entry.instance else {
            refused.no_instance += 1;
            continue;
        };
        let instance = ChannelInstance::from(named);
        let (Some(coverage), Some(role)) = (
            manifest.instances.get(&instance),
            role_on(manifest, group, instance.dst_port),
        ) else {
            refused.undescribed_instance += 1;
            continue;
        };
        let (verdict, detail) = match unrun(manifest, group, entry) {
            // Before the stated outcome, whatever it is: a rule that read a
            // role nothing arrived on judged nothing, and a verdict it stated
            // over that nothing is not one this table may carry.
            Some(why) => (FindingVerdict::Na, with_note(&entry.detail, &why.note())),
            None => downgraded(entry, &instance, holes),
        };
        placed.push(Placed {
            entry,
            instance,
            role,
            coverage,
            verdict,
            detail,
        });
    }
    (placed, refused)
}

/// The stated verdict, after this object's own holes.
///
/// **It only ever moves towards `unverifiable`**, never away from it: a hole
/// can take away a runner's grounds for a verdict and can never supply them.
///
/// - A **violation** whose evidence overlaps a hole is `unverifiable`, keeping
///   the rule's own message and the hole in `detail`. This is the failure the
///   whole design is arranged against: a publisher violation manufactured out
///   of the recorder's own drop. A violation that cites no evidence range
///   cannot be tested against the holes at all, which is itself a reason not
///   to write it.
/// - A **pass** whose evidence overlaps a hole is `unverifiable` too. A pass is
///   written only over a window the runner can vouch for, and the report does
///   not say whether a rule reasons about absences — so a pass over a hole is
///   not one this runner can stand behind. A pass that cites no range stands
///   only if the instance has no hole in this object at all.
fn downgraded(
    entry: &RuleOutcome,
    instance: &ChannelInstance,
    holes: &Holes,
) -> (FindingVerdict, String) {
    let verdict = stated(entry.outcome);
    if !matches!(verdict, FindingVerdict::Violation | FindingVerdict::Pass) {
        return (verdict, entry.detail.clone());
    }
    let hole = match &entry.evidence {
        Some(evidence) => holes.overlapping(instance, evidence).map(|hole| {
            format!(
                "unverifiable: the evidence {}:{}-{} overlaps {}",
                evidence.reset_count,
                evidence.first_seq,
                evidence.last_seq,
                hole.note()
            )
        }),
        None if verdict == FindingVerdict::Violation => Some(
            "unverifiable: the rule set cited no evidence range, so the violation cannot be \
             told from this object's own holes"
                .to_owned(),
        ),
        None => holes.first_on(instance).map(|hole| {
            format!(
                "unverifiable: the rule set cited no evidence range and the instance has {}",
                hole.note()
            )
        }),
    };
    match hole {
        Some(note) => (
            FindingVerdict::Unverifiable,
            with_note(&entry.detail, &note),
        ),
        None => (verdict, entry.detail.clone()),
    }
}

/// The rule's own message, with what this runner concluded appended and never
/// in its place.
pub(crate) fn with_note(detail: &str, note: &str) -> String {
    if detail.is_empty() {
        note.to_owned()
    } else {
        format!("{detail} [{note}]")
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;

    use dz_recorder_archive::manifest::JoinedRole;
    use dz_recorder_conformance::{EvidenceRange, NeededRole, ReportInstance};

    use super::*;

    pub const GROUP: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 10);
    pub const OTHER_GROUP: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 11);
    pub const SOURCE: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 10);
    pub const MKTDATA: u16 = 40_000;
    pub const SNAPSHOT: u16 = 40_002;

    pub fn joined(role: PortRole, group: Ipv4Addr, port: u16) -> JoinedRole {
        JoinedRole {
            role: role.as_str().to_owned(),
            group,
            port,
            interface: None,
            source: None,
        }
    }

    pub fn coverage(first_seq: u64, last_seq: u64) -> InstanceCoverage {
        InstanceCoverage {
            first_seq,
            last_seq,
            count: last_seq - first_seq + 1,
            reset_counts_seen: vec![0],
        }
    }

    /// A manifest with `joins` and one instance per `(port, coverage)`.
    pub fn manifest(
        joins: Vec<JoinedRole>,
        instances: &[(u16, InstanceCoverage)],
    ) -> SegmentManifest {
        SegmentManifest {
            site: "site-1".to_owned(),
            recorder: "recorder-1".to_owned(),
            env: "test".to_owned(),
            feed: "market-by-price".to_owned(),
            build_version: "0.1.0".to_owned(),
            build_commit: "0000000".to_owned(),
            config_hash: "a".repeat(64),
            segment_seq: 7,
            start_ns: 1_000,
            end_ns: 9_000,
            datagram_count: 10,
            payload_byte_count: 1_000,
            object_key: "feed=market-by-price/obj-7".to_owned(),
            byte_count: 500,
            sha256: "b".repeat(64),
            instances: instances
                .iter()
                .map(|(port, c)| (ChannelInstance::new(SOURCE, 1, *port), c.clone()))
                .collect::<BTreeMap<_, _>>(),
            short_datagrams: 0,
            instances_dropped: 0,
            capture_drop_total: 0,
            capture_drop_scope: "port-role".to_owned(),
            interface_drop_total: 0,
            roles_joined: joins,
            link_headers: "synthesised".to_owned(),
            link_header_exceptions: 0,
        }
    }

    pub fn entry(rule_id: &str, outcome: Outcome, port: Option<u16>) -> RuleOutcome {
        RuleOutcome {
            rule_id: rule_id.to_owned(),
            outcome,
            instance: port.map(|dst_port| ReportInstance {
                source: SOURCE,
                channel_id: 1,
                dst_port,
            }),
            evidence: Some(EvidenceRange {
                reset_count: 0,
                first_seq: 3,
                last_seq: 4,
            }),
            detail: format!("{rule_id} said so"),
            needs: Vec::new(),
        }
    }

    pub fn report(rules: Vec<RuleOutcome>) -> RuleSetReport {
        RuleSetReport {
            report_format: dz_recorder_conformance::report::REPORT_FORMAT,
            feed: "mbp".to_owned(),
            rules,
        }
    }

    fn needing(mut e: RuleOutcome, role: NeededRole) -> RuleOutcome {
        e.needs.push(role);
        e
    }

    /// The snapshot role never claimed: a rule that reads it is `na`, and the
    /// rules that read only mktdata are left exactly as stated.
    #[test]
    fn a_role_nobody_joined_makes_the_rules_that_read_it_na_and_leaves_the_others_alone() {
        let m = manifest(
            vec![joined(PortRole::Mktdata, GROUP, MKTDATA)],
            &[(MKTDATA, coverage(0, 99))],
        );
        let r = report(vec![
            needing(
                entry("TEST.ANCHORED", Outcome::Pass, Some(MKTDATA)),
                NeededRole::Snapshot,
            ),
            entry("TEST.STRUCTURE", Outcome::Pass, Some(MKTDATA)),
            entry("TEST.OTHER", Outcome::Violation, Some(MKTDATA)),
        ]);
        let (placed, refused) = place(&m, GROUP, &r, &Holes::default());
        assert_eq!(refused, Refused::default());
        let verdicts: Vec<_> = placed
            .iter()
            .map(|p| (p.entry.rule_id.as_str(), p.verdict))
            .collect();
        assert_eq!(
            verdicts,
            vec![
                ("TEST.ANCHORED", FindingVerdict::Na),
                ("TEST.STRUCTURE", FindingVerdict::Pass),
                ("TEST.OTHER", FindingVerdict::Violation),
            ]
        );
        assert!(
            placed[0].detail.contains("did not join"),
            "{}",
            placed[0].detail
        );
        assert_eq!(
            placed[1].detail, "TEST.STRUCTURE said so",
            "the rule's own words, untouched"
        );
    }

    /// Claimed and silent is the same verdict for a different reason, and the
    /// test asserts both halves: that the verdicts agree, and that the reasons
    /// do not — because the code paths differ and only one of them is obvious.
    #[test]
    fn a_role_joined_that_carried_nothing_is_na_too_for_a_different_reason() {
        let never = manifest(
            vec![joined(PortRole::Mktdata, GROUP, MKTDATA)],
            &[(MKTDATA, coverage(0, 99))],
        );
        let silent = manifest(
            vec![
                joined(PortRole::Mktdata, GROUP, MKTDATA),
                joined(PortRole::Snapshot, GROUP, SNAPSHOT),
            ],
            &[(MKTDATA, coverage(0, 99))],
        );
        let r = report(vec![needing(
            entry("TEST.ANCHORED", Outcome::Pass, Some(MKTDATA)),
            NeededRole::Snapshot,
        )]);
        let e = &r.rules[0];
        assert_eq!(
            unrun(&never, GROUP, e),
            Some(Unrun::NotJoined(PortRole::Snapshot))
        );
        assert_eq!(
            unrun(&silent, GROUP, e),
            Some(Unrun::Silent(PortRole::Snapshot, SNAPSHOT))
        );
        let (a, _) = place(&never, GROUP, &r, &Holes::default());
        let (b, _) = place(&silent, GROUP, &r, &Holes::default());
        assert_eq!(a[0].verdict, FindingVerdict::Na);
        assert_eq!(b[0].verdict, FindingVerdict::Na);
        assert_ne!(a[0].detail, b[0].detail, "same verdict, different reasons");
        assert!(b[0].detail.contains("carried nothing"), "{}", b[0].detail);

        // And once the port carries something, the stated verdict stands.
        let carrying = manifest(
            silent.roles_joined.clone(),
            &[(MKTDATA, coverage(0, 99)), (SNAPSHOT, coverage(0, 9))],
        );
        let (c, _) = place(&carrying, GROUP, &r, &Holes::default());
        assert_eq!(c[0].verdict, FindingVerdict::Pass);
    }

    /// A role joined in another group is not joined in this one.
    #[test]
    fn a_join_in_another_group_does_not_count_for_this_one() {
        let m = manifest(
            vec![
                joined(PortRole::Mktdata, GROUP, MKTDATA),
                joined(PortRole::Snapshot, OTHER_GROUP, SNAPSHOT),
            ],
            &[(MKTDATA, coverage(0, 99)), (SNAPSHOT, coverage(0, 9))],
        );
        let r = report(vec![needing(
            entry("TEST.ANCHORED", Outcome::Pass, Some(MKTDATA)),
            NeededRole::Snapshot,
        )]);
        assert_eq!(
            unrun(&m, GROUP, &r.rules[0]),
            Some(Unrun::NotJoined(PortRole::Snapshot))
        );
    }

    /// A stated violation over a role that carried nothing is not written as
    /// one: the rule judged nothing.
    #[test]
    fn na_takes_precedence_over_whatever_was_stated() {
        let m = manifest(
            vec![joined(PortRole::Mktdata, GROUP, MKTDATA)],
            &[(MKTDATA, coverage(0, 99))],
        );
        let r = report(vec![needing(
            entry("TEST.ANCHORED", Outcome::Violation, Some(MKTDATA)),
            NeededRole::Snapshot,
        )]);
        let (placed, _) = place(&m, GROUP, &r, &Holes::default());
        assert_eq!(placed[0].verdict, FindingVerdict::Na);
        assert!(
            placed[0].detail.starts_with("TEST.ANCHORED said so ["),
            "{}",
            placed[0].detail
        );
    }

    /// An entry naming no instance is refused and counted, and produces no
    /// row; so is one naming an instance the manifest does not describe.
    #[test]
    fn an_entry_that_cannot_be_placed_is_refused_and_counted_never_guessed() {
        let m = manifest(
            vec![joined(PortRole::Mktdata, GROUP, MKTDATA)],
            &[(MKTDATA, coverage(0, 99))],
        );
        let r = report(vec![
            entry("TEST.NOWHERE", Outcome::Violation, None),
            entry("TEST.ELSEWHERE", Outcome::Violation, Some(41_999)),
            entry("TEST.HERE", Outcome::Pass, Some(MKTDATA)),
        ]);
        let (placed, refused) = place(&m, GROUP, &r, &Holes::default());
        assert_eq!(placed.len(), 1);
        assert_eq!(placed[0].entry.rule_id, "TEST.HERE");
        assert_eq!(
            refused,
            Refused {
                no_instance: 1,
                undescribed_instance: 1,
            }
        );
    }

    /// An instance the manifest describes, on a port not joined in the group
    /// being judged, has no role to be filed under.
    #[test]
    fn an_instance_on_a_port_this_group_did_not_join_is_refused() {
        let m = manifest(
            vec![joined(PortRole::Mktdata, OTHER_GROUP, MKTDATA)],
            &[(MKTDATA, coverage(0, 99))],
        );
        let r = report(vec![entry("TEST.HERE", Outcome::Pass, Some(MKTDATA))]);
        let (placed, refused) = place(&m, GROUP, &r, &Holes::default());
        assert!(placed.is_empty());
        assert_eq!(refused.undescribed_instance, 1);
    }

    /// The rule set's own vocabulary, mapped: `suspected` is not a violation.
    #[test]
    fn a_suspected_mismatch_is_not_written_as_a_violation() {
        let m = manifest(
            vec![joined(PortRole::Mktdata, GROUP, MKTDATA)],
            &[(MKTDATA, coverage(0, 99))],
        );
        let r = report(vec![
            entry("TEST.A", Outcome::Suspected, Some(MKTDATA)),
            entry("TEST.B", Outcome::Unverifiable, Some(MKTDATA)),
            entry("TEST.C", Outcome::Na, Some(MKTDATA)),
        ]);
        let (placed, _) = place(&m, GROUP, &r, &Holes::default());
        let v: Vec<_> = placed.iter().map(|p| p.verdict).collect();
        assert_eq!(
            v,
            vec![
                FindingVerdict::Unverifiable,
                FindingVerdict::Unverifiable,
                FindingVerdict::Na
            ]
        );
    }

    /// A role token the manifest spells some other way is not read as one.
    #[test]
    fn only_the_three_specified_role_tokens_are_roles() {
        assert_eq!(role_named("snapshot"), Some(PortRole::Snapshot));
        assert_eq!(role_named("Snapshot"), None);
        assert_eq!(role_named("snap"), None);
    }
}

/// The absence downgrade, over archives the real record path wrote.
#[cfg(test)]
mod downgrade_tests {
    use std::net::SocketAddrV4;

    use dz_recorder_conformance::{EvidenceRange, ReportInstance};
    use dz_recorder_replay::synthetic::{
        port_for, Fault, StarvationWindow, SyntheticPublisher, CHANNEL_ID, GROUP, PRIMARY_SOURCE,
    };
    use dz_recorder_rows::{derive_object, Verdict};

    use super::tests::report;
    use super::*;
    use crate::conformance::testing::record;

    const MKTDATA: u16 = port_for(PortRole::Mktdata);

    fn stated(outcome: Outcome, evidence: Option<(u8, u64, u64)>) -> RuleOutcome {
        RuleOutcome {
            rule_id: "TEST.RULE".to_owned(),
            outcome,
            instance: Some(ReportInstance {
                source: PRIMARY_SOURCE,
                channel_id: CHANNEL_ID,
                dst_port: MKTDATA,
            }),
            evidence: evidence.map(|(reset_count, first_seq, last_seq)| EvidenceRange {
                reset_count,
                first_seq,
                last_seq,
            }),
            detail: "the rule's own words".to_owned(),
            needs: Vec::new(),
        }
    }

    /// One stated outcome over `publisher`'s archive, as the runner would write
    /// it.
    fn judged(publisher: &SyntheticPublisher, entry: RuleOutcome) -> (FindingVerdict, String) {
        let (_dir, recorded) = record(publisher);
        let holes = recorded.holes();
        let r = report(vec![entry]);
        let (placed, refused) = place(&recorded.manifest, GROUP, &r, &holes);
        assert_eq!(
            refused,
            Refused::default(),
            "the fixture's instance is described"
        );
        assert_eq!(placed.len(), 1);
        (placed[0].verdict, placed[0].detail.clone())
    }

    /// A structural violation with no hole anywhere near it is the publisher's
    /// to answer for, and stays one.
    #[test]
    fn a_violation_with_no_hole_near_it_stays_a_violation() {
        let (verdict, detail) = judged(
            &SyntheticPublisher::clean(1000),
            stated(Outcome::Violation, Some((0, 10, 12))),
        );
        assert_eq!(verdict, FindingVerdict::Violation);
        assert_eq!(detail, "the rule's own words");
    }

    /// The same violation with a gap across its evidence range is
    /// `unverifiable`, and says why without losing what the rule said.
    #[test]
    fn the_same_violation_across_a_gap_is_unverifiable() {
        // The publisher skips seven sequence numbers half way through.
        let publisher = SyntheticPublisher::with_fault(1000, Fault::SequenceGap);
        let (verdict, detail) = judged(&publisher, stated(Outcome::Violation, Some((0, 501, 503))));
        assert_eq!(verdict, FindingVerdict::Unverifiable);
        assert!(detail.starts_with("the rule's own words ["), "{detail}");
        assert!(detail.contains("0:500-506 missing"), "{detail}");

        // Away from the gap, in the same archive, the violation stands.
        let (verdict, _) = judged(&publisher, stated(Outcome::Violation, Some((0, 10, 12))));
        assert_eq!(verdict, FindingVerdict::Violation);
    }

    /// **The test this task exists for.** The hole is demonstrably the
    /// recorder's own — the archive's own sequence gap row says so — and a
    /// violation whose evidence lies in it must not reach the table as a
    /// publisher-facing violation. Revert the downgrade and this asserts on
    /// exactly the row the design is arranged against.
    #[test]
    fn a_violation_over_the_recorders_own_drop_is_never_written_as_the_publishers() {
        let publisher = SyntheticPublisher::clean(1000).starved(&[StarvationWindow {
            first: 250,
            count: 40,
        }]);
        let (_dir, recorded) = record(&publisher);

        // The hole is ours, by the loss derivation's own account.
        let rows = derive_object(&recorded.object, &recorded.manifest, None)
            .expect("the object derives")
            .rows;
        assert_eq!(rows.sequence_gap.len(), 1, "{:?}", rows.sequence_gap);
        assert_eq!(rows.sequence_gap[0].verdict, Verdict::Recorder);
        assert_eq!(
            (
                rows.sequence_gap[0].missing_from,
                rows.sequence_gap[0].missing_to
            ),
            (250, 289)
        );

        let holes = recorded.holes();
        let r = report(vec![
            stated(Outcome::Violation, Some((0, 260, 262))),
            // Touching the edge of the hole by one sequence number is touching
            // it: both bounds name a number inside the range.
            stated(Outcome::Violation, Some((0, 289, 300))),
            stated(Outcome::Violation, Some((0, 240, 250))),
            // And so is starting on the datagram right after it, which is where
            // a rule about an absence cites its evidence.
            stated(Outcome::Violation, Some((0, 290, 300))),
        ]);
        let (placed, _) = place(&recorded.manifest, GROUP, &r, &holes);
        for p in &placed {
            assert_eq!(
                p.verdict,
                FindingVerdict::Unverifiable,
                "a publisher-facing violation manufactured out of our own drop: {p:?}"
            );
            assert!(p.detail.contains("0:250-289 missing"), "{}", p.detail);
        }
        assert!(
            !placed
                .iter()
                .any(|p| p.verdict == FindingVerdict::Violation),
            "no violation row may stand over this hole"
        );

        // Clear of it on either side does not touch it: one before the first
        // missing number, and two past the last.
        let r = report(vec![
            stated(Outcome::Violation, Some((0, 291, 300))),
            stated(Outcome::Violation, Some((0, 240, 249))),
        ]);
        let (placed, _) = place(&recorded.manifest, GROUP, &r, &holes);
        assert!(placed
            .iter()
            .all(|p| p.verdict == FindingVerdict::Violation));
    }

    /// A gap-type rule cites the datagram after the hole, not the hole: its
    /// evidence begins at `missing_to + 1`. That is adjacency, not overlap, and
    /// it must still be read as the hole's.
    #[test]
    fn evidence_that_begins_on_the_datagram_after_a_gap_is_the_gaps() {
        let publisher = SyntheticPublisher::with_fault(1000, Fault::SequenceGap);
        // The publisher skipped 500..=506, so the next datagram it sent is 507.
        let (verdict, detail) = judged(&publisher, stated(Outcome::Violation, Some((0, 507, 507))));
        assert_eq!(verdict, FindingVerdict::Unverifiable, "{detail}");
        assert!(detail.contains("0:500-506 missing"), "{detail}");
        let (verdict, _) = judged(&publisher, stated(Outcome::Violation, Some((0, 508, 510))));
        assert_eq!(
            verdict,
            FindingVerdict::Violation,
            "one further on is clear of it"
        );
        // A truncated datagram reaches only itself.
        let mut holes = Holes::default();
        let instance = ChannelInstance::new(PRIMARY_SOURCE, CHANNEL_ID, MKTDATA);
        holes.truncated(instance, 0, 7);
        let at = |first_seq, last_seq| EvidenceRange {
            reset_count: 0,
            first_seq,
            last_seq,
        };
        assert!(holes.overlapping(&instance, &at(7, 7)).is_some());
        assert!(holes.overlapping(&instance, &at(8, 8)).is_none());
    }

    /// A range means nothing across a reset: a hole in one era does not
    /// excuse a violation in the next.
    #[test]
    fn a_hole_in_one_era_does_not_touch_evidence_in_another() {
        let publisher = SyntheticPublisher::clean(1000).starved(&[StarvationWindow {
            first: 250,
            count: 40,
        }]);
        let (verdict, _) = judged(&publisher, stated(Outcome::Violation, Some((1, 260, 262))));
        assert_eq!(verdict, FindingVerdict::Violation);
    }

    /// A violation that cites no range cannot be held against the holes, and
    /// is not written as one.
    #[test]
    fn a_violation_with_no_evidence_range_is_unverifiable() {
        let (verdict, detail) = judged(
            &SyntheticPublisher::clean(100),
            stated(Outcome::Violation, None),
        );
        assert_eq!(verdict, FindingVerdict::Unverifiable);
        assert!(detail.contains("cited no evidence range"), "{detail}");
    }

    /// A pass is written only over a window the runner can vouch for.
    #[test]
    fn a_pass_over_a_hole_is_not_one_this_runner_can_stand_behind() {
        let starved = SyntheticPublisher::clean(1000).starved(&[StarvationWindow {
            first: 250,
            count: 40,
        }]);
        let (verdict, _) = judged(&starved, stated(Outcome::Pass, Some((0, 0, 999))));
        assert_eq!(verdict, FindingVerdict::Unverifiable);
        let (verdict, _) = judged(&starved, stated(Outcome::Pass, None));
        assert_eq!(
            verdict,
            FindingVerdict::Unverifiable,
            "no range, and the instance has a hole"
        );
        let (verdict, _) = judged(&starved, stated(Outcome::Pass, Some((0, 0, 100))));
        assert_eq!(verdict, FindingVerdict::Pass, "clear of it");

        let clean = SyntheticPublisher::clean(1000);
        let (verdict, _) = judged(&clean, stated(Outcome::Pass, None));
        assert_eq!(verdict, FindingVerdict::Pass);
    }

    /// The downgrade only ever moves towards `unverifiable`.
    #[test]
    fn a_hole_never_turns_an_unverifiable_or_na_into_anything_else() {
        let starved = SyntheticPublisher::clean(1000).starved(&[StarvationWindow {
            first: 250,
            count: 40,
        }]);
        for outcome in [Outcome::Unverifiable, Outcome::Suspected, Outcome::Na] {
            let (verdict, detail) = judged(&starved, stated(outcome, Some((0, 260, 262))));
            assert_ne!(verdict, FindingVerdict::Violation);
            assert_ne!(verdict, FindingVerdict::Pass);
            assert_eq!(detail, "the rule's own words", "{outcome:?}");
        }
    }

    /// A datagram the capture cut short is a hole at its own sequence number:
    /// a rule handed its body sees a length the bytes do not fill.
    #[test]
    fn a_datagram_the_capture_cut_short_is_a_hole_at_its_sequence_number() {
        let mut datagrams = SyntheticPublisher::clean(20).datagrams();
        datagrams[7].wire_payload_len += 100;
        let mut finder = HoleFinder::new(CaptureDropScope::PortRole);
        for dg in &datagrams {
            finder.observe(&dg.as_recorded());
        }
        let holes = finder.finish();
        let instance = ChannelInstance::new(PRIMARY_SOURCE, CHANNEL_ID, MKTDATA);
        let hole = holes
            .overlapping(
                &instance,
                &EvidenceRange {
                    reset_count: 0,
                    first_seq: 7,
                    last_seq: 7,
                },
            )
            .expect("the truncated datagram is a hole");
        assert_eq!(hole.cause, HoleCause::Truncated);
        assert!(holes
            .overlapping(
                &instance,
                &EvidenceRange {
                    reset_count: 0,
                    first_seq: 8,
                    last_seq: 19,
                },
            )
            .is_none());
        assert_eq!(*datagrams[7].dst.ip(), GROUP);
        let _: SocketAddrV4 = datagrams[7].src;
    }
}
