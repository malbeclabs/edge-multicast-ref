//! What a stated verdict becomes, over one object: placed on an instance, or
//! refused; and `na` when the rule could not have run.
//!
//! A pure function of the manifest and the rule set's report. Nothing here reads
//! configuration: the manifest describes the recorder that observed the bytes,
//! and a configuration describes what somebody believes.

use std::net::Ipv4Addr;

use dz_edge_core::PortRole;
use dz_recorder_archive::{InstanceCoverage, SegmentManifest};
use dz_recorder_conformance::{Outcome, RuleOutcome, RuleSetReport};
use dz_recorder_core::ChannelInstance;
use dz_recorder_rows::FindingVerdict;

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
            None => (stated(entry.outcome), entry.detail.clone()),
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
        let (placed, refused) = place(&m, GROUP, &r);
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
        let (a, _) = place(&never, GROUP, &r);
        let (b, _) = place(&silent, GROUP, &r);
        assert_eq!(a[0].verdict, FindingVerdict::Na);
        assert_eq!(b[0].verdict, FindingVerdict::Na);
        assert_ne!(a[0].detail, b[0].detail, "same verdict, different reasons");
        assert!(b[0].detail.contains("carried nothing"), "{}", b[0].detail);

        // And once the port carries something, the stated verdict stands.
        let carrying = manifest(
            silent.roles_joined.clone(),
            &[(MKTDATA, coverage(0, 99)), (SNAPSHOT, coverage(0, 9))],
        );
        let (c, _) = place(&carrying, GROUP, &r);
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
        let (placed, _) = place(&m, GROUP, &r);
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
        let (placed, refused) = place(&m, GROUP, &r);
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
        let (placed, refused) = place(&m, GROUP, &r);
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
        let (placed, _) = place(&m, GROUP, &r);
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
