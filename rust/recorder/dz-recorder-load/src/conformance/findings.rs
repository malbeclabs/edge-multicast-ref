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

use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;

use dz_edge_core::{DatagramHeader, PortRole};
use dz_recorder_archive::{InstanceCoverage, SegmentManifest};
use dz_recorder_conformance::{EvidenceRange, Outcome, RuleOutcome, RuleSetReport};
use dz_recorder_core::{CaptureDropScope, ChannelInstance, RecordedDatagram};
use dz_recorder_loss::{DeriverLimits, LossDeriver, LossReport, SequenceRun};
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
    /// The era's ordinal on the instance, counting from 1, where the loss
    /// derivation assigned one. A truncated datagram states none.
    pub era: Option<u64>,
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

/// Whether a hole and a range are in one era, where both say which.
///
/// The `Reset Count` is a `u8` and wraps, so two eras 256 resets apart share it;
/// an era ordinal does not wrap, and where the range and the hole both state
/// one, they must agree. Where either does not, the `Reset Count` alone decides,
/// which can only match more holes and never fewer.
fn same_era(hole: Option<u64>, evidence: Option<u64>) -> bool {
    match (hole, evidence) {
        (Some(h), Some(e)) => h == e,
        _ => true,
    }
}

/// This object's own holes, per instance.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Holes {
    by_instance: BTreeMap<ChannelInstance, Vec<Hole>>,
    /// Instances whose loss derivation hit a bound — eras or delivered ranges
    /// refused — so the runs reported for them are an under-count, and a
    /// stretch with no reported hole may still be one.
    incomplete: BTreeSet<ChannelInstance>,
    /// The derivation refused whole instances, so which ones it did not track
    /// cannot be said: every instance in the object is in doubt.
    incomplete_object: bool,
}

impl Holes {
    /// One run the loss derivation produced.
    pub fn missing(&mut self, run: &SequenceRun) {
        self.push(
            run.instance,
            Hole {
                reset_count: run.reset_count,
                era: Some(run.era_ordinal),
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
                era: None,
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
            h.reset_count == evidence.reset_count
                && same_era(h.era, evidence.era_index)
                && evidence.overlaps(h.first, reach)
        })
    }

    /// Any hole on any path of one channel and port: every source address that
    /// carried `channel_id` to `dst_port` in this object.
    #[must_use]
    pub fn first_on_channel(
        &self,
        channel_id: u8,
        dst_port: u16,
    ) -> Option<(ChannelInstance, Hole)> {
        self.by_instance
            .iter()
            .filter(|(i, _)| i.channel_id == channel_id && i.dst_port == dst_port)
            .find_map(|(i, holes)| holes.first().map(|h| (*i, *h)))
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
        self.by_instance.is_empty() && self.incomplete.is_empty() && !self.incomplete_object
    }

    /// Marks `instance` as one whose holes are not all known.
    pub fn incomplete(&mut self, instance: ChannelInstance) {
        self.incomplete.insert(instance);
    }

    /// Marks every instance in the object as one whose holes are not all known.
    pub fn incomplete_everywhere(&mut self) {
        self.incomplete_object = true;
    }

    /// Why the loss coverage of `instance` cannot be vouched for, if it cannot.
    #[must_use]
    pub fn coverage_gap(&self, instance: &ChannelInstance) -> Option<&'static str> {
        if self.incomplete_object {
            Some(
                "the loss derivation refused whole instances in this object, so its holes are not \
                 all known",
            )
        } else if self.incomplete.contains(instance) {
            Some(
                "the loss derivation hit its bound on this instance, so its holes are not all known",
            )
        } else {
            None
        }
    }

    /// The same, for any path of one channel and port.
    #[must_use]
    pub fn channel_coverage_gap(&self, channel_id: u8, dst_port: u16) -> Option<&'static str> {
        if self.incomplete_object {
            return self.coverage_gap(&ChannelInstance::new(CHANNEL_SOURCE, channel_id, dst_port));
        }
        self.incomplete
            .iter()
            .find(|i| i.channel_id == channel_id && i.dst_port == dst_port)
            .and_then(|i| self.coverage_gap(i))
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
        Self::with_limits(scope, DeriverLimits::default())
    }

    /// The same, under stated bounds. The bounds are the loss derivation's own;
    /// stating them is for a test that needs to reach one.
    #[must_use]
    pub fn with_limits(scope: CaptureDropScope, limits: DeriverLimits) -> Self {
        Self {
            loss: LossDeriver::with_limits(scope, limits),
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
        // The runs are exhaustive only where the derivation tracked everything.
        // A bound it hit is a stretch it did not look at, and a stretch nobody
        // looked at is not a stretch with no hole in it.
        for loss in report.instances() {
            if loss.eras_refused > 0 || loss.ranges_refused > 0 {
                holes.incomplete(loss.instance);
            }
        }
        if report.instances_refused() > 0 {
            holes.incomplete_everywhere();
        }
        holes
    }
}

/// What arrived, per multicast group, in this object.
///
/// Per group and not per object: two groups can be joined on the same port,
/// and the rule set is run over one group's datagrams at a time. A port that
/// carried something in another group carried nothing in the file this group's
/// rule set read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Carried {
    ports: BTreeSet<(Ipv4Addr, u16)>,
    channels: BTreeSet<(Ipv4Addr, u8, u16)>,
}

impl Carried {
    pub fn observe(&mut self, dg: &RecordedDatagram<'_>) {
        let group = *dg.dst.ip();
        let port = dg.dst.port();
        self.ports.insert((group, port));
        if let Ok(header) = DatagramHeader::peek(dg.payload) {
            self.channels.insert((group, header.channel_id, port));
        }
    }

    /// States that `port` carried `channel_id` in `group`, for a test that has
    /// no archive to observe.
    pub fn mark(&mut self, group: Ipv4Addr, channel_id: u8, port: u16) {
        self.ports.insert((group, port));
        self.channels.insert((group, channel_id, port));
    }

    #[must_use]
    pub fn port(&self, group: Ipv4Addr, port: u16) -> bool {
        self.ports.contains(&(group, port))
    }

    #[must_use]
    pub fn channel(&self, group: Ipv4Addr, channel_id: u8, port: u16) -> bool {
        self.channels.contains(&(group, channel_id, port))
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
/// received nothing produces no data either, and it is what arrived *in this
/// group* that tells it from a clean one. A port that carried nothing looks
/// exactly like a clean feed to the rule set, whose exit code cannot say it
/// looked at nothing.
#[must_use]
pub fn unrun(
    manifest: &SegmentManifest,
    group: Ipv4Addr,
    entry: &RuleOutcome,
    carried: &Carried,
) -> Option<Unrun> {
    entry.needs.iter().find_map(|needed| {
        let role = PortRole::from(*needed);
        let Some(port) = joined_port(manifest, group, role) else {
            return Some(Unrun::NotJoined(role));
        };
        (!carried.port(group, port)).then_some(Unrun::Silent(role, port))
    })
}

/// Entries that could not be written, by cause. Counted, never filed under a
/// guess.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Refused {
    /// The rule set named neither an instance nor a channel. A finding on the
    /// wrong instance sends a reader to a sequence space where the evidence is
    /// not, so none is chosen. Counted by rule as well.
    pub no_instance: u64,
    /// The rule set named an instance this object's manifest does not describe,
    /// or one on a port the recorder did not join in that group — so there is
    /// no coverage to state its window by and no role to file it under.
    pub undescribed_instance: u64,
    /// A rule decided at channel grain whose port could not be resolved to one
    /// the recorder joined, in this group, for the role the rule stated — or
    /// whose channel never appeared on it. Without a port there is no row key.
    pub channel_unresolved: u64,
    /// The rule set stopped tracking source addresses for the channel at its
    /// cap, so the path the finding is about cannot be named. Never a channel
    /// row: the finding is about one path, unknown, and not about every path.
    pub overflow: u64,
    /// `no_instance`, by rule. The rule id is the rule set's, carried through
    /// opaquely; nothing here knows what any of them mean.
    pub no_instance_by_rule: BTreeMap<String, u64>,
}

impl Refused {
    pub fn add(&mut self, other: &Self) {
        self.no_instance += other.no_instance;
        self.undescribed_instance += other.undescribed_instance;
        self.channel_unresolved += other.channel_unresolved;
        self.overflow += other.overflow;
        for (rule, n) in &other.no_instance_by_rule {
            *self.no_instance_by_rule.entry(rule.clone()).or_default() += n;
        }
    }

    #[must_use]
    pub const fn total(&self) -> u64 {
        self.no_instance + self.undescribed_instance + self.channel_unresolved + self.overflow
    }
}

/// The source address a channel-grain row carries.
///
/// `0.0.0.0` means *decided over state every path of the channel fills; no
/// single publisher is the subject* — the same statement the rule set's own
/// metrics make with an empty `source_addr`. It is unambiguous because it is
/// never a publisher's address: `0.0.0.0` is the unspecified address, which no
/// host sends from, so no datagram in an archive carries it.
pub const CHANNEL_SOURCE: Ipv4Addr = Ipv4Addr::UNSPECIFIED;

/// What a placed verdict is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Subject {
    /// One path's view of one channel: the instance the rule set named.
    Instance,
    /// The channel on one port, over every path: the instance's source is
    /// [`CHANNEL_SOURCE`].
    Channel,
}

/// One stated verdict, placed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placed<'a> {
    pub entry: &'a RuleOutcome,
    /// For a channel row, `CHANNEL_SOURCE` with the channel and its port.
    pub instance: ChannelInstance,
    pub subject: Subject,
    /// The role the recorder joined the instance's port as.
    pub role: PortRole,
    /// The instance's coverage in this object. `None` for a channel row,
    /// which spans several sequence spaces and so has no one coverage.
    pub coverage: Option<&'a InstanceCoverage>,
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
    carried: &Carried,
) -> (Vec<Placed<'a>>, Refused) {
    let mut placed = Vec::with_capacity(report.rules.len());
    let mut refused = Refused::default();
    for entry in &report.rules {
        let (instance, subject, role, coverage) = match entry.instance {
            Some(named) => {
                let instance = ChannelInstance::from(named);
                let (Some(coverage), Some(role)) = (
                    manifest.instances.get(&instance),
                    role_on(manifest, group, instance.dst_port),
                ) else {
                    refused.undescribed_instance += 1;
                    continue;
                };
                (instance, Subject::Instance, role, Some(coverage))
            }
            None if entry.overflow => {
                refused.overflow += 1;
                continue;
            }
            None => {
                let Some(channel_id) = entry.channel_id else {
                    refused.no_instance += 1;
                    *refused
                        .no_instance_by_rule
                        .entry(entry.rule_id.clone())
                        .or_default() += 1;
                    continue;
                };
                let Some((dst_port, role)) =
                    channel_port(manifest, group, channel_id, entry, carried)
                else {
                    refused.channel_unresolved += 1;
                    continue;
                };
                (
                    ChannelInstance::new(CHANNEL_SOURCE, channel_id, dst_port),
                    Subject::Channel,
                    role,
                    None,
                )
            }
        };
        let (verdict, detail) = match unrun(manifest, group, entry, carried) {
            // Before the stated outcome, whatever it is: a rule that read a
            // role nothing arrived on judged nothing, and a verdict it stated
            // over that nothing is not one this table may carry.
            Some(why) => (FindingVerdict::Na, with_note(&entry.detail, &why.note())),
            None => match subject {
                Subject::Instance => downgraded(entry, &instance, holes),
                Subject::Channel => channel_downgraded(entry, &instance, holes),
            },
        };
        placed.push(Placed {
            entry,
            instance,
            subject,
            role,
            coverage,
            verdict,
            detail,
        });
    }
    (placed, refused)
}

/// The port a channel-grain entry's evidence arrived on: the one port the
/// recorder joined, in this group, for the role the entry states — and only if
/// the channel appears on it in this object.
///
/// `None` when the entry states no role, when the group joined that role on
/// no port or on more than one, or when no path carried the channel to that
/// port. Every one of those is a row key this runner would be guessing at.
fn channel_port(
    manifest: &SegmentManifest,
    group: Ipv4Addr,
    channel_id: u8,
    entry: &RuleOutcome,
    carried: &Carried,
) -> Option<(u16, PortRole)> {
    let role = PortRole::from(entry.port_role?);
    let mut ports: Vec<u16> = manifest
        .roles_joined
        .iter()
        .filter(|j| j.group == group && role_named(&j.role) == Some(role))
        .map(|j| j.port)
        .collect();
    ports.sort_unstable();
    ports.dedup();
    let [port] = ports[..] else {
        return None;
    };
    carried
        .channel(group, channel_id, port)
        .then_some((port, role))
}

/// The stated verdict of a channel-grain rule, after this object's holes.
///
/// **Stricter than an instance's.** A channel rule is decided over state every
/// path fills, and the evidence it cites does not lie in one sequence space —
/// so a range cannot be held against one path's holes. A violation or a pass is
/// `unverifiable` if *any* path that carried the channel to that port has *any*
/// hole in this object: the state the rule judged may be missing exactly what
/// that hole took.
fn channel_downgraded(
    entry: &RuleOutcome,
    channel: &ChannelInstance,
    holes: &Holes,
) -> (FindingVerdict, String) {
    let verdict = stated(entry.outcome);
    if !matches!(verdict, FindingVerdict::Violation | FindingVerdict::Pass) {
        return (verdict, entry.detail.clone());
    }
    if let Some(why) = holes.channel_coverage_gap(channel.channel_id, channel.dst_port) {
        return (
            FindingVerdict::Unverifiable,
            with_note(&entry.detail, &format!("unverifiable: {why}")),
        );
    }
    match holes.first_on_channel(channel.channel_id, channel.dst_port) {
        Some((path, hole)) => (
            FindingVerdict::Unverifiable,
            with_note(
                &entry.detail,
                &format!(
                    "unverifiable: decided over every path of channel {} on port {}, and the \
                     path from {} has {}",
                    channel.channel_id,
                    channel.dst_port,
                    path.source,
                    hole.note()
                ),
            ),
        ),
        None => (verdict, entry.detail.clone()),
    }
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
    if let Some(why) = holes.coverage_gap(instance) {
        return (
            FindingVerdict::Unverifiable,
            with_note(&entry.detail, &format!("unverifiable: {why}")),
        );
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
    pub const SOURCE_IP: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 10);
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
                .map(|(port, c)| (ChannelInstance::new(SOURCE_IP, 1, *port), c.clone()))
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

    /// What a manifest's instances say arrived, all of it in [`GROUP`].
    pub fn carried(m: &SegmentManifest) -> Carried {
        let mut c = Carried::default();
        for i in m.instances.keys() {
            c.mark(GROUP, i.channel_id, i.dst_port);
        }
        c
    }

    pub fn entry(rule_id: &str, outcome: Outcome, port: Option<u16>) -> RuleOutcome {
        RuleOutcome {
            rule_id: rule_id.to_owned(),
            outcome,
            instance: port.map(|dst_port| ReportInstance {
                source_addr: SOURCE_IP,
                channel_id: 1,
                dst_port,
            }),
            evidence: Some(EvidenceRange {
                reset_count: 0,
                era_index: None,
                first_seq: 3,
                last_seq: 4,
            }),
            detail: format!("{rule_id} said so"),
            needs: Vec::new(),
            channel_id: None,
            port_role: None,
            overflow: false,
        }
    }

    pub fn report(rules: Vec<RuleOutcome>) -> RuleSetReport {
        RuleSetReport {
            report_format: dz_recorder_conformance::report::REPORT_FORMAT,
            feed: "mbp".to_owned(),
            rules,
            unplaced_findings: 0,
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
        let (placed, refused) = place(&m, GROUP, &r, &Holes::default(), &carried(&m));
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
            unrun(&never, GROUP, e, &carried(&never)),
            Some(Unrun::NotJoined(PortRole::Snapshot))
        );
        assert_eq!(
            unrun(&silent, GROUP, e, &carried(&silent)),
            Some(Unrun::Silent(PortRole::Snapshot, SNAPSHOT))
        );
        let (a, _) = place(&never, GROUP, &r, &Holes::default(), &carried(&never));
        let (b, _) = place(&silent, GROUP, &r, &Holes::default(), &carried(&silent));
        assert_eq!(a[0].verdict, FindingVerdict::Na);
        assert_eq!(b[0].verdict, FindingVerdict::Na);
        assert_ne!(a[0].detail, b[0].detail, "same verdict, different reasons");
        assert!(b[0].detail.contains("carried nothing"), "{}", b[0].detail);

        // And once the port carries something, the stated verdict stands.
        let carrying = manifest(
            silent.roles_joined.clone(),
            &[(MKTDATA, coverage(0, 99)), (SNAPSHOT, coverage(0, 9))],
        );
        let (c, _) = place(&carrying, GROUP, &r, &Holes::default(), &carried(&carrying));
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
            unrun(&m, GROUP, &r.rules[0], &carried(&m)),
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
        let (placed, _) = place(&m, GROUP, &r, &Holes::default(), &carried(&m));
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
        let (placed, refused) = place(&m, GROUP, &r, &Holes::default(), &carried(&m));
        assert_eq!(placed.len(), 1);
        assert_eq!(placed[0].entry.rule_id, "TEST.HERE");
        assert_eq!(refused.no_instance, 1);
        assert_eq!(refused.undescribed_instance, 1);
        assert_eq!(
            refused.no_instance_by_rule,
            BTreeMap::from([("TEST.NOWHERE".to_owned(), 1)])
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
        let (placed, refused) = place(&m, GROUP, &r, &Holes::default(), &carried(&m));
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
        let (placed, _) = place(&m, GROUP, &r, &Holes::default(), &carried(&m));
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

    fn channel_wide(
        rule: &str,
        outcome: Outcome,
        channel: Option<u8>,
        role: Option<NeededRole>,
    ) -> RuleOutcome {
        let mut e = entry(rule, outcome, None);
        e.evidence = None;
        e.channel_id = channel;
        e.port_role = role;
        e
    }

    fn both_roles() -> SegmentManifest {
        manifest(
            vec![
                joined(PortRole::Mktdata, GROUP, MKTDATA),
                joined(PortRole::Snapshot, GROUP, SNAPSHOT),
            ],
            &[(MKTDATA, coverage(0, 99)), (SNAPSHOT, coverage(0, 9))],
        )
    }

    /// A rule decided over every path of a channel names no instance; it is
    /// written at channel grain, at source address 0.0.0.0, on the port the
    /// recorder joined for the role the rule states.
    #[test]
    fn a_channel_scoped_entry_is_placed_on_the_channel_at_the_unspecified_address() {
        let m = both_roles();
        let r = report(vec![channel_wide(
            "TEST.CHANNEL_WIDE",
            Outcome::Pass,
            Some(1),
            Some(NeededRole::Snapshot),
        )]);
        let (placed, refused) = place(&m, GROUP, &r, &Holes::default(), &carried(&m));
        assert_eq!(refused, Refused::default());
        assert_eq!(placed.len(), 1);
        let p = &placed[0];
        assert_eq!(p.subject, Subject::Channel);
        assert_eq!(
            p.instance,
            ChannelInstance::new(Ipv4Addr::UNSPECIFIED, 1, SNAPSHOT)
        );
        assert_eq!(p.role, PortRole::Snapshot);
        assert_eq!(p.verdict, FindingVerdict::Pass);
        assert!(
            p.coverage.is_none(),
            "no one sequence space describes a channel"
        );
    }

    /// A channel row whose port cannot be resolved to exactly one joined port
    /// that carried the channel has no key, and is refused and counted.
    #[test]
    fn a_channel_scoped_entry_whose_port_cannot_be_resolved_is_refused_and_counted() {
        let m = both_roles();
        let r = report(vec![
            // Never joined.
            channel_wide("TEST.A", Outcome::Pass, Some(1), Some(NeededRole::Refdata)),
            // Joined, but this channel never appeared on it.
            channel_wide("TEST.B", Outcome::Pass, Some(9), Some(NeededRole::Snapshot)),
            // No role stated, so no port to look for.
            channel_wide("TEST.C", Outcome::Pass, Some(1), None),
            // Neither an instance nor a channel: refused as before.
            channel_wide("TEST.D", Outcome::Pass, None, Some(NeededRole::Snapshot)),
        ]);
        let (placed, refused) = place(&m, GROUP, &r, &Holes::default(), &carried(&m));
        assert!(placed.is_empty(), "{placed:?}");
        assert_eq!(refused.channel_unresolved, 3);
        assert_eq!(refused.no_instance, 1);
        assert_eq!(
            refused.no_instance_by_rule,
            BTreeMap::from([("TEST.D".to_owned(), 1)])
        );

        // Two ports joined for one role in one group: which one is a guess.
        let ambiguous = manifest(
            vec![
                joined(PortRole::Mktdata, GROUP, MKTDATA),
                joined(PortRole::Snapshot, GROUP, SNAPSHOT),
                joined(PortRole::Snapshot, GROUP, SNAPSHOT + 10),
            ],
            &[(MKTDATA, coverage(0, 99)), (SNAPSHOT, coverage(0, 9))],
        );
        let r = report(vec![channel_wide(
            "TEST.A",
            Outcome::Pass,
            Some(1),
            Some(NeededRole::Snapshot),
        )]);
        let (placed, refused) = place(
            &ambiguous,
            GROUP,
            &r,
            &Holes::default(),
            &carried(&ambiguous),
        );
        assert!(placed.is_empty());
        assert_eq!(refused.channel_unresolved, 1);
    }

    /// Two groups on the same snapshot port, and only the other group's
    /// carried anything. The rule set ran over this group's file, which held
    /// no snapshot datagram, so a rule that reads the snapshot role judged
    /// nothing here — whatever arrived in the other group.
    #[test]
    fn a_role_that_carried_in_another_group_carried_nothing_in_this_one() {
        let m = manifest(
            vec![
                joined(PortRole::Mktdata, GROUP, MKTDATA),
                joined(PortRole::Snapshot, GROUP, SNAPSHOT),
                joined(PortRole::Mktdata, OTHER_GROUP, MKTDATA),
                joined(PortRole::Snapshot, OTHER_GROUP, SNAPSHOT),
            ],
            &[(MKTDATA, coverage(0, 99)), (SNAPSHOT, coverage(0, 9))],
        );
        let mut c = Carried::default();
        c.mark(GROUP, 1, MKTDATA);
        c.mark(OTHER_GROUP, 1, MKTDATA);
        c.mark(OTHER_GROUP, 1, SNAPSHOT);
        let r = report(vec![needing(
            entry("TEST.ANCHORED", Outcome::Pass, Some(MKTDATA)),
            NeededRole::Snapshot,
        )]);
        let (here, _) = place(&m, GROUP, &r, &Holes::default(), &c);
        assert_eq!(here[0].verdict, FindingVerdict::Na);
        assert!(
            here[0].detail.contains("carried nothing"),
            "{}",
            here[0].detail
        );
        let (there, _) = place(&m, OTHER_GROUP, &r, &Holes::default(), &c);
        assert_eq!(there[0].verdict, FindingVerdict::Pass);
    }

    /// An entry the rule set could not place because it stopped tracking
    /// addresses is about one unknown path. It names a channel, and is still
    /// never written as the channel's.
    #[test]
    fn an_overflow_entry_is_refused_and_never_a_channel_row() {
        let m = both_roles();
        let mut e = channel_wide(
            "TEST.CHURN",
            Outcome::Violation,
            Some(1),
            Some(NeededRole::Snapshot),
        );
        e.overflow = true;
        let r = report(vec![e]);
        let (placed, refused) = place(&m, GROUP, &r, &Holes::default(), &carried(&m));
        assert!(placed.is_empty(), "{placed:?}");
        assert_eq!(refused.overflow, 1);
        assert_eq!(refused.channel_unresolved, 0);
        assert_eq!(refused.no_instance, 0);
    }

    /// The `Reset Count` wraps; the era ordinal does not. A hole in the first
    /// era and a range in an era 256 resets later share a `Reset Count`, and
    /// only the ordinal tells them apart.
    #[test]
    fn an_era_ordinal_tells_apart_two_eras_that_share_a_reset_count() {
        let instance = ChannelInstance::new(SOURCE_IP, 1, MKTDATA);
        let mut holes = Holes::default();
        holes.missing(&SequenceRun {
            instance,
            group: GROUP,
            role: PortRole::Mktdata,
            era_ordinal: 1,
            reset_count: 0,
            missing_from: 10,
            missing_to: 12,
            before_ts_ns: 0,
            after_ts_ns: 0,
        });
        let at = |era_index| EvidenceRange {
            reset_count: 0,
            era_index,
            first_seq: 11,
            last_seq: 11,
        };
        assert!(
            holes.overlapping(&instance, &at(Some(1))).is_some(),
            "the same era"
        );
        assert!(
            holes.overlapping(&instance, &at(Some(257))).is_none(),
            "the same Reset Count, 256 resets later"
        );
        assert!(
            holes.overlapping(&instance, &at(None)).is_some(),
            "no ordinal stated: the Reset Count decides, and matches"
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
                source_addr: PRIMARY_SOURCE,
                channel_id: CHANNEL_ID,
                dst_port: MKTDATA,
            }),
            evidence: evidence.map(|(reset_count, first_seq, last_seq)| EvidenceRange {
                reset_count,
                era_index: None,
                first_seq,
                last_seq,
            }),
            detail: "the rule's own words".to_owned(),
            needs: Vec::new(),
            channel_id: None,
            port_role: None,
            overflow: false,
        }
    }

    /// One stated outcome over `publisher`'s archive, as the runner would write
    /// it.
    fn judged(publisher: &SyntheticPublisher, entry: RuleOutcome) -> (FindingVerdict, String) {
        let (_dir, recorded) = record(publisher);
        let holes = recorded.holes();
        let r = report(vec![entry]);
        let (placed, refused) = place(&recorded.manifest, GROUP, &r, &holes, &recorded.carried());
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
        let (placed, _) = place(&recorded.manifest, GROUP, &r, &holes, &recorded.carried());
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
        let (placed, _) = place(&recorded.manifest, GROUP, &r, &holes, &recorded.carried());
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
            era_index: None,
            first_seq,
            last_seq,
        };
        assert!(holes.overlapping(&instance, &at(7, 7)).is_some());
        assert!(holes.overlapping(&instance, &at(8, 8)).is_none());
    }

    fn channel_wide(outcome: Outcome, evidence: Option<(u8, u64, u64)>) -> RuleOutcome {
        let mut e = stated(outcome, evidence);
        e.instance = None;
        e.channel_id = Some(CHANNEL_ID);
        e.port_role = Some(dz_recorder_conformance::NeededRole::Mktdata);
        e
    }

    /// A channel rule's evidence is not one sequence space, so its range is
    /// not held against one path's holes: a hole on *any* path of the channel
    /// makes its violation `unverifiable`, wherever the range it cites lies.
    /// Judged by the instance rule instead, the violation below — cited far
    /// from the hole — would stand, and it is a verdict over book state that
    /// hole may have taken from.
    #[test]
    fn a_hole_on_any_path_of_the_channel_makes_a_channel_violation_unverifiable() {
        let starved = SyntheticPublisher::clean(1000).starved(&[StarvationWindow {
            first: 250,
            count: 40,
        }]);
        let (_dir, recorded) = record(&starved);
        let holes = recorded.holes();
        let r = report(vec![channel_wide(Outcome::Violation, Some((0, 10, 12)))]);
        let (placed, refused) = place(&recorded.manifest, GROUP, &r, &holes, &recorded.carried());
        assert_eq!(refused, Refused::default());
        assert_eq!(placed[0].subject, Subject::Channel);
        assert_eq!(placed[0].instance.source, CHANNEL_SOURCE);
        assert_eq!(
            placed[0].verdict,
            FindingVerdict::Unverifiable,
            "a channel violation over a channel with a hole in it"
        );
        assert!(
            placed[0].detail.contains("0:250-289 missing"),
            "{}",
            placed[0].detail
        );
        assert!(
            placed[0].detail.contains(&PRIMARY_SOURCE.to_string()),
            "the path that has the hole is named: {}",
            placed[0].detail
        );

        // The instance rule, over the same range, would have let it stand:
        // which is why channel rows do not use it.
        let instance = ChannelInstance::new(PRIMARY_SOURCE, CHANNEL_ID, MKTDATA);
        assert!(holes
            .overlapping(
                &instance,
                &EvidenceRange {
                    reset_count: 0,
                    era_index: None,
                    first_seq: 10,
                    last_seq: 12
                }
            )
            .is_none());

        // A pass is held to the same rule; a clean channel keeps both.
        let r = report(vec![channel_wide(Outcome::Pass, None)]);
        let (placed, _) = place(&recorded.manifest, GROUP, &r, &holes, &recorded.carried());
        assert_eq!(placed[0].verdict, FindingVerdict::Unverifiable);
        for outcome in [Outcome::Violation, Outcome::Pass] {
            let (verdict, _) = judged(
                &SyntheticPublisher::clean(1000),
                channel_wide(outcome, None),
            );
            assert_eq!(verdict, stated_verdict(outcome));
        }
    }

    fn stated_verdict(outcome: Outcome) -> FindingVerdict {
        match outcome {
            Outcome::Violation => FindingVerdict::Violation,
            _ => FindingVerdict::Pass,
        }
    }

    /// A hole on a second publisher's path counts too: the channel is every
    /// path's.
    #[test]
    fn a_hole_on_the_second_path_counts_for_the_channel() {
        let mut holes = Holes::default();
        holes.truncated(
            ChannelInstance::new(
                dz_recorder_replay::synthetic::SECOND_SOURCE,
                CHANNEL_ID,
                MKTDATA,
            ),
            0,
            5,
        );
        let (_dir, recorded) = record(&SyntheticPublisher::clean(100));
        let r = report(vec![channel_wide(Outcome::Violation, None)]);
        let (placed, _) = place(&recorded.manifest, GROUP, &r, &holes, &recorded.carried());
        assert_eq!(placed[0].verdict, FindingVerdict::Unverifiable);
        // And a hole on another channel or another port does not.
        let mut elsewhere = Holes::default();
        elsewhere.truncated(
            ChannelInstance::new(PRIMARY_SOURCE, CHANNEL_ID + 1, MKTDATA),
            0,
            5,
        );
        elsewhere.truncated(
            ChannelInstance::new(PRIMARY_SOURCE, CHANNEL_ID, MKTDATA + 2),
            0,
            5,
        );
        let (placed, _) = place(
            &recorded.manifest,
            GROUP,
            &r,
            &elsewhere,
            &recorded.carried(),
        );
        assert_eq!(placed[0].verdict, FindingVerdict::Violation);
    }

    /// Datagrams of one instance whose sequence numbers descend two at a time,
    /// so every one opens a delivered range of its own.
    fn descending(count: usize) -> Vec<dz_recorder_core::OwnedDatagram> {
        let mut datagrams = SyntheticPublisher::clean(count).datagrams();
        for (i, dg) in datagrams.iter_mut().enumerate() {
            let seq = 1_000u64 - 2 * i as u64;
            dg.payload[4..12].copy_from_slice(&seq.to_le_bytes());
        }
        datagrams
    }

    fn found_under(
        limits: dz_recorder_loss::DeriverLimits,
        datagrams: &[dz_recorder_core::OwnedDatagram],
    ) -> Holes {
        let mut finder = HoleFinder::with_limits(CaptureDropScope::PortRole, limits);
        for dg in datagrams {
            finder.observe(&dg.as_recorded());
        }
        finder.finish()
    }

    /// The loss derivation is bounded, and a bound it hits is a stretch it did
    /// not look at. A violation on that instance — even one cited in an era no
    /// reported hole is in — is not one the runner can stand behind, and nor is
    /// a pass; the channel row over that instance's port is held to the same.
    #[test]
    fn a_loss_derivation_that_hit_its_bound_leaves_nothing_to_vouch_for() {
        let datagrams = descending(30);
        let limits = dz_recorder_loss::DeriverLimits {
            max_ranges_per_era: 4,
            ..dz_recorder_loss::DeriverLimits::default()
        };
        let holes = found_under(limits, &datagrams);
        let instance = ChannelInstance::new(PRIMARY_SOURCE, CHANNEL_ID, MKTDATA);
        assert!(holes.coverage_gap(&instance).is_some(), "the bound was hit");

        let m = super::tests::manifest(
            vec![super::tests::joined(PortRole::Mktdata, GROUP, MKTDATA)],
            &[(MKTDATA, super::tests::coverage(0, 1_000))],
        );
        let c = super::tests::carried(&m);
        let r = report(vec![
            // In an era nothing was reported missing from.
            stated(Outcome::Violation, Some((7, 10, 12))),
            stated(Outcome::Pass, Some((7, 10, 12))),
            channel_wide(Outcome::Violation, None),
        ]);
        let (placed, _) = place(&m, GROUP, &r, &holes, &c);
        for p in &placed {
            assert_eq!(p.verdict, FindingVerdict::Unverifiable, "{p:?}");
            assert!(p.detail.contains("hit its bound"), "{}", p.detail);
        }

        // Under the default bounds the same feed is tracked whole.
        let holes = found_under(dz_recorder_loss::DeriverLimits::default(), &datagrams);
        assert!(holes.coverage_gap(&instance).is_none());
        let (placed, _) = place(&m, GROUP, &r, &holes, &c);
        assert_eq!(placed[0].verdict, FindingVerdict::Violation);
    }

    /// A derivation that refused whole instances cannot say which: every
    /// verdict in the object is in doubt.
    #[test]
    fn a_loss_derivation_that_refused_instances_leaves_the_whole_object_in_doubt() {
        let datagrams = SyntheticPublisher::clean(30).datagrams();
        let limits = dz_recorder_loss::DeriverLimits {
            max_instances: 0,
            ..dz_recorder_loss::DeriverLimits::default()
        };
        let holes = found_under(limits, &datagrams);
        let m = super::tests::manifest(
            vec![super::tests::joined(PortRole::Mktdata, GROUP, MKTDATA)],
            &[(MKTDATA, super::tests::coverage(0, 29))],
        );
        let r = report(vec![
            stated(Outcome::Violation, Some((0, 10, 12))),
            channel_wide(Outcome::Pass, None),
        ]);
        let (placed, _) = place(&m, GROUP, &r, &holes, &super::tests::carried(&m));
        for p in &placed {
            assert_eq!(p.verdict, FindingVerdict::Unverifiable, "{p:?}");
            assert!(p.detail.contains("refused whole instances"), "{}", p.detail);
        }
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
                    era_index: None,
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
                    era_index: None,
                    first_seq: 8,
                    last_seq: 19,
                },
            )
            .is_none());
        assert_eq!(*datagrams[7].dst.ip(), GROUP);
        let _: SocketAddrV4 = datagrams[7].src;
    }
}
