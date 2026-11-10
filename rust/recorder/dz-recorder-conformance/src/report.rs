//! The report the rule set states, one entry per rule evaluated.
//!
//! **The runner does not parse standard error, and this module is the reason.**
//! Today's interface is an exit code plus rule identifiers appearing in a log
//! stream, which `dz-recorder-e2e` matches by substring — enough for a gate,
//! and not enough for a table whose grain is one row per rule. A format nobody
//! declared changes with a log line, and a `rule_id` recovered by a regular
//! expression becomes an empty string on the day somebody improves the wording:
//! silently, in the one table whose entire value is that it can be trusted about
//! last month.
//!
//! So the boundary is a declared file, and this is its shape. It is the shape
//! asked of `edge-feed-spec` rather than one this repository reads out of what
//! the tool happens to print, and until that ask lands the fixtures in
//! `tests/fixtures` are the only reports there are.
//!
//! # What each field is for
//!
//! - `rule_id` travels through opaquely. This repository holds no enumeration
//!   of rules, no allow-list and no mapping from rule to meaning: a runner that
//!   refused an identifier it did not know would refuse precisely the rule that
//!   was added to catch the thing nobody had thought of.
//! - `outcome` is the rule set's own vocabulary and not the table's. Turning one
//!   into a `FindingVerdict` is a judgement — the absence downgrade, the `na`
//!   placement — and it is made above this module, over the object's own losses
//!   and its manifest. Nothing here decides anything.
//! - `instance` is what places the row. A finding filed against the wrong
//!   instance is worse than one nobody wrote, because it sends a reader to a
//!   sequence space where the evidence is not — so it is `Option`, and an entry
//!   naming none is refused above rather than guessed at here.
//! - `evidence` is what the absence downgrade tests against this object's own
//!   `SequenceRun`s. `reset_count` is in it because a sequence range means
//!   nothing across an era boundary: a predicate carried across a reset is
//!   comparing two rulers.

use std::net::Ipv4Addr;

use dz_edge_core::PortRole;
use dz_recorder_core::ChannelInstance;
use serde::{Deserialize, Serialize};

/// The report format this runner understands.
///
/// A declared integer rather than a guess at compatibility: a report the runner
/// cannot interpret is an error, and reading a later format's entries as though
/// they meant what this one's mean is how a verdict acquires a meaning nobody
/// stated. Unknown *fields* are tolerated, so that a widening is not a break;
/// an unknown format number is not.
pub const REPORT_FORMAT: u32 = 1;

/// What the rule set concluded, as the rule set's own vocabulary states it.
///
/// Not `FindingVerdict`, deliberately: that table's four values are what this
/// repository decides after the manifest and the object's own holes have been
/// consulted, and `Suspected` has no home among them at all. Mapping the two is
/// judgement and lives above this module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Pass,
    Violation,
    /// A first mismatch awaiting confirmation. The rule set states it and does
    /// not fail its own gate on it, so it is carried here as itself rather than
    /// flattened into one of the other four on the way in.
    Suspected,
    Unverifiable,
    /// The rule did not run. The rule set says so where it can; the manifest
    /// says so for a port role nobody joined, which is a fact no capture file
    /// carries.
    Na,
}

/// The channel instance an entry applies to, as the report names it.
///
/// The three fields of a [`ChannelInstance`] spelled out, because the report is
/// a file another repository writes and a Rust type's field order is not an
/// interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportInstance {
    /// The publisher's source address, keyed `source_addr` as the rule set's
    /// metrics label it.
    pub source_addr: Ipv4Addr,
    pub channel_id: u8,
    pub dst_port: u16,
}

impl From<ReportInstance> for ChannelInstance {
    fn from(r: ReportInstance) -> Self {
        Self::new(r.source_addr, r.channel_id, r.dst_port)
    }
}

/// The sequence range an entry's evidence lies in, within one era.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceRange {
    /// The wire `Reset Count` the range was read under. Kept as a fact and
    /// never as a key: it is a `u8` and it wraps, so two eras 256 resets apart
    /// share a value.
    pub reset_count: u8,
    /// The era's ordinal on the instance within the rule set's run, counting
    /// from 1. Unlike `reset_count` it does not wrap, so it is what tells two
    /// eras 256 resets apart; absent from a report that does not state it.
    #[serde(default)]
    pub era_index: Option<u64>,
    pub first_seq: u64,
    pub last_seq: u64,
}

impl EvidenceRange {
    /// Whether this range shares a sequence value with `other`.
    ///
    /// Inclusive at both ends, because both bounds name a sequence number that
    /// is part of the range rather than one past it.
    #[must_use]
    pub const fn overlaps(&self, other_first: u64, other_last: u64) -> bool {
        self.first_seq <= other_last && other_first <= self.last_seq
    }
}

/// One rule, evaluated once, over one channel instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleOutcome {
    pub rule_id: String,
    pub outcome: Outcome,
    /// Absent where the rule set named no instance. Refused above rather than
    /// filed under a guess.
    #[serde(default)]
    pub instance: Option<ReportInstance>,
    /// Absent where the rule set cited no range. A rule whose evidence cannot
    /// be placed in the sequence space cannot be tested against this object's
    /// holes, which is itself a reason not to write a violation.
    #[serde(default)]
    pub evidence: Option<EvidenceRange>,
    /// The rule's own message, carried into the row's `detail` unaltered.
    #[serde(default)]
    pub detail: String,
    /// The port roles the rule reads, beyond the instance it is placed on.
    ///
    /// What lets a rule that never ran be told from one that ran and held. A
    /// rule anchored on the snapshot port and placed on a mktdata instance
    /// names the snapshot role here; if the recorder never joined that role,
    /// or joined it and received nothing, the rule judged nothing and its row
    /// is `na` — decided from the manifest, above this module. Empty means the
    /// rule set stated no such need, and nothing is inferred from the silence.
    #[serde(default)]
    pub needs: Vec<NeededRole>,
    /// The `Channel ID` the rule was decided on, stated beside the instance and
    /// in place of one. A rule decided over state every path of a channel fills
    /// — the book, the snapshot groups, the reference data — names no instance,
    /// because no single publisher is its subject; it names the channel and the
    /// port role instead, and its row is placed at channel grain above this
    /// module.
    #[serde(default)]
    pub channel_id: Option<u8>,
    /// The port role the rule's evidence arrived on.
    #[serde(default)]
    pub port_role: Option<NeededRole>,
    /// The rule set stopped tracking source addresses for this channel at its
    /// cap, so the finding belongs to a path it can no longer name. Such an
    /// entry names no instance and is never a channel's: it is about one path,
    /// unknown.
    #[serde(default)]
    pub overflow: bool,
}

/// A port role, as a report names one.
///
/// Its own type rather than `PortRole`, for the reason [`ReportInstance`]
/// spells its fields out: the report is a file another repository writes, and
/// a token this runner does not know is a report it cannot read rather than a
/// role it may guess at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NeededRole {
    Mktdata,
    Refdata,
    Snapshot,
}

impl From<NeededRole> for PortRole {
    fn from(role: NeededRole) -> Self {
        match role {
            NeededRole::Mktdata => Self::Mktdata,
            NeededRole::Refdata => Self::Refdata,
            NeededRole::Snapshot => Self::Snapshot,
        }
    }
}

/// One run of the rule set over one group's capture file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleSetReport {
    pub report_format: u32,
    /// The feed the rule set was run for. It exists to be checked rather than
    /// read: [`crate::tool::RuleSet::judge`] holds it against the feed the
    /// invocation named and refuses the pair when they differ, because a report
    /// about another feed parses exactly as well as the right one.
    pub feed: String,
    /// One entry per rule evaluated, in the order the rule set stated them.
    pub rules: Vec<RuleOutcome>,
    /// Findings the rule set could not place at all and did not list.
    #[serde(default)]
    pub unplaced_findings: u64,
}

/// Why a report could not be believed.
#[derive(Debug, thiserror::Error)]
pub enum ReportError {
    #[error("the report is not the declared shape: {0}")]
    Malformed(String),
    /// Refused rather than read leniently: a format this runner does not know
    /// is one whose entries may not mean what these fields mean.
    #[error(
        "the report declares format {found}, and this runner reads format {}",
        REPORT_FORMAT
    )]
    UnknownFormat { found: u32 },
}

impl RuleSetReport {
    /// Parses a report, refusing anything it cannot interpret.
    ///
    /// An empty set of entries parses, and is not an error: a rule set that
    /// evaluated nothing is a fact worth having. What it must never become is
    /// an empty set of *passes*, and it does not — nothing here invents an
    /// entry, so a report with no rules yields no rows.
    pub fn from_json(bytes: &[u8]) -> Result<Self, ReportError> {
        let report: Self =
            serde_json::from_slice(bytes).map_err(|e| ReportError::Malformed(e.to_string()))?;
        if report.report_format != REPORT_FORMAT {
            return Err(ReportError::UnknownFormat {
                found: report.report_format,
            });
        }
        Ok(report)
    }
}
