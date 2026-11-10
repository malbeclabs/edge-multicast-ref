//! The conformance runner: the specification's own rule set, run over completed
//! objects, and its verdicts kept in `conformance_finding`.
//!
//! The judgement is not made here. The rule set lives in `edge-feed-spec`, is a
//! binary this repository does not build, and is reached through
//! `dz-recorder-conformance`'s [`RuleSet`](dz_recorder_conformance::RuleSet).
//! What this module decides is only what a verdict the rule set stated may be
//! written as, over this object:
//!
//! - **where it goes** — the channel instance the rule set named, and no other
//!   ([`findings`]);
//! - **whether the rule ran at all** — a port role the recorder never joined, or
//!   joined and received nothing on, makes the rule `na` ([`findings`]);
//! - **whether a violation is the publisher's to answer for** — not when its
//!   evidence overlaps a hole this object's own loss derivation found
//!   ([`findings::Holes`]).
//!
//! **No rule is named anywhere in this module**, and `rule_id` travels through
//! as an opaque string. Off by default and per feed: nothing here runs for a
//! feed no `[[conformance]]` entry names.

pub mod findings;
pub mod judge;
pub mod ledger;
pub mod rows;
pub mod stage;
#[cfg(test)]
pub(crate) mod testing;

/// What the runner declined to write, by cause. Every one is counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConformanceRefusal {
    /// The rule set could not run over an object: exit 2, no binary, a report
    /// it did not write or one this runner cannot read. The object stays
    /// unjudged.
    ToolCouldNotRun,
    /// The tool could not say which rule set it is. Nothing is judged.
    VersionUnresolvable,
    /// The tool answered with a version other than the configured one.
    /// Nothing is judged.
    VersionDisagrees,
    /// A stated verdict that named no channel instance.
    NoInstance,
    /// A stated verdict naming an instance the manifest does not describe.
    UndescribedInstance,
}

impl ConformanceRefusal {
    pub const ALL: [Self; 5] = [
        Self::ToolCouldNotRun,
        Self::VersionUnresolvable,
        Self::VersionDisagrees,
        Self::NoInstance,
        Self::UndescribedInstance,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ToolCouldNotRun => "tool_could_not_run",
            Self::VersionUnresolvable => "version_unresolvable",
            Self::VersionDisagrees => "version_disagrees",
            Self::NoInstance => "no_instance",
            Self::UndescribedInstance => "undescribed_instance",
        }
    }
}
