//! Between a replayed archive and the specification's own rule set.
//!
//! The recorder keeps bytes and decodes nothing. This crate is what puts those
//! bytes back in front of the rule set that says whether they were legal — the
//! `dz-conformance` tool, which lives in `edge-feed-spec`, is written in Go and
//! is pinned by commit. **No rule is written, encoded, enumerated or
//! allow-listed here**, and a `rule_id` travels through as an opaque string: a
//! runner that knew the names of rules would refuse the next one added, which is
//! exactly the rule that was added to catch the thing nobody had thought of.
//!
//! Two halves, and they are separable on purpose.
//!
//! - [`segment`] writes a replayed archive back out as the pcapng segment the
//!   tool reads, using the recorder's own `SegmentWriter`. It is the *only* such
//!   conversion in this repository. `dz-recorder-e2e`'s conformance gate ran its
//!   own copy until this crate existed, and a bridge with two implementations is
//!   a bridge where the gate and the runner can disagree about what the tool was
//!   shown — with the gate being the one nobody would think to re-check.
//!
//!   It uses pcapng rather than the classic `pcap` the tool also accepts,
//!   because a classic record has nowhere to write `epb_dropcount`. That field
//!   is the recorder's admission of what it failed to record, and it is the
//!   only thing in a segment that separates capture loss from publisher loss: a
//!   conversion that drops it hands the rule set every gap the recorder caused
//!   with nothing to say the publisher did not cause it.
//! - [`tool`] is the boundary against the rule set: a trait, one implementation
//!   that runs the binary, and the version resolution that has to happen before
//!   any verdict may be stamped.
//!
//! # What this crate does not do
//!
//! It writes no row and reads no manifest. Whether a rule that named no port
//! role is `na`, whether a violation over a hole this object's own loss
//! derivation found becomes `unverifiable`, and what a `pass` row has to satisfy
//! before it is honest — all of that is judgement over the object, and it sits
//! above this seam rather than in it. What is here is only the two things that
//! have to be exactly right before any judgement is worth making: what the tool
//! was shown, and which rule set answered.
#![forbid(unsafe_code)]

pub mod report;
pub mod segment;
pub mod tool;

pub use report::{EvidenceRange, Outcome, ReportError, ReportInstance, RuleOutcome, RuleSetReport};
pub use segment::{
    write_group_segments, write_segment, BridgeError, GroupSegment, SectionProvenance,
};
pub use tool::{
    ConformanceTool, Invocation, PinnedRuleSet, PortRoles, RuleSet, ToolError, ToolRun,
};
