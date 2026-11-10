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
