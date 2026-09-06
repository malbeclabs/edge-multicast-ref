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
//! [`pcap`] converts a replayed archive into the classic pcap the tool reads,
//! and it is the *only* such conversion in this repository. `dz-recorder-e2e`'s
//! conformance gate ran its own copy until this crate existed, and a bridge with
//! two implementations is a bridge where the gate and the runner can disagree
//! about what the tool was shown — with the gate being the one nobody would
//! think to re-check.
//!
//! # What this crate does not do
//!
//! It writes no row and reads no manifest. Whether a rule that named no port
//! role is `na`, whether a violation over a hole this object's own loss
//! derivation found becomes `unverifiable`, and what a `pass` row has to satisfy
//! before it is honest — all of that is judgement over the object, and it sits
//! above this seam rather than in it. What is here is only what has to be
//! exactly right before any judgement is worth making: what the tool was shown.
#![forbid(unsafe_code)]

pub mod pcap;

pub use pcap::{write_group_pcaps, write_pcap, BridgeError, GroupPcap};
