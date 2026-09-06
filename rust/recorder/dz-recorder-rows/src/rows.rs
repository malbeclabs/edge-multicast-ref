//! The rows, with the column names as field names.
//!
//! Every struct here is the shape of one table. `serde` is the only thing that
//! reads them, and `tests/column_names.rs` asserts each one against a literal
//! JSON object, so a field renamed without the DDL renamed with it fails a test
//! rather than a load.
//!
//! # Timestamps are nanosecond counts, and say so in the type
//!
//! Every stamp is a [`Nanos`]: a bare integer count of nanoseconds since the
//! Unix epoch, which is what a `DateTime64(9)` column reads a JSON number as.
//! It is a newtype rather than a `u64` so that a field cannot be filled from a
//! second count or a millisecond one without the compiler noticing — the archive
//! deals in `_ns` throughout and there is exactly one place, here, where that
//! becomes a column.
//!
//! # Nullable is not a convenience
//!
//! Several columns are `Option`, and each one is a place where *unknown* is a
//! third answer that a zero would tell a reader was a measurement:
//!
//! - [`SequenceGap::unexplained_count`] — the residue after the recorder's own
//!   admitted loss is taken off. At a scope where that subtraction is not valid
//!   there is no residue: zero would exonerate the publisher and the missing
//!   count would accuse it, and the archive can support neither.
//! - [`SequenceGap::interface_drops`] — the *delta* over the window, and absent
//!   when the preceding segment is not available to subtract from. The counter
//!   itself is cumulative and never resets, so its total is a statement about
//!   the host's whole history and never about this window.
//! - [`SequenceGap::seen_elsewhere`], [`SequenceGap::on_redundant_path`] — a
//!   cross-site or cross-instance observation. Absent means the window that
//!   would answer it was not complete, which is why the verdict beside it is
//!   `unverifiable` rather than `publisher`.
//! - [`SequenceGap::sent_from_ts`], [`SequenceGap::sent_to_ts`] — when the
//!   missing datagrams were actually sent, which only a site that received them
//!   can say. A site has no clock reading for a datagram it never received.

use std::net::Ipv4Addr;

use dz_edge_core::PortRole;
use dz_recorder_core::{CaptureDropScope, RecvTsKind};
use serde::{Deserialize, Serialize};

/// A count of nanoseconds since the Unix epoch, as a `DateTime64(9)` reads one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Nanos(pub u64);

impl From<u64> for Nanos {
    fn from(ns: u64) -> Self {
        Self(ns)
    }
}

/// How a receive stamp was obtained, as the column holds it.
///
/// Carried rather than assumed: a latency computed from a stamp this process
/// wrote is measuring the recorder's own scheduler, and a panel that averages
/// the two kinds together is measuring neither. The column exists so a query can
/// exclude one.
///
/// The tokens are hyphenated, as the design's DDL states them. The health tier's
/// Prometheus label values for the same distinction are underscored, because a
/// label value is read by a query language that treats a hyphen as an operator;
/// they are the same fact in two notations, and this is the only place both are
/// named.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum RecvTsKindLabel {
    #[serde(rename = "kernel-software")]
    KernelSoftware,
    #[serde(rename = "application-fallback")]
    ApplicationFallback,
}

impl From<RecvTsKind> for RecvTsKindLabel {
    fn from(kind: RecvTsKind) -> Self {
        match kind {
            RecvTsKind::KernelSoftware => Self::KernelSoftware,
            RecvTsKind::ApplicationFallback => Self::ApplicationFallback,
        }
    }
}

/// The scope an admitted capture loss may be subtracted at.
///
/// The field a dashboard is most likely to get wrong, which is why it travels on
/// every row that carries a number derived from a subtraction. A ring counts
/// frames dropped *before* demultiplexing, so at `capture-handle` scope the
/// number belongs to the handle and to no port role in particular, and
/// subtracting it per role credits one role with another's losses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DropScope {
    #[serde(rename = "port-role")]
    PortRole,
    #[serde(rename = "capture-handle")]
    CaptureHandle,
}

impl From<CaptureDropScope> for DropScope {
    fn from(scope: CaptureDropScope) -> Self {
        match scope {
            CaptureDropScope::PortRole => Self::PortRole,
            CaptureDropScope::CaptureHandle => Self::CaptureHandle,
        }
    }
}

impl DropScope {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PortRole => "port-role",
            Self::CaptureHandle => "capture-handle",
        }
    }
}

/// A port role, in the three spellings the specification mandates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PortRoleLabel {
    Mktdata,
    Refdata,
    Snapshot,
}

impl From<PortRole> for PortRoleLabel {
    fn from(role: PortRole) -> Self {
        match role {
            PortRole::Mktdata => Self::Mktdata,
            PortRole::Refdata => Self::Refdata,
            PortRole::Snapshot => Self::Snapshot,
        }
    }
}

/// Whose loss a gap is, and the order the five are tested in is the whole
/// design.
///
/// `recorder`, `upstream` and `path` are exculpatory: each says the loss was
/// ours, the network's upstream of the capture point, or covered by a redundant
/// instance, and each is decided from evidence one object holds.
/// [`Self::Publisher`] is the accusation, and it needs a datagram absent from
/// *every* site with no recorder overflow anywhere — a join this crate cannot
/// perform. Until that join has run the answer is [`Self::Unverifiable`], and
/// saying so is the point: a rule set that reports a violation where it merely
/// could not see is a rule set nobody trusts twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Verdict {
    /// Covered by our own admitted drops, at a scope where the subtraction is
    /// valid. A counter and an alert on us, never a publisher finding.
    Recorder,
    /// Not covered by ours, and interface drops rose over the window. A switch
    /// or link question.
    Upstream,
    /// Absent from this instance and present in a redundant instance on the
    /// same channel and port. The redundancy earned its cost.
    Path,
    /// The residue could not be computed, or the cross-site window that would
    /// decide `publisher` was not complete. Costs nothing, and saying so is the
    /// point.
    Unverifiable,
    /// Absent from every site, with no recorder overflow anywhere and coverage
    /// intact. Never written by this crate; see the type's own documentation.
    Publisher,
}

/// A conformance verdict, as the rule set states it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FindingVerdict {
    Pass,
    Violation,
    Unverifiable,
    /// The rule did not apply — chiefly a port role nothing joined. Reporting
    /// `pass` there is reporting a pass over a rule that never ran.
    Na,
}

/// One port role the recorder was asked to join, as the column store holds it.
///
/// A three-element tuple rather than a struct, because the column is
/// `Array(Tuple(String, IPv4, UInt16))` and an unnamed tuple is an array in
/// JSON. The intent is carried, not only the role: a port joined on the wrong
/// port is silent in exactly the way a port nobody joined is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleJoinRow(pub String, pub Ipv4Addr, pub u16);

/// One row per archived datagram: the base fact everything else is derived
/// from, and therefore the one row that must be exactly right.
///
/// **There is no `era_index` here, deliberately.** The design's own DDL listed
/// one and put it in the sort key, and its own principle — a row carries only
/// what its object states — forbids it: a stored rank is renumbered by any
/// later-arriving *earlier* object, which is what a backfill is, and renumbering
/// a column inside the sort key of the largest table is a rewrite of that table.
/// `reset_count` is the wire fact and `segment_seq` places the row in the
/// archive; the era is resolved by range join to [`Era`], where the openings are.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Datagram {
    pub recv_ts: Nanos,
    /// From the datagram header, as the publisher stated it. Never compared
    /// against `recv_ts` here: `send_recv_ms` is a materialised column, so the
    /// subtraction happens once, in the schema.
    pub send_ts: Nanos,
    pub recv_ts_kind: RecvTsKindLabel,

    pub source_addr: Ipv4Addr,
    pub channel_id: u8,
    pub dst_port: u16,

    pub feed: String,
    pub port_role: PortRoleLabel,
    pub group_addr: Ipv4Addr,

    pub sequence_number: u64,
    /// The wire value, as sent. Kept as a fact and never used as a key: it is a
    /// `u8` and it wraps, so two eras 256 resets apart share a value, and
    /// partitioning by it *hides* every gap between them.
    pub reset_count: u8,
    /// Monotonic per recorder run. A hole in it is a hole in the archive, which
    /// is what distinguishes a recorder that was down from a feed that was
    /// quiet.
    pub segment_seq: u64,

    /// What the archive holds.
    pub payload_len: u16,
    /// What was sent. Larger means the capture length cut it short — and an
    /// over-cap datagram is a publisher violation the archive keeps rather than
    /// a sequence gap somebody else is blamed for.
    pub wire_payload_len: u32,
    /// What the recorder lost between the previous datagram and this one, at
    /// the scope `drop_scope` declares.
    pub drop_delta: u32,

    pub site: String,
    pub recorder: String,
    pub env: String,
    pub drop_scope: DropScope,
    pub object_key: String,
    pub object_sha256: String,
}

/// Where one era opened, so that the monotonic index is a rank over openings.
///
/// One row per reset rather than one per datagram, and every field is observable
/// inside the object being loaded: a transition is a datagram whose `Reset
/// Count` differs from the previous datagram's *on the same channel instance,
/// within this object*. No cross-object cursor, no dependence on load order, and
/// re-running an object replaces its own rows and nothing else.
///
/// # `anchor_certain`, and why a continuation still writes a row
///
/// An object's *first* era for an instance may be a continuation of one that
/// opened in an earlier object, or a new era that happens to carry the same
/// `Reset Count`. The evidence is adjacency — the immediately preceding segment
/// and its last `Reset Count` for that instance — and under a staging budget
/// that evicts, the predecessor is routinely gone.
///
/// The loader never waits for it. It writes the row with `anchor_certain = 0`
/// immediately, and a later load with the predecessor in hand rewrites that one
/// row. The design's phrasing is that a settled continuation writes *no* row;
/// this writes one with `continuation = 1` instead, for two reasons. A row that
/// cannot be deleted cannot be corrected by omission, so a boundary first seen
/// uncertain could never afterwards be settled as a continuation. And the
/// absence of a row is indistinguishable from an object nobody loaded, while a
/// row that states *this boundary opens no era* is evidence. The rank that
/// `era_index` means is taken over `continuation = 0` rows, so the effect on
/// every query is identical.
///
/// `anchor_certain` is the `ReplacingMergeTree` version column, which is what
/// makes late evidence an upgrade and never a regression: a settled row always
/// wins over an unsettled one, whichever order the two loads happened in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Era {
    pub site: String,
    pub recorder: String,
    pub feed: String,
    pub source_addr: Ipv4Addr,
    pub channel_id: u8,
    pub dst_port: u16,
    /// Receive stamp of the era's first datagram in this object.
    pub anchor_ts: Nanos,
    /// Its sequence number.
    pub anchor_seq: u64,
    /// The wire value, as a fact.
    pub reset_count: u8,
    pub segment_seq: u64,
    /// 1 when the adjacency evidence was available, 0 when the preceding
    /// segment could not be consulted. A gap whose era carries 0 cannot be
    /// escalated past `unverifiable`.
    pub anchor_certain: u8,
    /// 1 when the evidence said this anchor continues the era the preceding
    /// segment ended in, so it opens no era. Always 0 for a transition observed
    /// inside this object.
    pub continuation: u8,
    pub object_key: String,
    pub object_sha256: String,
}

/// The manifest, as a table: one row per segment per channel instance.
///
/// Loaded without opening a single object, which is what makes a coverage
/// question cheap and a **missing object** visible — a hole in `segment_seq` for
/// a recorder run is a hole in the archive, and without it a recorder that was
/// down for an hour is indistinguishable from a feed that was quiet for an hour.
///
/// It is also where `roles_joined` lets a silent port report `na` instead of
/// `pass`: a port nobody joined produces no data, and no data looks exactly like
/// a clean feed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentCoverage {
    pub site: String,
    pub recorder: String,
    pub env: String,
    pub feed: String,
    pub source_addr: Ipv4Addr,
    pub channel_id: u8,
    pub dst_port: u16,
    pub segment_seq: u64,
    /// Receive timestamps, not send timestamps: this is the window the recorder
    /// can vouch for.
    pub start_ts: Nanos,
    pub end_ts: Nanos,
    /// In arrival order, not in value order: the segment is a time window, and
    /// what a cross-object join needs is the window's edges.
    pub first_seq: u64,
    pub last_seq: u64,
    pub datagram_count: u64,
    /// A set, and therefore silent about which member was last. That is the
    /// limit of the adjacency check this row can settle on its own; see
    /// [`Era`].
    pub reset_counts_seen: Vec<u8>,
    /// Cumulative and never reset. Only the delta between two of these rows
    /// says anything about a window.
    pub capture_drop_total: u64,
    pub interface_drop_total: u64,
    pub drop_scope: DropScope,
    pub roles_joined: Vec<RoleJoinRow>,
    pub object_key: String,
    pub object_sha256: String,
    pub build_version: String,
    pub build_commit: String,
    pub config_hash: String,
}

/// One row per contiguous run of missing sequence numbers, with a verdict.
///
/// The row a dashboard actually wants: derived, re-derivable, and the only place
/// attribution is decided.
///
/// **The measure is [`Self::missing_count`], which counts sequence values.** At
/// fifty datagrams a second a three-second gap is a hundred and fifty missing
/// and on a channel that only heartbeats it is three, so a figure in seconds
/// compares neither two channels nor two hours of one: it measures how busy the
/// feed was as much as what was lost. [`Self::before_ts`] and [`Self::after_ts`]
/// place the run against an incident and never quantify it.
///
/// **A gap can be partly ours.** Five missing with three admitted is neither
/// `recorder` nor `publisher`, which is why the verdict is decided on
/// [`Self::unexplained_count`] and not on the missing count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceGap {
    pub site: String,
    pub recorder: String,
    pub env: String,
    pub feed: String,
    pub port_role: PortRoleLabel,
    /// The multicast group. Carried because the consuming report keys on it and
    /// a gap row without it cannot be placed.
    pub group_addr: Ipv4Addr,
    pub source_addr: Ipv4Addr,
    pub channel_id: u8,
    pub dst_port: u16,
    /// The wire value at the time.
    pub reset_count: u8,
    /// The era's ordinal within this object, counting from 1 at the instance's
    /// first datagram in it. A gap never spans an era.
    ///
    /// This is not the globally dense rank, which is a rank over every [`Era`]
    /// row and therefore a property of the whole archive rather than of one
    /// object. [`Self::era_anchor_ts`] is the join key that reaches it, and a
    /// loader that stored the global rank here would be storing a number any
    /// later backfill renumbers.
    pub era_index: u32,
    /// The anchor of the era this run sits in, which is how the row joins to
    /// [`Era`] and thence to the global rank.
    pub era_anchor_ts: Nanos,
    /// Copied off the era, so a query deciding whether to escalate a verdict
    /// needs no join.
    pub anchor_certain: u8,
    pub missing_from: u64,
    pub missing_to: u64,
    pub missing_count: u64,
    /// What the missing count is a share of: the sequence numbers this site
    /// should have seen over the window. Without it there is no rate, and a
    /// bare count of missing datagrams says nothing about a feed's health.
    pub reference_seqs: u64,
    /// Placement, never the measure: the datagrams either side, locally.
    pub before_ts: Nanos,
    pub after_ts: Nanos,
    /// When the missing datagrams were actually sent, from a site that did
    /// record them. Absent here, because a site has no clock reading for a
    /// datagram it never received.
    pub sent_from_ts: Option<Nanos>,
    pub sent_to_ts: Option<Nanos>,
    /// Our own admitted drops on this instance over the window.
    pub admitted_recorder: u64,
    pub admitted_scope: DropScope,
    /// The missing count less what we admit, and absent when that subtraction
    /// is not valid at this scope. The verdict is decided on this residue.
    pub unexplained_count: Option<u64>,
    /// The *delta* over the window, upstream of the capture point. Absent when
    /// the preceding segment was not available to subtract from.
    pub interface_drops: Option<u64>,
    /// Present at another site. Absent until the cross-site join has run.
    pub seen_elsewhere: Option<u8>,
    /// Present in another instance on this channel and port. Absent when this
    /// channel and port carried no second source in this object, because then
    /// there is nothing to have looked in.
    pub on_redundant_path: Option<u8>,
    pub verdict: Verdict,
    /// Where the evidence is.
    pub object_key: String,
}

/// The rule set's verdicts, kept.
///
/// `rule_set_version` and `run_ts` are load-bearing rather than bookkeeping: a
/// rule added next month runs against last month's traffic, so one window
/// legally holds two verdicts from two versions, and a dashboard that cannot say
/// which version produced a verdict cannot show that the rule set improved.
///
/// Nothing in this crate produces one. The table is written here as the shape a
/// runner fills; the runner over replay is the other half of the design's plan 3.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConformanceFinding {
    /// When the rule set ran, not when the traffic passed.
    pub run_ts: Nanos,
    pub rule_id: String,
    pub rule_set_version: String,
    pub site: String,
    pub recorder: String,
    pub env: String,
    pub feed: String,
    pub port_role: PortRoleLabel,
    pub source_addr: Ipv4Addr,
    pub channel_id: u8,
    pub dst_port: u16,
    pub window_start: Nanos,
    pub window_end: Nanos,
    pub verdict: FindingVerdict,
    pub detail: String,
    pub object_key: String,
    /// The evidence range.
    pub first_seq: u64,
    pub last_seq: u64,
}

/// The message a market data row was decoded from, as the column holds it.
///
/// Spelled as the codec spells the type, so that a row and a specification can
/// be read against each other without a translation table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum MessageTypeLabel {
    Quote,
    Trade,
    LevelUpdate,
    BookClear,
    InstrumentReset,
    SnapshotBegin,
    SnapshotLevel,
    SnapshotEnd,
}

impl std::fmt::Display for MessageTypeLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Quote => "Quote",
            Self::Trade => "Trade",
            Self::LevelUpdate => "LevelUpdate",
            Self::BookClear => "BookClear",
            Self::InstrumentReset => "InstrumentReset",
            Self::SnapshotBegin => "SnapshotBegin",
            Self::SnapshotLevel => "SnapshotLevel",
            Self::SnapshotEnd => "SnapshotEnd",
        })
    }
}

/// The depth feed's absent-value sentinel, as a `Nullable` column holds it.
///
/// `order_count` and `level_index` carry `0xFFFF` when the venue exposes
/// neither, and the specification is explicit that the value is not a count and
/// not a rank. Written through, it becomes an instrument with sixty-five
/// thousand orders at a level — which is not a subtle wrongness, but it is a
/// silent one, and it survives every average taken over it.
///
/// Top of book says *unavailable* with zero instead, which is the opposite
/// answer to the same question from the other specification. So this translation
/// belongs to the depth fields alone and is deliberately not a blanket rule.
#[must_use]
pub fn absent_if_sentinel(value: u16) -> Option<u16> {
    const U16_UNAVAILABLE: u16 = 0xFFFF;
    (value != U16_UNAVAILABLE).then_some(value)
}

/// One decoded message.
///
/// The expensive table, and the only one whose row count is not a function of
/// the datagram count: a datagram carries as many messages as the publisher
/// packed into it, so a burst batched into one datagram is one transport row and
/// hundreds of these.
///
/// **One table with nullable per-type columns rather than one table per message
/// type.** The types share every column above `message_type` and differ in at
/// most six, and the dominant question — everything that happened to this
/// instrument over this window — is a union across eight tables otherwise, one
/// that has to be rewritten whenever the family gains a message.
///
/// **Prices and quantities stay raw, with their exponents beside them.** A
/// decimal computed at load time bakes in a scale a later era can change and
/// loses the integer the wire carried, which is the only value a conformance
/// question can be asked against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub recv_ts: Nanos,
    pub send_ts: Nanos,
    /// The venue's own event time, where the message carries one.
    ///
    /// Never part of an equivalence key: its resolution and its meaning differ
    /// between transports, so one book state carried over two of them would hash
    /// two ways and no pair would ever be found.
    pub upstream_ts: Option<Nanos>,
    pub recv_ts_kind: RecvTsKindLabel,

    pub site: String,
    pub recorder: String,
    pub env: String,
    pub feed: String,
    pub port_role: PortRoleLabel,
    pub source_addr: Ipv4Addr,
    pub channel_id: u8,
    pub dst_port: u16,

    pub sequence_number: u64,
    /// The wire value, as sent. A fact and never a key: it is a `u8` and it
    /// wraps, so two eras 256 resets apart share a value.
    pub reset_count: u8,
    /// Monotonic per recorder run, and what places this row in the archive.
    ///
    /// The era is **not** a column here, for the reason `datagram` has none: an
    /// era's anchor is only observable as *the first datagram of that era in
    /// this object*, so a stored anchor differs between two objects of one era
    /// and would split that era across sort-key prefixes. `reset_count` is the
    /// wire fact, `segment_seq` places the row, and the era is resolved by range
    /// join to `era` — where the openings, and their certainty, already are.
    pub segment_seq: u64,
    /// Position of the message inside its datagram.
    ///
    /// In the sort key, because a publisher may pack several messages for one
    /// instrument into one datagram: they share a sequence number and a receive
    /// stamp, and without this a `ReplacingMergeTree` collapses a run of genuine
    /// events into whichever one merged last.
    pub message_index: u8,

    /// From the message where it carries one, and from era-qualified reference
    /// data where it does not. Never invented, and never carried over from an
    /// adjacent message of another type.
    pub source_id: u16,
    pub instrument_id: u32,
    /// Display and filtering only, resolved at this era. Nothing joins on it.
    pub symbol: String,
    pub price_exp: i8,
    pub qty_exp: i8,
    pub per_instrument_seq: Option<u32>,

    pub message_type: MessageTypeLabel,
    pub side_raw: Option<u8>,
    pub action_raw: Option<u8>,
    pub reason_raw: Option<u8>,
    pub flags_raw: Option<u8>,
    pub price_raw: Option<i64>,
    pub qty_raw: Option<u64>,
    /// `NULL` where the wire said `0xFFFF`. See [`absent_if_sentinel`].
    pub order_count: Option<u16>,
    /// `NULL` where the wire said `0xFFFF`, and derived rather than read on a
    /// snapshot level, which carries no such field at all.
    pub level_index: Option<u16>,

    pub bid_px_raw: Option<i64>,
    pub bid_qty_raw: Option<u64>,
    pub bid_source_count: Option<u16>,
    pub ask_px_raw: Option<i64>,
    pub ask_qty_raw: Option<u64>,
    pub ask_source_count: Option<u16>,

    pub trade_id: Option<u64>,
    pub cumulative_volume: Option<u64>,

    pub snapshot_id: Option<u32>,
    /// On a `SnapshotBegin` and a `SnapshotEnd`, the sequence number the book is
    /// true as of. On an `InstrumentReset`, `new_anchor_seq` — the terms of its
    /// own recovery, and the reason a deriver that drops this field is unsafe
    /// rather than lossy: without it, a snapshot already in flight when the
    /// reset was published is accepted, and a book the publisher had disowned is
    /// rebuilt from as certain.
    pub anchor_seq: Option<u64>,
    pub total_levels: Option<u32>,
    /// How many levels the cycle actually carried, on its `SnapshotEnd`.
    ///
    /// Against `total_levels` on the begin row, this answers *was the snapshot
    /// complete* from rows alone — which is what makes persisting every level
    /// optional rather than the only way to ask.
    pub levels_seen: Option<u32>,
    pub depth_bound: Option<u32>,

    pub object_key: String,
    pub object_sha256: String,
    pub datagram_index: u64,
}

/// One instrument, as one definition stated it, in force from a sequence number.
///
/// Era-qualified, and it carries the whole identity block including the channel
/// instance. An `era_anchor_ts` is only meaningful for one instance, because a
/// `Reset Count` is that instance's: two paths publishing one `Channel ID` open
/// their eras independently, so a key without the address and the port merges
/// two eras that are not the same era and lets one path's exponents decode the
/// other path's prices.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Instrument {
    pub site: String,
    pub recorder: String,
    pub env: String,
    pub feed: String,
    /// Reference data arrives on the `refdata` role. It is on the row so that a
    /// reader joining from a `mktdata` event can see that the roles differ
    /// rather than discover it.
    pub port_role: PortRoleLabel,
    pub source_addr: Ipv4Addr,
    pub channel_id: u8,
    pub dst_port: u16,
    pub source_id: u16,
    pub instrument_id: u32,
    /// The sequence number this statement came into force at.
    ///
    /// A stable era-scoped identity where an anchor timestamp is not: it is the
    /// position of the definition that made the statement, identical in every
    /// object that carries it, so two loads of one era replace each other
    /// instead of accumulating.
    pub from_sequence: u64,
    pub reset_count: u8,
    pub symbol: String,
    pub price_exp: i8,
    pub qty_exp: i8,
    pub contract_value: u64,
    pub first_seen_ts: Nanos,
    pub last_seen_ts: Nanos,
    pub manifest_seq: Option<u16>,
    /// What a valid `ManifestSummary` said the published set held.
    ///
    /// Absent rather than zero while a summary is not valid yet: a zero here
    /// reads as a feed publishing nothing. Against the count of distinct
    /// instruments observed, it is the only statement of published-set coverage
    /// an archive can make.
    pub declared_count: Option<u32>,
    pub object_key: String,
}

/// One change in an instrument's top of book.
///
/// A change is a change in **either** the visible top **or** the certainty of
/// it. Emitting only on price movement loses the transition that matters most: a
/// gap arrives, nothing later happens to move the top, and every lookup from
/// then on returns a row saying the book is certain, which is now false.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookTop {
    pub recv_ts: Nanos,
    pub send_ts: Nanos,
    pub site: String,
    pub recorder: String,
    pub env: String,
    pub feed: String,
    /// Where this view of the book came from, as `site` names a recorder.
    ///
    /// Two recorders of one multicast feed are two observations; a multicast
    /// feed and some other transport carrying the same instruments are two
    /// observations. Nothing in the schema knows which is which, and nothing
    /// should — a race is one `state_key` seen at more than one of these.
    pub observation: String,
    pub source_addr: Ipv4Addr,
    pub channel_id: u8,
    pub dst_port: u16,
    pub source_id: u16,
    pub instrument_id: u32,
    pub symbol: String,
    pub sequence_number: u64,
    pub message_index: u8,
    pub reset_count: u8,
    pub segment_seq: u64,
    pub bid_px_raw: Option<i64>,
    pub bid_qty_raw: Option<u64>,
    pub bid_source_count: Option<u16>,
    pub ask_px_raw: Option<i64>,
    pub ask_qty_raw: Option<u64>,
    pub ask_source_count: Option<u16>,
    pub price_exp: i8,
    pub qty_exp: i8,
    /// The equivalence key: a hash over the instrument and both sides, and over
    /// nothing else. No timestamp, no sequence number, no bytes.
    pub state_key: u64,
    /// 0 once the book is unknowable.
    pub book_certain: u8,
    pub uncertain_since: Option<u64>,
    pub uncertain_reason: UncertainReason,
    pub object_key: String,
}

/// Why a top of book cannot be believed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum UncertainReason {
    /// Certain. The column is not nullable because a reason of *none* is a
    /// reading a query can filter on, where a NULL invites a join that drops the
    /// row.
    #[serde(rename = "none")]
    None,
    /// A run of sequence values nobody delivered, between two deltas.
    #[serde(rename = "gap")]
    Gap,
    /// The publisher disowned its own book for this instrument.
    #[serde(rename = "instrument_reset")]
    InstrumentReset,
    /// A delta book with no complete snapshot cycle yet.
    ///
    /// Emitted once, with no prices, rather than represented by absence:
    /// absence cannot be told from a silent feed, and a lookup into an
    /// unanchored window would return whatever preceded it — possibly from
    /// another era.
    #[serde(rename = "no_anchor")]
    NoAnchor,
}

/// One table's worth of rows, named so that a sink can label a counter and a
/// batch can name the destination it failed to reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Grain {
    Datagram,
    Era,
    SegmentCoverage,
    SequenceGap,
    ConformanceFinding,
    Event,
    Instrument,
    BookTop,
}

impl Grain {
    pub const ALL: [Self; 8] = [
        Self::Datagram,
        Self::Era,
        Self::SegmentCoverage,
        Self::SequenceGap,
        Self::ConformanceFinding,
        Self::Event,
        Self::Instrument,
        Self::BookTop,
    ];
    pub const COUNT: usize = Self::ALL.len();

    /// The table name, which is also the metric label and the `FileSink` file
    /// name. One spelling, so a counter and a `CREATE TABLE` cannot disagree.
    #[must_use]
    pub const fn table(self) -> &'static str {
        match self {
            Self::Datagram => "datagram",
            Self::Era => "era",
            Self::SegmentCoverage => "segment_coverage",
            Self::SequenceGap => "sequence_gap",
            Self::ConformanceFinding => "conformance_finding",
            Self::Event => "event",
            Self::Instrument => "instrument",
            Self::BookTop => "book_top",
        }
    }

    pub(crate) const fn index(self) -> usize {
        match self {
            Self::Datagram => 0,
            Self::Era => 1,
            Self::SegmentCoverage => 2,
            Self::SequenceGap => 3,
            Self::ConformanceFinding => 4,
            Self::Event => 5,
            Self::Instrument => 6,
            Self::BookTop => 7,
        }
    }
}

impl std::fmt::Display for Grain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.table())
    }
}

/// Everything one object derived into, as one unit.
///
/// The object it came from is named on the batch as well as on every row: a sink
/// that has to reject the batch reports which object was not loaded, and a
/// loader that treats the object as unloaded is what keeps a half-landed object
/// from reading as a clean feed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowBatch {
    pub object_key: String,
    pub object_sha256: String,
    pub datagram: Vec<Datagram>,
    pub era: Vec<Era>,
    pub segment_coverage: Vec<SegmentCoverage>,
    pub sequence_gap: Vec<SequenceGap>,
    pub conformance_finding: Vec<ConformanceFinding>,
    pub event: Vec<Event>,
    pub instrument: Vec<Instrument>,
    pub book_top: Vec<BookTop>,
}

impl RowBatch {
    #[must_use]
    pub fn rows(&self, grain: Grain) -> usize {
        match grain {
            Grain::Datagram => self.datagram.len(),
            Grain::Era => self.era.len(),
            Grain::SegmentCoverage => self.segment_coverage.len(),
            Grain::SequenceGap => self.sequence_gap.len(),
            Grain::ConformanceFinding => self.conformance_finding.len(),
            Grain::Event => self.event.len(),
            Grain::Instrument => self.instrument.len(),
            Grain::BookTop => self.book_top.len(),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        Grain::ALL.iter().map(|g| self.rows(*g)).sum()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
