//! Drives a [`Source`] to exhaustion and says, per channel instance, which
//! sequence numbers nobody delivered and whose they are.
//!
//! Continuity, reordering, duplication and the era ordinal are decided by
//! [`SequenceTracker`], which is the live health tier's tracker: one
//! implementation in `dz-recorder-core`, driven here in receive order. A
//! dashboard whose live panel and historical panel disagree about the same feed
//! teaches nobody anything, and two copies of that rule set is how they come to
//! disagree.
//!
//! What is this crate's own is the per-era delivered ranges below. The tracker
//! says what each datagram meant at the moment it arrived, which is all a live
//! tier can say; these ranges say which sequence values the archive does not
//! hold once every late arrival is in. They differ by exactly the reordering.
//!
//! Nothing here calls `DatagramHeader::decode`. `decode` refuses an unsupported
//! schema version and an out-of-range declared length, which is correct for a
//! subscriber and wrong for anything counting loss: the datagram a decoder would
//! refuse still carries the sequence number whose absence is the finding, so
//! refusing it manufactures a gap out of a datagram we hold.

use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use dz_edge_core::{DatagramHeader, PortRole};
use dz_recorder_core::{
    CaptureDropScope, ChannelInstance, RecordedDatagram, SequenceOutcome, SequenceTracker, Source,
    SourceError, MAX_FORWARD_JUMP,
};
use thiserror::Error;

use crate::run::SequenceRun;

/// What is left of an instance's missing count once the recorder's own admitted
/// loss is taken off it. This, and not the missing count, is what a dashboard
/// shows: comparing recording nodes catches a loss one node has alone and
/// cannot catch one they all share.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unexplained {
    /// The residue: the missing count less what the recorder admits, at a scope
    /// where that subtraction is valid.
    Count(u64),
    /// No per-instance subtraction is meaningful, so there is no residue to
    /// report: the scope is the capture handle and the handle admitted
    /// something over the window. At that scope the archive can only exonerate
    /// itself, and only when its own total is zero — a recorder that dropped
    /// anything cannot say which role lost it, and must not guess.
    ///
    /// Precision we do not have is worse than scope we declare.
    Unverifiable,
}

/// A [`Source`] failed before it was exhausted.
///
/// Returned rather than counted, because a short replay read as a complete
/// window is a sequence gap with nothing admitted behind it, and a gap with
/// nothing admitted behind it is a publisher finding drawn from our own
/// truncation.
#[derive(Debug, Error)]
pub enum LossError {
    #[error("the source failed before it was exhausted, so no window here is complete: {0}")]
    Source(#[from] SourceError),
}

/// One era of one channel instance: the span of sequence numbers it covered and
/// how much of that span was delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EraCoverage {
    /// The monotonic ordinal this deriver assigned, counting from 1.
    pub ordinal: u64,
    /// The wire `Reset Count`, kept as a fact and never used as a key.
    pub reset_count: u8,
    /// The sequence number of the datagram that *opened* this era.
    ///
    /// Not [`first_seq`](Self::first_seq), and the difference is the whole
    /// reason it is carried: a datagram below the opening value arriving
    /// afterwards — a reordering, or backward motion the archive holds anyway —
    /// lowers the delivered span without moving the opening. The era's anchor is
    /// what an offline tier records as *where this era began*, and a rank over
    /// those openings is what identifies an era; deriving it from the span would
    /// make an era's identity depend on a datagram that arrived later.
    pub anchor_seq: u64,
    /// Receive stamp of that same datagram, which is what a range join from a
    /// per-datagram row resolves an era by.
    pub anchor_ts_ns: u64,
    /// Lowest sequence number delivered in this era.
    pub first_seq: u64,
    /// Highest sequence number delivered in this era.
    pub last_seq: u64,
    /// Distinct sequence values delivered.
    pub delivered: u64,
}

impl EraCoverage {
    /// The sequence numbers this era should have carried.
    #[must_use]
    pub const fn reference_seqs(&self) -> u64 {
        self.last_seq
            .saturating_sub(self.first_seq)
            .saturating_add(1)
    }

    /// Sequence values in this era's span that nobody delivered.
    #[must_use]
    pub const fn missing(&self) -> u64 {
        self.reference_seqs().saturating_sub(self.delivered)
    }
}

/// Everything derivable about one channel instance's sequence space over the
/// window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceLoss {
    /// `(source address, Channel ID, destination port)`, never abbreviated.
    pub instance: ChannelInstance,
    /// The multicast group, which the consuming report keys on.
    pub group: Ipv4Addr,
    pub role: PortRole,
    /// Datagrams whose header was readable.
    pub datagrams: u64,
    /// One per contiguous run of missing sequence numbers, which is what an
    /// episode is.
    pub runs: Vec<SequenceRun>,
    /// Sequence values in the window that nobody delivered: the sum of
    /// [`runs`](Self::runs). This is the row's missing count.
    pub missing: u64,
    /// What [`missing`](Self::missing) is a share of: the sequence numbers this
    /// window should have carried, summed over the instance's eras. Without it
    /// there is no rate, and a bare count of missing datagrams says nothing
    /// about a feed's health.
    ///
    /// Bounded by what was delivered at each end, so loss before the first
    /// datagram or after the last is outside it. That is the boundary case the
    /// design reports as `unverifiable` rather than as a gap.
    pub reference_seqs: u64,
    /// The recorder's own admitted loss on this instance: the sum of
    /// `drop_delta`. Valid to subtract only at
    /// [`CaptureDropScope::PortRole`] — see [`LossReport::unexplained`].
    pub admitted: u64,
    /// Forward discontinuities as they arrived, before any late arrival was
    /// taken off them. This is what a live tracker counts, and it is an upper
    /// bound: a reordered datagram fills a value already counted here.
    ///
    /// Kept beside [`runs`](Self::runs) rather than instead of it because the
    /// two answer different questions. `runs` says what the archive does not
    /// hold; this says what looked absent at the moment it looked absent, which
    /// is the only thing a live tier can say. They differ by exactly the
    /// reordering, and a consumer that mixed them would compare a live panel
    /// against a historical one and find a disagreement that is not there.
    pub gaps_on_arrival: u64,
    /// Sequence values skipped by [`gaps_on_arrival`](Self::gaps_on_arrival).
    pub missing_on_arrival: u64,
    /// Sequence numbers seen twice within the reordering window.
    pub duplicates: u64,
    /// Datagrams that arrived after a higher sequence number but inside the
    /// reordering window.
    pub reordered: u64,
    /// Backward motion beyond the reordering window with no reset behind it: a
    /// publisher that restarted its sequence space without advancing
    /// `Reset Count`. Its own finding, and nothing else would notice it.
    pub backward: u64,
    /// Datagrams whose sequence number was too far ahead to be loss. Nothing
    /// is credited to `missing_on_arrival` for one of these and the tracker
    /// does not adopt the number, so the instance's accounting survives it. The
    /// live tier counts the same thing under
    /// `dz_recorder_forward_jump_total`, and the two agreeing is what the
    /// agreement test is for.
    pub forward_jumps: u64,
    /// `Reset Count` transitions.
    pub resets: u64,
    /// Era boundaries crossed, including the era the first datagram opened.
    /// Always `resets + 1`.
    pub era_transitions: u64,
    /// Datagrams carrying a schema version this build does not implement. They
    /// are still counted in the sequence accounting above: a subscriber must
    /// discard one, and anything measuring loss must not, or the sequence
    /// number it carries becomes a gap we invented.
    pub unknown_schema: u64,
    /// Datagrams the network held back across a reset and delivered into the
    /// era they came from, rather than opening one for them.
    pub stragglers: u64,
    /// Datagrams whose destination group was not the one this instance's row is
    /// labelled with. `ChannelInstance` carries no group, so two groups on one
    /// port key here together; a non-zero count is a row describing two.
    pub group_mismatches: u64,
    /// Eras this instance was refused past its bound.
    pub eras_refused: u64,
    /// Delivered ranges this instance was refused past its bound. A non-zero
    /// count means the runs below are an under-count.
    pub ranges_refused: u64,
    /// Datagrams whose sequence number was further from their era's span than
    /// any outage explains, and which were therefore not recorded as delivered.
    ///
    /// The forward case has its own name — see
    /// [`forward_jumps`](Self::forward_jumps) — because the tracker can see it
    /// coming from an established position. This is the rest: chiefly a value
    /// that *opened* an era, which is adopted with nothing to compare it
    /// against, leaving every ordinary datagram after it to fall behind by the
    /// whole distance.
    pub implausible_deliveries: u64,
    /// One per era, in the order the eras opened.
    pub eras: Vec<EraCoverage>,
}

/// What one window of one source produced.
#[derive(Debug, Clone)]
pub struct LossReport {
    scope: CaptureDropScope,
    instances: BTreeMap<ChannelInstance, InstanceLoss>,
    datagrams: u64,
    short_datagrams: u64,
    instances_refused: u64,
    handle_admitted: u64,
    role_admitted: [u64; 3],
}

impl LossReport {
    /// Datagrams from a channel instance the window would not admit, because
    /// its instance bound was reached. Counted rather than silent: a window
    /// that quietly stopped tracking would report a live feed as absent.
    #[must_use]
    pub const fn instances_refused(&self) -> u64 {
        self.instances_refused
    }

    /// The scope the admitted losses were declared at.
    #[must_use]
    pub const fn scope(&self) -> CaptureDropScope {
        self.scope
    }

    /// Datagrams the source produced, readable header or not.
    #[must_use]
    pub const fn datagrams(&self) -> u64 {
        self.datagrams
    }

    /// Datagrams too short to hold a 24-byte header.
    ///
    /// Counted and never silently skipped: a datagram whose header cannot be
    /// read is attributable to no channel instance, and a window that dropped
    /// it on the floor would under-report the traffic it claims to describe.
    #[must_use]
    pub const fn short_datagrams(&self) -> u64 {
        self.short_datagrams
    }

    /// Everything the capture handle admitted losing over the window, across
    /// every role and instance. This is the total that decides whether a
    /// capture-handle-scoped archive can exonerate itself.
    #[must_use]
    pub const fn handle_admitted(&self) -> u64 {
        self.handle_admitted
    }

    /// What the handle admitted on one port role.
    ///
    /// This is the accumulator's own grain in socket mode, where there is one
    /// per role because there is one socket per role.
    #[must_use]
    pub const fn admitted_on_role(&self, role: PortRole) -> u64 {
        self.role_admitted[role_index(role)]
    }

    /// How many instances the window saw on one port role.
    ///
    /// The role's accumulator is shared between them, which is what decides
    /// whether its total can be attributed to any one of them.
    #[must_use]
    pub fn instances_on_role(&self, role: PortRole) -> usize {
        self.instances
            .values()
            .filter(|loss| loss.role == role)
            .count()
    }

    #[must_use]
    pub fn instance(&self, key: &ChannelInstance) -> Option<&InstanceLoss> {
        self.instances.get(key)
    }

    /// Every instance, ordered by the instance key.
    pub fn instances(&self) -> impl ExactSizeIterator<Item = &InstanceLoss> {
        self.instances.values()
    }

    /// Every run on every instance, ordered by instance then by sequence.
    pub fn runs(&self) -> impl Iterator<Item = &SequenceRun> {
        self.instances.values().flat_map(|loss| loss.runs.iter())
    }

    /// The residue a dashboard shows: this instance's missing count less what
    /// the recorder admits, at the scope the archive declared.
    ///
    /// `None` for an instance the window holds nothing about.
    ///
    /// The two scopes take different arithmetic and not the same arithmetic at
    /// two grains. At [`CaptureDropScope::PortRole`] the per-instance sum is a
    /// valid subtraction. At [`CaptureDropScope::CaptureHandle`] it is
    /// meaningless, so the answer is [`Unexplained::Unverifiable`] whenever the
    /// handle admitted anything at all over the window — and the common, interesting case is a
    /// handle that admitted nothing, which turns every gap into someone else's
    /// with evidence rather than by inference.
    #[must_use]
    pub fn unexplained(&self, key: &ChannelInstance) -> Option<Unexplained> {
        let loss = self.instances.get(key)?;
        Some(match self.scope {
            // Per instance only when the instance is the only one on its role.
            // The accumulator is the socket, not the channel: a role's socket
            // carries every instance on that group and port, and its delta
            // rides on whichever datagram next gets through — from any of them.
            // Subtracting one instance's share therefore exonerates whichever
            // instance happened to arrive next and charges the loss to the one
            // that did not, which manufactures exactly the false publisher
            // finding this crate exists to prevent. Two publishers on a group,
            // or two Channel IDs on a port, are enough.
            CaptureDropScope::PortRole
                if self.instances_on_role(loss.role) == 1
                    || self.role_admitted[role_index(loss.role)] == 0 =>
            {
                Unexplained::Count(loss.missing.saturating_sub(loss.admitted))
            }
            CaptureDropScope::PortRole => Unexplained::Unverifiable,
            CaptureDropScope::CaptureHandle if self.handle_admitted == 0 => {
                Unexplained::Count(loss.missing)
            }
            CaptureDropScope::CaptureHandle => Unexplained::Unverifiable,
        })
    }
}

/// Sequence loss over any [`Source`]: a live capture, or a replayed archive.
#[derive(Debug, Clone)]
pub struct LossDeriver {
    scope: CaptureDropScope,
    limits: DeriverLimits,
    instances: BTreeMap<ChannelInstance, InstanceState>,
    datagrams: u64,
    short_datagrams: u64,
    instances_refused: u64,
    handle_admitted: u64,
    role_admitted: [u64; 3],
}

/// What one window may hold, on keys the wire controls.
///
/// `observe` is public so this runs beside a live capture, which puts it on the
/// same footing as the health tier: an any-source join accepts datagrams from
/// any sender, so the key space is not ours to trust, and neither is the number
/// of eras a sender can open by alternating its `Reset Count`. The health tier
/// bounds the same space with `InstanceLimits` and a counted refusal; these are
/// the equivalents, and they are the crate's rather than a configuration's for
/// the same reason: no key may raise a bound the process's own memory rests on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeriverLimits {
    /// Channel instances one window may track.
    pub max_instances: usize,
    /// Eras one instance may hold. A `Reset Count` is a `u8` a sender writes.
    pub max_eras_per_instance: usize,
    /// Disjoint delivered ranges one era may hold. A stream whose sequence
    /// numbers descend inserts a new range per datagram, and every insertion is
    /// at the front of the vector: unbounded, that is quadratic work on a
    /// thread that may be a live one.
    pub max_ranges_per_era: usize,
}

impl Default for DeriverLimits {
    fn default() -> Self {
        Self {
            max_instances: 4096,
            max_eras_per_instance: 256,
            max_ranges_per_era: 4096,
        }
    }
}

impl LossDeriver {
    /// The scope is a parameter because the archive declares it and a guess
    /// here is a false publisher finding there. It need not be guessed over an
    /// archive: `ArchiveSource::capture_drop_scope` hands back what the section
    /// declared, and `None` for a capture that declared nothing — and `None` is
    /// not a scope to derive under.
    #[must_use]
    pub fn new(scope: CaptureDropScope) -> Self {
        Self::with_limits(scope, DeriverLimits::default())
    }

    /// The same, with bounds a caller can tighten. They cannot be loosened past
    /// what the process can hold, which is why the default is the crate's.
    #[must_use]
    pub fn with_limits(scope: CaptureDropScope, limits: DeriverLimits) -> Self {
        Self {
            scope,
            limits,
            instances: BTreeMap::new(),
            datagrams: 0,
            short_datagrams: 0,
            instances_refused: 0,
            handle_admitted: 0,
            role_admitted: [0; 3],
        }
    }

    /// Reads `source` to exhaustion.
    ///
    /// # Errors
    ///
    /// [`LossError::Source`] if the source failed before EOF. The datagrams
    /// already folded in are kept, but the window is not complete and nothing
    /// derived from it may be reported as one.
    pub fn drive<S: Source + ?Sized>(&mut self, source: &mut S) -> Result<(), LossError> {
        while let Some(dg) = source.next()? {
            self.observe(&dg);
        }
        Ok(())
    }

    /// Folds one datagram in, in receive order.
    ///
    /// Public so that this runs beside a live capture as well as over an
    /// archive: the `Source` symmetry is what makes the two halves comparable
    /// at all.
    pub fn observe(&mut self, dg: &RecordedDatagram<'_>) {
        self.datagrams += 1;
        // The handle's loss is the handle's whatever we can read of the
        // datagram that carried the delta, so this is counted before the header
        // is looked at.
        self.handle_admitted += u64::from(dg.drop_delta);
        self.role_admitted[role_index(dg.role)] += u64::from(dg.drop_delta);

        let Ok(header) = DatagramHeader::peek(dg.payload) else {
            self.short_datagrams += 1;
            return;
        };

        // A source address never seen before opens a series silently: no gap,
        // no loss. A tunnel address is a lease and a reassignment must not
        // become a finding.
        let key = ChannelInstance::new(*dg.src.ip(), header.channel_id, dg.dst.port());
        let limits = self.limits;
        // Refused rather than admitted past the bound, and counted so the
        // refusal is visible: a window that quietly stopped tracking instances
        // would report a feed as silent.
        if !self.instances.contains_key(&key) && self.instances.len() >= limits.max_instances {
            self.instances_refused += 1;
            return;
        }
        self.instances
            .entry(key)
            .or_insert_with(|| InstanceState::new(*dg.dst.ip(), dg.role))
            .observe(&header, dg, limits);
    }

    /// Closes the window and derives the runs.
    #[must_use]
    pub fn finish(self) -> LossReport {
        let instances = self
            .instances
            .into_iter()
            .map(|(key, state)| (key, state.finish(key)))
            .collect();
        LossReport {
            scope: self.scope,
            instances,
            datagrams: self.datagrams,
            short_datagrams: self.short_datagrams,
            instances_refused: self.instances_refused,
            handle_admitted: self.handle_admitted,
            role_admitted: self.role_admitted,
        }
    }
}

/// The slot a port role's admitted total occupies.
const fn role_index(role: PortRole) -> usize {
    match role {
        PortRole::Mktdata => 0,
        PortRole::Refdata => 1,
        PortRole::Snapshot => 2,
    }
}

/// A contiguous run of sequence numbers that were delivered, with the arrival
/// stamp of the datagram at each end.
///
/// Ranges rather than a set of sequence numbers because a clean window is one
/// range however long it is, and because the runs of *missing* values fall out
/// as the spaces between adjacent ranges. The stamps are held here so a run can
/// carry the arrival of the datagrams either side of it without a second pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Delivered {
    start: u64,
    end: u64,
    start_ts_ns: u64,
    end_ts_ns: u64,
}

impl Delivered {
    const fn single(seq: u64, ts_ns: u64) -> Self {
        Self {
            start: seq,
            end: seq,
            start_ts_ns: ts_ns,
            end_ts_ns: ts_ns,
        }
    }

    /// Saturating, because `start` and `end` are wire values: the tracker
    /// bounds how far a sequence number may jump, but a range's own width must
    /// not be able to overflow even if that bound is ever relaxed.
    const fn width(&self) -> u64 {
        self.end.saturating_sub(self.start).saturating_add(1)
    }
}

/// What became of one attempt to record a sequence value as delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Delivery {
    Recorded,
    /// Past [`DeriverLimits::max_ranges_per_era`]: the runs are an under-count
    /// from here on, and the instance says so.
    TooManyRanges,
    /// Further from this era's span than any outage explains. Recording it
    /// would make the era span the distance and report it as loss.
    TooDistant,
}

/// One era's sequence space. Kept per era, never merged across one: a reset
/// opens a new space, so a comparison across the boundary is an artefact.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Era {
    ordinal: u64,
    reset_count: u8,
    /// The datagram that opened the era, kept apart from the ranges below
    /// because a later arrival at a lower sequence number widens those and must
    /// not move this.
    anchor_seq: u64,
    anchor_ts_ns: u64,
    /// Disjoint and non-adjacent, ascending.
    delivered: Vec<Delivered>,
}

impl Era {
    fn opened(ordinal: u64, reset_count: u8, seq: u64, ts_ns: u64) -> Self {
        Self {
            ordinal,
            reset_count,
            anchor_seq: seq,
            anchor_ts_ns: ts_ns,
            delivered: vec![Delivered::single(seq, ts_ns)],
        }
    }

    /// The index of the first range that reaches `seq - 1` or beyond, which is
    /// the only range `seq` can be inside of, adjacent to on the left, or
    /// inserted before.
    fn slot(&self, seq: u64) -> usize {
        // saturating_add, because `range.end` comes off the wire: at u64::MAX
        // the unchecked form panics in a debug build and wraps to 0 in a
        // release one, where it silently reorders the range set.
        self.delivered
            .partition_point(|range| range.end.saturating_add(1) < seq)
    }

    /// Records `seq` as delivered, or says why it was not.
    fn deliver(&mut self, seq: u64, ts_ns: u64, limits: DeriverLimits) -> Delivery {
        // The bound on how far apart two values in one era may be, and the
        // reason it is here rather than only in the tracker. The tracker bounds
        // a forward *jump* from an established position — but the value that
        // opens an era is adopted with nothing to compare it against, and a
        // datagram arriving afterwards at an ordinary number then falls behind
        // it. That is backward motion, which credits no gap and moves no
        // counter, and the era it lands in nonetheless spans the distance
        // between the two: one run of some 1.8e19 missing values, from one
        // datagram, with `forward_jumps` still at zero. The rows design expands
        // a run with `arrayJoin(range(...))`, so the fabricated finding is one a
        // loader would try to materialise.
        //
        // Bounding the span covers both directions at once, and it is the
        // quantity that actually matters: an era wider than any outage explains
        // is not an era.
        if let (Some(first), Some(last)) = (self.delivered.first(), self.delivered.last()) {
            let low = first.start.min(seq);
            let high = last.end.max(seq);
            if high.saturating_sub(low) > MAX_FORWARD_JUMP {
                return Delivery::TooDistant;
            }
        }
        let index = self.slot(seq);
        let Some(&range) = self.delivered.get(index) else {
            // Extending the set, which is the case the bound governs. Appending
            // at the end is cheap; the refusal exists for the stream that
            // inserts at the front every time.
            if self.delivered.len() >= limits.max_ranges_per_era {
                return Delivery::TooManyRanges;
            }
            self.delivered.push(Delivered::single(seq, ts_ns));
            return Delivery::Recorded;
        };
        if range.start <= seq && seq <= range.end {
            // Already delivered, so the first arrival's stamp stands: a
            // duplicate is not a second delivery of the sequence value.
            return Delivery::Recorded;
        }
        if range.end.saturating_add(1) == seq {
            self.delivered[index].end = seq;
            self.delivered[index].end_ts_ns = ts_ns;
            // This value may have been the only one missing between two runs,
            // in which case they are now one.
            if self
                .delivered
                .get(index + 1)
                .is_some_and(|next| next.start == seq.saturating_add(1))
            {
                let next = self.delivered.remove(index + 1);
                self.delivered[index].end = next.end;
                self.delivered[index].end_ts_ns = next.end_ts_ns;
            }
            return Delivery::Recorded;
        }
        if range.start == seq.saturating_add(1) {
            self.delivered[index].start = seq;
            self.delivered[index].start_ts_ns = ts_ns;
            return Delivery::Recorded;
        }
        // A new disjoint range. Strictly descending sequence numbers land here
        // every time, each insertion at index 0, which is quadratic in the
        // number of datagrams — measured at minutes for a few hundred thousand,
        // on a thread that may be a live capture's. Bounded and counted.
        if self.delivered.len() >= limits.max_ranges_per_era {
            return Delivery::TooManyRanges;
        }
        self.delivered.insert(index, Delivered::single(seq, ts_ns));
        Delivery::Recorded
    }

    fn coverage(&self) -> EraCoverage {
        let first = self
            .delivered
            .first()
            .expect("an era opens with a datagram");
        let last = self.delivered.last().expect("an era opens with a datagram");
        EraCoverage {
            ordinal: self.ordinal,
            reset_count: self.reset_count,
            anchor_seq: self.anchor_seq,
            anchor_ts_ns: self.anchor_ts_ns,
            first_seq: first.start,
            last_seq: last.end,
            delivered: self.delivered.iter().map(Delivered::width).sum(),
        }
    }
}

/// One channel instance's state while the window is open.
#[derive(Debug, Clone, PartialEq, Eq)]
struct InstanceState {
    group: Ipv4Addr,
    role: PortRole,
    /// The one implementation of continuity, reordering, duplication and the
    /// era ordinal, shared with the live tier.
    sequence: SequenceTracker,
    datagrams: u64,
    admitted: u64,
    gaps_on_arrival: u64,
    missing_on_arrival: u64,
    duplicates: u64,
    reordered: u64,
    backward: u64,
    forward_jumps: u64,
    resets: u64,
    stragglers: u64,
    group_mismatches: u64,
    eras_refused: u64,
    ranges_refused: u64,
    implausible_deliveries: u64,
    unknown_schema: u64,
    eras: Vec<Era>,
}

impl InstanceState {
    fn new(group: Ipv4Addr, role: PortRole) -> Self {
        Self {
            group,
            role,
            sequence: SequenceTracker::new(),
            datagrams: 0,
            admitted: 0,
            gaps_on_arrival: 0,
            missing_on_arrival: 0,
            duplicates: 0,
            reordered: 0,
            backward: 0,
            forward_jumps: 0,
            stragglers: 0,
            group_mismatches: 0,
            eras_refused: 0,
            ranges_refused: 0,
            implausible_deliveries: 0,
            resets: 0,
            unknown_schema: 0,
            eras: Vec::new(),
        }
    }

    /// Folds one datagram's header in.
    ///
    /// The classification is [`SequenceTracker`]'s, so this reaches the same
    /// verdict the live tier reaches about the same datagram. What is added here
    /// is the delivered range: the tracker knows what a sequence number meant on
    /// arrival, and the ranges know what the archive ends up holding.
    ///
    /// A datagram is recorded as delivered whatever the verdict was, backward
    /// motion included — we hold it, whatever the publisher meant by it — and a
    /// duplicate lands inside a range already covered, so the first arrival's
    /// stamp stands.
    fn observe(
        &mut self,
        header: &DatagramHeader,
        dg: &RecordedDatagram<'_>,
        limits: DeriverLimits,
    ) {
        self.datagrams += 1;
        // `ChannelInstance` carries no group, and in AF_PACKET mode the filter
        // is a cross product of the joined groups and the ports — so two
        // groups' traffic on one port keys to the same instance, and the group
        // label is whichever arrived first. Counted rather than corrected: the
        // row is still the best available description of the traffic, and a
        // reader has to be able to see that it describes two groups.
        if *dg.dst.ip() != self.group {
            self.group_mismatches += 1;
        }
        self.admitted += u64::from(dg.drop_delta);
        if !header.schema_is_supported() {
            self.unknown_schema += 1;
        }

        let seq = header.sequence_number;
        let ts_ns = dg.recv_ts_ns;

        // Before the tracker, because the tracker cannot be asked and then
        // un-asked: observing a straggler moves its Reset Count and its era
        // ordinal, and the next genuine datagram of the new era then reads as a
        // second reset. A datagram the network held back across a reset is not
        // a transition at all — it is a late delivery into the era it came
        // from, and everything about that era's continuity is already known.
        if let Some(index) = self.era_of_straggler(header.reset_count, seq) {
            self.stragglers += 1;
            match self.eras[index].deliver(seq, ts_ns, limits) {
                Delivery::Recorded => {}
                Delivery::TooManyRanges => self.ranges_refused += 1,
                Delivery::TooDistant => self.implausible_deliveries += 1,
            }
            return;
        }

        match self.sequence.observe(seq, header.reset_count) {
            SequenceOutcome::Opened => {
                self.eras.push(Era::opened(
                    self.sequence.era_ordinal(),
                    header.reset_count,
                    seq,
                    ts_ns,
                ));
                return;
            }
            SequenceOutcome::Reset { era_ordinal } => {
                self.resets += 1;
                // A Reset Count is a u8 a sender writes, so the number of eras
                // one instance can be made to open is not ours to trust.
                if self.eras.len() >= limits.max_eras_per_instance {
                    self.eras_refused += 1;
                    return;
                }
                self.eras
                    .push(Era::opened(era_ordinal, header.reset_count, seq, ts_ns));
                return;
            }
            SequenceOutcome::InOrder => {}
            SequenceOutcome::Gap { missing } => {
                self.gaps_on_arrival += 1;
                self.missing_on_arrival += missing;
            }
            SequenceOutcome::Duplicate => self.duplicates += 1,
            SequenceOutcome::Reordered { .. } => self.reordered += 1,
            SequenceOutcome::Backward { .. } => self.backward += 1,
            // Counted, and the datagram is *not* delivered into the era. Its
            // sequence number is one the tracker refused to adopt because no
            // outage explains it, and recording it as a member of this space
            // would open an era spanning the distance to it — a run claiming
            // billions of missing values, which is a fabricated finding rather
            // than an absurd-looking one. The datagram is in the archive
            // either way; what it must not do is redefine the space.
            SequenceOutcome::ForwardJump { .. } => {
                self.forward_jumps += 1;
                return;
            }
        }
        let Some(era) = self.eras.last_mut() else {
            // Only reachable once the era bound has refused one: the instance
            // has state and no open era to deliver into.
            self.eras_refused += 1;
            return;
        };
        match era.deliver(seq, ts_ns, limits) {
            Delivery::Recorded => {}
            Delivery::TooManyRanges => self.ranges_refused += 1,
            Delivery::TooDistant => self.implausible_deliveries += 1,
        }
    }

    /// The index of the era a late datagram belongs to, when there is one.
    ///
    /// A datagram carrying the previous era's `Reset Count` is ambiguous by
    /// construction: it is either the network handing back a datagram from
    /// before the reset, or a publisher whose `u8` counter has come back around
    /// to a value it used before. The two are indistinguishable from the count
    /// alone, and the wrong choice is costly in both directions — treat a
    /// restart as a straggler and two sequence spaces merge into one era
    /// spanning both; treat a straggler as a restart and the real era splits in
    /// two, so every value missing across the split falls between the halves
    /// and is reported by neither.
    ///
    /// The direction separates them. A straggler *extends* the space it came
    /// from: it was in flight when the reset happened, so its number is beyond
    /// what that era had already delivered. A restarted space begins again from
    /// the bottom, at or below where the old one already was. So only a value
    /// above the previous era's last is taken as late; anything else opens an
    /// era, which is what a publisher reusing a count deserves.
    ///
    /// Only the immediately previous era is considered. Further back, a `u8`
    /// that wraps every 256 restarts makes the claim unsafe.
    fn era_of_straggler(&self, reset_count: u8, seq: u64) -> Option<usize> {
        let previous = self.eras.len().checked_sub(2)?;
        let era = &self.eras[previous];
        let last_delivered = era.delivered.last()?.end;
        (era.reset_count == reset_count && seq > last_delivered).then_some(previous)
    }

    fn finish(self, instance: ChannelInstance) -> InstanceLoss {
        let mut runs = Vec::new();
        let mut missing: u64 = 0;
        let mut reference_seqs: u64 = 0;
        let mut eras = Vec::with_capacity(self.eras.len());
        for era in &self.eras {
            for pair in era.delivered.windows(2) {
                let (before, after) = (pair[0], pair[1]);
                let run = SequenceRun {
                    instance,
                    group: self.group,
                    role: self.role,
                    era_ordinal: era.ordinal,
                    reset_count: era.reset_count,
                    // Saturating for the reason `Delivered::width` is: these
                    // are wire values, and the ranges are disjoint and
                    // non-adjacent by construction, so the saturation is
                    // unreachable rather than load-bearing — which is exactly
                    // when an unchecked `+ 1` gets written and later turns out
                    // to be reachable after all.
                    missing_from: before.end.saturating_add(1),
                    missing_to: after.start.saturating_sub(1),
                    before_ts_ns: before.end_ts_ns,
                    after_ts_ns: after.start_ts_ns,
                };
                missing = missing.saturating_add(run.missing_count());
                runs.push(run);
            }
            let coverage = era.coverage();
            reference_seqs = reference_seqs.saturating_add(coverage.reference_seqs());
            eras.push(coverage);
        }
        InstanceLoss {
            instance,
            group: self.group,
            role: self.role,
            datagrams: self.datagrams,
            runs,
            missing,
            reference_seqs,
            admitted: self.admitted,
            gaps_on_arrival: self.gaps_on_arrival,
            missing_on_arrival: self.missing_on_arrival,
            duplicates: self.duplicates,
            reordered: self.reordered,
            backward: self.backward,
            forward_jumps: self.forward_jumps,
            resets: self.resets,
            era_transitions: self.eras.len() as u64,
            unknown_schema: self.unknown_schema,
            stragglers: self.stragglers,
            group_mismatches: self.group_mismatches,
            eras_refused: self.eras_refused,
            ranges_refused: self.ranges_refused,
            implausible_deliveries: self.implausible_deliveries,
            eras,
        }
    }
}
