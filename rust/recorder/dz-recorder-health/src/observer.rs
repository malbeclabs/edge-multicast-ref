//! The header-only [`Observer`], and the bounded channel-instance map it keeps.
//!
//! This runs on the drain thread, so the per-datagram path resolves no labels,
//! formats no strings and allocates nothing: every counter a datagram can touch
//! is a handle already held, either on the role's children or on the channel
//! instance's entry. The two paths that do allocate — opening a channel
//! instance, and admitting a header value never seen before — are bounded, and
//! [`InstanceLimits`] is where those bounds are set.
//!
//! Nothing here calls `DatagramHeader::decode` and nothing walks messages.
//! `decode` refuses an unsupported schema version and an out-of-range declared
//! length, which is correct for a subscriber and wrong for a tier whose job is
//! to count exactly those datagrams.

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use dz_edge_core::{
    DatagramHeader, PortRole, DATAGRAM_HEADER_SIZE, MAX_DATAGRAM_SIZE, SIZE_HEARTBEAT,
    SUPPORTED_SCHEMA_VERSIONS,
};
use dz_recorder_core::{
    CaptureDropScope, ChannelInstance, Observer, RecordedDatagram, RecvTsKind, SequenceOutcome,
    SequenceTracker,
};
use prometheus::IntCounter;

use crate::error::HealthError;
use crate::instance::{InstanceState, SourceLabel};
use crate::metrics::{
    magic_label, u8_label, DeclaredLengthMismatch, DeclaredLengthViolation, FeedChildren,
    HealthMetrics, LatencyDropReason, RecvTimestampKind, RoleChildren, UnreadableReason,
    OTHER_VALUE,
};

const NANOS_PER_SECOND: f64 = 1_000_000_000.0;

/// A datagram of exactly one message of the heartbeat's size.
///
/// This is as far as the header alone can go, and the name says so: the tier is
/// forbidden to walk messages, so it cannot read the message type. No other
/// message defined in this repository is 16 bytes, which makes the shape a sound
/// signal for cadence today and a conservative one if a future feed defines
/// another 16-byte message — such a message would be counted as a heartbeat,
/// widening the cadence histogram rather than hiding a silence.
const HEARTBEAT_SHAPED_LEN: u64 = (DATAGRAM_HEADER_SIZE + SIZE_HEARTBEAT) as u64;

/// Above this, an interval is a clock and not a path.
///
/// The latency histogram's top bucket is 10 seconds, chosen to hold a
/// wide-area path and a clock that has drifted. An interval past it cannot be
/// observed usefully — it lands in `+Inf` either way — but it still enters
/// `_sum`, where it stays.
const IMPLAUSIBLE_LATENCY_SECONDS: f64 = 10.0;

/// The bounds on the state one observer keeps.
///
/// An any-source join accepts datagrams from any sender, so the key space is not
/// ours to trust and every dimension of it has a bound here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstanceLimits {
    /// Channel instances held at once. Beyond this, the least recently seen is
    /// evicted and counted.
    pub max_instances: usize,
    /// How long an instance must have been quiet before it may be evicted.
    ///
    /// Eviction is what keeps the map bounded, and opening the replacement
    /// allocates. Without a minimum age, a sender emitting one datagram per
    /// source address would put that allocation, and a new pair of label
    /// vectors, on every datagram. With it, a map full of instances that are all
    /// still live refuses the newcomer and counts it on
    /// `dz_recorder_instances_refused_total` instead — while a genuine tunnel
    /// reassignment, where the old address goes quiet as the new one appears,
    /// is admitted exactly as before.
    pub min_evict_age: Duration,
    /// Distinct `Magic` values, and distinct `Schema Version` values, that get a
    /// label of their own before the rest are counted under `other`.
    ///
    /// `Magic` is 16 sender-controlled bits. Counting it by value is required,
    /// and giving every value a series is a cardinality bomb an unknown sender
    /// gets to detonate, so the budget is what reconciles the two.
    pub max_distinct_header_values: usize,
}

impl Default for InstanceLimits {
    fn default() -> Self {
        Self {
            // 128 bytes of reordering window plus a handful of scalars and
            // metric handles per instance, so a few thousand instances is a
            // megabyte or so — far above any real publisher count, and far
            // below anything that threatens a recorder host.
            max_instances: 4096,
            min_evict_age: Duration::from_secs(60),
            max_distinct_header_values: 16,
        }
    }
}

/// The health tier: everything a recorder can say about a feed, and about
/// itself, from the 24-byte datagram header.
pub struct HealthObserver {
    metrics: Arc<HealthMetrics>,
    feed: String,
    /// Indexed by [`role_index`], `None` for a port role this feed was not
    /// declared to carry.
    roles: [Option<RoleChildren>; 3],
    feed_children: FeedChildren,
    instances: HashMap<ChannelInstance, InstanceState>,
    /// Sorted, so a source's declared status is a binary search rather than a
    /// scan, and so eviction can tell an operator's publisher from a stranger.
    declared_sources: Vec<Ipv4Addr>,
    /// The `Channel ID`s an operator declared, sorted. Empty means none was
    /// declared, which is not a declaration that there are none.
    declared_channels: Vec<u8>,
    limits: InstanceLimits,
    min_evict_age_ns: u64,
    /// Where the capture's own losses may be charged. See [`HealthObserver::new`].
    drop_scope: CaptureDropScope,
    /// The earliest `last_seen_ns` among instances a stranger may displace, as
    /// of the last scan.
    ///
    /// Only ever a lower bound: `last_seen_ns` moves forward and entries are
    /// only removed, so the true earliest can be later than this but never
    /// earlier. That is the direction that makes it safe to short-circuit on —
    /// it can cost one scan that finds nothing, and can never skip an eviction
    /// that would have succeeded.
    earliest_undeclared_last_seen_ns: u64,
    magic: Vec<(u16, IntCounter)>,
    magic_other: IntCounter,
    schema_versions: Vec<(u8, IntCounter)>,
    schema_version_other: IntCounter,
}

/// The slot in [`HealthObserver::roles`] a port role occupies.
const fn role_index(role: PortRole) -> usize {
    match role {
        PortRole::Mktdata => 0,
        PortRole::Refdata => 1,
        PortRole::Snapshot => 2,
    }
}

impl HealthObserver {
    /// Builds an observer for one feed of `metrics`.
    ///
    /// The feed's port roles, Channel IDs and declared sources are taken from
    /// the declaration `metrics` already holds rather than restated here: two
    /// copies of that list would drift, and the copy that drifted would be the
    /// one deciding which series exist before the first datagram.
    ///
    /// # Errors
    ///
    /// [`HealthError::UnknownFeed`] if `feed` was not among the feeds `metrics`
    /// was constructed with. Refused rather than accepted, because an observer
    /// on an undeclared feed would emit series that exist only after the traffic
    /// they describe — the failure pre-creation exists to prevent, arriving
    /// through a typo in a feed name.
    /// `drop_scope` must be the capture's own, not a preference: it decides
    /// where `drop_delta` may be charged. A ring counts frames dropped before
    /// anything demultiplexed them into port roles, so charging that number to
    /// the role of whichever datagram happened to arrive next attributes our
    /// loss to a feed that may not have lost anything — and the archive this
    /// same process writes branches on the scope, so the live metrics would
    /// contradict the object on disk.
    pub fn new(
        metrics: Arc<HealthMetrics>,
        feed: &str,
        limits: InstanceLimits,
        drop_scope: CaptureDropScope,
    ) -> Result<Self, HealthError> {
        if limits.max_instances == 0 {
            return Err(HealthError::NoInstanceBudget);
        }
        let definition = metrics
            .feed_definition(feed)
            .ok_or_else(|| HealthError::UnknownFeed {
                feed: feed.to_owned(),
            })?;

        let mut roles = [None, None, None];
        for role in &definition.port_roles {
            roles[role_index(*role)] = Some(metrics.role_children(feed, *role));
        }

        let mut declared_sources = definition.expected_sources.clone();
        declared_sources.sort_unstable();
        declared_sources.dedup();

        let mut declared_channels = definition.channel_ids.clone();
        declared_channels.sort_unstable();
        declared_channels.dedup();

        let mut magic = Vec::with_capacity(limits.max_distinct_header_values);
        if let Some(expected) = definition.expected_magic {
            magic.push((expected, metrics.magic_child(feed, &magic_label(expected))));
        }
        let mut schema_versions = Vec::with_capacity(
            limits
                .max_distinct_header_values
                .max(SUPPORTED_SCHEMA_VERSIONS.len()),
        );
        for version in SUPPORTED_SCHEMA_VERSIONS {
            schema_versions.push((
                version,
                metrics.schema_version_child(feed, u8_label(version)),
            ));
        }

        let feed_children = metrics.feed_children(feed);
        let magic_other = metrics.magic_child(feed, OTHER_VALUE);
        let schema_version_other = metrics.schema_version_child(feed, OTHER_VALUE);

        Ok(Self {
            metrics,
            feed: feed.to_owned(),
            roles,
            feed_children,
            instances: HashMap::with_capacity(limits.max_instances),
            declared_sources,
            declared_channels,
            limits,
            min_evict_age_ns: nanos_saturating(limits.min_evict_age),
            earliest_undeclared_last_seen_ns: 0,
            drop_scope,
            magic,
            magic_other,
            schema_versions,
            schema_version_other,
        })
    }

    /// Channel instances currently held.
    #[must_use]
    pub fn instances_tracked(&self) -> usize {
        self.instances.len()
    }

    /// Records drops the arrival interface reported, as a delta since the
    /// previous reading.
    ///
    /// The delta is the caller's to compute, because only the caller knows when
    /// the counter it read was reset and the counters wrap: the first reading on
    /// a handle establishes a baseline rather than being reported as loss.
    pub fn record_interface_drops(&self, delta: u64) {
        self.feed_children.interface_drops.inc_by(delta);
    }

    /// Records one group membership replaced on `role`.
    ///
    /// Ignored for a port role this feed was not declared to carry, for the same
    /// reason a datagram on one is: every series is keyed on a role this
    /// recorder was told about.
    pub fn record_rejoin(&self, role: PortRole) {
        if let Some(children) = self.roles[role_index(role)].as_ref() {
            children.rejoins.inc();
        }
    }

    /// Carries the capture's own counters across, as deltas over a sweep.
    ///
    /// These are the capture crate's counters and not this tier's: a membership
    /// replaced, a replacement that failed, a datagram from a source nobody
    /// declared, a datagram addressed to a group this handle did not join. The
    /// tier pre-creates every one of them, so left unfed they read as a healthy
    /// zero for the life of the process — and the rejoin counters are the
    /// diagnostic for the exact failure they exist for: a stranded membership
    /// on a socket that stays open, readable and permanently silent.
    pub fn record_capture_deltas(&self, deltas: CaptureDeltas) {
        self.feed_children.rejoins.inc_by(deltas.rejoins);
        self.feed_children
            .rejoin_failures
            .inc_by(deltas.rejoin_failures);
        self.feed_children
            .unexpected_source_datagrams
            .inc_by(deltas.unexpected_source_datagrams);
        self.feed_children
            .foreign_group_datagrams
            .inc_by(deltas.foreign_group_datagrams);
    }

    /// Records one completed archive segment deleted to stay inside the staging
    /// budget.
    ///
    /// Recorder-wide rather than per feed, because the staging budget is.
    pub fn record_segment_evicted(&self) {
        self.metrics.segments_evicted().inc();
    }

    /// Whether an instance is one an operator declared, and so one whose series
    /// survive eviction and whose datagrams displace a stranger.
    ///
    /// Both halves, because eviction survival is keyed on the whole instance —
    /// `(role, channel, source)` — while only the address was ever checked.
    /// Spoofing a declared address while cycling `Channel ID` then kept series
    /// alive on channels the feed never carried, which is the bounded map's
    /// guarantee defeated one label at a time.
    ///
    /// An empty channel declaration admits every channel. No configuration key
    /// states them today for most feeds, and reading "unstated" as "none" would
    /// turn every declared publisher into a stranger on its own recorder.
    fn is_declared(&self, source: Ipv4Addr, channel_id: u8) -> bool {
        self.declared_sources.binary_search(&source).is_ok()
            && (self.declared_channels.is_empty()
                || self.declared_channels.binary_search(&channel_id).is_ok())
    }

    /// Frees a slot in the instance map, if any instance has been quiet long
    /// enough to evict. Returns whether there is now room.
    fn make_room(&mut self, now_ns: u64, arrival_is_declared: bool) -> bool {
        if self.instances.len() < self.limits.max_instances {
            return true;
        }
        // The refusal path is the hot one under a spoofed-source flood: every
        // datagram from an unknown key arrives here, and scanning the whole map
        // for each of them puts tens of microseconds on the drain thread, which
        // is the record path shedding datagrams to answer a question about
        // metrics. The floor is a lower bound on the earliest sighting among
        // the instances a stranger may displace, so while it says nothing can
        // be evicted, nothing can, and the answer costs a comparison.
        if !arrival_is_declared
            && now_ns.saturating_sub(self.earliest_undeclared_last_seen_ns) < self.min_evict_age_ns
        {
            return false;
        }

        let old_enough = |state: &InstanceState| {
            now_ns.saturating_sub(state.last_seen_ns) >= self.min_evict_age_ns
        };
        let oldest = |instances: &HashMap<ChannelInstance, InstanceState>,
                      keep: &dyn Fn(&InstanceState) -> bool| {
            instances
                .iter()
                .filter(|(_, state)| keep(state))
                .min_by_key(|(_, state)| state.last_seen_ns)
                .map(|(key, _)| *key)
        };

        // A stranger never displaces a declared publisher. Ordering victims on
        // age alone — or even on age within a preference — makes the declared
        // one the victim as soon as it is the only entry old enough to evict,
        // which is precisely what a flood of fresh strangers produces. The
        // default eviction age is 60 seconds against a heartbeat histogram that
        // measures to 300, so "quiet" is the normal state of the feed being
        // watched. Its tracker goes with it, the next datagram reopens the
        // instance silently, and real loss reads as zero.
        let victim = if arrival_is_declared {
            // A declared arrival takes a stranger of any age first: the map
            // stays bounded either way, and between an unknown sender and the
            // publisher an operator named in the configuration, the
            // configuration wins.
            oldest(&self.instances, &|state| !state.declared_source).or_else(|| {
                oldest(&self.instances, &|state| {
                    state.declared_source && old_enough(state)
                })
            })
        } else {
            oldest(&self.instances, &|state| {
                !state.declared_source && old_enough(state)
            })
        };
        // Whatever the scan saw, it saw all of it, so the floor is exact again.
        // An insertion only moves it later and `last_seen_ns` only advances, so
        // a stale floor is always early — which is the direction that can cost
        // one wasted scan and can never skip an eviction that would have worked.
        self.earliest_undeclared_last_seen_ns = self
            .instances
            .values()
            .filter(|state| !state.declared_source)
            .map(|state| state.last_seen_ns)
            .min()
            .unwrap_or(now_ns);
        let Some(key) = victim else {
            return false;
        };
        // Only reachable when every instance in a full map is a declared one,
        // which is a configuration that has outgrown max_instances rather than
        // a flood.
        if self
            .instances
            .get(&key)
            .is_some_and(|state| state.declared_source)
        {
            self.feed_children.declared_evicted.inc();
        }
        let state = self
            .instances
            .remove(&key)
            .expect("the victim was just found in this map");
        // A declared source's series were pre-created at startup, so removing
        // them would delete a panel an operator built against a publisher that
        // is merely quiet. A stranger's series are removed, because otherwise
        // the label vectors keep every instance the bounded map ever held and
        // the bound moves one layer down without changing anything.
        if !state.declared_source {
            // The cadence histogram is keyed by channel and shared, so it is
            // only reclaimable once no instance is left on that channel.
            let channel_is_now_empty = !self.instances.iter().any(|(other_key, other)| {
                other.role == state.role && other_key.channel_id == key.channel_id
            });
            self.metrics.remove_instance_children(
                &self.feed,
                state.role.as_str(),
                u8_label(key.channel_id),
                state.source.as_str(),
                channel_is_now_empty,
            );
        }
        self.feed_children.instances_evicted.inc();
        true
    }
}

/// One sweep's worth of the capture's own counters, as deltas.
///
/// Deltas rather than totals, because these counters are cumulative on both
/// sides and a total handed to a counter would count every earlier sweep again.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CaptureDeltas {
    pub rejoins: u64,
    pub rejoin_failures: u64,
    pub unexpected_source_datagrams: u64,
    pub foreign_group_datagrams: u64,
}

/// Nanoseconds, saturating rather than wrapping: a `Duration` above 584 years is
/// a misconfiguration, and wrapping it would turn "never evict" into "evict
/// anything".
fn nanos_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Counts one header value by value, giving it its own label until the budget is
/// spent and folding the rest into `other`.
fn count_by_value<K: Copy + PartialEq>(
    table: &mut Vec<(K, IntCounter)>,
    other: &IntCounter,
    budget: usize,
    value: K,
    child: impl FnOnce(K) -> IntCounter,
) {
    // A linear scan over a table bounded at a handful of entries, which beats a
    // hash on the drain thread and, unlike a hash map, cannot grow.
    if let Some((_, counter)) = table.iter().find(|(held, _)| *held == value) {
        counter.inc();
        return;
    }
    if table.len() >= budget {
        other.inc();
        return;
    }
    let counter = child(value);
    counter.inc();
    table.push((value, counter));
}

impl Observer for HealthObserver {
    fn on_datagram(&mut self, dg: &RecordedDatagram<'_>) {
        let index = role_index(dg.role);
        if self.roles[index].is_none() {
            self.feed_children.unexpected_role.inc();
            // The capture handed this delta over once and will not again, so
            // returning without it makes our own loss vanish from the
            // exposition — and loss that is not admitted anywhere reads as a
            // publisher gap. At handle scope, where one ring carries every
            // role, traffic on an undeclared role is exactly what is most
            // likely to be there.
            self.feed_children
                .capture_drops_handle
                .inc_by(u64::from(dg.drop_delta));
            return;
        }
        let role_label = dg.role.as_str();

        // Scoped so the role's children are released before the instance map is
        // touched: the two are disjoint fields, and keeping the borrows apart is
        // what lets both paths stay on already-resolved handles.
        let drop_scope = self.drop_scope;
        let handle_drops = &self.feed_children.capture_drops_handle;
        let header = {
            let role = self.roles[index]
                .as_ref()
                .expect("the role was checked above");
            role.datagrams.inc();
            role.bytes.inc_by(u64::from(dg.wire_payload_len));
            // Our own loss, recorded before anything is concluded about the
            // feed: a gap covered by this is not a publisher finding. Where it
            // is recorded depends on what the number means — at handle scope it
            // is the ring's, covering every role at once, and a per-role
            // subtraction of it is arithmetic on a quantity that does not exist.
            match drop_scope {
                CaptureDropScope::PortRole => role.capture_drops.inc_by(u64::from(dg.drop_delta)),
                CaptureDropScope::CaptureHandle => {
                    handle_drops.inc_by(u64::from(dg.drop_delta));
                }
            }
            let kind = match dg.recv_ts_kind {
                RecvTsKind::KernelSoftware => RecvTimestampKind::KernelSoftware,
                RecvTsKind::ApplicationFallback => RecvTimestampKind::ApplicationFallback,
            };
            role.recv_ts[kind as usize].inc();

            let Ok(header) = DatagramHeader::peek(dg.payload) else {
                // `peek` validates only the buffer's length, so this is the
                // short-header case and nothing else.
                role.unreadable[UnreadableReason::ShortHeader as usize].inc();
                return;
            };

            if !header.declared_len_is_in_range() {
                let violation = if usize::from(header.datagram_len) > MAX_DATAGRAM_SIZE {
                    DeclaredLengthViolation::OverCap
                } else {
                    DeclaredLengthViolation::UnderHeader
                };
                role.declared_violation[violation as usize].inc();
            }
            // Against the wire length, not the captured length: a capture that
            // truncates would otherwise report every datagram as short.
            let declared = u32::from(header.datagram_len);
            if declared != dg.wire_payload_len {
                let mismatch = if declared > dg.wire_payload_len {
                    DeclaredLengthMismatch::DeclaredExceedsReceived
                } else {
                    DeclaredLengthMismatch::DeclaredBelowReceived
                };
                role.declared_mismatch[mismatch as usize].inc();
            }

            match dg.recv_ts_kind {
                // An application-level fallback stamp measures this recorder's
                // scheduler. It is counted, never observed: averaging it
                // together with a kernel stamp measures neither.
                RecvTsKind::ApplicationFallback => {
                    role.latency_dropped[LatencyDropReason::ApplicationFallback as usize].inc()
                }
                RecvTsKind::KernelSoftware => match dg
                    .recv_ts_ns
                    .checked_sub(header.send_timestamp_ns)
                    .map(|elapsed| elapsed as f64 / NANOS_PER_SECOND)
                {
                    // Symmetric with the negative case, and for the same
                    // reason. A send timestamp near zero — a field on the wire,
                    // from anybody the join accepts — observes a billion-second
                    // interval into a histogram whose average an operator is
                    // pointed at by its own help text, and _sum does not come
                    // back down. The series is (feed, role)-scoped, so the
                    // bounded instance map contains nothing here.
                    Some(seconds) if seconds > IMPLAUSIBLE_LATENCY_SECONDS => {
                        role.latency_dropped[LatencyDropReason::ImplausibleInterval as usize].inc();
                    }
                    Some(seconds) => role.latency.observe(seconds),
                    None => {
                        role.latency_dropped[LatencyDropReason::NegativeInterval as usize].inc()
                    }
                },
            }
            header
        };

        {
            let metrics = &self.metrics;
            let feed = self.feed.as_str();
            count_by_value(
                &mut self.magic,
                &self.magic_other,
                self.limits.max_distinct_header_values,
                header.magic,
                |magic| metrics.magic_child(feed, &magic_label(magic)),
            );
            count_by_value(
                &mut self.schema_versions,
                &self.schema_version_other,
                self.limits
                    .max_distinct_header_values
                    .max(SUPPORTED_SCHEMA_VERSIONS.len()),
                header.schema_version,
                |version| metrics.schema_version_child(feed, u8_label(version)),
            );
        }

        let key = ChannelInstance::new(*dg.src.ip(), header.channel_id, dg.dst.port());
        if !self.instances.contains_key(&key) {
            // Decided before admission and not after it: a declared publisher
            // that is refused entry never gets to be a declared instance, which
            // is the same loss of accounting as evicting it.
            let declared_source = self.is_declared(*dg.src.ip(), header.channel_id);
            if !self.make_room(dg.recv_ts_ns, declared_source) {
                self.feed_children.instances_refused.inc();
                return;
            }
            // A source address not seen before opens a new series silently: no
            // gap, no loss, no alert. A tunnel address is a lease, it can be
            // reassigned under a live host, and a reassignment must not page.
            let source = SourceLabel::new(*dg.src.ip());
            let children = self.metrics.instance_children(
                &self.feed,
                role_label,
                u8_label(header.channel_id),
                source.as_str(),
            );
            // The gauge outlives the entry when the series were kept — a
            // declared source's are — so the ordinal is read back rather than
            // restarted. era_ordinal is documented as monotonic and as the
            // value to group an era by, and era_transitions_total as resets + 1;
            // restarting at 1 merges two eras under one number and breaks both
            // statements at once.
            let resumed_era = u64::try_from(children.era_ordinal.get()).unwrap_or(0);
            self.instances.insert(
                key,
                InstanceState {
                    sequence: SequenceTracker::resuming_from_era(resumed_era),
                    source,
                    role: dg.role,
                    last_seen_ns: dg.recv_ts_ns,
                    last_heartbeat_ns: None,
                    declared_source,
                    children,
                },
            );
            self.feed_children.instances_opened.inc();
        }
        // The gauge is set from the map's own length rather than incremented, so
        // it cannot drift away from the truth across an eviction.
        self.feed_children
            .instances_tracked
            .set(self.instances.len() as i64);

        let state = self
            .instances
            .get_mut(&key)
            .expect("the instance was just inserted or already present");
        state.last_seen_ns = dg.recv_ts_ns;
        let children = &state.children;

        match state
            .sequence
            .observe(header.sequence_number, header.reset_count)
        {
            SequenceOutcome::Opened => children.era_transitions.inc(),
            SequenceOutcome::InOrder => {}
            SequenceOutcome::Gap { missing } => {
                children.gaps.inc();
                children.missing.inc_by(missing);
            }
            SequenceOutcome::Duplicate => children.duplicates.inc(),
            SequenceOutcome::Reordered { .. } => children.reordered.inc(),
            SequenceOutcome::Reset { .. } => {
                children.resets.inc();
                children.era_transitions.inc();
            }
            SequenceOutcome::Backward { .. } => children.backward.inc(),
            // Counted and nothing else: no gap, no missing datagrams, and the
            // tracker did not adopt the number. A sequence number is a field on
            // the wire, an any-source join accepts it from anybody, and this is
            // the one outcome that exists because of that rather than because a
            // publisher can produce it.
            SequenceOutcome::ForwardJump { .. } => children.forward_jump.inc(),
        }
        children
            .sequence_current
            // Saturating, not wrapping: a u64 above i64::MAX renders as a
            // negative sequence number, and a gauge that can read -1 for a
            // number that is merely large is a gauge nobody can act on.
            .set(i64::try_from(state.sequence.highest()).unwrap_or(i64::MAX));
        children
            .era_ordinal
            .set(state.sequence.era_ordinal() as i64);
        children
            .last_datagram_timestamp
            .set(dg.recv_ts_ns as f64 / NANOS_PER_SECOND);

        // wire_payload_len and not header.datagram_len: the declared length is
        // the sender's claim about itself, and it is checked against the wire a
        // few lines above precisely because the wire is the truth. This series
        // carries no source label, so believing the claim lets any sender write
        // fabricated cadence into a declared channel's percentiles and mask a
        // real silence.
        if header.msg_count == 1 && u64::from(dg.wire_payload_len) == HEARTBEAT_SHAPED_LEN {
            if let Some(previous) = state.last_heartbeat_ns {
                if let Some(elapsed) = dg.recv_ts_ns.checked_sub(previous) {
                    children
                        .heartbeat_interval
                        .observe(elapsed as f64 / NANOS_PER_SECOND);
                }
            }
            state.last_heartbeat_ns = Some(dg.recv_ts_ns);
            children
                .heartbeat_last_timestamp
                .set(dg.recv_ts_ns as f64 / NANOS_PER_SECOND);
        }
    }
}
