//! The reference-data owner: who an instrument is, and how a subscriber comes
//! to know it.

use std::collections::HashMap;

use dz_adapter_core::{InstrumentRef, InstrumentSpec, ListingSink, DEFAULT_SHARD};
use dz_edge_refdata::{InstrumentDefinition, ManifestSummary, SYMBOL_LEN};
use dz_publisher_lowering::{InstrumentTable, SourceId};

use crate::clock::Clock;
use crate::definition::{self, Fits};
use crate::error::RefdataError;
use crate::pacer::DefinitionPacer;
use crate::policy::{Phase, SelectionPolicy};
use crate::refusal::Refusal;
use crate::state::{Entry, StateRecord};
use crate::store::{StateError, StateStore};
use crate::CycleSchedule;

/// A venue's ticker as the wire carries it, which is the identity everything
/// here keys on.
type SymbolKey = [u8; SYMBOL_LEN];

/// One shard, and the `Channel ID` its reference data is published on.
///
/// A shard is the partition the venue names at admission; a `Channel ID` is the
/// configuration's, and the mapping between the two is the operator's. Both are
/// here because a manifest composed without a datagram builder still has to
/// state a truthful `Channel ID`, and the only truthful one is the channel that
/// carries this shard's reference data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardConfig {
    /// What the venue names in
    /// [`ListingSink::list_on`](dz_adapter_core::ListingSink::list_on).
    /// Checked at load, because it becomes a path component elsewhere.
    pub name: String,
    /// The shard's own `Channel ID`.
    ///
    /// A shard carrying two feed specifications is two channel instances and
    /// therefore two `Channel ID`s, sharing one published set. This is the one
    /// a manifest states when nothing overwrites it; the datagram builder
    /// stamps the header's `Channel ID` at push, so the copy in the message
    /// body cannot disagree with the port it left by.
    pub channel_id: u8,
}

impl ShardConfig {
    /// The shard a document that names none resolves to.
    ///
    /// Spelled from [`DEFAULT_SHARD`] rather than typed, here and in the
    /// configuration, so that the one token cannot become two.
    #[must_use]
    pub fn default_shard(channel_id: u8) -> Self {
        Self {
            name: DEFAULT_SHARD.to_string(),
            channel_id,
        }
    }
}

/// What this publisher is, on the wire.
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    /// Checked once at startup against the ranges the source registry reserves,
    /// and against the `Source ID` the persisted state was minted under.
    pub source_id: SourceId,
    /// Every shard an adapter may admit to, in the order the document states
    /// them.
    ///
    /// One entry is the ordinary case and is what a document naming no shard
    /// resolves to. A name that is not in here is refused rather than
    /// defaulted — see [`Refusal::UnknownShard`] — so an empty set or a
    /// repeated name is a startup failure rather than a publisher whose
    /// channels are quietly empty.
    pub shards: Vec<ShardConfig>,
    pub selection: SelectionPolicy,
    pub schedule: CycleSchedule,
}

/// One published instrument.
#[derive(Debug, Clone, Copy)]
struct Published {
    symbol: SymbolKey,
    /// Which shard's published set this instrument is in, as an index into the
    /// configured shards.
    ///
    /// Held here rather than on [`InstrumentTable`] deliberately. The table is
    /// the lowering's, and the lowering is linked by the offline re-lowering
    /// without the runtime; a shard on it would drag a crate that publishes
    /// nothing into knowing where publication goes.
    shard: usize,
    /// `Manifest Seq` is [`definition::stamped`] on the way out, never held
    /// here: a definition sitting in this table between two changes to the
    /// published set would otherwise carry a manifest that no longer exists.
    definition: InstrumentDefinition,
}

/// One shard's published set.
///
/// `reference-data/spec.md` defines `Manifest Seq` as incrementing "every time
/// the published instrument set changes **on this channel**", `Valid` against
/// "**channel** state", and `Instrument Count` as what a subscriber to that
/// channel compares its collected definitions against. One of these per shard
/// is what makes those three true; one per process makes all three describe
/// something no subscriber can see.
///
/// The pacer is here for the same reason and one of its own: obligation 6 —
/// restart the definition cycle when `Manifest Seq` changes — is per channel,
/// so a single pacer would restart every channel's cycle for an admission on
/// one of them, which is the burst obligation 2 forbids arriving through the
/// correct handling of obligation 6.
#[derive(Debug)]
struct PublishedSet {
    published: usize,
    manifest_seq: u16,
    pacer: DefinitionPacer,
    /// Where this shard's next lap resumes in [`Registry::slots`]. The slots
    /// are shared, so a cursor is per shard and skips what is not its own.
    cursor: usize,
}

/// Counts worth reporting, and where each one goes.
///
/// This crate constructs no metric — the normative `dz_publisher_*` set is
/// closed by the playbook and a series is not this crate's to invent — so what
/// it owes the runtime is the numbers, named so that the mapping is not a
/// guess:
///
/// - [`admitted`](Self::admitted) is `refdata_new_listings_total`.
/// - [`delisted`](Self::delisted) is `refdata_delistings_total`.
/// - [`definitions_emitted`](Self::definitions_emitted) is
///   `refdata_definitions_emitted_total`.
/// - [`Registry::published`] is `refdata_instruments_current`, and
///   [`Registry::published_on`] is that gauge's value for one `Channel ID` —
///   the wire's `Instrument Count` is a channel's rather than a process's.
/// - [`Registry::manifest_seq`] and [`Registry::is_valid`] are
///   `refdata_manifest_seq` and `refdata_manifest_valid`, both by `Channel ID`,
///   and each takes the shard that channel carries.
/// - [`declined_unrepresentable`](Self::declined_unrepresentable) is a
///   reference-data load that did not fully load, under the load-error
///   family's `schema` reason.
///
/// [`declined_at_cap`](Self::declined_at_cap) maps to nothing, deliberately: it
/// is the selection policy working, and a series that climbs whenever a venue
/// lists more instruments than a feed publishes would be alerting on the normal
/// case.
///
/// # The two shard counts map to nothing, and what carries them instead
///
/// Also deliberate, and for the same reason the rest of this crate constructs
/// no metric: the normative set is closed and this crate does not own it.
///
/// The signal for a shard the venue can never reach is
/// `refdata_instruments_current` sitting at 0 for that shard's `Channel ID`,
/// which is true from startup and needs no datagram. **That covers the total
/// case only.** A gauge at 0 says a channel is empty; it cannot say a venue
/// asked for a name this publisher has no channel for. And a venue that
/// misnames only *some* of its offers leaves the gauge non-zero with those
/// instruments unpublished, which no series in the closed set separates from a
/// channel that holds fewer instruments.
///
/// So for the partial case the only signal is the name, and
/// [`Registry::take_unknown_shards`] is where the runtime gets it. These two
/// numbers are what the exit report prints beside it; between them a log line
/// says which name was offered and a number says how much was declined under
/// it. Neither is a substitute for a series and neither is offered as one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    pub admitted: u64,
    pub delisted: u64,
    pub definitions_emitted: u64,
    pub declined_at_cap: u64,
    pub declined_unrepresentable: u64,
    /// Offers naming a shard this publisher was not configured with. Counted
    /// apart from [`declined_unrepresentable`](Self::declined_unrepresentable)
    /// because nothing about the instrument was wrong.
    pub declined_unknown_shard: u64,
    /// Re-offers naming a different shard for a published instrument. The
    /// instrument stayed where it was; see [`Refusal::ShardRestated`].
    pub declined_shard_restated: u64,
    /// Symbols or legs that could not be stated honestly in their fixed-width
    /// field: truncated, or not representable as NUL-padded ASCII. Reported
    /// once per load rather than per message, which is what the codec's own
    /// documentation for those cases asks for.
    pub imperfect_symbols: u64,
}

/// The reference-data owner.
///
/// It is the thing that decides who an instrument is: it applies the selection
/// policy, mints and persists the `Instrument ID`, composes the
/// `InstrumentDefinition`, populates the lowering's
/// [`InstrumentTable`], maintains
/// `Manifest Seq` and the `Valid` flag, and paces the definition cycle. A venue
/// reaches all of that through [`ListingSink`] and can express none of it.
///
/// # One registry, N published sets
///
/// The registry is one per process and the **published set** is one per shard.
/// The split is not a preference: identity can only be one thing, so the
/// `Instrument ID` table, its persistence, the state-directory claim and the
/// selection policy's caps are process-wide — while `Manifest Seq`, `Valid`,
/// `Instrument Count` and the definition pacer are defined by
/// `reference-data/spec.md` against the channel, so there is one of each per
/// shard.
///
/// A registry per shard would be the other split, and it is wrong twice over:
/// N `Instrument ID` spaces under one `Source ID`, and N writers on one state
/// directory — where the single-writer guard would refuse the second, so it
/// fails at startup rather than subtly, and is still a failure.
///
/// # The guarantee, and what it costs
///
/// **A published `Instrument ID` always resolves to a published definition.**
/// Everything awkward in here follows from that one sentence:
///
/// - An ID is minted only after its definition has composed, so an instrument
///   whose numbers cannot be stated on the wire never gets one.
/// - An ID is persisted before it is admitted, so a restart cannot hand it to
///   something else. A write that fails means nothing further is minted; see
///   [`fault`](Self::fault).
/// - An ID is never re-used, not even for a delisted instrument, because a
///   subscriber holding a book keyed on one must never find it pointing at
///   something else. A relisted symbol gets its own ID back, since a symbol is
///   the identity.
/// - The state directory takes one writer. Two writers means the last flush
///   wins and half the published IDs resolve to nothing after a restart.
///
/// # What it does not do
///
/// It does not transmit. [`definition_tick`](Self::definition_tick) hands back
/// the definitions this tick owes and [`manifest`](Self::manifest) composes the
/// summary; framing them into datagrams and putting them on the wire belongs to
/// the egress layer, and keeping the two apart is what lets the cycle be tested
/// against a stated clock with no socket anywhere.
#[derive(Debug)]
pub struct Registry<S: StateStore, C: Clock> {
    config: RegistryConfig,
    store: S,
    clock: C,
    /// Every `Instrument ID` ever minted, by symbol. Append-only: a delisting
    /// leaves the entry, which is what makes the ID unreusable and what lets a
    /// relisted symbol come back as itself.
    minted: HashMap<SymbolKey, u32>,
    next_id: u32,
    /// The live handle for each published symbol, so a re-offer is one lookup.
    handles: HashMap<SymbolKey, InstrumentRef>,
    /// Parallel to the instrument table's slots, so the definition cycle walks
    /// the published set in admission order and a withdrawn instrument leaves
    /// the same hole in both.
    slots: Vec<Option<Published>>,
    /// What is published across every shard, which is what the selection
    /// policy's caps are measured against. `max_published` is a cap on what
    /// this publisher publishes and not on what a channel carries, so the
    /// number it is compared against has to be the process's.
    published: usize,
    instruments: InstrumentTable,
    /// One per configured shard, in the order [`RegistryConfig::shards`] states
    /// them, so an index into either is an index into the other.
    sets: Vec<PublishedSet>,
    phase: Phase,
    counts: Counts,
    /// Distinct shard names offered that this publisher has none of, in the
    /// order they were first seen, and how many of them a caller has been
    /// handed. An adapter re-offers its whole set every poll, so a name is
    /// remembered rather than reported again.
    unknown_shards: Vec<String>,
    unknown_shards_taken: usize,
    last_refusal: Option<Refusal>,
    fault: Option<StateError>,
}

/// The most distinct unknown shard names one process reports.
///
/// A venue computing a name per instrument would otherwise grow that list from
/// data this publisher does not control. Past the ceiling the count still
/// climbs and [`Registry::last_refusal`] still names the refusal; what stops is
/// the naming, which by then has already said the thing an operator has to act
/// on.
const MAX_REPORTED_UNKNOWN_SHARDS: usize = 64;

impl<S: StateStore, C: Clock> Registry<S, C> {
    /// Claim the state directory, read what is in it, and be ready to admit.
    ///
    /// # The single-writer guard
    ///
    /// The claim is taken first, before anything is read and long before
    /// anything is written. **A directory already held live refuses the
    /// newcomer, and the incumbent keeps running**: it is already publishing
    /// definitions that subscribers hold IDs from, so refusing the second
    /// process costs one failed start, while letting it in costs both of them —
    /// each mints from its own copy of `next_id`, each flush overwrites the
    /// other's, and after the next restart whichever IDs lost the last flush
    /// resolve to nothing.
    ///
    /// # The shard set, read first
    ///
    /// A configuration with no shard, or with one name twice, is refused before
    /// the claim is taken. Neither is reachable from a document — the load
    /// checks refuse both — so what this catches is a caller composing the
    /// configuration itself, and both failures are otherwise silent: every
    /// offer refused as an unknown shard, or a `Channel ID` whose manifest
    /// stays empty for the life of the process.
    ///
    /// # The three ways the persisted state can fail
    ///
    /// - **Missing** — no record at all. A cold start: minting begins at
    ///   [`FIRST_INSTRUMENT_ID`](crate::FIRST_INSTRUMENT_ID) and this is not an error. It is worth knowing
    ///   that it is indistinguishable from a state directory somebody has
    ///   emptied, which is why the directory is durable state and not a cache.
    /// - **Unreadable** — a record that exists and cannot be read.
    ///   [`RefdataError::State`], and the publisher does not start. Continuing
    ///   would mint from the beginning of the ID space while subscribers still
    ///   hold yesterday's IDs.
    /// - **Damaged, or somebody else's** — a record that reads back as
    ///   something other than a record this build wrote, or one minted under a
    ///   different `Source ID`. [`RefdataError::CorruptState`] and
    ///   [`RefdataError::StateBelongsToAnotherSource`], and again the publisher
    ///   does not start. The `Source ID` check is what catches two feeds
    ///   configured to share one `state_dir`, where the guard cannot help
    ///   because they never run at the same time.
    ///
    /// # Errors
    ///
    /// Every [`RefdataError`]. All are startup failures and none is
    /// recoverable by continuing.
    pub fn open(config: RegistryConfig, mut store: S, clock: C) -> Result<Self, RefdataError> {
        // The shard set is read before the directory is claimed, because a
        // configuration this registry cannot serve is a failure that costs
        // nothing to report and should not first take a claim off an incumbent
        // that is serving one.
        if config.shards.is_empty() {
            return Err(RefdataError::NoShardConfigured);
        }
        for (index, shard) in config.shards.iter().enumerate() {
            if config.shards[..index]
                .iter()
                .any(|earlier| earlier.name == shard.name)
            {
                return Err(RefdataError::ShardConfiguredTwice {
                    shard: shard.name.clone(),
                });
            }
        }

        match store.claim() {
            Ok(()) => {}
            Err(StateError::AlreadyHeld) => return Err(RefdataError::StateHeldByAnotherWriter),
            Err(error) => return Err(RefdataError::State(error)),
        }
        let record = match store.load().map_err(RefdataError::State)? {
            None => StateRecord::empty(config.source_id.get()),
            Some(bytes) => StateRecord::decode(&bytes)?,
        };
        if record.source_id != config.source_id.get() {
            return Err(RefdataError::StateBelongsToAnotherSource {
                persisted: record.source_id,
                configured: config.source_id.get(),
            });
        }

        let minted = record
            .entries
            .iter()
            .map(|entry| (entry.symbol, entry.instrument_id))
            .collect();
        let sets = config
            .shards
            .iter()
            .map(|_| PublishedSet {
                published: 0,
                manifest_seq: 0,
                pacer: DefinitionPacer::new(config.schedule),
                cursor: 0,
            })
            .collect();
        Ok(Self {
            store,
            clock,
            minted,
            next_id: record.next_id,
            handles: HashMap::new(),
            slots: Vec::new(),
            published: 0,
            instruments: InstrumentTable::new(),
            sets,
            phase: Phase::Seeding,
            counts: Counts::default(),
            unknown_shards: Vec::new(),
            unknown_shards_taken: 0,
            last_refusal: None,
            fault: None,
            config,
        })
    }

    /// The lowering's view of the admitted set.
    ///
    /// Handed out by reference because this registry is the only thing that
    /// admits: a second path into the table would be a second `Instrument ID`
    /// minter, and the identity is the one thing there can only be one of.
    #[must_use]
    pub const fn instruments(&self) -> &InstrumentTable {
        &self.instruments
    }

    /// How many instruments this publisher has published, across every shard.
    ///
    /// The number the selection policy's caps are measured against, and not the
    /// `Instrument Count` any channel carries — that is
    /// [`published_on`](Self::published_on). The caps are stated once for the
    /// process and the counts are reported per channel, which is the pair that
    /// lets an operator watch a shard consume headroom that is not per shard.
    #[must_use]
    pub const fn published(&self) -> usize {
        self.published
    }

    /// The `Instrument Count` one shard's channel carries.
    ///
    /// `None` for a shard this registry was not configured with, which is the
    /// answer to a question about a channel that does not exist. `Some(0)`
    /// says the shard exists and holds nothing — and paired with a
    /// [`manifest_seq`](Self::manifest_seq) of 0 it says nothing was ever
    /// admitted to it, which is a different operator problem from a shard
    /// everything was withdrawn from.
    #[must_use]
    pub fn published_on(&self, shard: &str) -> Option<usize> {
        Some(self.sets[self.shard_index(shard)?].published)
    }

    /// One shard's `Manifest Seq`.
    ///
    /// Per shard because the specification defines it per channel: it
    /// increments every time the published instrument set changes *on this
    /// channel*. A process-wide one would advance on a quiet channel for an
    /// admission its subscribers cannot see, and each of them would re-check a
    /// set that had not changed.
    ///
    /// Zero until the shard's first published set exists, which is the value a
    /// subscriber only ever sees paired with `Valid` at 0. `None` for a shard
    /// this registry was not configured with.
    #[must_use]
    pub fn manifest_seq(&self, shard: &str) -> Option<u16> {
        Some(self.sets[self.shard_index(shard)?].manifest_seq)
    }

    /// The `Valid` flag for one shard: whether its published set is
    /// established.
    ///
    /// False while seeding and false again from the start of shutdown, which is
    /// the codec's own definition of the field — 1 once the published set is
    /// established, 0 while uninitialized or shutting down. It is not a health
    /// signal: a channel whose instruments are all dormant is silent and valid.
    ///
    /// False, too, for a shard this registry was not configured with. That is
    /// the honest answer rather than a convenience: a channel with no published
    /// set behind it has not established one.
    #[must_use]
    pub fn is_valid(&self, shard: &str) -> bool {
        self.shard_index(shard).is_some() && matches!(self.phase, Phase::Established)
    }

    #[must_use]
    pub const fn phase(&self) -> Phase {
        self.phase
    }

    #[must_use]
    pub const fn counts(&self) -> Counts {
        self.counts
    }

    /// The most recent refusal, for the log line that goes with the count.
    #[must_use]
    pub const fn last_refusal(&self) -> Option<Refusal> {
        self.last_refusal
    }

    /// Whether the published count is above the policy's warning threshold.
    ///
    /// The threshold sits below the cap so that an operator hears about the
    /// headroom being consumed while there is still headroom.
    #[must_use]
    pub const fn warns(&self) -> bool {
        self.config.selection.warns_at(self.published)
    }

    /// The state fault, if the persisted record has stopped being writable.
    ///
    /// Reported rather than acted on. A publisher that cannot persist a minted
    /// ID must stop minting, which this registry does on its own; whether the
    /// process should then end is the runtime's decision, because the
    /// alternative to a fault-and-continue is a feed that goes dark over an
    /// instrument nobody had asked for yet.
    #[must_use]
    pub fn fault(&self) -> Option<&StateError> {
        self.fault.as_ref()
    }

    /// The seeding phase is over: the first poll has returned.
    ///
    /// Process-wide, across every shard, because one poll is what the venue
    /// answered with: an adapter offers its whole universe and the shards it
    /// named are the shards that have members. A per-shard `seeding_complete`
    /// would be waiting for a second statement the boundary never makes.
    ///
    /// Two things change, and they change together because they are the same
    /// statement. The published set is established, so the manifest becomes
    /// `Valid`; and the seed limit gives way to the cap, so the headroom the
    /// cap leaves above the seed becomes available to instruments the venue
    /// lists later. Calling it before the first poll has finished would spend
    /// that headroom on the venue's opening offer, which is what the two limits
    /// exist to prevent.
    pub fn seeding_complete(&mut self) {
        if matches!(self.phase, Phase::Seeding) {
            self.phase = Phase::Established;
        }
    }

    /// The publisher is going down.
    ///
    /// `Valid` returns to 0 and nothing further is admitted, so that no ID is
    /// minted and persisted for an instrument no definition cycle will publish.
    /// The published set stays as it is: it is still what the last manifest
    /// described, and the shutdown itself is announced by `EndOfSession`.
    pub fn begin_shutdown(&mut self) {
        self.phase = Phase::ShuttingDown;
    }

    /// The definition of a published instrument, as it would go on the wire
    /// now.
    ///
    /// Stamped with its **own shard's** `Manifest Seq`, because that is the
    /// manifest a subscriber to the channel it goes out on is reconciling
    /// against. Stamping another shard's would hand a subscriber a definition
    /// belonging to a manifest it has never seen.
    #[must_use]
    pub fn definition(&self, instrument: InstrumentRef) -> Option<InstrumentDefinition> {
        let entry = self.slot(instrument)?;
        Some(definition::stamped(
            &entry.definition,
            self.sets[entry.shard].manifest_seq,
        ))
    }

    /// The definitions one shard's tick owes, paced.
    ///
    /// `out` is cleared and filled, so a caller keeps one buffer for the life of
    /// the process. The count is that shard's [`DefinitionPacer`]'s and is
    /// capped, so a caller cannot obtain the whole published set in one call
    /// however it arranges its loop — the rule that publishers must not emit the
    /// entire published set as a single burst is kept here rather than asked of
    /// the caller.
    ///
    /// **Called once per shard per tick, and the buffer is then packed onto
    /// every feed of that shard.** Per shard rather than per feed because the
    /// pacer is per shard: asking it once per feed would ask for the lap's debt
    /// twice and emit twice as much of the set, which is the burst arriving
    /// through the caller. A shard this registry was not configured with owes
    /// nothing and clears the buffer, so a caller iterating a stale shard list
    /// emits nothing rather than another shard's set.
    ///
    /// The cycle continues while shutting down and while seeding. A definition
    /// is publishable the moment it composes, and a subscriber joining during
    /// the seed collects definitions it can already use; what the manifest's
    /// `Valid` flag tells it is whether the *set* is final yet.
    pub fn definition_tick(&mut self, shard: &str, out: &mut Vec<InstrumentDefinition>) {
        out.clear();
        let Some(index) = self.shard_index(shard) else {
            return;
        };
        let now_ns = self.clock.monotonic_ns();
        let published = self.sets[index].published;
        let due = self.sets[index].pacer.due(now_ns, published);
        if due == 0 {
            return;
        }
        let manifest_seq = self.sets[index].manifest_seq;
        let mut cursor = self.sets[index].cursor;
        let slots = self.slots.len();
        while out.len() < due {
            let before = out.len();
            for _ in 0..slots {
                if out.len() == due {
                    break;
                }
                // Every slot is walked and only this shard's are emitted. The
                // slots are shared because an `Instrument ID` is, and a lap
                // that emitted another shard's definition would put it on a
                // channel whose manifest does not count it.
                if let Some(entry) = &self.slots[cursor] {
                    if entry.shard == index {
                        out.push(definition::stamped(&entry.definition, manifest_seq));
                    }
                }
                cursor = (cursor + 1) % slots;
            }
            // A pass over every slot that emitted nothing cannot be repeated
            // into progress. Reachable only if the published count and the
            // slots disagree, and an infinite loop in the emit path is a
            // publisher that goes dark rather than a defect somebody notices.
            if out.len() == before {
                break;
            }
        }
        self.sets[index].cursor = cursor;
        self.counts.definitions_emitted += out.len() as u64;
    }

    /// One shard's manifest, as of now.
    ///
    /// `None` for a shard this registry was not configured with: there is no
    /// truthful `Channel ID` to state for a channel it has none of, and
    /// composing one from another shard's would describe the wrong feed.
    ///
    /// `Channel ID` is set from configuration even though a builder-framed
    /// message has it stamped from the datagram header afterwards: a caller
    /// that encodes one without a builder still gets a truthful field, and a
    /// caller that uses a builder cannot end up with two different answers.
    /// Where a shard carries two feed specifications, one composed summary is
    /// truthful on both of its refdata ports, because both describe the one
    /// published set and each datagram stamps its own header.
    #[must_use]
    pub fn manifest(&self, shard: &str) -> Option<ManifestSummary> {
        let index = self.shard_index(shard)?;
        Some(ManifestSummary {
            channel_id: self.config.shards[index].channel_id,
            valid: u8::from(self.is_valid(shard)),
            manifest_seq: self.sets[index].manifest_seq,
            // Saturating rather than truncating: a published set larger than a
            // u32 is unreachable through a policy whose cap is a `usize` an
            // operator sets, and a wrapped count would read as a small feed.
            instrument_count: u32::try_from(self.sets[index].published).unwrap_or(u32::MAX),
            timestamp_ns: self.clock.unix_ns(),
        })
    }

    /// The unknown shard names offered since this was last called.
    ///
    /// Each distinct name once, in the order it was first seen, for the log
    /// line the runtime writes — this crate writes none. Once per **distinct
    /// value** rather than once per offer, because an adapter may re-offer its
    /// whole set every second and a line per offer would bury the first one.
    /// Nothing is reported twice, so a caller that logs whatever it gets back
    /// cannot repeat itself; see [`MAX_REPORTED_UNKNOWN_SHARDS`] for what
    /// happens to a venue that invents names without bound.
    pub fn take_unknown_shards(&mut self) -> Vec<String> {
        let taken = self.unknown_shards[self.unknown_shards_taken..].to_vec();
        self.unknown_shards_taken = self.unknown_shards.len();
        taken
    }

    /// Offer an instrument on a shard, and report why it was declined.
    ///
    /// [`ListingSink::list_on`] is this without the reason. An adapter is given
    /// the `Option`, because the boundary carries no vocabulary for a refusal
    /// and a venue can act on none of them; the runtime wiring the registry up
    /// gets this one, because it can.
    ///
    /// The shard is resolved before anything else is considered, including
    /// whether the symbol is already published: a name this publisher has no
    /// channel for is a statement it cannot honour whatever the instrument is.
    ///
    /// # Errors
    ///
    /// Every [`Refusal`]. [`Refusal::Capped`] is ordinary; the rest are not.
    pub fn offer(
        &mut self,
        shard: &str,
        spec: &InstrumentSpec<'_>,
    ) -> Result<InstrumentRef, Refusal> {
        let Some(index) = self.shard_index(shard) else {
            self.remember_unknown_shard(shard);
            self.count_refusal(Refusal::UnknownShard);
            return Err(Refusal::UnknownShard);
        };
        let (symbol, _fit) = definition::symbol_field(spec.symbol);
        if let Some(&handle) = self.handles.get(&symbol) {
            self.reoffer(handle, index, spec);
            return Ok(handle);
        }
        self.admit(symbol, index, spec).inspect_err(|&refusal| {
            self.count_refusal(refusal);
        })
    }

    /// A symbol that is already published.
    ///
    /// Cheap, and it has to be: the boundary promises an adapter may offer its
    /// whole set on every poll without tracking what it has already offered. So
    /// this is one hash lookup, one composition — arithmetic on stack values,
    /// no allocation and no I/O — and a comparison. Only a definition that has
    /// actually changed touches anything.
    fn reoffer(&mut self, handle: InstrumentRef, shard: usize, spec: &InstrumentSpec<'_>) {
        let Some(entry) = self.slot(handle).copied() else {
            return;
        };
        // Checked before the definition is composed, because it is not a
        // question about the definition. The instrument stays on the shard it
        // was admitted to and the restatement is counted; moving it would be a
        // channel change no message in the family can announce.
        if entry.shard != shard {
            self.count_refusal(Refusal::ShardRestated);
            return;
        }
        let current = entry.definition;
        let composed = match definition::compose(spec, current.instrument_id, self.config.source_id)
        {
            Ok(composed) => composed,
            Err(refusal) => {
                // The venue has restated something we cannot represent. The
                // last good definition stands and the refusal is counted:
                // withdrawing a live instrument over a restated tick size would
                // tell every subscriber holding its book that the instrument
                // had ended, which is a far larger claim than the one the
                // venue actually made.
                self.count_refusal(refusal);
                return;
            }
        };
        // A restated exponent or contract factor is refused rather than
        // published. Those three numbers are the ones the lowering holds and
        // converts every price and quantity against, and the table admits no
        // replacement in place — so accepting the restatement would publish a
        // definition declaring one scale while every quote for the instrument
        // went out at the other. Self-consistent on each side and invisible to
        // any test that encodes and decodes, which is the exact failure shape
        // this crate family is built against. Re-admitting instead would hand
        // the adapter's live handle to a different slot.
        //
        // Checked ahead of the comparison below, because the contract factor is
        // the one of the three the definition does not carry: a venue that
        // changed the factor and restated its tick and lot to match would
        // compose byte-identical definitions, and the change would be invisible
        // in the only place a subscriber could see it.
        if composed.instrument != *self.instruments.get(handle).expect("published") {
            self.count_refusal(Refusal::ScaleRestated);
            return;
        }
        if definition::same_definition(&current, &composed.definition) {
            return;
        }
        self.count_fits(composed.fits);
        if let Some(slot) = self.slot_mut(handle) {
            slot.definition = composed.definition;
        }
        // The published content changed, so the manifest a subscriber is
        // reconciling against has too — this shard's, and no other's.
        self.advance_manifest(shard);
    }

    /// A symbol that has never been published in this process.
    fn admit(
        &mut self,
        symbol: SymbolKey,
        shard: usize,
        spec: &InstrumentSpec<'_>,
    ) -> Result<InstrumentRef, Refusal> {
        if matches!(self.phase, Phase::ShuttingDown) {
            return Err(Refusal::ShuttingDown);
        }
        if self.fault.is_some() {
            return Err(Refusal::Unpersistable);
        }
        if self.published >= self.config.selection.limit(self.phase) {
            return Err(Refusal::Capped);
        }

        // The ID this instrument would get, resolved before anything is
        // committed. A symbol that has been admitted before - in an earlier
        // run, or before a delisting - keeps the ID it was published under; a
        // new one takes the next, and only if the definition composes.
        let recalled = self.minted.get(&symbol).copied();
        let instrument_id = recalled.unwrap_or(self.next_id);
        if instrument_id == 0 {
            return Err(Refusal::IdSpaceExhausted);
        }
        let composed = definition::compose(spec, instrument_id, self.config.source_id)?;

        if recalled.is_none() {
            let next_id = self
                .next_id
                .checked_add(1)
                .ok_or(Refusal::IdSpaceExhausted)?;
            // Persisted before it is admitted, and a failure to persist admits
            // nothing: an `Instrument ID` published from memory and absent from
            // the record is one that resolves to nothing after a restart, and
            // one that the next run will hand to a different instrument.
            self.persist(instrument_id, symbol, next_id)?;
            self.minted.insert(symbol, instrument_id);
            self.next_id = next_id;
        }

        let handle = self.instruments.admit(composed.instrument);
        let index = handle.index() as usize;
        if index >= self.slots.len() {
            self.slots.resize(index + 1, None);
        }
        self.slots[index] = Some(Published {
            symbol,
            shard,
            definition: composed.definition,
        });
        self.handles.insert(symbol, handle);
        self.published += 1;
        self.sets[shard].published += 1;
        self.counts.admitted += 1;
        self.count_fits(composed.fits);
        self.advance_manifest(shard);
        Ok(handle)
    }

    /// Write the record this admission would produce, before relying on it.
    ///
    /// The whole record, every time, because the write has to be atomic against
    /// a reader and a rename of a whole file is what makes it so. The cost is
    /// one rewrite per instrument the venue has *never* listed before, sized by
    /// everything it has ever listed — so a re-offer writes nothing, a
    /// delisting writes nothing, and the steady state is no writes at all. What
    /// pays it is a first start, once, bounded by the policy's cap.
    fn persist(
        &mut self,
        instrument_id: u32,
        symbol: SymbolKey,
        next_id: u32,
    ) -> Result<(), Refusal> {
        let mut entries: Vec<Entry> = self
            .minted
            .iter()
            .map(|(&symbol, &instrument_id)| Entry {
                instrument_id,
                symbol,
            })
            .collect();
        entries.push(Entry {
            instrument_id,
            symbol,
        });
        let record = StateRecord {
            source_id: self.config.source_id.get(),
            next_id,
            entries,
        };
        match self.store.store(&record.encode()) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.fault = Some(error);
                Err(Refusal::Unpersistable)
            }
        }
    }

    /// Withdraw a published instrument.
    fn withdraw(&mut self, instrument: InstrumentRef) {
        let Some(index) = self.published_index(instrument) else {
            return;
        };
        let entry = self.slots[index].expect("checked above");
        self.slots[index] = None;
        self.handles.remove(&entry.symbol);
        self.instruments.withdraw(instrument);
        self.published -= 1;
        self.sets[entry.shard].published -= 1;
        self.counts.delisted += 1;
        // The persisted entry stays. It is what stops the ID being minted for
        // something else, and what gives the symbol its own ID back if the
        // venue relists it - so a delisting writes nothing, which is why it
        // cannot fail.
        self.advance_manifest(entry.shard);
    }

    /// One shard's published set has changed, so the manifest describing it
    /// must.
    ///
    /// That shard's and no other's: a subscriber whose manifest sequence
    /// advanced for an admission on a channel it is not bound to would re-check
    /// its set, find it unchanged, and do so again on the next unrelated
    /// admission anywhere in the process.
    ///
    /// Wraps to 1 rather than to 0, because 0 is the value a subscriber sees
    /// only alongside `Valid` at 0. Reaching it again in flight would make an
    /// established manifest indistinguishable from one that has never been
    /// established.
    fn advance_manifest(&mut self, shard: usize) {
        let seq = &mut self.sets[shard].manifest_seq;
        *seq = seq.checked_add(1).unwrap_or(1);
    }

    /// Which published set a shard name resolves to.
    ///
    /// A scan rather than a map: the shards are the enabled `[[feed]]` blocks
    /// of one process, this is not on the datagram path, and an index is what
    /// the published entries hold so that the hot path compares integers.
    fn shard_index(&self, shard: &str) -> Option<usize> {
        self.config
            .shards
            .iter()
            .position(|configured| configured.name == shard)
    }

    /// Remember an unknown shard name, once, for the caller that logs it.
    fn remember_unknown_shard(&mut self, shard: &str) {
        if self.unknown_shards.len() >= MAX_REPORTED_UNKNOWN_SHARDS
            || self.unknown_shards.iter().any(|seen| seen == shard)
        {
            return;
        }
        self.unknown_shards.push(shard.to_string());
    }

    fn count_refusal(&mut self, refusal: Refusal) {
        self.last_refusal = Some(refusal);
        // Written out rather than split on `is_ordinary`, so that a refusal
        // added later cannot land in a bucket by default and be reported as
        // something it is not.
        match refusal {
            Refusal::Capped => self.counts.declined_at_cap += 1,
            Refusal::UnknownShard => self.counts.declined_unknown_shard += 1,
            Refusal::ShardRestated => self.counts.declined_shard_restated += 1,
            Refusal::ContractSize
            | Refusal::Field(_)
            | Refusal::ScaleRestated
            | Refusal::IdSpaceExhausted
            | Refusal::Unpersistable
            | Refusal::ShuttingDown => self.counts.declined_unrepresentable += 1,
        }
    }

    fn count_fits(&mut self, fits: Fits) {
        if !fits.all_fitted() {
            self.counts.imperfect_symbols += 1;
        }
    }

    fn published_index(&self, instrument: InstrumentRef) -> Option<usize> {
        let index = instrument.index() as usize;
        self.slots.get(index)?.is_some().then_some(index)
    }

    /// Which shard's published set an instrument is in, as an index into the
    /// configured shards.
    ///
    /// The one thing a caller cannot work out for itself: the shard is recorded
    /// on the published entry, and the entry is this registry's. A caller that
    /// tried to keep its own map would be keeping a second answer to *where
    /// does this instrument publish*, and the two would disagree the first time
    /// an instrument was withdrawn.
    ///
    /// `None` for a handle with no published entry — forged, or outliving its
    /// instrument's withdrawal. That is the same set of handles
    /// [`InstrumentTable::holds`] answers `false` for, because a withdrawal
    /// clears both.
    ///
    /// [`InstrumentTable::holds`]: dz_publisher_lowering::InstrumentTable::holds
    #[must_use]
    pub fn shard_of(&self, instrument: InstrumentRef) -> Option<usize> {
        self.slot(instrument).map(|published| published.shard)
    }

    fn slot(&self, instrument: InstrumentRef) -> Option<&Published> {
        self.slots.get(instrument.index() as usize)?.as_ref()
    }

    fn slot_mut(&mut self, instrument: InstrumentRef) -> Option<&mut Published> {
        self.slots.get_mut(instrument.index() as usize)?.as_mut()
    }
}

impl<S: StateStore, C: Clock> ListingSink for Registry<S, C> {
    fn list_on(&mut self, shard: &str, spec: &InstrumentSpec<'_>) -> Option<InstrumentRef> {
        self.offer(shard, spec).ok()
    }

    fn delist(&mut self, instrument: InstrumentRef) {
        self.withdraw(instrument);
    }
}
