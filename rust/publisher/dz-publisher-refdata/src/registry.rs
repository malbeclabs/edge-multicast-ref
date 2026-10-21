//! The reference-data owner: who an instrument is, and how a subscriber comes
//! to know it.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use dz_adapter_core::{InstrumentRef, InstrumentSpec, ListingSink, Strike, DEFAULT_SHARD};
use dz_edge_core::AppMessage;
use dz_edge_refdata::{InstrumentDefinition, ManifestSummary, StrikeInterval, SYMBOL_LEN};
use dz_publisher_lowering::{InstrumentTable, SourceId};

use crate::clock::Clock;
use crate::definition::{self, Fits};
use crate::error::RefdataError;
use crate::pacer::DefinitionPacer;
use crate::policy::{Phase, SelectionPolicy};
use crate::refusal::Refusal;
use crate::state::{
    derive_instrument_id, derive_instrument_id_from_key, encode_keyed_line, encode_line,
    is_venue_key, symbol_is_whole, venue_key_field, Entry, IdAllocation, Identity, StateRecord,
    VenueKeys,
};
use crate::store::{StateError, StateStore};
use crate::strike;
use crate::CycleSchedule;

/// A venue's ticker as the wire carries it.
type SymbolKey = [u8; SYMBOL_LEN];

/// What an instrument is recalled by: the 64 bytes of its padded `Symbol`
/// under [`Identity::Symbol`], and its venue key under
/// [`Identity::VenueKey`]. Boxed so that a re-offer looks it up by `&[u8]`
/// with no allocation, and the two identities share one set of maps.
type RecallKey = Box<[u8]>;

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
    /// `[refdata] forget_delisted_after`: how long after it was last published
    /// a delisted instrument's entry is kept. `None` keeps every entry for
    /// good.
    ///
    /// **A symbol relisted after it has been forgotten is minted a new
    /// `Instrument ID`**, and a subscriber that kept the old one sees an
    /// instrument end and a different one begin. The configuration refuses
    /// anything under a second; a caller composing this itself gets whole
    /// seconds, rounded up, so the horizon is never shorter than stated.
    pub forget_delisted_after: Option<Duration>,
    /// `[refdata] id_allocation`: how a symbol the record does not hold is
    /// given its `Instrument ID`. See [`IdAllocation`] for what each promises,
    /// and for the one rule a derived channel's paths must keep between them.
    ///
    /// [`Derived`](IdAllocation::Derived) forgets nothing, so it is refused
    /// together with [`forget_delisted_after`](Self::forget_delisted_after).
    pub id_allocation: IdAllocation,
    /// `[refdata] identity`: what an instrument is recalled by. See
    /// [`Identity`].
    pub identity: Identity,
    /// `[refdata] venue_keys`: whether the adapter states keys of its own.
    /// [`Stated`](VenueKeys::Stated) is refused under
    /// [`Identity::Symbol`], where there is no key to state.
    pub venue_keys: VenueKeys,
}

/// An offer declined because the `Instrument ID` its symbol derives is not
/// available, and what holds it.
///
/// Handed to the runtime by [`Registry::take_unavailable_ids`] for the line
/// that goes with the count. Both symbols, because the declined one alone says
/// nothing an operator can act on: the pair is what says whether this is a
/// collision between two live instruments or one against a symbol long gone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnavailableId {
    /// The symbol that was declined, as the wire would have carried it.
    pub symbol: [u8; SYMBOL_LEN],
    /// Under [`Identity::VenueKey`], the venue key that was declined, beside
    /// its `Symbol`. `None` under [`Identity::Symbol`].
    pub venue_key: Option<Box<[u8]>>,
    /// Under [`Identity::VenueKey`], the venue key holding the ID, where one
    /// does. `None` under [`Identity::Symbol`].
    pub holder_venue_key: Option<Box<[u8]>>,
    /// The ID it derives.
    pub instrument_id: u32,
    /// The symbol the record holds under that ID. `None` when no symbol does:
    /// the ID is `0`, or below the floor and never issued. One below the floor
    /// that a seeded symbol holds names it.
    pub holder: Option<[u8; SYMBOL_LEN]>,
    /// Whether the ID is below the floor, `0` included. Such an ID is closed to
    /// every publisher started from the same seed, so the decline does not
    /// depend on the order anything was admitted in. One at or above it is
    /// held, and a publisher that admitted the two symbols in the other order
    /// holds it under this one instead.
    pub below_floor: bool,
}

/// The fewest appended lines that set off a compaction while running.
///
/// The threshold is otherwise the number of retained entries, which is what
/// makes a mint pay for at most one rewritten line. The floor is so that a
/// record of a handful of entries is not rewritten every handful of mints.
pub const COMPACTION_FLOOR: usize = 1_024;

/// One minted `Instrument ID`, and what the forgetting needs to know about it.
#[derive(Debug, Clone, Copy)]
struct Minted {
    instrument_id: u32,
    /// The wire `Symbol` it was published under, which the record writes
    /// beside the key. Under [`Identity::Symbol`] it is the key.
    symbol: SymbolKey,
    /// Whether the record holds this entry as published when written. A
    /// relisting of one it does not is written down, so that a timestamp in
    /// the record is never older than the instrument's last publication.
    recorded_published: bool,
    /// The Unix second this instrument was last published. Read only while it
    /// is not published.
    last_published_s: u64,
}

/// One published instrument.
#[derive(Debug, Clone, Copy)]
struct Published {
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
    /// The strike this instrument publishes after its definition, pending or
    /// fixed. `None` is a contract that pays on no strike.
    strike: Option<StrikeInterval>,
    /// Whether a definition of this instrument has been handed out to send.
    /// From then on a subscriber may hold it, so whether it has a strike is
    /// settled.
    defined: bool,
}

/// One definition a tick owes, with the strike that must follow it in the same
/// datagram.
///
/// The two are handed out together because the supplement allows the strike
/// nowhere else: a caller that sent `definition` and dropped `strike` would
/// publish a contract that pays on a strike as one that does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishedDefinition {
    pub definition: InstrumentDefinition,
    /// `None` for an instrument whose contract pays on no strike.
    pub strike: Option<StrikeInterval>,
}

impl PublishedDefinition {
    /// What this entry takes of a datagram: the definition, and its strike if
    /// it has one, which may not start the next.
    #[must_use]
    pub const fn wire_size(&self) -> usize {
        wire_size(self.strike.is_some())
    }
}

/// Whether two strikes of one instrument state the same interval: the same
/// bounds, present and inclusive alike, at the same exponent.
///
/// `Fixing Time` is left out. It says when the venue fixed the strike and not
/// what the strike is, so two statements that differ only there are not a
/// correction. `strike::compose` gives one value one exponent, so equal
/// intervals compare equal here however the venue wrote their digits.
const fn same_interval(left: &StrikeInterval, right: &StrikeInterval) -> bool {
    left.strike_exponent == right.strike_exponent
        && left.bound_flags == right.bound_flags
        && left.lower_bound == right.lower_bound
        && left.upper_bound == right.upper_bound
}

const fn wire_size(has_strike: bool) -> usize {
    if has_strike {
        InstrumentDefinition::SIZE + StrikeInterval::SIZE
    } else {
        InstrumentDefinition::SIZE
    }
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
    /// Whether a poll in progress has changed this set. The poll's end
    /// advances `manifest_seq` once for it; see [`Registry::poll`].
    changed: bool,
    pacer: DefinitionPacer,
    /// Where this shard's next lap resumes in [`Registry::slots`]. The slots
    /// are shared, so a cursor is per shard and skips what is not its own.
    cursor: usize,
    /// Published instruments on this shard whose strike is pending.
    strikes_pending: usize,
    /// Instruments whose strike changed after their definition was first
    /// handed out, oldest first. Each is owed a definition and its strike
    /// ahead of its place in the cycle.
    strikes_changed: Vec<InstrumentRef>,
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
    /// Fixed strikes restated as pending. The published strike stood; see
    /// [`Refusal::StrikeUnfixed`]. A strike that cannot be carried or is empty
    /// is counted in [`declined_unrepresentable`](Self::declined_unrepresentable)
    /// with the other statements about an instrument the wire cannot hold.
    pub declined_strike_unfixed: u64,
    /// Strikes stated for an instrument already published without one; see
    /// [`Refusal::StrikePresence`].
    pub declined_strike_presence: u64,
    /// Strikes that changed after their instrument was first defined: a pending
    /// strike the venue fixed, or a fixed one it corrected.
    pub strikes_changed: u64,
    /// Offers declined because the ID their symbol derives is unavailable; see
    /// [`Refusal::IdUnavailable`]. Zero under sequential allocation.
    pub declined_id_unavailable: u64,
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
///   something else. `next_id` is what guarantees it, and nothing lowers it.
/// - A relisted symbol gets its own ID back, since a symbol is the identity —
///   unless it was delisted for longer than
///   [`forget_delisted_after`](RegistryConfig::forget_delisted_after), when
///   its entry has been forgotten and it is minted a new one. A forgotten
///   entry belongs to an instrument no definition names, so the sentence above
///   still holds.
/// - A mint is one appended line, and the record is rewritten only when the
///   lines appended reach the size of the last base. What a mint costs is
///   therefore constant, and not the size of the venue's history.
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
    /// Every retained `Instrument ID`, by what it is recalled by. A delisting
    /// leaves the entry, which is what lets a relisted instrument come back as
    /// itself; only the horizon removes one. What makes an ID unreusable is
    /// `next_id`.
    minted: HashMap<RecallKey, Minted>,
    next_id: u32,
    /// Whether the directory holds a base that lines can be appended to.
    /// False only before the first mint of a publisher that has never minted.
    base_written: bool,
    /// Entries in the last base, which is what the appended lines are
    /// measured against.
    base_entries: usize,
    /// Lines appended since the last base.
    appended: usize,
    /// The appended lines at which the next compaction is tried. The last
    /// base's size, or further on after a compaction that could not be
    /// written, so a full disk is not asked to take the whole record again on
    /// every mint.
    compaction_at: usize,
    /// The live handle for each published instrument, by what it is recalled
    /// by, so a re-offer is one lookup.
    handles: HashMap<RecallKey, InstrumentRef>,
    /// Parallel to the instrument table's slots, so the definition cycle walks
    /// the published set in admission order and a withdrawn instrument leaves
    /// the same hole in both.
    slots: Vec<Option<Published>>,
    /// Parallel to `slots`: what each published instrument is recalled by,
    /// kept apart so that a published entry stays `Copy` for the cycle.
    slot_keys: Vec<Option<RecallKey>>,
    /// What is published across every shard, which is what the selection
    /// policy's caps are measured against. `max_published` is a cap on what
    /// this publisher publishes and not on what a channel carries, so the
    /// number it is compared against has to be the process's.
    published: usize,
    instruments: InstrumentTable,
    /// One per configured shard, in the order [`RegistryConfig::shards`] states
    /// them, so an index into either is an index into the other.
    sets: Vec<PublishedSet>,
    /// Whether a [`poll`](Self::poll) is in progress, which is what holds a
    /// changed set's `Manifest Seq` until the poll ends.
    polling: bool,
    phase: Phase,
    counts: Counts,
    /// Distinct shard names offered that this publisher has none of, in the
    /// order they were first seen, and how many of them a caller has been
    /// handed. An adapter re-offers its whole set every poll, so a name is
    /// remembered rather than reported again.
    unknown_shards: Vec<String>,
    unknown_shards_taken: usize,
    /// Under derived allocation, every `Instrument ID` the record holds and
    /// what holds it, so a derived ID already taken is one lookup and the
    /// declined offer can name its holder. Keyed by ID, the direction that
    /// question needs. Empty under sequential allocation, where `next_id`
    /// alone is the guarantee.
    in_use: HashMap<u32, RecallKey>,
    /// Offers declined as [`Refusal::IdUnavailable`], one per symbol, and how
    /// many a caller has been handed. Bounded as the unknown shard names are.
    unavailable: Vec<UnavailableId>,
    unavailable_taken: usize,
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

/// The most distinct declined symbols one process names. A collision is rare,
/// so reaching this says something else is wrong, and the count goes on.
const MAX_REPORTED_UNAVAILABLE_IDS: usize = 64;

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

        // Like the shard set, a configuration that cannot be served is refused
        // before the directory is claimed.
        if config.venue_keys == VenueKeys::Stated && config.identity == Identity::Symbol {
            return Err(RefdataError::VenueKeysWithoutVenueKey);
        }
        let keyed = config.identity == Identity::VenueKey;
        if config.id_allocation == IdAllocation::DerivedKey && config.identity == Identity::Symbol {
            return Err(RefdataError::DerivedKeyWithoutVenueKey);
        }
        if config.id_allocation.is_derived() && config.forget_delisted_after.is_some() {
            return Err(RefdataError::ForgettingUnderDerivedIds);
        }

        match store.claim() {
            Ok(()) => {}
            Err(StateError::AlreadyHeld) => return Err(RefdataError::StateHeldByAnotherWriter),
            Err(error) => return Err(RefdataError::State(error)),
        }
        let loaded = match store.load().map_err(RefdataError::State)? {
            None => None,
            Some(bytes) => Some(StateRecord::load(&bytes)?),
        };
        let record = loaded.as_ref().map_or_else(
            || StateRecord::empty(config.source_id.get()),
            |loaded| loaded.record.clone(),
        );
        if record.source_id != config.source_id.get() {
            return Err(RefdataError::StateBelongsToAnotherSource {
                persisted: record.source_id,
                configured: config.source_id.get(),
            });
        }
        // A derived record cannot be continued sequentially. A sequential one
        // opened under derived allocation keeps its `next_id` as the floor and
        // is rewritten in the derived layout below, which is what seeding is.
        if record.allocation.is_derived() && config.id_allocation == IdAllocation::Sequential {
            return Err(RefdataError::StateIsDerived);
        }
        // Every derived ID above the floor would change.
        if record.allocation.is_derived()
            && config.id_allocation.is_derived()
            && record.allocation != config.id_allocation
        {
            return Err(RefdataError::DerivationChanged {
                persisted: record.allocation,
                configured: config.id_allocation,
            });
        }
        match (record.keyed, keyed) {
            // Its entries may share a Symbol, which a symbol-keyed registry
            // cannot hold.
            (Some(_), false) => return Err(RefdataError::StateIsKeyed),
            (Some(persisted), true) if persisted != config.venue_keys => {
                return Err(RefdataError::VenueKeysChanged {
                    persisted,
                    configured: config.venue_keys,
                });
            }
            // An unkeyed record, opened by a keyed publisher. An entry whose
            // key is not known is not guessed at: giving its ID to whichever
            // key is offered first depends on what this host saw first.
            (None, true) => {
                let unresolved: Vec<(u32, SymbolKey)> = record
                    .entries
                    .iter()
                    .filter(|entry| {
                        config.venue_keys == VenueKeys::Stated || !symbol_is_whole(&entry.symbol)
                    })
                    .map(|entry| (entry.instrument_id, entry.symbol))
                    .collect();
                if !unresolved.is_empty() {
                    return Err(RefdataError::UnresolvedVenueKeys {
                        entries: unresolved,
                    });
                }
            }
            (Some(_) | None, _) => {}
        }
        let recall_key = |entry: &Entry| -> RecallKey {
            if keyed {
                entry.key().into()
            } else {
                entry.symbol.to_vec().into_boxed_slice()
            }
        };
        let in_use: HashMap<u32, RecallKey> = if config.id_allocation.is_derived() {
            record
                .entries
                .iter()
                .map(|entry| (entry.instrument_id, recall_key(entry)))
                .collect()
        } else {
            HashMap::new()
        };

        // An entry recorded as published was published at the last write and
        // may have gone on being published until the process stopped. When is
        // not known; the open is later than both, so it is the answer that
        // can only keep an entry longer.
        let opened_s = stamp_s(&clock);
        let minted = record
            .entries
            .iter()
            .map(|entry| {
                (
                    recall_key(entry),
                    Minted {
                        instrument_id: entry.instrument_id,
                        symbol: entry.symbol,
                        recorded_published: entry.delisted_at.is_none(),
                        last_published_s: entry.delisted_at.unwrap_or(opened_s),
                    },
                )
            })
            .collect();
        let sets = config
            .shards
            .iter()
            .map(|_| PublishedSet {
                published: 0,
                manifest_seq: 0,
                changed: false,
                pacer: DefinitionPacer::new(config.schedule),
                cursor: 0,
                strikes_pending: 0,
                strikes_changed: Vec::new(),
            })
            .collect();
        let mut registry = Self {
            store,
            clock,
            minted,
            next_id: record.next_id,
            base_written: loaded.is_some(),
            base_entries: loaded.as_ref().map_or(0, |loaded| loaded.base),
            appended: loaded.as_ref().map_or(0, |loaded| loaded.appended),
            compaction_at: loaded
                .as_ref()
                .map_or(0, |loaded| loaded.base)
                .max(COMPACTION_FLOOR),
            handles: HashMap::new(),
            slots: Vec::new(),
            slot_keys: Vec::new(),
            published: 0,
            instruments: InstrumentTable::new(),
            sets,
            polling: false,
            phase: Phase::Seeding,
            counts: Counts::default(),
            unknown_shards: Vec::new(),
            unknown_shards_taken: 0,
            in_use,
            unavailable: Vec::new(),
            unavailable_taken: 0,
            last_refusal: None,
            fault: None,
            config,
        };
        // Rewritten here only when it has to be, or when it is owed: a layout
        // this build does not append to has to be; appended lines past the
        // threshold, or an entry to forget, are a compaction. A compaction
        // that cannot be written leaves the record as it is, which reads back
        // whole and takes the next mint's line: a disk too full for the whole
        // record starts and serves every `Instrument ID` it holds. A torn final
        // line, which the next append would otherwise run on from, goes with
        // the rewrite, or is cut off when there is none: cutting needs no free
        // space, and a torn line is what an append that ran the disk out of
        // room leaves.
        if let Some(loaded) = &loaded {
            let rewritten = if loaded.needs_rewrite(registry.config.identity)
                || loaded.record.allocation != registry.config.id_allocation
            {
                let forgetting = registry.forgettable(now_s(&registry.clock));
                registry
                    .write_base(None, &forgetting)
                    .map_err(RefdataError::State)?;
                true
            } else {
                registry.compact(true).map_err(RefdataError::State)?
            };
            if !rewritten && loaded.torn {
                registry
                    .store
                    .truncate(loaded.complete)
                    .map_err(RefdataError::State)?;
            }
        }
        Ok(registry)
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
    /// It advances by one for each [`poll`](Self::poll) that changed the
    /// shard's set, however many instruments that poll admitted, withdrew or
    /// restated. The field says the set changed, and not how many changes went
    /// into it.
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
    ///
    /// # What the shared slot table costs a tick, measured
    ///
    /// The slots are one table for the process, because an `Instrument ID` is,
    /// so a shard's tick walks past the slots of shards that are not its own
    /// and the search for the next definition is linear in the number of
    /// shards rather than in one. The snapshot rotation says the same of
    /// itself and for the same reason; this is the definition cycle's half of
    /// it, and a shard count is sized against both of them or against neither.
    ///
    /// **The cursor is what makes the walk affordable, and that is arithmetic
    /// rather than a hope.** It persists across ticks and a shard emits its
    /// whole published set exactly once a lap, so over one lap the cursor goes
    /// round the table exactly once — `slots` visits per shard per lap,
    /// whatever order admission interleaved the shards in, and `N * slots`
    /// over the process. With equal shards `slots = N * p`, so the per-lap
    /// work is quadratic in the shard count; divided by the ticks in a lap it
    /// is `N * slots * tick / lap` a tick. The default 30 s cycle laps in 24 s
    /// against a 10 ms runtime tick, which is 2,400 ticks a lap:
    ///
    /// | Shards | Published each | Slots | Walk visits a tick |
    /// |---|---|---|---|
    /// | 1 | 100 | 100 | 0.04 |
    /// | 31 | 100 | 3,100 | 40 |
    /// | 128 | 100 | 12,800 | 683 |
    ///
    /// A visit is an index into a `Vec<Option<_>>`, a discriminant test and a
    /// `usize` comparison, and the walk is entered at all only on the ticks a
    /// shard owes something — one tick in twenty-four at the middle row.
    ///
    /// **The larger term is the name lookup, which is worth knowing before
    /// sizing anything against the walk.** `shard_index` resolves the shard by
    /// scanning the configured shards and comparing strings, and it runs
    /// before `due` is consulted — so it costs N comparisons on *every* tick
    /// rather than on the ticks something is owed, `N^2` a tick over the
    /// process. Timed over a whole lap on one core: 31 shards of 100
    /// instruments cost 1.3 µs a tick, of which 1.1 µs is the name scan and
    /// the remainder the walk; 128 shards of 100 cost 18.6 µs a tick, of which
    /// 15.0 µs is the name scan.
    ///
    /// **Both are written down rather than removed.** 1.3 µs is thirteen parts
    /// in a hundred thousand of a 10 ms tick, and the ceiling is not far above
    /// it: `Channel ID` is a `u8` and no two feeds may state the same one, so
    /// 256 is the most shards any document can describe, and half that where
    /// every shard carries both feed specifications. What would remove the
    /// walk is a per-shard list of slots, and what that costs is the property
    /// that makes one table right — identity is one thing, so a withdrawn
    /// instrument has to leave one hole rather than two that can disagree.
    /// Neither trade is taken here; if one ever is, the name lookup is the one
    /// to take first, and this note is the reason why.
    pub fn definition_tick(&mut self, shard: &str, out: &mut Vec<PublishedDefinition>) {
        out.clear();
        let Some(index) = self.shard_index(shard) else {
            return;
        };
        let now_ns = self.clock.monotonic_ns();
        let published = self.sets[index].published;
        // The pacer's debt first, without taking it. This runs on the thread
        // that carries ingress, on every tick, and the slots are walked only on
        // a tick that owes something. A tick that owes nothing still tells the
        // pacer the time, which is what starts a lap and ends an empty one.
        let owed = self.sets[index].pacer.owed(now_ns, published);
        if owed == 0 {
            let _ = self.sets[index].pacer.due_at_most(now_ns, published, 0);
            return;
        }
        // A definition with a strike is a larger message than the pacer's
        // ceiling counts, so what the tick's datagrams hold is read off the
        // entries as they are walked: one walk, no further than what is owed.
        let mut room = self.config.schedule.room_behind(0);
        let manifest_seq = self.sets[index].manifest_seq;
        let mut cursor = self.sets[index].cursor;
        let slots = self.slots.len();
        let mut full = false;
        while out.len() < owed && !full {
            let before = out.len();
            for _ in 0..slots {
                if out.len() == owed {
                    break;
                }
                // Every slot is walked and only this shard's are emitted. The
                // slots are shared because an `Instrument ID` is, and a lap
                // that emitted another shard's definition would put it on a
                // channel whose manifest does not count it.
                if let Some(entry) = &mut self.slots[cursor] {
                    if entry.shard == index {
                        if !room.take(wire_size(entry.strike.is_some())) {
                            // The cursor stays on it: it is the next tick's
                            // first, and still owed.
                            full = true;
                            break;
                        }
                        entry.defined = true;
                        out.push(PublishedDefinition {
                            definition: definition::stamped(&entry.definition, manifest_seq),
                            strike: entry.strike,
                        });
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
        // Taken from the pacer only now, and only what was handed out: what the
        // datagrams did not hold stays owed.
        let _ = self.sets[index]
            .pacer
            .due_at_most(now_ns, published, out.len());
        self.counts.definitions_emitted += out.len() as u64;
    }

    /// The definitions one shard owes ahead of the cycle, because their strike
    /// changed: a pending strike the venue fixed, or a fixed one it corrected.
    ///
    /// `out` is cleared and filled, as [`definition_tick`](Self::definition_tick)
    /// fills it, and each entry carries its strike. The supplement asks that a
    /// fixed strike be sent when the venue fixes it and not at the instrument's
    /// place in the cycle, which for a 30-second cycle is up to 30 seconds of a
    /// subscriber trading a contract whose strike it has not been told.
    ///
    /// **`Manifest Seq` does not move.** A strike is not the published set, and
    /// these definitions carry the sequence the shard already had. A caller
    /// sends them behind a `ManifestSummary` stating it, because a subscriber
    /// discards a definition tagged with a sequence it has not been told, and
    /// after an admission the summary is otherwise sent only on its cadence.
    ///
    /// Bounded by what one tick's datagrams hold behind that summary, so a
    /// venue that fixes a thousand strikes in one poll is spread over the ticks
    /// that follow and is not a burst. Where a tick is one datagram, which is
    /// what the runtime configures, that is six at the mandated datagram size:
    /// the summary's 24 bytes are the room a seventh would have needed, and the
    /// batch and its summary are one datagram. A schedule that allows a tick
    /// more datagrams lets the batch run on into them; the summary still leads
    /// the first. What is not taken stays
    /// owed, oldest first. These are extra to the cycle and are not counted
    /// against its lap.
    pub fn strike_changes(&mut self, shard: &str, out: &mut Vec<PublishedDefinition>) {
        out.clear();
        let Some(index) = self.shard_index(shard) else {
            return;
        };
        if self.sets[index].strikes_changed.is_empty() {
            return;
        }
        let waiting = std::mem::take(&mut self.sets[index].strikes_changed);
        // A handle that has since been withdrawn owes nothing, and resolves to
        // nothing: a slot is never used again, so a symbol admitted again has
        // another handle. Whatever still resolves is owed.
        let sizes = waiting
            .iter()
            .filter_map(|handle| self.slot(*handle))
            .map(|entry| wire_size(entry.strike.is_some()));
        let fitting = self
            .config
            .schedule
            .fitting_behind(ManifestSummary::SIZE, sizes);
        let manifest_seq = self.sets[index].manifest_seq;
        let mut kept = Vec::new();
        for handle in waiting {
            let Some(entry) = self.slot(handle) else {
                continue;
            };
            if out.len() == fitting {
                kept.push(handle);
                continue;
            }
            out.push(PublishedDefinition {
                definition: definition::stamped(&entry.definition, manifest_seq),
                strike: entry.strike,
            });
        }
        self.sets[index].strikes_changed = kept;
        self.counts.definitions_emitted += out.len() as u64;
    }

    /// How many published instruments on a shard have a pending strike.
    ///
    /// The number an operator watches at a venue that fixes strikes after
    /// listing: it rises at each listing and returns to zero when the venue has
    /// fixed them. One that stays up is a strike this publisher was never told,
    /// which no refusal counts, because nothing was refused.
    ///
    /// `None` for a shard this registry was not configured with.
    #[must_use]
    pub fn strikes_pending_on(&self, shard: &str) -> Option<usize> {
        Some(self.sets[self.shard_index(shard)?].strikes_pending)
    }

    /// State the strike of a published instrument, and report why it was
    /// refused.
    ///
    /// [`ListingSink::strike`] is this without the reason, as
    /// [`ListingSink::list_on`] is [`offer`](Self::offer) without it.
    ///
    /// A strike with the bounds of the published one changes nothing and costs
    /// a comparison, so an adapter may state every strike on every poll. Its
    /// fixing time is not part of that comparison: one stated for a strike held
    /// without it is kept from then on, and neither that nor a later statement
    /// without it is a change. A strike whose bounds differ replaces the
    /// published one, and if a definition of the instrument has already
    /// been handed out it is owed again at once; see
    /// [`strike_changes`](Self::strike_changes). `Manifest Seq` is not
    /// advanced in either case.
    ///
    /// A handle this registry does not hold is ignored, as
    /// [`ListingSink::delist`] ignores one.
    ///
    /// # Errors
    ///
    /// [`Refusal::StrikeUnrepresentable`], [`Refusal::StrikeEmpty`],
    /// [`Refusal::StrikeUnfixed`] and [`Refusal::StrikePresence`]. Each is
    /// counted, and what the instrument publishes is unchanged, with one
    /// exception: a first statement the wire cannot carry leaves the
    /// instrument with a pending strike, where it had none.
    pub fn state_strike(
        &mut self,
        instrument: InstrumentRef,
        stated: &Strike<'_>,
    ) -> Result<(), Refusal> {
        let Some(entry) = self.slot(instrument).copied() else {
            return Ok(());
        };
        let result = self.restate_strike(instrument, &entry, stated);
        if let Err(refusal) = result {
            self.count_refusal(refusal);
        }
        result
    }

    fn restate_strike(
        &mut self,
        instrument: InstrumentRef,
        entry: &Published,
        stated: &Strike<'_>,
    ) -> Result<(), Refusal> {
        let instrument_id = entry.definition.instrument_id;
        let composed = match strike::compose(stated, instrument_id, self.config.source_id) {
            Ok(composed) => composed,
            Err(refusal) => {
                // The venue said this contract pays on a strike and said it in
                // a way the wire cannot carry. Before the first definition that
                // is still something to publish: a strike, pending. Publishing
                // the instrument bare instead would state that it pays on no
                // strike, and would settle that for the era, so the valid
                // strike the venue states a poll later would be refused too.
                if entry.strike.is_none() && !entry.defined {
                    let source_id = self.config.source_id.get();
                    let pending = StrikeInterval::pending(instrument_id, source_id, 0);
                    self.replace_strike(instrument, entry, pending);
                }
                return Err(refusal);
            }
        };
        match entry.strike {
            // The same interval is the same strike, whatever its fixing time.
            // A venue that gives the time on one of its paths and not on
            // another has stated one strike twice, and sending it again for
            // that would be a correction that corrects nothing, every poll.
            Some(current) if same_interval(&current, &composed) => {
                // The time is kept once it has been stated, and taken when it
                // is first stated for a strike held without one. Neither is a
                // change: it goes out at the instrument's place in the cycle.
                if current.fixing_time_ns == 0 && composed.fixing_time_ns != 0 {
                    if let Some(slot) = self.slot_mut(instrument) {
                        slot.strike = Some(StrikeInterval {
                            fixing_time_ns: composed.fixing_time_ns,
                            ..current
                        });
                    }
                }
                return Ok(());
            }
            Some(current) if !current.is_pending() && composed.is_pending() => {
                return Err(Refusal::StrikeUnfixed);
            }
            None if entry.defined => return Err(Refusal::StrikePresence),
            _ => {}
        }
        self.replace_strike(instrument, entry, composed);
        Ok(())
    }

    /// Hold `strike` for an instrument whose entry was `entry` until now.
    fn replace_strike(
        &mut self,
        instrument: InstrumentRef,
        entry: &Published,
        strike: StrikeInterval,
    ) {
        if let Some(slot) = self.slot_mut(instrument) {
            slot.strike = Some(strike);
        }
        let set = &mut self.sets[entry.shard];
        if entry.strike.is_some_and(|current| current.is_pending()) {
            set.strikes_pending -= 1;
        }
        if strike.is_pending() {
            set.strikes_pending += 1;
        }
        // Before its first definition there is nothing to send again: the
        // strike goes out with that definition, at its place in the cycle.
        if entry.defined {
            self.counts.strikes_changed += 1;
            if !set.strikes_changed.contains(&instrument) {
                set.strikes_changed.push(instrument);
            }
        }
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

    /// Run one listings poll against this registry, and advance `Manifest Seq`
    /// once for each shard whose set the poll changed.
    ///
    /// `drain` is handed the registry as the [`ListingSink`] an adapter's
    /// `poll_listings` takes. Every admission, withdrawal and restated
    /// definition it makes takes effect as it is made; what waits for the end
    /// of the poll is the `Manifest Seq`. A shard the poll changed advances by
    /// exactly one, and a shard it did not change does not advance.
    ///
    /// **One advance per poll, because the unit a subscriber can see is the
    /// manifest and not the admission.** No manifest and no definition leaves
    /// while a poll is running, so the intermediate sets never exist on the
    /// wire. An advance for each of them moved the seq by `k` between two
    /// manifests for a poll of `k` changes, where the specification's rule is
    /// that a set change moves it by one, and the first `Valid` manifest of a
    /// process stated the size of the seeded set rather than 1. Each advance
    /// also costs a subscriber the definitions it has collected.
    ///
    /// A poll that admits an instrument and withdraws it again has changed the
    /// set as far as this is concerned, and advances. It is not worth telling
    /// apart: the `Instrument ID` it used is spent either way.
    ///
    /// # A change made outside a poll
    ///
    /// [`offer`](Self::offer), [`ListingSink::list_on`] and
    /// [`ListingSink::delist`] called on the registry directly, outside this,
    /// advance the shard's `Manifest Seq` at once, each call on its own. There
    /// is no later moment to hold such a change for, and a set that changed
    /// under a seq that did not is the failure the field exists to prevent.
    /// The runtime makes no such call: every change it makes comes through
    /// here.
    ///
    /// # A poll that panics
    ///
    /// The poll ends as `drain` unwinds, exactly as it does when `drain`
    /// returns: the shards changed so far advance, and the registry is no
    /// longer polling. A caller that catches the panic therefore never sees a
    /// changed set under the seq it had before, and a change it makes
    /// afterwards advances at once rather than waiting for a poll that is not
    /// running.
    pub fn poll<R>(&mut self, drain: impl FnOnce(&mut dyn ListingSink) -> R) -> R {
        /// Ends the poll when dropped, which is on return and on unwind alike.
        struct Polling<'a, S: StateStore, C: Clock>(&'a mut Registry<S, C>);

        impl<S: StateStore, C: Clock> Drop for Polling<'_, S, C> {
            fn drop(&mut self) {
                self.0.end_poll();
            }
        }

        self.polling = true;
        let poll = Polling(self);
        drain(&mut *poll.0)
    }

    /// Advance every shard the poll changed, by one, and stop holding changes.
    fn end_poll(&mut self) {
        self.polling = false;
        for shard in 0..self.sets.len() {
            if std::mem::take(&mut self.sets[shard].changed) {
                self.advance_manifest(shard);
            }
        }
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

    /// The offers declined as [`Refusal::IdUnavailable`] since the last call,
    /// each with the ID it derives and the symbol that holds it.
    ///
    /// Once per symbol, not once per offer, for the reason
    /// [`take_unknown_shards`](Self::take_unknown_shards) gives: an adapter
    /// re-offers its whole set every poll.
    pub fn take_unavailable_ids(&mut self) -> Vec<UnavailableId> {
        let taken = self.unavailable[self.unavailable_taken..].to_vec();
        self.unavailable_taken = self.unavailable.len();
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
        let key: &[u8] = match self.config.identity {
            Identity::Symbol => &symbol,
            Identity::VenueKey => match self.venue_key(spec) {
                Ok(key) => key,
                Err(refusal) => {
                    self.count_refusal(refusal);
                    return Err(refusal);
                }
            },
        };
        if let Some(&handle) = self.handles.get(key) {
            self.reoffer(handle, index, spec);
            return Ok(handle);
        }
        self.admit(key, symbol, index, spec)
            .inspect_err(|&refusal| {
                self.count_refusal(refusal);
            })
    }

    /// The venue key an offer is recalled by under [`Identity::VenueKey`]:
    /// the one it states, or its `symbol` untruncated.
    ///
    /// Checked before the lookup, because a key that is not one, or one the
    /// configuration did not declare, would otherwise be recalled or minted
    /// under.
    fn venue_key<'a>(&self, spec: &InstrumentSpec<'a>) -> Result<&'a [u8], Refusal> {
        if self.config.venue_keys == VenueKeys::Symbol
            && spec.venue_key.is_some_and(|key| key != spec.symbol)
        {
            return Err(Refusal::VenueKeyNotDeclared);
        }
        let key = spec.venue_key.unwrap_or(spec.symbol).as_bytes();
        if !is_venue_key(key) {
            return Err(Refusal::BadVenueKey);
        }
        Ok(key)
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
        self.set_changed(shard);
    }

    /// A symbol that has never been published in this process.
    fn admit(
        &mut self,
        key: &[u8],
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
        let recalled = self.minted.get(key).copied();
        let derived = recalled.is_none() && self.config.id_allocation.is_derived();
        let instrument_id = match (recalled, self.config.id_allocation) {
            (Some(minted), _) => minted.instrument_id,
            (None, IdAllocation::Sequential) => self.next_id,
            (None, IdAllocation::Derived) => derive_instrument_id(&symbol),
            (None, IdAllocation::DerivedKey) => derive_instrument_id_from_key(key),
        };
        if instrument_id == 0 && !derived {
            return Err(Refusal::IdSpaceExhausted);
        }
        // Composed before a derived ID is checked. A definition that could
        // never be published is refused for that, rather than reported as a
        // collision that sends the operator after another publisher's record.
        let composed = definition::compose(spec, instrument_id, self.config.source_id)?;
        // Declined, never moved: see `Refusal::IdUnavailable`. `next_id` is the
        // floor here, and nothing below it is derived into.
        if derived
            && (instrument_id == 0
                || instrument_id < self.next_id
                || self.in_use.contains_key(&instrument_id))
        {
            self.remember_unavailable(key, symbol, instrument_id);
            return Err(Refusal::IdUnavailable);
        }

        match recalled {
            None => {
                // The floor does not move under derived allocation; only a
                // sequential mint advances `next_id`.
                let next_id = match self.config.id_allocation {
                    IdAllocation::Sequential => self
                        .next_id
                        .checked_add(1)
                        .ok_or(Refusal::IdSpaceExhausted)?,
                    IdAllocation::Derived | IdAllocation::DerivedKey => self.next_id,
                };
                // Persisted before it is admitted, and a failure to persist
                // admits nothing: an `Instrument ID` published from memory and
                // absent from the record is one that resolves to nothing after
                // a restart, and one that the next run will hand to a
                // different instrument.
                self.persist(instrument_id, key, symbol, next_id)?;
                self.minted.insert(
                    key.into(),
                    Minted {
                        instrument_id,
                        symbol,
                        recorded_published: true,
                        last_published_s: 0,
                    },
                );
                if self.config.id_allocation.is_derived() {
                    self.in_use.insert(instrument_id, key.into());
                }
                self.next_id = next_id;
            }
            // A relisting the record holds as delisted. Written down before it
            // is admitted, because the timestamp in the record would otherwise
            // outlive this publication: after a restart that the venue did not
            // relist it across, it would read as delisted since then, and be
            // forgotten early.
            Some(minted) if !minted.recorded_published => {
                self.persist(instrument_id, key, symbol, self.next_id)?;
                if let Some(minted) = self.minted.get_mut(key) {
                    minted.recorded_published = true;
                }
            }
            Some(_) => {}
        }

        let handle = self.instruments.admit(composed.instrument);
        let index = handle.index() as usize;
        if index >= self.slots.len() {
            self.slots.resize(index + 1, None);
            self.slot_keys.resize(index + 1, None);
        }
        self.slot_keys[index] = Some(key.into());
        self.slots[index] = Some(Published {
            shard,
            definition: composed.definition,
            strike: None,
            defined: false,
        });
        self.handles.insert(key.into(), handle);
        self.published += 1;
        self.sets[shard].published += 1;
        self.counts.admitted += 1;
        self.count_fits(composed.fits);
        self.set_changed(shard);
        // After the admission and not before it, so the base holds the
        // instrument as published. The line is already durable, so a base
        // that fails faults what follows and not this admission.
        if let Err(error) = self.compact(false) {
            self.fault = Some(error);
        }
        Ok(handle)
    }

    /// Write down an `Instrument ID` this admission relies on, before relying
    /// on it: a mint, or a relisting the record holds as delisted.
    ///
    /// **One appended line**, flushed, whatever the size of the record. The
    /// only exception is the first write a directory ever sees, which is a
    /// base, since a line has nothing to be appended to until one exists.
    fn persist(
        &mut self,
        instrument_id: u32,
        key: &[u8],
        symbol: SymbolKey,
        next_id: u32,
    ) -> Result<(), Refusal> {
        let written = if self.base_written {
            let line = if self.keyed() {
                encode_keyed_line(
                    instrument_id,
                    &symbol,
                    venue_key_field(key, &symbol).as_deref(),
                )
            } else {
                encode_line(instrument_id, &symbol)
            };
            self.store.append(&line).map(|()| self.appended += 1)
        } else {
            self.write_base(Some((instrument_id, key, symbol, next_id)), &HashSet::new())
        };
        written.map_err(|error| {
            self.fault = Some(error);
            Refusal::Unpersistable
        })
    }

    /// Whether the appended lines have reached the last base's size.
    ///
    /// A base holds at most the last one's entries plus the lines appended
    /// since, so it rewrites at most two entries for every line appended.
    /// That is what keeps a mint's cost constant however long the history is.
    const fn compaction_due(&self) -> bool {
        self.appended >= self.compaction_at
    }

    /// Fold the appended lines into a new base when they are due, or at open
    /// when there is an entry to forget.
    ///
    /// Returns whether the record was rewritten. A base that could not be
    /// written and left the record as it was is not an error: every entry is
    /// still in it, and the next is tried a threshold's worth of lines later.
    /// Only a store that cannot say which record it left is.
    fn compact(&mut self, opening: bool) -> Result<bool, StateError> {
        let forgetting = if opening || self.compaction_due() {
            self.forgettable(now_s(&self.clock))
        } else {
            HashSet::new()
        };
        if !self.compaction_due() && forgetting.is_empty() {
            return Ok(false);
        }
        match self.write_base(None, &forgetting) {
            Ok(()) => Ok(true),
            Err(StateError::NotReplaced(_)) => {
                self.compaction_at = self.appended + self.base_entries.max(COMPACTION_FLOOR);
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    /// Every entry that is not published and was last published at least the
    /// horizon before `now_s`.
    ///
    /// Forgotten only by a base that lands: an entry dropped here and still in
    /// the record would come back as a second entry for its symbol the next
    /// time the venue listed it, and a record holding a symbol twice is
    /// refused at the next start.
    ///
    /// `next_id` is untouched, and it is what keeps a forgotten ID from being
    /// minted again. What goes is the promise that the symbol comes back under
    /// that ID, which is the cost the configuration's key states.
    fn forgettable(&self, now_s: u64) -> HashSet<RecallKey> {
        let Some(horizon) = self.config.forget_delisted_after else {
            return HashSet::new();
        };
        // Rounded up, like every stamp, and compared against the current second
        // rounded down: each rounding can only keep an entry longer, so none
        // is forgotten before the horizon has passed.
        let horizon_s = horizon.as_secs() + u64::from(horizon.subsec_nanos() > 0);
        self.minted
            .iter()
            .filter(|(key, minted)| {
                !self.handles.contains_key(*key)
                    && now_s >= minted.last_published_s.saturating_add(horizon_s)
            })
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Replace the record with a base of every entry but `forgetting`, plus
    /// `minting` when this is a directory's first write. The forgotten entries
    /// leave this registry only once the base holding none of them has
    /// landed.
    ///
    /// An entry is written as published when it is, and — while seeding — when
    /// the record already holds it so and the venue has not offered it yet: a
    /// seed that has not finished has not said it is gone. Anything else is
    /// written with the second it was last published.
    fn write_base(
        &mut self,
        minting: Option<(u32, &[u8], SymbolKey, u32)>,
        forgetting: &HashSet<RecallKey>,
    ) -> Result<(), StateError> {
        let seeding = matches!(self.phase, Phase::Seeding);
        let keyed = self.keyed();
        let field =
            |key: &[u8], symbol: &SymbolKey| keyed.then(|| venue_key_field(key, symbol)).flatten();
        let mut entries: Vec<Entry> = self
            .minted
            .iter()
            .filter(|(key, _)| !forgetting.contains(*key))
            .map(|(key, minted)| {
                let published =
                    self.handles.contains_key(key) || (seeding && minted.recorded_published);
                Entry {
                    instrument_id: minted.instrument_id,
                    symbol: minted.symbol,
                    venue_key: field(key, &minted.symbol),
                    delisted_at: (!published).then_some(minted.last_published_s),
                }
            })
            .collect();
        let mut next_id = self.next_id;
        if let Some((instrument_id, key, symbol, next)) = minting {
            entries.push(Entry {
                instrument_id,
                symbol,
                venue_key: field(key, &symbol),
                delisted_at: None,
            });
            next_id = next;
        }
        let record = StateRecord {
            source_id: self.config.source_id.get(),
            next_id,
            entries,
            allocation: self.config.id_allocation,
            keyed: keyed.then_some(self.config.venue_keys),
        };
        self.store.store(&record.encode())?;
        for key in forgetting {
            self.minted.remove(key);
        }
        self.base_entries = record.entries.len();
        self.compaction_at = self.base_entries.max(COMPACTION_FLOOR);
        let handles = &self.handles;
        for (key, minted) in &mut self.minted {
            minted.recorded_published =
                handles.contains_key(key) || (seeding && minted.recorded_published);
        }
        self.base_written = true;
        self.appended = 0;
        Ok(())
    }

    /// Withdraw a published instrument.
    fn withdraw(&mut self, instrument: InstrumentRef) {
        let Some(index) = self.published_index(instrument) else {
            return;
        };
        let entry = self.slots[index].expect("checked above");
        self.slots[index] = None;
        let key = self.slot_keys[index].take().expect("kept beside the slot");
        self.handles.remove(&key);
        self.instruments.withdraw(instrument);
        self.published -= 1;
        self.sets[entry.shard].published -= 1;
        if entry.strike.is_some_and(|strike| strike.is_pending()) {
            self.sets[entry.shard].strikes_pending -= 1;
        }
        self.counts.delisted += 1;
        // The entry stays, and gives the symbol its own ID back if the venue
        // relists it. A delisting writes nothing, which is why it cannot fail:
        // when it happened is held here until the next base writes it.
        let delisted_s = stamp_s(&self.clock);
        if let Some(minted) = self.minted.get_mut(&key) {
            minted.last_published_s = delisted_s;
        }
        self.set_changed(entry.shard);
    }

    /// One shard's published set has changed, so the manifest describing it
    /// must: at the end of the poll that changed it, or now when no poll is in
    /// progress.
    ///
    /// Marking rather than advancing is what makes a poll of `k` changes one
    /// advance. See [`poll`](Self::poll).
    fn set_changed(&mut self, shard: usize) {
        if self.polling {
            self.sets[shard].changed = true;
        } else {
            self.advance_manifest(shard);
        }
    }

    /// Advance one shard's `Manifest Seq` by one.
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

    /// Remember a symbol declined as [`Refusal::IdUnavailable`], once, with
    /// what holds its ID, for the caller that logs it.
    fn remember_unavailable(&mut self, key: &[u8], symbol: SymbolKey, instrument_id: u32) {
        let keyed = self.keyed();
        let venue_key: Option<Box<[u8]>> = keyed.then(|| key.into());
        if self.unavailable.len() >= MAX_REPORTED_UNAVAILABLE_IDS
            || self
                .unavailable
                .iter()
                .any(|seen| seen.symbol == symbol && seen.venue_key == venue_key)
        {
            return;
        }
        let holding = self.in_use.get(&instrument_id);
        let holder = holding
            .and_then(|held| self.minted.get(held))
            .map(|minted| minted.symbol);
        let holder_venue_key = holding.filter(|_| keyed).cloned();
        self.unavailable.push(UnavailableId {
            symbol,
            venue_key,
            holder_venue_key,
            instrument_id,
            holder,
            below_floor: instrument_id == 0 || instrument_id < self.next_id,
        });
    }

    /// Whether this registry recalls by venue key, and so writes version 4.
    fn keyed(&self) -> bool {
        self.config.identity == Identity::VenueKey
    }

    fn count_refusal(&mut self, refusal: Refusal) {
        self.last_refusal = Some(refusal);
        // Written out rather than split on `is_ordinary`, so that a refusal
        // added later cannot land in a bucket by default and be reported as
        // something it is not.
        match refusal {
            Refusal::Capped => self.counts.declined_at_cap += 1,
            Refusal::IdUnavailable => self.counts.declined_id_unavailable += 1,
            Refusal::UnknownShard => self.counts.declined_unknown_shard += 1,
            Refusal::ShardRestated => self.counts.declined_shard_restated += 1,
            Refusal::StrikeUnfixed => self.counts.declined_strike_unfixed += 1,
            Refusal::StrikePresence => self.counts.declined_strike_presence += 1,
            Refusal::ContractSize
            | Refusal::BadVenueKey
            | Refusal::VenueKeyNotDeclared
            | Refusal::Field(_)
            | Refusal::StrikeUnrepresentable
            | Refusal::StrikeEmpty
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

    fn strike(&mut self, instrument: InstrumentRef, strike: &Strike<'_>) {
        // Counted where it was refused. An adapter is told nothing, as for an
        // offer: it can act on none of the reasons.
        let _ = self.state_strike(instrument, strike);
    }
}

/// The clock's Unix reading in whole seconds, rounded down: the second a
/// horizon is measured at.
fn now_s<C: Clock>(clock: &C) -> u64 {
    clock.unix_ns() / 1_000_000_000
}

/// The clock's Unix reading in whole seconds, rounded up: the second an entry
/// is recorded as last published at, which is what the record holds.
///
/// Up and not down. A delisting at 10.999 s recorded as second 10 would be
/// forgotten at 11.000 s under a one-second horizon, a millisecond after it.
fn stamp_s<C: Clock>(clock: &C) -> u64 {
    clock.unix_ns().div_ceil(1_000_000_000)
}
