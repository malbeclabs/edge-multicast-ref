//! The reference-data owner: who an instrument is, and how a subscriber comes
//! to know it.

use std::collections::HashMap;

use dz_adapter_core::{InstrumentRef, InstrumentSpec, ListingSink};
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

/// What this publisher is, on the wire.
#[derive(Debug, Clone, Copy)]
pub struct RegistryConfig {
    /// Checked once at startup against the ranges the source registry reserves,
    /// and against the `Source ID` the persisted state was minted under.
    pub source_id: SourceId,
    /// The `Channel ID` this publisher's reference data is published on, for
    /// `ManifestSummary`'s own copy of it.
    pub channel_id: u8,
    pub selection: SelectionPolicy,
    pub schedule: CycleSchedule,
}

/// One published instrument.
#[derive(Debug, Clone, Copy)]
struct Published {
    symbol: SymbolKey,
    /// `Manifest Seq` is [`definition::stamped`] on the way out, never held
    /// here: a definition sitting in this table between two changes to the
    /// published set would otherwise carry a manifest that no longer exists.
    definition: InstrumentDefinition,
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
/// - [`Registry::published`] is `refdata_instruments_current`.
/// - [`Registry::manifest_seq`] and [`Registry::is_valid`] are
///   `refdata_manifest_seq` and `refdata_manifest_valid`, both by `Channel ID`.
/// - [`declined_unrepresentable`](Self::declined_unrepresentable) is a
///   reference-data load that did not fully load, under the load-error
///   family's `schema` reason.
///
/// [`declined_at_cap`](Self::declined_at_cap) maps to nothing, deliberately: it
/// is the selection policy working, and a series that climbs whenever a venue
/// lists more instruments than a feed publishes would be alerting on the normal
/// case.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    pub admitted: u64,
    pub delisted: u64,
    pub definitions_emitted: u64,
    pub declined_at_cap: u64,
    pub declined_unrepresentable: u64,
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
    published: usize,
    instruments: InstrumentTable,
    manifest_seq: u16,
    phase: Phase,
    pacer: DefinitionPacer,
    cursor: usize,
    counts: Counts,
    last_refusal: Option<Refusal>,
    fault: Option<StateError>,
}

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
        Ok(Self {
            store,
            clock,
            minted,
            next_id: record.next_id,
            handles: HashMap::new(),
            slots: Vec::new(),
            published: 0,
            instruments: InstrumentTable::new(),
            manifest_seq: 0,
            phase: Phase::Seeding,
            pacer: DefinitionPacer::new(config.schedule),
            cursor: 0,
            counts: Counts::default(),
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

    /// How many instruments are published right now.
    #[must_use]
    pub const fn published(&self) -> usize {
        self.published
    }

    /// The current `Manifest Seq`.
    ///
    /// Zero until the first published set exists, which is the value a
    /// subscriber only ever sees paired with `Valid` at 0.
    #[must_use]
    pub const fn manifest_seq(&self) -> u16 {
        self.manifest_seq
    }

    /// The `Valid` flag: whether the published set is established.
    ///
    /// False while seeding and false again from the start of shutdown, which is
    /// the codec's own definition of the field — 1 once the published set is
    /// established, 0 while uninitialized or shutting down. It is not a health
    /// signal: a channel whose instruments are all dormant is silent and valid.
    #[must_use]
    pub const fn is_valid(&self) -> bool {
        matches!(self.phase, Phase::Established)
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
    #[must_use]
    pub fn definition(&self, instrument: InstrumentRef) -> Option<InstrumentDefinition> {
        self.slot(instrument)
            .map(|published| definition::stamped(&published.definition, self.manifest_seq))
    }

    /// The definitions this tick owes, paced.
    ///
    /// `out` is cleared and filled, so a caller keeps one buffer for the life of
    /// the process. The count is [`DefinitionPacer`]'s and is capped, so a
    /// caller cannot obtain the whole published set in one call however it
    /// arranges its loop — the rule that publishers must not emit the entire
    /// published set as a single burst is kept here rather than asked of the
    /// caller.
    ///
    /// The cycle continues while shutting down and while seeding. A definition
    /// is publishable the moment it composes, and a subscriber joining during
    /// the seed collects definitions it can already use; what the manifest's
    /// `Valid` flag tells it is whether the *set* is final yet.
    pub fn definition_tick(&mut self, out: &mut Vec<InstrumentDefinition>) {
        out.clear();
        let due = self.pacer.due(self.clock.monotonic_ns(), self.published);
        if due == 0 {
            return;
        }
        let slots = self.slots.len();
        while out.len() < due {
            let before = out.len();
            for _ in 0..slots {
                if out.len() == due {
                    break;
                }
                if let Some(published) = &self.slots[self.cursor] {
                    out.push(definition::stamped(
                        &published.definition,
                        self.manifest_seq,
                    ));
                }
                self.cursor = (self.cursor + 1) % slots;
            }
            // A pass over every slot that emitted nothing cannot be repeated
            // into progress. Reachable only if the published count and the
            // slots disagree, and an infinite loop in the emit path is a
            // publisher that goes dark rather than a defect somebody notices.
            if out.len() == before {
                break;
            }
        }
        self.counts.definitions_emitted += out.len() as u64;
    }

    /// The manifest, as of now.
    ///
    /// `Channel ID` is set from configuration even though a builder-framed
    /// message has it stamped from the datagram header afterwards: a caller
    /// that encodes one without a builder still gets a truthful field, and a
    /// caller that uses a builder cannot end up with two different answers.
    #[must_use]
    pub fn manifest(&self) -> ManifestSummary {
        ManifestSummary {
            channel_id: self.config.channel_id,
            valid: u8::from(self.is_valid()),
            manifest_seq: self.manifest_seq,
            // Saturating rather than truncating: a published set larger than a
            // u32 is unreachable through a policy whose cap is a `usize` an
            // operator sets, and a wrapped count would read as a small feed.
            instrument_count: u32::try_from(self.published).unwrap_or(u32::MAX),
            timestamp_ns: self.clock.unix_ns(),
        }
    }

    /// Offer an instrument, and report why it was declined.
    ///
    /// [`ListingSink::list`] is this without the reason. An adapter is given
    /// the `Option`, because the boundary carries no vocabulary for a refusal
    /// and a venue can act on none of them; the runtime wiring the registry up
    /// gets this one, because it can.
    ///
    /// # Errors
    ///
    /// Every [`Refusal`]. [`Refusal::Capped`] is ordinary; the rest are not.
    pub fn offer(&mut self, spec: &InstrumentSpec<'_>) -> Result<InstrumentRef, Refusal> {
        let (symbol, _fit) = definition::symbol_field(spec.symbol);
        if let Some(&handle) = self.handles.get(&symbol) {
            self.reoffer(handle, spec);
            return Ok(handle);
        }
        self.admit(symbol, spec).inspect_err(|&refusal| {
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
    fn reoffer(&mut self, handle: InstrumentRef, spec: &InstrumentSpec<'_>) {
        let Some(current) = self.slot(handle).map(|published| published.definition) else {
            return;
        };
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
        // reconciling against has too.
        self.advance_manifest();
    }

    /// A symbol that has never been published in this process.
    fn admit(
        &mut self,
        symbol: SymbolKey,
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
            definition: composed.definition,
        });
        self.handles.insert(symbol, handle);
        self.published += 1;
        self.counts.admitted += 1;
        self.count_fits(composed.fits);
        self.advance_manifest();
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
        let symbol = self.slots[index].expect("checked above").symbol;
        self.slots[index] = None;
        self.handles.remove(&symbol);
        self.instruments.withdraw(instrument);
        self.published -= 1;
        self.counts.delisted += 1;
        // The persisted entry stays. It is what stops the ID being minted for
        // something else, and what gives the symbol its own ID back if the
        // venue relists it - so a delisting writes nothing, which is why it
        // cannot fail.
        self.advance_manifest();
    }

    /// The published set has changed, so the manifest describing it must.
    ///
    /// Wraps to 1 rather than to 0, because 0 is the value a subscriber sees
    /// only alongside `Valid` at 0. Reaching it again in flight would make an
    /// established manifest indistinguishable from one that has never been
    /// established.
    fn advance_manifest(&mut self) {
        self.manifest_seq = self.manifest_seq.checked_add(1).unwrap_or(1);
    }

    fn count_refusal(&mut self, refusal: Refusal) {
        self.last_refusal = Some(refusal);
        if refusal.is_ordinary() {
            self.counts.declined_at_cap += 1;
        } else {
            self.counts.declined_unrepresentable += 1;
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

    fn slot(&self, instrument: InstrumentRef) -> Option<&Published> {
        self.slots.get(instrument.index() as usize)?.as_ref()
    }

    fn slot_mut(&mut self, instrument: InstrumentRef) -> Option<&mut Published> {
        self.slots.get_mut(instrument.index() as usize)?.as_mut()
    }
}

impl<S: StateStore, C: Clock> ListingSink for Registry<S, C> {
    fn list(&mut self, spec: &InstrumentSpec<'_>) -> Option<InstrumentRef> {
        self.offer(spec).ok()
    }

    fn delist(&mut self, instrument: InstrumentRef) {
        self.withdraw(instrument);
    }
}
