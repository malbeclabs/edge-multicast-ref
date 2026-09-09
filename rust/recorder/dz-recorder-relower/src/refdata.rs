//! The instrument table, reconstructed from what the archive says — and never
//! from live state.
//!
//! This is the third of Mode C's three requirements, and the one most easily got
//! wrong by being helpful. A publisher's reference-data registry is *right there*
//! and it resolves every symbol; using it re-runs today's mapping over
//! yesterday's bytes, agrees with itself, and reports nothing. An exponent
//! restated last week makes every price in the archive a field difference
//! against a re-lowering that used this week's — or, worse, makes a real defect
//! invisible because the registry has since been corrected to match what the
//! publisher actually sent.
//!
//! So the only source admitted here is the capture: `InstrumentDefinition` for
//! the identity and the exponents, `ManifestSummary` for whether the archive
//! carries the whole published set. There is no constructor that takes anything
//! else.
//!
//! # What the wire carries, and what it does not
//!
//! It carries the `Instrument ID`, both exponents, the `Source ID`, the tick and
//! lot grid, and `Contract Value`. It does **not** carry
//! [`quoted_per_contract`](dz_publisher_lowering::Instrument::quoted_per_contract),
//! the factor between a venue's quoted unit and the wire's. `Contract Value` is
//! not that factor and cannot be converted into it: one states what a contract
//! is *worth*, at `Price Exponent`; the other states how much of the underlying
//! one contract *is*. A definition therefore does not carry what a re-lowering
//! would need to reproduce a per-contract venue's scaling, and this module says
//! so — [`Caveat::ContractFactorNotOnTheWire`] — rather than guessing a factor
//! that would make every price and quantity for that instrument differ.

use std::collections::{BTreeMap, BTreeSet};

use dz_edge_refdata::{InstrumentDefinition, ManifestSummary, SYMBOL_LEN};
use dz_publisher_lowering::Instrument;

use crate::finding::Caveat;

/// One instrument as the archive states it.
///
/// Every field is read off an `InstrumentDefinition` that was on the wire. The
/// three the lowering needs are [`instrument_id`](Self::instrument_id) and the
/// two exponents; the rest are carried so that a report can name an instrument
/// the way an operator will look for it, and so that
/// [`contract_value`](Self::contract_value) can raise its own caveat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchivedInstrument {
    pub instrument_id: u32,
    /// The `Symbol` field, NUL-padded exactly as the wire carries it. This is
    /// the identity an adapter's listing is resolved against, because it is the
    /// only identity a venue states and the runtime does not mint.
    pub symbol: [u8; SYMBOL_LEN],
    pub price_exponent: i8,
    pub qty_exponent: i8,
    /// The publisher's own identity, `0` at schema 1 where the field did not
    /// exist.
    pub source_id: u16,
    /// What one contract is worth, at `Price Exponent`. **Not** a conversion
    /// factor; see this module's own note.
    pub contract_value: u64,
    /// The manifest this definition claimed to belong to when it was first seen.
    pub manifest_seq: u16,
}

impl ArchivedInstrument {
    /// The symbol as text, with the NUL padding removed.
    ///
    /// Lossy for a definition whose `Symbol` is not UTF-8, which is a thing an
    /// archive can hold: the field is `char[64]` on the wire and a publisher
    /// that wrote something else wrote it, so this reports what is there rather
    /// than refusing to name the instrument at all.
    #[must_use]
    pub fn symbol_text(&self) -> String {
        let end = self
            .symbol
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(SYMBOL_LEN);
        String::from_utf8_lossy(&self.symbol[..end]).into_owned()
    }

    /// What the lowering's table holds for this instrument.
    ///
    /// `quoted_per_contract` is `None` and can only be `None`: the wire does not
    /// carry the factor. See this module's note, and
    /// [`Caveat::ContractFactorNotOnTheWire`] for how a reader is told.
    #[must_use]
    pub const fn as_instrument(&self) -> Instrument {
        Instrument {
            instrument_id: self.instrument_id,
            price_exponent: self.price_exponent,
            qty_exponent: self.qty_exponent,
            quoted_per_contract: None,
        }
    }
}

/// An instrument an adapter offered that the archive has no definition for.
///
/// **Reported, never guessed.** A re-lowering that invented an `Instrument ID`
/// or an exponent for a symbol the capture never defined would produce messages
/// that join against nothing, and every one of them would be reported as *the
/// publisher dropped it* — a false accusation drawn from our own missing
/// reference data. So the listing is declined, the symbol is recorded here, and
/// the events for it are never lowered.
///
/// The likely causes, in order: the window opened after the definition cycle had
/// already sent that instrument's definition and closed before the next cycle;
/// the refdata port role was not recorded at all; or the instrument really was
/// never published, which is itself the finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingDefinition {
    /// The symbol the adapter offered.
    pub symbol: String,
    /// How many times it was offered. An adapter may re-offer its whole set on
    /// every poll, and one line per poll would bury everything else.
    pub offers: u64,
}

/// What the highest valid manifest declared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ManifestState {
    manifest_seq: u16,
    instrument_count: u32,
}

/// The published set, as reconstructed from one or more archives.
///
/// Keyed on `Symbol`, because that is the join between the two sides: an adapter
/// offers a symbol and gets a handle, and the archive says which `Instrument ID`
/// and which exponents that symbol was published under. Neither side can state
/// the other's identity — an adapter cannot name an `Instrument ID`, and the wire
/// does not carry a venue's handle — so the symbol is the only key there is.
///
/// # The manifest is per channel, and the symbol table is not
///
/// Two different scopes in one type, and both are deliberate.
///
/// `Instrument Count` and `Manifest Seq` describe **one channel's** published
/// set: that is the reference-data specification's definition, and it is what a
/// publisher operating several channel instances from one process states on each
/// of them. So a manifest is held per `Channel ID` and each one is compared
/// against the definitions that channel carried. Held as one for the archive —
/// which is what this did — the check compares a union of every channel's
/// definitions against whichever channel happened to have the highest
/// `Manifest Seq`, and reports incomplete reference data on a complete capture
/// or nothing at all on a short one, depending on nothing but the arrangement.
///
/// The symbol table stays one for the archive, because the re-lowering resolves
/// a symbol without knowing which channel it will be found on: an adapter offers
/// a symbol and the archive answers. Splitting it per channel would mean
/// resolving against a channel a payload archive cannot name.
///
/// # Per channel, not per channel instance
///
/// The redundant paths of one channel publish the same set under the same count,
/// and [`by_symbol`](Self::by_symbol) already unions them — raising
/// [`Caveat::ScaleRestated`] where they disagree. Keyed on the instance, this
/// check would raise a caveat against a path whose refdata window was shorter
/// even where the union covers the set and the re-lowering declines nothing:
/// a statement about capture coverage wearing the clothes of one about the
/// archive. `GLOSSARY.md` has an instrument unique *within a channel*, the count
/// is stated per channel, and the reconstruction resolves per channel.
///
/// The key is the **datagram header's** `Channel ID` rather than the
/// `ManifestSummary`'s own field. The header is what a definition has — there is
/// no `Channel ID` on `InstrumentDefinition` — so it is the only key both
/// messages share, and keying the two halves of one comparison on two different
/// fields would compare sets that are not the same channel's.
#[derive(Debug, Clone, Default)]
pub struct ArchivedRefdata {
    by_symbol: BTreeMap<[u8; SYMBOL_LEN], ArchivedInstrument>,
    /// `Instrument ID` back to `Symbol`, so a finding can name an instrument the
    /// way an operator will search for it.
    by_id: BTreeMap<u32, [u8; SYMBOL_LEN]>,
    /// The symbols each channel defined. Separate from
    /// [`by_symbol`](Self::by_symbol) because that keeps the *first* statement
    /// of a symbol and this is a membership: a symbol carried on two channels
    /// belongs to both counts, and only one of them would insert.
    defined_on: BTreeMap<u8, BTreeSet<[u8; SYMBOL_LEN]>>,
    /// The highest valid manifest each channel stated.
    manifests: BTreeMap<u8, ManifestState>,
    caveats: Vec<Caveat>,
}

impl ArchivedRefdata {
    /// An empty published set.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            by_symbol: BTreeMap::new(),
            by_id: BTreeMap::new(),
            defined_on: BTreeMap::new(),
            manifests: BTreeMap::new(),
            caveats: Vec::new(),
        }
    }

    /// Take one `InstrumentDefinition` found in an archive into the set.
    ///
    /// # A restatement keeps the first statement, and says so
    ///
    /// A definition that restates a symbol's exponents is a real thing — the
    /// reference-data owner has a `replace` for exactly it — but a re-lowering
    /// cannot place *when* it took effect. The two archives are two streams: the
    /// multicast capture stamps its own receive clock at a subscriber, the
    /// payload archive stamps the publisher's, and neither carries a key that
    /// orders one against the other. So there is no defensible way to lower the
    /// payloads before the restatement at one exponent and the payloads after it
    /// at another.
    ///
    /// The first statement is kept, because it is the one in force when the
    /// window opened, and [`Caveat::ScaleRestated`] tells the reader that every
    /// message for that instrument after the restatement is compared against the
    /// wrong exponent. Keeping the last would have the same flaw pointed the
    /// other way and would silently discard the start of the window.
    pub fn observe_definition(&mut self, definition: &InstrumentDefinition, channel_id: u8) {
        // Recorded before anything is decided about the symbol, and recorded
        // even for a restatement: the question this answers is *which
        // instruments did this channel define*, and a channel that stated one
        // twice defined one. `channel_id` is the datagram header's, which is
        // the only channel identity a definition carries.
        self.defined_on
            .entry(channel_id)
            .or_default()
            .insert(definition.symbol);
        if let Some(existing) = self.by_symbol.get(&definition.symbol).copied() {
            if existing.price_exponent != definition.price_exponent
                || existing.qty_exponent != definition.qty_exponent
            {
                self.push_once(Caveat::ScaleRestated {
                    instrument_id: existing.instrument_id,
                    kept: (existing.price_exponent, existing.qty_exponent),
                    later: (definition.price_exponent, definition.qty_exponent),
                });
            }
            if existing.instrument_id != definition.instrument_id {
                self.push_once(Caveat::SymbolRepublishedUnderANewId {
                    kept: existing.instrument_id,
                    later: definition.instrument_id,
                });
            }
            return;
        }

        let archived = ArchivedInstrument {
            instrument_id: definition.instrument_id,
            symbol: definition.symbol,
            price_exponent: definition.price_exponent,
            qty_exponent: definition.qty_exponent,
            source_id: definition.source_id,
            contract_value: definition.contract_value,
            manifest_seq: definition.manifest_seq,
        };
        if archived.contract_value != 0 {
            // A non-zero `Contract Value` does not prove the venue quotes per
            // contract — it is a number a subscriber reads, not a factor anyone
            // applies. It is, however, the only hint the wire gives that a
            // contract exists at all, and if the publisher did apply a factor
            // then every price and quantity for this instrument will differ.
            // Saying so is what stops a reader attributing our own gap to the
            // publisher.
            self.push_once(Caveat::ContractFactorNotOnTheWire {
                instrument_id: archived.instrument_id,
            });
        }
        if let Some(previous) = self.by_id.insert(archived.instrument_id, archived.symbol) {
            if previous != archived.symbol {
                self.push_once(Caveat::IdSharedByTwoSymbols {
                    instrument_id: archived.instrument_id,
                });
            }
        }
        self.by_symbol.insert(archived.symbol, archived);
    }

    /// Take one `ManifestSummary` found in an archive into the set.
    ///
    /// The manifest is what makes *incomplete reference data* detectable before
    /// a single message is compared: it declares how many instruments the
    /// published set holds, so an archive that yielded fewer definitions than
    /// that is one whose refdata window is short. Without it, a missing
    /// definition is only discovered when an adapter offers the symbol — and an
    /// adapter that never reaches that instrument in the window would leave the
    /// gap invisible.
    ///
    /// Only a summary with `valid = 1` counts. Zero is what a publisher sends
    /// while its set is not yet established and while it is shutting down, and
    /// the count beside it describes neither state.
    pub fn observe_manifest(&mut self, summary: &ManifestSummary, channel_id: u8) {
        if summary.valid != 1 {
            return;
        }
        // Highest `Manifest Seq` **on this channel**. Held as one for the
        // archive it was highest across channels, which is a comparison between
        // sequences that do not share a space: `Manifest Seq` increments when
        // the published set changes on its own channel, so a busy channel's 40
        // is not later than a quiet one's 3 and picking between them picks a
        // number describing a set the other channel never published.
        let held = self.manifests.entry(channel_id).or_insert(ManifestState {
            manifest_seq: summary.manifest_seq,
            instrument_count: summary.instrument_count,
        });
        if summary.manifest_seq > held.manifest_seq {
            *held = ManifestState {
                manifest_seq: summary.manifest_seq,
                instrument_count: summary.instrument_count,
            };
        }
    }

    /// The instrument this symbol was published as, if the archive says.
    #[must_use]
    pub fn by_symbol(&self, symbol: &str) -> Option<&ArchivedInstrument> {
        let (field, _fit) = dz_edge_core::pad_ascii::<SYMBOL_LEN>(symbol);
        self.by_symbol.get(&field)
    }

    /// The symbol an `Instrument ID` was published under, for naming a finding.
    #[must_use]
    pub fn symbol_of(&self, instrument_id: u32) -> Option<String> {
        self.by_id
            .get(&instrument_id)
            .and_then(|symbol| self.by_symbol.get(symbol))
            .map(ArchivedInstrument::symbol_text)
    }

    /// Every instrument the archive defined, in `Symbol` order.
    pub fn instruments(&self) -> impl Iterator<Item = &ArchivedInstrument> {
        self.by_symbol.values()
    }

    /// How many definitions were reconstructed.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_symbol.len()
    }

    /// Whether the archive defined nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_symbol.is_empty()
    }

    /// What one channel's highest valid `ManifestSummary` declared its published
    /// set to hold, if the archive carried one for that channel.
    ///
    /// **Per `Channel ID`, and there is no process-wide answer to offer
    /// instead.** `Instrument Count` describes the channel the datagram went out
    /// on; several channels' counts do not reduce to one number, and their sum
    /// is not one either — it is the size of the union of disjoint published
    /// sets, which no manifest states and no subscriber ever sees.
    #[must_use]
    pub fn declared_instrument_count(&self, channel_id: u8) -> Option<u32> {
        self.manifests
            .get(&channel_id)
            .map(|state| state.instrument_count)
    }

    /// The channels this archive carried a valid manifest for, ascending.
    pub fn channels(&self) -> impl Iterator<Item = u8> + '_ {
        self.manifests.keys().copied()
    }

    /// The `Source ID` values the definitions state.
    ///
    /// Deliberately a set and not a value: two identities in one capture is a
    /// condition to refuse rather than to pick from. Schema 1 has no such field
    /// and decodes as `0`, which
    /// [`SourceId`](dz_publisher_lowering::SourceId) does not admit, so it never
    /// becomes an answer.
    pub fn source_ids(&self) -> impl Iterator<Item = u16> + '_ {
        self.by_symbol
            .values()
            .map(|instrument| instrument.source_id)
    }

    /// Everything the reconstruction could not state, and everything it had to
    /// choose between.
    ///
    /// Ends up in the report beside the findings, because a caveat is what stops
    /// a reader attributing a gap in the archive to the publisher.
    #[must_use]
    pub fn caveats(&self) -> &[Caveat] {
        &self.caveats
    }

    /// The caveats the reconstruction owes on top of the ones it collected as it
    /// went.
    ///
    /// Called once the archives are exhausted, because completeness is a
    /// property of the whole window: a definition cycle is paced, so a set that
    /// is short at the first manifest may be whole by the last.
    pub(crate) fn finalise(&mut self) {
        // One comparison per channel, each against its own definitions. A union
        // against one channel's count is wrong in both directions at once: over
        // an archive of two channels it exceeds the count of either, so a
        // complete capture reports incomplete reference data, and a capture
        // that is genuinely short on the channel with the lower `Manifest Seq`
        // reports nothing at all.
        for (channel_id, state) in &self.manifests {
            let reconstructed = self
                .defined_on
                .get(channel_id)
                .map_or(0, std::collections::BTreeSet::len);
            if u64::from(state.instrument_count) != reconstructed as u64 {
                self.caveats.push(Caveat::ReferenceDataIncomplete {
                    channel_id: *channel_id,
                    manifest_seq: state.manifest_seq,
                    declared: state.instrument_count,
                    reconstructed,
                });
            }
        }
    }

    /// Record a caveat unless the identical one is already held.
    ///
    /// A definition cycle repeats, so the same restatement is seen on every
    /// pass. One line per pass would bury the findings under the caveats.
    fn push_once(&mut self, caveat: Caveat) {
        if !self.caveats.contains(&caveat) {
            self.caveats.push(caveat);
        }
    }
}
