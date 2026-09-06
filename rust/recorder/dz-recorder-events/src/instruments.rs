//! Reference data as a deriver needs it: keyed on the instrument, scoped to an
//! era, and placed in the sequence space.
//!
//! # Why this is not `ArchivedRefdata`
//!
//! `dz-recorder-relower` has an accumulator over the same two messages and it
//! makes two choices this one reverses. Both of its choices are correct there.
//!
//! **It keys by symbol.** A re-lowering compares a multicast archive against an
//! archive of upstream payloads, and the upstream side speaks the venue's name
//! for an instrument rather than a wire `Instrument ID`, so the symbol is the
//! only key the two sides share. A deriver has one archive and every message in
//! it carries the `Instrument ID`, so it keys on that — and must, because a
//! symbol is `char[64]` of venue-chosen text that is unique within a channel at
//! an instant and not across eras. An instrument retired and another published
//! later under the same name are two instruments, and a symbol key silently
//! merges them.
//!
//! **It keeps the first statement after a restatement** and raises a caveat.
//! That is the honest half of an unanswerable question: its two archives are
//! stamped by a subscriber's clock and a publisher's, with no key that orders
//! one against the other, so there is no defensible instant at which to switch
//! exponents. This accumulator is not in that position. It has one archive in
//! which every definition arrives at a sequence number, so a restatement has an
//! exact position and the prices before it and after it decode at different
//! scales — which is what actually happened.
//!
//! # An era is where a table ends
//!
//! Statements are placed by sequence number, and a `Reset Count` restarts the
//! sequence space. Carrying statements across that boundary would order them
//! against numbers from a different space, so a new era clears the instruments
//! of the channel instance that opened it. Nothing is merged across an era, in
//! either direction.

use std::collections::BTreeMap;

use dz_edge_refdata::{InstrumentDefinition, ManifestSummary, SYMBOL_LEN};
use dz_recorder_core::ChannelInstance;

/// Where a statement was made.
///
/// A definition without a position is a definition that cannot be placed, which
/// is the whole difference between this accumulator and the one it is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct At {
    /// The channel instance the datagram arrived on. A `Reset Count` belongs to
    /// one of these, so an era does too.
    pub instance: ChannelInstance,
    pub sequence_number: u64,
    pub reset_count: u8,
    pub recv_ts_ns: u64,
}

/// One instrument, as one definition stated it, in force from a sequence number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statement {
    pub instrument_id: u32,
    pub source_id: u16,
    /// Display and filtering only. Never a key — see this module's note.
    pub symbol: String,
    pub price_exponent: i8,
    pub qty_exponent: i8,
    pub contract_value: u64,
    pub manifest_seq: u16,
    /// The sequence number of the datagram that carried this definition.
    ///
    /// It is in force from here until the next statement, which is what lets a
    /// price before a restatement and a price after it decode differently.
    pub from_sequence: u64,
    pub first_seen_ts_ns: u64,
}

impl Statement {
    /// Whether two statements say the same thing about an instrument.
    ///
    /// Position is excluded deliberately: the definition cycle republishes the
    /// same definition indefinitely, and every one of those carries a new
    /// sequence number. Comparing positions would make every repetition a
    /// change.
    fn says_the_same_as(&self, other: &Self) -> bool {
        self.source_id == other.source_id
            && self.symbol == other.symbol
            && self.price_exponent == other.price_exponent
            && self.qty_exponent == other.qty_exponent
            && self.contract_value == other.contract_value
    }
}

/// What observing a definition did.
///
/// A deriver writes an `instrument` row on [`First`](Observed::First) and
/// [`Restated`](Observed::Restated) and on nothing else. The distinction is not
/// pedantry: the definition cycle repeats every instrument on the runtime's
/// cadence forever, so a deriver that wrote a row per definition observed would
/// write the reference data table over and over and would be reporting the
/// publisher's pacing rather than the venue's changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Observed {
    /// The first statement of this instrument in this era.
    First,
    /// A statement that changes what was in force, from its sequence number on.
    Restated,
    /// A statement identical to the one in force. The cycle came round again.
    Repeated,
    /// A statement positioned at or before the one already in force.
    ///
    /// Not inserted. An archive is read in order, so this is a fact about the
    /// reader rather than about the feed, and quietly accepting it would put the
    /// table's statements out of the order every lookup depends on.
    OutOfOrder,
}

/// Reference data for every channel instance an archive holds.
#[derive(Debug, Clone, Default)]
pub struct InstrumentTable {
    instances: BTreeMap<ChannelInstance, EraTable>,
}

/// One channel instance's instruments, within one era.
#[derive(Debug, Clone)]
struct EraTable {
    reset_count: u8,
    /// Statements per instrument, in ascending `from_sequence` order.
    instruments: BTreeMap<u32, Vec<Statement>>,
    /// What the last valid `ManifestSummary` said the published set held.
    declared_count: Option<u32>,
}

impl EraTable {
    const fn new(reset_count: u8) -> Self {
        Self {
            reset_count,
            instruments: BTreeMap::new(),
            declared_count: None,
        }
    }
}

impl InstrumentTable {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Take one `InstrumentDefinition`, at the position it was carried.
    pub fn observe_definition(&mut self, definition: &InstrumentDefinition, at: At) -> Observed {
        let table = self.era_table(at);
        let statement = Statement {
            instrument_id: definition.instrument_id,
            source_id: definition.source_id,
            symbol: symbol_text(&definition.symbol),
            price_exponent: definition.price_exponent,
            qty_exponent: definition.qty_exponent,
            contract_value: definition.contract_value,
            manifest_seq: definition.manifest_seq,
            from_sequence: at.sequence_number,
            first_seen_ts_ns: at.recv_ts_ns,
        };

        let history = table
            .instruments
            .entry(definition.instrument_id)
            .or_default();
        let Some(in_force) = history.last() else {
            history.push(statement);
            return Observed::First;
        };
        if in_force.says_the_same_as(&statement) {
            return Observed::Repeated;
        }
        if statement.from_sequence <= in_force.from_sequence {
            return Observed::OutOfOrder;
        }
        history.push(statement);
        Observed::Restated
    }

    /// Take one `ManifestSummary`.
    ///
    /// A summary that is not valid yet declares nothing, and is recorded as
    /// nothing rather than as zero: the count of a published set that has not
    /// been published is absent, and a zero there would read as an empty feed.
    pub fn observe_manifest(&mut self, summary: &ManifestSummary, at: At) {
        let table = self.era_table(at);
        if summary.valid != 0 {
            table.declared_count = Some(summary.instrument_count);
        }
    }

    /// The statement in force for an instrument at a sequence number.
    ///
    /// The last statement positioned at or before `at_sequence`. A price carried
    /// before the first definition of its instrument resolves to `None` — the
    /// exponent that decodes it was not on the wire yet, and inventing one would
    /// produce a number rather than an answer.
    #[must_use]
    pub fn resolve(
        &self,
        instance: ChannelInstance,
        instrument_id: u32,
        at_sequence: u64,
    ) -> Option<&Statement> {
        self.instances
            .get(&instance)?
            .instruments
            .get(&instrument_id)?
            .iter()
            .rev()
            .find(|statement| statement.from_sequence <= at_sequence)
    }

    /// What the published set declared, if a valid summary said so.
    #[must_use]
    pub fn declared_count(&self, instance: ChannelInstance) -> Option<u32> {
        self.instances.get(&instance)?.declared_count
    }

    /// The era this instance's statements belong to.
    #[must_use]
    pub fn era(&self, instance: ChannelInstance) -> Option<u8> {
        self.instances.get(&instance).map(|table| table.reset_count)
    }

    /// How many instruments this instance has defined in the current era.
    #[must_use]
    pub fn defined_count(&self, instance: ChannelInstance) -> usize {
        self.instances
            .get(&instance)
            .map_or(0, |table| table.instruments.len())
    }

    /// The instance's table, cleared first if this position opens a new era.
    fn era_table(&mut self, at: At) -> &mut EraTable {
        let table = self
            .instances
            .entry(at.instance)
            .or_insert_with(|| EraTable::new(at.reset_count));
        if table.reset_count != at.reset_count {
            *table = EraTable::new(at.reset_count);
        }
        table
    }
}

/// The wire's fixed-width symbol as text, to the first NUL.
fn symbol_text(symbol: &[u8; SYMBOL_LEN]) -> String {
    let end = symbol
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(SYMBOL_LEN);
    String::from_utf8_lossy(&symbol[..end]).into_owned()
}
