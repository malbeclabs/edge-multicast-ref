//! The fold: an archive of bytes, as rows about instruments.
//!
//! One pass, in archive order, over everything the walk decoded. No book here —
//! `book_top` needs state that spans objects and is task 6 — so this is a pure
//! function of one object and its reference data.
//!
//! # Why the three outputs are merged rather than read in turn
//!
//! [`WireCapture`] yields market data, state and reference messages as three
//! vectors, and the order *between* them is what this fold depends on: a
//! definition restates an exponent from its own sequence number, so a price that
//! arrived before it must decode at the old scale and one that arrived after it
//! at the new. Reading the three vectors in turn would apply every definition in
//! the object to every price in it.
//!
//! Provenance carries `datagram_index` and `message_index`, which totally order
//! the archive, so the merge is a sort rather than a change to the walk.
//!
//! # What is refused, and why refusing is the point
//!
//! An instrument with no definition in force resolves to nothing, and its
//! messages are **refused rather than filled in**. `source_id`, `price_exp` and
//! `qty_exp` are not nullable on the row, and the values that would have to be
//! invented are exactly the ones that decide what a price means. A refused
//! message is counted; a guessed one is a number nobody can audit.

use std::collections::BTreeMap;

use dz_edge_core::PortRole;
use dz_recorder_core::{ChannelInstance, RecorderIdentity, Source};
use dz_recorder_relower::{
    MessageBody, ReferenceBody, RelowerError, StateBody, WireCapture, WireProvenance,
};
use dz_recorder_rows::{
    absent_if_sentinel, BookTop, Derivation, Event, Instrument, MessageTypeLabel, Nanos,
    PortRoleLabel, RecvTsKindLabel,
};

use crate::book::{book_key, state_key, Book, BookRefused, Change};
use crate::instruments::{At, Channel, InstrumentTable, Observed, Statement};

/// What a derivation needs that the archive does not carry.
#[derive(Debug, Clone)]
pub struct EventInput<'a> {
    pub identity: &'a RecorderIdentity,
    pub feed: &'a str,
    pub object_key: &'a str,
    pub object_sha256: &'a str,
    pub segment_seq: u64,
    /// The feed's `Magic`. Required and with no default, for the reason the
    /// codec's own walk requires it: it is the only thing that stops a datagram
    /// misrouted from another feed in the family being parsed at the wrong
    /// layout.
    pub magic: u16,
    /// Which **publisher-side** observation point this view of the book came
    /// from, as `site` names a recorder. Two recorders of one multicast feed
    /// are two observations, and a race is one `state_key` seen at more than
    /// one of them.
    ///
    /// **Never a venue-side observation.** This value reaches `book_top` rows,
    /// and each of those carries `source_addr`, `channel_id`, `dst_port`,
    /// `sequence_number`, `message_index` and `reset_count`, none of them
    /// nullable and every one a statement about a datagram — a venue's upstream
    /// message is not one. (`book_top.observation`'s own comment names the
    /// neighbours that are non-nullable for reasons of their own, so the
    /// criterion can be checked against the DDL rather than counted.) The
    /// pairing then groups on four expressions — `channel_id`,
    /// `instrument_id`, `state_key` and the occurrence ordinal, which is the
    /// query's own, numbered rather than supplied — and a venue side can
    /// compute none of the three identifiers. A venue-side observation is its
    /// own grain, paired on `(feed, symbol, book_key, occurrence)` with
    /// [`book_key`](crate::book_key) — see
    /// `docs/superpowers/specs/2026-09-09-recorder-venue-observation-design.md`.
    pub observation: &'a str,
    /// Whether `SnapshotLevel` messages become rows.
    ///
    /// Off by default at every layer above this one. A cycle is `total_levels`
    /// messages per instrument on the runtime's cadence, so persisting every one
    /// puts the largest row count in the system on the port role with the least
    /// analytical value per row. The book consumes them either way, and
    /// `total_levels` against `levels_seen` keeps completeness answerable from
    /// the begin and end rows alone.
    pub persist_snapshot_levels: bool,
    /// Whether the datagrams behind these rows were kept and verified.
    ///
    /// An input for the reason [`object_sha256`](Self::object_sha256) is one: a
    /// fold reads a `Source` and cannot see whether the bytes behind it were an
    /// object whose digest was checked or a capture that kept nothing. No
    /// default — a derivation states its provenance or does not compile.
    pub derivation: Derivation,
}

/// What a fold could not attribute, counted rather than guessed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Refused {
    /// Messages for an instrument with no definition in force at their position.
    pub unresolved_instrument: u64,
    /// Snapshot levels whose `snapshot_id` matches no open cycle.
    ///
    /// A level carries neither an instrument nor a timestamp — it inherits both
    /// from the `SnapshotBegin` its `snapshot_id` ties it to — so a level with no
    /// open cycle is a level nothing can attribute. Guessing the most recent
    /// instrument would silently move levels between books.
    pub orphan_snapshot_level: u64,
    /// Definitions positioned behind the statement already in force.
    pub out_of_order_definition: u64,
}

/// One object's market data rows.
#[derive(Debug, Clone, Default)]
pub struct DerivedEvents {
    pub event: Vec<Event>,
    pub instrument: Vec<Instrument>,
    pub book_top: Vec<BookTop>,
    pub refused: Refused,
    pub book_refused: BookRefused,
}

/// One snapshot cycle, while it is open.
#[derive(Debug, Clone, Copy)]
struct OpenCycle {
    instrument_id: u32,
    upstream_ts: u64,
    levels: u32,
    /// Whether this cycle's `SnapshotEnd` has been folded.
    ///
    /// The entry outlives the end within a call, because a level that arrives
    /// after its own end is still attributed to it and still counts toward
    /// `levels_seen` — the behaviour the archive path has and this does not
    /// change. It is dropped when the call ends, which is what keeps the map to
    /// the cycles actually in flight in a derivation that outlives one call.
    ended: bool,
}

/// Everything one statement needs to become a row, plus when it was last seen.
#[derive(Debug, Clone)]
struct Seen {
    statement: Statement,
    port_role: PortRole,
    last_seen_ts_ns: u64,
    reset_count: u8,
}

/// The state one derivation hands to the next.
///
/// [`derive_events`] builds one of these per object, folds into it and ends it,
/// which is what an archive object is: a unit that begins empty and ends
/// complete. A caller reading a socket has no such unit — it has to cut arrivals
/// into windows, because the fold sorts a whole input before folding any of it —
/// so it keeps one of these across the cut instead, and a window stops being a
/// fresh recorder.
///
/// Three things cross a call and each is load-bearing. The reference data,
/// without which every message preceding a window's first definition for its
/// instrument is refused as `unresolved_instrument`. The book, which carries
/// both its open cycles and its per-instance sequence high-water marks, so a gap
/// straddling a boundary is counted instead of merely establishing the mark
/// again. And the snapshot-id attribution map, without which the levels after a
/// boundary land as `orphan_snapshot_level` — a level carries neither an
/// instrument nor a timestamp, which is precisely why it carries the id.
///
/// See `docs/superpowers/specs/2026-09-10-recorder-derivation-state-design.md`.
#[derive(Debug, Default)]
pub struct DerivationState {
    table: InstrumentTable,
    /// Open snapshot cycles, keyed on the `snapshot_id` a level is attributed
    /// through.
    ///
    /// Separate from the book's own cycle map, and not a duplicate of it: this
    /// one answers *which instrument is this level's*, and the book's
    /// accumulates the prices to anchor from.
    cycles: BTreeMap<(ChannelInstance, u32), OpenCycle>,
    book: Book,
    /// Where this derivation's datagram numbering has reached.
    ///
    /// [`WireProvenance::datagram_index`] is a position in what the `Source`
    /// yielded, and every call absorbs into a fresh [`WireCapture`] whose
    /// counter starts at zero. Without a base, the same bytes split at a
    /// datagram boundary would produce rows numbering the same datagram
    /// differently on either side of the cut, for no reason a reader of the rows
    /// could see.
    datagrams: u64,
    /// The book's cumulative refusals as at the end of the previous call.
    ///
    /// [`Book::refused`] is a running total for the life of the book, so a
    /// caller that sums it per window double-counts. Subtracting this makes the
    /// reported figure per call, like every other counter on [`DerivedEvents`].
    reported: BookRefused,
}

impl DerivationState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The derivation ended: close open cycles and report what they refused.
    ///
    /// A cycle still open here anchored nothing, and nothing else in the fold
    /// would ever say so.
    ///
    /// **A live caller calls this once — at shutdown, or when it abandons the
    /// state — and never per window.** A cycle open at a window boundary is
    /// still open, and counting it there would make `unclosed_cycle` rise once
    /// per boundary per cycle as a matter of course, which is the failure
    /// keeping the state across the cut exists to remove.
    ///
    /// `#[must_use]` because the figure has nowhere else to go: the cycles are
    /// cleared here, so a caller that drops it has lost what its last window
    /// stranded rather than deferred reading it.
    #[must_use]
    pub fn close_object(&mut self) -> BookRefused {
        self.book.close_object();
        // The book's close clears its own cycles; the attribution map is the
        // other half of the same statement and is cleared with it, or a level
        // arriving after the derivation ended would be attributed to a cycle
        // the book has already counted as having anchored nothing.
        self.cycles.clear();
        refused_since(self.book.refused, &mut self.reported)
    }

    /// The reference data this derivation has accumulated so far.
    ///
    /// Everything this entry point buys depends on this table being populated,
    /// so a live caller needs to be able to say whether it is, and nothing else
    /// on the result answers that: a window that refused nothing and a window
    /// with no prices in it look the same from the rows.
    /// [`InstrumentTable::defined_count`] is the gauge, per channel, and
    /// [`InstrumentTable::era`] says which era those statements belong to.
    ///
    /// It is also the measurement that settles whether a window's refusal share
    /// tracks the cadence on which definitions are restated — a phase
    /// relationship rather than a number to tune — which is worth settling
    /// before a window length is chosen to match it by coincidence.
    #[must_use]
    pub const fn table(&self) -> &InstrumentTable {
        &self.table
    }
}

/// The refusals one call is responsible for: the book's running total, less
/// whatever earlier calls have already reported.
fn refused_since(total: BookRefused, reported: &mut BookRefused) -> BookRefused {
    // Saturating because the alternative to a nonsense count is a visible zero,
    // not because it can trigger: `Book::refused` only ever rises, the book is
    // private to the `DerivationState`, and `reported` is only ever assigned from it.
    // A plain subtraction would wrap in release and put a number near u64::MAX
    // in a row, which is the one outcome worse than under-reporting.
    let delta = BookRefused {
        incomplete_cycle: total
            .incomplete_cycle
            .saturating_sub(reported.incomplete_cycle),
        stale_cycle: total.stale_cycle.saturating_sub(reported.stale_cycle),
        unclosed_cycle: total.unclosed_cycle.saturating_sub(reported.unclosed_cycle),
    };
    *reported = total;
    delta
}

/// Two per-call refusal counts as one.
///
/// Every figure in [`BookRefused`] is a count of occurrences, so a derivation
/// reported in parts is the sum of its parts. This is the rule that lets
/// [`derive_events`] report a whole object while folding and ending it
/// separately, and the rule a caller applies to sum its windows.
const fn plus(a: BookRefused, b: BookRefused) -> BookRefused {
    BookRefused {
        incomplete_cycle: a.incomplete_cycle + b.incomplete_cycle,
        stale_cycle: a.stale_cycle + b.stale_cycle,
        unclosed_cycle: a.unclosed_cycle + b.unclosed_cycle,
    }
}

/// Walk one object and derive its market data rows.
///
/// The state is built here and ended here, which is what an object is. A caller
/// cutting a live feed into windows wants [`derive_events_into`] and a
/// [`DerivationState`] it keeps across the cut.
///
/// # Errors
///
/// [`RelowerError::MulticastArchive`] if the source fails before it is
/// exhausted. A partial object must not be derived: every message after the tear
/// would be missing from a table that is supposed to hold all of them.
pub fn derive_events<S: Source + ?Sized>(
    source: &mut S,
    input: &EventInput<'_>,
) -> Result<DerivedEvents, RelowerError> {
    let mut state = DerivationState::new();
    let mut out = derive_events_into(&mut state, source, input)?;
    // The object ended, so the cycles it stranded are counted before the
    // counters are read. Both figures are per call and per call they compose by
    // addition, so over fresh state this is the book's whole running total —
    // which is why this path reads exactly as it did before.
    out.book_refused = plus(out.book_refused, state.close_object());
    Ok(out)
}

/// Walk one input into state that outlives it, and derive its rows.
///
/// The fold is [`derive_events`]'s and the difference is only where the state
/// lives. Two things stay local to the call rather than crossing it: the
/// instrument grain for definitions observed *in this call*, which persisting
/// would re-emit in every later window; and the once-per-datagram dedup for
/// sequence observation, which has to reset so that the first datagram of a
/// window is tested against the previous window's high-water mark rather than
/// only establishing it.
///
/// `book_refused` on the result is **this call's own** and not the book's
/// running total. The end of the derivation is [`DerivationState::close_object`] and
/// is not this.
///
/// # Errors
///
/// [`RelowerError::MulticastArchive`] if the source fails before it is
/// exhausted, for the reason [`derive_events`] gives.
///
/// **The state is untouched when that happens, and is the thing worth keeping
/// rather than the thing to discard.** The absorb is the only fallible step
/// here and it completes before anything is folded, so a call that fails folds
/// nothing: no row is derived, no counter moves, and the datagram base does not
/// advance. The next window folds into the state as though this call had not
/// happened, and nothing can be folded twice.
///
/// What the failure costs is on the source's side. [`WireCapture::absorb`]
/// consumes as it reads, so the datagrams it took before the tear go with the
/// capture dropped here and a retry over the same source resumes after them
/// rather than at them. The loss is a partial window's datagrams, which is the
/// direction to fail in — a caller that discarded the state as well would pay
/// that loss and then re-pay the first-window refusals this entry point exists
/// to remove.
pub fn derive_events_into<S: Source + ?Sized>(
    state: &mut DerivationState,
    source: &mut S,
    input: &EventInput<'_>,
) -> Result<DerivedEvents, RelowerError> {
    let mut capture = WireCapture::new();
    capture.absorb(source, input.magic)?;

    let mut ordered: Vec<Decoded<'_>> = Vec::new();
    ordered.extend(capture.reference_messages().iter().map(Decoded::Reference));
    ordered.extend(capture.messages().iter().map(Decoded::Market));
    ordered.extend(capture.state_messages().iter().map(Decoded::State));
    ordered.sort_by_key(|decoded| {
        let at = decoded.provenance();
        (at.datagram_index, at.message_index)
    });

    // Borrowed out of the state rather than constructed, which is the whole of
    // the difference: the fold below is the fold it was, and these three now
    // outlive the call.
    let DerivationState {
        table,
        cycles,
        book,
        datagrams,
        reported,
    } = state;
    let base = *datagrams;

    let mut seen: BTreeMap<(ChannelInstance, u32, u64), Seen> = BTreeMap::new();
    let mut at_datagram: Option<u64> = None;
    let mut out = DerivedEvents::default();

    for decoded in ordered {
        let mut provenance = *decoded.provenance();
        // Numbered from where this derivation has reached rather than from zero.
        // The sort above is by the unshifted index, which orders identically:
        // adding a constant to every position moves none of them past another.
        provenance.datagram_index += base;
        let instance = instance_of(&provenance);
        // Reference data is keyed on the channel, not the instance: definitions
        // arrive on `refdata` and prices on `mktdata`, which are two instances,
        // and a lookup keyed on the port would never find them.
        let channel = Channel::of(instance);
        let at = At {
            instance,
            sequence_number: provenance.sequence_number,
            reset_count: provenance.reset_count,
            recv_ts_ns: provenance.recv_ts_ns,
        };

        // Once per datagram, not once per message: a gap belongs to the channel
        // instance's sequence space, and every message inside one datagram
        // carries that datagram's sequence number.
        if at_datagram != Some(provenance.datagram_index) {
            at_datagram = Some(provenance.datagram_index);
            for (instrument_id, change) in
                book.observe_sequence(instance, provenance.role, provenance.sequence_number)
            {
                if let Some(statement) = table.resolve(channel, instrument_id, at.recv_ts_ns) {
                    out.book_top
                        .push(book_row(input, &provenance, statement, &change));
                }
            }
        }

        match decoded {
            Decoded::Reference(message) => match message.body {
                ReferenceBody::Definition(definition) => {
                    match table.observe_definition(&definition, at) {
                        Observed::OutOfOrder => out.refused.out_of_order_definition += 1,
                        Observed::First | Observed::Restated | Observed::Repeated => {}
                    }
                    if let Some(statement) =
                        table.resolve(channel, definition.instrument_id, at.recv_ts_ns)
                    {
                        seen.entry((instance, definition.instrument_id, statement.from_sequence))
                            .and_modify(|entry| entry.last_seen_ts_ns = provenance.recv_ts_ns)
                            .or_insert_with(|| Seen {
                                statement: statement.clone(),
                                port_role: provenance.role,
                                last_seen_ts_ns: provenance.recv_ts_ns,
                                reset_count: provenance.reset_count,
                            });
                    }
                }
                ReferenceBody::Manifest(summary) => table.observe_manifest(&summary, at),
            },
            Decoded::Market(message) => {
                let instrument_id = instrument_of_market(&message.body);
                let Some(statement) = table.resolve(channel, instrument_id, at.recv_ts_ns) else {
                    out.refused.unresolved_instrument += 1;
                    continue;
                };
                let statement = statement.clone();
                out.event
                    .push(market_row(input, &provenance, &statement, &message.body));
                let change = match &message.body {
                    MessageBody::Quote(quote) => book.quote(channel, quote),
                    MessageBody::Level(level) => book.level(channel, level),
                    MessageBody::Clear(clear) => book.clear(channel, clear),
                    // A trade moves no book. It is an event, not a state.
                    MessageBody::Trade(_) => None,
                };
                if let Some(change) = change {
                    out.book_top
                        .push(book_row(input, &provenance, &statement, &change));
                }
            }
            Decoded::State(message) => {
                let Some(instrument_id) =
                    instrument_of_state(&message.body, instance, cycles, &mut out.refused)
                else {
                    continue;
                };
                let Some(statement) = table.resolve(channel, instrument_id, at.recv_ts_ns) else {
                    out.refused.unresolved_instrument += 1;
                    continue;
                };
                let statement = statement.clone();
                // **The book consumes every level; only persistence is
                // optional.** Skipping the level before the book sees it leaves
                // a cycle that never completes, so nothing ever anchors — which
                // is the one thing consuming them is for.
                if !matches!(message.body, StateBody::SnapshotLevel(_))
                    || input.persist_snapshot_levels
                {
                    out.event.push(state_row(
                        input,
                        &provenance,
                        &statement,
                        &message.body,
                        instrument_id,
                        cycles,
                    ));
                }
                let change = match &message.body {
                    StateBody::Reset(reset) => book.reset(channel, reset),
                    StateBody::SnapshotBegin(begin) => {
                        book.snapshot_begin(channel, begin);
                        None
                    }
                    StateBody::SnapshotLevel(level) => {
                        book.snapshot_level(channel, level);
                        None
                    }
                    StateBody::SnapshotEnd(end) => book.snapshot_end(channel, end),
                };
                if let Some(change) = change {
                    out.book_top
                        .push(book_row(input, &provenance, &statement, &change));
                }
            }
        }
    }

    // Cycles that ended in this call are dropped, and the ones still in flight
    // are kept for the next one. `Book::snapshot_end` removes from the book's
    // own map on every end; nothing removed from this one, which cost nothing
    // while it died with the call and would grow by one entry per cycle for the
    // life of a derivation that outlives it. Pruning here rather than on the
    // end itself is what keeps the archive path identical: one call, so an
    // entry lives exactly as long as it did.
    cycles.retain(|_, cycle| !cycle.ended);

    // This call's own refusals, not the book's running total. The cycles still
    // open are left open: they may yet be closed by the next call, and only the
    // end of the derivation can say they anchored nothing.
    out.book_refused = refused_since(book.refused, reported);
    // Everything the source yielded, foreign and undecodable datagrams
    // included, because that is what `datagram_index` is an index into.
    *datagrams += capture.datagrams();
    out.instrument = seen
        .into_iter()
        .map(|((instance, _, _), entry)| Instrument {
            site: input.identity.site.clone(),
            recorder: input.identity.recorder.clone(),
            env: input.identity.env.clone(),
            feed: input.feed.to_owned(),
            port_role: role_label(entry.port_role),
            source_addr: instance.source,
            channel_id: instance.channel_id,
            dst_port: instance.dst_port,
            source_id: entry.statement.source_id,
            instrument_id: entry.statement.instrument_id,
            from_sequence: entry.statement.from_sequence,
            reset_count: entry.reset_count,
            symbol: entry.statement.symbol.clone(),
            price_exp: entry.statement.price_exponent,
            qty_exp: entry.statement.qty_exponent,
            contract_value: entry.statement.contract_value,
            first_seen_ts: Nanos(entry.statement.first_seen_ts_ns),
            last_seen_ts: Nanos(entry.last_seen_ts_ns),
            manifest_seq: Some(entry.statement.manifest_seq),
            declared_count: table.declared_count(Channel::of(instance)),
            object_key: input.object_key.to_owned(),
            derivation: input.derivation,
        })
        .collect();

    Ok(out)
}

/// One decoded message from any of the walk's three outputs.
enum Decoded<'a> {
    Market(&'a dz_recorder_relower::WireMessage),
    State(&'a dz_recorder_relower::StateMessage),
    Reference(&'a dz_recorder_relower::ReferenceMessage),
}

impl Decoded<'_> {
    const fn provenance(&self) -> &WireProvenance {
        match self {
            Self::Market(message) => &message.provenance,
            Self::State(message) => &message.provenance,
            Self::Reference(message) => &message.provenance,
        }
    }
}

/// The channel instance a message arrived on.
///
/// `pub(crate)` because the sizing measurement keys on the same thing this fold
/// does: two readings of one archive that disagreed about which instance a
/// datagram belonged to would be two answers about one feed.
pub(crate) fn instance_of(provenance: &WireProvenance) -> ChannelInstance {
    ChannelInstance::new(
        *provenance.src.ip(),
        provenance.channel_id,
        provenance.dst.port(),
    )
}

const fn instrument_of_market(body: &MessageBody) -> u32 {
    match body {
        MessageBody::Quote(quote) => quote.instrument_id,
        MessageBody::Trade(trade) => trade.instrument_id,
        MessageBody::Level(level) => level.instrument_id,
        MessageBody::Clear(clear) => clear.instrument_id,
    }
}

/// The instrument a state message belongs to, opening and closing cycles as it
/// goes.
fn instrument_of_state(
    body: &StateBody,
    instance: ChannelInstance,
    cycles: &mut BTreeMap<(ChannelInstance, u32), OpenCycle>,
    refused: &mut Refused,
) -> Option<u32> {
    match body {
        StateBody::Reset(reset) => Some(reset.instrument_id),
        StateBody::SnapshotBegin(begin) => {
            cycles.insert(
                (instance, begin.snapshot_id),
                OpenCycle {
                    instrument_id: begin.instrument_id,
                    upstream_ts: begin.timestamp_ns,
                    levels: 0,
                    ended: false,
                },
            );
            Some(begin.instrument_id)
        }
        StateBody::SnapshotLevel(level) => {
            let Some(cycle) = cycles.get_mut(&(instance, level.snapshot_id)) else {
                refused.orphan_snapshot_level += 1;
                return None;
            };
            cycle.levels += 1;
            Some(cycle.instrument_id)
        }
        StateBody::SnapshotEnd(end) => {
            // Marked rather than removed: `state_row` still reads `levels` off
            // this entry for the end row's `levels_seen`, and a level arriving
            // after the end is still attributed within this call.
            if let Some(cycle) = cycles.get_mut(&(instance, end.snapshot_id)) {
                cycle.ended = true;
            }
            Some(end.instrument_id)
        }
    }
}

fn market_row(
    input: &EventInput<'_>,
    provenance: &WireProvenance,
    statement: &Statement,
    body: &MessageBody,
) -> Event {
    let mut row = base_row(input, provenance, statement);
    match body {
        MessageBody::Quote(quote) => {
            row.message_type = MessageTypeLabel::Quote;
            row.upstream_ts = Some(Nanos(quote.source_timestamp_ns));
            row.flags_raw = Some(quote.update_flags);
            row.bid_px_raw = Some(quote.bid_price);
            row.bid_qty_raw = Some(quote.bid_qty);
            row.bid_source_count = Some(quote.bid_source_count);
            row.ask_px_raw = Some(quote.ask_price);
            row.ask_qty_raw = Some(quote.ask_qty);
            row.ask_source_count = Some(quote.ask_source_count);
        }
        MessageBody::Trade(trade) => {
            row.message_type = MessageTypeLabel::Trade;
            row.upstream_ts = Some(Nanos(trade.source_timestamp_ns));
            row.side_raw = Some(trade.aggressor_side);
            row.flags_raw = Some(trade.trade_flags);
            row.price_raw = Some(trade.trade_price);
            row.qty_raw = Some(trade.trade_qty);
            row.trade_id = Some(trade.trade_id);
            row.cumulative_volume = Some(trade.cumulative_volume);
        }
        MessageBody::Level(level) => {
            row.message_type = MessageTypeLabel::LevelUpdate;
            row.upstream_ts = Some(Nanos(level.timestamp_ns));
            row.per_instrument_seq = Some(level.per_instrument_seq);
            row.side_raw = Some(level.side);
            row.action_raw = Some(level.action);
            row.reason_raw = Some(level.update_reason);
            row.flags_raw = Some(level.level_flags);
            row.price_raw = Some(level.price_raw);
            row.qty_raw = Some(level.qty_raw);
            row.order_count = absent_if_sentinel(level.order_count);
            row.level_index = absent_if_sentinel(level.level_index);
        }
        MessageBody::Clear(clear) => {
            row.message_type = MessageTypeLabel::BookClear;
            row.upstream_ts = Some(Nanos(clear.timestamp_ns));
            row.per_instrument_seq = Some(clear.per_instrument_seq);
            row.side_raw = Some(clear.clear_side);
            row.action_raw = Some(clear.scope);
            row.reason_raw = Some(clear.clear_reason);
            row.price_raw = Some(clear.from_price_raw);
        }
    }
    row
}

fn state_row(
    input: &EventInput<'_>,
    provenance: &WireProvenance,
    statement: &Statement,
    body: &StateBody,
    instrument_id: u32,
    cycles: &BTreeMap<(ChannelInstance, u32), OpenCycle>,
) -> Event {
    let mut row = base_row(input, provenance, statement);
    row.instrument_id = instrument_id;
    match body {
        StateBody::Reset(reset) => {
            row.message_type = MessageTypeLabel::InstrumentReset;
            row.upstream_ts = Some(Nanos(reset.timestamp_ns));
            row.reason_raw = Some(reset.reason);
            // The terms of its own recovery. A deriver that drops this accepts a
            // snapshot already in flight when the reset was published — a book
            // the publisher had disowned — and rebuilds from it as certain.
            row.anchor_seq = Some(reset.new_anchor_seq);
        }
        StateBody::SnapshotBegin(begin) => {
            row.message_type = MessageTypeLabel::SnapshotBegin;
            row.upstream_ts = Some(Nanos(begin.timestamp_ns));
            row.snapshot_id = Some(begin.snapshot_id);
            row.anchor_seq = Some(begin.anchor_seq);
            row.total_levels = Some(begin.total_levels);
            row.depth_bound = Some(begin.depth_bound);
            row.per_instrument_seq = Some(begin.last_instrument_seq);
        }
        StateBody::SnapshotLevel(level) => {
            let cycle = cycles.get(&(instance_of(provenance), level.snapshot_id));
            row.message_type = MessageTypeLabel::SnapshotLevel;
            // Inherited from the begin this level's `snapshot_id` ties it to. The
            // level carries neither, which is precisely why it carries the id.
            row.upstream_ts = cycle.map(|open| Nanos(open.upstream_ts));
            row.snapshot_id = Some(level.snapshot_id);
            row.side_raw = Some(level.side);
            row.flags_raw = Some(level.level_flags);
            row.price_raw = Some(level.price_raw);
            row.qty_raw = Some(level.qty_raw);
            row.order_count = absent_if_sentinel(level.order_count);
            // Assigned by this fold from arrival order within the cycle, not read
            // from a field that does not exist. One-based, so it reads as an
            // ordinal rather than as an offset somebody will compare to a wire
            // value.
            row.level_index = cycle.and_then(|open| u16::try_from(open.levels).ok());
        }
        StateBody::SnapshotEnd(end) => {
            row.message_type = MessageTypeLabel::SnapshotEnd;
            row.snapshot_id = Some(end.snapshot_id);
            row.anchor_seq = Some(end.anchor_seq);
            row.levels_seen = cycles
                .get(&(instance_of(provenance), end.snapshot_id))
                .map(|open| open.levels);
        }
    }
    row
}

fn base_row(input: &EventInput<'_>, provenance: &WireProvenance, statement: &Statement) -> Event {
    Event {
        recv_ts: Nanos(provenance.recv_ts_ns),
        send_ts: Nanos(provenance.send_timestamp_ns),
        upstream_ts: None,
        recv_ts_kind: RecvTsKindLabel::from(provenance.recv_ts_kind),
        site: input.identity.site.clone(),
        recorder: input.identity.recorder.clone(),
        env: input.identity.env.clone(),
        feed: input.feed.to_owned(),
        port_role: role_label(provenance.role),
        source_addr: *provenance.src.ip(),
        channel_id: provenance.channel_id,
        dst_port: provenance.dst.port(),
        sequence_number: provenance.sequence_number,
        reset_count: provenance.reset_count,
        segment_seq: input.segment_seq,
        message_index: provenance.message_index,
        source_id: statement.source_id,
        instrument_id: statement.instrument_id,
        symbol: statement.symbol.clone(),
        price_exp: statement.price_exponent,
        qty_exp: statement.qty_exponent,
        per_instrument_seq: None,
        message_type: MessageTypeLabel::Quote,
        side_raw: None,
        action_raw: None,
        reason_raw: None,
        flags_raw: None,
        price_raw: None,
        qty_raw: None,
        order_count: None,
        level_index: None,
        bid_px_raw: None,
        bid_qty_raw: None,
        bid_source_count: None,
        ask_px_raw: None,
        ask_qty_raw: None,
        ask_source_count: None,
        trade_id: None,
        cumulative_volume: None,
        snapshot_id: None,
        anchor_seq: None,
        total_levels: None,
        levels_seen: None,
        depth_bound: None,
        object_key: input.object_key.to_owned(),
        object_sha256: input.object_sha256.to_owned(),
        derivation: input.derivation,
        datagram_index: provenance.datagram_index,
    }
}

const fn role_label(role: PortRole) -> PortRoleLabel {
    match role {
        PortRole::Mktdata => PortRoleLabel::Mktdata,
        PortRole::Refdata => PortRoleLabel::Refdata,
        PortRole::Snapshot => PortRoleLabel::Snapshot,
    }
}

/// One change in a top of book, as a row.
fn book_row(
    input: &EventInput<'_>,
    provenance: &WireProvenance,
    statement: &Statement,
    change: &Change,
) -> BookTop {
    BookTop {
        recv_ts: Nanos(provenance.recv_ts_ns),
        send_ts: Nanos(provenance.send_timestamp_ns),
        site: input.identity.site.clone(),
        recorder: input.identity.recorder.clone(),
        env: input.identity.env.clone(),
        feed: input.feed.to_owned(),
        observation: input.observation.to_owned(),
        source_addr: *provenance.src.ip(),
        channel_id: provenance.channel_id,
        dst_port: provenance.dst.port(),
        source_id: statement.source_id,
        instrument_id: statement.instrument_id,
        symbol: statement.symbol.clone(),
        sequence_number: provenance.sequence_number,
        message_index: provenance.message_index,
        reset_count: provenance.reset_count,
        segment_seq: input.segment_seq,
        bid_px_raw: change.top.bid.price_raw,
        bid_qty_raw: change.top.bid.qty_raw,
        bid_source_count: change.top.bid.source_count,
        ask_px_raw: change.top.ask.price_raw,
        ask_qty_raw: change.top.ask.qty_raw,
        ask_source_count: change.top.ask.source_count,
        price_exp: statement.price_exponent,
        qty_exp: statement.qty_exponent,
        state_key: state_key(provenance.channel_id, statement.instrument_id, &change.top),
        // The shared function and never a second fold written here. It is not
        // `state_key`'s fold with the identifiers left out: `book_key` reads a
        // zero source count as the absence the top-of-book specification says
        // it is, which is what makes one book one key on both sides of the
        // race, and `state_key` may not — its value is in rows already.
        book_key: book_key(&change.top),
        from_anchor: u8::from(change.from_anchor),
        book_certain: u8::from(change.certainty.certain),
        uncertain_since: change.certainty.since,
        uncertain_reason: change.certainty.reason,
        object_key: input.object_key.to_owned(),
        derivation: input.derivation,
    }
}
