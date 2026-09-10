//! The derivation: a venue's own `Adapter`, driven over an archived object.
//!
//! # The adapter is an argument, because this repository links no venue
//!
//! Decoding a venue's bytes requires the venue's `Adapter`, and nothing here
//! may contain one. The publisher side already solved exactly this: `run` takes
//! an `AdapterRegistry` composed by the venue's `main`, and the adapter arrives
//! through a registry this repository never populates. So
//! [`derive_venue_object`] takes `&mut dyn Adapter` and a venue's own recorder
//! binary is three lines, the way its publisher's `main` is.
//!
//! # The same sink the publisher uses
//!
//! The events are collected through [`EventSink`], the trait the publisher's
//! runtime hands an adapter — not a second sink shape. An adapter that could
//! tell it was under a recorder rather than under a publisher would be an
//! adapter that could behave differently there, and the whole value of this
//! comparison is that it cannot.
//!
//! That also means the ordering contract is the same one: `upstream_message`
//! states the message boundary, `upstream_identity` states which message it is
//! and is in force only until the next boundary, and the events of that message
//! follow. Nothing above the boundary can check it, so what is done here is to
//! honour it exactly — an identity is attached to the events that *follow* it
//! and is dropped at the next boundary rather than carried across one.
//!
//! # A refusal costs one message
//!
//! An adapter that refuses a message costs **that message** and is counted by
//! the reason it gave. A derivation that stopped at the first message a venue's
//! own adapter could not parse would report the venue's feed as having ended
//! there — and the rows would be indistinguishable from a venue that went
//! quiet. So the loop continues, the refusal lands in `venue_object.refusals`
//! under its own reason, and the rows either side of it are written.
//!
//! # The listings are polled per message, and that is what makes this
//! idempotent
//!
//! The publisher's runtime drains `poll_listings` on its own cadence, and an
//! instrument becomes available to a payload only once a poll has minted its
//! handle. A derivation cannot observe that cadence: it is a wall clock, and a
//! wall clock in a derivation means the same object derived twice produces two
//! different sets of rows.
//!
//! So the cadence here is the object's own — one poll per message. It is the
//! only cadence that is a function of the object rather than of when the
//! derivation happened to run, which is precisely what
//! `(object key, sha256)` idempotence requires. Re-offering an admitted
//! instrument is free and returns the handle already minted, which the adapter
//! boundary states as a property an implementation may rely on, so the cost is
//! a map lookup per instrument per message and no allocation.

use std::collections::BTreeMap;

use dz_adapter_core::{
    Adapter, ClearScope, Desync, Event, EventSink, InstrumentRef, InstrumentSpec, ListingSink,
    ParseError, Payload, Scalar, Side as EventSide, SideUpdate,
};
use dz_publisher_lowering::{price_at, qty_at};
use dz_recorder_archive::upstream::UpstreamFormatError;
use dz_recorder_events::{book_key, Side, Top};
use dz_recorder_rows::Nanos;

use crate::object::{VenueObject, VenueObjectId};
use crate::rows::{
    RefusalCount, VenueBookTop, VenueObjectRow, VenueRowBatch, VenueRowSink, VenueRowSinkError,
};

/// An object could not be derived.
///
/// **Three failures, and every one of them refuses the whole object.** That is
/// the opposite of how an adapter's refusal of a message is treated, and the
/// difference is which side the fault is on: a message the venue's adapter
/// cannot parse is a fact about that message, while an object that cannot be
/// read or a connection nobody declared is a fact about the object or about the
/// configuration, and deriving part of it would put rows in a table under a
/// window nobody can state.
#[derive(Debug, thiserror::Error)]
pub enum DeriveError {
    /// The object is not readable as one, or ends inside a record.
    #[error(transparent)]
    Format(#[from] UpstreamFormatError),

    /// The object attributes a message to a connection the caller did not
    /// declare.
    ///
    /// `Payload::connection` is the only thing that distinguishes one upstream's
    /// data from another's, and an adapter's mapping may depend on it. A
    /// derivation that substituted some other connection would hand the adapter
    /// a payload attributed to an upstream it did not come from, and the events
    /// that came out would be a statement about the wrong market.
    #[error("{object_key} names the connection {connection:?}, which this caller did not declare")]
    UndeclaredConnection {
        object_key: String,
        connection: String,
    },

    /// The object holds no messages.
    ///
    /// An empty rotation is not published — a gap in the sequence of objects is
    /// how a reader learns the archive has one — so an object with nothing in it
    /// is a defect rather than a quiet window. It has no receive window to
    /// state, and stating one as zero would put a row in a partition dated 1970.
    #[error("{object_key} holds no upstream messages")]
    EmptyObject { object_key: String },

    #[error(transparent)]
    Sink(#[from] VenueRowSinkError),
}

/// What one object produced.
///
/// The same numbers the [`VenueObjectRow`] carries, handed back so that a
/// caller can count a pass without reading its own rows back out of a sink.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Derived {
    pub message_count: u64,
    pub refused_count: u64,
    pub refusals: Vec<RefusalCount>,
    pub event_count: u64,
    pub unpriced_count: u64,
    pub desync_count: u64,
    pub unattributed_count: u64,
    pub book_top_count: u64,
    pub instrument_count: u32,
}

/// Drives one venue's `Adapter` over one archived object and writes the rows.
///
/// The adapter is the caller's: this repository never constructs one. The object
/// is read in recorded order, because an adapter keeps a book and an object
/// replayed out of order re-derives a different one.
///
/// One batch is written, at the end, holding both grains. The object is the unit
/// that either landed or did not — an object whose book rows landed while its
/// object row did not is an object that reads as never having been derived.
///
/// # Errors
///
/// [`DeriveError`]. An adapter's refusal of a message is **not** one of them: it
/// is counted and the loop continues.
pub fn derive_venue_object(
    adapter: &mut dyn Adapter,
    object: &mut dyn VenueObject,
    out: &mut dyn VenueRowSink,
) -> Result<Derived, DeriveError> {
    let id = object.id().clone();
    let format_version = object.format_version();
    let connections = object.declared_connections();
    let mut state = Fold::new(&id);

    let mut index = 0u64;
    loop {
        // Before the message and not after it: an instrument the adapter has
        // discovered has no handle until a poll mints one, so a poll that ran
        // after the payload would drop that payload's events.
        adapter.poll_listings(&mut state);

        let Some(message) = object.next_message()? else {
            break;
        };
        let Some(connection) = id.connection(message.connection) else {
            return Err(DeriveError::UndeclaredConnection {
                object_key: id.object_key,
                connection: message.connection.to_owned(),
            });
        };

        state.begin_message(index, message.connection, message.recv_ts_ns);
        let payload = Payload {
            bytes: message.bytes,
            recv_ts_ns: message.recv_ts_ns,
            connection,
        };
        // `payload_scope` around the call, as the runtime does it: an event
        // reported between the two is attributable to this payload, and one
        // reported outside them is attributable to nothing.
        state.payload_scope(Some(message.recv_ts_ns));
        let refusal = adapter.on_payload(&payload, &mut state).err();
        state.payload_scope(None);
        if let Some(refusal) = refusal {
            state.refuse(refusal);
        }
        state.end_message();
        index += 1;
    }

    if state.message_count == 0 {
        return Err(DeriveError::EmptyObject {
            object_key: id.object_key,
        });
    }

    let derived = state.derived();
    let batch = VenueRowBatch {
        book_tops: state.rows,
        objects: vec![VenueObjectRow {
            recv_ts_start: Nanos(state.first_recv_ts.unwrap_or_default()),
            recv_ts_end: Nanos(state.last_recv_ts.unwrap_or_default()),
            observation: id.observation.clone(),
            env: id.env.clone(),
            feed: id.feed.clone(),
            object_key: id.object_key.clone(),
            object_sha256: id.object_sha256.clone(),
            format_version,
            connections,
            message_count: derived.message_count,
            refused_count: derived.refused_count,
            refusals: derived.refusals.clone(),
            event_count: derived.event_count,
            unpriced_count: derived.unpriced_count,
            desync_count: derived.desync_count,
            unattributed_count: derived.unattributed_count,
            book_top_count: derived.book_top_count,
            instrument_count: derived.instrument_count,
        }],
    };
    out.write_batch(batch)?;
    Ok(derived)
}

/// What rests at one price, at the instrument's own exponents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Resting {
    qty_raw: u64,
    /// How many upstreams contributed to this side of the top, and `None` for a
    /// level a `Level` stated.
    ///
    /// A level's `order_count` is orders at a price and a quote's
    /// `source_count` is upstreams contributing to a top. Different quantities,
    /// so mapping one onto the other would put a number in a column that does
    /// not mean what the column says — and a level therefore rests with this
    /// absent, while the level a `Quote` established keeps the number the quote
    /// stated.
    source_count: Option<u16>,
}

/// One instrument the adapter offered, and the book this derivation holds for
/// it.
#[derive(Debug)]
struct Instrument {
    symbol: String,
    price_exp: i8,
    qty_exp: i8,
    top: Top,
    /// The book's resting levels, price to what rests there, both already at the
    /// instrument's exponents.
    ///
    /// **[`Fold::top_of_levels`] over these is the top, whichever event moved
    /// them.** A `Quote` states a complete two-sided top and replaces these with
    /// the one level per side it states; a `Level` and a `Clear` move them. So
    /// one connection carrying both shapes — a book snapshot and then the
    /// increments over it, which is the commonest shape a venue publishes —
    /// derives the book the venue published.
    ///
    /// **The two shapes have to compose, because nothing states them apart.** A
    /// `Quote` that established the top and left these alone is discarded by the
    /// very next `Level` on the instrument: the top is recomputed from these
    /// maps, so the side that `Level` did not touch comes back absent and the row
    /// says a side is gone that the venue never withdrew. `dz-recorder-events`'
    /// own book keys a quote and a level on a `Channel` and can hold them apart
    /// for that reason — a top-of-book feed and a depth feed are different
    /// channels. One venue connection is not two, and `Event` is one enumeration
    /// over it.
    ///
    /// A quote **replaces** rather than merges, because it is authoritative
    /// about the top and silent about the depth beneath it: a level it superseded
    /// would otherwise be computed as a top the venue has already replaced.
    bids: BTreeMap<i64, Resting>,
    asks: BTreeMap<i64, Resting>,
    /// Whether anything has been applied. A book with both sides absent and
    /// nothing applied is the state before anything happened, not a change.
    established: bool,
    /// Whether the adapter has withdrawn this listing.
    ///
    /// **What a delisting ends is this listing, and everything it held.** The
    /// handle stays minted and keeps resolving to this symbol, because a row
    /// already written under it must keep meaning what it meant — that is the
    /// failure `delist` is documented as never causing. What stops is the book:
    /// [`Fold::settle`] writes nothing for a withdrawn listing, so an event the
    /// adapter reports on a handle it has already delisted is counted as the
    /// event it was and produces no row.
    ///
    /// A relisting does not come back here. [`Fold::delist`] drops the symbol's
    /// mapping, so the next `list_on` mints a **new** handle over the spec the
    /// venue states then — its own exponents, its own empty book. Two listings
    /// of one symbol are two instruments that happen to share a name, and this
    /// is what keeps the second from inheriting the first's top.
    delisted: bool,
}

/// The derivation's own state: the listing sink, the event sink, and the book.
///
/// One object implementing both sinks, because they are two halves of one
/// derivation: the listings are what the events' prices are scaled against, and
/// a second object holding one of them would have to be handed the other's
/// table.
struct Fold {
    observation: String,
    env: String,
    feed: String,
    object_key: String,
    object_sha256: String,

    instruments: Vec<Instrument>,
    /// Symbol to handle index, so that re-offering an admitted instrument
    /// returns the handle already minted rather than a second one.
    by_symbol: BTreeMap<String, u32>,

    rows: Vec<VenueBookTop>,

    message_index: u64,
    /// Top changes written for the record being read, counted from zero.
    ///
    /// Reset at every record and **not** at every `upstream_message`, because
    /// what it has to distinguish is a row: a batched payload settles once per
    /// member, and one member settles once per event that moves the top. See
    /// [`VenueBookTop::change_index`].
    change_index: u64,
    connection: String,
    recv_ts_ns: u64,
    /// In force only until the next `upstream_message`, exactly as the sink's
    /// contract states. A sink that carried it across one would attribute every
    /// event to the message before.
    identity: Option<(Option<u64>, Option<u64>)>,
    /// `Some` while a payload is being mapped. An event reported outside it is
    /// attributable to no payload, and is counted rather than written.
    in_payload: Option<u64>,

    message_count: u64,
    event_count: u64,
    unpriced_count: u64,
    desync_count: u64,
    unattributed_count: u64,
    refusals: BTreeMap<&'static str, u64>,
    refused_count: u64,
    first_recv_ts: Option<u64>,
    last_recv_ts: Option<u64>,
}

impl Fold {
    fn new(id: &VenueObjectId) -> Self {
        Self {
            observation: id.observation.clone(),
            env: id.env.clone(),
            feed: id.feed.clone(),
            object_key: id.object_key.clone(),
            object_sha256: id.object_sha256.clone(),
            instruments: Vec::new(),
            by_symbol: BTreeMap::new(),
            rows: Vec::new(),
            message_index: 0,
            change_index: 0,
            connection: String::new(),
            recv_ts_ns: 0,
            identity: None,
            in_payload: None,
            message_count: 0,
            event_count: 0,
            unpriced_count: 0,
            desync_count: 0,
            unattributed_count: 0,
            refusals: BTreeMap::new(),
            refused_count: 0,
            first_recv_ts: None,
            last_recv_ts: None,
        }
    }

    fn begin_message(&mut self, index: u64, connection: &str, recv_ts_ns: u64) {
        self.message_index = index;
        // Back to zero at every record, so the ordinal is *within* the record
        // the way the record's index is within the object. A counter that ran
        // over the object would be a second numbering of the same rows and
        // would still be in the sort key — the pair is what identifies a
        // change, and each half says what it says.
        self.change_index = 0;
        if self.connection != connection {
            self.connection.clear();
            self.connection.push_str(connection);
        }
        self.recv_ts_ns = recv_ts_ns;
        self.identity = None;
        self.message_count += 1;
        self.first_recv_ts = Some(
            self.first_recv_ts
                .map_or(recv_ts_ns, |at| at.min(recv_ts_ns)),
        );
        self.last_recv_ts = Some(
            self.last_recv_ts
                .map_or(recv_ts_ns, |at| at.max(recv_ts_ns)),
        );
    }

    fn end_message(&mut self) {
        self.identity = None;
    }

    fn refuse(&mut self, error: ParseError) {
        self.refused_count += 1;
        *self.refusals.entry(error.as_str()).or_default() += 1;
    }

    fn derived(&self) -> Derived {
        Derived {
            message_count: self.message_count,
            refused_count: self.refused_count,
            refusals: self
                .refusals
                .iter()
                .map(|(reason, count)| RefusalCount((*reason).to_owned(), *count))
                .collect(),
            event_count: self.event_count,
            unpriced_count: self.unpriced_count,
            desync_count: self.desync_count,
            unattributed_count: self.unattributed_count,
            book_top_count: self.rows.len() as u64,
            instrument_count: u32::try_from(self.instruments.len()).unwrap_or(u32::MAX),
        }
    }

    /// The side an update states, at the instrument's own exponents, or `None`
    /// when it cannot be stated exactly.
    ///
    /// Exact or refused, never rounded. A rounded price is a price the venue did
    /// not quote, and a conversion taken as zero is a real-looking quote at
    /// nothing — the shipped defect the adapter boundary was shaped around.
    fn side_of(update: SideUpdate<'_>, price_exp: i8, qty_exp: i8) -> Option<Side> {
        match update {
            // Nothing rests here. All three fields absent, which is the
            // distinguished tag `book_key` folds: an empty side and a side
            // priced at zero are different books.
            SideUpdate::Gone => Some(Side {
                price_raw: None,
                qty_raw: None,
                source_count: None,
            }),
            SideUpdate::Present {
                px,
                qty,
                source_count,
            } => Some(Side {
                price_raw: Some(price_at(px, price_exp).ok()?),
                qty_raw: Some(qty_at(qty, qty_exp).ok()?),
                // A zero is the top-of-book field's own *unavailable*, and the
                // adapter boundary says as much: `Some(0)` is the absence said
                // in a way that does not survive. Read as the absence here so
                // that the row and `book_key` describe the same book — a row
                // saying zero beside a key that folded an absence is two
                // answers to one question.
                source_count: source_count.filter(|count| *count != 0),
            }),
        }
    }

    /// One scalar at an exponent, or `None` when it cannot be stated exactly.
    fn raw_price(value: Scalar<'_>, exponent: i8) -> Option<i64> {
        price_at(value, exponent).ok()
    }

    fn raw_qty(value: Scalar<'_>, exponent: i8) -> Option<u64> {
        qty_at(value, exponent).ok()
    }

    /// The top the book's levels state, whichever event established them.
    ///
    /// The one place a top is computed, so that a quote and a level compose
    /// rather than answer the question twice.
    fn top_of_levels(instrument: &Instrument) -> Top {
        let bid = instrument
            .bids
            .iter()
            .next_back()
            .map_or(Side::default(), |(px, resting)| Side {
                price_raw: Some(*px),
                qty_raw: Some(resting.qty_raw),
                source_count: resting.source_count,
            });
        let ask = instrument
            .asks
            .iter()
            .next()
            .map_or(Side::default(), |(px, resting)| Side {
                price_raw: Some(*px),
                qty_raw: Some(resting.qty_raw),
                source_count: resting.source_count,
            });
        Top { bid, ask }
    }

    /// Replaces one side's levels with the single level a quote states.
    ///
    /// A side the quote states as gone leaves the side empty, which is the book
    /// the quote stated: all three fields absent, the distinguished tag
    /// `book_key` folds.
    fn seed_side(levels: &mut BTreeMap<i64, Resting>, side: &Side) {
        levels.clear();
        if let (Some(price_raw), Some(qty_raw)) = (side.price_raw, side.qty_raw) {
            levels.insert(
                price_raw,
                Resting {
                    qty_raw,
                    source_count: side.source_count,
                },
            );
        }
    }

    /// Writes a row if the top moved.
    fn settle(&mut self, handle: u32, was: Top) {
        let Some(instrument) = self.instruments.get(handle as usize) else {
            return;
        };
        // A withdrawn listing's book cannot change, and this is the one place
        // that is enforced. An event the adapter reports on a handle it has
        // already delisted re-fills the level maps above like any other, and
        // stops here: it is counted as the event it was, and no row claims the
        // venue moved a book it has withdrawn.
        if instrument.delisted {
            return;
        }
        if instrument.top == was {
            return;
        }
        // A book with both sides absent and nothing ever applied is the state
        // before anything happened, not a change worth a row.
        if !instrument.established && instrument.top == Top::default() {
            return;
        }
        let (sid, seq) = self.identity.unwrap_or((None, None));
        let row = VenueBookTop {
            recv_ts: Nanos(self.recv_ts_ns),
            observation: self.observation.clone(),
            env: self.env.clone(),
            feed: self.feed.clone(),
            connection: self.connection.clone(),
            upstream_sid: sid,
            upstream_seq: seq,
            symbol: instrument.symbol.clone(),
            price_exp: instrument.price_exp,
            qty_exp: instrument.qty_exp,
            bid_px_raw: instrument.top.bid.price_raw,
            bid_qty_raw: instrument.top.bid.qty_raw,
            bid_source_count: instrument.top.bid.source_count,
            ask_px_raw: instrument.top.ask.price_raw,
            ask_qty_raw: instrument.top.ask.qty_raw,
            ask_source_count: instrument.top.ask.source_count,
            book_key: book_key(&instrument.top),
            message_index: self.message_index,
            change_index: self.change_index,
            object_key: self.object_key.clone(),
            object_sha256: self.object_sha256.clone(),
        };
        self.rows.push(row);
        // Only for a change that produced a row. An ordinal that advanced on a
        // settle that wrote nothing would leave holes a reader would read as
        // rows that had been collapsed away.
        self.change_index += 1;
    }
}

impl ListingSink for Fold {
    fn list_on(&mut self, _shard: &str, spec: &InstrumentSpec<'_>) -> Option<InstrumentRef> {
        // **The shard is deliberately dropped.** It is a venue's own word for a
        // partition it computes, and what it resolves to — a `Channel ID`, a
        // group, a port, a sequence series, an era — is the operator's and is
        // unreachable from the adapter boundary. A venue-side observation
        // therefore has no `Channel ID` to record, and this is the one place
        // where it might have looked as though it did.
        if let Some(handle) = self.by_symbol.get(spec.symbol) {
            // Re-offering an admitted instrument returns the handle already
            // minted, which is what lets an adapter offer its whole set on
            // every poll — and what makes a poll per message cheap.
            //
            // **A delisted symbol is not in this map**, so a venue that
            // relists one arrives below and is admitted as a new instrument
            // with the spec it states now. Returning the withdrawn handle
            // would interpret the new listing's events at the old listing's
            // exponents and over the old listing's book.
            return Some(InstrumentRef::from_admission(*handle));
        }
        let handle = u32::try_from(self.instruments.len()).ok()?;
        self.instruments.push(Instrument {
            symbol: spec.symbol.to_owned(),
            price_exp: spec.price_exponent,
            qty_exp: spec.qty_exponent,
            top: Top::default(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            established: false,
            delisted: false,
        });
        self.by_symbol.insert(spec.symbol.to_owned(), handle);
        Some(InstrumentRef::from_admission(handle))
    }

    fn delist(&mut self, instrument: InstrumentRef) {
        let index = instrument.index();
        // Silent for a handle this derivation never minted, and idempotent:
        // withdrawing something already gone is the state the caller asked for,
        // which is how `InstrumentTable::withdraw` answers the same question on
        // the publisher side.
        let Some(held) = self.instruments.get_mut(index as usize) else {
            return;
        };
        // **The whole book, and not the level maps alone.** The top and the
        // `established` flag are the same listing's state as the levels are, so
        // clearing two of the four leaves the slot describing a live listing
        // whose depth has gone: the next `top_of_levels` reads empty maps and
        // is compared against a top that is still there. Emptied together, and
        // then `delisted` is what `settle` reads.
        held.bids.clear();
        held.asks.clear();
        held.top = Top::default();
        held.established = false;
        held.delisted = true;
        // The exponents and the symbol **stay**, because they are this handle's
        // own identity: a row already written under it names them, and the
        // handle keeps resolving. What keeps them out of the next listing's way
        // is the line below rather than a reset here.
        let symbol = held.symbol.clone();
        // The symbol stops resolving to this handle, so a relisting mints a new
        // one over the spec the venue states then rather than inheriting this
        // one's — the handle itself is **not** reused and is not removed from
        // `instruments`, because a row already written under it must keep
        // meaning what it meant.
        //
        // Guarded on the handle, because the mapping may already have moved on:
        // an adapter that relists and only then delists the handle it was
        // holding would otherwise unadmit the live listing.
        if self.by_symbol.get(&symbol) == Some(&index) {
            self.by_symbol.remove(&symbol);
        }
    }
}

impl EventSink for Fold {
    fn upstream_message(&mut self, _message_type: &'static str) {
        // The message boundary a sink sees. The identity in force belonged to
        // the message that has just ended, so it is dropped here rather than
        // carried into this one — an adapter whose upstream numbers some
        // members of a batch and not others leaves those events with no
        // identity, which is the honest answer.
        self.identity = None;
    }

    fn upstream_identity(&mut self, sid: Option<u64>, seq: Option<u64>) {
        self.identity = Some((sid, seq));
    }

    fn payload_scope(&mut self, recv_ts_ns: Option<u64>) {
        self.in_payload = recv_ts_ns;
    }

    fn desynchronised(&mut self, _instrument: InstrumentRef, _reason: Desync) {
        // Counted on the object and never written as a certainty column. What a
        // venue's resynchronisation means for a book is the venue's, and
        // `book_certain` on the publisher side means something else — a gap in
        // the publisher's own sequence space. One column with two meanings,
        // minimum-aggregated over a pair, mixes them silently.
        self.desync_count += 1;
    }

    fn event(&mut self, event: Event<'_>) {
        if self.in_payload.is_none() {
            // Attributable to no payload: a runtime's own tick, or a sink
            // written to by something that is not an adapter mapping a message.
            // Counted onto the object row rather than dropped in silence — the
            // derivation opens the scope around `on_payload`, so an event
            // outside it is an adapter that closed the scope itself, which the
            // sink's contract permits and nothing above could otherwise see.
            self.unattributed_count += 1;
            return;
        }
        self.event_count += 1;

        match event {
            Event::Quote {
                instrument,
                bid,
                ask,
                ..
            } => {
                let handle = instrument.index();
                let Some(held) = self.instruments.get(handle as usize) else {
                    // A handle this derivation never minted. Reachable, because
                    // an `InstrumentRef` is a handle and not a capability — the
                    // lowering refuses one for the same reason.
                    self.unpriced_count += 1;
                    return;
                };
                let (price_exp, qty_exp) = (held.price_exp, held.qty_exp);
                let was = held.top;
                let (Some(bid), Some(ask)) = (
                    Self::side_of(bid, price_exp, qty_exp),
                    Self::side_of(ask, price_exp, qty_exp),
                ) else {
                    self.unpriced_count += 1;
                    return;
                };
                let held = &mut self.instruments[handle as usize];
                // The quote's own two levels, and nothing the quote superseded.
                // The top is then read back off the levels like any other, so a
                // `Level` or a `Clear` that follows composes with the quote
                // instead of discarding it.
                Self::seed_side(&mut held.bids, &bid);
                Self::seed_side(&mut held.asks, &ask);
                held.established = true;
                let now = Self::top_of_levels(&self.instruments[handle as usize]);
                self.instruments[handle as usize].top = now;
                self.settle(handle, was);
            }

            Event::Level {
                instrument,
                side,
                px,
                qty,
                ..
            } => {
                let handle = instrument.index();
                let Some(held) = self.instruments.get(handle as usize) else {
                    self.unpriced_count += 1;
                    return;
                };
                let (price_exp, qty_exp) = (held.price_exp, held.qty_exp);
                let was = held.top;
                let (Some(px), Some(qty)) =
                    (Self::raw_price(px, price_exp), Self::raw_qty(qty, qty_exp))
                else {
                    self.unpriced_count += 1;
                    return;
                };
                let held = &mut self.instruments[handle as usize];
                let levels = match side {
                    EventSide::Bid => &mut held.bids,
                    EventSide::Ask => &mut held.asks,
                };
                // Absolute and never a delta: a quantity of zero removes the
                // level, and the specification's own reason is that a subscriber
                // that added it to what it held would drift.
                if qty == 0 {
                    levels.remove(&px);
                } else {
                    levels.insert(
                        px,
                        Resting {
                            qty_raw: qty,
                            source_count: None,
                        },
                    );
                }
                held.established = true;
                let now = Self::top_of_levels(&self.instruments[handle as usize]);
                self.instruments[handle as usize].top = now;
                self.settle(handle, was);
            }

            Event::Clear {
                instrument, scope, ..
            } => {
                let handle = instrument.index();
                let Some(held) = self.instruments.get(handle as usize) else {
                    self.unpriced_count += 1;
                    return;
                };
                let price_exp = held.price_exp;
                let was = held.top;
                let bounded = match scope {
                    ClearScope::FromPrice { px, .. } => match Self::raw_price(px, price_exp) {
                        Some(px) => Some(px),
                        None => {
                            self.unpriced_count += 1;
                            return;
                        }
                    },
                    ClearScope::EntireSide(_) | ClearScope::BothSides => None,
                };
                let held = &mut self.instruments[handle as usize];
                match scope {
                    ClearScope::BothSides => {
                        held.bids.clear();
                        held.asks.clear();
                    }
                    ClearScope::EntireSide(EventSide::Bid) => held.bids.clear(),
                    ClearScope::EntireSide(EventSide::Ask) => held.asks.clear(),
                    // Outward from `px`, inclusive: away from the top of that
                    // side, which for bids is downward and for asks upward.
                    ClearScope::FromPrice {
                        side: EventSide::Bid,
                        ..
                    } => {
                        let from = bounded.expect("a bounded clear resolved its price");
                        held.bids.retain(|price, _| *price > from);
                    }
                    ClearScope::FromPrice {
                        side: EventSide::Ask,
                        ..
                    } => {
                        let from = bounded.expect("a bounded clear resolved its price");
                        held.asks.retain(|price, _| *price < from);
                    }
                }
                held.established = true;
                let now = Self::top_of_levels(&self.instruments[handle as usize]);
                self.instruments[handle as usize].top = now;
                self.settle(handle, was);
            }

            // A trade moves no book. Counted as an event, because the adapter
            // emitted one, and no top-of-book row follows from it.
            Event::Trade { .. } => {}

            // `Event` is `#[non_exhaustive]`: a variant added to the boundary
            // is a variant this derivation has not been taught, and counting it
            // as an event it could not place is the honest answer. Writing a
            // book row from a message shape nobody mapped is not.
            _ => {}
        }
    }
}
