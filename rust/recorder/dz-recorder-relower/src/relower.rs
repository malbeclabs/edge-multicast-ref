//! Re-running the adapter over the archived payloads, and lowering what it
//! emits.
//!
//! This is the half of Mode C that the whole boundary was constrained for.
//! [`Adapter::on_payload`] is synchronous, does no I/O, and is a pure function
//! of its payload and the adapter's own state, so replaying an archive of what
//! the upstream actually sent produces the same events the publisher produced.
//! Lowering those with **the publisher's own lowering** — the same crate, not a
//! copy of it — then produces the same messages, and the difference between the
//! two sides is the publisher's behaviour rather than two implementations of one
//! rule.
//!
//! Three things about the driving are load-bearing:
//!
//! - **The payloads are replayed in receive order.** An adapter keeps a book and
//!   the lowering keeps `Per-Instrument Seq`; both are functions of the order
//!   the events arrived in. Replaying out of order re-lowers a different stream
//!   and every depth key is wrong. [`PayloadArchive`] states that in its
//!   contract.
//! - **One [`DepthLowering`] drives the whole window.** It carries the sequence,
//!   so rebuilding it would restart the counter — which is the one thing a
//!   subscriber cannot tell apart from a channel reset, and here it would break
//!   the join key outright.
//! - **The instrument table is the archive's**, and there is no way to pass in
//!   another. See [`crate::refdata`].
//!
//! [`Adapter::on_payload`]: dz_adapter_core::Adapter::on_payload

use std::collections::BTreeMap;

use dz_adapter_core::{
    Adapter, Event, EventSink, InstrumentRef, InstrumentSpec, ListingSink, ParseError,
};
use dz_edge_refdata::SYMBOL_LEN;
use dz_publisher_lowering::{DepthLowering, InstrumentTable, Lowering, LoweringError, SourceId};

use crate::archive::PayloadArchive;
use crate::error::RelowerError;
use crate::finding::Caveat;
use crate::refdata::{ArchivedRefdata, MissingDefinition};
use crate::wire::MessageBody;

/// Where a re-lowered message came from.
///
/// **Provenance, never compared**, and the counterpart of
/// [`WireProvenance`](crate::WireProvenance): what it is for is finding the
/// upstream bytes again. An operator handed a finding replays this payload
/// through the adapter's own fixture test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReLoweredProvenance {
    /// Position of the payload in the archive, counting from 0.
    pub payload_index: u64,
    /// Position of the event within that payload's events, counting from 0.
    pub event_index: u32,
    /// When the transport received the payload, on the publisher's host.
    pub recv_ts_ns: u64,
}

impl core::fmt::Display for ReLoweredProvenance {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "payload {}, event {}",
            self.payload_index, self.event_index
        )
    }
}

/// One message the re-lowering produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoweredMessage {
    pub body: MessageBody,
    pub provenance: ReLoweredProvenance,
}

/// An event the re-lowering could not turn into a message.
///
/// **In the report, and not folded into the findings.** A refusal here means the
/// wire copy of that message has nothing to join against, so it is reported as
/// *on the wire, not in the re-lowered stream* — and a reader who cannot see the
/// refusal reads that as the publisher inventing traffic. The commonest cause is
/// the honest one: the archived exponent cannot state the venue's value exactly,
/// which is what the lowering refuses rather than rounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub payload_index: u64,
    pub event_index: u32,
    /// The message type the event would have become.
    pub message_type: &'static str,
    /// The instrument, where the handle still resolves to one.
    pub instrument_id: Option<u32>,
    /// The lowering's own reason token, which is the metric label a live
    /// publisher would have counted this under.
    pub reason: &'static str,
    /// The wire field the refusal is about, where there is one.
    pub field: Option<&'static str>,
}

impl core::fmt::Display for Refusal {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "payload {}, event {}: {} refused ({}",
            self.payload_index, self.event_index, self.message_type, self.reason
        )?;
        if let Some(field) = self.field {
            write!(f, " on {field}")?;
        }
        write!(f, ")")
    }
}

/// A payload the adapter could not parse.
///
/// The publisher's own adapter refused the identical bytes and counted it under
/// the identical reason, so this is not a finding — but the events it would have
/// produced are absent from both sides, and a window full of them is a window
/// that compared very little.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseFailure {
    pub payload_index: u64,
    /// The parse-error taxonomy's own token.
    pub reason: &'static str,
    pub detail: String,
}

/// Everything re-running the adapter produced.
#[derive(Debug, Clone, Default)]
pub struct ReLowered {
    messages: Vec<LoweredMessage>,
    refusals: Vec<Refusal>,
    parse_failures: Vec<ParseFailure>,
    missing_definitions: Vec<MissingDefinition>,
    caveats: Vec<Caveat>,
    payloads: u64,
    upstream_messages: u64,
    events: u64,
    events_not_lowerable: u64,
}

impl ReLowered {
    /// The messages, in the order the adapter's events produced them.
    #[must_use]
    pub fn messages(&self) -> &[LoweredMessage] {
        &self.messages
    }

    /// Events the lowering refused.
    #[must_use]
    pub fn refusals(&self) -> &[Refusal] {
        &self.refusals
    }

    /// Payloads the adapter refused.
    #[must_use]
    pub fn parse_failures(&self) -> &[ParseFailure] {
        &self.parse_failures
    }

    /// Instruments the adapter offered that the archive holds no definition
    /// for, in `Symbol` order.
    #[must_use]
    pub fn missing_definitions(&self) -> &[MissingDefinition] {
        &self.missing_definitions
    }

    /// What the re-lowering could not establish about its own inputs.
    #[must_use]
    pub fn caveats(&self) -> &[Caveat] {
        &self.caveats
    }

    /// How many payloads were replayed.
    #[must_use]
    pub const fn payloads(&self) -> u64 {
        self.payloads
    }

    /// How many upstream messages the adapter recognised inside them.
    #[must_use]
    pub const fn upstream_messages(&self) -> u64 {
        self.upstream_messages
    }

    /// How many events the adapter emitted.
    #[must_use]
    pub const fn events(&self) -> u64 {
        self.events
    }

    /// Events of a kind this build has no lowering for.
    ///
    /// `Event` is `#[non_exhaustive]` and the market-by-order variants land with
    /// their codec crate. Counted rather than ignored, so a window of them is
    /// visible as a window this comparison did not cover.
    #[must_use]
    pub const fn events_not_lowerable(&self) -> u64 {
        self.events_not_lowerable
    }
}

/// Replay an archive of upstream payloads through an adapter and lower what it
/// emits.
///
/// `refdata` is the published set as reconstructed from the multicast archive,
/// and `source_id` the publisher's identity as the same archive states it.
/// Neither has a live counterpart in this signature, which is the enforcement of
/// Mode C's third requirement rather than a note about it.
///
/// # Errors
///
/// [`RelowerError::PayloadArchive`] if the archive fails before it is
/// exhausted. A partial re-lowering must not be compared: every message after
/// the tear is on the wire and absent from the re-lowering, which reads as a
/// publisher inventing traffic.
pub fn relower<A: Adapter + ?Sized, P: PayloadArchive + ?Sized>(
    adapter: &mut A,
    payloads: &mut P,
    refdata: &ArchivedRefdata,
    source_id: SourceId,
) -> Result<ReLowered, RelowerError> {
    let mut admissions = Admissions::new(refdata);
    let tob = Lowering::new(source_id);
    // One for the whole window: it carries `Per-Instrument Seq`, and rebuilding
    // it would restart the series the depth join key *is*.
    let mut depth = DepthLowering::new(source_id);
    let mut out = ReLowered::default();

    // Before the first payload, because an adapter that has already loaded its
    // universe offers it before it has seen anything.
    admissions.poll(adapter, 0);

    loop {
        let archived = payloads.next().map_err(RelowerError::PayloadArchive)?;
        let Some(archived) = archived else { break };
        let payload_index = out.payloads;
        out.payloads += 1;

        let payload = archived.as_payload();
        let mut sink = Collector {
            table: &admissions.table,
            tob: &tob,
            depth: &mut depth,
            payload_index,
            recv_ts_ns: archived.recv_ts_ns,
            event_index: 0,
            out: &mut out,
        };
        if let Err(error) = adapter.on_payload(&payload, &mut sink) {
            let failure = ParseFailure {
                payload_index,
                reason: error.as_str(),
                detail: parse_detail(&error),
            };
            out.parse_failures.push(failure);
        }

        // After every payload, because an instrument the venue listed in it must
        // be admitted before the events that follow. The runtime polls on its
        // own cadence instead, and the archive does not record that cadence —
        // see `Caveat::InstrumentAdmittedInsideWindow`.
        admissions.poll(adapter, out.payloads);
    }

    out.missing_definitions = admissions.missing_definitions();
    out.caveats = admissions.caveats;
    Ok(out)
}

/// The detail string off a parse error, which is `&'static str` inside the error
/// and worth keeping.
fn parse_detail(error: &ParseError) -> String {
    error.to_string()
}

/// The admitted set, resolved against the archive.
///
/// This is the [`ListingSink`] the adapter is handed, and it is where the
/// archive's authority over reference data is actually applied: a symbol the
/// capture defined is admitted with **the `Instrument ID` and the exponents the
/// capture states**, and a symbol the capture did not define is declined.
struct Admissions<'a> {
    refdata: &'a ArchivedRefdata,
    table: InstrumentTable,
    handles: BTreeMap<[u8; SYMBOL_LEN], InstrumentRef>,
    missing: BTreeMap<String, u64>,
    caveats: Vec<Caveat>,
    /// The payload index the current poll is happening at, so an admission
    /// inside the window can be flagged.
    at_payload: u64,
}

impl<'a> Admissions<'a> {
    fn new(refdata: &'a ArchivedRefdata) -> Self {
        Self {
            refdata,
            table: InstrumentTable::new(),
            handles: BTreeMap::new(),
            missing: BTreeMap::new(),
            caveats: Vec::new(),
            at_payload: 0,
        }
    }

    fn poll<A: Adapter + ?Sized>(&mut self, adapter: &mut A, at_payload: u64) {
        self.at_payload = at_payload;
        adapter.poll_listings(self);
    }

    fn missing_definitions(&self) -> Vec<MissingDefinition> {
        self.missing
            .iter()
            .map(|(symbol, offers)| MissingDefinition {
                symbol: symbol.clone(),
                offers: *offers,
            })
            .collect()
    }
}

impl ListingSink for Admissions<'_> {
    fn list(&mut self, spec: &InstrumentSpec<'_>) -> Option<InstrumentRef> {
        let (field, _fit) = dz_edge_core::pad_ascii::<SYMBOL_LEN>(spec.symbol);

        // Already admitted, and still admitted: the same handle, so an adapter
        // may re-offer its whole set on every poll.
        if let Some(handle) = self.handles.get(&field) {
            if self.table.get(*handle).is_ok() {
                return Some(*handle);
            }
        }

        let Some(archived) = self.refdata.by_symbol(spec.symbol) else {
            // Declined, and recorded. Minting an `Instrument ID` or inventing an
            // exponent here would produce messages that join against nothing,
            // and every one of them would be reported as the publisher dropping
            // a message it never had reference data for.
            *self.missing.entry(spec.symbol.to_owned()).or_insert(0) += 1;
            return None;
        };

        // The venue's own exponents in the listing are the ones it stated to the
        // publisher; the archive's are the ones the publisher published. Where
        // they differ, the archive wins — that is what "reference data comes
        // from the archive" means — and the difference itself is a finding for
        // the reference-data tier rather than for this join.
        let handle = self.table.admit(archived.as_instrument());
        self.handles.insert(field, handle);
        if self.at_payload > 0 {
            let caveat = Caveat::InstrumentAdmittedInsideWindow {
                instrument_id: archived.instrument_id,
                at_payload: self.at_payload,
            };
            if !self.caveats.contains(&caveat) {
                self.caveats.push(caveat);
            }
        }
        Some(handle)
    }

    fn delist(&mut self, instrument: InstrumentRef) {
        // Honoured, because the publisher honoured it: events after a delisting
        // were refused there and are refused here, for the same reason and under
        // the same token.
        self.table.withdraw(instrument);
    }
}

/// The [`EventSink`] the adapter writes into: lower, and keep.
///
/// The event borrows the payload, so it is lowered here rather than collected
/// and lowered afterwards — which is also how a live runtime does it, and the
/// reason the two sides produce the same bytes.
struct Collector<'a> {
    table: &'a InstrumentTable,
    tob: &'a Lowering,
    depth: &'a mut DepthLowering,
    payload_index: u64,
    recv_ts_ns: u64,
    event_index: u32,
    out: &'a mut ReLowered,
}

impl Collector<'_> {
    fn provenance(&self) -> ReLoweredProvenance {
        ReLoweredProvenance {
            payload_index: self.payload_index,
            event_index: self.event_index,
            recv_ts_ns: self.recv_ts_ns,
        }
    }

    fn keep(&mut self, body: MessageBody) {
        let provenance = self.provenance();
        self.out.messages.push(LoweredMessage { body, provenance });
    }

    fn refuse(
        &mut self,
        message_type: &'static str,
        instrument: InstrumentRef,
        error: LoweringError,
    ) {
        let instrument_id = self
            .table
            .get(instrument)
            .ok()
            .map(|instrument| instrument.instrument_id);
        self.out.refusals.push(Refusal {
            payload_index: self.payload_index,
            event_index: self.event_index,
            message_type,
            instrument_id,
            reason: error.reason(),
            field: error.field(),
        });
    }
}

impl EventSink for Collector<'_> {
    fn upstream_message(&mut self, _message_type: &'static str) {
        self.out.upstream_messages += 1;
    }

    fn event(&mut self, event: Event<'_>) {
        self.out.events += 1;
        match event {
            Event::Quote {
                instrument,
                source_ts_ns,
                bid,
                ask,
            } => match self
                .tob
                .lower_quote(self.table, instrument, source_ts_ns, bid, ask)
            {
                Ok(quote) => self.keep(MessageBody::Quote(quote)),
                Err(error) => self.refuse("Quote", instrument, error),
            },
            // Through the top-of-book lowering whichever feed the archive holds,
            // and that is safe structurally rather than by inspection: the wire
            // requires a venue's `Trade` to be byte-identical between its feeds,
            // and both channels' `lower_trade` delegate to one function. A
            // depth-feed trade re-lowered here is the same bytes the depth
            // channel would have produced, so this comparison does not have to
            // know which channel emitted it — which is just as well, because the
            // event does not say.
            Event::Trade {
                instrument,
                source_ts_ns,
                px,
                qty,
                aggressor,
                trade_id,
                cumulative_volume,
                flags,
            } => match self.tob.lower_trade(
                self.table,
                instrument,
                source_ts_ns,
                px,
                qty,
                aggressor,
                trade_id,
                cumulative_volume,
                flags,
            ) {
                Ok(trade) => self.keep(MessageBody::Trade(trade)),
                Err(error) => self.refuse("Trade", instrument, error),
            },
            Event::Level {
                instrument,
                source_ts_ns,
                side,
                px,
                qty,
                order_count,
                presence,
            } => match self.depth.lower_level(
                self.table,
                instrument,
                source_ts_ns,
                side,
                px,
                qty,
                order_count,
                presence,
            ) {
                Ok(level) => self.keep(MessageBody::Level(level)),
                Err(error) => self.refuse("LevelUpdate", instrument, error),
            },
            Event::Clear {
                instrument,
                source_ts_ns,
                scope,
            } => match self
                .depth
                .lower_clear(self.table, instrument, source_ts_ns, scope)
            {
                Ok(clear) => self.keep(MessageBody::Clear(clear)),
                Err(error) => self.refuse("BookClear", instrument, error),
            },
            // `Event` is `#[non_exhaustive]`: the market-by-order variants land
            // with their codec crate. Counted, so that a window of events this
            // build cannot lower is visible rather than reported as a publisher
            // inventing every message it sent.
            _ => self.out.events_not_lowerable += 1,
        }
        self.event_index += 1;
    }
}
