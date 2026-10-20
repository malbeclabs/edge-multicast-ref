//! The second input model: decode on each connection, apply on the loop.
//!
//! An [`Adapter`] maps a payload inline, on the publisher's own loop, and that
//! is the right model for a cheap decode. It is the wrong one for a venue whose
//! payload is text that is expensive to decode, delivered redundantly over
//! several connections at a high rate: every copy is decoded on one core, and
//! most copies are found to be duplicates only after the work is done.
//!
//! A [`ThreadedAdapter`] splits [`Adapter::on_payload`] in two.
//!
//! - **[`Decoder::decode`] runs on the connection's own task**, one decoder per
//!   connection, in parallel with the others. It turns a payload into owned
//!   [`Event<'static>`](Event)s, each tagged with an [`Order`], and may drop a
//!   copy another connection already claimed before it has paid for the rest of
//!   the decode.
//! - **[`ThreadedAdapter::on_decoded`] runs on the publisher loop**, serially,
//!   for every event that survived the runtime's dedupe. An adapter holding a
//!   book suppresses an unchanged quote here, as it would in `on_payload`.
//!
//! Nothing a specification decided moves into the decode stage: a decoder
//! produces the same events an adapter produces today.
//!
//! # Dedupe has two stages, and the runtime owns both
//!
//! - **The connection stage, for [`Order::Supersedes`] only.** A decoder reads
//!   the instrument and the key first and calls [`ClaimTable::claim`]. A lost
//!   claim is a copy some connection has already delivered, so the decoder calls
//!   [`DecodedSink::duplicate`] and stops, before parsing the prices.
//! - **The loop stage, the authority.** Two claims can reach the queue in the
//!   other order — A claims 10, B claims 11, and B's event is dequeued first —
//!   so the loop repeats the check against the last key it applied and drops
//!   anything not above it. [`Order::Identity`] is checked only here, against a
//!   bounded window, because a point event is not superseded by a later one.
//!   [`Dedupe`] is that stage, and the inline helper runs the same one.
//!
//! A venue that restarts an instrument's counter would leave a newest-wins key
//! below its own high-water mark for good. [`CounterEras`] is the decoder's way
//! out: an era it advances when it judges a restart, carried in the key, which
//! both stages then order correctly.
//!
//! # Only quotes and trades, and no text
//!
//! [`DecodedSink::push`] carries [`Event::Quote`] and [`Event::Trade`]. A
//! [`Event::Level`] or [`Event::Clear`] is a delta, and two connections' deltas
//! reordered on a queue corrupt a book while every per-connection check passes,
//! which no first-copy floor can prevent. A [`Scalar::Text`] is the expensive
//! half of the decode left undone, and an owned event cannot borrow the payload
//! it came from anyway. Both are refused at `push`: the event is dropped and the
//! payload is counted as a parse error. See [`carried`].
//!
//! # A threaded adapter is also an inline one
//!
//! [`on_payload`] runs a decode and then `on_decoded` in place, through the
//! same [`Dedupe`], and [`Inline`] keeps a decoder per connection for it. An
//! implementation's [`Adapter::on_payload`] calls it, so the recorder's
//! re-lowering, a test and a single-connection deployment drive the same code
//! serially and replay stays a diff:
//!
//! ```
//! use dz_adapter_core::threaded::{ClaimTable, DecodedSink, Decoder, Inline, Order, ThreadedAdapter};
//! use dz_adapter_core::{
//!     Adapter, ConnectionId, Event, EventSink, InstrumentRef, ListingSink, ParseError, Payload,
//!     Scalar, SideUpdate,
//! };
//!
//! /// Payloads of the form `<instrument>:<update id>:<bid in cents>`.
//! struct Quotes {
//!     inline: Option<Inline>,
//! }
//!
//! struct QuoteDecoder {
//!     claims: ClaimTable,
//! }
//!
//! impl Decoder for QuoteDecoder {
//!     fn decode(
//!         &mut self,
//!         payload: &Payload<'_>,
//!         out: &mut dyn DecodedSink,
//!     ) -> Result<(), ParseError> {
//!         let text = std::str::from_utf8(payload.bytes).map_err(|_| ParseError::malformed("utf-8"))?;
//!         let mut fields = text.split(':');
//!         let mut next = |what| {
//!             fields
//!                 .next()
//!                 .and_then(|field| field.parse::<u64>().ok())
//!                 .ok_or(ParseError::malformed(what))
//!         };
//!         let instrument = InstrumentRef::from_admission(next("instrument")? as u32);
//!         let update = next("update")?;
//!         out.upstream_message("quote");
//!         // Claimed before the expensive part, so a copy another connection
//!         // has already delivered costs no more than this.
//!         if !self.claims.claim(instrument, update) {
//!             out.duplicate();
//!             return Ok(());
//!         }
//!         let bid = next("bid")? as i64;
//!         out.push(
//!             Event::Quote {
//!                 instrument,
//!                 source_ts_ns: 0,
//!                 bid: SideUpdate::Present {
//!                     px: Scalar::fixed(bid, -2),
//!                     qty: Scalar::fixed(1, 0),
//!                     source_count: None,
//!                 },
//!                 ask: SideUpdate::Gone,
//!             },
//!             Order::Supersedes(update),
//!         );
//!         Ok(())
//!     }
//! }
//!
//! impl ThreadedAdapter for Quotes {
//!     fn decoder(&self, _conn: ConnectionId, claims: ClaimTable) -> Box<dyn Decoder> {
//!         Box::new(QuoteDecoder { claims })
//!     }
//! }
//!
//! impl Adapter for Quotes {
//!     fn message_types(&self) -> &[&'static str] {
//!         &["quote"]
//!     }
//!
//!     fn poll_listings(&mut self, _out: &mut dyn ListingSink) {}
//!
//!     /// The inline model: the same decode, the same dedupe, in place.
//!     fn on_payload(
//!         &mut self,
//!         payload: &Payload<'_>,
//!         out: &mut dyn EventSink,
//!     ) -> Result<(), ParseError> {
//!         // Taken out and put back, because `Inline` borrows the adapter it
//!         // is held by. Allocates only on the first payload.
//!         let mut inline = self.inline.take().unwrap_or_default();
//!         let result = inline.on_payload(self, payload, out);
//!         self.inline = Some(inline);
//!         result
//!     }
//! }
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use crate::adapter::Adapter;
use crate::error::ParseError;
use crate::event::{Event, SideUpdate};
use crate::instrument::InstrumentRef;
use crate::payload::{ConnectionId, Payload};
use crate::scalar::Scalar;
use crate::sink::EventSink;

/// An adapter whose decode can run off the publisher loop.
///
/// Composed with `Venue::threaded` rather than `Venue::new`, which is the whole
/// of how a runtime knows to drive it this way. Everything else about it is an
/// [`Adapter`]: its listings, its connect and disconnect callbacks, its upstream
/// writes and its snapshots are called exactly as they are for an inline one.
/// The connection callbacks are called from that connection's task, under the
/// lock that already serialises an adapter, so they are as rare as they always
/// were and now lightly contended.
///
/// [`Adapter::on_payload`] is still required, and an implementation writes it
/// with [`on_payload`] or [`Inline`]: see the module documentation. A runtime
/// driving the threaded model never calls it.
pub trait ThreadedAdapter: Adapter {
    /// One decoder per connection, built once at startup and kept across
    /// reconnects. It runs on that connection's task.
    ///
    /// `claims` is shared by every connection's decoder: it is how one learns
    /// that another has already delivered an update. See [`ClaimTable`].
    fn decoder(&self, conn: ConnectionId, claims: ClaimTable) -> Box<dyn Decoder>;

    /// One decoded event that survived dedupe, on the publisher loop.
    ///
    /// Defaulted to forward. An adapter holding a book suppresses an unchanged
    /// quote here, as it does in `on_payload` today. Called in the order the
    /// events are applied: per connection, the order they were pushed; across
    /// connections, none; per instrument under [`Order::Supersedes`],
    /// increasing.
    fn on_decoded(&mut self, decoded: &Decoded, out: &mut dyn EventSink) {
        out.event(decoded.event);
    }
}

/// One connection's decode.
///
/// Runs on a connection task. The rule [`Adapter::on_payload`] states holds
/// here too: no blocking, no I/O, no allocation per payload, and a pure function
/// of the payload, the decoder's own state and read-mostly shared state — a
/// [`ClaimTable`] is the shared state it is meant to read.
pub trait Decoder: Send {
    /// One upstream payload in; zero or more owned events out.
    ///
    /// # Errors
    ///
    /// [`ParseError`], counted under its reason exactly as `on_payload`'s is.
    /// One error ends this payload and nothing else.
    fn decode(
        &mut self,
        payload: &Payload<'_>,
        out: &mut dyn DecodedSink,
    ) -> Result<(), ParseError>;
}

/// Where a [`Decoder`] writes.
pub trait DecodedSink {
    /// Name the upstream message type just recognised, before what it produced.
    ///
    /// The same call, counted the same way, as
    /// [`EventSink::upstream_message`]: once per upstream message, and before
    /// its events. Under the threaded model it is counted at decode, so the
    /// rate it shows is arrival rather than drain.
    fn upstream_message(&mut self, message_type: &'static str);

    /// Emit one event, and say how the runtime dedupes it across connections.
    ///
    /// Only [`Event::Quote`] and [`Event::Trade`], and no [`Scalar::Text`] in
    /// either. Anything else is dropped and the payload is counted as a parse
    /// error: see [`carried`] for why.
    fn push(&mut self, event: Event<'static>, order: Order);

    /// A copy dropped by a lost claim, before it was fully decoded.
    ///
    /// Counted on `dz_publisher_ingress_duplicates_total`, with the copies the
    /// loop drops. A decoder that does not claim never calls it.
    fn duplicate(&mut self);
}

/// How the runtime dedupes one event across connections.
///
/// `#[non_exhaustive]`, so that a third rule does not break an implementation
/// that matches on this one. A decoder constructs these and does not need to
/// match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Order {
    /// No cross-connection identity. Forwarded as delivered.
    None,
    /// State: a strictly increasing per-instrument key, newest wins.
    ///
    /// The key is the venue's own update number for the instrument, which every
    /// connection carrying it states identically. An event whose key is not
    /// above the last one applied for its instrument is a duplicate or a stale
    /// copy and is dropped.
    Supersedes(u64),
    /// A point event: each key once per instrument, inside a window.
    ///
    /// The key is the venue's own identity for the event — a trade ID — and is
    /// not assumed to be ordered. The window is the last [`IDENTITY_WINDOW`]
    /// keys applied for the instrument, so a copy that arrives after that many
    /// newer events has been forgotten and is applied again.
    Identity(u64),
}

/// One decoded event, as the publisher loop applies it.
///
/// Fields rather than accessors, and not `#[non_exhaustive]`, so that a venue's
/// own tests can build one to drive [`ThreadedAdapter::on_decoded`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decoded {
    /// The event, owned: it borrows nothing from the payload it came from.
    pub event: Event<'static>,
    /// How it was deduped.
    pub order: Order,
    /// The connection that delivered it.
    pub connection: ConnectionId,
    /// [`Payload::recv_ts_ns`] of the payload it was decoded from.
    pub recv_ts_ns: u64,
}

/// The instrument a pushed event belongs to, or why `push` refuses it.
///
/// The one rule both sides of [`DecodedSink::push`] apply, so a runtime and the
/// inline helper refuse the same events.
///
/// # Errors
///
/// [`ParseError::Schema`] for an [`Event::Level`] or an [`Event::Clear`]: a
/// delta, and two connections' deltas reordered on a queue corrupt a book with
/// every per-connection check passing. And for a [`Scalar::Text`] anywhere in a
/// quote or a trade: the decode stage exists to parse the decimal, and a text
/// scalar is that work handed back to the loop.
pub fn carried(event: &Event<'_>) -> Result<InstrumentRef, ParseError> {
    fn text(scalar: Scalar<'_>) -> bool {
        matches!(scalar, Scalar::Text(_))
    }
    fn side_text(side: SideUpdate<'_>) -> bool {
        match side {
            SideUpdate::Gone => false,
            SideUpdate::Present { px, qty, .. } => text(px) || text(qty),
        }
    }
    const TEXT: ParseError =
        ParseError::schema("the threaded model carries no text scalar: decode it to a fixed one");
    match *event {
        Event::Quote {
            instrument,
            bid,
            ask,
            ..
        } => {
            if side_text(bid) || side_text(ask) {
                Err(TEXT)
            } else {
                Ok(instrument)
            }
        }
        Event::Trade {
            instrument,
            px,
            qty,
            cumulative_volume,
            ..
        } => {
            if text(px) || text(qty) || cumulative_volume.is_some_and(text) {
                Err(TEXT)
            } else {
                Ok(instrument)
            }
        }
        Event::Level { .. } | Event::Clear { .. } => Err(ParseError::schema(
            "the threaded model carries only quotes and trades",
        )),
    }
}

/// Instruments per [`ClaimTable`] segment.
const SEGMENT_BITS: u32 = 12;
const SEGMENT: usize = 1 << SEGMENT_BITS;
/// Segments in a [`ClaimTable`]'s directory.
const SEGMENTS: usize = 1024;

/// The most instruments [`ClaimTable`] and [`Dedupe`] track.
///
/// An [`InstrumentRef`] is a dense admission index that is never reused, so
/// this is the number of admissions, delisted ones included, a process can make
/// before the first two stages stop tracking new ones. Beyond it an event is
/// never claimed and never dropped: the publisher still applies each copy, and
/// redundancy costs what it costs the inline model. Four million admissions is
/// far more than a publisher holds definitions for.
pub const MAX_TRACKED_INSTRUMENTS: usize = SEGMENT * SEGMENTS;

/// The connection stage of [`Order::Supersedes`] dedupe: one `AtomicU64` per
/// instrument, shared by every connection's decoder.
///
/// # Keyed by the instrument's handle, and grown on listing
///
/// An [`InstrumentRef`] is the index of the instrument's admission, dense and
/// never reused. So the table is a directory of fixed-size segments of slots,
/// and a segment is allocated the first time an instrument in it is claimed. An
/// instrument listed mid-session is claimable on its first payload, with no
/// restart and no resize of anything another thread is reading: a segment, once
/// allocated, never moves. That allocation is once per 4,096 admissions, not
/// per payload.
///
/// The directory is allocated with the table and is the whole fixed cost, a few
/// tens of kilobytes; segments hold 4,096 instruments each, 32 KiB apiece. A
/// handle beyond [`MAX_TRACKED_INSTRUMENTS`] always claims.
///
/// # Cheap to clone, and shared rather than copied
///
/// A clone is another handle on the same table, which is the point: the runtime
/// builds one and hands a clone to each connection's decoder. `Send + Sync`.
#[derive(Clone)]
pub struct ClaimTable {
    segments: Arc<[OnceLock<Box<[AtomicU64]>>]>,
}

impl ClaimTable {
    /// An empty table: nothing claimed for any instrument.
    #[must_use]
    pub fn new() -> Self {
        Self {
            segments: (0..SEGMENTS).map(|_| OnceLock::new()).collect(),
        }
    }

    /// Claim `key` for `instrument`: `true` when it is above every key claimed
    /// for that instrument so far, by any connection, and is now the highest.
    ///
    /// `false` is a copy already delivered, or older than one: the decoder
    /// calls [`DecodedSink::duplicate`] and stops. A load first, as a
    /// pre-check, so the ordinary losing copy reads a cache line rather than
    /// taking it exclusively; then a `fetch_max`, which is the claim.
    ///
    /// Claim before what is expensive and after what is likely to fail. A claim
    /// that wins and then fails to parse loses that update on every connection,
    /// since the others' copies now lose. For state that is superseded by the
    /// next update anyway, which is what [`Order::Supersedes`] is for, that is
    /// the trade the design makes.
    ///
    /// Keys are compared as stored plus one, so `0` is a key like any other;
    /// the cost is that `u64::MAX` cannot follow `u64::MAX - 1`.
    #[must_use]
    pub fn claim(&self, instrument: InstrumentRef, key: u64) -> bool {
        let index = instrument.index() as usize;
        let Some(segment) = self.segments.get(index >> SEGMENT_BITS) else {
            return true;
        };
        let segment = segment.get_or_init(|| (0..SEGMENT).map(|_| AtomicU64::new(0)).collect());
        let slot = &segment[index & (SEGMENT - 1)];
        let stored = key.saturating_add(1);
        // Relaxed is enough: the claim is a race on this one location, which a
        // read-modify-write totally orders, and nothing else is published
        // under it.
        if slot.load(Ordering::Relaxed) >= stored {
            return false;
        }
        slot.fetch_max(stored, Ordering::Relaxed) < stored
    }
}

impl Default for ClaimTable {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ClaimTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let allocated = self
            .segments
            .iter()
            .filter(|segment| segment.get().is_some())
            .count();
        f.debug_struct("ClaimTable")
            .field("segments_allocated", &allocated)
            .finish()
    }
}

/// One decoder's count of how often each instrument's update counter has
/// restarted, for an [`Order::Supersedes`] key that survives a restart.
///
/// # The failure this exists for
///
/// `Supersedes` is newest-wins, so a venue that restarts an instrument's
/// counter — the key drops to near zero — would leave that instrument silent
/// until the new counter overtook the old high-water mark: every new key loses
/// its claim, and the loop drops every one that survives. Resetting the shared
/// table cannot fix it, because a copy of an *old* key from a connection that
/// has not yet seen the restart is then indistinguishable from progress.
///
/// So the era is each decoder's own view, and it travels in the key. A decoder
/// that judges a restart — by whatever rule its venue warrants, usually a drop
/// far below the last key *it* saw for the instrument — calls
/// [`restart`](Self::restart), and from then on builds its keys with
/// [`key`](Self::key): the era in the top 16 bits, the counter in the low
/// [`COUNTER_BITS`](Self::COUNTER_BITS). Every connection carrying the
/// instrument sees the same restarts, so they agree on the era; a key from the
/// new counter is above every key from the old one, in the [`ClaimTable`] and
/// in the loop's [`Dedupe`] alike, and a late copy from the old counter is below
/// it and is dropped as stale.
///
/// # What it costs
///
/// A counter of 48 bits or fewer: [`key`](Self::key) refuses a larger one. A
/// connection that never saw an instrument before its counter restarted counts
/// one era fewer than the others for it, so its copies of that instrument lose
/// every claim: it delivers nothing for that instrument, and the others still
/// do. Eras stop advancing after 65,535 restarts of one instrument.
#[derive(Debug, Clone, Default)]
pub struct CounterEras {
    eras: Vec<u16>,
}

impl CounterEras {
    /// The low bits of a key that hold the venue's own counter.
    pub const COUNTER_BITS: u32 = 48;

    /// Every instrument in its first era.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// This decoder judged that `instrument`'s counter restarted: its keys from
    /// now on are in the next era.
    ///
    /// Call it once per restart, before building the key of the first update
    /// after it. A handle beyond [`MAX_TRACKED_INSTRUMENTS`] stays in era 0.
    pub fn restart(&mut self, instrument: InstrumentRef) {
        let index = instrument.index() as usize;
        if index >= MAX_TRACKED_INSTRUMENTS {
            return;
        }
        if self.eras.len() <= index {
            self.eras.resize(index + 1, 0);
        }
        self.eras[index] = self.eras[index].saturating_add(1);
    }

    /// The era `instrument` is in, as this decoder has counted it.
    #[must_use]
    pub fn era(&self, instrument: InstrumentRef) -> u16 {
        self.eras
            .get(instrument.index() as usize)
            .copied()
            .unwrap_or(0)
    }

    /// The key to claim and to push under [`Order::Supersedes`] for `counter`
    /// on `instrument`: the era above the counter. `None` for a counter that
    /// does not fit in [`COUNTER_BITS`](Self::COUNTER_BITS).
    #[must_use]
    pub fn key(&self, instrument: InstrumentRef, counter: u64) -> Option<u64> {
        if counter >> Self::COUNTER_BITS != 0 {
            return None;
        }
        Some(u64::from(self.era(instrument)) << Self::COUNTER_BITS | counter)
    }
}

/// How many [`Order::Identity`] keys are remembered per instrument.
///
/// A window rather than a set, because a point event's identity has no order
/// to compact on and a set of every trade ever applied grows for the life of
/// the process. It has to cover the copies still in flight on the slowest
/// connection: 256 trades on one instrument between the first copy and the
/// last is a skew no healthy redundant upstream reaches.
pub const IDENTITY_WINDOW: usize = 256;

/// The loop stage of dedupe, and the authority.
///
/// Per instrument: the last [`Order::Supersedes`] key applied, and the last
/// [`IDENTITY_WINDOW`] [`Order::Identity`] keys applied. Grown on the first
/// event for an instrument, on the loop, which is allowed to allocate.
#[derive(Debug, Default)]
pub struct Dedupe {
    /// The last applied `Supersedes` key plus one, by instrument; `0` is none.
    applied: Vec<u64>,
    /// The recent `Identity` keys, by instrument.
    seen: Vec<Option<Box<Window>>>,
}

#[derive(Debug)]
struct Window {
    keys: [u64; IDENTITY_WINDOW],
    len: usize,
    next: usize,
}

impl Window {
    fn new() -> Self {
        Self {
            keys: [0; IDENTITY_WINDOW],
            len: 0,
            next: 0,
        }
    }

    /// Whether `key` is new, remembering it when it is.
    fn admit(&mut self, key: u64) -> bool {
        if self.keys[..self.len].contains(&key) {
            return false;
        }
        self.keys[self.next] = key;
        self.next = (self.next + 1) % IDENTITY_WINDOW;
        self.len = (self.len + 1).min(IDENTITY_WINDOW);
        true
    }
}

impl Dedupe {
    /// Nothing applied yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether to apply an event for `instrument` under `order`, recording it
    /// as applied when the answer is `true`. `false` is a duplicate.
    ///
    /// [`Order::None`] is always applied. A handle beyond
    /// [`MAX_TRACKED_INSTRUMENTS`] is always applied.
    pub fn admit(&mut self, instrument: InstrumentRef, order: Order) -> bool {
        let index = instrument.index() as usize;
        if index >= MAX_TRACKED_INSTRUMENTS {
            return true;
        }
        match order {
            Order::None => true,
            Order::Supersedes(key) => {
                if self.applied.len() <= index {
                    self.applied.resize(index + 1, 0);
                }
                let stored = key.saturating_add(1);
                if self.applied[index] >= stored {
                    return false;
                }
                self.applied[index] = stored;
                true
            }
            Order::Identity(key) => {
                if self.seen.len() <= index {
                    self.seen.resize_with(index + 1, || None);
                }
                self.seen[index]
                    .get_or_insert_with(|| Box::new(Window::new()))
                    .admit(key)
            }
        }
    }
}

/// Decode one payload and apply what it produced, in place.
///
/// The inline model for a [`ThreadedAdapter`]: `decoder` runs, every event it
/// pushes goes through `dedupe` — the same loop stage the threaded runtime
/// applies — and each survivor reaches [`ThreadedAdapter::on_decoded`] with
/// `out`. Fed the same payloads in the same order, it emits what the threaded
/// path emits.
///
/// A duplicate is dropped here and not counted: [`EventSink`] has no method
/// for one. The threaded runtime counts both stages.
///
/// # Errors
///
/// The decoder's [`ParseError`]; or, when it pushed an event [`carried`]
/// refuses, the first such refusal. Events pushed before or after a refused one
/// are still applied.
pub fn on_payload<A: ThreadedAdapter + ?Sized>(
    adapter: &mut A,
    decoder: &mut dyn Decoder,
    dedupe: &mut Dedupe,
    payload: &Payload<'_>,
    out: &mut dyn EventSink,
) -> Result<(), ParseError> {
    let mut sink = InPlace {
        adapter,
        dedupe,
        out,
        connection: payload.connection,
        recv_ts_ns: payload.recv_ts_ns,
        refused: None,
    };
    decoder.decode(payload, &mut sink)?;
    sink.refused.map_or(Ok(()), Err)
}

/// The sink [`on_payload`] hands a decoder.
struct InPlace<'a, A: ThreadedAdapter + ?Sized> {
    adapter: &'a mut A,
    dedupe: &'a mut Dedupe,
    out: &'a mut dyn EventSink,
    connection: ConnectionId,
    recv_ts_ns: u64,
    refused: Option<ParseError>,
}

impl<A: ThreadedAdapter + ?Sized> DecodedSink for InPlace<'_, A> {
    fn upstream_message(&mut self, message_type: &'static str) {
        self.out.upstream_message(message_type);
    }

    fn push(&mut self, event: Event<'static>, order: Order) {
        match carried(&event) {
            Err(error) => {
                self.refused.get_or_insert(error);
            }
            Ok(instrument) => {
                if self.dedupe.admit(instrument, order) {
                    let decoded = Decoded {
                        event,
                        order,
                        connection: self.connection,
                        recv_ts_ns: self.recv_ts_ns,
                    };
                    self.adapter.on_decoded(&decoded, self.out);
                }
            }
        }
    }

    fn duplicate(&mut self) {}
}

/// What a [`ThreadedAdapter`] holds to be an inline adapter too: a decoder per
/// connection, the [`ClaimTable`] they share, and the loop stage's [`Dedupe`].
///
/// A connection's decoder is built from [`ThreadedAdapter::decoder`] on that
/// connection's first payload and kept, as the runtime keeps it. See the module
/// documentation for how an `on_payload` holds one.
#[derive(Default)]
pub struct Inline {
    claims: ClaimTable,
    dedupe: Dedupe,
    decoders: Vec<(ConnectionId, Box<dyn Decoder>)>,
}

impl Inline {
    /// No decoders yet, nothing claimed, nothing applied.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// [`on_payload`], with the decoder for `payload.connection`.
    ///
    /// # Errors
    ///
    /// As [`on_payload`].
    pub fn on_payload<A: ThreadedAdapter + ?Sized>(
        &mut self,
        adapter: &mut A,
        payload: &Payload<'_>,
        out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        let index = match self
            .decoders
            .iter()
            .position(|(conn, _)| *conn == payload.connection)
        {
            Some(index) => index,
            None => {
                let decoder = adapter.decoder(payload.connection, self.claims.clone());
                self.decoders.push((payload.connection, decoder));
                self.decoders.len() - 1
            }
        };
        on_payload(
            adapter,
            &mut *self.decoders[index].1,
            &mut self.dedupe,
            payload,
            out,
        )
    }
}

impl std::fmt::Debug for Inline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let connections: Vec<ConnectionId> = self.decoders.iter().map(|(conn, _)| *conn).collect();
        f.debug_struct("Inline")
            .field("claims", &self.claims)
            .field("connections", &connections)
            .finish_non_exhaustive()
    }
}
