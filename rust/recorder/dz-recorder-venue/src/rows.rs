//! The venue-side grains, and the columns they do not have.
//!
//! Two grains. [`VenueBookTop`] is one row per change in the top of book as an
//! observer of a venue's own upstream states it — the rows the race pairs.
//! [`VenueObjectRow`] is one row per archived object: what the derivation read,
//! what it refused, and the `(object key, sha256)` a re-derivation replaces on.
//!
//! Field names are the column names, exactly, and `tests/column_names.rs` holds
//! every one of them against a literal so that a rename cannot pass.
//!
//! # The absence is the point
//!
//! There is no `channel_id`, `instrument_id`, `sequence_number`, `reset_count`,
//! `segment_seq`, `drop_delta` or `era` column on either grain, and the request
//! this design answers asked for exactly those to be filled in.
//!
//! Each one is a statement about a datagram on a channel instance, and a
//! venue's upstream message is not one:
//!
//! - `channel_id` is the operator's mapping from a shard to a channel, and the
//!   whole adapter boundary exists so that a venue cannot be handed one.
//! - `instrument_id` is minted by the publisher's reference-data registry, is
//!   unique only within an era, and is not derivable from anything a venue
//!   sends. A venue knows the symbol.
//! - `sequence_number` and `reset_count` belong to one channel instance's
//!   sequence space. A venue's own counter — a session sequence, an update id —
//!   is a different series with a different owner and different loss semantics,
//!   and it reaches [`VenueBookTop::upstream_seq`] where nothing joins on it.
//! - `segment_seq` numbers the objects of a capture.
//! - `drop_delta` is what a capture handle lost, charged to the handle. A venue
//!   transport's loss is its session's, measured by the venue's own resend
//!   mechanism.
//! - An `era` is a publisher's `Reset Count` span. The venue side has none, in
//!   any form.
//!
//! Every one of them has a plausible value that is also a real reading, which
//! is why the columns are absent rather than nullable: channel `0` is a
//! channel, sequence `0` is the first sequence of an era, and a `NULL` invites
//! a join that drops the row. The alternative — writing a venue-side row into
//! `book_top`, where all eight publisher-provenance columns are non-nullable
//! and in the sort key — is what the design refused.
//!
//! # What is here instead
//!
//! The observation, the upstream connection, the object key and its digest, the
//! venue's own message identity where it has one, the symbol, the exponents as
//! the venue states them, both sides of the top, and `book_key` — computed by
//! [`dz_recorder_events::book_key`], the same function the publisher side uses.
//! Not a second implementation: two hashes of one book state pair with nothing,
//! and the predicate that decides an absent side is private in that crate for
//! exactly that reason.

use std::fmt;

use dz_recorder_rows::Nanos;
use serde::{Deserialize, Serialize};

/// The venue-side grains, one per table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VenueGrain {
    BookTop,
    Object,
}

impl VenueGrain {
    /// Every grain, in the order `009` declares them.
    pub const ALL: [Self; 2] = [Self::BookTop, Self::Object];
    pub const COUNT: usize = Self::ALL.len();

    /// The table this grain lands in, which is also the metric label and the
    /// file name — so one spelling has to serve all three.
    #[must_use]
    pub const fn table(self) -> &'static str {
        match self {
            Self::BookTop => "venue_book_top",
            Self::Object => "venue_object",
        }
    }

    #[must_use]
    pub const fn index(self) -> usize {
        match self {
            Self::BookTop => 0,
            Self::Object => 1,
        }
    }
}

impl fmt::Display for VenueGrain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.table())
    }
}

/// One row per change in the top of book, as an observer of a venue's own
/// upstream states it.
///
/// A change is a change in the visible top. There is no certainty column: on
/// the publisher side `book_certain` falls when a gap in the publisher's
/// sequence means the book cannot be believed, and a venue-side observation has
/// no sequence space of its own that this repository defines. The same column
/// with two meanings, minimum-aggregated over a pair, would silently mix them —
/// so the tier says what it does not know rather than filling it with a number.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VenueBookTop {
    /// When the upstream message that moved the top was received, on the host
    /// that was recording. Our own clock, never the venue's.
    ///
    /// This is the quantity the race measures, which is why it is in no
    /// equivalence key.
    pub recv_ts: Nanos,
    /// Which observation point this recording is, as `site` names a host.
    ///
    /// The same opaque string `book_top.observation` is, and opaque for the
    /// same reason: the pairing names no observation point, so two of them are
    /// a race and three of them are the same query.
    pub observation: String,
    pub env: String,
    /// The feed specification whose instruments this recording covers.
    ///
    /// A key here and a label there. On `book_top` the feed is recoverable from
    /// the channel instance, because no two feeds serve one `(source address,
    /// destination port)`. There is no channel instance on this side, so
    /// nothing else tells two feeds apart at one observation point.
    pub feed: String,
    /// The upstream connection the message arrived on, as the object's header
    /// names it.
    ///
    /// Evidence and not a key. An adapter whose mapping depends on the
    /// connection — one upstream for depth, another for trades — reproduces
    /// nothing offline without it, and a row that cannot say which upstream it
    /// came from cannot be checked against that upstream's own logs.
    pub connection: String,
    /// The venue's own session or connection identifier for the message that
    /// moved this top, where the venue publishes one.
    ///
    /// `None` where it does not, and never a zero: `0` is a session identifier
    /// a venue really sends. Nothing joins on this — a numbering whose
    /// resolution and meaning differ between transports would give one book
    /// state two keys — and it is here because it separates a venue *resending*
    /// a state from the venue producing that state again. Those are the same
    /// book and not the same event, and without this an unpaired occurrence has
    /// one fewer explanation available to it.
    pub upstream_sid: Option<u64>,
    /// The venue's own number for the message within that session, where it
    /// publishes one.
    ///
    /// **Not a `Sequence Number` in this family's sense**, which is why it is
    /// not called one: that belongs to a channel instance and is minted above
    /// the adapter boundary. Passed through exactly as the upstream stated it,
    /// with no renumbering and no gap filled in.
    pub upstream_seq: Option<u64>,
    /// The instrument, as the venue names it.
    ///
    /// The only instrument identity both sides of the race hold. Coarser than
    /// an `Instrument ID`: it cannot separate two instruments that shared a
    /// symbol across an era boundary, which is a cost the design states rather
    /// than hides.
    pub symbol: String,
    /// The exponents **as the venue states them**, through the adapter's own
    /// `InstrumentSpec`.
    ///
    /// Carried rather than assumed. The equivalence key covers the raw prices
    /// and leaves these out, so a pair whose exponents disagree is two
    /// different prices wearing one key — and `exponents_agree` in `009` is
    /// what makes that visible instead of averaged.
    pub price_exp: i8,
    pub qty_exp: i8,
    pub bid_px_raw: Option<i64>,
    pub bid_qty_raw: Option<u64>,
    /// How many distinct upstreams contribute to the side, where the venue says.
    ///
    /// `None` where it does not. **Never a zero for *did not say***: the
    /// top-of-book specification states this field as "0 if unavailable", so a
    /// zero written here is the multicast side's spelling of an absence, and
    /// [`dz_recorder_events::book_key`] reads the two alike. Writing the zero
    /// would give one book two keys and the race would find no pair — which
    /// reads as a quiet feed on both paths.
    pub bid_source_count: Option<u16>,
    pub ask_px_raw: Option<i64>,
    pub ask_qty_raw: Option<u64>,
    pub ask_source_count: Option<u16>,
    /// **Is this the same book?** — [`dz_recorder_events::book_key`], over the
    /// two sides and nothing else.
    ///
    /// Computable by anyone holding a top of book, including an observer that
    /// never saw a datagram, which is what makes it the key two observers of one
    /// market pair on. **Not `state_key`**: that one folds the `Channel ID` and
    /// the `Instrument ID` in before it folds a price, and a venue side can name
    /// neither. Keyed on it, this race would return zero pairs and read as each
    /// side missing every state the other saw.
    pub book_key: u64,
    /// Which upstream message in the object moved the top, counted from zero.
    ///
    /// In the sort key, and that is what it is for. Two messages one instant
    /// apart on one connection are two observations of the market; without this
    /// the second replaces the first under `ReplacingMergeTree` and the book's
    /// history has a hole in it that no count would show.
    pub message_index: u64,
    pub object_key: String,
    pub object_sha256: String,
}

/// One row per archived object a derivation read.
///
/// **The idempotence row.** `(object_key, object_sha256)` is what a
/// re-derivation replaces on, so this is where a reader looks to see whether an
/// object was read at all, how much of it the adapter could parse, and what it
/// refused. An object derived twice is one row here and one set of rows there.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VenueObjectRow {
    /// The first and last receive stamps in the object's window.
    ///
    /// From the object itself and not from the manifest beside it, so a window
    /// stated here is one the derivation actually read.
    pub recv_ts_start: Nanos,
    pub recv_ts_end: Nanos,
    pub observation: String,
    pub env: String,
    pub feed: String,
    pub object_key: String,
    pub object_sha256: String,
    /// The archive format the object was written in, as its own header states
    /// it.
    pub format_version: u16,
    /// The connections the object declared, in the order its records index them.
    pub connections: Vec<String>,
    /// Upstream messages read out of the object.
    pub message_count: u64,
    /// Messages the adapter refused.
    ///
    /// **Charged to the message and never to the object.** A derivation that
    /// stopped at the first message a venue's own adapter could not parse would
    /// report the venue's feed as having ended there, and the rows would be
    /// indistinguishable from a venue that went quiet.
    pub refused_count: u64,
    /// The refusals by the reason the adapter gave, as `(reason, count)`.
    ///
    /// The reasons are `ParseError`'s own four tokens — `schema`,
    /// `unknown_field`, `malformed`, `truncated` — because an operator acts
    /// differently on each: a schema refusal says the venue changed its
    /// interface, and a truncated one says the transport cut a message. A bare
    /// total sends somebody to read the objects to find out which.
    pub refusals: Vec<RefusalCount>,
    /// Market events the adapter emitted.
    pub event_count: u64,
    /// Events whose price or quantity could not be stated exactly at the
    /// instrument's own declared exponent.
    ///
    /// Counted rather than rounded, and the event is dropped rather than
    /// written. A rounded price is a price the venue did not quote, and a
    /// conversion taken as zero is a real-looking quote at nothing — which is
    /// the shipped defect the adapter boundary was shaped around.
    pub unpriced_count: u64,
    /// Times the adapter said it no longer trusts its own book for an
    /// instrument.
    ///
    /// Evidence and not a verdict. It is the one thing a venue knows that
    /// nothing above it can, and it belongs beside the object rather than as a
    /// certainty column on a row: what a venue's resynchronisation means for a
    /// book is the venue's, and this tier does not have a definition of it that
    /// a `min()` over a pair could safely mix with the publisher side's.
    pub desync_count: u64,
    /// Events the adapter reported outside any payload scope.
    ///
    /// **A defect in the adapter, and visible rather than silent.** The
    /// derivation opens the scope around `on_payload` and closes it after, so an
    /// event outside one is an adapter that closed the scope itself — which the
    /// sink's contract permits it to do. Such an event is attributable to no
    /// upstream message: it has no receive stamp, no message index and no
    /// identity, so there is no honest row to write and it is dropped. A drop
    /// nothing counted would read as a venue that said less than it did.
    pub unattributed_count: u64,
    /// Rows written to `venue_book_top` from this object.
    pub book_top_count: u64,
    /// Instruments the adapter offered while this object was being read.
    pub instrument_count: u32,
}

/// One refusal reason and how often the adapter gave it.
///
/// An unnamed tuple, so it reaches `Array(Tuple(String, UInt64))` as an array —
/// the same shape `segment_coverage.roles_joined` already uses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefusalCount(pub String, pub u64);

/// Every venue-side row one object produced.
///
/// **One batch, one unit of idempotence**, for the reason the publisher side's
/// `RowBatch` is: a re-derivation is idempotent on `(object key, sha256)`, so an
/// object whose book rows landed while its object row did not is an object that
/// reads as never having been derived. Partial credit is how a refusal becomes
/// invisible.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VenueRowBatch {
    pub book_tops: Vec<VenueBookTop>,
    pub objects: Vec<VenueObjectRow>,
}

impl VenueRowBatch {
    /// How many rows of one grain the batch holds.
    #[must_use]
    pub fn rows(&self, grain: VenueGrain) -> usize {
        match grain {
            VenueGrain::BookTop => self.book_tops.len(),
            VenueGrain::Object => self.objects.len(),
        }
    }

    /// Every row in the batch, over both grains.
    #[must_use]
    pub fn total(&self) -> usize {
        self.book_tops.len() + self.objects.len()
    }

    /// Whether the batch holds nothing at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total() == 0
    }
}

/// A [`VenueRowSink`] could not accept the batch.
#[derive(Debug, thiserror::Error)]
pub enum VenueRowSinkError {
    #[error("writing venue rows: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialising a {grain} row: {source}")]
    Encode {
        grain: VenueGrain,
        #[source]
        source: serde_json::Error,
    },
    /// The destination refused the batch. The object stays unloaded.
    #[error("{object_key} was refused: {detail}")]
    Rejected { object_key: String, detail: String },
}

/// Somewhere venue-side rows are written.
///
/// One method taking the whole batch, for the reason the publisher side's
/// `RowSink` has one: the object is the unit that either landed or did not.
pub trait VenueRowSink {
    /// Takes every row in the batch, or none of them.
    ///
    /// # Errors
    ///
    /// [`VenueRowSinkError`], and then the object stays unloaded and is
    /// retried. `ReplacingMergeTree` and the key make the retry a replace, so a
    /// success reported over a partial write is the only outcome that loses
    /// rows for good.
    fn write_batch(&mut self, rows: VenueRowBatch) -> Result<(), VenueRowSinkError>;
}

/// A sink that keeps the batches in memory.
///
/// The reference implementation and what the tests assert against: a derivation
/// this expensive has to be exercisable with no filesystem, no privilege and no
/// server, exactly as the publisher side's `FileSink` makes its golden tests
/// possible.
#[derive(Debug, Default)]
pub struct CollectingSink {
    pub batches: Vec<VenueRowBatch>,
}

impl CollectingSink {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            batches: Vec::new(),
        }
    }

    /// Every book row written, in the order the batches arrived.
    #[must_use]
    pub fn book_tops(&self) -> Vec<&VenueBookTop> {
        self.batches.iter().flat_map(|b| &b.book_tops).collect()
    }

    /// Every object row written.
    #[must_use]
    pub fn objects(&self) -> Vec<&VenueObjectRow> {
        self.batches.iter().flat_map(|b| &b.objects).collect()
    }
}

impl VenueRowSink for CollectingSink {
    fn write_batch(&mut self, rows: VenueRowBatch) -> Result<(), VenueRowSinkError> {
        self.batches.push(rows);
        Ok(())
    }
}
