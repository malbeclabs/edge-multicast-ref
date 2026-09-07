//! The book, and the part of it that can say it does not know.
//!
//! Two derivations, because the family has two shapes of feed and only one of
//! them needs an anchor.
//!
//! **A `Quote` is self-anchoring.** It states a complete two-sided top, so it
//! establishes a certain top by itself, with no prior state and no snapshot —
//! and after a gap it *restores* certainty the moment the next one arrives,
//! because nothing about a missed `Quote` makes the next one less true. A
//! quote-only feed produces rows from its first message, and a rule requiring a
//! snapshot cycle would have produced none at all for it.
//!
//! **A delta book has exactly one anchor: a complete snapshot cycle.** A
//! `BookClear` is not one — it asserts that named levels are gone and a
//! subscriber applying it stays ready — and an `InstrumentReset` is the opposite
//! of one: it is the message a publisher owes when it has lost confidence in its
//! own book, so it *destroys* certainty and states the terms of its own recovery
//! in `new_anchor_seq`.
//!
//! # The column this exists for
//!
//! A live book that missed datagrams applies the deltas that did arrive and
//! keeps quoting a top that has silently diverged from the publisher's. It
//! cannot notice, because the thing it would need in order to notice is the
//! datagram it did not receive. A derived book can: a gap in the channel
//! instance's sequence space is observable *here*, so `book_certain` falls to 0
//! and stays there until the derivation's own rule restores it.
//!
//! **A certainty transition emits a row on its own**, carrying the same top as
//! the row before it and a different verdict on whether that top can be believed.
//! Emitting only on price movement would lose exactly the transition that
//! matters most: a gap arrives, nothing later moves the top, and every lookup
//! from then on keeps returning a row that says the book is certain.
//!
//! # A book spans port roles, and is therefore keyed on the channel
//!
//! The anchor arrives on the `snapshot` port role and the deltas that follow it
//! on `mktdata` — two channel instances, because the destination port is part of
//! that key. A book keyed on the instance would anchor one book and update a
//! different one, and neither would ever be both certain and current. So the
//! book is keyed on the **channel**, `(source address, Channel ID)`, exactly as
//! the reference data is and for the same structural reason.
//!
//! **Gap detection stays per instance**, because a sequence space does: a hole
//! is a hole in one port role's numbering. Only a hole on `mktdata` touches
//! certainty — a missed definition is the reference data's problem, and a missed
//! snapshot message is already caught by the cycle's own level count.

use std::collections::BTreeMap;

use dz_edge_core::PortRole;
use dz_edge_mbp::{
    BookClear, InstrumentReset, LevelUpdate, SnapshotBegin, SnapshotEnd, SnapshotLevel, CLEAR_ASK,
    CLEAR_BID, CLEAR_BOTH, SCOPE_FROM_PRICE, SIDE_BID,
};
use dz_edge_tob::Quote;
use dz_recorder_core::ChannelInstance;
use dz_recorder_rows::UncertainReason;

use crate::instruments::Channel;

/// One side's top, as a book states it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Side {
    pub price_raw: Option<i64>,
    pub qty_raw: Option<u64>,
    /// Present only on a feed whose messages carry it.
    ///
    /// A `Quote` states how many upstream sources contributed to the top; a
    /// depth feed states how many *orders* sit at a level. Those are different
    /// quantities and mapping one onto the other would put a number in a column
    /// that does not mean what the column says, so a delta-derived top leaves
    /// this absent.
    pub source_count: Option<u16>,
}

impl Side {
    const fn absent() -> Self {
        Self {
            price_raw: None,
            qty_raw: None,
            source_count: None,
        }
    }

    const fn is_absent(&self) -> bool {
        self.price_raw.is_none() && self.qty_raw.is_none()
    }
}

/// A top of book, as one instrument's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Top {
    pub bid: Side,
    pub ask: Side,
}

/// What a book state can be believed to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Certainty {
    pub certain: bool,
    pub since: Option<u64>,
    pub reason: UncertainReason,
}

impl Certainty {
    const fn known() -> Self {
        Self {
            certain: true,
            since: None,
            reason: UncertainReason::None,
        }
    }

    const fn unknown(since: u64, reason: UncertainReason) -> Self {
        Self {
            certain: false,
            since: Some(since),
            reason,
        }
    }
}

/// What one instrument's book is, and whether it can be believed.
#[derive(Debug, Clone, Default)]
struct InstrumentBook {
    bids: BTreeMap<i64, u64>,
    asks: BTreeMap<i64, u64>,
    top: Top,
    /// A delta book has no top at all before its first anchor, which is not the
    /// same as a top with both sides absent.
    established: bool,
    certainty: Option<Certainty>,
    /// The `new_anchor_seq` of a reset that has not been recovered from yet.
    ///
    /// A cycle whose `anchor_seq` is behind this one was already in flight when
    /// the reset was published, so it carries a book state the publisher had
    /// already disowned.
    awaiting_anchor: Option<u64>,
    /// Whether the one `no_anchor` row has been emitted.
    said_no_anchor: bool,
}

/// A snapshot cycle while it is open.
#[derive(Debug, Clone)]
struct OpenCycle {
    instrument_id: u32,
    anchor_seq: u64,
    total_levels: u32,
    bids: BTreeMap<i64, u64>,
    asks: BTreeMap<i64, u64>,
    levels: u32,
}

/// What a book refused to apply, counted rather than guessed at.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BookRefused {
    /// Cycles that carried fewer levels than their `SnapshotBegin` promised.
    ///
    /// A subscriber that counts fewer has an incomplete book and must not apply
    /// it, which the specification states outright.
    pub incomplete_cycle: u64,
    /// Cycles whose `anchor_seq` was behind an unrecovered reset's.
    ///
    /// The one refusal that is a safety property rather than a completeness one:
    /// applying it would rebuild from a book the publisher had disowned, and mark
    /// it certain.
    pub stale_cycle: u64,
}

/// What a message did to a book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Change {
    pub top: Top,
    pub certainty: Certainty,
    /// Whether this change came from applying a snapshot.
    ///
    /// A snapshot is pulled by the runtime on its own cadence and the archive
    /// records when it was published rather than when it was asked for, so a top
    /// derived from one is a starting state and never an observation in a race.
    pub from_anchor: bool,
}

/// Every instrument's book, for every channel instance in one object.
#[derive(Debug, Clone, Default)]
pub struct Book {
    books: BTreeMap<(Channel, u32), InstrumentBook>,
    cycles: BTreeMap<(Channel, u32), OpenCycle>,
    /// The last sequence number seen per channel instance, for gap detection.
    last_sequence: BTreeMap<ChannelInstance, u64>,
    pub refused: BookRefused,
}

impl Book {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Note a datagram's arrival, and report which instruments a gap made
    /// unknowable.
    ///
    /// A gap belongs to the **channel instance**, not to an instrument: the
    /// missing datagrams could have carried deltas for any instrument on it, and
    /// there is no way to know which. So every established book on that instance
    /// becomes uncertain, which is the honest reading and the expensive one.
    ///
    /// A `Quote` feed is untouched by this — see [`Book::quote`].
    pub fn observe_sequence(
        &mut self,
        instance: ChannelInstance,
        role: PortRole,
        sequence_number: u64,
    ) -> Vec<(u32, Change)> {
        let previous = self.last_sequence.insert(instance, sequence_number);
        let Some(previous) = previous else {
            return Vec::new();
        };
        if sequence_number <= previous + 1 {
            return Vec::new();
        }
        // A hole on `refdata` is the reference data's problem, and one on
        // `snapshot` is already caught by the cycle's own level count.
        if role != PortRole::Mktdata {
            return Vec::new();
        }
        let channel = Channel::of(instance);
        let missing_from = previous + 1;
        let mut changed = Vec::new();
        for ((book_channel, instrument_id), book) in &mut self.books {
            if *book_channel != channel || !book.established {
                continue;
            }
            if book.certainty.is_some_and(|c| !c.certain) {
                continue;
            }
            book.certainty = Some(Certainty::unknown(missing_from, UncertainReason::Gap));
            changed.push((
                *instrument_id,
                Change {
                    top: book.top,
                    certainty: Certainty::unknown(missing_from, UncertainReason::Gap),
                    from_anchor: false,
                },
            ));
        }
        changed
    }

    /// A `Quote`: a complete two-sided top, and its own anchor.
    pub fn quote(&mut self, channel: Channel, quote: &Quote) -> Option<Change> {
        let top = Top {
            bid: side_of_quote(quote.bid_price, quote.bid_qty, quote.bid_source_count),
            ask: side_of_quote(quote.ask_price, quote.ask_qty, quote.ask_source_count),
        };
        let book = self.book(channel, quote.instrument_id);
        let was = (book.top, book.certainty);
        book.top = top;
        book.established = true;
        // Restored by the next Quote, always: nothing about a missed one makes
        // this one less true.
        book.certainty = Some(Certainty::known());
        book.awaiting_anchor = None;
        changed(was, book)
    }

    /// A `LevelUpdate`: a delta, which needs a book to be applied to.
    pub fn level(&mut self, channel: Channel, level: &LevelUpdate) -> Option<Change> {
        let book = self.book(channel, level.instrument_id);
        if !book.established {
            if book.said_no_anchor {
                return None;
            }
            book.said_no_anchor = true;
            let certainty = Certainty::unknown(0, UncertainReason::NoAnchor);
            book.certainty = Some(certainty);
            // One row, with no prices, rather than absence: absence cannot be
            // told from a silent feed, and a lookup into an unanchored window
            // would return whatever preceded it — possibly from another era.
            return Some(Change {
                top: Top::default(),
                certainty,
                from_anchor: false,
            });
        }
        let was = (book.top, book.certainty);
        apply_level(book, level);
        recompute_top(book);
        changed(was, book)
    }

    /// A `BookClear`: a delta that removes levels, and never an anchor.
    pub fn clear(&mut self, channel: Channel, clear: &BookClear) -> Option<Change> {
        let book = self.book(channel, clear.instrument_id);
        if !book.established {
            return None;
        }
        let was = (book.top, book.certainty);
        let from = (clear.scope == SCOPE_FROM_PRICE).then_some(clear.from_price_raw);
        // `clear_side` is BookClear's own enumeration and not the level's
        // `side`: the two happen to agree on 0 for a bid, and a comparison
        // against the wrong constant would be right by coincidence.
        if matches!(clear.clear_side, CLEAR_BID | CLEAR_BOTH) {
            clear_side(&mut book.bids, from, true);
        }
        if matches!(clear.clear_side, CLEAR_ASK | CLEAR_BOTH) {
            clear_side(&mut book.asks, from, false);
        }
        recompute_top(book);
        changed(was, book)
    }

    /// An `InstrumentReset`: the publisher disowning its own book.
    pub fn reset(&mut self, channel: Channel, reset: &InstrumentReset) -> Option<Change> {
        let book = self.book(channel, reset.instrument_id);
        let was = (book.top, book.certainty);
        book.bids.clear();
        book.asks.clear();
        book.top = Top::default();
        book.established = false;
        book.said_no_anchor = false;
        // The terms of its own recovery. A cycle behind this was in flight when
        // the reset was published.
        book.awaiting_anchor = Some(reset.new_anchor_seq);
        book.certainty = Some(Certainty::unknown(
            reset.new_anchor_seq,
            UncertainReason::InstrumentReset,
        ));
        changed(was, book)
    }

    pub fn snapshot_begin(&mut self, channel: Channel, begin: &SnapshotBegin) {
        self.cycles.insert(
            (channel, begin.snapshot_id),
            OpenCycle {
                instrument_id: begin.instrument_id,
                anchor_seq: begin.anchor_seq,
                total_levels: begin.total_levels,
                bids: BTreeMap::new(),
                asks: BTreeMap::new(),
                levels: 0,
            },
        );
    }

    pub fn snapshot_level(&mut self, channel: Channel, level: &SnapshotLevel) {
        let Some(cycle) = self.cycles.get_mut(&(channel, level.snapshot_id)) else {
            return;
        };
        cycle.levels += 1;
        // Quantity is non-zero by rule on a snapshot level, so a zero is a
        // publisher defect rather than an instruction to delete. Kept as sent.
        let side = if level.side == SIDE_BID {
            &mut cycle.bids
        } else {
            &mut cycle.asks
        };
        side.insert(level.price_raw, level.qty_raw);
    }

    /// A `SnapshotEnd`: the only thing that anchors a delta book.
    pub fn snapshot_end(&mut self, channel: Channel, end: &SnapshotEnd) -> Option<Change> {
        let cycle = self.cycles.remove(&(channel, end.snapshot_id))?;
        let key = (channel, cycle.instrument_id);
        if cycle.levels != cycle.total_levels {
            self.refused.incomplete_cycle += 1;
            return None;
        }
        if self
            .books
            .get(&key)
            .and_then(|book| book.awaiting_anchor)
            .is_some_and(|awaiting| cycle.anchor_seq < awaiting)
        {
            self.refused.stale_cycle += 1;
            return None;
        }
        let book = self.book(channel, cycle.instrument_id);
        let was = (book.top, book.certainty);
        book.bids = cycle.bids;
        book.asks = cycle.asks;
        book.established = true;
        book.said_no_anchor = false;
        book.awaiting_anchor = None;
        book.certainty = Some(Certainty::known());
        recompute_top(book);
        changed(was, book).map(|change| Change {
            from_anchor: true,
            ..change
        })
    }

    fn book(&mut self, channel: Channel, instrument_id: u32) -> &mut InstrumentBook {
        self.books.entry((channel, instrument_id)).or_default()
    }
}

const fn side_of_quote(price_raw: i64, qty_raw: u64, source_count: u16) -> Side {
    // Top of book says *unavailable* with a zero price and a zero quantity,
    // which is the opposite of the depth feed's `0xFFFF` — two specifications,
    // one question, opposite answers.
    if price_raw == 0 && qty_raw == 0 {
        return Side::absent();
    }
    Side {
        price_raw: Some(price_raw),
        qty_raw: Some(qty_raw),
        source_count: Some(source_count),
    }
}

fn apply_level(book: &mut InstrumentBook, level: &LevelUpdate) {
    let side = if level.side == SIDE_BID {
        &mut book.bids
    } else {
        &mut book.asks
    };
    // Absolute aggregate quantity at the price, and zero removes the level —
    // the codec says so on the field itself.
    if level.qty_raw == 0 {
        side.remove(&level.price_raw);
    } else {
        side.insert(level.price_raw, level.qty_raw);
    }
}

fn clear_side(side: &mut BTreeMap<i64, u64>, from_price: Option<i64>, is_bid: bool) {
    match from_price {
        None => side.clear(),
        Some(bound) => {
            // Inclusive, and *from* the price in the direction the side runs:
            // a bid side clears downward from the bound, an ask side upward.
            side.retain(|price, _| {
                if is_bid {
                    *price > bound
                } else {
                    *price < bound
                }
            });
        }
    }
}

fn recompute_top(book: &mut InstrumentBook) {
    book.top = Top {
        bid: book
            .bids
            .iter()
            .next_back()
            .map_or_else(Side::absent, |(price, qty)| Side {
                price_raw: Some(*price),
                qty_raw: Some(*qty),
                source_count: None,
            }),
        ask: book
            .asks
            .iter()
            .next()
            .map_or_else(Side::absent, |(price, qty)| Side {
                price_raw: Some(*price),
                qty_raw: Some(*qty),
                source_count: None,
            }),
    };
}

/// A change is a change in the visible top **or** in the certainty of it.
fn changed(was: (Top, Option<Certainty>), book: &InstrumentBook) -> Option<Change> {
    let now = book.certainty.unwrap_or_else(Certainty::known);
    let top_moved = was.0 != book.top;
    let certainty_moved = was.1 != book.certainty;
    if !top_moved && !certainty_moved {
        return None;
    }
    // A book with both sides absent and nothing ever applied is not a change
    // worth a row: it is the state before anything happened.
    if !book.established && book.top == Top::default() && !certainty_moved {
        return None;
    }
    Some(Change {
        top: book.top,
        certainty: now,
        from_anchor: false,
    })
}

/// The equivalence key: a hash over the instrument and both sides, and nothing
/// else.
///
/// **FNV-1a rather than the standard library's hasher.** `DefaultHasher` is
/// explicitly not stable across releases, and this value is compared between two
/// observation points that may be running two builds — a key that changed with a
/// toolchain upgrade would silently stop finding pairs, which is the failure mode
/// hardest to notice because it looks like a quiet feed.
///
/// An absent side is a distinguished tag rather than zeros: an empty side and a
/// side priced at zero are different books, and the top-of-book convention of
/// stating *unavailable* with a zero is exactly what would collapse them.
#[must_use]
pub fn state_key(channel_id: u8, instrument_id: u32, top: &Top) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    let mut eat = |bytes: &[u8]| {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
    };
    eat(&[channel_id]);
    eat(&instrument_id.to_be_bytes());
    for side in [&top.bid, &top.ask] {
        if side.is_absent() {
            eat(&[0]);
            continue;
        }
        eat(&[1]);
        eat(&side.price_raw.unwrap_or(0).to_be_bytes());
        eat(&side.qty_raw.unwrap_or(0).to_be_bytes());
        // Absent and zero are distinguished here too: a feed that carries no
        // count and one that counts none are not the same reading.
        match side.source_count {
            None => eat(&[0]),
            Some(count) => {
                eat(&[1]);
                eat(&count.to_be_bytes());
            }
        }
    }
    hash
}
