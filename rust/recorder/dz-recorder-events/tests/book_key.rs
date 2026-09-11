//! The split: a key over the book alone, and the key that folds onto it.
//!
//! `state_key` answers *is this the same state of this channel's instrument*
//! and `book_key` answers *is this the same book*. The first is what two
//! recorders of one multicast feed pair on, because both read the same
//! `Channel ID` and the same `Instrument ID` off the same datagrams. The second
//! is what two observers of one market pair on, and neither identifier is
//! available to an observer that never saw a datagram: the channel is the
//! operator's mapping from a shard to a channel, and the `Instrument ID` is
//! minted by the reference-data registry, which makes it a number in that
//! publisher's own space — never re-used, and never held by a side that reads
//! the venue instead of the wire.
//!
//! **`state_key`'s value may not move.** It is written into rows that exist and
//! compared against rows loaded before this split, so the first test pins it to
//! literals computed from the function as it stood beforehand. A refactor that
//! changed it by a byte would not fail anything else here — every other test in
//! the crate asserts the key against itself.
//!
//! That pin builds its tops by hand, so it holds the *hash* and not the
//! derivation that feeds it. The second pin closes that: it takes a `Quote` off
//! the wire, through the decode and the real book, and asserts the key over the
//! top that comes out — because a change to what the derivation puts into the
//! key moves a stored value just as surely as a change to the fold.
#![forbid(unsafe_code)]

mod common;

use std::net::Ipv4Addr;

use common::{definition, identity, pack, DatagramLog, Msg, AAA, SOURCE_ID};
use dz_edge_core::PortRole;
use dz_edge_tob::{Quote, TopOfBook, MAGIC_TOB, QUOTE_ASK_UPDATED, QUOTE_BID_UPDATED};
use dz_recorder_events::{
    book_key, derive_events, state_key, Book, Channel, EventInput, Side, Top,
};
use dz_recorder_rows::{BookTop, Derivation};

/// The channel and the instrument the pinned literals were computed under.
const CHANNEL: u8 = 7;
const INSTRUMENT: u32 = 42;

fn side(price_raw: Option<i64>, qty_raw: Option<u64>, source_count: Option<u16>) -> Side {
    Side {
        price_raw,
        qty_raw,
        source_count,
    }
}

fn present(price_raw: i64, qty_raw: u64, source_count: u16) -> Side {
    side(Some(price_raw), Some(qty_raw), Some(source_count))
}

/// A two-sided top with both counts stated, as a `Quote` produces one.
fn two_sided() -> Top {
    Top {
        bid: present(9_950, 12, 2),
        ask: present(10_050, 7, 3),
    }
}

/// `state_key` returns what it returned before `book_key` existed.
///
/// The literals below were computed by calling the single-function `state_key`
/// at the commit before the split, over the five shapes a top comes in — both
/// sides stated, one side absent, both absent, a side priced and sized at zero,
/// and a delta-derived top that carries no counts — and then over each of the
/// two identifiers it eats ahead of them. Nothing in the crate can regenerate
/// them, and that is the point: they are the only record here of what the rows
/// already written were keyed with, so the assertion is against the numbers
/// rather than against the function.
#[test]
fn state_keys_value_did_not_move() {
    assert_eq!(
        state_key(CHANNEL, INSTRUMENT, &two_sided()),
        0x0285_0be3_1923_8c7f,
        "both sides stated"
    );

    assert_eq!(
        state_key(
            CHANNEL,
            INSTRUMENT,
            &Top {
                bid: side(None, None, None),
                ask: present(10_050, 7, 3),
            }
        ),
        0xdb20_22aa_76b0_fb0b,
        "the bid absent"
    );

    assert_eq!(
        state_key(
            CHANNEL,
            INSTRUMENT,
            &Top {
                bid: side(None, None, None),
                ask: side(None, None, None),
            }
        ),
        0x70bb_e976_7b7b_bf78,
        "both sides absent"
    );

    assert_eq!(
        state_key(
            CHANNEL,
            INSTRUMENT,
            &Top {
                bid: present(0, 0, 0),
                ask: present(10_050, 7, 3),
            }
        ),
        0x8c55_6589_6478_ee61,
        "a bid priced and sized at zero, which is a side and not an absence"
    );

    assert_eq!(
        state_key(
            CHANNEL,
            INSTRUMENT,
            &Top {
                bid: side(Some(9_950), Some(12), None),
                ask: side(Some(10_050), Some(7), None),
            }
        ),
        0xa460_7fb4_a9fc_14a4,
        "a delta-derived top, which carries no counts"
    );

    assert_eq!(
        state_key(CHANNEL + 2, INSTRUMENT, &two_sided()),
        0xbe64_3ce3_8d7f_73e5,
        "another channel"
    );

    assert_eq!(
        state_key(CHANNEL, INSTRUMENT + 1, &two_sided()),
        0x669d_54a7_5d1e_0234,
        "another instrument"
    );
}

/// The channel and the instrument the wire-derived literals were computed
/// under. The instrument is the one the bytes below carry, because the
/// derivation reads it off them.
const WIRE_CHANNEL: u8 = 1;
const WIRE_INSTRUMENT: u32 = 11;

/// One `Quote`, 60 bytes, exactly as a datagram carries it.
///
/// Little-endian per the codec, bid 9_950/12 and ask 10_050/7, both sides
/// updated, and both source counts zero — which is the field's own *unavailable*
/// and the commonest thing a venue states, since neither publisher exposes the
/// number on top of book.
const QUOTE_ON_THE_WIRE: [u8; 60] = [
    0x03, 0x3c, 0x00, 0x00, // type 0x03, length 60, reserved
    0x0b, 0x00, 0x00, 0x00, // Instrument ID 11
    0xe8, 0x03, // Source ID 1_000
    0x03, 0x00, // Update Flags: bid updated, ask updated; reserved
    0x01, 0xca, 0x9a, 0x3b, 0x00, 0x00, 0x00, 0x00, // Source Timestamp 1_000_000_001 ns
    0xde, 0x26, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Bid Price 9_950
    0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Bid Qty 12
    0x42, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Ask Price 10_050
    0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Ask Qty 7
    0x00, 0x00, // Bid Source Count 0
    0x00, 0x00, // Ask Source Count 0
    0x00, 0x00, 0x00, 0x00, // reserved
];

/// Where the two counts sit in those bytes, so a variant states one.
const BID_SOURCE_COUNT: usize = 52;
const ASK_SOURCE_COUNT: usize = 54;

/// The top the multicast side derives from those bytes: the real decode, and
/// then the real book.
fn as_the_wire_carries_it(bytes: &[u8; 60]) -> Top {
    let quote = Quote::decode(bytes).expect("the bytes are a well-formed Quote");
    Book::new()
        .quote(
            Channel {
                source_addr: Ipv4Addr::new(198, 51, 100, 7),
                channel_id: WIRE_CHANNEL,
            },
            &quote,
        )
        .expect("a quote is its own anchor and establishes a top")
        .top
}

/// `state_key` returns what it returned before, over a `Quote` off the wire.
///
/// **The pin above cannot see this.** It builds every `Top` by hand, so it holds
/// the fold against a given top and says nothing about which top the derivation
/// produces from given bytes. A row's key is the composition of the two, so a
/// change on either side of it moves a value that rows already carry — and a
/// change to the derivation alone moves it with the fold untouched and every
/// other assertion in this file still green, which is the shape a stored value
/// moves in when nothing fails.
///
/// The literals were computed the same way as the first pin's, by an
/// independent implementation of the fold over the top these bytes derive to,
/// and validated against all seven of them.
#[test]
fn state_keys_value_did_not_move_over_a_quote_from_the_wire() {
    assert_eq!(
        state_key(
            WIRE_CHANNEL,
            WIRE_INSTRUMENT,
            &as_the_wire_carries_it(&QUOTE_ON_THE_WIRE)
        ),
        0xf7c2_e99f_a4f4_714d,
        "a venue that states no count, which the wire carries as a zero"
    );

    let mut stated = QUOTE_ON_THE_WIRE;
    stated[BID_SOURCE_COUNT] = 2;
    stated[ASK_SOURCE_COUNT] = 3;
    assert_eq!(
        state_key(
            WIRE_CHANNEL,
            WIRE_INSTRUMENT,
            &as_the_wire_carries_it(&stated)
        ),
        0x1fee_63ed_e3df_8b42,
        "and a venue that does state one"
    );

    assert_eq!(
        as_the_wire_carries_it(&QUOTE_ON_THE_WIRE).bid.source_count,
        Some(0),
        "the derivation keeps the wire's number, zero included: reading the \
         zero as an absence is `book_key`'s to do and not this path's"
    );
}

/// A book that moved gets a new key, and a book that returned gets its old one.
///
/// The second half is the property the whole equivalence key exists for: a top
/// that goes away and comes back is the same state and must hash the same way,
/// or nothing downstream can tell that the book returned to where it had been.
#[test]
fn a_price_moves_the_book_key_and_a_repeated_book_does_not() {
    let book = two_sided();
    let mut moved = book;
    moved.bid.price_raw = Some(9_951);

    assert_ne!(
        book_key(&book),
        book_key(&moved),
        "one side's price is the whole difference between two books"
    );
    assert_eq!(
        book_key(&book),
        book_key(&two_sided()),
        "the same book stated twice is one key"
    );
}

/// An empty side and a side priced at zero are different books.
///
/// The tag the hash carries, asserted on the key that now stands alone. Top of
/// book states *unavailable* with a zero price and a zero quantity, so a hash
/// that wrote zeros for an absent side would give a real quote at nothing the
/// key of a side that is not there.
#[test]
fn an_absent_side_and_a_side_priced_at_zero_are_different_book_keys() {
    let absent = Top {
        bid: side(None, None, None),
        ask: present(10_050, 7, 3),
    };
    let at_zero = Top {
        bid: present(0, 0, 0),
        ask: present(10_050, 7, 3),
    };

    assert_ne!(
        book_key(&absent),
        book_key(&at_zero),
        "a side that is not there is not a side quoting nothing"
    );
}

/// The point of the split, as one assertion.
///
/// One book seen on two channels is one book and two states. A cross-observer
/// join keyed on `state_key` would find zero pairs here and read as each side
/// having missed every state the other saw — a total outage reported as a clean
/// feed on both paths.
#[test]
fn one_book_under_two_channels_is_one_book_key_and_two_state_keys() {
    // Two observers of one market, each holding the same top and each naming a
    // channel the other does not.
    let (here, seen_here) = (CHANNEL, two_sided());
    let (there, seen_there) = (CHANNEL + 1, two_sided());

    assert_eq!(
        book_key(&seen_here),
        book_key(&seen_there),
        "one book is one book key, whichever channel carried it"
    );
    assert_ne!(
        state_key(here, INSTRUMENT, &seen_here),
        state_key(there, INSTRUMENT, &seen_there),
        "and two states, which is why one key could not answer both questions"
    );
}

/// The channel a `Quote` arrives on, which the book is keyed by and the key is
/// not.
fn channel() -> Channel {
    Channel {
        source_addr: Ipv4Addr::new(198, 51, 100, 7),
        channel_id: CHANNEL,
    }
}

/// One `Quote`, as the multicast side receives it.
fn quote(bid_source_count: u16, ask_source_count: u16) -> Quote {
    Quote {
        instrument_id: INSTRUMENT,
        source_id: 1_000,
        update_flags: QUOTE_BID_UPDATED | QUOTE_ASK_UPDATED,
        source_timestamp_ns: 1_000_000_001,
        bid_price: 9_950,
        bid_qty: 12,
        ask_price: 10_050,
        ask_qty: 7,
        bid_source_count,
        ask_source_count,
    }
}

/// The top the multicast side derives from one `Quote`, through the real book.
fn as_the_wire_states_it(bid_source_count: u16, ask_source_count: u16) -> Top {
    Book::new()
        .quote(channel(), &quote(bid_source_count, ask_source_count))
        .expect("a quote is its own anchor and establishes a top")
        .top
}

/// One book, observed on the wire and beside the venue, is one `book_key`.
///
/// **The count is the field the two observers had to agree about.** The
/// top-of-book specification states `Bid Source Count` as *"Orders/sources at
/// best bid. 0 if unavailable"*, so zero is that field's absence and not a
/// count: a venue that exposes no number is lowered to zero, and the multicast
/// side reads that zero back where an observer of the venue's own upstream holds
/// `None`. A key that told those apart gave one book two keys, and a race keyed
/// on them pairs nothing while both paths read as clean.
///
/// `book_key` reads them alike, and it is the only thing that does: the
/// derivation keeps the wire's zero, because `state_key` has rows written under
/// it and its value over given bytes may not move.
///
/// Both halves are asserted, because either one alone is passed by a wrong
/// answer: dropping the count from the key entirely passes the first, and taking
/// the zero for a count passes the second.
#[test]
fn one_book_seen_beside_the_venue_and_on_the_wire_is_one_book_key() {
    // A venue that states no count. `None` on the venue side because its
    // adapter said so; `None` on the wire side because zero says so.
    let unstated = Top {
        bid: side(Some(9_950), Some(12), None),
        ask: side(Some(10_050), Some(7), None),
    };
    assert_eq!(
        book_key(&as_the_wire_states_it(0, 0)),
        book_key(&unstated),
        "a count the venue does not expose is an absence on both sides"
    );

    // A venue that does state one. The wire carries it, so both observers read
    // the same number and the field still separates two books that differ by it.
    let stated = two_sided();
    assert_eq!(
        book_key(&as_the_wire_states_it(2, 3)),
        book_key(&stated),
        "a count the venue does expose is the same count on both sides"
    );
    assert_ne!(
        book_key(&unstated),
        book_key(&stated),
        "and the two readings are still two books"
    );
}

/// **The row carries the shared fold, and not a second one written beside it.**
///
/// The mutant this kills is the plausible one, and it is a two-line change:
/// `book_row` computes the key itself over `change.top` as the derivation holds
/// it, rather than calling [`book_key`]. That reads as tidier and it is a
/// different key — because the shared function normalises its subject first,
/// reading a zero source count as the absence the top-of-book specification
/// says it is, and the derivation deliberately keeps the wire's zero so that
/// `state_key`'s stored value does not move.
///
/// A zero source count is the commonest shape there is: a venue that exposes no
/// number is published as a zero. So a second fold over the raw top would agree
/// with the shared function on almost nothing, would still produce a stable,
/// plausible-looking hash for every row, and would pair with the venue side
/// **never** — a race reading as a quiet feed on both paths, which is the
/// failure this whole key exists to avoid.
///
/// The expectation is therefore stated as the same function over the top *as
/// either observer states it*, which is the one value that separates the shared
/// fold from a fold of the row's own columns.
#[test]
fn a_derived_row_carries_the_shared_book_key_and_never_a_second_fold() {
    let rows = derived_book_tops(0, 0);
    assert_eq!(
        rows.len(),
        1,
        "one quote states one complete top, so one row"
    );
    let row = &rows[0];

    // What the row stores, which is the wire's zero: the derivation may not read
    // it as an absence, because `state_key` is folded over this top and its
    // value is in rows already.
    assert_eq!(
        (row.bid_source_count, row.ask_source_count),
        (Some(0), Some(0)),
        "the derivation kept the wire's zero, or this test is about nothing"
    );

    // And what the key is: the fold over the top as an observer of the venue's
    // own upstream states it, where that zero is `None`.
    let as_either_observer_states_it = Top {
        bid: side(Some(9_950), Some(12), None),
        ask: side(Some(10_050), Some(7), None),
    };
    assert_eq!(
        row.book_key,
        book_key(&as_either_observer_states_it),
        "the row's key is not `dz_recorder_events::book_key`'s"
    );

    // And the two subjects really are two, or the assertion above holds nothing:
    // a second fold would be over the top as the *row* spells it, zero and all.
    // `state_key` is the fold exposed — it hashes the top it is handed and reads
    // nothing into it — so it is what says the difference survives the hash.
    let as_the_row_spells_it = Top {
        bid: side(Some(9_950), Some(12), Some(0)),
        ask: side(Some(10_050), Some(7), Some(0)),
    };
    assert_eq!(
        row.state_key,
        state_key(row.channel_id, row.instrument_id, &as_the_row_spells_it),
        "`state_key` is folded over the top as the row spells it"
    );
    assert_ne!(
        state_key(row.channel_id, row.instrument_id, &as_the_row_spells_it),
        state_key(
            row.channel_id,
            row.instrument_id,
            &as_either_observer_states_it
        ),
        "the fold cannot tell a stated zero from an absence, so this test \
         could not tell a second fold over the row's own columns from the \
         shared function either"
    );

    // Not `state_key` written into the column, which is the other way one row
    // ends up with two copies of one answer and no answer to the other question.
    assert_ne!(
        row.book_key, row.state_key,
        "the two keys are two values on one row"
    );
    assert_ne!(row.book_key, 0, "an unwritten key is a book nobody hashed");
}

/// One book, on the wire and beside the venue, is one key **through the rows**.
///
/// [`one_book_seen_beside_the_venue_and_on_the_wire_is_one_book_key`] asserts it
/// of the function over a `Top` built by hand. This asserts it of the value a
/// `book_top` row actually carries, which is the composition of the decode, the
/// book and the fold — and a change to any of the three moves a stored value
/// with the fold untouched.
#[test]
fn a_derived_row_pairs_with_a_venue_side_reading_of_one_book() {
    // The venue side holds `None` because its adapter said so; the publisher
    // side derives `Some(0)` from the wire because the specification states the
    // field as "0 if unavailable".
    let venue_side = Top {
        bid: side(Some(9_950), Some(12), None),
        ask: side(Some(10_050), Some(7), None),
    };
    assert_eq!(
        derived_book_tops(0, 0)[0].book_key,
        book_key(&venue_side),
        "one book observed two ways is two keys, so the race finds no pair"
    );

    // A count the venue does expose reaches both sides as the same number, and
    // still separates two books that differ by it.
    let stated = Top {
        bid: side(Some(9_950), Some(12), Some(2)),
        ask: side(Some(10_050), Some(7), Some(3)),
    };
    assert_eq!(derived_book_tops(2, 3)[0].book_key, book_key(&stated));
    assert_ne!(
        derived_book_tops(0, 0)[0].book_key,
        derived_book_tops(2, 3)[0].book_key,
        "and the two readings are still two books"
    );
}

/// The `book_top` rows one `Quote` becomes, through the decode and the book.
///
/// The real derivation and not a hand-built `Top`: what a row carries is the
/// composition of the decode, the book and the fold, and only a test that runs
/// all three holds the value a query will read.
fn derived_book_tops(bid_source_count: u16, ask_source_count: u16) -> Vec<BookTop> {
    let quote = Msg::Quote(Quote {
        instrument_id: AAA,
        source_id: SOURCE_ID,
        update_flags: QUOTE_BID_UPDATED | QUOTE_ASK_UPDATED,
        source_timestamp_ns: 1_000_000_001,
        bid_price: 9_950,
        bid_qty: 12,
        ask_price: 10_050,
        ask_qty: 7,
        bid_source_count,
        ask_source_count,
    });
    let mut datagrams = pack::<TopOfBook>(
        &[Msg::Definition(definition(AAA, "AAA", -2))],
        PortRole::Refdata,
        1,
    );
    datagrams.extend(pack::<TopOfBook>(&[quote], PortRole::Mktdata, 100));

    let mut log = DatagramLog::new(datagrams);
    let id = identity();
    derive_events(
        &mut log,
        &EventInput {
            identity: &id,
            feed: "feed",
            object_key: "object",
            object_sha256: "sha",
            segment_seq: 3,
            magic: MAGIC_TOB,
            observation: "observation",
            persist_snapshot_levels: false,
            derivation: Derivation::Archive,
        },
    )
    .expect("the log does not fail")
    .book_top
}
