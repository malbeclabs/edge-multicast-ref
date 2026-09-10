//! The split: a key over the book alone, and the key that folds onto it.
//!
//! `state_key` answers *is this the same state of this channel's instrument*
//! and `book_key` answers *is this the same book*. The first is what two
//! recorders of one multicast feed pair on, because both read the same
//! `Channel ID` and the same `Instrument ID` off the same datagrams. The second
//! is what two observers of one market pair on, and neither identifier is
//! available to an observer that never saw a datagram: the channel is the
//! operator's mapping from a shard to a channel, and the `Instrument ID` is
//! minted by the reference-data registry and is unique only within an era.
//!
//! **`state_key`'s value may not move.** It is written into rows that exist and
//! compared against rows loaded before this split, so the first test pins it to
//! literals computed from the function as it stood beforehand. A refactor that
//! changed it by a byte would not fail anything else here — every other test in
//! the crate asserts the key against itself.
#![forbid(unsafe_code)]

use std::net::Ipv4Addr;

use dz_edge_tob::{Quote, QUOTE_ASK_UPDATED, QUOTE_BID_UPDATED};
use dz_recorder_events::{book_key, state_key, Book, Channel, Side, Top};

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
/// count: a venue that exposes no number is lowered to zero, and a side that is
/// quoted has something resting on it, so no `Quote` can mean *none*. A
/// derivation reading the zero as `Some(0)` gave the multicast side a count for
/// every quoted side, while an observer of the venue's own upstream states
/// `None` — one book, two keys, and a race that pairs nothing while both paths
/// read as clean.
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
