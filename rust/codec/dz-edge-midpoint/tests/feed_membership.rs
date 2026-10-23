//! `0x03` is `Quote` in top-of-book and `Midpoint` here. Both feeds' tables
//! list it, so the Type ID alone admits either message into the other's
//! datagram. `AppMessage::FEED_LOCAL` closes that.

use dz_edge_core::{
    AppMessage, ChannelSequence, DatagramBuilder, EncodeError, Feed, PortRole, ResetCount,
    MAX_DATAGRAM_SIZE,
};
use dz_edge_midpoint::{Midpoint, MidpointFeed, MAGIC_MIDPOINT, METHOD_BBO_MID};
use dz_edge_tob::{Quote, TopOfBook, MAGIC_TOB};

fn builder<F: Feed>() -> DatagramBuilder<F> {
    DatagramBuilder::new(
        ChannelSequence::new(0, ResetCount::NEVER_RESET),
        PortRole::Mktdata,
        MAX_DATAGRAM_SIZE as u16,
    )
}

fn midpoint() -> Midpoint {
    Midpoint {
        instrument_id: 41,
        source_id: 7,
        method: METHOD_BBO_MID,
        quality_flags: 0,
        input_timestamp_ns: 1,
        compute_timestamp_ns: 2,
        price: 4_200,
        window_ms: 0,
        volume: 0,
    }
}

fn quote() -> Quote {
    Quote {
        instrument_id: 41,
        source_id: 7,
        update_flags: 0x03,
        source_timestamp_ns: 1,
        bid_price: 4_100,
        bid_qty: 500,
        ask_price: 4_300,
        ask_qty: 700,
        bid_source_count: 0,
        ask_source_count: 0,
    }
}

#[test]
fn both_messages_are_local_to_their_own_feed() {
    assert_eq!(Midpoint::TYPE_ID, Quote::TYPE_ID);
    assert_eq!(Midpoint::FEED_LOCAL, Some(MAGIC_MIDPOINT));
    assert_eq!(Quote::FEED_LOCAL, Some(MAGIC_TOB));
}

#[test]
fn a_quote_cannot_go_into_a_midpoint_datagram() {
    let mut b = builder::<MidpointFeed>();
    assert_eq!(
        b.push(&quote()).expect_err("0x03 is Midpoint on this feed"),
        EncodeError::NotCarriedByFeed {
            feed: "midpoint",
            message: core::any::type_name::<Quote>(),
            type_id: 0x03,
        }
    );
    b.push(&midpoint()).expect("the builder is unchanged");
}

#[test]
fn a_midpoint_cannot_go_into_a_top_of_book_datagram() {
    let mut b = builder::<TopOfBook>();
    assert_eq!(
        b.push(&midpoint()).expect_err("0x03 is Quote on this feed"),
        EncodeError::NotCarriedByFeed {
            feed: "top-of-book",
            message: core::any::type_name::<Midpoint>(),
            type_id: 0x03,
        }
    );
    b.push(&quote()).expect("the builder is unchanged");
}
