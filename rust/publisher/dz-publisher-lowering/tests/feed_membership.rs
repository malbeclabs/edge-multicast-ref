//! A message can only go into a datagram of a feed that carries it.
//!
//! This is the defect the runtime had to work around with a type of its own,
//! reached from the other side. A [`DatagramBuilder`] is generic over its feed,
//! so the magic on the wire was always right — and `push` validated only the
//! port role. Nothing refused a `Quote` in a market-by-price datagram.
//!
//! The market-by-price specification anticipates exactly that and says why
//! `0x03` is reserved there: *"Quote in the top-of-book feed, Midpoint in the
//! midpoint feed. Intentionally unused here to prevent accidental
//! cross-decoding if a frame is misrouted."* Until `Feed::CARRIES` existed,
//! nothing enforced it on the emitting side — only a subscriber would have
//! found out, by decoding a message its feed does not define.
//!
//! This crate is where the test lives because it is the one that composes both
//! feeds' messages, so it is the one that could have made the mistake.

use dz_edge_core::{
    AppMessage, ChannelSequence, DatagramBuilder, EncodeError, Feed, PortRole, ResetCount,
    MAX_DATAGRAM_SIZE,
};
use dz_edge_mbp::{
    BookClear, LevelUpdate, MarketByPrice, SnapshotBegin, SnapshotEnd, SnapshotLevel,
};
use dz_edge_tob::{Quote, TopOfBook, Trade};

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

fn trade() -> Trade {
    Trade {
        instrument_id: 41,
        source_id: 7,
        aggressor_side: 1,
        trade_flags: 0,
        source_timestamp_ns: 1,
        trade_price: 4_100,
        trade_qty: 500,
        trade_id: 0,
        cumulative_volume: 0,
    }
}

fn level() -> LevelUpdate {
    LevelUpdate {
        instrument_id: 41,
        source_id: 7,
        side: dz_edge_mbp::SIDE_BID,
        action: dz_edge_mbp::ACTION_NEW,
        per_instrument_seq: 1,
        price_raw: 4_100,
        qty_raw: 500,
        timestamp_ns: 1,
        order_count: dz_edge_mbp::U16_UNAVAILABLE,
        level_index: dz_edge_mbp::U16_UNAVAILABLE,
        update_reason: 0,
        level_flags: 0,
    }
}

fn builder<F: Feed>(port_role: PortRole) -> DatagramBuilder<F> {
    DatagramBuilder::new(
        ChannelSequence::new(0, ResetCount::NEVER_RESET),
        port_role,
        MAX_DATAGRAM_SIZE as u16,
    )
}

#[test]
fn a_quote_cannot_go_into_a_market_by_price_datagram() {
    // **The defect, refused.** The magic would have been the depth feed's, the
    // port role is the right one for a quote on the feed that carries it, and
    // the body is valid. Only the feed's own message table can tell.
    let mut depth = builder::<MarketByPrice>(PortRole::Mktdata);
    let refused = depth
        .push(&quote())
        .expect_err("0x03 is reserved on this feed");
    assert_eq!(
        refused,
        EncodeError::NotCarriedByFeed {
            feed: "market-by-price",
            type_id: 0x03,
        }
    );

    // And the datagram is unchanged, so a caller that refused one message can
    // carry on with the next - the same contract every other refusal at the
    // push has.
    depth.push(&level()).expect("this feed carries 0x40");
}

#[test]
fn a_level_update_cannot_go_into_a_top_of_book_datagram() {
    // The other direction, which no specification text calls out because
    // nobody expected it - and which is exactly why a table beats a comment.
    let mut tob = builder::<TopOfBook>(PortRole::Mktdata);
    assert_eq!(
        tob.push(&level())
            .expect_err("0x40 is not a top-of-book type"),
        EncodeError::NotCarriedByFeed {
            feed: "top-of-book",
            type_id: 0x40,
        }
    );
}

#[test]
fn a_trade_goes_into_either_feed_because_both_carry_it() {
    // The cross-specification policy in one assertion: `0x04` is byte-for-byte
    // identical between the two feeds, so both tables list it and neither
    // refuses it. A membership check that refused a shared Type ID would have
    // broken the one message the whole boundary made structural.
    builder::<TopOfBook>(PortRole::Mktdata)
        .push(&trade())
        .expect("top-of-book carries 0x04");
    builder::<MarketByPrice>(PortRole::Mktdata)
        .push(&trade())
        .expect("market-by-price carries 0x04 too");
}

#[test]
fn the_membership_check_comes_before_every_other_refusal() {
    // A message the feed does not carry is not made carriable by a correct port
    // role, a valid body or a bigger datagram - so it is refused first, and the
    // error a caller sees names the real problem rather than the first one the
    // send path happened to notice.
    //
    // Asserted with a builder whose capacity cannot hold the message either: if
    // the order were reversed this would report the capacity.
    // The capacity clamps to the header, so nothing at all fits.
    let mut tiny: DatagramBuilder<MarketByPrice> = DatagramBuilder::new(
        ChannelSequence::new(0, ResetCount::NEVER_RESET),
        PortRole::Mktdata,
        0,
    );
    assert_eq!(
        tiny.push(&quote())
            .expect_err("neither carried nor small enough"),
        EncodeError::NotCarriedByFeed {
            feed: "market-by-price",
            type_id: 0x03,
        }
    );
}

#[test]
fn the_snapshot_messages_are_the_depth_feeds_and_not_the_top_of_books() {
    let snapshot = SnapshotBegin {
        instrument_id: 41,
        anchor_seq: 1,
        total_levels: 0,
        snapshot_id: 1,
        last_instrument_seq: 0,
        timestamp_ns: 1,
        depth_bound: 0,
    };
    builder::<MarketByPrice>(PortRole::Snapshot)
        .push(&snapshot)
        .expect("market-by-price carries 0x20");
    assert!(matches!(
        builder::<TopOfBook>(PortRole::Snapshot).push(&snapshot),
        Err(EncodeError::NotCarriedByFeed { type_id: 0x20, .. })
    ));
}

#[test]
fn each_feeds_table_is_the_one_its_specification_states() {
    // Transcribed by hand from each specification's message table, and
    // deliberately not derived from the constants those crates export: a table
    // built from the thing it checks agrees with it by construction. This one
    // fails when either side moves.
    //
    // Top-of-book: 0x01 Heartbeat, 0x02 InstrumentDefinition, 0x03 Quote,
    // 0x04 Trade, 0x06 EndOfSession, 0x07 ManifestSummary, 0x08 Liquidation.
    // The table steps over 0x05.
    assert_eq!(
        <TopOfBook as Feed>::CARRIES,
        &[0x01, 0x02, 0x03, 0x04, 0x06, 0x07, 0x08]
    );

    // Market-by-price: the same inherited five, plus Liquidation, the two
    // shared with the market-by-order feed at its own numbers, the two snapshot
    // bookends, and this feed's own three. 0x03 and 0x05 are reserved and
    // absent - 0x03 explicitly, to stop the cross-decoding this file tests.
    assert_eq!(
        <MarketByPrice as Feed>::CARRIES,
        &[0x01, 0x02, 0x04, 0x06, 0x07, 0x08, 0x13, 0x14, 0x20, 0x22, 0x40, 0x41, 0x42]
    );

    // And every type this workspace can encode for a feed is in that feed's
    // table, which is the direction a new message type gets wrong: added to a
    // codec crate and not to the table, it would be refused at every push.
    for type_id in [
        Quote::TYPE_ID,
        Trade::TYPE_ID,
        dz_edge_core::Heartbeat::TYPE_ID,
        dz_edge_core::EndOfSession::TYPE_ID,
        dz_edge_refdata::InstrumentDefinition::TYPE_ID,
        dz_edge_refdata::ManifestSummary::TYPE_ID,
    ] {
        assert!(
            TopOfBook::carries(type_id),
            "top-of-book cannot push {type_id:#04x}, which this workspace encodes for it"
        );
    }
    for type_id in [
        LevelUpdate::TYPE_ID,
        BookClear::TYPE_ID,
        SnapshotBegin::TYPE_ID,
        SnapshotLevel::TYPE_ID,
        SnapshotEnd::TYPE_ID,
        Trade::TYPE_ID,
        dz_edge_core::Heartbeat::TYPE_ID,
        dz_edge_core::EndOfSession::TYPE_ID,
        dz_edge_refdata::InstrumentDefinition::TYPE_ID,
        dz_edge_refdata::ManifestSummary::TYPE_ID,
    ] {
        assert!(
            MarketByPrice::carries(type_id),
            "market-by-price cannot push {type_id:#04x}, which this workspace encodes for it"
        );
    }
}
