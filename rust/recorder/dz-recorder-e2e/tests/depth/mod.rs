//! One depth feed's three port roles, as a publisher drives them.
//!
//! Shared by the two market data suites — the one that writes into a file and
//! the one that writes into a column store — so that both are held against the
//! same encoded bytes. Nothing here is hand-assembled: the real
//! [`DatagramBuilder`] frames every datagram, and the fixture states only what a
//! publisher states.
//!
//! # Why every role carries one `Channel ID`
//!
//! Reference data is keyed on the **channel** — `(source address, Channel ID)` —
//! and never on the channel instance, because definitions arrive on `refdata`
//! and prices on `mktdata` and the destination port is part of an instance. A
//! fixture that gave the three roles three channels would file the definitions
//! where the prices could never find them, and every event row would be refused
//! for an unresolved instrument. That is a property of the feed and not of this
//! file: the glossary has an instrument as unique within a channel.
//!
//! # And why the cycle comes before the deltas
//!
//! A delta book anchors only on a complete snapshot cycle, so every top derived
//! before the first one is `book_certain = 0` with `no_anchor`. A subscriber
//! snapshots and then applies; a fixture that applied first would be asserting
//! the certainty rule rather than the prices.
#![allow(dead_code)]

use dz_edge_core::{ChannelSequence, DatagramBuilder, PortRole, ResetCount};
use dz_edge_mbp::{
    BookClear, LevelUpdate, MarketByPrice, SnapshotBegin, SnapshotEnd, SnapshotLevel,
    ACTION_CHANGE, ACTION_DELETE, ACTION_NEW, CLEAR_BID, SCOPE_ENTIRE_SIDE, SIDE_ASK, SIDE_BID,
};
use dz_edge_refdata::{
    InstrumentDefinition, ManifestSummary, ASSET_CLASS_CRYPTO_SPOT, PRICE_BOUND_NON_NEGATIVE,
    SETTLE_TYPE_CASH, SYMBOL_LEN,
};
use dz_edge_tob::{Trade, AGGRESSOR_BUY};
use dz_recorder_core::RecorderIdentity;
use dz_recorder_events::{derive_events, DerivedEvents, EventInput};
use dz_recorder_replay::{ArchiveSource, OwnedDatagram};

use crate::common::{Recorded, Wire, PUBLISHER_A, PUBLISHER_MTU};

/// Every role of a depth feed, which is what makes this feed the one worth
/// deriving: the anchor arrives on `snapshot` and the deltas on `mktdata`.
pub const DEPTH_ROLES: &[PortRole] = &[PortRole::Mktdata, PortRole::Refdata, PortRole::Snapshot];

/// One `Channel ID` for all three roles. See the module comment.
pub const CHANNEL: u8 = 3;
pub const INSTRUMENT: u32 = 4_242;
pub const SOURCE_ID: u16 = 2;
pub const SNAPSHOT_ID: u32 = 77;
pub const SYMBOL: &str = "BTC-USDT";
pub const PRICE_EXPONENT: i8 = -2;
pub const QTY_EXPONENT: i8 = -8;

/// The first live sequence number, and the anchor immediately behind it.
///
/// The cycle states the book as of `ANCHOR_SEQ`, and every delta below is after
/// it: a subscriber discards what precedes its anchor, so a fixture whose
/// deltas straddled it would be asserting that rule instead of these prices.
pub const FIRST_LIVE_SEQ: u64 = 100;
pub const ANCHOR_SEQ: u64 = FIRST_LIVE_SEQ - 1;

/// The two levels the cycle carries, which are the top of both sides.
pub const LEVELS: [(i64, u64, u8); 2] = [
    (ANCHOR_BID_PRICE, ANCHOR_BID_QTY, SIDE_BID),
    (ANCHOR_ASK_PRICE, ANCHOR_ASK_QTY, SIDE_ASK),
];
pub const ANCHOR_BID_PRICE: i64 = 9_999_500;
pub const ANCHOR_BID_QTY: u64 = 12_500;
pub const ANCHOR_ASK_PRICE: i64 = 10_000_500;
pub const ANCHOR_ASK_QTY: u64 = 7_250;

/// A better bid, then a better ask, then the bid taken away again.
pub const BETTER_BID_PRICE: i64 = 9_999_600;
pub const BETTER_BID_QTY: u64 = 11_000;
pub const BETTER_ASK_PRICE: i64 = 10_000_400;
pub const BETTER_ASK_QTY: u64 = 5_000;

pub const TRADE_ID: u64 = 99;
pub const CUMULATIVE_VOLUME: u64 = 4_000;

/// The wire's fixed-width symbol field, NUL-padded.
///
/// NUL and not spaces: the field is `char[64]` and a reader takes it to the
/// first NUL, so a space-padded fixture would assert a symbol with fifty-six
/// trailing blanks in it and quietly agree that that is the instrument's name.
fn symbol_bytes() -> [u8; SYMBOL_LEN] {
    let mut out = [0u8; SYMBOL_LEN];
    out[..SYMBOL.len()].copy_from_slice(SYMBOL.as_bytes());
    out
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition {
        instrument_id: INSTRUMENT,
        source_id: SOURCE_ID,
        symbol: symbol_bytes(),
        leg1: *b"BTC     ",
        leg2: *b"USDT    ",
        asset_class: ASSET_CLASS_CRYPTO_SPOT,
        price_exponent: PRICE_EXPONENT,
        qty_exponent: QTY_EXPONENT,
        market_model: 1,
        tick_size: 1,
        lot_size: 1,
        contract_value: 1,
        expiry_ns: 0,
        settle_type: SETTLE_TYPE_CASH,
        price_bound: PRICE_BOUND_NON_NEGATIVE,
        manifest_seq: 1,
    }
}

/// One level update, with the `Action` the specification pairs with its
/// quantity: a zero is a removal and must say so, and a Delete carries nothing
/// else.
fn level(seq: u32, price_raw: i64, qty_raw: u64, side: u8, action: u8) -> LevelUpdate {
    LevelUpdate {
        instrument_id: INSTRUMENT,
        source_id: SOURCE_ID,
        side,
        action,
        per_instrument_seq: seq,
        price_raw,
        qty_raw,
        timestamp_ns: 1_772_000_000_000_000_000 + u64::from(seq),
        order_count: 3,
        level_index: 0,
        update_reason: 0,
        level_flags: 0,
    }
}

/// One datagram of this feed, framed by the real builder.
fn datagram(
    sequence: ChannelSequence,
    role: PortRole,
    push: impl FnOnce(&mut DatagramBuilder<MarketByPrice>),
) -> Vec<u8> {
    let mut builder = DatagramBuilder::<MarketByPrice>::new(sequence, role, PUBLISHER_MTU);
    push(&mut builder);
    builder
        .finish(sequence.sequence_number() * 1_000 + 1_772_000_000_000_000_000)
        .expect("a datagram with at least one message is emittable")
}

/// The stream: reference data, then a complete cycle, then the deltas.
#[must_use]
pub fn depth_stream() -> Vec<OwnedDatagram> {
    let mut wire = Wire::new();

    // Reference data first, because a statement is in force from the instant it
    // was received and everything below decodes at the scale it states.
    let refdata = ChannelSequence::new(CHANNEL, ResetCount::NEVER_RESET);
    wire.arrive(
        datagram(refdata, PortRole::Refdata, |b| {
            b.push(&definition())
                .expect("refdata carries an instrument definition");
            b.push(&ManifestSummary {
                channel_id: 0,
                valid: 1,
                manifest_seq: 1,
                instrument_count: 1,
                timestamp_ns: 1_772_000_000_000_000_200,
            })
            .expect("and the summary that closes the published set");
        }),
        PUBLISHER_A,
        PortRole::Refdata,
    );

    // The cycle, split across two datagrams: the ordinary case for a real book,
    // and the case where a begin and its levels are not one unit of delivery.
    let mut snapshot = ChannelSequence::new(CHANNEL, ResetCount::NEVER_RESET);
    wire.arrive(
        datagram(snapshot, PortRole::Snapshot, |b| {
            b.push(&SnapshotBegin {
                instrument_id: INSTRUMENT,
                anchor_seq: ANCHOR_SEQ,
                total_levels: LEVELS.len() as u32,
                snapshot_id: SNAPSHOT_ID,
                last_instrument_seq: 0,
                timestamp_ns: 1_772_000_000_000_000_300,
                depth_bound: 50,
            })
            .expect("snapshot carries a begin");
            b.push(&snapshot_level(LEVELS[0])).expect("and a level");
        }),
        PUBLISHER_A,
        PortRole::Snapshot,
    );
    snapshot.advance();
    wire.arrive(
        datagram(snapshot, PortRole::Snapshot, |b| {
            b.push(&snapshot_level(LEVELS[1]))
                .expect("the second level");
            b.push(&SnapshotEnd {
                instrument_id: INSTRUMENT,
                anchor_seq: ANCHOR_SEQ,
                snapshot_id: SNAPSHOT_ID,
            })
            .expect("and the end that closes it");
        }),
        PUBLISHER_A,
        PortRole::Snapshot,
    );

    // The deltas, every one of them after the anchor.
    let mut mktdata = ChannelSequence::resume(CHANNEL, ResetCount::NEVER_RESET, FIRST_LIVE_SEQ);
    for (price, qty, side, action) in [
        (BETTER_BID_PRICE, BETTER_BID_QTY, SIDE_BID, ACTION_NEW),
        (BETTER_ASK_PRICE, BETTER_ASK_QTY, SIDE_ASK, ACTION_NEW),
        // Zero is a removal, and it has to survive the round trip as one rather
        // than as an absent value.
        (BETTER_BID_PRICE, 0, SIDE_BID, ACTION_DELETE),
    ] {
        let seq = u32::try_from(mktdata.sequence_number()).expect("a small test sequence");
        wire.arrive(
            datagram(mktdata, PortRole::Mktdata, |b| {
                b.push(&level(seq, price, qty, side, action))
                    .expect("mktdata carries a level update");
            }),
            PUBLISHER_A,
            PortRole::Mktdata,
        );
        mktdata.advance();
    }

    // A bulk removal and a trade in one datagram, so two messages share a
    // sequence number and a receive stamp and are told apart by nothing but
    // their position in it.
    let seq = u32::try_from(mktdata.sequence_number()).expect("a small test sequence");
    wire.arrive(
        datagram(mktdata, PortRole::Mktdata, |b| {
            b.push(&BookClear {
                instrument_id: INSTRUMENT,
                source_id: SOURCE_ID,
                clear_side: CLEAR_BID,
                scope: SCOPE_ENTIRE_SIDE,
                per_instrument_seq: seq,
                from_price_raw: 0,
                timestamp_ns: 1_772_000_000_000_000_400,
                clear_reason: 1,
            })
            .expect("mktdata carries a book clear");
            b.push(&Trade {
                instrument_id: INSTRUMENT,
                source_id: SOURCE_ID,
                aggressor_side: AGGRESSOR_BUY,
                trade_flags: 0,
                source_timestamp_ns: 1_772_000_000_000_000_401,
                trade_price: BETTER_ASK_PRICE,
                trade_qty: 4,
                trade_id: TRADE_ID,
                cumulative_volume: CUMULATIVE_VOLUME,
            })
            .expect("and the trade that followed it");
        }),
        PUBLISHER_A,
        PortRole::Mktdata,
    );

    wire.sent
}

fn snapshot_level((price_raw, qty_raw, side): (i64, u64, u8)) -> SnapshotLevel {
    SnapshotLevel {
        snapshot_id: SNAPSHOT_ID,
        price_raw,
        qty_raw,
        order_count: 1,
        side,
        level_flags: 0,
    }
}

/// A level update whose action a caller states, for a suite about actions.
#[must_use]
pub fn change(seq: u32, price_raw: i64, qty_raw: u64, side: u8) -> LevelUpdate {
    level(seq, price_raw, qty_raw, side, ACTION_CHANGE)
}

/// Derives the market data rows out of a published object, as the loader does.
///
/// The identity comes from the manifest and not from a fixture, because that is
/// where the loader takes it from: these rows are signed by the recorder that
/// observed the bytes.
#[must_use]
pub fn derive(recorded: &Recorded, magic: u16, persist_snapshot_levels: bool) -> DerivedEvents {
    let manifest = &recorded.manifest;
    let identity = RecorderIdentity {
        site: manifest.site.clone(),
        recorder: manifest.recorder.clone(),
        env: manifest.env.clone(),
        build_version: manifest.build_version.clone(),
        build_commit: manifest.build_commit.clone(),
        config_hash: manifest.config_hash.clone(),
    };
    let mut source = ArchiveSource::open(&recorded.object).expect("the archive opens");
    derive_events(
        &mut source,
        &EventInput {
            identity: &identity,
            feed: &manifest.feed,
            object_key: &manifest.object_key,
            object_sha256: &manifest.sha256,
            segment_seq: manifest.segment_seq,
            magic,
            observation: &identity.hardware(),
            persist_snapshot_levels,
        },
    )
    .expect("the object the writer just published derives")
}
