//! The property that matters: a record round-trips losslessly **with respect
//! to the lowering**.
//!
//! Not "the event compares equal" — that would pass for an encoding that
//! preserved a field nothing reads and lost one that reaches the wire. What
//! this asserts is that the original event and the decoded one lower to the
//! same bytes, for every variant and every shape inside one. If they do, the
//! encoding is lossless for every purpose either direction has: a source
//! process in another language publishes what a Rust adapter would have, and a
//! reference stream can be re-lowered and compared.

use dz_adapter_core::{
    Adapter, Aggressor, AssetClass, ClearScope, ConnectionId, Event, EventSink, InstrumentRef,
    InstrumentSpec, ListingSink, MarketModel, Payload, Presence, PriceBound, Scalar, SettleType,
    Side, SideUpdate, TradeFlags,
};
use dz_adapter_uds::{decode, RecordError, RecordWriter, UdsAdapter, UdsListing, VERSION};
use dz_edge_core::AppMessage;
use dz_publisher_lowering::{DepthLowering, Instrument, InstrumentTable, Lowering, SourceId};

const SYMBOL: &str = "EXAMPLE-1";
const PRICE_EXPONENT: i8 = -4;
const QTY_EXPONENT: i8 = -2;

fn source_id() -> SourceId {
    SourceId::new(7).expect("7 is an assigned production id")
}

fn table() -> InstrumentTable {
    let mut instruments = InstrumentTable::new();
    instruments.admit(Instrument {
        instrument_id: 41,
        price_exponent: PRICE_EXPONENT,
        qty_exponent: QTY_EXPONENT,
        quoted_per_contract: None,
    });
    instruments
}

fn handle() -> InstrumentRef {
    InstrumentRef::from_admission(0)
}

/// The four flag states, through the encoding, compared as wire bytes.
///
/// Its own test rather than a row of the table below, because the flags byte is
/// derived from the pair of sides and an encoding that lost one side's shape
/// would still produce a valid quote — just a wrong one.
#[test]
fn every_quote_shape_lowers_the_same_before_and_after_the_encoding() {
    let present = |px, qty, count| SideUpdate::Present {
        px: Scalar::fixed(px, PRICE_EXPONENT),
        qty: Scalar::fixed(qty, QTY_EXPONENT),
        source_count: count,
    };
    let shapes = [
        (
            "both present",
            present(4_100, 500, Some(3)),
            present(4_300, 700, Some(4)),
        ),
        ("bid only", present(4_100, 500, None), SideUpdate::Gone),
        ("ask only", SideUpdate::Gone, present(4_300, 700, None)),
        ("both gone", SideUpdate::Gone, SideUpdate::Gone),
    ];

    for (state, bid, ask) in shapes {
        let event = Event::Quote {
            instrument: handle(),
            source_ts_ns: 1_700_000_000_000_000_000,
            bid,
            ask,
        };
        assert_eq!(
            lowered_directly(&[event]),
            lowered_through_the_encoding(&[event]),
            "{state}: the encoding changed what reaches the wire"
        );
    }
}

/// A sink that lowers whatever it is handed, so a borrowed event can be
/// compared without outliving its buffer.
struct Lowered {
    instruments: InstrumentTable,
    bytes: Vec<Vec<u8>>,
}

impl EventSink for Lowered {
    fn upstream_message(&mut self, _message_type: &'static str) {}

    fn event(&mut self, event: Event<'_>) {
        let mut depth = DepthLowering::new(source_id());
        let lowering = Lowering::new(source_id());
        let bytes = match event {
            Event::Quote {
                instrument,
                source_ts_ns,
                bid,
                ask,
            } => {
                let q = lowering
                    .lower_quote(&self.instruments, instrument, source_ts_ns, bid, ask)
                    .expect("lowers");
                let mut b = vec![0u8; dz_edge_tob::Quote::SIZE];
                q.encode_into(&mut b);
                b
            }
            Event::Trade {
                instrument,
                source_ts_ns,
                px,
                qty,
                aggressor,
                trade_id,
                cumulative_volume,
                flags,
            } => {
                let t = lowering
                    .lower_trade(
                        &self.instruments,
                        instrument,
                        source_ts_ns,
                        px,
                        qty,
                        aggressor,
                        trade_id,
                        cumulative_volume,
                        flags,
                    )
                    .expect("lowers");
                let mut b = vec![0u8; dz_edge_tob::Trade::SIZE];
                t.encode_into(&mut b);
                b
            }
            Event::Level {
                instrument,
                source_ts_ns,
                side,
                px,
                qty,
                order_count,
                presence,
            } => {
                let l = depth
                    .lower_level(
                        &self.instruments,
                        instrument,
                        source_ts_ns,
                        side,
                        px,
                        qty,
                        order_count,
                        presence,
                    )
                    .expect("lowers");
                let mut b = vec![0u8; dz_edge_mbp::LevelUpdate::SIZE];
                l.encode_into(&mut b);
                b
            }
            Event::Clear {
                instrument,
                source_ts_ns,
                scope,
            } => {
                let c = depth
                    .lower_clear(&self.instruments, instrument, source_ts_ns, scope)
                    .expect("lowers");
                let mut b = vec![0u8; dz_edge_mbp::BookClear::SIZE];
                c.encode_into(&mut b);
                b
            }
            other => panic!("unhandled {other:?}"),
        };
        self.bytes.push(bytes);
    }
}

/// Each event lowered straight from the boundary's own value.
fn lowered_directly(events: &[Event<'_>]) -> Vec<Vec<u8>> {
    let mut sink = Lowered {
        instruments: table(),
        bytes: Vec::new(),
    };
    for event in events {
        sink.event(*event);
    }
    sink.bytes
}

/// Each event written as a record, read back through the adapter, and lowered.
///
/// Through the adapter rather than through `decode`, so the payload splitting,
/// the symbol resolution and the sink are all in the path — an encoding being
/// lossless is not much use if the adapter around it is not.
fn lowered_through_the_encoding(events: &[Event<'_>]) -> Vec<Vec<u8>> {
    let mut writer = RecordWriter::new();
    let mut stream = Vec::new();
    for event in events {
        writer.write(SYMBOL, event, &mut stream);
    }

    let mut sink = Lowered {
        instruments: table(),
        bytes: Vec::new(),
    };
    admitted_adapter()
        .on_payload(
            &Payload {
                bytes: &stream,
                recv_ts_ns: 1,
                connection: ConnectionId::new("mktdata"),
            },
            &mut sink,
        )
        .expect("every record reads");
    sink.bytes
}

/// Every variant and every shape inside one, lowered directly and then through
/// the encoding, and the two runs compared.
///
#[test]
fn every_event_variant_survives_the_encoding_intact() {
    let events: Vec<Event<'static>> = vec![
        Event::Quote {
            instrument: handle(),
            source_ts_ns: 1,
            bid: SideUpdate::Present {
                px: Scalar::fixed(4_100, PRICE_EXPONENT),
                qty: Scalar::fixed(500, QTY_EXPONENT),
                source_count: Some(3),
            },
            ask: SideUpdate::Gone,
        },
        Event::Trade {
            instrument: handle(),
            source_ts_ns: 2,
            px: Scalar::fixed(4_100, PRICE_EXPONENT),
            qty: Scalar::fixed(500, QTY_EXPONENT),
            aggressor: Aggressor::Sell,
            trade_id: Some(0xDEAD_BEEF),
            cumulative_volume: Some(Scalar::fixed(12_000, QTY_EXPONENT)),
            flags: TradeFlags {
                block: true,
                sweep: true,
                cross: true,
            },
        },
        // Every sentinel of the same variant, because the absent cases are a
        // different path through the encoding.
        Event::Trade {
            instrument: handle(),
            source_ts_ns: 3,
            px: Scalar::fixed(4_100, PRICE_EXPONENT),
            qty: Scalar::fixed(500, QTY_EXPONENT),
            aggressor: Aggressor::Unknown,
            trade_id: None,
            cumulative_volume: None,
            flags: TradeFlags::NONE,
        },
        Event::Level {
            instrument: handle(),
            source_ts_ns: 4,
            side: Side::Ask,
            px: Scalar::fixed(4_300, PRICE_EXPONENT),
            qty: Scalar::fixed(700, QTY_EXPONENT),
            order_count: Some(5),
            presence: Presence::Change,
        },
        // A removal, which is the row of the action table that matters most.
        Event::Level {
            instrument: handle(),
            source_ts_ns: 5,
            side: Side::Bid,
            px: Scalar::fixed(4_100, PRICE_EXPONENT),
            qty: Scalar::fixed(0, QTY_EXPONENT),
            order_count: None,
            presence: Presence::Unknown,
        },
        Event::Clear {
            instrument: handle(),
            source_ts_ns: 6,
            scope: ClearScope::BothSides,
        },
        Event::Clear {
            instrument: handle(),
            source_ts_ns: 7,
            scope: ClearScope::EntireSide(Side::Bid),
        },
        Event::Clear {
            instrument: handle(),
            source_ts_ns: 8,
            scope: ClearScope::FromPrice {
                side: Side::Ask,
                px: Scalar::fixed(4_300, PRICE_EXPONENT),
            },
        },
        // The text path, which is the shape a source in another language is
        // most likely to write.
        Event::Quote {
            instrument: handle(),
            source_ts_ns: 9,
            bid: SideUpdate::Present {
                px: Scalar::text("0.41"),
                qty: Scalar::text("5"),
                source_count: None,
            },
            ask: SideUpdate::Present {
                px: Scalar::text("0.43"),
                qty: Scalar::text("7"),
                source_count: None,
            },
        },
    ];

    let direct = lowered_directly(&events);
    assert_eq!(direct.len(), events.len(), "every fixture lowered directly");
    assert_eq!(
        lowered_through_the_encoding(&events),
        direct,
        "the encoding changed what reaches the wire for at least one event"
    );
}

/// An adapter that has been offered its one instrument and holds the handle.
fn admitted_adapter() -> UdsAdapter {
    struct Admit;
    impl ListingSink for Admit {
        fn list(&mut self, _spec: &InstrumentSpec<'_>) -> Option<InstrumentRef> {
            Some(handle())
        }
        fn delist(&mut self, _instrument: InstrumentRef) {}
    }

    let mut adapter = UdsAdapter::new(vec![UdsListing {
        symbol: SYMBOL.to_string(),
        leg1: None,
        leg2: None,
        asset_class: AssetClass::CryptoSpot,
        price_exponent: PRICE_EXPONENT,
        qty_exponent: QTY_EXPONENT,
        market_model: MarketModel::Clob,
        tick_size: "0.0001".to_string(),
        lot_size: "0.01".to_string(),
        contract_value: None,
        quoted_per_contract: None,
        expiry_ns: None,
        settle_type: SettleType::Cash,
        price_bound: PriceBound::NonNegative,
    }]);
    adapter.poll_listings(&mut Admit);
    assert!(adapter.handle(SYMBOL).is_some());
    adapter
}

/// A sink that counts and discards, for the cases where the events do not
/// matter.
#[derive(Default)]
struct Counting {
    events: usize,
    messages: usize,
}

impl EventSink for Counting {
    fn upstream_message(&mut self, _message_type: &'static str) {
        self.messages += 1;
    }
    fn event(&mut self, _event: Event<'_>) {
        self.events += 1;
    }
}

#[test]
fn one_payload_holding_several_records_yields_every_one() {
    // A transport reading a byte stream hands over whatever arrived, so the
    // record boundary and the payload boundary have no reason to coincide.
    let mut writer = RecordWriter::new();
    let mut stream = Vec::new();
    for ts in 1..=3 {
        writer.write(
            SYMBOL,
            &Event::Clear {
                instrument: handle(),
                source_ts_ns: ts,
                scope: ClearScope::BothSides,
            },
            &mut stream,
        );
    }

    let mut adapter = admitted_adapter();
    let mut sink = Counting::default();
    adapter
        .on_payload(
            &Payload {
                bytes: &stream,
                recv_ts_ns: 1,
                connection: ConnectionId::new("mktdata"),
            },
            &mut sink,
        )
        .expect("three records read");
    assert_eq!(sink.events, 3);
    assert_eq!(sink.messages, 3, "each record is an upstream message");
}

#[test]
fn a_record_split_across_payloads_is_truncated_and_not_silently_dropped() {
    // A reader that ignored a partial tail would lose one event per read on a
    // stream that never aligns, and lose it silently.
    let mut stream = Vec::new();
    RecordWriter::new().write(
        SYMBOL,
        &Event::Clear {
            instrument: handle(),
            source_ts_ns: 1,
            scope: ClearScope::BothSides,
        },
        &mut stream,
    );
    let cut = stream.len() - 2;

    let mut adapter = admitted_adapter();
    let mut sink = Counting::default();
    let error = adapter
        .on_payload(
            &Payload {
                bytes: &stream[..cut],
                recv_ts_ns: 1,
                connection: ConnectionId::new("mktdata"),
            },
            &mut sink,
        )
        .expect_err("the record declared more than it held");
    assert_eq!(error.as_str(), "truncated");
    assert_eq!(sink.events, 0);
}

#[test]
fn an_unknown_version_or_kind_is_skipped_and_the_stream_continues() {
    // **The reason the length comes first.** A newer writer stays readable, and
    // one record this build does not understand does not end the stream. A
    // version inside a self-describing body would make it unskippable.
    let mut writer = RecordWriter::new();
    let mut stream = Vec::new();
    let clear = Event::Clear {
        instrument: handle(),
        source_ts_ns: 1,
        scope: ClearScope::BothSides,
    };
    writer.write(SYMBOL, &clear, &mut stream);
    let one = stream.len();

    // A record from a newer writer: the same bytes with the version bumped.
    writer.write(SYMBOL, &clear, &mut stream);
    stream[one + 4] = VERSION + 1;
    // And one whose event kind this build does not implement.
    writer.write(SYMBOL, &clear, &mut stream);
    let third = one * 2;
    stream[third + 5] = 200;
    writer.write(SYMBOL, &clear, &mut stream);

    let mut adapter = admitted_adapter();
    let mut sink = Counting::default();
    adapter
        .on_payload(
            &Payload {
                bytes: &stream,
                recv_ts_ns: 1,
                connection: ConnectionId::new("mktdata"),
            },
            &mut sink,
        )
        .expect("two unreadable records do not end the stream");

    assert_eq!(sink.messages, 4, "all four arrived");
    assert_eq!(sink.events, 2, "the two this build understands");
}

#[test]
fn a_record_for_an_instrument_this_runtime_did_not_admit_is_ordinary() {
    // The selection policy is the runtime's, and a source offering more than it
    // admits is the normal case rather than an error.
    let mut stream = Vec::new();
    RecordWriter::new().write(
        "SOMETHING-ELSE",
        &Event::Clear {
            instrument: handle(),
            source_ts_ns: 1,
            scope: ClearScope::BothSides,
        },
        &mut stream,
    );

    let mut adapter = admitted_adapter();
    let mut sink = Counting::default();
    adapter
        .on_payload(
            &Payload {
                bytes: &stream,
                recv_ts_ns: 1,
                connection: ConnectionId::new("mktdata"),
            },
            &mut sink,
        )
        .expect("not an error");
    assert_eq!(sink.messages, 1, "the record still arrived");
    assert_eq!(sink.events, 0);
}

#[test]
fn a_body_carrying_more_than_its_shape_is_malformed_rather_than_tolerated() {
    // A writer that added a field and a reader that ignored it is exactly what
    // the version exists to catch. Tolerating trailing bytes would let half a
    // fleet ignore a field the other half started sending.
    let mut stream = Vec::new();
    RecordWriter::new().write(
        SYMBOL,
        &Event::Clear {
            instrument: handle(),
            source_ts_ns: 1,
            scope: ClearScope::BothSides,
        },
        &mut stream,
    );
    // Grow the declared length by one and append a byte, so the record is
    // structurally complete and one byte too long.
    let declared = u32::from_le_bytes(stream[..4].try_into().expect("four")) + 1;
    stream[..4].copy_from_slice(&declared.to_le_bytes());
    stream.push(0);

    let error = decode(&stream, |_| Some(handle())).expect_err("one byte too many");
    assert!(matches!(error, RecordError::Malformed { .. }));
}

#[test]
fn a_tag_outside_its_range_is_malformed_and_names_what_it_was() {
    // Every enumeration this encoding carries is a closed set, and a value
    // outside one is the failure the boundary's own enums exist to prevent -
    // reached here from the outside, by a writer we do not control.
    let mut stream = Vec::new();
    RecordWriter::new().write(
        SYMBOL,
        &Event::Level {
            instrument: handle(),
            source_ts_ns: 1,
            side: Side::Bid,
            px: Scalar::fixed(4_100, PRICE_EXPONENT),
            qty: Scalar::fixed(500, QTY_EXPONENT),
            order_count: None,
            presence: Presence::New,
        },
        &mut stream,
    );

    // The side byte sits after the header, the symbol and the timestamp.
    let side_at = 4 + 1 + 1 + 2 + SYMBOL.len() + 8;
    assert_eq!(stream[side_at], 0, "the bid side, as written");
    stream[side_at] = 9;

    let error = decode(&stream, |_| Some(handle())).expect_err("nine is not a side");
    assert!(matches!(error, RecordError::Malformed { .. }));
}
