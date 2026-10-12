//! Two messages that must share a datagram, on the way out.
//!
//! The reference-data supplement forbids dividing an `InstrumentDefinition` from
//! the `StrikeInterval` that follows it: a subscriber reads the strike against
//! the definition before it in the same datagram and ignores one that stands
//! anywhere else. So the datagram that is full is finished before the pair, and
//! never between its two messages.
//!
//! The sizes are transcribed from the specifications' field tables, 130 and 40
//! bytes, and the fixtures below are those sizes and nothing else: this crate
//! cannot name the codec's own types.

mod common;

use std::sync::Arc;

use dz_edge_core::{AppMessage, EncodeError, PortRole, ResetCount};
use dz_publisher_egress::{ChannelEgress, EgressEndpoint};
use dz_publisher_metrics::{EgressErrorReason, EgressMessageType};

use common::{doc_source, metrics, sample, FakeSink, TestFeed, OFF_MSG_COUNT};

const HEADER: usize = 24;
const DEFINITION: usize = 130;
const STRIKE: usize = 40;
const TYPE_DEFINITION: u8 = 0x02;
const TYPE_STRIKE: u8 = 0x09;

/// The size and Type ID of an `InstrumentDefinition`.
struct Definition;
impl AppMessage for Definition {
    const TYPE_ID: u8 = TYPE_DEFINITION;
    const SIZE: usize = DEFINITION;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Refdata];
    fn encode_into(&self, dst: &mut [u8]) {
        dst.fill(0);
    }
    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}

/// The size and Type ID of a `StrikeInterval`, and whether it may be sent.
struct Strike {
    sendable: bool,
}
impl AppMessage for Strike {
    const TYPE_ID: u8 = TYPE_STRIKE;
    const SIZE: usize = STRIKE;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Refdata];
    fn validate(&self) -> Result<(), EncodeError> {
        if self.sendable {
            Ok(())
        } else {
            Err(EncodeError::MalformedMessage {
                message: core::any::type_name::<Self>(),
                what: "a fixture strike that may not be sent",
            })
        }
    }
    fn encode_into(&self, dst: &mut [u8]) {
        dst.fill(0);
    }
    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}

const STRIKE_OK: Strike = Strike { sendable: true };

fn egress(sink: &FakeSink) -> ChannelEgress<TestFeed, FakeSink> {
    let mut egress = ChannelEgress::<TestFeed, _>::new(
        EgressEndpoint::new(PortRole::Refdata, doc_source(), 13_001),
        sink.clone(),
        metrics(&[PortRole::Refdata], &[7]),
        ResetCount(1),
        u16::MAX,
    );
    assert!(egress.register(7));
    egress
}

fn pair(egress: &mut ChannelEgress<TestFeed, FakeSink>) {
    egress
        .push_pair(
            7,
            (&Definition, EgressMessageType::InstrumentDefinition),
            (&STRIKE_OK, EgressMessageType::StrikeInterval),
            0,
        )
        .expect("a pair fits in a fresh datagram");
}

fn bare(egress: &mut ChannelEgress<TestFeed, FakeSink>) {
    egress
        .push(7, &Definition, EgressMessageType::InstrumentDefinition, 0)
        .expect("a definition fits in a fresh datagram");
}

/// The Type ID of every message in a datagram, in order, walked by each
/// message's own Length.
fn types(datagram: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut at = HEADER;
    while at < datagram.len() {
        out.push(datagram[at]);
        at += datagram[at + 1] as usize;
    }
    assert_eq!(out.len(), datagram[OFF_MSG_COUNT] as usize);
    out
}

/// No datagram ends on a definition whose strike opens the next one, none
/// starts with a strike, and every message packed was sent once.
///
/// The definitions are counted as well as the strikes. A builder that wrote a
/// pair's definition before refusing its strike would send that definition
/// alone at the end of one datagram and the whole pair again in the next, and
/// counting strikes alone does not see it.
fn assert_no_pair_is_divided(sent: &[Vec<u8>], definitions: usize, strikes: usize) {
    let mut seen = 0;
    let mut defined = 0;
    for datagram in sent {
        let types = types(datagram);
        assert_ne!(types[0], TYPE_STRIKE, "a datagram starts with a strike");
        for (index, type_id) in types.iter().enumerate() {
            if *type_id == TYPE_DEFINITION {
                defined += 1;
            }
            if *type_id == TYPE_STRIKE {
                seen += 1;
                assert_eq!(
                    types[index - 1],
                    TYPE_DEFINITION,
                    "a strike does not follow a definition: {types:02x?}"
                );
            }
        }
    }
    assert_eq!(seen, strikes, "a strike was lost or sent twice");
    assert_eq!(defined, definitions, "a definition was lost or sent twice");
}

#[test]
fn seven_pairs_fill_a_datagram_and_the_eighth_starts_the_next() {
    // 24 + 7 x 170 is 1,214 of 1,232 bytes. The 18 left hold neither message.
    const {
        assert!(HEADER + 7 * (DEFINITION + STRIKE) == 1214);
        assert!(HEADER + 8 * (DEFINITION + STRIKE) > 1232);
    }
    let sink = FakeSink::new("refdata");
    let mut egress = egress(&sink);

    for _ in 0..8 {
        pair(&mut egress);
    }
    egress.flush(7, 0).expect("the tail datagram");

    let sent = sink.accepted();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0].len(), 1214);
    assert_eq!(types(&sent[0]), [TYPE_DEFINITION, TYPE_STRIKE].repeat(7));
    assert_eq!(types(&sent[1]), [TYPE_DEFINITION, TYPE_STRIKE]);
}

/// The case two separate pushes get wrong. After three bare definitions and
/// four pairs, 138 bytes remain: a definition fits and its strike does not.
#[test]
fn a_pair_whose_strike_would_not_fit_moves_whole_to_the_next_datagram() {
    const {
        assert!(HEADER + 3 * DEFINITION + 4 * (DEFINITION + STRIKE) == 1094);
        assert!(1232 - 1094 >= DEFINITION, "the definition alone fits");
        assert!(1232 - 1094 < DEFINITION + STRIKE, "the pair does not");
    }
    let sink = FakeSink::new("refdata");
    let mut egress = egress(&sink);

    for _ in 0..3 {
        bare(&mut egress);
    }
    for _ in 0..5 {
        pair(&mut egress);
    }
    egress.flush(7, 0).expect("the tail datagram");

    let sent = sink.accepted();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0].len(), 1094, "finished with room for a definition");
    assert_eq!(types(&sent[1]), [TYPE_DEFINITION, TYPE_STRIKE]);
    assert_no_pair_is_divided(&sent, 3 + 5, 5);
}

#[test]
fn a_mixed_set_never_ends_a_datagram_between_a_definition_and_its_strike() {
    let sink = FakeSink::new("refdata");
    let mut egress = egress(&sink);

    // Every arrangement of bare definitions and pairs up to a few datagrams'
    // worth, so that each remainder a datagram can be left with is met.
    let mut strikes = 0;
    let mut definitions = 0;
    for round in 0..40 {
        for _ in 0..(round % 4) {
            bare(&mut egress);
            definitions += 1;
        }
        for _ in 0..(1 + round % 3) {
            pair(&mut egress);
            strikes += 1;
            definitions += 1;
        }
    }
    egress.flush(7, 0).expect("the tail datagram");

    let sent = sink.accepted();
    assert!(sent.len() > 10);
    for datagram in &sent {
        assert!(datagram.len() <= 1232);
    }
    assert_no_pair_is_divided(&sent, definitions, strikes);
}

#[test]
fn a_pair_is_counted_as_its_two_messages_once_it_is_sent() {
    let sink = FakeSink::new("refdata");
    let metrics = metrics(&[PortRole::Refdata], &[7]);
    let mut egress = ChannelEgress::<TestFeed, _>::new(
        EgressEndpoint::new(PortRole::Refdata, doc_source(), 13_001),
        sink.clone(),
        Arc::clone(&metrics),
        ResetCount(1),
        u16::MAX,
    );
    assert!(egress.register(7));

    pair(&mut egress);
    pair(&mut egress);
    egress.flush(7, 0).expect("one datagram");

    for (message_type, want) in [("instrument_definition", 2), ("strike_interval", 2)] {
        assert_eq!(
            sample(
                &metrics,
                "dz_publisher_egress_messages_total",
                &[("port_role", "refdata"), ("message_type", message_type)],
            ),
            want,
            "{message_type}"
        );
    }
}

/// A definition whose strike is refused does not go out without it: a
/// subscriber would read the instrument as one that pays on no strike.
#[test]
fn a_strike_that_may_not_be_sent_takes_its_definition_with_it() {
    let sink = FakeSink::new("refdata");
    let metrics = metrics(&[PortRole::Refdata], &[7]);
    let mut egress = ChannelEgress::<TestFeed, _>::new(
        EgressEndpoint::new(PortRole::Refdata, doc_source(), 13_001),
        sink.clone(),
        Arc::clone(&metrics),
        ResetCount(1),
        u16::MAX,
    );
    assert!(egress.register(7));

    egress
        .push_pair(
            7,
            (&Definition, EgressMessageType::InstrumentDefinition),
            (
                &Strike { sendable: false },
                EgressMessageType::StrikeInterval,
            ),
            0,
        )
        .expect_err("the strike is refused");
    egress.flush(7, 0).expect("nothing to send");
    assert!(sink.accepted().is_empty(), "neither message was packed");
    assert_eq!(
        sample(
            &metrics,
            "dz_publisher_egress_errors_total",
            &[("reason", EgressErrorReason::MalformedMessage.as_str())],
        ),
        1,
        "counted once, as one refused push"
    );
}
