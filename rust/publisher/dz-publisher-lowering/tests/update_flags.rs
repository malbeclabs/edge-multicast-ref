//! The flags byte, as a table, because a byte derived by an `if` is a byte one
//! reading of the code can get wrong in a way no round-trip test can see.
//!
//! The correspondence is transcribed from the two live publishers rather than
//! computed from the same expression the lowering uses: the specification fixes
//! the four bit positions and settles nothing about how they pair, so what the
//! shipped encoders do is the only authority. A table generated from the code
//! it checks agrees with it by construction and proves nothing.

use dz_adapter_core::{Scalar, SideUpdate};
use dz_edge_tob::{QUOTE_ASK_GONE, QUOTE_ASK_UPDATED, QUOTE_BID_GONE, QUOTE_BID_UPDATED};
use dz_publisher_lowering::{Instrument, InstrumentTable, Lowering};

/// A side that rests somewhere, in the venue's own decimal.
fn present(px: &str) -> SideUpdate<'_> {
    SideUpdate::Present {
        px: Scalar::text(px),
        qty: Scalar::text("5"),
        source_count: None,
    }
}

fn table() -> InstrumentTable {
    let mut instruments = InstrumentTable::new();
    instruments.admit(Instrument {
        instrument_id: 41,
        price_exponent: -4,
        qty_exponent: -2,
    });
    instruments
}

#[test]
fn every_pair_of_sides_produces_the_byte_the_live_publishers_produce() {
    let instruments = table();
    let lowering = Lowering::new(&instruments, source_id());
    let instrument = dz_adapter_core::InstrumentRef::from_admission(0);

    // | book state    | update_flags                  |
    // |---------------|-------------------------------|
    // | both present  | BID_UPDATED | ASK_UPDATED     |
    // | bid only      | BID_UPDATED | ASK_GONE        |
    // | ask only      | BID_GONE    | ASK_UPDATED     |
    // | both gone     | BID_GONE    | ASK_GONE        |
    let cases = [
        (
            "both present",
            present("0.41"),
            present("0.43"),
            QUOTE_BID_UPDATED | QUOTE_ASK_UPDATED,
        ),
        (
            "bid only",
            present("0.41"),
            SideUpdate::Gone,
            QUOTE_BID_UPDATED | QUOTE_ASK_GONE,
        ),
        (
            "ask only",
            SideUpdate::Gone,
            present("0.43"),
            QUOTE_BID_GONE | QUOTE_ASK_UPDATED,
        ),
        (
            "both gone",
            SideUpdate::Gone,
            SideUpdate::Gone,
            QUOTE_BID_GONE | QUOTE_ASK_GONE,
        ),
    ];

    for (state, bid, ask, expected) in cases {
        let quote = lowering
            .lower_quote(instrument, 7, bid, ask)
            .expect("the table's instrument and exact decimals");
        assert_eq!(
            quote.update_flags, expected,
            "{state}: expected {expected:#04x}, got {:#04x}",
            quote.update_flags
        );
    }
}

#[test]
fn an_encoder_writing_both_updated_bits_is_wrong_for_three_of_the_four() {
    // The constant a publisher's other encoder writes on every quote. Under
    // presence semantics it is the correct byte for a two-sided book and wrong
    // for every book that is missing a side, which is why that encoder is only
    // safe while its book refuses to be one-sided — a property of its caller
    // rather than of the byte.
    let unconditional = QUOTE_BID_UPDATED | QUOTE_ASK_UPDATED;

    let instruments = table();
    let lowering = Lowering::new(&instruments, source_id());
    let instrument = dz_adapter_core::InstrumentRef::from_admission(0);

    let one_sided_or_empty = [
        (present("0.41"), SideUpdate::Gone),
        (SideUpdate::Gone, present("0.43")),
        (SideUpdate::Gone, SideUpdate::Gone),
    ];

    for (bid, ask) in one_sided_or_empty {
        let quote = lowering
            .lower_quote(instrument, 7, bid, ask)
            .expect("lowers");
        assert_ne!(
            quote.update_flags, unconditional,
            "a book missing a side must not report both sides updated"
        );
    }

    let two_sided = lowering
        .lower_quote(instrument, 7, present("0.41"), present("0.43"))
        .expect("lowers");
    assert_eq!(two_sided.update_flags, unconditional);
}

#[test]
fn a_side_never_sets_both_of_its_bits() {
    // The property the pairing rests on, asserted per side rather than per
    // byte: whatever a side is, exactly one of its two bits is set. A
    // derivation that ever set both would make a gone side indistinguishable
    // from a quoted one for any subscriber testing a single bit.
    let instruments = table();
    let lowering = Lowering::new(&instruments, source_id());
    let instrument = dz_adapter_core::InstrumentRef::from_admission(0);

    for bid in [present("0.41"), SideUpdate::Gone] {
        for ask in [present("0.43"), SideUpdate::Gone] {
            let flags = lowering
                .lower_quote(instrument, 7, bid, ask)
                .expect("lowers")
                .update_flags;

            assert_eq!(
                (flags & QUOTE_BID_UPDATED != 0) as u8 + (flags & QUOTE_BID_GONE != 0) as u8,
                1,
                "bid set {flags:#04x}"
            );
            assert_eq!(
                (flags & QUOTE_ASK_UPDATED != 0) as u8 + (flags & QUOTE_ASK_GONE != 0) as u8,
                1,
                "ask set {flags:#04x}"
            );
        }
    }
}

#[test]
fn no_pair_of_sides_can_produce_an_empty_flags_byte() {
    // **The one thing the specification's conformance subscriber grades a
    // violation on this byte.** Its `TOB.QUOTE.UPDATE_FLAGS_COHERENCE` fires
    // when bits 0-3 are all zero — "a quote that claims nothing changed" — and
    // the same rule's implementation states that it deliberately does *not*
    // couple the gone bit to the updated bit, because either pairing occurs on
    // conformant publishers.
    //
    // So the convention this lowering follows is a free choice, and this is the
    // part that is not: every quote must flag something. Two cases per side is
    // what makes it unreachable — a side is present or it is gone, and each
    // sets a bit. A third case meaning "unchanged" would have set none, and a
    // quote with both sides unchanged would have been a violation on the wire.
    let instruments = table();
    let lowering = Lowering::new(&instruments, source_id());
    let instrument = dz_adapter_core::InstrumentRef::from_admission(0);

    for bid in [present("0.41"), SideUpdate::Gone] {
        for ask in [present("0.43"), SideUpdate::Gone] {
            let flags = lowering
                .lower_quote(instrument, 7, bid, ask)
                .expect("lowers")
                .update_flags;
            assert_ne!(
                flags & 0x0F,
                0,
                "a quote that flags nothing is a conformance violation"
            );
        }
    }
}

#[test]
fn a_gone_side_is_zeroed_and_only_the_flag_says_so() {
    // Zero is an in-range price on the wire, so the zeros a gone side carries
    // mean nothing on their own. This is the assertion that makes the flag
    // load-bearing rather than decorative, and it is why an adapter must not be
    // able to write one.
    //
    // The price half is a **must**: the conformance subscriber's
    // `TOB.QUOTE.GONE_VS_ZERO_PRICE` refuses a gone side carrying a non-zero
    // price. The quantity half is not mandated by anything - the same rule's
    // implementation says so - and is written anyway, because both existing
    // publishers write it and a subscriber reading size without checking the
    // flag is better served by a zero than by a stale number.
    let instruments = table();
    let lowering = Lowering::new(&instruments, source_id());
    let instrument = dz_adapter_core::InstrumentRef::from_admission(0);

    let quote = lowering
        .lower_quote(instrument, 7, SideUpdate::Gone, present("0.43"))
        .expect("lowers");

    assert_eq!(quote.bid_price, 0);
    assert_eq!(quote.bid_qty, 0);
    assert_eq!(quote.bid_source_count, 0);
    assert_ne!(quote.update_flags & QUOTE_BID_GONE, 0);

    // And a real bid at zero is byte-identical apart from that one bit, which
    // is the whole hazard in one assertion.
    let at_zero = lowering
        .lower_quote(instrument, 7, present("0.0000"), present("0.43"))
        .expect("lowers");
    assert_eq!(at_zero.bid_price, quote.bid_price);
    assert_eq!(
        at_zero.update_flags ^ quote.update_flags,
        QUOTE_BID_UPDATED | QUOTE_BID_GONE,
        "the flag is the only thing separating a gone side from a bid at zero"
    );
}

#[test]
fn a_venues_source_count_reaches_the_wire_and_its_absence_is_zero() {
    let instruments = table();
    let lowering = Lowering::new(&instruments, source_id());
    let instrument = dz_adapter_core::InstrumentRef::from_admission(0);

    let counted = lowering
        .lower_quote(
            instrument,
            7,
            SideUpdate::Present {
                px: Scalar::text("0.41"),
                qty: Scalar::text("5"),
                source_count: Some(3),
            },
            present("0.43"),
        )
        .expect("lowers");

    assert_eq!(counted.bid_source_count, 3);
    // The specification's sentinel for this field is zero itself, so a venue
    // that does not expose it is truthfully unavailable rather than claiming
    // none. Neither existing publisher exposes it on top-of-book.
    assert_eq!(counted.ask_source_count, 0);
}

/// An assigned production id, which is what a publisher runs under. Zero would
/// be refused by the type - see `tests/source_id.rs`.
fn source_id() -> dz_publisher_lowering::SourceId {
    dz_publisher_lowering::SourceId::new(7).expect("7 is in the assigned range")
}
