//! `Venue Update ID`: the event states the venue's counter whole, and the
//! lowering writes its low 32 bits, or zero when there is none.

use dz_adapter_core::{InstrumentRef, Scalar, SideUpdate};
use dz_edge_core::AppMessage;
use dz_edge_tob::Quote;
use dz_publisher_lowering::{Instrument, InstrumentTable, Lowering, SourceId};

fn table() -> InstrumentTable {
    let mut instruments = InstrumentTable::new();
    instruments.admit(Instrument {
        instrument_id: 41,
        price_exponent: -4,
        qty_exponent: -2,
        quoted_per_contract: None,
    });
    instruments
}

fn lowered(venue_update_id: Option<u64>) -> Quote {
    Lowering::new(SourceId::new(7).expect("7 is in the assigned range"))
        .lower_quote(
            &table(),
            InstrumentRef::from_admission(0),
            0,
            SideUpdate::Present {
                px: Scalar::text("0.41"),
                qty: Scalar::text("5"),
                source_count: None,
            },
            SideUpdate::Gone,
            venue_update_id,
        )
        .expect("the table's instrument and exact decimals")
}

#[test]
fn a_quote_without_a_counter_writes_zero() {
    assert_eq!(lowered(None).venue_update_id, 0);
}

#[test]
fn the_wire_carries_the_counters_low_32_bits() {
    // (counter the venue stated, what the wire carries)
    let cases = [
        (1, 1),
        (u64::from(u32::MAX), u32::MAX),
        // Above 2^32: the high bits go, the low bits stay.
        ((1 << 32) + 5, 5),
        (0xDEAD_BEEF_0000_0007, 7),
    ];
    for (counter, wire) in cases {
        assert_eq!(lowered(Some(counter)).venue_update_id, wire, "{counter:#x}");
    }
}

#[test]
fn a_counter_whose_low_bits_are_zero_is_written_as_zero_and_not_replaced() {
    // The specification requires zero here, read as *unavailable*. Any other
    // value is the identifier of a real update, which a subscriber would then
    // drop as a copy.
    assert_eq!(lowered(Some(1 << 32)).venue_update_id, 0);
}

#[test]
fn the_field_reaches_the_bytes_at_offset_56() {
    let mut b = [0u8; Quote::SIZE];
    lowered(Some((7 << 32) | 0x0102_0304)).encode_into(&mut b);
    assert_eq!(&b[56..60], &0x0102_0304u32.to_le_bytes());
}
