//! `0x03 Midpoint`: a VWAP, with the quality flags derived from the event.

use std::num::NonZeroU32;

use dz_adapter_core::{InstrumentRef, Scalar};
use dz_edge_core::{ChannelSequence, DatagramBuilder, PortRole, ResetCount, MAX_DATAGRAM_SIZE};
use dz_edge_midpoint::{Midpoint, MidpointFeed, METHOD_VWAP, QUALITY_STALE, QUALITY_SYNTHETIC};
use dz_publisher_lowering::{
    Instrument, InstrumentTable, LoweringError, MidpointLowering, SourceId,
};

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

fn lowering() -> MidpointLowering {
    MidpointLowering::new(SourceId::new(7).expect("7 is in the assigned range"))
}

fn vwap(px: &str, volume: &str, stale: bool) -> Result<Midpoint, LoweringError> {
    lowering().lower_vwap(
        &table(),
        InstrumentRef::from_admission(0),
        1_700_000_000_000_000_000,
        NonZeroU32::new(1_000).expect("not zero"),
        1_700_000_000_000_000_500,
        Scalar::text(px),
        Scalar::text(volume),
        stale,
    )
}

#[test]
fn a_vwap_lowers_its_price_window_volume_and_identity() {
    let m = vwap("0.41", "5", false).expect("lowers");

    assert_eq!(m.instrument_id, 41);
    assert_eq!(m.source_id, 7);
    assert_eq!(m.method, METHOD_VWAP);
    assert_eq!(m.quality_flags, 0);
    assert_eq!(m.input_timestamp_ns, 1_700_000_000_000_000_000);
    assert_eq!(m.compute_timestamp_ns, 1_700_000_000_000_000_500);
    assert_eq!(m.price, 4_100);
    assert_eq!(m.window_ms, 1_000);
    assert_eq!(m.volume, 500);
    assert_eq!(m.check(), Ok(()));
}

#[test]
fn an_empty_window_is_synthetic() {
    // The specification requires the bit for an empty window. Every spelling
    // of zero is an empty window.
    for zero in ["0", "0.00", "0.0000"] {
        let m = vwap("0.41", zero, false).expect("lowers");
        assert_eq!(m.volume, 0);
        assert_eq!(m.quality_flags, QUALITY_SYNTHETIC, "volume {zero}");
        assert_eq!(m.check(), Ok(()));
    }
}

#[test]
fn stale_is_the_adapters_and_independent_of_synthetic() {
    assert_eq!(
        vwap("0.41", "5", true).expect("lowers").quality_flags,
        QUALITY_STALE
    );
    assert_eq!(
        vwap("0.41", "0", true).expect("lowers").quality_flags,
        QUALITY_STALE | QUALITY_SYNTHETIC
    );
}

#[test]
fn every_lowered_vwap_is_one_the_builder_accepts() {
    let mut builder = DatagramBuilder::<MidpointFeed>::new(
        ChannelSequence::new(0, ResetCount::NEVER_RESET),
        PortRole::Mktdata,
        MAX_DATAGRAM_SIZE as u16,
    );
    for (volume, stale) in [("5", false), ("5", true), ("0", false), ("0", true)] {
        let m = vwap("0.41", volume, stale).expect("lowers");
        builder.push(&m).expect("a lowered VWAP passes the push");
    }
}

#[test]
fn a_value_the_exponent_cannot_state_is_refused_naming_its_field() {
    let price = vwap("0.41005", "5", false).expect_err("a fifth decimal place at -4");
    assert_eq!(price.reason(), "too_precise");
    assert_eq!(price.field(), Some("price"));

    let volume = vwap("0.41", "5.001", false).expect_err("a third decimal place at -2");
    assert_eq!(volume.reason(), "too_precise");
    assert_eq!(volume.field(), Some("volume"));

    let negative = vwap("0.41", "-5", false).expect_err("a negative volume");
    assert_eq!(negative.reason(), "malformed");
    assert_eq!(negative.field(), Some("volume"));
}

#[test]
fn an_unknown_instrument_is_refused() {
    let refused = lowering().lower_vwap(
        &table(),
        InstrumentRef::from_admission(9),
        1,
        NonZeroU32::new(1_000).expect("not zero"),
        2,
        Scalar::text("0.41"),
        Scalar::text("5"),
        false,
    );
    assert_eq!(refused, Err(LoweringError::UnknownInstrument));
}
