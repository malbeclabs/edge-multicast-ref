//! The registry's reserved ranges, as a type.
//!
//! Transcribed by hand from the source registry's own table rather than derived
//! from the constructor it checks. The specification's conformance subscriber
//! grades `source_id == 0` a **must** and refuses it unconditionally, so a
//! publisher that reached the wire with one would fail conformance on every
//! quote and every trade it ever sent.

use dz_publisher_lowering::SourceId;

#[test]
fn zero_is_refused_because_the_wire_forbids_it() {
    // "Reserved. MUST NOT be used on the wire." It is also the value a
    // configuration key that was never set hands you, which is why refusing it
    // in the type matters more than documenting it.
    assert_eq!(SourceId::new(0), None);
}

#[test]
fn the_assigned_production_range_is_admitted() {
    for value in [1, 2, 500, 1023] {
        assert!(
            SourceId::new(value).is_some(),
            "{value} is an assigned production id"
        );
        assert_eq!(SourceId::new(value).map(SourceId::get), Some(value));
    }
}

#[test]
fn the_range_reserved_for_future_assignment_is_refused() {
    // A publisher using one of these is claiming an identity nobody assigned.
    // The conformance subscriber defers this half of the check to a registry it
    // may not have been given; the range itself is stated unconditionally, so it
    // is checkable without one.
    for value in [1024, 20_000, 32_767] {
        assert_eq!(SourceId::new(value), None, "{value} is not assigned");
    }
}

#[test]
fn the_private_range_is_admitted_because_publishers_may_use_it() {
    for value in [32_768, 50_000, 65_535] {
        assert!(
            SourceId::new(value).is_some(),
            "{value} is private or experimental"
        );
    }
}

#[test]
fn the_boundaries_are_where_the_table_puts_them() {
    // Written as the four transitions rather than as four memberships, because
    // an off-by-one is what this test exists to catch and a membership list
    // read from the same mental model as the code would share it.
    assert_eq!(SourceId::new(0), None);
    assert!(SourceId::new(1).is_some());
    assert!(SourceId::new(1023).is_some());
    assert_eq!(SourceId::new(1024), None);
    assert_eq!(SourceId::new(32_767), None);
    assert!(SourceId::new(32_768).is_some());
}
