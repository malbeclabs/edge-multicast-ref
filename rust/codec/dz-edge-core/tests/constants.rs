use dz_edge_core as core;

#[test]
fn wire_constants_match_the_spec() {
    assert_eq!(
        core::MAGIC_TOB,
        0x445A,
        "\"DZ\", top-of-book datagram delimiter"
    );
    assert_eq!(core::SCHEMA_VERSION, 3, "publishers emit schema 3 only");
    assert_eq!(core::DATAGRAM_HEADER_SIZE, 24);
    assert_eq!(core::MSG_HEADER_SIZE, 4);
    assert_eq!(core::MAX_DATAGRAM_SIZE, 1232, "mandated for GRE headroom");
}

#[test]
fn schema_two_is_not_accepted() {
    // The 128-byte InstrumentDefinition was superseded before any publisher
    // emitted it. Accepting it would invent a generation that never existed.
    assert_eq!(core::SUPPORTED_SCHEMA_VERSIONS, [3, 1]);
    assert!(!core::SUPPORTED_SCHEMA_VERSIONS.contains(&2));
}

#[test]
fn shared_payload_sizes_match_the_spec() {
    assert_eq!(core::SIZE_HEARTBEAT, 16);
    assert_eq!(core::SIZE_END_OF_SESSION, 12);
    assert_eq!(core::TYPE_HEARTBEAT, 0x01);
    assert_eq!(core::TYPE_END_OF_SESSION, 0x06);
}
