use dz_edge_core::PortRole;

#[test]
fn as_str_matches_the_spec_tokens() {
    assert_eq!(PortRole::Mktdata.as_str(), "mktdata");
    assert_eq!(PortRole::Refdata.as_str(), "refdata");
    assert_eq!(PortRole::Snapshot.as_str(), "snapshot");
}
