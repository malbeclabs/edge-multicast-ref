//! `[[feed]] spec`: a closed set, resolved by the codec's own names.
//!
//! Three lists have to agree and each is written out separately, because a list
//! derived from another is not a control on it: the enumeration, the tokens, and
//! the port roles. Each of these tests is what fails when one of the three is
//! added to without the others.

use dz_edge_core::{AppMessage, EndOfSession, Feed as WireFeed, Heartbeat, PortRole};
use dz_edge_mbp::{
    BookClear, LevelUpdate, MarketByPrice, SnapshotBegin, SnapshotEnd, SnapshotLevel,
};
use dz_edge_refdata::{InstrumentDefinition, ManifestSummary};
use dz_edge_tob::{Quote, TopOfBook, Trade};
use dz_publisher_runtime::{EmittedFeed, FeedSpec};

#[test]
fn every_token_is_the_codec_crates_own_feed_name() {
    // The tokens are not literals in this crate. A configuration names a feed
    // by the name the crate that implements it gives it, so the two cannot
    // drift — and a feed renamed upstream renames the key rather than silently
    // becoming unresolvable.
    assert_eq!(FeedSpec::TopOfBook.as_str(), <TopOfBook as WireFeed>::NAME);
    assert_eq!(
        FeedSpec::MarketByPrice.as_str(),
        <MarketByPrice as WireFeed>::NAME
    );
}

#[test]
fn every_token_resolves_to_the_specification_that_spells_it() {
    for spec in FeedSpec::ALL {
        assert_eq!(
            FeedSpec::resolve(spec.as_str()).expect("its own token resolves"),
            spec
        );
    }
}

#[test]
fn no_two_specifications_share_a_token() {
    let mut tokens: Vec<&str> = FeedSpec::ALL.iter().map(|spec| spec.as_str()).collect();
    tokens.sort_unstable();
    let count = tokens.len();
    tokens.dedup();
    assert_eq!(tokens.len(), count, "two specifications share a token");
}

#[test]
fn the_supported_list_is_the_specification_set() {
    // The list is a literal so that it can be a `&'static str` in an error
    // format string. This is what keeps it from drifting from `ALL`.
    let built: Vec<&str> = FeedSpec::ALL.iter().map(|spec| spec.as_str()).collect();
    assert_eq!(FeedSpec::SUPPORTED, built.join(", "));
}

#[test]
fn the_type_level_specification_agrees_with_the_value() {
    // `EmittedFeed::SPEC` is what the routing reads, and it exists so the feed
    // and the specification cannot disagree — the codec will not stop a `Quote`
    // being pushed into a market-by-price datagram, because
    // `DatagramBuilder::push` checks `PORT_ROLES` and nothing checks feed
    // membership. So the association is asserted rather than assumed.
    assert_eq!(<TopOfBook as EmittedFeed>::SPEC, FeedSpec::TopOfBook);
    assert_eq!(
        <MarketByPrice as EmittedFeed>::SPEC,
        FeedSpec::MarketByPrice
    );
    assert_eq!(
        <TopOfBook as EmittedFeed>::SPEC.as_str(),
        <TopOfBook as WireFeed>::NAME
    );
    assert_eq!(
        <MarketByPrice as EmittedFeed>::SPEC.as_str(),
        <MarketByPrice as WireFeed>::NAME
    );
}

#[test]
fn each_specifications_port_roles_are_the_union_its_message_types_declare() {
    // Derived here from `AppMessage::PORT_ROLES` — the codec's own transcription
    // of the specifications' message tables — and compared against the list
    // this crate hands the metrics registry. That list decides which child
    // series are pre-created, so an omission leaves a panel blank until the
    // first datagram and an extra asserts a channel that does not exist.
    fn union(roles: &[&[PortRole]]) -> Vec<PortRole> {
        let mut out = Vec::new();
        for role in roles.iter().flat_map(|set| set.iter()) {
            if !out.contains(role) {
                out.push(*role);
            }
        }
        out
    }

    // The family's own messages plus the ones this feed defines. Every message
    // type the runtime can push on each feed is named.
    let top_of_book = union(&[
        Heartbeat::PORT_ROLES,
        EndOfSession::PORT_ROLES,
        Quote::PORT_ROLES,
        Trade::PORT_ROLES,
        InstrumentDefinition::PORT_ROLES,
        ManifestSummary::PORT_ROLES,
    ]);
    assert_eq!(FeedSpec::TopOfBook.port_roles(), top_of_book);

    let market_by_price = union(&[
        Heartbeat::PORT_ROLES,
        EndOfSession::PORT_ROLES,
        Trade::PORT_ROLES,
        LevelUpdate::PORT_ROLES,
        BookClear::PORT_ROLES,
        InstrumentDefinition::PORT_ROLES,
        ManifestSummary::PORT_ROLES,
        SnapshotBegin::PORT_ROLES,
        SnapshotLevel::PORT_ROLES,
        SnapshotEnd::PORT_ROLES,
    ]);
    assert_eq!(FeedSpec::MarketByPrice.port_roles(), market_by_price);
}

#[test]
fn a_specification_carries_a_snapshot_port_role_exactly_when_it_lists_one() {
    // The predicate the configuration check reads, held to the port-role list
    // rather than written twice.
    for spec in FeedSpec::ALL {
        assert_eq!(
            spec.has_snapshot_port(),
            spec.port_roles().contains(&PortRole::Snapshot),
            "{} disagrees with itself about the snapshot port role",
            spec.as_str()
        );
    }
}
