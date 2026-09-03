//! Route-derived egress policy: where the source address comes from, and what
//! is refused.
//!
//! Every address here is documentation-range, and no test opens a socket: the
//! kernel's own route lookup is behind [`RouteLookup`], so what is asserted is
//! the policy's decisions rather than this host's routing table.

mod common;

use std::net::Ipv4Addr;

use dz_publisher_egress::{EgressPolicy, Ipv4Prefix, PolicyError, PrefixError, DEFAULT_TTL};

use common::{doc_group, doc_source, other_doc_source, FakeRoute};

#[test]
fn the_source_address_comes_from_the_route_and_not_from_configuration() {
    // The address is a pool lease, not a host identity. A publisher that read
    // it from configuration met a tunnel address that had moved, found the
    // configured address no longer existed, and crash-looped tens of thousands
    // of times over two days.
    let policy = EgressPolicy::default();
    let route = FakeRoute::resolving(doc_source());

    assert_eq!(
        policy
            .resolve_source(doc_group(13_000), &route)
            .expect("the route resolves"),
        doc_source()
    );
    assert_eq!(policy.ttl, DEFAULT_TTL);
    assert_eq!(DEFAULT_TTL, 1, "one hop, as the configuration default says");
}

#[test]
fn no_route_to_the_group_is_a_startup_failure() {
    // Not something to retry per datagram: a publisher with no route sends
    // nothing, and it must say so at startup rather than count a socket error
    // per tick forever.
    let policy = EgressPolicy::default();
    let route = FakeRoute::unreachable();

    assert!(matches!(
        policy.resolve_source(doc_group(13_000), &route),
        Err(PolicyError::NoRoute { .. })
    ));
}

#[test]
fn a_route_resolving_the_wildcard_address_is_refused() {
    // The wildcard is what an unrouted socket reports, and binding to it hands
    // the source-address choice back to the kernel per datagram. The channel
    // instance a subscriber tracks is keyed on the source address, so a
    // publisher whose datagrams change source mid-run is read as two
    // publishers alternating, each seeing the other's gaps.
    let policy = EgressPolicy::default();
    let route = FakeRoute::resolving(Ipv4Addr::UNSPECIFIED);

    assert!(matches!(
        policy.resolve_source(doc_group(13_000), &route),
        Err(PolicyError::Unspecified { .. })
    ));
}

#[test]
fn a_discovered_address_outside_the_expected_prefix_is_refused() {
    // The invariant an operator asked for, doing its job. A source address
    // from the wrong interface produces datagrams that are well formed, carry
    // a dense series, and are read by every subscriber as a *different channel
    // instance* from the one they were told to expect — which is a silent
    // failure, and the reason the check exists at all.
    let policy = EgressPolicy {
        pin: None,
        expected_prefix: Some(Ipv4Prefix::parse("192.0.2.0/24").expect("a prefix")),
        ttl: DEFAULT_TTL,
    };
    let route = FakeRoute::resolving(other_doc_source());

    let error = policy
        .resolve_source(doc_group(13_000), &route)
        .expect_err("the discovered address is in another range");
    assert!(
        matches!(error, PolicyError::OutsideExpectedPrefix { found, .. } if found == other_doc_source()),
        "got {error:?}"
    );
}

#[test]
fn a_discovered_address_inside_the_expected_prefix_is_accepted() {
    let policy = EgressPolicy {
        pin: None,
        expected_prefix: Some(Ipv4Prefix::parse("192.0.2.0/24").expect("a prefix")),
        ttl: DEFAULT_TTL,
    };
    let route = FakeRoute::resolving(doc_source());

    assert_eq!(
        policy
            .resolve_source(doc_group(13_000), &route)
            .expect("inside the prefix"),
        doc_source()
    );
}

#[test]
fn a_pin_overrides_route_discovery() {
    // The escape hatch for a host where discovery is wrong. It is not the
    // normal path, and it is why the key exists rather than the address being
    // read from configuration in the first place.
    let policy = EgressPolicy {
        pin: Some(doc_source()),
        expected_prefix: None,
        ttl: DEFAULT_TTL,
    };
    let route = FakeRoute::unreachable();

    assert_eq!(
        policy
            .resolve_source(doc_group(13_000), &route)
            .expect("the pin is not asked of the route"),
        doc_source()
    );
}

#[test]
fn a_pinned_address_is_still_held_to_the_expected_prefix() {
    // An operator who pins an address outside the prefix they declared has
    // contradicted themselves in one file, and a startup is the cheapest place
    // to find that out.
    let policy = EgressPolicy {
        pin: Some(other_doc_source()),
        expected_prefix: Some(Ipv4Prefix::parse("192.0.2.0/24").expect("a prefix")),
        ttl: DEFAULT_TTL,
    };
    let route = FakeRoute::resolving(doc_source());

    assert!(matches!(
        policy.resolve_source(doc_group(13_000), &route),
        Err(PolicyError::OutsideExpectedPrefix { .. })
    ));
}

#[test]
fn a_prefix_masks_the_host_bits_it_was_written_with() {
    // An operator who writes the tunnel's own address with the pool's prefix
    // length has said exactly what they meant, and refusing it would be
    // pedantry that costs a startup.
    let written_with_host_bits = Ipv4Prefix::parse("192.0.2.10/24").expect("a prefix");

    assert_eq!(written_with_host_bits.to_string(), "192.0.2.0/24");
    assert!(written_with_host_bits.contains(doc_source()));
    assert!(!written_with_host_bits.contains(other_doc_source()));
}

#[test]
fn the_widest_and_narrowest_prefixes_both_work() {
    // `/0` needs a special case, because shifting a `u32` by 32 is undefined.
    let everything = Ipv4Prefix::parse("192.0.2.0/0").expect("a prefix");
    assert!(everything.contains(other_doc_source()));

    let one_host = Ipv4Prefix::parse("192.0.2.10/32").expect("a prefix");
    assert!(one_host.contains(doc_source()));
    assert!(!one_host.contains(Ipv4Addr::new(192, 0, 2, 11)));
}

#[test]
fn a_prefix_that_is_not_one_is_refused_rather_than_defaulted() {
    // A misread invariant that silently becomes "any address" is worse than no
    // invariant: an operator believes the check is running.
    assert_eq!(Ipv4Prefix::parse("192.0.2.0"), Err(PrefixError::NoLength));
    assert_eq!(
        Ipv4Prefix::parse("not-an-address/24"),
        Err(PrefixError::NotAnAddress)
    );
    assert_eq!(
        Ipv4Prefix::parse("192.0.2.0/33"),
        Err(PrefixError::NotALength)
    );
    assert_eq!(
        Ipv4Prefix::parse("192.0.2.0/x"),
        Err(PrefixError::NotALength)
    );
}
