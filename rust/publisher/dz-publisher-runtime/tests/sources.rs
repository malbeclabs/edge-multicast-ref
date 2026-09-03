//! `[[source]]`: a feed with more than one upstream, and which one publishes.
//!
//! A venue often carries the same book twice by different paths — a websocket
//! and a FIX session, a local socket and a remote stream, two validators of one
//! chain. They are not the same stream: conflation differs, per-connection
//! sequencing differs, and each arrives at its own moment. So which one a
//! publisher publishes from is a decision, and every test here is about that
//! decision being stated in the file rather than implied by which binary ran.

mod harness;

use dz_adapter_core::ConnectionId;
use dz_ingress_core::Kind;
use dz_publisher_runtime::{
    AdapterContext, AdapterRegistry, Document, SourceRole, StartupError, Venue,
};
use harness::Doc;

/// A `[[source]]` block, with only the keys a test varies stated.
fn source(name: &str, ingress: &str, role: &str, carries: &str) -> String {
    let mut block = format!("[[source]]\nname = \"{name}\"\ningress = \"{ingress}\"\n");
    if !role.is_empty() {
        block.push_str(&format!("role = \"{role}\"\n"));
    }
    if !carries.is_empty() {
        block.push_str(&format!("carries = [{carries}]\n"));
    }
    block
}

/// The document with `[ingress] kind` removed, which is what a multi-source
/// document must not carry.
fn ingress_policy_only() -> String {
    "[ingress]\nconnect_timeout = \"5s\"\n".to_owned()
}

fn with_sources(sources: &str) -> Doc {
    Doc::valid()
        .ingress(ingress_policy_only())
        .adapter(format!("{}\n{sources}", Doc::valid().adapter))
}

// ---------------------------------------------------------------------------
// The array itself
// ---------------------------------------------------------------------------

#[test]
fn a_document_with_no_sources_is_the_publisher_with_one_upstream() {
    // The shape every document had before the array existed, and still the
    // ordinary one: `[ingress] kind` names the transport, the venue builds it,
    // and the connection's name is the venue's own.
    let config = Document::parse(&Doc::valid().render())
        .expect("valid")
        .resolve()
        .expect("resolvable");

    assert!(config.sources.is_empty());
    assert_eq!(config.ingress_kind, Some(Kind::Uds));
}

#[test]
fn two_sources_resolve_with_their_transports_names_and_roles() {
    let doc = with_sources(&format!(
        "{}\n{}",
        source("ws", "uds", "primary", ""),
        source("fix", "uds", "comparison", "")
    ));
    let config = Document::parse(&doc.render())
        .expect("valid")
        .resolve()
        .expect("resolvable");

    assert_eq!(config.sources.len(), 2);
    assert_eq!(config.sources[0].connection.as_str(), "ws");
    assert_eq!(config.sources[0].role, SourceRole::Primary);
    assert_eq!(config.sources[1].connection.as_str(), "fix");
    assert_eq!(config.sources[1].role, SourceRole::Comparison);
    // The transport is named per source now, so there is no document-level
    // answer to give.
    assert_eq!(config.ingress_kind, None);
}

#[test]
fn the_transport_named_in_both_places_is_refused() {
    // A key that is read only when another is absent is a key an operator
    // cannot reason about from the file in front of them.
    let doc = Doc::valid().adapter(format!(
        "{}\n{}",
        Doc::valid().adapter,
        source("ws", "uds", "primary", "")
    ));
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();

    let message = error.to_string();
    assert!(message.contains("named twice"), "{message}");
}

#[test]
fn a_transport_named_nowhere_is_refused_naming_both_places() {
    let error = Document::parse(&Doc::valid().ingress(ingress_policy_only()).render())
        .expect("parses")
        .resolve()
        .unwrap_err();

    let message = error.to_string();
    assert!(message.contains("[ingress] kind"), "{message}");
    assert!(message.contains("[[source]] ingress"), "{message}");
}

// ---------------------------------------------------------------------------
// The rule the array exists to make checkable
// ---------------------------------------------------------------------------

#[test]
fn a_feed_with_two_primaries_is_refused() {
    // **Two publishers' worth of events on one channel instance.** The
    // `Sequence Number` series is per channel instance, so a subscriber's gap
    // detection reads the two interleaved as its own losses and cannot tell
    // which. This is the one rule that has to be a startup error.
    let doc = with_sources(&format!(
        "{}\n{}",
        source("ws", "uds", "primary", ""),
        source("fix", "uds", "primary", "")
    ));
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();

    match error {
        StartupError::FeedPrimaries { spec, primaries } => {
            assert_eq!(spec, "top-of-book");
            // Both are named: the operator has to know which two blocks are in
            // conflict.
            assert!(primaries.contains("ws"), "{primaries}");
            assert!(primaries.contains("fix"), "{primaries}");
        }
        other => panic!("expected a primaries error, got {other}"),
    }
}

#[test]
fn a_feed_with_no_primary_is_refused() {
    // A feed whose block is enabled and whose data has no path to the wire is a
    // publisher heartbeating a channel it never fills.
    let doc = with_sources(&source("fix", "uds", "comparison", ""));
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();

    match error {
        StartupError::FeedPrimaries { spec, primaries } => {
            assert_eq!(spec, "top-of-book");
            assert_eq!(primaries, "none");
        }
        other => panic!("expected a primaries error, got {other}"),
    }
}

#[test]
fn the_primary_is_per_feed_and_not_per_publisher() {
    // Two feeds, each with its own primary, is not a conflict: `carries` is what
    // says which sources are alternatives for the same data, and these two are
    // not alternatives at all.
    let doc = Doc::valid()
        .feed(format!(
            "{}\n{}",
            Doc::valid().feed,
            Doc::depth_feed_block()
        ))
        .ingress(ingress_policy_only())
        .adapter(format!(
            "{}\n{}\n{}",
            Doc::valid().adapter,
            source("ws", "uds", "primary", "\"top-of-book\""),
            source("fix", "uds", "primary", "\"market-by-price\"")
        ));
    let config = Document::parse(&doc.render())
        .expect("valid")
        .resolve()
        .expect("each feed has exactly one primary");

    assert_eq!(config.sources.len(), 2);
    let ws = &config.sources[0];
    assert!(ws.carries(dz_publisher_runtime::FeedSpec::TopOfBook));
    assert!(!ws.carries(dz_publisher_runtime::FeedSpec::MarketByPrice));
}

#[test]
fn a_source_with_no_carries_carries_every_feed() {
    // The single-source case, and the reason `carries` is defaultable: a
    // publisher whose one upstream feeds both its feeds should not have to
    // enumerate them.
    let doc = with_sources(&source("ws", "uds", "primary", ""));
    let config = Document::parse(&doc.render())
        .expect("valid")
        .resolve()
        .expect("resolvable");

    let ws = &config.sources[0];
    assert!(ws.carries.is_empty());
    assert!(ws.carries(dz_publisher_runtime::FeedSpec::TopOfBook));
    assert!(ws.carries(dz_publisher_runtime::FeedSpec::MarketByPrice));
}

#[test]
fn primary_is_the_default_role() {
    // A publisher with one source states a transport and nothing else, and the
    // role it gets is the one that publishes. The alternative default -
    // `comparison` - would be a publisher that came up and published nothing.
    let doc = with_sources(&source("ws", "uds", "", ""));
    let config = Document::parse(&doc.render())
        .expect("valid")
        .resolve()
        .expect("resolvable");

    assert_eq!(config.sources[0].role, SourceRole::Primary);
}

// ---------------------------------------------------------------------------
// Everything a document can say wrongly about a source
// ---------------------------------------------------------------------------

#[test]
fn a_role_outside_the_closed_set_is_refused_naming_the_set() {
    let doc = with_sources(&source("ws", "uds", "secondary", ""));
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();

    match error {
        StartupError::UnknownSourceRole { token, supported } => {
            assert_eq!(token, "secondary");
            assert_eq!(supported, "primary, comparison");
        }
        other => panic!("expected an unknown role, got {other}"),
    }
}

#[test]
fn two_sources_sharing_a_name_are_refused() {
    // Two blocks with one name are two descriptions of a single connection, and
    // which of them is in force would depend on which happened to be enabled.
    let doc = with_sources(&format!(
        "{}\n{}",
        source("ws", "uds", "primary", ""),
        source("ws", "uds", "comparison", "")
    ));
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();

    assert!(
        matches!(&error, StartupError::DuplicateSourceName { name } if name == "ws"),
        "{error}"
    );
}

#[test]
fn a_duplicate_name_is_refused_even_when_one_block_is_disabled() {
    let doc = with_sources(&format!(
        "{}enabled = false\n{}",
        source("ws", "uds", "primary", ""),
        source("ws", "uds", "primary", "")
    ));
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();

    assert!(
        matches!(&error, StartupError::DuplicateSourceName { .. }),
        "{error}"
    );
}

#[test]
fn a_source_carrying_a_feed_this_publisher_does_not_emit_is_refused() {
    // A key nobody reads, refused for the reason every other one is: an
    // operator who wrote it believes that feed is being served from there.
    let doc = with_sources(&source("ws", "uds", "primary", "\"market-by-price\""));
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();

    match error {
        StartupError::SourceCarriesUnknownFeed { name, spec } => {
            assert_eq!(name, "ws");
            assert_eq!(spec, "market-by-price");
        }
        other => panic!("expected an unknown carried feed, got {other}"),
    }
}

#[test]
fn a_disabled_source_is_not_opened_and_not_declared() {
    // Not opened, not handed to the adapter, and deliberately not declared to
    // the metrics registry: a connection-state series pre-created at 0 for a
    // connection nobody meant to open is an alert firing for a decision
    // somebody took on purpose.
    let doc = with_sources(&format!(
        "{}\n{}enabled = false\n",
        source("ws", "uds", "primary", ""),
        source("fix", "uds", "comparison", "")
    ));
    let config = Document::parse(&doc.render())
        .expect("valid")
        .resolve()
        .expect("resolvable");

    assert_eq!(config.sources.len(), 1);
    assert_eq!(config.sources[0].connection.as_str(), "ws");
}

#[test]
fn every_source_disabled_is_refused() {
    let doc = with_sources(&format!(
        "{}enabled = false\n",
        source("ws", "uds", "primary", "")
    ));
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();

    assert!(matches!(error, StartupError::NoEnabledSource), "{error}");
}

#[test]
fn a_transport_this_binary_was_not_built_with_is_refused_per_source() {
    // The same resolution `[ingress] kind` gets, and the same two
    // distinguishable failures: this one is a build to redo rather than a typo
    // to fix, which is why the message says which.
    //
    // `fix` and not `websocket`, deliberately. Whether a marker feature is on
    // depends on what else is in the build — cargo unifies features across a
    // workspace, so `dz-ingress-websocket` being a member makes `websocket`
    // linked in a whole-workspace test run and unlinked in a single-crate one.
    // A test that asserted the unlinked case over that token would pass alone
    // and fail in CI. No crate implements `fix` at all, so it is unlinked in
    // every build — and it is the transport a shipped venue is actually waiting
    // for.
    let doc = with_sources(&source("ws", "fix", "primary", ""));
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();

    let message = error.to_string();
    assert!(message.contains("not built with it"), "{message}");
}

#[test]
fn a_credential_that_is_not_a_path_is_refused_per_source_too() {
    // `[[source]] credentials` is checked exactly as `[adapter.credentials]`
    // is: the two shapes that are decidably not paths are a value that is not a
    // string and a string carrying a line break, which is a private key
    // somebody pasted in.
    let doc = with_sources(&format!(
        "{}\n[source.credentials]\nkey = \"\"\"\n-----BEGIN PRIVATE KEY-----\nx\n\"\"\"\n",
        source("ws", "uds", "primary", "")
    ));
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();

    assert!(
        matches!(&error, StartupError::NotACredentialPath { key, .. } if key == "key"),
        "{error}"
    );
}

// ---------------------------------------------------------------------------
// The document and the binary have to agree
// ---------------------------------------------------------------------------

/// A registry whose adapter builds the sources `builds` names, whatever the
/// document says.
fn registry_building(builds: &'static [&'static str]) -> AdapterRegistry {
    AdapterRegistry::new().with("a-venue", move |_cx| {
        Ok(Venue {
            adapter: Box::new(harness::FakeAdapter::new(&["A-B"])),
            sources: builds
                .iter()
                .map(|name| {
                    Box::new(harness::refusing_input(ConnectionId::new(name)))
                        as Box<dyn dz_ingress_core::Input>
                })
                .collect(),
        })
    })
}

fn context<'a>(config: &'a dz_publisher_runtime::Config) -> AdapterContext<'a> {
    AdapterContext::new(
        &config.adapter,
        config.ingress_kind,
        &config.venue,
        &config.sources,
    )
}

#[test]
fn a_venue_that_builds_the_declared_sources_composes() {
    let doc = with_sources(&format!(
        "{}\n{}",
        source("ws", "uds", "primary", ""),
        source("fix", "uds", "comparison", "")
    ));
    let config = Document::parse(&doc.render())
        .expect("valid")
        .resolve()
        .expect("resolvable");

    let venue = registry_building(&["fix", "ws"])
        .open(&context(&config))
        .expect("both declared sources were built");
    // Order does not matter: the check is on the set, because the document's
    // order is a reading order and the venue's is a construction order.
    assert_eq!(venue.sources.len(), 2);
    dz_publisher_runtime::check_sources(&config, &venue).expect("the sets match");
}

#[test]
fn a_venue_that_skips_a_declared_source_is_refused_naming_both_sets() {
    // Silent otherwise: the missing connection's series sits at zero, which
    // reads exactly like an upstream that is down.
    let doc = with_sources(&format!(
        "{}\n{}",
        source("ws", "uds", "primary", ""),
        source("fix", "uds", "comparison", "")
    ));
    let config = Document::parse(&doc.render())
        .expect("valid")
        .resolve()
        .expect("resolvable");
    let venue = registry_building(&["ws"])
        .open(&context(&config))
        .expect("constructs");

    let error = dz_publisher_runtime::check_sources(&config, &venue).unwrap_err();
    match error {
        StartupError::SourcesDisagree { declared, built } => {
            assert_eq!(declared, "fix, ws");
            assert_eq!(built, "ws");
        }
        other => panic!("expected a disagreement, got {other}"),
    }
}

#[test]
fn a_venue_that_builds_a_source_nobody_declared_is_refused() {
    // Its traffic would move under a `connection` label the metric registry
    // never pre-created, so it would be counted under no series at all.
    let doc = with_sources(&source("ws", "uds", "primary", ""));
    let config = Document::parse(&doc.render())
        .expect("valid")
        .resolve()
        .expect("resolvable");
    let venue = registry_building(&["ws", "surprise"])
        .open(&context(&config))
        .expect("constructs");

    let error = dz_publisher_runtime::check_sources(&config, &venue).unwrap_err();
    assert!(
        matches!(&error, StartupError::SourcesDisagree { built, .. } if built.contains("surprise")),
        "{error}"
    );
}

#[test]
fn a_venue_that_builds_nothing_is_refused() {
    let config = Document::parse(&Doc::valid().render())
        .expect("valid")
        .resolve()
        .expect("resolvable");
    let venue = registry_building(&[])
        .open(&context(&config))
        .expect("constructs");

    assert!(
        matches!(
            dz_publisher_runtime::check_sources(&config, &venue),
            Err(StartupError::NoVenueSource)
        ),
        "a publisher with nothing to read from would look like a quiet venue"
    );
}

#[test]
fn several_transports_with_no_document_to_declare_them_are_refused() {
    // Nothing would say what the second connection is, which feed it carries or
    // whether it is meant to publish - and its name would be the venue's rather
    // than the operator's.
    let config = Document::parse(&Doc::valid().render())
        .expect("valid")
        .resolve()
        .expect("resolvable");
    let venue = registry_building(&["ws", "fix"])
        .open(&context(&config))
        .expect("constructs");

    assert!(
        matches!(
            dz_publisher_runtime::check_sources(&config, &venue),
            Err(StartupError::SourcesUndeclared { built: 2 })
        ),
        "{:?}",
        dz_publisher_runtime::check_sources(&config, &venue)
    );
}

// ---------------------------------------------------------------------------
// Two sources, one adapter
// ---------------------------------------------------------------------------

#[test]
fn one_adapter_tells_its_sources_apart_by_the_connection_that_delivered_them() {
    // **The whole of what the runtime promises a multi-source venue**, and the
    // property that makes reconciling a websocket against a FIX session
    // possible at all: every source reaches one adapter, and every payload
    // carries the connection it arrived on.
    //
    // The runtime does not merge them. That is the venue's, for the same reason
    // the book state machine is: which of two prices is current, and when to
    // fail over, follows the venue's microstructure and nothing above the
    // boundary can know it. What is asserted here is that the adapter is handed
    // what it needs to decide.
    //
    // Driven directly rather than through two `Driver`s: what a driver adds is
    // the connecting, the backoff and the reconnect, which `dz-ingress-core`
    // tests over its own fakes. Whether `run()` polls several of them in one
    // task is not assertable without sockets, and is stated in that module
    // rather than mocked here.
    use dz_adapter_core::{Adapter, Payload};
    use std::sync::Arc;

    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut adapter = harness::ConnectionRecorder {
        seen: Arc::clone(&seen),
    };
    let mut sink = harness::NoEvents;

    for (name, bytes) in [
        ("ws", &b"ws-1"[..]),
        ("fix", &b"fix-1"[..]),
        ("ws", &b"ws-2"[..]),
    ] {
        adapter
            .on_payload(
                &Payload {
                    bytes,
                    recv_ts_ns: 1,
                    connection: ConnectionId::new(name),
                },
                &mut sink,
            )
            .expect("the recorder reads anything");
    }

    let attributed: Vec<(&str, String)> = seen
        .lock()
        .expect("not poisoned")
        .iter()
        .map(|(name, bytes)| (*name, String::from_utf8_lossy(bytes).into_owned()))
        .collect();

    assert_eq!(
        attributed,
        [
            ("ws", "ws-1".to_owned()),
            ("fix", "fix-1".to_owned()),
            ("ws", "ws-2".to_owned()),
        ],
        "each payload is attributed to the connection that delivered it"
    );
}
