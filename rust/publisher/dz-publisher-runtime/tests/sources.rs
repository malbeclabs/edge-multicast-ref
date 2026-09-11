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
use harness::{Doc, GROUP, SOURCE_ID};

/// A `[[source]]` block, with only the keys a test varies stated.
fn source(name: &str, ingress: &str, role: &str) -> String {
    let mut block = format!("[[source]]\nname = \"{name}\"\ningress = \"{ingress}\"\n");
    if !role.is_empty() {
        block.push_str(&format!("role = \"{role}\"\n"));
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
        source("ws", "uds", "primary"),
        source("fix", "uds", "comparison")
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
        source("ws", "uds", "primary")
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
        source("ws", "uds", "primary"),
        source("fix", "uds", "primary")
    ));
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();

    match error {
        StartupError::SourcePrimaries { primaries } => {
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
    let doc = with_sources(&source("fix", "uds", "comparison"));
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();

    match error {
        StartupError::SourcePrimaries { primaries } => assert_eq!(primaries, "none"),
        other => panic!("expected a primaries error, got {other}"),
    }
}

/// The rule is publisher-wide, and two primaries are refused however many feeds
/// the publisher emits.
///
/// It was per feed, grouped by a `carries` key that said which feeds a source's
/// data reached. That key could not be honoured: every source's payloads reach
/// one adapter, the adapter emits events, and no event carries the source it
/// came from — so nothing in the runtime can confine a source to a subset of
/// feeds. Two primaries with disjoint declarations therefore resolved cleanly
/// while both upstreams' events landed on one channel instance under one
/// `Sequence Number` series, which a subscriber reads as its own gap-detection
/// losses. A rule that describes routing the runtime does not do is worse than
/// no rule, so the key is gone and the rule is the one that holds.
#[test]
fn two_primaries_are_refused_on_a_publisher_with_two_feeds() {
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
            source("ws", "uds", "primary"),
            source("fix", "uds", "primary")
        ));
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .expect_err("two primaries are two publishers' worth of events");

    match error {
        StartupError::SourcePrimaries { primaries } => {
            assert!(
                primaries.contains("ws") && primaries.contains("fix"),
                "{primaries}"
            );
        }
        other => panic!("expected a primaries error, got {other}"),
    }
}

/// One primary and one comparison is the shape the array exists for, and it
/// resolves on a publisher with two feeds as it does on one.
#[test]
fn one_primary_beside_a_comparison_resolves_however_many_feeds_there_are() {
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
            source("ws", "uds", "primary"),
            source("fix", "uds", "comparison")
        ));
    let config = Document::parse(&doc.render())
        .expect("valid")
        .resolve()
        .expect("one primary is the rule");

    assert_eq!(config.sources.len(), 2);
    assert!(config.sources[0].is_primary());
    assert!(!config.sources[1].is_primary());
}

/// `carries` is not a key any more, so a document stating it is refused by
/// `deny_unknown_fields` rather than accepted and ignored.
///
/// A key that used to mean something and now means nothing is the one an
/// operator is most likely to still have in a file, and reading it as a
/// partition nothing performs is what the removal is for.
#[test]
fn the_carries_key_is_refused_rather_than_ignored() {
    let mut block = source("ws", "uds", "primary");
    block.push_str("carries = [\"top-of-book\"]\n");
    let doc = with_sources(&block);
    let error = Document::parse(&doc.render()).expect_err("`carries` is not a key");
    assert!(error.to_string().contains("carries"), "{error}");
}

#[test]
fn primary_is_the_default_role() {
    // A publisher with one source states a transport and nothing else, and the
    // role it gets is the one that publishes. The alternative default -
    // `comparison` - would be a publisher that came up and published nothing.
    let doc = with_sources(&source("ws", "uds", ""));
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
    let doc = with_sources(&source("ws", "uds", "secondary"));
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
        source("ws", "uds", "primary"),
        source("ws", "uds", "comparison")
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
        source("ws", "uds", "primary"),
        source("ws", "uds", "primary")
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

/// A document that names its transport per source need not write `[ingress]`
/// at all.
///
/// Required, this failed at parse with `missing field `ingress`` reported at
/// line 1, column 1 — an error pointing an operator at the whole file rather
/// than at the section they did not write. Every key in the section has a
/// default and `kind` is optional, so nothing in it must be stated.
#[test]
fn a_document_with_sources_and_no_ingress_section_resolves() {
    let doc = Doc::valid().ingress(String::new()).adapter(format!(
        "{}\n{}",
        Doc::valid().adapter,
        source("ws", "uds", "primary")
    ));
    let config = Document::parse(&doc.render())
        .expect("the section is defaultable")
        .resolve()
        .expect("resolvable");

    assert_eq!(config.sources.len(), 1);
    assert_eq!(config.sources[0].kind, Kind::Uds);
    // And the defaults are the section's own, not zeros.
    assert!(config.ingress.connect_timeout > std::time::Duration::ZERO);
}

/// And a document that names a transport in neither place still fails — with
/// the error that names both ways of stating it, rather than with a missing
/// section.
#[test]
fn a_document_with_no_transport_anywhere_still_names_both_places() {
    let doc = Doc::valid().ingress(String::new());
    let error = Document::parse(&doc.render())
        .expect("the section is defaultable")
        .resolve()
        .expect_err("no transport is named anywhere");

    let message = error.to_string();
    assert!(message.contains("[ingress] kind"), "{message}");
    assert!(message.contains("[[source]] ingress"), "{message}");
}

/// A name that differs from its trim is refused, not trimmed.
///
/// The name is used three times: the emptiness check reads the trimmed string,
/// and the duplicate check and the leak that produces the `connection` metric
/// label read what was written. So `"ws"` and `"ws "` resolved as two distinct
/// sources with two label values a dashboard cannot tell apart, and an error
/// listing them rendered them as `ws, ws `. Trimming silently would fix the
/// label and leave the file saying something else; refusing names the typo where
/// it was made.
#[test]
fn a_source_name_with_surrounding_whitespace_is_refused() {
    let doc = with_sources(&source("ws ", "uds", "primary"));
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .expect_err("a name that is not its own trim");

    match error {
        StartupError::SourceNameNotTrimmed { name, trimmed } => {
            assert_eq!(name, "ws ");
            assert_eq!(trimmed, "ws");
            // Both spellings are in the message, because the point is that they
            // look the same and are not.
            let message = StartupError::SourceNameNotTrimmed { name, trimmed }.to_string();
            assert!(message.contains("connection"), "{message}");
        }
        other => panic!("expected a whitespace refusal, got {other}"),
    }

    // And a name that is all whitespace is still the empty case, which has its
    // own error: an operator who wrote `name = " "` wrote no name.
    let doc = with_sources(&source("   ", "uds", "primary"));
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .expect_err("whitespace is not a name");
    assert!(matches!(error, StartupError::UnnamedSource), "{error}");
}

/// Two names that differ only by whitespace are no longer two sources.
///
/// This is the failure the refusal above prevents, asserted from the other
/// side: without it these resolve as two connections and the metric registry
/// pre-creates two series a dashboard renders identically.
#[test]
fn two_names_differing_only_by_whitespace_are_not_two_sources() {
    let doc = with_sources(&format!(
        "{}\n{}",
        source("ws", "uds", "primary"),
        source("ws ", "uds", "comparison")
    ));
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .expect_err("`ws` and `ws ` are not two connections");
    assert!(
        matches!(error, StartupError::SourceNameNotTrimmed { .. }),
        "{error}"
    );
}

#[test]
fn a_disabled_source_is_not_opened_and_not_declared() {
    // Not opened, not handed to the adapter, and deliberately not declared to
    // the metrics registry: a connection-state series pre-created at 0 for a
    // connection nobody meant to open is an alert firing for a decision
    // somebody took on purpose.
    let doc = with_sources(&format!(
        "{}\n{}enabled = false\n",
        source("ws", "uds", "primary"),
        source("fix", "uds", "comparison")
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
        source("ws", "uds", "primary")
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
    // `multicast`, and not `websocket` or `fix`, deliberately. Whether a
    // marker feature is on depends on what else is in the build — cargo
    // unifies features across a workspace, so a transport crate being a member
    // makes its token linked in a whole-workspace test run and unlinked in a
    // single-crate one. A test that asserted the unlinked case over
    // `websocket` or `fix` would pass alone and fail in CI, because
    // `dz-ingress-websocket` and `dz-ingress-fix` are both members. No crate
    // implements `multicast`, so it is unlinked in every build.
    let doc = with_sources(&source("ws", "multicast", "primary"));
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
        source("ws", "uds", "primary")
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
        let sources = builds
            .iter()
            .map(|name| {
                Box::new(harness::refusing_input(ConnectionId::new(name)))
                    as Box<dyn dz_ingress_core::Input>
            })
            .collect();
        Ok(Venue::new(
            Box::new(harness::FakeAdapter::new(&["A-B"])),
            sources,
        ))
    })
}

fn context(config: &dz_publisher_runtime::Config) -> AdapterContext<'_> {
    // Leaked because the context borrows every section it carries and the feed
    // specifications are derived rather than stored. One allocation per case in
    // a test binary, which is what a `&'static` costs here.
    let feeds: &'static [dz_publisher_runtime::FeedSpec] =
        Box::leak(config.feed_specs().into_boxed_slice());
    AdapterContext::new(
        &config.adapter,
        config.ingress_kind,
        &config.venue,
        &config.sources,
        feeds,
    )
}

#[test]
fn a_venue_that_builds_the_declared_sources_composes() {
    let doc = with_sources(&format!(
        "{}\n{}",
        source("ws", "uds", "primary"),
        source("fix", "uds", "comparison")
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
        source("ws", "uds", "primary"),
        source("fix", "uds", "comparison")
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
    let doc = with_sources(&source("ws", "uds", "primary"));
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

// ---------------------------------------------------------------------------
// One session per source, which is the operator's statement and not a
// consequence
// ---------------------------------------------------------------------------

/// One driver is opened per enabled `[[source]]`, and per nothing else.
///
/// Asserted rather than left as a thing that happens to be true, because it is
/// what matters when a venue permits one session per credential and answers a
/// second logon by evicting the first. A publisher carrying sixty-two channel
/// instances of one feed specification over one source opens **one** session: a
/// shard is a partition of the published set and has nothing to do with how
/// many upstream connections exist.
///
/// Sixty-two, and of one specification, because that is the shape the design
/// names — a publisher this size is what makes the question worth asking, and
/// it is the size at which a rule that had quietly become per-feed or per-shard
/// would be caught by its own arithmetic rather than by a venue.
#[test]
fn sixty_two_channel_instances_over_one_source_open_one_session() {
    const INSTANCES: u8 = 62;

    let mut blocks = String::new();
    for index in 0..INSTANCES {
        // Index 0 states no shard: it is the default one, and a deployment that
        // grows into shards grows out of a document that had none.
        let shard = if index == 0 {
            String::new()
        } else {
            format!("shard = \"shard-{index:02}\"\n")
        };
        // Every port and every `Channel ID` distinct across the document, which
        // is what a document nobody would deploy would not have.
        let base = 41_000 + u16::from(index) * 4;
        blocks.push_str(&format!(
            "[[feed]]\n\
             spec = \"top-of-book\"\n\
             {shard}\
             channel_id = {index}\n\
             source_id = {SOURCE_ID}\n\
             multicast_group = \"{GROUP}\"\n\
             mktdata_port = {mktdata}\n\
             refdata_port = {refdata}\n\
             heartbeat_interval = \"1s\"\n\
             definition_cycle = \"30s\"\n\
             manifest_cadence = \"1s\"\n\
             idle_guard = \"60s\"\n\
             \n",
            mktdata = base,
            refdata = base + 1,
        ));
    }

    let doc = with_sources(&source("mktdata", "uds", "primary")).feed(blocks);
    let config = Document::parse(&doc.render())
        .expect("sixty-two channel instances of one specification")
        .resolve()
        .expect("resolvable");

    assert_eq!(
        config.feeds.len(),
        usize::from(INSTANCES),
        "the document really does carry sixty-two channel instances"
    );
    assert_eq!(
        config.sources.len(),
        1,
        "one enabled `[[source]]` is one session, whatever the published set is \
         partitioned into"
    );
    assert_eq!(config.sources[0].connection.as_str(), "mktdata");

    // And the whole chain, not only the count: one declared source, one
    // transport built, and `check_sources` holding the two to each other — so
    // one driver, so one session. A venue that built one per channel instance
    // is refused rather than opening sixty-two of them.
    let venue = registry_building(&["mktdata"])
        .open(&context(&config))
        .expect("one source built");
    assert_eq!(venue.sources.len(), 1);
    dz_publisher_runtime::check_sources(&config, &venue).expect("one declared, one built");

    let per_instance = registry_building(&["mktdata", "mktdata-shard-01"])
        .open(&context(&config))
        .expect("constructs");
    let error = dz_publisher_runtime::check_sources(&config, &per_instance)
        .expect_err("a session per channel instance is not what the document says");
    assert!(
        matches!(&error, StartupError::SourcesDisagree { built, .. } if built.contains("mktdata-shard-01")),
        "{error}"
    );
}

#[test]
fn a_disabled_source_opens_no_session() {
    // The count is the *enabled* blocks, so a block kept and turned off is a
    // decision an operator took on purpose and not a session.
    let doc = with_sources(&format!(
        "{}{}enabled = false\n",
        source("ws", "uds", "primary"),
        source("standby", "uds", "comparison")
    ));
    let config = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .expect("resolvable");

    assert_eq!(config.sources.len(), 1);
    assert_eq!(config.sources[0].connection.as_str(), "ws");
}

#[test]
fn two_enabled_sources_with_the_same_credential_table_are_refused_naming_both() {
    // The revert this test exists for: resolve the document and open both
    // sessions. What that costs is the failure nobody diagnoses from one
    // publisher's logs — a venue that permits one session per credential
    // answers the second logon by evicting the first, and the two connections
    // take turns knocking each other off while each looks, in isolation,
    // exactly like a venue that keeps closing the connection.
    //
    // The copy-paste shape it catches: a second block with a new endpoint and
    // the credential nobody changed.
    let doc = with_sources(&format!(
        "{}[source.credentials]\nkey = \"/etc/a-publisher/session.key\"\n\n\
         {}[source.credentials]\nkey = \"/etc/a-publisher/session.key\"\n",
        source("primary-session", "uds", "primary"),
        source("second-session", "uds", "comparison")
    ));
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .expect_err("two logons with one credential");

    match &error {
        StartupError::SourceCredentialsShared { one, another } => {
            // Both blocks, because being told that *a* credential is shared
            // leaves an operator with the same search they started with.
            assert_eq!(one, "primary-session");
            assert_eq!(another, "second-session");
        }
        other => panic!("{other}"),
    }
    let message = error.to_string();
    assert!(message.contains("primary-session"), "{message}");
    assert!(message.contains("second-session"), "{message}");
    // And the limit of the check is stated in the message rather than left for
    // somebody to discover: two *different* paths holding one account is the
    // case nothing here can see.
    assert!(
        message.contains("reconnecting in step"),
        "the message must name the symptom of the case it cannot catch: {message}"
    );
}

#[test]
fn a_credential_a_second_block_added_a_key_to_is_still_one_credential() {
    // The revert this test exists for: compare whole tables. Two blocks naming
    // one `key_path`, where the second also writes the passphrase file for it,
    // are unequal tables — so equality resolves the document and opens both
    // sessions, for exactly the failure the exact-duplicate case above is
    // refused to prevent. It is the likelier copy-paste of the two: the block
    // was copied, one line was added to it, and the credential was the line
    // nobody looked at.
    //
    // Both orders, because containment is directional: whichever block is the
    // larger one, the pair states one credential, and a check written one way
    // round accepts the other half of the documents it was meant to refuse.
    let key = "key_path = \"/etc/a-publisher/session.key\"\n";
    let passphrase = "passphrase_path = \"/etc/a-publisher/session.pass\"\n";
    for (which, first, second) in [
        ("the added key on the second block", "", passphrase),
        ("the added key on the first block", passphrase, ""),
    ] {
        let doc = with_sources(&format!(
            "{}[source.credentials]\n{key}{first}\n\
             {}[source.credentials]\n{key}{second}",
            source("primary-session", "uds", "primary"),
            source("second-session", "uds", "comparison")
        ));
        let resolved = Document::parse(&doc.render()).expect("parses").resolve();
        let error = match resolved {
            Ok(_) => panic!(
                "{which}: two blocks naming one `key_path`, one of which also writes the \
                 passphrase file for it, resolved into two sessions — they are unequal tables \
                 and one credential"
            ),
            Err(error) => error,
        };

        match &error {
            StartupError::SourceCredentialsShared { one, another } => {
                // Both blocks, in the order the document writes them, because
                // being told that *a* credential is shared leaves an operator
                // with the same search they started with.
                assert_eq!(one, "primary-session", "{which}");
                assert_eq!(another, "second-session", "{which}");
            }
            other => panic!("{which}: {other}"),
        }
    }
}

#[test]
fn two_sources_that_disagree_on_a_key_they_both_write_are_two_credentials() {
    // The boundary of the rule, stated as a document rather than left to the
    // prose. The keys under `credentials` are the venue adapter's, so nothing
    // in this runtime can tell an identity path from a trust root — which is
    // why the rule is containment and not "any key two blocks agree on". These
    // two accounts each hold their own key and both trust the same CA bundle,
    // which is an ordinary deployment; refusing it would leave the operator no
    // way to start but to duplicate the bundle.
    //
    // What separates it from the refusals above is not which key is shared but
    // that somebody edited one: the two disagree on a key they both write.
    let ca = "ca_path = \"/etc/ssl/a-venue.pem\"\n";
    let doc = with_sources(&format!(
        "{}[source.credentials]\nkey_path = \"/etc/a-publisher/one.key\"\n{ca}\n\
         {}[source.credentials]\nkey_path = \"/etc/a-publisher/another.key\"\n{ca}",
        source("one", "uds", "primary"),
        source("another", "uds", "comparison")
    ));
    let config = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .expect("two accounts that share a trust root are two credentials");

    assert_eq!(config.sources.len(), 2);
}

#[test]
fn a_source_with_no_credential_shares_one_with_nobody() {
    // The carve-out, under a rule that needs it stated on both sides. The empty
    // table is contained in every table, so a containment check that skipped
    // only the later block would read the first block writing no `credentials`
    // as sharing a credential with every block that writes one — and refuse
    // the ordinary document where one upstream authenticates elsewhere and
    // another does not.
    let doc = with_sources(&format!(
        "{}\n{}[source.credentials]\nkey_path = \"/etc/a-publisher/session.key\"\n",
        source("needs-none", "uds", "primary"),
        source("needs-one", "uds", "comparison")
    ));
    let config = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .expect("no credential is not every other credential");

    assert_eq!(config.sources.len(), 2);
}

#[test]
fn two_sources_with_their_own_credentials_are_two_sessions() {
    // The document the refusal above exists to distinguish from: two blocks,
    // two credentials, two logons a venue can hold at once.
    let doc = with_sources(&format!(
        "{}[source.credentials]\nkey = \"/etc/a-publisher/one.key\"\n\n\
         {}[source.credentials]\nkey = \"/etc/a-publisher/another.key\"\n",
        source("one", "uds", "primary"),
        source("another", "uds", "comparison")
    ));
    let config = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .expect("two credentials are two sessions");

    assert_eq!(config.sources.len(), 2);
}

#[test]
fn several_sources_that_need_no_credential_are_not_two_logons_with_one() {
    // An empty `credentials` table is not a shared credential. A venue reached
    // over a path that needs none leaves the table unwritten, and refusing that
    // document would refuse every publisher whose upstream authenticates
    // elsewhere.
    let doc = with_sources(&format!(
        "{}\n{}",
        source("one", "uds", "primary"),
        source("another", "uds", "comparison")
    ));
    let config = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .expect("no credential is not a shared credential");

    assert_eq!(config.sources.len(), 2);
}

#[test]
fn a_credential_shared_with_a_disabled_block_is_not_two_logons() {
    // A disabled block opens no session, so it cannot be one of two logons —
    // unlike the name check, which reads every block because two blocks with
    // one name are two descriptions of a single connection.
    let doc = with_sources(&format!(
        "{}[source.credentials]\nkey = \"/etc/a-publisher/session.key\"\n\n\
         {}enabled = false\n[source.credentials]\nkey = \"/etc/a-publisher/session.key\"\n",
        source("live", "uds", "primary"),
        source("standby", "uds", "comparison")
    ));
    let config = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .expect("a disabled block is not a session");

    assert_eq!(config.sources.len(), 1);
    assert_eq!(config.sources[0].connection.as_str(), "live");
}
