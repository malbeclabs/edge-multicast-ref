//! The configuration document: what it accepts, and everything it refuses.
//!
//! Every refusal in here is the same finding from a different angle. A
//! publisher had a misspelled section parse cleanly, fall back to a default, and
//! run the wrong transport while its operator believed otherwise — so a key
//! nobody reads is a load error in every table this crate owns, and a value
//! that is wrong about the wire is a refusal to start rather than a number
//! carried on with.

mod harness;

use std::time::Duration;

use dz_edge_core::PortRole;
use dz_publisher_runtime::{Document, FeedSpec, StartupError};
use harness::{Doc, CHANNEL_ID, GROUP, MKTDATA_PORT, REFDATA_PORT, SOURCE_ID};

// ---------------------------------------------------------------------------
// `[adapter.tee]`
// ---------------------------------------------------------------------------

#[test]
fn the_adapter_tee_defaults_off_when_the_section_is_absent() {
    let document = Document::parse(&Doc::valid().render()).expect("valid");
    assert!(
        !document.adapter.tee.enabled,
        "a tee nobody asked for must not be on"
    );
    assert_eq!(document.adapter.tee.path, None);
}

#[test]
fn the_adapter_tee_defaults_off_when_the_section_is_present_without_the_key() {
    let mut doc = Doc::valid();
    doc.adapter = "[adapter]\nkind = \"a-venue\"\n\n[adapter.tee]\n".to_owned();
    let document = Document::parse(&doc.render()).expect("valid");
    assert!(!document.adapter.tee.enabled);
}

#[test]
fn the_adapter_tee_parses_when_it_is_present() {
    let mut doc = Doc::valid();
    doc.adapter = "[adapter]\n\
                   kind = \"a-venue\"\n\
                   \n\
                   [adapter.tee]\n\
                   enabled = true\n\
                   path = \"/run/a-publisher/tee.sock\"\n"
        .to_owned();
    let document = Document::parse(&doc.render()).expect("valid");
    assert!(document.adapter.tee.enabled);
    assert_eq!(
        document.adapter.tee.path.as_deref(),
        Some(std::path::Path::new("/run/a-publisher/tee.sock"))
    );
}

#[test]
fn the_adapter_tee_refuses_a_key_it_does_not_know() {
    // `[adapter.tee]` is under `[adapter]`, and task 7 asks for
    // `deny_unknown_fields` on `[adapter]` *and every section under it*. A tee
    // with a misspelled `path` would otherwise be an enabled tee with no
    // destination.
    let mut doc = Doc::valid();
    doc.adapter =
        "[adapter]\nkind = \"a-venue\"\n\n[adapter.tee]\nenabled = true\nsocket = \"/x\"\n"
            .to_owned();
    let error = Document::parse(&doc.render()).expect_err("`socket` is not a key of the tee");
    assert!(error.to_string().contains("socket"));
}

#[test]
fn the_tee_is_configured_under_adapter_and_not_under_egress() {
    // The placement is the design's and it is not cosmetic: the tee darkens
    // nothing when it fails and must never be able to end a send, so it does
    // not belong beside the keys an operator reads as *this can take the feed
    // down*. Written as a test because the wrong placement would parse
    // perfectly well.
    let mut doc = Doc::valid();
    doc.egress = "[egress]\nttl = 1\n\n[egress.tee]\nenabled = true\n".to_owned();
    let error = Document::parse(&doc.render()).expect_err("the tee is not an egress key");
    assert!(error.to_string().contains("tee"));
}

// ---------------------------------------------------------------------------
// `deny_unknown_fields`, section by section.
// ---------------------------------------------------------------------------

#[test]
fn an_unknown_key_is_refused_in_every_section_this_crate_owns() {
    // A table over the sections rather than one test each, so that a section
    // added to the document without the attribute fails here — the list below
    // is the whole document, and a reader comparing it to `config.rs` can see
    // if one is missing.
    /// One case: the section's name, how to break it, and the key the message
    /// must name.
    type Case = (&'static str, fn(&mut Doc), &'static str);

    let cases: [Case; 8] = [
        (
            "the document root",
            |doc| doc.root.push_str("venu = \"a-venue\"\n"),
            "venu",
        ),
        (
            "[egress]",
            |doc| doc.egress.push_str("interface = \"dz0\"\n"),
            "interface",
        ),
        ("[[feed]]", |doc| doc.feed.push_str("mtu = 1448\n"), "mtu"),
        (
            "[refdata]",
            |doc| doc.refdata = doc.refdata.replace("state_dir =", "state_directory ="),
            "state_directory",
        ),
        (
            "[refdata.selection]",
            |doc| doc.refdata.push_str("evict_below = 1\n"),
            "evict_below",
        ),
        (
            "[metrics]",
            |doc| doc.metrics.push_str("path = \"/metrics\"\n"),
            "path",
        ),
        (
            "[ingress]",
            |doc| doc.ingress.push_str("reconnect_backoff = \"1s\"\n"),
            "reconnect_backoff",
        ),
        (
            "[adapter]",
            |doc| doc.adapter.push_str("credential = \"/x\"\n"),
            "credential",
        ),
    ];

    for (section, break_it, key) in cases {
        let mut doc = Doc::valid();
        break_it(&mut doc);
        let error = match Document::parse(&doc.render()) {
            Err(error) => error,
            Ok(_) => panic!("{section} accepted the unknown key `{key}`"),
        };
        let message = error.to_string();
        assert!(
            message.contains(key),
            "{section} refused the document without naming `{key}`: {message}"
        );
    }
}

#[test]
fn a_top_level_venue_key_is_a_load_error() {
    // The design's fourth adapter rule: everything venue-specific lives under
    // `[adapter.*]`, and a top-level venue key is a load error. It is
    // `deny_unknown_fields` on the document root that makes that a mechanism
    // rather than a request.
    let mut doc = Doc::valid();
    doc.root
        .push_str("api_key_path = \"/etc/a-publisher/key\"\n");
    let error = Document::parse(&doc.render()).expect_err("a top-level venue key is refused");
    assert!(error.to_string().contains("api_key_path"));
}

#[test]
fn the_free_tables_under_adapter_stay_free() {
    // The other half of the rule. An adapter reading a local directory, one
    // holding two credentialed APIs and one reading a chain RPC plus a local
    // socket have nothing useful in common, so forcing a shape on
    // `[adapter.upstream]` would move the sprawl up a level. What is checked is
    // the *name* `upstream`, which is what makes `[adapter.upstrem]` a refusal.
    let mut doc = Doc::valid();
    doc.adapter = "[adapter]\n\
                   kind = \"a-venue\"\n\
                   \n\
                   [adapter.upstream]\n\
                   whatever_this_venue_calls_it = 7\n\
                   nested = { deeper = [1, 2, 3] }\n"
        .to_owned();
    let document = Document::parse(&doc.render()).expect("the adapter's keys are the adapter's");
    assert_eq!(document.adapter.upstream.len(), 2);
}

// ---------------------------------------------------------------------------
// Values that are wrong about the wire.
// ---------------------------------------------------------------------------

#[test]
fn a_valid_document_resolves_end_to_end() {
    // The control for every refusal below, and the one test that proves the
    // composition actually composes: each section reaches the constructor of
    // whichever crate owns it, and what comes back is checked values rather
    // than written ones.
    let config = Document::parse(&Doc::valid().render())
        .expect("valid")
        .resolve()
        .expect("every section is acceptable to its owner");

    assert_eq!(config.venue, "a-venue");
    assert_eq!(config.feeds.len(), 1);
    let feed = &config.feeds[0];
    assert_eq!(feed.spec, FeedSpec::TopOfBook);
    assert_eq!(feed.channel_id, CHANNEL_ID);
    assert_eq!(feed.source_id.get(), SOURCE_ID);
    assert_eq!(feed.group, GROUP);
    assert_eq!(feed.mktdata_port, MKTDATA_PORT);
    assert_eq!(feed.refdata_port, REFDATA_PORT);
    assert_eq!(feed.heartbeat_interval, Duration::from_secs(1));
    assert_eq!(feed.definition_cycle, Duration::from_secs(30));
    assert_eq!(feed.manifest_cadence, Duration::from_secs(1));
    assert_eq!(feed.idle_guard, Duration::from_secs(60));
    // The TTL default: one hop, because the group is delivered on the attached
    // segment and the network's own last mile carries it from there.
    assert_eq!(config.egress.ttl, 1);
    assert_eq!(config.egress.pin, None);
    assert_eq!(config.refdata.selection.bootstrap_top_n(), 8);
    assert_eq!(config.refdata.selection.max_published(), 16);
    // Exactly the roles a top-of-book feed operates, and not the snapshot role
    // it does not: passing a role this publisher does not operate would assert
    // a channel that does not exist.
    assert_eq!(
        config.port_roles(),
        [
            dz_edge_core::PortRole::Mktdata,
            dz_edge_core::PortRole::Refdata
        ]
    );
    assert_eq!(config.channel_ids(), [CHANNEL_ID]);
}

#[test]
fn the_durations_default_to_the_values_the_design_states() {
    // Transcribed from the design's own configuration block, which is where a
    // reader would go to check them.
    let mut doc = Doc::valid();
    doc.feed = format!(
        "[[feed]]\n\
         spec = \"top-of-book\"\n\
         channel_id = {CHANNEL_ID}\n\
         source_id = {SOURCE_ID}\n\
         multicast_group = \"{GROUP}\"\n\
         mktdata_port = {MKTDATA_PORT}\n\
         refdata_port = {REFDATA_PORT}\n"
    );
    let config = Document::parse(&doc.render())
        .expect("valid")
        .resolve()
        .expect("resolvable");
    let feed = &config.feeds[0];
    assert_eq!(feed.heartbeat_interval, Duration::from_secs(1));
    assert_eq!(feed.definition_cycle, Duration::from_secs(30));
    assert_eq!(feed.manifest_cadence, Duration::from_secs(1));
    assert_eq!(feed.idle_guard, Duration::from_secs(60));
    // A feed with no `enabled` key is enabled, so a document that names one
    // feed publishes it.
    assert_eq!(config.feeds.len(), 1);
}

#[test]
fn a_duration_without_a_unit_is_refused_rather_than_guessed_at() {
    // One publisher suffixes its duration keys `_seconds` and takes integers,
    // so `30` is thirty of something and picking a unit for it is how a
    // heartbeat interval becomes thirty milliseconds.
    let mut doc = Doc::valid();
    doc.feed = doc
        .feed
        .replace("heartbeat_interval = \"1s\"", "heartbeat_interval = \"1\"");
    let error = Document::parse(&doc.render()).expect_err("`1` has no unit");
    assert!(error.to_string().contains("no unit"), "{error}");
}

#[test]
fn a_depth_feed_resolves_with_the_three_port_roles_it_operates() {
    // The other half of `a_valid_document_resolves_end_to_end`. The depth
    // specification is named by `dz_edge_mbp::MarketByPrice`'s own
    // `Feed::NAME`, and what distinguishes it here is the third port role: a
    // subscriber to a depth feed holds a book that only exists because it
    // applied every message in order, so it needs somewhere to recover from.
    let config = Document::parse(&Doc::valid().feed(Doc::depth_feed_block()).render())
        .expect("valid")
        .resolve()
        .expect("resolvable");

    let feed = &config.feeds[0];
    assert_eq!(feed.spec, FeedSpec::MarketByPrice);
    assert_eq!(feed.spec.as_str(), "market-by-price");
    assert_eq!(feed.snapshot_port, Some(harness::DEPTH_SNAPSHOT_PORT));
    assert!(feed.spec.has_snapshot_port());
    assert_eq!(
        config.port_roles(),
        [PortRole::Mktdata, PortRole::Refdata, PortRole::Snapshot]
    );
    assert_eq!(config.channel_ids(), [harness::DEPTH_CHANNEL_ID]);
}

#[test]
fn a_depth_feed_with_no_snapshot_port_is_refused() {
    // Refused rather than run without one. A subscriber that lost a datagram
    // would have nowhere to recover from, and the publisher would look healthy
    // the whole time.
    let block = Doc::depth_feed_block()
        .lines()
        .filter(|line| !line.starts_with("snapshot_port"))
        .fold(String::new(), |mut text, line| {
            text.push_str(line);
            text.push('\n');
            text
        });
    let error = Document::parse(&Doc::valid().feed(block).render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    assert!(
        matches!(
            error,
            StartupError::SnapshotPortRequired {
                spec: "market-by-price"
            }
        ),
        "{error}"
    );
}

#[test]
fn a_top_of_book_feed_with_a_snapshot_port_is_refused() {
    // The other direction, and it is the audit's failure in miniature: a key
    // nobody reads. An operator who wrote a port believes something is
    // listening on it, and top-of-book has no snapshot port role at all.
    let error = Document::parse(
        &Doc::valid()
            .feed(format!("{}snapshot_port = 30003\n", Doc::valid().feed))
            .render(),
    )
    .expect("parses")
    .resolve()
    .unwrap_err();
    assert!(
        matches!(
            error,
            StartupError::SnapshotPortNotCarried {
                spec: "top-of-book",
                port: 30003
            }
        ),
        "{error}"
    );
}

#[test]
fn both_feeds_in_one_document_resolve() {
    // `[[feed]]` is an array because a publisher may emit several, which one
    // existing publisher expresses as repeated blocks and another as four
    // differently-named sections.
    let doc = Doc::valid();
    let both = format!("{}\n{}", doc.feed, Doc::depth_feed_block());
    let config = Document::parse(&doc.feed(both).render())
        .expect("valid")
        .resolve()
        .expect("two feeds is what the array is for");

    assert_eq!(config.feeds.len(), 2);
    assert_eq!(config.feeds[0].spec, FeedSpec::TopOfBook);
    assert_eq!(config.feeds[1].spec, FeedSpec::MarketByPrice);
    // The union, deduplicated: the metrics crate pre-creates one child series
    // per role and per channel, so an omission leaves a panel blank and an
    // extra asserts a channel that does not exist.
    assert_eq!(
        config.port_roles(),
        [PortRole::Mktdata, PortRole::Refdata, PortRole::Snapshot]
    );
    assert_eq!(
        config.channel_ids(),
        [harness::CHANNEL_ID, harness::DEPTH_CHANNEL_ID]
    );
}

#[test]
fn two_feeds_naming_different_source_ids_are_refused() {
    // A `Source ID` is the publisher's registered identity and is the same for
    // every message a process sends - the lowering takes it once, for that
    // reason. Obeying either of two would put an identity on one feed's wire
    // that its own block did not ask for.
    let doc = Doc::valid();
    let other = Doc::depth_feed_block().replace(
        &format!("source_id = {SOURCE_ID}"),
        &format!("source_id = {}", SOURCE_ID + 1),
    );
    let both = format!("{}\n{other}", doc.feed);
    let error = Document::parse(&doc.feed(both).render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    assert!(
        matches!(error, StartupError::SeveralSourceIds { .. }),
        "{error}"
    );
}

#[test]
fn a_feed_specification_this_build_has_no_codec_for_names_the_ones_it_has() {
    // Market-by-order is the live example: `dz-edge-mbo` does not exist, so the
    // boundary has no event variants for it and this crate has nothing to
    // compose. Named rather than defaulted, and the message lists both
    // specifications that do resolve.
    let error = Document::parse(
        &Doc::valid()
            .edit_feed("spec = \"top-of-book\"", "spec = \"market-by-order\"")
            .render(),
    )
    .expect("the document parses")
    .resolve()
    .expect_err("this build has no market-by-order codec");
    let message = error.to_string();
    assert!(message.contains("market-by-order"), "{message}");
    assert!(message.contains("top-of-book"), "{message}");
    assert!(message.contains("market-by-price"), "{message}");
    assert!(matches!(error, StartupError::UnsupportedFeedSpec { .. }));
}

#[test]
fn a_source_id_the_registry_reserves_is_refused() {
    // Zero is reserved and MUST NOT reach the wire, and it is exactly what a
    // half-read configuration file hands you. A publisher with no valid
    // identity must fail at startup rather than fail conformance on every
    // message it ever sends.
    for reserved in [0, 1024, 32767] {
        let mut doc = Doc::valid();
        doc.feed = doc.feed.replace(
            &format!("source_id = {SOURCE_ID}"),
            &format!("source_id = {reserved}"),
        );
        let error = Document::parse(&doc.render())
            .expect("parses")
            .resolve()
            .unwrap_err();
        assert!(
            matches!(error, StartupError::BadSourceId { source_id } if source_id == reserved),
            "source_id {reserved} was accepted: {error}"
        );
        // The message names the ranges, because the operator's next question is
        // which value to write.
        let message = error.to_string();
        assert!(message.contains("1-1023"), "{message}");
        assert!(message.contains("32768-65535"), "{message}");
    }
}

#[test]
fn a_group_that_is_not_a_multicast_address_is_refused() {
    let mut doc = Doc::valid();
    doc.feed = doc.feed.replace(
        &format!("multicast_group = \"{GROUP}\""),
        "multicast_group = \"203.0.113.9\"",
    );
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    assert!(
        matches!(error, StartupError::NotMulticast { .. }),
        "{error}"
    );
}

#[test]
fn two_port_roles_on_one_port_are_refused() {
    // The port is what separates the roles - the specification mandates one
    // group with distinct destination ports - and the channel instance a
    // subscriber tracks is keyed on it. Two roles on one port interleave two
    // independent sequence series into one that goes backwards on every
    // alternation.
    let mut doc = Doc::valid();
    doc.feed = doc.feed.replace(
        &format!("refdata_port = {REFDATA_PORT}"),
        &format!("refdata_port = {MKTDATA_PORT}"),
    );
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    assert!(
        matches!(error, StartupError::PortsCollide { port, .. } if port == MKTDATA_PORT),
        "{error}"
    );
}

#[test]
fn a_zero_port_is_refused() {
    let mut doc = Doc::valid();
    doc.feed = doc.feed.replace(
        &format!("mktdata_port = {MKTDATA_PORT}"),
        "mktdata_port = 0",
    );
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    assert!(
        matches!(
            error,
            StartupError::ZeroPort {
                key: "mktdata_port"
            }
        ),
        "{error}"
    );
}

#[test]
fn a_document_with_no_enabled_feed_is_refused() {
    let mut doc = Doc::valid();
    doc.feed = doc.feed.replace("enabled = true", "enabled = false");
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    assert!(matches!(error, StartupError::NoEnabledFeed), "{error}");
}

#[test]
fn two_feed_blocks_naming_one_specification_are_refused() {
    let mut doc = Doc::valid();
    let one = doc.feed.clone();
    doc.feed = format!("{one}\n{one}");
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    assert!(
        matches!(error, StartupError::DuplicateFeedSpec { .. }),
        "{error}"
    );
}

#[test]
fn an_incoherent_selection_policy_is_refused() {
    // A cap below the seed, which is the policy's own refusal reported against
    // the keys an operator wrote.
    let mut doc = Doc::valid();
    doc.refdata = doc
        .refdata
        .replace("max_published = 16", "max_published = 4");
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    assert!(matches!(error, StartupError::Selection { .. }), "{error}");
    assert!(error.to_string().contains("[refdata.selection]"));
}

#[test]
fn an_expected_prefix_that_is_not_a_prefix_is_refused() {
    let mut doc = Doc::valid();
    doc.egress = "[egress]\nexpected_prefix = \"203.0.113.0\"\n".to_owned();
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    assert!(matches!(error, StartupError::BadPrefix { .. }), "{error}");
}

#[test]
fn a_pinned_source_address_that_is_not_an_address_is_refused() {
    let mut doc = Doc::valid();
    doc.egress = "[egress]\npin = \"the-tunnel\"\n".to_owned();
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    assert!(
        matches!(
            error,
            StartupError::NotAnAddress {
                key: "[egress] pin",
                ..
            }
        ),
        "{error}"
    );
}

// ---------------------------------------------------------------------------
// Credentials.
// ---------------------------------------------------------------------------

#[test]
fn a_credential_that_is_a_path_is_accepted() {
    let mut doc = Doc::valid();
    doc.adapter = "[adapter]\n\
                   kind = \"a-venue\"\n\
                   \n\
                   [adapter.credentials]\n\
                   api_key = \"/etc/a-publisher/api.key\"\n\
                   signing_key = \"/etc/a-publisher/signing.pem\"\n"
        .to_owned();
    let config = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .expect("paths are what credentials are");
    assert_eq!(config.adapter.credentials.len(), 2);
}

#[test]
fn an_inline_secret_is_refused() {
    // Whether a string is a secret is not decidable, and the two shapes that
    // are decidable are the ones worth failing a startup over: a value that is
    // not a string at all, and a string carrying a line break - which is a
    // private key or a certificate somebody pasted into a configuration file.
    let mut doc = Doc::valid();
    doc.adapter = "[adapter]\n\
                   kind = \"a-venue\"\n\
                   \n\
                   [adapter.credentials]\n\
                   signing_key = \"\"\"\n\
                   -----BEGIN PRIVATE KEY-----\n\
                   not-a-real-key\n\
                   -----END PRIVATE KEY-----\n\
                   \"\"\"\n"
        .to_owned();
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    assert!(
        matches!(&error, StartupError::NotACredentialPath { key, .. } if key == "signing_key"),
        "{error}"
    );
}

#[test]
fn a_credential_that_is_not_a_string_is_refused() {
    let mut doc = Doc::valid();
    doc.adapter = "[adapter]\n\
                   kind = \"a-venue\"\n\
                   \n\
                   [adapter.credentials]\n\
                   api_key = 12345\n"
        .to_owned();
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    assert!(
        matches!(&error, StartupError::NotACredentialPath { what, .. } if *what == "not a string"),
        "{error}"
    );
}

// ---------------------------------------------------------------------------
// `[ingress]`, which this crate composes and does not own.
// ---------------------------------------------------------------------------

#[test]
fn a_transport_this_binary_was_not_built_with_is_a_different_error() {
    // Two distinguishable failures, and the difference is the operator's next
    // action: an unknown kind is a typo to fix in the file, and a kind in the
    // family that this binary was not built with is a build to redo. Collapsing
    // them would send someone hunting for a spelling mistake in a value that is
    // spelled correctly.
    //
    // Every transport is unlinked in this crate's own build; the test harness
    // turns on the marker for `uds` alone, so `fix` is the honest example.
    let mut doc = Doc::valid();
    doc.ingress = "[ingress]\nkind = \"fix\"\n".to_owned();
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("not built with it"), "{message}");

    doc.ingress = "[ingress]\nkind = \"web-socket\"\n".to_owned();
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("names no transport"), "{message}");
    // And it names the built-in set, for the same reason the adapter registry
    // names itself.
    for kind in ["websocket", "fix", "multicast", "rest", "filetail", "uds"] {
        assert!(message.contains(kind), "{message}");
    }
}

// ---------------------------------------------------------------------------
// `[[feed]] snapshot_cycle`: the periodic rotation.
// ---------------------------------------------------------------------------

#[test]
fn a_depth_feed_resolves_a_snapshot_cycle() {
    let config = Document::parse(
        &Doc::valid()
            .feed(format!(
                "{}snapshot_cycle = \"5s\"\n",
                Doc::depth_feed_block()
            ))
            .render(),
    )
    .expect("valid")
    .resolve()
    .expect("resolvable");

    assert_eq!(config.feeds[0].snapshot_cycle, Some(Duration::from_secs(5)));
}

#[test]
fn a_depth_feed_without_the_key_rotates_nothing() {
    // Absent is a real answer and the one every existing configuration gives:
    // recovery snapshots and nothing else. It is `None` rather than a default
    // cadence because a cadence nobody asked for would put datagrams on the
    // snapshot port of every depth publisher that upgrades.
    let config = Document::parse(&Doc::valid().feed(Doc::depth_feed_block()).render())
        .expect("valid")
        .resolve()
        .expect("resolvable");

    assert_eq!(config.feeds[0].snapshot_cycle, None);
}

#[test]
fn a_snapshot_cycle_on_a_feed_with_no_snapshot_port_is_refused() {
    // The same rule as `snapshot_port` itself, one key along: a cadence for a
    // port role the feed does not carry is a key nobody reads, and an operator
    // who wrote it believes snapshots are going out.
    let error = Document::parse(
        &Doc::valid()
            .feed(format!("{}snapshot_cycle = \"5s\"\n", Doc::valid().feed))
            .render(),
    )
    .expect("parses")
    .resolve()
    .unwrap_err();

    assert!(
        matches!(
            error,
            StartupError::SnapshotCycleWithoutPort {
                spec: "top-of-book"
            }
        ),
        "{error}"
    );
}

#[test]
fn a_zero_snapshot_cycle_is_refused_rather_than_run_every_tick() {
    let error = Document::parse(
        &Doc::valid()
            .feed(format!(
                "{}snapshot_cycle = \"0s\"\n",
                Doc::depth_feed_block()
            ))
            .render(),
    )
    .expect("parses")
    .resolve()
    .unwrap_err();

    assert!(
        matches!(
            error,
            StartupError::ZeroDuration {
                key: "[[feed]] snapshot_cycle"
            }
        ),
        "{error}"
    );
}

// ---------------------------------------------------------------------------
// Two feeds, one publisher: the keys that cannot differ.
// ---------------------------------------------------------------------------

#[test]
fn two_feeds_disagreeing_on_the_definition_cycle_are_refused() {
    // **This runtime used to take the first block's answer and ignore the
    // second.** One reference-data registry serves every feed, because
    // `Instrument ID` identity is the one thing there can only be one of - so
    // there is one cadence to pace it with, and a document stating two is a
    // document that cannot be obeyed. An operator who set the second was being
    // ignored in fact while being obeyed on paper.
    // The first block states 30s, which is also the default; the second states
    // 10s, so the two disagree.
    let two = format!(
        "{}\n{}definition_cycle = \"10s\"\n",
        Doc::valid().feed,
        Doc::depth_feed_block()
    );
    let error = Document::parse(&Doc::valid().feed(two).render())
        .expect("parses")
        .resolve()
        .unwrap_err();

    match error {
        StartupError::FeedsDisagree { key, one, another } => {
            assert_eq!(key, "[[feed]] definition_cycle");
            // Both values are named, because the operator has to be told which
            // two of their own keys are in conflict.
            assert_eq!(one, Duration::from_secs(30));
            assert_eq!(another, Duration::from_secs(10));
        }
        other => panic!("expected a disagreement, got {other}"),
    }
}

#[test]
fn two_feeds_disagreeing_on_the_idle_guard_are_refused() {
    // One guard, because the silence it measures is the publisher's: upstream
    // delivering and nothing reaching any wire. The shipped publisher that once
    // had one guard per feed now has exactly one venue-wide guard with a
    // fallback to its first feed's key, and the fallback is the trap this
    // refuses instead.
    let two = format!(
        "{}\n{}idle_guard = \"5m\"\n",
        Doc::valid().feed,
        Doc::depth_feed_block()
    );
    let error = Document::parse(&Doc::valid().feed(two).render())
        .expect("parses")
        .resolve()
        .unwrap_err();

    match error {
        StartupError::FeedsDisagree { key, one, another } => {
            assert_eq!(key, "[[feed]] idle_guard");
            assert_eq!(one, Duration::from_secs(60));
            assert_eq!(another, Duration::from_secs(300));
        }
        other => panic!("expected a disagreement, got {other}"),
    }
}

#[test]
fn two_feeds_agreeing_on_both_resolve() {
    // The control. Two feeds are the ordinary case - it is what `[[feed]]`
    // being an array is for - and the refusal above must be about the values
    // rather than about there being two blocks.
    // The depth block states neither key, so it takes the defaults - which are
    // what the first block states. Agreement by default is still agreement, and
    // it is the path every existing single-feed document already takes.
    let two = format!("{}\n{}", Doc::valid().feed, Doc::depth_feed_block());
    let config = Document::parse(&Doc::valid().feed(two).render())
        .expect("parses")
        .resolve()
        .expect("two feeds that agree are one publisher");

    assert_eq!(config.feeds.len(), 2);
    // And a snapshot cycle stated on the depth feed only is not a
    // disagreement: it is a key the other feed cannot carry at all.
    assert_eq!(config.feeds[0].snapshot_cycle, None);
}

#[test]
fn a_snapshot_cycle_on_one_of_two_feeds_is_not_a_disagreement() {
    let two = format!(
        "{}\n{}snapshot_cycle = \"5s\"\n",
        Doc::valid().feed,
        Doc::depth_feed_block()
    );
    let config = Document::parse(&Doc::valid().feed(two).render())
        .expect("parses")
        .resolve()
        .expect("resolvable");

    assert_eq!(config.feeds.len(), 2);
    let depth = config
        .feeds
        .iter()
        .find(|feed| feed.spec == FeedSpec::MarketByPrice)
        .expect("the depth feed");
    assert_eq!(depth.snapshot_cycle, Some(Duration::from_secs(5)));
}

#[test]
fn a_tee_enabled_with_no_path_is_refused_at_load() {
    // The same shape as `[adapter.replay]`: a section switched on and left
    // incomplete is an operator who believes copies are being archived. Refused
    // before a socket is opened, because nothing about it needs one.
    let error = Document::parse(
        &Doc::valid()
            .adapter("[adapter]\nkind = \"a-venue\"\n\n[adapter.tee]\nenabled = true\n")
            .render(),
    )
    .expect("parses")
    .resolve()
    .unwrap_err();

    assert!(matches!(error, StartupError::TeeWithoutPath), "{error}");
}

#[test]
fn a_tee_with_a_path_resolves() {
    let config = Document::parse(
        &Doc::valid()
            .adapter(
                "[adapter]\nkind = \"a-venue\"\n\n[adapter.tee]\nenabled = true\n\
                 path = \"/run/a-publisher/tee\"\n",
            )
            .render(),
    )
    .expect("parses")
    .resolve()
    .expect("resolvable");

    assert!(config.adapter.tee.enabled);
    // A prefix, not a socket: the port role's token is appended per role.
    assert_eq!(
        config.adapter.tee.path.as_deref(),
        Some(std::path::Path::new("/run/a-publisher/tee"))
    );
}
