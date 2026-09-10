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
use dz_publisher_egress::DEFAULT_TTL;
use dz_publisher_runtime::config::ShardName;
use dz_publisher_runtime::{Document, FeedSpec, StartupError, TeeConfig};
use harness::{Doc, CHANNEL_ID, DEPTH_CHANNEL_ID, GROUP, MKTDATA_PORT, REFDATA_PORT, SOURCE_ID};

// ---------------------------------------------------------------------------
// `[adapter.tee]`
// ---------------------------------------------------------------------------

#[test]
fn the_adapter_tee_defaults_off_when_the_section_is_absent() {
    let document = Document::parse(&Doc::valid().render()).expect("valid");
    assert!(
        !document.adapter.tee.enabled,
        "a fan-out nobody asked for must not be on"
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
                   path = \"/run/a-publisher/fan-out.sock\"\n"
        .to_owned();
    let document = Document::parse(&doc.render()).expect("valid");
    assert!(document.adapter.tee.enabled);
    assert_eq!(
        document.adapter.tee.path.as_deref(),
        Some(std::path::Path::new("/run/a-publisher/fan-out.sock"))
    );
}

#[test]
fn the_adapter_tee_refuses_a_key_it_does_not_know() {
    // `[adapter.tee]` is under `[adapter]`, and task 7 asks for
    // `deny_unknown_fields` on `[adapter]` *and every section under it*. A
    // section with a misspelled `path` would otherwise be an enabled fan-out
    // with no destination.
    let mut doc = Doc::valid();
    doc.adapter =
        "[adapter]\nkind = \"a-venue\"\n\n[adapter.tee]\nenabled = true\nsocket = \"/x\"\n"
            .to_owned();
    let error =
        Document::parse(&doc.render()).expect_err("`socket` is not a key of `[adapter.tee]`");
    assert!(error.to_string().contains("socket"));
}

#[test]
fn the_tee_is_configured_under_adapter_and_not_under_egress() {
    // The placement is the design's and it is not cosmetic: the fan-out darkens
    // nothing when it fails and must never be able to end a send, so it does
    // not belong beside the keys an operator reads as *this can take the feed
    // down*. Written as a test because the wrong placement would parse
    // perfectly well.
    let mut doc = Doc::valid();
    doc.egress = "[egress]\nttl = 1\n\n[egress.tee]\nenabled = true\n".to_owned();
    let error = Document::parse(&doc.render()).expect_err("`tee` is not an egress key");
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
    // The TTL this document states, not a default: `Doc::valid` writes
    // `ttl = 1`, and a document that omitted the key would not resolve at all.
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
        matches!(error, StartupError::DuplicateFeedShard { .. }),
        "{error}"
    );
}

/// Two blocks of one specification on **different** shards resolve.
///
/// The whole change, in one assertion. This was refused outright until the gate
/// lifted, and everything before it was ordered so that lifting it would not
/// produce a publisher that starts and is wrong on the wire.
#[test]
fn two_blocks_of_one_specification_on_different_shards_resolve() {
    let mut doc = Doc::valid();
    let second = doc
        .feed
        .replace(
            &format!("channel_id = {CHANNEL_ID}"),
            "channel_id = 9\nshard = \"beta\"",
        )
        .replace(
            &format!("mktdata_port = {MKTDATA_PORT}"),
            &format!("mktdata_port = {}", MKTDATA_PORT + 20),
        )
        .replace(
            &format!("refdata_port = {REFDATA_PORT}"),
            &format!("refdata_port = {}", REFDATA_PORT + 20),
        );
    doc.feed = format!("{}\n{second}", doc.feed);

    let config = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .expect("two shards of one specification is what this change is for");
    assert_eq!(config.feeds.len(), 2);
    assert_eq!(config.shards().len(), 2, "two distinct shards");
    assert_eq!(
        config.feed_specs().len(),
        1,
        "and one specification, however many shards carry it"
    );
}

/// The document at the scale this change exists for: 31 shards, both
/// specifications, 62 channel instances, and it starts.
///
/// The acceptance criterion, as a test rather than as a hand-run. Everything
/// else about shards is asserted on two of them, which is enough to make a
/// partition falsifiable and not enough to say the document scales: `channel_id`
/// is a `u8`, so 62 is well inside the ceiling but the *set* checks — one shard
/// missing a specification, two blocks on one `(spec, shard)` pair, a repeated
/// `Channel ID` — are the ones that would quietly turn quadratic or, worse,
/// disagree with themselves at size.
///
/// One shard is the default, named by the absence of the key, because a
/// deployment that grows into shards grows out of a document that had none and
/// the block it already had keeps meaning what it meant.
#[test]
fn thirty_one_shards_of_both_specifications_resolve_as_sixty_two_channel_instances() {
    const SHARDS: u8 = 31;

    let mut blocks = String::new();
    for index in 0..SHARDS {
        // Index 0 states no shard: it is the default one, and the era file and
        // the reference-copy socket it keeps are the upgrade this document
        // shape has to survive.
        let shard = if index == 0 {
            String::new()
        } else {
            format!("shard = \"shard-{index:02}\"\n")
        };
        // Two blocks per shard, and every port distinct across the document.
        // Distinct because an operator writing 62 blocks by hand is exactly who
        // collides two, and a test that shared them would be asserting against
        // a document nobody would deploy.
        let base = 41_000 + u16::from(index) * 10;
        blocks.push_str(&format!(
            "[[feed]]\n\
             spec = \"top-of-book\"\n\
             {shard}\
             channel_id = {tob}\n\
             source_id = {SOURCE_ID}\n\
             multicast_group = \"{GROUP}\"\n\
             mktdata_port = {mktdata}\n\
             refdata_port = {refdata}\n\
             heartbeat_interval = \"1s\"\n\
             definition_cycle = \"30s\"\n\
             manifest_cadence = \"1s\"\n\
             idle_guard = \"60s\"\n\
             \n\
             [[feed]]\n\
             spec = \"market-by-price\"\n\
             {shard}\
             channel_id = {mbp}\n\
             source_id = {SOURCE_ID}\n\
             multicast_group = \"{GROUP}\"\n\
             mktdata_port = {depth_mktdata}\n\
             refdata_port = {depth_refdata}\n\
             snapshot_port = {snapshot}\n\
             heartbeat_interval = \"1s\"\n\
             definition_cycle = \"30s\"\n\
             manifest_cadence = \"1s\"\n\
             idle_guard = \"60s\"\n\
             \n",
            tob = index * 2,
            mbp = index * 2 + 1,
            mktdata = base,
            refdata = base + 1,
            depth_mktdata = base + 2,
            depth_refdata = base + 3,
            snapshot = base + 4,
        ));
    }

    let config = Document::parse(&Doc::valid().feed(blocks).render())
        .expect("parses")
        .resolve()
        .expect("31 shards of both specifications is the deployment this is for");

    assert_eq!(config.feeds.len(), 62, "62 channel instances");
    assert_eq!(config.shards().len(), usize::from(SHARDS), "31 shards");
    assert_eq!(
        config.feed_specs().len(),
        2,
        "and two specifications, however many shards carry them"
    );

    // 62 distinct `Channel ID`s, counted rather than assumed. A document this
    // long is one an operator writes with a generator, and the duplicate a
    // generator produces is the one nothing on the wire can tell apart.
    let mut ids: Vec<u8> = config.feeds.iter().map(|feed| feed.channel_id).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 62, "62 distinct channel ids");

    // Every shard carries a block for every specification. That is the property
    // making `list_on` total, asserted here across the whole set rather than on
    // the pair a refusal test uses.
    for shard in config.shards() {
        let carried = config
            .feeds
            .iter()
            .filter(|feed| feed.shard == shard)
            .count();
        assert_eq!(carried, 2, "shard `{shard}` carries {carried} blocks");
    }
}

/// A shard with a block for one specification and not another is refused.
///
/// **This is the check that makes `list_on` total.** An instrument admitted to a
/// shard with no block for a specification another shard has would have messages
/// that reach no wire and are counted only as unroutable — the venue doing
/// exactly what the interface asked, and a feed silently missing for part of the
/// instrument set.
#[test]
fn a_shard_missing_a_specification_another_shard_has_is_refused_naming_both() {
    let mut doc = Doc::valid();
    // Shard beta carries market-by-price and nothing else; the default shard
    // carries top-of-book. Neither covers what the other does.
    let second = Doc::depth_feed_block().replace(
        &format!("channel_id = {DEPTH_CHANNEL_ID}"),
        "channel_id = 9\nshard = \"beta\"",
    );
    doc.feed = format!("{}\n{second}", doc.feed);

    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    let message = error.to_string();
    assert!(
        matches!(error, StartupError::ShardSpecsDisagree { .. }),
        "a shard with no block for a specification another has was accepted: {message}"
    );
    assert!(
        message.contains("beta") || message.contains("default"),
        "{message}"
    );
    assert!(
        message.contains("top-of-book") || message.contains("market-by-price"),
        "{message}"
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
    // `ttl` is stated so that this document has exactly one thing wrong with
    // it. Without it the refusal below also has a second cause, and the test
    // would pass on whichever `resolve` happens to check first.
    doc.egress = "[egress]\nttl = 1\nexpected_prefix = \"203.0.113.0\"\n".to_owned();
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    assert!(matches!(error, StartupError::BadPrefix { .. }), "{error}");
}

/// A document that states no TTL does not start.
///
/// **The key lost its default because being wrong about it is silent in every
/// direction an operator can look.** A locally attached subscriber receives, so
/// a smoke test on the publisher's own host passes. Every datagram is sent
/// successfully, so nothing in the egress series moves — the kernel accepted
/// each one and a router discarded it. A subscriber that never joined has
/// nothing to number, so gap detection reports nothing either. The publisher is
/// healthy and the feed is empty.
#[test]
fn a_document_that_states_no_ttl_is_refused() {
    let mut doc = Doc::valid();
    // No `[egress]` section at all, which is the shape a document that never
    // thought about the hop count has.
    doc.egress = String::new();
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    assert!(matches!(error, StartupError::TtlUnstated), "{error}");
}

/// An `[egress]` section that states everything except the TTL is the same
/// mistake as no section at all, and gets the same refusal.
///
/// Both cases are asserted because the refusal is in `resolve` rather than in
/// serde. A required *field* would have made the section required too, and this
/// repository has already recorded what that costs: `missing field` at line 1,
/// column 1 — an error pointing at the whole file rather than at the section
/// nobody wrote.
#[test]
fn an_egress_section_without_a_ttl_is_refused_like_an_absent_one() {
    let mut doc = Doc::valid();
    doc.egress =
        "[egress]\nexpected_prefix = \"203.0.113.0/24\"\npin = \"203.0.113.7\"\n".to_owned();
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    assert!(matches!(error, StartupError::TtlUnstated), "{error}");
}

/// The message carries the line an operator has to write.
///
/// Asserted as substrings because the message **is** the remedy: an operator
/// upgrading has one key to add, and a message that stopped naming the value
/// would leave them to guess which number reproduces what they had.
///
/// The value is asserted through `DEFAULT_TTL` rather than as the literal in
/// the message's own text, because that constant's documentation claims this
/// refusal names its value. A message that retyped the number would let the two
/// disagree — `EgressPolicy::default` sending one hop count while an operator
/// is told to write another — with the suite still green.
#[test]
fn the_refusal_names_the_key_and_the_value_that_reproduces_one_hop() {
    let message = StartupError::TtlUnstated.to_string();
    assert!(message.contains("[egress] ttl"), "{message}");
    assert!(
        message.contains(&format!("ttl = {DEFAULT_TTL}")),
        "the line to write carries the constant's own value: {message}"
    );
    assert!(
        message.contains("attached segment"),
        "the message says what one hop means: {message}"
    );
}

/// Zero is refused, because it is not a smaller hop count.
///
/// **The value this key's own refusal invites.** That message says `ttl = 1`
/// publishes on the attached segment only, so an operator who wants exactly
/// that learns the key is a hop count and has no reason to read `0` as anything
/// but *fewer hops than one*.
///
/// And it satisfies every clause of the argument for requiring the key, plus
/// one more: the kernel accepts every datagram so the egress series stay green,
/// nothing joined so gap detection reports nothing, and the one check that
/// catches a hop count set too low — a subscriber on the publisher's own
/// segment — fails too, because at zero the datagram never leaves the host.
#[test]
fn a_ttl_of_zero_is_refused_because_it_is_no_hop_at_all() {
    let mut doc = Doc::valid();
    doc.egress = "[egress]\nttl = 0\n".to_owned();
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    assert!(matches!(error, StartupError::TtlZero), "{error}");
    let message = error.to_string();
    assert!(message.contains("[egress] ttl = 0"), "{message}");
    assert!(
        message.contains("inside this host"),
        "the refusal has to say what zero does, not only that it is refused: {message}"
    );
}

/// One hop is still expressible, and now it is stated.
#[test]
fn a_stated_ttl_of_one_resolves_to_one_hop() {
    let mut doc = Doc::valid();
    doc.egress = "[egress]\nttl = 1\n".to_owned();
    let config = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .expect("one hop is a value, not a default");
    assert_eq!(config.egress.ttl, 1);
}

/// And the value a deployment that exists uses.
///
/// 64 rather than 2: a publisher in production states it because its groups
/// cross several hops, which is the whole reason the default was not the
/// operating value.
#[test]
fn a_stated_ttl_of_sixty_four_reaches_the_policy() {
    let mut doc = Doc::valid();
    doc.egress = "[egress]\nttl = 64\n".to_owned();
    let config = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .expect("a routed group is the case this key exists for");
    assert_eq!(config.egress.ttl, 64);
}

#[test]
fn a_pinned_source_address_that_is_not_an_address_is_refused() {
    let mut doc = Doc::valid();
    // Stated, for the reason above.
    doc.egress = "[egress]\nttl = 1\npin = \"the-tunnel\"\n".to_owned();
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
fn a_value_stated_on_one_feed_and_omitted_on_the_other_governs_both() {
    // **A key the file does not contain cannot be one of two conflicting
    // values.** Both keys were serde-defaulted, so `feeds[0]` carried a value
    // whether or not that block stated one - and a document setting
    // `idle_guard = "300s"` on its depth feed and omitting it on its
    // top-of-book feed was refused for a conflict between 300s and a 60s
    // default the operator never typed. A publisher that started yesterday
    // would refuse to start today, naming two values one of which is not in
    // the file.
    //
    // Absent is absent: one stated value settles it, and it governs every feed
    // rather than only the block it appears in.
    let top_of_book_without_the_keys = Doc::valid()
        .feed
        .replace("definition_cycle = \"30s\"\n", "")
        .replace("idle_guard = \"60s\"\n", "");
    let two = format!(
        "{}\n{}definition_cycle = \"45s\"\nidle_guard = \"300s\"\n",
        top_of_book_without_the_keys,
        Doc::depth_feed_block()
    );
    let config = Document::parse(&Doc::valid().feed(two).render())
        .expect("parses")
        .resolve()
        .expect("one stated value is not a disagreement");

    assert_eq!(config.feeds.len(), 2);
    for feed in &config.feeds {
        assert_eq!(
            (feed.definition_cycle, feed.idle_guard),
            (Duration::from_secs(45), Duration::from_secs(300)),
            "the stated value governs the feed that omitted it too: {:?}",
            feed.spec
        );
    }
}

#[test]
fn a_zero_stated_for_a_publisher_wide_cadence_is_still_refused() {
    // The zero check moved with the key, and zero is what an unset integer
    // reads as in a document that spells its durations as bare numbers.
    let error = Document::parse(
        &Doc::valid()
            .feed(
                Doc::valid()
                    .feed
                    .replace("definition_cycle = \"30s\"", "definition_cycle = \"0s\""),
            )
            .render(),
    )
    .expect("parses")
    .resolve()
    .unwrap_err();

    assert!(
        matches!(
            error,
            StartupError::ZeroDuration {
                key: "[[feed]] definition_cycle"
            }
        ),
        "{error}"
    );
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
fn a_fan_out_enabled_with_no_path_is_refused_at_load() {
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
fn a_fan_out_with_a_path_resolves() {
    let config = Document::parse(
        &Doc::valid()
            .adapter(
                "[adapter]\nkind = \"a-venue\"\n\n[adapter.tee]\nenabled = true\n\
                 path = \"/run/a-publisher/fan-out\"\n",
            )
            .render(),
    )
    .expect("parses")
    .resolve()
    .expect("resolvable");

    assert!(config.adapter.tee.enabled);
    // A prefix, not a socket: the feed's `spec` and the port role's token are
    // appended per socket. See the test below.
    assert_eq!(
        config.adapter.tee.path.as_deref(),
        Some(std::path::Path::new("/run/a-publisher/fan-out"))
    );
}

#[test]
fn a_fan_out_socket_is_named_by_the_feed_as_well_as_the_port_role() {
    // **The feed is in the name because a publisher emits more than one.** A
    // Unix datagram carries neither a destination port nor a group, and the diff
    // this fan-out exists for is keyed on both - so two feeds' mktdata copies
    // arriving on one socket are datagrams a recorder cannot attribute without
    // decoding them, which is the one thing a record path does not do. Keyed on
    // the port role alone, that is exactly what a two-feed publisher produced,
    // and the per-role split was for this very problem.
    let fan_out = TeeConfig {
        enabled: true,
        path: Some(std::path::PathBuf::from("/run/a-publisher/fan-out")),
    };

    let named = |spec: FeedSpec, role: PortRole| {
        fan_out
            .destination(spec, &ShardName::default_shard(), role)
            .expect("the path is stated")
            .display()
            .to_string()
    };

    // The five sockets a publisher emitting both feeds of the default shard
    // opens, in full. Stated as literals rather than composed, because the
    // whole point of the name is that a recorder's configuration spells it the
    // same way by hand.
    assert_eq!(
        named(FeedSpec::TopOfBook, PortRole::Mktdata),
        "/run/a-publisher/fan-out.top-of-book.mktdata"
    );
    assert_eq!(
        named(FeedSpec::TopOfBook, PortRole::Refdata),
        "/run/a-publisher/fan-out.top-of-book.refdata"
    );
    assert_eq!(
        named(FeedSpec::MarketByPrice, PortRole::Mktdata),
        "/run/a-publisher/fan-out.market-by-price.mktdata"
    );
    assert_eq!(
        named(FeedSpec::MarketByPrice, PortRole::Refdata),
        "/run/a-publisher/fan-out.market-by-price.refdata"
    );
    assert_eq!(
        named(FeedSpec::MarketByPrice, PortRole::Snapshot),
        "/run/a-publisher/fan-out.market-by-price.snapshot"
    );

    // The property, stated as one: every socket a publisher emitting both feeds
    // opens is distinct. A name missing either half collapses two of these.
    let mut sockets: Vec<String> = FeedSpec::ALL
        .into_iter()
        .flat_map(|spec| {
            spec.port_roles()
                .iter()
                .map(move |role| named(spec, *role))
                .collect::<Vec<_>>()
        })
        .collect();
    let opened = sockets.len();
    sockets.sort();
    sockets.dedup();
    assert_eq!(
        sockets.len(),
        opened,
        "two port roles of two feeds share a socket: {sockets:?}"
    );
}

/// And by the shard, because two channel instances of one specification are
/// ordinary now.
///
/// The same argument one noun further along: a Unix datagram carries neither a
/// destination port nor a group, so two shards' copies of one feed's one role
/// arriving on one socket are datagrams a recorder cannot attribute without
/// decoding them. Before this, four channel instances fanned out to five
/// sockets.
#[test]
fn a_fan_out_socket_is_named_by_the_shard_as_well_as_the_feed_and_the_role() {
    let fan_out = TeeConfig {
        enabled: true,
        path: Some(std::path::PathBuf::from("/run/a-publisher/fan-out")),
    };
    let named = |spec: FeedSpec, shard: &ShardName, role: PortRole| {
        fan_out
            .destination(spec, shard, role)
            .expect("the path is stated")
            .display()
            .to_string()
    };
    let alpha = ShardName::new("alpha").expect("one path component");
    let beta = ShardName::new("beta").expect("one path component");

    assert_eq!(
        named(FeedSpec::TopOfBook, &alpha, PortRole::Mktdata),
        "/run/a-publisher/fan-out.top-of-book.alpha.mktdata"
    );
    assert_eq!(
        named(FeedSpec::MarketByPrice, &beta, PortRole::Snapshot),
        "/run/a-publisher/fan-out.market-by-price.beta.snapshot"
    );

    // **The default shard is spelled by its absence**, as its era file is. A
    // shard segment for it renames the socket every existing deployment's
    // recorder is bound to, and the fan-out then writes to a path with nobody
    // on it - which is the upgrade meant to be safe taking the fan-out down.
    assert_eq!(
        named(
            FeedSpec::TopOfBook,
            &ShardName::default_shard(),
            PortRole::Mktdata
        ),
        "/run/a-publisher/fan-out.top-of-book.mktdata"
    );

    // Every socket two shards of both feeds open, distinct: two shards, two
    // specifications, three port roles between them. A name missing the shard
    // collapses these in half, which is the pair of channel instances writing
    // to one socket.
    let shards = [alpha, beta, ShardName::default_shard()];
    let mut sockets: Vec<String> = shards
        .iter()
        .flat_map(|shard| {
            FeedSpec::ALL.into_iter().flat_map(move |spec| {
                spec.port_roles()
                    .iter()
                    .map(move |role| named(spec, shard, *role))
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let opened = sockets.len();
    assert_eq!(
        opened, 15,
        "three shards of both feeds open five sockets each"
    );
    sockets.sort();
    sockets.dedup();
    assert_eq!(
        sockets.len(),
        opened,
        "two channel instances share a socket: {sockets:?}"
    );

    // The suffix lands on the last component rather than becoming a child
    // directory, which is what the `OsString` construction exists for and what
    // a `join` or a `set_extension` would each get wrong in its own way.
    assert_eq!(
        named(FeedSpec::TopOfBook, &shards[0], PortRole::Refdata),
        "/run/a-publisher/fan-out.top-of-book.alpha.refdata"
    );
}

#[test]
fn a_fan_out_that_is_on_with_no_path_names_no_socket() {
    // The same refusal the load already produced, checked again where the
    // socket is named: a prefix is not something to default, and a fan-out
    // quietly writing to a relative path is an operator believing copies are
    // archived.
    let fan_out = TeeConfig {
        enabled: true,
        path: None,
    };
    let error = fan_out
        .destination(
            FeedSpec::TopOfBook,
            &ShardName::default_shard(),
            PortRole::Mktdata,
        )
        .expect_err("no path was stated");
    assert!(matches!(error, StartupError::TeeWithoutPath), "{error}");
}

/// A shard name that cannot be a path component is refused at load.
///
/// The name reaches a path in two places — the block's era file and its
/// reference-copy socket — so it is checked where the value enters the process
/// rather than at each use, where the third use is the one that forgets. A
/// slash writes somewhere nobody configured; a name differing from another only
/// past the length bound shares its era file.
#[test]
fn a_shard_name_that_cannot_be_a_path_component_is_refused() {
    for bad in [
        "sports_events", // an underscore
        "sports/events", // a path separator
        "Sports",        // an upper-case letter
        &"s".repeat(65), // one byte past the bound
        "",              // and nothing at all
    ] {
        let mut doc = Doc::valid();
        doc.feed = doc.feed.replace(
            &format!("channel_id = {CHANNEL_ID}"),
            &format!("channel_id = {CHANNEL_ID}\nshard = \"{bad}\""),
        );
        let error = Document::parse(&doc.render())
            .expect("parses")
            .resolve()
            .unwrap_err();
        assert!(
            matches!(error, StartupError::UnsafeShardName { .. }),
            "`{bad}` was accepted: {error}"
        );
        // The message has to say what would have been accepted, or an operator
        // is left guessing which of four rules they broke.
        let message = error.to_string();
        assert!(
            message.contains("lower-case letters, digits and hyphens"),
            "the refusal does not say what a shard name may be: {message}"
        );
    }
}

/// A name at the bound is accepted, so the refusal is a bound and not a mood.
#[test]
fn a_shard_name_of_exactly_the_bound_is_accepted() {
    let mut doc = Doc::valid();
    let name = "s".repeat(64);
    doc.feed = doc.feed.replace(
        &format!("channel_id = {CHANNEL_ID}"),
        &format!("channel_id = {CHANNEL_ID}\nshard = \"{name}\""),
    );
    let config = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .expect("sixty-four bytes is the bound, not one past it");
    assert_eq!(config.feeds[0].shard.as_str(), name);
}

/// Spelling the default shard's own token is refused.
///
/// Two spellings of one shard are two era files and two published sets, for one
/// channel. Leaving the key out is how a block says it carries the default.
#[test]
fn a_block_may_not_spell_the_default_shard() {
    let mut doc = Doc::valid();
    doc.feed = doc.feed.replace(
        &format!("channel_id = {CHANNEL_ID}"),
        &format!(
            "channel_id = {CHANNEL_ID}\nshard = \"{}\"",
            dz_adapter_core::DEFAULT_SHARD
        ),
    );
    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    assert!(
        matches!(error, StartupError::ReservedShardName { .. }),
        "{error}"
    );
    assert!(
        error.to_string().contains("Leave the key out"),
        "the refusal has to say what to do instead: {error}"
    );
}

/// A document naming no shard resolves every block to the default.
///
/// This is what a publisher with one channel per specification has always been,
/// and it has to keep being it: the change is additive or it is a migration.
#[test]
fn a_document_with_no_shard_key_resolves_to_the_default_shard() {
    let config = Document::parse(&Doc::valid().render())
        .expect("parses")
        .resolve()
        .expect("the fixture is a document a publisher can start on");
    for feed in &config.feeds {
        assert_eq!(feed.shard.as_str(), dz_adapter_core::DEFAULT_SHARD);
    }
    assert_eq!(
        config.shards().len(),
        1,
        "one shard, however many blocks carry it"
    );
}

/// Two enabled blocks claiming one `Channel ID` are refused, naming both.
///
/// **The mutant to check on this one is the check itself.** `channel_ids()`
/// sorts and dedups, so without it the document loads, one set of series is
/// pre-created, two channel instances write to it, and nothing anywhere says
/// so — not an error, not a counter, not a log line.
#[test]
fn two_blocks_claiming_one_channel_id_are_refused_naming_both() {
    let mut doc = Doc::valid();
    // The harness's own depth block, so this test is about the `Channel ID`
    // collision rather than about a market-by-price block's snapshot port —
    // which is refused first, and by a different check.
    let second = Doc::depth_feed_block().replace(
        &format!("channel_id = {DEPTH_CHANNEL_ID}"),
        &format!("channel_id = {CHANNEL_ID}"),
    );
    doc.feed = format!("{}\n{second}", doc.feed);

    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    let message = error.to_string();
    assert!(
        matches!(error, StartupError::DuplicateChannelId { .. }),
        "two blocks shared a Channel ID and it was accepted: {message}"
    );
    assert!(message.contains("top-of-book"), "{message}");
    assert!(message.contains("market-by-price"), "{message}");
}

/// And the shard, because the specification alone stopped identifying a block.
///
/// **This is the likely shape of the mistake now.** Two blocks of one
/// specification on different shards are ordinary, so a `Channel ID` collision
/// between them is what an operator will actually produce — and a message
/// naming only the specification prints the same word twice and sends them
/// looking for a duplicate that reads as one block.
#[test]
fn two_shards_of_one_specification_sharing_a_channel_id_are_refused_naming_both_shards() {
    let mut doc = Doc::valid();
    let second = doc
        .feed
        .replace(
            &format!("channel_id = {CHANNEL_ID}"),
            &format!("channel_id = {CHANNEL_ID}\nshard = \"beta\""),
        )
        .replace(
            &format!("mktdata_port = {MKTDATA_PORT}"),
            &format!("mktdata_port = {}", MKTDATA_PORT + 20),
        )
        .replace(
            &format!("refdata_port = {REFDATA_PORT}"),
            &format!("refdata_port = {}", REFDATA_PORT + 20),
        );
    doc.feed = format!("{}\n{second}", doc.feed);

    let error = Document::parse(&doc.render())
        .expect("parses")
        .resolve()
        .unwrap_err();
    let message = error.to_string();
    assert!(
        matches!(error, StartupError::DuplicateChannelId { .. }),
        "{message}"
    );
    // Both shards named. Without them the message says `top-of-book` twice and
    // identifies neither block.
    assert!(message.contains("`default`"), "{message}");
    assert!(message.contains("`beta`"), "{message}");
}
