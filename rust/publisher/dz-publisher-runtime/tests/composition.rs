//! The composition itself: what order the send paths land in, and which shard
//! each one carries.
//!
//! # Why this file exists
//!
//! Nothing reached `run.rs`'s composition. Reversing the shard order there —
//! the edit that publishes each shard's instruments under another channel
//! instance's sequence series, which no subscriber can detect — left the whole
//! suite passing, and left the by-hand offline run passing too.
//!
//! Two things hid it. The end-to-end harness composes its **own** `Feeds`,
//! shard-outer and block-inner, so those tests assert the harness's ordering
//! rather than the runtime's. And the definition path is keyed on a shard's
//! *name* — `ShardFeeds` derives it from one of its own send paths — so a
//! reversal leaves every reference-data port carrying exactly its own shard's
//! definitions, which is what the offline run asserts. The event path is keyed
//! on the *index*, so a quote reaches another shard's pipeline, whose lowering
//! does not hold the instrument, and is dropped before any wire.
//!
//! So this file composes through `compose_feeds` — the real one — with ports
//! that are not sockets.
#![forbid(unsafe_code)]

mod harness;

use std::cell::RefCell;
use std::sync::Arc;

use dz_edge_mbp::MAGIC_MBP;
use dz_edge_tob::MAGIC_TOB;
use std::time::Duration;

use dz_adapter_core::EventSink as _;
use dz_publisher_egress::EraStore;
use dz_publisher_metrics::{PublisherMetrics, PublisherMetricsConfig};
use dz_publisher_refdata::{
    CycleSchedule, MemoryStore, Registry, RegistryConfig, SelectionPolicy, ShardConfig,
};
use dz_publisher_runtime::config::{Feed, FeedSpec, ShardName};
use dz_publisher_runtime::pipeline::Ports;
use dz_publisher_runtime::{compose_feeds, ManualClock, PortOpener, Publisher, StartupError};
use harness::{quote, two_shard_feeds, FakeAdapter};

/// A directory that goes away with the test.
///
/// Hand-rolled rather than a dependency, because this workspace has none for it
/// and `dz-publisher-egress`'s own era tests hand-roll the same thing: a state
/// directory is two `fs` calls and a `Drop`, and a dev-dependency for it would
/// be a dependency every consumer resolves.
struct TempStateDir {
    path: std::path::PathBuf,
}

impl TempStateDir {
    fn new(tag: &str) -> Self {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "dz-composition-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("a temporary state directory");
        Self { path }
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TempStateDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// A port opener that opens no socket.
///
/// It hands back the recording ports the end-to-end suites already use, and
/// keeps the order it was asked in — which is the other half of what these
/// tests assert: the composition has to walk a shard's blocks before moving on.
#[derive(Default)]
struct RecordingPorts {
    asked: RefCell<Vec<u8>>,
    metrics: Option<Arc<PublisherMetrics>>,
}

impl RecordingPorts {
    fn new(metrics: Arc<PublisherMetrics>) -> Self {
        Self {
            asked: RefCell::new(Vec::new()),
            metrics: Some(metrics),
        }
    }

    /// The `Channel ID`s it was asked to open, in order.
    fn asked(&self) -> Vec<u8> {
        self.asked.borrow().clone()
    }
}

impl PortOpener for RecordingPorts {
    fn open(&self, feed: &Feed) -> Result<Ports, StartupError> {
        self.asked.borrow_mut().push(feed.channel_id);
        let magic = match feed.spec {
            FeedSpec::TopOfBook => MAGIC_TOB,
            FeedSpec::MarketByPrice => MAGIC_MBP,
        };
        let metrics = self.metrics.as_ref().expect("metrics");
        // The recorders are dropped: what these tests read is the shape of the
        // composition, not what left a port.
        let (ports, _recorders) = harness::ports(feed, metrics, magic);
        Ok(ports)
    }
}

fn metrics_for(feeds: &[Feed]) -> Arc<PublisherMetrics> {
    let identity = feeds.first().expect("at least one feed");
    let mut port_roles = Vec::new();
    for feed in feeds {
        for role in feed.spec.port_roles() {
            if !port_roles.contains(role) {
                port_roles.push(*role);
            }
        }
    }
    let channel_ids: Vec<u8> = feeds.iter().map(|feed| feed.channel_id).collect();
    Arc::new(PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "a-venue",
        source_id: identity.source_id.get(),
        port_roles: &port_roles,
        connections: &["upstream"],
        channel_ids: &channel_ids,
        ingress_message_types: &["quote"],
    }))
}

/// The distinct shards of a feed list, in first-appearance order — what
/// `Config::shards()` answers, and what the registry's shard list is built from.
fn shards_of(feeds: &[Feed]) -> Vec<ShardName> {
    let mut shards: Vec<ShardName> = Vec::new();
    for feed in feeds {
        if !shards.contains(&feed.shard) {
            shards.push(feed.shard.clone());
        }
    }
    shards
}

/// **The test the perturbation was missing.** `Feeds` lands in the order the
/// shard list states, and each entry carries that shard's own `Channel ID`s.
///
/// Asserted as the whole sequence rather than as a membership, because the
/// failure this exists for is a *permutation*: every shard is present, every
/// channel instance exists, every port is open, and the index a routing
/// decision resolves against belongs to somebody else. A subscriber cannot see
/// it — the sequence series it reads is dense and its definitions are its own.
#[test]
fn the_composition_orders_the_shards_as_the_document_states_them() {
    let feeds = two_shard_feeds();
    let shards = shards_of(&feeds);
    let metrics = metrics_for(&feeds);
    let dir = TempStateDir::new("order");
    let eras = EraStore::open(dir.path()).expect("an era store");
    let ports = RecordingPorts::new(Arc::clone(&metrics));

    let composed =
        compose_feeds(&shards, &feeds, &eras, &metrics, &ports).expect("two shards compose");

    assert_eq!(composed.shard_count(), 2);
    let names: Vec<&str> = composed.shards().map(|shard| shard.name()).collect();
    let expected: Vec<&str> = shards.iter().map(ShardName::as_str).collect();
    assert_eq!(
        names, expected,
        "the send paths are in a different order from the shard list the \
         registry is built from"
    );

    // Each shard's own channel instances, and no other's. Both specifications
    // per shard, which is what makes a swap between them visible here.
    let carried: Vec<Vec<u8>> = composed
        .shards()
        .map(|shard| shard.channel_ids().collect())
        .collect();
    let mut per_shard: Vec<Vec<u8>> = Vec::new();
    for shard in &shards {
        per_shard.push(
            feeds
                .iter()
                .filter(|feed| feed.shard == *shard)
                .map(|feed| feed.channel_id)
                .collect(),
        );
    }
    assert_eq!(carried, per_shard);
}

/// A shard's blocks are opened before the next shard's.
///
/// Shard-outer and block-inner is what keeps a document that interleaves its
/// blocks from separating a shard's two specifications, and the opener's own
/// record is the only place that order is observable.
#[test]
fn a_shards_blocks_are_opened_together_before_the_next_shards() {
    let feeds = two_shard_feeds();
    let shards = shards_of(&feeds);
    let metrics = metrics_for(&feeds);
    let dir = TempStateDir::new("order");
    let eras = EraStore::open(dir.path()).expect("an era store");
    let ports = RecordingPorts::new(Arc::clone(&metrics));

    // The feed list is deliberately given interleaved: shard A's top-of-book,
    // shard A's depth, shard B's top-of-book, shard B's depth is already
    // shard-outer, so reorder it to interleave the shards and assert the
    // composition puts them back.
    let interleaved = vec![
        feeds[0].clone(),
        feeds[2].clone(),
        feeds[1].clone(),
        feeds[3].clone(),
    ];
    compose_feeds(&shards, &interleaved, &eras, &metrics, &ports).expect("composes");

    let asked = ports.asked();
    let first_shard: Vec<u8> = feeds
        .iter()
        .filter(|feed| feed.shard == shards[0])
        .map(|feed| feed.channel_id)
        .collect();
    assert_eq!(
        &asked[..first_shard.len()],
        &first_shard[..],
        "a document that interleaves its blocks separated a shard's \
         specifications: {asked:?}"
    );
}

/// The era file is per shard, and the composition is what asks for it.
///
/// One file per channel instance under the state directory, the default shard's
/// keeping the name it has always had. This is the third thing the composition
/// covers that nothing covered before: the store is real here, in a temporary
/// directory, because a fake would assert the fake.
#[test]
fn each_channel_instance_takes_its_own_era_file() {
    let feeds = two_shard_feeds();
    let shards = shards_of(&feeds);
    let metrics = metrics_for(&feeds);
    let dir = TempStateDir::new("order");
    let eras = EraStore::open(dir.path()).expect("an era store");
    let ports = RecordingPorts::new(Arc::clone(&metrics));

    compose_feeds(&shards, &feeds, &eras, &metrics, &ports).expect("composes");

    let mut written: Vec<String> = std::fs::read_dir(dir.path())
        .expect("readable")
        .map(|entry| {
            entry
                .expect("an entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|name| name.ends_with(".era"))
        .collect();
    written.sort();
    // Two shards of two specifications: four channel instances, four files, and
    // neither shard's pair shares one with the other's.
    assert_eq!(written.len(), 4, "{written:?}");
    assert_eq!(
        written
            .iter()
            .filter(|name| name.contains(harness::SHARD_A))
            .count(),
        2,
        "{written:?}"
    );
}

/// A shard the feed list does not mention is refused, naming it.
///
/// **This is the only way `StartupError::ShardWithNoFeed` can be reached.** No
/// document produces it and no resolved `Config` can either: `Config::shards()`
/// is the distinct shards *of the enabled blocks*, so the shard list and the
/// feed list cannot disagree there. The variant guards a refactor rather than a
/// state, and a variant nothing exercises is worse than no variant — so it is
/// exercised here, at the one altitude where the two lists arrive separately.
///
/// The alternative the refusal rejects is a skip, which would shift every later
/// shard's index one off the registry's.
#[test]
fn a_shard_with_no_block_is_refused_rather_than_skipped() {
    let feeds = two_shard_feeds();
    let mut shards = shards_of(&feeds);
    shards.push(ShardName::new("delta").expect("one lowercase path component"));
    let metrics = metrics_for(&feeds);
    let dir = TempStateDir::new("order");
    let eras = EraStore::open(dir.path()).expect("an era store");
    let ports = RecordingPorts::new(Arc::clone(&metrics));

    // `Feeds` is not `Debug` — it holds sockets — so the `Ok` side is discarded
    // by hand rather than through `expect_err`.
    let error = match compose_feeds(&shards, &feeds, &eras, &metrics, &ports) {
        Ok(_) => panic!("a shard with neither specification is not a shard"),
        Err(error) => error,
    };

    assert!(
        matches!(&error, StartupError::ShardWithNoFeed { shard } if shard == "delta"),
        "{error}"
    );
}

/// The era file's shard is one mapping, and it is the egress crate's.
///
/// `ShardName::era_shard` used to reimplement `Shard::resolve`, which had no
/// caller outside its own test — so the decision that the default shard
/// contributes no path component lived in two places. The mapping is what a
/// renamed era file costs: it reads as *no file*, resolves to the first era, and
/// a publisher on era 7 restarts on era 1 announcing nothing.
#[test]
fn the_default_shard_contributes_no_component_and_a_named_one_does() {
    let default = ShardName::default_shard();
    let alpha = ShardName::new("alpha").expect("one lowercase path component");

    assert_eq!(default.era_shard(), dz_publisher_egress::Shard::DEFAULT);
    assert_eq!(
        alpha.era_shard(),
        dz_publisher_egress::Shard::named("alpha")
    );
}

/// A shard index the guard admits and the send paths do not hold is **counted,
/// not a panic**.
///
/// The guard answers `true` for an instrument on no shard, deliberately, so
/// that the lowering refuses it as an unknown instrument — a better diagnostic
/// than *unroutable*. What that leaves is a send reached with an index the send
/// paths may not hold, and it used to `expect("checked above")`.
///
/// It survives in production only because the lowering opens with
/// `instruments.get(instrument)?` and the registry clears that table and the
/// slot together — an invariant in another crate that nothing at the send site
/// states. This test breaks that invariant on purpose: a registry that knows
/// two shards and send paths that hold one, which is the state a later refactor
/// of either list produces.
#[test]
fn a_send_for_a_shard_with_no_pipeline_is_counted_rather_than_a_panic() {
    let feeds_list = two_shard_feeds();
    let metrics = metrics_for(&feeds_list);
    let shards = shards_of(&feeds_list);
    let ports = RecordingPorts::new(Arc::clone(&metrics));
    let dir = TempStateDir::new("no-pipeline");
    let eras = EraStore::open(dir.path()).expect("an era store");

    // Send paths for the **first** shard only.
    let first_only: Vec<Feed> = feeds_list
        .iter()
        .filter(|feed| feed.shard == shards[0])
        .cloned()
        .collect();
    let feeds = compose_feeds(&shards[..1], &first_only, &eras, &metrics, &ports)
        .expect("one shard composes");

    // A registry that knows **both**, which is the disagreement.
    let refdata = Registry::open(
        RegistryConfig {
            source_id: feeds_list[0].source_id,
            shards: shards
                .iter()
                .map(|shard| ShardConfig {
                    name: shard.as_str().to_owned(),
                    channel_id: feeds_list
                        .iter()
                        .find(|feed| &feed.shard == shard)
                        .expect("a shard came from a feed")
                        .channel_id,
                })
                .collect(),
            selection: SelectionPolicy::new(8, 16, 8).expect("a coherent policy"),
            schedule: CycleSchedule::new(Duration::from_secs(30), 1232, 1),
        },
        MemoryStore::new(),
        ManualClock::at_unix_ns(1_700_000_000_000_000_000),
    )
    .expect("a registry");

    let mut publisher = Publisher::new(
        Arc::clone(&metrics),
        refdata,
        ManualClock::at_unix_ns(1_700_000_000_000_000_000),
        feeds_list[0].source_id,
        feeds,
        Duration::from_secs(3_600),
    );

    // Admitted on the second shard, whose send paths this publisher does not
    // hold.
    let mut adapter = FakeAdapter::on_shards(&[("B-D", harness::SHARD_B)]);
    assert!(publisher.poll_listings(&mut adapter));
    let on_b = adapter.handles()[0];

    let before = publisher.unroutable();
    publisher.event(quote(on_b, 1));

    assert_eq!(
        publisher.unroutable(),
        before + 1,
        "a send for a shard with no pipeline has to be counted, and it used to panic"
    );
}
