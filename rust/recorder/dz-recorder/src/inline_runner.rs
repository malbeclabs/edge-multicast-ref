//! The record path for inline mode: a capture thread per feed, offering into a
//! ring the pipeline drains.
//!
//! # What this owns and what it does not
//!
//! The capture loop is here because the socket is here — `dz-recorder-inline`
//! never opens one, which is what lets every one of its tests run with no
//! privileges. Everything past the ring belongs to that crate.
//!
//! # It is the archive runner's shape, with the writer replaced by a ring
//!
//! [`pump`] and [`drain_and_stop`] are the archive path's own, unchanged. The
//! shutdown ordering they encode — take what the capture is already holding,
//! *then* stop it — is the part of this binary least worth rewriting: both live
//! sources report `Ended` as soon as their stop flag is set, so a shutdown that
//! stopped the capture first would discard every datagram its drain threads had
//! already queued.
//!
//! # A per-feed spool and a per-feed ledger, for the archive's own reason
//!
//! Archive mode gives each feed its own staging directory because two writers
//! sharing one would each see the other's open segment as an orphan and evict
//! it. The same holds here: two pipelines sharing a spool would each evict the
//! other's windows under a budget neither could account for, and two appending
//! to one ledger would interleave lines into a file that parses as neither.
//!
//! [`pump`]: crate::runner::pump
//! [`drain_and_stop`]: crate::runner::drain_and_stop

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dz_recorder_archive::JoinedRole;
use dz_recorder_clickhouse::ClickHouseSink;
use dz_recorder_core::Observer as _;
use dz_recorder_health::{
    FeedSeries, HealthMetrics, HealthMetricsConfig, HealthObserver, InstanceLimits,
};
use dz_recorder_inline::metrics::InlineMetrics;
use dz_recorder_inline::pipeline::{self, DerivationConfig, InlineCounters, Pipeline};
use dz_recorder_inline::ring::{ring, RingCounters, RingSender};
use dz_recorder_inline::spool::Spool;
use dz_recorder_inline::window::WindowBound;
use dz_recorder_load::Ledger;

use crate::endpoint::serve_rendering;
use crate::inline_config::InlineConfig;
use crate::runner::{drain_and_stop, now_ns, open_capture, pump, Capture, RunError};
use crate::startup::{FeedPlan, Plan};

/// How long the capture loop waits before noticing a shutdown.
const POLL: Duration = Duration::from_millis(100);

/// How long shutdown spends taking datagrams the capture already holds.
///
/// Bounded, because on a busy feed the queue is never empty and a drain that
/// waited for it to be would never end. The archive path's own value, for the
/// same reason.
const DRAIN_WINDOW: Duration = Duration::from_secs(2);
const DRAIN_POLL: Duration = Duration::from_millis(50);

/// What one feed's scrape reads.
struct Series {
    feed: String,
    ring: Arc<RingCounters>,
    counters: Arc<InlineCounters>,
    spool: Arc<std::sync::Mutex<Spool>>,
}

/// Records every configured feed until a signal or the bounded run ends.
///
/// # Errors
///
/// [`RunError`] if the metrics endpoint cannot bind, a capture cannot start, or
/// a feed's spool or ledger cannot be opened. Every one of them is refused
/// before a datagram is taken: a recorder that started on three feeds out of
/// four is a recorder whose rows have a hole nothing in them explains.
pub fn run(plan: &Plan, config: &InlineConfig, run_for: Option<Duration>) -> Result<(), RunError> {
    let series: Vec<FeedSeries<'_>> = plan
        .feeds
        .iter()
        .map(|feed| FeedSeries {
            feed: &feed.spec,
            port_roles: &feed.port_roles,
            channel_ids: &feed.expected_channel_ids,
            expected_sources: &feed.expected_sources,
            expected_magic: None,
        })
        .collect();
    // Unchanged from archive mode, and that is the point: the health tier means
    // the same thing in both arrangements, so a `dz_recorder_*` panel does not
    // have to know which one is running.
    let health = Arc::new(HealthMetrics::new(&HealthMetricsConfig {
        site: &plan.identity.site,
        recorder: &plan.identity.recorder,
        feeds: &series,
    }));
    let inline = Arc::new(InlineMetrics::new(
        &plan.identity.site,
        &plan.identity.recorder,
    ));

    // The budget is the host's and the feeds share the disk, exactly as the
    // staging budget is divided in archive mode.
    let feeds = plan.feeds.len().max(1) as u64;
    let spool_max_per_feed = config.inline.spool_max / feeds;

    let mut started = Vec::with_capacity(plan.feeds.len());
    let mut scraped = Vec::with_capacity(plan.feeds.len());
    for feed in &plan.feeds {
        let started_feed = start_feed(plan, feed, config, spool_max_per_feed, &health)?;
        scraped.push(Series {
            feed: feed.spec.clone(),
            ring: Arc::clone(started_feed.sender.counters()),
            counters: Arc::clone(started_feed.pipeline.counters()),
            spool: Arc::clone(started_feed.pipeline.spool()),
        });
        eprintln!(
            "dz-recorder: feed {} deriving into {}",
            feed.spec,
            config.inline.spool_dir.join(&feed.spec).display()
        );
        started.push(started_feed);
    }

    let endpoint = {
        let health = Arc::clone(&health);
        let inline = Arc::clone(&inline);
        serve_rendering(
            move || {
                let now = now_ns();
                // Sampled and rendered under one lock: the endpoint serves every
                // request on its own thread, and the inline counters are
                // advanced by a difference rather than assigned, so two scrapes
                // landing together would each add the same difference.
                let inline_text = inline.scrape(|m| {
                    for s in &scraped {
                        let spool = s
                            .spool
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        m.observe(&s.feed, &s.ring, &s.counters, &spool, now);
                    }
                });
                // Two families from two registries, concatenated. They are
                // disjoint, so there is nothing to merge.
                format!("{}{inline_text}", health.render())
            },
            plan.listen_addr,
        )
        .map_err(|source| RunError::Endpoint {
            addr: plan.listen_addr,
            source,
        })?
    };
    let bound = endpoint.local_addr().unwrap_or(plan.listen_addr);
    eprintln!("dz-recorder: metrics on http://{bound}/metrics");

    let shutdown = Arc::new(AtomicBool::new(false));
    crate::runner::install_signal_handler(&shutdown, "the window it is deriving");

    let threads: Vec<_> = started
        .into_iter()
        .map(|feed| {
            let name = feed.spec.clone();
            let stop = Arc::clone(&shutdown);
            let handle = std::thread::Builder::new()
                .name(format!("derive-{name}"))
                .spawn(move || feed.run(&stop))
                .expect("a thread per configured feed");
            (name, handle)
        })
        .collect();

    let deadline = run_for.map(|window| Instant::now() + window);
    loop {
        if shutdown.load(Ordering::Relaxed) || threads.iter().any(|(_, h)| h.is_finished()) {
            break;
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            break;
        }
        std::thread::sleep(POLL);
    }
    shutdown.store(true, Ordering::Relaxed);

    let mut failure = None;
    for (feed, handle) in threads {
        match handle.join() {
            Ok(()) => eprintln!("dz-recorder: feed {feed} stopped"),
            Err(_) => {
                let e = RunError::Panicked { feed };
                eprintln!("dz-recorder: {e}");
                failure = failure.or(Some(e));
            }
        }
    }
    // Held until every feed has finished, so a scrape taken during the shutdown
    // still sees the counters rather than a refused connection.
    drop(endpoint);
    failure.map_or(Ok(()), Err)
}

/// One feed, capturing into a ring the pipeline drains.
struct StartedFeed {
    spec: String,
    capture: Capture,
    observer: HealthObserver,
    sender: RingSender,
    pipeline: Pipeline,
}

fn start_feed(
    plan: &Plan,
    feed: &FeedPlan,
    config: &InlineConfig,
    spool_max: u64,
    health: &Arc<HealthMetrics>,
) -> Result<StartedFeed, RunError> {
    let capture = open_capture(plan, feed)?;
    let observer = HealthObserver::new(
        Arc::clone(health),
        &feed.spec,
        InstanceLimits::default(),
        crate::startup::drop_scope(plan.mode),
    )
    .map_err(|source| RunError::Health {
        feed: feed.spec.clone(),
        source,
    })?;

    let spool =
        Spool::open(config.inline.spool_dir.join(&feed.spec), spool_max).map_err(|source| {
            RunError::Spool {
                feed: feed.spec.clone(),
                message: source.to_string(),
            }
        })?;
    let ledger = Ledger::open(ledger_for(&config.inline.ledger, &feed.spec)).map_err(|source| {
        RunError::Ledger {
            feed: feed.spec.clone(),
            message: source.to_string(),
        }
    })?;

    let (sender, receiver) = ring(config.inline.ring_datagrams);
    let pipeline = pipeline::start(
        receiver,
        spool,
        ledger,
        ClickHouseSink::over_http(config.clickhouse.clone()),
        DerivationConfig {
            identity: plan.identity.clone(),
            feed: feed.spec.clone(),
            roles_joined: roles_joined(feed),
            // The capture's own scope, never a preference: the same value
            // archive mode declares in its segments, so a subtraction reads the
            // same word whichever arrangement produced the row.
            drop_scope: crate::startup::drop_scope(plan.mode),
            link_headers_captured: matches!(
                crate::startup::link_headers(plan.mode),
                dz_recorder_archive::LinkHeaders::Captured
            ),
            bound: WindowBound {
                bytes: config.inline.window_bytes,
                interval: config.inline.window_interval,
            },
        },
    );

    Ok(StartedFeed {
        spec: feed.spec.clone(),
        capture,
        observer,
        sender,
        pipeline,
    })
}

impl StartedFeed {
    /// The capture loop, and then the shutdown that keeps what it is holding.
    fn run(mut self, stop: &AtomicBool) {
        while !stop.load(Ordering::Relaxed) {
            let Self {
                capture,
                observer,
                sender,
                ..
            } = &mut self;
            let mut deliver = |dg: &dz_recorder_core::RecordedDatagram<'_>| {
                // The health tier first, because it is allocation-free and
                // cannot fail: it observes what arrived whether or not the
                // derivation is keeping up, which is what makes its counters
                // and the ring's drop counter comparable.
                observer.on_datagram(dg);
                // Never waits. A datagram that does not fit is dropped and its
                // loss charged to the next one that gets through.
                let _ = sender.offer(dg);
            };
            if pump(capture, &mut deliver, POLL).is_err() {
                eprintln!(
                    "dz-recorder: feed {}: the capture handle was lost",
                    self.spec
                );
                break;
            }
        }

        // Take what the capture is already holding, then stop it. In that
        // order: both live sources report `Ended` as soon as their stop flag is
        // set, so stopping first would discard datagrams that were received and
        // that the publisher will not send again.
        let Self {
            capture,
            observer,
            sender,
            ..
        } = &mut self;
        let mut deliver = |dg: &dz_recorder_core::RecordedDatagram<'_>| {
            observer.on_datagram(dg);
            let _ = sender.offer(dg);
        };
        let drained = drain_and_stop(capture, &mut deliver, DRAIN_WINDOW, DRAIN_POLL);

        // Read before the sender is handed over, because that is what it goes
        // with — and the ring's drop count is the one number in this line that
        // says whether the derivation kept up with the feed.
        let ring = Arc::clone(self.sender.counters());
        // The pipeline takes the sending end, which is what ends the open
        // window: everything drained above is derived and spooled rather than
        // abandoned.
        let counters = self.pipeline.stop(self.sender);
        eprintln!(
            "dz-recorder: feed {}: {drained} drained at shutdown, {} windows derived, {} landed, \
             {} ring drops, {} stage restarts",
            self.spec,
            counters.windows_derived(),
            counters.windows_landed(),
            ring.dropped(),
            counters.stage_restarts(),
        );
    }
}

/// What the recorder was asked to join, in the manifest's own shape.
///
/// A port that was never joined produces no data, and no data looks exactly
/// like a clean feed — so the coverage row carries the intent whichever
/// arrangement wrote it.
///
/// Built from the bindings rather than copied from a writer configuration,
/// because an inline plan has none: that is the whole point of it being
/// `Option`. The address is written only when the join actually named one — an
/// unspecified membership interface means route discovery was asked for, and
/// the address the kernel then picked is not something this build observed. An
/// unobserved value must not become a written one.
fn roles_joined(feed: &FeedPlan) -> Vec<JoinedRole> {
    feed.bindings
        .iter()
        .map(|binding| JoinedRole {
            role: binding.role.as_str().to_owned(),
            group: binding.group,
            port: binding.port,
            interface: feed.device.clone(),
            source: (!feed.membership_interface.is_unspecified())
                .then_some(feed.membership_interface),
        })
        .collect()
}

/// A ledger per feed, beside the configured path.
///
/// Two pipelines appending to one file would interleave their lines into
/// something that parses as neither, and the ledger is what a restart uses to
/// know which windows are already in the store. Beside rather than inside the
/// spool, because a file the spool's budget cannot classify is a file eviction
/// cannot reach.
fn ledger_for(base: &Path, feed: &str) -> PathBuf {
    let stem = base
        .file_stem()
        .map_or_else(|| "ledger".to_owned(), |s| s.to_string_lossy().into_owned());
    let extension = base
        .extension()
        .map_or_else(String::new, |e| format!(".{}", e.to_string_lossy()));
    base.with_file_name(format!("{stem}-{feed}{extension}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One ledger per feed, and the feed is in the name rather than in a
    /// directory: a reader listing the loader's own path sees which feeds a host
    /// derives without opening anything.
    #[test]
    fn a_ledger_is_named_for_the_feed_it_records() {
        let base = Path::new("/var/lib/dz-recorder/inline/ledger.jsonl");
        assert_eq!(
            ledger_for(base, "top-of-book"),
            Path::new("/var/lib/dz-recorder/inline/ledger-top-of-book.jsonl")
        );
        assert_ne!(
            ledger_for(base, "top-of-book"),
            ledger_for(base, "market-by-price"),
            "two feeds must not share a ledger"
        );
    }

    /// A configured path with no extension still yields one file per feed.
    #[test]
    fn a_ledger_path_without_an_extension_is_still_split_per_feed() {
        let base = Path::new("/var/lib/dz-recorder/inline/ledger");
        assert_eq!(
            ledger_for(base, "top-of-book"),
            Path::new("/var/lib/dz-recorder/inline/ledger-top-of-book")
        );
    }
}
