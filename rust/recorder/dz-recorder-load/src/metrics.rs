//! The normative `dz_loader_*` metric set.
//!
//! Built the way `dz-recorder-health` builds `dz_recorder_*`: nothing outside
//! this module constructs a metric, `site` and `recorder` are constant labels
//! applied once, and every family is created at startup so it renders at 0
//! before the first object. A metric that first appears after the event it counts
//! is a metric no dashboard can chart, and a panel that is blank because nothing
//! has happened yet is indistinguishable from one that is blank because the
//! loader is dead.
//!
//! # Lag is the metric this whole tier is gated on
//!
//! Objects are deleted under the recorder's staging budget, so a loader slower
//! than the write rate loses history permanently and silently. That makes lag a
//! first-class metric with an alert and not a log line, and it is published as
//! two numbers because either alone can mislead:
//! [`unloaded_objects`](LoaderMetrics::unloaded_objects) is how much is waiting,
//! and [`oldest_unloaded_age_seconds`](LoaderMetrics::oldest_unloaded_age_seconds)
//! is how close the oldest of it is to being evicted. A backlog of two hundred
//! young objects is a busy loader; one object an hour older than the eviction
//! window is history already gone.
//!
//! # And market data has a lag of its own, which is not that one
//!
//! The datagram tier loads **every** object; the derivation loads the objects of
//! the feeds it was turned on for, and it is off by default. So the two tiers
//! are behind on two different sets of objects, and one gauge cannot answer for
//! both: on a host where no feed derives — which is every host today —
//! `dz_loader_unloaded_objects` may be anything at all while nothing whatever is
//! at risk of being lost as market data, because none of those objects was ever
//! going to produce a row about an instrument.
//!
//! [`market_data_unloaded_objects`](LoaderMetrics::market_data_unloaded_objects)
//! and its age are the same two numbers over the objects derivation is on for.
//! They are always a subset, and reading the load's gauges as if they were these
//! is how a page gets sent about book rows for objects that hold none — or, in
//! the direction that costs history, how a feed that *is* deriving hides behind
//! a backlog of feeds that are not.
//!
//! # Why the last error is a timestamp and not a label
//!
//! A message is unbounded text written by a column store, and a label value
//! becomes a time series: one malformed row would open a series per distinct
//! server message and never close it. So the *fact* of an error is counted by
//! kind, its *time* is a gauge a panel can annotate, and its text goes to the
//! log and to `--check`, which are the two places somebody is reading prose.

use prometheus::{IntCounter, IntCounterVec, IntGauge, Opts, Registry, TextEncoder};

use dz_recorder_rows::Grain;

use crate::market_data::RefusalKind;

/// What went wrong, as a bounded label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// The object's sha256 is not the one its manifest states.
    Digest,
    /// The manifest beside the object was missing or unreadable.
    Manifest,
    /// The replay failed, or ended short of a block boundary.
    Replay,
    /// The archive does not state the scope its drop counts are valid at, or
    /// states two different ones.
    Scope,
    /// The destination refused the batch, or could not be reached.
    Sink,
    /// The ledger could not be written.
    Ledger,
    /// Reading the directory or an object failed.
    Io,
}

impl ErrorKind {
    pub const ALL: [Self; 7] = [
        Self::Digest,
        Self::Manifest,
        Self::Replay,
        Self::Scope,
        Self::Sink,
        Self::Ledger,
        Self::Io,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Digest => "digest",
            Self::Manifest => "manifest",
            Self::Replay => "replay",
            Self::Scope => "scope",
            Self::Sink => "sink",
            Self::Ledger => "ledger",
            Self::Io => "io",
        }
    }
}

/// Why an object in the directory was passed over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// Its `(object key, sha256)` is in the ledger. The ordinary case, and
    /// counted so that "nothing to do" is distinguishable from "not looking".
    AlreadyLoaded,
    /// Its manifest names another site or recorder. Counted rather than loaded,
    /// because a series labelled with this host's name for another host's
    /// archive is worse than a gap in the numbers.
    ForeignHost,
    /// A manifest with no object beside it, or an object with no manifest. The
    /// recorder writes the manifest first, so this is what a pass that ran
    /// during a publication sees, and it resolves itself on the next one.
    Unpaired,
    /// Its rows are already with the sink, waiting for the insert that carries
    /// them. It has no ledger entry by design — the entry is written when the
    /// rows land — so this is what stops the pass from deriving it again, and
    /// under a poll interval far shorter than `insert_max_delay` that is most
    /// of the passes an object is alive for.
    Held,
}

impl SkipReason {
    pub const ALL: [Self; 4] = [
        Self::AlreadyLoaded,
        Self::ForeignHost,
        Self::Unpaired,
        Self::Held,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AlreadyLoaded => "already_loaded",
            Self::ForeignHost => "foreign_host",
            Self::Unpaired => "unpaired",
            Self::Held => "held",
        }
    }
}

/// The whole `dz_loader_*` set.
#[derive(Debug)]
pub struct LoaderMetrics {
    registry: Registry,
    objects_loaded_total: IntCounter,
    objects_skipped_total: IntCounterVec,
    rows_written_total: IntCounterVec,
    batches_failed_total: IntCounter,
    bytes_read_total: IntCounter,
    bytes_written_total: IntCounter,
    errors_total: IntCounterVec,
    last_error_timestamp_seconds: IntGauge,
    passes_total: IntCounter,
    last_pass_timestamp_seconds: IntGauge,
    unloaded_objects: IntGauge,
    held_objects: IntGauge,
    oldest_unloaded_age_seconds: IntGauge,
    ledger_entries: IntGauge,
    market_data_unloaded_objects: IntGauge,
    market_data_oldest_unloaded_age_seconds: IntGauge,
    market_data_refused_total: IntCounterVec,
}

impl LoaderMetrics {
    /// Builds and pre-creates the whole set.
    #[must_use]
    pub fn new(site: &str, recorder: &str) -> Self {
        let registry = Registry::new();
        let labels = std::collections::HashMap::from([
            ("site".to_owned(), site.to_owned()),
            ("recorder".to_owned(), recorder.to_owned()),
        ]);

        let metrics = Self {
            objects_loaded_total: counter(
                &registry,
                "dz_loader_objects_loaded_total",
                "Objects whose whole row set landed and whose ledger entry was written.",
                &labels,
            ),
            objects_skipped_total: counter_vec(
                &registry,
                "dz_loader_objects_skipped_total",
                "Objects in the directory this pass did not load, by reason. \
                 already_loaded is the ordinary case and is counted so that \"nothing to \
                 do\" is distinguishable from \"not looking\".",
                &labels,
                &["reason"],
            ),
            rows_written_total: counter_vec(
                &registry,
                "dz_loader_rows_written_total",
                "Rows written, by grain. Per grain and not in total because the grains are \
                 orders of magnitude apart in volume: a load that wrote a hundred thousand \
                 datagram rows and no gap rows is not a load that went well.",
                &labels,
                &["grain"],
            ),
            batches_failed_total: counter(
                &registry,
                "dz_loader_batches_failed_total",
                "Requests that spent their attempts. Every one of them leaves an object \
                 unloaded, so this rising while dz_loader_objects_loaded_total does not is \
                 a destination problem and not a feed one.",
                &labels,
            ),
            bytes_read_total: counter(
                &registry,
                "dz_loader_bytes_read_total",
                "Object bytes read, which is what the digest was taken over. Compare \
                 against dz_loader_bytes_written_total: the ratio is why the rows travel \
                 and the objects stay local.",
                &labels,
            ),
            bytes_written_total: counter(
                &registry,
                "dz_loader_bytes_written_total",
                "Row bytes put on the wire, summed per request. Per request and not per \
                 object: an insert spans objects, so the rows of several of them have one \
                 length between them and dividing it up would be inventing a number.",
                &labels,
            ),
            errors_total: counter_vec(
                &registry,
                "dz_loader_errors_total",
                "Failures by kind. The message is not a label — it is unbounded text a \
                 column store writes, and one malformed row would open a series per \
                 distinct message and never close it. The text goes to the log and to \
                 --check.",
                &labels,
                &["kind"],
            ),
            last_error_timestamp_seconds: gauge(
                &registry,
                "dz_loader_last_error_timestamp_seconds",
                "When the last failure happened, for a panel to annotate. Zero means none \
                 since this process started.",
                &labels,
            ),
            passes_total: counter(
                &registry,
                "dz_loader_passes_total",
                "Walks of the objects directory that completed.",
                &labels,
            ),
            last_pass_timestamp_seconds: gauge(
                &registry,
                "dz_loader_last_pass_timestamp_seconds",
                "When the last walk finished. A loader whose pass count has stopped moving \
                 is stuck, and that is invisible in every other series here.",
                &labels,
            ),
            unloaded_objects: gauge(
                &registry,
                "dz_loader_unloaded_objects",
                "Objects in the directory with no ledger entry. Half of lag: how much is \
                 waiting.",
                &labels,
            ),
            held_objects: gauge(
                &registry,
                "dz_loader_held_objects",
                "Objects the sink has taken and not yet posted, so their rows are in memory \
                 rather than in the store. Compare against dz_loader_unloaded_objects: a \
                 backlog that is all held is a sink coalescing as designed, and one that is \
                 all underived is a loader behind. This is also why the unloaded count \
                 includes these — rows in memory are not loaded, and counting them as loaded \
                 would report a loader caught up while its last insert sat unsent.",
                &labels,
            ),
            oldest_unloaded_age_seconds: gauge(
                &registry,
                "dz_loader_oldest_unloaded_age_seconds",
                "How old the oldest unloaded object is, from its own receive window. THE \
                 GATE ON THIS WHOLE TIER: objects are evicted under the recorder's staging \
                 budget, so a loader slower than the write rate loses history permanently \
                 and silently. Alert on this against the eviction window, not on the \
                 backlog count — two hundred young objects is a busy loader, and one \
                 object older than the window is history already gone.",
                &labels,
            ),
            ledger_entries: gauge(
                &registry,
                "dz_loader_ledger_entries",
                "Lines in the load ledger after compaction.",
                &labels,
            ),
            market_data_unloaded_objects: gauge(
                &registry,
                "dz_loader_market_data_unloaded_objects",
                "Unloaded objects of a feed whose market data derivation is on. A subset of \
                 dz_loader_unloaded_objects and never the same number: derivation is per feed \
                 and off by default, so an object of a feed that does not derive is in that \
                 count and not in this one. Zero here with a backlog there means nothing is at \
                 risk as market data.",
                &labels,
            ),
            market_data_oldest_unloaded_age_seconds: gauge(
                &registry,
                "dz_loader_market_data_oldest_unloaded_age_seconds",
                "How old the oldest of those is, from its own receive window. THE GATE ON \
                 DERIVATION, and the reason it is not dz_loader_oldest_unloaded_age_seconds: \
                 that one rises for feeds nobody derives, so alerting on it for market data \
                 pages about objects holding no book rows — and, worse, lets a feed that IS \
                 deriving hide inside a backlog of feeds that are not. Alert on this against \
                 the recorder's eviction window.",
                &labels,
            ),
            market_data_refused_total: counter_vec(
                &registry,
                "dz_loader_market_data_refused_total",
                "Messages the derivation declined to attribute, by cause. Every one is a row \
                 that is not in a table, and a derivation that resolved nothing writes an \
                 empty event table — which is indistinguishable from a feed nobody published \
                 on. unresolved_instrument climbing from zero is reference data that never \
                 arrived, and it is the shape a wrong Magic or an unjoined refdata port takes.",
                &labels,
                &["reason"],
            ),
            // Last, so every borrow above has been released by the time the
            // registry itself is moved into the struct.
            registry,
        };

        // Pre-created, so every series renders at 0 before the first object.
        for reason in SkipReason::ALL {
            metrics
                .objects_skipped_total
                .with_label_values(&[reason.as_str()]);
        }
        for grain in Grain::ALL {
            metrics
                .rows_written_total
                .with_label_values(&[grain.table()]);
        }
        for kind in ErrorKind::ALL {
            metrics.errors_total.with_label_values(&[kind.as_str()]);
        }
        for reason in RefusalKind::ALL {
            metrics
                .market_data_refused_total
                .with_label_values(&[reason.as_str()]);
        }
        metrics
    }

    pub fn object_loaded(&self, written: &dz_recorder_rows::Written, bytes_read: u64) {
        self.objects_loaded_total.inc();
        self.bytes_read_total.inc_by(bytes_read);
        for grain in Grain::ALL {
            self.rows_written_total
                .with_label_values(&[grain.table()])
                .inc_by(written.rows(grain));
        }
    }

    /// Bytes one request put on the wire.
    ///
    /// Counted per request and never per object, which is why it is not part of
    /// [`object_loaded`](Self::object_loaded): once an insert spans objects the
    /// rows of four of them have one length between them, and dividing it up
    /// would be inventing a number. A sink that coalesces reports `0` bytes on
    /// the per-object count for exactly that reason, so a counter fed from
    /// there stayed at 0 for ever.
    pub fn bytes_posted(&self, bytes: u64) {
        self.bytes_written_total.inc_by(bytes);
    }

    pub fn object_skipped(&self, reason: SkipReason) {
        self.objects_skipped_total
            .with_label_values(&[reason.as_str()])
            .inc();
    }

    pub fn error(&self, kind: ErrorKind, now_unix_seconds: i64) {
        self.errors_total.with_label_values(&[kind.as_str()]).inc();
        self.last_error_timestamp_seconds.set(now_unix_seconds);
    }

    pub fn batch_failed(&self) {
        self.batches_failed_total.inc();
    }

    /// Both halves of lag, published together at the end of every pass.
    pub fn pass_finished(
        &self,
        unloaded: i64,
        held: i64,
        oldest_unloaded_age_seconds: i64,
        ledger_entries: i64,
        now_unix_seconds: i64,
    ) {
        self.unloaded_objects.set(unloaded);
        self.held_objects.set(held);
        self.oldest_unloaded_age_seconds
            .set(oldest_unloaded_age_seconds);
        self.ledger_entries.set(ledger_entries);
        self.passes_total.inc();
        self.last_pass_timestamp_seconds.set(now_unix_seconds);
    }

    /// The derivation's own lag, published beside the load's and never as it.
    ///
    /// Both halves again, because either alone misleads in exactly the way the
    /// load's do — and over the objects of feeds that derive, because those are
    /// the only objects whose market data an eviction can take.
    pub fn market_data_pass_finished(&self, unloaded: i64, oldest_unloaded_age_seconds: i64) {
        self.market_data_unloaded_objects.set(unloaded);
        self.market_data_oldest_unloaded_age_seconds
            .set(oldest_unloaded_age_seconds);
    }

    /// What one object's derivation declined to attribute.
    pub fn market_data_refused(&self, refusals: &[(RefusalKind, u64)]) {
        for (reason, count) in refusals {
            self.market_data_refused_total
                .with_label_values(&[reason.as_str()])
                .inc_by(*count);
        }
    }

    /// The exposition text `GET /metrics` serves.
    #[must_use]
    pub fn render(&self) -> String {
        let mut out = String::new();
        // An encoder that fails has produced nothing worth serving, and a
        // scrape that fails is visible on its own; the alternative is a panic
        // inside an HTTP handler.
        let _ = TextEncoder::new().encode_utf8(&self.registry.gather(), &mut out);
        out
    }
}

type Labels = std::collections::HashMap<String, String>;

fn counter(registry: &Registry, name: &str, help: &str, labels: &Labels) -> IntCounter {
    let metric = IntCounter::with_opts(Opts::new(name, help).const_labels(labels.clone()))
        .expect("a static metric definition");
    registry
        .register(Box::new(metric.clone()))
        .expect("static metric registration");
    metric
}

fn counter_vec(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &Labels,
    variable: &[&str],
) -> IntCounterVec {
    let metric = IntCounterVec::new(Opts::new(name, help).const_labels(labels.clone()), variable)
        .expect("a static metric definition");
    registry
        .register(Box::new(metric.clone()))
        .expect("static metric registration");
    metric
}

fn gauge(registry: &Registry, name: &str, help: &str, labels: &Labels) -> IntGauge {
    let metric = IntGauge::with_opts(Opts::new(name, help).const_labels(labels.clone()))
        .expect("a static metric definition");
    registry
        .register(Box::new(metric.clone()))
        .expect("static metric registration");
    metric
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every family renders at 0 before the first object.
    ///
    /// A metric that first appears after the event it counts is a metric no
    /// dashboard can chart, and a panel blank because nothing has happened yet
    /// is indistinguishable from one blank because the loader is dead.
    #[test]
    fn every_series_renders_at_zero_before_the_first_object() {
        let metrics = LoaderMetrics::new("site-1", "recorder-1");
        let text = metrics.render();

        for family in [
            "dz_loader_objects_loaded_total",
            "dz_loader_objects_skipped_total",
            "dz_loader_rows_written_total",
            "dz_loader_batches_failed_total",
            "dz_loader_bytes_read_total",
            "dz_loader_bytes_written_total",
            "dz_loader_errors_total",
            "dz_loader_last_error_timestamp_seconds",
            "dz_loader_passes_total",
            "dz_loader_last_pass_timestamp_seconds",
            "dz_loader_unloaded_objects",
            "dz_loader_held_objects",
            "dz_loader_oldest_unloaded_age_seconds",
            "dz_loader_ledger_entries",
            "dz_loader_market_data_unloaded_objects",
            "dz_loader_market_data_oldest_unloaded_age_seconds",
            "dz_loader_market_data_refused_total",
        ] {
            assert!(text.contains(family), "{family} is absent from:\n{text}");
        }
        // Every label value of every bounded family, so a reason or a grain that
        // has not happened yet still charts.
        for grain in Grain::ALL {
            assert!(
                text.contains(&format!("grain=\"{}\"", grain.table())),
                "{grain} is absent"
            );
        }
        for reason in SkipReason::ALL {
            assert!(text.contains(&format!("reason=\"{}\"", reason.as_str())));
        }
        for kind in ErrorKind::ALL {
            assert!(text.contains(&format!("kind=\"{}\"", kind.as_str())));
        }
        for reason in RefusalKind::ALL {
            assert!(
                text.contains(&format!("reason=\"{}\"", reason.as_str())),
                "{} is absent",
                reason.as_str()
            );
        }
    }

    /// `site` and `recorder` on every series, applied once, so there is no path
    /// to a `dz_loader_*` series that cannot say which host produced it.
    #[test]
    fn every_series_says_which_host_produced_it() {
        let metrics = LoaderMetrics::new("site-1", "recorder-1");
        for line in metrics
            .render()
            .lines()
            .filter(|l| l.starts_with("dz_loader_"))
        {
            assert!(line.contains("site=\"site-1\""), "{line}");
            assert!(line.contains("recorder=\"recorder-1\""), "{line}");
        }
        assert!(
            metrics
                .render()
                .lines()
                .any(|l| l.starts_with("dz_loader_")),
            "nothing was registered at all"
        );
    }

    /// The labels the health tier uses for the same things.
    ///
    /// A dashboard where the live panel and the historical panel disagree about
    /// what a recorder is teaches nobody anything, and the two halves are two
    /// processes with two metric sets — so the parity is asserted rather than
    /// intended.
    #[test]
    fn the_label_names_are_the_ones_the_health_tier_uses() {
        let text = LoaderMetrics::new("s", "r").render();
        assert!(text.contains("site=\"s\""));
        assert!(text.contains("recorder=\"r\""));
        // And nothing invents a second spelling of either.
        assert!(!text.contains("host="), "{text}");
        assert!(!text.contains("node="), "{text}");
    }

    /// Both halves of lag, and neither alone.
    #[test]
    fn a_pass_publishes_the_backlog_and_the_age_of_its_oldest() {
        let metrics = LoaderMetrics::new("s", "r");
        metrics.pass_finished(7, 3, 4_000, 12, 1_700_000_000);
        let text = metrics.render();
        assert!(
            text.contains("dz_loader_unloaded_objects{recorder=\"r\",site=\"s\"} 7"),
            "{text}"
        );
        assert!(
            text.contains("dz_loader_oldest_unloaded_age_seconds{recorder=\"r\",site=\"s\"} 4000"),
            "{text}"
        );
        assert!(text.contains("dz_loader_ledger_entries{recorder=\"r\",site=\"s\"} 12"));
        // The held count, which is the part of the backlog that is a sink
        // coalescing rather than a loader behind.
        assert!(
            text.contains("dz_loader_held_objects{recorder=\"r\",site=\"s\"} 3"),
            "{text}"
        );
        assert!(text.contains("dz_loader_passes_total{recorder=\"r\",site=\"s\"} 1"));
    }

    /// Two lags, two series, and the derivation's is the smaller one.
    ///
    /// The number that would be wrong if they shared a name is this one: a host
    /// with a backlog of objects and no feed deriving is a host with nothing at
    /// all at risk as market data, and a single gauge would have it paging.
    #[test]
    fn the_derivations_lag_is_its_own_series_and_not_the_loads() {
        let metrics = LoaderMetrics::new("s", "r");
        metrics.pass_finished(7, 3, 4_000, 12, 1_700_000_000);
        metrics.market_data_pass_finished(2, 900);
        let text = metrics.render();

        assert!(
            text.contains("dz_loader_market_data_unloaded_objects{recorder=\"r\",site=\"s\"} 2"),
            "{text}"
        );
        assert!(
            text.contains(
                "dz_loader_market_data_oldest_unloaded_age_seconds{recorder=\"r\",site=\"s\"} 900"
            ),
            "{text}"
        );
        // And the load's are untouched by it: seven objects are waiting, two of
        // them hold market data anybody asked for.
        assert!(
            text.contains("dz_loader_unloaded_objects{recorder=\"r\",site=\"s\"} 7"),
            "{text}"
        );
        assert!(
            text.contains("dz_loader_oldest_unloaded_age_seconds{recorder=\"r\",site=\"s\"} 4000"),
            "{text}"
        );
    }

    /// A refusal is a row that is not there, and the causes do not have the same
    /// answer.
    ///
    /// Counted rather than logged because the shape it takes downstream is an
    /// empty table, and an empty table is indistinguishable from a feed nobody
    /// published on.
    #[test]
    fn what_the_derivation_would_not_attribute_is_counted_by_cause() {
        let metrics = LoaderMetrics::new("s", "r");
        metrics.market_data_refused(&[
            (RefusalKind::UnresolvedInstrument, 12),
            (RefusalKind::StaleCycle, 1),
        ]);
        let text = metrics.render();
        assert!(
            text.contains(
                "dz_loader_market_data_refused_total{reason=\"unresolved_instrument\",\
                 recorder=\"r\",site=\"s\"} 12"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "dz_loader_market_data_refused_total{reason=\"stale_cycle\",recorder=\"r\",\
                 site=\"s\"} 1"
            ),
            "{text}"
        );
        // Only the bounded causes, and never a series per message.
        assert_eq!(
            text.lines()
                .filter(|l| l.starts_with("dz_loader_market_data_refused_total{"))
                .count(),
            RefusalKind::ALL.len()
        );
    }

    /// The message is not a label, and this is where that stays true.
    ///
    /// A column store's message is unbounded text, and a label value becomes a
    /// time series: one malformed row would open a series per distinct message
    /// and never close it.
    #[test]
    fn an_error_is_counted_by_kind_and_never_carries_its_message() {
        let metrics = LoaderMetrics::new("s", "r");
        metrics.error(ErrorKind::Sink, 1_700_000_123);
        let text = metrics.render();
        assert!(
            text.contains("dz_loader_errors_total{kind=\"sink\",recorder=\"r\",site=\"s\"} 1"),
            "{text}"
        );
        assert!(text.contains(
            "dz_loader_last_error_timestamp_seconds{recorder=\"r\",site=\"s\"} 1700000123"
        ));
        // Only the seven bounded kinds, and no series per message.
        assert_eq!(
            text.lines()
                .filter(|l| l.starts_with("dz_loader_errors_total{"))
                .count(),
            ErrorKind::ALL.len()
        );
    }

    #[test]
    fn rows_are_counted_per_grain_because_the_grains_are_orders_apart() {
        let metrics = LoaderMetrics::new("s", "r");
        let mut written = dz_recorder_rows::Written::default();
        written.add(dz_recorder_rows::Written::of(
            &dz_recorder_rows::RowBatch {
                datagram: vec![],
                ..dz_recorder_rows::RowBatch::default()
            },
            0,
        ));
        metrics.object_loaded(&written, 4096);
        let text = metrics.render();
        assert!(text.contains("dz_loader_objects_loaded_total{recorder=\"r\",site=\"s\"} 1"));
        assert!(text.contains("dz_loader_bytes_read_total{recorder=\"r\",site=\"s\"} 4096"));
        assert!(text.contains("grain=\"sequence_gap\""), "{text}");
    }
}
