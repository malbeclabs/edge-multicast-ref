# dz-publisher-metrics

The normative Prometheus set every DoubleZero Edge publisher emits, so one dashboard and one alert set work across the fleet.

```rust
use dz_publisher_metrics::{PortRole, PublisherMetrics, PublisherMetricsConfig, serve};

let metrics = Arc::new(PublisherMetrics::new(&PublisherMetricsConfig {
    venue: "example",
    source_id: 7,
    port_roles: &[PortRole::Mktdata, PortRole::Refdata],
    connections: &["primary", "backup"],
    channels: &[("top-of-book", 0), ("market-by-price", 0)],
    ingress_message_types: &["trade", "book_delta"],
}));

let _server = serve(Arc::clone(&metrics), "127.0.0.1:9100".parse()?)?;

metrics.ingress().message("trade", "primary");
metrics.egress().datagram(PortRole::Mktdata);
```

Hold the value `serve` returns — it is a drop guard, and the endpoint stops when it is dropped. Bind it to a non-public interface: the exposition describes a live trading data path, its instrument set and its timing.

## Config

Every field names a set whose series are created at 0 up front, so an `== 0` alert can fire on a publisher that never started. Pre-creation is gated on what can actually happen: no `quote` on the refdata port, no heartbeat on a role the spec forbids one on, no manifest gauge without a refdata port.

`ingress_message_types` is the one open vocabulary; anything undeclared is counted under `other`, which is what bounds it.

`channels` is the set that can get long, and it should be sized rather than discovered. It holds one `(feed, Channel ID)` pair per `[[feed]]` block, the feed being the codec's `Feed::NAME` (`top-of-book`, `market-by-price`). A shard's two feeds may share a `Channel ID`, and each is still a channel instance of its own, so the pair and not the ID is the unit. The sequence gauge pre-creates a series per pair and port role, the heartbeat and last-published gauges once per pair, and the manifest and instrument-count gauges once per distinct `Channel ID`, because the published set behind one ID is one shard's whichever feed carries it — all before a single datagram is sent. Nothing else grows with them: the port-role and message-type families carry no `channel_id`, and a label value costs nothing, since the decimal string for every `Channel ID` is interned at first use rather than formatted per call.

## Constraints

- No method accepts an `instrument_id`.
- Every `reason`, `kind` and `outcome` is an enum, not a string.
- `venue` and `source_id` are constant labels on every series.
- [`venue_registry`](src/venue_registry.rs) takes venue-specific series but refuses the `dz_publisher_` prefix and the two constant label names; `render` re-checks what collectors gather, since a duplicate label or unencodable family would make Prometheus reject the whole scrape.

## Metrics

Thirty-six names across `ingress_*`, `book_*`, `refdata_*`, `egress_*`, latency histograms and process metrics; the source is the authority. Declared normative in the Feed Publisher Playbook, Phase 6.5.

Four further families, two label values and one label are **proposals the playbook does not yet carry**. Each exists because work in this workspace produced a number with nowhere to go and refused to invent a series for it. They say so in their own `HELP` text and are listed separately from the normative set in `tests/normative_names.rs`:

| Proposal | Counts what no existing family could |
| --- | --- |
| `dz_publisher_lowering_refusals_total{reason}` | An event refused between the payload and the datagram. Parse errors are about reading upstream; egress errors are about a datagram and a socket. |
| `dz_publisher_ingress_connect_failures_total{reason}` | A connect that never produced a connection. All four reconnect reasons describe a session that existed and then stopped. |
| `dz_publisher_ingress_adapter_errors_total{reason}` | An adapter method that failed when the driver called it. Neither a parse error nor a reconnect. |
| `dz_publisher_channel_last_published_timestamp_seconds{feed, channel_id}` | One channel's own silence. `dz_publisher_idle_guard_last_update_timestamp_seconds` is process-wide, so a busy feed holds it at *now* while a sibling is dead; the heartbeat gauge is per channel and paced by this publisher, so it is exactly what stays fresh on a dead one. |
| `feed` on `dz_publisher_egress_sequence_current`, `dz_publisher_egress_heartbeat_last_sent_timestamp_seconds` and `dz_publisher_channel_last_published_timestamp_seconds` | Which of a shard's two feeds a series is, when both share a `Channel ID`. Without it the two channel instances write one series. A publisher whose feeds have distinct IDs gains no series from it. |
| `not_carried_by_feed`, `malformed_message` on `dz_publisher_egress_errors_total` | The two codec refusals that had no reason. Values rather than families, because both are per-message send failures on the same `port_role` as the other five. |

Only the upstream's own activity sets the last-published gauge: a quote, a trade, a level update, a book clear or an instrument reset. Heartbeats, definitions, manifests and snapshots do not — all four are paced here, so all four go on being sent by a channel whose upstream has died.

A channel whose instruments are dormant is silent and healthy, so the rule for *this feed has died while its siblings have not* is a comparison between channels rather than a threshold on one:

```promql
  max without(channel_id) (dz_publisher_channel_last_published_timestamp_seconds > 0)
- ignoring(channel_id) group_right()
  (dz_publisher_channel_last_published_timestamp_seconds > 0)
> 900
```

It reads *this channel is fifteen minutes staler than the freshest channel of the same feed of the same publisher*. `max without(channel_id)` keeps `feed`, so the comparison is between channels of one feed: a Top-of-Book channel is measured against the freshest Top-of-Book channel and not against a Market-by-Price one.

**The fifteen minutes is the operator's number and the rule is a heuristic.** What separates a dead channel from a dormant one is whether its instruments would have traded, and no series a publisher emits knows that: a genuinely quiet instrument set on a busy venue meets this rule too, and the only thing the comparison buys is that it is not defeated by the venue being closed — when every channel goes quiet together, none of them is stale relative to the others. Set the threshold from the venue's calendar, at longer than the quietest channel's longest legitimate gap between prints, and read a firing as *go and look* rather than as proof.

**It is blind in the other direction too, and for a reason specific to this pair of feeds.** A trade is the one message both specifications carry: it is lowered once and handed to both send paths, so where a Top-of-Book channel and a Market-by-Price channel cover the same instruments, one trade refreshes both gauges. A venue whose level stream dies while its trades go on printing therefore keeps the two channels equally fresh and never makes this rule fire. Only the per-message-type egress rate separates them — `dz_publisher_egress_messages_total` carries it — so on a publisher with a depth feed, alert on that alongside this.

Two details of the expression are load-bearing. `ignoring(channel_id)` rather than `on(venue, source_id)` on both joins, because two redundant paths of one channel carry the same `venue` and `source_id`: what tells them apart is the scrape's own target labels, and ignoring one label keeps them. And `> 0` on both sides, because the pre-created 0 is the whole Unix epoch away from a real timestamp — a rule that included it would page for every channel that had not yet published, at any uptime, which is why no `dz_publisher_uptime_seconds` guard appears here.

A channel that has published nothing at all is the other half of the condition and is its own rule, because only an operator knows how long their venue's calendar makes that normal:

```promql
  dz_publisher_channel_last_published_timestamp_seconds == 0
and ignoring(channel_id, feed) dz_publisher_uptime_seconds > 3600
```

`ignoring(channel_id, feed)` and not `ignoring(channel_id)` alone: `dz_publisher_uptime_seconds` is the process's and carries no `feed`, so a join that kept `feed` in its key would match nothing.

`dz_publisher_egress_sequence_current` carries `feed` for the same reason it carries `port_role`: each value is one channel instance's own series. `sum by (channel_id)` over it adds a shard's two feeds' sequence numbers together, which is a number no datagram carries.

`dz_publisher_uptime_seconds` is maintained here and refreshed on every scrape, so the `and on() dz_publisher_uptime_seconds > 60` guard several `HELP` strings recommend cannot be forgotten.

That guard is also the whole of this rule's coverage, and it buys the silence at startup at a price worth saying out loud: uptime resets with the process, so a channel that has published nothing is invisible for the first hour of every process life, and on a publisher restarting more often than hourly it is invisible always. Pick the hour against how often this publisher actually restarts as well as against the venue's calendar, and treat a restart loop as hiding this rule rather than as a separate incident.

Use `LATENCY_BUCKETS` and `REFDATA_LOAD_DURATION_BUCKETS` rather than local buckets, or two venues' percentiles will not compare.
