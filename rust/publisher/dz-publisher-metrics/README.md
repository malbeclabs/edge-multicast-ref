# dz-publisher-metrics

The normative Prometheus set every DoubleZero Edge publisher emits, so one dashboard and one alert set work across the fleet.

```rust
use dz_publisher_metrics::{PortRole, PublisherMetrics, PublisherMetricsConfig, serve};

let metrics = Arc::new(PublisherMetrics::new(&PublisherMetricsConfig {
    venue: "example",
    source_id: 7,
    port_roles: &[PortRole::Mktdata, PortRole::Refdata],
    connections: &["primary", "backup"],
    channel_ids: &[0, 1],
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

`channel_ids` is the set that can get long, and it should be sized rather than discovered. One `Channel ID` is declared per channel instance, so a publisher operating several channels of one feed specification from one process declares one per `[[feed]]` block, and every family keyed on `channel_id` pre-creates a series for each of them — the sequence gauge once per port role, the heartbeat, last-published, manifest and instrument-count gauges once — before a single datagram is sent. Nothing else grows with them: the port-role and message-type families carry no `channel_id`, and a label value costs nothing, since the decimal string for every `Channel ID` is interned at first use rather than formatted per call.

## Constraints

- No method accepts an `instrument_id`.
- Every `reason`, `kind` and `outcome` is an enum, not a string.
- `venue` and `source_id` are constant labels on every series.
- [`venue_registry`](src/venue_registry.rs) takes venue-specific series but refuses the `dz_publisher_` prefix and the two constant label names; `render` re-checks what collectors gather, since a duplicate label or unencodable family would make Prometheus reject the whole scrape.

## Metrics

Thirty-six names across `ingress_*`, `book_*`, `refdata_*`, `egress_*`, latency histograms and process metrics; the source is the authority. Declared normative in the Feed Publisher Playbook, Phase 6.5.

Four further families, and two label values, are **proposals the playbook does not yet carry**. Each exists because work in this workspace produced a number with nowhere to go and refused to invent a series for it. They say so in their own `HELP` text and are listed separately from the normative set in `tests/normative_names.rs`:

| Proposal | Counts what no existing family could |
| --- | --- |
| `dz_publisher_lowering_refusals_total{reason}` | An event refused between the payload and the datagram. Parse errors are about reading upstream; egress errors are about a datagram and a socket. |
| `dz_publisher_ingress_connect_failures_total{reason}` | A connect that never produced a connection. All four reconnect reasons describe a session that existed and then stopped. |
| `dz_publisher_ingress_adapter_errors_total{reason}` | An adapter method that failed when the driver called it. Neither a parse error nor a reconnect. |
| `dz_publisher_channel_last_published_timestamp_seconds{channel_id}` | One channel's own silence. `dz_publisher_idle_guard_last_update_timestamp_seconds` is process-wide, so a busy feed holds it at *now* while a sibling is dead; the heartbeat gauge is per channel and paced by this publisher, so it is exactly what stays fresh on a dead one. |
| `not_carried_by_feed`, `malformed_message` on `dz_publisher_egress_errors_total` | The two codec refusals that had no reason. Values rather than families, because both are per-message send failures on the same `port_role` as the other five. |

Only the upstream's own activity sets the last-published gauge: a quote, a trade, a level update, a book clear or an instrument reset. Heartbeats, definitions, manifests and snapshots do not — all four are paced here, so all four go on being sent by a channel whose upstream has died.

A channel whose instruments are dormant is silent and healthy, so the rule for *this feed has died while its siblings have not* is a comparison between channels rather than a threshold on one:

```promql
  max without(channel_id) (dz_publisher_channel_last_published_timestamp_seconds > 0)
- ignoring(channel_id) group_right()
  (dz_publisher_channel_last_published_timestamp_seconds > 0)
> 900
```

It reads *this channel is fifteen minutes staler than the freshest channel of the same publisher*.

**The fifteen minutes is the operator's number and the rule is a heuristic.** What separates a dead channel from a dormant one is whether its instruments would have traded, and no series a publisher emits knows that: a genuinely quiet instrument set on a busy venue meets this rule too, and the only thing the comparison buys is that it is not defeated by the venue being closed — when every channel goes quiet together, none of them is stale relative to the others. Set the threshold from the venue's calendar, at longer than the quietest channel's longest legitimate gap between prints, and read a firing as *go and look* rather than as proof.

Two details of the expression are load-bearing. `ignoring(channel_id)` rather than `on(venue, source_id)` on both joins, because two redundant paths of one channel carry the same `venue` and `source_id`: what tells them apart is the scrape's own target labels, and ignoring one label keeps them. And `> 0` on both sides, because the pre-created 0 is the whole Unix epoch away from a real timestamp — a rule that included it would page for every channel that had not yet published, at any uptime, which is why no `dz_publisher_uptime_seconds` guard appears here.

A channel that has published nothing at all is the other half of the condition and is its own rule, because only an operator knows how long their venue's calendar makes that normal:

```promql
  dz_publisher_channel_last_published_timestamp_seconds == 0
and ignoring(channel_id) dz_publisher_uptime_seconds > 3600
```

`dz_publisher_uptime_seconds` is maintained here and refreshed on every scrape, so the `and on() dz_publisher_uptime_seconds > 60` guard several `HELP` strings recommend cannot be forgotten.

Use `LATENCY_BUCKETS` and `REFDATA_LOAD_DURATION_BUCKETS` rather than local buckets, or two venues' percentiles will not compare.
