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

## Constraints

- No method accepts an `instrument_id`.
- Every `reason`, `kind` and `outcome` is an enum, not a string.
- `venue` and `source_id` are constant labels on every series.
- [`venue_registry`](src/venue_registry.rs) takes venue-specific series but refuses the `dz_publisher_` prefix and the two constant label names; `render` re-checks what collectors gather, since a duplicate label or unencodable family would make Prometheus reject the whole scrape.

## Metrics

Thirty-six names across `ingress_*`, `book_*`, `refdata_*`, `egress_*`, latency histograms and process metrics; the source is the authority. Declared normative in the Feed Publisher Playbook, Phase 6.5.

Three further families, and two label values, are **proposals the playbook does not yet carry**. Each exists because work in this workspace produced a number with nowhere to go and refused to invent a series for it. They say so in their own `HELP` text and are listed separately from the normative set in `tests/normative_names.rs`:

| Proposal | Counts what no existing family could |
| --- | --- |
| `dz_publisher_lowering_refusals_total{reason}` | An event refused between the payload and the datagram. Parse errors are about reading upstream; egress errors are about a datagram and a socket. |
| `dz_publisher_ingress_connect_failures_total{reason}` | A connect that never produced a connection. All four reconnect reasons describe a session that existed and then stopped. |
| `dz_publisher_ingress_adapter_errors_total{reason}` | An adapter method that failed when the driver called it. Neither a parse error nor a reconnect. |
| `not_carried_by_feed`, `malformed_message` on `dz_publisher_egress_errors_total` | The two codec refusals that had no reason. Values rather than families, because both are per-message send failures on the same `port_role` as the other five. |

`dz_publisher_uptime_seconds` is maintained here and refreshed on every scrape, so the `and on() dz_publisher_uptime_seconds > 60` guard several `HELP` strings recommend cannot be forgotten.

Use `LATENCY_BUCKETS` and `REFDATA_LOAD_DURATION_BUCKETS` rather than local buckets, or two venues' percentiles will not compare.
