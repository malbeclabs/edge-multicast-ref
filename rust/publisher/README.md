# Publisher crates

What a publisher needs that is not the wire format. The wire format is in [`codec/`](../codec/).

| Crate | |
|---|---|
| [`dz-publisher-metrics`](dz-publisher-metrics/) | The normative `dz_publisher_*` Prometheus set and the `/metrics` endpoint |

A fleet dashboard only works if every publisher emits the same names, so publishers inherit the set rather than reimplementing it.

## Planned

Each waits on a second publisher to show what is genuinely common.

| Crate | Would own |
|---|---|
| `dz-publisher-egress` | Multicast sender: source address from the route, sequencing per channel instance, `Reset Count` across restarts |
| `dz-publisher-refdata` | Reference-data cadence: retransmission, manifest handling, listings and delistings |
| `dz-publisher-runtime` | Configuration, idle guard, consistency guard, exit reasons |
| `dz-ingress-*` | One crate per upstream transport — websocket, multicast, FIX — behind a common trait |

Design: [2026-08-26-edge-publisher-crates-design.md](../../docs/superpowers/specs/2026-08-26-edge-publisher-crates-design.md).
