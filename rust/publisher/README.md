# Publisher crates

What a publisher needs that is not the wire format. The wire format is in [`codec/`](../codec/); the boundary a venue implements is in [`adapter/`](../adapter/) and the transports in [`ingress/`](../ingress/).

| Crate | |
|---|---|
| [`dz-publisher-metrics`](dz-publisher-metrics/) | The normative `dz_publisher_*` Prometheus set and the `/metrics` endpoint |
| [`dz-publisher-lowering`](dz-publisher-lowering/) | Normalized venue events to wire messages: the one implementation every venue shares |
| [`dz-publisher-refdata`](dz-publisher-refdata/) | Instrument identity, the selection policy, and the definition cycle |
| [`dz-publisher-egress`](dz-publisher-egress/) | The transmitter, the per-channel sequencer, `Reset Count` across restarts, and the `DatagramSink` |
| [`dz-publisher-runtime`](dz-publisher-runtime/) | The crate a venue links: config composition, the adapter registry, the guards, the wiring |

A fleet dashboard only works if every publisher emits the same names, so publishers inherit the metric set rather than reimplementing it. The same argument runs through the rest: each of these owns a decision that a publisher re-deciding is a defect class rather than a matter of taste.

## Where the decisions went

| Concern | Owner | Why not the venue |
|---|---|---|
| `Instrument ID` minting and persistence, `Manifest Seq` | `-refdata` | IDs must survive a restart and resolve to a published definition; two writers means published IDs resolve to nothing |
| Decimal and contract scaling | `-lowering` | The conversion has distinct failure modes and each is a different operator action; a venue doing it inline reports none of them |
| `Update Flags`, `Action`, `Per-Instrument Seq` | `-lowering` | Each is derived, and two of them are bytes a venue was allowed to author and got wrong |
| `Sequence Number`, `Reset Count`, the datagram | `-egress` | Per channel instance, persisted across restarts, and capped by the specification |
| Config, guards, shutdown, `EndOfSession` | `-runtime` | Spec-timed, and the venue's `main` is one call into it |

## Composing one

`dz-publisher-runtime::run` takes an `AdapterRegistry` the venue's `main` populates. `[adapter] kind` resolves against it, and a `kind` naming an unregistered adapter is a startup error listing what *is* registered — never a fallback and never a default.

## Still planned

| Crate | Waits on |
|---|---|
| `dz-ingress-fix` and the other transports | A venue that needs one; `dz-ingress-websocket` is the shape they follow |
| Market-by-order support | `dz-edge-mbo`, which does not exist |
| The egress tee | A framing to write, which [`dz-adapter-uds`](../adapter/dz-adapter-uds/) now provides |

Design: [the publisher crates](../../docs/superpowers/specs/2026-08-26-edge-publisher-crates-design.md) and [the venue adapter interface](../../docs/superpowers/specs/2026-09-02-venue-adapter-interface-design.md).
