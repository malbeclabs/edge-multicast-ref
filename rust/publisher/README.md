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

One name resolves without a venue registering it: `uds`, the built-in record adapter, for an integration that is not Rust and therefore cannot implement the trait. It is a registered kind and not a fallback — it is consulted after the venue's own entries, a venue registering the same name wins, and a `kind` naming neither is still the startup error. Its transport does not exist yet, so its `Input` refuses at connect and names `[adapter.replay]`, which is the path that works.

## Depth: the two things a snapshot needs

| Decision | Owner | Why there |
|---|---|---|
| `Depth Bound` — complete book, or top N | the **adapter**, returned from `snapshot` | The wire's `0` is a positive claim of completeness, so there is no honest default for a layer that does not hold the book. Returned rather than passed in, so it cannot be omitted |
| The cadence — `[[feed]] snapshot_cycle` | `-runtime` | One full pass over the published set, one instrument per derived tick. A recovery snapshot answers a reset; only a periodic one lets a subscriber join mid-session |

`snapshot_cycle` is optional, and absent means recovery snapshots and nothing else. Both shipped publishers run a periodic snapshot at five seconds; a depth feed configured without one says so at startup, because the symptom otherwise is a subscriber that can never build a book and a publisher that looks healthy throughout.

## Still planned

| Crate | Waits on |
|---|---|
| `dz-ingress-fix` and the other transports | A venue that needs one; `dz-ingress-websocket` is the shape they follow |
| `dz-ingress-uds` | The production half of the non-Rust path; the adapter and the record encoding exist, the socket reader does not |
| Market-by-order support | `dz-edge-mbo`, which does not exist |
| A level-budget snapshot scheduler | A published set large enough to need one: the rotation is one instrument per tick, and a set whose per-instrument tick falls below the runtime's own laps more slowly than configured |

Design: [the publisher crates](../../docs/superpowers/specs/2026-08-26-edge-publisher-crates-design.md) and [the venue adapter interface](../../docs/superpowers/specs/2026-09-02-venue-adapter-interface-design.md).
