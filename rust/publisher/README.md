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

## The shard and the channel instance

One `[[feed]]` block is one **channel instance**: its own `Channel ID`, its own group and ports, its own sequence series, its own `Reset Count`, its own era, its own snapshot cycle. `[[feed]] shard` names which partition of the instrument set the block carries, in the venue's own word, and several blocks may share one — a block of each specification for one shard is two channel instances carrying one published set. Absent is the default shard, which is what a publisher with one channel per specification has always been.

**The shard is the unit of reference data, and the channel instance is the unit of sequencing.** The two are different, and that difference is what the rest of this turns on:

| The shard owns | The channel instance owns |
|---|---|
| the published set and its `Instrument Count` | the `Sequence Number` series |
| `Manifest Seq` | `Reset Count`, and the era it is persisted as |
| `Valid` | the snapshot cycle |
| one `DefinitionPacer`, and so one definition cycle | |

Neither is the process, and reading either as the process is a wrong answer rather than a slow one. `reference-data/spec.md` increments `Manifest Seq` "every time the published instrument set changes on this channel" and defines `Valid` against "channel state", so one process-wide published set packed onto every refdata port gives a subscriber a manifest that advances for an admission on a channel it cannot see and an `Instrument Count` it will never receive that many definitions for. `GLOSSARY.md` puts the sequence series, the `Reset Count` and the snapshot cycle on the channel instance, so channel instances drawing eras from one counter get an era decided by their position in the document — and eventually one they have published under before, which a subscriber reads as no restart at all and answers by applying fresh deltas onto a stale book.

What does not partition is identity. One `Registry`, one `Instrument ID` minting table, one writer, one `[refdata] state_dir`: a registry per shard would be N identity spaces over one state directory, and the single-writer claim refuses the second at startup. The selection policy's caps stay publisher-wide too — they are a cap on what this publisher publishes — while each shard's `Instrument Count` is stated on its own channel, which is the combination that shows a shard approaching a cap that is not per shard.

**The default shard keeps the names it has**, and that is an upgrade property rather than a preference. Its era file is `<spec>.era` and not `<spec>.default.era`, because a renamed era file reads as *no* era file and resolves to the first era — a publisher on era 7 restarting on era 1 and announcing nothing is the corruption the corrupt-file refusal exists to prevent, delivered by the upgrade meant to be safe. And a document with no `shard` key means what it always meant, so there is no migration.

Refused at load, each for a failure that is otherwise silent:

| Refused | The failure it prevents |
|---|---|
| two blocks naming one `(spec, shard)` pair | two channel instances publishing one partition, whose numbering a subscriber on either reads as its own gaps |
| two blocks sharing a `channel_id` | `Config::channel_ids()` sorts and dedups, so one set of metric series is pre-created and two channel instances write to it with nothing saying so |
| a shard with no block for a specification another shard has one for | an instrument admitted to it has quotes that reach no wire, which is the check that makes `list_on` total |
| a shard name that is not one lowercase path component of at most sixty-four bytes | the name becomes a path component later, and one with a slash in it writes somewhere nobody configured |
| a block spelling the default shard's own token | two spellings of one shard, and so two era files for one channel instance |

A venue names a shard and nothing else about where an instrument goes. `ListingSink::list_on` takes the name; the `Channel ID`, the group, the ports, the sequence series and the era that shard resolves to remain the configuration's, and the mapping between the two is the operator's. `list` is the defaulted method, forwarding to `list_on` with `DEFAULT_SHARD`, so an adapter with no partition to state never sees a shard — and the direction is deliberate, because a defaulted `list_on` would let an implementor who did not update admit every instrument to the default shard, every shard collapsed onto one channel with no error, no counter and no log. An unknown name is refused rather than defaulted, and the shard is fixed at admission: a re-offer naming a different one is refused too, since no message in the family says an instrument moved and a subscriber on the old channel would see it stop updating.

## Composing one

`dz-publisher-runtime::run` takes an `AdapterRegistry` the venue's `main` populates. `[adapter] kind` resolves against it, and a `kind` naming an unregistered adapter is a startup error listing what *is* registered — never a fallback and never a default.

One name resolves without a venue registering it: `uds`, the built-in record adapter, for an integration that is not Rust and therefore cannot implement the trait. It is a registered kind and not a fallback — it is consulted after the venue's own entries, a venue registering the same name wins, and a `kind` naming neither is still the startup error. Its transport does not exist yet, so its `Input` refuses at connect and names `[adapter.replay]`, which is the path that works.

It serves a top-of-book feed and **refuses a depth one at startup**. The record encoding carries `Level` and `Clear`, so it is depth-capable on the delta path — but it holds no book by design, the source process having already applied the microstructure, so it can answer no snapshot. Run against a `market-by-price` feed it would publish deltas with no recovery snapshot after a reset and no periodic snapshot at all, which is the mid-session-join failure `snapshot_cycle` closes reopened one `kind` along. A depth feed needs an adapter that holds the book.

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
