# Bringing up a feed

From nothing to a feed that publishes, records and can be audited afterwards.

This is the path, in order, with the decision at each step and where the work
lands. It spans four places: this repository (the libraries), the venue's own
repository (the adapter and the binary), the feed specification
([edge-feed-spec](https://github.com/malbeclabs/edge-feed-spec)), and the
infrastructure repositories (addresses, hosts, rollout). Read
[GLOSSARY.md](https://github.com/malbeclabs/edge-feed-spec/blob/main/GLOSSARY.md)
first if `datagram`, `channel`, `era` and `port role` are not already fixed
terms for you.

## First: which of two jobs is this?

They share almost nothing, and starting the wrong one costs weeks.

| | **A venue on an existing feed** | **A new feed type** |
|---|---|---|
| Example | a second exchange on Top-of-Book | a feed with its own datagram magic and message table |
| Wire format | already specified and implemented | has to be specified, then implemented and cross-verified |
| Codec crate | none — reuse `dz-edge-tob` / `-mbp` / `-refdata` | a new one under [`rust/codec`](rust/codec/) |
| Subscriber | already exists | a new parser, and usually a book-builder |
| Golden vectors | already exist | new ones, asserted by both languages |
| Where the work is | the venue's repository, mostly | this repository, mostly |

Most work is the left-hand column. **If that is you, skip to
[The publisher](#the-publisher).** The right-hand column comes first only when
no existing feed can carry what the venue produces.

## A new feed type

Do these in order. Each step exists because the next one cannot be verified
without it.

1. **Specify it in `edge-feed-spec` and get that merged.** The message table,
   the field offsets, the port roles, the sequencing rules, the reset reasons.
   Nothing here may decide any of it, and a publisher that decided one of these
   for itself is the defect class this whole design exists to prevent.
2. **Write golden vectors** into [`testdata/golden/`](testdata/golden/). They
   are the cross-language contract: the Rust encoder and the Go decoder each
   assert against the bytes, and neither is checked against the other. Agreement
   between our own encoder and decoder proves nothing.
3. **Add the codec crate** under [`rust/codec`](rust/codec/), implementing
   `dz_edge_core::Feed`. Two constants carry the specification's own decisions:
   `MAGIC`, and `CARRIES` — the message table transcribed, which is what makes
   `DatagramBuilder::push` refuse a Type ID the feed does not list rather than
   emit a datagram no conformant subscriber can act on.
4. **Teach the lowering to build those messages**, in
   [`rust/publisher/dz-publisher-lowering`](rust/publisher/dz-publisher-lowering/).
   This is the only place events become wire messages, and it is deliberately
   the only place: `Per-Instrument Seq`, the flags byte and fixed-point scaling
   live here so that no venue can spell any of them differently.
5. **Add the normalized events the feed needs**, if the boundary cannot express
   them yet — in [`rust/adapter/dz-adapter-core`](rust/adapter/dz-adapter-core/).
   Adding a variant here is a deliberate widening of what every venue may say,
   so it wants the same review as a spec change.
6. **Write the Go parser** under [`go/`](go/), and a book-builder if the feed
   has book state. The parser is not optional: it is how a publisher is proven
   to be readable by something that was not written alongside it, and every
   end-to-end exercise in this repository reads its output.
7. **Add the feed to the recorder's spec list** so a recorder can be configured
   for it. Nothing in the record path decodes a datagram, so this is naming and
   accounting rather than parsing.

## The publisher

A venue owns two things: **its upstream protocol, and its own book state
machine.** It owns nothing else. Everything a specification already decided —
the `Instrument ID`, the `Source ID`, the `Channel ID`, the sequence numbers,
the scaling, the `Update Flags` byte, the `Action`, the datagram and its
1,232-byte cap — belongs to the crates here and is not expressible through the
boundary. Not by convention: there is no parameter to pass one through.

See [`rust/adapter/README.md`](rust/adapter/README.md) for the trait and
[`rust/publisher/README.md`](rust/publisher/README.md) for what sits behind it.

### 1. Implement the adapter, in the venue's repository

One type implementing `dz_adapter_core::Adapter`:

| Method | What the venue answers |
|---|---|
| `message_types` | the upstream message names it will report, for one metric label |
| `poll_listings` | the instruments it knows about, as `InstrumentSpec`s |
| `on_connected` | the subscription it wants sent, after every connect — reconnects included, which is what makes a silently lost subscription come back |
| `on_payload` | one upstream payload, mapped onto normalized events |
| `on_disconnected` | the session ended, and why |

Two rules that are easy to get wrong and expensive to get wrong:

- **`Update Flags` states presence, not change.** A side's *updated* and *gone*
  bits are mutually exclusive. This is the opposite of what the field's name
  suggests, and it is what conformant publishers do. The boundary expresses a
  side as `Gone` or `Present`, so a venue cannot state the combination the wire
  has no encoding for.
- **A quantity of zero is a removal and nothing else.** The `Action` is derived
  from the quantity plus a presence hint; a venue does not choose it.

Keep the adapter's own book state machine and reuse the venue's existing decoder
if it has one. An adapter that folds the book a second way is validating itself.

### 2. Write the binary

It is a registry and nothing else — the one thing the runtime cannot know is
which adapters a binary contains:

```rust
fn main() -> std::process::ExitCode {
    dz_publisher_runtime::run(
        AdapterRegistry::new()
            .with("a-venue-tob", |cx| Ok(Venue {
                adapter: Box::new(VenueAdapter::new(cx)?),
                input: venue_input(cx)?,
            })),
    )
}
```

`run()` owns configuration loading, the guards, the signals, the metrics, the
egress, the reference data and the teardown. There is no default adapter and no
fallback: a `[adapter] kind` this binary did not register is a startup error
naming every token it did, because *what is in this binary* is the question an
operator cannot answer from the config file in front of them.

The transport comes from [`rust/ingress`](rust/ingress/). A WebSocket venue that
authenticates its upgrade passes a header provider, which runs on **every**
connect attempt — a venue signing a fresh timestamp and headers computed once at
startup gives a publisher that connects and can then never reconnect. A
transport whose crate is not linked is refused at startup, so the binary depends
on `dz-ingress-core` with the marker feature for the transports it means to
allow.

### 3. Prove it offline before you point it at anything

`[adapter.replay]` substitutes recorded upstream bytes for the transport. The
adapter cannot tell the difference, which is the property that makes the
exercise worth anything: the real config document, the real registry, the real
adapter, the real lowering, real sockets, real teardown.

Lay recorded payloads out in name order, point `[adapter.replay] path` at them,
and read the other end with this repository's own Go parser — asserting
*values*, not datagram counts. Every end-to-end exercise here does exactly
that, and each one has found bugs that no unit test did.

### 4. Configuration

One document, checked at startup, with unknown keys refused — so a misspelled
section is a startup failure rather than a default nobody noticed.

```toml
venue = "a-venue"

[egress]
pin = "192.0.2.10"      # the source address to send from, not discovered
ttl = 1

[[feed]]
spec = "top-of-book"
channel_id = 0
source_id = 1
multicast_group = "233.252.0.10"
mktdata_port = 41000
refdata_port = 41001
# snapshot_port = 41002     # depth feeds only
heartbeat_interval = "1s"
definition_cycle = "30s"
manifest_cadence = "5s"
idle_guard = "30s"

[refdata]
state_dir = "/var/lib/a-venue-publisher"

[refdata.selection]
bootstrap_top_n = 64
max_published = 256
warn_published_above = 200

[ingress]
kind = "websocket"

[adapter]
kind = "a-venue-tob"

[adapter.upstream]
# the venue's own keys, deserialized by the venue's own code

[adapter.credentials]
# paths, never secrets
```

- **`pin` is not optional in practice.** An egress that resolves its source
  address off the default route sends from the wrong interface the moment the
  feed lives on a tunnel — and the IGMP report leaves by the wrong path too, so
  the symptom is silence that reads as a clean feed.
- **`source_id` and `channel_id` are identity on the wire.** Two publishers
  sharing a `Source ID` on one group are indistinguishable to a subscriber's gap
  detection.
- **Addresses in examples are documentation ranges** (RFC 5737 and
  MCAST-TEST-NET). `scripts/check-public-repo-rules.sh` enforces that for code
  in this repository, and it is the same habit worth keeping in a config
  template: an address in an example is copied into production sooner or later.

## The recorder

The recorder is agnostic to the feed, to the venue, and to whether it is reading
a socket or a file, and **nothing in its record path decodes a datagram** — a
message a decoder would reject is exactly the message the archive must hold,
because the evidence needed to diagnose that bug is what the bug destroys. So
recording a new feed is configuration, not code.

See [`rust/recorder/README.md`](rust/recorder/README.md).

```toml
site = "a-site"
recorder = "recorder-1"
env = "prod"

[[feed]]
spec = "top-of-book"
multicast_group = "233.252.0.10"
interface = "dz0"          # the device, not the address, in AF_PACKET mode
mktdata_port = 41000
refdata_port = 41001
expected_sources = ["192.0.2.10"]
expected_channel_ids = [0]

[capture]
mode = "afpacket"          # the network's copy, not one socket's
buffer = "64MiB"

[archive]
# rotation, compression, staging budget, where objects land

[health]
[metrics]
```

Three things to know before the first run:

- **`afpacket` needs `CAP_NET_RAW` and an Ethernet capture device.** The parse
  reads a 14-byte Ethernet header, so a handle on any other datalink is refused
  at open rather than drained — a tunnel with no link layer of its own opens on
  bare IP, and without the refusal every frame fails the parse, nothing is
  archived, and the recorder reports itself healthy against a live feed. Point
  the feed at a device that has an Ethernet link layer, or run the recorder in
  socket mode, which records on any device and declares its synthesised headers
  as synthesised. Socket mode is recorder-wide, so it applies to every feed in
  that configuration.
- **`expected_sources` and `expected_channel_ids` gate counting and alerting,
  never the archive.** A wrongly recorded datagram is filterable afterwards on
  its source address; a wrongly dropped one is gone.
- **The staging budget is enforced, and it evicts.** A recorder that blocked on
  a full disk would stall its drain thread and turn an object-storage outage
  into a feed-loss incident. It gives up history instead, counts what it gave
  up, and never gives up the segment it is publishing.

### Auditing what was recorded

The point of keeping the bytes is answering *did the publisher send what the
spec says it must, and did it arrive?* after the fact. The analysis tier reads
an archive back and, for a feed whose publisher is built on these crates, can
re-lower the venue's own recorded events offline and join them to the recorded
multicast on `(Instrument ID, Per-Instrument Seq)` — a diff rather than a guess.
Two limits worth knowing before relying on it: a factor the publisher applied
that the wire does not carry cannot be recovered offline, and the join needs the
recorded reference stream as well as the market data.

## The infrastructure repositories

This repository holds no addresses, no hosts and no inventory. What follows is
the shape of the work and the order that matters; the values live in the
infrastructure repositories, and each of those owns its own review.

1. **Allocate the multicast group and ports.** One group per feed, one port per
   port role, from the range your fleet owns. Two feeds sharing a group is the
   mistake that produces a subscriber decoding another feed's magic; write the
   allocation down where the next person will look for it, not only in the
   config that consumes it.
2. **Decide the publisher's egress interface and its source address**, and set
   `pin` to it. On a topology where the feed leaves over a tunnel, this is the
   difference between a live feed and silence.
3. **Provision the host**: the binary, a service unit, the state directory, the
   credentials as files with an owner and a mode, and — for a recorder —
   `CAP_NET_RAW`, the capture device and the staging disk. Publisher and
   recorder are separate concerns and usually separate hosts.
4. **Pin the version.** Deploy a version, not a branch. A pin that says which
   build is running is what makes an incident answerable; a role that installs
   *latest* is a fleet nobody can describe.
5. **Roll subscribers before publishers whenever the wire changes.** A
   subscriber implementing an older schema discards a datagram it cannot read
   rather than misreading it, so the symptom of the wrong order is **silence,
   not errors** — and silence is what a healthy feed also looks like from the
   publisher's side.
6. **Canary one publisher, then walk the rest.** Restarting an era is visible to
   every subscriber of that feed: a `Reset Count` change is a reset, and a
   subscriber is expected to drop its book and re-bootstrap. Doing that to a
   whole fleet at once is a decision, not a side effect of a deploy.
7. **Scrape the metrics before you need them.** The normative `dz_publisher_*`
   and `dz_recorder_*` sets are in
   [`rust/publisher/dz-publisher-metrics`](rust/publisher/dz-publisher-metrics/).
   The one series to alert on first is the connection-state gauge sitting at
   zero: a publisher whose upstream never came up looks identical, from the
   outside, to a market that is quiet.

## Checklists

**A venue on an existing feed**

- [ ] adapter implemented, with the venue's own book state machine reused rather than rewritten
- [ ] `poll_listings` produces the instruments, and can be told about ones that open later
- [ ] `on_connected` composes the subscription, and is idempotent across reconnects
- [ ] the binary registers the adapter under a `kind` token, and links the transports it allows
- [ ] an offline replay run publishes, and this repository's Go parser reads the values back
- [ ] config reviewed for `pin`, `source_id`, `channel_id`, group and ports
- [ ] a recorder is configured for the feed before the publisher is pointed at production
- [ ] metrics scraped, and the connection-state alert exists

**A new feed type**, additionally

- [ ] specified in `edge-feed-spec` and merged there first
- [ ] golden vectors in `testdata/golden/`, asserted by both languages
- [ ] codec crate with `MAGIC` and `CARRIES` transcribed from the message table
- [ ] the lowering builds every message the feed carries
- [ ] a Go parser exists, and a book-builder if the feed has book state
- [ ] the feed is nameable in a recorder configuration
