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
| `poll_upstream` | whatever is still outstanding on a connection that is already open — optional, and what a venue needs when its instrument set changes without a reconnect |
| `on_payload` | one upstream payload, mapped onto normalized events |
| `on_disconnected` | the session ended, and why |
| `snapshot` | the book it holds for one instrument, and how deep that book goes — depth feeds only |

Three rules that are easy to get wrong and expensive to get wrong:

- **An instrument admitted mid-session is not subscribed by admitting it.**
  `poll_listings` offers instruments to the publisher; it says nothing to the
  venue. A publisher whose venue lists a new instrument during a session will
  publish that instrument's definition, count it in the manifest, and receive
  nothing for it — the feed is healthy and the instrument is silent. If the
  venue's set can change without a reconnect, implement `poll_upstream` and
  write there what has not been written **on that connection**. Unlike
  re-offering a listing, this write is not deduplicated: what it queues reaches
  the venue every time it is queued.

- **`Update Flags` states presence, not change.** A side's *updated* and *gone*
  bits are mutually exclusive. This is the opposite of what the field's name
  suggests, and it is what conformant publishers do. The boundary expresses a
  side as `Gone` or `Present`, so a venue cannot state the combination the wire
  has no encoding for.
- **A quantity of zero is a removal and nothing else.** The `Action` is derived
  from the quantity plus a presence hint; a venue does not choose it.
- **`Depth Bound` of zero claims the complete book.** A depth adapter's
  `snapshot` returns a `DepthBound`, and `Complete` is a positive claim a
  subscriber is entitled to sum into available liquidity. An adapter that writes
  the top N of its book returns `DepthBound::levels(n)`; one that writes all of
  it returns `Complete`, and an empty book is legitimately complete. There is no
  default, because the value a default would pick is the strongest claim on the
  feed.

Keep the adapter's own book state machine and reuse the venue's existing decoder
if it has one. An adapter that folds the book a second way is validating itself.

### 2. Depend on these crates

A tag in this repository is the release; there is nothing on crates.io. See
[RELEASING.md](RELEASING.md) for the whole of it, including how to cut the next
one, but the two lines that matter here:

```toml
[workspace.dependencies]
dz-adapter-core = { git = "https://github.com/malbeclabs/edge-multicast-ref", tag = "v0.1.0" }
dz-publisher-runtime = { git = "https://github.com/malbeclabs/edge-multicast-ref", tag = "v0.1.0" }
dz-ingress-core = { git = "https://github.com/malbeclabs/edge-multicast-ref", tag = "v0.1.0", features = ["websocket"] }
# or, for a venue whose feed arrives over a session:
dz-ingress-fix = { git = "https://github.com/malbeclabs/edge-multicast-ref", tag = "v0.1.0" }
```

**Every crate from the same tag, pinned in one place.** Two tags in one graph
means two copies of `dz-edge-core`, and then `Scalar` from one is not `Scalar`
from the other — a type mismatch between two types with the same name, which is
among the least legible errors cargo produces. The transport marker features are
not optional decoration either: `[ingress] kind` refuses a transport the binary
does not link.

### 3. Write the binary

It is a registry and nothing else — the one thing the runtime cannot know is
which adapters a binary contains:

```rust
fn main() -> std::process::ExitCode {
    dz_publisher_runtime::run(
        AdapterRegistry::new()
            .with("a-venue-tob", |cx| Ok(Venue::single(
                Box::new(VenueAdapter::new(cx)?),
                venue_input(cx)?,
            ))),
    )
}
```

A venue with several upstreams for one feed builds one `Input` per
`cx.sources()` entry and returns them together — see
[Several sources for one feed](#several-sources-for-one-feed).

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

**A venue on the session transport writes its logon where it writes its
subscriptions.** `[ingress] kind = "fix"` resolves to `dz-ingress-fix`, which
owns the framing, the outbound sequence, the heartbeat cadence, the logout and
the close — everything except the one part that is genuinely the venue's, which
is the logon's own fields. Those are composed in `on_connected`, beside the
subscriptions, as a body of `tag=value` fields with no `8`, `9`, `34`, `52`,
`141` or `10` in it: the transport frames it, numbers it 1, states the reset
flag on it, and **reads the heartbeat interval out of it** rather than from any
key. Write the logon first and the subscriptions after it — that order is the
only way to express it, and a subscription written first is refused with nothing
sent. A connect at which the adapter writes no logon is a startup failure
naming `on_connected`, because a transport that logged on with a body it
composed itself would be signing for the venue.

An instrument admitted mid-session reaches a subscription through
`poll_upstream`, which is what a session transport needs and a re-subscribing
one hides: subscriptions live on the session, so without it the instrument
waits for a reconnect while the feed looks healthy. Write only what is
outstanding there — nothing on that path is deduplicated.

The session's own keys are `[source.upstream.session]`, a subtable of the
venue's own `upstream`:

```toml
[source.upstream.session]
endpoint = "203.0.113.10:9443"        # host:port, no scheme
server_name = "session.example.com"   # what the certificate is verified against
```

TLS is on and there is no key to turn it off anywhere but a loopback endpoint.
The outbound sequence resets at every logon and nothing is persisted, because a
resend delivers deltas whose value has expired while the publisher's own
snapshot recovery is a sequence-correct repair; a document asking for
continuity with `persist_sequence = true` is refused at load, naming the key. A
venue that requires continuity is one this transport does not serve.

### 4. Prove it offline before you point it at anything

`[adapter.replay]` substitutes recorded upstream bytes for the transport. The
adapter cannot tell the difference, which is the property that makes the
exercise worth anything: the real config document, the real registry, the real
adapter, the real lowering, real sockets, real teardown.

Lay recorded payloads out in name order, point `[adapter.replay] path` at them,
and read the other end with this repository's own Go parser — asserting
*values*, not datagram counts. Every end-to-end exercise here does exactly
that, and each one has found bugs that no unit test did.

### 5. Configuration

One document, checked at startup, with unknown keys refused — so a misspelled
section is a startup failure rather than a default nobody noticed.

```toml
venue = "a-venue"

[egress]
pin = "192.0.2.10"      # the source address to send from, not discovered
ttl = 1

[[feed]]
spec = "top-of-book"
# shard = "alpha"           # optional; absent is the default shard
channel_id = 0
source_id = 1
multicast_group = "233.252.0.10"
mktdata_port = 41000
refdata_port = 41001
# snapshot_port = 41002     # depth feeds only
# snapshot_cycle = "5s"     # depth feeds only: one full rotation of the book
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
  detection. Two `[[feed]]` blocks in one document sharing a `channel_id` are
  refused at load naming both, rather than started: the metric families keyed on
  `channel_id` cannot tell two channel instances apart, so the pair would
  pre-create one set of series and both would write to it with nothing saying
  so.
- **`shard` is optional, and an operator needs it only when one process carries
  several channels of one specification.** Absent means the default shard, which
  is what a publisher with one channel per specification has always been, so a
  document that never names one keeps meaning what it always meant. Blocks
  sharing a `shard` carry one published set — one `Instrument Count`, one
  `Manifest Seq`, one definition cycle — while each block stays its own channel
  instance, with its own sequence series, `Reset Count` and era. The venue's
  adapter admits an instrument by shard name and still cannot name the
  `Channel ID` that shard resolves to; the mapping between the two is this
  document's. Every shard carries a block for every specification any shard
  carries, and a document leaving one out is refused at load naming the shard
  and the specification it has no block for: an instrument admitted to a shard
  with no top-of-book block would have quotes that reach no wire.

  A name is one lowercase path component of at most sixty-four bytes, because it
  becomes one. Spelling the default shard's own token is refused rather than
  taken as a synonym: two spellings of one shard would be two era files. And the
  default shard keeps `<spec>.era` under `[refdata] state_dir` rather than
  gaining a shard component, so adding the key to a document for the first time
  renames no state a running deployment reads — a renamed era file reads as *no*
  era file, and a publisher that has been on era 7 for months would restart on
  era 1 and announce nothing.
- **`definition_cycle` and `idle_guard` are one answer per publisher**, even
  though they are written per feed. One reference-data registry serves every
  feed, because `Instrument ID` identity can only be one thing, and the idle
  guard measures one publisher's silence. Two enabled feeds stating different
  values is a startup error naming both, rather than the first block's answer
  quietly winning.
- **A depth feed with no `snapshot_cycle` cannot be joined mid-session.** It
  still emits the recovery snapshots a reset obliges, but a subscriber that
  arrives after the deltas started has nothing to build a book from — a level
  update states the resting quantity at a price, so nothing later corrects a
  missing start. The publisher says so at startup; both shipped publishers run
  a periodic snapshot, both at five seconds.
- **Addresses in examples are documentation ranges** (RFC 5737 and
  MCAST-TEST-NET). `scripts/check-public-repo-rules.sh` enforces that for code
  in this repository, and it is the same habit worth keeping in a config
  template: an address in an example is copied into production sooner or later.

### Several sources for one feed

A venue often carries the same book twice by different paths — a websocket and a
FIX session, a local socket and a remote stream, two validators of one chain.
They are **not the same stream**: conflation differs, per-connection sequencing
differs, and each arrives at its own moment. So which one publishes is a
decision, and `[[source]]` is where it is stated:

```toml
[[source]]
name = "ws"                 # the `connection` metric label, from the file
ingress = "websocket"
role = "primary"            # this one publishes

[source.upstream]
# the venue's own keys, for this source

[[source]]
name = "fix"
ingress = "fix"
role = "comparison"         # connected, driven, counted — for the race

[source.upstream]
```

- **Exactly one `primary`, publisher-wide, and it is a startup error
  otherwise.** Two primaries are two publishers' worth of events on the channel
  instances they reach: the `Sequence Number` series is per channel instance, so
  a subscriber's gap detection reads the two interleaved as its own losses and
  cannot tell which. None is a publisher whose data has no path to the wire at
  all, heartbeating channels it never fills.

  Publisher-wide and *not* per feed, and that is not a simplification. A per-feed
  rule would be a statement about routing this runtime does not do: every
  source's payloads reach one adapter, the adapter emits events, and no event
  carries the source it came from — so nothing here can confine one source's data
  to one feed. There is no `carries` key for the same reason. A key that reads as
  a partition while nothing partitions is worse than no key: it made two
  primaries with disjoint declarations resolve cleanly while both upstreams'
  events landed on one channel instance.
- **The transport is named once.** Either `[ingress] kind` for a publisher with
  one source, or `[[source]] ingress` per source. Both is refused: a key read
  only when another is absent is a key an operator cannot reason about. A
  document that names it per source need not write `[ingress]` at all.
- **The name in the file is the metric label.** `dz_publisher_ingress_*` carries
  `connection`, pre-created at 0 for every declared source, so a second upstream
  that never came up is a series sitting at zero rather than no series at all. A
  name with leading or trailing whitespace is refused rather than trimmed —
  `"ws"` and `"ws "` would be two series a dashboard cannot tell apart.
- **One session per source, and per nothing else.** One driver is opened per
  enabled `[[source]]`, so a publisher carrying sixty-two channel instances of
  one feed specification over one source opens **one** upstream connection: a
  shard partitions the published set and says nothing about how many upstream
  connections exist. That matters on a session transport, because a venue may
  permit one session per credential and answer a second logon by evicting the
  first.

  So **two enabled sources whose `credentials` tables are equal are refused at
  startup, naming both blocks.** That is the copy-paste failure: a second block
  with a new endpoint and the credential nobody changed. What the check cannot
  see is two *different* paths holding the same account — nothing here can know
  that, and its symptom is both sources reconnecting in step, with
  `dz_publisher_ingress_connection_state` alternating between them. The venue's
  own logon refusal is the authority.
- **Absent `[[source]]` is one source**, named by the transport the venue builds.
  Every document written before the array existed still means exactly that,
  including that its fatal errors end the process.

**The runtime does not merge two sources — the venue does.** Every source reaches
one adapter, and each payload carries the connection that delivered it, so the
adapter decides which of two prices is current and when to fail over. That is
the same rule as the book state machine, for the same reason: it follows the
venue's microstructure, and one shipped publisher already reconciles two
validator streams this way with a reorder window and a grace fallback.

Two consequences an operator has to know before configuring a second source.

**`role` is not a gate on what reaches the wire.** The runtime cannot keep a
`comparison` source off it, because the adapter emits events and no event
carries the source it came from. What it buys is the label, the startup check
above, what an analysis tier reads to know which side of a race is which — and
one runtime behaviour: **only a `primary`'s fatal error ends the process.** A
driver returns only on a fatal error, and those are the per-source configuration
faults found at connect: an invalid endpoint, a missing credential path, an
unsupported scheme. A mistyped URL on a comparison source is now that source's
driver dropped and named, with its `connection_state` left at 0 — the alert for
exactly this case — and the primary carrying on.

**Nothing retries that source, and a restart is what does.** Some of those
causes are only fatal for one attempt — a credential path that does not exist
*yet*, under late secret injection, is the plain one — and before this the
process exited and both sources came back together. So the trade has a cost,
and it is this: a fault that used to clear on a restart the process took itself
now needs one somebody takes, and the gauge sitting at 0 is the only thing that
says so. Watch `dz_publisher_ingress_connection_state == 0` per `connection`,
not just the process being up.

**An adapter serving two sources must key its per-connection state by
`conn`.** One adapter object receives every source's `on_connected` and
`on_disconnected`, so an adapter that keeps one upstream sequence cursor, one
authentication token, or one "have I subscribed yet" flag is correct with one
source and silently wrong with two: a comparison connection flaps, the
primary's cursor is cleared, and the primary's next payload is read as a
discontinuity — which an adapter that answers discontinuities with a reset turns
into an `InstrumentReset` and a recovery snapshot on the live wire, from a
connection that publishes nothing. Migration is one config block and one line in
the venue's `main` with the adapter untouched, which is exactly why this is
worth reading first.

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

### Pointing a feed at a mode

The configuration above is **archive mode**: `dz-recorder` writes objects and
`dz-recorder-load` derives rows from them. It is what a host recording a
production feed for evidence runs, and **the `[archive] staging_dir` and
`completed_dir` in that file are what select it** — there is no flag:

```bash
dz-recorder --config /etc/dz-recorder/recorder.toml
```

**Neither arrangement is a default and no flag names either.** Each is selected
by the resource only it can run on, and the four cases are all the cases there
are:

| `[archive]` directories | `--inline-config` | What runs |
|---|---|---|
| stated | not given | archive mode |
| not stated | given | inline mode |
| stated | given | **refused**, naming the key and the file |
| not stated | not given | **refused**, naming both |

So a host cannot drift between the two: changing arrangement takes an edit that
positively states the new one, and every half-finished edit is a non-zero exit
code before a socket is bound. Deleting the `[archive]` section to stop keeping
bytes for an afternoon reaches the fourth row and refuses — it does not become
inline mode. And nothing in any unit file, pipeline or runbook needs an edit for
this: an archive host selects its arrangement by saying what it already said.

**Inline mode** is the other arrangement — one process that captures the feed,
derives its rows and loads them, keeping no datagrams. It is for bringing a feed
up, for a host that was never going to keep the bytes, and for one deploy unit.
What it gives up is not small: a conformance rule written next month has nothing
to run against, a row cannot be re-derived, nothing verified the bytes the rows
came from, and **no market data rows are derived at all** — `event`,
`instrument` and `book_top` come from a codec walk that archive mode runs in the
loader. Read
[the two modes](rust/recorder/README.md#the-two-modes) before putting a host in
it.

**Two files, and neither has a password key.** The feed above stays in the
recorder's own file — group, ports, `expected_sources`, `site` and `recorder` —
because that file's `config_hash` is written into every row as provenance and a
database endpoint is not part of what a recorder does. Inline mode's own file
carries the window bound, the ring, the spool and its budget, the ledger and the
destination, and is passed beside it:

```bash
dz-recorder --config /etc/dz-recorder/recorder.toml \
            --inline-config /etc/dz-recorder/inline.toml
```

```toml
# /etc/dz-recorder/inline.toml — see rust/recorder/dz-recorder/inline.example.toml
[inline]
window_bytes    = "16MiB"
window_interval = "10s"
ring_datagrams  = 8192
spool_dir       = "/var/lib/dz-recorder-inline/spool"
spool_max       = "8GiB"
ledger          = "/var/lib/dz-recorder-inline/ledger.jsonl"

[clickhouse]
endpoint = "http://192.0.2.20:8123"
database = "recorder"
user     = "dz_loader"
```

- **The `[archive]` section comes out.** Nothing writes an object here, so a
  configuration stating both this file and a `staging_dir` or `completed_dir` is
  a contradiction rather than a mode, and is refused at startup naming the key
  and the file. Ignoring it quietly is how a host is believed to be keeping bytes
  for a year that it never kept for a second.
- **A configuration stating neither an archive nor this file is refused too**,
  naming both. Inline mode needs a spool directory, a ledger and a destination,
  archive mode needs its two directories, and there is no defensible value to
  invent for any of the five: a recorder that guessed a destination would load
  rows into a database nobody chose. Silence is not a mode.
- **No `[[market_data]]` entry is accepted**, and one is refused by name rather
  than ignored. `event`, `instrument` and `book_top` come from a codec walk, and
  nothing in the record path decodes a datagram — the rule that makes the
  transport grains trustworthy, since a message a decoder would reject still
  carries the sequence number whose absence is the finding. A feed whose market
  data rows are wanted runs archive mode, and the loader derives them.
- **`spool_dir` must exist and be writable by the service user**, and `ledger`
  must not be inside it. Both are refused at startup, the second for the reason
  the loader's ledger may not live inside its objects directory: a file the
  budget cannot classify is a file eviction cannot reach.
- **The password comes from `DZ_LOADER_CLICKHOUSE_PASSWORD_FILE`** — a systemd
  credential, readable by the service user alone — or from
  `DZ_LOADER_CLICKHOUSE_PASSWORD`, and from nowhere else. That is where the
  loader's already comes from; inline mode invents no second mechanism.
- **The build carries the mode by default.** `inline` is a default feature,
  because the arrangement is a property of a host's configuration and the
  released asset is one asset for the fleet — a build carrying one arrangement
  would have to be matched to configurations at deploy time. Add `--features tls`
  for an `https` destination: that endpoint is refused rather than silently
  downgraded to plain HTTP with the password on the wire. A binary built with
  `--no-default-features` is the record-only one and can only be in archive mode,
  so it refuses a configuration that selects inline mode by the feature's name.

**`--check` is what the deployment pipeline runs before it restarts anything**,
in either mode, and it is an `ExecStartPre` in the units:

```bash
dz-recorder --config /etc/dz-recorder/recorder.toml --check
dz-recorder --config /etc/dz-recorder/recorder.toml \
            --inline-config /etc/dz-recorder/inline.toml --check
```

It validates both files, plans the feeds — every refusal archive mode makes
about a group, a port role or an interface is made here too — prints which
arrangement is running and what it keeps, and asks the destination for
`SELECT 1`. **The arrangement is the first line of its output in both modes**, so
a deployment pipeline can read which one it has just deployed rather than infer
it from which keys were echoed back. It is also where a configuration stating
both arrangements, or neither, is caught: run as an `ExecStartPre`, an unclear
arrangement costs a failed pre-check rather than a recorder that will not
start. Nothing is bound, nothing is created and nothing is joined, and the
spool and the ledger are not touched, so it is safe against a host that is
already recording. A gate that passed without reaching the destination would let
a pipeline restart a recorder that cannot write.

**Alert on the age of the oldest unposted window, and never on the eviction
counter.** A full spool budget evicts on every window at steady state by design,
so the counter rises whether or not anything is wrong; one window older than the
eviction horizon is history already gone, and no re-run recovers it. This is the
same rule as
[`dz_loader_oldest_unloaded_age_seconds`](rust/recorder/dz-recorder-load/README.md#the-gate-on-that-arrangement)
in archive mode, over windows instead of objects. Size `spool_max` against the
column-store outage the host intends to survive.

### Auditing what was recorded

The point of keeping the bytes is answering *did the publisher send what the
spec says it must, and did it arrive?* after the fact. The analysis tier reads
an archive back and, for a feed whose publisher is built on these crates, can
re-lower the venue's own recorded events offline and join them to the recorded
multicast on `(Instrument ID, Per-Instrument Seq)` — a diff rather than a guess.
Two limits worth knowing before relying on it: a factor the publisher applied
that the wire does not carry cannot be recovered offline, and the join needs the
recorded reference data as well as the market data.

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
- [ ] for a venue with several upstreams: one `[[source]]` per connection, exactly one `primary` **publisher-wide** (not per feed), and one `Input` built per `cx.sources()` entry
- [ ] for a venue with several upstreams: per-connection state in the adapter keyed by `conn`, and `on_payload` emitting from the connection it means to publish — the runtime cannot hold a `comparison` source's events back
- [ ] an offline replay run publishes, and this repository's Go parser reads the values back
- [ ] config reviewed for `pin`, `source_id`, `channel_id`, group and ports — and `shard`, for a venue whose instrument set is sharded across channels
- [ ] for a depth feed: `snapshot_cycle` set, and the adapter's `DepthBound` checked against what its book actually holds
- [ ] a recorder is configured for the feed before the publisher is pointed at production
- [ ] the recorder's arrangement is a decision, and the configuration states it: the two `[archive]` directories for a feed being recorded for evidence, `--inline-config` only where nobody was going to keep the bytes. `--check` prints which one on its first line, and refuses if the configuration states both or neither. In inline mode, the alert on the age of the oldest unposted window exists, and nothing on this host expects `event`, `instrument` or `book_top` rows
- [ ] metrics scraped, and the connection-state alert exists

**A new feed type**, additionally

- [ ] specified in `edge-feed-spec` and merged there first
- [ ] golden vectors in `testdata/golden/`, asserted by both languages
- [ ] codec crate with `MAGIC` and `CARRIES` transcribed from the message table
- [ ] the lowering builds every message the feed carries
- [ ] a Go parser exists, and a book-builder if the feed has book state
- [ ] the feed is nameable in a recorder configuration
