# A multicast transport, and the silence it has to notice

**Status:** draft, pending review
**Applies to:** `rust/ingress/`, venue publisher repositories
**Authority:** [`edge-feed-spec`](https://github.com/malbeclabs/edge-feed-spec), its [`GLOSSARY.md`](https://github.com/malbeclabs/edge-feed-spec/blob/main/GLOSSARY.md) and [`VERSIONING.md`](https://github.com/malbeclabs/edge-feed-spec/blob/main/VERSIONING.md)
**Builds on:** [2026-09-02-venue-adapter-interface-design.md](2026-09-02-venue-adapter-interface-design.md), [2026-08-26-edge-publisher-crates-design.md](2026-08-26-edge-publisher-crates-design.md), [2026-09-09-session-transport-design.md](2026-09-09-session-transport-design.md), [2026-09-09-polled-transport-design.md](2026-09-09-polled-transport-design.md)

---

## Naming

Three terms:

- **Payload**, not datagram. `dz-ingress-core` reserves *datagram* for what this
  family encodes and transmits — "the wire's unit is a datagram and it does not
  appear in this crate: nothing here has reached the encoder yet. What a
  transport delivers is a payload." Upstream traffic arriving on a joined group
  is a payload throughout.
- **Source IP address**, the glossary's qualified form, wherever this document
  means the layer-3 sender. The configuration key `source_address` matches it.
- **Group**, never *channel*. `Channel ID` is a wire field naming a shard of our
  own published set and is unrelated to the upstream's addressing.

---

## Purpose

`[ingress] kind` names `multicast` in its closed set, gated by a cargo feature,
and nothing constructs it. Every transport in the family pulls over a session or
an endpoint that answers. This is the first that is datagram-oriented and
receive-only.

**A membership is not a session.** Three assumptions `Driver` is written
against — a connection that can close, silence that means something, traffic
that proves liveness — are false here or true for different reasons. Each is
answered in the transport rather than by weakening the driver's contract, which
is one implementation for every venue and serves the three transports that
ship today.

---

## What the driver decides

Four properties of `driver.rs` constrain this design.

**A connection is dead when `recv` returns `Received::Idle`, and nothing else
says so.** The driver computes a receive budget as what is left of
`[ingress] idle_timeout` measured from `last_payload_ns`, hands it to `recv`,
and turns an elapsed budget into `DisconnectReason::Timeout`. That ends the
connection, tells the adapter, counts a reconnect, releases the transport
through `shutdown`, and waits out a backoff delay. With `idle_timeout` absent,
its default, the budget is `None` and the driver never decides a connection is
dead.

**`Received::Liveness` does not reset that budget.** Its rustdoc gives the
reason: a websocket whose subscription the venue quietly dropped answers pings
forever, so a guard that any traffic satisfied would never fire on the failure
it exists for. The budget counts time since the last payload.

**One delivered payload resets the whole delay sequence.** `Cycle::Proven` keys
on `delivered`, set by the first payload of the connection, so a connection that
received once and ended returns to the opening backoff window.

**The transport holds no retry of its own.** The polled transport's design
states it: the driver holds the cadence, the backoff, the failure counting and
the connection state.

---

## Liveness and the idle guard

Some groups on a live, healthy feed carry one heartbeat per second and no
application message for an entire session while neighbouring groups are busy. A
transport reading that as a dead connection would leave and rejoin a working
membership.

The hazard is in the classification. A heartbeat matches what
`Received::Liveness` describes — traffic proving the far side is alive and
carrying nothing for the adapter. A transport classifying it that way would stop
resetting the guard, trip `Timeout` on the next window, and reconnect-loop
against a feed with no fault in it.

**This transport never returns `Received::Liveness`. Every payload it receives
is `Received::Payload`.** Two reasons, each sufficient:

- **It cannot classify.** Telling a heartbeat from an application message means
  parsing the venue's header, which is `Adapter::on_payload`'s by contract. A
  transport that learned the upstream's message taxonomy would be
  venue-specific.
- **It has nothing else to offer.** There is no ping, no acknowledgement and no
  answered keepalive on a receive-only membership. The only evidence this
  transport holds that the path works is that something arrived.

`Input::recv` writes the liveness cadence down as an obligation: a transport
that neither delivers nor reports liveness, with no idle guard configured,
leaves the driver inside one `recv` for the life of the connection and is never
asked for `poll_upstream`. This transport meets it differently — it is
receive-only, so `poll_upstream` has nothing to produce and the `UPSTREAM_POLL`
cadence is inert.

**`idle_timeout` measures silence across one `[[source]]` block's membership
set.** Every payload resets that block's guard. Where a block holds one group,
the guard watches that group alone; where it holds several, a quiet group cannot
trip it while any other in the same block carries anything. Which shape applies
is the composition question in **Connection granularity**, and for a venue whose
partitions must be distinguishable the answer is one block per group.

That reading is what makes the guard the detector for the characteristic
multicast failure, recorded in the recorder's `rejoin` module: "A membership
goes away with the interface it was joined on, and nothing reports that: the
socket stays open, stays readable, and is permanently silent. Silence is
therefore the only symptom there is, and a cadence to act on it is the only
remedy." The recorder answers it with its own rejoin cadence because it has no
driver. Here the driver's reconnect is the rejoin: `shutdown` leaves and closes,
`connect` binds and joins again.

**`[ingress] idle_timeout` is absent by default across the family and must be
set for this transport**, to a value clearing the upstream's slowest heartbeat
interval across the whole membership set by a wide margin. Nothing can check it:
the transport cannot see `[ingress]`, which is the core's section, and the core
cannot know a `[[source]]` block is a multicast one. This is the structural
limit the polled transport documents for its own cadence, answered the same
way: a stated rule in the crate's rustdoc and in `BRINGING-UP-A-FEED.md`.

---

## The `Input` mapping

| `Input` | This transport |
|---|---|
| `connect(timeout)` | One socket per group, socket options set, every group joined on the stated interface. **All or none**: a partial set is torn down and reported as a connect failure |
| `recv(budget)` | The next payload from any joined group, as `Received::Payload` with the kernel's receive timestamp. An elapsed budget is `Received::Idle`. Never `Received::Liveness` |
| `send` | `IngressError::Fatal`. See Receive-only |
| `shutdown` | Leave every group, close every socket. Prompt and infallible, as the trait requires |
| `stamp_source` | `StampSource::Kernel` |

**`stamp_source` is the first use of the kernel case in this family.** Both
existing transports read the clock after the read returns. `SO_TIMESTAMPNS` on a
datagram socket is the case `Received::Payload`'s `ts_ns` field was written for
— "a kernel receive timestamp is the case that matters, since it predates every
scheduling delay between the packet and the read" — and the recorder already
sets it on its capture sockets. One `setsockopt` and one control-message read
per receive separates a venue-to-receive latency measuring the network from one
measuring our scheduler.

`StampSource` itself has no destination: the metric family is closed and has
neither a series nor a label for it, and nothing in the workspace calls
`stamp_source()`. Returning `Kernel` is correct and currently inert. What
carries the timestamp is `recv_ts_ns`.

---

## Connection granularity

`ConnectionId` is a `&'static str` declared at startup, because
`dz_publisher_ingress_connection_state` is pre-created at 0 per declared name so
an `== 0` alert can fire for an upstream that never came up. One driver opens
per enabled `[[source]]` block and per nothing else, so the block count is the
connection count.

**Where the upstream's partitions must be distinguishable, that is one block per
group.** `poll_listings` takes no `ConnectionId` and is called from a context
with no connection, so an adapter cannot learn a partition at admission; it must
record the mapping during `on_payload`, keyed on `Payload::connection`, and
replay it at the next poll. A single block for the whole membership set gives
every payload one name, and every partition collapses onto one with nothing to
detect it.

Composition then follows: exactly one enabled block must be `role = "primary"`
publisher-wide, so N blocks are one `primary` and N−1 `upstream-partition`, the
variant for an upstream carrying its instruments across several connections. A
fatal error ends the process under both roles, which is what keeps part of the
published set from going stale behind a healthy-looking publisher; `comparison`
is the role that does not, and is not this.

The role is a property of the `[[source]]` block, not of a group: a block may
hold several groups.

Two properties of the driver govern the single-block case, and are why it
remains correct where partitions need no distinguishing.

**`on_disconnected` is the adapter's reset point, keyed by connection.** The
driver's docstring names the hazard: an adapter clearing *the* cursor there is
correct with one connection and wrong with two, because one upstream flapping
clears the other's state and the next payload reads as a discontinuity — which
an adapter answering a discontinuity with a reset turns into an
`InstrumentReset` and a recovery snapshot on the live wire. One name per
membership set is sound **because `connect` is all-or-nothing**: a reconnect
rejoins every group, so every group's numbering restarts together, which is what
a single `on_disconnected` tells the adapter. Per-group joins under one shared
name would be unsound; per-group names would mean one driver, one backoff and
one pre-created series each.

**`Payload::connection` is the only thing distinguishing one upstream's data
from another's**, and after `on_payload` returns nothing else can. A single name
therefore costs the adapter the ability to tell which group a payload arrived
on. Where the venue's own header names its partition this costs nothing, since
the adapter reads it out of the bytes regardless. Where the group address is the
only partition identity, the existing mechanism answers it: **one `[[source]]`
block per group**, one driver each, one name each, at the price of a backoff and
a series set per group. That is a venue's decision, stated in its document.

---

## Disconnect reasons

| `DisconnectReason` | Reachable |
|---|---|
| `Timeout` | Yes, from the driver's idle guard. The transport never produces it |
| `RemoteClose` | Not truthfully. Nothing closes a membership from the far side; a UDP socket has no close to receive |
| `RateLimit` | No. A receiver is not paced by a transmitter that does not know it exists |
| `AuthExpired` | No. There is no credential and no session to expire |

Local failures — the interface going down, a socket error, a failed read — match
none of the four. This transport reports them as `IngressError::Ended { reason:
RemoteClose }` with the errno in the detail, the same choice
`IngressError::disconnect_reason` makes for a fatal fault on a live connection:
"the least specific of the four rather than an invented fifth."

**Named rather than proposed.** `remote_close` on a multicast transport means
the local path stopped working, which is true and not useful, and the detail
string is the only place the cause lives. A fifth value would widen a closed
set that `dz-adapter-core` and `dz-publisher-metrics` hold to each other by
arity.

---

## Membership

Membership is **any-source, IGMPv2**. Venue multicast paths carry IGMPv2 static
joins or PIM-SM. Source-specific membership is a non-goal.

A venue publishing over multicast publishes a known transmitter address per
group, and a payload from any other sender must not reach the adapter. Two
layers could hold that line, and one of them cannot.

**The adapter cannot check it.** `Payload` carries `bytes`, `recv_ts_ns` and
`connection`. There is no source IP address on it, no accessor for one, and
adding one would put a transport-layer fact on a struct the boundary exists to
keep transport-free. So the check belongs to the transport, and `source_address`
is required per group.

**The transport compares and drops.** Every received payload's sender is
compared against the group's stated `source_address`. A mismatch is dropped.
There is no configuration of this transport that accepts a payload from an
unstated sender.

A drop has nowhere to be counted: the `dz_publisher_ingress_*` family is closed
and `IngressObserver` has no method for one. It reaches a log line until that
family gains a series. **What this needs from the framework**, proposal 1.

**This inverts the recorder's gate deliberately.** `dz-recorder-capture` joins
any-source and runs a `SourceGate` that counts and never drops, stating the
trade in its rustdoc: "A wrongly recorded datagram is filterable afterwards on
the source address; a wrongly dropped one is gone." That is right for an
archive, where the record is evidence and over-capture is recoverable. It
inverts for a publisher: a payload from an unexpected sender that reaches
`on_payload` becomes published market data on our wire, under our `Source ID`,
inside a sequence series a subscriber trusts, and nothing downstream can filter
it back out.

The cost of a userspace comparison is that the payload has already been received
and queued. That is accepted: the alternative needs IGMPv3 on the path.

---

## Routes and interface binding

**The host must have a route to each group, and the membership must be joined
on the interface that route uses.** Both are deployment facts this transport
depends on and neither is its to arrange: a join on a host with no route to the
group, or on the wrong interface, succeeds locally and receives nothing.

Joining on the wrong interface receives nothing, reports no error, and leaves
every series in the publisher reading as a clean feed with a quiet venue. This
is the egress `ttl` problem from the receiving side — "a wrong hop count is
invisible from every direction an operator can look" — and gets the same
treatment: refuse the omission at load, make the one observable symptom
actionable.

**`interface` is required, with no route-discovery fallback.** The recorder
permits one, offering "leave `feed.interface` unset to accept route
discovery." The consequence differs: a recorder joining by the default route
archives nothing and the gap is visible in the archive afterwards; a publisher
doing it publishes nothing and the subscriber's first evidence is an empty feed.
`BRINGING-UP-A-FEED.md` says of the transmitting side that pinning "is not
optional in practice", naming the membership report as half of the failure —
"the IGMP report leaves by the wrong path too, so the symptom is silence that
reads as a clean feed." This transport makes that a refusal.

**It is the interface's IPv4 address, not a device name**, because the
membership is requested against an address and nothing in this crate resolves a
name to one. A value that is not an address is refused at load naming the key,
as the recorder refuses it.

| When | What |
|---|---|
| Load | A group address that is not a multicast address, an `interface` that is not an IPv4 address, a group with no `source_address`, a port of `0` — each refused naming the key and what would have been accepted. None starts the process |
| Start | The route to each group and the interface it resolves to are read and logged at connect, so a wrong binding is visible in the first lines of a run rather than inferred from silence |
| Construction | `IngressError::Fatal`, as `WebSocketInput::new` refuses a misspelled endpoint. A publisher asking for something impossible fails where it is diagnosable, not under a backoff |
| Connect | A failed bind or join is `IngressError::Connect`, counted by reason, with the membership set torn down. `connection_state` reaches 1 only once every join has succeeded, the rule the driver applies to subscriptions |
| Running | Silence across the membership set trips the idle guard, which reconnects and rejoins |

**The connect-failure reasons are imprecise in the same way.**
`ConnectFailureReason`'s seven values are written for a connection to a far
side. An interface address naming
no device on this host is reported as `Unresolved`, the nearest true statement
in the set; anything else the kernel refuses is `Refused`, with the errno in the
detail. The errno classes the recorder calls reprovision cases, where an
interface is not there yet on a host still being built, are retried rather than
fatal: the recorder reached that with only a sleep available, and this transport
has the driver's backoff.

---

## Backoff and membership churn

A process that crash-loops while joining and leaving puts churn on the
upstream's multicast routing, and nothing on our side of the path observes the
cost.

**`backoff.rs` already provides the remedy.** It draws equal jitter — uniform
over `[ceiling / 2, ceiling]`, clamped to the configured pair — which holds the
average delay at three quarters of the ceiling, giving a sustained attempt rate
of about four attempts per three ceilings against the two full jitter would
give. Each sequence is seeded from the clock and the connection's name, so two
memberships held by one publisher do not retry at the same instants. None of it
needs changing and none may be duplicated inside the transport.

**The existing defaults are wrong for this transport in one place.**
`Cycle::Proven` resets the delay sequence after a single payload. A membership
that joins, receives one heartbeat and loses its interface therefore never
leaves the opening window — at the documented pair of 500 ms and 30 s, about
four attempts per three seconds, indefinitely, each a leave and a join against
the upstream. For a session transport that is a publisher reconnecting; here it
is routing churn.

So `[ingress] reconnect_backoff_initial` for a multicast block belongs in
seconds rather than hundreds of milliseconds, and the crate documents the number
with that arithmetic beside it. It is not refused in code: the transport cannot
see `[ingress]`, and `[ingress]`'s keys are the policy every `[[source]]` block
shares, so a floor imposed here would bind a session block in the same document.
A publisher mixing transports and needing two sequences needs two documents, or
the per-block override in **What this needs from the framework**, proposal 3.

**`shutdown` leaves explicitly rather than relying on the socket closing.**
Dropping the socket achieves the same thing; doing it explicitly makes the
teardown a step a test can observe.

---

## Receive-only

**Nothing leaves this transport toward the upstream except membership reports.**
That is a property of the crate, not of a venue's configuration.

`Input::send` returns `IngressError::Fatal`, the variant the trait specifies for
"a message this transport cannot carry at all" and classes as the adapter or
the document to correct rather than anything a reconnect reaches. An adapter
writing into `on_connected`'s queue against a multicast `[[source]]` block has
a composition error, and the loud failure at the first connect is the one that
gets fixed.

- **`[ingress] rate_limit_per_second` is inert here.** It paces outbound
  messages and there are none. The crate states this rather than leaving a key
  that appears to do something.
- **A venue whose recovery is a request is not fully served by this transport.**
  Where a snapshot or a retransmit must be asked for, the asking needs a path
  this crate refuses to have. The existing mechanism answers it: a second
  `[[source]]` on a transport that can transmit, reaching the same adapter, with
  `Payload::connection` distinguishing them.

---

## Loss, reordering and duplication

UDP, and multicast UDP. Loss is ordinary, reordering is permitted, and
duplication arrives with redundant paths.

**This transport** hands every payload up whole and counts bytes through
`IngressObserver::bytes`. One `recv` returns one payload, so a block holding
several groups needs a rule for which socket goes first. Draining one socket
before looking at the next inverts receive order whenever a socket has two
payloads queued and another has one stamped between them.

**The rule is the earliest kernel timestamp, re-evaluated per delivery:** hold
at most one payload read from each ready socket, and return the one with the
lowest timestamp. A payload that arrives after a readiness check is stamped
later than everything already held, so it cannot be owed an earlier slot. That
makes delivery exact receive order across the block, not an approximation of
it, which is what offline re-lowering requires of an implementation that
"cannot be used here, and should say so rather than approximate it".

The cost is a one-payload lookahead per socket. That is not the reorder buffer
below: it holds what has arrived, in receive order, for as long as it takes to
compare timestamps, and never waits for a payload that has not arrived.

**It does not** buffer for reorder, request retransmission, detect gaps,
deduplicate, reassemble, or infer that a quiet group has lost anything. Each
requires reading the venue's sequence field, which means parsing.

**Sequence handling belongs above this crate.** The adapter is the only layer
that can read the upstream's numbering and the only layer that can declare a
book diverged, which it does through `EventSink::desynchronised` — the sink
method the driver's `PayloadSink` forwards so a publisher's recovery can run.
Our own wire's numbering is the egress side's and a different series.

**`IngressObserver::duplicate` stays uncalled, as it is everywhere else.** Its
rustdoc records why: "the boundary gives an adapter no way to report the
upstream's own sequence number or to declare a payload a repeat of one already
published, so the driver cannot tell." Two paths carrying one group is the
textbook duplicate and is still unreachable, for the same reason.

**Truncation is unreachable rather than counted.** The receive buffer is sized
at the largest payload UDP can carry, so no configuration exists in which a
payload arrives and this transport hands up part of it. The recorder truncates
deliberately, capping at the datagram size our own wire mandates; nothing here
caps an upstream.

---

## Configuration

**This adds no `[ingress]` keys.** `kind = "multicast"` is already a token in
the closed set, though `is_linked` is false in every binary today so it reaches
`KindNotLinked` until the feature carries a crate. The section's remaining keys
are the policy every `[[source]]` block shares. The
transport's keys go in its own subtable, the rule the session and polled
transports follow so a transport's keys and a venue's keys cannot collide.

```toml
[[source]]
name    = "..."              # the ConnectionId, and every metric label's value
role    = "primary"          # exactly one enabled primary publisher-wide;
                             # further blocks are "upstream-partition"
ingress = "multicast"

[source.upstream.multicast]
interface          = "..."   # this host's IPv4 address on the path the feed arrives on
recv_buffer_bytes  = 0       # requested SO_RCVBUF; the kernel clamps it

[[source.upstream.multicast.group]]
group          = "..."       # must be a multicast address
port           = 0
source_address = "..."       # the venue's transmitter; required

[source.upstream]
# the venue's own keys for this block
```

`deny_unknown_fields` on both tables, for the reason `[ingress]` carries it: the
audit's failure was a misspelled section that parsed cleanly, fell back to a
default, and ran something its operator did not believe it was running.

| Key | Default | Reasoning |
|---|---|---|
| `interface` | **none; required** | The silent failure this design is organised around. No route-discovery fallback, and an IPv4 address rather than a device name, because the membership is requested against an address |
| `group`, `port` | **none; required** | A group that is not a multicast address and a port of `0` are refused naming the key. `0` is what a half-written document carries |
| `source_address` | **none; required** | The glossary's qualified form; bare `source` is banned and `[[source]]` already means a block here. The transport compares every payload's sender against it and drops a mismatch |
| `recv_buffer_bytes` | a stated constant | A burst overrunning the socket's queue is loss this host caused. The number is a property of the venue's peak rate and the host, so it is an operator's. The kernel clamps a request above its limit |

Deliberately absent from this transport's own tables: `connect_timeout` and the
backoff pair, which are the family's and stated once in `[ingress]`; a receive
timeout, which is the budget the driver hands over; and any cadence of this
transport's own. Proposal 3 would let a `[[source]]` block override three
`[ingress]` values, which is the block's table and not this transport's.

`[ingress]` is one table for the whole document, so `idle_timeout` and the
backoff pair bind every block in it. A publisher mixing this transport with a
session one gets one window and one backoff for both, and the slowest heartbeat
in the document sets the detection latency for the busiest group.

---

## What this needs from the framework

Four things this transport cannot supply for itself, each stated as a proposal.
Proposals 1, 2 and 4 follow a shape the crate already has. Proposal 3 does not,
and says so.

### 1. Two families, because loss and refusal are different failures

Two numbers have nowhere to go. `SO_RXQ_OVFL` says the kernel dropped what the
network delivered, before this process read it. A sender that does not match a
group's `source_address` is read and then declined by this transport. The
`dz_publisher_ingress_*` family is closed, and `IngressObserver` has one method
per family and no defaults.

**Proposal: two families, not one.** One family under a shared `{reason}` label
would conflate a capacity problem with a configuration or security one, and an
operator needs them apart: the first says buy more buffer, the second says
someone is sending to this group who should not be.

```
dz_publisher_ingress_overflow_total{connection}
dz_publisher_ingress_refusals_total{reason, connection}
    reason = "unexpected_sender"
```

with `fn overflow(&self, count: u64, connection: &'static str)` and
`fn refusal(&self, reason: IngressRefusalReason, connection: &'static str)`.

Both names are already this workspace's. `dz_publisher_lowering_refusals_total`
is an `IntCounterVec` keyed on a reason, so `refusals_total{reason}` is an
established shape for *we declined to act on this*. The recorder calls kernel
loss `overflow_total`, read from `ControlMessageOwned::RxqOvfl`, so `overflow`
names the same thing the same way.

**Not `dropped`.** `Tee::dropped` already names a fan-out member removed from
the tee, and its own doc comment says that name is "for a log line and for
`Tee::dropped`. Never a metric." Reusing the word for a discarded payload would
give one term two meanings in one publisher.

### 2. `connection` on `reconnects_total` and `connect_failures_total`

**Proposal: add it to both.**

The test that forbids it gives the reason, and the reason is sound: "a label
that is not a dimension of what is being counted multiplies the series for
nothing. `connection` is left off `connect_failures_total` for the same reason
it is left off `reconnects_total`, which is its sibling: the two must aggregate
the same way."

With one upstream the connection is not a dimension. With a membership set
across several groups it is: a join that fails on one and a rejoin loop on
another render identically today. The sibling property the test protects is
preserved by adding the label to both.

`message` and `connection_state` already take `connection: &'static str`, so the
trait shape is unchanged. Pre-creation follows the existing cross-product: N
connections against `DisconnectReason::ALL` and against
`ConnectFailureReason`'s values, which for three groups is twelve and
twenty-one series.

Without this, the only per-group signal is `connection_state{connection}`, and
that reads 1 for a membership that is joined and permanently silent — the
failure this design is organised around.

### 3. Per-block overrides for `idle_timeout` and the backoff pair

**Configuration** states why one `[ingress]` table cannot serve two transports
in one document. The churn argument asks the multicast block for an opening
delay in seconds; a session block in the same document wants the family's
hundreds of milliseconds; and the transport cannot impose either, because it
cannot see `[ingress]`.

**Proposal: `idle_timeout`, `reconnect_backoff_initial` and
`reconnect_backoff_max` become optional keys on `[[source]]`, overriding the
`[ingress]` value of the same name.** `[ingress]` keeps the defaults and stays
the only place a single-transport publisher needs to look.

This is a new shape and the cost should be stated: a `[[source]]` block carries
`name`, `ingress`, `enabled`, `role`, `upstream` and `credentials` today, none
of which overrides an `[ingress]` default, and `upstream` is a transport's own
table rather than a policy override. So policy would appear in two places, and
an operator reading `[ingress]` alone would no longer see what a block runs
with. Two things contain that: the override set is closed to these three keys,
and the resolved value per block is logged at connect beside the route and
interface the same section already logs.

The alternative shape is per-kind defaults under `[ingress]`, which keeps policy
in one table but makes the kind a key within it and leaves two blocks of one
kind unable to differ. The per-block form is the one that answers the case this
transport actually has.

**One document per transport works today** and is the answer until this lands.

### 4. Publisher alert rules

No alert rule exists for `dz_publisher_*` in this repository or in the
specification repository. `demo/prometheus/prometheus.yml` is scrape
configuration with no `rule_files`, and the specification repository's only
rule file covers `dz_conformance_*`, which is the subscriber-side checker.

**Proposal: a rules file beside the metric set that defines the names.** Two
expressions exist already as prose and need only moving: the
`connection_state{connection} == 0` check, and the cross-channel staleness
expression in `dz-publisher-metrics/README.md`. A third becomes writable once
proposal 2 lands — a rejoin loop on one connection while its siblings are
healthy.

`BRINGING-UP-A-FEED.md` asks an operator to watch `reconnects_total` per
connection, which the label set does not permit. That text and proposal 2 are
the same change.

### Outside this crate's reach

- **Privileges and host state.** A membership needs the interface provisioned
  and a route to each group over it. Neither is this crate's to arrange, and
  both are what a bring-up meets first.
- **IPv4 only.** The keys above are IPv4, as is every multicast path in this
  repository today.

The recorder links no `Input` and no `Driver`, so running an independent adapter
at the recorder is unreachable for any venue today. This transport is a
prerequisite for ever doing it against a multicast one.

---

## Verification

Almost none of this needs a network.

- **Every refusal is a load-time test**: a group that is not a multicast
  address, an interface that is not an address, a missing `source_address`, a
  port of `0`, an unknown key. Each asserts the
  message names the key and what would have been accepted, the standard the
  core's `ConfigError` tests hold.
- **Every policy is a pure function**, as the recorder's rejoin policy is: the
  all-or-nothing teardown of a partial membership set, the errno classification
  into a connect-failure reason, and the sender comparison are each testable
  with no socket and no privileges.
- **The driver interaction is testable with the existing fakes.** `Clock` is
  injected and `tests/driver.rs` drives a fake `Input`, so the two claims that
  matter most are assertions: that a membership delivering one heartbeat per
  second never trips a correctly set idle guard, and that a membership
  delivering nothing does.
- **One loopback example covers the rest**, behind a feature, as the recorder's
  `IP_MULTICAST_LOOP` tests are: a real join on a real interface, a real
  receive, a kernel timestamp read back off the control message, and a leave.
- **The offline replay path cannot prove any of it.** The runtime's replay
  collapses every enabled block to one connection named after the primary, so
  nothing that turns on which connection delivered a payload is exercised. A
  composition of N blocks is proven by the loopback example or by a live run.

---

## Decisions

| Decision | Why |
|---|---|
| Every payload is `Received::Payload`; this transport never returns `Liveness` | Telling a heartbeat from an application message means parsing the venue's header, which is `on_payload`'s, and a receive-only membership holds no other evidence that the path works |
| `[ingress] idle_timeout` is required, set clear of the slowest heartbeat in the membership set | A membership lost with its interface leaves the socket open, readable and permanently silent, so silence is the only symptom and the driver's reconnect is the rejoin |
| `stamp_source` is `StampSource::Kernel` | `SO_TIMESTAMPNS` predates every scheduling delay between the packet and the read, which is the difference between measuring the network and measuring our scheduler |
| `connect` joins every group or none | One `on_disconnected` per membership set is sound only because a reconnect restarts every group's numbering together |
| One `[[source]]` block per group where the upstream's partitions must be distinguishable | `Payload::connection` is the only partition identity that survives `on_payload`, and `poll_listings` is called with no connection |
| Any-source IGMPv2, with the transport comparing every sender against `source_address` and dropping a mismatch | No venue path here offers source-specific membership, and a payload from an unstated sender that reaches `on_payload` becomes published market data nothing downstream can filter back out |
| `interface` is required, is an IPv4 address, and has no route-discovery fallback | A join on the wrong interface receives nothing, reports no error, and reads as a clean feed with a quiet venue |
| Across a block's sockets, the lowest kernel receive timestamp is delivered first, re-evaluated per delivery | Offline re-lowering requires exact receive order, and draining one socket at a time inverts it whenever a second socket is stamped between two of the first's payloads |
| `send` returns `IngressError::Fatal` | Nothing leaves a membership toward the upstream, so an adapter writing against a multicast block is a composition error, and the loud failure at the first connect is the one that gets fixed |
| A local failure is `Ended { reason: RemoteClose }` with the errno in the detail | The least specific of four values, in place of widening a closed set `dz-adapter-core` and `dz-publisher-metrics` hold to each other by arity |

---

## Non-goals

A sequence or gap-detection path. A retransmission or recovery request. Any
decoding of upstream payloads, heartbeats included. Source-specific membership,
and IGMPv3. IPv6. A reorder or deduplication buffer. A second backoff. Anything
about which groups a venue publishes or how its partitions are addressed.
