# A session transport, and where a venue's signature lives

**Status:** design. Answers a request to build the declared-and-unbuilt session transport, so that a venue integration stops carrying a session layer that is not its own.

## The question it starts from

`[ingress] kind = "fix"` is declared and documented as "a session-oriented order-entry protocol. Not yet built." So a venue reading its feed over one carries the whole thing itself: tag-value framing with `BodyLength` and the checksum, the timestamp format, the logon, heartbeat and market-data-request builders, and the session lifecycle over TLS — connect, logon, heartbeat cadence, sequence numbers, teardown.

About 870 lines of that is venue-independent. What is genuinely the venue's is three things: which tags its market-data messages carry, how those fold into a book, and the signature its logon requires. The first two are already the adapter's and are in the right place. The third is what this design has to put somewhere, and it is the reason the answer is not "move the 870 lines."

The variant's own doc comment is also wrong in a way worth fixing while we are here: the protocol carries market data as well as order entry, and describing it as order entry invites the reading that a market-data publisher should not name it.

## The crux: a venue cannot inject anything into a transport

Transports are constructed by the runtime from a closed `Kind` match, deliberately: "the family is fixed, it lives in this repository, and the set is therefore a closed enum and a total match." Adapters are the opposite — a registry the venue's own `main` populates.

So there is no seam through which a venue hands a logon signer to a transport this repository constructs. This is the same ordering problem the venue-metrics change met from the other direction, where a venue's collectors could not be handed a registry that did not exist yet and had to travel up instead.

**The answer is that the logon body is written by the adapter, and everything around it belongs to the transport.**

`on_connected` already exists for this, and already says so: "Write whatever must be sent upstream after a connection is established… Authentication and subscription frames go here." A venue composes the logon's venue-specific fields — the identity, the signature, whatever the venue's scheme requires — and the transport:

- frames it: `BodyLength`, the checksum, the field order the protocol mandates;
- numbers it, and every message after it, on the session's own outbound sequence;
- reads the heartbeat interval out of it and runs the cadence from that value, rather than from a key an operator could set to disagree with what was logged on with;
- refuses to send anything else before it, so a subscription cannot precede a logon on a session that has not been established.

The signature therefore stays in venue code, in the method that already writes at logon, and no injection point has to be invented. The transport reads the one field it must understand out of a message it is already framing, which is inside the job it already has.

**This is also what makes an upstream write that is not a connect load-bearing rather than convenient.** A session transport is precisely the one that cannot re-subscribe for reasons of its own: its subscriptions live on the session, and an instrument admitted mid-session waits for a reconnect unless the adapter can write again. That mechanism is a [separate design](2026-09-09-upstream-write-after-a-listing-change-design.md), and this transport is the case that measured it.

## One session per credential, made an operator's decision

A venue may permit one session per credential and answer a second logon by evicting the first. Two publishers, or one publisher with two sources, then take turns knocking each other off — and each looks, in isolation, like a venue that keeps closing the connection.

**The session count is already the operator's statement and not a consequence.** One driver is opened per enabled `[[source]]`, so sessions are per source: not per `[[feed]]`, not per shard, not per channel instance. A publisher carrying 62 channel instances of one feed specification over one session opens one session, because the shard is a partition of the *published set* and has nothing to do with how many upstream connections exist. That property is worth asserting in a test rather than being left as a thing that happens to be true.

**What must be refused at load is two enabled sources sharing a credential**, which is the copy-paste failure: a second `[[source]]` block with a new endpoint and the credential nobody changed. `[[source]] credentials` is a free table of paths, checked but not interpreted, and two enabled sources whose credential tables are *equal* are two logons with one credential. Table equality is computable without understanding what a credential is, so the refusal names both blocks and starts nothing.

**What it does not catch** is stated rather than implied: two different paths holding the same account. Nothing here can know that. What the design can say is what happens then — the eviction shows as both sources reconnecting in step, which is at least a symptom an operator can see, and the venue's own logon refusal is the authority. A check that pretended to cover it would be worse than one that names its limit.

## Sequence numbers reset at logon, and that is a decision

The protocol's session layer numbers messages in both directions and defines a resend for a gap. A transport can either persist its sequence state across restarts or reset it at every logon.

**This one resets, and the reason is what the feed is for.** A resend delivers the book updates that were missed — and a market-data consumer that receives them minutes later is applying deltas whose value has expired to a book it has already rebuilt. The publisher's own recovery path is better in every respect: it announces a reset, pauses the instrument, and republishes from a snapshot, which is a subscriber-visible, sequence-correct repair rather than a replay of stale intent.

Persisting session state would also put a second thing under the state directory whose corruption stops a publisher from starting — and the era store's own design argues at length about how expensive that file is to get wrong. Paying that for a resend the feed does not want is the wrong trade.

**A venue that requires sequence continuity is a venue this transport does not serve**, and it says so at load rather than logging on and misbehaving: a configuration asking for continuity is refused naming the key, because a transport that silently resets against a venue that expects continuity produces a session the venue tears down for a reason our logs will not carry.

## What the transport owns, and what it must not

**Owns:** the socket and TLS; the framing, in both directions, including the refusal of a message whose declared length or checksum does not hold; the timestamp format; the session lifecycle — logon, the heartbeat cadence, the test request that answers a suspicion of silence, the logout, and the orderly close; the outbound sequence; and the classification of every failure into an `IngressError`, which is the load-bearing part because a disconnect reason is a metric label with four values and the transport is the only layer that can see a session-level reject.

**Must not:** decide when to connect, how long to wait before retrying, what a payload means, or whether silence means anything. Those are the driver's, so that they are one implementation for every transport rather than one per publisher.

**Does not build an order-entry path.** The protocol has one and this repository has no reason to reach it: what leaves this transport is what the adapter wrote plus the session layer's own messages, and the session layer composes nothing but session messages. Stated because the variant's doc comment currently calls the protocol an order-entry protocol, and a reader could take that as a description of what we would send.

## The test surface, and why it needs no socket

A session layer is a state machine over a byte stream, so the byte stream goes behind a trait this crate owns — the same move `RouteLookup` makes for the routing table and `Clock` makes for time. Then every case that matters is a test that runs unprivileged with no network: a logon answered, a logon rejected, a heartbeat due, a test request answered, a message whose checksum does not hold, a message split across two reads, two messages in one read, a logout from the venue, and a session that goes silent.

TLS and the real socket are exercised by one example against a loopback endpoint, the way the multicast send path is — because that is the half no fake proves, and the venue adapter plan's own experience is that a real run finds things no fake could.

## What it costs

- **A new crate, and a protocol implementation in it.** Roughly the 870 lines the request measured, plus its tests, which will be more.
- **TLS in the ingress family**, which the websocket transport already brings, pinned the same way and with the same discipline about which backends a default must not be able to pull in.
- **A protocol version.** The tag-value encoding is stable and the field sets are not; a venue on a different version needs its adapter to carry different tags, which is where version differences belong, but the session layer's own messages are version-sensitive and the crate has to say which version it composes.
- **A refusal that will annoy somebody**: two enabled sources with one credential is refused, and an operator who was getting away with it because only one source was ever up will now be told to say what they meant.

## Decisions

| Decision | Why |
|---|---|
| The adapter composes the logon body; the transport frames, numbers and paces it | Transports are constructed by the runtime from a closed match, so there is no seam for a venue to inject a signer through — and `on_connected` already exists for authentication frames |
| The heartbeat cadence comes from the logon the adapter wrote | A key for it could disagree with what was logged on with, and the venue believes the logon |
| Sessions are per `[[source]]`, never per feed or shard | The shard partitions a published set and has nothing to do with upstream connections; the source count is the operator's statement |
| Two enabled sources with equal credential tables are refused, naming both | It is the copy-paste failure, it is computable without interpreting a credential, and the eviction it prevents looks like a venue closing the connection |
| Sequence numbers reset at logon | A resend delivers deltas whose value has expired; the publisher's snapshot recovery is the better repair, and persisting session state buys it at the price of another file that stops a publisher starting |
| A venue requiring continuity is refused at load | Silently resetting produces a session the venue tears down for a reason our logs do not carry |
| The byte stream is behind a trait | A session layer's every interesting case is then a test with no socket and no privileges |

## Non-goals

Order entry. Sequence persistence. A resend or gap-fill path. Anything about which tags a venue's market-data messages carry, which is the adapter's and is where a version difference belongs.
