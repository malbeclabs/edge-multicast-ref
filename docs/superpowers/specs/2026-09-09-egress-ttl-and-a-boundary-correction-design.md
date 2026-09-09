# A document states its TTL, and one sentence at the boundary is wrong

**Status:** design. Two small changes with one property in common: each is a place where this repository currently answers a question nobody asked it, and is believed.

## 1. `[egress] ttl` has a default and should not

`ttl` defaults to one hop. A document that omits it publishes on the attached segment and nowhere else.

That is not a wrong default. It is the right *value* for a host whose subscribers share its segment, and it is the value the constant's own doc comment argues for: "the group is delivered on the attached segment and the network's own last-mile carries it from there." The problem is what it does when it is wrong.

**The failure is silent in every direction an operator can look.** A locally attached subscriber receives normally, so a smoke test on the publisher's own host passes. Every datagram is sent successfully, so `dz_publisher_egress_*` stays green — there is no send error, no dark transmitter, no dropped sink; the kernel accepted every one of them and the router discarded them. Gap detection at a subscriber that never joined sees nothing to report, because nothing arrived to be numbered. The publisher is healthy and the feed is empty, and nothing in the exposition distinguishes that from a market with no activity.

A publisher already in production states 64 for this key because its groups cross several hops. So the operating value and the default are not the same number, in a deployment that exists.

### Why the refusal the request suggested cannot be written

The other shape offered was a startup refusal when `ttl` is 1 and `expected_prefix` names a routed network. It cannot be computed, and the reason is worth stating because the key looks as though it carries the information.

`expected_prefix` is an assertion about **the publisher's own source address** — that the address route discovery resolved falls in the prefix the operator expected — and its whole documented purpose is catching a source address from the wrong interface. It says nothing about where the group has to reach. A publisher whose source address is on a tunnel prefix and whose subscribers are on that same segment is correct with one hop; a publisher with no `expected_prefix` at all and a group that crosses two routers is broken with one hop and would pass the check. The condition would refuse correct configurations and admit the broken one, which is worse than no check: it teaches an operator that the absence of a refusal means the TTL is right.

There is also no key in the document that says how far a group must travel, and inventing one — a `hops` or a `routed = true` — would be a second statement of the same fact with two ways to disagree.

### What is being changed

`ttl` becomes a required statement: an `Option<u8>` in the section, refused at load when nothing states it, with an error that names the key, says there is no default, and states the value that reproduces the old behaviour and what that value means.

**A required *key* and not a required *section*.** `[egress]` stays optional and the refusal comes from `resolve()` rather than from serde, because this repository has already met the alternative and written down what it costs: making a section required "failed at parse with `missing field ingress` at line 1, column 1 — an error pointing at the whole file rather than at the section nobody wrote." One error covers both cases: a document with no `[egress]` and a document with an `[egress]` that omits `ttl` are the same mistake and get the same message.

**The value is named in the message.** An operator upgrading has one line to write, and the message writes it for them. An operator who did not know they were publishing one hop learns it here rather than from a subscriber that never received anything.

**`DEFAULT_TTL` survives as a value, not as a default.** It is what the message names and what a hand-composed `EgressPolicy::default()` documents, and its doc comment now says that it is not the document's default — so that the next reader who finds a constant called `DEFAULT_TTL` does not restore `#[serde(default)]` from its name.

**`EgressPolicy::default()` is left as one hop**, and this is the one asymmetry in the change, so it is argued rather than assumed. The failure above is a failure of *omission*: a key nobody wrote, in a file nobody diffed for it. `EgressPolicy::default()` is not an omission — it is a call, in Rust, in a crate somebody authored, whose doc comment says "discovery, no invariant, one hop" at the definition and appears in a diff at the call site. A `Default` impl removed would also break `..Default::default()` for the two fields that are genuinely optional, which buys nothing: neither of them can make a feed silently invisible.

### What it costs

Every document that omits `ttl` stops loading. That is the point of the change and it is still a cost worth naming precisely:

- **A publisher that was correct becomes a publisher that will not start** until one line is added. The failure is at load, before a socket, and the message contains the line — which is the whole difference between this and the failure it replaces.
- **It is a breaking change to a key that currently has an answer**, so the crate takes the version bump that costs and the guide gains the key as required rather than optional.
- **A configuration generator that omits the key** — a template, a chart, a rendered file — fails for every publisher it generates at once, rather than one at a time. That is loud in the way this change intends and it is worth an operator knowing before they upgrade, which is why the guide's row for the key changes in the same change.

## 2. `ListingSink::list_on` argues from something untrue

The doc comment justifies its version bump like this:

> a venue *calls* this trait rather than implementing it, so the break falls on implementors, and every implementor of it is in this workspace.

The last clause is false. Implementors exist outside this workspace — a venue's own test doubles implement the trait even where its adapter only calls it, and six of them exist in one integration. So the bump is not free, and the sentence that says it is makes the next decision of this shape easier than it should be.

**The direction the trait chose is still right**, and the correction must not read as though it were not. `list_on` is required and `list` is defaulted because the other direction leaves an adapter that had not been updated admitting its whole universe to one shard — with no error, no counter and no log. That is the trade, and it is still worth a compile error: a break in a test double is found by the next `cargo test`, where a silently collapsed published set is found by a subscriber holding definitions for instruments no message will ever arrive for.

What changes is one clause, and it says what is true without naming anyone: implementors outside this workspace exist.

## What neither change does

- **No new configuration key.** The TTL change adds none; it removes a default from one that exists. There is no `hops`, no `routed`, and no second way to say the same thing.
- **No change to the send path.** `ttl` reaches the socket exactly as it did.
- **No change to `list_on`'s signature, its default or its direction.** One clause of one doc comment.
- **No new metric.** The failure this makes loud is a failure a metric cannot see: every datagram was sent successfully. A series that counted "datagrams that a router discarded" would be a series a publisher cannot observe, and pre-creating one at zero would assert that none were discarded.

## Decisions

| Decision | Why |
|---|---|
| `ttl` is required, with no default | The operating value and the default differ in a deployment that exists, and the difference is invisible in the exposition |
| Refused in `resolve()`, not by serde | A required section produces an error pointing at the whole file rather than at what is missing — this repository's own recorded finding |
| The error names `ttl = 1` and what it means | An operator upgrading has one line to write, and one who did not know their hop count learns it |
| `expected_prefix` is not consulted | It asserts something about the source address, not about the group's reach; the condition would refuse correct documents and admit broken ones |
| `EgressPolicy::default()` stays one hop | The failure is omission in a document, not a documented call in code somebody wrote |
| `DEFAULT_TTL` stays, with its doc saying it is not the document's default | So that the constant's name does not restore the default it was named for |

## Non-goals

A key stating how far a group travels. A metric for datagrams a router discarded. Any change to `list_on`'s shape.
