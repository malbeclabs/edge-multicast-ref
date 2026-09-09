# A polled transport — plan

Turns [the design](../specs/2026-09-09-polled-transport-design.md) into ordered tasks.

**Base:** `main`. Independent of `jo/publisher-feed-routes`: nothing here touches the publisher runtime, the reference-data registry or the era store.

**Depends on nothing, and is depended on.** A polled transport is what makes a mid-session listing routine rather than incidental, and [`poll_upstream`](2026-09-09-upstream-write-after-a-listing-change.md) is what subscribes what it discovers. Either can land first; a venue needs both.

## The ordering constraint

**Task 1 is the rename and it lands alone, before anything is built on the token.** Nothing constructs the variant today, so no document can name it usefully and the rename is free. Once the transport exists, a document naming the token is in an operator's configuration management and the rename is a coordinated change across repositories.

## Global constraints

- **Vocabulary:** `GLOSSARY.md` governs every identifier, comment, test name, config key and metric label. The token argument is in the design and a task that renames it back has to answer the *resting quantity* collision. A response body is a `payload`, never a `datagram`; the endpoint is an `endpoint`; `source` never appears bare.
- **Every test must be shown to kill its mutant.** The two that matter here assert an absence and a classification — an unchanged response that is *not* a payload, and a failed request that *is* a disconnect.
- **No second poller, no second backoff, no second failure count.** A task that puts any of them in the transport has misread the design.

---

## Tasks

### 1. `Kind::Poll`, and the token `poll`

- [ ] `Kind::Rest` becomes `Kind::Poll`; the token `"rest"` becomes `"poll"`; `ALL`, `TOKEN_LIST` and `as_token` move with it, and the doc comment keeps "polled request/response" and drops nothing else.
- [ ] The variant's doc comment carries the reason for the name, in one line, so that the next reader does not restore the old one from familiarity: a book's *resting* quantity already owns that word here.

**Test:** `dz-ingress-core/tests/config.rs::every_kind_has_a_token` already holds the set; it passes only if the token moved with the variant. A document naming `kind = "poll"` resolves to the variant, and one naming `kind = "rest"` is refused with a message listing the acceptable tokens — which is the test that says the old spelling is gone rather than aliased.

**The revert:** leave `TOKEN_LIST` saying `rest` while the variant is `Poll`. `every_kind_has_a_token` fails. That test exists because a variant added without a token is a value an operator can name and nothing can resolve, and a rename is the same defect arriving from the other direction.

---

### 2. The crate, and the client decision in its manifest

- [ ] `dz-ingress-poll`, alongside `dz-ingress-websocket`, with the marker feature on `dz-ingress-core` that makes `kind = "poll"` resolve — the mechanism that lets the core answer *is that transport in this binary* without depending on the transports.
- [ ] An async HTTP client, pinned exactly, `default-features = false`, with only the features used and TLS behind a feature of this crate's own. The manifest comment states what the websocket crate's states: which backends are deliberately excluded and why a default must not be able to pull one in.
- [ ] The crate documentation names the cost the design names: this is the family's first HTTP client and the workspace's second, the other is blocking and belongs to a different process and tier, and neither should migrate toward the other because they look alike in a manifest.
- [ ] An `https` endpoint in a build without the TLS feature is refused at configuration load, naming the scheme and the feature — the shape the column-store writer already uses.

**Test:** the refusal, which needs no network; and a crate that builds with and without the TLS feature, which is what says the feature is real rather than declared.

**The revert:** accept `https` without the feature. `an_https_endpoint_without_tls_is_refused_at_load` fails, and what it would have cost is a publisher that starts and fails on every request.

---

### 3. `recv`, and the three answers it has to tell apart

- [ ] A poll due, a response with a body: `Received::Payload`, with no timestamp of its own — the driver stamps it, because a response body carries no receive time this transport knows better than the driver's.
- [ ] A response that says nothing changed — `304`, or a body whose digest has not moved: `Received::Liveness`.
- [ ] The budget elapsing before the poll is due: `Received::Idle`.
- [ ] A failed request: `IngressError::Ended` with the reason, classified by what happened — a refused connection, a timeout, a status the endpoint should not have returned.

**Test** (no network: the client is behind a trait this crate owns, the way `RouteLookup` puts the routing table behind one):
- a changed body is a payload and reaches the driver;
- **an unchanged response is `Liveness` and not a payload**, asserted as the idle guard still firing afterwards — which is the assertion that matters, because a `Liveness` that behaved like a payload would satisfy any test that only checked the return value's discriminant;
- a budget that elapses before the poll is due is `Idle` and not an error;
- a failed request is `Ended`, and the reason it carries is the one the failure had rather than a single catch-all.

**The revert (the plan's centre):** return an unchanged response as a payload. `the_idle_guard_still_fires_on_an_endpoint_that_answers_forever` fails. That is the failure the whole distinction exists for: an endpoint answering `304` for a week is a catalogue that has stopped changing, and a publisher whose guard cannot fire on it reports a healthy feed.

---

### 4. What is polled is the adapter's to change

- [ ] `send` holds what the adapter wrote as the next request's parameters. A cursor, a page token and a symbol list are all the venue's, and none of them is parsed here.
- [ ] The parameters are per connection, for the reason `on_connected` gives: one adapter serves every source, and two polled sources are two cursors.

**Test:** what the adapter writes at connect reaches the first request; what it writes through `poll_upstream` reaches the next one. The second half is what makes this transport and that method one mechanism rather than two.

**The revert:** hold the parameters on the transport rather than per connection. The two-source test fails with one cursor serving both.

---

### 5. `poll_interval`, and the documents

- [ ] `poll_interval` on the transport's own configuration table, with the design's argument in its doc comment: an interval and not a cycle, because a cycle is one pass over a set divided by its size and one tick here is one request.
- [ ] `BRINGING-UP-A-FEED.md` gains the transport in its list of what `[ingress] kind` can name, and one line on what a venue uses it for — a catalogue that is a request rather than a subscription.
- [ ] `docs/README.md` carries the row for this pair.

**Test:** `scripts/check-public-repo-rules.sh`, a document that states `poll_interval` resolving, and one that omits it refused — a transport with no cadence is a transport that polls in a loop or never, and both are worse than a refusal.

---

## Acceptance

The plan is done when:

1. a document naming `kind = "poll"` resolves in a binary that links the crate, and is refused naming what is linked in one that does not;
2. a catalogue endpoint's body arrives at an adapter as a payload, with no code in the venue's binary holding a timer, a backoff or a failure count;
3. an endpoint that answers and has stopped changing does not satisfy the idle guard;
4. an endpoint that has stopped answering shows as `dz_publisher_ingress_connection_state` at 0, with reconnects counted by reason;
5. `kind = "rest"` is refused with a message listing the acceptable tokens;

and when reverting task 3's `Liveness` makes a named test fail.

## What this plan does not do

It does not decide what a catalogue's contents mean, which is the adapter's, or which of them get published, which is the selection policy's and already exists. It builds no second HTTP client for the recorder tier.
