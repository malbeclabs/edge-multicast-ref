# An upstream write that is not a connect — plan

Turns [the design](../specs/2026-09-09-upstream-write-after-a-listing-change-design.md) into ordered tasks.

**Base:** `jo/publisher-feed-routes`. The boundary crate and the driver are both touched by that branch, and its `list_on` change is the one an adapter uses to learn which instruments it holds handles for — which is the set this mechanism writes about.

## Global constraints

- **Vocabulary:** `GLOSSARY.md` governs every identifier, comment, test name and commit message. `upstream` never appears as bare `source`; a venue's message is not a `datagram`; the connection is a `connection` and never a `channel`.
- **Every test must be shown to kill its mutant.** Three of the four tests below assert a *negative* — nothing written when nothing is outstanding, the connection surviving a refusal, the cadence not firing early — which is the shape that passes against an unfixed tree.
- **No breaking change.** The method is defaulted. A task that makes an existing adapter fail to compile has misread the design.

---

## Tasks

### 1. `Adapter::poll_upstream`, defaulted, carrying the connection

- [x] `fn poll_upstream(&mut self, conn: ConnectionId, out: &mut dyn UpstreamSink) -> Result<(), AdapterError>`, defaulted to `Ok(())`.
- [x] The doc comment carries three things the runtime cannot enforce: that the write is **not** deduplicated, unlike re-offering a listing, so an adapter must write only what is outstanding *on this connection*; that state belongs per `conn` for the reason `on_connected` gives; and what getting it wrong looks like from here, which is a venue rate-limiting a publisher that appears to be working.
- [x] It says what the method is for beyond the measured failure: a subscription for an instrument admitted mid-session, and a request the repair path needs, which had nowhere to go before.

**Test** (`dz-adapter-core`, doctest): an adapter implementing only the required methods compiles and inherits the default — which is the whole of "no existing adapter changes", stated as a compile.

**The revert:** remove the default. Every adapter in the workspace fails to compile, including the built-in and the two examples. That is a stronger signal than a test and the plan records it as the reason the default is not optional.

---

### 2. The driver asks, on its own connection, and pays the same rate limit

- [x] `UPSTREAM_POLL`, a constant beside the driver, documented against the runtime's listing poll: nothing can be outstanding that a poll has not admitted, so asking more often buys nothing.
- [x] In `pump`, when due, `poll_upstream(connection, &mut queue)` into a fresh queue, then the existing paced `flush`. Due-ness is computed where the idle guard's arithmetic already is, so it costs a comparison and no second clock read.
- [x] A refusal is `observer.adapter_error(error)` and the connection **survives**. A send failure ends the connection through the existing path, which reconnects and lets `on_connected` write the whole set again.
- [x] The queue is per call, not carried: a message the adapter queued and the flush failed to send belongs to a connection that is now gone.

**Test** (`dz-ingress-core/tests/driver.rs`, over the scripted transport and the test clock):
- with the clock advanced past the cadence, the adapter is asked and what it queued reaches `send`;
- before the cadence, over several receives, it is **not** asked — asserted as a count of calls, because a driver asking every receive would pass any test that only checks the message arrived;
- an adapter that refuses is counted once and the connection stays up, asserted by the receives that follow still being delivered;
- a send failure on the mid-session write ends the connection and the next connect calls `on_connected` again;
- what the adapter queues is paced: with a rate limit that admits one message, two queued messages produce a wait between them, asserted from the clock's own record of what the driver slept.

**The revert (the plan's centre), and what it found.** Moving the call from `pump` to the connect path, beside `on_connected`, fails **`the_adapter_is_asked_while_the_connection_is_up`**, **`the_adapter_is_not_asked_before_the_cadence`** and **`a_failed_mid_session_send_ends_the_connection_and_the_reconnect_subscribes_again`**.

The first of those did **not** fail when the revert was first run. It asserted "asked once, on this connection, and the message reached `send`" — every word of which a call at logon satisfies exactly. So the test the plan called its centre did not distinguish the mechanism from the publisher it exists to replace, and the revert is what said so. It now records how many payloads the adapter had seen when it was asked and asserts one; a connect-time ask records zero.

**A second revert:** dropping the due-ness check and asking on every receive fails `the_adapter_is_not_asked_before_the_cadence` and `the_adapter_is_asked_while_the_connection_is_up` — the second because the count is then two rather than one. Without the check a venue receives a subscription set per payload.

---

### 3. The documents that have to stay true

- [x] `BRINGING-UP-A-FEED.md`'s table of the methods a venue implements gains the row, marked optional, with one line on when a venue needs it: its instrument set changes without a reconnect.
- [x] The boundary crate's own module documentation names the method where it lists what an adapter may write.
- [x] `docs/README.md` carries the row for this pair.

**Test:** `scripts/check-public-repo-rules.sh`, plus a read of the new prose against the glossary's banned-word table.

---

## Acceptance

The plan is done when:

1. an adapter can write to its upstream while a connection is up, on a connection it names, without the runtime changing;
2. an adapter that implements nothing new compiles and behaves exactly as it did;
3. an adapter that refuses mid-session costs one counted error and no connection;
4. a mid-session write is paced by the same rate limit as a logon;

and when moving the call to the connect path makes a named test fail — because a mechanism whose tests pass against a call at logon has documented `on_connected` rather than added anything. That revert was run, and on its first run it failed nothing that a call at logon could not satisfy; the test was strengthened rather than the finding explained away.

## What this plan does not do

It does not give the repair path its metric, and it does not build the polled transport that would make an instrument's *discovery* mid-session routine rather than incidental. Those are separate, and the second one is what makes this one matter: a poll tells an adapter an instrument exists, and this is how it gets subscribed.
