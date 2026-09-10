# A session transport — plan

Turns [the design](../specs/2026-09-09-session-transport-design.md) into ordered tasks.

**Base:** `main`. Independent of `jo/publisher-feed-routes`.

**Depends on** `Adapter::poll_upstream`, which is its own design and plan and **is on `main`** — `rust/adapter/dz-adapter-core/src/adapter.rs`, merged as #101. A session transport is the one that cannot re-subscribe for reasons of its own, so without that method an instrument admitted mid-session waits for a reconnect — which is the failure that measured it. Task 5 is where the two meet, and it is the last task for that reason.

> **Corrected at execution.** This line said the method "is not in this repository yet", and put task 5 last because of it. It is a status fact this dated document got wrong rather than an argument to re-open: the method landed before execution began, so task 5 was never blocked and the plan was executable end to end. The ordering it produced is kept — task 5 is still the last task, because the boundary's half is still the half that needs an adapter.

## The ordering constraint

Tasks 1 to 3 are the protocol with no session and no socket: framing, timestamps, and the state machine over a byte stream behind a trait. Task 4 is the socket and TLS. Task 5 is the boundary's half — the logon the adapter writes and the mid-session write. Nothing before task 4 needs a network, and nothing before task 5 needs an adapter.

## Global constraints

- **Vocabulary:** `GLOSSARY.md` governs every identifier, comment, test name, config key and metric label. A message on this transport is a `message` and never a `datagram`; the session's own numbering is a `sequence` and the publisher's era is an `era`; `source` never appears bare — `upstream source` where the venue is meant, `source_id` where the wire field is.
- **Every test must be shown to kill its mutant.** The framing tasks have obvious mutants and the plan names them; the session tasks' mutants are the interesting ones, because a state machine that skips a transition usually still connects.
- **No order entry.** A task that composes a message the protocol defines for order entry has misread the design.

---

## Tasks

### 1. Framing, in both directions, and the two refusals

- [x] Encode: field order the protocol mandates, `BodyLength` computed over the right span, the checksum over the right bytes, and the timestamp format at the precision the venue's own session expects.
- [x] Decode: a message split across two reads, two messages in one read, a declared length that does not match, and a checksum that does not hold.
- [x] A message whose length or checksum does not hold is **refused, not skipped**, and the refusal ends the session. A corrupt message on a session-numbered connection means the numbering can no longer be trusted, and carrying on reads the next message against a sequence that has moved for a reason nobody recorded.

**Test:** a golden vector per direction, byte for byte, plus the four decode cases above. The split-read and two-in-one-read cases are the ones a hand-written reader gets wrong.

**The revert:** compute `BodyLength` over the whole message rather than the mandated span. The golden encode test fails. And: skip a message whose checksum does not hold instead of ending the session — `a_corrupt_message_ends_the_session` fails, and what it would have cost is a session that keeps reading against numbering it can no longer trust.

---

### 2. The session state machine, over a byte stream behind a trait

- [x] A trait for the byte stream, owned by this crate, so that every case below is a test with no socket and no privileges — the move `RouteLookup` makes for the routing table and `Clock` makes for time.
- [x] States and transitions: sending the logon, awaiting its answer, established, logging out, closed. Nothing but session messages may be sent before *established*.
- [x] The heartbeat cadence is read from the logon the adapter wrote, not from a configuration key. A key could disagree with what was logged on with, and the venue believes the logon.
- [x] A test request when the venue has been silent for longer than the cadence, and an answer to the venue's own.
- [x] The outbound sequence numbers every message, including the session's own.

**Test:** a logon answered, a logon rejected, a heartbeat due, a test request answered, the venue's logout, and a session that goes silent — each an assertion about what was written and what state followed.

**The revert (the plan's centre for this task):** allow a subscription to be sent before *established*. `nothing_but_a_logon_is_sent_before_the_session_is_established` fails. A state machine that skips this transition still connects and still receives, which is exactly why the test has to assert the *order of what was written* rather than that the session came up.

---

### 3. Sequence numbers reset at logon, and continuity is refused

- [x] The outbound sequence starts at 1 on every logon, with the flag that says so, and nothing is persisted.
- [x] A configuration asking for continuity is refused at load, naming the key: a transport that silently resets against a venue expecting continuity produces a session the venue tears down for a reason our logs will not carry.
- [x] The reason the reset is right, in the crate's own documentation rather than only in the design: a resend delivers deltas whose value has expired, and the publisher's snapshot recovery is the better repair.

**Test:** two successive logons both number from 1; a document asking for continuity is refused.

**The revert:** carry the sequence across a reconnect. `a_second_logon_numbers_from_one` fails.

---

### 4. The socket, TLS, and the failure classification

- [x] `Input` implemented over the state machine: `connect` opens the socket and performs TLS; `send` writes what the adapter queued, framed and numbered, and the *first* `send` is what carries the session to *established*; `recv` returns a decoded application message as a payload and a session message as `Received::Liveness`; `shutdown` attempts an orderly logout and gives up quickly.
- [x] Every failure classified: a refused connection, a failed negotiation, a rejected logon, a session-level reject, a silence. The disconnect reason is a metric label with four values and this is the only layer that can see which applies.
- [x] TLS pinned as the websocket transport pins it, `default-features = false`, with the manifest saying which backends a default must not be able to pull in.
- [x] A session message is `Liveness` and never a payload, for the reason that case exists: the idle guard counts time since the last *payload*, so a session that heartbeats forever and delivers nothing must still trip it.

> **Corrected at execution.** The first line said `connect` "drives the logon to *established*". It does not, and task 5 below is the reason: the driver connects, *then* asks the adapter what to send, so there is no logon to drive at the moment `connect` runs and the first `send` is what establishes the session. That is a status fact this dated document got wrong about the code it went on to produce — and one this same plan contradicts three tasks later — rather than an argument made on the day and since lost, which is why it is corrected here and not left to stand with a note. What *was* argued in this task is unchanged and kept: `connect` owns the socket and the negotiation, the classification is this layer's because it is the only layer that can see which failure applies, and a session message is never a payload.

**Test:** the classification, over the scripted byte stream; and one example against a loopback endpoint for TLS and the real socket, which is the half no fake proves.

**The revert:** report a heartbeat as a payload. `the_idle_guard_fires_on_a_session_that_only_heartbeats` fails — the failure the whole `Liveness` case exists for.

---

### 5. The logon is the adapter's, and so is the mid-session write

- [x] The transport reads the heartbeat interval out of the logon body the adapter wrote through `on_connected`, frames it, numbers it, and sends nothing else before it.
- [x] A connect where the adapter wrote no logon is a refusal naming the adapter's method, not a session that waits: a transport that logged on with a body it composed itself would be signing for the venue.
- [x] What the adapter writes through `poll_upstream` is framed and numbered on the established session, which is what makes an instrument admitted mid-session reach a subscription without a reconnect.
- [x] Sessions are per `[[source]]`, and two enabled sources whose `credentials` tables are equal are refused at load naming both blocks. What that does **not** catch — two paths holding one account — is documented with its symptom, which is both sources reconnecting in step.

**Test:** a scripted adapter's logon body reaches the wire framed and numbered; an adapter that writes nothing at connect is a refusal; a mid-session write is framed and numbered on the same session; a document with two enabled sources sharing a credential table is refused; and — asserted rather than assumed — a document with 62 channel instances over one source opens **one** session.

**The revert:** let the transport compose a logon when the adapter wrote none. `a_connect_with_no_logon_from_the_adapter_is_refused` fails. That is the one where the failure is not a crash: it is this repository signing a logon on a venue's behalf.

---

### 6. The documents that have to stay true

- [x] `Kind::Fix`'s doc comment stops calling the protocol an order-entry protocol without qualification. It carries market data too, and the current wording invites a market-data publisher to think the token is not for it.
- [x] `BRINGING-UP-A-FEED.md` gains the transport, and one line on the logon being the adapter's: a venue implementing this writes its logon where it writes its subscriptions.
- [x] `docs/README.md` carries the row for this pair.

**Test:** `scripts/check-public-repo-rules.sh`, plus a read of the new prose against the glossary's banned-word table.

---

## Acceptance

The plan is done when:

1. a venue can read its feed over a session with no session code of its own — no framing, no sequence, no heartbeat, no logout;
2. its logon signature is composed in its own repository and this repository composes none;
3. an instrument admitted mid-session reaches a subscription without a reconnect;
4. a session that heartbeats forever and delivers nothing trips the idle guard;
5. a document with two enabled sources sharing a credential table is refused naming both, and one with 62 channel instances over one source opens one session;

and when reverting task 2's ordering or task 5's logon makes a named test fail — because a state machine that connects and a transport that signs are both things that look like they work.

## The reverts, run

**Every test was shown to kill its mutant.** Each revert below was applied to a
committed tree, the suite was run, and the file was restored from a copy taken
first.

| Reverted | What was put back | Tests that failed, and with what values |
|---|---|---|
| Task 1 | `BodyLength` computed over more than the mandated span | `the_declared_length_measures_the_mandated_span_and_not_the_message` — declared 55 against a span of 35; and `a_framed_logon_is_the_bytes_written_out_by_hand` — `9=99\|…\|10=070` against the hand-computed `9=79\|…\|10=068`. Four decode tests fell with them, because a length that lies is a message the decoder cannot locate the end of |
| Task 1 | a message whose checksum does not hold is skipped rather than refused | `a_checksum_that_does_not_hold_is_refused` — `expect_err` was handed `Ok(false)`, which is the decoder silently dropping the message and reading on |
| Task 2 | anything may be sent before the session is established | `nothing_but_a_logon_is_sent_before_the_session_is_established` — the subscription was accepted and written, so `expect_err` was handed `Ok(())`. The session still came up afterwards, which is exactly why the test asserts the order of what was written |
| Task 2 | the cadence is a fixed 30s rather than the value the logon stated | `the_cadence_is_the_one_the_logon_stated_and_no_other` — with a logon of `108=10`, the writes were `["A"]` where `["A", "0"]` was due; and `silence_is_questioned_once_and_then_ends_the_session` — `Silent { interval: 30s }` against the agreed `10s` |
| Task 3 | the outbound sequence carries across a reconnect | `a_second_logon_numbers_from_one` — the second logon went out on 4, not 1 |
| Task 3 | `persist_sequence = true` accepted, and the sequence reset anyway | `a_document_asking_for_sequence_continuity_is_refused_naming_the_key` — resolved to `Endpoint { address: "203.0.113.10:9443", … }` instead of refusing |
| Task 4 | a heartbeat reported as a payload | `the_idle_guard_fires_on_a_session_that_only_heartbeats` — the bound elapsed with the driver still running (`Elapsed(())`). The failure is not a late guard or a wrong reason: the guard never fires, and the publisher runs against a dead subscription for the life of the process |
| Task 5 | the transport composes a logon when the adapter wrote none | `a_connect_with_no_logon_from_the_adapter_is_refused` — the bound elapsed with the driver still running (`Elapsed(())`), waiting on a venue's answer to a logon this repository had signed on its behalf |
| Task 5 | two enabled sources with equal `credentials` tables accepted | `two_enabled_sources_with_the_same_credential_table_are_refused_naming_both` — resolved with `sources: ["primary-session", "second-session"]`, which is two logons with one credential |
| Task 5 | a venue may open more sessions than the document declares | `sixty_two_channel_instances_over_one_source_open_one_session` — the per-channel-instance venue was accepted, so `expect_err` was handed `Ok(())`; `a_venue_that_builds_a_source_nobody_declared_is_refused` fell with it |

**Two of these are worth reading for their failure shape rather than their
name.** Task 4's and task 5's reverts do not produce a wrong value: they produce
a publisher that keeps running. A session that heartbeats forever looks healthy
in every series a dashboard carries, and a transport that signs its own logon
connects. Both tests therefore bound the driver's run and fail on the bound,
because *the driver came back at all* is the property under test.

**One thing this plan asked for was not done.** Task 4 asks for TLS to be
exercised by an example against a loopback endpoint. The real socket is —
`tests/loopback.rs` runs the session, the framing and the driver against a
listener on `127.0.0.1` — but TLS is not, and it is not faked. Verifying the
compiled-in trust anchors against a certificate chain needs a real endpoint, and
a self-signed root of our own would exercise a configuration this crate does not
build: it would assert that a test harness works. That is the standard
`dz-ingress-websocket` set for this family, in its own loopback suite, and this
follows it. What is checkable without a network is checked — that the client
configuration is constructible with the provider named rather than discovered,
which is where the `rustls` provider-selection panic would land.

## What this plan does not do

No order entry, no sequence persistence, no resend. It decides nothing about which tags a venue's market-data messages carry: that is the adapter's, and it is where a protocol version difference belongs.
