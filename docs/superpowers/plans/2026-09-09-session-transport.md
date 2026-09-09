# A session transport — plan

Turns [the design](../specs/2026-09-09-session-transport-design.md) into ordered tasks.

**Base:** `main`. Independent of `jo/publisher-feed-routes`.

**Depends on** `Adapter::poll_upstream`, which is its own design and plan and is not in this repository yet. A session transport is the one that cannot re-subscribe for reasons of its own, so without that method an instrument admitted mid-session waits for a reconnect — which is the failure that measured it. Task 5 is where the two meet, and it is the last task for that reason.

## The ordering constraint

Tasks 1 to 3 are the protocol with no session and no socket: framing, timestamps, and the state machine over a byte stream behind a trait. Task 4 is the socket and TLS. Task 5 is the boundary's half — the logon the adapter writes and the mid-session write. Nothing before task 4 needs a network, and nothing before task 5 needs an adapter.

## Global constraints

- **Vocabulary:** `GLOSSARY.md` governs every identifier, comment, test name, config key and metric label. A message on this transport is a `message` and never a `datagram`; the session's own numbering is a `sequence` and the publisher's era is an `era`; `source` never appears bare — `upstream source` where the venue is meant, `source_id` where the wire field is.
- **Every test must be shown to kill its mutant.** The framing tasks have obvious mutants and the plan names them; the session tasks' mutants are the interesting ones, because a state machine that skips a transition usually still connects.
- **No order entry.** A task that composes a message the protocol defines for order entry has misread the design.

---

## Tasks

### 1. Framing, in both directions, and the two refusals

- [ ] Encode: field order the protocol mandates, `BodyLength` computed over the right span, the checksum over the right bytes, and the timestamp format at the precision the venue's own session expects.
- [ ] Decode: a message split across two reads, two messages in one read, a declared length that does not match, and a checksum that does not hold.
- [ ] A message whose length or checksum does not hold is **refused, not skipped**, and the refusal ends the session. A corrupt message on a session-numbered connection means the numbering can no longer be trusted, and carrying on reads the next message against a sequence that has moved for a reason nobody recorded.

**Test:** a golden vector per direction, byte for byte, plus the four decode cases above. The split-read and two-in-one-read cases are the ones a hand-written reader gets wrong.

**The revert:** compute `BodyLength` over the whole message rather than the mandated span. The golden encode test fails. And: skip a message whose checksum does not hold instead of ending the session — `a_corrupt_message_ends_the_session` fails, and what it would have cost is a session that keeps reading against numbering it can no longer trust.

---

### 2. The session state machine, over a byte stream behind a trait

- [ ] A trait for the byte stream, owned by this crate, so that every case below is a test with no socket and no privileges — the move `RouteLookup` makes for the routing table and `Clock` makes for time.
- [ ] States and transitions: sending the logon, awaiting its answer, established, logging out, closed. Nothing but session messages may be sent before *established*.
- [ ] The heartbeat cadence is read from the logon the adapter wrote, not from a configuration key. A key could disagree with what was logged on with, and the venue believes the logon.
- [ ] A test request when the venue has been silent for longer than the cadence, and an answer to the venue's own.
- [ ] The outbound sequence numbers every message, including the session's own.

**Test:** a logon answered, a logon rejected, a heartbeat due, a test request answered, the venue's logout, and a session that goes silent — each an assertion about what was written and what state followed.

**The revert (the plan's centre for this task):** allow a subscription to be sent before *established*. `nothing_but_a_logon_is_sent_before_the_session_is_established` fails. A state machine that skips this transition still connects and still receives, which is exactly why the test has to assert the *order of what was written* rather than that the session came up.

---

### 3. Sequence numbers reset at logon, and continuity is refused

- [ ] The outbound sequence starts at 1 on every logon, with the flag that says so, and nothing is persisted.
- [ ] A configuration asking for continuity is refused at load, naming the key: a transport that silently resets against a venue expecting continuity produces a session the venue tears down for a reason our logs will not carry.
- [ ] The reason the reset is right, in the crate's own documentation rather than only in the design: a resend delivers deltas whose value has expired, and the publisher's snapshot recovery is the better repair.

**Test:** two successive logons both number from 1; a document asking for continuity is refused.

**The revert:** carry the sequence across a reconnect. `a_second_logon_numbers_from_one` fails.

---

### 4. The socket, TLS, and the failure classification

- [ ] `Input` implemented over the state machine: `connect` opens the socket, performs TLS and drives the logon to *established*; `send` writes what the adapter queued, framed and numbered; `recv` returns a decoded application message as a payload and a session message as `Received::Liveness`; `shutdown` attempts an orderly logout and gives up quickly.
- [ ] Every failure classified: a refused connection, a failed negotiation, a rejected logon, a session-level reject, a silence. The disconnect reason is a metric label with four values and this is the only layer that can see which applies.
- [ ] TLS pinned as the websocket transport pins it, `default-features = false`, with the manifest saying which backends a default must not be able to pull in.
- [ ] A session message is `Liveness` and never a payload, for the reason that case exists: the idle guard counts time since the last *payload*, so a session that heartbeats forever and delivers nothing must still trip it.

**Test:** the classification, over the scripted byte stream; and one example against a loopback endpoint for TLS and the real socket, which is the half no fake proves.

**The revert:** report a heartbeat as a payload. `the_idle_guard_fires_on_a_session_that_only_heartbeats` fails — the failure the whole `Liveness` case exists for.

---

### 5. The logon is the adapter's, and so is the mid-session write

- [ ] The transport reads the heartbeat interval out of the logon body the adapter wrote through `on_connected`, frames it, numbers it, and sends nothing else before it.
- [ ] A connect where the adapter wrote no logon is a refusal naming the adapter's method, not a session that waits: a transport that logged on with a body it composed itself would be signing for the venue.
- [ ] What the adapter writes through `poll_upstream` is framed and numbered on the established session, which is what makes an instrument admitted mid-session reach a subscription without a reconnect.
- [ ] Sessions are per `[[source]]`, and two enabled sources whose `credentials` tables are equal are refused at load naming both blocks. What that does **not** catch — two paths holding one account — is documented with its symptom, which is both sources reconnecting in step.

**Test:** a scripted adapter's logon body reaches the wire framed and numbered; an adapter that writes nothing at connect is a refusal; a mid-session write is framed and numbered on the same session; a document with two enabled sources sharing a credential table is refused; and — asserted rather than assumed — a document with 62 channel instances over one source opens **one** session.

**The revert:** let the transport compose a logon when the adapter wrote none. `a_connect_with_no_logon_from_the_adapter_is_refused` fails. That is the one where the failure is not a crash: it is this repository signing a logon on a venue's behalf.

---

### 6. The documents that have to stay true

- [ ] `Kind::Fix`'s doc comment stops calling the protocol an order-entry protocol without qualification. It carries market data too, and the current wording invites a market-data publisher to think the token is not for it.
- [ ] `BRINGING-UP-A-FEED.md` gains the transport, and one line on the logon being the adapter's: a venue implementing this writes its logon where it writes its subscriptions.
- [ ] `docs/README.md` carries the row for this pair.

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

## What this plan does not do

No order entry, no sequence persistence, no resend. It decides nothing about which tags a venue's market-data messages carry: that is the adapter's, and it is where a protocol version difference belongs.
