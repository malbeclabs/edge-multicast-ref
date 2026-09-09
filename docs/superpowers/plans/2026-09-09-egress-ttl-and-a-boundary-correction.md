# A document states its TTL, and one sentence at the boundary — plan

Turns [the design](../specs/2026-09-09-egress-ttl-and-a-boundary-correction-design.md) into ordered tasks.

**Base:** `jo/publisher-feed-routes`. The TTL change is independent of that branch, but the boundary correction is a clause in a doc comment that branch wrote, so both land on top of it rather than being split across two bases for one review.

## Global constraints

- **Vocabulary:** `GLOSSARY.md` governs every identifier, comment, test name, config key and commit message. No new key is added here, so the words at risk are in prose: a hop count is a hop count, `datagram` never `frame`, and `source` never appears bare — `source address` where the address is meant.
- **Every test must be shown to kill its mutant.** Both tasks below have an obvious mutant and it is named with the task.
- **The guide has to stay true.** `BRINGING-UP-A-FEED.md` is the one document in this repository that must not become a record of a date, and a key that changes from optional to required is exactly what an operator reads it for.

---

## Tasks

### 1. `[egress] ttl` is stated or the publisher does not start

- [x] `EgressSection::ttl` becomes `Option<u8>` with `#[serde(default)]`. The section stays optional.
- [x] `StartupError::TtlUnstated`, refused in `EgressSection::resolve`, naming the key, saying there is no default, and stating that `ttl = 1` publishes on the attached segment only — which is what a document omitting it did before. The message is the operator's one line.
- [x] `default_ttl` and `#[serde(default = "default_ttl")]` go. `DEFAULT_TTL` stays and its doc comment gains the sentence that it is not the document's default, so that the constant's name cannot restore one.
- [x] `EgressPolicy::default()` is unchanged at one hop, and its doc comment says where the document's requirement now lives, so the two cannot be read as disagreeing.
- [x] Every in-tree document that omits `ttl` gains it. **There were none**: the test harness's `Doc`, `loopback.sh` and `replay.sh` all state `ttl = 1` already, and the whole suite stayed green when the key became required. Two *tests* did need it — see the revert below — and no document in a dated design or plan was touched, because a fence in a dated document is a record of that day.
- [x] Nothing asserted the old default. The suite passing unchanged is the evidence: no test resolved a document that omitted `ttl` and checked the 1 that came back, so the value a publisher would have shipped with was never held to anything.

**Test** (`dz-publisher-runtime/tests/config_document.rs`):
- a document with no `[egress]` section at all is refused with `TtlUnstated`;
- a document with an `[egress]` that states `pin` and `expected_prefix` but no `ttl` is refused with the same error — the two cases are one mistake;
- `ttl = 1` resolves and the policy carries 1, so the old behaviour is still expressible and is now stated;
- `ttl = 64` resolves and the policy carries 64, which is the value a deployment that exists uses;
- the message names the key and the value, asserted as substrings, because the message *is* the remedy and a message that stopped naming the value would leave an operator to guess it.

**The revert, and what it actually killed.** Restoring `#[serde(default = "default_ttl")]` fails **`an_egress_section_without_a_ttl_is_refused_like_an_absent_one`** and *not* `a_document_that_states_no_ttl_is_refused`. This plan predicted both and was wrong, which is the reason for running the revert rather than reasoning about it.

The two cases are one refusal and **two mechanisms**. `Document::egress` is `#[serde(default)]`, so a document with no `[egress]` section builds `EgressSection::default()` — the derived one, where `Option::default()` is `None` — and never consults a field-level serde default at all. That default only applies to a field missing from a table that is present. So the absent-section case refuses under the mutant too, and the only test guarding the field's default is the one whose document states the other two keys.

Both tests stay, and the reason is now stated rather than assumed: they cover two paths to one error, and a change to either mechanism can break one without the other. Two existing tests were also found to be passing for a second reason — the expected-prefix and pin refusals used documents that stated no TTL, so each had two things wrong with it and passed on whichever `resolve` checked first. Both now state a TTL.

---

### 2. The clause at the boundary that is untrue

- [x] `ListingSink::list_on`'s doc comment stops claiming that every implementor is in this workspace. It says implementors outside this workspace exist, and keeps the argument: the break falls on implementors rather than callers, and a compile error in a test double is found by the next `cargo test` where a silently collapsed published set is found by a subscriber.
- [x] Nothing else in that doc comment moves. The `compile_fail,E0046` doctest, the direction of the default and the signature are the design's and are not what was wrong.

**Test:** the existing `compile_fail,E0046` doctest and the doctest beside it still compile and still fail respectively, which is what says the correction touched prose and not the trait.

**The revert:** there is no test that can distinguish two true sentences from one true and one false, and the plan says so rather than asserting a doc comment's text against itself. What guards this task is that the two doctests in the same comment are compiled by `cargo test`, so a correction that broke the example is a build failure.

---

### 3. The documents that have to stay true

- [x] `BRINGING-UP-A-FEED.md`'s configuration block shows `ttl` without a comment marking it optional, and the note beside `pin` gains one line: the key has no default, and one hop is what a document that omitted it used to publish.
- [x] `docs/README.md` carries the row for this pair.

**Test:** `scripts/check-public-repo-rules.sh`, plus a read of the new prose against the glossary's banned-word table.

---

## Acceptance

The plan is done when:

1. a document that states no `ttl`, with or without an `[egress]` section, is refused at load with a message naming the key and the value that reproduces one hop;
2. `ttl = 1` and `ttl = 64` both resolve and both reach the policy as stated;
3. the guide shows the key as required;
4. `list_on`'s doc comment claims nothing about implementors outside this workspace that is untrue;

and when restoring the serde default makes `an_egress_section_without_a_ttl_is_refused_like_an_absent_one` fail — one test and not both, for the reason recorded under task 1.

## What this plan does not do

It does not touch the two other requests this cluster was triaged with. A venue handing metric collectors up is already built on `jo/publisher-venue-metrics` — one field on `Venue`, registration in the runtime's composition, and a startup refusal naming a reserved series — and it needs nothing from here. That branch carries no spec and no plan of its own, which is a gap in the record rather than in the code.
