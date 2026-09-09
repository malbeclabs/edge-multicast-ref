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

- [ ] `EgressSection::ttl` becomes `Option<u8>` with `#[serde(default)]`. The section stays optional.
- [ ] `StartupError::TtlUnstated`, refused in `EgressSection::resolve`, naming the key, saying there is no default, and stating that `ttl = 1` publishes on the attached segment only — which is what a document omitting it did before. The message is the operator's one line.
- [ ] `default_ttl` and `#[serde(default = "default_ttl")]` go. `DEFAULT_TTL` stays and its doc comment gains the sentence that it is not the document's default, so that the constant's name cannot restore one.
- [ ] `EgressPolicy::default()` is unchanged at one hop, and its doc comment says where the document's requirement now lives, so the two cannot be read as disagreeing.
- [ ] Every in-tree document that omits `ttl` gains it: the test harness's `Doc`, the two example scripts, and any fixture under `rust/`. A document in a *dated* design or plan is left as written — a fence in a dated document is a record of that day.

**Test** (`dz-publisher-runtime/tests/config_document.rs`):
- a document with no `[egress]` section at all is refused with `TtlUnstated`;
- a document with an `[egress]` that states `pin` and `expected_prefix` but no `ttl` is refused with the same error — the two cases are one mistake;
- `ttl = 1` resolves and the policy carries 1, so the old behaviour is still expressible and is now stated;
- `ttl = 64` resolves and the policy carries 64, which is the value a deployment that exists uses;
- the message names the key and the value, asserted as substrings, because the message *is* the remedy and a message that stopped naming the value would leave an operator to guess it.

**The revert:** restore `#[serde(default = "default_ttl")]`. `a_document_that_states_no_ttl_is_refused` and `an_egress_section_without_a_ttl_is_refused_like_an_absent_one` both fail — they are the same mistake and both have to be able to fail, or the section-absent case is passing for the wrong reason.

---

### 2. The clause at the boundary that is untrue

- [ ] `ListingSink::list_on`'s doc comment stops claiming that every implementor is in this workspace. It says implementors outside this workspace exist, and keeps the argument: the break falls on implementors rather than callers, and a compile error in a test double is found by the next `cargo test` where a silently collapsed published set is found by a subscriber.
- [ ] Nothing else in that doc comment moves. The `compile_fail,E0046` doctest, the direction of the default and the signature are the design's and are not what was wrong.

**Test:** the existing `compile_fail,E0046` doctest and the doctest beside it still compile and still fail respectively, which is what says the correction touched prose and not the trait.

**The revert:** there is no test that can distinguish two true sentences from one true and one false, and the plan says so rather than asserting a doc comment's text against itself. What guards this task is that the two doctests in the same comment are compiled by `cargo test`, so a correction that broke the example is a build failure.

---

### 3. The documents that have to stay true

- [ ] `BRINGING-UP-A-FEED.md`'s configuration block shows `ttl` without a comment marking it optional, and the note beside `pin` gains one line: the key has no default, and one hop is what a document that omitted it used to publish.
- [ ] `docs/README.md` carries the row for this pair.

**Test:** `scripts/check-public-repo-rules.sh`, plus a read of the new prose against the glossary's banned-word table.

---

## Acceptance

The plan is done when:

1. a document that states no `ttl`, with or without an `[egress]` section, is refused at load with a message naming the key and the value that reproduces one hop;
2. `ttl = 1` and `ttl = 64` both resolve and both reach the policy as stated;
3. the guide shows the key as required;
4. `list_on`'s doc comment claims nothing about implementors outside this workspace that is untrue;

and when restoring the serde default makes both of task 1's refusal tests fail.

## What this plan does not do

It does not touch the two other requests this cluster was triaged with. A venue handing metric collectors up is already built on `jo/publisher-venue-metrics` — one field on `Venue`, registration in the runtime's composition, and a startup refusal naming a reserved series — and it needs nothing from here. That branch carries no spec and no plan of its own, which is a gap in the record rather than in the code.
