# Minting an Instrument ID costs one line: plan

Breaks [the design](../specs/2026-09-28-refdata-append-only-state-design.md) into ordered tasks. Closes [#175](https://github.com/malbeclabs/edge-multicast-ref/issues/175).

**Base:** `main`.

## Global constraints

- **Vocabulary:** `GLOSSARY.md` governs every identifier, comment, test name, config key and commit message. The new key is `forget_delisted_after`.
- **Every test must be shown to kill its mutant.** Each task names the revert that must turn a named test red. Commit before each round of mutants, and restore from a copy taken for that mutant alone.
- **The invariant is not up for trade.** A published `Instrument ID` always resolves to a published definition, and no ID is ever re-issued. A task that makes a forgotten entry lower `next_id`, or admits an instrument before its line is flushed, has misread the design.

---

## Tasks

### 1. The record: version 2, appended lines, and a torn tail

- [ ] `FORMAT_VERSION` becomes 2, and `decode` also accepts 1.
- [ ] `Entry` gains `delisted_at: Option<u64>`, the Unix second it was last published, for a snapshot entry that was not published when written.
- [ ] `decode` reads the appended lines under the design's two rules, mint and restate, and returns the running `next_id`. It refuses a skipped or reused ID, a known ID with a different `Symbol`, and a timestamp on an appended line, each as its own `RecordError`.
- [ ] `decode` drops a final line with no newline, and reports that it did, so the registry can say so.
- [ ] `encode` writes a snapshot; `encode_line` writes one appended line.
- [ ] The `StateRecord` doc comment on `next_id` states the rule as it now stands: entries are removed, so `next_id` is the only thing standing between a retired ID and a new instrument.

**Test** (`tests/identity.rs` and the unit tests in `state.rs`): round trip with timestamps; a version-1 record reads with every entry recorded as published; each refusal above; a torn final line is dropped, and a complete malformed final line is refused.

**The revert:** accept a skipped ID in the appended lines, and the skipped-ID test fails. Drop the newline check, and the torn-tail test fails, because the partial line parses as a malformed entry.

---

### 2. `StateStore::append`

- [ ] The trait gains `append(&mut self, bytes: &[u8])`, with the design's contract on what a `load` after an error may see.
- [ ] `FileStore`: an `O_APPEND` handle on the record, opened lazily and reopened after every `store`, written with `write_all` and flushed with `sync_data`.
- [ ] `MemoryStore`: extends its record, and counts `store` and `append` calls for tests. `break_writes` fails both.

**Test** (`file_store.rs` unit tests, over a temporary directory): an append after a `store` lands in the new record rather than the renamed-away inode; appends survive a reopen of the store.

**The revert:** skip the reopen after `store`, and the first test fails: the line is written to the unlinked inode and `load` does not see it.

---

### 3. The registry appends, compacts, and forgets

- [ ] Per entry: the ID, whether the record holds it as published, and when it was last published.
- [ ] `open` sets the time an entry recorded as published was last published to the open time, applies the horizon, and compacts, every time.
- [ ] A mint appends one line. A relisting of an entry the record holds with a timestamp appends a restating line. Both happen before admission, and a failure is the existing fault.
- [ ] `withdraw` stamps the time in memory and writes nothing.
- [ ] Compaction runs when the appended lines reach `max(snapshot lines, 1_024)`, after the append that crosses the threshold. It forgets on the horizon, then stores the snapshot. A failure faults the registry and does not refuse the admission already persisted.
- [ ] `RegistryConfig::forget_delisted_after: Option<Duration>`.
- [ ] The doc comments on `persist`, `withdraw`, `minted` and the registry's guarantee state the new rule, including what a forgotten symbol costs when it is relisted.

**Test** (`tests/identity.rs`, over `MemoryStore` and `ManualClock`):
- a mint is one `append` and no `store` once the registry is open, asserted as counts and as the record growing by one line;
- 2,000 mints produce a bounded number of `store` calls, and the record after them decodes to the same map;
- with a horizon: an entry delisted for longer is gone after the next compaction, and relisting it mints `next_id`, not the old ID; an entry delisted for less is kept; a published entry is never forgotten, however old its mint;
- an entry recorded as published before a restart, and not offered after it, is not forgotten until the horizon has passed since the restart;
- a relisting of an entry recorded with a timestamp appends one restating line, and a restart after it keeps the entry past the old timestamp's horizon;
- `next_id` after forgetting is unchanged, across a restart.

**The revert (the plan's centre):** persist the whole record per mint again, and the append-count test fails. Stamp an entry recorded as published with its snapshot time instead of the open time, and the restart test fails. Skip the restating append, and the relisting-across-a-restart test fails.

---

### 4. The key

- [ ] `[refdata] forget_delisted_after`, an optional duration parsed by the existing `de_optional_duration`, refused at zero.
- [ ] Wired from `Config` into `RegistryConfig` in `run.rs`, and every other `RegistryConfig` literal gains `forget_delisted_after: None`.
- [ ] `BRINGING-UP-A-FEED.md`'s `[refdata]` section gains the key, its default, and what a relisting past it costs.

**Test** (`config.rs` unit tests): the key parses; absent is `None`; `"0s"` is refused with the key named.

**The revert:** drop the zero check, and the refusal test fails.

---

## Acceptance

The plan is done when:

1. a mint is one appended line, whatever the size of the record;
2. the record is bounded by twice the retained set, and with a horizon the retained set is bounded;
3. no ID is re-issued and no published ID resolves to nothing, across restarts, torn appends and forgetting;
4. a version-1 record starts a publisher with every ID it held;

and when each revert above turns its named test red.

## What this plan does not do

It does not group a poll's mints into one write, and it does not move writes off the tick. The design says why for both.
