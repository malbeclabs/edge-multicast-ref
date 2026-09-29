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

- [x] `FORMAT_VERSION` becomes 2, and `decode` also accepts 1.
- [x] `Entry` gains `delisted_at: Option<u64>`, the Unix second it was last published, rounded up, for a snapshot entry that was not published when written.
- [x] `decode` reads the appended lines under the design's two rules, mint and restate, and returns the running `next_id`. It refuses a skipped or reused ID, a known ID with a different `Symbol`, and a timestamp on an appended line, each as its own `RecordError`.
- [x] `decode` drops a final appended line with no newline, one past every entry the version-2 header counts, and reports that it did and the length before it, so the registry can cut it off. A snapshot entry or a version-1 line with no newline is refused.
- [x] `encode` writes a snapshot; `encode_line` writes one appended line.
- [x] The `StateRecord` doc comment on `next_id` states the rule as it now stands: entries are removed, so `next_id` is the only thing standing between a retired ID and a new instrument.

**Test** (`tests/persistence.rs`): round trip with timestamps; a version-1 record reads with every entry recorded as published; each refusal above; a torn final appended line is dropped, a complete malformed final line is refused, and an unterminated snapshot entry or version-1 final line is refused.

**The revert:** accept a skipped ID in the appended lines, and the skipped-ID test fails. Drop the newline check, and the torn-tail test fails, because the partial line parses as a malformed entry.

---

### 2. `StateStore::append`

- [x] The trait gains `append(&mut self, bytes: &[u8])`, with the design's contract on what a `load` after an error may see: none of the bytes, a prefix, or all of them.
- [x] The trait gains `truncate(&mut self, len: usize)`, which cuts the record back to `len` bytes, durably.
- [x] `FileStore`: an `O_APPEND` handle on the record, opened lazily and reopened after every `store`, written with `write_all` and flushed with `sync_data`. `truncate` is `set_len` and `sync_data`.
- [x] `MemoryStore`: extends its record, and counts `store` and `append` calls for tests. `break_writes` fails all three writes. `break_flushes` lets an append land whole and then fails it.

**Test** (`tests/persistence.rs`, over a temporary directory): an append after a `store` lands in the new record rather than the renamed-away inode; appends survive a reopen of the store; a torn final line is cut off a real record in place.

**The revert:** skip the reopen after `store`, and the first test fails: the line is written to the unlinked inode and `load` does not see it.

---

### 3. The registry appends, compacts, and forgets

- [x] Per entry: the ID, whether the record holds it as published, and when it was last published.
- [x] `open` sets the time an entry recorded as published was last published to the open time, applies the horizon, and compacts when the design's three conditions call for it. A torn final line with no compaction due is cut off with `truncate`, not rewritten.
- [x] Every stamp rounds the clock up to a whole second, the horizon rounds up, and the current second rounds down, so no rounding forgets an entry early.
- [x] A mint appends one line. A relisting of an entry the record holds with a timestamp appends a restating line. Both happen before admission, and a failure is the existing fault.
- [x] `withdraw` stamps the time in memory and writes nothing.
- [x] Compaction runs when the appended lines reach `max(snapshot entries, COMPACTION_FLOOR)`, after the append that crosses the threshold. It forgets on the horizon, then stores the snapshot. A failure faults the registry and does not refuse the admission already persisted.
- [x] `RegistryConfig::forget_delisted_after: Option<Duration>`.
- [x] The doc comments on `persist`, `withdraw`, `minted` and the registry's guarantee state the new rule, including what a forgotten symbol costs when it is relisted.

**Test** (`tests/persistence.rs`, over `MemoryStore` and `ManualClock`):
- the first mint a directory ever sees is one `store`, and every mint after it is one `append` and no `store`, asserted as counts and as the record growing by one line;
- 2,000 mints produce a bounded number of `store` calls, and the record after them decodes to the same map;
- with a horizon: an entry delisted for longer is gone after the next compaction, and relisting it mints `next_id`, not the old ID; an entry delisted for less is kept; a published entry is never forgotten, however old its mint;
- an entry recorded as published before a restart, and not offered after it, is not forgotten until the horizon has passed since the restart;
- a relisting of an entry recorded with a timestamp appends one restating line, and a restart after it keeps the entry past the old timestamp's horizon;
- `next_id` after forgetting is unchanged, across a restart;
- a delisting at 0.999 s into a second is not forgotten at the start of the next under a one-second horizon, and a `1500ms` horizon does not forget at 1.2 s;
- an append whose flush fails admits nothing and faults the registry, and after a restart the whole line it left reserves its ID, which no other instrument is given.

**The revert (the plan's centre):** persist the whole record per mint again, and the append-count test fails. Stamp an entry recorded as published with its snapshot time instead of the open time, and the restart test fails. Skip the restating append, and the relisting-across-a-restart test fails.

---

### 4. The key

- [x] `[refdata] forget_delisted_after`, an optional duration parsed by the existing `de_optional_duration`, refused under a second.
- [x] Wired from `Config` into `RegistryConfig` in `run.rs`, and every other `RegistryConfig` literal gains `forget_delisted_after: None`.
- [x] `BRINGING-UP-A-FEED.md`'s `[refdata]` section gains the key, its default, and what a relisting past it costs.

**Test** (`tests/config_document.rs`): the key parses; absent is `None`; `"0s"` and `"500ms"` are refused with the key named.

**The revert:** drop the zero check, and the refusal test fails.

---

## The reverts, as run

Each was applied to a committed tree, from a copy taken for that mutant alone, with the replacement asserted to have matched.

| Revert | Fails |
|---|---|
| Write a whole snapshot per mint | `a_mint_appends_one_line_and_rewrites_nothing`, `a_venue_that_lists_forever_rewrites_the_record_a_bounded_number_of_times`, and two more |
| Measure the threshold against the map instead of the snapshot | `a_venue_that_lists_forever_rewrites_the_record_a_bounded_number_of_times`, and four more |
| Stamp an entry recorded as published with 0 instead of the open time | `an_entry_published_when_the_publisher_stopped_is_measured_from_the_restart`, and four more |
| Skip the restating append | `a_relisting_the_record_holds_as_delisted_is_written_down_before_it_is_admitted`, `a_relisting_that_cannot_be_written_down_admits_nothing` |
| Forget a published entry | `a_published_instrument_is_never_forgotten_however_old_its_mint` |
| Forget at open without rewriting | `an_entry_forgotten_at_open_is_gone_from_the_record_before_its_symbol_is_minted_again` |
| Keep the append handle across a `store` | `an_append_after_a_snapshot_lands_in_the_record_that_replaced_the_old_one` |
| Refuse a torn final line | `a_torn_final_line_is_dropped_and_the_next_mint_does_not_run_on_from_it` |
| Leave a torn final line in place at open | `a_torn_final_line_is_dropped_and_the_next_mint_does_not_run_on_from_it` |
| Accept an appended line that skips an ID | `a_record_that_is_complete_and_wrong_is_refused` |
| Accept a horizon under a second | `a_forget_delisted_after_under_a_second_is_refused` |
| Stamp a delisting with the second rounded down | `a_delisting_late_in_a_second_is_not_forgotten_early_in_the_next` |
| Round the horizon down | `a_horizon_with_a_fraction_of_a_second_is_not_cut_short` |
| Rewrite a torn final line at open instead of cutting it off | `a_torn_final_line_is_dropped_and_the_next_mint_does_not_run_on_from_it`, `a_torn_final_line_is_cut_off_a_real_record_in_place` |
| Make `FileStore::truncate` cut nothing | `a_torn_final_line_is_cut_off_a_real_record_in_place` |
| Drop a torn final line of a version-1 record | `a_record_that_is_complete_and_wrong_is_refused` |
| Leave the registry unfaulted after a failed append | `an_append_whose_flush_fails_costs_an_id_and_admits_nothing`, `a_relisting_that_cannot_be_written_down_admits_nothing` |

## Acceptance

The plan is done when:

1. every mint after a directory's first is one appended line, whatever the size of the record;
2. the record is bounded by twice the retained set, and with a horizon the retained set is bounded;
3. no ID is re-issued and no published ID resolves to nothing, across restarts, torn appends, failed flushes and forgetting;
4. a version-1 record starts a publisher with every ID it held;

and when each revert above turns its named test red.

## What this plan does not do

It does not group a poll's mints into one write, and it does not move writes off the tick. The design says why for both.
