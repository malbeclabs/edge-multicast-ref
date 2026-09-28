# Minting an Instrument ID costs one line

**Status:** design. Answers [#175](https://github.com/malbeclabs/edge-multicast-ref/issues/175).

## The failure

Every new `Instrument ID` rewrites the whole state record: each entry ever minted is serialised (about 137 bytes, since the symbol is written as 128 hexadecimal digits), written to a pending file, flushed, renamed over the record, and the directory flushed, all before the instrument is admitted. The write runs inside `poll_listings`, under the adapter lock the ingest path also takes, on a single-threaded runtime. The whole publisher waits on each one.

The argument that this is paid once rests on the selection policy's cap. The cap counts what is **published**; the record keeps every entry **ever minted**, delisted ones included. A venue that lists short-lived instruments never reaches a steady state. Fourteen new fifteen-minute windows every fifteen minutes is 1,344 mints a day. After a year the record holds about 490,000 entries and 67 MB, and it is rewritten 1,344 times a day: about 90 GB a day of flushed writes, still growing, each one stalling the feed.

Two things are wrong, and they have separate fixes:

- **A mint costs the size of the history.** It should cost one entry.
- **The history has no end.** The record and the map in memory grow for as long as the venue lists.

## A mint appends one line

The record stays one file, `instruments.state`, and becomes a snapshot followed by appended lines.

```
dz-refdata-state 2 <source_id> <next_id> <snapshot entries>
<instrument_id> <symbol, 128 hex digits>
<instrument_id> <symbol, 128 hex digits> <unix seconds>
...
<instrument_id> <symbol, 128 hex digits>
```

The **snapshot** is the header and as many entries as the header counts. The **appended lines** follow it. The count is what tells them apart, since a restating line names an ID already below `next_id`, just as a snapshot entry does. Every entry line uses one grammar:

- Two fields: this `Instrument ID` is this `Symbol`, and the instrument was published when the line was written.
- Three fields (snapshot only): this `Instrument ID` is this `Symbol`, the instrument was not published when the snapshot was written, and it was last published at that Unix second.

An appended line either **mints**, meaning its `Instrument ID` equals the running `next_id` and its `Symbol` is not yet in the record, which advances the running `next_id` by one, or **restates** an `(Instrument ID, Symbol)` pair already in the record as published. Anything else is a damaged record and a startup refusal, as it is now. That includes an appended line that skips an ID, reuses one, pairs a known ID with a different `Symbol`, or carries a timestamp.

**A final line with no terminating newline is discarded.** It is an append that never completed. The admission it was persisting did not happen, because an instrument is admitted only after its line is flushed, so nothing published depends on it. The compaction at startup rewrites the file without it. Every other kind of damage stays a refusal: a line that is complete and wrong was written by something this build did not write.

**What writes, and when:**

| Event | Write |
|---|---|
| A symbol never minted | One minting line, appended and flushed before the instrument is admitted. The first mint a directory ever sees writes a snapshot instead, because a line has nothing to be appended to until one exists. |
| A relisting of a symbol the record holds with a timestamp | One restating line, appended and flushed before admission. |
| A relisting of a symbol the record holds as published | Nothing. |
| A delisting | Nothing. The time is held in memory until the next compaction. |
| A re-offer of a published instrument | Nothing. |

A failed append is the fault it is now: the instrument is not admitted and nothing further is minted.

The restating line is what keeps a timestamp honest. A symbol delisted, compacted with a timestamp, and then relisted would otherwise carry the old timestamp through a restart. If the venue did not offer it again after that restart, it would look like it had been delisted since the old timestamp, and it could be forgotten early.

### Why a poll's mints are not grouped into one write

A group would save flushes, fourteen per window becoming one, but `ListingSink::list_on` returns the handle synchronously, and a handle is a promise that its `Instrument ID` is already persisted. Grouping means deferring the handle past the end of the poll, which changes the boundary for a saving that appending already makes small: fourteen flushes of 140 bytes each.

## Compaction

Compaction writes a fresh snapshot of every retained entry, with the atomic pending-file-and-rename that is used for every write now. It runs:

- **At open, when the record needs it:** a torn final line, which the next append would otherwise run on from; a version-1 record, which has no entry count to append after; an entry the horizon forgot while the publisher was down, which the record must not go on holding, since a later mint of the same symbol would then appear in it twice; or appended lines past the threshold. A record that needs none of these is not written at open.
- **While running, when the appended lines reach the snapshot's size**, and never below 1,024 lines. It runs after the append that crosses the threshold. That mint is already durable, so a failed compaction faults the registry without refusing the admission that set it off.

The threshold is what makes the cost constant. A snapshot holds at most the last one's entries plus the lines appended since, so it rewrites at most two entries for each line appended. Compaction runs on the tick like every other write. Its size is set by what is retained, which is why the horizon exists.

| Case | Snapshot | Appended between compactions |
|---|---|---|
A snapshot writes an entry as published when it is published. While seeding, it also does so when the record already holds the entry as published and the venue has not offered it yet: a seed that has not finished has not said the instrument is gone. Every other entry is written with the second it was last published.

| 1,344 mints a day, horizon `168h` | ~9,400 entries, ~1.3 MB | ~9,400 lines, about a week |
| 1,344 mints a day, no horizon, after a year | ~490,000 entries, ~67 MB | ~490,000 lines, about a year |

Without a horizon a mint is still constant and the file stays below twice the history, but the history still grows, and so does the compaction that eventually rewrites it.

## Forgetting delisted instruments

`[refdata] forget_delisted_after` is an optional duration. When it is set, compaction drops every entry that is not published and was last published at least that long ago. When it is absent, every entry is retained for good. Anything under a second is refused at load, because the record counts whole seconds and a horizon of zero forgets an instrument the moment it is delisted.

```toml
[refdata]
state_dir = "/var/lib/a-venue-publisher"
forget_delisted_after = "168h"
```

**What forgetting keeps.** `next_id` is the guarantee that an `Instrument ID` is never re-issued. It lives in the snapshot header, it is never derived from the entries, and forgetting never lowers it. What the map holds is the other promise: that a relisted symbol comes back under its own ID. A delisted instrument appears in no published definition, so dropping it cannot break the invariant that **a published `Instrument ID` always resolves to a published definition**.

**What forgetting costs, stated in the key's contract.** A symbol relisted after it has been forgotten is minted a **new** `Instrument ID`. A subscriber that kept the old one sees an instrument end and a different one begin. Set the horizon longer than any gap after which the venue relists a symbol it has delisted.

**When an entry was last published:**

- Delisted in this process: the time of the delisting, exactly.
- Recorded as published when the publisher last stopped, and not offered since: the time this process opened. The truth is somewhere between the last write and the stop. The open time is later than both, so the error only ever keeps an entry longer.
- Recorded with a timestamp: that timestamp.

So an entry is never forgotten before `forget_delisted_after` has passed since it was last published. The one exception is the wall clock itself: a clock stepped forward forgets early, and a clock stepped back keeps longer. The times are Unix seconds from the registry's `Clock`, which is the clock the manifest's `Timestamp` already uses.

## Compatibility

- **Upgrade:** a version-1 record is read as version 2, with every entry recorded as published, and rewritten as version 2 by the compaction at open.
- **Rollback:** a build that reads only version 1 refuses a version-2 record as `UnsupportedVersion` and does not start. That is the refusal the format tag exists to produce. A publisher rolled back across this change needs its record converted to a version-1 snapshot of the same entries and the same `next_id`, with the appended lines folded in and the timestamps dropped.
- **Wire:** no change. No message, field or metric is added.

## What changes

- `StateStore` gains `append`: the bytes are durable when it returns, and after an error a later `load` sees the record without them or with an unterminated prefix of them.
- `FileStore::append` writes through a handle opened with `O_APPEND` on the record, reopened after each `store`, and flushes with `sync_data`.
- `MemoryStore` appends to its record and counts `store` and `append` calls, so a test can show that a mint writes one line.
- `StateRecord` reads and writes version 2, reads version 1, and carries the appended lines' rules above.
- `Registry` holds per entry whether it is recorded as published and when it was last published, appends instead of rewriting, compacts on the threshold, and forgets on the horizon.
- `RegistryConfig` gains `forget_delisted_after: Option<Duration>`, and `[refdata]` gains the key.

## What this does not do

- **No change to what a subscriber sees** while the horizon is unset. With it set, the only change is a new `Instrument ID` for a symbol relisted after it was forgotten.
- **No change to the boundary.** `list_on` and `delist` keep their signatures and meaning.
- **No write moves off the tick.** Appends are small and compactions are rare and bounded. Moving either to another thread would give the store a second owner, and a single owner is what the single-writer guard is built on.
