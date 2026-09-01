# Sequence loss and conformance, as rows a dashboard can ask

**Status:** draft, pending review
**Date:** 2026-08-31
**Applies to:** the recorder's analysis tier, and the column store a dashboard reads
**Authority:** [`edge-feed-spec`](https://github.com/malbeclabs/edge-feed-spec) and its [`GLOSSARY.md`](https://github.com/malbeclabs/edge-feed-spec/blob/main/GLOSSARY.md); `2026-08-28-edge-recorder-crates-design.md`, whose analysis tier this specifies the output of

---

## Naming

This repository is public. This document names no venue, venue repository, host,
bucket, dashboard or issue tracker, and gives no count of publishers or of
recorder sites. It states only what is to be built.

`GLOSSARY.md` governs the vocabulary: `datagram` never `frame`, **`era` never
`epoch`** for the sequence space a `Reset Count` opens, `channel` only for the
`Channel ID` shard, port roles are `mktdata`/`refdata`/`snapshot`.

---

## The question

**Sequence is measured per channel instance — `(source address, Channel ID,
destination port)`.** Everything below follows from that, because it is the only
key under which a sequence number means anything: an operator may run redundant
publishers serving one channel to one group and port, each advancing its own
space and its own `Reset Count`, and a tracker keyed any coarser reads every
alternation as backward motion in one direction while letting one publisher's
heartbeats cover the other's total outage in the other.

Two things are to be answerable on one dashboard, over any window in the past:

1. **Loss** — how many datagrams are missing per channel instance, and *whose*
   they are.

**Loss is measured in sequence values, never in time.** A gap is a run of
sequence numbers nobody delivered, and its size is how many of them there were.
Duration is not a second way of saying the same thing: at 50 datagrams a second
a three-second gap is a hundred and fifty missing, and on a channel that only
heartbeats it is three, so a figure in seconds is not comparable between two
channels, between two hours of one channel, or against itself after a rate
change. It measures the feed's activity as much as the loss. Timestamps stay on
every row, because *when* is how a reader places a gap against an incident — but
the quantity is the count, and the rate is that count over the sequence numbers
the window should have carried.
2. **Conformance** — which spec rules passed, were violated, or could not be
   judged, per channel instance.

---

## What is generated today, and why it cannot answer either

**The archive and its manifest.** Per segment, the recorder writes provenance
(site, recorder, env, build, config hash), the object key and its sha256, the
receive window, and per channel instance: first and last sequence number,
datagram count, and the reset counts seen. Beside that: capture drop total,
interface drop total, short datagrams, the port roles it was *asked* to join
with their group, port, interface and source address, and whether the link
headers are captured or synthesised.

That is enough to answer coverage without opening an object, and it is the
input every row below is derived from. It is not enough for a dashboard: it is
per segment, it is JSON beside an object, and it has no rule verdicts.

**The health tier's metrics** answer the minutes-scale question — is the feed
alive, is this recorder dropping — and hold no history. They are the other half
of the dashboard, not this half.

**A sequence column already exists, and its model is right.** An operational
report already keys sequence health on the channel instance, quoting the same
glossary these documents do; it carries per-recording-node series with gap
episodes, and it already separates a loss that is one node's branch from one the
nodes share — reporting the shared case as an emptiness that *is* the finding.
Anything specified here has to fold into that, not beside it.

It makes one distinction these documents did not, and it is right: **the
destination port is folded for book-level fault counters.** Only the sequence
number is per port role; `Reset Count`, the manifest and the channel state they
govern span the three ports one publisher serves a channel on, so splitting a
book's gap, its reset and its snapshot cycle across three rows leaves each of
them looking incomplete. The full instance including the port stays the key for
*continuity* — that is what a sequence number is minted per — and the port is
folded when the question is about a book. The recording node is never folded:
two vantages of one instance are two observations, and merging them hides a
recorder that is missing the feed.

**What that column lacks is a source it can trust, and that is what these rows
are for.** Its gap counters come from a decoded level-grain table: TTL-less,
sorted for symbol and instrument questions rather than for sequence ones, so a
fifteen-minute question reads most of a day through a remote proxy — on the
order of a hundred million rows. The page therefore cannot query it at all and
folds a ten-minute refresher's cached payload instead, which means the freshness
of a sequence verdict is bounded by the refresher and not by the feed. Three
things follow that no amount of care in that handler can fix:

- **It exists only where a decoder does.** A feed without a bot writing level
  rows has no sequence column at all, while a recorder that keeps bytes it does
  not understand has one on the day the feed is first recorded.
- **The recorder's own loss is not separable.** Comparing nodes is a good proxy
  and it is the best one available without the recorder, but when every node
  drops together — a load spike reaches them all — a shared loss reads as the
  publisher's. `drop_delta` in the row makes "ours" a subtraction instead of an
  inference, and `drop_scope` says at what scope that subtraction is valid.
- **Header grain is the right grain for the question.** A per-datagram row is
  tens of bytes against a level row, and sorted by the instance and the era it
  turns a fifteen-minute question into a partition prune.

So these rows are a better source under an existing column, not a new column.

---

## The rows

Four grains, each earning its place by answering something the one above it
cannot. Every one of them carries the channel instance in full.

### 1. `datagram` — the base fact

One row per archived datagram. Everything else is derivable from this, which is
why it is the one row that must be exactly right.

```sql
CREATE TABLE recorder.datagram (
    recv_ts          DateTime64(9),
    send_ts          DateTime64(9),
    send_recv_ms     Float64 MATERIALIZED
                       (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(send_ts)) / 1e6,
    recv_ts_kind     LowCardinality(String),   -- kernel-software | application-fallback

    -- the channel instance, in full and never abbreviated
    source_addr      IPv4,
    channel_id       UInt8,
    dst_port         UInt16,

    feed             LowCardinality(String),   -- the spec name, never a venue
    port_role        LowCardinality(String),   -- mktdata | refdata | snapshot
    group_addr       IPv4,

    sequence_number  UInt64,
    reset_count      UInt8,                    -- the wire value, as sent
    era_index        UInt32,                   -- the era, assigned by the loader

    payload_len      UInt16,                   -- what the archive holds
    wire_payload_len UInt32,                   -- what was sent; larger means truncated
    drop_delta       UInt32,                   -- what the recorder lost before this one

    site             LowCardinality(String),
    recorder         LowCardinality(String),
    env              LowCardinality(String),
    drop_scope       LowCardinality(String),   -- port-role | capture-handle
    object_key       String,
    object_sha256    String
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (source_addr, channel_id, dst_port, era_index, sequence_number, site);
```

The order key is the channel instance, then the era, then the sequence. Every
loss query is a scan along that key, which is what makes it cheap.

**The era is `era_index`, not `reset_count`, and this is not a refinement.**
`Reset Count` is a `u8` on the wire: it wraps, and two eras on a long-lived
instance then share a number. Partitioning by it does not invent a gap — it
*hides* one. Measured on two eras that both carry reset count 3, the second of
which is missing five datagrams: partitioning by the wire value detects **zero**
gaps, because the earlier era's rows sit at exactly those sequence numbers.
Partitioning by a monotonic index detects the gap and its five datagrams. For a
system whose whole purpose is attribution, silently losing a finding is worse
than raising a false one, so the wire value is kept as a fact and never used as a
key.

The index is the loader's to assign, not a query's to derive: it increments per
channel instance whenever the reset count changes in receive order, which the
archive's segment sequence supplies. A window function cannot be trusted with it
because a query's window may not contain the transition.

`ReplacingMergeTree` because reprocessing is keyed on `(object_key,
object_sha256)`: a re-run after an analyser fix replaces rather than duplicates.

`recv_ts_kind` is carried because a latency computed from an application
fallback measures the recorder's own scheduler, and a panel that averages the
two together is measuring nothing.

`drop_scope` is the field a dashboard is most likely to get wrong, and it is
explained in *The arithmetic* below.

### 2. `segment_coverage` — the manifest, as a table

One row per segment per channel instance, loaded straight from the manifest and
without opening a single object.

```sql
CREATE TABLE recorder.segment_coverage (
    site                 LowCardinality(String),
    recorder             LowCardinality(String),
    env                  LowCardinality(String),
    feed                 LowCardinality(String),
    source_addr          IPv4,
    channel_id           UInt8,
    dst_port             UInt16,
    segment_seq          UInt64,
    start_ts             DateTime64(9),
    end_ts               DateTime64(9),
    first_seq            UInt64,
    last_seq             UInt64,
    datagram_count       UInt64,
    reset_counts_seen    Array(UInt8),
    capture_drop_total   UInt64,
    interface_drop_total UInt64,
    drop_scope           LowCardinality(String),
    roles_joined         Array(Tuple(String, IPv4, UInt16)),  -- role, group, port
    object_key           String,
    object_sha256        String,
    build_version        String,
    build_commit         String,
    config_hash          String
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(start_ts)
ORDER BY (source_addr, channel_id, dst_port, segment_seq);
```

This is what makes a coverage question cheap and a **missing object** visible: a
gap in `segment_seq` for a recorder run is a hole in the archive, and without it
a recorder that was down for an hour is indistinguishable from a feed that was
quiet for an hour. It is also where `roles_joined` lets a silent port report
`na` instead of `pass` — a port nobody joined produces no data, and no data
looks exactly like a clean feed.

### 3. `sequence_gap` — one row per gap, with a verdict

The row the dashboard actually wants. Derived, re-derivable, and the only place
attribution is decided.

```sql
CREATE TABLE recorder.sequence_gap (
    site              LowCardinality(String),
    recorder          LowCardinality(String),
    env               LowCardinality(String),
    feed              LowCardinality(String),
    port_role         LowCardinality(String),
    group_addr        IPv4,               -- the consuming report keys on it
    source_addr       IPv4,
    channel_id        UInt8,
    dst_port          UInt16,
    reset_count       UInt8,              -- the wire value at the time
    era_index         UInt32,             -- the era; a gap never spans two
    missing_from      UInt64,             -- first sequence number absent
    missing_to        UInt64,             -- last sequence number absent
    missing_count     UInt64,
    /// What the missing count is a share of: the sequence numbers this site
    /// should have seen over the window. Without it there is no rate, and a
    /// bare count of missing datagrams says nothing about a feed's health.
    reference_seqs    UInt64,
    -- Placement, never the measure: these say when to look, and the count says
    -- how much was lost.
    before_ts         DateTime64(9),      -- the datagrams either side, locally
    after_ts          DateTime64(9),
    /// When the missing datagrams were actually sent, from a site that did
    /// record them. A site has no clock reading for a datagram it never
    /// received, so its own bracket is the weaker answer and the publisher's
    /// send timestamp — recovered from elsewhere — is the stronger one.
    sent_from_ts      Nullable(DateTime64(9)),
    sent_to_ts        Nullable(DateTime64(9)),

    admitted_recorder UInt64,             -- our own drops covering this gap
    admitted_scope    LowCardinality(String),
    unexplained_count UInt64,             -- missing_count less what we admit
    interface_drops   UInt64,             -- upstream of the capture point
    seen_elsewhere    UInt8,              -- present at another site
    on_redundant_path UInt8,              -- present in another instance on this channel and port
    verdict           LowCardinality(String),
    object_key        String              -- where the evidence is
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(before_ts)
ORDER BY (source_addr, channel_id, dst_port, era_index, missing_from);
```

`verdict` is one of five, and the order they are tested in is the whole design:

| verdict | when | what it costs |
|---|---|---|
| `recorder` | the gap is covered by our own admitted drops, at a scope where the subtraction is valid | a counter and an alert on us, never a publisher finding |
| `upstream` | not covered by ours, but interface drops rose over the window | a switch or link question, not a publisher one |
| `path` | absent from this instance, present in a redundant instance on the same channel and port | the redundancy earned its cost; not feed loss |
| `unverifiable` | the gap touches a segment boundary, a missing `segment_seq`, or a window with no coverage row | nothing — and saying so is the point |
| `publisher` | absent from *every* site, with no recorder overflow anywhere and coverage intact | the finding, and now a strong one |

**A gap can be partly ours.** Five datagrams missing with three admitted is neither
`recorder` nor `publisher`, and a single verdict per gap cannot say so — which is
why the row carries `unexplained_count` and the verdict is decided on *that*
residue rather than on `missing_count`. A gap fully covered by our own drops has
an unexplained count of zero and never leaves our own alerting.

**`unverifiable` is a first-class verdict, not a failure to compute.** A rule
set that reports a violation where it merely could not see is a rule set nobody
trusts twice. The conformance tool already works this way for live capture; the
archive is what makes the gate open far more often, because lossless replay
turns most `unverifiable` into `pass` or `violation`.

### 4. `conformance_finding` — the rule set's verdicts, kept

```sql
CREATE TABLE recorder.conformance_finding (
    run_ts           DateTime64(9),       -- when the rule set ran
    rule_id          LowCardinality(String),
    rule_set_version LowCardinality(String),
    site             LowCardinality(String),
    recorder         LowCardinality(String),
    env              LowCardinality(String),
    feed             LowCardinality(String),
    port_role        LowCardinality(String),
    source_addr      IPv4,
    channel_id       UInt8,
    dst_port         UInt16,
    window_start     DateTime64(9),
    window_end       DateTime64(9),
    verdict          LowCardinality(String),   -- pass | violation | unverifiable | na
    detail           String,
    object_key       String,
    first_seq        UInt64,                   -- the evidence range
    last_seq         UInt64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (rule_id, source_addr, channel_id, dst_port, window_start);
```

`rule_set_version` and `run_ts` are load-bearing rather than bookkeeping: a rule
added next month runs against last month's traffic, so the same window legally
holds two verdicts from two versions, and a dashboard that cannot say which
version produced a verdict cannot show that the rule set improved.

---

## The arithmetic, and the three ways it goes wrong

**Continuity keys on the full instance; a book-level counter folds the port.**
A sequence number is minted per channel instance including the destination port,
so that is the key a gap is computed under. `Reset Count`, the manifest and the
channel state they govern span the three ports one publisher serves a channel
on, so a counter about a *book* folds the port — otherwise one book's gap, its
reset and its snapshot cycle land on three rows that each look like they are
missing something. The rows below carry the port so both are derivable; what
must never be folded is the recording site, because two vantages of one instance
are two observations and merging them hides a recorder that is missing the feed.

**A gap is per instance *and* per era.** A reset opens a new sequence space, so
a sequence number that goes backwards across one is not backward motion and a
gap computed across the boundary is an artefact. Every window function therefore
partitions by `(source_addr, channel_id, dst_port, era_index)` — the loader's
monotonic index, never the wrapping wire value, for the reason given above.

```sql
-- gaps, per channel instance, per era
SELECT source_addr, channel_id, dst_port, era_index,
       prev_seq + 1 AS missing_from,
       sequence_number - 1 AS missing_to,
       sequence_number - prev_seq - 1 AS missing_count
FROM (
    SELECT source_addr, channel_id, dst_port, era_index, sequence_number, recv_ts,
           lagInFrame(sequence_number) OVER w AS prev_seq
    FROM recorder.datagram
    WHERE recv_ts BETWEEN {from} AND {to}
    WINDOW w AS (PARTITION BY source_addr, channel_id, dst_port, era_index
                 ORDER BY sequence_number
                 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
)
WHERE prev_seq > 0 AND sequence_number - prev_seq > 1;
```

**The deriver expands before it joins.** ClickHouse has no correlated
subqueries, so "was this sequence number seen anywhere else" cannot be a
per-row subselect — that fails outright. Expand each gap into its missing
sequence numbers with `arrayJoin(range(missing_from, missing_to + 1))` and join
those on equality instead. That is not a workaround: it attributes per datagram
rather than per range, so a gap half of which appears at another site is
reported as half, and the join keys are the sort key's own columns.

**Wrong way 1: subtracting our drops at the wrong scope.** `drop_delta` is what
*we* lost. In socket mode there is one accumulator per port role, so the number
is per role and may be subtracted per role. In `AF_PACKET` mode the ring counts
frames dropped **before demultiplexing**, so the number belongs to the capture
handle and not to any one role — a delta caused by `mktdata` frames may ride on
the next `refdata` datagram that gets through. Subtracting it per role would
credit one role with another's losses and leave the first role's gap looking
unexplained, which manufactures exactly the publisher finding this whole design
exists to prevent.

That is why `drop_scope` travels on every row — and the rule is not a sum, which
is what the first draft of this document got wrong. Measured on mixed-scope data:
a ring dropped forty `mktdata` datagrams and the delta rode on the next
`refdata` datagram that got through. Summing admitted drops per instance
reported **forty unexplained** against `mktdata` and a false publisher finding,
while the handle had admitted all forty.

The two scopes take different arithmetic, not the same arithmetic at different
grains:

| Scope | Instances on the role | Admitted is | A gap of *n* is |
|---|---|---|---|
| `port-role` | one | that instance's own `drop_delta` sum | `recorder` when *n* ≤ admitted; the residue carries on to the next verdict |
| `port-role` | more than one | the *socket's*, and no instance's | `unverifiable`, unless the role admitted nothing at all |
| `capture-handle` | any | meaningless per instance — the ring counts frames dropped before demultiplexing | `unverifiable` for recorder attribution whenever the handle admitted anything at all over the window, and **never** `publisher` |

The middle row is the same mistake as the handle scope, one grain finer, and it
is the easier of the two to write by accident: the accumulator at `port-role` is
the *socket*, and a socket carries every instance on its group and port. Its
delta rides on whichever datagram next gets through, from any of them. Two
publishers on a group, or two `Channel ID`s on one port, are enough — and
subtracting one instance's share then exonerates whichever arrived next and
charges the other for loss the recorder caused.

So at handle scope the archive can only *exonerate* itself, and only when its
own total is zero — and at role scope with more than one instance, the same. That is the common case and the interesting one: a recorder
admitting nothing turns every gap into someone else's, with evidence. A recorder
that dropped anything cannot say which role lost it, and must not guess.

Precision we do not have is worse than scope we declare.

**Wrong way 2: reading a cumulative counter as a rate.** `interface_drops` and
capture totals are cumulative and never reset, so a host carries the sum of
every burst it ever had. A panel showing the total shows history; only the delta
over the window says anything about now.

**Wrong way 3: calling a boundary a gap.** A gap whose either side falls on a
segment boundary, or in a window where `segment_coverage` has a hole in
`segment_seq`, is `unverifiable`. So is a gap in a port role that
`roles_joined` never claimed — that is `na`, and reporting `pass` there is
reporting a pass over a rule that never ran.

**Redundant publishers are the useful case, not the awkward one.** Two source
addresses on one channel and port are two instances, and a sequence number
absent from one but present in the other is `path` loss. The ratio of those is
the fill rate — the number that says whether the redundancy is earning its cost.

---

## Feeding the Sequence column

The decision is that the operational report's Sequence column is fed by the
recorders' own sequence-loss detection, replacing the derivation from a decoded
level-grain table. That makes this table's shape a contract rather than a
proposal, and the consumer's shape is already fixed: one row per
`(group, publisher source, Channel ID, recording node)` carrying a missing
count, the reference count it is a share of, and the episodes.

The mapping is nearly one to one, and where it is not, this table was the one
that was short:

| The report needs | From here |
|---|---|
| multicast group | `group_addr` — added for this, because the report keys on it and a gap row without it cannot be placed |
| publisher source, Channel ID | `source_addr`, `channel_id` |
| recording node, location | `recorder`, `site` — never folded together, because two vantages of one instance are two observations |
| missing | `unexplained_count` summed over the window, **not** `missing_count` — the recorder's own admitted loss is subtracted first, which is the thing the previous source could not do |
| reference count | `reference_seqs` — added for this: without it there is no rate |
| episodes | one row per contiguous run of missing sequence numbers — which is what a gap row already is — carrying `missing_from`, `missing_to` and the count; the timestamps place it |

**One divergence to settle in the consumer, not here.** Its episodes are
contiguous runs of *seconds*, and the measure has to be sequence values: a run
of seconds cannot be compared between two channels or between two hours of one,
because it is a statement about how busy the feed was as much as about what was
lost. A gap row already is a contiguous run — of sequence numbers — so the
mapping is to send the run and its count, and to let the timestamps place it on
a chart rather than quantify it. A seconds figure derived from a run is
presentable beside the count; it is not a substitute for it.

Two properties the recorder brings that the level-grain source could not:

- **A feed with no decoder still has a Sequence column.** The counters come from
  the datagram header, so a feed is covered on the day it is first recorded
  rather than on the day a bot learns to fold its book.
- **The recorder's own loss is subtracted rather than inferred.** Comparing nodes
  catches a loss one node has alone; it cannot catch one they share, and a load
  spike reaches them all. `drop_delta` makes that a subtraction, at the scope
  `drop_scope` declares it valid at.

And one the recorder must not break: the episode timestamps have to come from a
site that recorded the datagram, because a site has no clock for what it never
received. That is a cross-site read, so a gap row is complete only after the
join below — which is why `verdict` has an `unverifiable` value and why a row
may be written before it can say `publisher`.

## Cross-site, which needs no new table

`(channel instance, sequence number)` identifies a datagram independently of who
received it, so the same datagram recorded at two sites joins on that key in
`datagram`. That yields, with no credentials and no venue involvement:

- per-site loss on one feed over one window — a datagram present at one site and
  absent at another was not a publisher gap
- per-site arrival latency from one publisher send timestamp, so sites are
  compared on a single clock rather than on their own
- which site saw it first, and by how much
- publisher-attributable loss, isolated: absent from *every* site, with no
  recorder overflow anywhere

The `seen_elsewhere` column on `sequence_gap` is this join, precomputed for the
panels that cannot afford it live.

---

## The dashboard

Two rows of panels, because the two questions have different clocks.

**Now (from the health tier's metrics, minutes-scale):** feed alive per channel
instance; this recorder's own drop rate as a delta; send-to-receive latency
histogram per port role; heartbeat cadence and channel silence; a recorder whose
capture drops are rising, which is a fact you want *before* you rely on its
archive.

**After the fact (from these rows):**

| Panel | Row | The question |
|---|---|---|
| Missing datagrams by verdict, stacked over time | `sequence_gap` | is loss ours, upstream, a path, or the publisher's? |
| Publisher loss per channel instance, top N | `sequence_gap` where `verdict = 'publisher'` | who is actually skipping |
| Coverage heatmap: instance × hour, with holes | `segment_coverage` | can this window be trusted at all? |
| Conformance verdicts by rule, stacked | `conformance_finding` | what is failing, and what could not be judged |
| `unverifiable` share, over time | `conformance_finding` | is the archive making the gate open? |
| Fill rate per redundant path | `sequence_gap` where `verdict = 'path'` | is the redundancy earning its cost? |
| Site-to-site delta on one feed | `datagram`, self-joined | which site leads, and by how much |
| Truncated and over-cap datagrams | `datagram` where `wire_payload_len > payload_len` | a publisher violation the archive keeps |

**The two halves must not disagree.** The health tier's labels are `site`,
`recorder`, `feed`, `channel`, `role`, `source`; the row columns above are the
same names for the same things. A dashboard where the live panel and the
historical panel disagree about what a channel is teaches nobody anything.

---

## What this needs that does not exist yet

Stated plainly, because the analysis tier is plan 3 and none of it is built:

- **The loaders.** Per-datagram and per-segment, idempotent on `(object key,
  sha256)`. The per-datagram loader is a replay plus a header read — no message
  walk — so it is the cheap half.
- **`drop_scope` reaching the rows.** The archive now declares it per segment;
  the loader must carry it, or every subtraction above is silently wrong.
- **The gap deriver**, which is where the five verdicts are decided, and the
  only component here that needs the cross-site join to be complete before it
  can say `publisher` rather than `unverifiable`.
- **The conformance runner over replay**, writing verdicts with their rule set
  version instead of discarding them.
- **A retention split.** `datagram` is the expensive table and the one every
  question is asked against; `sequence_gap`, `segment_coverage` and
  `conformance_finding` are tens of bytes against a 1232-byte datagram and are
  worth keeping indefinitely. Expire the base rows on the same window as the raw
  `mktdata` objects, and keep the derived ones.
- **A `Reset Count` wrap decision.** It is a `u8`. A long-lived instance wraps,
  and two eras then share a number. Either the loader carries a monotonic era
  index derived at load time, or every query above silently merges two spaces
  once per 256 resets. This is the one open question here worth settling before
  the loader is written rather than after.

---

## Non-goals

No venue clients, credentials, or comparison against a venue's own service. The
comparison here is a feed against itself as seen from somewhere else.

No new conformance rule set. These rows *keep* the existing rule set's verdicts;
rules are proposed upstream, not forked here.

No repoint of the existing per-event tables. They are keyed for instrument and
symbol questions and cannot express a channel instance; adding these tables
beside them is cheaper and clearer than widening them.
