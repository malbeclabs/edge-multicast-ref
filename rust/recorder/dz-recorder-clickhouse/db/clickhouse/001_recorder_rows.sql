-- The recorder's analysis tier: four grains, plus the era openings a monotonic
-- index is a rank over.
--
-- Applied by hand or by the deploy, as the demo's schema already is. There is no
-- migration framework here on purpose: a schema that a process applies to itself
-- at startup is a schema that changes when a binary is rolled, and these tables
-- are read by dashboards that outlive any one loader build.
--
-- ONE GENERIC SET OF TABLES, WITH THE FEED AS A COLUMN. Never a table per feed.
-- The order key is the channel instance, so a per-feed table moves the
-- discriminator into the table *name*, where no query can range over it: every
-- cross-feed question becomes a UNION written by hand, and the join that
-- isolates publisher loss — the same datagram seen from somewhere else — becomes
-- a fan-out over the product of feeds and sites instead of one scan along a sort
-- key. `feed` is LowCardinality(String), dictionary-encoded per part, and costs
-- approximately nothing beside that. The grain earns a table; the feed is a
-- value at every grain.
--
-- WHERE A PER-SHAPE TABLE IS RIGHT, and it is not per feed: the decoded message
-- grain, because a top-of-book quote row and a level row have genuinely
-- different columns. Nothing here decodes a payload.
--
-- TIMESTAMPS ARRIVE AS INTEGER NANOSECOND COUNTS. Every DateTime64(9) below is
-- inserted from a JSON number, which ClickHouse reads in the column's own
-- precision units — so the loader sends `recv_ts_ns` unchanged and there is no
-- formatting step to get a time zone wrong in.
--
--
-- WHERE THIS DIFFERS FROM THE DESIGN'S OWN DDL, AND WHY
--
-- Each of these is a place where the design's DDL and the design's stated
-- principles disagreed, or where a key the DDL gave would collapse two genuine
-- rows into one. They are stated here together rather than discovered in a
-- query that returned the wrong number.
--
-- 1. `datagram` carries no `era_index`. The design listed one and put it in the
--    sort key; it also says a datagram row carries only what its own object
--    states and that the index is a rank over the `era` table. Both cannot hold:
--    a stored rank is renumbered by any later-arriving *earlier* object, which
--    is what a backfill is, and renumbering a column inside the sort key of the
--    largest table rewrites that table. The row carries `reset_count` — the wire
--    fact — and `segment_seq`, and the era is resolved by range join to `era`.
--
-- 2. `recv_ts` is the last column of `datagram`'s sort key. Without it two
--    genuine rows collapse under ReplacingMergeTree: a datagram the network
--    delivered twice, and two eras 256 resets apart sharing a `Reset Count` at
--    overlapping sequence numbers. With it, a re-run of the same object still
--    produces byte-identical rows and still collapses, so idempotence is intact.
--    The leading columns are unchanged, so every loss query is the same prefix
--    scan. One case remains indistinguishable — a duplicate whose receive stamp
--    matches to the nanosecond — and `segment_coverage.datagram_count` is the
--    arrival count that keeps it.
--
-- 3. `sequence_gap` is keyed on `era_anchor_ts` rather than on `era_index`. The
--    index a loader can compute is local to the object it loaded; the anchor is
--    the era's own opening stamp and is therefore unique per instance per site,
--    which is what a deduplication key has to be.
--
-- 4. `era`, `segment_coverage` and `conformance_finding` carry `site` and
--    `recorder` inside their sort keys. `segment_seq` restarts at 0 on every
--    recorder run and is not unique across recorders at all; an era's anchor is
--    a *receive* stamp and is therefore one site's observation of that era. The
--    design's own rule is that the recording site is never folded, because two
--    vantages of one instance are two observations and merging them hides a
--    recorder that is missing the feed.
--
-- 5. Four columns on `sequence_gap` are Nullable where the design had them
--    plain: `unexplained_count`, `interface_drops`, `seen_elsewhere` and
--    `on_redundant_path`. Each is a place where *unknown* is a third answer and
--    both plausible defaults are wrong — a zero residue exonerates the
--    publisher, a full one accuses it — and the design's own rule is that
--    precision we do not have is worse than scope we declare.
--
-- 6. `era` carries `continuation`, and a settled continuation writes a row
--    saying so rather than writing none. A row that cannot be deleted cannot be
--    corrected by omission, so a boundary first seen uncertain could never
--    afterwards be settled as a continuation; and the absence of a row is
--    indistinguishable from an object nobody loaded. The rank ranks
--    `continuation = 0` rows, so every query sees what the design intended.

CREATE DATABASE IF NOT EXISTS recorder;

-- 1. The base fact. Everything else is derivable from this, which is why it is
--    the one row that must be exactly right — and the one table with a TTL.
CREATE TABLE IF NOT EXISTS recorder.datagram (
    recv_ts          DateTime64(9),
    send_ts          DateTime64(9),
    -- Materialised, so the subtraction happens once and in the schema. The
    -- loader never sends this column.
    send_recv_ms     Float64 MATERIALIZED
                       (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(send_ts)) / 1e6,
    -- kernel-software | application-fallback. A latency computed from an
    -- application fallback measures the recorder's own scheduler; this column
    -- exists so a panel can exclude it, and averaging the two measures nothing.
    recv_ts_kind     LowCardinality(String),

    -- The channel instance, in full and never abbreviated. It is the only key
    -- under which a sequence number means anything: an operator may run
    -- redundant publishers serving one channel to one group and port, each
    -- advancing its own space and its own Reset Count.
    source_addr      IPv4,
    channel_id       UInt8,
    dst_port         UInt16,

    feed             LowCardinality(String),
    port_role        LowCardinality(String),   -- mktdata | refdata | snapshot
    group_addr       IPv4,

    sequence_number  UInt64,
    -- The wire value, as sent. Kept as a fact and used as a key nowhere: it is a
    -- u8 and it wraps, so two eras 256 resets apart share a value. Measured on
    -- two eras both carrying reset count 3, the second missing five datagrams:
    -- partitioning by the wire value detects ZERO gaps, because the earlier
    -- era's rows sit at exactly those sequence numbers.
    reset_count      UInt8,
    segment_seq      UInt64,

    payload_len      UInt16,                   -- what the archive holds
    wire_payload_len UInt32,                   -- what was sent; larger means truncated
    drop_delta       UInt32,                   -- what the recorder lost before this one

    site             LowCardinality(String),
    recorder         LowCardinality(String),
    env              LowCardinality(String),
    -- port-role | capture-handle: the scope drop_delta may be subtracted at. A
    -- ring counts frames dropped BEFORE demultiplexing, so at capture-handle
    -- scope the number belongs to the handle and to no port role in particular.
    -- Subtracting it per role credits one role with another's losses and leaves
    -- the first role's gap looking unexplained, which manufactures exactly the
    -- publisher finding this whole tier exists to prevent.
    drop_scope       LowCardinality(String),
    object_key       String,
    object_sha256    String
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (source_addr, channel_id, dst_port, sequence_number, site, recv_ts);

-- 2. Where each era opened. One row per reset rather than one per datagram, so
--    the monotonic index is a rank over a small table and never a number
--    anybody has to trust because it was written down once.
--
--    ReplacingMergeTree(anchor_certain) makes late evidence an upgrade and never
--    a regression: a settled row always wins over an unsettled one, whichever
--    order the two loads happened in. Evidence arriving late upgrades a verdict;
--    its absence never blocks one.
CREATE TABLE IF NOT EXISTS recorder.era (
    site           LowCardinality(String),
    recorder       LowCardinality(String),
    feed           LowCardinality(String),
    source_addr    IPv4,
    channel_id     UInt8,
    dst_port       UInt16,
    anchor_ts      DateTime64(9),  -- receive stamp of the era's first datagram
    anchor_seq     UInt64,         -- its sequence number
    reset_count    UInt8,          -- the wire value, as a fact
    segment_seq    UInt64,
    -- 1 when the preceding segment was available to settle the boundary. A gap
    -- whose era carries 0 cannot be escalated past `unverifiable`: a finding
    -- that might be an artefact of what we failed to keep is not a finding.
    anchor_certain UInt8,
    -- 1 when the evidence said this anchor continues the era the preceding
    -- segment ended in, so it opens no era.
    continuation   UInt8,
    object_key     String,
    object_sha256  String
)
ENGINE = ReplacingMergeTree(anchor_certain)
ORDER BY (site, recorder, source_addr, channel_id, dst_port, anchor_ts);

-- 3. The manifest, as a table: loaded without opening a single object.
--
--    This is what makes a coverage question cheap and a MISSING OBJECT visible.
--    A hole in segment_seq for a recorder run is a hole in the archive, and
--    without it a recorder that was down for an hour is indistinguishable from a
--    feed that was quiet for an hour. roles_joined is what lets a silent port
--    report `na` rather than `pass`: a port nobody joined produces no data, and
--    no data looks exactly like a clean feed.
CREATE TABLE IF NOT EXISTS recorder.segment_coverage (
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
    -- A set, and therefore silent about which member came last. That is why a
    -- loader carries the last value forward itself rather than reading it back
    -- from here.
    reset_counts_seen    Array(UInt8),
    -- Cumulative and never reset, both of them. A panel showing the total shows
    -- the host's whole history; only the delta between two of these rows says
    -- anything about a window.
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
ORDER BY (source_addr, channel_id, dst_port, segment_seq, site, recorder, start_ts);

-- 4. One row per gap, with a verdict. The row a dashboard actually wants:
--    derived, re-derivable, and the only place attribution is decided.
--
--    LOSS IS MEASURED IN SEQUENCE VALUES, NEVER IN TIME. At fifty datagrams a
--    second a three-second gap is a hundred and fifty missing, and on a channel
--    that only heartbeats it is three — so a figure in seconds compares neither
--    two channels nor two hours of one. before_ts and after_ts place a gap
--    against an incident; missing_count is the quantity.
CREATE TABLE IF NOT EXISTS recorder.sequence_gap (
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
    -- The era's ordinal within the object this row was derived from, counting
    -- from 1. Not the globally dense rank: that is a rank over `era` and
    -- therefore a property of the whole archive. era_anchor_ts is the join key
    -- that reaches it.
    era_index         UInt32,
    era_anchor_ts     DateTime64(9),
    anchor_certain    UInt8,
    missing_from      UInt64,             -- first sequence number absent
    missing_to        UInt64,             -- last sequence number absent
    missing_count     UInt64,
    -- What the missing count is a share of: the sequence numbers this site
    -- should have seen over the window. Without it there is no rate, and a bare
    -- count of missing datagrams says nothing about a feed's health.
    reference_seqs    UInt64,
    before_ts         DateTime64(9),      -- the datagrams either side, locally
    after_ts          DateTime64(9),
    -- When the missing datagrams were actually sent, from a site that did record
    -- them. A site has no clock reading for a datagram it never received, so its
    -- own bracket above is the weaker answer and these are the stronger one.
    -- NULL until the cross-site pass has run.
    sent_from_ts      Nullable(DateTime64(9)),
    sent_to_ts        Nullable(DateTime64(9)),

    admitted_recorder UInt64,             -- our own drops over the window
    admitted_scope    LowCardinality(String),
    -- missing_count less what we admit, and NULL when that subtraction is not
    -- valid at this scope. The verdict is decided on this residue and never on
    -- missing_count, because a gap can be partly ours: five missing with three
    -- admitted is neither `recorder` nor `publisher`.
    unexplained_count Nullable(UInt64),
    -- The DELTA over the window, upstream of the capture point. NULL when the
    -- preceding segment was not available to subtract from.
    interface_drops   Nullable(UInt64),
    seen_elsewhere    Nullable(UInt8),    -- present at another site
    on_redundant_path Nullable(UInt8),    -- present in another instance here
    -- recorder | upstream | path | unverifiable | publisher, tested in that
    -- order. `unverifiable` is a first-class verdict and not a failure to
    -- compute: a rule set that reports a violation where it merely could not see
    -- is a rule set nobody trusts twice. A loader over one object never writes
    -- `publisher`; the cross-site pass does.
    verdict           LowCardinality(String),
    object_key        String              -- where the evidence is
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(before_ts)
ORDER BY (source_addr, channel_id, dst_port, era_anchor_ts, missing_from, site);

-- 5. The rule set's verdicts, kept.
--
--    rule_set_version and run_ts are load-bearing rather than bookkeeping: a
--    rule added next month runs against last month's traffic, so one window
--    legally holds two verdicts from two versions, and a dashboard that cannot
--    say which version produced a verdict cannot show that the rule set
--    improved. ReplacingMergeTree(run_ts) so a later run of one version
--    replaces its own earlier verdict and never another version's.
CREATE TABLE IF NOT EXISTS recorder.conformance_finding (
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
ENGINE = ReplacingMergeTree(run_ts)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (rule_id, source_addr, channel_id, dst_port, window_start, site, recorder, rule_set_version);
