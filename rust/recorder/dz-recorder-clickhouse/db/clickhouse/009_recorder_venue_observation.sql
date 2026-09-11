-- The venue half of a feed race: its own two grains, and the race as a query.
--
-- A feed race compares what a venue said with what a publisher sent. `006` is
-- the publisher half of it — occurrences numbered per observation point and
-- paired ordinal to ordinal over `book_top`. This file is the other half.
--
--
-- WHY THESE ARE NEW TABLES AND NOT ROWS IN `book_top`
--
-- `book_top` cannot hold a venue-side row, for two reasons, and both are about
-- the columns rather than about the volume.
--
-- `event` and `book_top` carry eight non-nullable provenance columns —
-- `source_addr`, `channel_id`, `dst_port`, `source_id`, `instrument_id`,
-- `sequence_number`, `reset_count`, `segment_seq` — and they are in the sort
-- key. Each is a statement about a datagram on a channel instance, and a venue's
-- upstream message is not one: a websocket message, a tag-value message on a
-- session and a response body have no source address, no destination group, no
-- port role and no `Sequence Number`. There is no honest value and no sentinel
-- either, because every plausible one is also a real reading — channel `0` is a
-- channel, port `0` is a port a document can state, sequence `0` is the first
-- sequence of an era, and `0.0.0.0` reads as *unset* to a person and as a value
-- to a query. Making one of them nullable would weaken every publisher-side row
-- to accommodate a row of a different kind, which is the rule
-- `uncertain_reason` already states: a column is not nullable when a NULL
-- invites a join that drops the row.
--
-- And the pairing in `006` groups on `channel_id` and `instrument_id`, neither
-- of which a venue side may compute. The channel is the operator's mapping from
-- a shard to a channel and the whole adapter boundary exists so that a venue
-- cannot be handed one; the identifier is minted by the publisher's
-- reference-data registry and is unique only within an era. A venue knows the
-- symbol.
--
-- So the venue's rows are their own grains with their own provenance, and the
-- comparison is a query over them.
--
--
-- WHY THE KEY IS `book_key` AND NOT `state_key`
--
-- `state_key` is the equivalence key **two recorders of one multicast feed**
-- pair on: `state_key(channel_id, instrument_id, top)` folds both identifiers
-- into the accumulator before it folds a price, so it is transport-independent
-- and *observer-dependent*. Two recorders of one feed pair on it because both
-- read the same identifiers off the same datagrams.
--
-- A venue-side observer can compute neither identifier, so keyed on `state_key`
-- this race would return **zero pairs** and read as each side missing every
-- state the other saw — which is the failure that function's own comment names,
-- where a key that stops matching "looks like a quiet feed".
--
-- `book_key` is the book-only key: a hash over the two sides and nothing else,
-- computable by anyone holding a top of book. It is written by
-- `dz_recorder_events::book_key` and never by a second implementation, in SQL or
-- anywhere else — two hashes of one book state pair with nothing, and the
-- predicate that decides whether a side is absent is private in that crate for
-- exactly that reason.
--
--
-- WHERE `env` IS AND WHERE IT IS NOT, WHICH IS `006`'S ARRANGEMENT
--
-- `env` is a column on both grains and is selected by the ordinal. It is in
-- neither sort key, in neither the ordinal's partition nor the pairing's
-- grouping. That is not an omission: `001`, `005` and `006` do exactly this —
-- every grain carries `env`, and no sort key, numbering or pairing key mentions
-- one — and this file races against those tables, so a column that was part of
-- a row's identity here and a label there would be one column with two meanings
-- inside a query that reads both.
--
-- The consequence is stated rather than left to be found. Two environments
-- writing one database at one observation point would interleave into a single
-- ordinal sequence, and two rows equal on `(observation, feed, symbol,
-- recv_ts, object_key, message_index, change_index)` would collapse across them
-- under `ReplacingMergeTree`. That is the exposure `book_top` and `event`
-- already have under the same arrangement, and it belongs wherever theirs is
-- answered: adding `env` to these two tables alone would answer it for the
-- venue half of a pair and leave the publisher half as it is.
--
--
-- HOW THE PUBLISHER SIDE ENTERS THE PAIRING, WHICH IS NOT BY BEING NAMED IN IT
--
-- Nothing below names an observation point. The pairing is an aggregate over the
-- ordinal, exactly as `006`'s is, so two observations are a race, three of them
-- are the same query, and a side enters by contributing rows rather than by this
-- view learning its name — which is the one thing `observation` was declared an
-- opaque string to avoid.
--
-- So the pairing reads `recorder.feed_race_occurrence`, and that view is where a
-- side is admitted. This file declares it with the venue branch alone, because
-- the publisher side needs a `book_key` on `book_top` and this file does not add
-- columns to that table. `010` adds the column and the second branch, which is
-- one branch of a union and not a rewrite of anything here.
--
--
-- THE ARGUMENTS THIS FILE CITES RATHER THAN RESTATES
--
-- Every one of them is made in full in `006`'s header and carries over
-- unchanged. They are named here so that a reader knows they were considered,
-- and left there so that there is one copy of each to keep true:
--
--   * WHY THIS IS NOT AN `ASOF JOIN`, WHICH IS THE OBVIOUS MOVE. A book state
--     repeats, `ASOF` has no notion of consuming a match, and the lead times it
--     produces are plausible, biased, and derived from counting one arrival
--     several times.
--   * AN UNPAIRED OCCURRENCE IS A ROW, NOT AN ABSENCE. The pairing is an
--     aggregate over the ordinal rather than a join between two named points,
--     so a state one side saw survives with `observations = 1` and a null
--     `lead_ms`.
--   * THE BOUND ON |Δt| IS THE CALLER'S PREDICATE, NOT A CONSTANT HERE. The
--     bound is a property of the two paths being compared, so `lead_ms` is a
--     column and `WHERE abs(lead_ms) < ...` is the caller's.
--   * `FINAL` AND WHY A DUPLICATE IS WORSE THAN A DOUBLE COUNT. Numbering over
--     an unmerged re-load counts one arrival as two occurrences, and the surplus
--     copy then pairs with nothing — so a duplicate does not inflate a count, it
--     manufactures evidence of loss.
--
-- Two of `006`'s arguments have **no counterpart here**, and saying so is the
-- point of naming them:
--
--   * THE ERA. `006` resolves an era by range join, because an `Instrument ID`
--     is only unique within one. There is no era on this side in any form: an
--     era is a publisher's `Reset Count` span, and a venue has none. The symbol
--     is the instrument identity here, which is coarser — it cannot separate two
--     instruments that shared a symbol across an era boundary — and that cost is
--     stated rather than papered over with a column nothing could fill.
--   * `from_anchor = 0`, FILTERED BEFORE THE WINDOW. `006` excludes
--     snapshot-anchored rows because a snapshot is pulled on the publisher's own
--     cadence and times nothing. There is no such row here: every
--     `venue_book_top` row comes from an upstream message the venue produced, so
--     there is nothing to exclude and no ordinal to shift by excluding it late.
--
--
-- RE-APPLY `004` AFTER THIS FILE, OR THE TWO TABLES BELOW ARE UNWRITABLE
--
-- `004_recorder_loader_user.sql` is where the grants for these two tables
-- live, written there exactly as they are written here:
--
--   GRANT INSERT ON recorder.venue_book_top TO dz_loader;
--   GRANT INSERT ON recorder.venue_object TO dz_loader;
--
-- and there is no migration framework here: the files are applied by hand or by
-- the deploy, as `001`'s own header states. So the hazard is a *partial* apply,
-- and it has one shape. An operator upgrading an existing cluster applies this
-- file, because this file is the new thing, and the venue derivation then fails
-- on its first insert with `Not enough privileges` — a message that names a
-- table and not the file that would fix it. Re-apply `004`; it is idempotent by
-- construction, every statement in it guarded by `IF NOT EXISTS` or replaying a
-- grant the account already holds.
--
-- A fresh install by the numbers is **not** a hazard, and the reason is worth
-- stating so that nobody reorders the files to fix a problem they do not have.
-- `004` runs before this file and its two venue grants therefore name tables
-- that do not exist yet. A grant in ClickHouse is stored against the name, not
-- against the object, so it is accepted and it takes effect when this file
-- creates them.
--
-- THE GRANTS ARE NOT MOVED HERE, and three things keep them in `004`. A `GRANT`
-- names `dz_loader`, which only `004` creates, so a grant in this file would
-- stop this file applying anywhere the account has not been made — which is
-- every run of the container suite, since `004` takes a password parameter and
-- `dz_recorder_clickhouse::schema` excludes it by name. That suite also
-- rewrites `recorder.` to a scratch database per run, so a grant in an applied
-- file would spend privileges on ephemeral databases under an account name that
-- means something in production. And this file is in `schema`, which is what a
-- schema deploy applies: by the argument `004`'s own header makes, whoever
-- applies it holds neither access-management rights nor the secret, so a
-- privilege statement here is one that actor cannot execute.
--
-- What that costs is that the two grants are the only statements in this pair
-- of files no test runs against a server — `ddl.rs` asserts them as substrings
-- of `004` instead. The dependency is stated in both directions rather than
-- inferred, and `ddl.rs` pins that this paragraph names `004` for every venue
-- grain, so a third grain added next year cannot get a grant in `004` and no
-- instruction here.


-- 1. The venue-side top of book.
--
-- One row per change in the top of book, as an observer of a venue's own
-- upstream states it.
--
-- THERE IS NO `book_certain` COLUMN, AND THAT IS DELIBERATE. On the publisher
-- side certainty falls when a gap in the publisher's own sequence space means
-- the book cannot be believed — an observation this tier can make, because the
-- gap is visible in the archive. A venue-side observation has no sequence space
-- of its own that this repository defines: what would make its book untrustworthy
-- is the venue's own resynchronisation, which is a different thing measured a
-- different way. One column carrying both meanings, minimum-aggregated over a
-- pair, mixes them silently. So the venue side says what it does not know, and
-- `venue_object.desync_count` is where the evidence lands instead.
--
-- `feed` IS IN THE SORT KEY HERE AND IS A LABEL THERE, WHICH IS NOT AN
-- INCONSISTENCY. `event`, `instrument` and `book_top` leave it out because it is
-- recoverable from the channel instance: no two feeds serve one `(source
-- address, destination port)`. There is no channel instance on this side, so
-- nothing else tells two feeds apart at one observation point, and leaving it
-- out would collapse two feeds' rows into one under `ReplacingMergeTree`.
--
-- `message_index` IS IN THE KEY FOR THE REASON `book_top` NEEDS IT. Two archived
-- records one instant apart move the book twice, and a key without the index
-- makes the second replace the first — a hole in the book's history that no
-- count would show.
--
-- AND `change_index` IS THERE BECAUSE THE RECORD IS NOT A FINE ENOUGH GRAIN.
-- `message_index` counts the records of the object, and one record is one
-- payload the adapter is handed — which the sink contract allows to carry a
-- batch, with `upstream_message` called once per member, and allows any one
-- member to move a top more than once. Every row of one record carries that
-- record's own receive stamp, because that is the only stamp the transport took,
-- so a key ending at `message_index` is one key for all of them: two genuine
-- book states from one payload collapse under `ReplacingMergeTree` and the loss
-- is a row that was never written rather than a count that is wrong.
--
-- `change_index` is the ordinal of the top change within the record, counted
-- from zero in the order the derivation read it — deterministic for one object,
-- so a re-derivation writes the same ordinal for the same change and the replace
-- above still replaces. Over the record and **not** over the member,
-- deliberately: a per-member ordinal would leave two level updates on one
-- instrument inside one member sharing a key, which answers the batch and not
-- the collapse. Which member of a batch moved a top is what `upstream_sid` and
-- `upstream_seq` say where the venue numbers its messages, and nothing on this
-- side can state it where the venue does not.
--
-- `book_key` IS **NOT** IN THE KEY, and the ordinal above is why it does not
-- need to be. Adding it would also separate two states from one payload, but it
-- would separate them by *what the book was* rather than by *which change this
-- is* — and two changes that happened to return the book to one state would
-- collapse again while carrying a key that says they cannot have.
--
-- `object_key` IS IN THE KEY, AND IT IS THE ONLY COLUMN THAT CAN BE. Not the
-- only one that tells two objects apart — `object_sha256` does that too and is
-- on this table, and its own paragraph below is why it is ruled out anyway.
-- `message_index` restarts at zero in every object, so it is a record's
-- position *within* one and separates nothing across two. A rotation closes one
-- object and opens the next, and a clock coarser than the gap between them
-- stamps records either side of the boundary alike — the case the occurrence
-- view's tie-break below is written for — so a record of the closing object and
-- a record of the opening one can agree on
-- `(observation, feed, symbol, recv_ts, message_index, change_index)` to the
-- last component. Without the object in the key those two genuine book states
-- collapse into one, and the loss is a row that was never written rather than a
-- count that is wrong. The view cannot repair it either: a row a merge removed
-- is not there to be numbered.
--
-- AND IT COSTS NO IDEMPOTENCE FOR A RE-DERIVATION, which is the first claim a
-- reader reaching for the opposite conclusion has to check. A re-derivation
-- reads *the same object* — `(object key, sha256)` is the pair it replaces on —
-- so the key it produces is the same key, this column included, and the second
-- load still replaces the first **where it writes the same row**. What no key
-- makes it do is remove a row it no longer writes at all, which is a limit of
-- the engine rather than of this column: see the corrected-adapter section
-- below, which is where it is stated and where an operator is told what to do
-- about it.
--
-- WHAT IT DOES COST IS A WIDER RE-CUT EXPOSURE, AND THAT IS THE SECOND CLAIM.
-- An object **re-cut** is the same records read out of an archive whose window
-- boundaries were moved, and it lands under a new `object_key`, so its rows do
-- not replace the rows of the cut it supersedes. For the records whose index
-- actually moved this column changes nothing: the re-cut renumbers
-- `message_index` too, so those rows sit beside the old ones under any key that
-- holds a record index at all. The case this column adds is the **identical
-- prefix** — a re-cut that moved only the *later* boundary, so the leading
-- records are the same records with the same indexes and the same stamps.
-- Under a key without the object those rows replace their predecessors; under
-- this one they double them, because the object name changed and the object
-- name is in the key. So the exposure this column carries is strictly wider
-- than the exposure a key without it carries, not the same one, and a reader
-- who constructs that case is right.
--
-- IT IS STILL THE TRADE TO TAKE, because the two failures are not comparable.
-- A rotation-boundary collapse is silent and unrecoverable: the row is never
-- written, nothing counts it, and no later query separates a book state that
-- collapsed from one that never happened. An identical-prefix double is visible
-- and countable: `venue_object` holds one row per `(object_key,
-- object_sha256)`, so two cuts of one window are two rows there with
-- overlapping `recv_ts_start`, and the doubled rows here are the ones that
-- carry the superseded `object_key`. No merge collapses them — different keys,
-- which is the whole point — so removing them is an operator's `DELETE` on that
-- `object_key`. A duplicate a query can find and a statement can remove is
-- worth taking over a loss no query can find at all.
--
-- `object_sha256` IS **NOT** IN THE KEY, and the difference from the row above
-- is what each table is for. Two digests under one key are one window the
-- archive re-published, and the rows of the object that is there now should
-- replace the rows of the object that was. `venue_object` keeps both, because it
-- is the ledger of what was read; this table holds the book, and two books for
-- one window is the duplicate that manufactures evidence of loss.
--
-- A REPLACE IS NOT A DELETE, AND THAT IS WHERE THE PARAGRAPH ABOVE STOPS BEING
-- TRUE. `ReplacingMergeTree` replaces a row only where the whole `ORDER BY`
-- tuple matches, and it removes nothing a later insert does not contain. So the
-- rows of the object that is there now replace the rows of the object that was
-- change for change only while both derivations write the same
-- `(message_index, change_index)` set. Neither this key nor any other makes a
-- second load withdraw a row the first load wrote and the second does not.
--
-- THE DERIVATION THAT WRITES A DIFFERENT SET IS THE ONE THIS TIER EXISTS FOR.
-- `dz-recorder-venue` and the upstream object format both say why the bytes are
-- kept verbatim: so that a finding can be re-examined next month with a
-- corrected adapter. An adapter correction is exactly what changes the set — it
-- emits *fewer* top changes for a record, because a level update was never a
-- move of the top or a malformed member should have been refused and counted
-- rather than folded, or it renumbers them. Every surplus row of the superseded
-- derivation then stays in this table under a key the corrected one never
-- writes. Nothing collapses them and nothing fails: the occurrence view numbers
-- them beside the corrected rows, so `observations`, `lead_ms` and the ordinals
-- are computed partly from evidence the corrected adapter withdrew. The
-- re-derivation that was meant to repair a finding manufactures one instead,
-- and it does it to the oldest rows in the deployment.
--
-- SO A CORRECTED-ADAPTER RE-DERIVATION IS TWO STATEMENTS AND NOT ONE:
-- A `DELETE` AND THEN A LOAD, IN THAT ORDER. The order is the whole of the
-- instruction: the statement matches on the object, so run second it removes
-- the corrected rows it was supposed to make room for.
--
--     DELETE FROM recorder.venue_book_top WHERE object_key = '<the object>';
--     -- and then load the corrected derivation of that object
--
-- Measured on the 24.8 the suite pins rather than assumed: the lightweight
-- `DELETE` applies to this engine, and
-- `a_corrected_adapter_re_derivation_replaces_the_rows_it_supersedes` in
-- `tests/container.rs` performs both halves against a server — the surplus row
-- left by the load on its own, and the corrected set the documented order
-- leaves. `venue_object` needs no such statement: it is keyed on
-- `(object_key, object_sha256)`, and a corrected adapter reading the same bytes
-- writes the same pair, so its row replaces. This table is the one that holds a
-- row per change, and a change is the thing a correction can take away.
--
-- NOT A VERSION COLUMN AND NOT A TOMBSTONE, though either would work. Both need
-- something an adapter can state, and an adapter does not know it has been
-- corrected: a `ReplacingMergeTree(version)` would have to be handed a version
-- that rises with a code change, and `is_deleted` needs a row written for a
-- change that no longer exists to say so. The `DELETE` is a statement an
-- operator runs at the one moment the fact is actually known, which is when
-- they decide that this derivation supersedes that one.
--
-- THE OBJECT GOES BEFORE THE RECORD INDEX, in the order the occurrence view's
-- window already reads them: the object is what separates two records across a
-- rotation and the index is what orders them within one object, so the table's
-- own order and the numbering's are one order rather than two over the same
-- rows. *Separates* and not *orders*, deliberately — see the view's own
-- paragraph on what the key does and does not say about which object came
-- first.
CREATE TABLE IF NOT EXISTS recorder.venue_book_top (
    recv_ts           DateTime64(9),
    -- Which observation point this recording is, as `site` names a host. The
    -- same opaque string `book_top.observation` is, and opaque for the same
    -- reason: nothing below names an observation point.
    observation       LowCardinality(String),
    env               LowCardinality(String),
    feed              LowCardinality(String),
    -- The upstream connection the message arrived on. Evidence and not a key: an
    -- adapter whose mapping depends on the connection reproduces nothing offline
    -- without it, and a row that cannot say which upstream it came from cannot
    -- be checked against that upstream's own logs.
    connection        LowCardinality(String),
    -- The venue's own session identity and its own number within that session,
    -- where the venue publishes them. NULL where it does not, and never zero:
    -- both are values a venue really sends.
    --
    -- NOTHING JOINS ON EITHER. A numbering whose resolution and meaning differ
    -- between transports would give one book state two keys, and a race keyed on
    -- it would find no pair. They are here because they separate a venue
    -- *resending* a state from the venue producing that state again — the same
    -- book and not the same event — and without them an unpaired occurrence has
    -- one fewer explanation available to it.
    upstream_sid      Nullable(UInt64),
    upstream_seq      Nullable(UInt64),
    -- The instrument as the venue names it, and the only instrument identity
    -- both sides of the race hold.
    symbol            LowCardinality(String),
    -- The exponents as the venue states them, through the adapter's own
    -- `InstrumentSpec`. Carried rather than assumed: `book_key` covers the raw
    -- prices and leaves these out, so `exponents_agree` below is what makes a
    -- disagreement visible instead of averaged.
    price_exp         Int8,
    qty_exp           Int8,
    bid_px_raw        Nullable(Int64),
    bid_qty_raw       Nullable(UInt64),
    -- NULL and never zero for *the venue did not say*. The top-of-book
    -- specification states this field as "0 if unavailable", so a zero is the
    -- multicast side's spelling of an absence — and `book_key` reads the two
    -- alike, so a zero here would leave the row and the key describing two
    -- different books.
    bid_source_count  Nullable(UInt16),
    ask_px_raw        Nullable(Int64),
    ask_qty_raw       Nullable(UInt64),
    ask_source_count  Nullable(UInt16),
    -- `dz_recorder_events::book_key`: a hash over the two sides and nothing
    -- else. Not `state_key`, which folds the `Channel ID` and the `Instrument ID`
    -- in first, and not a second implementation of either.
    book_key          UInt64,
    -- Which archived record in the object moved the top, counted from zero. One
    -- record is one message the transport delivered — not a message's position
    -- inside a datagram, which is what the column of this name means on the
    -- publisher side and which there is no datagram here to have.
    message_index     UInt64,
    -- Which change in the top this row is within that record, counted from zero.
    -- What makes a row identifiable when one payload carried a batch, or when one
    -- of its members moved a top twice; see the note above.
    change_index      UInt64,
    object_key        String,
    object_sha256     String
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (observation, feed, symbol, recv_ts, object_key,
          message_index, change_index);


-- 2. The object a derivation read.
--
-- THE IDEMPOTENCE ROW. `(object_key, object_sha256)` is what a re-derivation
-- replaces on, so this is where a reader looks to see whether an object was read
-- at all, how much of it the adapter could parse, and what it refused. An object
-- derived twice is one row here.
--
-- WHY THE REFUSALS ARE HERE AND NOT ON A ROW. An adapter that refuses a message
-- costs **that message** and is counted; a derivation that stopped at the first
-- message a venue's own adapter could not parse would report the venue's feed as
-- having ended there, and those rows are indistinguishable from a venue that
-- went quiet. So the refusal has to be visible somewhere, and the object is the
-- grain it belongs to: it is a fact about a window rather than about a book
-- state, and the message it cost has no row of its own to carry it.
--
-- WHAT A REFUSAL COSTS IS THE REST OF ITS MESSAGE, AND NOT WHAT CAME BEFORE IT.
-- One archived record may carry a batch, and an adapter may report events for
-- several members of one and then refuse a later one. Those events are already
-- in the adapter's own book, so the rows they produced stand and this count is
-- the only thing that says a refusal happened in that record at all. Dropping
-- them instead would take a book state out of the history with nothing counting
-- it, which is the hole `message_index` is in the key to prevent.
--
-- WHY `refusals` IS BY REASON AND NOT A BARE TOTAL. The four tokens are
-- `ParseError`'s own — `schema`, `unknown_field`, `malformed`, `truncated` —
-- and an operator acts differently on each: a schema refusal says the venue
-- changed its interface, a truncated one says the transport cut a message. A
-- bare total sends somebody to read the objects to find out which.
CREATE TABLE IF NOT EXISTS recorder.venue_object (
    recv_ts_start     DateTime64(9),
    recv_ts_end       DateTime64(9),
    observation       LowCardinality(String),
    env               LowCardinality(String),
    feed              LowCardinality(String),
    object_key        String,
    object_sha256     String,
    -- The archive format the object was written in, as its own header states it
    -- — not as a manifest beside it claims. A window derived by a reader that
    -- knew one version and an object written at another is the disagreement this
    -- column exists to make visible.
    format_version    UInt16,
    connections       Array(LowCardinality(String)),
    message_count     UInt64,
    refused_count     UInt64,
    -- Array(Tuple(String, UInt64)): `(reason, count)`, the same unnamed-tuple
    -- shape `segment_coverage.roles_joined` uses.
    refusals          Array(Tuple(String, UInt64)),
    event_count       UInt64,
    -- Events whose price or quantity could not be stated exactly at the
    -- instrument's own declared exponent. Counted rather than rounded, and the
    -- event is dropped: a rounded price is a price the venue did not quote, and
    -- a conversion taken as zero is a real-looking quote at nothing.
    --
    -- Exponent conversions and nothing else. An event naming an instrument the
    -- derivation never admitted is the column below, because an operator checks
    -- a venue's declared scale for one and an adapter for the other.
    unpriced_count    UInt64,
    -- Events naming an instrument the derivation never admitted, which is a
    -- defect in the adapter. A handle is not a capability — it carries no proof
    -- of its own origin, so it can be forged and it can be one minted over a
    -- different object — and either is refused here rather than resolved to
    -- whatever instrument now sits at that index and written as a top of book
    -- under its symbol. The event is dropped; the publisher side's lowering
    -- refuses the same thing under its own `unknown_instrument` token.
    unknown_instrument_count UInt64,
    -- Times the adapter said it no longer trusts its own book. Evidence and not
    -- a verdict; see the note on the absent `book_certain` above.
    desync_count      UInt64,
    -- Events the adapter reported outside any payload scope, which is a defect
    -- in the adapter. The derivation opens the scope around the adapter's call
    -- and closes it after, so an event outside one is attributable to no
    -- upstream message: no receive stamp, no message index, no identity, and
    -- therefore no honest row. Counted here because a drop nothing counted reads
    -- as a venue that said less than it did.
    unattributed_count UInt64,
    book_top_count    UInt64,
    instrument_count  UInt32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(recv_ts_start)
-- The pair a re-derivation replaces on, and the observation and feed ahead of
-- it because those two are the coarse filter a reader supplies: the mark range
-- narrows on them before `object_key` is compared.
--
-- Not for pruning, which `PARTITION BY toYYYYMMDD(recv_ts_start)` above
-- decides on its own: no sort-key column order participates in it. The
-- distinction is written down because the two are easy to conflate, and a key
-- defended as a pruning device is a key nobody re-examines when the
-- partitioning changes underneath it.
ORDER BY (observation, feed, object_key, object_sha256);


-- THE RETENTION SPLIT, THE SAME ONE `005` MAKES ONE TABLE FURTHER DOWN.
--
-- `venue_book_top` is per change in the top rather than per upstream message, so
-- it is worth the same window `book_top` gets — and it has to be the *same*
-- window, because the race reads both sides and a pair whose publisher half has
-- expired is a state that reads as seen by one observation point only. Thirty
-- days, stated here rather than left to be inferred from an absent line.
--
-- A whole number of days, for the reason `002` gives: a TTL that does not align
-- to the partition is a treadmill of part rewrites rather than a partition drop.
ALTER TABLE recorder.venue_book_top
    MODIFY TTL toDateTime(recv_ts) + INTERVAL 30 DAY;

-- `recorder.venue_object` has no TTL, deliberately. It is one row per object,
-- which is `segment_coverage`'s cardinality, and it is the only thing that says
-- an object was derived at all — so expiring it turns a window nobody derived
-- into a window indistinguishable from one that held nothing.


-- 3. The book, collapsed.
--
-- `venue_book_top` is a `ReplacingMergeTree` and re-deriving an object is a
-- replace, so between a re-derivation and the merge that follows it one top of
-- book is in the table twice. `FINAL` applies the collapse at read time, for the
-- reason `006` gives in full: numbering over the duplicate counts one arrival as
-- two occurrences, and the surplus copy then pairs with nothing and is reported
-- as a state the other observation point missed — so a duplicate does not
-- inflate a count here, it manufactures evidence of loss.
--
-- Affordable for the reason `003` gives: it forces merge-on-read over the parts
-- a query reads, and this table is partitioned by day, so a predicate on
-- `recv_ts` prunes the partitions first and `FINAL` pays for what is left.
CREATE OR REPLACE VIEW recorder.venue_book_top_settled AS
SELECT *
FROM recorder.venue_book_top FINAL;


-- 4. The occurrence ordinal, per observation point.
--
-- Numbered rather than joined by proximity, which is `006`'s argument in full
-- and is not restated here: a book state repeats, `ASOF` has no notion of
-- consuming a match, and the lead times it produces are plausible, biased and
-- derived from counting one arrival several times.
--
-- `symbol_key` IS THE SYMBOL WITH ITS CASE FOLDED, AND THAT IS WHAT MAKES
-- `symbols_agree` A COLUMN RATHER THAN AN ASSUMPTION. The design's own cost
-- list says the comparison "depends on the venue's symbol and the published
-- symbol agreeing — which is a reference-data assertion, and worth being a
-- column rather than an assumption, the way `exponents_agree` is." A key on the
-- symbol exactly as each side spells it cannot carry that assertion at all:
-- a disagreement would produce no pair, which reads as a quiet feed on both
-- paths — the same failure keying on `state_key` would produce. So the ordinal
-- and the pairing key on the folded form, and the raw spellings are compared
-- below where a disagreement is a value somebody can see.
--
-- THE FOLD IS `upper(trimBoth(symbol))`, WHICH IS ASCII CASE AND THE PADDING
-- AROUND IT. Case is the divergence that actually occurs between a venue's own
-- naming and a published symbol, and it is reversible: `symbols` on the pairing
-- carries every spelling that went into a pair, so nothing is lost. `trimBoth`
-- is there because whitespace either side of a symbol is padding from a
-- fixed-width field rather than part of an identifier — no two instruments
-- differ only by the spaces around them, so trimming merges nothing and the raw
-- spellings still come back beside the verdict.
--
-- `upper` folds ASCII case and `upperUTF8` is the Unicode one, so which is
-- written is part of what the fold means and is written down rather than left to
-- be discovered from the function name. The coarseness it buys is stated the way
-- the paragraph below states the numbering's: two spellings differing only in
-- the case of a non-ASCII letter do not pair, which reads as a quiet feed on
-- both paths. A venue that spells one is the reason to reach for `upperUTF8` —
-- in the ordinal and in the `symbol_key` column together, because two folds
-- that disagree number a state under a key nobody selected.
--
-- Anything more aggressive — stripping separators, normalising a suffix — would
-- start merging instruments, which is the one thing an equivalence key must not
-- do.
--
-- THERE IS NO ERA IN THIS PARTITION, AND NOTHING TO PUT THERE. `006` numbers
-- within an era because an `Instrument ID` is only unique within one. An era is
-- a publisher's `Reset Count` span and a venue has none, so the symbol is the
-- instrument identity and the numbering runs over the whole window. That is the
-- coarseness the design states: two instruments that shared a symbol across an
-- era boundary cannot be separated here.
--
-- AND NOTHING IS FILTERED OUT BEFORE THE WINDOW. `006` filters `from_anchor = 0`
-- there rather than after, so that an anchor row consumes no ordinal. Every row
-- here comes from an upstream message the venue produced, so there is no
-- equivalent row to exclude — and therefore no way for a late exclusion to leave
-- every later occurrence numbered one too high.
--
-- THE WINDOW'S ORDER IS TOTAL, AND IT IS `recv_ts` THAT IS NOT. Equal receive
-- stamps are ordinary in this archive rather than a coincidence: every row a
-- batched payload produced carries the record's own stamp, because that is the
-- only stamp the transport took. Ordered on the stamp alone, `row_number()` is
-- free to number two changes at one stamp either way — and it may answer
-- differently after a merge, so the same rows numbered on two runs pair
-- differently and `observations` and `lead_ms` are not reproducible.
--
-- So the tie is broken by what the rows already carry, in the order that says
-- what it means: `object_key`, then `message_index`, then `change_index`. That
-- triple is unique for every row of this table — a record belongs to one object,
-- and a change to one record — so the order is total and there is nothing left
-- for the database to choose.
--
-- THE OBJECT COMES BEFORE THE RECORD INDEX, and the rotation boundary is why.
-- The index restarts at zero in each object, so it is a record's position
-- *within* one and orders nothing across two: a clock coarser than the gap
-- between a closing object and the one that opens stamps the last record of the
-- first and the first record of the second alike, and comparing `5` against `0`
-- there numbers the later object's row first. `object_key` is the only column a
-- venue-side row carries that separates two objects — `segment_seq` numbers the
-- objects of a capture and is one of the columns this file declares nothing
-- of — so the object is what the tie-break has to be made of.
--
-- WHAT THE KEY GIVES IS A TOTAL ORDER AND NOT THE OBJECTS' OWN, AND THE
-- DIFFERENCE IS WRITTEN DOWN HERE BECAUSE THE COLUMN DOES NOT SHOW IT. An
-- object key ends in the name the archive tier mints for it,
-- `<start_ns>-<end_ns>-<segment_seq>`, and the sequence is written without
-- padding — so a pair that compares that far puts segment 10 ahead of segment
-- 9, because `1` sorts before `9`. A pair gets that far exactly where this
-- tie-break is needed: the two stamps are the smallest and the largest the
-- window saw, so an object whose whole window fits inside one clock tick states
-- one stamp for both, and a rotation inside that tick hands the next object the
-- same two. Where two windows differ the keys do put the earlier object first,
-- though on a second property nothing writes down either: the stamps lead the
-- name, and they are of one width only for as long as a nanosecond stamp is
-- nineteen digits.
-- Where the windows agree, nothing this table holds orders the two objects at
-- all — not a column carrying `segment_seq` either, had this file declared one,
-- because a recorder's numbering restarts at zero on every run and two
-- recorders at one observation point both begin there. The archive tier itself
-- does not order objects by name: its eviction scan parses the three numbers
-- out of the name and sorts on those. This view will not write that parser a
-- second time in SQL, which would pin the archive's file naming into a
-- migration — and `dz-recorder-archive`'s own publication test pins what the
-- name does and does not give, so this paragraph and that naming cannot drift
-- apart in silence.
--
-- SO WHAT THIS ORDER RESTS ON IS UNIQUENESS, WHICH IS STATED, AND NOT
-- COLLATION, WHICH IS AN ACCIDENT OF A FILE NAME. The uniqueness is the one two
-- paragraphs up, and it is the archive tier's own contract rather than an
-- observation about a string: a key names one object and cannot collide,
-- because the site and the recorder are in it. That is what the ordinal needs
-- and the whole of it. The tie-break only ever decides between rows that
-- already agree on `recv_ts`, which is the quantity `lead_ms` is measured from,
-- and an `occurrence` is only ever compared against another observation point's
-- `occurrence` for the same `book_key` — so two rows of one book at one stamp
-- pair the same way and yield the same lead time whichever of them is numbered
-- first. The container suite asserts that invariance over two objects whose
-- keys differ only across the 9-to-10 boundary, rather than leaving it here as
-- prose.
--
-- `recv_ts` STAYS FIRST, because it is the quantity the race measures. The
-- tie-break decides between rows that arrived at one stamp and never reorders
-- two that did not — a numbering that ordered by the object first would number a
-- re-derived object's rows against a newer object's and produce lead times
-- measured backwards.
CREATE OR REPLACE VIEW recorder.venue_book_top_occurrence AS
SELECT
    observation,
    env,
    feed,
    connection,
    symbol,
    upper(trimBoth(symbol)) AS symbol_key,
    book_key,
    recv_ts,
    price_exp,
    qty_exp,
    upstream_sid,
    upstream_seq,
    object_key,
    row_number() OVER (
        PARTITION BY observation, feed, upper(trimBoth(symbol)), book_key
        ORDER BY recv_ts, object_key, message_index, change_index
    ) AS occurrence
FROM recorder.venue_book_top_settled;


-- 5. The occurrences the race reads, from every observation of a book.
--
-- One branch here, and the seam the pairing above is written against. A side
-- enters the race by contributing rows to this view, so the pairing never learns
-- a name and stays the same query for two observation points or ten.
--
-- The venue branch is the one this file can declare. The publisher branch needs
-- `book_key` on `book_top`, which `010` adds along with the branch itself: the
-- columns below are exactly what both sides hold, so that union's two halves
-- line up by position and by type.
--
-- SO THIS FILE IS NEVER APPLIED ON ITS OWN AFTER `010`. `010` declares this
-- same view with both branches, and the files are applied in order as a set —
-- re-applying this one by itself would replace the two-sided seam with the
-- venue branch alone, leaving a race that pairs the venue against itself and a
-- `feed_race` whose `observed_by` silently stops naming a publisher
-- observation point. `010` states the converse, which is a deployment that
-- applied this file and skipped that one; this is the same hazard reached from
-- the other end, and neither is a reason for the view to have two names.
--
-- `env` IS CARRIED AND IS NOT GROUPED ON, the way it is a label on every table
-- in `005` and in none of their keys: one database holds one environment, and a
-- reader filtering by it wants the column rather than a key that restates it.
CREATE OR REPLACE VIEW recorder.feed_race_occurrence AS
SELECT
    observation,
    env,
    feed,
    symbol,
    symbol_key,
    book_key,
    recv_ts,
    price_exp,
    qty_exp,
    occurrence
FROM recorder.venue_book_top_occurrence;


-- 6. The race.
--
-- NAMED FOR THE RACE AND NOT FOR A SIDE, the way the seam above is. Nothing
-- below names an observation point, so this is one query whether one side
-- contributes rows or both — and a name carrying `venue` would be read as the
-- venue's own recordings raced against each other, which is what it would
-- aggregate on a deployment where nothing else contributes and is not what it
-- means. `010` adds the publisher branch to the seam, and on a deployment that
-- has it a row here may be either side's: `observations = 1` is as likely
-- publisher-only as venue-only, and `observed_by` names publisher observation
-- points as readily as venue ones.
--
-- An aggregate over the ordinal and **not** a join between two named observation
-- points, which is what keeps an unpaired occurrence visible and what keeps this
-- generic: nothing below names an observation point, so two of them are a race
-- and three of them are the same query. `006` makes both arguments in full.
--
-- `uniqExact(observation)` RATHER THAN `count()`. `006` needs it for the era
-- boundary; here the reason is a re-derivation seen before its merge, which
-- `FINAL` above already collapses — so this is belt and braces, and it costs
-- nothing but says what the number means. Counting rows would report three
-- observations of a state two points saw.
--
-- `lead_ms` IS NULL AND NEVER ZERO WHEN ONE POINT SAW THE STATE. A zero would be
-- a lead time nobody measured, and it would enter every average over the column
-- as evidence that the two paths tied.
--
-- `exponents_agree` IS THE ASSERTION `book_key` MADE AND DID NOT HASH, exactly
-- as it is the assertion `state_key` made and did not hash in `006`. The key
-- covers the raw prices and quantities and leaves the exponents out, so a pair
-- whose exponents disagree is two different prices wearing one key — visible as
-- a column rather than silently averaged into the result.
--
-- `symbols_agree` IS THE SAME KIND OF ASSERTION ABOUT REFERENCE DATA. The key is
-- on the folded symbol, so a pair whose sides spell the instrument differently
-- is a pair — and this is what says so. `symbols` carries every spelling that
-- went into it, because "they disagree" without the strings is a finding nobody
-- can act on.
--
-- WHAT IS NOT HERE: a bound on |Δt|, and a verdict. The bound is a property of
-- the two paths being compared, so it is the caller's predicate over `lead_ms`.
-- And nothing here claims that a state the venue published and a publisher never
-- sent is the publisher's fault: that stays the loss derivation's question and
-- `007`'s. A venue-side observation adds one more thing that can be missing, not
-- an answer about whose fault it is.
CREATE OR REPLACE VIEW recorder.feed_race AS
SELECT
    feed,
    symbol_key,
    book_key,
    occurrence,
    uniqExact(observation)                 AS observations,
    arraySort(groupUniqArray(observation)) AS observed_by,
    argMin(observation, recv_ts)           AS first_observation,
    argMax(observation, recv_ts)           AS last_observation,
    min(recv_ts)                           AS first_recv_ts,
    max(recv_ts)                           AS last_recv_ts,
    if(uniqExact(observation) > 1,
       (toUnixTimestamp64Nano(max(recv_ts)) - toUnixTimestamp64Nano(min(recv_ts))) / 1e6,
       NULL)                               AS lead_ms,
    arraySort(groupUniqArray(symbol))      AS symbols,
    (uniqExact(symbol) = 1)                AS symbols_agree,
    (uniqExact(price_exp) = 1) AND (uniqExact(qty_exp) = 1) AS exponents_agree
FROM recorder.feed_race_occurrence
GROUP BY feed, symbol_key, book_key, occurrence;
