-- The loader's database account, bounded at creation rather than after an
-- incident.
--
-- **A writer that arrives with a ceiling already set costs far less than one
-- given a ceiling afterwards.** Every workload added to the destination cluster
-- in the last month was discovered weeks later by somebody reading a graph — one
-- of them at a quarter of total cluster CPU, another adding a third of cluster
-- load overnight — and none of them were doing anything wrong. They were
-- unbounded. This file is what keeps the loader out of that category
-- permanently, and it is checked in beside the schema so that creating the
-- tables and creating the account that writes to them are one act.
--
-- The password is **not** here and is not settable from a file this repository
-- holds. `IDENTIFIED WITH sha256_password BY` takes a literal, so it is supplied
-- at apply time by whoever runs this — from a secret manager, into a shell that
-- does not log — and the loader reads the same secret from
-- DZ_LOADER_CLICKHOUSE_PASSWORD_FILE. There is no configuration key for it in
-- either place, because a key that exists is a key somebody fills in.
--
--   clickhouse-client --queries-file 004_recorder_loader_user.sql \
--     --param_password="$(read_the_secret)"
--
-- Applied by an administrator, not by the loader: the loader's own account
-- cannot be allowed to grant itself anything, which is the point.
--
--
-- WHAT IT MAY DO, AND WHY EACH GRANT IS THERE
--
-- INSERT on every table the loader writes, because that is the whole job. Ten
-- of them at the last count, and the count is not the point: the list below is
-- grouped by the file that declares each table — `001`'s five transport grains,
-- `005`'s three market data tables, `009`'s two venue-side ones — so that a
-- reader checking the list against the schema reads it file by file.
--
-- THIS FILE IS RE-APPLIED WHENEVER A LATER FILE ADDS A TABLE, and that is a
-- standing rule rather than a note about one file. The grants stay here, where
-- an administrator holds a secret and access-management rights, and the schema
-- files hold no privilege statement — so a file that adds a table adds its
-- `GRANT INSERT` to this list and says in its own header that this file has to
-- be applied again. `009` states it; a file that adds a table and does not is
-- the one that ships a table the loader cannot write, found on the first insert
-- of a deployment rather than in review. This file is idempotent by
-- construction: every statement is guarded by `IF NOT EXISTS` or replays a
-- grant the account already holds, so re-applying it is always safe and is
-- never a schema change.
--
-- A grant naming a table that does not exist yet is fine, which is what makes a
-- fresh install by the numbers work: ClickHouse stores a grant against the
-- name, not against the object, so `009`'s two grants below are accepted here
-- and take effect when `009` creates the tables.
--
-- **No SELECT on anything.** The adjacency check reads the preceding segment's
-- trailer from the loader's own on-disk ledger and from the objects it is still
-- holding, never from the destination, and the only non-INSERT statement the
-- loader ever issues is `--check`'s `SELECT 1` — which reads no table and needs
-- no grant. An account that cannot read `datagram` cannot accidentally become
-- the most expensive query on the cluster; an account that cannot read anything
-- cannot become one at all.
--
-- No DDL at all. A loader that could create or alter a table is a loader that
-- can apply a schema change nobody reviewed, and the schema here is checked in
-- precisely so that it is reviewed.
--
--
-- THE ORDER OF THE STATEMENTS IS LOAD-BEARING
--
-- Profile, then user, then quota, then grants — and every one of those edges is
-- a name resolved at apply time rather than a preference. ClickHouse resolves
-- `SETTINGS PROFILE` when it stores the user entity, so a profile created after
-- the user is a profile the user was never given; `CREATE QUOTA ... TO
-- dz_loader` and every `GRANT` name a user that has to exist by then. Written
-- the other way round the first statement fails and nothing is created — and a
-- re-run after a partial fix would find the user already there behind `IF NOT
-- EXISTS` and leave it without its ceilings for ever, which is the one outcome
-- this whole file exists to prevent.

-- The ceiling. `max_read_bytes` is the one that matters: it is what turns a
-- query somebody adds later from an incident into an error, and it is set well
-- above anything the adjacency check needs and far below a `datagram` scan.
CREATE SETTINGS PROFILE IF NOT EXISTS dz_loader SETTINGS
    -- The adjacency check reads a handful of rows from two small tables. A
    -- gigabyte is generous by three orders of magnitude and still a bound.
    max_bytes_to_read = 1073741824 READONLY,
    max_execution_time = 60 READONLY,
    -- One thread. The loader's reads are point lookups, and a writer that can
    -- fan out across cores is a writer that can take a share of the cluster
    -- nobody sized for it.
    max_threads = 1 READONLY,
    -- Inserts arrive already batched by the loader — see `insert_max_rows` — so
    -- the server does not need to buffer or squash them, and asynchronous
    -- inserts would put a second, invisible batching policy underneath the one
    -- the loader states.
    async_insert = 0 READONLY,
    -- Deduplication is the loader's, on `(object_key, object_sha256)` through
    -- ReplacingMergeTree. The server's own insert-level deduplication window
    -- would silently drop a *legitimate* re-load of an unchanged object, which
    -- is exactly the operation a re-run after an analyser fix performs.
    insert_deduplicate = 0 READONLY;

-- The account, after the profile it names: the profile is resolved here, not on
-- the first query.
CREATE USER IF NOT EXISTS dz_loader
    IDENTIFIED WITH sha256_password BY {password:String}
    SETTINGS PROFILE 'dz_loader';

-- A quota as well as a profile, because a profile bounds one query and a quota
-- bounds a day of them. Generous, and its purpose is to exist: an account with
-- no quota is an account nothing will ever alert on.
CREATE QUOTA IF NOT EXISTS dz_loader
    KEYED BY user_name
    FOR INTERVAL 1 hour MAX queries = 100000, errors = 10000, read_rows = 100000000
    TO dz_loader;

-- The transport grains of `001`.
GRANT INSERT ON recorder.datagram TO dz_loader;
GRANT INSERT ON recorder.era TO dz_loader;
GRANT INSERT ON recorder.segment_coverage TO dz_loader;
GRANT INSERT ON recorder.sequence_gap TO dz_loader;
GRANT INSERT ON recorder.conformance_finding TO dz_loader;
-- The market data tables of `005`. Granted here rather than in `005` itself for
-- the reason this whole file is separate: a grant needs access-management rights
-- and a password in hand, and a loader that could grant itself privileges is the
-- thing this arrangement exists to prevent.
GRANT INSERT ON recorder.event TO dz_loader;
GRANT INSERT ON recorder.instrument TO dz_loader;
GRANT INSERT ON recorder.book_top TO dz_loader;
-- The venue-side tables of `009`, granted here for the same reason. `009`'s own
-- header says to apply this file again after it, which is what an operator
-- upgrading an existing cluster has to do: the alternative is a venue
-- derivation that fails on its first insert with `Not enough privileges`, a
-- message that names a table and not the file that fixes it.
--
-- The same account, deliberately. A venue-side derivation is a different process
-- on a different host writing different tables, and a second account would be a
-- second password to rotate for one bound nobody would set differently. The
-- ceilings above are what matter and they are per user, so sharing the account
-- shares the ceiling — which is the intent: two writers of one database should
-- not be able to spend twice the day's reads between them.
GRANT INSERT ON recorder.venue_book_top TO dz_loader;
GRANT INSERT ON recorder.venue_object TO dz_loader;
