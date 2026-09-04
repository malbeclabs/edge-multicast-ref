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
-- INSERT on the five tables, because that is the whole job.
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

GRANT INSERT ON recorder.datagram TO dz_loader;
GRANT INSERT ON recorder.era TO dz_loader;
GRANT INSERT ON recorder.segment_coverage TO dz_loader;
GRANT INSERT ON recorder.sequence_gap TO dz_loader;
GRANT INSERT ON recorder.conformance_finding TO dz_loader;
