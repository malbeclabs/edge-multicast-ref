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
-- SELECT on `segment_coverage` and `era` **only**. Those two are what the
-- adjacency check reads — the preceding segment's coverage and the boundary rows
-- a later load settles — and nothing else in the loader reads anything. In
-- particular no SELECT on `datagram`: it is the largest table by three orders of
-- magnitude, and an account that cannot scan it cannot accidentally become the
-- most expensive query on the cluster.
--
-- No DDL at all. A loader that could create or alter a table is a loader that
-- can apply a schema change nobody reviewed, and the schema here is checked in
-- precisely so that it is reviewed.

CREATE USER IF NOT EXISTS dz_loader
    IDENTIFIED WITH sha256_password BY {password:String}
    SETTINGS PROFILE 'dz_loader';

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

-- The two the adjacency check reads, and no others.
GRANT SELECT ON recorder.segment_coverage TO dz_loader;
GRANT SELECT ON recorder.era TO dz_loader;
