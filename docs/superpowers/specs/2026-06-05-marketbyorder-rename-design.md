# Design: rename depth-of-book → market-by-order

**Date:** 2026-06-05
**Branch:** `refactor/marketbyorder-rename`

## Background

The feed spec was renamed in [edge-feed-spec](https://github.com/malbeclabs/edge-feed-spec)
to **Market-by-Order** to more accurately describe a feed that carries the full
resting-order population per instrument (rather than a fixed number of aggregated
price levels). The spec lives at
`market-by-order/spec.md`, alongside `top-of-book/`, `midpoint/`, etc.

This repo's parser and bot still use the old "depth of book" / `depthofbook` /
`dob` naming. This effort renames those components and establishes a loose,
discoverable linkage between each parser/bot and its corresponding feed spec in
the (separate) spec repo.

## Naming mapping (two tokens)

The codebase uses two spellings that both refer to this feed. They map to two
replacements:

| Old | New | Used in |
|-----|-----|---------|
| `depthofbook` / `DepthOfBook` / `depthOfBook` | `marketbyorder` / `MarketByOrder` / `marketByOrder` | directory names, Go identifiers, registry key, ClickHouse database name, compose service/image names |
| `dob` / `DOB` (abbreviation) | `mbo` / `MBO` | env-var prefixes (`DZ_DOB_*` → `DZ_MBO_*`, `DOB_BOT_*` → `MBO_BOT_*`), socket `dob.sock` → `mbo.sock`, schema file `02_schema_dob.sql` → `02_schema_mbo.sql` |

Display/prose forms: "Depth-of-Book" → "Market-by-Order".

## Explicit non-goals — must NOT be renamed

1. **The `--depth` CLI flag.** `--depth` means "number of price levels to
   maintain," a genuine runtime parameter unrelated to the feed name. The flag
   name stays `--depth`. Only its env-var *prefix* changes:
   `DZ_DOB_DEPTH` → `DZ_MBO_DEPTH`. The flag string `--depth` is untouched.
2. **`topofbook` components.** Top-of-Book is a separate feed. Its code, dirs,
   and runtime keys are NOT renamed. It only receives the new spec linkage
   (see Linkage below).

## Scope of edits

### Go (2 modules: marketbyorder-parser, marketbyorder-bot)

- Rename directories:
  - `go/depthofbook-parser` → `go/marketbyorder-parser`
  - `go/depthofbook-bot` → `go/marketbyorder-bot`
- Rename files: `depthofbook.go`, `depthofbook_wire.go`, `depthofbook_test.go`
  → `marketbyorder*.go`.
- Rename identifiers: `depthOfBookParser` → `marketByOrderParser`,
  `TestDepthOfBookParser_*` → `TestMarketByOrderParser_*`, and any other
  `DepthOfBook`/`depthOfBook` symbols.
- Registry key: `registerParser("depthofbook", ...)` → `registerParser("marketbyorder", ...)`
  and the corresponding `Name()` return value.
- **Fix bare module paths** (currently inconsistent with topofbook):
  `module depthofbook-bot` → `module github.com/malbeclabs/edge-multicast-ref/go/marketbyorder-bot`;
  `module depthofbook-parser` → `.../go/marketbyorder-parser`. Update any
  internal import paths accordingly.
- `go/go.work`: update the two `use` entries.
- `.github/workflows/go-tests.yml`: update the two matrix paths.
- Dockerfiles: update any `depthofbook` paths/labels.

### Demo infrastructure

- `demo/docker-compose.yml`: service names `depthofbook-parser` / `depthofbook-bot`,
  images `dz/depthofbook-*`, `dockerfile:` paths, `depends_on`, env refs
  (`DZ_DOB_*`, `DOB_BOT_*`), socket `dob.sock` → `mbo.sock`,
  `--clickhouse-database=depthofbook` → `marketbyorder`.
- `demo/.env.example`: `DZ_DOB_*` → `DZ_MBO_*`, `DOB_BOT_METRICS_PORT` →
  `MBO_BOT_METRICS_PORT`, comments "Depth-of-book" → "Market-by-Order". Keep the
  `--depth`/`DZ_MBO_DEPTH` semantics (number of price levels) intact.
- `demo/clickhouse/init/02_schema_dob.sql` → `02_schema_mbo.sql`; database
  `depthofbook` → `marketbyorder` throughout (instruments, events,
  level_snapshots, wire_snapshots, channel_health tables).
- `demo/grafana/dashboards/depthofbook.json` → `marketbyorder.json`; update
  internal queries referencing the `depthofbook` database, panel titles, and
  dashboard title/uid as needed.

Because this is a reference/demo stack with no persisted production data, the
stack is simply re-initialized after rename — no data migration.

### Docs

- Rename + rewrite the 4 dated design/plan docs:
  - `docs/2026-04-23-depthofbook-design.md` → `2026-04-23-marketbyorder-design.md`
  - `docs/2026-04-23-depthofbook-plan.md` → `2026-04-23-marketbyorder-plan.md`
  - `docs/2026-05-19-depthofbook-bot-shard-dispatcher-design.md` → `...marketbyorder-bot...`
  - `docs/2026-05-19-depthofbook-bot-shard-dispatcher-plan.md` → `...marketbyorder-bot...`
  - Update `depthofbook`/`dob`/"Depth-of-Book" references within.
- Top-level `README.md` and per-component READMEs: update prose and paths.

## Loose linkage to the spec repo (all 4 feed components)

Applied to `marketbyorder-parser`, `marketbyorder-bot`, `topofbook-parser`,
`topofbook-bot`:

1. **Component README spec link.** Each README opens with a line such as:
   > Implements the [Market-by-Order Feed](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-order/spec.md) spec.

   (Top-of-Book components link to `top-of-book/spec.md`.)
2. **Wire-format source-of-truth comment.** A package doc comment at the top of
   the wire decoder (`marketbyorder_wire.go`, `tob/topofbook_wire.go`) naming the
   spec as the authoritative definition of the byte layout, with the spec URL.
3. **Top README feed table.** A table in the top-level `README.md` mapping each
   feed → spec link → local impl directory:

   | Feed | Spec | Implementation |
   |------|------|----------------|
   | Top-of-Book & Trades | spec↗ | `go/topofbook-parser`, `go/topofbook-bot` |
   | Market-by-Order | spec↗ | `go/marketbyorder-parser`, `go/marketbyorder-bot` |

No spec **version pin** — deliberately omitted to keep linkage loose (specs are
pre-v1.0.0 drafts; a manual bump step is not wanted yet).

## Verification

1. `go build ./...` and `go test ./...` across the Go workspace (run from `go/`).
2. `docker compose -f demo/docker-compose.yml config` validates the rewritten
   compose + env interpolation.
3. Grep sweep confirms zero residual references, excluding the intentional
   `--depth` flag:
   - `grep -rIiE 'depth[ -]?of[ -]?book|depthofbook'` → no matches
   - `grep -rIE 'DZ_DOB|DOB_BOT|schema_dob|dob\.sock'` → no matches
   - `grep -rn 'depthofbook'` (database/registry/service) → no matches
4. Confirm `--depth` flag still present and functional in marketbyorder-bot.
