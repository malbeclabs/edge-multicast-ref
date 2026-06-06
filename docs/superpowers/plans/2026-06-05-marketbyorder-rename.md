# Market-by-Order Rename Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename the depth-of-book parser/bot and all runtime keys to market-by-order, and add loose spec linkage from every feed component to its spec in edge-feed-spec.

**Architecture:** Behavior-preserving rename. Two token mappings: `depthofbook`→`marketbyorder` (names) and `dob`/`DOB`→`mbo`/`MBO` (abbreviations). The `--depth` CLI flag and all `topofbook` code are NOT renamed. Each task is verified by the existing Go tests staying green and grep sweeps confirming no residual references — there is no new behavior, so no new tests.

**Tech Stack:** Go 1.25 (multi-module workspace via `go.work`), Docker Compose, ClickHouse SQL, Grafana JSON dashboards, GitHub Actions.

**Reference:** Design spec at `docs/superpowers/specs/2026-06-05-marketbyorder-rename-design.md`.

---

## Conventions for every task

- Run Go commands from the `go/` directory (the workspace root).
- After file/dir renames, run `go build ./...` from `go/` to confirm the workspace still resolves.
- Use `git mv` for renames so history is preserved.
- Commit messages follow `component: short description`, lowercase, imperative, no trailing period, no Co-Authored-By trailer.

---

### Task 1: Rename the parser Go module (depthofbook-parser → marketbyorder-parser)

**Files:**
- Rename dir: `go/depthofbook-parser/` → `go/marketbyorder-parser/`
- Rename files within: `depthofbook.go` → `marketbyorder.go`, `depthofbook_wire.go` → `marketbyorder_wire.go`, `depthofbook_test.go` → `marketbyorder_test.go`
- Modify: `go/marketbyorder-parser/go.mod`, `go/marketbyorder-parser/main.go`, `go/marketbyorder-parser/sink.go`, `go/marketbyorder-parser/Dockerfile`, `go/marketbyorder-parser/.gitignore`
- Modify: `go/go.work`

- [ ] **Step 1: Rename the directory and files with git mv**

```bash
cd go
git mv depthofbook-parser marketbyorder-parser
cd marketbyorder-parser
git mv depthofbook.go marketbyorder.go
git mv depthofbook_wire.go marketbyorder_wire.go
git mv depthofbook_test.go marketbyorder_test.go
cd ../..
```

- [ ] **Step 2: Fix the module path in go.mod**

`go/marketbyorder-parser/go.mod` first line is currently `module depthofbook-parser`. Change it to the fully-qualified path matching the topofbook convention:

```
module github.com/malbeclabs/edge-multicast-ref/go/marketbyorder-parser
```

- [ ] **Step 3: Update go.work use entry**

In `go/go.work`, change `./depthofbook-parser` → `./marketbyorder-parser`.

- [ ] **Step 4: Rename identifiers and the registry key**

In `go/marketbyorder-parser/` replace, across all `.go` files:
- `depthOfBookParser` → `marketByOrderParser`
- `DepthOfBook` → `MarketByOrder` (e.g. test names `TestDepthOfBookParser_*` → `TestMarketByOrderParser_*`)
- the registry string `registerParser("depthofbook", ...)` → `registerParser("marketbyorder", ...)`
- the `Name()` return value `"depthofbook"` → `"marketbyorder"`
- any prose comments "depth of book" / "Depth-of-Book" → "market by order" / "Market-by-Order"

Use this to find every hit first, then edit each:

```bash
grep -rInE 'depth[ -]?of[ -]?book|DepthOfBook|depthOfBook|depthofbook' go/marketbyorder-parser
```

- [ ] **Step 5: Update Dockerfile and .gitignore paths**

In `go/marketbyorder-parser/Dockerfile` and `.gitignore`, replace any `depthofbook-parser` path or binary-name references with `marketbyorder-parser`. Find them:

```bash
grep -nE 'depthofbook' go/marketbyorder-parser/Dockerfile go/marketbyorder-parser/.gitignore
```

- [ ] **Step 6: Verify the module builds and tests pass**

Run from `go/`:
```bash
cd go && go build ./marketbyorder-parser/... && go test ./marketbyorder-parser/... && cd ..
```
Expected: build succeeds, all parser tests PASS.

- [ ] **Step 7: Verify no residual references in the parser module**

```bash
grep -rInE 'depth[ -]?of[ -]?book|depthofbook|DepthOfBook' go/marketbyorder-parser
```
Expected: no output.

- [ ] **Step 8: Commit**

```bash
git add -A go/marketbyorder-parser go/go.work
git commit -m "marketbyorder-parser: rename from depthofbook-parser"
```

---

### Task 2: Rename the bot Go module (depthofbook-bot → marketbyorder-bot)

**Files:**
- Rename dir: `go/depthofbook-bot/` → `go/marketbyorder-bot/`
- Modify: `go/marketbyorder-bot/go.mod` (currently `module depthofbook-bot`), `main.go`, `clickhouse_test.go`, `Dockerfile`, `.gitignore`, and any file with `DepthOfBook`/`depthofbook`
- Modify: `go/go.work`

- [ ] **Step 1: Rename the directory with git mv**

```bash
cd go && git mv depthofbook-bot marketbyorder-bot && cd ..
```

(No `depthofbook`-named source files exist in the bot — file names are generic like `bot.go`, `shard.go`. Verify with `ls go/marketbyorder-bot`.)

- [ ] **Step 2: Fix the module path in go.mod**

`go/marketbyorder-bot/go.mod` first line `module depthofbook-bot` → `module github.com/malbeclabs/edge-multicast-ref/go/marketbyorder-bot`.

- [ ] **Step 3: Update go.work use entry**

In `go/go.work`, change `./depthofbook-bot` → `./marketbyorder-bot`.

- [ ] **Step 4: Rename identifiers, registry/database references, and prose**

Find every hit, then edit each:
```bash
grep -rInE 'depth[ -]?of[ -]?book|DepthOfBook|depthOfBook|depthofbook' go/marketbyorder-bot
```
Replace:
- `DepthOfBook`/`depthOfBook` identifiers → `MarketByOrder`/`marketByOrder`
- the `--clickhouse-database` default / any literal `"depthofbook"` → `"marketbyorder"`
- prose comments "Depth-of-Book"/"depth of book" → "Market-by-Order"/"market by order"

**Do NOT touch** the `--depth` flag or any `Depth` field that refers to the number of price levels (it is a real parameter, not the feed name). Inspect each `Depth` hit before editing.

- [ ] **Step 5: Update Dockerfile and .gitignore paths**

```bash
grep -nE 'depthofbook' go/marketbyorder-bot/Dockerfile go/marketbyorder-bot/.gitignore
```
Replace `depthofbook-bot` → `marketbyorder-bot`.

- [ ] **Step 6: Verify the module builds and tests pass**

```bash
cd go && go build ./marketbyorder-bot/... && go test ./marketbyorder-bot/... && cd ..
```
Expected: build succeeds, all bot tests PASS (including `parity_test.go`, `shard_test.go`).

- [ ] **Step 7: Verify no residual feed-name references (the `--depth` flag is allowed)**

```bash
grep -rInE 'depth[ -]?of[ -]?book|depthofbook|DepthOfBook' go/marketbyorder-bot
```
Expected: no output. (`--depth` / `Depth` price-level references contain no "ofbook" and will not match.)

- [ ] **Step 8: Verify the whole workspace still builds**

```bash
cd go && go build ./... && cd ..
```
Expected: success.

- [ ] **Step 9: Commit**

```bash
git add -A go/marketbyorder-bot go/go.work
git commit -m "marketbyorder-bot: rename from depthofbook-bot"
```

---

### Task 3: Update CI workflow matrix

**Files:**
- Modify: `.github/workflows/go-tests.yml`

- [ ] **Step 1: Update the module matrix paths**

In `.github/workflows/go-tests.yml`, change:
- `- go/depthofbook-bot` → `- go/marketbyorder-bot`
- `- go/depthofbook-parser` → `- go/marketbyorder-parser`

Leave `go/topofbook-bot` and `go/topofbook-parser` unchanged.

- [ ] **Step 2: Verify no residual references**

```bash
grep -nE 'depthofbook' .github/workflows/go-tests.yml
```
Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/go-tests.yml
git commit -m "ci: rename depthofbook module paths to marketbyorder"
```

---

### Task 4: Rename demo infrastructure (compose, env, ClickHouse, Grafana)

**Files:**
- Modify: `demo/docker-compose.yml`
- Modify: `demo/.env.example`
- Rename + modify: `demo/clickhouse/init/02_schema_dob.sql` → `02_schema_mbo.sql`
- Rename + modify: `demo/grafana/dashboards/depthofbook.json` → `marketbyorder.json`

- [ ] **Step 1: Rename the ClickHouse schema and Grafana dashboard files**

```bash
git mv demo/clickhouse/init/02_schema_dob.sql demo/clickhouse/init/02_schema_mbo.sql
git mv demo/grafana/dashboards/depthofbook.json demo/grafana/dashboards/marketbyorder.json
```

- [ ] **Step 2: Update docker-compose.yml**

In `demo/docker-compose.yml` apply:
- service `depthofbook-parser:` → `marketbyorder-parser:`
- service `depthofbook-bot:` → `marketbyorder-bot:`
- `dockerfile: go/depthofbook-parser/Dockerfile` → `go/marketbyorder-parser/Dockerfile`
- `dockerfile: go/depthofbook-bot/Dockerfile` → `go/marketbyorder-bot/Dockerfile`
- `image: dz/depthofbook-parser:latest` → `dz/marketbyorder-parser:latest`
- `image: dz/depthofbook-bot:latest` → `dz/marketbyorder-bot:latest`
- `depends_on: - depthofbook-parser` → `- marketbyorder-parser`
- env refs: `${DZ_DOB_MULTICAST_GROUP}`→`${DZ_MBO_MULTICAST_GROUP}`, `${DZ_DOB_REFDATA_PORT}`→`${DZ_MBO_REFDATA_PORT}`, `${DZ_DOB_MKTDATA_PORT}`→`${DZ_MBO_MKTDATA_PORT}`, `${DZ_DOB_SNAPSHOT_PORT}`→`${DZ_MBO_SNAPSHOT_PORT}`, `${DZ_DOB_PARSER_METRICS_PORT}`→`${DZ_MBO_PARSER_METRICS_PORT}`, `${DZ_DOB_SYMBOLS}`→`${DZ_MBO_SYMBOLS}`, `${DZ_DOB_DEPTH:-20}`→`${DZ_MBO_DEPTH:-20}`, `${DZ_DOB_COALESCE_MS:-50}`→`${DZ_MBO_COALESCE_MS:-50}`, `${DOB_BOT_METRICS_PORT:-9092}`→`${MBO_BOT_METRICS_PORT:-9092}`
- socket `unix:///var/run/dz/dob.sock` → `unix:///var/run/dz/mbo.sock` and `--socket=/var/run/dz/dob.sock` → `--socket=/var/run/dz/mbo.sock`
- `--clickhouse-database=depthofbook` → `--clickhouse-database=marketbyorder`

**Keep** the `--depth=...` flag name itself unchanged (only its env var `DZ_DOB_DEPTH`→`DZ_MBO_DEPTH` changes).

- [ ] **Step 3: Update .env.example**

In `demo/.env.example`:
- rename every `DZ_DOB_*` var → `DZ_MBO_*` (MULTICAST_GROUP, REFDATA_PORT, MKTDATA_PORT, SNAPSHOT_PORT, SYMBOLS, DEPTH, COALESCE_MS, PARSER_METRICS_PORT)
- `DOB_BOT_METRICS_PORT` → `MBO_BOT_METRICS_PORT`
- comment headers "Depth-of-book feed" / "Depth-of-book parser" / "Depth-of-book bot" → "Market-by-Order feed/parser/bot"
- keep the "Book depth to maintain (number of price levels)" comment on `DZ_MBO_DEPTH` (the concept is unchanged).

- [ ] **Step 4: Update the ClickHouse schema**

In `demo/clickhouse/init/02_schema_mbo.sql`, replace every `depthofbook` (database name in `CREATE DATABASE` and all `depthofbook.<table>` references) → `marketbyorder`.

- [ ] **Step 5: Update the Grafana dashboard**

In `demo/grafana/dashboards/marketbyorder.json`, replace `depthofbook` references in datasource/database queries, panel titles, and the dashboard `title`/`uid` → `marketbyorder` (or "Market-by-Order" for human-facing titles). Find them:

```bash
grep -niE 'depthofbook|depth of book|depth-of-book' demo/grafana/dashboards/marketbyorder.json
```

- [ ] **Step 6: Validate compose config resolves**

```bash
docker compose -f demo/docker-compose.yml --env-file demo/.env.example config >/dev/null && echo OK
```
Expected: `OK` (no unset-variable or path errors). If Docker is unavailable in the environment, instead grep-verify Step 7 and note the skip.

- [ ] **Step 7: Verify no residual references in demo/**

```bash
grep -rInE 'depthofbook|DZ_DOB|DOB_BOT|dob\.sock|schema_dob|depth[ -]of[ -]book' demo
```
Expected: no output.

- [ ] **Step 8: Commit**

```bash
git add -A demo
git commit -m "demo: rename depthofbook stack to marketbyorder"
```

---

### Task 5: Rename and rewrite the dated design/plan docs

**Files:**
- Rename: `docs/2026-04-23-depthofbook-design.md` → `docs/2026-04-23-marketbyorder-design.md`
- Rename: `docs/2026-04-23-depthofbook-plan.md` → `docs/2026-04-23-marketbyorder-plan.md`
- Rename: `docs/2026-05-19-depthofbook-bot-shard-dispatcher-design.md` → `docs/2026-05-19-marketbyorder-bot-shard-dispatcher-design.md`
- Rename: `docs/2026-05-19-depthofbook-bot-shard-dispatcher-plan.md` → `docs/2026-05-19-marketbyorder-bot-shard-dispatcher-plan.md`

- [ ] **Step 1: Rename the four files**

```bash
git mv docs/2026-04-23-depthofbook-design.md docs/2026-04-23-marketbyorder-design.md
git mv docs/2026-04-23-depthofbook-plan.md docs/2026-04-23-marketbyorder-plan.md
git mv docs/2026-05-19-depthofbook-bot-shard-dispatcher-design.md docs/2026-05-19-marketbyorder-bot-shard-dispatcher-design.md
git mv docs/2026-05-19-depthofbook-bot-shard-dispatcher-plan.md docs/2026-05-19-marketbyorder-bot-shard-dispatcher-plan.md
```

- [ ] **Step 2: Rewrite references inside the four docs**

For each renamed doc, replace `depthofbook`→`marketbyorder`, `DepthOfBook`→`MarketByOrder`, "Depth-of-Book"/"depth of book"→"Market-by-Order"/"market by order", `dob`/`DOB` abbreviations→`mbo`/`MBO`, and any `go/depthofbook-*` paths → `go/marketbyorder-*`. Inspect each `--depth`/price-level `Depth` mention and leave those intact. Find hits per file:

```bash
grep -rInE 'depth[ -]?of[ -]?book|depthofbook|DepthOfBook|DZ_DOB|DOB' docs/2026-04-23-marketbyorder-design.md docs/2026-04-23-marketbyorder-plan.md docs/2026-05-19-marketbyorder-bot-shard-dispatcher-design.md docs/2026-05-19-marketbyorder-bot-shard-dispatcher-plan.md
```

- [ ] **Step 3: Verify no residual references in the renamed docs**

```bash
grep -rInE 'depthofbook|DepthOfBook|depth[ -]of[ -]book' docs/2026-04-23-marketbyorder-*.md docs/2026-05-19-marketbyorder-*.md
```
Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add -A docs
git commit -m "docs: rename depthofbook design/plan docs to marketbyorder"
```

---

### Task 6: Update top-level README and add loose spec linkage to all feed components

**Files:**
- Modify: `README.md` (top-level)
- Modify: `go/marketbyorder-parser/README.md`, `go/marketbyorder-bot/README.md`, `go/topofbook-parser/README.md`, `go/topofbook-bot/README.md`
- Modify: `go/marketbyorder-parser/marketbyorder_wire.go`, `go/topofbook-parser/tob/topofbook_wire.go`

Spec URLs:
- Market-by-Order: `https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-order/spec.md`
- Top-of-Book: `https://github.com/malbeclabs/edge-feed-spec/blob/main/top-of-book/spec.md`

- [ ] **Step 1: Update top-level README prose and paths**

In `README.md`, replace remaining `depthofbook`/"Depth-of-Book" references and `go/depthofbook-*` paths with the marketbyorder equivalents. Find them:

```bash
grep -nE 'depthofbook|depth[ -]of[ -]book|Depth-of-Book' README.md
```

- [ ] **Step 2: Add the feed table to the top-level README**

Add (or extend an existing feeds section in) `README.md` with a table linking each feed to its spec and local implementation:

```markdown
## Feeds

| Feed | Spec | Implementation |
|------|------|----------------|
| Top-of-Book & Trades | [spec](https://github.com/malbeclabs/edge-feed-spec/blob/main/top-of-book/spec.md) | `go/topofbook-parser`, `go/topofbook-bot` |
| Market-by-Order | [spec](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-order/spec.md) | `go/marketbyorder-parser`, `go/marketbyorder-bot` |
```

Place it where the README already introduces the feeds; match surrounding heading level and style.

- [ ] **Step 3: Add the spec link line to each component README**

At the top of each component README (just under the title), add one line:

- `go/marketbyorder-parser/README.md` and `go/marketbyorder-bot/README.md`:
  ```markdown
  > Implements the [Market-by-Order Feed](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-order/spec.md) spec.
  ```
- `go/topofbook-parser/README.md` and `go/topofbook-bot/README.md`:
  ```markdown
  > Implements the [Top-of-Book & Trades Feed](https://github.com/malbeclabs/edge-feed-spec/blob/main/top-of-book/spec.md) spec.
  ```

- [ ] **Step 4: Add the wire-format source-of-truth comment**

At the top of `go/marketbyorder-parser/marketbyorder_wire.go`, immediately above the `package main` line, add:

```go
// Wire format authoritatively defined by the Market-by-Order Feed spec:
// https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-order/spec.md
// Keep the byte layout below in sync with that document.
```

At the top of `go/topofbook-parser/tob/topofbook_wire.go`, immediately above its `package` line, add:

```go
// Wire format authoritatively defined by the Top-of-Book & Trades Feed spec:
// https://github.com/malbeclabs/edge-feed-spec/blob/main/top-of-book/spec.md
// Keep the byte layout below in sync with that document.
```

- [ ] **Step 5: Verify both wire modules still build**

```bash
cd go && go build ./marketbyorder-parser/... ./topofbook-parser/... && cd ..
```
Expected: success.

- [ ] **Step 6: Commit**

```bash
git add README.md go/marketbyorder-parser go/marketbyorder-bot go/topofbook-parser go/topofbook-bot
git commit -m "docs: link feed parsers/bots to edge-feed-spec specs"
```

---

### Task 7: Final whole-repo verification sweep

**Files:** none (verification only)

- [ ] **Step 1: Full workspace build and test**

```bash
cd go && go build ./... && go test ./... && cd ..
```
Expected: all modules build, all tests PASS.

- [ ] **Step 2: Whole-repo residual grep (excluding intentional `--depth`)**

```bash
grep -rInE 'depthofbook|DepthOfBook|depthOfBook|depth[ -]of[ -]book|DZ_DOB|DOB_BOT|dob\.sock|schema_dob' . \
  --include='*.go' --include='*.md' --include='*.yml' --include='*.yaml' \
  --include='*.json' --include='*.sql' --include='*.example' --include='Dockerfile' \
  --include='*.work' --include='*.mod' --include='.env.example' \
  | grep -v '/.git/'
```
Expected: no output. (Note: the historical design doc at
`docs/superpowers/specs/2026-06-05-marketbyorder-rename-design.md` and this plan
intentionally contain the old strings as the rename record — if they appear,
that is expected and acceptable; everything else must be clean.)

- [ ] **Step 3: Confirm the `--depth` flag survived**

```bash
grep -rn '\-\-depth' demo go/marketbyorder-bot
```
Expected: the `--depth` flag still present in compose and/or bot flag parsing.

- [ ] **Step 4: Confirm directory layout**

```bash
ls go | grep -E 'marketbyorder|depthofbook'
```
Expected: `marketbyorder-bot` and `marketbyorder-parser` present; no `depthofbook-*`.

- [ ] **Step 5: Final commit if any verification fixups were needed**

If steps 1–4 surfaced stragglers, fix them and commit:
```bash
git add -A && git commit -m "marketbyorder: clean up residual depthofbook references"
```
Otherwise, no commit needed.
