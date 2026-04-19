# DZ Top-of-Book Demo Stack

End-to-end demo: multicast feed → parser → example bot → ClickHouse → Grafana. One command to run against any DoubleZero multicast feed.

```
┌──────────────┐  multicast UDP   ┌────────────┐  Unix   ┌──────────────┐  HTTP JSONL  ┌────────────┐   native  ┌────────────┐
│ Edge feed    │─────────────────▶│   parser   │─socket─▶│ example-bot  │─────────────▶│ ClickHouse │◀──────────│ Grafana    │
│ (publisher)  │                  │ (container)│         │ (container)  │              │ (container)│           │ (container)│
└──────────────┘                  └────────────┘         └──────────────┘              └────────────┘           └────────────┘
```

Everything after the feed runs in Docker. The parser uses host networking (for IGMP); everything else uses the default compose network. Grafana auto-provisions the ClickHouse datasource and the TOB dashboard on first boot.

## Prerequisites

- Linux host with Docker and Docker Compose
- A DoubleZero tunnel up on the host (see the top-level project docs) so the parser can join the multicast group. Typically: `doublezerod` running, `doublezero connect multicast subscriber <group_code>` issued, `doublezero1` interface present.

The demo is designed for a host where the DoubleZero tunnel is already in place.

## Quick start

```bash
cd demo
cp .env.example .env
# Edit .env — at minimum set DZ_MULTICAST_GROUP, DZ_INTERFACE, ports
docker compose up -d --build
```

First boot builds the parser and bot images (a minute or two). ClickHouse loads `clickhouse/init/01_schema.sql` the first time and then skips it on subsequent boots. Grafana picks up the datasource + dashboard from `grafana/provisioning/`.

Reach Grafana at `http://localhost:3000` on the host. To reach it from your laptop, SSH-tunnel:

```bash
ssh -L 3000:localhost:3000 ubuntu@<host>
# Open http://localhost:3000 — login: admin / ${GF_ADMIN_PASSWORD}
```

Open the "DZ Top-of-Book" dashboard.

## What you should see

- **Latest top-of-book table** — row per symbol, updates every few seconds
- **Bid / ask time series** — templated on a single symbol, with trade prints overlaid
- **Spread (bps)** — multi-symbol
- **Trade tape** — most recent 100 trades across subscribed symbols
- **Volume bars** — per-symbol traded qty over time
- **Update rate and latency quantiles** — health signals

If the dashboard is empty after boot, give the cold-start buffer a few seconds to clear (the parser can't emit quotes until it sees the refdata cycle). Check:

```bash
docker compose logs parser | tail
docker compose logs bot    | tail
docker compose exec clickhouse clickhouse-client -q \
  "SELECT count(), min(recv_ts), max(recv_ts) FROM topofbook.quotes"
```

## Configuration

All tunables live in `.env`:

| Var | Default | Meaning |
|---|---|---|
| `DZ_MULTICAST_GROUP` | `239.10.10.10` | Multicast group to join |
| `DZ_MARKETDATA_PORT` | `7001` | UDP port for quotes/trades |
| `DZ_REFDATA_PORT` | `7002` | UDP port for instrument definitions |
| `DZ_INTERFACE` | `doublezero1` | Interface to join multicast on |
| `DZ_SYMBOLS` | empty (all) | Comma-separated filter for the bot |
| `GF_ADMIN_PASSWORD` | `admin` | Grafana admin password |
| `GRAFANA_HOST_PORT` | `3000` | Bound to `127.0.0.1` only |
| `CLICKHOUSE_HTTP_PORT` | `8123` | Bound to `127.0.0.1` only |
| `BOT_METRICS_PORT` | `9091` | Bot Prometheus scrape endpoint |
| `PARSER_METRICS_PORT` | `9090` | Parser Prometheus scrape endpoint |

## ClickHouse schema

Three tables under the `topofbook` database:

- `quotes` — one row per Quote frame. Materialized `mid`, `spread`, `spread_bps`, `wire_latency_ms` columns.
- `trades` — one row per Trade frame. `price`, `qty`, `aggressor_side`, `trade_id`, `cumulative_volume`.
- `instruments` — instrument definitions (ReplacingMergeTree — latest row per `instrument_id`).

All tables partition by day (`toYYYYMMDD(recv_ts)`) and `ORDER BY (symbol, recv_ts)` for efficient per-symbol time-range scans.

Full DDL in [clickhouse/init/01_schema.sql](clickhouse/init/01_schema.sql).

### Ad-hoc queries

```bash
docker compose exec clickhouse clickhouse-client
```

Examples:

```sql
-- Current TOB per symbol
SELECT symbol, argMax(bid_price, recv_ts) bid, argMax(ask_price, recv_ts) ask
FROM topofbook.quotes
GROUP BY symbol ORDER BY symbol;

-- 1-min quote rate per symbol
SELECT symbol, toStartOfMinute(recv_ts) t, count() cnt
FROM topofbook.quotes WHERE recv_ts > now() - INTERVAL 10 MINUTE
GROUP BY symbol, t ORDER BY t, symbol;

-- Latency distribution (includes clock skew)
SELECT quantile(0.5)(wire_latency_ms) p50,
       quantile(0.95)(wire_latency_ms) p95,
       quantile(0.99)(wire_latency_ms) p99
FROM topofbook.quotes WHERE recv_ts > now() - INTERVAL 5 MINUTE;
```

## Reaching things

| Service | URL (on host) | Notes |
|---|---|---|
| Grafana | http://localhost:3000 | admin / `$GF_ADMIN_PASSWORD` |
| ClickHouse HTTP | http://localhost:8123 | `SELECT now()` → `200 OK` |
| Bot metrics | http://localhost:9091/metrics | `dz_bot_*` |
| Parser metrics | http://localhost:9090/metrics | `dz_subscriber_*` (host-networked so no port mapping needed) |

All host ports bind `127.0.0.1` only. Edit `docker-compose.yml` if you want public access (not recommended).

## Lifecycle

```bash
# View live logs
docker compose logs -f bot parser

# Stop
docker compose down

# Wipe ClickHouse data
docker compose down -v
```

## Non-goals and caveats

- **Not production-hardened.** ClickHouse runs with no auth, one node, loose ulimits. Grafana has a single admin user. Fine for a demo on a trusted host; don't expose any port to the public internet.
- **Wire latency is relative.** `wire_latency_ms` is publisher wall-clock minus subscriber wall-clock. It folds in NTP skew between hosts; treat it as a trend indicator, not an absolute latency measurement.
- **Tick fidelity depends on the bot.** The bot writes every record to ClickHouse, so tick resolution is preserved. The Prometheus gauges are last-write-wins at scrape time (15s), so don't use them for tick-level analysis — use ClickHouse.
- **One bot per parser socket.** Multiple bots can connect to the parser socket (it's broadcast), but if you want that behavior just scale the `bot` service and point them all at the same socket volume.
