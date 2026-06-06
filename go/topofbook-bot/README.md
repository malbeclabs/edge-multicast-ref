# Top-of-Book Bot

> Implements the [Top-of-Book & Trades Feed](https://github.com/malbeclabs/edge-feed-spec/blob/main/top-of-book/spec.md) spec.

Reference Go subscriber that consumes the DoubleZero Top-of-Book parser's Unix socket, filters by symbol, exposes Prometheus metrics, and persists tick-level data into ClickHouse.

## What it does

```
topofbook-parser  ──unix socket JSONL──▶  topofbook-bot  ──/metrics──▶  Prometheus  ──▶  Grafana
```

- Connects to the parser's Unix domain socket
- Decodes each JSON Lines record
- Filters by `--symbol` (comma-separated; empty = pass-through)
- Tracks per-symbol bid/ask price and size, spread, and last trade
- Emits a `dz_bot_*` Prometheus scrape endpoint

The bot is single-host, single-process, and does no persistence. Historical storage, complex strategies, risk — all out of scope. This is the smallest interesting consumer.

## Quick start

Requires a running `topofbook-parser` on the same host writing to a Unix socket:

```bash
# Parser side (example):
dz-topofbook-parser \
  --group 239.10.10.10 \
  --marketdata-port 7001 --refdata-port 7002 \
  --interface doublezero1 \
  --output unix:///tmp/topofbook.sock \
  --metrics-addr 127.0.0.1:9090

# Bot side:
./dz-topofbook-bot \
  --socket /tmp/topofbook.sock \
  --symbol BTC,ETH,SOL \
  --metrics-addr 127.0.0.1:9091
```

Then:

```bash
curl -s http://127.0.0.1:9091/metrics | grep dz_bot_bid_price
# dz_bot_bid_price{symbol="BTC"} 67432.5
# dz_bot_bid_price{symbol="ETH"} 3245.1
# dz_bot_bid_price{symbol="SOL"} 142.88
```

## CLI flags

| Flag | Required | Default | Description |
|---|---|---|---|
| `--socket` | yes | | Path to the topofbook-parser Unix socket |
| `--symbol` | no | `""` (all) | Comma-separated symbol filter (case-sensitive) |
| `--metrics-addr` | no | `127.0.0.1:9091` | Prometheus scrape endpoint |
| `-v` | no | false | Enable debug logging |
| `--version` | no | | Print version and exit |

**Empty `--symbol`:** the bot accepts every record it sees. On a venue with hundreds of instruments this means the per-symbol gauges will have one series per symbol. For Grafana demos and small subscribe lists (< 100 symbols) this is fine.

## Metrics

All prefixed `dz_bot_`. Per-symbol gauges use the `symbol` label; cardinality is bounded by the `--symbol` filter (or by the venue's instrument count when the filter is empty).

### Process

| Name | Type | Labels | Meaning |
|---|---|---|---|
| `build_info` | gauge | `version`, `commit` | Always 1 |
| `uptime_seconds` | gauge | — | Seconds since start |
| `socket_connected` | gauge | — | 1 if connected to the parser socket, 0 otherwise |

### Intake

| Name | Type | Labels | Meaning |
|---|---|---|---|
| `records_total` | counter | `type` | Records processed after filtering |
| `records_dropped_total` | counter | `reason` | Records dropped (`reason=filter` for symbol mismatch) |
| `decode_errors_total` | counter | — | Lines that failed JSON decode |
| `socket_reconnects_total` | counter | `reason` | Reconnects by trigger (`eof`, `read_error`, `dial_failed`) |
| `socket_to_bot_latency_seconds` | histogram | `type` | Publisher `send_ts` → bot receive. Wall-clock across 3 hosts; includes clock skew |

### Top-of-book state (per subscribed symbol)

| Name | Type | Labels | Meaning |
|---|---|---|---|
| `bid_price` | gauge | `symbol` | Latest best bid price |
| `ask_price` | gauge | `symbol` | Latest best ask price |
| `bid_qty` | gauge | `symbol` | Latest best bid size |
| `ask_qty` | gauge | `symbol` | Latest best ask size |
| `spread` | gauge | `symbol` | `ask_price - bid_price` |
| `spread_bps` | gauge | `symbol` | Spread in basis points: `(ask-bid)/mid*10000` |
| `last_trade_price` | gauge | `symbol` | Price of the most recent observed trade |
| `last_trade_qty` | gauge | `symbol` | Quantity of the most recent observed trade |
| `last_update_timestamp_seconds` | gauge | `symbol` | Publisher `send_ts` (Unix seconds) of the most recent record |

### Latency caveat

`socket_to_bot_latency_seconds` compares the publisher's wall-clock timestamp to the bot's wall-clock receipt time. It includes:

1. Publisher → subscriber host wire time (the bulk, typically geographic)
2. Parser decode + sink write delay (sub-ms)
3. Socket read delay at the bot (sub-ms)
4. NTP skew between the publisher host and the bot host

It is a useful trend indicator and relative health signal. Absolute latency attribution requires single-host measurements or a shared time source.

## Reconnect behavior

The bot exponentially backs off on connect failure (250ms → 5s cap). On successful connect the backoff resets. The `socket_reconnects_total` counter records each reconnect with a `reason` label so you can differentiate parser restarts (`eof`) from parser crashes (`read_error`) from parser-missing-on-boot (`dial_failed`).

## Building

```bash
go build -o dz-topofbook-bot .
```

With embedded version info:

```bash
go build -ldflags "-X main.version=0.1.0 -X main.commit=$(git rev-parse --short HEAD) -X main.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)" -o dz-topofbook-bot .
```

Cross-compile to linux/amd64:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o dz-topofbook-bot-linux-amd64 .
```

## Testing

```bash
go test -v .
```

## Grafana tips

- Panel: bid/ask price as time series, templated on `$symbol`
- Panel: `spread_bps` as stat or gauge
- Panel: `rate(dz_bot_records_total[1m])` for update rate
- Panel: `histogram_quantile(0.95, rate(dz_bot_socket_to_bot_latency_seconds_bucket[5m]))` for p95 latency
- Panel: `time() - dz_bot_last_update_timestamp_seconds` to alert on staleness per symbol

## Design notes

- Single flat Go package, minimal external deps (only `prometheus/client_golang`).
- No persistence. State is in-memory gauges; Prometheus is the storage layer.
- The `Record` struct is a direct copy of the parser's output shape, not a shared import. Keeps the example self-contained — readers can understand the whole thing top-to-bottom without chasing cross-module imports.
- Reconnect loop is unconditional. If the parser restarts, the bot reconnects and resumes. No heartbeat tracking — a dead feed will show staleness via `last_update_timestamp_seconds`.
- Symbol filter is case-sensitive and matches the publisher's symbol encoding verbatim.
