# DZ Top-of-Book Parser

A standalone multicast subscriber that decodes DoubleZero Top-of-Book (DZ-TOB v0.1.0) wire-format frames and writes decoded market data records to a file or Unix socket.

## What it does

Joins a multicast group on two UDP ports (marketdata + refdata), decodes the binary DZ-TOB protocol, and outputs structured records (quotes, trades, instrument definitions, heartbeats) as JSON lines, CSV, or a broadcast Unix socket that trading bots connect to.

```
multicast group (UDP)
  ├── refdata port ──► InstrumentDefinition messages
  └── marketdata port ──► Quote (BBO), Trade, Heartbeat, etc.
                              │
                              ▼
                     TopOfBookParser
                              │
                              ▼
                        OutputSink
                     ┌────────┼────────┐
                     │        │        │
                  JSON file  CSV file  Unix socket
                                       (broadcast to
                                        all connected
                                        trader bots)
```

## Quick start

```bash
go build -o dz-topofbook-parser .

# JSON output to a file
./dz-topofbook-parser \
  --group 239.10.10.10 \
  --marketdata-port 7001 \
  --refdata-port 7002 \
  --format json \
  --output /tmp/topofbook.json

# Unix socket for trader bots
./dz-topofbook-parser \
  --group 239.10.10.10 \
  --marketdata-port 7001 \
  --refdata-port 7002 \
  --format json \
  --output unix:///tmp/topofbook.sock

# CSV output
./dz-topofbook-parser \
  --group 239.10.10.10 \
  --marketdata-port 7001 \
  --refdata-port 7002 \
  --format csv \
  --output /tmp/topofbook.csv
```

Runs until SIGINT or SIGTERM.

## CLI flags

| Flag | Required | Default | Description |
|---|---|---|---|
| `--group` | yes | | Multicast group IP address |
| `--marketdata-port` | yes | | UDP port for marketdata (quotes, trades) |
| `--refdata-port` | yes | | UDP port for refdata (instrument definitions) |
| `--format` | no | `json` | Output format: `json` or `csv` |
| `--output` | yes | | Output path: file path or `unix:///path/to/sock` |
| `--parser` | no | `topofbook` | Parser to use (currently only `topofbook`) |
| `--interface` | no | system-selected | Network interface to join multicast on (e.g. `doublezero1`) |
| `-v` | no | false | Enable debug logging |
| `--version` | no | | Print version and exit |

**`--interface` note:** On a host with multiple NICs (common alongside a DoubleZero GRE tunnel), the default multicast join may pick the wrong interface. Pass `--interface doublezero1` to join on the tunnel.

## Output formats

### JSON (default)

One JSON object per line (JSON Lines):

```json
{"type":"quote","ts":"2026-04-11T05:26:06.699Z","channel_id":0,"seq":5241,"instrument_id":40,"symbol":"SEI","fields":{"bid_price":0.055615,"bid_qty":14572,"ask_price":0.055631,"ask_qty":7874,"bid_source_count":3,"ask_source_count":1,"source_id":1,"update_flags":3,"snapshot":false}}
```

### CSV

Header row auto-inferred from the first record:

```csv
type,ts,channel_id,seq,instrument_id,symbol,bid_price,bid_qty,ask_price,ask_qty,...
quote,2026-04-11T05:26:06.699Z,0,5241,40,SEI,0.055615,14572,0.055631,7874,...
```

### Unix socket

Broadcast to all connected clients. Trader bots connect and read one record per line. Drop-on-slow-consumer: a stalled bot gets gaps rather than blocking the feed for everyone else.

```bash
socat UNIX-CONNECT:/tmp/topofbook.sock - | jq .
```

## Building

```bash
go build -o dz-topofbook-parser .
```

With version info:

```bash
go build -ldflags "-X main.version=0.1.0 -X main.commit=$(git rev-parse --short HEAD) -X main.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)" -o dz-topofbook-parser .
```

## Testing

```bash
go test -v .
```
