#!/usr/bin/env bash
# `run()` end to end, offline, read by the repository's Go subscriber.
#
# `loopback.sh` composed a publisher by hand. This goes through `run()` — the
# function a venue's `main` calls — with a real config document, the real
# registry, a real adapter reading real recorded bytes, real sockets and the
# real teardown.
set -euo pipefail

GROUP="${GROUP:-233.252.0.19}"
MKTDATA_PORT="${MKTDATA_PORT:-41063}"
REFDATA_PORT="${REFDATA_PORT:-41064}"
PIN="${PIN:-127.0.0.1}"
IFACE="${IFACE:-lo}"
SYMBOL="REPLAY-1"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../../.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"; jobs -p | xargs -r kill 2>/dev/null || true' EXIT
mkdir -p "$work/payloads" "$work/state"

echo "writing the recorded payloads"
(cd "$repo/rust" && cargo run -q -p dz-adapter-uds --example write_records -- \
    --symbol "$SYMBOL" --dir "$work/payloads")

cat >"$work/config.toml" <<TOML
venue = "replay"

[egress]
pin = "$PIN"
ttl = 1

[[feed]]
spec = "top-of-book"
channel_id = 0
source_id = 1
multicast_group = "$GROUP"
mktdata_port = $MKTDATA_PORT
refdata_port = $REFDATA_PORT
heartbeat_interval = "1s"
definition_cycle = "1s"
manifest_cadence = "200ms"
idle_guard = "1h"

[refdata]
state_dir = "$work/state"

[refdata.selection]
bootstrap_top_n = 8
max_published = 16
warn_published_above = 8

[metrics]
enabled = false

[ingress]
kind = "uds"

[adapter]
kind = "uds"

[adapter.replay]
enabled = true
path = "$work/payloads"

[adapter.upstream]
symbols = ["$SYMBOL"]
TOML

echo "building the subscriber"
(cd "$repo/go/topofbook-parser" && go build -o "$work/subscriber" .)

echo "starting the subscriber on $GROUP via $IFACE"
"$work/subscriber" \
    -group "$GROUP" -marketdata-port "$MKTDATA_PORT" -refdata-port "$REFDATA_PORT" \
    -output "$work/out.json" -format json -interface "$IFACE" >"$work/subscriber.log" 2>&1 &
sleep 2

echo "running run()"
(cd "$repo/rust" && cargo run -q -p dz-publisher-runtime --example replay_publisher -- \
    "$work/config.toml") || echo "  run() exited $?"
sleep 2
pkill -f "$work/subscriber" 2>/dev/null || true
sleep 1

echo
echo "what the subscriber decoded:"
python3 - "$work/out.json" "$SYMBOL" <<'PY'
import collections, json, sys

rows = [json.loads(line) for line in open(sys.argv[1])]
symbol = sys.argv[2]
for kind, n in sorted(collections.Counter(r["type"] for r in rows).items()):
    print(f"  {n:3} {kind}")

quotes = [r for r in rows if r["type"] == "quote"]
trades = [r for r in rows if r["type"] == "trade"]
definitions = [r for r in rows if r["type"] == "instrument_definition"]

assert definitions, "no definition reached the subscriber"
assert definitions[0]["symbol"] == symbol, definitions[0]["symbol"]
assert quotes, "no quote reached the subscriber"
assert trades, "no trade reached the subscriber"

# The values the records carried, transcribed from the writer rather than read
# back out of it.
q = quotes[0]["fields"]
assert q["bid_px_raw"] == 9_999_500, q["bid_px_raw"]
assert q["ask_px_raw"] == 10_000_500, q["ask_px_raw"]
assert q["bid_sz_raw"] == 12_500 and q["ask_sz_raw"] == 7_250
assert q["update_flags"] == 3, q["update_flags"]

gone = quotes[1]["fields"]
assert gone["update_flags"] == 6, gone["update_flags"]
assert gone["bid_px_raw"] == 0

t = trades[0]["fields"]
assert t["trade_id"] == 987_654_321, t["trade_id"]
assert t["aggressor_side"] == "buy", t["aggressor_side"]

print()
print("  run() published what the records said, and a Go subscriber read it back")
PY
