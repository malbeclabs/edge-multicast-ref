#!/usr/bin/env bash
# Run a real publisher and read it with the repository's Go subscriber.
#
# Everything in `cargo test` holds a fake socket. This holds a real one, and
# what reads the other end is a different language — which is the only check
# that is not our encoder agreeing with our decoder.
#
# Needs: a host that delivers multicast to itself, and a Go toolchain.
set -euo pipefail

GROUP="${GROUP:-233.252.0.9}"
MKTDATA_PORT="${MKTDATA_PORT:-41033}"
REFDATA_PORT="${REFDATA_PORT:-41034}"
PIN="${PIN:-127.0.0.1}"
IFACE="${IFACE:-lo}"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../../.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"; jobs -p | xargs -r kill 2>/dev/null || true' EXIT

echo "building the subscriber"
(cd "$repo/go/topofbook-parser" && go build -o "$work/subscriber" .)

echo "starting the subscriber on $GROUP via $IFACE"
"$work/subscriber" \
    -group "$GROUP" \
    -marketdata-port "$MKTDATA_PORT" \
    -refdata-port "$REFDATA_PORT" \
    -output "$work/out.json" \
    -format json \
    -interface "$IFACE" >"$work/subscriber.log" 2>&1 &
sleep 2

echo "publishing"
(cd "$repo/rust" && cargo run -q -p dz-publisher-runtime --example loopback_publisher -- \
    --group "$GROUP" \
    --mktdata-port "$MKTDATA_PORT" \
    --refdata-port "$REFDATA_PORT" \
    --pin "$PIN" \
    --state-dir "$work/state")
sleep 2
pkill -f "$work/subscriber" 2>/dev/null || true
sleep 1

echo
echo "what the subscriber decoded:"
python3 - "$work/out.json" <<'PY'
import collections, json, sys

rows = [json.loads(line) for line in open(sys.argv[1])]
counts = collections.Counter(r["type"] for r in rows)
for kind, n in sorted(counts.items()):
    print(f"  {n:3} {kind}")

# The values a subscriber must see, transcribed from the publisher's own
# fixture rather than read back out of it. These are the cross-language golden
# vector's raw integers, which is why they are these numbers and not others.
expected = {
    "bid_px_raw": 9_999_500,
    "bid_sz_raw": 12_500,
    "ask_px_raw": 10_000_500,
    "ask_sz_raw": 7_250,
    "bid_source_count": 3,
    "ask_source_count": 4,
    "update_flags": 3,
}
quotes = [r for r in rows if r["type"] == "quote"]
assert len(quotes) == 2, f"expected two quotes, got {len(quotes)}"
for field, want in expected.items():
    got = quotes[0]["fields"][field]
    assert got == want, f"{field}: subscriber read {got}, publisher sent {want}"

# The one-sided quote. `update_flags = 6` is bid-gone plus ask-updated, and the
# gone side carries zeros - which only mean nothing because the flag says so.
gone = quotes[1]["fields"]
assert gone["update_flags"] == 6, f"update_flags {gone['update_flags']}, wanted 6"
assert gone["bid_px_raw"] == 0 and gone["bid_sz_raw"] == 0, "a gone side carries zeros"

trades = [r for r in rows if r["type"] == "trade"]
assert len(trades) == 1, f"expected one trade, got {len(trades)}"
t = trades[0]["fields"]
assert t["trade_id"] == 987_654_321, t["trade_id"]
assert t["aggressor_side"] == "buy", t["aggressor_side"]

definitions = [r for r in rows if r["type"] == "instrument_definition"]
assert definitions, "no definition reached the subscriber"
d = definitions[0]
assert d["symbol"] == "LOOPBACK-1", d["symbol"]
assert d["fields"]["price_exponent"] == -4 and d["fields"]["qty_exponent"] == -2

assert any(r["type"] == "end_of_session" for r in rows), "no EndOfSession"
assert any(r["type"] == "manifest_summary" for r in rows), "no ManifestSummary"

print()
print("  every value the publisher sent is the value the subscriber read")
PY
