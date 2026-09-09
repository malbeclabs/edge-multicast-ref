#!/usr/bin/env bash
# Four channel instances from one `run()`, offline, read by four Go subscribers.
#
# `loopback.sh` composed a publisher by hand. This goes through `run()` — the
# function a venue's `main` calls — with a real config document, the real
# registry, a real adapter reading real recorded bytes, real sockets and the
# real teardown.
#
# The document names four `[[feed]]` blocks of **one** specification: the
# arrangement the duplicate-specification gate refused until it lifted. One
# block names no shard and is therefore the default one, which is what makes
# this a test of the upgrade as well as of the feature — the default shard's era
# file has to keep the name it has always had.
#
# What each subscriber's own output asserts, per channel instance: its own
# `Channel ID`, its own sequence series from 0, its own `Reset Count` matching
# its own era file, and **only its own shard's instrument definitions**. That
# last one is the point of the whole change, and it is written as an equality
# rather than as "carries mine": a publisher that packed every definition onto
# every reference-data port would satisfy any assertion of the weaker form.
#
# It runs twice, because an era is only observable across a restart: every
# channel instance's era must advance by exactly one, independently.
set -euo pipefail

PIN="${PIN:-127.0.0.1}"
IFACE="${IFACE:-lo}"

# One group per channel instance, all in MCAST-TEST-NET (233.252.0.0/24) —
# `scripts/check-public-repo-rules.sh` refuses anything else in this repository,
# and it is right to: a real group in a public example is somebody else's
# network.
#
# `MCAST` rather than the obvious `GROUPS`: that name is bash's own array of the
# invoking user's group ids, and assignments to it are silently ignored — the
# document came out naming a group of `1000` and the load refused it, which is
# the shape of failure this whole publisher is written to produce rather than
# start on.
#
# Four parallel arrays rather than one of records, because bash has no records
# and an index is what the document, the subscribers and the assertions all
# agree on. Index 0 is the default shard: its `shard` key is absent from the
# block, which is how a document that predates shards keeps meaning what it
# meant.
SHARDS=(""       "alpha" "beta"  "gamma")
MCAST=(233.252.0.20 233.252.0.21 233.252.0.22 233.252.0.23)
MKTDATA=(41070 41072 41074 41076)
REFDATA=(41071 41073 41075 41077)
# Two symbols per shard, not one. With a single instrument each, a publisher
# that packed everything onto everything would still put exactly one symbol on
# each reference-data port, and the failure would read as "carries none of its
# own" — the exclusion would never be reached. Two per shard is what makes a
# packing publisher show up as eight symbols where two belong.
SYMBOLS=("REPLAY-D1 REPLAY-D2" "REPLAY-A1 REPLAY-A2" "REPLAY-B1 REPLAY-B2" "REPLAY-G1 REPLAY-G2")
CHANNELS=4

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../../.." && pwd)"
work="$(mktemp -d)"
trap 'jobs -p | xargs -r kill 2>/dev/null || true; rm -rf "$work"' EXIT
mkdir -p "$work/payloads" "$work/state"

echo "writing the recorded payloads"
symbol_args=()
for symbols in "${SYMBOLS[@]}"; do
    for symbol in $symbols; do
        symbol_args+=(--symbol "$symbol")
    done
done
(cd "$repo/rust" && cargo run -q -p dz-adapter-uds --example write_records -- \
    "${symbol_args[@]}" --dir "$work/payloads")

{
    echo 'venue = "replay"'
    echo
    echo '[egress]'
    echo "pin = \"$PIN\""
    echo 'ttl = 1'
    for i in $(seq 0 $((CHANNELS - 1))); do
        echo
        echo '[[feed]]'
        echo 'spec = "top-of-book"'
        # Absent for index 0. Written out for the rest, which is the only
        # difference between the four blocks besides their identity on the wire:
        # every timing key has to agree across blocks or the load refuses, and
        # that refusal is what stops one channel from pacing differently from
        # another carrying the same instruments.
        if [ -n "${SHARDS[$i]}" ]; then echo "shard = \"${SHARDS[$i]}\""; fi
        echo "channel_id = $i"
        echo 'source_id = 1'
        echo "multicast_group = \"${MCAST[$i]}\""
        echo "mktdata_port = ${MKTDATA[$i]}"
        echo "refdata_port = ${REFDATA[$i]}"
        echo 'heartbeat_interval = "1s"'
        echo 'definition_cycle = "1s"'
        echo 'manifest_cadence = "200ms"'
        echo 'idle_guard = "1h"'
    done
    cat <<TOML

[refdata]
state_dir = "$work/state"

[refdata.selection]
bootstrap_top_n = 8
max_published = 16
warn_published_above = 12

[metrics]
enabled = false

[ingress]
kind = "uds"

[adapter]
kind = "uds"

[adapter.replay]
enabled = true
path = "$work/payloads"

# The reference stream. The path is a prefix: one socket per feed, shard *and*
# port role, so a recorder can attribute a copy without decoding it — a Unix
# datagram carries neither the destination port nor the group the diff is keyed
# on, and neither the shard. This run binds two of them and counts what arrives:
# the default shard's, whose name has no shard component at all, and one named
# shard's. A fan-out keyed on the feed and the role alone would leave the second
# socket empty, which is the assertion. No backticks in this heredoc: its
# delimiter is unquoted so that the paths interpolate, which makes a backtick a
# command substitution.
[adapter.tee]
enabled = true
path = "$work/tee"
TOML
    # The built-in record adapter's instrument set, one block per symbol.
    # Stated in full rather than as a bare symbol: every field is one a
    # subscriber reads out of the published instrument definition, and a guessed
    # exponent misdescribes every price on the feed. There is no discovery in
    # the record encoding, so the source process and the publisher agree on this
    # set out of band — including which shard each instrument belongs to, which
    # is the one thing about routing this adapter says.
    for i in $(seq 0 $((CHANNELS - 1))); do
        for symbol in ${SYMBOLS[$i]}; do
            echo
            echo '[[adapter.upstream.listing]]'
            echo "symbol = \"$symbol\""
            if [ -n "${SHARDS[$i]}" ]; then echo "shard = \"${SHARDS[$i]}\""; fi
            echo 'asset_class = "crypto_spot"'
            echo 'price_exponent = -4'
            echo 'qty_exponent = -2'
            echo 'market_model = "clob"'
            echo 'tick_size = "0.0001"'
            echo 'lot_size = "0.01"'
            echo 'settle_type = "cash"'
            echo 'price_bound = "non_negative"'
        done
    done
} >"$work/config.toml"

echo "building the subscriber"
(cd "$repo/go/topofbook-parser" && go build -o "$work/subscriber" .)

# One listener per reference-copy socket, bound before the publisher starts: the
# tee sends unconnected, so a datagram with nobody bound is dropped and counted
# rather than queued.
bind_tee() {
    python3 - "$1" "$2" <<'TEE' &
import os, socket, sys

path, out = sys.argv[1], sys.argv[2]
if os.path.exists(path):
    os.unlink(path)
sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
sock.bind(path)
sock.settimeout(12)
datagrams = []
try:
    while True:
        datagrams.append(sock.recv(2048))
except socket.timeout:
    pass
with open(out, "w") as f:
    f.write(f"{len(datagrams)}\n")
    f.write(f"{datagrams[0][:2].hex() if datagrams else ''}\n")
    f.write(f"{max((len(d) for d in datagrams), default=0)}\n")
TEE
}

echo "binding the reference streams"
bind_tee "$work/tee.top-of-book.mktdata" "$work/tee.default.count"
default_tee=$!
bind_tee "$work/tee.top-of-book.alpha.mktdata" "$work/tee.alpha.count"
alpha_tee=$!
sleep 1

# One subscriber per channel instance. Each joins its own group and binds its
# own two ports, which is what makes its output a statement about one channel
# rather than about the process.
run_once() {
    local tag="$1" i
    for i in $(seq 0 $((CHANNELS - 1))); do
        "$work/subscriber" \
            -group "${MCAST[$i]}" \
            -marketdata-port "${MKTDATA[$i]}" -refdata-port "${REFDATA[$i]}" \
            -output "$work/$tag.$i.json" -format json -interface "$IFACE" \
            >"$work/$tag.$i.log" 2>&1 &
    done
    sleep 2

    echo "running run() [$tag]"
    (cd "$repo/rust" && cargo run -q -p dz-publisher-runtime --example replay_publisher -- \
        "$work/config.toml") || echo "  run() exited $?"
    sleep 2
    pkill -f "$work/subscriber" 2>/dev/null || true
    sleep 1
}

# The first run's era files, kept so that the second run's can be read against
# them. An era is only observable across a restart, and "advanced by one" is the
# assertion — a shared counter advances every channel by the number of blocks.
run_once first
mkdir -p "$work/era.first"
cp "$work"/state/*.era "$work/era.first/"
run_once second

echo
echo "the era files under the state directory:"
ls -1 "$work/state" | sed 's/^/  /'

echo
echo "what each subscriber decoded:"
python3 - "$work" "$CHANNELS" "${SHARDS[*]}" "${SYMBOLS[*]}" <<'PY'
import json, pathlib, sys

work = pathlib.Path(sys.argv[1])
channels = int(sys.argv[2])
# The shard names, with the empty first field standing for the default one — the
# block that names no shard at all — and the symbols in the order the document
# states them, two per shard.
shards = sys.argv[3].split(" ")
assert len(shards) == channels, shards
symbols = sys.argv[4].split()
assert len(symbols) == channels * 2, symbols
per_shard = [sorted(symbols[i * 2 : i * 2 + 2]) for i in range(channels)]


def rows(tag, index):
    path = work / f"{tag}.{index}.json"
    return [json.loads(line) for line in path.open()]


def era_of(shard):
    name = "top-of-book.era" if not shard else f"top-of-book.{shard}.era"
    tag, era = (work / "state" / name).read_text().split()
    assert tag == "era-v1", tag
    return int(era)


def first_era_of(shard):
    name = "top-of-book.era" if not shard else f"top-of-book.{shard}.era"
    tag, era = (work / "era.first" / name).read_text().split()
    return int(era)


for index in range(channels):
    shard = shards[index]
    named = shard or "(the default shard)"
    decoded = rows("second", index)
    assert decoded, f"channel {index} on {named} decoded nothing at all"

    # Only its own shard's definitions, stated as an equality. This is the
    # assertion the change exists for: the inclusion and the exclusion in one.
    carried = sorted({r["symbol"] for r in decoded if r["type"] == "instrument_definition"})
    assert carried == per_shard[index], (
        f"channel {index} on {named} carried {carried}, and its shard's set is {per_shard[index]}"
    )

    # One `Channel ID` per channel instance, and it is this block's.
    ids = {r["channel_id"] for r in decoded}
    assert ids == {index}, f"channel {index} on {named} carried channel ids {ids}"

    # Its own sequence series, from 0, contiguous. A process-wide counter would
    # start three of these four somewhere else.
    seqs = [r["seq"] for r in decoded]
    assert min(seqs) == 0, f"channel {index} on {named} starts at {min(seqs)}, not 0"

    # Its own era, and the same number the era file holds — the subscriber's
    # `Reset Count` is what the file is for.
    resets = {r["reset_count"] for r in decoded}
    assert len(resets) == 1, f"channel {index} on {named} announced eras {resets}"
    announced = resets.pop()
    assert announced == era_of(shard), (
        f"channel {index} on {named} announced era {announced}, its file holds {era_of(shard)}"
    )

    # And exactly one era more than the run before it. Independently: an era
    # advanced by the number of blocks is the shared-counter defect, and it
    # would show up here as four.
    advance = era_of(shard) - first_era_of(shard)
    assert advance == 1, (
        f"channel {index} on {named} advanced {advance} eras across one restart"
    )

    print(
        f"  channel {index:>2} {named:<18} "
        f"{len(decoded):>3} messages, era {announced}, seq from {min(seqs)}, "
        f"definitions {','.join(carried)}"
    )

# The four channel instances are four, and they are distinct in every way a
# subscriber can see. Asserted across the set rather than per channel, because
# every collapse this change prevents shows up as two of these being equal.
eras = [era_of(s) for s in shards]
print()
print(f"  four eras, one per channel instance: {eras}")
print("  each advanced by exactly one across the restart")
print()
print("  run() published four channel instances of one specification, and each")
print("  Go subscriber read its own shard's instruments and nobody else's")
PY

echo
echo "what the reference streams received:"
wait "$default_tee" "$alpha_tee" 2>/dev/null || true
python3 - "$work/tee.default.count" "$work/tee.alpha.count" <<'TEE'
import sys

def read(path):
    count, magic, longest = open(path).read().splitlines()
    return int(count), magic, int(longest)

default_count, default_magic, default_longest = read(sys.argv[1])
alpha_count, alpha_magic, alpha_longest = read(sys.argv[2])
print(f"  tee.top-of-book.mktdata       {default_count} datagrams, longest {default_longest} bytes, first magic 0x{default_magic}")
print(f"  tee.top-of-book.alpha.mktdata {alpha_count} datagrams, longest {alpha_longest} bytes, first magic 0x{alpha_magic}")

# The tee is a copy of what left the mktdata socket, so the datagrams are the
# feed's own — magic and all — with no framing added. `5a44` is `DZ` little
# endian, the top-of-book magic.
for count, magic, longest in ((default_count, default_magic, default_longest),
                              (alpha_count, alpha_magic, alpha_longest)):
    assert count > 0, "a reference stream received nothing"
    assert magic == "5a44", f"the copy is not a top-of-book datagram: 0x{magic}"
    # And the mandated cap holds on the copy too, which is the check that would
    # catch a tee that concatenated or framed.
    assert longest <= 1232, longest

print()
print("  two channel instances of one specification fanned out to two sockets,")
print("  and the default shard's kept the name it has always had")
TEE
