# DoubleZero Market-by-Price Parser

> Implements the [Market-by-Price Feed](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-price/spec.md) spec.

A standalone multicast subscriber that decodes DoubleZero Market-by-Price wire-format frames and writes decoded market-data records to a file or Unix socket.

**Dual wire schema support.** `InstrumentDefinition` is decoded at schema versions 1 and 3, selected per frame from the frame header's Schema Version byte — not a build-time or CLI setting. v3 inserts `Source ID` (`u16`) after `Instrument ID` and widens `Symbol` from 16 to 64 bytes; all other fields are unchanged. There is no version 2: that layout was specified upstream and superseded before any publisher emitted it, so the accepted versions are the set `{1, 3}` and version 2 is rejected exactly like version 0. A frame whose declared Schema Version disagrees with the length its `InstrumentDefinition` body actually carries is counted malformed (as a frame-level `parse_errors_total{reason="truncated"}`) and the whole frame is skipped, not guessed at. `frames_total{port,schema_version}` (see [Metrics](#metrics)) is how to watch a publisher's v1-to-v3 cutover in production.

**The parser is stateless.** It decodes each wire message into a JSON record and forwards it; it does not track price levels, does not reconstruct an order book, and holds no per-instrument state across frames. Book construction, snapshot/delta reconciliation, and persistence belong to a separate consumer — the planned `marketbyprice-bot` — which subscribes to this parser's output socket and does that work.

Sibling to [marketbyorder-parser](../marketbyorder-parser/) and [topofbook-parser](../topofbook-parser/).

## Three-port channel model

The feed is delivered on one multicast group across three UDP ports. Concrete port numbers are assigned per deployment; this parser takes them as flags.

| Port flag | Channel | Carries |
|---|---|---|
| `--mktdata-port` | mktdata | `LevelUpdate`, `BookClear`, `Trade`, `Liquidation`, `BatchBoundary`, `InstrumentReset`, `Heartbeat`, `EndOfSession` |
| `--refdata-port` | refdata | `InstrumentDefinition`, `ManifestSummary` |
| `--snapshot-port` | snapshot | `SnapshotBegin`, `SnapshotLevel`, `SnapshotEnd` |

A cold-start subscriber binds all three ports. The snapshot stream lets a subscriber bootstrap or recover from packet loss without any out-of-band replay mechanism; a subscriber with its own recovery path may skip `--snapshot-port` at the cost of that in-band recovery.

## Build

```bash
go build -o dz-marketbyprice-parser .
```

## Run

```bash
./dz-marketbyprice-parser \
  --group 239.10.10.10 \
  --refdata-port 7101 \
  --mktdata-port 7102 \
  --snapshot-port 7103 \
  --interface doublezero1 \
  --output unix:///tmp/marketbyprice.sock \
  --format json
```

Runs until SIGINT or SIGTERM.

### CLI flags

| Flag | Required | Default | Description |
|---|---|---|---|
| `--group` | yes | | Multicast group IP address |
| `--refdata-port` | yes | | UDP port for refdata |
| `--mktdata-port` | yes | | UDP port for mktdata |
| `--snapshot-port` | yes | | UDP port for the snapshot stream |
| `--output` | yes | | Output path: file path or `unix:///path/to/sock` |
| `--format` | no | `json` | Output format: `json` |
| `--parser` | no | `marketbyprice` | Parser name from registry |
| `--interface` | no | system-selected | Network interface to join multicast on (e.g. `doublezero1`) |
| `--metrics-addr` | no | (off) | If set, serve Prometheus `/metrics` on this addr (e.g. `127.0.0.1:9090`) |
| `-v` | no | false | Enable debug logging |
| `--version` | no | | Print version and exit |

**`--interface` note:** on a host with multiple NICs (common alongside a DoubleZero GRE tunnel), the default multicast join may pick the wrong interface. Pass `--interface doublezero1` to join on the tunnel.

## Output record envelope

Every decoded message becomes one JSON line: an envelope of type/timestamp/sequencing fields plus a `fields` map holding the message-type-specific payload. One real `level_update` line, produced by the `msgTypeLevelUpdate` case in `marketbyprice.go`:

```json
{"type":"level_update","ts":"2026-08-02T14:23:01.5Z","source_ts_ns":1785680581498500000,"send_ts_ns":1785680581500000000,"parser_kernel_recv_ts_ns":1785680581502500000,"recv_ts_kind":"kernel_udp_software","channel_id":3,"port":"mktdata","seq":500,"reset_count":2,"instrument_id":11,"fields":{"action":"new","amm_synthetic":false,"implied":false,"level_flags":0,"level_index":2,"order_count":3,"per_instrument_seq":1010,"price_raw":1000,"qty_raw":75,"side":"bid","source_id":100,"timestamp":"2026-08-02T14:23:01.4985Z","update_reason":"new_order"}}
```

**`order_count` and `level_index` are omitted when the wire carries `0xFFFF`.** That sentinel means the value is absent, or too large to express in 16 bits — not literally 65535. A `LevelUpdate` with a real `order_count` of `0` still emits `"order_count":0`; only the `0xFFFF` sentinel triggers omission.

**`snapshot_level` records carry no `instrument_id`.** The wire format omits it because the containing `SnapshotBegin` implies it. A consumer must attribute each `snapshot_level` to the most recently seen `snapshot_begin` on the `snapshot` port — not by `snapshot_id`, which is monotonic per `(channel_id, instrument_id)` rather than per channel, so two instruments can be mid-snapshot at the same `snapshot_id` simultaneously. `snapshot_id` validates the association (discard on mismatch); it must never be used as the key. The parser preserves group ordering as received, so a consumer reading the output socket in order can rely on `snapshot_begin` → its `snapshot_level`s → `snapshot_end` arriving as a contiguous, correctly-ordered run per instrument.

## Metrics

Namespace `dz_mbp_parser`. All metrics are exposed on `--metrics-addr` at `/metrics` when set.

| Metric | Type | Meaning |
|---|---|---|
| `dz_mbp_parser_ingress_packets_total{port}` | counter | UDP datagrams received, by port |
| `dz_mbp_parser_ingress_bytes_total{port}` | counter | UDP bytes received, by port |
| `dz_mbp_parser_parse_errors_total{port,reason}` | counter | Frame decode failures, by port and failure reason |
| `dz_mbp_parser_records_total{type}` | counter | Records emitted, by record type |
| `dz_mbp_parser_source_latency_seconds{port}` | histogram | Latency from block/venue source timestamp to kernel receive, by port (crosses validator and local clocks) |
| `dz_mbp_parser_send_latency_seconds{port}` | histogram | Latency from publisher egress send timestamp to kernel receive, by port |
| `dz_mbp_parser_socket_clients` | gauge | Currently connected Unix socket clients |
| `dz_mbp_parser_socket_client_drops_total{reason}` | counter | Slow socket clients dropped, by reason |
| `dz_mbp_parser_socket_records_sent_total` | counter | Records written to at least one socket client |
| `dz_mbp_parser_sink_write_errors_total` | counter | Output sink write failures |
| `dz_mbp_parser_frames_total{port,schema_version}` | counter | Successfully parsed frames, by port and wire Schema Version. The way to watch a publisher's v1-to-v3 cutover: `schema_version="3"` climbing while `schema_version="1"` goes flat, then to zero, is when the v1 decode path can be retired. `schema_version="2"` should never appear; a nonzero count there means a publisher is emitting a version this parser believes does not exist |
| `dz_mbp_parser_frame_seq_gaps_total{port,source_ip,channel_id}` | counter | Frame-header sequence discontinuities detected (real UDP datagram loss events), by port, publisher source IP, and channel ID |
| `dz_mbp_parser_frames_missing_total{port,source_ip,channel_id}` | counter | Total frames missing, summed across gap magnitudes in the frame-header sequence, by port, publisher source IP, and channel ID |
| `dz_mbp_parser_snapshot_flag_mismatch_total{port}` | counter | Application-header snapshot flag disagreeing with the arrival port — a publisher defect, never used for routing |
| `dz_mbp_parser_malformed_total{reason}` | counter | Individual messages the spec declares malformed, dropped without failing the containing frame |
| `dz_mbp_parser_skipped_messages_total{reason}` | counter | Messages decoded but not emitted as records, by reason |
| `dz_mbp_parser_build_info{version,commit}` | gauge | Always `1`; labels carry build version/commit |
| `dz_mbp_parser_uptime_seconds` | gauge | Seconds since process start |

`snapshot_flag_mismatch_total` and `malformed_total` are publisher-defect counters: the parser still decodes and forwards these messages (or, for a malformed `BookClear`, drops just that message and keeps the rest of the frame), so they exist purely to surface upstream data-quality problems, not to indicate parser failure.

`skipped_messages_total` is deliberately separate from `malformed_total`. Its only reason today is `unknown_type`, raised when a Type ID this decoder does not implement is skipped by its `Message Length`. That is the spec's forward-compatibility rule working as intended, not a publisher defect, so it must not fire a malformed-message alert — but it is data the parser does not emit, so a sustained non-zero rate means the publisher has started sending a message type this build needs to learn.

## Architecture

```
multicast group (UDP)
  ├── refdata port   ──► InstrumentDefinition, ManifestSummary
  ├── mktdata port   ──► LevelUpdate, BookClear, Trade, Liquidation,
  │                      BatchBoundary, InstrumentReset, Heartbeat, EndOfSession
  └── snapshot port  ──► SnapshotBegin, SnapshotLevel, SnapshotEnd
                              │
                              ▼
                     marketByPriceParser (stateless)
                              │
                              ▼
                        OutputSink
                     ┌────────┴────────┐
                     │                 │
                  JSON file      Unix socket
                                 (broadcast to
                                  all connected
                                  consumers, e.g.
                                  marketbyprice-bot)
```

See the [spec](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-price/spec.md) for the full wire format, message layouts, and the snapshot/delta recovery model.
