# DZ Market-by-Order Parser

> Implements the [Market-by-Order Feed](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-order/spec.md) spec.

A standalone multicast subscriber that decodes DoubleZero Market-by-Order (DZ-MBO v0.1.0) wire-format frames and writes decoded market data records to a file or Unix socket.

Sibling to [topofbook-parser](../topofbook-parser/). Documentation will land as the implementation completes.

**Dual wire schema support.** `InstrumentDefinition` is decoded at schema versions 1 and 3, selected per frame from the frame header's Schema Version byte — not a build-time or CLI setting. v3 inserts `Source ID` (`u16`) after `Instrument ID` and widens `Symbol` from 16 to 64 bytes; all other fields are unchanged. There is no version 2: that layout was specified upstream and superseded before any publisher emitted it, so the accepted versions are the set `{1, 3}` and version 2 is rejected exactly like version 0. A frame whose declared Schema Version disagrees with the length its `InstrumentDefinition` body actually carries is counted malformed (as a frame-level `parse_errors_total{reason="truncated"}`) and the whole frame is skipped, not guessed at. `frames_total{port,schema_version}` (see [Metrics](#metrics)) is how to watch a publisher's v1-to-v3 cutover in production.

## Metrics

Namespace `dz_mbo_parser`. All metrics are exposed on `--metrics-addr` at `/metrics` when set.

| Metric | Type | Meaning |
|---|---|---|
| `dz_mbo_parser_ingress_packets_total{port}` | counter | UDP datagrams received, by port |
| `dz_mbo_parser_ingress_bytes_total{port}` | counter | UDP bytes received, by port |
| `dz_mbo_parser_parse_errors_total{port,reason}` | counter | Frame decode failures, by port and failure reason |
| `dz_mbo_parser_records_total{type}` | counter | Records emitted, by record type |
| `dz_mbo_parser_source_latency_seconds{port}` | histogram | Latency from block/venue source timestamp to kernel receive, by port (crosses validator and local clocks) |
| `dz_mbo_parser_send_latency_seconds{port}` | histogram | Latency from publisher egress send timestamp to kernel receive, by port |
| `dz_mbo_parser_socket_clients` | gauge | Currently connected Unix socket clients |
| `dz_mbo_parser_socket_client_drops_total{reason}` | counter | Slow socket clients dropped, by reason |
| `dz_mbo_parser_socket_records_sent_total` | counter | Records written to at least one socket client |
| `dz_mbo_parser_sink_write_errors_total` | counter | Output sink write failures |
| `dz_mbo_parser_frame_seq_gaps_total{port}` | counter | Frame-header sequence discontinuities detected (real UDP datagram loss events), by port |
| `dz_mbo_parser_frames_missing_total{port}` | counter | Total frames missing, summed across gap magnitudes in the frame-header sequence, by port |
| `dz_mbo_parser_frames_total{port,schema_version}` | counter | Successfully parsed frames, by port and wire Schema Version. The way to watch a publisher's v1-to-v3 cutover: `schema_version="3"` climbing while `schema_version="1"` goes flat, then to zero, is when the v1 decode path can be retired. `schema_version="2"` should never appear; a nonzero count there means a publisher is emitting a version this parser believes does not exist |
| `dz_mbo_parser_build_info{version,commit}` | gauge | Always `1`; labels carry build version/commit |
| `dz_mbo_parser_uptime_seconds` | gauge | Seconds since process start |
