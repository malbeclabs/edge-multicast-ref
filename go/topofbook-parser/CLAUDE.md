# CLAUDE.md

## What this is

A standalone Go binary that subscribes to a DoubleZero Edge multicast group carrying DZ Top-of-Book (DZ-TOB v0.1.0) market data frames, decodes them, and writes structured records to a file or Unix socket. It lives in the `edge-multicast-ref` repo alongside Rust XDP/kernel-receiver implementations for Solana shreds — same multicast infrastructure, different feed type and parser.

This tool has no dependency on `doublezerod` or any DoubleZero library. It is a plain multicast UDP subscriber.

## Build and test

```bash
go build -o dz-topofbook-parser .
go test -v .
```

Single Go module, one external dep (`golang.org/x/net/ipv4` for multicast control messages). Everything is `package main` in a flat directory.

## How to run

```bash
./dz-topofbook-parser \
  --group 239.10.10.10 \
  --marketdata-port 7001 \
  --refdata-port 7002 \
  --format json \
  --output /tmp/topofbook.json \
  --interface doublezero1
```

Runs until SIGINT/SIGTERM. One feed per process. The `--interface` flag is important on multi-NIC hosts — without it, the IGMP join goes to the system default interface instead of the DoubleZero tunnel.

## Wire format: DZ-TOB v0.1.0

Fixed-layout, little-endian binary protocol. One UDP datagram = one frame. No varints, no length-prefixed strings, no schema negotiation. The decoder is a straight positional reader.

### Frame header (24 bytes)

```
magic          u16   0x445A ("DZ", on wire: 5A 44)
schema_ver     u8    1 or 2 — selects InstrumentDefinition's layout, see below
channel_id     u8
sequence       u64   monotonic per publisher
send_ts        u64   publisher wall clock, ns since epoch
msg_count      u8
reserved       u8
frame_length   u16
```

### Application message header (4 bytes per message)

```
msg_type       u8
msg_length     u8    (includes this 4-byte header)
flags          u16   (0x0001 = snapshot)
```

### Message types

| ID | Name | Body bytes | Channel | Notes |
|---:|---|---:|---|---|
| 0x01 | Heartbeat | 16 | either | Idle liveness |
| 0x02 | InstrumentDefinition | 76 (schema 1) / 124 (schema 2) | refdata | instrument_id → symbol, price/qty exponents |
| 0x03 | Quote (BBO) | 60 | marketdata | Best bid/ask per instrument |
| 0x04 | Trade | 52 | marketdata | Single trade |
| 0x05 | ChannelReset | 12 | either | Publisher startup — drop cached state |
| 0x06 | EndOfSession | 12 | either | Publisher shutdown |
| 0x07 | ManifestSummary | variable | refdata | Periodic instrument count |

### Price/quantity encoding

Fixed-point integers with per-instrument exponents from InstrumentDefinition. `float64(raw) * 10^exponent`. Example: raw=6743250, price_exponent=-2 → 67432.50. The parser converts internally; output records contain floats.

### Forward compatibility

Unknown message types are skipped, not rejected. Schema version is checked — unsupported versions are rejected cleanly. Both 1 and 2 are decoded: schema 2 widened InstrumentDefinition's symbol from 16 to 64 bytes, and a publisher rollout is staged, so both generations are on the wire at once.

## Architecture and source map

| File | What it does |
|---|---|
| `main.go` | CLI flags, signal handling, creates parser + sink + runner, runs until signal |
| `runner.go` | Two goroutines: one on the marketdata port, one on the refdata port. Reads UDP datagrams, hands them to the parser, writes records to the sink. A third goroutine logs a summary every 30s. Accepts `--interface` to resolve and pass a `*net.Interface` to `ListenMulticastUDP` instead of nil. |
| `parser.go` | `Parser` interface + `Record` type + parser registry. `Record` is the unit of output — a typed struct with `Type`, `Timestamp`, `ChannelID`, `SequenceNumber`, `InstrumentID`, `Symbol`, and a `Fields map[string]any` for type-specific data. |
| `topofbook_wire.go` | Wire format types and the `decodeTopOfBookFrame` function. `wireReader` is a small helper with sticky errors so the decoder can do a block of reads and check `err` once. Types are unexported (`topOfBookFrame`, `topOfBookQuote`, etc.) — only the parser uses them. |
| `topofbook.go` | `TopOfBookParser` implementation. Stateful: holds `map[instrumentID]*instrumentInfo` learned from InstrumentDefinition messages. Uses those to convert raw ints → floats on Quote/Trade. |
| `sink.go` | `OutputSink` interface + `NewSink` factory. Routes on format (json/csv) and path prefix (unix:// → socket, else file). |
| `sink_json.go` | JSON Lines file sink. |
| `sink_csv.go` | CSV sink with auto-inferred header row. Pivots the `Fields` map into stable columns. |
| `sink_socket.go` | Unix domain socket broadcast sink. Drop-on-slow-consumer: a stalled reader gets gaps, not backpressure. |

## Parser state machine

The parser must see an `InstrumentDefinition` for an instrument before it can decode that instrument's Quote or Trade messages (because the definition carries the price/qty exponents needed for fixed-point conversion).

### Cold-start buffering

When a Quote or Trade arrives for an unknown instrument:

1. It's stored in `buffer map[uint32]bufferedMsg` — one slot per instrument_id, most-recent-wins (newer overwrites older for the same instrument).
2. Buffer is capped at `maxBufferedInstruments = 1000`. Overflow drops with a WARN log (first time) then DEBUG.
3. When the InstrumentDefinition arrives for that instrument, `flushBuffer` scans the map and releases matching records immediately.

This means cold-start subscribers produce output at the first refdata cycle without needing to wait for the *next* quote.

### Logging

Key state transitions logged at INFO:

- `instrument defined` — new InstrumentDefinition learned (DEBUG for redefinitions)
- `buffering messages, awaiting instrument definition` — first buffer insert per instrument (DEBUG for subsequent)
- `flushed buffered messages` — records released from buffer, with flushed/remaining counts
- `parser producing records` — first non-empty parse result (once per run)
- `runner summary` — every 30s: records_written, buffered, instruments_known
- `buffer full, dropping message` — WARN first time, then DEBUG

## Publisher counterpart

The publisher side of this wire format is [packethog/order_book_server](https://github.com/packethog/order_book_server) on the `binary-multicast-protocol` branch. It's a Rust program that reconstructs order books from a venue's native event stream and emits DZ-TOB frames onto a multicast group. The two implementations share no code — the wire format spec is the contract.

A Wireshark Lua dissector for the wire format is at `order_book_server/spec/dz_topofbook.lua` in that repo.

## End-to-end system context

This tool is part of a proof-of-concept for permissionless crypto market data over DoubleZero Edge:

1. **Permissionless node** — a non-validating venue node reads blocks from the gossip network and writes event files to disk. No data license required.
2. **Publisher** (order_book_server) — reconstructs the order book, emits DZ-TOB frames onto a multicast group via the DoubleZero tunnel.
3. **DoubleZero Edge** — multicast transport. DZDs replicate at the switch level over dedicated fiber.
4. **This tool** — subscriber. Decodes frames, writes records.
5. **Trader bots** — connect to the Unix socket sink and consume the feed.

Per-venue runbooks and the end-to-end POC writeup live in the `malbeclabs/doublezero` and `malbeclabs/infra` repos.

## Style

- Go. No codegen, no third-party frameworks. `encoding/binary` for wire decode, `log/slog` for structured logging, `flag` for CLI.
- `package main` — flat directory, single binary. If it ever needs to be importable as a library, split into a sub-package + `cmd/` directory.
- Tests use the standard `testing` package. No testify. Synthetic wire-format bytes are built by test helpers, not fixtures.
