# CLAUDE.md

## What this is

A standalone Go binary that subscribes to a DoubleZero Edge multicast group carrying DZ Top-of-Book (DZ-TOB v0.1.0) market data datagrams, decodes them, and writes structured records to a file or Unix socket. It lives in the `edge-multicast-ref` repo alongside Rust XDP/kernel-receiver implementations for Solana shreds — same multicast infrastructure, different feed type and parser.

This tool has no dependency on `doublezerod` or any DoubleZero library. It is a plain multicast UDP subscriber.

## Build and test

```bash
go build -o dz-topofbook-parser .
go test -v ./...
```

`tob/golden_test.go` decodes the five top-of-book and reference-data vectors in
`testdata/golden` — Quote, Trade, ManifestSummary, and InstrumentDefinition in
both schema generations — and asserts every field against the values
`manifest.json` records. Those vectors were transcribed by hand from the
`edge-feed-spec` field tables, so they bind this decoder to the wire rather than
to a fixture written from the same reading of the spec the decoder was.

One Go module in the `go/` workspace. The only external dep is `prometheus/client_golang` for `/metrics`; the sink transport and the UDP receive path come from the `go/internal` workspace member. The module root is `package main`; the wire decoder and the parser it drives are `package tob` under `tob/`, and the root `parser.go` re-exports `tob.Record`, `tob.PacketMeta` and `tob.Parser` so the rest of `main` names them unqualified.

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

Fixed-layout, little-endian binary protocol. One UDP datagram carries a 24-byte header and N messages. No varints, no length-prefixed strings, no schema negotiation. The decoder is a straight positional reader.

### Datagram header (24 bytes)

```
magic          u16   0x445A ("DZ", on wire: 5A 44)
schema_ver     u8    1 or 3 (no version 2; see InstrumentDefinition below)
channel_id     u8
sequence       u64   monotonic per publisher
send_ts        u64   publisher wall clock, ns since the Unix epoch
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

| ID | Name | Message bytes | Channel | Notes |
|---:|---|---:|---|---|
| 0x01 | Heartbeat | 16 | either | Idle liveness |
| 0x02 | InstrumentDefinition | 80 (v1) / 130 (v3) | refdata | instrument_id → source_id, symbol, price/qty exponents. Both lengths are exact, matching the Schema Version in the datagram header — a datagram whose declared version disagrees with the message length it actually carries is rejected, not guessed at. There is no version 2 |
| 0x03 | Quote (BBO) | 60 | marketdata | Best bid/ask per instrument |
| 0x04 | Trade | 52 | mktdata | Single trade |
| 0x05 | ChannelReset | 12 | either | Publisher startup — drop cached state |
| 0x06 | EndOfSession | 12 | either | Publisher shutdown |
| 0x07 | ManifestSummary | 24 | refdata | Periodic summary of the published set: valid, manifest_seq, instrument_count |

Every length in the table above is a whole message, `msg_length` included, and
every one of them is exact. A known message type whose `msg_length` disagrees
with its size is refused and counted as `parse_errors_total{reason="truncated"}`,
whether it came up short or ran long; an over-long body is never decoded with
its tail ignored. Subtract the 4-byte message header for the body length a
decoder reads (ManifestSummary: 24 on the wire, 20 of body).

### Price/quantity encoding

Fixed-point integers with per-instrument exponents from InstrumentDefinition. `float64(raw) * 10^exponent`. Example: raw=6743250, price_exponent=-2 → 67432.50. The parser converts internally; output records contain floats.

### Forward compatibility

Unknown message types are skipped, not rejected. Schema version is checked — unsupported versions are rejected cleanly.

## Architecture and file map

| File | What it does |
|---|---|
| `main.go` | CLI flags, signal handling, creates parser + sink + runner, runs until signal |
| `runner.go` | Two goroutines: one on the marketdata port, one on the refdata port. Reads UDP datagrams, hands them to the parser, writes records to the sink. A third goroutine logs a summary every 30s. Accepts `--interface` to resolve and pass a `*net.Interface` to `ListenMulticastUDP` instead of nil. |
| `parser.go` | Parser registry (`NewParser`, `RegisteredParsers`) and the aliases that re-export `tob.Record`, `tob.PacketMeta` and `tob.Parser` into `main`. |
| `tob/parser.go` | `Parser` interface + `Record` type. `Record` is the unit of output — a typed struct with `Type`, `Timestamp`, `ChannelID`, `SequenceNumber`, `InstrumentID`, `Symbol`, and a `Fields map[string]any` for type-specific data. |
| `tob/topofbook_wire.go` | Wire format types and the `decodeTopOfBookDatagram` function. `wireReader` is a small helper with sticky errors so the decoder can do a block of reads and check `err` once. Types are unexported (`topOfBookDatagram`, `topOfBookQuote`, etc.) — only the parser uses them. |
| `tob/topofbook.go` | `TopOfBookParser` implementation. Stateful: holds `map[instrumentID]*instrumentInfo` learned from InstrumentDefinition messages. Uses those to convert raw ints → floats on Quote/Trade. |
| `sink.go` | `OutputSink` + `JSONFileSink` over `go/internal/sink`, and the `NewSink` factory. Routes on format (json/csv) and path prefix (unix:// → socket, else file), and the format picks which per-client encoder a socket sink is handed. |
| `sink_csv.go` | CSV file sink and CSV socket writer, sharing the quote/trade column layout. Pivots the `Fields` map into stable columns. |

The JSON Lines file sink and the Unix domain socket broadcast sink live in
`go/internal/sink`, shared with the market-by-order and market-by-price
parsers and generic over each feed's own `Record`. The socket sink is
drop-on-slow-consumer: a stalled reader gets gaps, not backpressure. The UDP
receive path with its kernel receive timestamp is `go/internal/udp`.

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

The publisher side of this wire format is [packethog/order_book_server](https://github.com/packethog/order_book_server) on the `binary-multicast-protocol` branch. It's a Rust program that reconstructs order books from a venue's native event stream and emits DZ-TOB datagrams onto a multicast group. The two implementations share no code — the wire format spec is the contract.

A Wireshark Lua dissector for the wire format is at `order_book_server/spec/dz_topofbook.lua` in that repo.

## End-to-end system context

This tool is part of a proof-of-concept for permissionless crypto market data over DoubleZero Edge:

1. **Permissionless node** — a non-validating venue node reads blocks from the gossip network and writes event files to disk. No data license required.
2. **Publisher** (order_book_server) — reconstructs the order book, emits DZ-TOB datagrams onto a multicast group via the DoubleZero tunnel.
3. **DoubleZero Edge** — multicast transport. DZDs replicate at the switch level over dedicated fiber.
4. **This tool** — subscriber. Decodes datagrams, writes records.
5. **Trader bots** — connect to the Unix socket sink and consume the feed.

Per-venue runbooks and the end-to-end POC writeup live in the `malbeclabs/doublezero` and `malbeclabs/infra` repos.

## Style

- Go. No codegen, no third-party frameworks. `encoding/binary` for wire decode, `log/slog` for structured logging, `flag` for CLI.
- Single binary. The module root is `package main`; the wire format and the parser state machine are the `tob` sub-package, and a new shared type belongs with the layer it serves.
- Tests use the standard `testing` package. No testify. Synthetic wire-format bytes are built by test helpers rather than fixtures checked in beside them, with one deliberate exception: `tob/golden_test.go` reads the shared vectors in `testdata/golden`, which are the cross-language contract and carry their force precisely because this package did not write them.
