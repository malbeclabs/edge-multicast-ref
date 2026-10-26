# dz-edge-refdata

Reference-data message types: the instrument set a subscriber needs before market data means anything.

| Message | Type ID | Port role |
|---|---|---|
| `InstrumentDefinition` | `0x02` | `refdata` |
| `ManifestSummary` | `0x07` | `refdata` |
| `StrikeInterval` | `0x09` | `refdata` |

`InstrumentDefinition` carries an instrument's identity and the price and quantity exponents its market-data values are scaled by. `ManifestSummary` describes the published set, so a subscriber can tell whether it holds all of it. `StrikeInterval` carries the strike of a contract that pays on a threshold or a range, as a lower bound, an upper bound or both; it is pending until the venue fixes it.

There is no query interface. A publisher retransmits the published set on a cadence, paced across the cycle rather than burst — the cycle period is a maximum on the gap between retransmissions of any one definition, not a lap target.

`ManifestSummary` is also the refdata port's liveness signal, since `Heartbeat` is `mktdata`-only.

## The strike

A `StrikeInterval` follows its instrument's `InstrumentDefinition` in the same datagram, and is sent nowhere else. This crate encodes and decodes the message. It does not hold that rule: nothing here fills a datagram.

`validate` refuses fields that disagree with `Bound Flags` and an interval no value is inside, so a builder refuses to push one. `decode` does not. The specification gives a subscriber one rule for every `Bound Flags` value and tells it to ignore the reserved bits, so a body that breaks a publisher rule still decodes, and `defect` says what is wrong with it.

## Schema versions

`InstrumentDefinition` decodes at versions 1 and 3, chosen per datagram from the header's Schema Version byte. Version 3 inserts `Source ID` (`u16`) after `Instrument ID` and widens `Symbol` from 16 to 64 bytes. Version 2 was superseded before any publisher emitted it and is rejected like version 0. Encoding is version 3 only.

Offsets come from the [reference-data supplement](https://github.com/malbeclabs/edge-feed-spec/blob/main/reference-data/spec.md) and are asserted against [`testdata/golden/`](../../../testdata/golden/), which covers both versions.
