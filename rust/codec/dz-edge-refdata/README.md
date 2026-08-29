# dz-edge-refdata

Reference-data message types: the instrument set a subscriber needs before market data means anything.

| Message | Type ID | Port role |
|---|---|---|
| `InstrumentDefinition` | `0x02` | `refdata` |
| `ManifestSummary` | `0x07` | `refdata` |

`InstrumentDefinition` carries an instrument's identity and the price and quantity exponents its market-data values are scaled by. `ManifestSummary` describes the published set, so a subscriber can tell whether it holds all of it.

There is no query interface. A publisher retransmits the published set on a cadence, paced across the cycle rather than burst — the cycle period is a maximum on the gap between retransmissions of any one definition, not a lap target.

`ManifestSummary` is also the refdata port's liveness signal, since `Heartbeat` is `mktdata`-only.

## Schema versions

`InstrumentDefinition` decodes at versions 1 and 3, chosen per datagram from the header's Schema Version byte. Version 3 inserts `Source ID` (`u16`) after `Instrument ID` and widens `Symbol` from 16 to 64 bytes. Version 2 was superseded before any publisher emitted it and is rejected like version 0. Encoding is version 3 only.

Offsets come from the [reference-data supplement](https://github.com/malbeclabs/edge-feed-spec/blob/main/reference-data/spec.md) and are asserted against [`testdata/golden/`](../../../testdata/golden/), which covers both versions.
