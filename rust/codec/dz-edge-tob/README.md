# dz-edge-tob

Top-of-Book message types. Framing, sequencing and control messages are in [`dz-edge-core`](../dz-edge-core/).

| Message | Type ID | Size | Port role |
|---|---|---|---|
| `Quote` | `0x03` | 60 bytes | `mktdata` |
| `Trade` | `0x04` | 52 bytes | `mktdata` |

`Quote` flags say which side changed: `QUOTE_BID_UPDATED`, `QUOTE_ASK_UPDATED`, `QUOTE_BID_GONE`, `QUOTE_ASK_GONE`. A zero price is not a substitute for the gone flag.

Prices and quantities are integers scaled by the exponents in the instrument's `InstrumentDefinition` — see [`dz-edge-refdata`](../dz-edge-refdata/) and `fixed_point` in core.

Offsets come from the [Top-of-Book spec](https://github.com/malbeclabs/edge-feed-spec/blob/main/top-of-book/spec.md) and are asserted against [`testdata/golden/`](../../../testdata/golden/).

`0x05` is not defined here: the spec's table steps from `0x04` to `0x06`, while the reference Go parser decodes it as `ChannelReset`. Nothing here emits it or rejects it.
