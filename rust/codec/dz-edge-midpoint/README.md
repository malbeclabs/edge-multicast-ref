# dz-edge-midpoint

Midpoint message type. Framing, sequencing and control messages are in [`dz-edge-core`](../dz-edge-core/). `InstrumentDefinition` and `ManifestSummary` are in [`dz-edge-refdata`](../dz-edge-refdata/).

| Message | Type ID | Size | Port role |
|---|---|---|---|
| `Midpoint` | `0x03` | 48 bytes | `mktdata` |

Targets midpoint `3.0.0` (`Schema Version = 3`). The `1.x` layout (64-byte `InstrumentDefinition`, 40-byte `Midpoint`) is not implemented.

`Method` says how the price was computed. Book methods: `METHOD_BBO_MID`, `METHOD_MICRO_PRICE`, `METHOD_WEIGHTED_MID`. Trade methods: `METHOD_LAST_TRADE_FALLBACK`, `METHOD_VWAP`. For VWAP, `window_ms` and `volume` describe the window `(input_timestamp_ns − window, input_timestamp_ns]`; `Midpoint::window_ns` returns it.

`encode_into` and `decode` do not apply the field rules. A publisher calls `Midpoint::check` before it pushes.

`price` carries the instrument's Price Exponent and `volume` its Qty Exponent. This crate does no scaling.

Offsets come from the [Midpoint spec](https://github.com/malbeclabs/edge-feed-spec/blob/main/midpoint/spec.md) and are asserted against [`testdata/golden/`](../../../testdata/golden/).
