# Codec crates

The DoubleZero Edge wire format. One crate for what every feed shares, one per feed for its own messages.

| Crate | |
|---|---|
| [`dz-edge-core`](dz-edge-core/) | Datagram and message headers, sequencing, receive-side walk, decimal conversion, control messages |
| [`dz-edge-tob`](dz-edge-tob/) | Top-of-Book: `Quote` (`0x03`), `Trade` (`0x04`) |
| [`dz-edge-refdata`](dz-edge-refdata/) | Reference data: `InstrumentDefinition` (`0x02`), `ManifestSummary` (`0x07`) |

The format is specified in [edge-feed-spec](https://github.com/malbeclabs/edge-feed-spec); where these crates disagree with it, they have a bug.

## Feed crates and core

A feed crate defines message types only. It reaches core through [`AppMessage`](dz-edge-core/src/message.rs):

```rust
pub trait AppMessage {
    const TYPE_ID: u8;
    const SIZE: usize;
    const PORT_ROLES: &'static [PortRole];
    fn encode_into(&self, dst: &mut [u8]);
    fn stamp_channel_id(dst: &mut [u8], channel_id: u8);
}
```

`stamp_channel_id` is required rather than defaulted: the offset differs per message, so a new type cannot silently omit it.

Type ID space is per feed. `0x05` is reserved on some feeds and decoded as `ChannelReset` on Top-of-Book, so core constrains no type ID.

## Adding a feed

1. New crate under `codec/`, added to workspace `members`.
2. One type per message implementing `AppMessage`, offsets from that feed's spec table.
3. Golden vectors in [`testdata/golden/`](../../testdata/golden/), asserted from both this crate and Go.
4. New message types need entries in `EgressMessageType` in [`dz-publisher-metrics`](../publisher/dz-publisher-metrics/); a unit test there fails until they do.
