# dz-edge-core

What every feed in the DoubleZero Edge family shares: datagram framing, the message header, sequencing, the receive-side walk, decimal conversion.

## Encoding

```rust
use dz_edge_core::{ChannelSequence, DatagramBuilder, PortRole, ResetCount};

let mut sequence = ChannelSequence::new(channel_id, ResetCount(reset_count));
let mut builder = DatagramBuilder::new(Feed::MAGIC, &mut sequence, mtu, PortRole::Mktdata);

builder.push(&quote)?;
builder.push(&trade)?;

if let Some(bytes) = builder.finish(send_timestamp_ns) {
    socket.send(&bytes)?;
}
```

- `finish` returns `None` on an empty builder — `Message Count` has range 1–255, so an empty datagram is not representable.
- `finish` takes the send timestamp, because the field means the instant the datagram left the host.
- `ChannelSequence` carries `Channel ID`, the sequence series and `Reset Count` together, with private fields; `ResetCount` is a newtype so the two `u8`s cannot be transposed.
- The builder is bound to a port role. A message declares the roles its spec permits and the builder returns `EncodeError::WrongPortRole` for others. The snapshot flag follows from the role.

## Decoding

```rust
let datagram = Datagram::decode(buf, Feed::MAGIC)?;
for message in datagram.messages() {
    match message.type_id() { /* ... */ }
}
```

The walk is infallible once the header validates and yields unknown type IDs rather than rejecting them. It reads exactly `Message Count` messages and ignores trailing bytes, matching the Go implementation.

## Decimal conversion

`fixed_point::parse_signed` and `parse_unsigned` convert a decimal string to a scaled integer exactly or not at all, with no floating point.

`ScaleError` is `TooPrecise { beyond }`, `Malformed` or `Overflow`. `beyond` is the distance to the last non-zero discarded digit — how far the configured exponent is off. Trailing zeros past the exponent are not precision.

## Modules

| | |
|---|---|
| `datagram` | `DatagramBuilder`, `DatagramHeader` |
| `walk` | `Datagram`, `Messages`, `MessageRef` |
| `channel` | `ChannelSequence`, `ResetCount` |
| `message` | `AppMessage` |
| `port_role` | `PortRole` — `mktdata`, `refdata`, `snapshot` |
| `fixed_point` | Decimal to scaled integer |
| `ascii` | `pad_ascii`, `Fit` for fixed-width symbol fields |
| `heartbeat`, `end_of_session` | Control messages every feed carries |
| `constants` | Sizes, schema versions, type IDs, flags |
| `error`, `encode_error` | `DecodeError`, `EncodeError` |

Encodes schema version 3; decodes 1 and 3. Version 2 was superseded before any publisher emitted it and is rejected like version 0.
