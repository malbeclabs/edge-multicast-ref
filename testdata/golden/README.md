# Golden vectors

One canonical byte vector per message type per schema version. These are the
cross-language contract: the Rust encoders, the Rust decoders, the Go decoders,
the conformance tool and the Wireshark dissectors must all reproduce them.

The bytes were transcribed by hand from the field tables in `edge-feed-spec`,
not captured from an encoder. That is the point — a vector generated from the
code under test proves only that the code agrees with itself.

`manifest.json` carries each vector's field values, so an implementation in any
language can check both directions without re-reading the specs.

## Vectors

| File | Message | Type ID | Size | Schema |
| --- | --- | --- | --- | --- |
| `quote-v3.bin` | Quote | `0x03` | 60 | 3 |
| `trade-v3.bin` | Trade | `0x04` | 52 | 3 |
| `instrument-definition-v3.bin` | InstrumentDefinition | `0x02` | 130 | 3 |
| `instrument-definition-v1.bin` | InstrumentDefinition | `0x02` | 80 | 1 (decode-only) |
| `manifest-summary-v3.bin` | ManifestSummary | `0x07` | 24 | 3 |

`instrument-definition-v1.bin` and `instrument-definition-v3.bin` carry the
same logical field values apart from `source_id`, which schema 1 has no field
for and which must decode as 0. `InstrumentDefinition` is the only message in
this family whose layout changed between schema generations, so it is the one
most likely to drift between implementations, and both generations are
represented here for that reason.

**Changing a vector is a wire change.** Justify it against `edge-feed-spec` and
record the spec revision in `manifest.json`. Never edit a vector to make a
failing test pass.
