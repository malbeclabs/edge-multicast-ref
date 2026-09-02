# Rust in this repository

Two unrelated bodies of Rust, not part of the same build.

| Path | | In the workspace? |
|---|---|---|
| [`codec/`](codec/), [`publisher/`](publisher/) | Libraries a venue publisher is built from | Yes |
| [`recorder/`](recorder/) | The recorder: keeps the bytes a host received, with its own losses inside the archive | Yes |
| `kernel-receiver/`, `xdp-receiver/` | Standalone shred receivers | No — `exclude`d |

The receivers are binaries with their own dependency trees; see [kernel-receiver](kernel-receiver/) and [xdp-receiver](xdp-receiver/). They are `exclude`d rather than merely absent from `members` so `cargo metadata` here does not try to resolve them.

## Workspace

| | |
|---|---|
| [`codec/`](codec/) | The wire format ([README](codec/README.md)) |
| [`publisher/`](publisher/) | Everything else a publisher needs ([README](publisher/README.md)) |
| [`recorder/`](recorder/) | Eight crates, from the capture to the binary ([README](recorder/README.md)) |

```sh
cd rust
cargo test --all
cargo test --all --release      # debug_assert! differs; CI runs both
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all --check
```

CI runs all four on every pull request — unfiltered, because a required check that never reports blocks a pull request forever. Three further jobs gate what the default build cannot reach: `afpacket` (needs `libpcap-dev`), `e2e` (needs a runner that can deliver multicast to itself) and `conformance` (builds edge-feed-spec's own rule set from a pinned revision and applies it to what this repository produces).

`Cargo.lock` is tracked. MSRV and the `prometheus` version are pinned at the workspace level.

## Conventions

Wire behaviour is verified against [`testdata/golden/`](../testdata/golden/), which the Go implementation asserts against too — that is the cross-language contract, not agreement between our own encoder and decoder.

On a publisher's send path a countable error beats a panic. Vocabulary follows [GLOSSARY.md](https://github.com/malbeclabs/edge-feed-spec/blob/main/GLOSSARY.md): `datagram` not `frame`, `era` not `epoch`, `channel` only for the `Channel ID` shard.
