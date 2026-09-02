# Rust in this repository

Two unrelated bodies of Rust, not part of the same build.

| Path | | In the workspace? |
|---|---|---|
| [`codec/`](codec/), [`adapter/`](adapter/), [`ingress/`](ingress/), [`publisher/`](publisher/), [`recorder/`](recorder/) | Libraries a venue publisher and the recorder are built from | Yes |
| `kernel-receiver/`, `xdp-receiver/` | Standalone shred receivers | No — `exclude`d |

The receivers are binaries with their own dependency trees; see [kernel-receiver](kernel-receiver/) and [xdp-receiver](xdp-receiver/). They are `exclude`d rather than merely absent from `members` so `cargo metadata` here does not try to resolve them.

## Workspace

| | |
|---|---|
| [`codec/`](codec/) | The wire format ([README](codec/README.md)) |
| [`adapter/`](adapter/) | The boundary a venue implements ([README](adapter/README.md)) |
| [`ingress/`](ingress/) | The transports that drive it ([README](ingress/README.md)) |
| [`publisher/`](publisher/) | Everything else a publisher needs ([README](publisher/README.md)) |
| [`recorder/`](recorder/) | Capture, archive and the analysis tier ([README](recorder/README.md)) |

```sh
cd rust
cargo test --all
cargo test --all --release      # debug_assert! differs; CI runs both
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all --check
```

CI runs all four on every pull request, plus `scripts/check-public-repo-rules.sh`. There is no path filter, so a change anywhere runs the whole workspace — which is what keeps a codec change from breaking a publisher silently.

`Cargo.lock` is tracked. MSRV and the `prometheus` version are pinned at the workspace level.

## Conventions

Wire behaviour is verified against [`testdata/golden/`](../testdata/golden/), which the Go implementation asserts against too — that is the cross-language contract, not agreement between our own encoder and decoder.

On a publisher's send path a countable error beats a panic. Vocabulary follows [GLOSSARY.md](https://github.com/malbeclabs/edge-feed-spec/blob/main/GLOSSARY.md): `datagram` not `frame`, `era` not `epoch`, `channel` only for the `Channel ID` shard.
