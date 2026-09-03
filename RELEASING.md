# Depending on these crates, and cutting the version you depend on

The libraries here are consumed by other repositories — a venue's publisher, a
recorder host — and this is how. They are not on crates.io: a tag in this
repository is the release, and a consumer pins the tag.

## What a consumer writes

```toml
[dependencies]
dz-adapter-core = { git = "https://github.com/malbeclabs/edge-multicast-ref", tag = "v0.1.0" }

# Everything from the same tag. See below — this is the one rule that matters.
dz-publisher-runtime = { git = "https://github.com/malbeclabs/edge-multicast-ref", tag = "v0.1.0" }
dz-ingress-core = { git = "https://github.com/malbeclabs/edge-multicast-ref", tag = "v0.1.0", features = ["uds"] }
```

The workspace manifest lives in [`rust/`](rust/) rather than at the repository
root, which changes nothing for a consumer: cargo finds a package by name
anywhere in the repository it cloned.

Commit your `Cargo.lock`. The tag pins what this repository resolves to; your
lockfile pins everything underneath it, and the two together are what make a
build reproducible a month later.

### One tag for every crate, and why it is not a style preference

A consumer that pins `dz-adapter-core` at one tag and `dz-publisher-lowering` at
another gets **two copies of `dz-edge-core`** in its dependency graph — cargo
treats two git revisions as two different sources — and then `Scalar` from one is
not `Scalar` from the other. The compiler says so, in a message about a type
mismatch between two types with the same name and the same fields, and it is one
of the least legible errors cargo produces.

These crates are one workspace with one shared version for exactly this reason.
Pin the tag once, in one place:

```toml
[workspace.dependencies]
dz-adapter-core = { git = "https://github.com/malbeclabs/edge-multicast-ref", tag = "v0.1.0" }
dz-publisher-lowering = { git = "https://github.com/malbeclabs/edge-multicast-ref", tag = "v0.1.0" }
```

and let each crate write `dz-adapter-core = { workspace = true }`.

### The transport marker features

`[ingress] kind` refuses a transport the binary does not link, and the markers
that answer that question are turned on by whoever assembles the binary — see
[`rust/ingress/dz-ingress-core`](rust/ingress/dz-ingress-core/). So a consumer
depends on `dz-ingress-core` with the features for the transports it means to
allow: `features = ["websocket"]`, `["uds"]` for a replay run, and so on.
Without them the refusal at startup is correct and confusing.

### What is meant to be depended on

| Crate | For |
|---|---|
| `dz-adapter-core` | the boundary a venue implements — the one crate every venue compiles against |
| `dz-edge-core`, `dz-edge-tob`, `dz-edge-mbp`, `dz-edge-refdata` | the wire format |
| `dz-publisher-lowering`, `-egress`, `-refdata`, `-metrics` | the shared publisher pieces |
| `dz-publisher-runtime` | `run()`, the function a venue's `main` calls |
| `dz-ingress-core`, `dz-ingress-websocket` | the transports |
| `dz-recorder-core`, `-capture`, `-archive`, `-replay`, `-loss`, `-health` | a recorder built out of libraries |
| `dz-recorder-relower` | offline re-lowering, for the analysis tier |

`dz-recorder-e2e` is a test harness and `dz-recorder` is a binary. Neither is a
dependency anybody outside this repository wants.

## What a version promises

**Pre-1.0, so a minor release may break you.** `0.x` is what these crates are,
and it is honest: the boundary is young and has already gained methods twice
while being implemented for its first two venues. What a tag promises is that
*it* does not change — a tag is immutable and a build against `v0.1.0` resolves
the same bytes forever.

Read the release notes before moving a pin. A change that adds an `Event`
variant or a trait method is a break for an implementor even when it compiles
for everybody else, and those are called out.

## Cutting the next one

1. Land what the release contains, on `main`, green.
2. Bump `version` in `[workspace.package]` in [`rust/Cargo.toml`](rust/Cargo.toml).
   One version for every crate: see the two-copies trap above.
3. `cargo test --all`, `cargo test --all --release`,
   `cargo clippy --all-targets --all-features -- -D warnings`,
   `cargo fmt --all --check`, `./scripts/check-public-repo-rules.sh`. CI runs all
   of these, and a tag is worth cutting only from a commit that passed them.
4. Tag the merge commit and push the tag:

   ```sh
   git tag -a v0.2.0 -m "what changed, and what breaks an implementor"
   git push origin v0.2.0
   ```

5. Say what breaks. The tag message and the GitHub release are where a consumer
   who is about to move a pin looks, and *nothing breaks* is a sentence worth
   writing when it is true.

Do not move a tag. A consumer's lockfile records the revision the tag pointed at,
so moving one produces a build that cannot be reproduced and a lockfile that
disagrees with the tag it names.
