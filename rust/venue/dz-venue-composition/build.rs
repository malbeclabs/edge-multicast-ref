//! The **target** triple, for the dependency test's `--filter-platform`.
//!
//! The target and not the host, which cargo supplies separately as `HOST`: the
//! question the test asks is what the graph resolves to for the platform this
//! build is *for*, and under cross-compilation that is the target. They are the
//! same triple on every build this repository does today, which is exactly why
//! naming the wrong one would go unnoticed.
//!
//! `TARGET` is set for a build script and for nothing else, so this is the only
//! place a test can learn it. Without it the test would have to name a triple,
//! and a triple written down is one that is wrong on the next machine.

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!(
        "cargo:rustc-env=BUILD_TARGET={}",
        std::env::var("TARGET").expect("cargo sets TARGET for a build script")
    );
}
