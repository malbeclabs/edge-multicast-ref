//! The host triple, for the dependency test's `--filter-platform`.
//!
//! `TARGET` is set for a build script and for nothing else, so this is the only
//! place a test can learn which platform it is being asked about. Without it
//! the test would have to name a triple, and a triple written down is one that
//! is wrong on the next machine.

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!(
        "cargo:rustc-env=HOST_TARGET={}",
        std::env::var("TARGET").expect("cargo sets TARGET for a build script")
    );
}
