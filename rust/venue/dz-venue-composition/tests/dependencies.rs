//! The reason this crate exists, as a test.
//!
//! Composing a venue used to mean linking `dz-publisher-runtime`, and through
//! it the egress, the transmitters, the era store and the reference-data
//! registry — **in order to publish nothing.** Moving the composition out is
//! what fixes that, and a dependency shape is not observable from behaviour: a
//! move that left the three types where they were and re-exported them from
//! here would pass every behavioural test in the workspace. So the shape is the
//! assertion.
//!
//! # Why the resolved graph and not the manifest
//!
//! `dz-adapter-core`'s own `tests/dependencies.rs` reads its `[dependencies]`
//! section, and says why: its one allowed entry has no dependencies of its own,
//! so the section is the closure. That is not true here — this crate has ten
//! direct entries and each has a tree — so what has to be checked is the
//! closure itself. `cargo metadata` is where cargo has already computed it, and
//! `--offline` is what makes reading it a test rather than a network call.
//!
//! # Which edges count
//!
//! Normal and build edges, never `dev`. `cargo metadata` resolves
//! dev-dependencies for every workspace member, and several of the boundary
//! crates below this one take a publisher crate as a dev-dependency in order to
//! hold a mirrored enumeration to its original — `dz-ingress-core` on
//! `dz-publisher-metrics` is the one to expect. None of that is linked into
//! anything that depends on this crate, so following those edges would report a
//! dependency nobody has.

use std::collections::BTreeSet;
use std::process::Command;

/// This crate.
const ROOT: &str = "dz-venue-composition";

/// What a process that composes a venue must not be made to link, and what each
/// name is.
const FORBIDDEN: [(&str, &str); 6] = [
    (
        "dz-publisher-egress",
        "the transmitters, the datagram builder and the era store",
    ),
    ("dz-publisher-refdata", "the reference-data registry"),
    (
        "dz-publisher-runtime",
        "the crate this composition was moved out of - re-exporting from here \
         instead of moving is the non-move this assertion exists to catch",
    ),
    ("dz-publisher-lowering", "the publisher's lowering"),
    (
        "dz-publisher-metrics",
        "the normative metric set and, with it, the exposition server",
    ),
    (
        "tiny_http",
        "an HTTP server: a process that composes a venue in order to record \
         serves no scrape",
    ),
];

/// What must be in the closure, so that a traversal which silently found
/// nothing fails instead of passing.
///
/// `prometheus` is here deliberately and is not an oversight: `Venue::collectors`
/// is a Prometheus type by construction, and the client is what this crate pays
/// in order not to pay the exposition server. If it ever leaves, this test
/// should be the thing that says so.
const EXPECTED: [&str; 4] = [
    "dz-adapter-core",
    "dz-ingress-core",
    "dz-adapter-uds",
    "prometheus",
];

#[test]
fn nothing_that_publishes_is_in_this_crates_closure() {
    let closure = linked_closure();

    for (name, what) in FORBIDDEN {
        assert!(
            !closure.contains(name),
            "`{name}` is in `{ROOT}`'s resolved dependencies. That is {what}, and \
             the whole reason this crate exists is that composing a venue must \
             not link it: a process that records would be linking the publisher \
             in order to publish nothing. Found closure: {closure:?}"
        );
    }
}

#[test]
fn the_closure_is_the_one_this_crate_actually_has() {
    // The failure this exists for is the traversal finding nothing, which would
    // pass the test above no matter what was added.
    let closure = linked_closure();

    for name in EXPECTED {
        assert!(
            closure.contains(name),
            "`{name}` is a direct dependency of this crate and is not in the \
             closure the traversal found: the test above is asserting over an \
             empty set. Found: {closure:?}"
        );
    }
}

/// Every crate linked into anything that depends on this one, by name.
///
/// The names in this crate's linked closure, from `cargo tree`.
///
/// # Why `cargo tree` and not a hand-walked graph
///
/// The first version of this test walked `cargo metadata`'s resolve nodes by
/// hand — a hundred and thirty lines of breadth-first search whose own
/// correctness nothing checked. `cargo tree` answers the same question, cargo
/// computes the closure rather than this file, and the three flags carry the
/// three decisions:
///
/// - `-e normal` drops dev edges. Several boundary crates take a publisher
///   crate as a dev-dependency to hold a mirrored enumeration in step, and
///   following those would report a dependency nobody actually has.
/// - `--target` asks about one platform. Without it the answer is every
///   platform's, which on a Linux job names packages that were never fetched.
/// - `--prefix none` makes each line a package rather than a tree drawing.
///
/// `--offline` stays: a dependency graph is a fact about the committed
/// lockfile, so reading it must not be able to reach out and change one.
fn linked_closure() -> BTreeSet<String> {
    let output = Command::new(env!("CARGO"))
        .args([
            "tree",
            "--offline",
            "--package",
            ROOT,
            "--edges",
            "normal",
            "--prefix",
            "none",
            "--target",
            env!("BUILD_TARGET"),
            "--manifest-path",
        ])
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .output()
        .expect("`cargo tree` runs");

    assert!(
        output.status.success(),
        "`cargo tree` failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // `name version (path) (*)` — the name is the first field, and a line that
    // repeats a subtree already shown is marked and carries the same name.
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .collect()
}
