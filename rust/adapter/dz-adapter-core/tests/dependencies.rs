//! The constraint this crate exists under, as a test.
//!
//! Every entry in `[dependencies]` here is inherited by every venue repository
//! that implements the trait. A venue pinned to our async runtime's minor
//! version, or to our Prometheus client's, is a version conflict we caused by
//! naming one in the crate they had no choice but to link.
//!
//! So the list is checked rather than agreed to. Adding an entry means editing
//! this file, and editing this file is the review that a dependency every venue
//! inherits deserves.
//!
//! **What it checks and what it does not.** It reads this crate's own manifest,
//! so it covers direct dependencies exactly. The transitive closure is pinned a
//! second way: the one dependency allowed is `thiserror`, which has none of its
//! own beyond its proc-macro half, so the closure is the list. A test that
//! walked the resolved graph would need a JSON parser, which would be a
//! dev-dependency added to check that dependencies are not added.
//!
//! `[dev-dependencies]` is deliberately not checked. Those are not inherited by
//! a consumer, and they are how the enumerations this crate mirrors are held to
//! the originals instead of by hand.

/// Exactly what a venue is allowed to inherit.
const ALLOWED: [&str; 1] = ["thiserror"];

#[test]
fn the_crate_inherits_nothing_but_the_allowed_set() {
    let manifest = include_str!("../Cargo.toml");
    let declared = dependencies_section(manifest);

    for name in &declared {
        assert!(
            ALLOWED.contains(&name.as_str()),
            "`{name}` was added to [dependencies]. Every venue repository that \
             implements this trait now links it. If that is intended, add it to \
             ALLOWED in this file and say in the commit message why a venue \
             should inherit it."
        );
    }

    for allowed in ALLOWED {
        assert!(
            declared.iter().any(|name| name == allowed),
            "`{allowed}` is allowed but no longer declared: this test is now \
             guarding a list that does not describe the crate"
        );
    }
}

/// The dependency names declared in `[dependencies]`, and only that section.
///
/// A hand-rolled reader rather than a TOML parser, because a parser here would
/// be a dependency added to the crate whose dependencies this test exists to
/// hold at one. It reads the shape this manifest is actually written in — one
/// `name = ...` per line under one table header — and would rather see too much
/// than too little: an entry it failed to recognise would pass the test
/// silently, so the section boundaries are matched exactly and anything inside
/// them that names a key is a dependency.
fn dependencies_section(manifest: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut inside = false;

    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            inside = line == "[dependencies]";
            continue;
        }
        if !inside || line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, _)) = line.split_once('=') else {
            continue;
        };
        names.push(key.trim().trim_matches('"').to_string());
    }

    names
}

#[test]
fn the_reader_finds_a_dependency_it_is_shown() {
    // The failure mode this test exists for is the reader silently finding
    // nothing, which would pass the test above no matter what was added.
    let manifest = "\
[package]\n\
name = \"x\"\n\
\n\
[dependencies]\n\
thiserror = { workspace = true }\n\
tokio = \"1\"\n\
\n\
[dev-dependencies]\n\
not-inherited = \"1\"\n";

    let found = dependencies_section(manifest);
    assert_eq!(found, vec!["thiserror".to_string(), "tokio".to_string()]);
}
