//! The marker feature: `kind = "poll"` resolves in a binary that links this
//! crate.
//!
//! # Why this is a test and not a comment
//!
//! `Kind::is_linked` answers *is that transport built into this binary?* from a
//! crate which — being the one every transport depends on — can depend on none
//! of them. The mechanism is that the *transport* crate turns the marker on:
//! this crate's manifest says
//! `dz-ingress-core = { features = ["poll", "tokio"] }`.
//!
//! Nothing in the code refers to that feature, so nothing in the code breaks if
//! it is dropped. What breaks is a publisher, at startup, with
//! `ConfigError::KindNotLinked` — telling an operator to redo a build that is
//! already correct, over a manifest line nobody would think to look at. An
//! **integration** test rather than a unit one because that is the question:
//! not whether the feature is set while compiling this crate's own sources, but
//! whether something that links this crate gets it.

use dz_ingress_core::Kind;

// The transport is what makes the token resolve, so linking it is the point of
// the test. Named rather than glob-imported so that a reader can see there is
// nothing else in here doing the work.
use dz_ingress_poll::PollInput;

#[test]
fn linking_this_crate_is_what_makes_the_poll_token_resolve() {
    assert!(
        Kind::Poll.is_linked(),
        "a binary that links dz-ingress-poll must answer that `poll` is built \
         into it; the marker comes from this crate's own dependency on \
         dz-ingress-core with `features = [\"poll\"]`"
    );
    assert!(
        matches!(Kind::resolve("poll"), Ok(Kind::Poll)),
        "and `[ingress] kind = \"poll\"` must therefore resolve rather than \
         being refused as a transport this build does not carry"
    );
    // The type that makes the claim true. Without this the test would pass in a
    // binary that links only the core with the feature turned on by hand, which
    // is the arrangement the marker exists to make impossible.
    assert_eq!(
        core::mem::size_of::<Option<Box<PollInput>>>(),
        core::mem::size_of::<usize>(),
        "the transport is a type in this link, not a name in a comment"
    );
}

#[test]
fn the_old_spelling_resolves_to_nothing_rather_than_to_this_transport() {
    // Two spellings for one transport, with a configuration management system
    // holding whichever was written first, is what the rename landed early to
    // avoid. This is the assertion from the other side of it: the crate that
    // finally makes `poll` real must not also make `rest` real.
    let error = Kind::resolve("rest").expect_err("`rest` names no transport");
    let message = error.to_string();
    assert!(message.contains("rest"), "{message}");
    assert!(message.contains("poll"), "{message}");
}
