//! What this build says about itself, and what it says when it does not know.
//!
//! The same rule the recorder's own identity module states, for the same reason:
//! the commit is a property of the build, so it has to be fixed when the build
//! happens. A binary that asked `git` at startup would report the commit of
//! whatever tree it was standing in, which on a recorder host is no tree at all.

/// The version in `Cargo.toml`, which is the one thing about a build that is
/// always knowable from the source alone.
pub const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");

/// What a build that cannot know its commit reports instead.
///
/// A literal, and never an empty string: an empty field reads as one somebody
/// forgot to fill in, while this one reads as what it is.
pub const UNKNOWN_COMMIT: &str = "unknown";

/// The commit this binary was built from, or [`UNKNOWN_COMMIT`].
#[must_use]
pub const fn build_commit() -> &'static str {
    commit_or_unknown(option_env!("DZ_LOADER_BUILD_COMMIT"))
}

/// The decision, split from the environment read so that it is testable.
///
/// `option_env!` resolves at compile time, so a test of [`build_commit`] can
/// only ever see the environment the test binary itself was built in — which is
/// why the empty case has to be tested through this.
#[must_use]
pub const fn commit_or_unknown(raw: Option<&'static str>) -> &'static str {
    match raw {
        // Empty is *set and unknown*, not set: a pipeline whose `git rev-parse`
        // failed exports the variable with nothing in it.
        Some(commit) if !commit.is_empty() => commit,
        _ => UNKNOWN_COMMIT,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_build_commit_set_to_nothing_is_not_a_commit() {
        assert_eq!(super::commit_or_unknown(Some("")), super::UNKNOWN_COMMIT);
        assert_eq!(super::commit_or_unknown(None), super::UNKNOWN_COMMIT);
        assert_eq!(super::commit_or_unknown(Some("0f1e2d3")), "0f1e2d3");
    }

    #[test]
    fn a_build_that_was_not_told_its_commit_says_it_does_not_know() {
        let commit = super::build_commit();
        assert!(!commit.is_empty());
        match option_env!("DZ_LOADER_BUILD_COMMIT") {
            Some(stamped) => assert_eq!(commit, stamped),
            None => assert_eq!(commit, super::UNKNOWN_COMMIT),
        }
    }
}
