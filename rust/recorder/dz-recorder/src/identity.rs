//! What this build says about itself, and what it says when it does not know.

use dz_recorder_core::{RecorderConfig, RecorderIdentity};

/// The version in `Cargo.toml`, which is the one thing about a build that is
/// always knowable from the source alone.
pub const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");

/// What a build that cannot know its commit writes instead.
///
/// A literal, and never an empty string: an empty field in an archive reads as
/// a field somebody forgot to fill in, while this one reads as what it is.
pub const UNKNOWN_COMMIT: &str = "unknown";

/// The commit this binary was built from, or [`UNKNOWN_COMMIT`].
///
/// Read from the environment at *compile* time rather than discovered at run
/// time or by a `build.rs` that shells out to `git`, for three reasons that all
/// point the same way.
///
/// The commit is a property of the build, so it has to be fixed when the build
/// happens; a binary that asked `git` at startup would report the commit of
/// whatever tree it was standing in, which on a recorder host is no tree at
/// all. A `build.rs` running `git rev-parse` would answer for the *working
/// tree*, which is only the same thing when nothing is uncommitted — and a
/// build from a dirty tree that claims a clean commit is precisely the false
/// provenance this field exists to prevent, told convincingly. And a source
/// archive with no `.git` would have to fall back anyway, so the fallback is
/// the real design and the git call would only be a way of making it rare.
///
/// So the value comes from whatever performed the build, and a build performed
/// by something that did not state it says it does not know. There is no path
/// here that produces a commit nobody stamped, which is the only property that
/// matters: this string ends up in the Section Header block of every archive
/// and in every manifest row, and a wrong one attributes a finding to a build
/// that never wrote it.
#[must_use]
pub const fn build_commit() -> &'static str {
    commit_or_unknown(option_env!("DZ_RECORDER_BUILD_COMMIT"))
}

/// The decision, split from the environment read so that it is testable.
///
/// `option_env!` resolves at compile time, so a test of [`build_commit`] can
/// only ever see the environment the test binary itself was built in — which is
/// why the empty case survived: the assertion and the value came from the same
/// place.
#[must_use]
pub const fn commit_or_unknown(raw: Option<&'static str>) -> &'static str {
    match raw {
        // Empty is *set and unknown*, not set: a pipeline whose `git rev-parse`
        // failed exports the variable with nothing in it, and taking that
        // literally writes the blank commit this module promises never to emit
        // into every archive and every `--version` line.
        Some(commit) if !commit.is_empty() => commit,
        _ => UNKNOWN_COMMIT,
    }
}

/// The provenance that travels inside every archive this run writes.
#[must_use]
pub fn identity_of(config: &RecorderConfig) -> RecorderIdentity {
    RecorderIdentity {
        site: config.site.clone(),
        recorder: config.recorder.clone(),
        env: config.env.clone(),
        build_version: BUILD_VERSION.to_owned(),
        build_commit: build_commit().to_owned(),
        // Of the parsed configuration, so a reformatting or an added comment
        // does not invalidate the provenance of everything written before it.
        config_hash: config.config_hash(),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_build_commit_set_to_nothing_is_not_a_commit() {
        // The shape a pipeline produces when its `git rev-parse` failed: the
        // variable is exported, and empty. Taken literally it writes a blank
        // commit into the Section Header block of every archive, where it reads
        // as a field somebody forgot rather than as a build that did not know.
        assert_eq!(super::commit_or_unknown(Some("")), super::UNKNOWN_COMMIT);
        assert_eq!(super::commit_or_unknown(None), super::UNKNOWN_COMMIT);
        assert_eq!(super::commit_or_unknown(Some("0f1e2d3")), "0f1e2d3");
    }

    use super::*;

    #[test]
    fn the_build_version_is_the_package_version() {
        assert_eq!(BUILD_VERSION, env!("CARGO_PKG_VERSION"));
        assert!(!BUILD_VERSION.is_empty());
    }

    #[test]
    fn a_build_that_was_not_told_its_commit_says_it_does_not_know() {
        // Whichever way this build was made, the commit is either what the
        // pipeline stamped or the literal admission — never blank, and never
        // something invented at run time that would look like a real commit.
        let commit = build_commit();
        assert!(!commit.is_empty());
        match option_env!("DZ_RECORDER_BUILD_COMMIT") {
            Some(stamped) => assert_eq!(commit, stamped),
            None => assert_eq!(commit, UNKNOWN_COMMIT),
        }
    }

    #[test]
    fn the_identity_carries_the_configuration_hash_and_the_build() {
        let config = crate::startup::tests::valid_config();
        let identity = identity_of(&config);
        assert_eq!(identity.build_version, BUILD_VERSION);
        assert_eq!(identity.build_commit, build_commit());
        assert_eq!(identity.config_hash, config.config_hash());
        assert_eq!(identity.site, config.site);
        assert_eq!(identity.recorder, config.recorder);
        assert_eq!(identity.env, config.env);
    }
}
