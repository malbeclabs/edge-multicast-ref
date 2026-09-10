//! The registry at its **defaulted** error, and the two messages that reports.
//!
//! `AdapterRegistry<E = AdapterInitError>` carries a default, and this crate
//! carries the `AdapterResolution` implementation that goes with it. Nothing
//! that publishes reaches either: `dz-publisher-runtime` aliases the registry
//! at its own `StartupError` and implements the trait on that, so a suite over
//! there exercises that crate's wording and never these two strings. Without
//! this file the default has no user in the workspace and the fallback's two
//! `format!` calls never run — a documented behaviour nothing executes, which
//! is the same thing as an undocumented one.
//!
//! The caller shape here is the one the default exists for: a process that
//! composes a venue in order to record, with no startup enumeration of its own,
//! naming `AdapterRegistry` and letting the parameter default.
//!
//! **Both messages are asserted whole.**
//! [`AdapterResolution::unknown_kind`](dz_venue_composition::AdapterResolution::unknown_kind)
//! takes the token and the registry as two positional `String`s of the same
//! type, so a transposition compiles; an assertion that only looked for both
//! values somewhere in the message would pass with their roles swapped, which
//! is a message telling an operator that their registry is a misspelling.

use dz_ingress_core::Kind;
use dz_venue_composition::context::AdapterSection;
use dz_venue_composition::{
    AdapterContext, AdapterInitError, AdapterRegistry, ReplayConfig, Venue,
};

/// The four values [`AdapterContext::new`] reads, with no document above them.
///
/// Which is the claim `AdapterSection` is a trait for: the document a section
/// came out of is not one of the four, so a process with a configuration of its
/// own builds the same context for the same venue's constructor without owning
/// the publisher's.
struct Section {
    kind: &'static str,
    tables: toml::Table,
    replay: ReplayConfig,
}

impl Section {
    fn new(kind: &'static str) -> Self {
        Self {
            kind,
            tables: toml::Table::new(),
            replay: ReplayConfig::default(),
        }
    }
}

impl AdapterSection for Section {
    fn kind(&self) -> &str {
        self.kind
    }

    fn upstream(&self) -> &toml::Table {
        &self.tables
    }

    fn credentials(&self) -> &toml::Table {
        &self.tables
    }

    fn replay(&self) -> &ReplayConfig {
        &self.replay
    }
}

/// A registry at the defaulted parameter: no `E` is written anywhere here.
///
/// The return type is the whole point — a parameter default applies in type
/// position, and this is that position. `AdapterRegistry::new()` on its own,
/// with nothing naming the error, is `error[E0282]`.
fn defaulted() -> AdapterRegistry {
    AdapterRegistry::new()
}

/// An entry that refuses if it is reached.
///
/// A refusal rather than a construction, because what is under test is the
/// resolution and an entry that built a transport would be a test of one.
fn refuses(cx: &AdapterContext<'_>) -> Result<Venue, AdapterInitError> {
    Err(format!("reached the constructor registered as `{}`", cx.kind()).into())
}

#[test]
fn an_unknown_kind_names_the_token_and_the_whole_registry() {
    let section = Section::new("a-fourth");
    let cx = AdapterContext::new(&section, Some(Kind::Uds), "a-venue", &[], &[]);
    let registry = defaulted()
        .with("one-source", refuses)
        .with("another", refuses)
        .with("a-third", refuses);

    let error: AdapterInitError = registry
        .open(&cx)
        .expect_err("`a-fourth` was never registered");

    // Whole, and not two `contains` calls: the token and the registry are the
    // same type in positional order, so only the finished sentence says which
    // value went where. Sorted, because what an operator is shown must not
    // depend on the order somebody happened to write the `with` calls in.
    assert_eq!(
        error.to_string(),
        "`[adapter] kind = \"a-fourth\"` names no adapter this binary registered \
         (a-third, another, one-source (built in: uds))"
    );
}

#[test]
fn an_empty_registry_says_so_rather_than_printing_an_empty_list() {
    let section = Section::new("a-venue-adapter");
    let cx = AdapterContext::new(&section, Some(Kind::Uds), "a-venue", &[], &[]);

    let error: AdapterInitError = defaulted()
        .open(&cx)
        .expect_err("nothing is registered and the built-in answers another name");

    // A build that linked no venue adapter is a different problem from a
    // misspelled name and wants a different action, so the message says it in
    // words — and still names what a `kind` *could* resolve to, or it reads as
    // "nothing works here" when one name does.
    assert_eq!(
        error.to_string(),
        "`[adapter] kind = \"a-venue-adapter\"` names no adapter this binary \
         registered (none registered by this binary (built in: uds))"
    );
}

#[test]
fn a_constructor_that_refuses_is_reported_against_the_name_it_was_registered_under() {
    let section = Section::new("a-venue-adapter");
    let cx = AdapterContext::new(&section, Some(Kind::Uds), "a-venue", &[], &[]);
    let registry = defaulted()
        .with("a-venue-adapter", refuses)
        .with("another", refuses);

    let error: AdapterInitError = registry
        .open(&cx)
        .expect_err("the entry that answers this kind refuses");

    // The name the entry was registered under, not the token the document
    // carried, so a closure registered under several names reports against the
    // one that reached it — and the venue's own refusal is carried through
    // rather than summarised.
    assert_eq!(
        error.to_string(),
        "the adapter `a-venue-adapter` could not be built: \
         reached the constructor registered as `a-venue-adapter`"
    );
}
