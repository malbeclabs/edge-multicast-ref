//! `[adapter] kind`, resolved against the registry the venue's `main`
//! populated, with no default and no fallback.
//!
//! The first two tests here are the ones the plan's task 7 asks for by name,
//! and both are the same audit finding from two sides: a publisher had a
//! misspelled configuration section parse cleanly, fall back to a default, and
//! run the wrong transport while its operator believed otherwise. One test is
//! about the value being wrong; the other is about the *key* being wrong, which
//! is the half that failed silently.

mod harness;

use dz_ingress_core::Kind;
use dz_publisher_runtime::{AdapterContext, AdapterRegistry, Document, StartupError};
use harness::Doc;

/// A registry entry that refuses if it is reached.
///
/// Refusing rather than constructing, because nothing in this file gets as far
/// as needing a transport: what is under test is the resolution, and an entry
/// that built one would be a test of a transport. A refusal is also what makes
/// *reached* observable — see
/// [`the_registry_resolves_a_registered_kind`].
fn register(registry: AdapterRegistry, name: &'static str) -> AdapterRegistry {
    registry.with(name, |cx| {
        Err(format!("reached the constructor registered as `{}`", cx.kind()).into())
    })
}

fn context<'a>(adapter: &'a dz_publisher_runtime::AdapterConfig) -> AdapterContext<'a> {
    AdapterContext::new(adapter, Kind::Uds, "a-venue")
}

// ---------------------------------------------------------------------------
// Task 7's first test.
// ---------------------------------------------------------------------------

#[test]
fn an_unknown_kind_fails_and_the_message_lists_every_registered_kind() {
    let registry = register(
        register(register(AdapterRegistry::new(), "one-source"), "another"),
        "a-third",
    );

    let mut doc = Doc::valid();
    doc.adapter = "[adapter]\nkind = \"a-fourth\"\n".to_owned();
    let document = Document::parse(&doc.render()).expect("the document itself is valid");

    let error = registry
        .open(&context(&document.adapter))
        .expect_err("`a-fourth` was never registered");

    // Not merely an error: an error that answers the operator's next question.
    // Being told only that a value was refused leaves *is this a spelling
    // mistake or a build that did not link my adapter?* unanswered, and those
    // are opposite actions.
    let message = error.to_string();
    assert!(message.contains("a-fourth"), "{message}");
    for registered in ["one-source", "another", "a-third"] {
        assert!(
            message.contains(registered),
            "the message must name every registered kind; it did not name \
             `{registered}`: {message}"
        );
    }
    assert!(
        matches!(error, StartupError::UnknownAdapterKind { .. }),
        "an unregistered kind is its own error and not a fallback"
    );
}

#[test]
fn an_unknown_kind_never_falls_back_to_the_only_registered_adapter() {
    // The most tempting fallback of all, and the one that would have produced
    // the audit's finding exactly: with one adapter linked, running it is
    // *almost always* what the operator meant. It is still refused, because the
    // one time it is not what they meant is a publisher emitting the wrong
    // venue's data under this venue's `Source ID`.
    let registry = register(AdapterRegistry::new(), "the-only-one");
    let mut doc = Doc::valid();
    doc.adapter = "[adapter]\nkind = \"the-onlyone\"\n".to_owned();
    let document = Document::parse(&doc.render()).expect("valid");

    let error = registry
        .open(&context(&document.adapter))
        .expect_err("one registered adapter is not a default");
    assert!(error.to_string().contains("the-only-one"));
}

#[test]
fn an_empty_registry_says_so_rather_than_printing_an_empty_list() {
    // A binary that registered no adapter is a build that forgot one, and the
    // message should read as that rather than as a spelling problem. This
    // follows `Kind::linked_list`, which makes the same distinction for the
    // same reason one level down.
    let registry = AdapterRegistry::new();
    let document = Document::parse(&Doc::valid().render()).expect("valid");
    let error = registry
        .open(&context(&document.adapter))
        .expect_err("nothing is registered");
    let message = error.to_string();
    assert!(
        message.contains("none registered by this binary"),
        "an empty registry must say so: {message}"
    );
    // And it must still name what a `kind` *could* resolve to, or the message
    // reads as "nothing works here" when one name does.
    assert!(
        message.contains("built in: uds"),
        "the built-in is part of what this binary answers to: {message}"
    );
}

// ---------------------------------------------------------------------------
// Task 7's second test: the audit's own failure.
// ---------------------------------------------------------------------------

#[test]
fn a_misspelled_section_under_adapter_fails_to_load_rather_than_silently_defaulting() {
    // `[adapter.upstrem]`. Without `deny_unknown_fields` this parses cleanly,
    // `[adapter.upstream]` is an empty table, and the adapter is constructed
    // against defaults it never asked for - which is the audit's finding, one
    // level down from where it happened.
    let mut doc = Doc::valid();
    doc.adapter = "[adapter]\n\
                   kind = \"a-venue\"\n\
                   \n\
                   [adapter.upstrem]\n\
                   endpoint = \"wss://a.venue.example/stream\"\n"
        .to_owned();

    let error = Document::parse(&doc.render())
        .expect_err("a misspelled section under [adapter] must not load");

    let message = error.to_string();
    assert!(
        message.contains("upstrem"),
        "the message must name the key it did not recognise: {message}"
    );
    assert!(
        message.contains("upstream"),
        "and the keys it would have: {message}"
    );
}

#[test]
fn the_correctly_spelled_section_loads_and_reaches_the_adapter() {
    // The control for the test above. A test that only proved a misspelling
    // fails would pass just as well against a parser that refused everything.
    let mut doc = Doc::valid();
    doc.adapter = "[adapter]\n\
                   kind = \"a-venue\"\n\
                   \n\
                   [adapter.upstream]\n\
                   endpoint = \"wss://a.venue.example/stream\"\n"
        .to_owned();

    let document = Document::parse(&doc.render()).expect("the spelling is right");
    let adapter = &document.adapter;
    let cx = context(adapter);

    #[derive(serde::Deserialize)]
    struct Upstream {
        endpoint: String,
    }
    let upstream: Upstream = cx.upstream().expect("the adapter's own keys");
    assert_eq!(upstream.endpoint, "wss://a.venue.example/stream");
}

// ---------------------------------------------------------------------------
// Resolution that succeeds, and the registry as a value.
// ---------------------------------------------------------------------------

#[test]
fn the_registry_resolves_a_registered_kind() {
    let registry = register(register(AdapterRegistry::new(), "one-source"), "another");
    let mut doc = Doc::valid();
    doc.adapter = "[adapter]\nkind = \"another\"\n".to_owned();
    let document = Document::parse(&doc.render()).expect("valid");

    // The constructor is reached, which is all resolution owes. Its own
    // refusal is what proves *which* entry was reached: an `AdapterInit`
    // naming `another` cannot have come from `one-source`.
    let error = registry
        .open(&context(&document.adapter))
        .expect_err("the entry this test registered refuses");
    assert!(
        matches!(
            error,
            StartupError::AdapterInit {
                kind: "another",
                ..
            }
        ),
        "resolution reached the wrong entry: {error}"
    );
    assert!(error.to_string().contains("another"));
}

#[test]
fn the_kind_a_constructor_was_selected_by_reaches_it() {
    // A venue whose one adapter covers several of its own product lines
    // registers one closure under several names, and has to be able to tell
    // which name it was reached through.
    let seen = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let recorded = std::rc::Rc::clone(&seen);
    let registry = AdapterRegistry::new().with("one-line", move |cx| {
        recorded.borrow_mut().push(cx.kind().to_owned());
        Err("stopping here".into())
    });

    let mut doc = Doc::valid();
    doc.adapter = "[adapter]\nkind = \"one-line\"\n".to_owned();
    let document = Document::parse(&doc.render()).expect("valid");
    let error = registry
        .open(&context(&document.adapter))
        .expect_err("the constructor refused");

    assert_eq!(seen.borrow().as_slice(), ["one-line"]);
    assert!(
        matches!(
            error,
            StartupError::AdapterInit {
                kind: "one-line",
                ..
            }
        ),
        "a constructor's own refusal is reported against the kind that selected it"
    );
}

#[test]
fn the_registry_names_its_entries_in_a_stable_order() {
    // Sorted rather than in registration order, so that what an operator is
    // shown does not depend on the order somebody happened to write the calls
    // in.
    let registry = register(
        register(register(AdapterRegistry::new(), "zeta"), "alpha"),
        "mu",
    );
    assert_eq!(registry.kinds(), ["alpha", "mu", "zeta"]);
    // The built-ins are named separately rather than mixed in, because *what is
    // in this binary* has two sources and an operator reading the message needs
    // to know which one a name came from. `kinds()` and `len()` stay the
    // venue's own entries.
    assert_eq!(
        registry.registered_list(),
        "alpha, mu, zeta (built in: uds)"
    );
    assert_eq!(registry.len(), 3);
    assert!(!registry.is_empty());
}

#[test]
#[should_panic(expected = "registered twice")]
fn a_kind_registered_twice_panics_rather_than_shadowing_one_adapter_with_another() {
    // Both ways of absorbing a duplicate are worse than a panic: keeping the
    // first silently ignores the second, and keeping the second silently
    // replaces the first - which is an adapter shadowing another adapter, the
    // exact class of failure this registry exists to make impossible. The panic
    // happens before a socket exists and before a single datagram.
    let _ = register(register(AdapterRegistry::new(), "one-source"), "one-source");
}

// ---------------------------------------------------------------------------
// The built-in: the one adapter this crate contains itself.
// ---------------------------------------------------------------------------

/// A `[adapter]` section naming the built-in record adapter, with the listing
/// set a record source and a publisher have to agree on out of band.
fn uds_adapter_section(listings: &str) -> String {
    format!("[adapter]\nkind = \"uds\"\n\n[adapter.upstream]\n{listings}")
}

fn one_listing() -> String {
    "[[adapter.upstream.listing]]\n\
     symbol = \"A-B\"\n\
     asset_class = \"crypto_spot\"\n\
     price_exponent = -2\n\
     qty_exponent = -3\n\
     market_model = \"clob\"\n\
     tick_size = \"0.01\"\n\
     lot_size = \"0.001\"\n\
     settle_type = \"cash\"\n\
     price_bound = \"non_negative\"\n"
        .to_owned()
}

#[test]
fn the_builtin_record_adapter_resolves_with_no_venue_registered() {
    // The one case where there is no venue code to register: an integration
    // that is not Rust cannot implement `Adapter`, so it writes records and the
    // built-in reads them. An empty registry is the whole point of this test -
    // nothing here registered anything, and `kind = "uds"` still resolves.
    let mut doc = Doc::valid();
    doc.adapter = uds_adapter_section(&one_listing());
    let document = Document::parse(&doc.render()).expect("valid");

    AdapterRegistry::new()
        .open(&context(&document.adapter))
        .expect("the built-in resolves without a venue");
}

#[test]
fn a_venue_registering_the_same_name_is_the_one_that_runs() {
    // A built-in is a registered kind, not a precedence rule over the venue: a
    // venue that has its own reason to spell an adapter `uds` knows its own
    // upstream, and this crate does not get to overrule it.
    let mut doc = Doc::valid();
    doc.adapter = uds_adapter_section(&one_listing());
    let document = Document::parse(&doc.render()).expect("valid");

    let error = register(AdapterRegistry::new(), "uds")
        .open(&context(&document.adapter))
        .expect_err("the venue's own entry is reached");
    assert!(
        error.to_string().contains("reached the constructor"),
        "the venue's constructor must be the one called: {error}"
    );
}

#[test]
fn the_builtin_refuses_a_listing_set_of_nothing() {
    // Refused at startup rather than at the first poll. An adapter offering
    // nothing publishes nothing, and a record source writing for symbols the
    // publisher never admitted is a feed that looks alive and carries no
    // instrument.
    let mut doc = Doc::valid();
    doc.adapter = uds_adapter_section("");
    let document = Document::parse(&doc.render()).expect("valid");

    let error = AdapterRegistry::new()
        .open(&context(&document.adapter))
        .expect_err("no listing was stated");
    let message = error.to_string();
    assert!(message.contains("uds"), "the kind is named: {message}");
}

#[test]
fn the_builtin_transport_refuses_and_says_which_path_does_work() {
    // The honest state of this path: the record *adapter* exists and the record
    // *transport* does not, so a live run cannot be composed and an offline one
    // can. A transport that connected to nothing and stayed up would look
    // exactly like a healthy feed on a quiet venue, which is the failure this
    // whole family of crates is built to make impossible.
    let mut doc = Doc::valid();
    doc.adapter = uds_adapter_section(&one_listing());
    let document = Document::parse(&doc.render()).expect("valid");
    let mut venue = AdapterRegistry::new()
        .open(&context(&document.adapter))
        .expect("resolves");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("a current-thread runtime");
    let error = runtime
        .block_on(venue.input.connect(std::time::Duration::from_millis(1)))
        .expect_err("there is no transport to connect");

    let message = error.to_string();
    assert!(
        message.contains("adapter.replay"),
        "the refusal has to name the path that works: {message}"
    );
    assert!(
        message.contains("dz-ingress-uds"),
        "and the crate that is missing: {message}"
    );
    // `Fatal` rather than `Connect`: a transport that does not exist is not
    // worth retrying under a backoff forever, so the driver stops instead of
    // looping.
    assert!(matches!(error, dz_ingress_core::IngressError::Fatal { .. }));
}

#[test]
fn an_unknown_kind_names_the_built_ins_as_well_as_the_venues_own() {
    // *What is in this binary* has two sources now, and an operator reading the
    // message cannot act on it without being told both.
    let mut doc = Doc::valid();
    doc.adapter = "[adapter]\nkind = \"mistyped\"\n".to_owned();
    let document = Document::parse(&doc.render()).expect("valid");

    let error = register(AdapterRegistry::new(), "a-venue-tob")
        .open(&context(&document.adapter))
        .expect_err("`mistyped` is neither");
    let message = error.to_string();
    assert!(message.contains("a-venue-tob"), "{message}");
    assert!(message.contains("built in: uds"), "{message}");
}
