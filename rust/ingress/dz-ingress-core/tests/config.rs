//! The `[ingress]` section, from the text an operator writes.
//!
//! Parsed here from real TOML rather than from a constructed value, because
//! half of what this section is for is the *spelling*: six values appear in
//! every existing publisher and most are spelled two or three ways each, and a
//! test that skips the document cannot catch a key that has quietly changed
//! name.

use std::time::Duration;

use dz_ingress_core::{ConfigError, IngressConfig, Kind};

fn parse(text: &str) -> Result<IngressConfig, toml::de::Error> {
    toml::from_str(text)
}

#[test]
fn the_section_as_the_design_documents_it_parses_to_those_values() {
    // Exactly the block in the publisher crates design, values included. If a
    // key here is renamed, this is the test that says so.
    let config = parse(
        r#"
        kind                      = "websocket"
        connect_timeout           = "5s"
        reconnect_backoff_initial = "500ms"
        reconnect_backoff_max     = "30s"
        rate_limit_per_second     = 0
        "#,
    )
    .expect("the documented section must parse");

    assert_eq!(config.kind, "websocket");
    assert_eq!(config.connect_timeout, Duration::from_secs(5));
    assert_eq!(config.reconnect_backoff_initial, Duration::from_millis(500));
    assert_eq!(config.reconnect_backoff_max, Duration::from_secs(30));
    assert_eq!(config.rate_limit_per_second, 0);
    assert_eq!(config.idle_timeout, None);
}

#[test]
fn only_the_transport_is_required_and_the_defaults_are_the_documented_values() {
    let config = parse(r#"kind = "websocket""#).expect("only `kind` is required");
    assert_eq!(config.connect_timeout, Duration::from_secs(5));
    assert_eq!(config.reconnect_backoff_initial, Duration::from_millis(500));
    assert_eq!(config.reconnect_backoff_max, Duration::from_secs(30));
    assert_eq!(config.rate_limit_per_second, 0);
}

#[test]
fn a_missing_transport_is_a_load_error_rather_than_a_default() {
    // There is no default transport, and this is the one key that must not have
    // one: the audit's misspelled section became the wrong transport precisely
    // because something defaulted.
    let error = parse(r#"connect_timeout = "5s""#).expect_err("`kind` has no default");
    assert!(error.to_string().contains("kind"), "{error}");
}

#[test]
fn a_key_nobody_reads_is_refused_rather_than_ignored() {
    // `deny_unknown_fields`, and the reason it is load-bearing: a publisher had
    // a misspelled section parse cleanly, fall back to a default, and run a
    // transport its operator did not believe it was running.
    let error = parse(
        r#"
        kind = "websocket"
        reconnect_backoff = "500ms"
        "#,
    )
    .expect_err("an unknown key must not be ignored");
    assert!(error.to_string().contains("reconnect_backoff"), "{error}");
}

#[test]
fn a_duration_without_a_unit_is_refused() {
    let error = parse(
        r#"
        kind            = "websocket"
        connect_timeout = "5"
        "#,
    )
    .expect_err("a bare number is not a duration");
    assert!(error.to_string().contains("no unit"), "{error}");
}

#[test]
fn a_duration_written_as_a_number_is_refused() {
    // The publisher that suffixes its keys `_seconds` and takes integers is the
    // reason: `5` under a key spelled without a unit is five of something.
    let error = parse(
        r#"
        kind            = "websocket"
        connect_timeout = 5
        "#,
    )
    .expect_err("an integer is not a duration");
    assert!(!error.to_string().is_empty());
}

#[test]
fn a_transposed_backoff_pair_is_refused_rather_than_clamped() {
    let config = parse(
        r#"
        kind                      = "websocket"
        reconnect_backoff_initial = "30s"
        reconnect_backoff_max     = "500ms"
        "#,
    )
    .expect("the values parse; the pair is what is wrong");
    let error = config
        .resolve()
        .expect_err("the pair is the wrong way round");
    assert!(
        matches!(error, ConfigError::BackoffInverted { .. }),
        "{error}"
    );
}

#[test]
fn a_zero_idle_guard_is_refused() {
    // A zero guard ends every connection the instant it comes up, which reads
    // in the metrics exactly like a venue refusing us.
    let config = parse(
        r#"
        kind         = "websocket"
        idle_timeout = "0s"
        "#,
    )
    .expect("the value parses");
    let error = config.resolve().expect_err("a zero guard is not a guard");
    assert!(
        matches!(
            error,
            ConfigError::ZeroDuration {
                key: "idle_timeout"
            }
        ),
        "{error}"
    );
}

#[test]
fn an_idle_guard_reaches_the_policy_when_it_is_given() {
    let config = parse(
        r#"
        kind         = "websocket"
        idle_timeout = "60s"
        "#,
    )
    .expect("the section parses");
    assert_eq!(config.idle_timeout, Some(Duration::from_secs(60)));
}

#[test]
fn a_transport_no_token_answers_to_is_refused_naming_the_built_in_set() {
    let config = parse(r#"kind = "web-socket""#).expect("the value parses");
    let error = config.resolve().expect_err("that is not a token");
    assert!(matches!(error, ConfigError::UnknownKind { .. }), "{error}");
    let message = error.to_string();
    for kind in Kind::ALL {
        assert!(
            message.contains(kind.as_token()),
            "the message must name the acceptable values: {message}"
        );
    }
}

#[cfg(feature = "websocket")]
#[test]
fn a_linked_transport_resolves_together_with_its_policy() {
    let config = parse(r#"kind = "websocket""#).expect("the section parses");
    let (kind, policy) = config.resolve().expect("websocket is linked in this build");
    assert_eq!(kind, Kind::WebSocket);
    assert_eq!(policy.connect_timeout, Duration::from_secs(5));
    assert_eq!(policy.backoff.initial(), Duration::from_millis(500));
}

#[cfg(not(feature = "websocket"))]
#[test]
fn a_transport_this_binary_was_not_built_with_says_so_and_not_unknown() {
    // A different error from an unknown token, because the operator's next
    // action is different: one is a typo to fix in the file, the other is a
    // build to redo. Collapsing them sends someone hunting for a spelling
    // mistake in a value that is spelled correctly.
    let config = parse(r#"kind = "websocket""#).expect("the section parses");
    let error = config
        .resolve()
        .expect_err("this build links no websocket transport");
    assert!(
        matches!(error, ConfigError::KindNotLinked { token: "websocket" }),
        "{error}"
    );
}

#[cfg(not(feature = "fix"))]
#[test]
fn a_transport_the_family_names_but_nobody_has_built_is_not_unknown_either() {
    // The family is fixed by the design and most of it is not written yet. A
    // configuration naming one of those must not read as a spelling mistake.
    let config = parse(r#"kind = "fix""#).expect("the section parses");
    let error = config.resolve().expect_err("nothing implements it yet");
    assert!(
        matches!(error, ConfigError::KindNotLinked { token: "fix" }),
        "{error}"
    );
}
