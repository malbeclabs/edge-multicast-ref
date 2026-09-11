//! The transport's own table, parsed here so that it cannot be spelled two
//! ways.
//!
//! The rule is the core's: each shared crate parses its own section, so keys,
//! types and defaults cannot drift between venues. `[ingress]` is
//! [`dz_ingress_core`]'s; the endpoint and the cadence are this transport's,
//! and a venue hands this table over from its own document — a
//! `[source.upstream]` block for a polled `[[source]]`, or `[adapter.upstream]`
//! for a publisher with one.
//!
//! What is deliberately not here: `connect_timeout` and the reconnect delay,
//! which are the family's and are stated once in `[ingress]`; and a request
//! timeout, which is the receive budget the driver already hands over.

use std::time::Duration;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer};

/// Why a polled `[[source]]` cannot be run.
///
/// Every variant names what *is* acceptable and not only what was wrong, which
/// is the core's own standard for the same reason: an error that says a value
/// is unacceptable and stops there invites the same guess a second time.
///
/// # The three that name the endpoint name only its authority
///
/// Each names `endpoint` as the key and the scheme and host as the value, and
/// **none renders the endpoint itself** — not through `Display` and not
/// through `Debug`. A key on the query string is a shape this transport
/// documents as supported, because several venue catalogue APIs keep one
/// there, and a load failure is the most-logged line a publisher has: it is
/// what a supervisor captures when the process will not start. It is the rule
/// [`PollConfig`]'s own `Debug` keeps, and the one every error detail in
/// `poll.rs` keeps.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    /// The endpoint is `https` and this build carries no TLS stack.
    ///
    /// **Refused rather than downgraded.** An operator who wrote `https` asked
    /// for the wire to be encrypted, and a transport that quietly spoke plain
    /// HTTP instead would send whatever the query string carries — which for
    /// several venue APIs is a key — over the wire in the clear, having been
    /// told not to. Reported at load rather than at the first request, so that
    /// the answer is a startup failure and not a publisher that comes up
    /// healthy and fails forever afterwards.
    #[error(
        "`endpoint` is `{authority}`, and this build of dz-ingress-poll carries no TLS stack: \
         build it with the `tls` feature, or point this at an `http` endpoint on a path where \
         that is defensible"
    )]
    TlsUnsupported { authority: String },

    /// The endpoint is not an HTTP URL at all.
    ///
    /// Which includes the endpoint that names no scheme, and `authority_of`
    /// still answers for one of those: everything from the path onwards is
    /// dropped whether or not what precedes it is a scheme.
    #[error("`endpoint` is `{authority}`; a polled endpoint is http:// or https://")]
    NotAnHttpEndpoint { authority: String },

    /// The endpoint carries a `#`.
    ///
    /// **Refused at load, and this is the one check here that the connect
    /// probe does not also reach.** A fragment is not sent to a server, so
    /// `http://host/catalogue#overview` with `cursor=1` appended requests
    /// `/catalogue` and nothing else: `hyper::Uri` parses it happily and
    /// swallows the whole query string into the fragment. The probe succeeds,
    /// every poll succeeds, and the adapter's parameters are dropped from
    /// every request with no error anywhere — an endpoint answering the first
    /// page for ever while the cursor it was given goes nowhere. That quiet
    /// outcome is the one [`RequestFailure::Unusable`] refuses a `#` in the
    /// parameters to avoid, and the endpoint is the other half of the same
    /// input space.
    ///
    /// [`RequestFailure::Unusable`]: crate::RequestFailure::Unusable
    #[error(
        "`endpoint` is `{authority}` and carries a `#`: a fragment is not sent to a server, \
         so the query string — the adapter's parameters included — would be swallowed by it \
         and every request would go out without them; a literal `#` in a value is written \
         `%23`"
    )]
    FragmentInEndpoint { authority: String },

    /// A cadence of zero.
    ///
    /// Refused because it is the one value that turns this transport into the
    /// thing it exists instead of: a request in a loop, as fast as the endpoint
    /// will answer, which is how a publisher's address gets blocked rather than
    /// merely being wrong.
    #[error("`poll_interval` must be greater than zero: a cadence of zero is a request loop")]
    ZeroInterval,
}

/// A polled endpoint and how often to ask it.
///
/// `deny_unknown_fields`, and that is load-bearing rather than tidiness: the
/// audit's own failure was a misspelled section that parsed cleanly, fell back
/// to a default, and ran something its operator did not believe it was running.
///
/// # Why `poll_interval` has no default
///
/// Every other duration in this family has one. This does not, because there is
/// no defensible number: a catalogue polled once a minute and a book polled
/// every second are the same transport, and what an operator is willing to ask
/// of an endpoint is a property of that endpoint. A transport with no cadence
/// polls in a loop or never, and both are worse than a refusal — so a document
/// that omits it does not parse.
///
/// # It has to be well under `[ingress] idle_timeout`, and nothing checks it
///
/// The driver hands each receive **what is left of the idle guard**. When that
/// is less than the time to the next poll, the transport spends the budget and
/// returns [`Received::Idle`](dz_ingress_core::Received), which the driver ends
/// the connection with `timeout` for — so a cadence longer than the guard is
/// one payload per window, `connection_state` flapping, and
/// `reconnects_total{reason="timeout"}` climbing, on a document that reads as
/// correct. `poll_interval = "60s"` under `idle_timeout = "30s"` is the shape
/// of it.
///
/// Not refused at load, and the reason is structural rather than an omission:
/// this transport cannot see `[ingress]`, which is the core's section, and the
/// core cannot know that a source is polled. Checking it would mean one of the
/// two reading the other's table, which is the arrangement the per-crate rule
/// exists to prevent. So it is a stated rule, stated in
/// `BRINGING-UP-A-FEED.md` too.
///
/// Note also that an unchanged response is liveness and **does not reset the
/// guard** — deliberately, so that a catalogue which has stopped changing
/// still trips it — so the guard has to be long enough for a poll that
/// answers.
///
/// # Why an interval and not a cycle
///
/// This repository distinguishes the two deliberately. `definition_cycle` and
/// `snapshot_cycle` are **one full pass over a set**, divided by the set's
/// size, because a per-instrument interval has the whole set falling due
/// together. There is no set here: one tick is one request, so the honest word
/// is the one that means the time between two of them. A key spelled
/// `poll_cycle` would be read as *divide this by something*, and there is
/// nothing to divide it by.
///
/// # Its `Debug` prints no query string
///
/// See the implementation below. A derived one would put a venue's key in the
/// first line a publisher logs about its own configuration.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PollConfig {
    /// The endpoint to poll, scheme included.
    pub endpoint: String,

    /// The time between two requests. See this type's own note for why it is
    /// an interval, and why it has no default.
    #[serde(deserialize_with = "de_duration")]
    pub poll_interval: Duration,
}

impl PollConfig {
    /// Everything checkable without touching the network.
    ///
    /// # Errors
    ///
    /// [`ConfigError`], naming the key and what would have been accepted.
    pub fn check(&self) -> Result<(), ConfigError> {
        let endpoint = self.endpoint.trim();
        // The scheme, case-insensitively, because a scheme *is*
        // case-insensitive and `hyper::Uri` normalizes one: `HTTPS://host` is
        // an https endpoint, and refusing it as *not http(s)* would be a
        // refusal quoting the document's own value back at it as if it were
        // something else. It would also refuse it as the wrong one of the two,
        // sending an operator to look for a scheme they wrote.
        let scheme = endpoint.to_ascii_lowercase();
        if scheme.starts_with("https://") && !cfg!(feature = "tls") {
            return Err(ConfigError::TlsUnsupported {
                authority: authority_of(endpoint),
            });
        }
        if !scheme.starts_with("http://") && !scheme.starts_with("https://") {
            return Err(ConfigError::NotAnHttpEndpoint {
                authority: authority_of(endpoint),
            });
        }
        // After the scheme, so that an endpoint that is neither http nor https
        // is named as that first, and before the cadence, because this is a
        // fault in the same key.
        if endpoint.contains('#') {
            return Err(ConfigError::FragmentInEndpoint {
                authority: authority_of(endpoint),
            });
        }
        if self.poll_interval.is_zero() {
            return Err(ConfigError::ZeroInterval);
        }
        Ok(())
    }
}

/// Prints the scheme and host of the endpoint, whether it carries a query
/// string, and the cadence — and **never the endpoint itself**.
///
/// **A key on the query string is a shape this transport documents as
/// supported**, because that is where several venue catalogue APIs keep one,
/// and a configuration a publisher logs at startup is the easiest place in the
/// system for one to end up in a log file, a crash report or a support ticket.
/// A derived implementation would put it in all three the first time a venue's
/// `main` logged the table it resolved. The same standard `PollInput` and
/// `HttpClient` hold their own `Debug` to, and the standard the fatal
/// request-formation fault in `poll.rs` names the authority rather than the URI
/// for.
///
/// Whether a query string is present is printed, because that is the question
/// a `401` from an endpoint whose document looks right raises: a key that was
/// meant to be there and is not looks identical, in every other field, to one
/// that is.
impl core::fmt::Debug for PollConfig {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let endpoint = self.endpoint.trim();
        f.debug_struct("PollConfig")
            .field("authority", &authority_of(endpoint))
            .field("query", &endpoint.contains('?'))
            .field("poll_interval", &self.poll_interval)
            .finish()
    }
}

/// The scheme, host and port of an endpoint, and nothing after them.
///
/// A string operation and not a URL parse, because what it is for is a log
/// line, a `Debug` and a [`ConfigError`]: the part that must not be printed is
/// everything from the path onwards, and dropping it is the same operation
/// whether or not what follows parses — or whether, as in
/// [`ConfigError::NotAnHttpEndpoint`], what precedes it is a scheme at all.
///
/// It lives beside the key it reads. `endpoint` is this table's, and one
/// implementation of *what of an endpoint may be printed* is what keeps the
/// transport's log lines and this module's refusals to the same rule.
pub(crate) fn authority_of(endpoint: &str) -> String {
    let (scheme, after_scheme) = match endpoint.split_once("://") {
        Some((scheme, after_scheme)) => (Some(scheme), after_scheme),
        // An endpoint that names no scheme is one of the two this module
        // refuses, and it is refused by name — so the host is still worth
        // printing and the query string still must not be.
        None => (None, endpoint),
    };
    let host = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default()
        // A userinfo section is a credential more often than not, and this
        // string exists to be printed.
        .rsplit('@')
        .next()
        .unwrap_or_default();
    match scheme {
        Some(scheme) => format!("{scheme}://{host}"),
        None => host.to_string(),
    }
}

/// Durations are written with a unit — `"30s"` — and the unit is not optional.
///
/// The third implementation of this parse in the repository, and the core's
/// own copy says the third is the point at which it needs one home rather than
/// a copy per crate. It is not moved here because moving it is a change to
/// `[ingress]`'s and the recorder's parsing as well, which is its own task
/// with its own tests; what is not affordable is a *fourth* syntax, so this is
/// the same grammar and the same message as the core's, deliberately.
fn parse_duration(raw: &str) -> Result<Duration, String> {
    let digits = raw
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| format!("`{raw}` has no unit (ns, us, ms, s, m, h)"))?;
    if digits == 0 {
        return Err(format!("`{raw}` does not start with a number"));
    }
    let (value, unit) = raw.split_at(digits);
    let value: u64 = value
        .parse()
        .map_err(|_| format!("`{value}` is not a whole number"))?;
    let nanos = match unit {
        "ns" => 1,
        "us" => 1_000,
        "ms" => 1_000_000,
        "s" => 1_000_000_000,
        "m" => 60 * 1_000_000_000,
        "h" => 3_600 * 1_000_000_000,
        _ => {
            return Err(format!(
                "`{unit}` is not a duration unit (ns, us, ms, s, m, h)"
            ))
        }
    };
    value
        .checked_mul(nanos)
        .map(Duration::from_nanos)
        .ok_or_else(|| format!("`{raw}` does not fit in a 64-bit nanosecond count"))
}

fn de_duration<'de, D: Deserializer<'de>>(de: D) -> Result<Duration, D::Error> {
    let raw = String::deserialize(de)?;
    parse_duration(&raw).map_err(D::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A table an operator would write, for the tests that change one key of
    /// it.
    fn document() -> &'static str {
        "endpoint = \"http://192.0.2.10/catalogue\"\npoll_interval = \"30s\"\n"
    }

    #[test]
    fn a_document_that_states_the_cadence_resolves() {
        let config: PollConfig = toml::from_str(document()).expect("a stated cadence");
        assert_eq!(config.poll_interval, Duration::from_secs(30));
        assert_eq!(config.check(), Ok(()));
    }

    #[test]
    fn a_document_that_omits_the_cadence_is_refused_rather_than_given_a_default() {
        // There is no defensible default: a catalogue polled once a minute and
        // a book polled every second are the same transport. A transport with
        // no cadence polls in a loop or never, and both are worse than a
        // refusal.
        let error = toml::from_str::<PollConfig>("endpoint = \"http://192.0.2.10/catalogue\"\n")
            .expect_err("a document with no cadence");
        assert!(error.to_string().contains("poll_interval"), "{error}");
    }

    #[test]
    fn the_key_is_an_interval_and_a_cycle_is_not_a_spelling_of_it() {
        // A cycle is one full pass over a set divided by the set's size, which
        // is what `definition_cycle` and `snapshot_cycle` are. There is no set
        // here: one tick is one request. `deny_unknown_fields` is what makes
        // the other spelling a refusal rather than a key nobody reads.
        let error = toml::from_str::<PollConfig>(
            "endpoint = \"http://192.0.2.10/catalogue\"\npoll_cycle = \"30s\"\n",
        )
        .expect_err("`poll_cycle` is not this key");
        let message = error.to_string();
        assert!(message.contains("poll_cycle"), "{message}");
        assert!(message.contains("poll_interval"), "{message}");
    }

    #[test]
    fn a_cadence_of_zero_is_refused_because_it_is_a_request_loop() {
        let mut config: PollConfig = toml::from_str(document()).expect("a valid table");
        config.poll_interval = Duration::ZERO;
        assert_eq!(config.check(), Err(ConfigError::ZeroInterval));
    }

    /// An operator who wrote `https` asked for the wire to be encrypted, and a
    /// build with no TLS stack must say so at load rather than send whatever
    /// the query string carries in the clear.
    ///
    /// Both branches, so that this test means something in both builds — which is
    /// also what says the `tls` feature is real rather than declared.
    #[test]
    fn an_https_endpoint_without_tls_is_refused_at_load() {
        let mut config: PollConfig = toml::from_str(document()).expect("a valid table");
        config.endpoint = "https://192.0.2.10/catalogue".to_string();
        let outcome = config.check();
        if cfg!(feature = "tls") {
            assert_eq!(outcome, Ok(()), "this build carries a TLS stack");
        } else {
            let error = outcome.expect_err("an https endpoint in a build with no TLS stack");
            let message = error.to_string();
            // The scheme and the feature, because the operator's next action is
            // one or the other: change the endpoint, or change the build.
            assert!(message.contains("https"), "{message}");
            assert!(message.contains("tls"), "{message}");
        }
    }

    #[test]
    fn an_http_endpoint_is_accepted_whichever_way_this_build_was_made() {
        let config: PollConfig = toml::from_str(document()).expect("a valid table");
        assert_eq!(config.check(), Ok(()));
    }

    #[test]
    fn something_that_is_not_an_http_endpoint_names_the_two_schemes_that_are() {
        let mut config: PollConfig = toml::from_str(document()).expect("a valid table");
        config.endpoint = "192.0.2.10:8080".to_string();
        let error = config.check().expect_err("not an endpoint");
        assert!(error.to_string().contains("http://"), "{error}");
        assert!(error.to_string().contains("https://"), "{error}");
    }

    #[test]
    fn an_endpoint_carrying_a_fragment_is_refused_because_the_probe_would_not_fail_on_it() {
        // **The quiet outcome, and the only one of these the connect probe
        // does not also reach.** `http://host/catalogue#overview` with
        // `cursor=1` appended is `.../catalogue#overview?cursor=1`, which
        // `hyper::Uri` parses happily with the whole query string inside the
        // fragment — and a fragment is not sent to a server, so the request
        // goes out as `GET /catalogue`. The probe succeeds, every poll
        // succeeds, and the adapter's cursor is dropped from every request
        // with nothing failing anywhere. An operator reaches this by pasting a
        // catalogue URL off a docs page anchor.
        let mut config: PollConfig = toml::from_str(document()).expect("a valid table");
        config.endpoint = "http://192.0.2.10/catalogue#overview".to_string();
        let error = config.check().expect_err("a fragment in the endpoint");
        assert_eq!(
            error,
            ConfigError::FragmentInEndpoint {
                authority: "http://192.0.2.10".to_string(),
            }
        );
        // What the operator does next, and not the endpoint they wrote: the
        // rule every detail in this crate keeps.
        let message = error.to_string();
        assert!(message.contains('#'), "{message}");
        assert!(message.contains("%23"), "{message}");
        assert!(!message.contains("overview"), "{message}");
    }

    #[test]
    fn an_uppercase_scheme_is_the_scheme_it_spells() {
        // A scheme is case-insensitive and `hyper::Uri` normalizes one, so
        // `HTTPS://` is an https endpoint. Refusing it as *not http(s)* would
        // hand an operator a message quoting the two schemes back at a
        // document that names one of them, and send them looking for a scheme
        // they wrote.
        let mut config: PollConfig = toml::from_str(document()).expect("a valid table");
        config.endpoint = "HTTP://192.0.2.10/catalogue".to_string();
        assert_eq!(config.check(), Ok(()));

        // And the TLS refusal reads the same value the same way, which is the
        // half that matters: an uppercase `HTTPS` falling past it would be an
        // https endpoint in a build with no TLS stack.
        config.endpoint = "HTTPS://192.0.2.10/catalogue".to_string();
        let outcome = config.check();
        if cfg!(feature = "tls") {
            assert_eq!(outcome, Ok(()), "this build carries a TLS stack");
        } else {
            let error = outcome.expect_err("an https endpoint in a build with no TLS stack");
            assert!(
                matches!(error, ConfigError::TlsUnsupported { .. }),
                "an uppercase scheme is the scheme it spells, and this build has no TLS \
                 stack: {error:?}"
            );
        }
    }

    #[test]
    fn every_unit_the_error_message_offers_is_a_unit_the_parser_takes() {
        // The same grammar and the same message as the core's, deliberately: a
        // fourth duration syntax in this repository is what is not affordable.
        for unit in ["ns", "us", "ms", "s", "m", "h"] {
            assert!(
                parse_duration(&format!("1{unit}")).is_ok(),
                "`1{unit}` was offered by the error message and refused"
            );
        }
    }

    #[test]
    fn a_bare_number_is_refused_rather_than_guessed_at() {
        let error = parse_duration("30").expect_err("a bare number has no unit");
        assert!(error.contains("no unit"), "{error}");
    }

    /// An endpoint an operator would write with a key on it.
    ///
    /// Documentation-range host, and a secret that says in its own text that
    /// it is not one: a value in a fixture is copied into production sooner or
    /// later.
    const ENDPOINT_WITH_A_KEY: &str = "https://192.0.2.10:8443/catalogue?api_key=not-a-real-secret";

    /// The one substring that must not appear in anything rendered from a
    /// table.
    const SECRET: &str = "not-a-real-secret";

    #[test]
    fn the_debug_of_a_table_names_the_host_and_not_the_query_string() {
        // A derived `Debug` prints the endpoint verbatim, and a publisher that
        // logs its resolved configuration at startup is the easiest place in
        // the system for a venue's key to reach a log file.
        let mut config: PollConfig = toml::from_str(document()).expect("a valid table");
        config.endpoint = ENDPOINT_WITH_A_KEY.to_string();
        let rendered = format!("{config:?}");

        assert!(!rendered.contains(SECRET), "{rendered}");
        assert!(!rendered.contains("api_key"), "{rendered}");
        assert!(!rendered.contains('?'), "{rendered}");
        // And the half an operator needs is still there: which host, and
        // whether the endpoint carried a query string at all - because a key
        // that was meant to be there and is not looks identical, in every
        // other field, to one that is.
        assert!(rendered.contains("https://192.0.2.10:8443"), "{rendered}");
        assert!(rendered.contains("query: true"), "{rendered}");
        assert!(rendered.contains("30s"), "{rendered}");

        config.endpoint = "http://192.0.2.10/catalogue".to_string();
        let rendered = format!("{config:?}");
        assert!(
            rendered.contains("query: false"),
            "an endpoint with no query string says so: {rendered}"
        );
    }

    #[test]
    fn an_endpoint_refused_at_load_is_named_without_its_query_string() {
        // Both refusals render the endpoint, on the path a supervisor captures
        // when the process will not start. Three shapes: the `https` endpoint
        // a build with no TLS stack refuses, a scheme that is not HTTP at all,
        // and the endpoint that names no scheme - which is the one where
        // dropping everything from the path onwards is all there is to go on.
        let mut config: PollConfig = toml::from_str(document()).expect("a valid table");
        for endpoint in [
            ENDPOINT_WITH_A_KEY,
            "wss://192.0.2.10:8443/catalogue?api_key=not-a-real-secret",
            "192.0.2.10:8443/catalogue?api_key=not-a-real-secret",
        ] {
            config.endpoint = endpoint.to_string();
            let Err(error) = config.check() else {
                // The `https` endpoint is accepted where this build carries a
                // TLS stack, and then there is no message to read.
                assert!(
                    cfg!(feature = "tls") && endpoint == ENDPOINT_WITH_A_KEY,
                    "`{endpoint}` is refused in this build"
                );
                continue;
            };
            // `Display` and `Debug`, because a `Result` a caller logged with
            // `{:?}` renders the second and nothing else would have caught a
            // variant that kept the endpoint in a field.
            for rendered in [format!("{error}"), format!("{error:?}")] {
                assert!(!rendered.contains(SECRET), "`{endpoint}`: {rendered}");
                assert!(!rendered.contains("api_key"), "`{endpoint}`: {rendered}");
                assert!(!rendered.contains("catalogue"), "`{endpoint}`: {rendered}");
                assert!(
                    rendered.contains("192.0.2.10:8443"),
                    "the host is the half an operator needs: {rendered}"
                );
            }
        }
    }

    #[test]
    fn an_authority_keeps_the_host_and_drops_the_query_string_and_the_userinfo() {
        // Both halves matter: the host is what an operator needs, and a key in
        // a query string or a password in a userinfo section is what this
        // function exists to leave behind.
        assert_eq!(authority_of(ENDPOINT_WITH_A_KEY), "https://192.0.2.10:8443");
        assert_eq!(
            authority_of("http://user:not-a-real-password@192.0.2.10/catalogue"),
            "http://192.0.2.10"
        );
        // The endpoint that names no scheme is one this module refuses by
        // name, so it is rendered too - and it is rendered under the same rule
        // rather than whole.
        assert_eq!(
            authority_of("192.0.2.10:8443/catalogue?api_key=not-a-real-secret"),
            "192.0.2.10:8443"
        );
        assert_eq!(authority_of("not-an-endpoint"), "not-an-endpoint");
        // Each delimiter on its own, and this is the half a fixture with a
        // path cannot say anything about: in
        // `/catalogue?api_key=not-a-real-secret` the path is what the query
        // hides behind, so dropping `?` from the set changes nothing and an
        // endpoint whose key hangs straight off the authority is what proves
        // the character is read.
        assert_eq!(
            authority_of("https://192.0.2.10:8443?api_key=not-a-real-secret"),
            "https://192.0.2.10:8443"
        );
        assert_eq!(
            authority_of("https://192.0.2.10:8443#not-a-real-secret"),
            "https://192.0.2.10:8443"
        );
        assert_eq!(
            authority_of("https://192.0.2.10:8443/catalogue"),
            "https://192.0.2.10:8443"
        );
    }
}
