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
        "`endpoint` is `{endpoint}`, and this build of dz-ingress-poll carries no TLS stack: \
         build it with the `tls` feature, or point this at an `http` endpoint on a path where \
         that is defensible"
    )]
    TlsUnsupported { endpoint: String },

    /// The endpoint is not an HTTP URL at all.
    #[error("`endpoint` is `{endpoint}`; a polled endpoint is http:// or https://")]
    NotAnHttpEndpoint { endpoint: String },

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
#[derive(Debug, Clone, Deserialize)]
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
        if endpoint.starts_with("https://") && !cfg!(feature = "tls") {
            return Err(ConfigError::TlsUnsupported {
                endpoint: endpoint.to_string(),
            });
        }
        if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
            return Err(ConfigError::NotAnHttpEndpoint {
                endpoint: endpoint.to_string(),
            });
        }
        if self.poll_interval.is_zero() {
            return Err(ConfigError::ZeroInterval);
        }
        Ok(())
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
}
