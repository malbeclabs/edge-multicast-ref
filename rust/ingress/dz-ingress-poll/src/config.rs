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

/// The shortest cadence this transport will run.
///
/// **A floor, and it is not the default [`PollConfig`] deliberately does not
/// have.** What an operator is willing to ask of an endpoint is a property of
/// that endpoint, which is why there is no default cadence at all; this is the
/// separate and much narrower question of which values cannot have been meant.
///
/// Fifty milliseconds is twenty requests a second at one catalogue endpoint,
/// and nobody chooses that: a venue whose book has to be read that often is a
/// subscription, and this family has a transport for one. It is also well
/// clear of the fastest cadence this repository calls ordinary — a book polled
/// once a second, which [`PollConfig`]'s own note names — so the floor cannot
/// refuse a number an operator picked. A floor that did would be worse than
/// the mistake it catches.
///
/// What it catches is **the unit slip**, which is the mistake this grammar
/// makes easy: `"1ms"` where `"1m"` was meant is one character and reads
/// correctly at a glance, and `"30ms"` for `"30m"` reads better than that.
/// Every `ms` value that could be a deliberate choice — `"100ms"`,
/// `"500ms"` — is above the floor, and every number small enough to be a slip
/// in `ns` or `us` is below it, which is what makes fifty the boundary rather
/// than a round number.
///
/// **Not a rule about units, though.** The check is one comparison against a
/// [`Duration`], so the two smaller units are not bounded by construction:
/// `"50000us"` and `"50000000ns"` are both fifty milliseconds and both
/// resolve, as does anything above them. That costs nothing — nobody states a
/// plausible cadence in microseconds — but it is what a later change to this
/// constant or to the comparison gets checked against, which is why
/// `a_cadence_below_the_floor_is_the_unit_slip_the_zero_refusal_does_not_reach`
/// pins a `us` value on each side of the boundary rather than only below it.
pub const MIN_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Why a polled `[[source]]` cannot be run.
///
/// Every variant names what *is* acceptable and not only what was wrong, which
/// is the core's own standard for the same reason: an error that says a value
/// is unacceptable and stops there invites the same guess a second time.
///
/// # The four that name the endpoint name only its authority
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
    /// **Refused at load, and this is the one check here the connect probe
    /// does not fail on at all.** A fragment is not sent to a server, so
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

    /// The endpoint carries a userinfo section.
    ///
    /// **Refused at load, because the connect probe fails on it and blames
    /// the wrong thing.** Nothing here sends a userinfo section: `hyper`
    /// derives `Host` from the URI's host, which excludes it, and synthesises
    /// no `Authorization` header — that belongs to a higher-level client and
    /// this crate does not add one, for the reason the crate docs give about
    /// redirects. So `http://poller:secret@host/catalogue` goes out as `GET
    /// /catalogue` with one `host` header and no credential anywhere on the
    /// wire.
    ///
    /// What an operator sees then depends on the endpoint, and both answers
    /// are bad ones. An endpoint that wanted the credential replies `401`,
    /// which is `connect_failures_total{reason="unauthorized"}` and means
    /// *look at the credential* — and the credential is right there in the
    /// document, spelled correctly, having never left the process. The
    /// publisher retries under the delay sequence for as long as it takes
    /// somebody to work out that the client dropped the field rather than the
    /// venue rejecting it, and the probe cannot say so, because from its side
    /// a `401` is a `401`. An endpoint that wanted none answers `200`, the
    /// feed runs, and a credential sits in a configuration document for
    /// nothing — the half that makes this worth refusing even where it appears
    /// to work.
    ///
    /// `authority_of` already assumes an operator writes these, because it
    /// strips one before printing. This is the other half of that assumption:
    /// a shape common enough to keep out of a log line is common enough to
    /// refuse.
    ///
    /// # What it refuses is narrower than what `authority_of` strips
    ///
    /// A userinfo section here is an `@` in the **authority** — before the
    /// first `/`, `?` or `#` — which is what a URI means by one and what
    /// `hyper` drops. `authority_of` is deliberately looser, taking the last
    /// `@` before the query string, because the two answer different
    /// questions: this one asks whether an endpoint can work, and that one
    /// asks what is safe to print, where over-stripping costs a host and
    /// under-stripping costs a secret.
    #[error(
        "`endpoint` is `{authority}` and carries a userinfo section, which nothing sends: the \
         `Host` header is derived from the host alone and no `Authorization` header is \
         synthesised, so the request goes out with no credential at all and the endpoint \
         answers `401` to a document that looks right. A polled endpoint is the scheme, the \
         host and the path; a venue that authenticates a catalogue request takes its key on \
         the query string, written either on `endpoint` itself or by the adapter through \
         `send`"
    )]
    CredentialInEndpoint { authority: String },

    /// A cadence of zero.
    ///
    /// Refused because it is the one value that turns this transport into the
    /// thing it exists instead of: a request in a loop, as fast as the endpoint
    /// will answer, which is how a publisher's address gets blocked rather than
    /// merely being wrong.
    #[error("`poll_interval` must be greater than zero: a cadence of zero is a request loop")]
    ZeroInterval,

    /// A cadence above zero and below [`MIN_POLL_INTERVAL`].
    ///
    /// **The unit slip, which [`ZeroInterval`](Self::ZeroInterval) does not
    /// reach.** `poll_interval = "1ms"` is one character away from `"1m"` and
    /// reads correctly at a glance, and it is a thousand requests a second at
    /// a venue's catalogue endpoint — which is exactly the outcome that
    /// variant names, *how a publisher's address gets blocked rather than
    /// merely being wrong*, arrived at through a value it accepts.
    ///
    /// Its own variant rather than a widening of the zero refusal, because the
    /// two are different sentences to read. Zero says *no cadence*, and the
    /// answer to it is the one that table's note gives: there is no defensible
    /// default, go and read what the endpoint will bear. A cadence of `1ms`
    /// says *this cadence*, and the operator who wrote it believes a number —
    /// so what they need is the floor, which is the one thing that says which
    /// of the two characters was wrong.
    ///
    /// See [`MIN_POLL_INTERVAL`] for why that number, and for why a floor is
    /// not the default this table deliberately omits.
    #[error(
        "`poll_interval` is `{stated:?}`, and the shortest cadence this transport runs is \
         50ms — twenty requests a second at one endpoint: below that a value is a unit slip \
         rather than a choice, `1ms` where `1m` was meant, and a catalogue asked a thousand \
         times a second is how a publisher's address gets blocked rather than merely being \
         wrong. A venue whose book has to be read faster than that is a subscription, and \
         `[ingress] kind = \"websocket\"` is the transport for one"
    )]
    IntervalBelowFloor { stated: Duration },
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
/// There is still a **floor**, and it is not the same claim as a default: no
/// number is defensible as *the* cadence, and a small enough number is not a
/// cadence at all. See [`MIN_POLL_INTERVAL`], which sits where the values
/// below it are unit slips rather than choices.
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
        // After the fragment, because a `#` before the `@` would make the
        // reading below a reading of the fragment rather than of an authority,
        // and because either is a fault in this same key. The authority is
        // everything from the scheme to the first `/`, `?` or `#`, which is
        // what a URI means by one and what `hyper` reads the host out of - so
        // an `@` inside it is a userinfo section by the only definition that
        // matters here, and an `@` further along is a character in a path.
        let after_scheme = endpoint
            .split_once("://")
            .map_or(endpoint, |(_scheme, rest)| rest);
        if after_scheme
            .split(['/', '?', '#'])
            .next()
            .unwrap_or_default()
            .contains('@')
        {
            return Err(ConfigError::CredentialInEndpoint {
                authority: authority_of(endpoint),
            });
        }
        if self.poll_interval.is_zero() {
            return Err(ConfigError::ZeroInterval);
        }
        // After zero rather than instead of it: the two are different
        // sentences to read, and an operator who wrote `0s` needs the one
        // about there being no defensible default rather than a number to
        // clear.
        if self.poll_interval < MIN_POLL_INTERVAL {
            return Err(ConfigError::IntervalBelowFloor {
                stated: self.poll_interval,
            });
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
///
/// # The order the three delimiters come off in is the correctness
///
/// The query string and the fragment first, then the userinfo, then the path —
/// and each boundary is where it is because of which of two wrong answers is
/// dangerous.
///
/// A `?` and a `#` open the two regions this function exists to drop whole, so
/// an `@` inside either is a character in somebody's value and never a userinfo
/// delimiter: reading one as a delimiter would print the tail of a query
/// string, which is where several venue APIs keep a key.
///
/// **The userinfo then comes off before the path, and not after it.** A
/// credential is frequently base64 and base64 carries a `/` about half the
/// time, so `http://poller:aBc/dEf@192.0.2.10/catalogue` is the ordinary shape
/// rather than an odd one. Cutting at the first `/` first reads `poller:aBc` as
/// the authority — there is no `@` left in it to strip — and prints the
/// password's first characters with the host gone, in the one line a supervisor
/// captures when a publisher will not start. Cutting at the last `@` first
/// costs the opposite mistake, an `@` in a path taken for a delimiter and a
/// path segment printed where a host was wanted, which is a wrong answer and
/// not a disclosed secret.
///
/// **That cost is paid on correct endpoints, and it is accepted.**
/// `https://real.host/v1/a@b` carries no userinfo section at all — its
/// authority ends at the first `/` and holds no `@`, which is why
/// [`ConfigError::CredentialInEndpoint`] does not fire on it — and this order
/// still renders it `https://b`, so a `TlsUnsupported` or a
/// `FragmentInEndpoint` on that endpoint names a host the operator never
/// wrote. A wrong host sends an operator back to a document they can read; a
/// disclosed secret cannot be taken back. That is the trade, and
/// `an_authority_keeps_the_host_and_drops_the_query_string_and_the_userinfo`
/// asserts the `https://b` so the cost is pinned rather than described.
pub(crate) fn authority_of(endpoint: &str) -> String {
    let (scheme, after_scheme) = match endpoint.split_once("://") {
        Some((scheme, after_scheme)) => (Some(scheme), after_scheme),
        // An endpoint that names no scheme is one of the two this module
        // refuses, and it is refused by name — so the host is still worth
        // printing and the query string still must not be.
        None => (None, endpoint),
    };
    let before_query = after_scheme.split(['?', '#']).next().unwrap_or_default();
    // A userinfo section is a credential more often than not, and this string
    // exists to be printed. `rsplit_once` and not `split_once`, because the
    // delimiter is the last `@`: a username that is an email address is
    // ordinary, and cutting at the first one would leave the domain of it in
    // front of the host.
    let after_userinfo = match before_query.rsplit_once('@') {
        Some((_userinfo, rest)) => rest,
        None => before_query,
    };
    let host = after_userinfo.split('/').next().unwrap_or_default();
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

    #[test]
    fn a_cadence_below_the_floor_is_the_unit_slip_the_zero_refusal_does_not_reach() {
        let mut config: PollConfig = toml::from_str(document()).expect("a valid table");
        // `"1ms"` for `"1m"`: one character, reads correctly at a glance, and
        // a thousand requests a second at a venue's catalogue endpoint -
        // which is the outcome `ZeroInterval`'s own doc names, reached by a
        // value it accepts.
        config.poll_interval = parse_duration("1ms").expect("a stated unit");
        assert_eq!(
            config.check(),
            Err(ConfigError::IntervalBelowFloor {
                stated: Duration::from_millis(1),
            })
        );

        // The message names what IS acceptable, which is this module's
        // standard, and it names it as a literal because a `thiserror` format
        // string takes one. This is what keeps the literal and the constant
        // from drifting apart.
        assert_eq!(format!("{MIN_POLL_INTERVAL:?}"), "50ms");
        let message = config.check().expect_err("below the floor").to_string();
        assert!(message.contains("50ms"), "{message}");

        // The floor itself is acceptable. A floor that refused its own value
        // is a floor nobody can state.
        config.poll_interval = MIN_POLL_INTERVAL;
        assert_eq!(config.check(), Ok(()));

        // And a book polled once a second is the fastest cadence this
        // repository calls ordinary - this type's own note says so - so the
        // floor has to be well clear of it. A floor that refused a number an
        // operator picked would be worse than the slip it catches.
        config.poll_interval = Duration::from_secs(1);
        assert_eq!(config.check(), Ok(()));

        // A slip in a louder unit is the same mistake and is refused the
        // same way. These are the numbers a slip actually produces, and not
        // every value the two smaller units can spell - see below.
        for raw in ["1ns", "999us", "49ms"] {
            config.poll_interval = parse_duration(raw).expect("a stated unit");
            assert!(
                matches!(config.check(), Err(ConfigError::IntervalBelowFloor { .. })),
                "`{raw}`: {:?}",
                config.check()
            );
        }

        // And the floor is one comparison against a duration rather than a
        // rule about units, which is the half the loop above cannot say: a
        // `us` value reaches the floor and goes past it. `50000us` is fifty
        // milliseconds spelled in the unit a slip arrives in and `60000us`
        // is beyond it, and both resolve. Nobody states a cadence that way,
        // so this is not a gap - it is the boundary asserted on the side
        // that a claim about `ns` and `us` being below the floor *by
        // construction* gets wrong.
        for raw in ["50000us", "60000us"] {
            config.poll_interval = parse_duration(raw).expect("a stated unit");
            assert_eq!(config.check(), Ok(()), "`{raw}`");
        }
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
    fn an_endpoint_carrying_a_credential_is_refused_because_the_probe_blames_the_venue() {
        // Verified against `hyper` rather than assumed: an endpoint written
        // this way goes out as `GET /catalogue` with one `host` header and no
        // `Authorization` header at all, because the `Host` is derived from
        // the host alone and nothing here synthesises the other. So the
        // request is unauthenticated, the endpoint answers `401`, and
        // `connect_failures_total{reason="unauthorized"}` sends an operator to
        // look at a credential which is spelled correctly in the document and
        // has never left the process.
        let mut config: PollConfig = toml::from_str(document()).expect("a valid table");
        config.endpoint = "http://poller:not-a-real-secret@192.0.2.10/catalogue".to_string();
        let error = config
            .check()
            .expect_err("a userinfo section in the endpoint");
        assert_eq!(
            error,
            ConfigError::CredentialInEndpoint {
                authority: "http://192.0.2.10".to_string(),
            }
        );
        // The rule every refusal in this module keeps, and the one that
        // matters most for this variant: the value it names is the credential.
        for rendered in [format!("{error}"), format!("{error:?}")] {
            assert!(!rendered.contains("not-a-real-secret"), "{rendered}");
            assert!(!rendered.contains("poller"), "{rendered}");
            assert!(rendered.contains("192.0.2.10"), "{rendered}");
        }
        // What the operator does next, which is the standard the other
        // variants hold: where the key goes instead.
        let message = error.to_string();
        assert!(message.contains("query string"), "{message}");

        // A username with no password is the same fault and is refused the
        // same way: nothing sends either half. `http` and not `https`,
        // because this test has to mean the same thing in both builds and a
        // build with no TLS stack refuses the scheme first.
        config.endpoint = "http://poller@192.0.2.10:8443/catalogue".to_string();
        assert!(
            matches!(
                config.check(),
                Err(ConfigError::CredentialInEndpoint { .. })
            ),
            "{:?}",
            config.check()
        );

        // And an `@` past the authority is a character in a path, not a
        // credential. This is the narrower reading the variant's own doc
        // states, and the reason it is narrower than what `authority_of`
        // strips: refusing an endpoint that works is its own failure.
        config.endpoint = "http://192.0.2.10/books/me@example.com/catalogue".to_string();
        assert_eq!(config.check(), Ok(()));
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

        // **The cost of taking the userinfo off before the path, pinned.** An
        // `@` past the authority is a character in a path and not a
        // delimiter, but this function cuts at the last one while the path is
        // still there, so a correct endpoint carrying one renders as the tail
        // of its own path. `check` does not refuse this endpoint - its
        // authority ends at the first `/` and holds no `@` - so the value
        // below is what a `TlsUnsupported` or a `FragmentInEndpoint` on it
        // names. It is the accepted half of the trade this function's own
        // note argues, asserted rather than described because the other half
        // is a prefix of a credential.
        assert_eq!(authority_of("https://real.host/v1/a@b"), "https://b");
    }

    #[test]
    fn a_userinfo_section_carrying_a_slash_leaves_no_prefix_of_the_credential_behind() {
        // **The order the delimiters come off in, stated as the failure it
        // prevents.** A credential is frequently base64 and base64 carries a
        // `/` about half the time, so this endpoint is the ordinary shape
        // rather than an odd one. Cut the path off first and `poller:aBc` is
        // what is left — no `@` in it to strip — so the function returns
        // `http://poller:aBc`: the password's first characters printed and the
        // host gone, in the value all three endpoint-naming `ConfigError`s
        // carry, in `PollConfig`'s, `PollInput`'s and `Request`'s `Debug`, and
        // in front of every error detail in `poll.rs` — which is the startup
        // line a supervisor captures when the process will not start.
        const CREDENTIAL: &str = "aBc/dEf-not-a-real-secret";
        let rendered = authority_of(&format!("http://poller:{CREDENTIAL}@192.0.2.10/catalogue"));
        assert_eq!(rendered, "http://192.0.2.10");

        // Every prefix and not only the whole of it, because what the wrong
        // order printed was a prefix: three characters of a password is not a
        // disclosure to argue about, it is a disclosure.
        for length in 1..=CREDENTIAL.len() {
            let prefix = &CREDENTIAL[..length];
            assert!(
                !rendered.contains(prefix),
                "`{prefix}` is the start of the credential and it survived into `{rendered}`"
            );
        }
        // And the half an operator needs is still there, which is the other
        // half of the same mistake: the wrong order lost the host entirely, so
        // the refusal named neither the endpoint safely nor usefully.
        assert!(rendered.contains("192.0.2.10"), "{rendered}");

        // An `@` inside a query string is a character in somebody's value and
        // not a userinfo delimiter, so the two boundaries have to be read in
        // this order too: taking the last `@` in the whole endpoint would
        // print the tail of the query string, which is where several venue
        // APIs keep a key.
        assert_eq!(
            authority_of("http://192.0.2.10/catalogue?login=poller@not-a-real-secret"),
            "http://192.0.2.10"
        );
    }
}
