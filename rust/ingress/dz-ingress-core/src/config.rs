//! The `[ingress]` section, parsed here so that it cannot be spelled two ways.
//!
//! Six values appear in every existing publisher and most are spelled two or
//! three ways each. The rule that fixes it is that each shared crate parses its
//! own section: keys, types and defaults then cannot drift between venues,
//! because there is one implementation of them.
//!
//! What this crate does *not* do is load a file. The runtime owns the document
//! and hands this section over, so nothing here needs a TOML parser — only a
//! deserializer.

use std::time::Duration;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer};

use crate::backoff::BackoffPolicy;
use crate::error::ConfigError;
use crate::kind::Kind;
use crate::limit::RateLimiter;

/// The `[ingress]` table.
///
/// `deny_unknown_fields`, and that is the load-bearing attribute rather than a
/// tidiness one: a publisher in the audit had a misspelled section parse
/// cleanly, fall back to a default, and run a transport its operator did not
/// believe it was running. A key nobody reads is the same failure one level
/// down.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngressConfig {
    /// Which transport, for a publisher with one source.
    ///
    /// **Optional, and still with no default.** A publisher may name its
    /// transport here or once per `[[source]]` block, and naming it in both
    /// places is refused rather than resolved in favour of one — a key that is
    /// read only when another is absent is a key an operator cannot reason
    /// about. Naming it in neither is refused too, and the error says both
    /// ways of stating it. See [`Self::resolve`] and [`Self::policy`].
    #[serde(default)]
    pub kind: Option<String>,

    /// How long a connect attempt may take before it counts as failed.
    #[serde(default = "default_connect_timeout", deserialize_with = "de_duration")]
    pub connect_timeout: Duration,

    /// The first delay before a retry, and the one a proven connection resets
    /// to.
    #[serde(default = "default_backoff_initial", deserialize_with = "de_duration")]
    pub reconnect_backoff_initial: Duration,

    /// The ceiling on that delay.
    #[serde(default = "default_backoff_max", deserialize_with = "de_duration")]
    pub reconnect_backoff_max: Duration,

    /// Messages per second this publisher may send upstream. `0` disables the
    /// pacing. See [`RateLimiter`](crate::RateLimiter) for what it does and does
    /// not limit.
    #[serde(default)]
    pub rate_limit_per_second: u32,

    /// How long a connection may go without delivering a payload before it is
    /// treated as dead and reconnected. Absent disables the guard.
    ///
    /// # Why this key is here and why it is not `idle_guard`
    ///
    /// Two silences exist and conflating them lets one quiet channel restart
    /// every other. `[[feed]] idle_guard` is *feed* silence — a property of one
    /// channel's published set, where a channel whose instruments are dormant is
    /// silent and healthy. This is *upstream* silence, a property of the
    /// connection, and it alone justifies a reconnect. They are deliberately
    /// spelled differently so that neither can be read as the other.
    ///
    /// The guard is what catches an adapter that connects and subscribes to
    /// nothing, and — the case it is really for — a venue that has quietly
    /// dropped a subscription while keeping the socket up. See
    /// [`Received::Liveness`](crate::Received) for why the socket being up is
    /// no evidence at all.
    #[serde(default, deserialize_with = "de_optional_duration")]
    pub idle_timeout: Option<Duration>,
}

/// Everything the driver needs to run, validated.
///
/// Separate from [`IngressConfig`] because the driver takes what has been
/// checked, not what was written: a backoff pair that is the wrong way round
/// cannot reach a [`Policy`], so the driver has no case for it. It is also
/// constructible directly, which is what lets a test state a policy without a
/// document.
#[derive(Debug, Clone, Copy)]
pub struct Policy {
    /// Passed to [`Input::connect`](crate::Input::connect).
    pub connect_timeout: Duration,
    /// The reconnect delay sequence.
    pub backoff: BackoffPolicy,
    /// Outbound pacing; `0` for none.
    pub rate_limit_per_second: u32,
    /// The upstream silence guard; `None` for none.
    pub idle_timeout: Option<Duration>,
}

impl IngressConfig {
    /// Resolve the transport and validate the policy, together.
    ///
    /// Together on purpose: a policy is of no use without a transport to apply
    /// it to, and returning one without the other invites a caller that builds
    /// a policy and then resolves the kind somewhere else, where a second
    /// default can appear.
    ///
    /// # Errors
    ///
    /// [`ConfigError`], naming both the value that was refused and the values
    /// that would have been accepted.
    pub fn resolve(&self) -> Result<(Kind, Policy), ConfigError> {
        let policy = self.policy()?;
        let kind = Kind::resolve(self.kind.as_deref().ok_or(ConfigError::NoKind)?)?;
        Ok((kind, policy))
    }

    /// The policy alone, for a document whose transports are named per source.
    ///
    /// Split out rather than duplicated: every check below applies whichever
    /// way the transport is named, and a second copy of them is a second place
    /// for a rule to be forgotten. [`resolve`](Self::resolve) is this plus the
    /// document-level `kind`, and the two are deliberately not offered as one
    /// function that sometimes returns a transport — a caller that has sources
    /// must not be handed a `Kind` it has no use for.
    ///
    /// # Errors
    ///
    /// [`ConfigError`], naming both the value that was refused and what would
    /// have been accepted.
    pub fn policy(&self) -> Result<Policy, ConfigError> {
        // The durations first, and the transport second. Both are reported one
        // at a time, so the order decides which an operator sees: a backoff
        // pair that is the wrong way round is wrong whichever transport runs,
        // and reporting the transport ahead of it would hide a bad value behind
        // a build that has to be redone first.
        if self.connect_timeout.is_zero() {
            return Err(ConfigError::ZeroDuration {
                key: "connect_timeout",
            });
        }
        if self.idle_timeout == Some(Duration::ZERO) {
            // A zero guard would end every connection the instant it came up,
            // which reads in the metrics exactly like a venue refusing us.
            return Err(ConfigError::ZeroDuration {
                key: "idle_timeout",
            });
        }
        // After the durations and before the transport, for the same reason
        // they are in that order: a rate the clock cannot express is wrong
        // whichever transport runs.
        if self.rate_limit_per_second > RateLimiter::FINEST_PER_SECOND {
            return Err(ConfigError::RateTooFine {
                key: "rate_limit_per_second",
                stated: self.rate_limit_per_second,
                most: RateLimiter::FINEST_PER_SECOND,
            });
        }
        let backoff =
            BackoffPolicy::new(self.reconnect_backoff_initial, self.reconnect_backoff_max)?;
        Ok(Policy {
            connect_timeout: self.connect_timeout,
            backoff,
            rate_limit_per_second: self.rate_limit_per_second,
            idle_timeout: self.idle_timeout,
        })
    }
}

fn default_connect_timeout() -> Duration {
    Duration::from_secs(5)
}

fn default_backoff_initial() -> Duration {
    Duration::from_millis(500)
}

fn default_backoff_max() -> Duration {
    Duration::from_secs(30)
}

/// Durations are written with a unit — `"500ms"` — and the unit is not
/// optional.
///
/// The units and the spelling are the recorder's, on purpose. One publisher
/// suffixes its duration keys `_seconds` and takes integers while others parse
/// strings, and a second syntax in this repository would be the same divergence
/// arriving from the other direction. This is the second implementation of it
/// in this repository, and it should be the last: the third is the point at
/// which it needs one home rather than a copy per crate that parses a section.
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

fn de_optional_duration<'de, D: Deserializer<'de>>(de: D) -> Result<Option<Duration>, D::Error> {
    let raw = Option::<String>::deserialize(de)?;
    raw.map(|raw| parse_duration(&raw).map_err(D::Error::custom))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_unit_the_error_message_offers_is_a_unit_the_parser_takes() {
        // The message and the match arms are two lists, and a unit named in one
        // and missing from the other is a documented syntax that does not
        // parse.
        for unit in ["ns", "us", "ms", "s", "m", "h"] {
            assert!(
                parse_duration(&format!("1{unit}")).is_ok(),
                "`1{unit}` was offered by the error message and refused"
            );
        }
    }

    #[test]
    fn a_bare_number_is_refused_rather_than_guessed_at() {
        // The publisher that spells its keys `_seconds` and takes integers is
        // the reason: `30` is thirty of something, and picking a unit for it is
        // how a heartbeat interval becomes thirty milliseconds.
        let error = parse_duration("30").expect_err("a bare number has no unit");
        assert!(error.contains("no unit"), "{error}");
    }

    #[test]
    fn the_units_convert_as_they_are_spelled() {
        assert_eq!(parse_duration("500ms"), Ok(Duration::from_millis(500)));
        assert_eq!(parse_duration("30s"), Ok(Duration::from_secs(30)));
        assert_eq!(parse_duration("2m"), Ok(Duration::from_secs(120)));
        assert_eq!(parse_duration("1h"), Ok(Duration::from_secs(3_600)));
        assert_eq!(parse_duration("250us"), Ok(Duration::from_micros(250)));
        assert_eq!(parse_duration("40ns"), Ok(Duration::from_nanos(40)));
    }

    #[test]
    fn a_unit_that_is_not_one_names_the_units_that_are() {
        let error = parse_duration("5sec").expect_err("`sec` is not a unit");
        for unit in ["ns", "us", "ms", "s", "m", "h"] {
            assert!(error.contains(unit), "{error}");
        }
    }
}
