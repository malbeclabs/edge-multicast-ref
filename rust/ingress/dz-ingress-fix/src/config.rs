//! `[source.upstream.session]`: the transport's own keys, and the one refusal
//! that matters.
//!
//! # Where this table lives, and why it is its own
//!
//! `[[source]] upstream` is the venue's table, deserialized by the venue's own
//! code. This is a *sub*table of it, so the transport's keys and a venue's keys
//! cannot collide and neither has to know about the other:
//!
//! ```toml
//! [[source]]
//! name = "mktdata"
//! ingress = "fix"
//!
//! [source.upstream.session]
//! endpoint = "203.0.113.10:9443"
//! server_name = "session.example.com"
//!
//! [source.upstream]
//! # the venue's own keys for this `[[source]]` block
//! ```
//!
//! `deny_unknown_fields`, for the reason `[ingress]` has it: a misspelled key
//! that parses cleanly and falls back to a default is a publisher running
//! something its operator does not believe it is running.
//!
//! # Sequence continuity is refused here rather than discovered at logon
//!
//! This transport resets its outbound sequence at every logon, and that is a
//! decision rather than an omission — see [`Session::open`](crate::Session).
//! **A venue that requires continuity is a venue this transport does not
//! serve**, and it is told so at load, naming the key. The alternative is a
//! transport that silently resets against a venue expecting continuity, which
//! produces a session the venue tears down for a reason our own logs will not
//! carry.

use std::net::IpAddr;

use serde::Deserialize;

/// The transport's own keys for one session.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionConfig {
    /// `host:port`, or `address:port`.
    ///
    /// No scheme: this protocol has no URL form, and a key that accepted one
    /// would be a second way to spell the same thing.
    pub endpoint: String,

    /// The name the certificate is verified against, when it is not the
    /// endpoint's own host.
    ///
    /// Needed where the endpoint is an address and the certificate names a
    /// host, which is the ordinary shape for a venue reached over a private
    /// path.
    #[serde(default)]
    pub server_name: Option<String>,

    /// Whether to negotiate TLS. **On by default.**
    ///
    /// `false` exists for one purpose: exercising the framing and the session
    /// lifecycle against a loopback endpoint, which is the half no fake proves.
    /// It is therefore accepted **only** for a loopback endpoint — see
    /// [`SessionConfigError::PlaintextOffLoopback`]. A venue endpoint is never
    /// plaintext, and a key that could quietly make one so is a key that will
    /// eventually be set in a document nobody re-read.
    #[serde(default = "yes")]
    pub tls: bool,

    /// Whether the outbound sequence carries across a logon.
    ///
    /// **Declared so that it can be refused by name.** `true` is a venue this
    /// transport does not serve, and `false` — or the key's absence — is what
    /// it does. A key that was simply unknown would be refused as a typo, which
    /// sends an operator hunting for a spelling mistake in a value spelled
    /// correctly.
    #[serde(default)]
    pub persist_sequence: bool,
}

/// `true`, as a function, because serde defaults are functions.
const fn yes() -> bool {
    true
}

/// Why a session cannot be run as configured.
///
/// Every variant is a document to change rather than a condition that clears,
/// so a transport maps all of them onto
/// [`IngressError::Fatal`](dz_ingress_core::IngressError::Fatal): a process
/// that exits loudly at startup is diagnosable and a driver that hides the same
/// fault behind a backoff is not.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SessionConfigError {
    /// The document asks for a sequence that carries across a logon.
    #[error(
        "`persist_sequence = true` asks this transport to carry its outbound sequence across a \
         logon, which it does not do: the sequence resets at every logon and nothing is \
         persisted, because a resend delivers deltas whose value has expired and the \
         publisher's own snapshot recovery is the better repair. A venue that requires \
         continuity is a venue this transport does not serve — state `persist_sequence = \
         false`, or remove the key, to say that this one does not"
    )]
    SequenceContinuity,

    /// `tls = false` on an endpoint that is not loopback.
    #[error(
        "`tls = false` is accepted only for a loopback endpoint, and `{endpoint}` is not one. \
         It exists for exercising the framing and the session lifecycle against a local \
         endpoint; a venue session is negotiated"
    )]
    PlaintextOffLoopback { endpoint: String },

    /// The endpoint is not `host:port`.
    #[error("`endpoint = \"{endpoint}\"` is not `host:port`: {detail}")]
    Endpoint { endpoint: String, detail: String },
}

/// A checked endpoint: what to connect to, and what to verify against.
///
/// Separate from [`SessionConfig`] for the reason
/// [`Policy`](dz_ingress_core::Policy) is separate from
/// `IngressConfig`: what the transport takes is what has been checked, so a
/// document asking for sequence continuity cannot reach one and the transport
/// has no case for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// `host:port`, as the document wrote it.
    pub address: String,
    /// The host part alone, which is what the certificate is verified against
    /// unless `server_name` said otherwise.
    pub server_name: String,
    /// Whether to negotiate TLS.
    pub tls: bool,
}

impl SessionConfig {
    /// Check the document and produce what the transport takes.
    ///
    /// # Errors
    ///
    /// [`SessionConfigError`], naming the key that was refused and what would
    /// have been accepted instead.
    pub fn resolve(&self) -> Result<Endpoint, SessionConfigError> {
        // The refusal first. A document that asks for continuity is wrong
        // whatever its endpoint is, and reporting the endpoint ahead of it
        // would hide a decision nobody can honour behind an address to
        // correct.
        if self.persist_sequence {
            return Err(SessionConfigError::SequenceContinuity);
        }
        let host = host_of(&self.endpoint)?;
        if !self.tls && !is_loopback(host) {
            return Err(SessionConfigError::PlaintextOffLoopback {
                endpoint: self.endpoint.clone(),
            });
        }
        Ok(Endpoint {
            address: self.endpoint.clone(),
            server_name: self.server_name.clone().unwrap_or_else(|| host.to_owned()),
            tls: self.tls,
        })
    }
}

/// The host part of `host:port`.
///
/// Split from the right, so that a bracketed address literal keeps its colons.
fn host_of(endpoint: &str) -> Result<&str, SessionConfigError> {
    let (host, port) = endpoint
        .rsplit_once(':')
        .ok_or_else(|| SessionConfigError::Endpoint {
            endpoint: endpoint.to_owned(),
            detail: "there is no `:port`".to_owned(),
        })?;
    if host.is_empty() {
        return Err(SessionConfigError::Endpoint {
            endpoint: endpoint.to_owned(),
            detail: "there is no host before the `:`".to_owned(),
        });
    }
    port.parse::<u16>()
        .map_err(|_| SessionConfigError::Endpoint {
            endpoint: endpoint.to_owned(),
            detail: format!("`{port}` is not a port"),
        })?;
    Ok(host.trim_start_matches('[').trim_end_matches(']'))
}

/// Whether a host is unambiguously this machine.
///
/// A literal address is decided by the address; a name is decided only for
/// `localhost`. Every other name is *not* loopback as far as this check is
/// concerned, and deliberately: a name resolves to whatever the resolver says
/// today, and `tls = false` must not become negotiable by way of a DNS record.
fn is_loopback(host: &str) -> bool {
    if let Ok(address) = host.parse::<IpAddr>() {
        return address.is_loopback();
    }
    host.eq_ignore_ascii_case("localhost")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A configuration from the text an operator writes.
    fn from_document(document: &str) -> Result<SessionConfig, toml::de::Error> {
        toml::from_str(document)
    }

    #[test]
    fn a_document_asking_for_sequence_continuity_is_refused_naming_the_key() {
        // The revert this test exists for: accept the key and reset anyway. A
        // transport that silently resets against a venue expecting continuity
        // produces a session the venue tears down for a reason our logs do not
        // carry.
        let config = from_document("endpoint = \"203.0.113.10:9443\"\npersist_sequence = true\n")
            .expect("the key parses");
        let error = config.resolve().expect_err("continuity is not served");
        assert_eq!(error, SessionConfigError::SequenceContinuity);
        let message = error.to_string();
        assert!(message.contains("persist_sequence"), "{message}");
        // And what would have been accepted, not only what was wrong.
        assert!(message.contains("persist_sequence = false"), "{message}");
    }

    #[test]
    fn the_absent_key_and_the_stated_false_are_the_same_document() {
        for document in [
            "endpoint = \"203.0.113.10:9443\"\n",
            "endpoint = \"203.0.113.10:9443\"\npersist_sequence = false\n",
        ] {
            let config = from_document(document).expect("a document");
            assert!(config.resolve().is_ok(), "{document}");
        }
    }

    #[test]
    fn the_server_name_defaults_to_the_endpoints_own_host() {
        let config =
            from_document("endpoint = \"session.example.com:9443\"\n").expect("a document");
        let endpoint = config.resolve().expect("a usable endpoint");
        assert_eq!(endpoint.server_name, "session.example.com");
        assert!(endpoint.tls, "TLS is on unless a document turns it off");
    }

    #[test]
    fn a_stated_server_name_is_what_the_certificate_is_verified_against() {
        // The ordinary shape for a venue reached over a private path: the
        // endpoint is an address and the certificate names a host.
        let config = from_document(
            "endpoint = \"203.0.113.10:9443\"\nserver_name = \"session.example.com\"\n",
        )
        .expect("a document");
        let endpoint = config.resolve().expect("a usable endpoint");
        assert_eq!(endpoint.server_name, "session.example.com");
        assert_eq!(endpoint.address, "203.0.113.10:9443");
    }

    #[test]
    fn plaintext_is_accepted_for_a_loopback_endpoint_and_nowhere_else() {
        for loopback in ["127.0.0.1:9443", "[::1]:9443", "localhost:9443"] {
            let config = from_document(&format!("endpoint = \"{loopback}\"\ntls = false\n"))
                .expect("a document");
            let endpoint = config
                .resolve()
                .unwrap_or_else(|error| panic!("{loopback}: {error}"));
            assert!(!endpoint.tls);
        }
        for elsewhere in ["203.0.113.10:9443", "session.example.com:9443"] {
            let config = from_document(&format!("endpoint = \"{elsewhere}\"\ntls = false\n"))
                .expect("a document");
            let error = config.resolve().expect_err("a venue session is negotiated");
            assert_eq!(
                error,
                SessionConfigError::PlaintextOffLoopback {
                    endpoint: elsewhere.to_owned()
                }
            );
        }
    }

    #[test]
    fn a_name_that_is_not_localhost_is_not_loopback_whatever_it_resolves_to() {
        // A name resolves to whatever the resolver says today, so `tls = false`
        // must not become negotiable by way of a DNS record.
        assert!(is_loopback("127.0.0.1"));
        assert!(is_loopback("127.5.5.5"));
        assert!(is_loopback("::1"));
        assert!(is_loopback("LOCALHOST"));
        assert!(!is_loopback("localhost.example.com"));
        assert!(!is_loopback("203.0.113.10"));
    }

    #[test]
    fn an_endpoint_that_is_not_host_and_port_says_which_half_is_missing() {
        for (endpoint, expected) in [
            ("203.0.113.10", "there is no `:port`"),
            (":9443", "there is no host before the `:`"),
            ("203.0.113.10:https", "is not a port"),
            ("203.0.113.10:99999", "is not a port"),
        ] {
            let config =
                from_document(&format!("endpoint = \"{endpoint}\"\n")).expect("a document");
            let error = config.resolve().expect_err("not an endpoint");
            assert!(error.to_string().contains(expected), "{endpoint}: {error}");
        }
    }

    #[test]
    fn a_misspelled_key_is_a_startup_failure_and_not_a_default() {
        // `deny_unknown_fields` is the load-bearing attribute rather than a
        // tidiness one: the audit's own failure was a misspelled section that
        // parsed cleanly and fell back to a default.
        let error = from_document("endpoint = \"203.0.113.10:9443\"\npersist_seqence = true\n")
            .expect_err("a misspelled key is refused");
        assert!(error.to_string().contains("persist_seqence"), "{error}");
    }
}
