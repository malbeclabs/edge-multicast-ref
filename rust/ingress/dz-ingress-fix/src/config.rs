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

use std::net::{IpAddr, Ipv6Addr};

use serde::Deserialize;
use tokio_rustls::rustls::pki_types::ServerName;

/// The transport's own keys for one session.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionConfig {
    /// `host:port`, `address:port`, or `[address]:port` for an IPv6 literal.
    ///
    /// No scheme: this protocol has no URL form, and a key that accepted one
    /// would be a second way to spell the same thing. One way to write each of
    /// the three shapes, for the same reason — an IPv6 literal is bracketed and
    /// an unbracketed host holds no colon — and a value that is neither is
    /// refused at load naming it. See [`host_of`].
    pub endpoint: String,

    /// The name the certificate is verified against, when it is not the
    /// endpoint's own host.
    ///
    /// Needed where the endpoint is an address and the certificate names a
    /// host, which is the ordinary shape for a venue reached over a private
    /// path.
    ///
    /// Checked at load like the endpoint is, and against the same verifier the
    /// negotiation would use — see
    /// [`SessionConfigError::ServerName`]. A value no certificate can be
    /// verified against is a document to correct, and correcting it at the
    /// first connect is three layers and a backoff away from the key that
    /// caused it.
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

    /// The name the certificate would be verified against is not a name.
    ///
    /// Refused at load rather than at the first connect, which is where the
    /// negotiation's own `ServerName::try_from` would raise it — and where it
    /// would be a fatal error under a backoff naming a socket, on a publisher
    /// that started cleanly. On a `role = "comparison"` source that fatal kills
    /// one driver and leaves a process that looks healthy with one upstream
    /// that never connects, which is the shape this refusal exists to prevent.
    #[error(
        "`server_name = \"{server_name}\"` is not a name a certificate can be verified \
         against: state a DNS name, or an IP address literal for a certificate that names \
         an address, or remove the key to verify against the host in `endpoint`"
    )]
    ServerName { server_name: String },
}

/// A checked endpoint: what to connect to, and what to verify against.
///
/// Separate from [`SessionConfig`] for the reason
/// [`Policy`](dz_ingress_core::Policy) is separate from
/// `IngressConfig`: what the transport takes is what has been checked, so a
/// document asking for sequence continuity cannot reach one and the transport
/// has no case for it.
///
/// # The fields are private, because that last sentence has to be true
///
/// [`SessionConfig::resolve`] is the only thing that builds one, and that is
/// what makes "what the transport takes is what has been checked" a property
/// of the type rather than a habit of this crate's own call sites. Public
/// fields offered two things at once — reading a value that has been checked,
/// and composing one that has not — and only the first was ever wanted.
/// `Endpoint { address: "203.0.113.10:9443", tls: false, .. }` handed to
/// [`SocketConnector::new`](crate::SocketConnector::new) is a plaintext
/// session to a venue, which is the one thing
/// [`SessionConfigError::PlaintextOffLoopback`] exists to refuse: the refusal
/// was in `resolve` and the door beside it stood open, so the safeguard held
/// for a document and not for a caller of this library.
///
/// Checking it a second time in the connector was the alternative, and this
/// family argues against that in the crate next door. The rule is `host_of`
/// and `is_loopback` together — both private to this module — so a connector
/// that made it again would be a second home for it, and "a second copy of
/// them is a second place for a rule to be forgotten" is
/// [`IngressConfig::policy`](dz_ingress_core::IngressConfig::policy)'s own
/// reasoning about its own checks. The forgetting is not hypothetical here: a
/// rule split across two files diverges the first time one of them learns
/// something the other does not about which addresses are this machine, and
/// the copy that did not learn is the one a venue endpoint goes through. A
/// private field cannot be forgotten, and what it costs is a constructor
/// nothing outside this module was calling.
///
/// So there is no unchecked construction left to refuse, because there is no
/// unchecked construction:
///
/// ```compile_fail,E0451
/// // Plaintext to an address that is not this machine. The refusal is that
/// // this does not compile — which is what pins it, because a check here
/// // could only ever be a copy of `resolve`'s.
/// let endpoint = dz_ingress_fix::Endpoint {
///     address: "203.0.113.10:9443".to_owned(),
///     server_name: "203.0.113.10".to_owned(),
///     tls: false,
/// };
/// ```
///
/// The door is the check, and it refuses by name:
///
/// ```
/// use dz_ingress_fix::{SessionConfig, SessionConfigError};
///
/// let document = SessionConfig {
///     endpoint: "203.0.113.10:9443".to_owned(),
///     server_name: None,
///     tls: false,
///     persist_sequence: false,
/// };
/// assert_eq!(
///     document.resolve(),
///     Err(SessionConfigError::PlaintextOffLoopback {
///         endpoint: "203.0.113.10:9443".to_owned(),
///     })
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    address: String,
    server_name: String,
    tls: bool,
}

impl Endpoint {
    /// `host:port`, as the document wrote it.
    ///
    /// Readable because a failure has to name it: this is what
    /// [`Connector::authority`](crate::Connector::authority) hands back, and
    /// what every connect refusal, timeout and negotiation failure carries in
    /// its detail. An operator reading one wants to know which endpoint it was
    /// about.
    #[must_use]
    pub fn address(&self) -> &str {
        &self.address
    }

    /// The name the certificate is verified against — the endpoint's own host
    /// unless `server_name` said otherwise.
    ///
    /// Resolved at load rather than left to the negotiation, so that the key's
    /// absence and the key's presence are held to the same check. See
    /// [`SessionConfigError::ServerName`].
    #[must_use]
    pub fn server_name(&self) -> &str {
        &self.server_name
    }

    /// Whether to negotiate TLS.
    ///
    /// `false` only for a loopback endpoint, and that is the whole of what a
    /// reader can do with it: asking is public and answering differently is
    /// not.
    #[must_use]
    pub const fn tls(&self) -> bool {
        self.tls
    }
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
        let server_name = self.server_name.clone().unwrap_or_else(|| host.to_owned());
        // **The same check the negotiation would make, made here.** What comes
        // out of this is what the transport takes, so a `server_name` that is
        // not a name must not reach one: `ServerName::try_from` inside the
        // negotiation is a fatal error at the *first connect*, which is a
        // publisher that started cleanly and a document nobody is looking at
        // any more. `server_name = "session example.com"` and `server_name =
        // ""` are both that, and both are a key to correct.
        //
        // Checked whether or not `tls` is on, because a document is refused for
        // what it says rather than for what today's other keys make of it: a
        // name left unverifiable under `tls = false` is a document that breaks
        // on the change that turns negotiation on, which is the change nobody
        // re-reads this key for. And checked against the resolved name rather
        // than the stated one, so that the endpoint's own host — which is what
        // the key's absence means — is held to the same thing.
        if ServerName::try_from(server_name.as_str()).is_err() {
            return Err(SessionConfigError::ServerName { server_name });
        }
        Ok(Endpoint {
            address: self.endpoint.clone(),
            server_name,
            tls: self.tls,
        })
    }
}

/// The host part of `host:port`, and a refusal for anything that is not that
/// shape.
///
/// Two ways to write a host, and one way to write each:
///
/// - `[address]:port`. The brackets mean an IPv6 address literal, so a `]` has
///   to close them, `:port` has to follow that `]` immediately, and what is
///   between them has to parse as an address.
/// - `host:port`. One colon, and none inside the host.
///
/// **Both halves are checked here rather than left to the connect.** Splitting
/// at the last colon and stripping brackets wherever they appear accepts three
/// values that are not endpoints — `[::1:9443` with nothing closing the
/// bracket, `host::9443` with a colon too many, and a bare `::1:9443` whose
/// colons are the address's own — and each of them resolves to a host and a
/// port that look usable. What follows is a connect that fails under a backoff,
/// three layers from the document that caused it, on an error naming a socket
/// rather than a key. A configuration mistake is refused at load, naming the
/// value, which is what an operator can act on.
fn host_of(endpoint: &str) -> Result<&str, SessionConfigError> {
    let refuse = |detail: &str| SessionConfigError::Endpoint {
        endpoint: endpoint.to_owned(),
        detail: detail.to_owned(),
    };
    let (host, port) = if let Some(bracketed) = endpoint.strip_prefix('[') {
        let (inside, after) = bracketed
            .split_once(']')
            .ok_or_else(|| refuse("a `[` opens an address literal and nothing closes it"))?;
        if inside.parse::<Ipv6Addr>().is_err() {
            return Err(refuse(
                "the brackets mean an IPv6 address literal, and what is between them is not one",
            ));
        }
        let port = after
            .strip_prefix(':')
            .ok_or_else(|| refuse("the closing `]` is not followed by `:port`"))?;
        (inside, port)
    } else {
        let (host, port) = endpoint
            .rsplit_once(':')
            .ok_or_else(|| refuse("there is no `:port`"))?;
        if host.contains(':') {
            return Err(refuse(
                "an unbracketed host holds no colon; an IPv6 address literal is written \
                 `[address]:port`",
            ));
        }
        (host, port)
    };
    if host.is_empty() {
        return Err(refuse("there is no host before the `:`"));
    }
    if port.parse::<u16>().is_err() {
        return Err(SessionConfigError::Endpoint {
            endpoint: endpoint.to_owned(),
            detail: format!("`{port}` is not a port"),
        });
    }
    Ok(host)
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
    fn a_server_name_no_certificate_can_be_verified_against_is_refused_at_load() {
        // The revert this test exists for: pass the key through and let the
        // negotiation's own `ServerName::try_from` raise it. The publisher then
        // starts cleanly and the fault arrives at the first connect, three
        // layers and a backoff from the key that caused it — and on a `role =
        // "comparison"` source that fatal kills one driver and leaves a process
        // that looks healthy with an upstream that never connects.
        //
        // A space is the plausible value: a name copied out of a sentence with
        // the word before it. An empty string is the other, and it is what an
        // operator writes to mean "the endpoint's own host" — which is what
        // leaving the key out means, and what this message says.
        for stated in ["session example.com", "", "https://session.example.com"] {
            let config = from_document(&format!(
                "endpoint = \"203.0.113.10:9443\"\nserver_name = \"{stated}\"\n"
            ))
            .expect("the key parses");
            let error = config
                .resolve()
                .expect_err(&format!("`{stated}` is not a name"));
            assert_eq!(
                error,
                SessionConfigError::ServerName {
                    server_name: stated.to_owned()
                },
                "{stated}"
            );
            let message = error.to_string();
            // The value it refused, and what would have been accepted instead.
            assert!(message.contains("server_name"), "{message}");
            assert!(message.contains("remove the key"), "{message}");
        }
    }

    #[test]
    fn the_endpoints_own_host_is_held_to_the_same_name_the_key_is() {
        // The key's absence means the endpoint's host, so the check has to be
        // on the resolved name: a host that reaches `ServerName` unchecked is
        // the same first-connect fatal by a different route. `tls` is left at
        // its default, because what is refused here is refused for the name and
        // not for the negotiation it would be used in.
        let config =
            from_document("endpoint = \"session example.com:9443\"\n").expect("a document");
        let error = config
            .resolve()
            .expect_err("not a name a certificate names");
        assert_eq!(
            error,
            SessionConfigError::ServerName {
                server_name: "session example.com".to_owned()
            }
        );
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
    fn a_malformed_endpoint_is_refused_at_load_and_not_resolved_as_an_address() {
        // The revert this test exists for: split at the last colon and strip
        // brackets wherever they appear. Every value below then resolves —
        // `[::1:9443` and `::1:9443` to the loopback address, `host::9443` to a
        // host named `host:` — and the document that caused it is three layers
        // from the connect that fails on it, under a backoff, on an error
        // naming a socket rather than a key.
        //
        // `tls` is left at its default here on purpose: with `tls = false` two
        // of these are caught by `PlaintextOffLoopback` instead, which reports
        // the wrong fault about the right value.
        for (endpoint, expected) in [
            ("[::1:9443", "nothing closes it"),
            ("[::1]9443", "is not followed by `:port`"),
            ("[not-an-address]:9443", "is not one"),
            ("[]:9443", "is not one"),
            ("host::9443", "holds no colon"),
            ("::1:9443", "holds no colon"),
            ("[::1]:9443:9443", "is not a port"),
        ] {
            let config =
                from_document(&format!("endpoint = \"{endpoint}\"\n")).expect("a document");
            let error = config
                .resolve()
                .expect_err(&format!("{endpoint} is not `host:port`"));
            let message = error.to_string();
            assert!(message.contains(expected), "{endpoint}: {message}");
            // Naming the value is what makes it a document to correct.
            assert!(message.contains(endpoint), "{endpoint}: {message}");
        }
    }

    #[test]
    fn a_bracketed_address_literal_keeps_its_colons_and_every_other_host_has_none() {
        assert_eq!(host_of("[::1]:9443"), Ok("::1"));
        assert_eq!(host_of("[2001:db8::1]:9443"), Ok("2001:db8::1"));
        assert_eq!(host_of("127.0.0.1:9443"), Ok("127.0.0.1"));
        assert_eq!(
            host_of("session.example.com:9443"),
            Ok("session.example.com")
        );
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
