//! One request to the endpoint, behind a trait this crate owns.
//!
//! # Why a trait, and what it buys
//!
//! The same reason `RouteLookup` puts the host's routing table behind one: the
//! answer comes from outside the process, so every test of what this transport
//! *decides* would otherwise be a test that needs a network. Everything the
//! transport decides — an unchanged response is liveness and not a payload, a
//! failed request ends the connection with the reason it had, a budget that
//! elapses before the poll is due is idle — is decided against this trait and
//! is therefore decided in a test that opens no socket and sleeps for nothing.
//!
//! What is left on the other side of the trait is [`HttpClient`], which holds
//! the parts nothing can assert without a network: a connection pool, chunked
//! transfer encoding, a TLS handshake. That is the smallest thing this crate
//! could have left untested, and it is deliberately dull.

use std::time::Duration;

use dz_ingress_core::BoxFuture;

/// One request to the endpoint.
///
/// Borrowed throughout, because every field is something the transport already
/// holds and a request is built once per poll.
#[derive(Debug, Clone, Copy)]
pub struct Request<'a> {
    /// The endpoint, exactly as configuration stated it.
    pub endpoint: &'a str,

    /// What the adapter last wrote through
    /// [`Input::send`](dz_ingress_core::Input::send), to go on the query
    /// string. `None` until it writes anything.
    ///
    /// **Not parsed here and not parsed by the transport.** A cursor, a page
    /// token and a symbol list are the venue's, and an adapter is the only
    /// layer that knows which of the three it wrote.
    pub parameters: Option<&'a str>,

    /// The entity tag the body last delivered was served under, offered back so
    /// that an endpoint whose catalogue has not moved can answer `304` instead
    /// of sending it again.
    pub validator: Option<&'a str>,

    /// How long the request may take. See
    /// [`PollInput::recv`](crate::PollInput) for where this comes from, which
    /// is the driver's own receive budget and not a timeout this transport
    /// holds a key for.
    pub budget: Duration,
}

/// What the endpoint answered.
///
/// Owned, because a client with a connection pool cannot lend out a buffer it
/// is about to reuse, and one catalogue body per poll is not a hot path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    /// The HTTP status. Classified by the transport, which is the layer that
    /// knows whether it is inside a connect or a receive — the same status is
    /// a connect failure with one taxonomy and a disconnect with another.
    pub status: u16,

    /// The response body, whatever it is. Empty for a `304`, and empty is also
    /// a legitimate catalogue: whether a market with nothing listed is an
    /// outage is the adapter's to decide and this crate has no opinion.
    pub body: Vec<u8>,

    /// The entity tag this body was served under, to be offered back on the
    /// next request. `None` for an endpoint that does not offer one, which
    /// makes every poll unconditional and every unchanged catalogue a body
    /// compared rather than a `304`.
    pub validator: Option<String>,
}

/// Why a request produced no answer at all.
///
/// **Six values, and the split is what makes the classification worth having.**
/// The transport maps each of them onto the taxonomy the layer it is in counts
/// by: [`ConnectFailureReason`](dz_ingress_core::ConnectFailureReason) inside a
/// connect, [`DisconnectReason`](dz_adapter_core::DisconnectReason) inside a
/// receive. Collapsing them here would make both mappings a catch-all, and a
/// refused connection, a name that would not resolve and a certificate that
/// would not verify are three different people's problem.
///
/// **Five of the six are network events and the sixth is not**, which is the
/// one distinction that changes what the driver does rather than only which
/// series moves: see [`Unusable`](Self::Unusable).
///
/// Each carries a detail string for the log line, and none of them is a status:
/// a status means the endpoint answered, which is an [`Answer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestFailure {
    /// The far side refused the connection outright.
    Refused(String),
    /// The endpoint's host would not resolve.
    Unresolved(String),
    /// The TLS negotiation failed: a certificate, a chain, a protocol version.
    Tls(String),
    /// The budget elapsed with no response.
    Timeout(String),
    /// Anything else between this host and the endpoint.
    ///
    /// Its own value rather than folded into [`Refused`](Self::Refused),
    /// because a connection that was established and then broke mid-body is
    /// not a refusal, and an operator who reads *refused* goes and looks at a
    /// firewall.
    Transport(String),
    /// The request could not be formed, so **nothing was asked of the
    /// network**.
    ///
    /// The endpoint and the parameters the adapter last wrote cannot be
    /// carried on a query string: a character a URI does not allow, such as
    /// the space in `symbols=BTC USD`, or a `#`, which a URI does allow and
    /// which would drop the rest of the parameters from the request. A bare
    /// `%` is not one of these: it is a character a query string allows, so it
    /// reaches the endpoint as written.
    ///
    /// **Its own value, and it is the only one the transport calls fatal.**
    /// The other five say something about the wire and are worth retrying
    /// under the driver's delay sequence. This one says the same request will
    /// be formed again, so retrying it is a publisher looping at the backoff
    /// ceiling for ever: the connect probe carries no parameters and therefore
    /// succeeds, the first receive fails, the connection's state is forgotten,
    /// the adapter writes the same text at the next logon, repeat — with
    /// `reconnects_total{reason="remote_close"}` the only signal and nothing
    /// naming the parameters. Folded into [`Transport`](Self::Transport) that
    /// is exactly what it would be, which is why it is not folded in.
    Unusable(String),
}

impl RequestFailure {
    /// The detail, for the error the transport raises.
    #[must_use]
    pub fn detail(&self) -> &str {
        match self {
            Self::Refused(detail)
            | Self::Unresolved(detail)
            | Self::Tls(detail)
            | Self::Timeout(detail)
            | Self::Transport(detail)
            | Self::Unusable(detail) => detail,
        }
    }
}

/// One request/response exchange with the endpoint.
///
/// `Send + Sync` and taking `&self`, both for the same reason: a publisher with
/// two polled sources holds two [`PollInput`](crate::PollInput)s and should
/// hold **one** client between them, so that two catalogues on one host share a
/// connection pool. A trait needing `&mut self` would have forced a client per
/// connection.
pub trait PollClient: Send + Sync {
    /// Make the request, or say why there was no answer.
    ///
    /// # Errors
    ///
    /// [`RequestFailure`], classified. A status the endpoint should not have
    /// returned is **not** an error here: the endpoint answered, and what a
    /// status means is the transport's to decide.
    fn fetch<'a>(&'a self, request: Request<'a>) -> BoxFuture<'a, Result<Answer, RequestFailure>>;
}

// ---------------------------------------------------------------------------
// The client a running publisher uses
// ---------------------------------------------------------------------------

/// The status a well-behaved endpoint answers a conditional request with when
/// nothing has changed.
///
/// Named rather than written as a literal at the two places that read it,
/// because `304` appearing bare in a match beside `200` reads like a status
/// code table rather than like the one case this transport turns into
/// [`Received::Liveness`](dz_ingress_core::Received).
pub const NOT_MODIFIED: u16 = 304;

/// The header a conditional request offers its validator in.
const IF_NONE_MATCH: &str = "if-none-match";

/// The header an endpoint states a body's validator in.
const ETAG: &str = "etag";

/// The largest response body this client will assemble.
///
/// It exists because the endpoint chooses the size and the buffer is ours. A
/// venue's whole instrument catalogue is the large one and eight megabytes is
/// well past any of them — the same bound, for the same reason, as the
/// websocket transport's largest message.
const MAX_BODY_BYTES: u64 = 8 * 1024 * 1024;

/// The connector, which is the one thing the `tls` feature changes.
///
/// Two aliases rather than one generic parameter on [`HttpClient`]: a venue
/// naming this type in its own `main` should not have to name a connector, and
/// the whole point of the feature is that a build without it links no TLS
/// stack at all.
#[cfg(feature = "tls")]
type Connector = hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>;

/// The connector in a build without `tls`. Plain HTTP, and an `https` endpoint
/// never reaches it: [`PollConfig`](crate::PollConfig) refuses one at load.
#[cfg(not(feature = "tls"))]
type Connector = hyper_util::client::legacy::connect::HttpConnector;

/// A [`PollClient`] over `hyper`.
///
/// One instance may be shared by every polled connection in a publisher, which
/// is what makes the connection pool worth having: a venue with a catalogue
/// endpoint and a reference-data endpoint on one host opens one connection.
///
/// # What it is not
///
/// It holds no cadence, no retry and no failure count — see the crate docs.
/// A `fetch` is one request, and what to do about the answer is the
/// transport's.
pub struct HttpClient {
    inner: hyper_util::client::legacy::Client<Connector, http_body_util::Empty<hyper::body::Bytes>>,
}

impl HttpClient {
    /// A client with a connection pool, over plain HTTP or TLS according to the
    /// `tls` feature.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner:
                hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                    .build(Self::connector()),
        }
    }

    /// The TLS client configuration, with the provider named rather than
    /// discovered.
    ///
    /// `rustls::ClientConfig::builder()` takes the process-wide default
    /// provider, or the one a feature installed, or panics. All three decide
    /// this somewhere other than here, and the panic arrives on the first
    /// `https` request — the one moment a suite that cannot use the network
    /// never reaches. So the provider is constructed explicitly, and the trust
    /// anchors are the compiled-in webpki set, so that a host with an empty or
    /// stale CA bundle verifies a certificate exactly like every other host.
    ///
    /// # Panics
    ///
    /// If the compiled-in provider cannot offer a protocol version, which is a
    /// build that cannot speak TLS at all. Panicking here rather than returning
    /// an error is deliberate and it is the only panic in this crate: this runs
    /// at startup, before a connection exists to report a failure on, and a
    /// publisher that carried on without TLS after being asked for it is the
    /// failure the `tls` feature exists to prevent.
    #[cfg(feature = "tls")]
    #[must_use]
    pub fn tls_config() -> rustls::ClientConfig {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
        rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("the compiled-in TLS provider offers a protocol version")
            .with_root_certificates(roots)
            .with_no_client_auth()
    }

    /// The connector, with TLS when this build has it.
    #[cfg(feature = "tls")]
    fn connector() -> Connector {
        hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(Self::tls_config())
            // `https_or_http` and not `https_only`: which schemes are allowed
            // is `PollConfig`'s answer, given at load with the scheme and the
            // feature named, and a connector that refused `http` here would
            // report the same mistake as a request failure hours later.
            .https_or_http()
            .enable_http1()
            .build()
    }

    /// The connector in a build without `tls`.
    #[cfg(not(feature = "tls"))]
    fn connector() -> Connector {
        hyper_util::client::legacy::connect::HttpConnector::new()
    }

    /// The URI for one request: the endpoint, and the adapter's parameters on
    /// the query string.
    ///
    /// The parameters are appended rather than merged, and an endpoint that
    /// already carries a query string gets an `&`. Nothing here parses either
    /// side: what the adapter wrote is the venue's own syntax, and a transport
    /// that re-encoded it would be deciding what a cursor means.
    ///
    /// # Errors
    ///
    /// [`RequestFailure::Unusable`] when the parameters cannot be carried on a
    /// query string: a character a URI does not allow, such as the space in
    /// `symbols=BTC USD`, or a `#`. Not encoded around, for the reason above,
    /// and not retried, for the reason that value gives.
    ///
    /// A `%` that starts no escape is **not** refused. It is a character a
    /// query string allows, so it reaches the endpoint exactly as the adapter
    /// wrote it, and what a venue makes of it is the venue's — the same rule
    /// as every other byte here.
    fn uri(request: &Request<'_>) -> Result<hyper::Uri, RequestFailure> {
        // Refused ahead of the parse, because the parse *accepts* it: a `#`
        // opens a fragment, a fragment is not sent to a server, and a URI
        // built from `cursor=a#b` requests `cursor=a`. So the one outcome this
        // must not have is the quiet one - an adapter's parameters half
        // delivered, no error anywhere, and a venue answering the wrong
        // question. An adapter that wants a literal `#` in a value writes
        // `%23`, which is what a query string means by one.
        if let Some(parameters) = request.parameters {
            if parameters.contains('#') {
                return Err(RequestFailure::Unusable(
                    "the request URI is not usable: the parameters carry a `#`, which opens a \
                     fragment and would drop the rest of them from the request; a literal `#` \
                     in a value is written `%23`"
                        .to_string(),
                ));
            }
        }
        let target = match request.parameters {
            None => request.endpoint.to_string(),
            Some(parameters) if request.endpoint.contains('?') => {
                format!("{}&{parameters}", request.endpoint)
            }
            Some(parameters) => format!("{}?{parameters}", request.endpoint),
        };
        hyper::Uri::try_from(target).map_err(|error| {
            // `Unusable` and not one of the five network values: nothing was
            // asked of the network, and the transport turns this into a fault
            // retrying cannot fix because the same endpoint and the same
            // parameters produce the same unusable URI on every attempt.
            //
            // The detail names the failure and **not the URI**, for the reason
            // every other detail here names the authority instead: a venue
            // endpoint's query string is where several venue APIs keep a key.
            RequestFailure::Unusable(format!("the request URI is not usable: {error}"))
        })
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Prints whether TLS is compiled in, and **not the endpoint of any request**.
///
/// A client holds no endpoint, so there is little to print — but a derived
/// implementation would print the connection pool's contents, which is a list
/// of hosts, and the same rule that keeps a venue endpoint's query string out
/// of a log line applies to it.
impl core::fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("HttpClient")
            .field("tls", &cfg!(feature = "tls"))
            .finish()
    }
}

impl PollClient for HttpClient {
    fn fetch<'a>(&'a self, request: Request<'a>) -> BoxFuture<'a, Result<Answer, RequestFailure>> {
        Box::pin(async move {
            let uri = Self::uri(&request)?;
            let mut builder = hyper::Request::get(uri);
            if let Some(validator) = request.validator {
                builder = builder.header(IF_NONE_MATCH, validator);
            }
            // The second way a request can fail to be formed, and it gets the
            // same answer as the first: the URI is already checked above, so
            // what is left is the header, and a validator that came out of an
            // endpoint's own `etag` through `to_str` is visible ASCII and
            // therefore a value a header accepts. Unreachable in practice,
            // classified honestly rather than left as a network failure it is
            // not.
            let outgoing = builder
                .body(http_body_util::Empty::<hyper::body::Bytes>::new())
                .map_err(|error| {
                    RequestFailure::Unusable(format!("the request is not usable: {error}"))
                })?;

            let response =
                match tokio::time::timeout(request.budget, self.inner.request(outgoing)).await {
                    Err(_elapsed) => {
                        return Err(RequestFailure::Timeout(format!(
                            "no response within {:?}",
                            request.budget
                        )))
                    }
                    Ok(Err(error)) => return Err(classify(&error)),
                    Ok(Ok(response)) => response,
                };

            let status = response.status().as_u16();
            let validator = response
                .headers()
                .get(ETAG)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);

            // Bounded **while it is read** and not by what it claims. A
            // `Content-Length` is a number the endpoint chose, and a chunked
            // response declares no length at all - so a ceiling checked against
            // the claim is a ceiling an endpoint can walk straight past by
            // omitting the header, which is the whole shape of the failure the
            // bound exists for. `Limited` stops taking bytes at the ceiling
            // instead.
            use http_body_util::BodyExt as _;
            let collected = http_body_util::Limited::new(
                response.into_body(),
                usize::try_from(MAX_BODY_BYTES).unwrap_or(usize::MAX),
            )
            .collect()
            .await
            .map_err(|error| {
                // Two failures with one answer: a body that started and
                // stopped, and one that went past the ceiling. `Transport` and
                // not `Refused`, which is the distinction that value exists
                // for: the connection was established. The detail names the
                // ceiling, because *the endpoint sent too much* and *the
                // endpoint stopped sending* are not the same conversation to
                // have with a venue.
                RequestFailure::Transport(format!(
                    "the response body did not arrive whole, within the \
                     {MAX_BODY_BYTES}-byte ceiling: {error}"
                ))
            })?
            .to_bytes();

            Ok(Answer {
                status,
                body: collected.to_vec(),
                validator,
            })
        })
    }
}

/// What the client's own error says, in the five words this crate counts by.
///
/// Loose where it has to be, and documented as such rather than tidied. A name
/// that would not resolve has no stable `std::io::ErrorKind` — the candidates
/// are unstable — so the string is the only signal, exactly as it is in the
/// websocket transport's own connect classification. The alternative is
/// counting every socket error as a refusal, which is the catch-all this
/// function exists instead of.
fn classify(error: &hyper_util::client::legacy::Error) -> RequestFailure {
    let rendered = chain(error);
    let lowered = rendered.to_ascii_lowercase();
    if lowered.contains("dns")
        || lowered.contains("resolve")
        || lowered.contains("name or service")
        || lowered.contains("nodename")
    {
        return RequestFailure::Unresolved(rendered);
    }
    // Before the refusal check, because a TLS failure arrives inside a connect
    // and would otherwise be counted as one.
    if lowered.contains("certificate")
        || lowered.contains("tls")
        || lowered.contains("handshake")
        || lowered.contains("invalid peer")
    {
        return RequestFailure::Tls(rendered);
    }
    if lowered.contains("refused") {
        return RequestFailure::Refused(rendered);
    }
    if lowered.contains("timed out") || lowered.contains("timeout") {
        return RequestFailure::Timeout(rendered);
    }
    RequestFailure::Transport(rendered)
}

/// The error and everything under it, as one line.
///
/// `hyper-util`'s own `Display` is a category — *client error (Connect)* — and
/// the cause underneath it is where the refusal, the unresolved name and the
/// certificate live. An operator handed only the category has been told
/// nothing, and [`classify`] would have nothing to read.
fn chain(error: &dyn std::error::Error) -> String {
    let mut rendered = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        rendered.push_str(": ");
        rendered.push_str(&cause.to_string());
        source = cause.source();
    }
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_adapters_parameters_go_on_the_query_string_of_an_endpoint_that_has_none() {
        let uri = HttpClient::uri(&Request {
            endpoint: "http://192.0.2.10/catalogue",
            parameters: Some("cursor=17"),
            validator: None,
            budget: Duration::from_secs(1),
        })
        .expect("a usable URI");
        assert_eq!(uri.to_string(), "http://192.0.2.10/catalogue?cursor=17");
    }

    #[test]
    fn an_endpoint_that_already_has_a_query_string_keeps_it() {
        // A venue whose catalogue endpoint carries a fixed parameter of its
        // own is ordinary, and replacing its query string with the adapter's
        // would drop it.
        let uri = HttpClient::uri(&Request {
            endpoint: "http://192.0.2.10/catalogue?market=spot",
            parameters: Some("cursor=17"),
            validator: None,
            budget: Duration::from_secs(1),
        })
        .expect("a usable URI");
        assert_eq!(
            uri.to_string(),
            "http://192.0.2.10/catalogue?market=spot&cursor=17"
        );
    }

    #[test]
    fn a_request_with_nothing_written_yet_carries_no_query_string() {
        // The connect probe and every poll before the adapter writes anything.
        let uri = HttpClient::uri(&Request {
            endpoint: "http://192.0.2.10/catalogue",
            parameters: None,
            validator: None,
            budget: Duration::from_secs(1),
        })
        .expect("a usable URI");
        assert_eq!(uri.to_string(), "http://192.0.2.10/catalogue");
    }

    #[test]
    fn a_parameter_string_that_makes_no_uri_is_unusable_and_not_a_network_failure() {
        // A space in a symbol list is the one an adapter reaches by accident,
        // and the rest are the other characters a query string does not allow.
        // Each is a request that cannot be formed, so nothing is asked of the
        // network and the next attempt forms the same one - which is what
        // makes `Unusable` the value rather than `Transport`, and fatal rather
        // than a connection the driver retries.
        for parameters in [
            "symbols=BTC USD",
            "cursor=a\nb",
            "cursor=a\tb",
            "cursor=a<b",
            "cursor=\"a\"",
        ] {
            let failure = HttpClient::uri(&Request {
                endpoint: "http://192.0.2.10/catalogue",
                parameters: Some(parameters),
                validator: None,
                budget: Duration::from_secs(1),
            })
            .expect_err("a space, a fragment marker and a bare percent are not a query string");
            assert!(
                matches!(failure, RequestFailure::Unusable(_)),
                "`{parameters}` produced {failure:?}, and any of the five network \
                 values is a fault the driver retries for ever: the connect probe \
                 carries no parameters and therefore succeeds, the first receive \
                 fails, and the adapter writes the same text again"
            );
            assert!(
                !failure.detail().contains(parameters),
                "the detail names the failure and not what was written: {}",
                failure.detail()
            );
        }
    }

    #[test]
    fn a_fragment_marker_in_the_parameters_is_refused_rather_than_quietly_dropped() {
        // A `#` is a character a URI *allows*, which is what makes it the
        // dangerous one: `hyper` parses `?cursor=a#b` happily and the fragment
        // is never sent, so the endpoint is asked `cursor=a` and nothing
        // anywhere says so. Refused instead, because half an adapter's
        // parameters answered by a venue is a wrong answer that looks like a
        // right one.
        let failure = HttpClient::uri(&Request {
            endpoint: "http://192.0.2.10/catalogue",
            parameters: Some("cursor=a#b"),
            validator: None,
            budget: Duration::from_secs(1),
        })
        .expect_err("a `#` cannot be carried on a query string");
        assert!(
            matches!(failure, RequestFailure::Unusable(_)),
            "{failure:?}"
        );
        assert!(
            failure.detail().contains("%23"),
            "and the detail says how to write one, because an adapter author is \
             who reads it: {}",
            failure.detail()
        );
    }

    #[test]
    fn a_percent_that_starts_no_escape_reaches_the_endpoint_as_written() {
        // Not refused, and deliberately: `%` is a character a query string
        // allows, and this transport does not parse what the adapter wrote.
        // What a venue makes of it is the venue's.
        let uri = HttpClient::uri(&Request {
            endpoint: "http://192.0.2.10/catalogue",
            parameters: Some("cursor=%"),
            validator: None,
            budget: Duration::from_secs(1),
        })
        .expect("a bare percent is a character a query string allows");
        assert_eq!(uri.to_string(), "http://192.0.2.10/catalogue?cursor=%");
    }

    #[test]
    fn a_debug_line_says_whether_tls_is_compiled_in() {
        // The one question a client's `Debug` can answer that an operator
        // reading a refused `https` endpoint actually has.
        let rendered = format!("{:?}", HttpClient::new());
        assert!(
            rendered.contains(if cfg!(feature = "tls") {
                "tls: true"
            } else {
                "tls: false"
            }),
            "{rendered}"
        );
    }

    #[cfg(feature = "tls")]
    #[test]
    fn the_tls_configuration_is_constructible_without_touching_a_network() {
        // The part of the TLS setup most likely to be wrong and least likely
        // to be reached: `rustls` panics when it has to choose a crypto
        // provider and cannot, and that panic would otherwise arrive on a
        // production `https` request, which no test that can run here makes.
        let config = HttpClient::tls_config();
        assert!(
            !config.crypto_provider().cipher_suites.is_empty(),
            "the provider named in code must be the one installed, and it must \
             offer cipher suites"
        );
    }
}
