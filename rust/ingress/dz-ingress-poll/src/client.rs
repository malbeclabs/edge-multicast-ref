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
///
/// # Its `Debug` prints no query string
///
/// See the implementation below. Two of its four fields are the two things in
/// this crate that must not reach a log line — the endpoint, whose query
/// string is where several venue APIs keep a key, and whatever the adapter
/// last wrote, which nothing here can tell a cursor from a signed token.
#[derive(Clone, Copy)]
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
    ///
    /// **The whole exchange, head and body**, and that is a requirement on an
    /// implementation of this trait rather than a detail of the one below. A
    /// client that bounded only the head returns neither a payload nor a
    /// failure for an endpoint that answers and then stops sending, which is
    /// the one state the driver cannot even time out of: it never reaches the
    /// top of its loop to notice the idle guard has gone.
    pub budget: Duration,
}

/// Prints the scheme and host of the endpoint, whether the request carries
/// parameters and whether it offers a validator — and **neither the endpoint
/// nor the parameters**.
///
/// The same rule `PollConfig`'s own `Debug`, `PollInput`'s and every error
/// detail in this crate keep, and this type is the one a venue writing its own
/// [`PollClient`] is handed: a derived implementation here would put a key in a
/// log line in somebody else's crate.
///
/// **Whether the request is conditional is printed**, and that one is not
/// decoration: a `304` means *not modified* only in answer to a validator, so
/// this is the field that separates a well-behaved endpoint from one whose
/// answer says nothing — the reading
/// [`PollInput::recv`](crate::PollInput) makes of the same status.
impl core::fmt::Debug for Request<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Request")
            .field("authority", &crate::config::authority_of(self.endpoint))
            .field("parameterised", &self.parameters.is_some())
            .field("conditional", &self.validator.is_some())
            .field("budget", &self.budget)
            .finish()
    }
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

/// The largest response body this client will assemble unless told otherwise.
///
/// It exists because the endpoint chooses the size and the buffer is ours. A
/// venue's whole instrument catalogue is the large one and eight megabytes is
/// well past any of them — the same bound, for the same reason, as the
/// websocket transport's largest message.
///
/// **A default and not a ceiling in the code**, for the reason that transport
/// makes its own adjustable: the number is a guess about somebody else's
/// catalogue, and a venue whose catalogue is genuinely larger than the guess
/// is unreachable for ever otherwise. Every poll ends `Limited`, which is
/// `Transport`, which is `remote_close`; the driver reconnects, the next poll
/// does the same, and the reconnect counter is the only thing that moves. See
/// [`HttpClient::with_max_body_bytes`].
pub const DEFAULT_MAX_BODY_BYTES: u64 = 8 * 1024 * 1024;

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
/// It holds no cadence and no failure count — see the crate docs. A `fetch` is
/// one request, and what to do about the answer is the transport's.
///
/// **One qualification on "no retry".** `hyper_util`'s legacy client defaults
/// `retry_canceled_requests` to true, which retries a request exactly once
/// when it was cancelled on a **reused pooled connection** the far side had
/// already closed — a connection this client established for an earlier poll
/// and the endpoint has since dropped. Left at the default: it breaks none of
/// the three bans, because it is not a cadence, not a delay sequence and not a
/// count of consecutive failures, and what it prevents is a failure the
/// transport would otherwise report for a connection that was already gone
/// before the request. Named here because *no retry* is an absolute claim and
/// this is not nothing.
pub struct HttpClient {
    inner: hyper_util::client::legacy::Client<Connector, http_body_util::Empty<hyper::body::Bytes>>,
    /// The largest body to assemble. See
    /// [`with_max_body_bytes`](HttpClient::with_max_body_bytes).
    max_body_bytes: u64,
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
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
        }
    }

    /// The largest response body to assemble. See
    /// [`DEFAULT_MAX_BODY_BYTES`], which is what this starts at.
    ///
    /// The escape hatch the websocket transport's
    /// `with_max_message_bytes` is, and it is here for the same reason: the
    /// default is a guess about a catalogue on somebody else's host, and a
    /// venue whose catalogue is genuinely larger than the guess has no way
    /// past it otherwise. What such a publisher does instead is poll, end
    /// `Limited` — which is `Transport`, which the transport ends the
    /// connection as `remote_close` — reconnect, and do it again, with
    /// `dz_publisher_ingress_reconnects_total{reason="remote_close"}` the only
    /// series that moves and nothing anywhere naming a size. A bound whose
    /// only remedy is a rebuild of the publisher is a bound that outlives
    /// whoever chose it.
    ///
    /// **Raising it is a memory commitment made to the endpoint**, which is
    /// what the bound exists to keep from being open-ended: one poll may hold
    /// this many bytes per polled connection, and the endpoint decides when.
    /// Lower it as readily as raise it — a venue whose catalogue is a hundred
    /// kilobytes has a bound worth eighty times tighter than the default.
    #[must_use]
    pub const fn with_max_body_bytes(mut self, bytes: u64) -> Self {
        self.max_body_bytes = bytes;
        self
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

/// Prints whether TLS is compiled in and the body ceiling this client is
/// holding, and **not the endpoint of any request**.
///
/// A client holds no endpoint, so there is little to print — but a derived
/// implementation would print the connection pool's contents, which is a list
/// of hosts, and the same rule that keeps a venue endpoint's query string out
/// of a log line applies to it.
///
/// The ceiling is printed because it is adjustable: a venue's `main` that
/// raised it and a build that did not are otherwise the same line, and the
/// failure that tells them apart arrives once, on the poll that exceeds it.
impl core::fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("HttpClient")
            .field("tls", &cfg!(feature = "tls"))
            .field("max_body_bytes", &self.max_body_bytes)
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

            // **One deadline for the whole exchange, and not a bound on the
            // head alone.** The budget is the driver's receive budget, so what
            // it has to bound is the time until there is a payload — and the
            // body is where an endpoint stalls without going away: it writes
            // `200 OK` and its headers, stops sending body bytes, and holds
            // the socket open. A bound on the head only is then no bound at
            // all: `collect` below waits for ever, `fetch` never returns,
            // `recv` never returns, and the driver never gets back to the top
            // of its loop to recompute the idle budget. `connection_state`
            // stays at 1, no reconnect is counted, no payload arrives — the
            // publisher is silently dead with nothing to look at, which is the
            // one failure this transport exists to turn into a number.
            //
            // `Limited` does not close this: it bounds bytes, and a body that
            // trickles or stops under the ceiling never reaches it.
            let deadline = tokio::time::Instant::now() + request.budget;

            let response =
                match tokio::time::timeout_at(deadline, self.inner.request(outgoing)).await {
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
            let body = http_body_util::Limited::new(
                response.into_body(),
                usize::try_from(self.max_body_bytes).unwrap_or(usize::MAX),
            )
            .collect();
            // The deadline the head was read against, and what is left of it.
            // A body still arriving when the budget runs out is a timeout and
            // not a transport failure: nothing about the wire went wrong, the
            // request simply outlived what the driver gave it, and the detail
            // names the bound so that the two are not read as one. The same
            // reason `Timeout` is its own value at the head.
            let collected = match tokio::time::timeout_at(deadline, body).await {
                Err(_elapsed) => {
                    return Err(RequestFailure::Timeout(format!(
                        "the response head arrived and its body did not, within {:?}",
                        request.budget
                    )))
                }
                Ok(Ok(collected)) => collected,
                Ok(Err(error)) => {
                    // Two failures with one answer: a body that started and
                    // stopped, and one that went past the ceiling. `Transport`
                    // and not `Refused`, which is the distinction that value
                    // exists for: the connection was established. The detail
                    // names the ceiling, because *the endpoint sent too much*
                    // and *the endpoint stopped sending* are not the same
                    // conversation to have with a venue.
                    return Err(RequestFailure::Transport(format!(
                        "the response body did not arrive whole, within the {}-byte ceiling: \
                         {error}",
                        self.max_body_bytes
                    )));
                }
            }
            .to_bytes();

            Ok(Answer {
                status,
                body: collected.to_vec(),
                validator,
            })
        })
    }
}

/// What the client's own error says, in the five network words this crate
/// counts by.
///
/// Two functions and not one, because a `hyper_util` error cannot be
/// constructed outside `hyper_util`: the reading is [`classify_rendered`], and
/// a test states a string. The boundary — that a real failure renders a string
/// the reading lands on the right value for — is what the tests driving actual
/// requests at a closed port and an undelegated name are for. Without both
/// halves this table can be collapsed to a catch-all with nothing failing, and
/// a DNS failure, an expired certificate and a firewalled port then move one
/// series between them.
fn classify(error: &hyper_util::client::legacy::Error) -> RequestFailure {
    classify_rendered(chain(error))
}

/// The reading itself, over the text the error and its causes rendered to.
///
/// Loose where it has to be, and documented as such rather than tidied. A name
/// that would not resolve has no stable `std::io::ErrorKind` — the candidates
/// are unstable — so the string is the only signal, exactly as it is in the
/// websocket transport's own connect classification. The alternative is
/// counting every socket error as a refusal, which is the catch-all this
/// function exists instead of.
fn classify_rendered(rendered: String) -> RequestFailure {
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
    fn a_debug_line_says_whether_tls_is_compiled_in_and_which_ceiling_is_held() {
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
        // And the ceiling, because it is adjustable: a venue's `main` that
        // raised it and a build that did not are otherwise the same line, and
        // the failure that tells them apart arrives once, on the poll that
        // exceeds it.
        assert!(
            rendered.contains(&format!("max_body_bytes: {DEFAULT_MAX_BODY_BYTES}")),
            "{rendered}"
        );
        let rendered = format!("{:?}", HttpClient::new().with_max_body_bytes(64 * 1024));
        assert!(rendered.contains("max_body_bytes: 65536"), "{rendered}");
    }

    #[test]
    fn a_requests_debug_names_the_host_and_neither_the_query_string_nor_the_parameters() {
        // The type a venue writing its own client is handed, holding the two
        // things in this crate that must not reach a log line: an endpoint
        // whose query string is where several venue APIs keep a key, and
        // whatever the adapter last wrote, which nothing here can tell a
        // cursor from a signed token.
        let rendered = format!(
            "{:?}",
            Request {
                endpoint: "https://192.0.2.10:8443/catalogue?api_key=not-a-real-secret",
                parameters: Some("cursor=not-a-real-token"),
                validator: Some("\"catalogue-1\""),
                budget: Duration::from_secs(30),
            }
        );
        assert!(!rendered.contains("api_key"), "{rendered}");
        assert!(!rendered.contains("not-a-real-secret"), "{rendered}");
        assert!(!rendered.contains("not-a-real-token"), "{rendered}");
        assert!(rendered.contains("https://192.0.2.10:8443"), "{rendered}");
        assert!(rendered.contains("parameterised: true"), "{rendered}");
        // The field the `304` reading turns on: *not modified* is an answer to
        // a validator and nothing else, so whether one was offered is what a
        // log line about a `304` has to be able to say.
        assert!(rendered.contains("conditional: true"), "{rendered}");

        let rendered = format!(
            "{:?}",
            Request {
                endpoint: "http://192.0.2.10/catalogue",
                parameters: None,
                validator: None,
                budget: Duration::from_secs(30),
            }
        );
        assert!(rendered.contains("parameterised: false"), "{rendered}");
        assert!(rendered.contains("conditional: false"), "{rendered}");
    }

    // -----------------------------------------------------------------------
    // The classification, and the boundary a real failure crosses
    // -----------------------------------------------------------------------

    #[test]
    fn each_of_the_five_network_failures_has_a_string_only_it_matches() {
        // The table, stated as the values themselves: each case is the
        // failure a string must produce, and the string is that failure's own
        // detail - so one assertion says both that the variant is right and
        // that the whole chain survives into it. The category alone tells an
        // operator nothing.
        //
        // Written out rather than derived, for the reason the codec's
        // vocabulary tests give: a table checked only against itself is a
        // table that agrees with its own mistake. Every string here is one
        // `hyper` and the platform actually render - see the tests below,
        // which drive real requests and assert the same values.
        let cases = [
            RequestFailure::Unresolved(
                "client error (Connect): dns error: failed to lookup address information: \
                 Name or service not known"
                    .to_string(),
            ),
            RequestFailure::Unresolved(
                "client error (Connect): dns error: nodename nor servname provided".to_string(),
            ),
            RequestFailure::Tls(
                "client error (Connect): invalid peer certificate: Expired".to_string(),
            ),
            RequestFailure::Tls("client error (Connect): tls handshake eof".to_string()),
            RequestFailure::Refused(
                "client error (Connect): tcp connect error: Connection refused (os error 111)"
                    .to_string(),
            ),
            RequestFailure::Timeout(
                "client error (Connect): tcp connect error: Connection timed out (os error 110)"
                    .to_string(),
            ),
            RequestFailure::Transport(
                "client error (Body): error reading a body from connection: connection reset \
                 by peer"
                    .to_string(),
            ),
            RequestFailure::Transport(
                "client error (SendRequest): connection closed before message completed"
                    .to_string(),
            ),
        ];

        for expected in cases {
            let rendered = expected.detail().to_string();
            assert_eq!(
                classify_rendered(rendered.clone()),
                expected,
                "`{rendered}` must be read as {expected:?}, keeping the whole chain"
            );
        }
    }

    #[test]
    fn a_certificate_inside_a_connect_is_tls_and_not_a_refusal() {
        // The one ordering in the table that is load-bearing, and the string
        // has to name **both** for the test to be about the ordering at all:
        // a chain carrying a certificate and a refusal together is read as
        // the certificate, because an operator who reads `refused` goes and
        // looks at a firewall rather than at an expiry. Reverse the two checks
        // and this is the assertion that fails.
        let failure = classify_rendered(
            "client error (Connect): tls handshake error: connection refused by peer after \
             invalid peer certificate: Expired"
                .to_string(),
        );
        assert!(
            matches!(failure, RequestFailure::Tls(_)),
            "a chain naming a certificate is a certificate, whatever else it \
             names: {failure:?}"
        );

        // And the plain refusal still is one, so the ordering cannot be
        // satisfied by calling everything TLS.
        let failure = classify_rendered(
            "client error (Connect): tcp connect error: Connection refused (os error 111)"
                .to_string(),
        );
        assert!(matches!(failure, RequestFailure::Refused(_)), "{failure:?}");
    }

    /// A port nothing is listening on, on this host.
    ///
    /// Bound and dropped rather than picked, so that the kernel is the one
    /// saying the port is free and no other test can be using it.
    fn a_closed_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback port to bind");
        listener.local_addr().expect("a bound address").port()
    }

    #[tokio::test]
    async fn a_request_to_a_closed_port_is_refused_and_not_a_catch_all() {
        // **The boundary.** Every other test of a failure in this crate builds
        // a `RequestFailure` by hand, so this is the only path a production
        // failure actually travels: a real `hyper` error, rendered, read by
        // `classify`. Without it the whole table can be collapsed to one
        // catch-all with nothing failing.
        let port = a_closed_port();
        // With a key on the query string, because this detail is the one
        // rendered string in this crate whose text is somebody else's: every
        // other one is a `format!` here, and this one is `hyper`'s error and
        // its causes. The transport prefixes the authority and prints no URI,
        // and the assertion below is what says the client hands it nothing to
        // print.
        let endpoint = format!("http://127.0.0.1:{port}/catalogue?api_key=not-a-real-secret");
        let failure = HttpClient::new()
            .fetch(Request {
                endpoint: &endpoint,
                parameters: Some("cursor=not-a-real-token"),
                validator: None,
                budget: Duration::from_secs(5),
            })
            .await
            .expect_err("nothing is listening on a port the kernel has just given back");

        assert!(
            matches!(failure, RequestFailure::Refused(_)),
            "a closed port is a firewall or a port, which is `connect_failures_total\
             {{reason=\"refused\"}}` and somebody's to go and look at: {failure:?}"
        );
        let detail = failure.detail();
        assert!(!detail.contains("api_key"), "{detail}");
        assert!(!detail.contains("not-a-real-secret"), "{detail}");
        assert!(!detail.contains("not-a-real-token"), "{detail}");
    }

    #[tokio::test]
    async fn a_name_that_does_not_resolve_is_unresolved_and_not_a_refusal() {
        // `.invalid` is reserved by RFC 2606 precisely so that it is never
        // delegated, so this asks the host's own resolver and gets no answer -
        // whether that is NXDOMAIN or a host with no resolver at all, both of
        // which render as a dns error.
        let failure = HttpClient::new()
            .fetch(Request {
                endpoint: "http://catalogue.this-name-is-not-delegated.invalid/catalogue",
                parameters: None,
                validator: None,
                budget: Duration::from_secs(10),
            })
            .await
            .expect_err("a reserved top-level domain is never delegated");

        assert!(
            matches!(failure, RequestFailure::Unresolved(_)),
            "a name that would not resolve is DNS or a typo, and it must not \
             share a series with a refused port: {failure:?}"
        );
    }

    #[tokio::test]
    async fn an_endpoint_that_accepts_and_never_answers_is_a_timeout() {
        // The third of the three, and the one the transport raises itself
        // rather than reading out of a string: the budget is the driver's, and
        // `fetch` bounds the whole exchange by it. A listener nobody accepts
        // on still completes the handshake out of the kernel's backlog, so the
        // connection succeeds and no response ever comes - which is the shape
        // of an endpoint that has stopped answering rather than gone away.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback port to bind");
        let port = listener.local_addr().expect("a bound address").port();
        let endpoint = format!("http://127.0.0.1:{port}/catalogue");

        let failure = HttpClient::new()
            .fetch(Request {
                endpoint: &endpoint,
                parameters: None,
                validator: None,
                budget: Duration::from_millis(200),
            })
            .await
            .expect_err("nobody accepts on this listener, so nothing answers");

        assert!(
            matches!(failure, RequestFailure::Timeout(_)),
            "the budget is the driver's receive budget, and a request that \
             outlives it is a timeout rather than anything about the wire: \
             {failure:?}"
        );
        assert!(
            failure.detail().contains("200ms"),
            "and the detail names the bound that was exceeded: {}",
            failure.detail()
        );
        // Held to here on purpose: dropped earlier, the port closes and the
        // request is refused instead of timing out.
        drop(listener);
    }

    // -----------------------------------------------------------------------
    // The response body's ceiling
    // -----------------------------------------------------------------------

    /// Serves one chunked response of `body_bytes` bytes and no
    /// `Content-Length`, then closes.
    ///
    /// **Chunked and lengthless on purpose.** A `Content-Length` is a number
    /// the endpoint chose, so a ceiling checked against the claim is one an
    /// endpoint walks past by omitting the header - which is the whole shape
    /// of the failure the bound exists for, and the shape a `size_hint` check
    /// let through. This server is the thing that tells those two apart.
    ///
    /// Hand-written rather than a server crate: the response is four lines and
    /// a loop, and a dev-dependency a venue does not inherit is still a
    /// dependency this suite would have to justify.
    ///
    /// # It lets go of a blocked write on a clock of its own
    ///
    /// **Without that guard the over-ceiling test hangs instead of failing**,
    /// which is the one outcome a test asserting a bound must not have. Once
    /// `Limited` trips, `collect` stops draining and the test blocks in
    /// `join`; `#[tokio::test]` is a current-thread runtime, so that parks the
    /// only thread the runtime has and `hyper`'s connection task is never
    /// polled to close the socket. This thread then escapes only if what it
    /// still has to write happens to fit in the socket buffers — true on a
    /// typical Linux host and not a property of one, so a host with a smaller
    /// `tcp_wmem` gets `write_all` blocking for ever and a suite that never
    /// returns.
    ///
    /// A write timeout is what makes that a failure: the loop already treats
    /// every write error as the expected end, so a blocked write becomes the
    /// same EPIPE the client's own close produces. It is the guard
    /// [`an_endpoint_that_answers_and_then_stops_sending`] has below for the
    /// same deadlock, in the form this server's own blocking point takes.
    /// Well past the time any body a test here serves takes to drain over
    /// loopback, so it can only fire on the deadlock.
    fn an_endpoint_serving_a_lengthless_body(
        body_bytes: usize,
    ) -> (u16, std::thread::JoinHandle<()>) {
        use std::io::{Read as _, Write as _};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback port to bind");
        let port = listener.local_addr().expect("a bound address").port();
        let handle = std::thread::spawn(move || {
            let Ok((mut socket, _)) = listener.accept() else {
                return;
            };
            // See this function's own note. The whole of what keeps the
            // over-ceiling test a failure rather than a hang.
            let _ = socket.set_write_timeout(Some(Duration::from_secs(2)));
            // Enough of the request to know it arrived. The client sends one
            // GET with no body, so the headers end at the blank line.
            let mut scratch = [0_u8; 1024];
            let _ = socket.read(&mut scratch);
            if socket
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                .is_err()
            {
                return;
            }
            // 64 KiB at a time. Every write is allowed to fail: once the
            // client has had its fill it drops the connection, and this thread
            // meeting an EPIPE — or the write timeout above, where the client
            // is still holding the runtime and has not got round to closing —
            // is the expected end rather than a fault.
            let chunk = vec![b'x'; 64 * 1024];
            let mut sent = 0;
            while sent < body_bytes {
                let take = chunk.len().min(body_bytes - sent);
                if socket
                    .write_all(format!("{take:x}\r\n").as_bytes())
                    .and_then(|()| socket.write_all(&chunk[..take]))
                    .and_then(|()| socket.write_all(b"\r\n"))
                    .is_err()
                {
                    return;
                }
                sent += take;
            }
            let _ = socket.write_all(b"0\r\n\r\n");
            let _ = socket.flush();
        });
        (port, handle)
    }

    #[tokio::test]
    async fn a_lengthless_body_under_the_ceiling_arrives_whole() {
        // One direction of the bound, and the one that says the ceiling is not
        // simply refusing chunked responses: a catalogue with no
        // `Content-Length` is ordinary, and every byte of it must arrive.
        let size = 128 * 1024;
        let (port, server) = an_endpoint_serving_a_lengthless_body(size);
        let endpoint = format!("http://127.0.0.1:{port}/catalogue");

        let answer = HttpClient::new()
            .fetch(Request {
                endpoint: &endpoint,
                parameters: None,
                validator: None,
                budget: Duration::from_secs(30),
            })
            .await
            .expect("a body under the ceiling");

        assert_eq!(answer.status, 200);
        assert_eq!(
            answer.body.len(),
            size,
            "a chunked catalogue is assembled whole, not truncated at some \
             convenient boundary"
        );
        server.join().expect("the endpoint thread");
    }

    #[tokio::test]
    async fn a_lengthless_body_past_the_ceiling_stops_at_it_rather_than_filling_memory() {
        // **The other direction, and the reason the bound exists.** The
        // endpoint chooses the size and the buffer is ours, so a chunked
        // response that never ends allocates until the publisher is
        // OOM-killed. `Limited` stops taking bytes at the ceiling, and this is
        // the assertion that removing it fails.
        //
        // A megabyte past the ceiling rather than an endless body, so that the
        // test ends whether or not the bound holds - an endless one would hang
        // in CI on the failure it is meant to report.
        //
        // The megabyte is finite and that alone is not enough: `join` below
        // parks the current-thread runtime, so the server thread has to be
        // able to let go of a write nobody is draining. See
        // `an_endpoint_serving_a_lengthless_body`'s own note for why, and for
        // what the size of the socket buffers would otherwise decide.
        let (port, server) = an_endpoint_serving_a_lengthless_body(
            usize::try_from(DEFAULT_MAX_BODY_BYTES).unwrap() + 1024 * 1024,
        );
        let endpoint = format!("http://127.0.0.1:{port}/catalogue");

        let failure = HttpClient::new()
            .fetch(Request {
                endpoint: &endpoint,
                parameters: None,
                validator: None,
                budget: Duration::from_secs(30),
            })
            .await
            .expect_err("a body past the ceiling is not an answer");

        assert!(
            matches!(failure, RequestFailure::Transport(_)),
            "the connection was established, which is the distinction that \
             value exists for: {failure:?}"
        );
        assert!(
            failure
                .detail()
                .contains(&DEFAULT_MAX_BODY_BYTES.to_string()),
            "and the detail names the ceiling, because *the endpoint sent too \
             much* and *the endpoint stopped sending* are not the same \
             conversation to have with a venue: {}",
            failure.detail()
        );
        server.join().expect("the endpoint thread");
    }

    #[tokio::test]
    async fn a_ceiling_a_venue_stated_is_the_one_the_body_is_bounded_by() {
        // **The escape hatch is real and not declared.** A venue whose
        // catalogue is genuinely larger than the default has no way past it
        // otherwise: every poll ends `Limited`, the connection ends
        // `remote_close`, the driver reconnects, and the reconnect counter is
        // the only thing that moves. A builder method nothing reads is that
        // same publisher with an extra line in its `main`.
        //
        // Stated downwards, which is the direction a test can assert cheaply
        // and is also a bound worth having: a venue whose catalogue is a
        // hundred kilobytes wants one far tighter than eight megabytes.
        let ceiling = 64 * 1024;
        let (port, server) = an_endpoint_serving_a_lengthless_body(ceiling + 64 * 1024);
        let endpoint = format!("http://127.0.0.1:{port}/catalogue");

        let failure = HttpClient::new()
            .with_max_body_bytes(u64::try_from(ceiling).unwrap())
            .fetch(Request {
                endpoint: &endpoint,
                parameters: None,
                validator: None,
                budget: Duration::from_secs(30),
            })
            .await
            .expect_err("a body past the ceiling this client was given is not an answer");

        assert!(
            matches!(failure, RequestFailure::Transport(_)),
            "{failure:?}"
        );
        // The stated ceiling and not the default, which is the whole of what
        // this asserts: a body of 128 KiB is well under the default, so a
        // client that ignored the override would have answered with it.
        assert!(
            failure.detail().contains(&ceiling.to_string()),
            "the detail names the ceiling that was actually applied: {}",
            failure.detail()
        );
        assert!(
            !failure
                .detail()
                .contains(&DEFAULT_MAX_BODY_BYTES.to_string()),
            "and not the default, which is the number a client that dropped \
             the override would print: {}",
            failure.detail()
        );
        server.join().expect("the endpoint thread");
    }

    /// Serves one response head and then no body bytes at all, holding the
    /// socket open.
    ///
    /// **The head is the whole point.** An endpoint that never answers is a
    /// different failure and already has a test; this one answers, promises a
    /// body, and stops — which is what a stalled proxy between a publisher and
    /// a venue looks like from here. Chunked, so that the client has no length
    /// to decide from that the body it is holding is already whole.
    fn an_endpoint_that_answers_and_then_stops_sending() -> (u16, std::thread::JoinHandle<()>) {
        use std::io::{Read as _, Write as _};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback port to bind");
        let port = listener.local_addr().expect("a bound address").port();
        let handle = std::thread::spawn(move || {
            let Ok((mut socket, _)) = listener.accept() else {
                return;
            };
            let mut scratch = [0_u8; 1024];
            let _ = socket.read(&mut scratch);
            if socket
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                .is_err()
            {
                return;
            }
            let _ = socket.flush();
            // Held open, and then let go on a clock of its own. Open is what
            // makes this a stall rather than a truncated body: a socket that
            // closed would end `collect` with a transport error and the bound
            // under test would never be reached. On a clock, because waiting
            // for the client to close instead would deadlock the `join` below
            // - a blocking join holds the test's worker thread, so the
            // client's connection task never gets polled to notice its client
            // is gone. Well past any budget a test here hands `fetch`.
            std::thread::sleep(Duration::from_secs(2));
        });
        (port, handle)
    }

    #[tokio::test]
    async fn a_body_that_stops_arriving_spends_the_budget_rather_than_never_returning() {
        // **The bound is on the exchange and not on the head.** `Limited`
        // bounds bytes, so a body that stops under the ceiling reaches no
        // ceiling: without a time bound around the collection, `fetch` never
        // returns, `recv` never returns, and the driver never recomputes the
        // idle guard — `connection_state` at 1, no reconnect counted, no
        // payload, and nothing anywhere to look at.
        let (port, server) = an_endpoint_that_answers_and_then_stops_sending();
        let endpoint = format!("http://127.0.0.1:{port}/catalogue");

        // **How this discriminates:** the endpoint holds the socket open for
        // two seconds and the budget is 300ms, so a bound on the exchange ends
        // this at 300ms with a `Timeout`, while a bound on the head alone waits
        // for the socket to close and reports the `Transport` failure of a body
        // that stopped. Both numbers are load-bearing - a budget above the hold
        // makes the two outcomes the same one.
        //
        // And an outer bound well above both, for the reason the ceiling test
        // uses a finite body rather than an endless one: the failure this states
        // must arrive as a failure and not as a suite that hangs in CI.
        let failure = tokio::time::timeout(
            Duration::from_secs(5),
            HttpClient::new().fetch(Request {
                endpoint: &endpoint,
                parameters: None,
                validator: None,
                budget: Duration::from_millis(300),
            }),
        )
        .await
        .expect("`fetch` returns on its own budget, and a bound on the head alone never would")
        .expect_err("a head with no body behind it is not an answer");

        assert!(
            matches!(failure, RequestFailure::Timeout(_)),
            "the request outlived the driver's receive budget, which is a \
             timeout and says nothing about the wire: {failure:?}"
        );
        assert!(
            failure.detail().contains("300ms"),
            "and the detail names the bound that was exceeded: {}",
            failure.detail()
        );
        server.join().expect("the endpoint thread");
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
