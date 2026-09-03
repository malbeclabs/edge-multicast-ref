//! One POST with a body, behind a trait.
//!
//! The trait is what makes batching and retry testable with no server: a fake
//! that records the requests it was given and hands back the responses a test
//! chose covers every branch of the sink, and the one implementation over HTTP
//! stays small enough to read.
//!
//! It is also the seam an outage arrives through. Everything here returns a
//! [`TransportError`] rather than blocking without a bound, because the loader
//! shares a directory with a recorder and a column store that is down must cost
//! loading progress and nothing else.

use std::time::Duration;

use thiserror::Error;

use crate::config::Credentials;

/// A request could not be completed, or completed with a refusal.
///
/// The two are separate because they retry differently, and the sink is what
/// decides: a connection that was refused may be a server restarting, and a
/// statement the server rejected will be rejected again however many times it is
/// sent.
#[derive(Debug, Error)]
pub enum TransportError {
    /// The request never got an answer: the destination is unreachable, the
    /// connection died, or the timeout expired. Worth another attempt.
    #[error("{url}: {message}")]
    Unreachable { url: String, message: String },
    /// The server answered, and the answer was a failure. The status and the
    /// body are carried verbatim, because a column store's own message names the
    /// column it could not parse and nothing else here can.
    #[error("{url}: HTTP {status}: {body}")]
    Refused {
        url: String,
        status: u16,
        body: String,
    },
}

impl TransportError {
    /// Whether sending the same bytes again could plausibly succeed.
    ///
    /// A 5xx is the server's own admission that the failure is not the
    /// request's, and 429 is the server asking for the request again later.
    /// Every other 4xx is the request being wrong — a column that does not
    /// exist, a value that will not parse, a password that is not the password —
    /// and a loader that retried those would spend its attempts learning nothing
    /// and then report the same error it had after the first.
    #[must_use]
    pub const fn is_worth_retrying(&self) -> bool {
        match self {
            Self::Unreachable { .. } => true,
            Self::Refused { status, .. } => *status >= 500 || *status == 429,
        }
    }
}

/// What a completed request said.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    pub body: String,
}

/// Somewhere a statement and a body can be posted.
pub trait Transport {
    /// Posts `body` to `url` and returns what the server said.
    ///
    /// # Errors
    ///
    /// [`TransportError`]. An implementation must not block without a bound: a
    /// destination that never answers has to become an error, or the loader
    /// stops making progress on every other object while looking busy.
    fn post(
        &self,
        url: &str,
        credentials: &Credentials,
        body: &[u8],
    ) -> Result<Response, TransportError>;
}

/// HTTP/1.1, over a plain connection.
///
/// No TLS in this build. The loader runs on the recorder host and reaches the
/// column store over a private path, and an `https://` endpoint is refused at
/// configuration load rather than silently downgraded — see
/// [`ClickHouseConfig::check`](crate::ClickHouseConfig::check).
#[derive(Debug, Clone)]
pub struct HttpTransport {
    timeout: Duration,
}

impl HttpTransport {
    #[must_use]
    pub const fn new(timeout: Duration) -> Self {
        Self { timeout }
    }
}

impl Transport for HttpTransport {
    fn post(
        &self,
        url: &str,
        credentials: &Credentials,
        body: &[u8],
    ) -> Result<Response, TransportError> {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(self.timeout))
            // A redirect would re-send the body to somewhere the configuration
            // did not name, and the credentials with it.
            .max_redirects(0)
            // A status outside 2xx comes back as a *response* and not as an
            // error, so the body can be read off it. This client's default is
            // the other way round, and that default discards exactly the thing
            // worth keeping: a column store's own message names the column it
            // could not parse, and a bounded retry that throws it away leaves an
            // operator with a status code and no cause.
            .http_status_as_error(false)
            .build()
            .into();

        let mut request = agent
            .post(url)
            // The user goes in a header rather than in the query string, because
            // a query string is what ends up in an access log.
            .header("X-ClickHouse-User", &credentials.user)
            .header("Content-Type", "application/json");
        if let Some(password) = credentials.password() {
            request = request.header("X-ClickHouse-Key", password);
        }

        match request.send(body) {
            Ok(mut response) => {
                let status = response.status().as_u16();
                let body = response
                    .body_mut()
                    .read_to_string()
                    .unwrap_or_else(|e| format!("<the response body was unreadable: {e}>"));
                if (200..300).contains(&status) {
                    Ok(Response { status, body })
                } else {
                    Err(TransportError::Refused {
                        url: url.to_owned(),
                        status,
                        body,
                    })
                }
            }
            // Only reachable if the configuration above is ever changed back:
            // kept so that a status arriving as an error is still reported as a
            // refusal rather than as an unreachable destination, which retries
            // differently.
            Err(ureq::Error::StatusCode(status)) => Err(TransportError::Refused {
                url: url.to_owned(),
                status,
                body: String::new(),
            }),
            Err(e) => Err(TransportError::Unreachable {
                url: url.to_owned(),
                message: e.to_string(),
            }),
        }
    }
}
