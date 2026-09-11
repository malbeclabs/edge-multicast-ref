//! The transport.

use std::sync::Arc;
use std::time::Duration;

use dz_adapter_core::{ConnectionId, DisconnectReason};
use dz_ingress_core::{
    BoxFuture, Clock, ConnectFailureReason, IngressError, Input, Received, UpstreamMessage,
};

use crate::client::{Answer, PollClient, Request, RequestFailure, NOT_MODIFIED};
use crate::config::{authority_of, ConfigError, PollConfig};

/// A polled [`Input`].
///
/// One instance is one connection. A publisher taking a catalogue from one
/// endpoint and reference data from another holds two of these with two
/// [`ConnectionId`]s, which is what makes
/// `dz_publisher_ingress_connection_state{connection}` say which one is down —
/// and they should share one [`PollClient`], so that two endpoints on one host
/// share a connection pool.
///
/// # What "connected" means for a transport that has no connection
///
/// A polled transport's connection is a fiction, and the honest mapping is
/// **the endpoint answered the last request**. With that, every series in the
/// closed `dz_publisher_ingress_*` family means for this transport what it
/// means for every other: the state gauge pre-created at 0 is a catalogue that
/// has stopped answering, the reconnect counter counts failures by reason, and
/// the driver's delay sequence paces the retries. An operator's `== 0` alert
/// then fires for the case pre-creating that gauge exists for.
///
/// # Why it holds a [`Clock`] when the websocket transport does not
///
/// That transport waits on a socket, and a test can drive one over loopback in
/// no time at all. This transport's waiting **is** the cadence: a test of the
/// cadence that waits takes as long as the cadence it is testing and proves
/// nothing about it, which is the whole argument the [`Clock`] module makes for
/// existing. So the wait is injected, and `poll_interval = "30s"` costs the
/// suite nothing.
pub struct PollInput {
    connection: ConnectionId,
    endpoint: String,
    /// The scheme, host and port of `endpoint`, and nothing else, from
    /// `authority_of` — which is this crate's one answer to *what of an
    /// endpoint may be printed*, and lives beside the key it reads. See the
    /// `Debug` implementation for why the remainder is dropped, and note that
    /// every error detail this type raises carries this and not the endpoint.
    authority: String,
    poll_interval: Duration,
    client: Arc<dyn PollClient>,
    clock: Arc<dyn Clock>,
    /// What the adapter last wrote through [`Input::send`], held as the next
    /// request's parameters.
    ///
    /// **Per connection, and never anywhere two connections share.** One
    /// adapter object serves every source, so it is handed a [`ConnectionId`] with
    /// every `on_connected` and every `poll_upstream` — two polled sources are
    /// two cursors, and a cursor kept anywhere shared would have one of them
    /// serving both. Cleared on connect for the reason the driver builds a
    /// queue per connection: a cursor the adapter wrote against a connection
    /// that has since gone is one it has had no chance to revise.
    parameters: Option<String>,
    /// The body last handed to the driver, kept alive because the payload
    /// borrows it — and compared against the next one, which is what makes an
    /// unchanged catalogue liveness rather than a payload.
    held: Option<Vec<u8>>,
    /// The entity tag [`held`](Self::held) was served under, offered back so
    /// that an endpoint whose catalogue has not moved can answer `304` rather
    /// than send it again.
    validator: Option<String>,
    /// When the next request falls due, on [`Clock::steady_ns`]. `None` before
    /// the first connect.
    ///
    /// A steady reading and not a wall one, for the reason the [`Clock`] states:
    /// a wall clock a time daemon steps backwards would either poll in a loop
    /// or stop polling, and neither is visible in anything but the request rate.
    next_poll_ns: Option<u64>,
    connected: bool,
}

impl PollInput {
    /// A transport for one endpoint.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for a table this build cannot run — an `https` endpoint
    /// with no TLS stack, something that is not an HTTP endpoint at all, an
    /// endpoint carrying a `#` or a userinfo section, or a cadence below the
    /// floor. Raised here rather than at the first request on purpose — a
    /// publisher whose endpoint or cadence is unusable should fail where it is
    /// diagnosable, and this is also the check a venue's `main` runs whether
    /// or not it called [`PollConfig::check`] itself.
    pub fn new(
        connection: ConnectionId,
        config: &PollConfig,
        client: Arc<dyn PollClient>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ConfigError> {
        config.check()?;
        let endpoint = config.endpoint.trim().to_string();
        Ok(Self {
            connection,
            authority: authority_of(&endpoint),
            endpoint,
            poll_interval: config.poll_interval,
            client,
            clock,
            parameters: None,
            held: None,
            validator: None,
            next_poll_ns: None,
            connected: false,
        })
    }

    /// Forget everything that belonged to one connection.
    ///
    /// Called on connect and on shutdown, so that a reconnect starts where a
    /// first connect does. The validator is the one that matters: kept across a
    /// connection whose body was never delivered, it makes the next poll a
    /// `304` and the adapter never sees the catalogue at all.
    fn forget(&mut self) {
        self.parameters = None;
        self.held = None;
        self.validator = None;
        self.next_poll_ns = None;
    }

    /// The request to make now, from what this connection holds.
    fn request(&self, budget: Duration) -> Request<'_> {
        Request {
            endpoint: &self.endpoint,
            parameters: self.parameters.as_deref(),
            validator: self.validator.as_deref(),
            budget,
        }
    }

    /// Wait, on the injected clock, unless there is nothing to wait for.
    async fn wait(&self, nanos: u64) {
        if nanos > 0 {
            self.clock.sleep(Duration::from_nanos(nanos)).await;
        }
    }
}

/// Prints the connection, the host and whether the endpoint is answering — and
/// **neither the endpoint nor what the adapter wrote**.
///
/// An endpoint carrying a key in its query string is a shape several venue APIs
/// use, and configuration keeps credentials in files precisely so that they do
/// not reach a log line. The adapter's parameters are held to the same
/// standard: a cursor is harmless and a signed page token is not, and nothing
/// here can tell them apart. Whether anything has been written is printed,
/// because that is the first question a poll returning the same catalogue for
/// an hour raises.
impl core::fmt::Debug for PollInput {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PollInput")
            .field("connection", &self.connection)
            .field("authority", &self.authority)
            .field("poll_interval", &self.poll_interval)
            .field("connected", &self.connected)
            .field("parameterised", &self.parameters.is_some())
            .finish()
    }
}

impl Input for PollInput {
    fn connection(&self) -> ConnectionId {
        self.connection
    }

    /// Resolve the endpoint and make the first request.
    ///
    /// **A connect that succeeded without asking the endpoint anything would be
    /// a transport reporting a healthy connection to a host that is not there.**
    /// So this is a real request, and its failures are
    /// [`IngressError::Connect`] with the seven-value taxonomy the family
    /// counts connect failures by — which is where a refused connection, a name
    /// that would not resolve and a certificate that would not verify are
    /// actually distinguishable, since the four disconnect reasons have no word
    /// for any of them.
    ///
    /// # The probe's answer is deliberately thrown away
    ///
    /// Neither its body nor its validator is kept. Keeping the validator would
    /// be the worse bug of the two: the first poll would offer it, a
    /// well-behaved endpoint would answer `304`, this transport would report
    /// liveness, and **the adapter would never see the catalogue at all** — a
    /// publisher whose feed is empty and whose every series says it is healthy.
    /// So the first poll falls due at once and is unconditional, and the cost
    /// of the probe is one extra request per connection.
    fn connect(&mut self, timeout: Duration) -> BoxFuture<'_, Result<(), IngressError>> {
        Box::pin(async move {
            self.forget();
            self.connected = false;
            let answer = self
                .client
                .fetch(self.request(timeout))
                .await
                .map_err(|failure| match connect_reason(&failure) {
                    Some(reason) => IngressError::connect(
                        reason,
                        format!("{}: {}", self.authority, failure.detail()),
                    ),
                    // An endpoint that is not a URI at all.
                    // `PollConfig::check` reads the endpoint as a string and
                    // never as a URI, so this is where that document
                    // arrives - and it arrives on the probe, which carries no
                    // parameters.
                    None => unusable(&self.authority, failure.detail()),
                })?;
            if !is_success(answer.status) {
                return Err(IngressError::connect(
                    connect_reason_for_status(answer.status),
                    format!(
                        "{} answered the first request with status {}",
                        self.authority, answer.status
                    ),
                ));
            }
            // Due at once, because the probe's body went nowhere. See this
            // method's own note.
            self.next_poll_ns = Some(self.clock.steady_ns());
            self.connected = true;
            Ok(())
        })
    }

    /// Hold what the adapter wrote as the next request's parameters.
    ///
    /// This is the mechanism, and there is not a second one: a symbol list at
    /// logon, a cursor after a page, a page token the venue handed back are all
    /// the venue's own syntax, and `send` is how an adapter tells a transport
    /// what to ask upstream. Nothing is parsed here, and nothing is appended to
    /// what was written before — the adapter states the whole of the next
    /// request's parameters each time, because it is the only layer that knows
    /// whether a new cursor replaces the old one or follows it.
    ///
    /// # Errors
    ///
    /// [`IngressError::Fatal`] for [`UpstreamMessage::Binary`]. What an adapter
    /// writes here becomes a request's parameters, and bytes that are not text
    /// are not parameters. Fatal rather than [`IngressError::Ended`] for the
    /// reason the websocket transport gives about an invalid header name: the
    /// adapter would write the same bytes on the next connection, so retrying
    /// under a backoff only hides a mapping that has to be fixed — and of the
    /// two mistakes available, the loud one is the recoverable one.
    ///
    /// **Text is accepted here and may still be unusable.** A string that
    /// makes no URI beside the endpoint — `symbols=BTC USD`, a `#`, a bare
    /// `%` — is [`RequestFailure::Unusable`] on the request that carries it,
    /// and the transport calls that fatal too. Both halves of the input space
    /// therefore end the same way; only the moment differs. Why the moment is
    /// the request rather than this write is that a [`PollClient`] owns URI
    /// formation: a check here would be a second parser, able to disagree with
    /// the one that matters and wrong outright for a client forming no `hyper`
    /// URI — and the endpoint reaches the same failure on the connect probe,
    /// which carries no parameters at all, so a refusal here would leave that
    /// half looping. A `#` is the one exception in the other direction, and
    /// [`PollConfig::check`](crate::PollConfig::check) refuses it in the
    /// endpoint for exactly that reason: it is the character the probe does
    /// *not* fail on.
    fn send<'a>(
        &'a mut self,
        message: UpstreamMessage<'a>,
    ) -> BoxFuture<'a, Result<(), IngressError>> {
        Box::pin(async move {
            match message {
                UpstreamMessage::Text(text) => {
                    self.parameters = Some(text.to_string());
                    Ok(())
                }
                UpstreamMessage::Binary(bytes) => Err(IngressError::fatal(format!(
                    "the adapter wrote {} binary bytes to a polled endpoint; what a poll \
                     sends upstream is the next request's parameters, which are text",
                    bytes.len()
                ))),
            }
        })
    }

    /// Wait for the next request to fall due, make it, and say what came back.
    ///
    /// Three answers, and the second is the one this transport exists to get
    /// right.
    ///
    /// - **A body the endpoint had not sent before is
    ///   [`Received::Payload`]**, with no timestamp of its own: a response body
    ///   carries no receive time this transport knows better than the driver's
    ///   reading of its own clock.
    /// - **A response that says nothing changed is
    ///   [`Received::Liveness`]** — a `304` **to a request that offered a
    ///   validator**, or a body identical to the last one delivered. See below,
    ///   because this is load-bearing.
    /// - **The budget elapsing before the request falls due is
    ///   [`Received::Idle`]**, which is the driver's answer to give and not
    ///   this transport's.
    ///
    /// # Why an unchanged response must not be a payload
    ///
    /// The driver's idle guard counts time since the last **payload**, not
    /// since the last anything, because a venue that has quietly stopped
    /// updating answers a poll perfectly. A transport that reported every
    /// unchanged poll as a payload would make that guard unable to fire on the
    /// one failure it exists for: an endpoint answering `304` for a week is a
    /// catalogue that has stopped changing, and a publisher whose guard cannot
    /// fire on it reports a healthy feed. `tests/polling.rs` asserts exactly
    /// that, and it asserts it as the guard still firing rather than as the
    /// value returned here — a `Liveness` that behaved like a payload would
    /// satisfy any test that only read the discriminant.
    ///
    /// # An unconditional `304` is not an unchanged response, on either path
    ///
    /// *Not modified* is an answer to a validator, and this transport offers
    /// one only for a body it has already delivered — so a `304` to a request
    /// that offered none says nothing about anything. The connect path refuses
    /// exactly that reply (see [`connect_reason_for_status`]), and this one
    /// refuses it too rather than reading a malfunctioning endpoint as a quiet
    /// one: taken as liveness it is the failure the guard exists for arriving
    /// where the guard cannot name it — nothing delivered, the connection held
    /// up, and either a feed reporting health for ever where no `[ingress]
    /// idle_timeout` is configured, or a `timeout` that blames a silent venue
    /// for a reply this transport had already been told was wrong. It ends the
    /// connection as [`disconnect_reason_for_status`] ends any other status the
    /// endpoint should not have returned, and the driver reconnects: an
    /// endpoint that does this on every request cannot deliver, and one that
    /// did it once has a probe to answer before it is asked again.
    ///
    /// # An unchanged body is compared and not digested
    ///
    /// The design says *a body whose digest has not moved*, and an exact
    /// comparison is what is implemented, which is strictly stronger for no
    /// cost: the last body is already held, because the payload handed out
    /// borrows it. A digest would add the one failure a digest has — a
    /// collision is a catalogue change that never reaches the adapter — to buy
    /// nothing this transport needs.
    ///
    /// # Errors
    ///
    /// [`IngressError::Ended`] for a failed request, carrying the reason it
    /// had: a request that timed out is `timeout`, an endpoint that answered
    /// `429` is `rate_limit`, one that answered `401` is `auth_expired`, and a
    /// refusal or a broken connection is `remote_close`. **Not a silent
    /// internal retry** — see this type's own note on what "connected" means
    /// here, which is what makes the connection gauge, the reconnect reasons
    /// and the driver's delay sequence mean the same thing for this transport
    /// as for every other.
    fn recv<'a>(
        &'a mut self,
        budget: Option<Duration>,
    ) -> BoxFuture<'a, Result<Received<'a>, IngressError>> {
        Box::pin(async move {
            if !self.connected {
                // A driver cannot reach this state - it connects before it
                // receives - so this is the case where something else drove the
                // transport. Reported as an ended connection rather than a
                // panic, because a publisher is a long-running process and
                // reconnecting is a better answer than exiting.
                return Err(IngressError::ended(
                    DisconnectReason::RemoteClose,
                    "not connected",
                ));
            }

            let now_ns = self.clock.steady_ns();
            let due_ns = self.next_poll_ns.unwrap_or(now_ns);
            let deadline_ns = budget.map(|budget| {
                now_ns.saturating_add(u64::try_from(budget.as_nanos()).unwrap_or(u64::MAX))
            });

            // The budget elapsing first is the driver's business, so the wait
            // is spent and `Idle` is returned rather than the request being
            // made early. A tie goes to the budget: the two outcomes end the
            // connection with the same reason, and `Idle` is the one that says
            // the guard fired.
            if let Some(deadline_ns) = deadline_ns {
                if deadline_ns <= due_ns {
                    self.wait(deadline_ns.saturating_sub(now_ns)).await;
                    return Ok(Received::Idle);
                }
            }

            self.wait(due_ns.saturating_sub(now_ns)).await;

            // The request's own bound is what is left of the driver's receive
            // budget, which is the timeout the family already has, and the
            // cadence when there is none — with no idle guard configured the
            // driver hands over `None`, and a request with no bound at all is a
            // transport that never returns from a receive.
            let sent_ns = self.clock.steady_ns();
            let request_budget = match deadline_ns {
                Some(deadline_ns) => Duration::from_nanos(deadline_ns.saturating_sub(sent_ns)),
                None => self.poll_interval,
            };

            // Read before the answer can replace it: a `304` is *not modified*
            // only in answer to a validator, and whether this request offered
            // one is a property of the request rather than of the reply. See
            // where it is used below.
            let conditional = self.validator.is_some();
            let outcome = self.client.fetch(self.request(request_budget)).await;

            // Measured from the request going out rather than from its answer
            // coming back, because `poll_interval` is the time between two
            // requests. A request that outlived the interval leaves the next
            // one due at once, which is the honest reading of a cadence that
            // has been missed.
            self.next_poll_ns =
                Some(sent_ns.saturating_add(
                    u64::try_from(self.poll_interval.as_nanos()).unwrap_or(u64::MAX),
                ));

            let Answer {
                status,
                body,
                validator,
            } = outcome.map_err(|failure| match disconnect_reason(&failure) {
                Some(reason) => {
                    IngressError::ended(reason, format!("{}: {}", self.authority, failure.detail()))
                }
                // The parameters the adapter wrote, which the driver must not
                // retry: see `unusable`.
                None => unusable(&self.authority, failure.detail()),
            })?;

            if status == NOT_MODIFIED {
                if !conditional {
                    // *Not modified* than what? This request asked nothing
                    // about a version, so the answer is malformed rather than
                    // quiet, and it is the same malformed answer the connect
                    // path already refuses - see `connect_reason_for_status`.
                    // Read as liveness it would be worse than a status the
                    // endpoint should not have returned: the connection stays
                    // up and nothing is ever delivered, which with no
                    // `[ingress] idle_timeout` configured is a feed reporting
                    // health for ever, and with one is a `timeout` blaming a
                    // silent venue for a reply this transport had already been
                    // told was wrong.
                    return Err(IngressError::ended(
                        disconnect_reason_for_status(status),
                        format!(
                            "{} answered status {status} to a request that offered no \
                             validator, and there is nothing that answer can mean: this \
                             transport offers one only for a body it has delivered",
                            self.authority
                        ),
                    ));
                }
                // The endpoint proved it is alive and produced nothing for the
                // adapter. Deliberately not a payload.
                return Ok(Received::Liveness);
            }
            if !is_success(status) {
                return Err(IngressError::ended(
                    disconnect_reason_for_status(status),
                    format!("{} answered with status {status}", self.authority),
                ));
            }
            // A body identical to the last one delivered. Same case as a `304`
            // and the same answer, for an endpoint that offers no validator.
            if self.held.as_deref() == Some(body.as_slice()) {
                self.validator = validator;
                return Ok(Received::Liveness);
            }

            self.held = Some(body);
            self.validator = validator;
            Ok(Received::Payload {
                // `None`, and the driver stamps it: a response body carries no
                // receive time this transport knows better than the driver's.
                ts_ns: None,
                bytes: self.held.as_deref().unwrap_or_default(),
            })
        })
    }

    /// Release the connection.
    ///
    /// There is no socket to close — the client owns the pool, and a pool
    /// outliving one connection is what makes it a pool. What this releases is
    /// the connection's own state, so that the next connect starts where a
    /// first one does.
    fn shutdown(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.connected = false;
            self.forget();
        })
    }
}

/// Whether the endpoint answered with a body worth reading.
const fn is_success(status: u16) -> bool {
    status >= 200 && status < 300
}

/// A failed **first** request, in the seven words
/// `dz_publisher_ingress_connect_failures_total{reason}` counts by.
///
/// Every value distinct, because this is the one place in this transport where
/// the distinctions survive: a refusal is a firewall or a port, a name that
/// would not resolve is DNS or a typo, and a certificate that would not verify
/// is a trust store or an expiry. Three different people's problem, and a
/// string nobody groups by cannot tell them apart.
///
/// `None` for [`RequestFailure::Unusable`], and that is not a gap in the
/// taxonomy: nothing was connected to, so there is no connect failure to
/// count. The caller raises [`unusable`]'s fault instead.
const fn connect_reason(failure: &RequestFailure) -> Option<ConnectFailureReason> {
    match failure {
        RequestFailure::Refused(_) => Some(ConnectFailureReason::Refused),
        RequestFailure::Unresolved(_) => Some(ConnectFailureReason::Unresolved),
        RequestFailure::Tls(_) => Some(ConnectFailureReason::Tls),
        RequestFailure::Timeout(_) => Some(ConnectFailureReason::Timeout),
        // Established and then broken, which is not a refusal and has no
        // nearer value than the one that means *the far side said no*.
        RequestFailure::Transport(_) => Some(ConnectFailureReason::Rejected),
        RequestFailure::Unusable(_) => None,
    }
}

/// The fault a request that could not be formed raises, on either path.
///
/// [`IngressError::Fatal`] and not a connection that ended, because `Ended` is
/// retried under the driver's delay sequence and the same request is formed on
/// the next attempt. That is the same reasoning
/// [`send`](PollInput::send) gives for refusing
/// [`UpstreamMessage::Binary`], applied to the other half of the input space:
/// of the two mistakes available, the loud one is the recoverable one.
///
/// # Why the transport and not `send`
///
/// A parameter string that is not URI-safe could be refused at the write, and
/// symmetry with `Binary` argues for it. It is refused here instead, for two
/// reasons. **A [`PollClient`] owns URI formation** — nothing above it parses
/// either half of a request, deliberately, so a `send` judging text with
/// `hyper`'s parser would be a second parser able to disagree with the one
/// that matters, and wrong outright for a client that forms no `hyper` URI.
/// And **the endpoint reaches the same failure**: [`PollConfig::check`] reads
/// the endpoint as a string and never as a URI — a scheme prefix, a `#`, an
/// `@` in the authority — so an endpoint that is not a URI for any other
/// reason arrives on the connect probe, which carries no parameters at all.
/// One value covers both; a refusal at the write would leave that half
/// looping.
///
/// The `#` is checked at load rather than left to the probe because the probe
/// does not fail on one. A fragment parses: the request goes out with its
/// query string swallowed, the endpoint answers, and nothing fails anywhere —
/// so for that character alone, *the endpoint reaches the same failure* would
/// be false, and the check is what makes it true.
///
/// The detail names neither the URI nor the parameters, for the reason every
/// other detail here names the authority instead: a venue endpoint's query
/// string is where several venue APIs keep a key.
fn unusable(authority: &str, detail: &str) -> IngressError {
    IngressError::fatal(format!(
        "{authority}: {detail}; the endpoint and the parameters the adapter last wrote do not \
         form a request URI, and the next attempt forms the same one"
    ))
}

/// A status on the **first** request, in the same seven words.
///
/// `304` is here rather than being liveness, and that is not an oversight: the
/// probe offers no validator, so an endpoint answering *not modified* to an
/// unconditional request has answered something it should not have, and reading
/// it as *alive with nothing new* would be reading a malfunction as health. The
/// receive path holds the same line for the same reply — see
/// [`PollInput::recv`], where an unconditional `304` ends the connection rather
/// than counting as liveness.
const fn connect_reason_for_status(status: u16) -> ConnectFailureReason {
    match status {
        401 | 403 => ConnectFailureReason::Unauthorized,
        429 => ConnectFailureReason::RateLimit,
        _ => ConnectFailureReason::Rejected,
    }
}

/// A failed request on an **established** connection, in the four words
/// `dz_publisher_ingress_reconnects_total{reason}` counts by.
///
/// Three of the five network failures land on `remote_close`, and the flatness
/// is the finding rather than laziness: the four reasons all describe a session
/// that existed and then stopped, and *the endpoint stopped answering* is what a
/// refusal, an unresolvable name and a broken body all are once a connection
/// has been proven. The seven-value taxonomy that does separate them is
/// [`connect_reason`]'s, and it is reached on the very next attempt, because a
/// failed request ends the connection and the driver reconnects. Nothing is
/// lost; it is counted under the series that has a word for it.
///
/// `None` for [`RequestFailure::Unusable`], which is the sixth and is not a
/// session that stopped: **a reason here would be a reason the driver retries
/// under**, and that request cannot succeed. The caller raises [`unusable`]'s
/// fault instead.
const fn disconnect_reason(failure: &RequestFailure) -> Option<DisconnectReason> {
    match failure {
        RequestFailure::Timeout(_) => Some(DisconnectReason::Timeout),
        RequestFailure::Refused(_)
        | RequestFailure::Unresolved(_)
        | RequestFailure::Tls(_)
        | RequestFailure::Transport(_) => Some(DisconnectReason::RemoteClose),
        RequestFailure::Unusable(_) => None,
    }
}

/// A status the endpoint should not have returned on an established
/// connection, in the same four words.
///
/// The two that are not `remote_close` are the two an operator acts differently
/// on, and both change what the driver does next: `rate_limit` never resets the
/// delay sequence, so a venue that has just told us to slow down is not
/// reconnected against at the initial delay; and `auth_expired` is a credential
/// to look at rather than an endpoint to look at.
///
/// `304` reaches this too, and only ever from a request that offered no
/// validator — [`PollInput::recv`] answers the conditional one with liveness
/// before it gets here. `remote_close` for it, which is what the four words
/// have for an endpoint that answered something it should not have.
const fn disconnect_reason_for_status(status: u16) -> DisconnectReason {
    match status {
        401 | 403 => DisconnectReason::AuthExpired,
        429 => DisconnectReason::RateLimit,
        _ => DisconnectReason::RemoteClose,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_five_network_failures_do_not_collapse_onto_one_connect_reason() {
        // Written out as pairs rather than derived, for the reason the codec's
        // vocabulary tests give: a table checked only against itself is a table
        // that agrees with its own mistake.
        let detail = || "detail".to_string();
        assert_eq!(
            connect_reason(&RequestFailure::Refused(detail())),
            Some(ConnectFailureReason::Refused)
        );
        assert_eq!(
            connect_reason(&RequestFailure::Unresolved(detail())),
            Some(ConnectFailureReason::Unresolved)
        );
        assert_eq!(
            connect_reason(&RequestFailure::Tls(detail())),
            Some(ConnectFailureReason::Tls)
        );
        assert_eq!(
            connect_reason(&RequestFailure::Timeout(detail())),
            Some(ConnectFailureReason::Timeout)
        );
        assert_eq!(
            connect_reason(&RequestFailure::Transport(detail())),
            Some(ConnectFailureReason::Rejected)
        );
    }

    #[test]
    fn a_request_that_could_not_be_formed_has_no_reason_in_either_taxonomy() {
        // The sixth failure, and the assertion is that it reaches neither
        // metric: a connect reason would be a connect that was attempted, and
        // a disconnect reason is a reason the driver retries under - which is
        // the loop this value exists to refuse.
        let detail = || "the request URI is not usable: invalid uri character".to_string();
        assert_eq!(connect_reason(&RequestFailure::Unusable(detail())), None);
        assert_eq!(disconnect_reason(&RequestFailure::Unusable(detail())), None);
    }

    #[test]
    fn the_five_network_failures_all_have_a_disconnect_reason_to_retry_under() {
        // The other side of the test above: every value that *is* a network
        // event must keep one, so that narrowing `disconnect_reason` to the
        // fatal answer cannot pass.
        let detail = || "detail".to_string();
        for failure in [
            RequestFailure::Refused(detail()),
            RequestFailure::Unresolved(detail()),
            RequestFailure::Tls(detail()),
            RequestFailure::Timeout(detail()),
            RequestFailure::Transport(detail()),
        ] {
            assert!(
                disconnect_reason(&failure).is_some(),
                "{failure:?} is a connection that stopped, and the reconnect \
                 counter has a word for it"
            );
            assert!(connect_reason(&failure).is_some(), "{failure:?}");
        }
    }

    #[test]
    fn the_fault_a_request_that_could_not_be_formed_raises_carries_no_query_string() {
        // Fatal, so that the driver stops instead of forming the same request
        // for ever - and naming the authority rather than the URI, because a
        // venue endpoint's query string is where several venue APIs keep a
        // key.
        let error = unusable(
            "http://192.0.2.10",
            "the request URI is not usable: invalid uri character",
        );
        assert!(error.is_fatal(), "{error}");
        let rendered = format!("{error}");
        assert!(rendered.contains("192.0.2.10"), "{rendered}");
        assert!(!rendered.contains("api_key"), "{rendered}");
    }

    #[test]
    fn a_rejected_first_request_names_the_credential_or_the_limit_rather_than_the_endpoint() {
        // The two startup failures a venue API produces most and the two an
        // operator acts differently on: a secret to rotate, and a limit to
        // respect.
        assert_eq!(
            connect_reason_for_status(401),
            ConnectFailureReason::Unauthorized
        );
        assert_eq!(
            connect_reason_for_status(403),
            ConnectFailureReason::Unauthorized
        );
        assert_eq!(
            connect_reason_for_status(429),
            ConnectFailureReason::RateLimit
        );
        // The redirects are in this list rather than absent from it, and that
        // is the assertion that says they are not followed: `hyper` implements
        // no redirect handling, so a `301` is a status the endpoint should not
        // have answered the probe with. A venue that has moved its catalogue
        // is an endpoint to change in the document, not one this transport
        // chases to a host nobody configured.
        for status in [
            301,
            302,
            307,
            308,
            400,
            404,
            418,
            500,
            502,
            503,
            NOT_MODIFIED,
        ] {
            assert_eq!(
                connect_reason_for_status(status),
                ConnectFailureReason::Rejected,
                "{status} is not a statement about our credential or our rate"
            );
        }
    }

    #[test]
    fn a_status_the_endpoint_should_not_have_returned_is_not_all_one_disconnect_reason() {
        assert_eq!(
            disconnect_reason_for_status(429),
            DisconnectReason::RateLimit,
            "a venue that has told us to slow down must not reset the delay sequence"
        );
        assert_eq!(
            disconnect_reason_for_status(401),
            DisconnectReason::AuthExpired
        );
        assert_eq!(
            disconnect_reason_for_status(403),
            DisconnectReason::AuthExpired
        );
        for status in [
            301,
            302,
            307,
            308,
            400,
            404,
            500,
            502,
            503,
            // The unconditional `304`, which reaches this function for the
            // reason the connect side's list has it too: it is a reply an
            // endpoint should not have made, and `remote_close` is what the
            // four words have for one. The conditional `304` never gets here -
            // `recv` answers that with liveness.
            NOT_MODIFIED,
        ] {
            assert_eq!(
                disconnect_reason_for_status(status),
                DisconnectReason::RemoteClose,
                "{status}"
            );
        }
    }

    #[test]
    fn only_the_two_hundreds_are_a_body_worth_reading() {
        for status in [200, 201, 204, 299] {
            assert!(is_success(status), "{status}");
        }
        for status in [100, 199, 300, 301, 302, NOT_MODIFIED, 400, 500] {
            assert!(!is_success(status), "{status}");
        }
    }
}
