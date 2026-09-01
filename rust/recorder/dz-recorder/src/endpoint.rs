//! `GET /metrics`, in the shape `dz-publisher-metrics` already serves it.
//!
//! The same shape rather than a second one: a scrape configuration, a 404 on a
//! query string, and the reason every response leaves the accept loop are all
//! things this repository has already decided once.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::JoinHandle;

use dz_recorder_health::HealthMetrics;
use tiny_http::{Method, Response, StatusCode};

/// A background HTTP endpoint serving `GET /metrics`.
///
/// Dropping this value stops the accept loop and joins its thread.
///
/// Bind it to a non-public interface. It describes a live data path — the
/// groups, the ports, the sources and the timing of a feed — and exposing it
/// publicly leaks all of that.
#[must_use = "the endpoint stops serving as soon as this value is dropped"]
pub struct MetricsServer {
    server: Arc<tiny_http::Server>,
    handle: Option<JoinHandle<()>>,
}

impl MetricsServer {
    /// The address actually bound, which is not the configured one when the
    /// configuration asked for port 0.
    #[must_use]
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.server.server_addr().to_ip()
    }
}

impl Drop for MetricsServer {
    fn drop(&mut self) {
        self.server.unblock();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Starts the endpoint, or fails.
///
/// Failing is the point: a recorder whose metrics never bind is a recorder
/// nobody can see, and a health tier nobody scrapes is the archive-and-forget
/// recorder the design rejects.
pub fn serve(metrics: Arc<HealthMetrics>, addr: SocketAddr) -> io::Result<MetricsServer> {
    let server = Arc::new(tiny_http::Server::http(addr).map_err(io::Error::other)?);

    let worker_server = Arc::clone(&server);
    let handle = std::thread::Builder::new()
        .name("dz-recorder-metrics".to_owned())
        .spawn(move || {
            for request in worker_server.incoming_requests() {
                // `url()` carries the query string, so an exact comparison
                // would 404 a scrape configuration that appends one — a
                // "target is up but returns 404" with nothing to explain it.
                let path = request.url().split('?').next().unwrap_or_default();
                let is_metrics = *request.method() == Method::Get && path == "/metrics";
                let metrics = Arc::clone(&metrics);

                // Every request leaves this thread. `respond` writes the whole
                // body synchronously, so a peer whose receive window fills and
                // stays full — a scraper killed mid-scrape, a blackholed route,
                // a paused container — would block the accept loop for ever and
                // take every later scrape with it. The recorder would keep
                // recording and look healthy while its metrics went dark.
                //
                // Dropping the request is not an escape either: tiny_http's
                // `Drop for Request` writes a 500 through the same synchronous
                // path. That is why there is no in-flight cap.
                std::thread::spawn(move || {
                    let response = if is_metrics {
                        Response::from_string(metrics.render()).boxed()
                    } else {
                        Response::empty(StatusCode(404)).boxed()
                    };
                    let _ = request.respond(response);
                });
            }
        })?;

    Ok(MetricsServer {
        server,
        handle: Some(handle),
    })
}
