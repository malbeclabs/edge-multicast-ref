use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

use tiny_http::{Method, Response, StatusCode};

use crate::PublisherMetrics;

/// How many responses may be in flight at once. A metrics endpoint serves
/// one scraper every few seconds; this is headroom for overlapping scrapes,
/// not a concurrency target.
const MAX_IN_FLIGHT_RESPONSES: usize = 8;

/// A background HTTP endpoint serving `GET /metrics`.
///
/// Dropping this value stops the server's accept loop and joins its thread.
///
/// This endpoint must be bound to a non-public interface: it describes a
/// live trading data path, including its instrument set and its timing, and
/// exposing it publicly leaks both.
pub struct MetricsServer {
    server: Arc<tiny_http::Server>,
    handle: Option<JoinHandle<()>>,
}

impl MetricsServer {
    /// The address the server actually bound to.
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

/// Starts a background thread answering `GET /metrics` with the Prometheus
/// text exposition of `metrics`, and 404 for anything else.
///
/// # Errors
///
/// Returns an error if the address cannot be bound.
///
/// Bind this to a non-public interface. It describes a live trading data
/// path, including its instrument set and its timing.
pub fn serve(metrics: Arc<PublisherMetrics>, addr: SocketAddr) -> io::Result<MetricsServer> {
    let server = tiny_http::Server::http(addr).map_err(io::Error::other)?;
    let server = Arc::new(server);

    let worker_server = Arc::clone(&server);
    let handle = std::thread::spawn(move || {
        let in_flight = Arc::new(AtomicUsize::new(0));

        for request in worker_server.incoming_requests() {
            // `url()` carries the query string, so an exact comparison
            // would 404 a scrape config that appends one - a confusing
            // "target is up but returns 404" with nothing to explain it.
            let path = request.url().split('?').next().unwrap_or_default();

            let response = if *request.method() == Method::Get && path == "/metrics" {
                Response::from_string(metrics.render()).boxed()
            } else {
                Response::empty(StatusCode(404)).boxed()
            };

            // Written on a short-lived thread, never on this one.
            // `respond` writes the whole body synchronously, so a peer
            // whose receive window fills and stays full - a scraper killed
            // mid-scrape, a blackholed route, a paused container - would
            // block the accept loop forever, and every later scrape with
            // it. The publisher would keep running and look healthy while
            // its metrics went permanently dark.
            //
            // Threads are capped so a stream of stuck peers cannot spawn
            // without bound; past the cap the request is dropped, which
            // closes the connection.
            if in_flight.fetch_add(1, Ordering::AcqRel) >= MAX_IN_FLIGHT_RESPONSES {
                in_flight.fetch_sub(1, Ordering::AcqRel);
                continue;
            }
            let done = Arc::clone(&in_flight);
            std::thread::spawn(move || {
                let _ = request.respond(response);
                done.fetch_sub(1, Ordering::AcqRel);
            });
        }
    });

    Ok(MetricsServer {
        server,
        handle: Some(handle),
    })
}
