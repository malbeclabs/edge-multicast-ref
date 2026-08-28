use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::JoinHandle;

use tiny_http::{Method, Response, StatusCode};

use crate::PublisherMetrics;

/// A background HTTP endpoint serving `GET /metrics`.
///
/// Dropping this value stops the server's accept loop and joins its thread.
///
/// This endpoint must be bound to a non-public interface: it describes a
/// live trading data path, including its instrument set and its timing, and
/// exposing it publicly leaks both.
#[must_use = "the endpoint stops serving as soon as this value is dropped"]
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

            // Every request leaves this thread, and nothing here may drop
            // one. `respond` writes the whole body synchronously, so a peer
            // whose receive window fills and stays full - a scraper killed
            // mid-scrape, a blackholed route, a paused container - would
            // block the accept loop forever and take every later scrape
            // with it. The publisher would keep running and look healthy
            // while its metrics went permanently dark.
            //
            // Dropping a request is not an escape: `tiny_http`'s `Drop for
            // Request` writes a 500 through the same synchronous path, so
            // disposing of one here would block exactly as responding does.
            // Handing every request to its own thread is what keeps this
            // loop free, and it is why there is no in-flight cap: a cap
            // would have to dispose of the requests past it, on this
            // thread, which is the thing that cannot be done safely. A
            // stalled peer therefore costs one thread until its socket
            // times out, and this endpoint is bound to a non-public
            // interface serving one scraper every few seconds.
            std::thread::spawn(move || {
                let _ = request.respond(response);
            });
        }
    });

    Ok(MetricsServer {
        server,
        handle: Some(handle),
    })
}
