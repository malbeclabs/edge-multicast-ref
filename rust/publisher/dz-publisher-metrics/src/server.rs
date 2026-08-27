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
            let response = if *request.method() == Method::Get && request.url() == "/metrics" {
                Response::from_string(metrics.render()).boxed()
            } else {
                Response::empty(StatusCode(404)).boxed()
            };
            let _ = request.respond(response);
        }
    });

    Ok(MetricsServer {
        server,
        handle: Some(handle),
    })
}
