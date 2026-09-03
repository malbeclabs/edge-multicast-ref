//! `GET /metrics`, in the shape the recorder already serves it.
//!
//! The same shape rather than a second one: a scrape configuration, a 404 on a
//! query string, and the reason every response leaves the accept loop are all
//! things this repository has decided once.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::JoinHandle;

use tiny_http::{Method, Response, StatusCode};

use crate::metrics::LoaderMetrics;

/// A background HTTP endpoint serving `GET /metrics`.
///
/// Dropping this value stops the accept loop and joins its thread.
///
/// Bind it to a non-public interface. It describes a live data path — the feeds,
/// the sites and the timing of an archive — and exposing it publicly leaks all
/// of that.
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
/// Failing is the point. Lag is the metric this whole tier is gated on — objects
/// are evicted under the recorder's staging budget, so a loader slower than the
/// write rate loses history permanently and silently — and a loader nobody can
/// scrape is a loader whose lag nobody can see.
///
/// # Errors
///
/// [`io::Error`] if the address cannot be bound. The most likely cause is the
/// recorder's own metrics port, which is why the default here is not it.
pub fn serve(metrics: Arc<LoaderMetrics>, addr: SocketAddr) -> io::Result<MetricsServer> {
    let server = Arc::new(tiny_http::Server::http(addr).map_err(io::Error::other)?);

    let worker_server = Arc::clone(&server);
    let handle = std::thread::Builder::new()
        .name("dz-loader-metrics".to_owned())
        .spawn(move || {
            for request in worker_server.incoming_requests() {
                // `url()` carries the query string, so an exact comparison would
                // 404 a scrape configuration that appends one — a "target is up
                // but returns 404" with nothing to explain it.
                let path = request.url().split('?').next().unwrap_or_default();
                let is_metrics = *request.method() == Method::Get && path == "/metrics";
                let metrics = Arc::clone(&metrics);

                // Every request leaves this thread. `respond` writes the whole
                // body synchronously, so a peer whose receive window fills and
                // stays full — a scraper killed mid-scrape, a blackholed route —
                // would block the accept loop for ever and take every later
                // scrape with it. The loader would keep loading and look healthy
                // while its lag went dark.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read as _, Write as _};
    use std::net::TcpStream;

    /// One request, written by hand: this crate has no HTTP client, and the
    /// endpoint's whole contract is two paths and two status codes.
    fn get(addr: SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(addr).expect("the endpoint is listening");
        write!(
            stream,
            "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
        )
        .expect("the request is writable");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .expect("the response is readable");
        response
    }

    #[test]
    fn the_endpoint_serves_the_exposition_and_404s_everything_else() {
        let metrics = Arc::new(LoaderMetrics::new("site-1", "recorder-1"));
        let server = serve(metrics, SocketAddr::from(([127, 0, 0, 1], 0)))
            .expect("port 0 binds to something");
        let addr = server.local_addr().expect("an address was bound");

        let metrics_response = get(addr, "/metrics");
        assert!(metrics_response.contains("200 OK"), "{metrics_response}");
        assert!(
            metrics_response.contains("dz_loader_objects_loaded_total"),
            "{metrics_response}"
        );

        // A scrape configuration that appends a query string must not 404: that
        // is a "target is up but returns 404" with nothing to explain it.
        let with_query = get(addr, "/metrics?collect[]=nothing");
        assert!(with_query.contains("200 OK"), "{with_query}");

        let other = get(addr, "/");
        assert!(other.contains("404"), "{other}");
    }

    /// A loader nobody can scrape is a loader whose lag nobody can see, so a
    /// port that cannot be bound is a startup failure and not a warning.
    #[test]
    fn a_port_that_cannot_be_bound_is_an_error_rather_than_a_warning() {
        let metrics = Arc::new(LoaderMetrics::new("s", "r"));
        let first = serve(Arc::clone(&metrics), SocketAddr::from(([127, 0, 0, 1], 0)))
            .expect("the first bind works");
        let taken = first.local_addr().expect("an address was bound");
        assert!(
            serve(metrics, taken).is_err(),
            "binding a port already in use has to fail"
        );
    }
}
