//! The metrics endpoint answers `GET /metrics` with the exposition and 404s
//! elsewhere. Bound to loopback only, as production deployments must be:
//! this endpoint describes a live trading data path.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use dz_publisher_metrics::{serve, PublisherMetrics, PublisherMetricsConfig};

fn http_get(addr: std::net::SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect to metrics server");
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).expect("write request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");
    response
}

#[test]
fn answers_metrics_with_a_known_name_and_404s_elsewhere() {
    let metrics = Arc::new(PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[],
        connections: &[],
        channel_ids: &[],
        ingress_message_types: &[],
    }));
    metrics.ingress().rate_limited();

    let server = serve(Arc::clone(&metrics), "127.0.0.1:0".parse().unwrap())
        .expect("metrics server must bind");
    let addr = server.local_addr().expect("server must report its address");

    let metrics_response = http_get(addr, "/metrics");
    assert!(metrics_response.starts_with("HTTP/1.1 200"));
    assert!(metrics_response.contains("dz_publisher_ingress_rate_limited_total"));

    let missing_response = http_get(addr, "/not-a-real-path");
    assert!(missing_response.starts_with("HTTP/1.1 404"));
}

#[test]
fn answers_metrics_with_a_query_string() {
    // `Request::url()` carries the query string. Comparing it whole would
    // 404 any scrape config that appends one, reported as the confusing
    // "the target is up but returns 404" with nothing to explain it.
    let metrics = Arc::new(PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[],
        connections: &[],
        channel_ids: &[],
        ingress_message_types: &[],
    }));
    metrics.ingress().rate_limited();

    let server = serve(Arc::clone(&metrics), "127.0.0.1:0".parse().unwrap())
        .expect("metrics server must bind");
    let addr = server.local_addr().expect("server must report its address");

    let response = http_get(addr, "/metrics?collect%5B%5D=foo&_=1700000000");
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("dz_publisher_ingress_rate_limited_total"));
}

#[test]
fn a_stalled_scraper_does_not_wedge_the_endpoint() {
    // `respond` writes the body synchronously. If that ran on the accept
    // loop, a peer that stops reading would block every later scrape for
    // the life of the process: the publisher keeps running and looks
    // healthy while its metrics go permanently dark.
    let metrics = Arc::new(PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[],
        connections: &[],
        channel_ids: &[],
        ingress_message_types: &[],
    }));

    let server = serve(Arc::clone(&metrics), "127.0.0.1:0".parse().unwrap())
        .expect("metrics server must bind");
    let addr = server.local_addr().expect("server must report its address");

    // Send a request and never read the response.
    let mut stalled = TcpStream::connect(addr).expect("connect");
    stalled
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .expect("write request");

    let response = http_get(addr, "/metrics");
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");

    drop(stalled);
}
