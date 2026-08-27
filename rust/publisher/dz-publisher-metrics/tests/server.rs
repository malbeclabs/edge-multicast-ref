//! The metrics endpoint answers `GET /metrics` with the exposition and 404s
//! elsewhere. Bound to loopback only, as production deployments must be:
//! this endpoint describes a live trading data path.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use dz_publisher_metrics::{serve, PublisherMetrics};

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
    let metrics = Arc::new(PublisherMetrics::new("test-venue", 1, &[]));
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
