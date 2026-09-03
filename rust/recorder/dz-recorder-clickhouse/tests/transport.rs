//! The real HTTP transport, against a listener that records what arrived.
//!
//! This is the one seam no fake can cover, and the one that failed silently:
//! a client that sent an empty body would have every request accepted, every
//! row reported written, and nothing in the table — which is exactly what a
//! column store answers `200` to. So the bytes on the wire are asserted here.
#![forbid(unsafe_code)]

use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::net::{SocketAddr, TcpListener};
use std::sync::mpsc;
use std::time::Duration;

use dz_recorder_clickhouse::{Credentials, HttpTransport, Transport, TransportError};

/// One request, as it arrived on the socket.
#[derive(Debug)]
struct Arrived {
    request_line: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Arrived {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Accepts one request, answers with `status`, and hands the request back.
///
/// Written by hand because the assertion is about the bytes: a client library
/// standing in for the server would agree with the client under test.
fn serve_one(status: u16) -> (SocketAddr, mpsc::Receiver<Arrived>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("a port");
    let addr = listener.local_addr().expect("an address");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let (stream, _) = listener.accept().expect("a connection");
        let mut reader = BufReader::new(stream);

        let mut request_line = String::new();
        reader.read_line(&mut request_line).expect("a request line");
        let mut headers = Vec::new();
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).expect("a header line");
            let line = line.trim_end();
            if line.is_empty() {
                break;
            }
            if let Some((name, value)) = line.split_once(':') {
                headers.push((name.trim().to_owned(), value.trim().to_owned()));
            }
        }
        let length: usize = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, v)| v.parse().ok())
            .unwrap_or(0);
        let mut body = vec![0u8; length];
        reader.read_exact(&mut body).expect("the whole body");

        let response =
            format!("HTTP/1.1 {status} X\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
        reader
            .into_inner()
            .write_all(response.as_bytes())
            .expect("the response is writable");

        let _ = tx.send(Arrived {
            request_line: request_line.trim_end().to_owned(),
            headers,
            body,
        });
    });
    (addr, rx)
}

/// **The body reaches the server.**
///
/// A `JSONEachRow` insert whose body were dropped is answered `200` by a column
/// store, which inserts nothing: every row would be reported written and the
/// table would be empty. Nothing in a fake transport can catch that, because a
/// fake is handed the bytes directly.
#[test]
fn the_body_handed_to_the_transport_is_the_body_that_arrives() {
    let (addr, rx) = serve_one(200);
    let transport = HttpTransport::new(Duration::from_secs(5));
    let body = b"{\"a\":1}\n{\"a\":2}\n";

    let response = transport
        .post(
            &format!("http://{addr}/?database=recorder"),
            &Credentials::new("loader", Some("from-the-environment".to_owned())),
            body,
        )
        .expect("the listener answered 200");
    assert_eq!(response.status, 200);

    let arrived = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the listener recorded the request");
    assert!(
        arrived
            .request_line
            .starts_with("POST /?database=recorder "),
        "{}",
        arrived.request_line
    );
    assert_eq!(
        arrived.body, body,
        "the body on the wire is not the body handed over"
    );
    assert_eq!(
        arrived.header("content-length"),
        Some(body.len().to_string().as_str())
    );
    // The credentials travel in headers, not in the query string: a query
    // string is what ends up in an access log.
    assert_eq!(arrived.header("x-clickhouse-user"), Some("loader"));
    assert_eq!(
        arrived.header("x-clickhouse-key"),
        Some("from-the-environment")
    );
    assert!(
        !arrived.request_line.contains("loader"),
        "{}",
        arrived.request_line
    );
}

/// A refusal carries the status and the server's own body, because a column
/// store's message names the column it could not parse and nothing else here
/// can.
#[test]
fn a_refusal_carries_the_status_and_the_servers_own_message() {
    let (addr, _rx) = serve_one(400);
    let transport = HttpTransport::new(Duration::from_secs(5));
    let error = transport
        .post(
            &format!("http://{addr}/"),
            &Credentials::new("loader", None),
            b"{}\n",
        )
        .expect_err("400 is not a success");
    let TransportError::Refused { status, body, .. } = &error else {
        panic!("expected a refusal, got {error}");
    };
    assert_eq!(*status, 400);
    assert_eq!(body, "ok", "the server's own body, verbatim");
    assert!(
        !error.is_worth_retrying(),
        "a request the server rejected will be rejected again"
    );
}

/// A 5xx is the server's own admission that the failure is not the request's.
#[test]
fn a_server_failure_is_worth_another_attempt_and_a_client_error_is_not() {
    let (addr, _rx) = serve_one(503);
    let transport = HttpTransport::new(Duration::from_secs(5));
    let error = transport
        .post(
            &format!("http://{addr}/"),
            &Credentials::new("loader", None),
            b"{}\n",
        )
        .expect_err("503 is not a success");
    assert!(error.is_worth_retrying(), "{error}");
}

/// A destination that is not there becomes an error rather than a wait without
/// a bound: the loader shares a directory with a recorder, and a column store
/// that is down must cost loading progress and nothing else.
#[test]
fn a_destination_that_is_not_there_is_unreachable_and_retryable() {
    // Bound and dropped, so nothing is listening on a port nothing else took.
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("a port");
    let addr = listener.local_addr().expect("an address");
    drop(listener);

    let transport = HttpTransport::new(Duration::from_secs(2));
    let error = transport
        .post(
            &format!("http://{addr}/"),
            &Credentials::new("loader", None),
            b"{}\n",
        )
        .expect_err("nothing is listening");
    assert!(
        matches!(error, TransportError::Unreachable { .. }),
        "{error}"
    );
    assert!(error.is_worth_retrying());
}
