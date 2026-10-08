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
    // Compressed on the wire, and labelled as such: a column store decompresses
    // a request body by this header and by nothing else, so a compressed body
    // without it is rows the server cannot parse.
    assert_eq!(arrived.header("content-encoding"), Some("zstd"));
    assert_eq!(
        zstd::stream::decode_all(arrived.body.as_slice()).expect("the body is one zstd frame"),
        body,
        "the body on the wire does not decompress to the body handed over"
    );
    assert_eq!(
        arrived.header("content-length"),
        Some(arrived.body.len().to_string().as_str())
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

/// A body of the shape the sink sends is several times smaller on the wire.
///
/// This is the reason the compression exists: a recorder far from the column
/// store sends at the rate the path allows and no faster, so the bytes are what
/// decide whether the loader keeps up.
#[test]
fn a_body_of_repeated_rows_is_several_times_smaller_on_the_wire() {
    let (addr, rx) = serve_one(200);
    let transport = HttpTransport::new(Duration::from_secs(5));
    let mut body = Vec::new();
    for i in 0..5_000u32 {
        body.extend_from_slice(
            format!(
                "{{\"recorder\":\"aws-tyo-mn-feedrecorder1\",\"feed\":\"tob_edge_binance_usdsm\",\"recv_ts\":{},\"bid_px_raw\":{},\"ask_px_raw\":{}}}\n",
                1_759_900_000_000_000_000u64 + u64::from(i) * 1_000,
                6_000_000 + i,
                6_000_010 + i
            )
            .as_bytes(),
        );
    }

    transport
        .post(
            &format!("http://{addr}/"),
            &Credentials::new("loader", None),
            &body,
        )
        .expect("the listener answered 200");

    let arrived = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the listener recorded the request");
    assert!(
        arrived.body.len() * 4 < body.len(),
        "{} bytes on the wire for a body of {}",
        arrived.body.len(),
        body.len()
    );
    assert_eq!(
        zstd::stream::decode_all(arrived.body.as_slice()).expect("the body is one zstd frame"),
        body
    );
}

/// Two requests through one transport arrive on one connection.
///
/// A connection per body pays a handshake and a slow start for every batch, and
/// pays most on the long paths where the loader was already behind.
#[test]
fn consecutive_requests_share_one_connection() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("a port");
    let addr = listener.local_addr().expect("an address");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        // One accept, and both requests read off it. A client that opened a
        // second connection would wait on a listener nobody is accepting from,
        // and its timeout is what fails the test.
        let (stream, _) = listener.accept().expect("a connection");
        let mut reader = BufReader::new(stream);
        for _ in 0..2 {
            let mut length = 0usize;
            let mut first = true;
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).expect("a line") == 0 {
                    return;
                }
                let line = line.trim_end();
                if first {
                    first = false;
                    continue;
                }
                if line.is_empty() {
                    break;
                }
                if let Some((name, value)) = line.split_once(':') {
                    if name.trim().eq_ignore_ascii_case("content-length") {
                        length = value.trim().parse().expect("a length");
                    }
                }
            }
            let mut body = vec![0u8; length];
            reader.read_exact(&mut body).expect("the whole body");
            reader
                .get_mut()
                .write_all(b"HTTP/1.1 200 X\r\nContent-Length: 2\r\n\r\nok")
                .expect("the response is writable");
            let _ = tx.send(());
        }
    });

    let transport = HttpTransport::new(Duration::from_secs(5));
    for _ in 0..2 {
        transport
            .post(
                &format!("http://{addr}/"),
                &Credentials::new("loader", None),
                b"{\"a\":1}\n",
            )
            .expect("the listener answered 200 on the connection it accepted");
    }
    rx.recv_timeout(Duration::from_secs(5)).expect("the first");
    rx.recv_timeout(Duration::from_secs(5)).expect("the second");
}

/// Three bodies through a transport allowed three at once are all in flight
/// together, each on a connection of its own.
///
/// The listener answers nobody until it has read all three requests, so a
/// transport that sent them one after another would wait on the first answer
/// for ever, and its timeout is what fails the test.
#[test]
fn bodies_sent_together_are_in_flight_together() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("a port");
    let addr = listener.local_addr().expect("an address");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut open = Vec::new();
        let mut bodies = Vec::new();
        for _ in 0..3 {
            let (stream, _) = listener.accept().expect("a connection");
            let mut reader = BufReader::new(stream);
            let mut length = 0usize;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).expect("a line");
                let line = line.trim_end();
                if line.is_empty() {
                    break;
                }
                if let Some((name, value)) = line.split_once(':') {
                    if name.trim().eq_ignore_ascii_case("content-length") {
                        length = value.trim().parse().expect("a length");
                    }
                }
            }
            let mut body = vec![0u8; length];
            reader.read_exact(&mut body).expect("the whole body");
            bodies.push(zstd::stream::decode_all(body.as_slice()).expect("a zstd frame"));
            open.push(reader);
        }
        // All three have arrived and none has been answered.
        for reader in &mut open {
            reader
                .get_mut()
                .write_all(b"HTTP/1.1 200 X\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .expect("the response is writable");
        }
        let _ = tx.send(bodies);
    });

    let transport = HttpTransport::new(Duration::from_secs(5)).with_concurrency(3);
    assert_eq!(transport.concurrency(), 3);
    let bodies: [&[u8]; 3] = [b"{\"a\":1}\n", b"{\"a\":2}\n", b"{\"a\":3}\n"];
    let results = transport.post_all(
        &format!("http://{addr}/"),
        &Credentials::new("loader", None),
        &bodies,
    );
    assert_eq!(results.len(), 3);
    for result in &results {
        assert_eq!(
            result.as_ref().expect("the listener answered 200").status,
            200
        );
    }
    let mut arrived = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the listener saw all three");
    arrived.sort();
    assert_eq!(
        arrived,
        bodies.iter().map(|b| b.to_vec()).collect::<Vec<_>>(),
        "each body arrived once"
    );
}

/// One at a time is the default, and zero is read as one.
#[test]
fn a_transport_sends_one_body_at_a_time_unless_told_otherwise() {
    assert_eq!(HttpTransport::new(Duration::from_secs(5)).concurrency(), 1);
    assert_eq!(
        HttpTransport::new(Duration::from_secs(5))
            .with_concurrency(0)
            .concurrency(),
        1
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
