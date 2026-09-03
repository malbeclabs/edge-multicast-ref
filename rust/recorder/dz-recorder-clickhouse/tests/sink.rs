//! Batching, retry, and the request body — against literals, with no server.
#![forbid(unsafe_code)]

mod common;

use std::collections::BTreeSet;

use common::{batch, config, no_wait, Answer, FakeTransport};
use dz_recorder_clickhouse::{send_order, ClickHouseSink, Credentials};
use dz_recorder_replay::Fault;
use dz_recorder_rows::{Grain, RowSink, RowSinkError};

fn sink(transport: FakeTransport) -> ClickHouseSink<FakeTransport> {
    ClickHouseSink::with_transport(
        config(),
        Credentials::new("loader", Some("from-the-environment".to_owned())),
        transport,
    )
    .waiting_with(no_wait)
}

/// The body is one JSON object per line, and the statement is in the query
/// string — so a retry re-sends bytes it already has rather than re-serialising
/// them.
#[test]
fn a_request_is_json_each_row_with_the_statement_in_the_url() {
    let rows = batch(100, Fault::SequenceGap);
    let expected: Vec<(Grain, usize)> = Grain::ALL.iter().map(|g| (*g, rows.rows(*g))).collect();

    let mut sink = sink(FakeTransport::new());
    let written = sink.write_batch(rows).expect("the batch lands");

    let sent = sink_sent(&sink);
    // One request per non-empty grain: the batch bounds are far above this
    // fixture, so nothing was split.
    let tables: Vec<String> = sent.iter().map(common::Sent::table).collect();
    assert_eq!(
        tables,
        vec![
            "recorder.datagram",
            "recorder.era",
            "recorder.segment_coverage",
            "recorder.sequence_gap"
        ],
        "the empty grain sends nothing at all"
    );

    for (grain, count) in expected {
        assert_eq!(written.rows(grain), count as u64, "{grain}");
        let Some(request) = sent
            .iter()
            .find(|s| s.table() == format!("recorder.{}", grain.table()))
        else {
            assert_eq!(count, 0, "{grain} had rows and no request");
            continue;
        };
        assert_eq!(request.rows().len(), count, "{grain}");
        assert!(
            request.body.ends_with('\n'),
            "{grain}: every line is terminated, including the last"
        );
        assert!(request.url.contains("FORMAT%20JSONEachRow"), "{grain}");
        assert!(request.url.contains("database=recorder"), "{grain}");
    }

    // The credentials travel in headers, and the user is the configured one.
    assert!(sent.iter().all(|s| s.user == "loader" && s.has_password));
    assert!(written.bytes() > 0);
}

/// A grain that produced no rows sends no request.
///
/// An `INSERT ... FORMAT JSONEachRow` with an empty body is a request the server
/// accepts and that inserts nothing, so this is not about correctness — it is
/// about what a request log says. A loader that posted five requests per object
/// whatever it derived would make "the conformance runner is not running" look
/// exactly like "the conformance runner found nothing".
#[test]
fn a_grain_with_no_rows_sends_no_request() {
    let rows = batch(20, Fault::None);
    assert_eq!(rows.rows(Grain::ConformanceFinding), 0);
    assert_eq!(rows.rows(Grain::SequenceGap), 0);

    let mut sink = sink(FakeTransport::new());
    sink.write_batch(rows).expect("the batch lands");
    let tables: BTreeSet<String> = sink_sent(&sink).iter().map(common::Sent::table).collect();
    assert!(!tables.contains("recorder.conformance_finding"));
    assert!(!tables.contains("recorder.sequence_gap"));
    assert!(tables.contains("recorder.datagram"));
}

/// Both bounds are enforced, and the byte bound is the one that binds: a row
/// count says nothing about a row's width.
#[test]
fn a_batch_is_split_by_row_count_and_by_bytes() {
    let rows = batch(100, Fault::None);
    let total = rows.rows(Grain::Datagram);
    assert_eq!(total, 100);

    let mut tuned = config();
    tuned.max_rows = 30;
    let mut sink = ClickHouseSink::with_transport(
        tuned,
        Credentials::new("loader", None),
        FakeTransport::new(),
    );
    sink.write_batch(rows.clone()).expect("the batch lands");
    let sent = sink_sent(&sink);
    let datagram: Vec<_> = sent
        .iter()
        .filter(|s| s.table() == "recorder.datagram")
        .collect();
    assert_eq!(datagram.len(), 4, "100 rows at 30 a request");
    assert_eq!(
        datagram.iter().map(|s| s.rows().len()).sum::<usize>(),
        total,
        "and every row was sent exactly once"
    );
    assert!(datagram.iter().all(|s| s.rows().len() <= 30));

    // The byte bound, on the same rows: a request is capped by whichever bound
    // is reached first.
    let mut by_bytes = config();
    by_bytes.max_rows = usize::MAX;
    by_bytes.max_bytes = 2_000;
    let mut sink = ClickHouseSink::with_transport(
        by_bytes,
        Credentials::new("loader", None),
        FakeTransport::new(),
    );
    sink.write_batch(rows).expect("the batch lands");
    let sent = sink_sent(&sink);
    let datagram: Vec<_> = sent
        .iter()
        .filter(|s| s.table() == "recorder.datagram")
        .collect();
    assert!(datagram.len() > 4, "the byte bound split it further");
    assert!(
        datagram.iter().all(|s| s.body.len() as u64 <= 2_000),
        "a request exceeded the byte bound"
    );
    assert_eq!(
        datagram.iter().map(|s| s.rows().len()).sum::<usize>(),
        total
    );
}

/// One row wider than the whole byte bound is sent on its own rather than
/// refused.
///
/// The bound exists to keep a request reasonable. Refusing a row for being wide
/// would silently drop the row most worth having — and a row is wide here
/// because it carries a long object key, which is a property of the deployment's
/// naming and not of the traffic.
#[test]
fn a_single_row_over_the_byte_bound_is_still_sent() {
    let rows = batch(3, Fault::None);
    let mut tuned = config();
    tuned.max_bytes = 1;
    let mut sink = ClickHouseSink::with_transport(
        tuned,
        Credentials::new("loader", None),
        FakeTransport::new(),
    );
    sink.write_batch(rows).expect("the batch lands");
    let datagram: Vec<_> = sink_sent(&sink)
        .into_iter()
        .filter(|s| s.table() == "recorder.datagram")
        .collect();
    assert_eq!(datagram.len(), 3, "one request per row, and none dropped");
    assert!(datagram.iter().all(|s| s.rows().len() == 1));
}

/// A destination that was unreachable is worth another attempt, and the batch is
/// the unit that is retried.
#[test]
fn an_unreachable_destination_is_retried_with_the_same_bytes() {
    let rows = batch(10, Fault::None);
    let transport =
        FakeTransport::answering(vec![Answer::Unreachable, Answer::Unreachable, Answer::Ok]);
    let mut sink = sink(transport);
    sink.write_batch(rows).expect("the third attempt landed it");

    let sent = sink_sent(&sink);
    let datagram: Vec<_> = sent
        .iter()
        .filter(|s| s.table() == "recorder.datagram")
        .collect();
    assert_eq!(
        datagram.len(),
        3,
        "two failures and the attempt that worked"
    );
    assert_eq!(
        datagram[0].body, datagram[2].body,
        "a retry re-sends the same bytes"
    );
    assert_eq!(sink.batches_failed(), 0, "nothing spent its attempts");
}

/// A statement the server rejected will be rejected again, so the attempts are
/// worth more spent on the next object.
#[test]
fn a_rejected_statement_is_not_retried_at_all() {
    let rows = batch(10, Fault::None);
    let object_key = rows.object_key.clone();
    let mut sink = sink(FakeTransport::answering(vec![Answer::Refused(400)]));

    let error = sink
        .write_batch(rows)
        .expect_err("a request the server refuses is a failed load");
    let RowSinkError::Rejected {
        object_key: named,
        last,
        ..
    } = &error
    else {
        panic!("expected a rejection, got {error}");
    };
    assert_eq!(named, &object_key, "the object is named");
    assert!(
        last.contains("Missing columns"),
        "the server's own message is kept verbatim: {last}"
    );
    assert_eq!(sink.batches_failed(), 1);
    assert_eq!(
        sink_sent(&sink).len(),
        1,
        "one attempt, because a 400 will be a 400 again"
    );
    assert!(sink.last_error().is_some_and(|e| e.contains("datagram")));
}

/// A 5xx and a 429 are the server's own admission that the failure is not the
/// request's, so those are retried and then given up on.
#[test]
fn a_server_failure_is_retried_until_the_attempts_are_spent() {
    for status in [500, 503, 429] {
        let rows = batch(10, Fault::None);
        let mut tuned = config();
        tuned.attempts = 3;
        let mut sink = ClickHouseSink::with_transport(
            tuned,
            Credentials::new("loader", None),
            FakeTransport::answering(vec![
                Answer::Refused(status),
                Answer::Refused(status),
                Answer::Refused(status),
            ]),
        )
        .waiting_with(no_wait);

        let error = sink.write_batch(rows).expect_err("every attempt failed");
        let RowSinkError::Rejected { attempts, .. } = &error else {
            panic!("expected a rejection, got {error}");
        };
        assert_eq!(*attempts, 3, "HTTP {status}");
        assert_eq!(sink_sent(&sink).len(), 3, "HTTP {status}");
        assert_eq!(sink.batches_failed(), 1, "HTTP {status}");
    }
}

/// A failure anywhere in the batch fails the whole batch, and the loader treats
/// the object as unloaded.
///
/// Rows already landed. That is the point: the tables are `ReplacingMergeTree`
/// and the rows are a pure function of `(object key, sha256)`, so loading the
/// object again replaces them. Reporting what got through would leave an object
/// whose datagram rows are present and whose gap rows are not — and that object
/// reads as a clean feed for ever.
#[test]
fn a_failure_on_a_later_grain_fails_the_whole_object() {
    let rows = batch(100, Fault::SequenceGap);
    assert!(rows.rows(Grain::SequenceGap) > 0);

    // The datagram, era and coverage requests succeed; the gap request does not.
    let mut tuned = config();
    tuned.attempts = 1;
    let mut sink = ClickHouseSink::with_transport(
        tuned,
        Credentials::new("loader", None),
        FakeTransport::answering(vec![
            Answer::Ok,
            Answer::Ok,
            Answer::Ok,
            Answer::Refused(500),
        ]),
    )
    .waiting_with(no_wait);

    let error = sink
        .write_batch(rows)
        .expect_err("the gap rows did not land");
    assert!(matches!(error, RowSinkError::Rejected { .. }), "{error}");
    let tables: Vec<String> = sink_sent(&sink).iter().map(common::Sent::table).collect();
    assert!(
        tables.contains(&"recorder.datagram".to_owned()),
        "the datagram rows did land, and the object is still unloaded"
    );
}

/// The base rows go first, so the alarming intermediate state cannot happen.
///
/// An object whose gap rows are present and whose datagram rows are not reads as
/// a finding with no evidence behind it, which is what an operator would chase.
#[test]
fn the_base_grain_is_sent_before_the_grains_derived_from_it() {
    assert_eq!(
        send_order(),
        [
            Grain::Datagram,
            Grain::Era,
            Grain::SegmentCoverage,
            Grain::SequenceGap,
            Grain::ConformanceFinding
        ]
    );

    let rows = batch(100, Fault::SequenceGap);
    let mut sink = sink(FakeTransport::new());
    sink.write_batch(rows).expect("the batch lands");
    let tables: Vec<String> = sink_sent(&sink).iter().map(common::Sent::table).collect();
    let datagram = tables
        .iter()
        .position(|t| t == "recorder.datagram")
        .expect("a datagram request");
    let gap = tables
        .iter()
        .position(|t| t == "recorder.sequence_gap")
        .expect("a gap request");
    assert!(datagram < gap);
}

/// Nothing is held across objects, so "this object is loaded" is a claim about
/// the store and not about memory.
#[test]
fn flush_holds_nothing_because_write_batch_held_nothing() {
    let rows = batch(10, Fault::None);
    let mut sink = sink(FakeTransport::new());
    sink.write_batch(rows).expect("the batch lands");
    let before = sink_sent(&sink).len();
    sink.flush().expect("flush");
    assert_eq!(sink_sent(&sink).len(), before);
}

/// A sink with no password still sends, because a store with no user
/// authentication is a legitimate deployment — and the header is simply absent
/// rather than sent empty.
#[test]
fn an_unauthenticated_sink_sends_no_password_header() {
    let rows = batch(5, Fault::None);
    let mut sink = ClickHouseSink::with_transport(
        config(),
        Credentials::new("default", None),
        FakeTransport::new(),
    );
    sink.write_batch(rows).expect("the batch lands");
    assert!(sink_sent(&sink).iter().all(|s| !s.has_password));
}

/// Reaching into the sink for what its transport recorded.
///
/// The sink owns the transport, which is what a caller wants — one value to
/// build and pass around — so a test that needs the recording reads it back
/// through a borrow rather than holding a second handle.
fn sink_sent(sink: &ClickHouseSink<FakeTransport>) -> Vec<common::Sent> {
    sink.transport().sent()
}
