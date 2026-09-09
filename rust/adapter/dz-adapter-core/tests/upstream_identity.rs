//! Which upstream message a top came from, and what it costs a sink that does
//! not want to know.
//!
//! Two properties, and the first is a compile rather than an assertion. A sink
//! written before this method existed still satisfies the trait, because the
//! method is defaulted — that is the whole of "no adapter changes", and the
//! only way to state it is to write such a sink and let it build. Remove the
//! default and every sink in the workspace stops compiling, which is a stronger
//! signal than any test here could be.
//!
//! The second is that an adapter can actually get the value out: a sink that
//! wants the identity sees exactly the pair the adapter passed, including the
//! half a given upstream does not number.
#![forbid(unsafe_code)]

use dz_adapter_core::{Adapter, ConnectionId, Event, EventSink, ListingSink, ParseError, Payload};

/// A sink written against the trait as it was: the two required methods and
/// nothing else.
///
/// Nothing here mentions `upstream_identity`, and that is the assertion. This
/// file failing to compile is the only report this property has.
#[derive(Default)]
struct Deaf {
    heard: Vec<&'static str>,
}

impl EventSink for Deaf {
    fn upstream_message(&mut self, message_type: &'static str) {
        self.heard.push(message_type);
    }

    fn event(&mut self, _event: Event<'_>) {}
}

/// A sink that keeps the identity, as a recorder's would.
#[derive(Default)]
struct Recording {
    identities: Vec<(Option<u64>, Option<u64>)>,
    heard: Vec<&'static str>,
}

impl EventSink for Recording {
    fn upstream_message(&mut self, message_type: &'static str) {
        self.heard.push(message_type);
    }

    fn upstream_identity(&mut self, sid: Option<u64>, seq: Option<u64>) {
        self.identities.push((sid, seq));
    }

    fn event(&mut self, _event: Event<'_>) {}
}

/// An adapter over one ASCII line: `<sid|-> <seq|-> <name>`.
///
/// It states the identity before naming the message, which is the order a
/// driver reads them in, and it states whichever halves its upstream gave it.
struct Numbered;

impl Adapter for Numbered {
    fn message_types(&self) -> &[&'static str] {
        &["quote"]
    }

    fn poll_listings(&mut self, _out: &mut dyn ListingSink) {}

    fn on_payload(
        &mut self,
        payload: &Payload<'_>,
        out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        let line = core::str::from_utf8(payload.bytes)
            .map_err(|_| ParseError::malformed("payload is not utf-8"))?;
        let mut fields = line.split(' ');
        let number = |field: Option<&str>| match field {
            None | Some("-") => None,
            Some(digits) => digits.parse::<u64>().ok(),
        };
        let sid = number(fields.next());
        let seq = number(fields.next());
        let kind = fields.next().ok_or(ParseError::truncated("no name"))?;
        if kind != "quote" {
            return Err(ParseError::malformed("unknown message"));
        }
        out.upstream_identity(sid, seq);
        out.upstream_message("quote");
        Ok(())
    }
}

fn payload(line: &str) -> Payload<'_> {
    Payload {
        bytes: line.as_bytes(),
        recv_ts_ns: 1_700_000_000_000_000_000,
        connection: ConnectionId::new("upstream"),
    }
}

/// A sink implementing nothing new compiles, and the default discards.
///
/// This is what a publisher's sink is: it never asked for the identity, it
/// never implemented the method, and calling it leaves it exactly as it was —
/// nothing counted, nothing to reach the wire.
#[test]
fn a_sink_that_implements_nothing_new_inherits_the_default() {
    let mut sink = Deaf::default();
    let driven: &mut dyn EventSink = &mut sink;

    driven.upstream_identity(Some(11), Some(4_002));
    driven.upstream_identity(None, None);
    driven.upstream_message("quote");

    assert_eq!(
        sink.heard,
        vec!["quote"],
        "the default discards the identity and touches nothing else"
    );
}

/// A recording sink sees what the adapter passed, both halves and one.
///
/// An upstream that numbers its messages and never names the session, and one
/// that does the reverse, are both ordinary — which is why the pair is two
/// `Option`s rather than two numbers with a reserved value between them.
#[test]
fn a_recording_sink_sees_what_an_adapter_passed() {
    let mut adapter = Numbered;
    let mut sink = Recording::default();

    for line in ["11 4002 quote", "- 4003 quote", "12 - quote", "- - quote"] {
        adapter
            .on_payload(&payload(line), &mut sink)
            .expect("the line is well formed");
    }

    assert_eq!(
        sink.identities,
        vec![
            (Some(11), Some(4_002)),
            (None, Some(4_003)),
            (Some(12), None),
            (None, None),
        ],
        "every half arrives as the upstream stated it, and an unstated half as None"
    );
    assert_eq!(
        sink.heard.len(),
        4,
        "the identity is stated beside the kind, not instead of it"
    );
}
