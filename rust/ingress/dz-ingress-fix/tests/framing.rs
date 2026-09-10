//! Framing, in both directions, against bytes written out by hand.
//!
//! # Why a golden vector and not a round trip
//!
//! Encoding and then decoding proves the two halves agree, which is exactly
//! what a length computed over the wrong span still does: the encoder measures
//! the whole message, the decoder measures the whole message, the round trip
//! passes, and every engine on the other side refuses the message because the
//! declared length is how it finds the end. So the expected bytes below are
//! written out in full, with the declared length and the checksum computed by
//! hand from the rule rather than from this crate.
//!
//! The four decode cases are the ones a hand-written reader gets wrong: a
//! message split across two reads, two messages in one read, a declared length
//! that does not match, and a checksum that does not hold.

use dz_ingress_fix::framing::{
    self, msg_type, Body, BodyError, Decoder, FramingError, Message, BEGIN_STRING, SOH,
    TAG_HEART_BT_INT, TAG_MSG_SEQ_NUM, TAG_MSG_TYPE, TAG_RESET_SEQ_NUM_FLAG, TAG_SENDING_TIME,
};

/// A `\x01`-separated message written with `|`, which is how a person reads
/// one.
fn wire(rendered: &str) -> Vec<u8> {
    rendered
        .replace('|', &(SOH as char).to_string())
        .into_bytes()
}

/// A body written with `|`, parsed.
///
/// The bytes are leaked so that the returned [`Body`] can borrow them for the
/// life of the test. A body *is* a borrow of what the adapter wrote — that is
/// the point of the type — and naming the buffer at each of a dozen call sites
/// would cost more legibility than the leak costs a test binary.
fn body(rendered: &str) -> Body<'static> {
    Body::parse(Box::leak(wire(rendered).into_boxed_slice())).expect("a body")
}

/// The `SendingTime` every vector here uses.
const AT: &str = "20260909-11:56:50.123";

// ---------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------

#[test]
fn a_framed_logon_is_the_bytes_written_out_by_hand() {
    // The adapter's body: its identity, its signature, and the heartbeat
    // interval the session will run at. No `8`, no `9`, no `34`, no `52`, no
    // `141`, no `10` — those are the transport's, and `Body::parse` refuses a
    // body that states one.
    let body = body("35=A|49=A-PUBLISHER|56=A-VENUE|98=0|108=30|");
    let mut out = Vec::new();
    framing::frame(&mut out, &body, 1, AT, true);

    // The measured span, by hand: everything from `35=` through the separator
    // before `10=`.
    //
    //   35=A|                    5
    //   34=1|                    5
    //   52=20260909-11:56:50.123| 25
    //   141=Y|                    6
    //   49=A-PUBLISHER|          15
    //   56=A-VENUE|              11
    //   98=0|                     5
    //   108=30|                   7
    //                           ---
    //                            79
    let expected_span = 5 + 5 + 25 + 6 + 15 + 11 + 5 + 7;
    assert_eq!(expected_span, 79);

    let without_checksum = wire(
        "8=FIX.4.4|9=79|35=A|34=1|52=20260909-11:56:50.123|141=Y|\
         49=A-PUBLISHER|56=A-VENUE|98=0|108=30|",
    );
    // The checksum is the sum of every byte before the `10=` field, modulo 256,
    // three digits.
    let sum: u32 = without_checksum.iter().map(|byte| u32::from(*byte)).sum();
    let expected = {
        let mut bytes = without_checksum.clone();
        bytes.extend_from_slice(format!("10={:03}\u{1}", sum % 256).as_bytes());
        bytes
    };
    assert_eq!(
        framing::rendered(&out),
        framing::rendered(&expected),
        "the framed logon is not the bytes the rule produces"
    );
}

#[test]
fn the_declared_length_measures_the_mandated_span_and_not_the_message() {
    // The revert this test exists for: compute the declared length over the
    // whole message rather than from the field after `9=` to the separator
    // before `10=`. Asserted as the arithmetic relation rather than as one
    // number, so that it holds for every message and not just the vector
    // above.
    let body = body("35=0|");
    let mut out = Vec::new();
    framing::frame(&mut out, &body, 7, AT, false);
    let message = Message::new(&out);

    let declared: usize = std::str::from_utf8(message.field(framing::TAG_BODY_LENGTH).unwrap())
        .unwrap()
        .parse()
        .unwrap();
    let text = String::from_utf8(out.clone()).unwrap();
    let span_begins = text.find("35=").expect("a message type");
    let span_ends = text.rfind("10=").expect("a checksum");
    assert_eq!(
        declared,
        span_ends - span_begins,
        "the declared length is not the span from `35=` to the separator before `10=`"
    );
    assert!(
        declared < out.len(),
        "a declared length as long as the message is the mistake this test is for"
    );
}

#[test]
fn the_checksum_covers_every_byte_before_its_own_field() {
    let body = body("35=0|");
    let mut out = Vec::new();
    framing::frame(&mut out, &body, 2, AT, false);

    let stated = std::str::from_utf8(&out[out.len() - 4..out.len() - 1]).unwrap();
    let computed = framing::checksum(&out[..out.len() - framing::CHECKSUM_FIELD_LEN]);
    assert_eq!(stated, format!("{computed:03}"));
    assert_eq!(stated.len(), 3, "the checksum is always three digits");
}

#[test]
fn the_reset_flag_is_stated_only_when_the_session_is_resetting() {
    let logon = body("35=A|108=30|");
    let mut with = Vec::new();
    framing::frame(&mut with, &logon, 1, AT, true);
    assert_eq!(
        Message::new(&with).field(TAG_RESET_SEQ_NUM_FLAG),
        Some(&b"Y"[..])
    );

    let heartbeat = body("35=0|");
    let mut without = Vec::new();
    framing::frame(&mut without, &heartbeat, 2, AT, false);
    assert_eq!(
        Message::new(&without).field(TAG_RESET_SEQ_NUM_FLAG),
        None,
        "a heartbeat is not a logon and states no reset"
    );
}

#[test]
fn the_header_this_transport_owns_comes_before_the_body_it_was_handed() {
    let body = body("35=V|262=req-1|");
    let mut out = Vec::new();
    framing::frame(&mut out, &body, 4, AT, false);
    let tags: Vec<u32> = Message::new(&out).fields().map(|field| field.tag).collect();
    assert_eq!(
        tags,
        vec![
            framing::TAG_BEGIN_STRING,
            framing::TAG_BODY_LENGTH,
            TAG_MSG_TYPE,
            TAG_MSG_SEQ_NUM,
            TAG_SENDING_TIME,
            262,
            framing::TAG_CHECKSUM,
        ],
        "the first three fields and the last are the protocol's own order"
    );
}

#[test]
fn framing_a_second_message_into_one_buffer_does_not_append_to_the_first() {
    // The buffer is reused per connection, and a `frame` that appended would
    // put two messages in one write with one checksum between them.
    let mut out = Vec::new();
    let first = body("35=0|");
    framing::frame(&mut out, &first, 1, AT, false);
    let length = out.len();
    let second = body("35=0|");
    framing::frame(&mut out, &second, 2, AT, false);
    assert_eq!(out.len(), length, "the buffer grew: `frame` appended");
    assert_eq!(Message::new(&out).field_u64(TAG_MSG_SEQ_NUM), Some(2));
}

// ---------------------------------------------------------------------------
// The body's refusals
// ---------------------------------------------------------------------------

#[test]
fn a_body_stating_a_tag_this_transport_owns_is_refused_and_says_which() {
    for (tag, body) in [
        (8u32, "35=A|8=FIX.4.4|"),
        (9, "35=A|9=42|"),
        (34, "35=A|34=1|"),
        (52, "35=A|52=20260909-11:56:50.123|"),
        (141, "35=A|141=N|"),
        (10, "35=A|10=000|"),
    ] {
        let error = Body::parse(&wire(body)).expect_err("an owned tag is refused");
        match error {
            BodyError::OwnedTag { tag: refused, why } => {
                assert_eq!(refused, tag, "{body}");
                assert!(!why.is_empty(), "{body}");
            }
            other => panic!("`{body}` was refused as {other}"),
        }
    }
}

#[test]
fn a_body_whose_first_field_is_not_the_message_type_is_refused() {
    // The protocol mandates `35` as the third field and this crate writes the
    // first two, so a body that leads with something else was composed against
    // a different framing.
    let error = Body::parse(&wire("49=A-PUBLISHER|35=A|")).expect_err("refused");
    assert!(
        matches!(error, BodyError::MsgTypeNotFirst { found: 49 }),
        "{error}"
    );
}

#[test]
fn an_empty_body_is_its_own_refusal() {
    assert!(matches!(Body::parse(b""), Err(BodyError::Empty)));
}

#[test]
fn an_unterminated_body_is_refused_rather_than_framed() {
    // A body whose last field has no separator would be framed with the
    // checksum field running straight into it.
    let error = Body::parse(b"35=A\x0149=A-PUBLISHER").expect_err("refused");
    assert!(matches!(error, BodyError::Malformed { .. }), "{error}");
}

#[test]
fn a_body_reads_back_the_fields_it_was_given() {
    let body = body("35=A|108=30|98=0|");
    assert_eq!(body.msg_type(), msg_type::LOGON);
    assert_eq!(body.field_u64(TAG_HEART_BT_INT), Some(30));
    assert_eq!(body.field(999), None);
}

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

/// A complete message, framed the way this crate frames one.
fn framed(rendered: &str, seq: u64) -> Vec<u8> {
    let mut out = Vec::new();
    framing::frame(&mut out, &body(rendered), seq, AT, false);
    out
}

#[test]
fn a_message_split_across_two_reads_is_one_message() {
    // The case a reader that treats each read as a message gets wrong, and the
    // reason the decoder is a buffer rather than a function over a slice.
    let message = framed("35=W|55=A-SYMBOL|", 3);
    let mut decoder = Decoder::new();
    let mut out = Vec::new();

    let (head, tail) = message.split_at(message.len() / 2);
    decoder.feed(head);
    assert!(
        !decoder
            .take(&mut out)
            .expect("a partial read is not an error"),
        "half a message is not a message"
    );
    assert_eq!(decoder.buffered(), head.len(), "the half was not kept");

    decoder.feed(tail);
    assert!(decoder.take(&mut out).expect("the rest arrived"));
    assert_eq!(out, message);
    assert_eq!(decoder.buffered(), 0);
}

#[test]
fn a_message_arriving_one_byte_at_a_time_is_one_message() {
    // The extreme of the split-read case, which also covers the two places the
    // decoder has to decide "not yet" from a prefix: inside `8=` and inside
    // `9=`.
    let message = framed("35=0|", 9);
    let mut decoder = Decoder::new();
    let mut out = Vec::new();
    for (index, byte) in message.iter().enumerate() {
        decoder.feed(&[*byte]);
        let complete = decoder.take(&mut out).expect("no byte of this is an error");
        assert_eq!(
            complete,
            index + 1 == message.len(),
            "the message completed at byte {index}"
        );
    }
    assert_eq!(out, message);
}

#[test]
fn two_messages_in_one_read_are_two_messages() {
    // The other case a hand-written reader gets wrong: it reads the first and
    // discards the buffer, and the second message is a payload nobody sees.
    let first = framed("35=W|55=FIRST|", 1);
    let second = framed("35=X|55=SECOND|", 2);
    let mut both = first.clone();
    both.extend_from_slice(&second);

    let mut decoder = Decoder::new();
    decoder.feed(&both);
    let mut out = Vec::new();

    assert!(decoder.take(&mut out).expect("the first"));
    assert_eq!(out, first);
    assert!(decoder.take(&mut out).expect("the second"));
    assert_eq!(out, second);
    assert!(
        !decoder.take(&mut out).expect("and no third"),
        "the decoder produced a message from an empty buffer"
    );
}

#[test]
fn a_declared_length_that_does_not_match_is_refused() {
    // Refused and not searched past: the length is how the end of a message is
    // located, so one that lands anywhere else means this decoder and the far
    // side no longer agree where the next message begins.
    let message = framed("35=W|55=A-SYMBOL|", 4);
    let text = String::from_utf8(message).unwrap();
    let broken = text.replacen("9=42", "9=41", 1);
    let broken = if broken == text {
        // The vector's own length is whatever it is; find it and shorten it by
        // one rather than assuming.
        let declared: usize = text
            .split('\u{1}')
            .find_map(|field| field.strip_prefix("9="))
            .unwrap()
            .parse()
            .unwrap();
        text.replacen(&format!("9={declared}"), &format!("9={}", declared - 1), 1)
    } else {
        broken
    };

    let mut decoder = Decoder::new();
    decoder.feed(broken.as_bytes());
    let mut out = Vec::new();
    let error = decoder.take(&mut out).expect_err("a length that lies");
    assert!(
        matches!(error, FramingError::LengthMismatch { .. }),
        "{error}"
    );
}

#[test]
fn a_checksum_that_does_not_hold_is_refused() {
    let message = framed("35=W|55=A-SYMBOL|", 5);
    let mut broken = message.clone();
    // The three digits, which are the fourth, third and second bytes from the
    // end.
    let last_digit = broken.len() - 2;
    broken[last_digit] = if broken[last_digit] == b'9' {
        b'8'
    } else {
        b'9'
    };

    let mut decoder = Decoder::new();
    decoder.feed(&broken);
    let mut out = Vec::new();
    let error = decoder
        .take(&mut out)
        .expect_err("a checksum that does not hold");
    match error {
        FramingError::ChecksumMismatch { stated, computed } => {
            assert_ne!(stated, computed);
        }
        other => panic!("{other}"),
    }
}

#[test]
fn a_stream_that_does_not_begin_with_a_begin_string_is_not_this_protocol() {
    let mut decoder = Decoder::new();
    decoder.feed(b"HTTP/1.1 400 Bad Request\r\n");
    let mut out = Vec::new();
    let error = decoder.take(&mut out).expect_err("not a session");
    assert!(matches!(error, FramingError::NotAMessage), "{error}");
}

#[test]
fn a_declared_length_past_the_ceiling_is_refused_before_the_bytes_are_kept() {
    // The size is chosen by whoever is on the other end and the buffer is
    // ours, so the ceiling is checked against the declared value rather than
    // against what has arrived.
    let mut decoder = Decoder::with_max_body_bytes(64);
    decoder.feed(format!("8={BEGIN_STRING}\u{1}9=65536\u{1}").as_bytes());
    let mut out = Vec::new();
    let error = decoder.take(&mut out).expect_err("past the ceiling");
    assert!(
        matches!(
            error,
            FramingError::TooLarge {
                declared: 65_536,
                limit: 64
            }
        ),
        "{error}"
    );
}

#[test]
fn bytes_with_no_separator_are_refused_rather_than_buffered_without_bound() {
    // The half of the ceiling a declared length cannot carry, and the case that
    // grows the buffer without any bound. A peer that writes `8=FIX.4.4` and
    // then megabytes with no separator, or ends that field and then writes `9=`
    // and megabytes with no separator, has declared nothing — so there is no
    // length to compare against a limit, and without this the session's read
    // loop keeps feeding, `take` keeps saying "not yet", and the process is
    // killed for memory rather than told what happened. A venue bug, a
    // truncated frame and a garbled stream all arrive this way.
    //
    // Both separator searches, because a guard on one of them leaves the other
    // exactly as it was.
    let limit = 64 + framing::MAX_HEADER_BYTES;
    for prefix in [format!("8={BEGIN_STRING}"), format!("8={BEGIN_STRING}|9=")] {
        let mut decoder = Decoder::with_max_body_bytes(64);
        decoder.feed(&wire(&prefix));
        let mut out = Vec::new();
        // Fed the way a socket delivers it, so what is asserted is a decoder
        // that refuses on the read which crosses the ceiling rather than one
        // handed the whole thing at once.
        let mut refusal = None;
        for _ in 0..64 {
            decoder.feed(&[b'7'; 16]);
            match decoder.take(&mut out) {
                Ok(false) => assert!(
                    decoder.buffered() <= limit,
                    "`{prefix}`: {} bytes held past the {limit}-byte ceiling",
                    decoder.buffered()
                ),
                Ok(true) => panic!("`{prefix}`: there is no message in bytes with no separator"),
                Err(error) => {
                    refusal = Some(error);
                    break;
                }
            }
        }
        match refusal.expect("a refusal, and not a buffer that keeps growing") {
            FramingError::HeaderNotTerminated {
                buffered,
                limit: stated,
            } => {
                assert_eq!(stated, limit, "`{prefix}`");
                assert!(buffered > limit, "`{prefix}`: {buffered}");
            }
            other => panic!("`{prefix}`: {other}"),
        }
    }
}

#[test]
fn a_decoded_message_reads_back_its_own_header() {
    let message = framed("35=W|55=A-SYMBOL|", 11);
    let mut decoder = Decoder::new();
    decoder.feed(&message);
    let mut out = Vec::new();
    assert!(decoder.take(&mut out).expect("a message"));

    let message = Message::new(&out);
    assert_eq!(message.msg_type(), Some("W"));
    assert_eq!(message.field_u64(TAG_MSG_SEQ_NUM), Some(11));
    assert_eq!(message.field(TAG_SENDING_TIME), Some(AT.as_bytes()));
    assert!(
        !message.is_session(),
        "`W` is a market-data message and not the session layer's"
    );
}

#[test]
fn the_session_layer_owns_exactly_the_seven_message_types() {
    for session in msg_type::SESSION {
        let message = framed(&format!("35={session}|"), 1);
        assert!(
            Message::new(&message).is_session(),
            "`{session}` is a session message"
        );
    }
    // The market-data types this transport carries, and one order-entry type,
    // which is nothing this crate composes and still not the session layer's.
    for application in ["W", "X", "Y", "V", "D"] {
        let message = framed(&format!("35={application}|"), 1);
        assert!(
            !Message::new(&message).is_session(),
            "`{application}` is not a session message"
        );
    }
}

#[test]
fn clearing_the_decoder_drops_a_partial_message() {
    // Called on every connect: a partial message from a connection that has
    // ended is bytes belonging to a numbering that no longer exists.
    let message = framed("35=W|55=A-SYMBOL|", 6);
    let mut decoder = Decoder::new();
    decoder.feed(&message[..8]);
    assert_eq!(decoder.buffered(), 8);
    decoder.clear();
    assert_eq!(decoder.buffered(), 0);
    let mut out = Vec::new();
    assert!(!decoder.take(&mut out).expect("nothing buffered"));
}
