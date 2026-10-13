//! A strike, from an adapter's statement to the reference-data port.
//!
//! The registry's own tests hold what is owed and when. These hold what
//! reaches the wire: that a `StrikeInterval` is in the datagram of its
//! `InstrumentDefinition` and directly behind it, on every feed the shard
//! carries, and that a strike the venue fixes is sent without waiting for the
//! cycle, behind a `ManifestSummary` whose sequence did not move.

#![forbid(unsafe_code)]

mod harness;

use std::time::Duration;

use dz_adapter_core::{
    Adapter, EventSink, InstrumentRef, ListingSink, ParseError, Payload, Scalar, Strike,
    StrikeBound,
};
use dz_edge_refdata::{InstrumentDefinition, ManifestSummary, StrikeInterval};
use harness::{harness_both, spec, Harness, Recorder};

const DEFINITION: u8 = 0x02;
const MANIFEST: u8 = 0x07;
const STRIKE: u8 = 0x09;
const SCHEMA: u8 = 3;
const HEADER: usize = 24;

/// Eight instruments whose venue fixes every strike in one poll.
struct Fleet {
    fixed: bool,
}

impl Adapter for Fleet {
    fn message_types(&self) -> &[&'static str] {
        &[]
    }

    fn poll_listings(&mut self, out: &mut dyn ListingSink) {
        for index in 0..8 {
            let Some(handle) = out.list(&spec(&format!("W{index}"))) else {
                continue;
            };
            let strike = if self.fixed {
                Strike::Fixed {
                    lower: Some(StrikeBound::inclusive(Scalar::text("82506.23"))),
                    upper: None,
                    fixing_time_ns: None,
                }
            } else {
                Strike::Pending
            };
            out.strike(handle, &strike);
        }
    }

    fn on_payload(
        &mut self,
        _payload: &Payload<'_>,
        _out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        Ok(())
    }

    fn snapshot(
        &self,
        _instrument: InstrumentRef,
        _out: &mut dyn dz_adapter_core::SnapshotSink,
    ) -> Result<dz_adapter_core::DepthBound, dz_adapter_core::AdapterError> {
        Err(dz_adapter_core::AdapterError::NotReady {
            detail: "no book in this test",
        })
    }
}

/// Two instruments: one whose contract pays on a strike the venue fixes after
/// listing, and one that pays on none.
struct Windows {
    /// `None` until the venue fixes it.
    fixed_at: Option<&'static str>,
    window: Option<InstrumentRef>,
}

impl Adapter for Windows {
    fn message_types(&self) -> &[&'static str] {
        &[]
    }

    fn poll_listings(&mut self, out: &mut dyn ListingSink) {
        self.window = out.list(&spec("WINDOW"));
        if let Some(handle) = self.window {
            let strike = match self.fixed_at {
                None => Strike::Pending,
                Some(value) => Strike::Fixed {
                    lower: Some(StrikeBound::inclusive(Scalar::text(value))),
                    upper: None,
                    fixing_time_ns: None,
                },
            };
            out.strike(handle, &strike);
        }
        out.list(&spec("PLAIN"));
    }

    fn on_payload(
        &mut self,
        _payload: &Payload<'_>,
        _out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        Ok(())
    }

    fn snapshot(
        &self,
        _instrument: InstrumentRef,
        _out: &mut dyn dz_adapter_core::SnapshotSink,
    ) -> Result<dz_adapter_core::DepthBound, dz_adapter_core::AdapterError> {
        Err(dz_adapter_core::AdapterError::NotReady {
            detail: "no book in this test",
        })
    }
}

/// Each datagram's messages, in order, walked by each message's own Length.
fn datagrams(recorder: &Recorder) -> Vec<Vec<(u8, Vec<u8>)>> {
    recorder
        .datagrams()
        .iter()
        .map(|datagram| {
            let mut messages = Vec::new();
            let mut at = HEADER;
            while at < datagram.len() {
                let length = datagram[at + 1] as usize;
                messages.push((datagram[at], datagram[at..at + length].to_vec()));
                at += length;
            }
            messages
        })
        .collect()
}

fn symbol(definition: &InstrumentDefinition) -> String {
    String::from_utf8_lossy(&definition.symbol)
        .trim_end_matches('\0')
        .to_owned()
}

/// Every definition on a port with the strike that follows it, and the proof
/// that no strike stands anywhere else.
fn defined(recorder: &Recorder) -> Vec<(String, Option<StrikeInterval>)> {
    let mut out = Vec::new();
    for messages in datagrams(recorder) {
        for (index, (type_id, bytes)) in messages.iter().enumerate() {
            match *type_id {
                DEFINITION => {
                    let definition = InstrumentDefinition::decode(bytes, SCHEMA).unwrap();
                    let strike = messages
                        .get(index + 1)
                        .filter(|(next, _)| *next == STRIKE)
                        .map(|(_, bytes)| StrikeInterval::decode(bytes).unwrap());
                    if let Some(strike) = strike {
                        assert_eq!(strike.instrument_id, definition.instrument_id);
                        assert_eq!(strike.source_id, definition.source_id);
                        assert_eq!(strike.defect(), None);
                    }
                    out.push((symbol(&definition), strike));
                }
                STRIKE => assert!(
                    index > 0 && messages[index - 1].0 == DEFINITION,
                    "a StrikeInterval does not directly follow a definition in its datagram"
                ),
                _ => {}
            }
        }
    }
    out
}

/// One lap of the definition cycle, a second at a time.
fn lap(h: &mut Harness) {
    for _ in 0..30 {
        h.clock.advance(Duration::from_secs(1));
        let _ = h.publisher.tick();
    }
}

fn refdata_ports(h: &Harness) -> [(&'static str, Recorder); 2] {
    [
        ("top-of-book", h.tob.as_ref().unwrap().refdata.clone()),
        ("market-by-price", h.mbp.as_ref().unwrap().refdata.clone()),
    ]
}

#[test]
fn a_strike_is_in_its_definitions_datagram_on_every_feed_and_nowhere_else() {
    let mut h = harness_both();
    let mut adapter = Windows {
        fixed_at: None,
        window: None,
    };
    assert!(h.publisher.poll_listings(&mut adapter));
    lap(&mut h);

    for (feed, recorder) in refdata_ports(&h) {
        let defined = defined(&recorder);
        let windows: Vec<_> = defined.iter().filter(|(s, _)| s == "WINDOW").collect();
        let plain: Vec<_> = defined.iter().filter(|(s, _)| s == "PLAIN").collect();
        assert!(!windows.is_empty() && !plain.is_empty(), "{feed}");
        for (_, strike) in windows {
            let strike = strike.unwrap_or_else(|| panic!("{feed}: WINDOW defined alone"));
            assert!(strike.is_pending(), "{feed}");
        }
        for (_, strike) in plain {
            assert_eq!(*strike, None, "{feed}: PLAIN pays on no strike");
        }
    }
}

#[test]
fn a_strike_the_venue_fixes_is_sent_at_once_behind_a_manifest_that_did_not_move() {
    let mut h = harness_both();
    let mut adapter = Windows {
        fixed_at: None,
        window: None,
    };
    assert!(h.publisher.poll_listings(&mut adapter));
    lap(&mut h);
    let seq = h.publisher.refdata().manifest_seq("default");
    let sent: Vec<usize> = refdata_ports(&h).iter().map(|(_, r)| r.len()).collect();

    adapter.fixed_at = Some("82506.23");
    h.clock.advance(dz_publisher_runtime::LISTING_POLL);
    assert!(h.publisher.poll_listings(&mut adapter));
    assert_eq!(h.publisher.refdata().manifest_seq("default"), seq);
    // One tick, with no time for the cycle to come round to the instrument.
    let _ = h.publisher.tick();

    for ((feed, recorder), before) in refdata_ports(&h).into_iter().zip(sent) {
        let all = datagrams(&recorder);
        let new = &all[before..];
        assert!(!new.is_empty(), "{feed}: nothing was sent");
        let first = &new[0];
        assert_eq!(first[0].0, MANIFEST, "{feed}: the summary leads");
        let manifest = ManifestSummary::decode(&first[0].1).unwrap();
        assert_eq!(Some(manifest.manifest_seq), seq, "{feed}");
        assert_eq!(first[1].0, DEFINITION, "{feed}");
        assert_eq!(first[2].0, STRIKE, "{feed}");
        let definition = InstrumentDefinition::decode(&first[1].1, SCHEMA).unwrap();
        assert_eq!(symbol(&definition), "WINDOW", "{feed}");
        assert_eq!(Some(definition.manifest_seq), seq, "{feed}");
        let strike = StrikeInterval::decode(&first[2].1).unwrap();
        assert_eq!(
            (
                strike.strike_exponent,
                strike.bound_flags,
                strike.lower_bound
            ),
            (-2, 0b0011, 8_250_623),
            "{feed}"
        );
    }

    // And from then on the cycle carries the fixed strike.
    lap(&mut h);
    for (feed, recorder) in refdata_ports(&h) {
        let (_, last) = defined(&recorder)
            .into_iter()
            .rev()
            .find(|(s, _)| s == "WINDOW")
            .unwrap();
        assert!(!last.unwrap().is_pending(), "{feed}");
    }
}

/// A full batch of changed strikes is one datagram, summary and all.
///
/// Six pairs behind the 24-byte summary are 1,068 bytes. Seven would be 1,238
/// with the header, over the 1,232 a datagram may be, so the seventh used to
/// leave in a second datagram that the summary was not in.
#[test]
fn a_batch_of_fixed_strikes_shares_one_datagram_with_its_manifest() {
    let mut h = harness_both();
    let mut adapter = Fleet { fixed: false };
    assert!(h.publisher.poll_listings(&mut adapter));
    lap(&mut h);
    let sent: Vec<usize> = refdata_ports(&h).iter().map(|(_, r)| r.len()).collect();

    adapter.fixed = true;
    h.clock.advance(dz_publisher_runtime::LISTING_POLL);
    assert!(h.publisher.poll_listings(&mut adapter));
    // Two ticks with no time between them, so the cycle owes nothing and what
    // leaves is the changed strikes alone: six, and then the two left over.
    let _ = h.publisher.tick();
    let _ = h.publisher.tick();

    for ((feed, recorder), before) in refdata_ports(&h).into_iter().zip(sent) {
        let raw = recorder.datagrams();
        let all = datagrams(&recorder);
        let (new, new_raw) = (&all[before..], &raw[before..]);
        assert_eq!(new.len(), 2, "{feed}: one datagram a tick");
        for (messages, pairs) in new.iter().zip([6usize, 2]) {
            let types: Vec<u8> = messages.iter().map(|(type_id, _)| *type_id).collect();
            let mut want = vec![MANIFEST];
            want.extend([DEFINITION, STRIKE].repeat(pairs));
            assert_eq!(types, want, "{feed}");
        }
        assert_eq!(new_raw[0].len(), 24 + 24 + 6 * 170, "{feed}");
        assert_eq!(new_raw[0].len(), 1068, "{feed}");
        for (_, bytes) in new.iter().flatten().filter(|(t, _)| *t == STRIKE) {
            assert!(
                !StrikeInterval::decode(bytes).unwrap().is_pending(),
                "{feed}"
            );
        }
    }
}
