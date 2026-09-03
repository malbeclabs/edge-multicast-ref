# Adapter crates

The boundary a venue repository implements, and nothing else.

| Crate | |
|---|---|
| [`dz-adapter-core`](dz-adapter-core/) | The `Adapter` trait, the events it produces, the sinks it writes into |
| [`dz-adapter-uds`](dz-adapter-uds/) | A record encoding for a source that is another process, and the built-in adapter that reads it |

## What a venue implements

A venue owns two things: its upstream protocol, and its own book state machine. It owns nothing else.

Everything a feed specification already decided — the `Instrument ID`, the `Source ID`, the `Channel ID`, the sequence numbers, the fixed-point scaling, the `Update Flags` byte, the `Action`, the datagram and its 1,232-byte cap — belongs to the crates above this one, and **none of it is expressible through any type here**. Not as a convention a venue is asked to observe: there is no parameter to pass one through.

That is the whole design, and it comes from what existing publishers did. Every defect a fleet-wide audit found was a publisher re-deciding something a specification had already decided.

```rust
use dz_adapter_core::{Adapter, EventSink, ListingSink, ParseError, Payload};

struct Quiet;

impl Adapter for Quiet {
    fn message_types(&self) -> &[&'static str] {
        &["heartbeat"]
    }

    fn poll_listings(&mut self, _out: &mut dyn ListingSink) {}

    fn on_payload(
        &mut self,
        payload: &Payload<'_>,
        out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        if payload.bytes.is_empty() {
            return Err(ParseError::truncated("empty payload"));
        }
        out.upstream_message("heartbeat");
        Ok(())
    }
}
```

Nothing else is imported, because there is nothing else to import. A real adapter parses its upstream in `on_payload`, offers its instruments in `poll_listings`, writes its subscriptions in `on_connected`, and — if its feed has a snapshot port — writes its book in `snapshot`.

`snapshot` returns a `DepthBound`, and that is the one place a venue states something about the wire: whether the levels it just wrote are the **complete** book or the top N of it. It is returned rather than passed in because a return value cannot be forgotten, and because the value a runtime would have to default it to is the wire's `0` — which is a positive claim of completeness. A shipped depth publisher reaches that same `0` legitimately, but only through an argument about its own upstream and a check against a full-depth REST book; the number is cheap and the evidence behind it is not, and the evidence lives here.

## Three properties, and what each one buys

**`dz-adapter-core` depends on `thiserror` and nothing else.** A venue inheriting our async runtime's minor version, or our Prometheus client's, is a version conflict we caused. `tests/dependencies.rs` fails the moment a second entry appears. The transport half of the boundary is async and carries a dependency tree of its own, so it lives in [`ingress/`](../ingress/) and a venue that does not need it does not link it.

**`on_payload` is synchronous, does no I/O and allocates nothing.** Not an ergonomic preference: it makes an adapter a pure function of its input bytes and its own state, which is what lets the same adapter be re-run offline over an archive of what the upstream actually sent and its output diffed against what was captured on the wire. An `async fn` here would pin every venue to one runtime version and make that comparison impossible.

**Everything borrows.** An adapter reads out of the receive buffer and writes into the encode buffer, and owns nothing in between.

## Composing a publisher

The generic publisher is a library, not a service. The venue repository owns `main`, and it is short: register the adapter under the name its configuration will select it by, and hand the registry to the runtime. See [`publisher/`](../publisher/) and, for the whole argument, the [venue adapter interface design](../../docs/superpowers/specs/2026-09-02-venue-adapter-interface-design.md).

A venue whose integration is not Rust uses [`dz-adapter-uds`](dz-adapter-uds/) instead: another process writes normalized-event records, and the built-in adapter reads them. It costs a serialization format and a copy per event, which is why a Rust venue should not.
