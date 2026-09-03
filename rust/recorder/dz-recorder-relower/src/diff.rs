//! Comparing two copies of one message, field by field.
//!
//! Every field of every compared message type is listed by hand below, under the
//! name the codec gives it — which is the name transcribed from the
//! specification's own field table. That is deliberate labour: a structural
//! comparison of two identical types would say *these differ* and name nothing,
//! and *a price differs* sends nobody anywhere. `bid_price` differs, `1234` on
//! the wire against `12340`, is an exponent off by one, and an operator can act
//! on it before finishing the sentence.
//!
//! Nothing about how a message was carried is compared here. See
//! [`Outcome::IdenticalDifferentTiming`](crate::Outcome::IdenticalDifferentTiming).

use core::fmt::Display;

use crate::wire::MessageBody;

/// One field that differs between the wire copy and the re-lowered copy.
///
/// The values are rendered rather than typed, because a report row is read and
/// the fields being compared are `u8`, `u16`, `u32`, `u64` and `i64`. A typed
/// union of those would be five variants nobody matches on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDiff {
    /// The wire field's name, as the codec names it.
    pub field: &'static str,
    /// What the archive holds.
    pub on_wire: String,
    /// What re-running the venue's mapping produced.
    pub re_lowered: String,
}

impl Display for FieldDiff {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "{}: wire {} against re-lowered {}",
            self.field, self.on_wire, self.re_lowered
        )
    }
}

/// Every compared field that differs, in wire-layout order.
///
/// An empty result is the fourth outcome: both copies carry the message and
/// every field of it agrees.
#[must_use]
pub fn diff(on_wire: &MessageBody, re_lowered: &MessageBody) -> Vec<FieldDiff> {
    let mut out = Vec::new();
    match (on_wire, re_lowered) {
        (MessageBody::Quote(wire), MessageBody::Quote(re)) => {
            cmp(
                &mut out,
                "instrument_id",
                wire.instrument_id,
                re.instrument_id,
            );
            cmp(&mut out, "source_id", wire.source_id, re.source_id);
            cmp_byte(&mut out, "update_flags", wire.update_flags, re.update_flags);
            cmp(
                &mut out,
                "source_timestamp_ns",
                wire.source_timestamp_ns,
                re.source_timestamp_ns,
            );
            cmp(&mut out, "bid_price", wire.bid_price, re.bid_price);
            cmp(&mut out, "bid_qty", wire.bid_qty, re.bid_qty);
            cmp(&mut out, "ask_price", wire.ask_price, re.ask_price);
            cmp(&mut out, "ask_qty", wire.ask_qty, re.ask_qty);
            cmp(
                &mut out,
                "bid_source_count",
                wire.bid_source_count,
                re.bid_source_count,
            );
            cmp(
                &mut out,
                "ask_source_count",
                wire.ask_source_count,
                re.ask_source_count,
            );
        }
        (MessageBody::Trade(wire), MessageBody::Trade(re)) => {
            cmp(
                &mut out,
                "instrument_id",
                wire.instrument_id,
                re.instrument_id,
            );
            cmp(&mut out, "source_id", wire.source_id, re.source_id);
            cmp_byte(
                &mut out,
                "aggressor_side",
                wire.aggressor_side,
                re.aggressor_side,
            );
            cmp_byte(&mut out, "trade_flags", wire.trade_flags, re.trade_flags);
            cmp(
                &mut out,
                "source_timestamp_ns",
                wire.source_timestamp_ns,
                re.source_timestamp_ns,
            );
            cmp(&mut out, "trade_price", wire.trade_price, re.trade_price);
            cmp(&mut out, "trade_qty", wire.trade_qty, re.trade_qty);
            cmp(&mut out, "trade_id", wire.trade_id, re.trade_id);
            cmp(
                &mut out,
                "cumulative_volume",
                wire.cumulative_volume,
                re.cumulative_volume,
            );
        }
        (MessageBody::Level(wire), MessageBody::Level(re)) => {
            cmp(
                &mut out,
                "instrument_id",
                wire.instrument_id,
                re.instrument_id,
            );
            cmp(&mut out, "source_id", wire.source_id, re.source_id);
            cmp_byte(&mut out, "side", wire.side, re.side);
            // The shipped defect this whole boundary was shaped around: a
            // publisher numbering the table from `New` emits every removal as a
            // change carrying zero, which is self-consistent and therefore
            // invisible to any test that encodes and then decodes. It is visible
            // here, because the other side of the comparison is a lowering that
            // derives the byte from the quantity.
            cmp_byte(&mut out, "action", wire.action, re.action);
            cmp(
                &mut out,
                "per_instrument_seq",
                wire.per_instrument_seq,
                re.per_instrument_seq,
            );
            cmp(&mut out, "price_raw", wire.price_raw, re.price_raw);
            cmp(&mut out, "qty_raw", wire.qty_raw, re.qty_raw);
            cmp(&mut out, "timestamp_ns", wire.timestamp_ns, re.timestamp_ns);
            cmp(&mut out, "order_count", wire.order_count, re.order_count);
            cmp(&mut out, "level_index", wire.level_index, re.level_index);
            cmp_byte(
                &mut out,
                "update_reason",
                wire.update_reason,
                re.update_reason,
            );
            cmp_byte(&mut out, "level_flags", wire.level_flags, re.level_flags);
        }
        (MessageBody::Clear(wire), MessageBody::Clear(re)) => {
            cmp(
                &mut out,
                "instrument_id",
                wire.instrument_id,
                re.instrument_id,
            );
            cmp(&mut out, "source_id", wire.source_id, re.source_id);
            cmp_byte(&mut out, "clear_side", wire.clear_side, re.clear_side);
            cmp_byte(&mut out, "scope", wire.scope, re.scope);
            cmp(
                &mut out,
                "per_instrument_seq",
                wire.per_instrument_seq,
                re.per_instrument_seq,
            );
            cmp(
                &mut out,
                "from_price_raw",
                wire.from_price_raw,
                re.from_price_raw,
            );
            cmp(&mut out, "timestamp_ns", wire.timestamp_ns, re.timestamp_ns);
            cmp_byte(&mut out, "clear_reason", wire.clear_reason, re.clear_reason);
        }
        // Two message types at one join key. Reported as a difference in the
        // type itself rather than as two absences, because that is what it is:
        // the publisher and the re-lowering agree about which upstream event
        // this is and disagree about what it became.
        (wire, re) => out.push(FieldDiff {
            field: "message_type",
            on_wire: wire.message_type().to_owned(),
            re_lowered: re.message_type().to_owned(),
        }),
    }
    out
}

/// Record a difference, if there is one.
fn cmp<T: PartialEq + Display>(out: &mut Vec<FieldDiff>, field: &'static str, wire: T, re: T) {
    if wire != re {
        out.push(FieldDiff {
            field,
            on_wire: wire.to_string(),
            re_lowered: re.to_string(),
        });
    }
}

/// The same, for a byte whose meaning is a flags word or an enumerated value.
///
/// Rendered in hex as well as decimal: `update_flags` and `action` are read
/// against the specification's own tables, and `3` against `0x03` is one lookup
/// either way.
fn cmp_byte(out: &mut Vec<FieldDiff>, field: &'static str, wire: u8, re: u8) {
    if wire != re {
        out.push(FieldDiff {
            field,
            on_wire: format!("{wire} (0x{wire:02x})"),
            re_lowered: format!("{re} (0x{re:02x})"),
        });
    }
}
