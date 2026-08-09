package main

import (
	"time"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/clickhouse"
)

// enqueuer is satisfied by *clickhouse.Client and by test stubs.
type enqueuer interface {
	Enqueue(table string, row map[string]any) bool
}

// EventsWriter maps one ChannelEvent to ClickHouse rows. It is stateless: the
// batching client is already the asynchronous boundary and already drops to a
// counter when full, so no second queue sits in front of it.
type EventsWriter struct {
	ch enqueuer
}

func NewEventsWriter(ch enqueuer) *EventsWriter {
	return &EventsWriter{ch: ch}
}

// Write routes a record to its table. Prices and quantities are scaled by the
// instrument's exponents here, at the persistence boundary — book state stays
// raw.
func (w *EventsWriter) Write(ev ChannelEvent, channelID uint8, symbol string, priceExp, qtyExp int8) {
	if w == nil || w.ch == nil {
		return
	}
	rec := ev.Record
	now := time.Now().UTC()

	switch rec.Type {
	case "instrument_definition":
		w.ch.Enqueue("instruments", map[string]any{
			"recv_ts":       clickhouse.ChTime(now),
			"channel_id":    channelID,
			"instrument_id": rec.InstrumentID,
			"source_id":     getUint16(rec.Fields, "source_id"),
			"symbol":        getString(rec.Fields, "symbol"),
			"leg1":          getString(rec.Fields, "leg1"),
			"leg2":          getString(rec.Fields, "leg2"),
			// These four arrive as raw uint8 enums: unlike side, action and the
			// reason fields, the parser does NOT stringify them. The schema
			// declares them LowCardinality(String), so getString would silently
			// write empty strings for every instrument.
			"asset_class":    assetClassString(getUint8(rec.Fields, "asset_class")),
			"market_model":   marketModelString(getUint8(rec.Fields, "market_model")),
			"price_exponent": getInt8(rec.Fields, "price_exponent"),
			"qty_exponent":   getInt8(rec.Fields, "qty_exponent"),
			"tick_size":      scalePrice(getInt64(rec.Fields, "tick_size_raw"), getInt8(rec.Fields, "price_exponent")),
			"lot_size":       scaleQty(getUint64(rec.Fields, "lot_size_raw"), getInt8(rec.Fields, "qty_exponent")),
			"contract_value": getUint64(rec.Fields, "contract_value"),
			"expiry_ts":      clickhouse.ChTime(getTime(rec.Fields, "expiry")),
			"settle_type":    settleTypeString(getUint8(rec.Fields, "settle_type")),
			"price_bound":    priceBoundString(getUint8(rec.Fields, "price_bound")),
			"manifest_seq":   getUint16(rec.Fields, "manifest_seq"),
		})

	case "heartbeat", "manifest_summary", "end_of_session":
		row := map[string]any{
			"recv_ts":           clickhouse.ChTime(rec.recvTime(now)),
			"publisher_send_ts": clickhouse.ChTime(rec.sendTime()),
			"recv_ts_kind":      rec.RecvTSKind,
			"channel_id":        channelID,
			"kind":              rec.Type,
		}
		if src, ok := rec.sourceTime(); ok {
			row["source_ts"] = clickhouse.ChTime(src)
		}
		if rec.Type == "manifest_summary" {
			row["manifest_seq"] = getUint16(rec.Fields, "manifest_seq")
			row["manifest_valid"] = getUint8(rec.Fields, "valid")
			row["instrument_count"] = getUint32(rec.Fields, "instrument_count")
		}
		w.ch.Enqueue("channel_health", row)

	case "level_update", "book_clear", "trade", "liquidation", "batch_boundary", "instrument_reset":
		row := buildEventRow(rec, channelID, symbol, now)
		switch rec.Type {
		case "level_update":
			row["source_id"] = getUint16(rec.Fields, "source_id")
			row["per_instrument_seq"] = getUint32(rec.Fields, "per_instrument_seq")
			row["side"] = getString(rec.Fields, "side")
			row["price"] = scalePrice(getInt64(rec.Fields, "price_raw"), priceExp)
			row["qty"] = scaleQty(getUint64(rec.Fields, "qty_raw"), qtyExp)
			row["action"] = getString(rec.Fields, "action")
			row["update_reason"] = getString(rec.Fields, "update_reason")
			row["level_flags"] = getUint8(rec.Fields, "level_flags")
			// Absent means the 0xFFFF sentinel, which is SQL NULL. Zero is a real
			// count and a real rank, so it must not be conflated with absent.
			row["order_count"] = getOptUint32(rec.Fields, "order_count")
			row["level_index"] = getOptUint16(rec.Fields, "level_index")
		case "book_clear":
			row["source_id"] = getUint16(rec.Fields, "source_id")
			row["per_instrument_seq"] = getUint32(rec.Fields, "per_instrument_seq")
			row["clear_side"] = getString(rec.Fields, "clear_side")
			row["clear_scope"] = getString(rec.Fields, "scope")
			row["from_price"] = scalePrice(getInt64(rec.Fields, "from_price_raw"), priceExp)
			row["clear_reason"] = getString(rec.Fields, "clear_reason")
		case "trade":
			row["source_id"] = getUint16(rec.Fields, "source_id")
			row["trade_id"] = getUint64(rec.Fields, "trade_id")
			row["aggressor_side"] = getString(rec.Fields, "aggressor_side")
			row["price"] = scalePrice(getInt64(rec.Fields, "trade_price_raw"), priceExp)
			row["qty"] = scaleQty(getUint64(rec.Fields, "trade_qty_raw"), qtyExp)
			row["cumulative_volume"] = scaleQty(getUint64(rec.Fields, "cumulative_volume_raw"), qtyExp)
			row["trade_flags"] = getUint8(rec.Fields, "trade_flags")
		case "liquidation":
			row["source_id"] = getUint16(rec.Fields, "source_id")
			row["trade_id"] = getUint64(rec.Fields, "trade_id")
			row["liquidation_flags"] = getUint8(rec.Fields, "liquidation_flags")
			row["method"] = getString(rec.Fields, "method")
			row["mark_price"] = scalePrice(getInt64(rec.Fields, "mark_price_raw"), priceExp)
			row["liquidated_user"] = getString(rec.Fields, "liquidated_user")
		case "batch_boundary":
			row["batch_id"] = getUint32(rec.Fields, "batch_id")
			row["batch_ts"] = clickhouse.ChTime(getTime(rec.Fields, "batch_ts"))
		case "instrument_reset":
			row["reset_reason"] = getString(rec.Fields, "reason")
			row["new_anchor_seq"] = getUint64(rec.Fields, "new_anchor_seq")
		}
		w.ch.Enqueue("events", row)
	}
}

// WriteWireLevel captures one raw SnapshotLevel for replay, denormalizing the
// group identity from the instrument's last SnapshotBegin.
func (w *EventsWriter) WriteWireLevel(rec Record, channelID uint8, g SnapshotGroup, symbol string, priceExp, qtyExp int8) {
	if w == nil || w.ch == nil {
		return
	}
	w.ch.Enqueue("wire_levels", map[string]any{
		"recv_ts":             clickhouse.ChTime(rec.recvTime(time.Now().UTC())),
		"publisher_send_ts":   clickhouse.ChTime(rec.sendTime()),
		"channel_id":          channelID,
		"instrument_id":       rec.InstrumentID,
		"symbol":              symbol,
		"snapshot_id":         g.SnapshotID,
		"anchor_seq":          g.AnchorSeq,
		"total_levels":        g.TotalLevels,
		"last_instrument_seq": g.LastInstrumentSeq,
		"depth_bound":         g.DepthBound,
		"side":                getString(rec.Fields, "side"),
		"price":               scalePrice(getInt64(rec.Fields, "price_raw"), priceExp),
		"qty":                 scaleQty(getUint64(rec.Fields, "qty_raw"), qtyExp),
		"order_count":         getOptUint32(rec.Fields, "order_count"),
		"level_flags":         getUint8(rec.Fields, "level_flags"),
	})
}

// buildEventRow fills the identity and timestamp columns shared by every kind.
func buildEventRow(rec Record, channelID uint8, symbol string, now time.Time) map[string]any {
	row := map[string]any{
		"recv_ts":           clickhouse.ChTime(rec.recvTime(now)),
		"publisher_send_ts": clickhouse.ChTime(rec.sendTime()),
		"recv_ts_kind":      rec.RecvTSKind,
		"channel_id":        channelID,
		"mktdata_seq":       rec.SequenceNumber,
		"reset_count":       rec.ResetCount,
		"kind":              rec.Type,
		"instrument_id":     rec.InstrumentID,
		"symbol":            symbol,
	}
	if src, ok := rec.sourceTime(); ok {
		row["source_ts"] = clickhouse.ChTime(src)
	}
	return row
}

// Field accessors. Records carry map[string]any after JSON decode.
//
// getUint32 is not declared here: coordinator.go already defines a
// byte-for-byte identical helper (func getUint32(fields map[string]any, key
// string) uint32 { return toUint32(fields[key]) }), and Go forbids
// redeclaring a package-level function. We reuse that one rather than
// duplicate it.
func getString(m map[string]any, k string) string  { return toString(m[k]) }
func getUint8(m map[string]any, k string) uint8    { return toUint8(m[k]) }
func getUint16(m map[string]any, k string) uint16  { return toUint16(m[k]) }
func getUint64(m map[string]any, k string) uint64  { return toUint64(m[k]) }
func getInt8(m map[string]any, k string) int8      { return toInt8(m[k]) }
func getInt64(m map[string]any, k string) int64    { return toInt64(m[k]) }
func getTime(m map[string]any, k string) time.Time { return toTime(m[k]) }

// getOptUint32 and getOptUint16 return nil for an absent key, which encodes as
// SQL NULL. The parser omits these keys when the wire carried 0xFFFF, so absent
// means "the venue did not supply it" — distinct from a supplied zero.
func getOptUint32(m map[string]any, k string) any {
	if _, present := m[k]; !present {
		return nil
	}
	return toUint32(m[k])
}

func getOptUint16(m map[string]any, k string) any {
	if _, present := m[k]; !present {
		return nil
	}
	return toUint16(m[k])
}

// scalePrice and scaleQty apply the per-instrument exponent at the persistence
// boundary. An exponent of 0 means raw integers as floats.
func scalePrice(raw int64, exp int8) float64 {
	if exp == 0 {
		return float64(raw)
	}
	return float64(raw) * pow10f(int(exp))
}

func scaleQty(raw uint64, exp int8) float64 {
	if exp == 0 {
		return float64(raw)
	}
	return float64(raw) * pow10f(int(exp))
}

func pow10f(e int) float64 {
	v := 1.0
	if e >= 0 {
		for i := 0; i < e; i++ {
			v *= 10
		}
		return v
	}
	for i := 0; i < -e; i++ {
		v /= 10
	}
	return v
}

// Enum stringers for instrument_definition. The parser stringifies side, action
// and the reason fields inline, but leaves these four as raw uint8, so the
// mapping lives here. Values match the sibling market-by-order bot.
func assetClassString(v uint8) string {
	switch v {
	case 1:
		return "crypto_spot"
	case 2:
		return "prediction_binary"
	case 3:
		return "prediction_scalar"
	case 4:
		return "prediction_categorical"
	default:
		return "unknown"
	}
}

func marketModelString(v uint8) string {
	switch v {
	case 1:
		return "clob"
	case 2:
		return "amm"
	default:
		return "unknown"
	}
}

func settleTypeString(v uint8) string {
	switch v {
	case 1:
		return "cash"
	case 2:
		return "physical"
	default:
		return "n_a"
	}
}

func priceBoundString(v uint8) string {
	switch v {
	case 1:
		return "bounded_01"
	case 2:
		return "non_negative"
	default:
		return "unbounded"
	}
}
