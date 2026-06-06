package main

import (
	"time"
)

// EventsWriter dispatches Records into ClickHouse rows for events / wire_snapshots /
// channel_health / instruments tables. Idempotent (just enqueues).
type EventsWriter struct {
	ch *ClickhouseClient
}

func NewEventsWriter(ch *ClickhouseClient) *EventsWriter {
	return &EventsWriter{ch: ch}
}

// Write maps a single ChannelEvent into the right ClickHouse table(s).
func (w *EventsWriter) Write(ev ChannelEvent, channelID uint8, instSymbol string, priceExp, qtyExp int8) {
	if w.ch == nil {
		return
	}
	rec := ev.Record
	now := time.Now().UTC()

	switch rec.Type {
	case "instrument_definition":
		w.ch.Enqueue("instruments", map[string]any{
			"recv_ts":        chTime(now),
			"channel_id":     channelID,
			"instrument_id":  rec.InstrumentID,
			"symbol":         getString(rec.Fields, "symbol"),
			"leg1":           getString(rec.Fields, "leg1"),
			"leg2":           getString(rec.Fields, "leg2"),
			"asset_class":    assetClassString(getUint8(rec.Fields, "asset_class")),
			"market_model":   marketModelString(getUint8(rec.Fields, "market_model")),
			"price_exponent": getInt8(rec.Fields, "price_exponent"),
			"qty_exponent":   getInt8(rec.Fields, "qty_exponent"),
			"tick_size":      scalePrice(getInt64(rec.Fields, "tick_size_raw"), getInt8(rec.Fields, "price_exponent")),
			"lot_size":       scaleQty(getUint64(rec.Fields, "lot_size_raw"), getInt8(rec.Fields, "qty_exponent")),
			"contract_value": getUint64(rec.Fields, "contract_value"),
			"expiry_ts":      chTime(getTime(rec.Fields, "expiry")),
			"settle_type":    settleTypeString(getUint8(rec.Fields, "settle_type")),
			"price_bound":    priceBoundString(getUint8(rec.Fields, "price_bound")),
			"manifest_seq":   getUint16(rec.Fields, "manifest_seq"),
		})

	case "heartbeat", "manifest_summary", "end_of_session":
		row := map[string]any{
			"recv_ts":           chTime(rec.recvTime(now)),
			"publisher_send_ts": chTime(rec.sendTime()),
			"recv_ts_kind":      rec.RecvTSKind,
			"channel_id":        channelID,
			"kind":              rec.Type,
		}
		if rec.Type == "manifest_summary" {
			row["manifest_seq"] = getUint16(rec.Fields, "manifest_seq")
			row["manifest_valid"] = getUint8(rec.Fields, "valid")
			row["instrument_count"] = getUint32(rec.Fields, "instrument_count")
		}
		w.ch.Enqueue("channel_health", row)

	case "order_add", "order_cancel", "order_execute", "trade", "instrument_reset", "batch_boundary":
		row := buildEventRow(rec, channelID, instSymbol)
		switch rec.Type {
		case "order_add":
			row["source_id"] = getUint16(rec.Fields, "source_id")
			row["per_instrument_seq"] = getUint32(rec.Fields, "per_instrument_seq")
			row["order_id"] = getUint64(rec.Fields, "order_id")
			row["side"] = getString(rec.Fields, "side")
			row["order_flags"] = getUint8(rec.Fields, "order_flags")
			row["price"] = scalePrice(getInt64(rec.Fields, "price_raw"), priceExp)
			row["qty"] = scaleQty(getUint64(rec.Fields, "qty_raw"), qtyExp)
			row["enter_ts"] = chTime(getTime(rec.Fields, "enter_ts"))
		case "order_cancel":
			row["source_id"] = getUint16(rec.Fields, "source_id")
			row["per_instrument_seq"] = getUint32(rec.Fields, "per_instrument_seq")
			row["order_id"] = getUint64(rec.Fields, "order_id")
			row["cancel_reason"] = getString(rec.Fields, "cancel_reason")
		case "order_execute":
			row["source_id"] = getUint16(rec.Fields, "source_id")
			row["per_instrument_seq"] = getUint32(rec.Fields, "per_instrument_seq")
			row["order_id"] = getUint64(rec.Fields, "order_id")
			row["aggressor_side"] = getString(rec.Fields, "aggressor_side")
			row["exec_flags"] = getUint8(rec.Fields, "exec_flags")
			row["price"] = scalePrice(getInt64(rec.Fields, "exec_price_raw"), priceExp)
			row["qty"] = scaleQty(getUint64(rec.Fields, "exec_qty_raw"), qtyExp)
			row["trade_id"] = getUint64(rec.Fields, "trade_id")
		case "trade":
			row["source_id"] = getUint16(rec.Fields, "source_id")
			row["aggressor_side"] = getString(rec.Fields, "aggressor_side")
			row["price"] = scalePrice(getInt64(rec.Fields, "trade_price_raw"), priceExp)
			row["qty"] = scaleQty(getUint64(rec.Fields, "trade_qty_raw"), qtyExp)
			row["trade_id"] = getUint64(rec.Fields, "trade_id")
			row["cumulative_volume"] = scaleQty(getUint64(rec.Fields, "cumulative_volume_raw"), qtyExp)
		case "instrument_reset":
			row["reset_reason"] = getString(rec.Fields, "reason")
			row["new_anchor_seq"] = getUint64(rec.Fields, "new_anchor_seq")
		case "batch_boundary":
			row["batch_id"] = getUint32(rec.Fields, "batch_id")
			row["batch_ts"] = chTime(getTime(rec.Fields, "batch_ts"))
		}
		w.ch.Enqueue("events", row)
	}
}

// buildEventRow constructs the common timestamp and identity columns for an events row.
func buildEventRow(rec Record, channelID uint8, instSymbol string) map[string]any {
	row := map[string]any{
		"recv_ts":           chTime(rec.recvTime(time.Now().UTC())),
		"publisher_send_ts": chTime(rec.sendTime()),
		"recv_ts_kind":      rec.RecvTSKind,
		"channel_id":        channelID,
		"mktdata_seq":       rec.SequenceNumber,
		"reset_count":       rec.ResetCount,
		"kind":              rec.Type,
		"instrument_id":     rec.InstrumentID,
		"symbol":            instSymbol,
	}
	if src, ok := rec.sourceTime(); ok {
		row["source_ts"] = chTime(src)
	}
	return row
}

// WriteSnapshotOrder writes one SnapshotOrder record to wire_snapshots with full context
// denormalized from the open SnapshotBegin.
type SnapshotContext struct {
	InstrumentID      uint32
	Symbol            string
	SnapshotID        uint32
	AnchorSeq         uint64
	TotalOrders       uint32
	LastInstrumentSeq uint32
	PriceExponent     int8
	QtyExponent       int8
}

func (w *EventsWriter) WriteSnapshotOrder(rec Record, channelID uint8, ctx SnapshotContext) {
	if w.ch == nil {
		return
	}
	w.ch.Enqueue("wire_snapshots", map[string]any{
		"recv_ts":             chTime(time.Now().UTC()),
		"publisher_send_ts":   chTime(rec.Timestamp),
		"channel_id":          channelID,
		"instrument_id":       ctx.InstrumentID,
		"symbol":              ctx.Symbol,
		"snapshot_id":         ctx.SnapshotID,
		"anchor_seq":          ctx.AnchorSeq,
		"total_orders":        ctx.TotalOrders,
		"last_instrument_seq": ctx.LastInstrumentSeq,
		"order_id":            getUint64(rec.Fields, "order_id"),
		"side":                getString(rec.Fields, "side"),
		"order_flags":         getUint8(rec.Fields, "order_flags"),
		"enter_ts":            chTime(getTime(rec.Fields, "enter_ts")),
		"price":               scalePrice(getInt64(rec.Fields, "price_raw"), ctx.PriceExponent),
		"qty":                 scaleQty(getUint64(rec.Fields, "qty_raw"), ctx.QtyExponent),
	})
}

// Helper accessors (Records use map[string]any after JSON decode).
func getString(m map[string]any, k string) string  { return toString(m[k]) }
func getUint8(m map[string]any, k string) uint8    { return toUint8(m[k]) }
func getUint16(m map[string]any, k string) uint16  { return toUint16(m[k]) }
func getUint32(m map[string]any, k string) uint32  { return toUint32(m[k]) }
func getUint64(m map[string]any, k string) uint64  { return toUint64(m[k]) }
func getInt8(m map[string]any, k string) int8      { return toInt8(m[k]) }
func getInt64(m map[string]any, k string) int64    { return toInt64(m[k]) }
func getTime(m map[string]any, k string) time.Time { return toTime(m[k]) }

// scalePrice / scaleQty apply the per-instrument exponent. An exponent of 0 means
// the caller has already pre-scaled or wants raw integers as floats.
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
	if e >= 0 {
		v := 1.0
		for i := 0; i < e; i++ {
			v *= 10
		}
		return v
	}
	v := 1.0
	for i := 0; i < -e; i++ {
		v /= 10
	}
	return v
}

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
