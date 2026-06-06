package main

import (
	"fmt"
	"time"
)

func init() {
	registerParser("marketbyorder", func() Parser { return &marketByOrderParser{} })
}

type marketByOrderParser struct{}

func (p *marketByOrderParser) Name() string { return "marketbyorder" }

// ParseFrame decodes one MBO frame and returns one Record per application message.
func (p *marketByOrderParser) ParseFrame(port string, frame []byte) ([]Record, error) {
	hdr, err := ParseFrameHeader(frame)
	if err != nil {
		return nil, fmt.Errorf("header: %w", err)
	}

	body := frame[frameHeaderSize:]
	records := make([]Record, 0, hdr.MessageCount)

	for i := uint8(0); i < hdr.MessageCount; i++ {
		mh, err := ParseMessageHeader(body)
		if err != nil {
			return nil, fmt.Errorf("msg %d header: %w", i, err)
		}
		if int(mh.Length) < messageHeaderSize {
			return nil, fmt.Errorf("%w: msg %d length %d", errMessageLength, i, mh.Length)
		}
		if int(mh.Length) > len(body) {
			return nil, fmt.Errorf("%w: msg %d length %d > %d remaining", errMessageLength, i, mh.Length, len(body))
		}
		msgBody := body[messageHeaderSize:mh.Length]

		rec, ok, err := p.decodeMessage(port, hdr, mh, msgBody)
		if err != nil {
			return nil, fmt.Errorf("msg %d type 0x%02x: %w", i, mh.Type, err)
		}
		if ok {
			records = append(records, rec)
		}

		body = body[mh.Length:]
	}

	return records, nil
}

func (p *marketByOrderParser) decodeMessage(port string, hdr FrameHeader, mh MessageHeader, body []byte) (Record, bool, error) {
	base := Record{
		Timestamp:      hdr.SendTimestamp,
		SendTSNS:       tsNS(hdr.SendTimestamp),
		ChannelID:      hdr.ChannelID,
		Port:           port,
		SequenceNumber: hdr.Sequence,
		ResetCount:     hdr.ResetCount,
	}

	switch mh.Type {
	case msgTypeHeartbeat:
		b, err := ParseHeartbeat(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "heartbeat"
		base.Fields = map[string]any{
			"channel_id_in_body": b.ChannelID,
			"timestamp":          b.Timestamp,
		}
		return base, true, nil

	case msgTypeInstrumentDefinition:
		b, err := ParseInstrumentDefinition(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "instrument_definition"
		base.InstrumentID = b.InstrumentID
		base.Fields = map[string]any{
			"symbol":         b.Symbol,
			"leg1":           b.Leg1,
			"leg2":           b.Leg2,
			"asset_class":    b.AssetClass,
			"price_exponent": b.PriceExponent,
			"qty_exponent":   b.QtyExponent,
			"market_model":   b.MarketModel,
			"tick_size_raw":  b.TickSizeRaw,
			"lot_size_raw":   b.LotSizeRaw,
			"contract_value": b.ContractValue,
			"expiry":         b.Expiry,
			"settle_type":    b.SettleType,
			"price_bound":    b.PriceBound,
			"manifest_seq":   b.ManifestSeq,
		}
		return base, true, nil

	case msgTypeTrade:
		b, err := ParseTrade(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "trade"
		base.InstrumentID = b.InstrumentID
		base.SourceTSNS = tsNS(b.SourceTimestamp)
		base.Fields = map[string]any{
			"source_id":             b.SourceID,
			"aggressor_side":        b.AggressorSide,
			"trade_flags":           b.TradeFlags,
			"source_timestamp":      b.SourceTimestamp,
			"trade_price_raw":       b.TradePriceRaw,
			"trade_qty_raw":         b.TradeQtyRaw,
			"trade_id":              b.TradeID,
			"cumulative_volume_raw": b.CumulativeVolumeRaw,
		}
		return base, true, nil

	case msgTypeEndOfSession:
		b, err := ParseEndOfSession(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "end_of_session"
		base.Fields = map[string]any{"timestamp": b.Timestamp}
		return base, true, nil

	case msgTypeManifestSummary:
		b, err := ParseManifestSummary(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "manifest_summary"
		base.Fields = map[string]any{
			"valid":            b.Valid,
			"manifest_seq":     b.ManifestSeq,
			"instrument_count": b.InstrumentCount,
			"timestamp":        b.Timestamp,
		}
		return base, true, nil

	case msgTypeOrderAdd:
		b, err := ParseOrderAdd(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "order_add"
		base.InstrumentID = b.InstrumentID
		base.SourceTSNS = tsNS(b.EnterTimestamp)
		base.Fields = map[string]any{
			"source_id":          b.SourceID,
			"side":               sideString(b.Side),
			"order_flags":        b.OrderFlags,
			"per_instrument_seq": b.PerInstrumentSeq,
			"order_id":           b.OrderID,
			"enter_ts":           b.EnterTimestamp,
			"price_raw":          b.PriceRaw,
			"qty_raw":            b.QtyRaw,
		}
		return base, true, nil

	case msgTypeOrderCancel:
		b, err := ParseOrderCancel(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "order_cancel"
		base.InstrumentID = b.InstrumentID
		base.SourceTSNS = tsNS(b.Timestamp)
		base.Fields = map[string]any{
			"source_id":          b.SourceID,
			"cancel_reason":      cancelReasonString(b.Reason),
			"per_instrument_seq": b.PerInstrumentSeq,
			"order_id":           b.OrderID,
			"timestamp":          b.Timestamp,
		}
		return base, true, nil

	case msgTypeOrderExecute:
		b, err := ParseOrderExecute(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "order_execute"
		base.InstrumentID = b.InstrumentID
		base.SourceTSNS = tsNS(b.Timestamp)
		base.Fields = map[string]any{
			"source_id":          b.SourceID,
			"aggressor_side":     aggressorString(b.AggressorSide),
			"exec_flags":         b.ExecFlags,
			"per_instrument_seq": b.PerInstrumentSeq,
			"order_id":           b.OrderID,
			"trade_id":           b.TradeID,
			"timestamp":          b.Timestamp,
			"exec_price_raw":     b.ExecPriceRaw,
			"exec_qty_raw":       b.ExecQtyRaw,
		}
		return base, true, nil

	case msgTypeBatchBoundary:
		b, err := ParseBatchBoundary(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "batch_boundary"
		base.SourceTSNS = tsNS(b.BatchTime)
		base.Fields = map[string]any{
			"batch_id": b.BatchID,
			"batch_ts": b.BatchTime,
		}
		return base, true, nil

	case msgTypeInstrumentReset:
		b, err := ParseInstrumentReset(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "instrument_reset"
		base.InstrumentID = b.InstrumentID
		base.SourceTSNS = tsNS(b.Timestamp)
		base.Fields = map[string]any{
			"reason":         resetReasonString(b.Reason),
			"new_anchor_seq": b.NewAnchorSeq,
			"timestamp":      b.Timestamp,
		}
		return base, true, nil

	case msgTypeSnapshotBegin:
		b, err := ParseSnapshotBegin(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "snapshot_begin"
		base.InstrumentID = b.InstrumentID
		base.Fields = map[string]any{
			"anchor_seq":          b.AnchorSeq,
			"total_orders":        b.TotalOrders,
			"snapshot_id":         b.SnapshotID,
			"last_instrument_seq": b.LastInstrumentSeq,
			"timestamp":           b.Timestamp,
		}
		return base, true, nil

	case msgTypeSnapshotOrder:
		// SnapshotOrder has no InstrumentID on the wire; instrument
		// association must be inferred from snapshot_id matching a SnapshotBegin.
		b, err := ParseSnapshotOrder(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "snapshot_order"
		base.SourceTSNS = tsNS(b.EnterTimestamp)
		base.Fields = map[string]any{
			"snapshot_id": b.SnapshotID,
			"order_id":    b.OrderID,
			"side":        sideString(b.Side),
			"order_flags": b.OrderFlags,
			"enter_ts":    b.EnterTimestamp,
			"price_raw":   b.PriceRaw,
			"qty_raw":     b.QtyRaw,
		}
		return base, true, nil

	case msgTypeSnapshotEnd:
		b, err := ParseSnapshotEnd(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "snapshot_end"
		base.InstrumentID = b.InstrumentID
		base.Fields = map[string]any{
			"anchor_seq":  b.AnchorSeq,
			"snapshot_id": b.SnapshotID,
		}
		return base, true, nil

	default:
		// Unknown type — skip per forward-compat rule. Caller advances by mh.Length.
		return Record{}, false, nil
	}
}

func sideString(s uint8) string {
	switch s {
	case 0:
		return "bid"
	case 1:
		return "ask"
	default:
		return "unknown"
	}
}

func aggressorString(s uint8) string {
	switch s {
	case 1:
		return "buy"
	case 2:
		return "sell"
	default:
		return "unknown"
	}
}

func cancelReasonString(r uint8) string {
	switch r {
	case 1:
		return "user_cancel"
	case 2:
		return "venue_expire"
	case 3:
		return "self_trade"
	case 4:
		return "margin"
	case 5:
		return "risk_limit"
	case 6:
		return "sibling_filled"
	case 255:
		return "other"
	default:
		return "unknown"
	}
}

// tsNS returns Unix-nanos for a non-zero time, else 0 (absent).
func tsNS(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	return uint64(t.UnixNano())
}

func resetReasonString(r uint8) string {
	switch r {
	case 1:
		return "publisher_inconsistency"
	case 2:
		return "venue_resync"
	case 3:
		return "upstream_gap"
	case 255:
		return "other"
	default:
		return "unknown"
	}
}
