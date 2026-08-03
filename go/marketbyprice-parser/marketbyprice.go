package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

func init() {
	registerParser("marketbyprice", func() Parser { return &marketByPriceParser{} })
}

// Defects counts publisher-side protocol violations the spec asks a subscriber
// to surface, for one frame. Observability only; they never change decoding.
type Defects struct {
	SnapshotFlagMismatch int
	MalformedBookClear   int
}

// marketByPriceParser is stateless. It deliberately holds no counters: the
// Runner shares one instance across all three port goroutines, so any mutable
// field here would be a data race. Defect counts are returned per frame instead.
type marketByPriceParser struct{}

func (p *marketByPriceParser) Name() string { return "marketbyprice" }

// ParseFrame decodes one frame and returns one Record per application message,
// plus the defects observed in this frame.
//
// A malformed individual message is dropped and counted; it does not fail the
// frame, because its neighbors are independently valid. A malformed frame
// structure (bad header, or a Message Length that cannot be trusted to advance
// the walk) fails the frame.
func (p *marketByPriceParser) ParseFrame(port string, frame []byte) ([]Record, Defects, error) {
	var defects Defects

	hdr, err := ParseFrameHeader(frame)
	if err != nil {
		return nil, defects, fmt.Errorf("header: %w", err)
	}

	body := frame[frameHeaderSize:]
	records := make([]Record, 0, hdr.MessageCount)

	for i := uint8(0); i < hdr.MessageCount; i++ {
		mh, err := ParseMessageHeader(body)
		if err != nil {
			return nil, defects, fmt.Errorf("msg %d header: %w", i, err)
		}
		// The < 4 floor prevents a slice-bounds panic on body[messageHeaderSize:mh.Length]
		// when Message Length is below the header size. It is not needed to
		// prevent a hang: the walk is bounded by Message Count (a u8), so an
		// infinite loop is not reachable regardless of how far body advances.
		if int(mh.Length) < messageHeaderSize {
			return nil, defects, fmt.Errorf("%w: msg %d length %d", errMessageLength, i, mh.Length)
		}
		if int(mh.Length) > len(body) {
			return nil, defects, fmt.Errorf("%w: msg %d length %d > %d remaining", errMessageLength, i, mh.Length, len(body))
		}
		msgBody := body[messageHeaderSize:mh.Length]

		// Flags bit 0 must be set on the snapshot port and clear on the other
		// two. Disagreement is a publisher defect; it never affects routing,
		// which uses Type ID and port only.
		if set := mh.Flags&flagSnapshot != 0; (port == "snapshot") != set {
			defects.SnapshotFlagMismatch++
		}

		rec, ok, decErr := p.decodeMessage(port, hdr, mh, msgBody)

		// Advance BEFORE any early-continue below. A `continue` that skips this
		// leaves the walk pointing at the message just consumed, so the next
		// iteration re-parses it and every subsequent message is misaligned.
		body = body[mh.Length:]

		if decErr != nil {
			// A body the spec declares malformed is dropped and counted, not
			// escalated to a frame failure — its neighbors are independently valid.
			if errors.Is(decErr, errMalformedBody) {
				if mh.Type == msgTypeBookClear {
					defects.MalformedBookClear++
				}
				continue
			}
			return nil, defects, fmt.Errorf("msg %d type 0x%02x: %w", i, mh.Type, decErr)
		}
		if ok {
			records = append(records, rec)
		}
	}

	return records, defects, nil
}

func (p *marketByPriceParser) decodeMessage(port string, hdr FrameHeader, mh MessageHeader, body []byte) (Record, bool, error) {
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
			"aggressor_side":        aggressorString(b.AggressorSide),
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

	case msgTypeLiquidation:
		b, err := ParseLiquidation(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "liquidation"
		base.InstrumentID = b.InstrumentID
		base.Fields = map[string]any{
			"source_id":         b.SourceID,
			"liquidation_flags": b.Flags,
			"liquidated_side":   liquidatedSideString(b.Flags),
			"adl":               b.Flags&0x02 != 0,
			"method":            liquidationMethodString(b.Method),
			"trade_id":          b.TradeID,
			"mark_price_raw":    b.MarkPriceRaw,
			"liquidated_user":   hex.EncodeToString(b.LiquidatedUser[:]),
		}
		return base, true, nil

	case msgTypeLevelUpdate:
		b, err := ParseLevelUpdate(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "level_update"
		base.InstrumentID = b.InstrumentID
		base.SourceTSNS = tsNS(b.Timestamp)
		base.Fields = map[string]any{
			"source_id":          b.SourceID,
			"side":               sideString(b.Side),
			"action":             actionString(b.Action),
			"per_instrument_seq": b.PerInstrumentSeq,
			"price_raw":          b.PriceRaw,
			"qty_raw":            b.QtyRaw,
			"timestamp":          b.Timestamp,
			"update_reason":      updateReasonString(b.UpdateReason),
			"level_flags":        b.LevelFlags,
			"implied":            b.LevelFlags&0x01 != 0,
			"amm_synthetic":      b.LevelFlags&0x02 != 0,
		}
		// 0xFFFF means absent. Omit rather than emit a number that would read
		// as a count or rank of 65535.
		if b.OrderCount != u16Unavailable {
			base.Fields["order_count"] = b.OrderCount
		}
		if b.LevelIndex != u16Unavailable {
			base.Fields["level_index"] = b.LevelIndex
		}
		return base, true, nil

	case msgTypeBookClear:
		b, err := ParseBookClear(body)
		if err != nil {
			// ParseFrame counts the malformed case and drops the message.
			return Record{}, false, err
		}
		base.Type = "book_clear"
		base.InstrumentID = b.InstrumentID
		base.SourceTSNS = tsNS(b.Timestamp)
		base.Fields = map[string]any{
			"source_id":          b.SourceID,
			"clear_side":         clearSideString(b.ClearSide),
			"scope":              clearScopeString(b.Scope),
			"per_instrument_seq": b.PerInstrumentSeq,
			"timestamp":          b.Timestamp,
			"clear_reason":       clearReasonString(b.ClearReason),
		}
		// From Price is only meaningful when Scope = 1.
		if b.Scope == 1 {
			base.Fields["from_price_raw"] = b.FromPriceRaw
		}
		return base, true, nil

	case msgTypeBatchBoundary:
		b, err := ParseBatchBoundary(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "batch_boundary"
		// A framing/control message. Batch Time is a batch marker rather than a
		// venue timestamp for a book event, so it gets no source_ts.
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
			"total_levels":        b.TotalLevels,
			"snapshot_id":         b.SnapshotID,
			"last_instrument_seq": b.LastInstrumentSeq,
			"timestamp":           b.Timestamp,
			"depth_bound":         b.DepthBound,
		}
		return base, true, nil

	case msgTypeSnapshotLevel:
		// No Instrument ID on the wire: InstrumentID stays 0. A consumer must
		// attribute this record to the currently-open SnapshotBegin on the
		// snapshot port; Snapshot ID is monotonic per (channel_id, instrument_id),
		// not per channel, so it validates that association (discard on
		// mismatch) rather than keying it.
		b, err := ParseSnapshotLevel(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "snapshot_level"
		base.Fields = map[string]any{
			"snapshot_id":   b.SnapshotID,
			"price_raw":     b.PriceRaw,
			"qty_raw":       b.QtyRaw,
			"side":          sideString(b.Side),
			"level_flags":   b.LevelFlags,
			"implied":       b.LevelFlags&0x01 != 0,
			"amm_synthetic": b.LevelFlags&0x02 != 0,
		}
		if b.OrderCount != u16Unavailable {
			base.Fields["order_count"] = b.OrderCount
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
		// Unknown type — skip per the forward-compatibility rule. This covers the
		// reserved 0x50-0x5F positional-index range. Caller advances by mh.Length.
		return Record{}, false, nil
	}
}

// --- enum stringers ---
//
// The spec requires receivers to accept any u8 and to treat unrecognised values
// as the unknown member, and permits new values without a Schema Version bump.
// An unrecognised value is therefore never an error.

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

func clearSideString(s uint8) string {
	switch s {
	case 0:
		return "bid"
	case 1:
		return "ask"
	case 2:
		return "both"
	default:
		return "unknown"
	}
}

func clearScopeString(s uint8) string {
	switch s {
	case 0:
		return "entire_side"
	case 1:
		return "from_price"
	default:
		return "unknown"
	}
}

func actionString(a uint8) string {
	switch a {
	case 0:
		return "unknown"
	case 1:
		return "new"
	case 2:
		return "change"
	case 3:
		return "delete"
	case 255:
		return "other"
	default:
		return "unknown"
	}
}

func updateReasonString(r uint8) string {
	switch r {
	case 0:
		return "unknown"
	case 1:
		return "trade"
	case 2:
		return "cancel"
	case 3:
		return "new_order"
	case 4:
		return "amend"
	case 5:
		return "venue_action"
	case 255:
		return "other"
	default:
		return "unknown"
	}
}

func clearReasonString(r uint8) string {
	switch r {
	case 0:
		return "unspecified"
	case 1:
		return "halt"
	case 2:
		return "session_end"
	case 3:
		return "venue_reset"
	case 4:
		return "settled"
	case 255:
		return "other"
	default:
		return "unknown"
	}
}

func resetReasonString(r uint8) string {
	switch r {
	case 0:
		return "unspecified"
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

func liquidationMethodString(m uint8) string {
	switch m {
	case 0:
		return "market"
	case 1:
		return "backstop"
	case 255:
		return "unknown"
	default:
		return "unknown"
	}
}

// liquidatedSideString reads Liquidation Flags bit 0.
func liquidatedSideString(flags uint8) string {
	if flags&0x01 != 0 {
		return "short"
	}
	return "long"
}

// tsNS returns Unix-nanos for a non-zero time, else 0 (absent).
func tsNS(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	return uint64(t.UnixNano())
}
