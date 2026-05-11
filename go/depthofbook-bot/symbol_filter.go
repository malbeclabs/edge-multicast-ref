package main

import (
	"sort"
	"strings"
)

// SymbolFilter limits the expensive DOB book/build/write path to selected
// instruments while still allowing channel-level health records through.
type SymbolFilter struct {
	allowed           map[string]struct{}
	instrumentAllowed map[uint32]struct{}
	activeSnapshots   map[uint8]uint32
}

func NewSymbolFilter(csv string) *SymbolFilter {
	f := &SymbolFilter{
		allowed:           map[string]struct{}{},
		instrumentAllowed: map[uint32]struct{}{},
		activeSnapshots:   map[uint8]uint32{},
	}
	for _, part := range strings.Split(csv, ",") {
		sym := normalizeSymbol(part)
		if sym == "" {
			continue
		}
		f.allowed[sym] = struct{}{}
	}
	return f
}

func (f *SymbolFilter) Enabled() bool {
	return f != nil && len(f.allowed) > 0
}

func (f *SymbolFilter) String() string {
	if !f.Enabled() {
		return "all"
	}
	syms := make([]string, 0, len(f.allowed))
	for sym := range f.allowed {
		syms = append(syms, sym)
	}
	sort.Strings(syms)
	return strings.Join(syms, ",")
}

func (f *SymbolFilter) Allow(rec Record) bool {
	if !f.Enabled() {
		return true
	}

	switch rec.Type {
	case "heartbeat", "manifest_summary", "end_of_session":
		return true
	case "instrument_definition":
		if f.symbolAllowed(toString(rec.Fields["symbol"])) {
			f.instrumentAllowed[rec.InstrumentID] = struct{}{}
			return true
		}
		delete(f.instrumentAllowed, rec.InstrumentID)
		return false
	case "snapshot_begin":
		if f.instrumentAllowedID(rec.InstrumentID) {
			f.activeSnapshots[rec.ChannelID] = rec.InstrumentID
			return true
		}
		return false
	case "snapshot_order":
		_, ok := f.activeSnapshots[rec.ChannelID]
		return ok
	case "snapshot_end":
		allowed := f.instrumentAllowedID(rec.InstrumentID)
		if active, ok := f.activeSnapshots[rec.ChannelID]; ok && active == rec.InstrumentID {
			delete(f.activeSnapshots, rec.ChannelID)
		}
		return allowed
	case "order_add", "order_cancel", "order_execute", "trade", "batch_boundary", "instrument_reset":
		return f.instrumentAllowedID(rec.InstrumentID)
	default:
		return false
	}
}

func (f *SymbolFilter) instrumentAllowedID(id uint32) bool {
	_, ok := f.instrumentAllowed[id]
	return ok
}

func (f *SymbolFilter) symbolAllowed(symbol string) bool {
	_, ok := f.allowed[normalizeSymbol(symbol)]
	return ok
}

func normalizeSymbol(symbol string) string {
	return strings.ToUpper(strings.TrimSpace(symbol))
}
