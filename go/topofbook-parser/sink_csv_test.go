package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCSVFileSink_QuotesAndTrades(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "output.csv")

	sink, err := NewCSVFileSink(path)
	if err != nil {
		t.Fatalf("error creating sink: %v", err)
	}

	ts := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	records := []Record{
		{
			Type:           "quote",
			Timestamp:      ts,
			ChannelID:      1,
			SequenceNumber: 100,
			InstrumentID:   42,
			Symbol:         "BTC-USDT",
			Fields: map[string]any{
				"source_id":        uint16(1),
				"bid_price":        67432.5,
				"bid_qty":          1.25,
				"ask_price":        67433.0,
				"ask_qty":          0.8,
				"bid_source_count": uint16(5),
				"ask_source_count": uint16(3),
				"update_flags":     uint8(3),
				"snapshot":         false,
			},
		},
		{
			Type:           "trade",
			Timestamp:      ts,
			ChannelID:      1,
			SequenceNumber: 101,
			InstrumentID:   42,
			Symbol:         "BTC-USDT",
			Fields: map[string]any{
				"source_id":         uint16(1),
				"trade_price":       67432.75,
				"trade_qty":         0.5,
				"aggressor_side":    "buy",
				"trade_id":          uint64(12345),
				"cumulative_volume": 100.0,
				"snapshot":          false,
			},
		},
	}

	if err := sink.Write(records); err != nil {
		t.Fatalf("error writing records: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("error closing sink: %v", err)
	}

	// Read back and verify.
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("error opening output: %v", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1 // quote and trade rows have different column counts
	allRows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("error reading CSV: %v", err)
	}

	// Expect: quote header, quote row, trade header, trade row = 4 rows.
	if len(allRows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(allRows))
	}

	// Quote header.
	if allRows[0][0] != "type" {
		t.Errorf("expected quote header first column 'type', got %q", allRows[0][0])
	}
	if len(allRows[0]) != len(quoteCSVHeader) {
		t.Errorf("quote header has %d columns, expected %d", len(allRows[0]), len(quoteCSVHeader))
	}

	// Quote row.
	if allRows[1][0] != "quote" {
		t.Errorf("expected 'quote', got %q", allRows[1][0])
	}
	if allRows[1][5] != "BTC-USDT" {
		t.Errorf("expected symbol BTC-USDT, got %q", allRows[1][5])
	}
	if allRows[1][7] != "67432.5" {
		t.Errorf("expected bid_price 67432.5, got %q", allRows[1][7])
	}

	// Trade header.
	if allRows[2][0] != "type" {
		t.Errorf("expected trade header first column 'type', got %q", allRows[2][0])
	}
	if len(allRows[2]) != len(tradeCSVHeader) {
		t.Errorf("trade header has %d columns, expected %d", len(allRows[2]), len(tradeCSVHeader))
	}

	// Trade row.
	if allRows[3][0] != "trade" {
		t.Errorf("expected 'trade', got %q", allRows[3][0])
	}
	if allRows[3][9] != "buy" {
		t.Errorf("expected aggressor_side 'buy', got %q", allRows[3][9])
	}
}

func TestCSVFileSink_SkipsNonQuoteTrade(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "output.csv")

	sink, err := NewCSVFileSink(path)
	if err != nil {
		t.Fatalf("error creating sink: %v", err)
	}

	ts := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	records := []Record{
		{Type: "heartbeat", Timestamp: ts, ChannelID: 1, SequenceNumber: 1},
		{Type: "instrument_definition", Timestamp: ts, ChannelID: 1, SequenceNumber: 2},
		{Type: "channel_reset", Timestamp: ts, ChannelID: 1, SequenceNumber: 3},
	}

	if err := sink.Write(records); err != nil {
		t.Fatalf("error writing records: %v", err)
	}
	sink.Close()

	// File should be empty — no quote or trade records.
	info, _ := os.Stat(path)
	if info.Size() != 0 {
		t.Errorf("expected empty file for non-quote/trade records, got %d bytes", info.Size())
	}
}

func TestCSVFileSink_HeaderWrittenOnce(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "output.csv")

	sink, err := NewCSVFileSink(path)
	if err != nil {
		t.Fatalf("error creating sink: %v", err)
	}

	ts := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	quote := Record{
		Type: "quote", Timestamp: ts, ChannelID: 1, SequenceNumber: 100,
		InstrumentID: 1, Symbol: "SOL-USDT",
		Fields: map[string]any{
			"source_id": uint16(1), "bid_price": 185.0, "bid_qty": 10.0,
			"ask_price": 186.0, "ask_qty": 5.0, "bid_source_count": uint16(1),
			"ask_source_count": uint16(1), "update_flags": uint8(3), "snapshot": false,
		},
	}

	// Write two batches of quotes.
	sink.Write([]Record{quote})
	quote.SequenceNumber = 101
	sink.Write([]Record{quote})
	sink.Close()

	f, _ := os.Open(path)
	defer f.Close()
	rows, _ := csv.NewReader(f).ReadAll()

	// 1 header + 2 data rows = 3.
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows (1 header + 2 data), got %d", len(rows))
	}
	if rows[0][0] != "type" {
		t.Errorf("expected header row first, got %q", rows[0][0])
	}
}

// shortTempSock returns a Unix-domain socket path short enough to fit within the
// macOS sockaddr_un.sun_path limit (104 bytes). t.TempDir() embeds the test's
// function name, which for longer names pushes the socket path over that limit
// and makes bind fail with EINVAL ("invalid argument"). A minimal-prefix temp
// dir keeps the path short regardless of the test name.
func shortTempSock(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "s")
	if err != nil {
		t.Fatalf("creating temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

// quoteRecord is a quote record carrying every field the CSV column layout
// reads, so it serves the CSV and the JSON socket tests alike.
func quoteRecord(ts time.Time) Record {
	return Record{
		Type: "quote", Timestamp: ts, ChannelID: 1, SequenceNumber: 100,
		InstrumentID: 42, Symbol: "BTC-USDT",
		Fields: map[string]any{
			"source_id": uint16(1), "bid_price": 67432.5, "bid_qty": 1.25,
			"ask_price": 67433.0, "ask_qty": 0.8, "bid_source_count": uint16(5),
			"ask_source_count": uint16(3), "update_flags": uint8(3), "snapshot": false,
		},
	}
}

// TestNewSink_CSVOverSocket covers the format seam this feed owns: a csv
// socket client is served the quote/trade columns, header first.
func TestNewSink_CSVOverSocket(t *testing.T) {
	sockPath := shortTempSock(t)

	s, err := NewSink(SinkConfig{Format: "csv", Path: "unix://" + sockPath})
	if err != nil {
		t.Fatalf("error creating socket sink: %v", err)
	}
	defer s.Close()

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("error connecting: %v", err)
	}
	defer conn.Close()

	time.Sleep(50 * time.Millisecond)

	ts := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	if err := s.Write([]Record{quoteRecord(ts)}); err != nil {
		t.Fatalf("error writing: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second)) //nolint:errcheck
	scanner := bufio.NewScanner(conn)

	// First line should be the CSV header.
	if !scanner.Scan() {
		t.Fatal("no header received")
	}
	header := scanner.Text()
	if !strings.HasPrefix(header, "type,") {
		t.Errorf("expected the CSV header, got %q", header)
	}
	if header != strings.Join(quoteCSVHeader, ",") {
		t.Errorf("header is %q, want %q", header, strings.Join(quoteCSVHeader, ","))
	}

	// Second line should be the data row.
	if !scanner.Scan() {
		t.Fatal("no data row received")
	}
	row := scanner.Text()
	if !strings.HasPrefix(row, "quote,") {
		t.Errorf("expected a quote row, got %q", row)
	}
}

// TestNewSink_CSVHeaderWrittenOncePerClient pins that the header tracking is
// per client rather than per batch.
func TestNewSink_CSVHeaderWrittenOncePerClient(t *testing.T) {
	sockPath := shortTempSock(t)

	s, err := NewSink(SinkConfig{Format: "csv", Path: "unix://" + sockPath})
	if err != nil {
		t.Fatalf("error creating socket sink: %v", err)
	}
	defer s.Close()

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("error connecting: %v", err)
	}
	defer conn.Close()

	time.Sleep(50 * time.Millisecond)

	ts := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 2; i++ {
		if err := s.Write([]Record{quoteRecord(ts)}); err != nil {
			t.Fatalf("error writing batch %d: %v", i, err)
		}
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second)) //nolint:errcheck
	scanner := bufio.NewScanner(conn)
	var lines []string
	for len(lines) < 3 && scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d: %q", len(lines), lines)
	}
	if !strings.HasPrefix(lines[0], "type,") {
		t.Errorf("first line is not the header: %q", lines[0])
	}
	for _, line := range lines[1:] {
		if strings.HasPrefix(line, "type,") {
			t.Errorf("the header was written more than once: %q", lines)
		}
	}
}

// TestNewSink_JSONOverSocket pins the other branch of the same switch: a json
// socket client is served JSONL by the shared writer.
func TestNewSink_JSONOverSocket(t *testing.T) {
	sockPath := shortTempSock(t)

	s, err := NewSink(SinkConfig{Format: "json", Path: "unix://" + sockPath})
	if err != nil {
		t.Fatalf("error creating socket sink: %v", err)
	}
	defer s.Close()

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("error connecting: %v", err)
	}
	defer conn.Close()

	time.Sleep(50 * time.Millisecond)

	ts := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	if err := s.Write([]Record{quoteRecord(ts)}); err != nil {
		t.Fatalf("error writing: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second)) //nolint:errcheck
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		t.Fatal("no data received")
	}
	var r Record
	if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
		t.Fatalf("error decoding JSON: %v", err)
	}
	if r.Symbol != "BTC-USDT" {
		t.Errorf("expected symbol BTC-USDT, got %q", r.Symbol)
	}
}

// TestNewSink_UnknownFormat keeps an unrecognised format an error rather than a
// sink that silently serves nothing.
func TestNewSink_UnknownFormat(t *testing.T) {
	if s, err := NewSink(SinkConfig{Format: "parquet", Path: filepath.Join(t.TempDir(), "out")}); err == nil {
		s.Close()
		t.Fatal("expected an error for an unknown format, got nil")
	}
}
