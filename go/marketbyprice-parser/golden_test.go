package main

import (
	"os"
	"path/filepath"
	"testing"
)

// The golden vectors are the cross-language contract. The Rust codec crate
// writes these five files and asserts the same field values against them; this
// side reads them with the decoder that has always been here.
//
// That is what makes them worth having. Two implementations tested only against
// themselves agree with themselves — including when both are wrong in the same
// way. A layout change made on one side alone fails here.
//
// The files carry the application message including its 4-byte header; the
// Parse* functions below take the body, so each case slices past it.
const goldenDir = "../../testdata/golden"

func goldenBytes(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(goldenDir, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return b
}

// header asserts the 4-byte application message header and returns the body.
func header(t *testing.T, buf []byte, wantType uint8, wantSize int) []byte {
	t.Helper()
	if len(buf) != wantSize {
		t.Fatalf("golden is %d bytes, want %d", len(buf), wantSize)
	}
	if buf[0] != wantType {
		t.Fatalf("type id 0x%02x, want 0x%02x", buf[0], wantType)
	}
	if int(buf[1]) != wantSize {
		t.Fatalf("declared length %d, want %d", buf[1], wantSize)
	}
	return buf[messageHeaderSize:]
}

func TestGoldenLevelUpdate(t *testing.T) {
	body := header(t, goldenBytes(t, "level-update-v3.bin"), msgTypeLevelUpdate, 48)
	b, err := ParseLevelUpdate(body)
	if err != nil {
		t.Fatalf("ParseLevelUpdate: %v", err)
	}
	if b.InstrumentID != 1 || b.SourceID != 2 || b.Side != 1 || b.Action != 1 {
		t.Errorf("identity fields: %+v", b)
	}
	if b.PerInstrumentSeq != 4242 || b.PriceRaw != 10000500 || b.QtyRaw != 7250 {
		t.Errorf("level fields: %+v", b)
	}
	if b.Timestamp.UnixNano() != 1700000000000000003 {
		t.Errorf("timestamp %d", b.Timestamp.UnixNano())
	}
	if b.OrderCount != 5 || b.LevelIndex != 6 || b.UpdateReason != 2 || b.LevelFlags != 8 {
		t.Errorf("annotations: %+v", b)
	}
}

func TestGoldenBookClear(t *testing.T) {
	body := header(t, goldenBytes(t, "book-clear-v3.bin"), msgTypeBookClear, 36)
	b, err := ParseBookClear(body)
	if err != nil {
		t.Fatalf("ParseBookClear: %v", err)
	}
	if b.InstrumentID != 1 || b.SourceID != 2 || b.ClearSide != 1 || b.Scope != 1 {
		t.Errorf("identity fields: %+v", b)
	}
	if b.PerInstrumentSeq != 4243 || b.FromPriceRaw != 10000500 || b.ClearReason != 3 {
		t.Errorf("clear fields: %+v", b)
	}
	if b.Timestamp.UnixNano() != 1700000000000000004 {
		t.Errorf("timestamp %d", b.Timestamp.UnixNano())
	}
}

func TestGoldenSnapshotBegin(t *testing.T) {
	body := header(t, goldenBytes(t, "snapshot-begin-v3.bin"), msgTypeSnapshotBegin, 40)
	b, err := ParseSnapshotBegin(body)
	if err != nil {
		t.Fatalf("ParseSnapshotBegin: %v", err)
	}
	if b.InstrumentID != 1 || b.AnchorSeq != 918273645 || b.TotalLevels != 2 {
		t.Errorf("anchor fields: %+v", b)
	}
	if b.SnapshotID != 77 || b.LastInstrumentSeq != 4241 || b.DepthBound != 50 {
		t.Errorf("snapshot fields: %+v", b)
	}
	if b.Timestamp.UnixNano() != 1700000000000000005 {
		t.Errorf("timestamp %d", b.Timestamp.UnixNano())
	}
}

func TestGoldenSnapshotLevel(t *testing.T) {
	body := header(t, goldenBytes(t, "snapshot-level-v3.bin"), msgTypeSnapshotLevel, 32)
	b, err := ParseSnapshotLevel(body)
	if err != nil {
		t.Fatalf("ParseSnapshotLevel: %v", err)
	}
	if b.SnapshotID != 77 || b.PriceRaw != 9999500 || b.QtyRaw != 12500 {
		t.Errorf("level fields: %+v", b)
	}
	if b.OrderCount != 3 || b.Side != 0 || b.LevelFlags != 4 {
		t.Errorf("annotations: %+v", b)
	}
}

func TestGoldenSnapshotEnd(t *testing.T) {
	body := header(t, goldenBytes(t, "snapshot-end-v3.bin"), msgTypeSnapshotEnd, 20)
	b, err := ParseSnapshotEnd(body)
	if err != nil {
		t.Fatalf("ParseSnapshotEnd: %v", err)
	}
	if b.InstrumentID != 1 || b.AnchorSeq != 918273645 || b.SnapshotID != 77 {
		t.Errorf("snapshot end: %+v", b)
	}
}
