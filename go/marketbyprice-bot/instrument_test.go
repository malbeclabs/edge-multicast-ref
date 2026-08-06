package main

import (
	"errors"
	"testing"
)

func ready(t *testing.T) *Instrument {
	t.Helper()
	i := NewInstrument(7, "BTC-USDT", -2, -8)
	i.Status = StatusReady
	return i
}

// Quantity is absolute, not a delta, and 0 removes the level.
func TestApplyLevelUpdate_AbsoluteAndDelete(t *testing.T) {
	i := ready(t)
	i.ApplyLevelUpdate(0, 1000, 50, 3, 0, 1)
	if got := i.Bids[1000].QtyRaw; got != 50 {
		t.Fatalf("qty: got %d want 50", got)
	}
	// Absolute: 75 replaces 50, it does not add to it.
	i.ApplyLevelUpdate(0, 1000, 75, 3, 0, 2)
	if got := i.Bids[1000].QtyRaw; got != 75 {
		t.Fatalf("absolute apply: got %d want 75", got)
	}
	i.ApplyLevelUpdate(0, 1000, 0, 0, 0, 3)
	if _, present := i.Bids[1000]; present {
		t.Fatal("qty 0 must remove the level")
	}
}

// Action must never gate the apply: a wrong Action byte cannot corrupt a book.
func TestApplyLevelUpdate_ActionDoesNotGate(t *testing.T) {
	i := ready(t)
	// Action=Delete but non-zero qty: the level must still be set to 90.
	div := i.ApplyLevelUpdate(1, 2000, 90, 1, 0, 3)
	if i.Asks[2000] == nil || i.Asks[2000].QtyRaw != 90 {
		t.Fatalf("level must be set despite Action=Delete: %+v", i.Asks[2000])
	}
	if len(div) != 1 || div[0] != DivergenceDeleteNonzeroQty {
		t.Fatalf("expected delete_nonzero_qty divergence, got %v", div)
	}
}

func TestApplyLevelUpdate_DivergenceCounters(t *testing.T) {
	i := ready(t)
	i.ApplyLevelUpdate(0, 1000, 50, 1, 0, 1)

	if div := i.ApplyLevelUpdate(0, 1000, 60, 1, 0, 1); len(div) != 1 || div[0] != DivergenceNewOnPresent {
		t.Errorf("New on present level: got %v", div)
	}
	if div := i.ApplyLevelUpdate(0, 9999, 60, 1, 0, 2); len(div) != 1 || div[0] != DivergenceChangeOnAbsent {
		t.Errorf("Change on absent level: got %v", div)
	}
	if div := i.ApplyLevelUpdate(0, 1000, 0, 0, 0, 2); len(div) != 1 || div[0] != DivergenceZeroQtyBadAction {
		t.Errorf("qty 0 with Action != Delete: got %v", div)
	}
	// A correct New on an absent level diverges not at all.
	if div := i.ApplyLevelUpdate(0, 1234, 10, 1, 0, 1); len(div) != 0 {
		t.Errorf("clean apply must not diverge: got %v", div)
	}
}

// The four divergence conditions are not mutually exclusive, and the spec asks a
// subscriber to surface each one. A single message can violate two at once, so
// the classification must report both rather than stopping at the first match.
func TestApplyLevelUpdate_OverlappingDivergencesBothReported(t *testing.T) {
	has := func(div []DivergenceKind, want DivergenceKind) bool {
		for _, d := range div {
			if d == want {
				return true
			}
		}
		return false
	}

	// Quantity=0 with Action=New on a level that IS present: violates both the
	// "zero quantity requires Action=Delete" rule and "New on a present level".
	i := ready(t)
	i.ApplyLevelUpdate(0, 1000, 50, 1, 0, 1)
	div := i.ApplyLevelUpdate(0, 1000, 0, 0, 0, 1)
	if len(div) != 2 {
		t.Fatalf("expected 2 divergences, got %d: %v", len(div), div)
	}
	if !has(div, DivergenceZeroQtyBadAction) || !has(div, DivergenceNewOnPresent) {
		t.Errorf("expected both zero_qty_wrong_action and new_on_present: %v", div)
	}
	// The apply itself is still correct: quantity 0 removed the level.
	if _, present := i.Bids[1000]; present {
		t.Error("qty 0 must still remove the level regardless of divergences")
	}

	// Quantity=0 with Action=Change on a level that is ABSENT: violates both the
	// zero-quantity rule and "Change on an absent level".
	j := ready(t)
	div = j.ApplyLevelUpdate(1, 2000, 0, 0, 0, 2)
	if len(div) != 2 {
		t.Fatalf("expected 2 divergences, got %d: %v", len(div), div)
	}
	if !has(div, DivergenceZeroQtyBadAction) || !has(div, DivergenceChangeOnAbsent) {
		t.Errorf("expected both zero_qty_wrong_action and change_on_absent: %v", div)
	}
}

func TestApplyBookClear_EntireSide(t *testing.T) {
	i := ready(t)
	i.ApplyLevelUpdate(0, 1000, 5, 1, 0, 1)
	i.ApplyLevelUpdate(0, 900, 5, 1, 0, 1)
	i.ApplyLevelUpdate(1, 1100, 5, 1, 0, 1)
	if err := i.ApplyBookClear(0, 0, 0); err != nil {
		t.Fatal(err)
	}
	if len(i.Bids) != 0 {
		t.Errorf("bids should be empty: %v", i.Bids)
	}
	if len(i.Asks) != 1 {
		t.Errorf("asks must be untouched: %v", i.Asks)
	}
}

// Scope=1 on bids clears at or BELOW the bound; on asks at or ABOVE it.
func TestApplyBookClear_FromPriceOutward(t *testing.T) {
	i := ready(t)
	for _, p := range []int64{800, 900, 1000, 1100} {
		i.ApplyLevelUpdate(0, p, 5, 1, 0, 1)
		i.ApplyLevelUpdate(1, p, 5, 1, 0, 1)
	}
	if err := i.ApplyBookClear(0, 1, 900); err != nil {
		t.Fatal(err)
	}
	if _, gone := i.Bids[800]; gone {
		t.Error("bid 800 is below the bound and must be cleared")
	}
	if _, gone := i.Bids[900]; gone {
		t.Error("bound is inclusive; 900 must be cleared")
	}
	if i.Bids[1000] == nil || i.Bids[1100] == nil {
		t.Error("bids above the bound must survive")
	}
	if err := i.ApplyBookClear(1, 1, 1000); err != nil {
		t.Fatal(err)
	}
	if i.Asks[800] == nil || i.Asks[900] == nil {
		t.Error("asks below the bound must survive")
	}
	if _, gone := i.Asks[1000]; gone {
		t.Error("bound is inclusive; ask 1000 must be cleared")
	}
	if _, gone := i.Asks[1100]; gone {
		t.Error("ask 1100 is above the bound and must be cleared")
	}
}

func TestApplyBookClear_ScopeBothSidesMalformed(t *testing.T) {
	i := ready(t)
	i.ApplyLevelUpdate(0, 1000, 5, 1, 0, 1)
	if err := i.ApplyBookClear(2, 1, 1000); !errors.Is(err, errBookClearScopeSide) {
		t.Fatalf("expected errBookClearScopeSide, got %v", err)
	}
	if i.Bids[1000] == nil {
		t.Error("a malformed BookClear must not mutate the book")
	}
}

// A short snapshot must NOT evict a live, correct book. This is the deliberate
// deviation from the spec's literal cold-start step 6.
func TestEndSnapshot_ShortDoesNotDemoteReadyBook(t *testing.T) {
	i := ready(t)
	i.ApplyLevelUpdate(0, 1000, 50, 1, 0, 1)
	i.LastAppliedInstrumentSeq = 42

	i.BeginSnapshot(9, 5000, 2 /*total*/, 60, 0)
	if i.Status != StatusReady {
		t.Fatal("BeginSnapshot must not change Status")
	}
	i.AddSnapshotLevel(9, 0, 1111, 7, 1, 0) // only 1 of 2
	err := i.EndSnapshot(9, 5000)
	if !errors.Is(err, errSnapshotShort) {
		t.Fatalf("expected errSnapshotShort, got %v", err)
	}
	if i.Status != StatusReady {
		t.Errorf("status must stay ready, got %v", i.Status)
	}
	if i.Bids[1000] == nil || i.Bids[1000].QtyRaw != 50 {
		t.Error("live book must survive a short snapshot")
	}
	if i.LastAppliedInstrumentSeq != 42 {
		t.Errorf("trackers must survive, got %d", i.LastAppliedInstrumentSeq)
	}
	if i.OpenSnapshot != nil {
		t.Error("shadow must be discarded")
	}
}

func TestEndSnapshot_CommitsAndSetsDepthBound(t *testing.T) {
	i := NewInstrument(7, "X", 0, 0)
	i.BeginSnapshot(3, 5000, 2, 77, 25)
	i.AddSnapshotLevel(3, 0, 1000, 10, 2, 0)
	i.AddSnapshotLevel(3, 1, 1100, 20, 4, 0)
	if err := i.EndSnapshot(3, 5000); err != nil {
		t.Fatal(err)
	}
	if i.Status != StatusReady {
		t.Errorf("status: %v", i.Status)
	}
	if i.LastAppliedMktdataSeq != 5000 || i.LastAppliedInstrumentSeq != 77 {
		t.Errorf("trackers: %d %d", i.LastAppliedMktdataSeq, i.LastAppliedInstrumentSeq)
	}
	if i.DepthBound == nil || *i.DepthBound != 25 {
		t.Errorf("depth bound: %v", i.DepthBound)
	}
}

// A committed snapshot must free Pending. The entries are keyed to the
// pre-snapshot per-instrument sequence and EndSnapshot jumps
// LastAppliedInstrumentSeq forward to the snapshot's Last Instrument Seq, so
// nothing left behind can ever match LastApplied+1 and drain. Left in place they
// still count toward the reorder-window bound in applyDeltaToReady, so ordinary
// reordering is eventually misread as a per-instrument gap.
func TestEndSnapshot_ClearsPending(t *testing.T) {
	i := NewInstrument(7, "X", 0, 0)
	i.LastAppliedInstrumentSeq = 4
	i.Pending = map[uint32]Record{
		6: {Type: "level_update"},
		7: {Type: "level_update"},
	}

	i.BeginSnapshot(3, 5000, 1, 77, 0)
	i.AddSnapshotLevel(3, 0, 1000, 10, 2, 0)
	if err := i.EndSnapshot(3, 5000); err != nil {
		t.Fatal(err)
	}

	if i.Pending != nil {
		t.Errorf("Pending must be cleared on commit, still holds %d entries: %+v", len(i.Pending), i.Pending)
	}
}

// Depth bound defaults to unknown, never 0. A never-snapshotted instrument must
// not assert completeness.
func TestDepthBound_DefaultsUnknown(t *testing.T) {
	i := NewInstrument(1, "X", 0, 0)
	if i.DepthBound != nil {
		t.Fatalf("depth bound must start nil (unknown), got %v", *i.DepthBound)
	}
	i.BeginSnapshot(1, 1, 0, 0, 0)
	if err := i.EndSnapshot(1, 1); err != nil {
		t.Fatal(err)
	}
	if i.DepthBound == nil || *i.DepthBound != 0 {
		t.Fatal("after a complete snapshot the bound is a positive claim of 0")
	}
	i.Reset(nil)
	if i.DepthBound != nil {
		t.Fatal("reset must return the bound to unknown, not 0")
	}
}

// The discriminator is Last Instrument Seq, not Anchor Seq.
func TestSnapshotAcceptable_ReadyDiscriminator(t *testing.T) {
	i := ready(t)
	i.LastAppliedInstrumentSeq = 100
	i.LastAppliedMktdataSeq = 500

	// Behind: snapshot captured after deltas we never applied.
	if ok, err := i.SnapshotAcceptable(600, 101); err != nil || !ok {
		t.Errorf("K > tracker must re-bootstrap: ok=%v err=%v", ok, err)
	}
	// Current: ordinary case, ignore.
	if ok, _ := i.SnapshotAcceptable(600, 100); ok {
		t.Error("K == tracker must be ignored")
	}
	if ok, _ := i.SnapshotAcceptable(600, 99); ok {
		t.Error("K < tracker must be ignored")
	}
	// A far-advanced Anchor Seq alone must NOT trigger a rebuild — this is the
	// trap that would rebuild every book on every rotation.
	if ok, _ := i.SnapshotAcceptable(999999, 100); ok {
		t.Error("anchor seq must not drive the decision")
	}
	// Not ready: always acceptable.
	i.Status = StatusGap
	if ok, _ := i.SnapshotAcceptable(1, 1); !ok {
		t.Error("a gap instrument must accept any snapshot")
	}
}

// A snapshot captured before an InstrumentReset but delivered after it must be
// discarded, or the instrument ends ready holding the diverged book the reset
// existed to discard — with no gap and no counter.
func TestRequiredAnchor_DiscardsStaleSnapshot(t *testing.T) {
	i := ready(t)
	anchor := uint64(9000)
	i.Reset(&anchor)

	if ok, err := i.SnapshotAcceptable(8999, 1); ok || !errors.Is(err, errStaleAnchor) {
		t.Fatalf("older anchor must be rejected: ok=%v err=%v", ok, err)
	}
	if ok, err := i.SnapshotAcceptable(9000, 1); !ok || err != nil {
		t.Fatalf("exact anchor must be accepted: ok=%v err=%v", ok, err)
	}
	// Cleared by ANY snapshot at or after S', not only an exact match — the
	// mandated snapshot at S' can itself be lost.
	i.BeginSnapshot(1, 9500, 0, 0, 0)
	if err := i.EndSnapshot(1, 9500); err != nil {
		t.Fatal(err)
	}
	if i.RequiredAnchorSeq != nil {
		t.Error("a newer accepted snapshot must clear the required anchor")
	}
}

func TestCrossed(t *testing.T) {
	i := ready(t)
	if i.Crossed() {
		t.Error("an empty book is not crossed")
	}
	i.ApplyLevelUpdate(0, 1000, 5, 1, 0, 1)
	if i.Crossed() {
		t.Error("one-sided book is not crossed")
	}
	i.ApplyLevelUpdate(1, 1100, 5, 1, 0, 1)
	if i.Crossed() {
		t.Error("bid 1000 < ask 1100 is not crossed")
	}
	// Locked book: routine on some venues, must not count as crossed.
	i.ApplyLevelUpdate(1, 1000, 5, 1, 0, 1)
	if i.Crossed() {
		t.Error("locked book (bid == ask) must not count as crossed")
	}
	i.ApplyLevelUpdate(0, 1200, 5, 1, 0, 1)
	if !i.Crossed() {
		t.Error("bid 1200 > ask 1000 is crossed")
	}
}
