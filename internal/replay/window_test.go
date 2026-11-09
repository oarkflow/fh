package replay

import "testing"

func TestWindow(t *testing.T) {
	var w Window
	for _, n := range []uint64{100, 99, 102, 101, 1126} {
		if !w.CheckAndMark(n) {
			t.Fatalf("failed to accept %d", n)
		}
		if w.CheckAndMark(n) {
			t.Fatalf("accepted replay %d", n)
		}
	}
	if w.CheckAndMark(100) {
		t.Fatal("accepted too-old packet")
	}
	if !w.CheckAndMark(1125) {
		t.Fatal("unexpected rejection")
	}
}
func TestShiftCrossWord(t *testing.T) {
	var w Window
	for _, n := range []uint64{1, 64, 127, 129, 128, 65, 2} {
		if !w.CheckAndMark(n) {
			t.Fatalf("false replay for %d", n)
		}
	}
	for _, n := range []uint64{1, 64, 127, 129, 128, 65, 2} {
		if w.CheckAndMark(n) {
			t.Fatalf("did not replay-reject %d", n)
		}
	}
}
