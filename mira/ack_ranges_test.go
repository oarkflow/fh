package mira

import (
	"bytes"
	"testing"
)

func TestAckRanges(t *testing.T) {
	nums := []uint64{9, 8, 7, 2, 5, 18, 17, 17}
	b := encodeAckRanges(nums)
	got, e := decodeAckRanges(b)
	if e != nil {
		t.Fatal(e)
	}
	expected := []ackRange{{17, 18}, {7, 9}, {5, 5}, {2, 2}}
	if len(got) != len(expected) {
		t.Fatalf("got %+v", got)
	}
	for i := range got {
		if got[i] != expected[i] {
			t.Fatalf("%v != %v", got, expected)
		}
	}
	for _, bad := range [][]byte{{0}, {0x81, 0}, {1, 10, 9}, {2, 2, 4, 3, 5}, {1, 2, 4, 0}} {
		if _, e := decodeAckRanges(bad); e == nil {
			t.Fatalf("accepted malformed %x", bad)
		}
	}
	if !bytes.Equal(b, encodeAckRanges(nums)) {
		t.Fatal("not deterministic")
	}
}
func FuzzAckRangeDecoder(f *testing.F) {
	f.Add([]byte{1, 2, 4})
	f.Add([]byte{255, 0})
	f.Fuzz(func(t *testing.T, b []byte) { _, _ = decodeAckRanges(b) })
}
