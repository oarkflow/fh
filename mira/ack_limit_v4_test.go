package mira

import (
	"encoding/binary"
	"testing"
)

// Authenticated peers must not be able to make the ACK handler walk unbounded
// packet numbers, even when the range count itself is within its limit.
func TestAckRangesAggregateWorkCap(t *testing.T) {
	b := binary.AppendUvarint(nil, 1)
	b = binary.AppendUvarint(b, 4)
	b = binary.AppendUvarint(b, 4+maxAckNumbers)
	if _, err := decodeAckRanges(b); err == nil {
		t.Fatal("accepted oversized ACK work")
	}
	b = binary.AppendUvarint(nil, 1)
	b = binary.AppendUvarint(b, 4)
	b = binary.AppendUvarint(b, 4+maxAckNumbers-1)
	got, err := decodeAckRanges(b)
	if err != nil || len(got) != 1 {
		t.Fatalf("valid boundary rejected: %v, %+v", err, got)
	}
}
