package mira

import (
	"encoding/binary"
	"errors"
	"sort"
)

// ackRange represents an inclusive interval of authenticated packet numbers.
// The wire encoding is count(uvarint), then [lo(uvarint), hi(uvarint)] in
// descending order. The maximum 32 ranges and canonical integers bound work.
type ackRange struct{ lo, hi uint64 }

const (
	maxAckRanges  = 32
	maxAckNumbers = 512 // bound CPU work for ACKs from authenticated peers
)

func encodeAckRanges(numbers []uint64) []byte {
	if len(numbers) == 0 {
		return nil
	}
	sort.Slice(numbers, func(i, j int) bool { return numbers[i] > numbers[j] })
	return encodeSortedAckRanges(numbers)
}

// encodeSortedAckRanges encodes packet numbers already ordered from newest to
// oldest. Its fixed range buffer keeps the ACK flush path allocation-free up
// to the output bytes.
func encodeSortedAckRanges(numbers []uint64) []byte {
	if len(numbers) == 0 {
		return nil
	}
	var ranges [maxAckRanges]ackRange
	rangeCount := 0
	var emitted int
	for _, n := range numbers {
		if emitted >= maxAckNumbers {
			break
		}
		if rangeCount > 0 && n <= ranges[rangeCount-1].hi &&
			(n >= ranges[rangeCount-1].lo || (ranges[rangeCount-1].lo > 0 && n == ranges[rangeCount-1].lo-1)) {
			if n < ranges[rangeCount-1].lo {
				ranges[rangeCount-1].lo = n
				emitted++
			}
			continue
		}
		if rangeCount >= maxAckRanges {
			break
		}
		ranges[rangeCount] = ackRange{n, n}
		rangeCount++
		emitted++
	}
	b := binary.AppendUvarint(nil, uint64(rangeCount))
	for _, r := range ranges[:rangeCount] {
		b = binary.AppendUvarint(b, r.lo)
		b = binary.AppendUvarint(b, r.hi)
	}
	return b
}
func decodeAckRanges(body []byte) ([]ackRange, error) {
	var ranges [maxAckRanges]ackRange
	count, err := decodeAckRangesInto(body, &ranges)
	if err != nil {
		return nil, err
	}
	return append([]ackRange(nil), ranges[:count]...), nil
}

// decodeAckRangesInto validates an ACK frame into caller-owned fixed storage.
// The session receive path uses this form to avoid allocating per ACK packet.
func decodeAckRangesInto(body []byte, result *[maxAckRanges]ackRange) (int, error) {
	count, n := readAckNumber(body)
	if n == 0 || count == 0 || count > maxAckRanges {
		return 0, errors.New("mira: invalid ACK range count")
	}
	body = body[n:]
	var prevLo uint64
	var total uint64
	for i := 0; i < int(count); i++ {
		lo, k := readAckNumber(body)
		if k == 0 {
			return 0, errors.New("mira: malformed ACK start")
		}
		body = body[k:]
		hi, k := readAckNumber(body)
		if k == 0 {
			return 0, errors.New("mira: malformed ACK end")
		}
		body = body[k:]
		if lo > hi || hi-lo > 4096 || (i > 0 && (hi >= prevLo || prevLo-hi <= 1)) {
			return 0, errors.New("mira: invalid ACK ordering or overlap")
		}
		total += hi - lo + 1
		if total > maxAckNumbers {
			return 0, errors.New("mira: ACK packet count exceeds limit")
		}
		result[i] = ackRange{lo, hi}
		prevLo = lo
	}
	if len(body) > 0 {
		return 0, errors.New("mira: trailing ACK bytes")
	}
	return int(count), nil
}
func readAckNumber(body []byte) (uint64, int) {
	n, k, ok := readCanonicalUvarint(body)
	if !ok {
		return 0, 0
	}
	return n, k
}
