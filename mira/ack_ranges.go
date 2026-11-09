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
	ranges := make([]ackRange, 0, maxAckRanges)
	var emitted int
	for _, n := range numbers {
		if emitted >= maxAckNumbers {
			break
		}
		if len(ranges) > 0 && n >= ranges[len(ranges)-1].lo-1 && n <= ranges[len(ranges)-1].hi {
			if n < ranges[len(ranges)-1].lo {
				ranges[len(ranges)-1].lo = n
				emitted++
			}
			continue
		}
		if len(ranges) >= maxAckRanges {
			break
		}
		ranges = append(ranges, ackRange{n, n})
		emitted++
	}
	b := binary.AppendUvarint(nil, uint64(len(ranges)))
	for _, r := range ranges {
		b = binary.AppendUvarint(b, r.lo)
		b = binary.AppendUvarint(b, r.hi)
	}
	return b
}
func decodeAckRanges(body []byte) ([]ackRange, error) {
	count, n := readAckNumber(body)
	if n == 0 || count == 0 || count > maxAckRanges {
		return nil, errors.New("mira: invalid ACK range count")
	}
	body = body[n:]
	result := make([]ackRange, 0, int(count))
	var prevLo uint64
	var total uint64
	for i := 0; i < int(count); i++ {
		lo, k := readAckNumber(body)
		if k == 0 {
			return nil, errors.New("mira: malformed ACK start")
		}
		body = body[k:]
		hi, k := readAckNumber(body)
		if k == 0 {
			return nil, errors.New("mira: malformed ACK end")
		}
		body = body[k:]
		if lo > hi || hi-lo > 4096 || (i > 0 && (hi >= prevLo || prevLo-hi <= 1)) {
			return nil, errors.New("mira: invalid ACK ordering or overlap")
		}
		total += hi - lo + 1
		if total > maxAckNumbers {
			return nil, errors.New("mira: ACK packet count exceeds limit")
		}
		result = append(result, ackRange{lo, hi})
		prevLo = lo
	}
	if len(body) > 0 {
		return nil, errors.New("mira: trailing ACK bytes")
	}
	return result, nil
}
func readAckNumber(body []byte) (uint64, int) {
	n, k := binary.Uvarint(body)
	if k <= 0 {
		return 0, 0
	}
	var b [10]byte
	if binary.PutUvarint(b[:], n) != k {
		return 0, 0
	}
	return n, k
}
