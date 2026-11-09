package wire

import (
	"encoding/binary"
	"fmt"
)

// Frame types occupy one byte; frame contents use a bounded varint length.
const (
	FrameData          uint8 = 1
	FrameAck           uint8 = 2
	FrameOpen          uint8 = 3
	FrameOpenAck       uint8 = 4
	FrameClose         uint8 = 5
	FrameReset         uint8 = 6
	FrameCredit        uint8 = 7
	FramePing          uint8 = 8
	FrameReceipt       uint8 = 9
	FrameCancel        uint8 = 10
	FramePathChallenge uint8 = 11
	FramePathResponse  uint8 = 12
	FrameKeyUpdate     uint8 = 13
	FrameGoAway        uint8 = 14
	FrameAckRanges     uint8 = 15
	FrameMTUProbe      uint8 = 16
	FrameHalfClose     uint8 = 17
)

// Layout: TYPE(1) FLAGS(1) BODY_LENGTH(uvarint) BODY(variable).
// Specific frame BODY formats are specified in docs/ARCHITECTURE.md.
type Frame struct {
	Type  uint8
	Flags uint8
	Body  []byte
}

func AppendFrame(dst []byte, f Frame) ([]byte, error) {
	if f.Type == 0 || f.Type > FrameHalfClose || len(f.Body) > MaxFrameBody {
		return nil, ErrInvalid
	}
	dst = append(dst, f.Type, f.Flags)
	dst = binary.AppendUvarint(dst, uint64(len(f.Body)))
	dst = append(dst, f.Body...)
	return dst, nil
}

// ParseFrames enforces a frame count limit and rejects overlong/noncanonical
// variable integers to avoid multiple encodings for the same value.
func ParseFrames(p []byte, maxFrames int) ([]Frame, error) {
	if maxFrames < 1 || maxFrames > 1024 {
		return nil, ErrInvalid
	}
	out := make([]Frame, 0, min(maxFrames, 8))
	for len(p) > 0 {
		if len(out) >= maxFrames || len(p) < 3 {
			return nil, ErrInvalid
		}
		typ, flags := p[0], p[1]
		if typ == 0 || typ > FrameHalfClose {
			return nil, fmt.Errorf("%w: unknown frame type %d", ErrInvalid, typ)
		}
		p = p[2:]
		n, consumed := binary.Uvarint(p)
		if consumed <= 0 || n > MaxFrameBody {
			return nil, ErrInvalid
		}
		var canonical [10]byte
		canonicalN := binary.PutUvarint(canonical[:], n)
		if canonicalN != consumed {
			return nil, ErrInvalid
		}
		if uint64(len(p[consumed:])) < n {
			return nil, ErrShort
		}
		p = p[consumed:]
		out = append(out, Frame{Type: typ, Flags: flags, Body: p[:int(n)]})
		p = p[int(n):]
	}
	return out, nil
}

// AppendNumber and ReadNumber use Go's canonical unsigned base-128 varints.
func AppendNumber(dst []byte, n uint64) []byte { return binary.AppendUvarint(dst, n) }
func ReadNumber(p []byte) (uint64, int, error) {
	n, consumed := binary.Uvarint(p)
	if consumed <= 0 {
		return 0, 0, ErrInvalid
	}
	var canonical [10]byte
	if binary.PutUvarint(canonical[:], n) != consumed {
		return 0, 0, ErrInvalid
	}
	return n, consumed, nil
}
