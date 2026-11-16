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
		if len(out) >= maxFrames {
			return nil, ErrInvalid
		}
		frame, consumed, err := parseFrame(p)
		if err != nil {
			return nil, err
		}
		out = append(out, frame)
		p = p[consumed:]
	}
	return out, nil
}

// ParseSingleFrame decodes one frame without allocating a result slice. It is
// intended for packet paths whose framing policy permits exactly one frame.
func ParseSingleFrame(p []byte) (Frame, error) {
	frame, consumed, err := parseFrame(p)
	if err != nil {
		return Frame{}, err
	}
	if consumed != len(p) {
		return Frame{}, ErrInvalid
	}
	return frame, nil
}

func parseFrame(p []byte) (Frame, int, error) {
	if len(p) < 3 {
		return Frame{}, 0, ErrInvalid
	}
	typ, flags := p[0], p[1]
	if typ == 0 || typ > FrameHalfClose {
		return Frame{}, 0, fmt.Errorf("%w: unknown frame type %d", ErrInvalid, typ)
	}
	lengthBytes := p[2:]
	n, consumed := binary.Uvarint(lengthBytes)
	if consumed <= 0 || n > MaxFrameBody {
		return Frame{}, 0, ErrInvalid
	}
	var canonical [10]byte
	if binary.PutUvarint(canonical[:], n) != consumed {
		return Frame{}, 0, ErrInvalid
	}
	if uint64(len(lengthBytes[consumed:])) < n {
		return Frame{}, 0, ErrShort
	}
	start := 2 + consumed
	end := start + int(n)
	return Frame{Type: typ, Flags: flags, Body: p[start:end]}, end, nil
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
