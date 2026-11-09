// Package wire contains dependency-free, bounded MIRA-T/1 binary codecs.
// The packet header is authenticated as AEAD associated data for encrypted packets.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	Magic        uint16 = 0x4D54 // ASCII "MT"
	Version      uint8  = 4
	HeaderSize          = 26
	AuthTagSize         = 16
	MaxDatagram         = 1472
	MaxFrameBody        = 1 << 20
)

// Packet kinds: pre-auth messages are different from authenticated data.
const (
	KindHello    uint8 = 1
	KindRetry    uint8 = 2
	KindHelloAck uint8 = 3
	KindFinish   uint8 = 4
	KindData     uint8 = 5
	KindReset    uint8 = 6
)

var (
	ErrShort   = errors.New("wire: truncated packet")
	ErrInvalid = errors.New("wire: invalid packet")
)

// Header is the fixed 26-byte packet header. Length is ciphertext length
// (including AEAD tag) for KindData; handshake lengths include all body bytes.
// Header fields are in network byte order. Packet numbers are never reused
// within a connection, direction, and key epoch.
type Header struct {
	Kind       uint8
	Flags      uint8
	Connection uint64
	Number     uint64
	Epoch      uint16
	Length     uint16
}

func (h Header) MarshalTo(b []byte) error {
	if len(b) < HeaderSize {
		return ErrShort
	}
	if h.Kind < KindHello || h.Kind > KindReset {
		return fmt.Errorf("%w: kind", ErrInvalid)
	}
	binary.BigEndian.PutUint16(b[0:2], Magic)
	b[2] = Version
	b[3] = h.Kind
	b[4] = h.Flags
	b[5] = HeaderSize
	binary.BigEndian.PutUint64(b[6:14], h.Connection)
	binary.BigEndian.PutUint64(b[14:22], h.Number)
	binary.BigEndian.PutUint16(b[22:24], h.Epoch)
	binary.BigEndian.PutUint16(b[24:26], h.Length)
	return nil
}

func ParseHeader(b []byte) (Header, error) {
	if len(b) < HeaderSize {
		return Header{}, ErrShort
	}
	if binary.BigEndian.Uint16(b[0:2]) != Magic || b[2] != Version || int(b[5]) != HeaderSize {
		return Header{}, ErrInvalid
	}
	h := Header{
		Kind: b[3], Flags: b[4],
		Connection: binary.BigEndian.Uint64(b[6:14]),
		Number:     binary.BigEndian.Uint64(b[14:22]),
		Epoch:      binary.BigEndian.Uint16(b[22:24]),
		Length:     binary.BigEndian.Uint16(b[24:26]),
	}
	if h.Kind < KindHello || h.Kind > KindReset || len(b) != HeaderSize+int(h.Length) || len(b) > MaxDatagram {
		return Header{}, ErrInvalid
	}
	if h.Kind == KindData && h.Length < AuthTagSize {
		return Header{}, ErrInvalid
	}
	return h, nil
}

// HeaderBytes is the exact AEAD AAD, including the encrypted body length.
func HeaderBytes(h Header) ([HeaderSize]byte, error) {
	var b [HeaderSize]byte
	err := h.MarshalTo(b[:])
	return b, err
}
