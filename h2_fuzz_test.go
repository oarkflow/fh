package fh

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/oarkflow/fh/pkg/hpack"
)

func FuzzH2HeaderFragment(f *testing.F) {
	f.Add(uint8(0), uint32(1), []byte("abc"))
	f.Add(h2FlagPadded, uint32(1), []byte{1, 'a', 'x'})
	f.Add(h2FlagPriority, uint32(1), []byte{0, 0, 0, 2, 16, 'a'})

	f.Fuzz(func(t *testing.T, flags uint8, streamID uint32, payload []byte) {
		streamID &= 0x7fffffff
		if streamID == 0 {
			streamID = 1
		}
		_, _ = headerFragment(h2Frame{flags: flags, streamID: streamID, payload: payload})
	})
}

func FuzzH2ReadFrame(f *testing.F) {
	var seed bytes.Buffer
	var head [9]byte
	head[2] = 1
	head[3] = h2Data
	binary.BigEndian.PutUint32(head[5:9], 1)
	seed.Write(head[:])
	seed.WriteByte('x')
	f.Add(seed.Bytes())
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, h2Settings, 0, 0, 0, 0, 0})

	f.Fuzz(func(t *testing.T, raw []byte) {
		h := newTestH2Conn(t)
		h.r = bytes.NewReader(raw)
		_, _ = h.readFrame()
	})
}

func FuzzH2ValidateRequestFields(f *testing.F) {
	f.Add("GET", "https", "example.com", "/", "x-test", "ok")
	f.Add("POST", "https", "example.com", "/submit", "content-length", "10")
	f.Add("GET", "http", "example.com", "bad", "accept", "*/*")

	f.Fuzz(func(t *testing.T, method, scheme, authority, path, name, value string) {
		s := &h2Stream{}
		fields := []hpack.HeaderField{
			{Name: ":method", Value: method},
			{Name: ":scheme", Value: scheme},
			{Name: ":authority", Value: authority},
			{Name: ":path", Value: path},
			{Name: name, Value: value},
		}
		_ = validateRequestFields(s, fields)
	})
}

func FuzzH2ValidateRequestTrailers(f *testing.F) {
	f.Add("x-checksum", "abc")
	f.Add("content-length", "1")
	f.Add(":path", "/")

	f.Fuzz(func(t *testing.T, name, value string) {
		_, _ = validateRequestTrailers([]hpack.HeaderField{{Name: name, Value: value}})
	})
}
