package fh

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/oarkflow/fh/pkg/hpack"
)

func BenchmarkH2ValidateRequestFields(b *testing.B) {
	fields := []hpack.HeaderField{
		{Name: ":method", Value: "GET"},
		{Name: ":scheme", Value: "https"},
		{Name: ":authority", Value: "example.com"},
		{Name: ":path", Value: "/hello"},
		{Name: "accept", Value: "*/*"},
		{Name: "user-agent", Value: "bench"},
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s := &h2Stream{}
		if err := validateRequestFields(s, fields); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkH2ReadFrameSmallData(b *testing.B) {
	payload := []byte("hello")
	var head [9]byte
	head[2] = byte(len(payload))
	head[3] = h2Data
	binary.BigEndian.PutUint32(head[5:9], 1)
	frame := append(head[:], payload...)

	h := newTestH2Conn(&testing.T{})
	var r bytes.Reader
	h.r = &r
	// Prime the per-connection frame buffer. Connection construction and its
	// one-time buffers are deliberately outside this frame parsing benchmark.
	r.Reset(frame)
	if _, err := h.readFrame(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Reset(frame)
		if _, err := h.readFrame(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkH2HandleWindowUpdate(b *testing.B) {
	h := newTestH2Conn(&testing.T{})
	var p [4]byte
	binary.BigEndian.PutUint32(p[:], 1)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := h.handleWindowUpdate(h2Frame{typ: h2WindowUpdate, streamID: 0, payload: p[:]}); err != nil {
			b.Fatal(err)
		}
	}
}
