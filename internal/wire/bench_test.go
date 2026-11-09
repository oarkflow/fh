package wire

import (
	"testing"
)

func BenchmarkEncodeFrame(b *testing.B) {
	data := make([]byte, 896)
	f := Frame{Type: FrameData, Body: data}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = AppendFrame(make([]byte, 0, 1024), f)
	}
}
func BenchmarkParseFrame(b *testing.B) {
	raw, _ := AppendFrame(nil, Frame{Type: FrameData, Body: make([]byte, 896)})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = ParseFrames(raw, 1)
	}
}
