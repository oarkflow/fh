package wire

import (
	"bytes"
	"testing"
)

func FuzzCanonicalFramesRoundTrip(f *testing.F) {
	seeds := [][]byte{{}, {1, 0, 0}, {1, 0, 1, 0x52}, {2, 0, 1, 1}, {0xff, 0, 0}, {1, 0, 0x80, 0x00}}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaxDatagram {
			return
		}
		frames, err := ParseFrames(data, 64)
		if err != nil {
			return
		}
		var rebuilt []byte
		for _, fr := range frames {
			rebuilt, err = AppendFrame(rebuilt, fr)
			if err != nil {
				t.Fatalf("parsed frame could not reencode: %v", err)
			}
		}
		if !bytes.Equal(rebuilt, data) {
			t.Fatalf("parse/encode mismatch")
		}
	})
}
func FuzzParseHeader(f *testing.F) {
	buf := make([]byte, HeaderSize)
	_ = (Header{Kind: KindHello, Connection: 123, Number: 1, Length: 0}).MarshalTo(buf)
	f.Add(buf)
	f.Add([]byte("MT"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaxDatagram+100 {
			return
		}
		h, err := ParseHeader(data)
		if err != nil {
			return
		}
		var b [HeaderSize]byte
		if err = h.MarshalTo(b[:]); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b[:], data[:HeaderSize]) {
			t.Fatal("header mismatch")
		}
	})
}

func BenchmarkFrameEncoding(b *testing.B) {
	data := bytes.Repeat([]byte{0x31}, 800)
	f := Frame{Type: FrameData, Body: data}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf, err := AppendFrame(make([]byte, 0, 1024), f)
		if err != nil {
			b.Fatal(err)
		}
		if _, err = ParseFrames(buf, 1); err != nil {
			b.Fatal(err)
		}
	}
}
