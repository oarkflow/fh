package wire

import (
	"bytes"
	"testing"
)

func TestHeaderRoundTrip(t *testing.T) {
	want := Header{Kind: KindData, Flags: 1, Connection: 991, Number: 9, Epoch: 2, Length: 18}
	buf := make([]byte, HeaderSize+int(want.Length))
	if err := want.MarshalTo(buf); err != nil {
		t.Fatal(err)
	}
	got, err := ParseHeader(buf)
	if err != nil || got != want {
		t.Fatalf("got %+v err %v", got, err)
	}
	if _, err := ParseHeader(buf[:len(buf)-1]); err == nil {
		t.Fatal("accepted truncated packet")
	}
}
func TestFramesRoundTrip(t *testing.T) {
	frames := []Frame{{Type: FramePing, Body: []byte("a")}, {Type: FrameData, Flags: 2, Body: bytes.Repeat([]byte{5}, 200)}}
	var b []byte
	var err error
	for _, f := range frames {
		b, err = AppendFrame(b, f)
		if err != nil {
			t.Fatal(err)
		}
	}
	got, err := ParseFrames(b, 4)
	if err != nil || len(got) != len(frames) {
		t.Fatalf("err=%v len=%d", err, len(got))
	}
	for i, f := range got {
		if f.Type != frames[i].Type || f.Flags != frames[i].Flags || !bytes.Equal(f.Body, frames[i].Body) {
			t.Fatal("mismatch")
		}
	}
	if _, err := ParseFrames(b, 1); err == nil {
		t.Fatal("accepted excessive frames")
	}
}
func FuzzParseFrames(f *testing.F) {
	f.Add([]byte{FramePing, 0, 0})
	f.Add([]byte{FrameData, 0, 128, 0})
	f.Fuzz(func(t *testing.T, b []byte) { _, _ = ParseFrames(b, 16) })
}
