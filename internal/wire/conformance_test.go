package wire

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "conformance", name))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := hex.DecodeString(strings.TrimSpace(string(b)))
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func TestConformanceFixtures(t *testing.T) {
	h := Header{Kind: KindHello, Flags: 0, Connection: 0x0102030405060708, Number: 0, Epoch: 0, Length: 80}
	raw, err := HeaderBytes(h)
	if err != nil {
		t.Fatal(err)
	}
	fixture := readFixture(t, "hello_header_v4.hex")
	if !bytes.Equal(raw[:], fixture) {
		t.Fatalf("header bytes do not match v4 fixture: %x vs %x", raw[:], fixture)
	}
	// A complete synthetic pre-auth packet has a matching body; this only tests
	// framing, not a valid authenticated handshake.
	full := append(raw[:], make([]byte, 80)...)
	parsed, err := ParseHeader(full)
	if err != nil || parsed != h {
		t.Fatalf("header round-trip: got %+v, err %v", parsed, err)
	}
	f := Frame{Type: FrameAckRanges, Body: []byte{1, 2, 5}}
	encoded, err := AppendFrame(nil, f)
	if err != nil {
		t.Fatal(err)
	}
	expected := readFixture(t, "ack_ranges_frame_v4.hex")
	if !bytes.Equal(encoded, expected) {
		t.Fatalf("frame bytes do not match fixture: %x vs %x", encoded, expected)
	}
	decoded, err := ParseFrames(encoded, 2)
	if err != nil || len(decoded) != 1 || decoded[0].Type != FrameAckRanges || !bytes.Equal(decoded[0].Body, f.Body) {
		t.Fatalf("frame round-trip: %v %v", decoded, err)
	}
}
