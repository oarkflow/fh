package securetransport

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestStreamRecordsRoundTripAndTruncation(t *testing.T) {
	var key [32]byte
	key[0] = 7
	meta := StreamMetadata{Sequence: 4, IssuedAt: 100, ExpiresAt: 200}
	meta.SessionID[0], meta.RequestID[0], meta.Nonce[0] = 1, 2, 3
	binding := StreamBinding{Method: "POST", Target: "/upload?part=1"}
	decoded, err := DecodeStreamMetadata(meta.Encode())
	if err != nil || decoded != meta {
		t.Fatalf("metadata round trip: decoded=%+v err=%v", decoded, err)
	}
	body := bytes.Repeat([]byte("x"), StreamChunkSize+19)
	var wire bytes.Buffer
	encoder, err := NewStreamEncoder(&wire, key, meta, binding, Limits{MaxBody: len(body)})
	if err != nil {
		t.Fatal(err)
	}
	if err := encoder.WriteHead([]byte("headers")); err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.Write(body); err != nil {
		t.Fatal(err)
	}
	withoutFinal := append([]byte(nil), wire.Bytes()...)
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	decode := func(data []byte, bind StreamBinding) ([]byte, error) {
		t.Helper()
		decoder, err := NewStreamDecoder(bytes.NewReader(data), key, meta, bind, Limits{MaxBody: len(body)})
		if err != nil {
			return nil, err
		}
		head, err := decoder.ReadHead()
		if err != nil || string(head) != "headers" {
			return nil, errors.Join(ErrMalformed, err)
		}
		return io.ReadAll(decoder)
	}
	got, err := decode(wire.Bytes(), binding)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("stream round trip: body=%d bytes err=%v", len(got), err)
	}
	if _, err := decode(withoutFinal, binding); err == nil {
		t.Fatal("truncated stream was accepted")
	}
	if _, err := decode(wire.Bytes(), StreamBinding{Method: "POST", Target: "/other"}); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("cross-target stream: %v", err)
	}
	tampered := append([]byte(nil), wire.Bytes()...)
	tampered[len(tampered)-1] ^= 1
	if _, err := decode(tampered, binding); err == nil {
		t.Fatal("tampered final record was accepted")
	}
}

func BenchmarkSecureStreamEncode1MiB(b *testing.B) {
	var key [32]byte
	meta := StreamMetadata{Sequence: 1}
	binding := StreamBinding{Method: "POST", Target: "/upload"}
	payload := bytes.Repeat([]byte("x"), 1<<20)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	var out bytes.Buffer
	for i := 0; i < b.N; i++ {
		out.Reset()
		encoder, err := NewStreamEncoder(&out, key, meta, binding, Limits{MaxBody: len(payload)})
		if err != nil {
			b.Fatal(err)
		}
		if err := encoder.WriteHead([]byte("head")); err != nil {
			b.Fatal(err)
		}
		if _, err := encoder.Write(payload); err != nil {
			b.Fatal(err)
		}
		if err := encoder.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
