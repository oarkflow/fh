package decompress

import (
	"bytes"
	"compress/gzip"
	"errors"
	"testing"
)

func gzipBody(t *testing.T, body []byte) []byte {
	t.Helper()
	var dst bytes.Buffer
	w := gzip.NewWriter(&dst)
	if _, err := w.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return dst.Bytes()
}

func TestDecodeGzip(t *testing.T) {
	want := []byte(`{"message":"hello"}`)
	compressed := gzipBody(t, want)
	got, err := DecodeGzip(compressed, 1024, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got=%q", got)
	}
}

func TestDecodeGzipLimitsExpansion(t *testing.T) {
	compressed := gzipBody(t, bytes.Repeat([]byte("a"), 4096))
	_, err := DecodeGzip(compressed, 8192, 2)
	if !errors.Is(err, ErrExpansionRatio) {
		t.Fatalf("err=%v", err)
	}
}
