package fh

import (
	"bytes"
	"compress/gzip"
	"io"
	"mime"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
)

func TestPreCompressedStaticFilesStreamWithLength(t *testing.T) {
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	_, _ = gz.Write(bytes.Repeat([]byte("asset"), 4096))
	_ = gz.Close()

	app := New()
	app.StaticFS("/assets", fstest.MapFS{
		"app.js":    &fstest.MapFile{Data: bytes.Repeat([]byte("original"), 4096)},
		"app.js.gz": &fstest.MapFile{Data: compressed.Bytes()},
	}, StaticConfig{PreCompressed: true})
	req := httptest.NewRequest("GET", "/assets/app.js", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("Content-Encoding = %q", resp.Header.Get("Content-Encoding"))
	}
	if resp.ContentLength != int64(compressed.Len()) {
		t.Fatalf("Content-Length = %d, want %d", resp.ContentLength, compressed.Len())
	}
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatal(err)
	}
}

func TestPreCompressedStaticFilesHonorQualityWeights(t *testing.T) {
	tests := []struct {
		name     string
		accept   string
		encoding string
		body     string
	}{
		{name: "gzip wins quality", accept: "br;q=0.2, gzip;q=0.8", encoding: "gzip", body: "gzip"},
		{name: "explicit zero", accept: "br, gzip;q=0", encoding: "br", body: "brotli"},
		{name: "wildcard", accept: "*;q=0.5", encoding: "br", body: "brotli"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := New()
			app.StaticFS("/assets", fstest.MapFS{
				"app.js":    &fstest.MapFile{Data: []byte("original")},
				"app.js.br": &fstest.MapFile{Data: []byte("brotli")},
				"app.js.gz": &fstest.MapFile{Data: []byte("gzip")},
			}, StaticConfig{PreCompressed: true})
			req := httptest.NewRequest("GET", "/assets/app.js", nil)
			req.Header.Set("Accept-Encoding", tt.accept)
			resp, err := app.Test(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if got := resp.Header.Get("Content-Encoding"); got != tt.encoding {
				t.Fatalf("Content-Encoding = %q, want %q", got, tt.encoding)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if string(body) != tt.body {
				t.Fatalf("body = %q, want %q", body, tt.body)
			}
		})
	}
}

func TestStaticRangeStreamsSelectedBytes(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789"), 10000)
	app := New()
	app.StaticFS("/assets", fstest.MapFS{
		"large.bin": &fstest.MapFile{Data: data},
	})
	req := httptest.NewRequest("GET", "/assets/large.bin", nil)
	req.Header.Set("Range", "bytes=1234-5678")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 206 {
		t.Fatalf("status = %d, want 206", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if want := data[1234 : 5678+1]; !bytes.Equal(body, want) {
		t.Fatalf("range body mismatch: got %d bytes, want %d", len(body), len(want))
	}
	if got := resp.Header.Get("Content-Range"); got != "bytes 1234-5678/100000" {
		t.Fatalf("Content-Range = %q", got)
	}
}

func TestStaticMultiRangeStreamsMultipartResponse(t *testing.T) {
	data := []byte("abcdefghijklmnopqrstuvwxyz")
	app := New()
	app.StaticFS("/assets", fstest.MapFS{
		"letters.txt": &fstest.MapFile{Data: data},
	})
	req := httptest.NewRequest("GET", "/assets/letters.txt", nil)
	req.Header.Set("Range", "bytes=0-2,23-25")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 206 {
		t.Fatalf("status = %d, want 206", resp.StatusCode)
	}
	mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/byteranges" || params["boundary"] == "" {
		t.Fatalf("Content-Type = %q, want multipart/byteranges with boundary", resp.Header.Get("Content-Type"))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	boundary := []byte("--" + params["boundary"])
	wants := [][]byte{
		append(append(append(append([]byte{}, boundary...), '\r', '\n'), []byte("Content-Type: text/plain; charset=utf-8\r\nContent-Range: bytes 0-2/26\r\n\r\n")...), []byte("abc\r\n")...),
		append(append(append(append([]byte{}, boundary...), '\r', '\n'), []byte("Content-Type: text/plain; charset=utf-8\r\nContent-Range: bytes 23-25/26\r\n\r\n")...), []byte("xyz\r\n")...),
	}
	for _, want := range wants {
		if !bytes.Contains(body, want) {
			t.Fatalf("multipart response missing part %q: %q", want, body)
		}
	}
	if !bytes.HasSuffix(body, append(append([]byte{}, boundary...), '-', '-', '\r', '\n')) {
		t.Fatalf("multipart response has invalid closing boundary: %q", body)
	}
}

func TestStaticMultiRangeLimit(t *testing.T) {
	app := New()
	app.StaticFS("/assets", fstest.MapFS{
		"letters.txt": &fstest.MapFile{Data: []byte("abcdefghijklmnopqrstuvwxyz")},
	}, StaticConfig{MaxRanges: 1})
	parts := make([]string, 0, 2)
	for _, n := range []int{0, 2} {
		parts = append(parts, strconv.Itoa(n)+"-"+strconv.Itoa(n))
	}
	req := httptest.NewRequest("GET", "/assets/letters.txt", nil)
	req.Header.Set("Range", "bytes="+strings.Join(parts, ","))
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 416 {
		t.Fatalf("status = %d, want 416", resp.StatusCode)
	}
}

func TestStaticCompressionStreamsLargeAssets(t *testing.T) {
	data := bytes.Repeat([]byte("compressible asset data\n"), 10000)
	app := New()
	app.StaticFS("/assets", fstest.MapFS{
		"large.txt": &fstest.MapFile{Data: data},
	}, StaticConfig{Compress: true})
	req := httptest.NewRequest("GET", "/assets/large.txt", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", got)
	}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, data) {
		t.Fatalf("decoded body mismatch: got %d bytes, want %d", len(decoded), len(data))
	}
}
