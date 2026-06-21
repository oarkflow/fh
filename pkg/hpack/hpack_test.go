package hpack

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"
)

// ── Helpers ──────────────────────────────────────────────────────────────────

func mustDecode(t *testing.T, d *Decoder, data []byte) []HeaderField {
	t.Helper()
	var out []HeaderField
	d.SetEmitFunc(func(hf HeaderField) { out = append(out, hf) })
	if _, err := d.Write(data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return out
}

func mustHex(s string) []byte {
	s = strings.ReplaceAll(s, " ", "")
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func newDecoder() *Decoder { return NewDecoder(4096, nil) }

// ── RFC 7541 Appendix C examples ─────────────────────────────────────────────

// C.3.1 – First Request (no Huffman)
func TestRFC7541_C3_1(t *testing.T) {
	wire := mustHex("828684410f7777772e6578616d706c652e636f6d")
	d := newDecoder()
	fields := mustDecode(t, d, wire)

	want := []HeaderField{
		{Name: ":method", Value: "GET"},
		{Name: ":scheme", Value: "http"},
		{Name: ":path", Value: "/"},
		{Name: ":authority", Value: "www.example.com"},
	}
	if len(fields) != len(want) {
		t.Fatalf("got %d fields, want %d", len(fields), len(want))
	}
	for i, w := range want {
		if fields[i] != w {
			t.Errorf("[%d] got %v, want %v", i, fields[i], w)
		}
	}
}

// C.3.2 – Second Request (uses dynamic table entry from first)
func TestRFC7541_C3_2(t *testing.T) {
	wire1 := mustHex("828684410f7777772e6578616d706c652e636f6d")
	wire2 := mustHex("828684be58086e6f2d6361636865")
	d := newDecoder()
	mustDecode(t, d, wire1) // prime dynamic table

	fields := mustDecode(t, d, wire2)
	want := []HeaderField{
		{Name: ":method", Value: "GET"},
		{Name: ":scheme", Value: "http"},
		{Name: ":path", Value: "/"},
		{Name: ":authority", Value: "www.example.com"},
		{Name: "cache-control", Value: "no-cache"},
	}
	if len(fields) != len(want) {
		t.Fatalf("got %d fields, want %d", len(fields), len(want))
	}
	for i, w := range want {
		if fields[i] != w {
			t.Errorf("[%d] got %v, want %v", i, fields[i], w)
		}
	}
}

// C.4.1 – First Request with Huffman encoding
func TestRFC7541_C4_1(t *testing.T) {
	wire := mustHex("828684418cf1e3c2e5f23a6ba0ab90f4ff")
	d := newDecoder()
	fields := mustDecode(t, d, wire)

	want := []HeaderField{
		{Name: ":method", Value: "GET"},
		{Name: ":scheme", Value: "http"},
		{Name: ":path", Value: "/"},
		{Name: ":authority", Value: "www.example.com"},
	}
	for i, w := range want {
		if i >= len(fields) {
			t.Fatalf("missing field %d", i)
		}
		if fields[i] != w {
			t.Errorf("[%d] got %v, want %v", i, fields[i], w)
		}
	}
}

// ── Encoder round-trip ────────────────────────────────────────────────────────

func TestEncoderDecoderRoundTrip(t *testing.T) {
	cases := [][]HeaderField{
		{
			{Name: ":method", Value: "GET"},
			{Name: ":path", Value: "/index.html"},
			{Name: ":scheme", Value: "https"},
			{Name: ":authority", Value: "example.com"},
			{Name: "accept-encoding", Value: "gzip"},
			{Name: "x-request-id", Value: "abc-123"},
		},
		{
			{Name: ":status", Value: "200"},
			{Name: "content-type", Value: "application/json"},
			{Name: "content-length", Value: "42"},
			{Name: "authorization", Value: "Bearer token123", Sensitive: true},
		},
		{
			// Large value to exercise Huffman vs. literal selection
			{Name: "x-large", Value: strings.Repeat("abcdefghij", 100)},
		},
	}

	for tc, fields := range cases {
		var buf bytes.Buffer
		enc := NewEncoder(&buf)
		for _, f := range fields {
			if err := enc.WriteField(f); err != nil {
				t.Fatalf("case %d: WriteField: %v", tc, err)
			}
		}

		dec := newDecoder()
		got := mustDecode(t, dec, buf.Bytes())

		if len(got) != len(fields) {
			t.Fatalf("case %d: got %d fields, want %d", tc, len(got), len(fields))
		}
		for i, w := range fields {
			if got[i].Name != w.Name || got[i].Value != w.Value || got[i].Sensitive != w.Sensitive {
				t.Errorf("case %d field %d: got %v, want %v", tc, i, got[i], w)
			}
		}
	}
}

// ── Dynamic table eviction ────────────────────────────────────────────────────

func TestDynamicTableEviction(t *testing.T) {
	// Use a tiny table (64 bytes) to force evictions.
	d := NewDecoder(64, nil)
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	enc.SetMaxDynamicTableSizeLimit(64)
	enc.SetMaxDynamicTableSize(64)

	fields := []HeaderField{
		{Name: "a", Value: "bb"},      // size = 1+2+32 = 35
		{Name: "cc", Value: "ddd"},    // size = 2+3+32 = 37 — evicts "a"
		{Name: "ee", Value: "ffffff"}, // size = 2+6+32 = 40 — evicts "cc"
	}
	for _, f := range fields {
		if err := enc.WriteField(f); err != nil {
			t.Fatalf("WriteField: %v", err)
		}
	}

	got := mustDecode(t, d, buf.Bytes())
	if len(got) != len(fields) {
		t.Fatalf("got %d, want %d", len(got), len(fields))
	}
}

// ── String length limit ───────────────────────────────────────────────────────

func TestMaxStringLength(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	_ = enc.WriteField(HeaderField{Name: "x-test", Value: "hello-world-too-long"})

	d := newDecoder()
	d.SetMaxStringLength(5)
	d.SetEmitFunc(func(HeaderField) {})
	_, err := d.Write(buf.Bytes())
	if err == nil {
		err = d.Close()
	}
	if err != ErrStringLength {
		t.Fatalf("want ErrStringLength, got %v", err)
	}
}

// ── Header list size limit ────────────────────────────────────────────────────

func TestMaxHeaderListSize(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	for i := 0; i < 10; i++ {
		_ = enc.WriteField(HeaderField{Name: "x-hdr", Value: "value"})
	}

	d := newDecoder()
	d.SetMaxHeaderBytes(100) // 10 × (5+5+32) = 420 > 100
	d.SetEmitFunc(func(HeaderField) {})
	_, err := d.Write(buf.Bytes())
	if err == nil {
		err = d.Close()
	}
	if err != ErrHeaderListSize {
		t.Fatalf("want ErrHeaderListSize, got %v", err)
	}
}

// ── Invalid index ─────────────────────────────────────────────────────────────

func TestInvalidIndex(t *testing.T) {
	// 0xFF = indexed representation with index 127 — well beyond any table.
	d := newDecoder()
	d.SetEmitFunc(func(HeaderField) {})
	_, err := d.Write([]byte{0xFF, 0x80, 0x80, 0x80, 0x01})
	if err == nil {
		err = d.Close()
	}
	if err == nil {
		t.Fatal("expected decoding error for out-of-range index")
	}
}

// ── Truncated block ───────────────────────────────────────────────────────────

func TestTruncatedBlock(t *testing.T) {
	// Write half a valid field and then close — should report truncation.
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	_ = enc.WriteField(HeaderField{Name: "x-test", Value: "v"})
	wire := buf.Bytes()

	d := newDecoder()
	d.SetEmitFunc(func(HeaderField) {})
	if _, err := d.Write(wire[:1]); err != nil {
		t.Fatalf("partial write error: %v", err)
	}
	if err := d.Close(); err == nil {
		t.Fatal("expected truncation error on Close")
	}
}

// ── Incremental writes ────────────────────────────────────────────────────────

func TestIncrementalWrites(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	want := HeaderField{Name: "content-type", Value: "text/html"}
	_ = enc.WriteField(want)
	wire := buf.Bytes()

	d := newDecoder()
	var got []HeaderField
	d.SetEmitFunc(func(hf HeaderField) { got = append(got, hf) })

	// Write one byte at a time.
	for _, b := range wire {
		if _, err := d.Write([]byte{b}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(got) != 1 || got[0] != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// ── DecoderPool ──────────────────────────────────────────────────────────────

func TestDecoderPool(t *testing.T) {
	pool := NewDecoderPool(4096, 0, 0)

	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	_ = enc.WriteField(HeaderField{Name: ":method", Value: "GET"})
	wire := buf.Bytes()

	var out []HeaderField
	d := pool.Get(func(hf HeaderField) { out = append(out, hf) })
	if _, err := d.Write(wire); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	pool.Put(d)

	if len(out) != 1 || out[0].Name != ":method" {
		t.Fatalf("unexpected fields: %v", out)
	}

	acq, rel := pool.Stats()
	if acq != 1 || rel != 1 {
		t.Fatalf("pool stats: acquired=%d released=%d", acq, rel)
	}
}

// ── Huffman encode/decode ────────────────────────────────────────────────────

func TestHuffmanRoundTrip(t *testing.T) {
	cases := []string{
		"",
		"a",
		"www.example.com",
		"application/json; charset=utf-8",
		string([]byte{0, 1, 2, 3, 127, 128, 254, 255}),
		strings.Repeat("GET ", 50),
	}
	for _, s := range cases {
		encoded := AppendHuffmanString(nil, s)
		got, err := HuffmanDecodeToString(encoded)
		if err != nil {
			t.Errorf("HuffmanDecode(%q): %v", s, err)
			continue
		}
		if got != s {
			t.Errorf("roundtrip(%q): got %q", s, got)
		}
	}
}

func TestHuffmanInvalidPadding(t *testing.T) {
	// A padding byte that is 0x00 instead of all-1s is invalid.
	_, err := HuffmanDecodeToString([]byte{0x00})
	if err != ErrInvalidHuffman {
		t.Fatalf("want ErrInvalidHuffman, got %v", err)
	}
}

// ── Sensitive headers ────────────────────────────────────────────────────────

func TestSensitiveHeaders(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	_ = enc.WriteField(HeaderField{Name: "authorization", Value: "secret", Sensitive: true})
	_ = enc.WriteField(HeaderField{Name: ":method", Value: "GET"})
	// Re-encode authorization — it should NOT be in the dynamic table.
	_ = enc.WriteField(HeaderField{Name: "authorization", Value: "secret", Sensitive: true})

	d := newDecoder()
	got := mustDecode(t, d, buf.Bytes())
	if len(got) != 3 {
		t.Fatalf("got %d fields", len(got))
	}
	if !got[0].Sensitive {
		t.Error("first field should be sensitive")
	}
	if !got[2].Sensitive {
		t.Error("third field should be sensitive")
	}
}

// ── Decoder Reset/reuse ───────────────────────────────────────────────────────

func TestDecoderReset(t *testing.T) {
	d := newDecoder()

	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	_ = enc.WriteField(HeaderField{Name: ":method", Value: "POST"})
	mustDecode(t, d, buf.Bytes())

	// Reset and decode a fresh block with a new dynamic table.
	buf.Reset()
	enc2 := NewEncoder(&buf)
	_ = enc2.WriteField(HeaderField{Name: ":status", Value: "200"})

	d.Reset(4096, nil)
	got := mustDecode(t, d, buf.Bytes())
	if len(got) != 1 || got[0].Name != ":status" {
		t.Fatalf("unexpected: %v", got)
	}
}

// ── Static table lookup ───────────────────────────────────────────────────────

func TestStaticTableIndexed(t *testing.T) {
	// Wire: 0x82 = indexed representation, index 2 = ":method: GET"
	d := newDecoder()
	got := mustDecode(t, d, []byte{0x82})
	if len(got) != 1 || got[0].Name != ":method" || got[0].Value != "GET" {
		t.Fatalf("got %v", got)
	}
}

// ── Benchmarks ───────────────────────────────────────────────────────────────

// BenchmarkDecodeStaticHit measures decoding of indexed fields fully satisfied
// by the static table — the absolute hot path.
func BenchmarkDecodeStaticHit(b *testing.B) {
	// 5 indexed static entries: :method=GET, :scheme=https, :path=/, :status=200, accept-encoding=gzip,deflate
	wire := []byte{0x82, 0x87, 0x84, 0x88, 0x90}
	d := newDecoder()
	d.SetEmitFunc(func(HeaderField) {})
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := d.Write(wire); err != nil {
			b.Fatal(err)
		}
		if err := d.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeHuffmanLiteral measures decoding of a Huffman-encoded literal header.
func BenchmarkDecodeHuffmanLiteral(b *testing.B) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	_ = enc.WriteField(HeaderField{Name: "x-request-id", Value: "abc-123-def-456-ghi-789"})
	wire := buf.Bytes()

	d := newDecoder()
	d.SetEmitFunc(func(HeaderField) {})
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := d.Write(wire); err != nil {
			b.Fatal(err)
		}
		if err := d.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEncodeTypical simulates encoding a typical HTTP/2 request.
// The encoder and buffer are created once; per-iteration cost is purely
// the field encoding and dynamic-table management.
func BenchmarkEncodeTypical(b *testing.B) {
	fields := []HeaderField{
		{Name: ":method", Value: "GET"},
		{Name: ":scheme", Value: "https"},
		{Name: ":path", Value: "/api/v1/users"},
		{Name: ":authority", Value: "api.example.com"},
		{Name: "accept", Value: "application/json"},
		{Name: "accept-encoding", Value: "gzip, deflate"},
		{Name: "user-agent", Value: "go-http2/1.0"},
		{Name: "x-request-id", Value: "abc-123"},
	}
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		for _, f := range fields {
			if err := enc.WriteField(f); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkHuffmanEncode measures AppendHuffmanString throughput.
func BenchmarkHuffmanEncode(b *testing.B) {
	s := "www.example.com/path/to/resource?query=value&other=thing"
	dst := make([]byte, 0, 128)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dst = AppendHuffmanString(dst[:0], s)
	}
}

// BenchmarkHuffmanDecode measures huffman decoding throughput.
func BenchmarkHuffmanDecode(b *testing.B) {
	s := "www.example.com/path/to/resource?query=value&other=thing"
	encoded := AppendHuffmanString(nil, s)
	dst := make([]byte, 0, len(s)+8)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var err error
		dst, err = huffmanDecode(dst[:0], 0, encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecoderPool measures pool Get/Put overhead.
func BenchmarkDecoderPool(b *testing.B) {
	pool := NewDecoderPool(4096, 0, 0)
	wire := []byte{0x82, 0x87, 0x84} // 3 static hits
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		d := pool.Get(func(HeaderField) {})
		if _, err := d.Write(wire); err != nil {
			b.Fatal(err)
		}
		_ = d.Close()
		pool.Put(d)
	}
}

// BenchmarkComparison keeps performance claims reproducible against the
// reference implementation used by net/http's HTTP/2 stack.
/*
func BenchmarkComparison(b *testing.B) {
	b.Run("DecodeStatic/Custom", func(b *testing.B) {
		wire := []byte{0x82, 0x87, 0x84, 0x88, 0x90}
		d := NewDecoder(4096, func(HeaderField) {})
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = d.Write(wire)
			_ = d.Close()
		}
	})
	b.Run("DecodeStatic/XNet", func(b *testing.B) {
		wire := []byte{0x82, 0x87, 0x84, 0x88, 0x90}
		d := xhpack.NewDecoder(4096, func(xhpack.HeaderField) {})
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = d.Write(wire)
			_ = d.Close()
		}
	})
	b.Run("HuffmanEncode/Custom", func(b *testing.B) {
		s := "www.example.com/path/to/resource?query=value&other=thing"
		dst := make([]byte, 0, 128)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			dst = AppendHuffmanString(dst[:0], s)
		}
	})
	b.Run("HuffmanEncode/XNet", func(b *testing.B) {
		s := "www.example.com/path/to/resource?query=value&other=thing"
		dst := make([]byte, 0, 128)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			dst = xhpack.AppendHuffmanString(dst[:0], s)
		}
	})
}
*/
// FuzzDecode is a fuzz target for the decoder. Run with:
//
//	go test -fuzz=FuzzDecode -fuzztime=30s
func FuzzDecode(f *testing.F) {
	f.Add([]byte{0x82})                                        // :method: GET
	f.Add(mustHex("828684410f7777772e6578616d706c652e636f6d")) // C.3.1
	f.Add([]byte{0x00})
	f.Add([]byte{0xFF, 0x80, 0x80, 0x80, 0x01})
	f.Add([]byte{0x20, 0x00}) // table size update to 0

	f.Fuzz(func(t *testing.T, data []byte) {
		d := NewDecoder(4096, func(HeaderField) {})
		d.SetMaxStringLength(1024)
		d.SetMaxHeaderBytes(65535)
		if _, err := d.Write(data); err != nil {
			return
		}
		_ = d.Close()
	})
}
