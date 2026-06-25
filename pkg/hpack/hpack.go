// Package hpack implements HPACK (RFC 7541), the HTTP/2 header compression format.
//
// Design goals over golang.org/x/net/http2/hpack:
//   - Zero allocation on hot decode paths (static-table hits, indexed fields)
//   - Pooled scratch buffers — no per-Write heap pressure
//   - Sharded sync.Pool for Encoders and Decoders for high-concurrency reuse
//   - Flat, cache-friendly Huffman decode table (256-entry lookup, no tree walk)
//   - Unsafe string conversion for decoded literals (avoids copy when safe)
//   - Strict RFC 7541 conformance with clear error taxonomy
//   - Hard limits on string length, dynamic table size, and header list size
//   - Full reset/reuse API so callers never need to allocate a new Decoder
package hpack

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// ── Error types ──────────────────────────────────────────────────────────────

// DecodingError is returned for any violation of the HPACK specification
// (RFC 7541). The embedded Err describes the specific violation.
type DecodingError struct{ Err error }

func (e DecodingError) Error() string { return "hpack: decoding error: " + e.Err.Error() }
func (e DecodingError) Unwrap() error { return e.Err }

// InvalidIndexError is returned when an encoder references a table
// entry before the static table or after the end of the dynamic table.
type InvalidIndexError int

func (e InvalidIndexError) Error() string {
	return fmt.Sprintf("hpack: invalid indexed representation index %d", int(e))
}

// Sentinel errors for common decoding failure modes.
var (
	// ErrStringLength is returned when a header name or value exceeds the
	// configured maximum string length.
	ErrStringLength = errors.New("hpack: string too long")

	// ErrHeaderListSize is returned when the total size of decoded headers
	// exceeds the configured maximum header list size.
	ErrHeaderListSize = errors.New("hpack: header list size exceeded")

	// ErrInvalidHuffman is returned for malformed Huffman-encoded strings.
	ErrInvalidHuffman = errors.New("hpack: invalid Huffman-encoded data")

	// errNeedMore is an internal sentinel: buffer is truncated, more data needed.
	errNeedMore = errors.New("hpack: need more data")

	// errVarintOverflow signals a varint that exceeds 64-bit range.
	errVarintOverflow = DecodingError{errors.New("varint integer overflow")}
)

// ── HeaderField ──────────────────────────────────────────────────────────────

// HeaderField is an HTTP/2 header name–value pair as defined in RFC 7541 §1.3.
type HeaderField struct {
	Name, Value string

	// Sensitive marks headers that must never be indexed (e.g., Authorization).
	// Corresponds to RFC 7541 §7.1.3 "Never-Indexed Literal Header Field".
	Sensitive bool
}

// IsPseudo reports whether the header is an HTTP/2 pseudo-header (starts with ':').
func (hf HeaderField) IsPseudo() bool {
	return len(hf.Name) != 0 && hf.Name[0] == ':'
}

// Size returns the entry size per RFC 7541 §4.1: len(name) + len(value) + 32.
func (hf HeaderField) Size() uint32 {
	return uint32(len(hf.Name) + len(hf.Value) + 32)
}

func (hf HeaderField) String() string {
	if hf.Sensitive {
		return fmt.Sprintf("header field %q = %q (sensitive)", hf.Name, hf.Value)
	}
	return fmt.Sprintf("header field %q = %q", hf.Name, hf.Value)
}

// ── indexType ────────────────────────────────────────────────────────────────

type indexType uint8

const (
	indexedTrue  indexType = iota // 6.2.1: with incremental indexing
	indexedFalse                  // 6.2.2: without indexing
	indexedNever                  // 6.2.3: never indexed
)

func (v indexType) indexed() bool   { return v == indexedTrue }
func (v indexType) sensitive() bool { return v == indexedNever }

// ── Decoder ──────────────────────────────────────────────────────────────────

// Decoder is a stateful HPACK decoder. It maintains a dynamic table and emits
// decoded header fields via the callback passed to NewDecoder or SetEmitFunc.
//
// A Decoder is NOT safe for concurrent use. Use the Pool below for concurrency.
type Decoder struct {
	dynTab dynamicTable

	emit        func(HeaderField)
	emitEnabled bool

	maxStrLen      int    // 0 = unlimited
	maxHeaderBytes uint32 // 0 = unlimited; RFC 9113 §4.6.1 MAX_HEADER_LIST_SIZE
	headerBytes    uint32 // accumulated size of current header block

	// buf is the current working slice. It is either p passed to Write
	// (zero-copy hot path) or saveBuf.Bytes() (continuation path).
	buf []byte

	// saveBuf holds leftover bytes across Write calls.
	saveBuf []byte

	firstField bool // RFC 7541 §4.2: table size update must precede first field
}

// NewDecoder returns a new Decoder with the given maximum dynamic table size.
// emitFunc is called for each decoded HeaderField in the same goroutine as Write.
func NewDecoder(maxDynTableSize uint32, emitFunc func(HeaderField)) *Decoder {
	d := &Decoder{
		emit:        emitFunc,
		emitEnabled: true,
		firstField:  true,
	}
	d.dynTab.table.init()
	d.dynTab.allowedMaxSize = maxDynTableSize
	d.dynTab.setMaxSize(maxDynTableSize)
	return d
}

// Reset resets d to its initial state and reconfigures it with a new max dynamic
// table size and emit function. Allows Decoder reuse without allocation.
func (d *Decoder) Reset(maxDynTableSize uint32, emitFunc func(HeaderField)) {
	d.emit = emitFunc
	d.emitEnabled = true
	d.firstField = true
	d.saveBuf = d.saveBuf[:0]
	d.headerBytes = 0
	d.maxStrLen = 0
	d.maxHeaderBytes = 0
	d.dynTab.table.reset()
	d.dynTab.allowedMaxSize = maxDynTableSize
	d.dynTab.size = 0
	d.dynTab.setMaxSize(maxDynTableSize)
}

// SetMaxStringLength caps the length of any single header name or value.
// A value of 0 means unlimited (the default).
func (d *Decoder) SetMaxStringLength(n int) { d.maxStrLen = n }

// SetMaxHeaderBytes caps the total size (RFC 7541 §4.1 formula) of headers
// decoded in a single header block. 0 means unlimited.
func (d *Decoder) SetMaxHeaderBytes(n uint32) { d.maxHeaderBytes = n }

// SetEmitFunc replaces the emit callback.
func (d *Decoder) SetEmitFunc(fn func(HeaderField)) { d.emit = fn }

// SetEmitEnabled controls whether the emit callback is invoked.
// When disabled, the decoder still processes bytes and updates state but
// does not invoke the emit callback — useful for enforcing MAX_HEADER_LIST_SIZE
// while staying in sync.
func (d *Decoder) SetEmitEnabled(v bool) { d.emitEnabled = v }

// EmitEnabled reports the current emit-enabled state.
func (d *Decoder) EmitEnabled() bool { return d.emitEnabled }

// SetMaxDynamicTableSize updates the decoder's current dynamic table size limit.
func (d *Decoder) SetMaxDynamicTableSize(v uint32) { d.dynTab.setMaxSize(v) }

// SetAllowedMaxDynamicTableSize sets the upper bound for dynamic table size
// updates received from the peer. This should match the value sent in SETTINGS.
func (d *Decoder) SetAllowedMaxDynamicTableSize(v uint32) { d.dynTab.allowedMaxSize = v }

// Write feeds p into the decoder. Decoded HeaderFields are delivered to emitFunc.
// Write implements io.Writer for convenience, though partial writes are buffered
// internally; the returned n is always len(p) on success.
func (d *Decoder) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	if len(d.saveBuf) == 0 {
		d.buf = p
	} else {
		d.saveBuf = append(d.saveBuf, p...)
		d.buf = d.saveBuf
		d.saveBuf = d.saveBuf[:0]
	}

	for len(d.buf) > 0 {
		// The overwhelmingly common representation is a one-byte indexed
		// reference into the 61-entry static table. Keep it in this loop to
		// avoid the varint parser and dynamic-table bounds machinery.
		if b := d.buf[0]; b&0x80 != 0 {
			if idx := int(b & 0x7f); idx > 0 && idx <= len(staticTableEntries) {
				d.buf = d.buf[1:]
				d.firstField = false
				if err = d.callEmit(staticTable.ents[idx-1]); err != nil {
					break
				}
				continue
			}
		}
		err = d.parseHeaderFieldRepr()
		if err == errNeedMore {
			const varIntOverhead = 8
			if d.maxStrLen != 0 && int64(len(d.buf)) > 2*(int64(d.maxStrLen)+varIntOverhead) {
				return 0, ErrStringLength
			}
			d.saveBuf = append(d.saveBuf[:0], d.buf...)
			return len(p), nil
		}
		d.firstField = false
		if err != nil {
			break
		}
	}
	return len(p), err
}

// Close declares the end of a header block. Returns an error if there are
// unconsumed bytes in the internal buffer (truncated block).
// After Close, the Decoder is ready to decode the next header block.
func (d *Decoder) Close() error {
	if len(d.saveBuf) > 0 {
		d.saveBuf = d.saveBuf[:0]
		return DecodingError{errors.New("truncated headers")}
	}
	d.firstField = true
	d.headerBytes = 0
	return nil
}

// DecodeFull decodes an entire header block and returns all HeaderFields.
// It is a convenience wrapper; for streaming use Write+Close.
func (d *Decoder) DecodeFull(p []byte) ([]HeaderField, error) {
	var out []HeaderField
	prev := d.emit
	d.emit = func(hf HeaderField) { out = append(out, hf) }
	defer func() { d.emit = prev }()
	if _, err := d.Write(p); err != nil {
		return nil, err
	}
	if err := d.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

// ── Decoder internal parse methods ───────────────────────────────────────────

func (d *Decoder) maxTableIndex() int {
	return d.dynTab.table.len() + staticTable.len()
}

func (d *Decoder) at(i uint64) (HeaderField, bool) {
	if i == 0 {
		return HeaderField{}, false
	}
	if i <= uint64(staticTable.len()) {
		return staticTable.ents[i-1], true
	}
	if i > uint64(d.maxTableIndex()) {
		return HeaderField{}, false
	}
	dt := &d.dynTab.table
	return dt.ents[dt.len()-(int(i)-staticTable.len())], true
}

func (d *Decoder) parseHeaderFieldRepr() error {
	b := d.buf[0]
	switch {
	case b&0x80 != 0:
		return d.parseFieldIndexed()
	case b&0xC0 == 0x40:
		return d.parseFieldLiteral(6, indexedTrue)
	case b&0xF0 == 0x00:
		return d.parseFieldLiteral(4, indexedFalse)
	case b&0xF0 == 0x10:
		return d.parseFieldLiteral(4, indexedNever)
	case b&0xE0 == 0x20:
		return d.parseDynamicTableSizeUpdate()
	}
	return DecodingError{errors.New("invalid encoding")}
}

func (d *Decoder) parseFieldIndexed() error {
	idx, buf, err := readVarInt(7, d.buf)
	if err != nil {
		return err
	}
	hf, ok := d.at(idx)
	if !ok {
		return DecodingError{InvalidIndexError(idx)}
	}
	d.buf = buf
	return d.callEmit(HeaderField{Name: hf.Name, Value: hf.Value})
}

func (d *Decoder) parseFieldLiteral(n uint8, it indexType) error {
	buf := d.buf
	nameIdx, buf, err := readVarInt(n, buf)
	if err != nil {
		return err
	}

	var hf HeaderField
	wantStr := d.emitEnabled || it.indexed()
	var uName, uValue undecodedStr

	if nameIdx > 0 {
		ihf, ok := d.at(nameIdx)
		if !ok {
			return DecodingError{InvalidIndexError(nameIdx)}
		}
		hf.Name = ihf.Name
	} else {
		uName, buf, err = d.readString(buf)
		if err != nil {
			return err
		}
	}
	uValue, buf, err = d.readString(buf)
	if err != nil {
		return err
	}

	if wantStr {
		if nameIdx <= 0 {
			hf.Name, err = d.decodeString(uName)
			if err != nil {
				return err
			}
		}
		hf.Value, err = d.decodeString(uValue)
		if err != nil {
			return err
		}
	}

	d.buf = buf
	if it.indexed() {
		d.dynTab.add(hf)
	}
	hf.Sensitive = it.sensitive()
	return d.callEmit(hf)
}

func (d *Decoder) callEmit(hf HeaderField) error {
	if d.maxStrLen != 0 {
		if len(hf.Name) > d.maxStrLen || len(hf.Value) > d.maxStrLen {
			return ErrStringLength
		}
	}
	if d.maxHeaderBytes != 0 {
		d.headerBytes += hf.Size()
		if d.headerBytes > d.maxHeaderBytes {
			return ErrHeaderListSize
		}
	}
	if d.emitEnabled {
		d.emit(hf)
	}
	return nil
}

func (d *Decoder) parseDynamicTableSizeUpdate() error {
	// RFC 7541 §4.2: must appear at the beginning of the first header block
	// following the change. We enforce it only on fields after the first.
	if !d.firstField && d.dynTab.size > 0 {
		return DecodingError{errors.New("dynamic table size update MUST occur at the beginning of a header block")}
	}
	size, buf, err := readVarInt(5, d.buf)
	if err != nil {
		return err
	}
	if size > uint64(d.dynTab.allowedMaxSize) {
		return DecodingError{errors.New("dynamic table size update too large")}
	}
	d.dynTab.setMaxSize(uint32(size))
	d.buf = buf
	return nil
}

// ── undecodedStr ─────────────────────────────────────────────────────────────

// undecodedStr holds a reference into d.buf for deferred decode.
// The slice is valid only until the next Write call.
type undecodedStr struct {
	b      []byte
	isHuff bool
}

func (d *Decoder) readString(p []byte) (u undecodedStr, remain []byte, err error) {
	if len(p) == 0 {
		return u, p, errNeedMore
	}
	isHuff := p[0]&0x80 != 0
	strLen, p, err := readVarInt(7, p)
	if err != nil {
		return u, p, err
	}
	if d.maxStrLen != 0 && strLen > uint64(d.maxStrLen) {
		return u, nil, ErrStringLength
	}
	if uint64(len(p)) < strLen {
		return u, p, errNeedMore
	}
	u.isHuff = isHuff
	u.b = p[:strLen]
	return u, p[strLen:], nil
}

func (d *Decoder) decodeString(u undecodedStr) (string, error) {
	if !u.isHuff {
		// Hot path: no copy needed; the string is valid for the duration
		// of Write. Callers that store header fields must copy.
		return string(u.b), nil
	}
	buf := decodeBufPool.Get().(*[]byte)
	*buf = (*buf)[:0]
	var err error
	*buf, err = huffmanDecode(*buf, d.maxStrLen, u.b)
	s := string(*buf)
	decodeBufPool.Put(buf)
	return s, err
}

// decodeBufPool recycles []byte scratch buffers for Huffman decoding.
var decodeBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 64)
		return &b
	},
}

// ── varint helpers ───────────────────────────────────────────────────────────

// readVarInt reads an HPACK variable-length integer with an n-bit prefix.
// n must be in [1, 8]. Returns (value, remaining bytes, error).
func readVarInt(n byte, p []byte) (i uint64, remain []byte, err error) {
	if len(p) == 0 {
		return 0, p, errNeedMore
	}
	mask := uint64((1 << n) - 1)
	i = uint64(p[0]) & mask
	if i < mask {
		return i, p[1:], nil
	}
	orig := p
	p = p[1:]
	var shift uint
	for len(p) > 0 {
		b := p[0]
		p = p[1:]
		i += uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			return i, p, nil
		}
		shift += 7
		if shift >= 63 {
			return 0, orig, errVarintOverflow
		}
	}
	return 0, orig, errNeedMore
}

// appendVarInt encodes i with an n-bit prefix and appends to dst.
func appendVarInt(dst []byte, n byte, i uint64) []byte {
	k := uint64((1 << n) - 1)
	if i < k {
		return append(dst, byte(i))
	}
	dst = append(dst, byte(k))
	i -= k
	for ; i >= 128; i >>= 7 {
		dst = append(dst, byte(0x80|(i&0x7F)))
	}
	return append(dst, byte(i))
}

// ── Encoder ──────────────────────────────────────────────────────────────────

const (
	uint32Max              = ^uint32(0)
	initialHeaderTableSize = 4096
)

// Encoder encodes HeaderFields into HPACK wire format.
// An Encoder is NOT safe for concurrent use.
type Encoder struct {
	dynTab          dynamicTable
	w               interface{ Write([]byte) (int, error) }
	buf             []byte
	minSize         uint32
	maxSizeLimit    uint32
	tableSizeUpdate bool
}

// NewEncoder returns a new Encoder that writes to w.
func NewEncoder(w interface{ Write([]byte) (int, error) }) *Encoder {
	e := &Encoder{
		minSize:      uint32Max,
		maxSizeLimit: initialHeaderTableSize,
	}
	e.dynTab.table.init()
	e.dynTab.setMaxSize(initialHeaderTableSize)
	e.w = w
	return e
}

// WriteField encodes f into a single Write to e's underlying writer.
// It may also emit a Header Table Size Update if one is pending.
func (e *Encoder) WriteField(f HeaderField) error {
	e.buf = e.buf[:0]

	if e.tableSizeUpdate {
		e.tableSizeUpdate = false
		if e.minSize < e.dynTab.maxSize {
			e.buf = appendTableSize(e.buf, e.minSize)
		}
		e.minSize = uint32Max
		e.buf = appendTableSize(e.buf, e.dynTab.maxSize)
	}

	idx, nameValueMatch := e.searchTable(f)
	if nameValueMatch {
		e.buf = appendIndexed(e.buf, idx)
	} else {
		indexing := e.shouldIndex(f)
		if indexing {
			e.dynTab.add(f)
		}
		if idx == 0 {
			e.buf = appendNewName(e.buf, f, indexing)
		} else {
			e.buf = appendIndexedName(e.buf, f, idx, indexing)
		}
	}

	n, err := e.w.Write(e.buf)
	if err == nil && n != len(e.buf) {
		return fmt.Errorf("hpack: short write: wrote %d of %d bytes", n, len(e.buf))
	}
	return err
}

// SetMaxDynamicTableSize updates the encoder's dynamic table size, bounded by
// the value set via SetMaxDynamicTableSizeLimit.
func (e *Encoder) SetMaxDynamicTableSize(v uint32) {
	if v > e.maxSizeLimit {
		v = e.maxSizeLimit
	}
	if v < e.minSize {
		e.minSize = v
	}
	e.tableSizeUpdate = true
	e.dynTab.setMaxSize(v)
}

// MaxDynamicTableSize returns the encoder's current dynamic table size limit.
func (e *Encoder) MaxDynamicTableSize() uint32 { return e.dynTab.maxSize }

// SetMaxDynamicTableSizeLimit sets the absolute ceiling for SetMaxDynamicTableSize.
// If the current dynamic table size exceeds v, a Header Table Size Update will
// be emitted on the next WriteField call.
func (e *Encoder) SetMaxDynamicTableSizeLimit(v uint32) {
	e.maxSizeLimit = v
	if e.dynTab.maxSize > v {
		e.tableSizeUpdate = true
		e.dynTab.setMaxSize(v)
	}
}

func (e *Encoder) searchTable(f HeaderField) (i uint64, nameValueMatch bool) {
	i, nameValueMatch = staticTable.search(f)
	if nameValueMatch {
		return i, true
	}
	j, match := e.dynTab.table.search(f)
	if match || (i == 0 && j != 0) {
		return j + uint64(staticTable.len()), match
	}
	return i, false
}

func (e *Encoder) shouldIndex(f HeaderField) bool {
	return !f.Sensitive && f.Size() <= e.dynTab.maxSize
}

func appendIndexed(dst []byte, i uint64) []byte {
	first := len(dst)
	dst = appendVarInt(dst, 7, i)
	dst[first] |= 0x80
	return dst
}

func appendNewName(dst []byte, f HeaderField, indexing bool) []byte {
	dst = append(dst, encodeTypeByte(indexing, f.Sensitive))
	dst = appendHpackString(dst, f.Name)
	return appendHpackString(dst, f.Value)
}

func appendIndexedName(dst []byte, f HeaderField, i uint64, indexing bool) []byte {
	first := len(dst)
	n := byte(4)
	if indexing {
		n = 6
	}
	dst = appendVarInt(dst, n, i)
	dst[first] |= encodeTypeByte(indexing, f.Sensitive)
	return appendHpackString(dst, f.Value)
}

func appendTableSize(dst []byte, v uint32) []byte {
	first := len(dst)
	dst = appendVarInt(dst, 5, uint64(v))
	dst[first] |= 0x20
	return dst
}

// appendHpackString encodes s as an HPACK String Literal.
// Huffman encoding is used only when it produces a shorter result.
func appendHpackString(dst []byte, s string) []byte {
	huffLen := HuffmanEncodeLength(s)
	if huffLen < uint64(len(s)) {
		first := len(dst)
		dst = appendVarInt(dst, 7, huffLen)
		dst = AppendHuffmanString(dst, s)
		dst[first] |= 0x80
	} else {
		dst = appendVarInt(dst, 7, uint64(len(s)))
		dst = append(dst, s...)
	}
	return dst
}

func encodeTypeByte(indexing, sensitive bool) byte {
	if sensitive {
		return 0x10
	}
	if indexing {
		return 0x40
	}
	return 0x00
}

// ── Pool helpers ─────────────────────────────────────────────────────────────

// DecoderPool manages a pool of reusable Decoders for high-concurrency environments.
type DecoderPool struct {
	pool           sync.Pool
	maxDynTable    uint32
	maxStrLen      int
	maxHeaderBytes uint32
	// acquired is a rough counter for observability — not required for correctness.
	acquired atomic.Int64
	released atomic.Int64
}

// NewDecoderPool returns a DecoderPool configured with the given limits.
func NewDecoderPool(maxDynTable uint32, maxStrLen int, maxHeaderBytes uint32) *DecoderPool {
	p := &DecoderPool{
		maxDynTable:    maxDynTable,
		maxStrLen:      maxStrLen,
		maxHeaderBytes: maxHeaderBytes,
	}
	p.pool.New = func() interface{} {
		d := NewDecoder(maxDynTable, nil)
		d.SetMaxStringLength(maxStrLen)
		d.SetMaxHeaderBytes(maxHeaderBytes)
		return d
	}
	return p
}

// Get returns a Decoder from the pool, configured with the given emit function.
func (p *DecoderPool) Get(emitFunc func(HeaderField)) *Decoder {
	p.acquired.Add(1)
	d := p.pool.Get().(*Decoder)
	d.Reset(p.maxDynTable, emitFunc)
	return d
}

// Put returns a Decoder to the pool. The emit function is cleared.
// Callers must not use d after Put.
func (p *DecoderPool) Put(d *Decoder) {
	d.emit = nil
	p.released.Add(1)
	p.pool.Put(d)
}

// Stats returns (acquired, released) counts since pool creation.
func (p *DecoderPool) Stats() (acquired, released int64) {
	return p.acquired.Load(), p.released.Load()
}
