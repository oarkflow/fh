package securetransport

import (
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
)

// StreamVersion is a separate wire format. Version 1 envelopes remain valid
// for clients that need a single authenticated body.
const (
	StreamVersion   = byte(2)
	MediaTypeStream = "application/fh-secure-stream"
	HeaderStream    = "X-FH-Stream"
	StreamChunkSize = 32 << 10
)

const (
	streamHead     = byte(1)
	streamData     = byte(2)
	streamEnd      = byte(3)
	streamMetaSize = 4 + 1 + 16 + 16 + 8 + 8 + 8 + 12
	streamMaxHead  = 2 << 20
)

var streamMagic = [4]byte{'F', 'H', 'S', '2'}

// StreamMetadata is sent once in X-FH-Stream. Every record authenticates it.
// Nonce is random per stream; its full value contributes to the record key
// schedule, while the record counter supplies the nonce suffix.
type StreamMetadata struct {
	SessionID ID16
	RequestID ID16
	Sequence  uint64
	IssuedAt  int64
	ExpiresAt int64
	Nonce     [12]byte
}

func (m StreamMetadata) Encode() []byte {
	b := make([]byte, streamMetaSize)
	copy(b[:4], streamMagic[:])
	b[4] = StreamVersion
	copy(b[5:21], m.SessionID[:])
	copy(b[21:37], m.RequestID[:])
	binary.BigEndian.PutUint64(b[37:45], m.Sequence)
	binary.BigEndian.PutUint64(b[45:53], uint64(m.IssuedAt))
	binary.BigEndian.PutUint64(b[53:61], uint64(m.ExpiresAt))
	copy(b[61:73], m.Nonce[:])
	return b
}

func DecodeStreamMetadata(b []byte) (StreamMetadata, error) {
	var m StreamMetadata
	if len(b) != streamMetaSize || string(b[:4]) != string(streamMagic[:]) {
		return m, ErrMalformed
	}
	if b[4] != StreamVersion {
		return m, ErrUnsupported
	}
	copy(m.SessionID[:], b[5:21])
	copy(m.RequestID[:], b[21:37])
	m.Sequence = binary.BigEndian.Uint64(b[37:45])
	m.IssuedAt = int64(binary.BigEndian.Uint64(b[45:53]))
	m.ExpiresAt = int64(binary.BigEndian.Uint64(b[53:61]))
	copy(m.Nonce[:], b[61:73])
	return m, nil
}

// EncodeRequestStreamHead authenticates the request metadata separately from
// the data records. Body must be empty because it is carried in data records.
func EncodeRequestStreamHead(payload RequestPayload, limits Limits) ([]byte, error) {
	if len(payload.Body) != 0 {
		return nil, ErrMalformed
	}
	return payload.encode(limits)
}

func DecodeRequestStreamHead(data []byte, limits Limits) (RequestPayload, error) {
	payload, err := decodeRequestPayload(data, limits)
	if err == nil && len(payload.Body) != 0 {
		return RequestPayload{}, ErrMalformed
	}
	return payload, err
}

func EncodeResponseStreamHead(payload ResponsePayload, limits Limits) ([]byte, error) {
	if len(payload.Body) != 0 {
		return nil, ErrMalformed
	}
	return payload.encode(limits)
}

func DecodeResponseStreamHead(data []byte, limits Limits) (ResponsePayload, error) {
	payload, err := decodeResponsePayload(data, limits)
	if err == nil && len(payload.Body) != 0 {
		return ResponsePayload{}, ErrMalformed
	}
	return payload, err
}

// StreamBinding binds records to the HTTP exchange. Response is true for a
// server response and Status is then the outer HTTP status. Method and Target
// are required in both directions to prevent cross-request substitution.
type StreamBinding struct {
	Method   string
	Target   string
	Status   int
	Response bool
}

func streamAADBase(meta []byte, binding StreamBinding) ([32]byte, error) {
	var out [32]byte
	if binding.Method == "" || binding.Target == "" || binding.Target[0] != '/' || len(binding.Method) > 255 || len(binding.Target) > 65535 {
		return out, ErrMalformed
	}
	if binding.Response && (binding.Status < 100 || binding.Status > 599) {
		return out, ErrMalformed
	}
	h := sha256.New()
	_, _ = h.Write([]byte("fh-secure-stream-v2"))
	_, _ = h.Write(meta)
	var numbers [4]byte
	if binding.Response {
		numbers[0] = 1
	}
	binary.BigEndian.PutUint16(numbers[1:3], uint16(binding.Status))
	_, _ = h.Write(numbers[:3])
	numbers[0] = byte(len(binding.Method))
	_, _ = h.Write(numbers[:1])
	_, _ = h.Write([]byte(binding.Method))
	binary.BigEndian.PutUint16(numbers[:2], uint16(len(binding.Target)))
	_, _ = h.Write(numbers[:2])
	_, _ = h.Write([]byte(binding.Target))
	copy(out[:], h.Sum(nil))
	return out, nil
}

type streamCipher struct {
	aead   cipher.AEAD
	base   [32]byte
	prefix [8]byte
	next   uint32
}

func newStreamCipher(key [32]byte, meta StreamMetadata, binding StreamBinding) (streamCipher, error) {
	var c streamCipher
	encoded := meta.Encode()
	base, err := streamAADBase(encoded, binding)
	if err != nil {
		return c, err
	}
	aead, err := newAEAD(key)
	if err != nil {
		return c, err
	}
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write([]byte("fh-secure-stream-v2-nonce"))
	_, _ = mac.Write(base[:])
	copy(c.prefix[:], mac.Sum(nil))
	c.aead, c.base = aead, base
	return c, nil
}

func (c *streamCipher) recordFields(kind byte) ([12]byte, [37]byte, error) {
	var nonce [12]byte
	var aad [37]byte
	if c.next == ^uint32(0) {
		return nonce, aad, ErrTooLarge
	}
	copy(nonce[:8], c.prefix[:])
	binary.BigEndian.PutUint32(nonce[8:], c.next)
	copy(aad[:32], c.base[:])
	aad[32] = kind
	binary.BigEndian.PutUint32(aad[33:], c.next)
	c.next++
	return nonce, aad, nil
}

// StreamEncoder writes independently authenticated records. Close emits a
// final authenticated byte count; a truncated stream cannot look complete.
type StreamEncoder struct {
	out      io.Writer
	cipher   streamCipher
	maxBody  uint64
	total    uint64
	head     bool
	finished bool
}

func NewStreamEncoder(out io.Writer, key [32]byte, meta StreamMetadata, binding StreamBinding, limits Limits) (*StreamEncoder, error) {
	if out == nil {
		return nil, ErrMalformed
	}
	c, err := newStreamCipher(key, meta, binding)
	if err != nil {
		return nil, err
	}
	return &StreamEncoder{out: out, cipher: c, maxBody: uint64(limits.withDefaults().MaxBody)}, nil
}

func (e *StreamEncoder) writeRecord(kind byte, plain []byte) error {
	nonce, aad, err := e.cipher.recordFields(kind)
	if err != nil {
		return err
	}
	frame := make([]byte, 9, 9+len(plain)+e.cipher.aead.Overhead())
	frame[0] = kind
	binary.BigEndian.PutUint32(frame[1:5], e.cipher.next-1)
	frame = e.cipher.aead.Seal(frame, nonce[:], plain, aad[:])
	binary.BigEndian.PutUint32(frame[5:9], uint32(len(frame)-9))
	return writeStreamAll(e.out, frame)
}

func (e *StreamEncoder) WriteHead(head []byte) error {
	if e.head || e.finished || len(head) > streamMaxHead {
		return ErrMalformed
	}
	if err := e.writeRecord(streamHead, head); err != nil {
		return err
	}
	e.head = true
	return nil
}

func (e *StreamEncoder) Write(p []byte) (int, error) {
	if !e.head || e.finished {
		return 0, ErrMalformed
	}
	if uint64(len(p)) > e.maxBody-e.total {
		return 0, ErrTooLarge
	}
	n := 0
	for len(p) > 0 {
		chunk := p
		if len(chunk) > StreamChunkSize {
			chunk = chunk[:StreamChunkSize]
		}
		if err := e.writeRecord(streamData, chunk); err != nil {
			return n, err
		}
		n += len(chunk)
		e.total += uint64(len(chunk))
		p = p[len(chunk):]
	}
	return n, nil
}

func (e *StreamEncoder) Close() error {
	if !e.head || e.finished {
		return ErrMalformed
	}
	var count [8]byte
	binary.BigEndian.PutUint64(count[:], e.total)
	if err := e.writeRecord(streamEnd, count[:]); err != nil {
		return err
	}
	e.finished = true
	return nil
}

// StreamDecoder returns plaintext only after the corresponding record has
// passed AEAD authentication. Read returns EOF only after the final record.
type StreamDecoder struct {
	in          io.Reader
	cipher      streamCipher
	maxBody     uint64
	total       uint64
	chunk       []byte
	chunkBacking []byte
	head        bool
	finished    bool
	terminalErr error
}

func NewStreamDecoder(in io.Reader, key [32]byte, meta StreamMetadata, binding StreamBinding, limits Limits) (*StreamDecoder, error) {
	if in == nil {
		return nil, ErrMalformed
	}
	c, err := newStreamCipher(key, meta, binding)
	if err != nil {
		return nil, err
	}
	return &StreamDecoder{in: in, cipher: c, maxBody: uint64(limits.withDefaults().MaxBody)}, nil
}

func (d *StreamDecoder) readRecord(max int) (byte, []byte, error) {
	var header [9]byte
	if _, err := io.ReadFull(d.in, header[:]); err != nil {
		return 0, nil, errors.Join(ErrMalformed, err)
	}
	kind := header[0]
	if binary.BigEndian.Uint32(header[1:5]) != d.cipher.next {
		return 0, nil, ErrAuthentication
	}
	size := binary.BigEndian.Uint32(header[5:9])
	if size < uint32(d.cipher.aead.Overhead()) || size > uint32(max+d.cipher.aead.Overhead()) {
		return 0, nil, ErrTooLarge
	}
	sealed := make([]byte, size)
	if _, err := io.ReadFull(d.in, sealed); err != nil {
		return 0, nil, errors.Join(ErrMalformed, err)
	}
	nonce, aad, err := d.cipher.recordFields(kind)
	if err != nil {
		return 0, nil, err
	}
	plain, err := d.cipher.aead.Open(sealed[:0], nonce[:], sealed, aad[:])
	if err != nil {
		return 0, nil, ErrAuthentication
	}
	return kind, plain, nil
}

func (d *StreamDecoder) ReadHead() ([]byte, error) {
	if d.head {
		return nil, ErrMalformed
	}
	kind, head, err := d.readRecord(streamMaxHead)
	if err != nil {
		d.terminalErr = err
		return nil, err
	}
	if kind != streamHead {
		d.terminalErr = ErrMalformed
		return nil, ErrMalformed
	}
	d.head = true
	return head, nil
}

func (d *StreamDecoder) Read(p []byte) (int, error) {
	if d.terminalErr != nil {
		return 0, d.terminalErr
	}
	if !d.head {
		return 0, ErrMalformed
	}
	if len(p) == 0 {
		return 0, nil
	}
	for len(d.chunk) == 0 && !d.finished {
		kind, plain, err := d.readRecord(StreamChunkSize)
		if err != nil {
			d.terminalErr = err
			return 0, err
		}
		switch kind {
		case streamData:
			if len(plain) == 0 || uint64(len(plain)) > d.maxBody-d.total {
				d.terminalErr = ErrTooLarge
				return 0, ErrTooLarge
			}
			d.total += uint64(len(plain))
			d.chunk = plain
			d.chunkBacking = plain
		case streamEnd:
			if len(plain) != 8 || binary.BigEndian.Uint64(plain) != d.total {
				d.terminalErr = ErrAuthentication
				return 0, ErrAuthentication
			}
			var extra [1]byte
			n, readErr := d.in.Read(extra[:])
			if n != 0 || readErr != io.EOF {
				d.terminalErr = ErrMalformed
				return 0, ErrMalformed
			}
			d.finished = true
		default:
			d.terminalErr = ErrMalformed
			return 0, ErrMalformed
		}
	}
	if d.finished {
		return 0, io.EOF
	}
	n := copy(p, d.chunk)
	d.chunk = d.chunk[n:]
	if len(d.chunk) == 0 {
		clear(d.chunkBacking)
		d.chunkBacking = nil
	}
	return n, nil
}

func (d *StreamDecoder) Close() error {
	clear(d.chunkBacking)
	d.chunkBacking = nil
	d.chunk = nil
	if closer, ok := d.in.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func writeStreamAll(out io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := out.Write(p)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}
