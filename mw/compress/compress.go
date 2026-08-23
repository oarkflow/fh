package compress

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/oarkflow/fh"
)

const (
	EncodingGzip    = "gzip"
	EncodingDeflate = "deflate"
)

type Encoder interface {
	Encoding() string
	Encode(dst *bytes.Buffer, src []byte) error
}

type Config struct {
	Level int

	MinSize int

	// CompressibleTypes uses prefix matching.
	CompressibleTypes []string

	Encoder Encoder

	Next func(ctx fh.Ctx) bool

	// BreachProtection skips compression when the response carries
	// security-sensitive headers (Set-Cookie, Content-Security-Policy,
	// Authorization) to mitigate BREACH-style side-channel attacks.
	BreachProtection bool
}

var DefaultConfig = Config{
	Level:   gzip.BestSpeed,
	MinSize: 512,
	CompressibleTypes: []string{
		"text/",
		"application/json",
		"application/xml",
		"application/javascript",
		"application/x-javascript",
		"image/svg+xml",
	},
	BreachProtection: true,
}

func New(config ...Config) fh.HandlerFunc {
	cfg := DefaultConfig
	if len(config) > 0 {
		cfg = mergeConfig(cfg, config[0])
	}

	return func(ctx fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(ctx) {
			return ctx.Next()
		}

		ae := ctx.Get("Accept-Encoding")

		// Pick encoder: user-supplied > negotiated best (gzip > deflate).
		enc := cfg.Encoder
		if enc == nil {
			enc = negotiateEncoder(ae, cfg.Level)
		}
		if enc == nil || !acceptsEncoding(ae, enc.Encoding()) {
			return ctx.Next()
		}

		encoding := enc.Encoding()

		ctx.AddBodyTransform(func(body []byte) ([]byte, error) {
			if len(body) < cfg.MinSize {
				return body, nil
			}

			// Avoid double compression if a previous middleware/app already set encoding.
			if ctx.ResponseHeader("Content-Encoding") != "" {
				return body, nil
			}

			// Skip compression if response has security-sensitive headers
			// to mitigate BREACH-style compression side-channel attacks.
			if cfg.BreachProtection {
				if ctx.ResponseHeader("Set-Cookie") != "" || ctx.ResponseHeader("Content-Security-Policy") != "" || ctx.ResponseHeader("Authorization") != "" {
					return body, nil
				}
			}

			contentType := ctx.ResponseHeader("Content-Type")
			if contentType != "" && !isCompressible(contentType, cfg.CompressibleTypes) {
				return body, nil
			}

			var dst bytes.Buffer
			dst.Grow(len(body) / 2)

			if err := enc.Encode(&dst, body); err != nil {
				return nil, err
			}

			if dst.Len() >= len(body) {
				return body, nil
			}

			ctx.Set("Content-Encoding", encoding)
			ctx.Append("Vary", "Accept-Encoding")
			ctx.Set("Content-Length", strconv.Itoa(dst.Len()))

			return dst.Bytes(), nil
		})

		return ctx.Next()
	}
}

// negotiateEncoder picks the best available encoder for the given Accept-Encoding header.
// Prefers gzip over deflate. Returns nil when neither is accepted.
func negotiateEncoder(acceptEncoding string, level int) Encoder {
	gzQ := encodingQuality(acceptEncoding, "gzip")
	deflQ := encodingQuality(acceptEncoding, "deflate")
	if gzQ <= 0 && deflQ <= 0 {
		return nil
	}
	if gzQ >= deflQ {
		return NewGzipEncoder(level)
	}
	return NewDeflateEncoder(level)
}

// encodingQuality returns the quality value for a specific encoding in an Accept-Encoding header.
func encodingQuality(header, encoding string) float64 {
	for len(header) > 0 {
		token := header
		if i := strings.IndexByte(header, ','); i >= 0 {
			token = strings.TrimSpace(header[:i])
			header = header[i+1:]
		} else {
			header = ""
		}
		token = strings.TrimSpace(token)
		q := 1.0
		if semi := strings.IndexByte(token, ';'); semi >= 0 {
			params := strings.TrimSpace(token[semi+1:])
			token = strings.TrimSpace(token[:semi])
			if strings.HasPrefix(params, "q=") {
				if v, err := strconv.ParseFloat(params[2:], 64); err == nil {
					q = v
				}
			}
		}
		if strings.EqualFold(token, encoding) || token == "*" {
			return q
		}
	}
	return 0
}

func mergeConfig(base Config, override Config) Config {
	if override.Level != 0 {
		base.Level = override.Level
	}
	if override.MinSize > 0 {
		base.MinSize = override.MinSize
	}
	if override.CompressibleTypes != nil {
		base.CompressibleTypes = override.CompressibleTypes
	}
	if override.Encoder != nil {
		base.Encoder = override.Encoder
	}
	if override.Next != nil {
		base.Next = override.Next
	}
	base.BreachProtection = override.BreachProtection

	return base
}

func isCompressible(contentType string, allowed []string) bool {
	if contentType == "" {
		return true
	}

	contentType = strings.ToLower(contentType)

	if i := strings.IndexByte(contentType, ';'); i >= 0 {
		contentType = strings.TrimSpace(contentType[:i])
	}

	for _, item := range allowed {
		item = strings.ToLower(strings.TrimSpace(item))
		if item == "" {
			continue
		}

		if strings.HasSuffix(item, "/") {
			if strings.HasPrefix(contentType, item) {
				return true
			}
			continue
		}

		if contentType == item {
			return true
		}
	}

	return false
}

// ─────────────────────────────────────────────────────────────────────────────
// Gzip encoder
// ─────────────────────────────────────────────────────────────────────────────

type GzipEncoder struct {
	level int
	pool  sync.Pool
}

func NewGzipEncoder(level int) *GzipEncoder {
	if level == 0 {
		level = gzip.BestSpeed
	}

	e := &GzipEncoder{
		level: level,
	}

	e.pool.New = func() any {
		w, _ := gzip.NewWriterLevel(io.Discard, level)
		return w
	}

	return e
}

func (e *GzipEncoder) Encoding() string { return EncodingGzip }

func (e *GzipEncoder) Encode(dst *bytes.Buffer, src []byte) error {
	w := e.pool.Get().(*gzip.Writer)
	w.Reset(dst)

	_, writeErr := w.Write(src)
	closeErr := w.Close()

	w.Reset(io.Discard)
	e.pool.Put(w)

	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

// ─────────────────────────────────────────────────────────────────────────────
// Deflate encoder
// ─────────────────────────────────────────────────────────────────────────────

// DeflateEncoder compresses responses using the DEFLATE algorithm (compress/flate).
// Deflate is universally supported by browsers and requires no external dependencies.
type DeflateEncoder struct {
	level int
}

// NewDeflateEncoder creates a new deflate encoder at the given compression level.
func NewDeflateEncoder(level int) *DeflateEncoder {
	if level == 0 {
		level = flate.BestSpeed
	}
	return &DeflateEncoder{level: level}
}

func (d *DeflateEncoder) Encoding() string { return EncodingDeflate }

func (d *DeflateEncoder) Encode(dst *bytes.Buffer, src []byte) error {
	w, err := flate.NewWriter(dst, d.level)
	if err != nil {
		return err
	}
	if _, err = w.Write(src); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

// ─────────────────────────────────────────────────────────────────────────────
// Accept-Encoding parser
// ─────────────────────────────────────────────────────────────────────────────

func acceptsEncoding(header string, want string) bool {
	if header == "" || want == "" {
		return false
	}

	var foundWant bool
	wantOK := false

	var foundStar bool
	starOK := false

	i := 0
	for i < len(header) {
		for i < len(header) && (header[i] == ',' || header[i] == ' ' || header[i] == '\t') {
			i++
		}
		if i >= len(header) {
			break
		}

		start := i
		for i < len(header) && header[i] != ';' && header[i] != ',' && header[i] != ' ' && header[i] != '\t' {
			i++
		}

		name := header[start:i]
		qOK := true

		for i < len(header) && header[i] != ',' {
			if header[i] == ';' {
				i++
				for i < len(header) && (header[i] == ' ' || header[i] == '\t') {
					i++
				}
				if i+2 <= len(header) && (header[i] == 'q' || header[i] == 'Q') && i+1 < len(header) && header[i+1] == '=' {
					i += 2
					qOK = !isQZeroValue(header[i:])
				}
			}
			i++
		}

		if i < len(header) && header[i] == ',' {
			i++
		}

		if strings.EqualFold(name, want) {
			foundWant = true
			wantOK = qOK
		} else if name == "*" {
			foundStar = true
			starOK = qOK
		}
	}

	if foundWant {
		return wantOK
	}
	if foundStar {
		return starOK
	}
	return false
}

func isQZeroValue(s string) bool {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	if len(s) == 0 {
		return false
	}
	if s[0] != '0' {
		return false
	}
	if len(s) == 1 {
		return true
	}
	if s[1] != '.' {
		return true
	}
	for i := 2; i < len(s); i++ {
		c := s[i]
		if c == ',' || c == ';' || c == ' ' || c == '\t' {
			return true
		}
		if c != '0' {
			return false
		}
	}
	return true
}
