package compress

import (
	"bytes"
	"compress/gzip"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/oarkflow/fh"
)

const (
	EncodingGzip = "gzip"
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

	Next func(ctx *fh.Ctx) bool
}

var DefaultConfig = Config{
	Level:   gzip.BestSpeed,
	MinSize: 512,
	CompressibleTypes: []string{
		"image/",
		"text/",
		"application/json",
		"application/xml",
		"application/javascript",
		"application/x-javascript",
		"image/svg+xml",
	},
}

func New(config ...Config) fh.HandlerFunc {
	cfg := DefaultConfig
	if len(config) > 0 {
		cfg = mergeConfig(cfg, config[0])
	}

	if cfg.Encoder == nil {
		cfg.Encoder = NewGzipEncoder(cfg.Level)
	}

	encoding := cfg.Encoder.Encoding()

	return func(ctx *fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(ctx) {
			return ctx.Next()
		}

		ae := ctx.Get("Accept-Encoding")
		if !acceptsEncoding(ae, encoding) {
			return ctx.Next()
		}

		ctx.AddBodyTransform(func(body []byte) ([]byte, error) {
			if len(body) < cfg.MinSize {
				return body, nil
			}

			// Avoid double compression if a previous middleware/app already set encoding.
			if ctx.ResponseHeader("Content-Encoding") != "" {
				return body, nil
			}

			contentType := ctx.ResponseHeader("Content-Type")
			if contentType != "" && !isCompressible(contentType, cfg.CompressibleTypes) {
				return body, nil
			}

			var dst bytes.Buffer
			dst.Grow(len(body) / 2)

			if err := cfg.Encoder.Encode(&dst, body); err != nil {
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

// -----------------------------------------------------------------------------
// Gzip encoder
// -----------------------------------------------------------------------------

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

func (e *GzipEncoder) Encoding() string {
	return EncodingGzip
}

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

// -----------------------------------------------------------------------------
// Accept-Encoding parser
// -----------------------------------------------------------------------------

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
