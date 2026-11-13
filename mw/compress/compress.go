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

	// DisableBreachProtection explicitly opts out of the safe default. Set this
	// when compression of responses with sensitive headers is intentional.
	DisableBreachProtection bool
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
		// Ensure the header survives handler code that replaces Vary, even when
		// this particular request does not negotiate a compressed encoding.
		ctx.OnBeforeResponse(func(response fh.Ctx) error {
			response.Vary("Accept-Encoding")
			return nil
		})

		ae := ctx.Get("Accept-Encoding")

		// Pick encoder: user-supplied > negotiated best (gzip > deflate).
		enc := cfg.Encoder
		if enc == nil {
			enc = negotiateEncoder(ae, cfg.Level)
			if enc == nil {
				return ctx.Next()
			}
		} else if !acceptsEncoding(ae, enc.Encoding()) {
			return ctx.Next()
		}

		encoding := enc.Encoding()

		ctx.AddBodyTransform(func(body []byte) ([]byte, error) {
			// This runs after before-response callbacks, so retain the cache
			// dimension if a later callback replaced Vary.
			ctx.Vary("Accept-Encoding")
			if len(body) < cfg.MinSize {
				return body, nil
			}

			// Avoid double compression if a previous middleware/app already set encoding.
			if ctx.ResponseHeader("Content-Encoding") != "" {
				return body, nil
			}

			// Skip compression around authenticated or cookie-bearing requests,
			// and responses with security-sensitive headers, to reduce
			// BREACH-style compression side-channel exposure.
			if cfg.BreachProtection {
				if ctx.Get("Authorization") != "" || ctx.Get("Proxy-Authorization") != "" || ctx.Get("Cookie") != "" ||
					ctx.ResponseHeader("Set-Cookie") != "" || ctx.ResponseHeader("Content-Security-Policy") != "" || ctx.ResponseHeader("Authorization") != "" {
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
			ctx.Set("Content-Length", strconv.Itoa(dst.Len()))

			return dst.Bytes(), nil
		})

		return ctx.Next()
	}
}

// negotiateEncoder picks the best available encoder for the given Accept-Encoding header.
// Prefers gzip over deflate. Returns nil when neither is accepted.
func negotiateEncoder(acceptEncoding string, level int) Encoder {
	qualities := parseEncodingQualities(acceptEncoding)
	gzQ := qualityForEncoding(qualities, "gzip")
	deflQ := qualityForEncoding(qualities, "deflate")
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
	return qualityForEncoding(parseEncodingQualities(header), encoding)
}

type encodingQualities struct {
	gzip        float64
	hasGzip     bool
	deflate     float64
	hasDeflate  bool
	other       map[string]float64
	wildcard    float64
	hasWildcard bool
}

func parseEncodingQualities(header string) encodingQualities {
	var qualities encodingQualities
	for len(header) > 0 {
		item := header
		if i := strings.IndexByte(header, ','); i >= 0 {
			item, header = header[:i], header[i+1:]
		} else {
			header = ""
		}
		namePart, params, hasParams := strings.Cut(item, ";")
		name := strings.ToLower(trimOWS(namePart))
		if name == "" || (name != "*" && !isHTTPToken(name)) {
			continue
		}
		q := 1.0
		if hasParams && strings.Contains(params, ";") {
			q = 0
		} else if hasParams {
			key, value, ok := strings.Cut(trimOWS(params), "=")
			if !ok || !strings.EqualFold(trimOWS(key), "q") {
				q = 0
			} else if parsed, ok := parseQValue(trimOWS(value)); ok {
				q = parsed
			} else {
				q = 0
			}
		}
		if name == "*" {
			if !qualities.hasWildcard || q < qualities.wildcard {
				qualities.wildcard = q
			}
			qualities.hasWildcard = true
			continue
		}
		switch name {
		case "gzip":
			if !qualities.hasGzip || q < qualities.gzip {
				qualities.gzip = q
			}
			qualities.hasGzip = true
		case "deflate":
			if !qualities.hasDeflate || q < qualities.deflate {
				qualities.deflate = q
			}
			qualities.hasDeflate = true
		default:
			if qualities.other == nil {
				qualities.other = make(map[string]float64)
			}
			if previous, exists := qualities.other[name]; !exists || q < previous {
				qualities.other[name] = q
			}
		}
	}
	return qualities
}

func trimOWS(value string) string { return strings.Trim(value, " \t") }

func qualityForEncoding(qualities encodingQualities, encoding string) float64 {
	switch {
	case strings.EqualFold(encoding, "gzip") && qualities.hasGzip:
		return qualities.gzip
	case strings.EqualFold(encoding, "deflate") && qualities.hasDeflate:
		return qualities.deflate
	default:
		if q, exists := qualities.other[strings.ToLower(encoding)]; exists {
			return q
		}
	}
	if qualities.hasWildcard {
		return qualities.wildcard
	}
	return 0
}

func parseQValue(value string) (float64, bool) {
	if value == "0" {
		return 0, true
	}
	if value == "1" {
		return 1, true
	}
	if len(value) < 3 || value[1] != '.' || len(value) > 5 {
		return 0, false
	}
	whole := value[0]
	if whole != '0' && whole != '1' {
		return 0, false
	}
	frac := value[2:]
	for i := 0; i < len(frac); i++ {
		if frac[i] < '0' || frac[i] > '9' || (whole == '1' && frac[i] != '0') {
			return 0, false
		}
	}
	if whole == '1' {
		return 1, true
	}
	parsed, err := strconv.ParseFloat(value, 64)
	return parsed, err == nil
}

func isHTTPToken(value string) bool {
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' {
			continue
		}
		switch c {
		case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		default:
			return false
		}
	}
	return true
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
	if override.BreachProtection {
		base.BreachProtection = true
	}
	if override.DisableBreachProtection {
		base.BreachProtection = false
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

// ─────────────────────────────────────────────────────────────────────────────
// Gzip encoder
// ─────────────────────────────────────────────────────────────────────────────

type GzipEncoder struct {
	level int
	pool  sync.Pool
}

func NewGzipEncoder(level int) *GzipEncoder {
	level = normalizeCompressionLevel(level)

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
	level = normalizeCompressionLevel(level)
	return &DeflateEncoder{level: level}
}

func normalizeCompressionLevel(level int) int {
	if level == 0 || level < flate.HuffmanOnly || level > flate.BestCompression {
		return flate.BestSpeed
	}
	return level
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
	return header != "" && want != "" && encodingQuality(header, want) > 0
}
