package requestid

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

const (
	DefaultHeader   = "X-Request-ID"
	DefaultLocalKey = "requestID"

	defaultMaxIncomingLen = 128
)

var ErrInvalidRequestID = errors.New("requestid: invalid request id")

type Generator interface {
	Generate(ctx *fh.Ctx) string
}

type Validator func(id string) bool

type ErrorHandler func(ctx *fh.Ctx, err error) error

type Config struct {
	Header   string
	LocalKey string

	// TrustIncoming controls whether an incoming request ID is accepted.
	TrustIncoming bool

	// MaxIncomingLength prevents oversized or malicious request IDs.
	MaxIncomingLength int

	Generator Generator
	Validator Validator

	ErrorHandler ErrorHandler
}

func New(config ...Config) fh.HandlerFunc {
	cfg := defaultConfig()
	if len(config) > 0 {
		cfg = mergeConfig(cfg, config[0])
	}

	return func(ctx *fh.Ctx) error {
		id := ""

		if cfg.TrustIncoming {
			incoming := ctx.Get(cfg.Header)
			if incoming != "" {
				if len(incoming) > cfg.MaxIncomingLength || !cfg.Validator(incoming) {
					return cfg.ErrorHandler(ctx, ErrInvalidRequestID)
				}
				id = incoming
			}
		}

		if id == "" {
			id = cfg.Generator.Generate(ctx)
		}

		ctx.Set(cfg.Header, id)
		ctx.Locals(cfg.LocalKey, id)

		return ctx.Next()
	}
}

func defaultConfig() Config {
	return Config{
		Header:            DefaultHeader,
		LocalKey:          DefaultLocalKey,
		TrustIncoming:     true,
		MaxIncomingLength: defaultMaxIncomingLen,
		Generator:         NewAtomicGenerator(),
		Validator:         DefaultValidator,
		ErrorHandler: func(ctx *fh.Ctx, err error) error {
			return ctx.Status(400).SendString("Invalid Request ID")
		},
	}
}

func mergeConfig(base Config, override Config) Config {
	if override.Header != "" {
		base.Header = override.Header
	}
	if override.LocalKey != "" {
		base.LocalKey = override.LocalKey
	}
	if override.MaxIncomingLength > 0 {
		base.MaxIncomingLength = override.MaxIncomingLength
	}
	if override.Generator != nil {
		base.Generator = override.Generator
	}
	if override.Validator != nil {
		base.Validator = override.Validator
	}
	if override.ErrorHandler != nil {
		base.ErrorHandler = override.ErrorHandler
	}

	base.TrustIncoming = override.TrustIncoming

	return base
}

func DefaultValidator(id string) bool {
	if id == "" {
		return false
	}

	for i := 0; i < len(id); i++ {
		c := id[i]

		if (c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') ||
			c == '-' ||
			c == '_' ||
			c == '.' ||
			c == ':' {
			continue
		}

		return false
	}

	return true
}

// AtomicGenerator is fast, deterministic, sortable-ish and safe for high throughput.
// Format:
//
//	<node-prefix>-<unixnano-base36>-<counter-base36>
type AtomicGenerator struct {
	prefix  string
	counter atomic.Uint64
}

func NewAtomicGenerator() *AtomicGenerator {
	return &AtomicGenerator{
		prefix: defaultNodePrefix(),
	}
}

func NewAtomicGeneratorWithPrefix(prefix string) *AtomicGenerator {
	if prefix == "" {
		prefix = defaultNodePrefix()
	}
	return &AtomicGenerator{
		prefix: sanitizePrefix(prefix),
	}
}

func (g *AtomicGenerator) Generate(ctx *fh.Ctx) string {
	n := g.counter.Add(1)

	var buf [128]byte
	m := copy(buf[:], g.prefix)

	buf[m] = '-'
	m++

	m += len(strconv.AppendInt(buf[m:m], time.Now().UnixNano(), 36))

	buf[m] = '-'
	m++

	m += len(strconv.AppendUint(buf[m:m], n, 36))

	return string(buf[:m])
}

func defaultNodePrefix() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err == nil {
		return base64.RawURLEncoding.EncodeToString(b[:])
	}

	host, _ := os.Hostname()
	if host == "" {
		host = "node"
	}

	return sanitizePrefix(host) + "-" + strconv.Itoa(os.Getpid())
}

func sanitizePrefix(s string) string {
	if s == "" {
		return "node"
	}

	var buf [128]byte
	n := 0

	for i := 0; i < len(s) && n < len(buf); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z':
			buf[n] = c
			n++
		case c >= 'A' && c <= 'Z':
			buf[n] = c
			n++
		case c >= '0' && c <= '9':
			buf[n] = c
			n++
		case c == '-', c == '_':
			buf[n] = c
			n++
		}
	}

	if n == 0 {
		return "node"
	}

	return string(buf[:n])
}
