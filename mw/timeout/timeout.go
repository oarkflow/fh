package timeout

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/oarkflow/fh"
)

var (
	ErrTimeout        = errors.New("request timeout")
	ErrInvalidTimeout = errors.New("invalid timeout duration")
)

const (
	defaultTimeout    = 30 * time.Second
	defaultStatusCode = 503
	defaultMessage    = "service unavailable: request timed out"
)

// Skipper allows selectively skipping timeout middleware.
type Skipper func(fh.Ctx) bool

// TimeoutHandler writes the timeout response.
type TimeoutHandler func(fh.Ctx, error) error

// ErrorHandler handles non-timeout errors returned by downstream handlers.
type ErrorHandler func(fh.Ctx, error) error

// Config configures timeout middleware.
//
// This middleware is intentionally cooperative and production-safe.
// It sets c.Context() to a deadline context and expects long-running handlers,
// database calls, queues, HTTP clients, and business logic to observe
// c.Context().Done().
//
// It does not forcibly kill the handler goroutine because fh.Ctx is usually
// pooled and response writing after timeout can cause races or corrupted output.
type Config struct {
	// Timeout is the maximum time allowed for downstream middleware/handler work.
	// If <= 0, DefaultTimeout is used unless RejectInvalidTimeout is true.
	Timeout time.Duration

	// StatusCode is used by the default timeout response.
	// Defaults to 503.
	StatusCode int

	// Message is used by the default timeout response.
	Message string

	// HeaderName is set on timeout responses.
	// Defaults to "X-Timeout".
	HeaderName string

	// HeaderValue is set on timeout responses.
	// Defaults to the timeout duration string.
	HeaderValue string

	// Skipper skips timeout handling for selected requests.
	Skipper Skipper

	// OnTimeout customizes timeout response behavior.
	OnTimeout TimeoutHandler

	// OnError customizes non-timeout error handling.
	OnError ErrorHandler

	// RejectInvalidTimeout makes NewWithConfig return middleware that rejects
	// every request with 500 if Timeout <= 0. Usually keep this false.
	RejectInvalidTimeout bool

	// PreserveContext controls whether the previous context is restored after
	// the request returns. This should normally stay true.
	PreserveContext bool
}

// New returns timeout middleware using the given duration.
func New(timeout time.Duration) fh.HandlerFunc {
	return NewWithConfig(Config{
		Timeout:         timeout,
		PreserveContext: true,
	})
}

// NewWithConfig returns production-safe timeout middleware.
func NewWithConfig(cfg Config) fh.HandlerFunc {
	cfg = normalize(cfg)

	if cfg.RejectInvalidTimeout && cfg.Timeout <= 0 {
		return func(c fh.Ctx) error {
			return c.Status(500).SendString(ErrInvalidTimeout.Error())
		}
	}

	return func(c fh.Ctx) error {
		if cfg.Skipper != nil && cfg.Skipper(c) {
			return c.Next()
		}

		parent := c.Context()
		if parent == nil {
			parent = context.Background()
		}

		deadlineCtx, cancel := context.WithTimeout(parent, cfg.Timeout)
		defer cancel()

		c.SetContext(deadlineCtx)

		if cfg.PreserveContext {
			defer c.SetContext(parent)
		}

		err := c.Next()

		if isTimedOut(deadlineCtx, err) {
			return handleTimeout(c, cfg, err)
		}

		if err != nil && cfg.OnError != nil {
			return cfg.OnError(c, err)
		}

		return err
	}
}

func normalize(cfg Config) Config {
	if cfg.Timeout <= 0 && !cfg.RejectInvalidTimeout {
		cfg.Timeout = defaultTimeout
	}

	if cfg.StatusCode == 0 {
		cfg.StatusCode = defaultStatusCode
	}

	if cfg.Message == "" {
		cfg.Message = defaultMessage
	}

	if cfg.HeaderName == "" {
		cfg.HeaderName = "X-Timeout"
	}

	if cfg.HeaderValue == "" && cfg.Timeout > 0 {
		cfg.HeaderValue = cfg.Timeout.String()
	}

	if !cfg.PreserveContext {
		// Production default should preserve parent context.
		cfg.PreserveContext = true
	}

	return cfg
}

func isTimedOut(ctx context.Context, err error) bool {
	if ctx == nil {
		return false
	}

	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return true
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	if errors.Is(err, ErrTimeout) {
		return true
	}

	return false
}

func handleTimeout(c fh.Ctx, cfg Config, err error) error {
	if cfg.HeaderName != "" {
		value := cfg.HeaderValue
		if value == "" {
			value = strconv.FormatInt(int64(cfg.Timeout/time.Millisecond), 10) + "ms"
		}
		c.Set(cfg.HeaderName, value)
	}

	if cfg.OnTimeout != nil {
		if err == nil {
			err = ErrTimeout
		}
		return cfg.OnTimeout(c, err)
	}

	return c.Status(cfg.StatusCode).SendString(cfg.Message)
}
