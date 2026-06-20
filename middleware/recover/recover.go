package recover

import (
	"fmt"
	"log"
	"runtime/debug"

	"github.com/oarkflow/fh"
)

type Logger interface {
	Printf(format string, args ...any)
}

type PanicHandler func(ctx *fh.Ctx, recovered any, stack []byte) error

type Config struct {
	EnableStackTrace bool
	StackTraceLimit  int

	Logger Logger
	Handler PanicHandler
}

func New(config ...Config) fh.HandlerFunc {
	cfg := defaultConfig()
	if len(config) > 0 {
		cfg = mergeConfig(cfg, config[0])
	}

	return func(ctx *fh.Ctx) (err error) {
		defer func() {
			if r := recover(); r != nil {
				var stack []byte
				if cfg.EnableStackTrace {
					stack = debug.Stack()
					if cfg.StackTraceLimit > 0 && len(stack) > cfg.StackTraceLimit {
						stack = stack[:cfg.StackTraceLimit]
					}
				}

				if cfg.Logger != nil {
					if len(stack) > 0 {
						cfg.Logger.Printf("panic recovered: %v\n%s", r, stack)
					} else {
						cfg.Logger.Printf("panic recovered: %v", r)
					}
				}

				err = cfg.Handler(ctx, r, stack)
			}
		}()

		return ctx.Next()
	}
}

func defaultConfig() Config {
	return Config{
		EnableStackTrace: true,
		StackTraceLimit:  64 << 10,
		Logger:           log.Default(),
		Handler:          DefaultHandler,
	}
}

func mergeConfig(base Config, override Config) Config {
	base.EnableStackTrace = override.EnableStackTrace

	if override.StackTraceLimit > 0 {
		base.StackTraceLimit = override.StackTraceLimit
	}
	if override.Logger != nil {
		base.Logger = override.Logger
	}
	if override.Handler != nil {
		base.Handler = override.Handler
	}

	return base
}

func DefaultHandler(ctx *fh.Ctx, recovered any, stack []byte) error {
	ctx.Set("Content-Type", "text/plain; charset=utf-8")
	return ctx.Status(500).SendString("Internal Server Error")
}

func Error(recovered any) error {
	return fmt.Errorf("panic: %v", recovered)
}
