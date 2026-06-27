package logger

import (
	"fmt"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/zlog"
)

// Compile-time check that ZlogAdapter implements fh.Logger.
var _ fh.Logger = (*ZlogAdapter)(nil)

// ZlogAdapter wraps a *zlog.Logger to satisfy the fh.Logger interface.
type ZlogAdapter struct {
	Logger *zlog.Logger
}

func (a *ZlogAdapter) Printf(format string, args ...any) {
	a.Logger.Info(fmt.Sprintf(format, args...))
}

func (a *ZlogAdapter) Info(msg string, args ...any) {
	a.Logger.Info(msg, toZlogAttrs(args)...)
}

func (a *ZlogAdapter) Warn(msg string, args ...any) {
	a.Logger.Warn(msg, toZlogAttrs(args)...)
}

func (a *ZlogAdapter) Error(msg string, args ...any) {
	a.Logger.Error(msg, toZlogAttrs(args)...)
}

func (a *ZlogAdapter) Debug(msg string, args ...any) {
	a.Logger.Debug(msg, toZlogAttrs(args)...)
}

func toZlogAttrs(args []any) []zlog.Attr {
	if len(args) == 0 {
		return nil
	}
	attrs := make([]zlog.Attr, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		key := fmt.Sprintf("%v", args[i])
		if i+1 < len(args) {
			attrs = append(attrs, zlog.Any(key, args[i+1]))
		} else {
			attrs = append(attrs, zlog.Any(key, nil))
		}
	}
	return attrs
}
