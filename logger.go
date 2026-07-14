package fh

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
)

// Logger is the interface for application-level logging in fh.
// It supports both printf-style (Printf) and structured leveled logging
// (Info, Warn, Error, Debug) with key-value pair arguments, following
// the same convention as log/slog.
//
// Implementations can adapt any third-party logger by wrapping it.
type Logger interface {
	Printf(format string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	Debug(msg string, args ...any)
}

type slogLogger struct {
	l *slog.Logger
}

// NewDefaultLogger returns the default fh Logger backed by log/slog.
// It writes text-formatted output to stderr at debug level and above.
func NewDefaultLogger() Logger { return newSlogLogger() }

func newSlogLogger() *slogLogger {
	return &slogLogger{
		l: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
	}
}

func (s *slogLogger) Printf(format string, args ...any) {
	s.l.Info(fmt.Sprintf(format, args...))
}

func (s *slogLogger) Info(msg string, args ...any) { s.l.Info(msg, args...) }

func (s *slogLogger) Warn(msg string, args ...any) { s.l.Warn(msg, args...) }

func (s *slogLogger) Error(msg string, args ...any) { s.l.Error(msg, args...) }

func (s *slogLogger) Debug(msg string, args ...any) { s.l.Debug(msg, args...) }

// LogAdapter wraps a standard library *log.Logger to satisfy the Logger
// interface. Use it when migrating from std log to the fh.Logger interface
// without changing existing logger setup.
type LogAdapter struct {
	Logger *log.Logger
}

func (a *LogAdapter) Printf(format string, args ...any) {
	a.Logger.Printf(format, args...)
}

func (a *LogAdapter) Info(msg string, args ...any) {
	a.log("INFO", msg, args...)
}

func (a *LogAdapter) Warn(msg string, args ...any) {
	a.log("WARN", msg, args...)
}

func (a *LogAdapter) Error(msg string, args ...any) {
	a.log("ERROR", msg, args...)
}

func (a *LogAdapter) Debug(msg string, args ...any) {
	a.log("DEBUG", msg, args...)
}

func (a *LogAdapter) log(level, msg string, args ...any) {
	if len(args) == 0 {
		a.Logger.Printf("[%s] %s", level, msg)
		return
	}
	var b strings.Builder
	b.WriteString("[")
	b.WriteString(level)
	b.WriteString("] ")
	b.WriteString(msg)
	for i := 0; i < len(args); i += 2 {
		b.WriteString(" ")
		if i+1 < len(args) {
			b.WriteString(fmt.Sprintf("%v=%v", args[i], args[i+1]))
		} else {
			b.WriteString(fmt.Sprintf("%v=<missing>", args[i]))
		}
	}
	a.Logger.Print(b.String())
}

// Logger returns the application logger so middleware and integrations can emit
// structured messages without reaching into App internals.
func (a *App) Logger() Logger {
	if a == nil || a.logger == nil {
		return NewDefaultLogger()
	}
	return a.logger
}

// NewNoopLogger returns a Logger that silently discards all log messages.
//
// It is useful for benchmarks, tests, embedded applications, or deployments
// where fh logging is handled elsewhere.
func NewNoopLogger() Logger {
	return noopLoggerInstance
}

var noopLoggerInstance Logger = noopLogger{}

type noopLogger struct{}

func (noopLogger) Printf(string, ...any) {}
func (noopLogger) Info(string, ...any)   {}
func (noopLogger) Warn(string, ...any)   {}
func (noopLogger) Error(string, ...any)  {}
func (noopLogger) Debug(string, ...any)  {}
