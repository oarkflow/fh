package pprof

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	rpprof "runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

const defaultMaxProfileBytes = 16 << 20

var errProfileTooLarge = errors.New("pprof profile exceeds configured size limit")

type AuthFunc func(fh.Ctx) bool
type Config struct {
	Prefix          string
	Auth            AuthFunc
	MaxProfileBytes int
}

func Enable(app *fh.App, cfg Config) *fh.App {
	if app == nil {
		return nil
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "/_fh/debug/pprof"
	}
	if cfg.MaxProfileBytes <= 0 {
		cfg.MaxProfileBytes = defaultMaxProfileBytes
	}
	p := trim(cfg.Prefix)
	profileSlot := make(chan struct{}, 1)
	auth := func(c fh.Ctx) error {
		if cfg.Auth == nil || !cfg.Auth(c) {
			return c.Status(fh.StatusUnauthorized).JSON(fh.Map{"error": "pprof_unauthorized"})
		}
		return c.Next()
	}
	app.Get(p, auth, func(c fh.Ctx) error {
		names := []string{}
		for _, prof := range rpprof.Profiles() {
			names = append(names, prof.Name())
		}
		return c.JSON(fh.Map{"profiles": names, "goroutines": runtime.NumGoroutine()})
	})
	app.Get(p+"/:name", auth, func(c fh.Ctx) error {
		select {
		case profileSlot <- struct{}{}:
			defer func() { <-profileSlot }()
		default:
			return c.Status(fh.StatusTooManyRequests).JSON(fh.Map{"error": "pprof_capture_in_progress"})
		}
		name := c.Param("name")
		seconds := parseInt(c.Query("seconds"))
		debug := parseInt(c.Query("debug"))
		if name == "profile" {
			const maxProfileSeconds = 30
			if seconds <= 0 {
				seconds = 30
			}
			if seconds > maxProfileSeconds {
				seconds = maxProfileSeconds
			}
			buf := boundedProfileBuffer{limit: cfg.MaxProfileBytes}
			if err := rpprof.StartCPUProfile(&buf); err != nil {
				return err
			}
			profileActive := true
			defer func() {
				if profileActive {
					rpprof.StopCPUProfile()
				}
			}()
			timer := time.NewTimer(time.Duration(seconds) * time.Second)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-c.Done():
				if err := c.Err(); err != nil {
					return err
				}
				return context.Canceled
			}
			rpprof.StopCPUProfile()
			profileActive = false
			if buf.err != nil {
				return profileTooLarge()
			}
			c.Type("application/octet-stream")
			return c.SendBytes(buf.Bytes())
		}
		prof := rpprof.Lookup(name)
		if prof == nil {
			return c.Status(fh.StatusNotFound).JSON(fh.Map{"error": "profile_not_found"})
		}
		buf := boundedProfileBuffer{limit: cfg.MaxProfileBytes}
		if err := prof.WriteTo(&buf, debug); err != nil {
			if errors.Is(err, errProfileTooLarge) || errors.Is(buf.err, errProfileTooLarge) {
				return profileTooLarge()
			}
			return err
		}
		if buf.err != nil {
			return profileTooLarge()
		}
		c.Type("text/plain; charset=utf-8")
		return c.SendBytes(buf.Bytes())
	})
	return app
}

type boundedProfileBuffer struct {
	bytes.Buffer
	limit int
	err   error
}

func (b *boundedProfileBuffer) Write(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	if len(p) > b.limit-b.Len() {
		b.err = errProfileTooLarge
		return 0, b.err
	}
	return b.Buffer.Write(p)
}

func profileTooLarge() error {
	return fh.NewHTTPError(fh.StatusPayloadTooLarge, "PPROF_PROFILE_TOO_LARGE", "Profile exceeds the configured size limit").WithCause(errProfileTooLarge)
}

func StaticToken(header, token string) AuthFunc {
	return func(c fh.Ctx) bool { return header != "" && token != "" && fh.ConstantTimeEqual(c.Get(header), token) }
}
func trim(s string) string {
	for len(s) > 1 && strings.HasSuffix(s, "/") {
		s = s[:len(s)-1]
	}
	return s
}
func parseInt(s string) int { i, _ := strconv.Atoi(s); return i }
