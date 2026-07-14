package pprof

import (
	"bytes"
	"runtime"
	rpprof "runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

type AuthFunc func(fh.Ctx) bool
type Config struct {
	Prefix string
	Auth   AuthFunc
}

func Enable(app *fh.App, cfg Config) *fh.App {
	if app == nil {
		return nil
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "/_fh/debug/pprof"
	}
	p := trim(cfg.Prefix)
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
		name := c.Param("name")
		seconds := parseInt(c.Query("seconds"))
		debug := parseInt(c.Query("debug"))
		if name == "profile" {
			if seconds <= 0 {
				seconds = 30
			}
			var buf bytes.Buffer
			if err := rpprof.StartCPUProfile(&buf); err != nil {
				return err
			}
			time.Sleep(time.Duration(seconds) * time.Second)
			rpprof.StopCPUProfile()
			c.Type("application/octet-stream")
			return c.SendBytes(buf.Bytes())
		}
		prof := rpprof.Lookup(name)
		if prof == nil {
			return c.Status(fh.StatusNotFound).JSON(fh.Map{"error": "profile_not_found"})
		}
		var buf bytes.Buffer
		if err := prof.WriteTo(&buf, debug); err != nil {
			return err
		}
		c.Type("text/plain; charset=utf-8")
		return c.SendBytes(buf.Bytes())
	})
	return app
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
