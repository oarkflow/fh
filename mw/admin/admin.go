package admin

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

type AuthFunc func(fh.Ctx) bool
type AuditFunc func(ctx fh.Ctx, action string, allowed bool)
type Config struct {
	Prefix string
	Auth   AuthFunc
	Timeout time.Duration
	// AllowInsecure disables all authentication and IP allowlisting for admin
	// endpoints. This exposes runtime introspection (goroutines, routes, queue
	// internals) to any client that can reach the server. NEVER enable this in
	// production or on a network reachable from untrusted clients.
	AllowInsecure bool
	AllowedIPs    []string
	Audit         AuditFunc
}

func Enable(app *fh.App, cfg Config) *fh.App {
	if app == nil {
		return nil
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "/_fh/admin"
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.AllowInsecure && cfg.Auth == nil && len(cfg.AllowedIPs) == 0 {
		app.Logger().Warn("fh: admin.Enable called with AllowInsecure=true and no Auth or AllowedIPs — admin endpoints are fully open; restrict access in production")
		panic(fmt.Errorf("admin: AllowInsecure requires Auth or AllowedIPs"))
	}
	mw := func(c fh.Ctx) error {
		allowed := cfg.AllowInsecure || cfg.Auth != nil
		if allowed && len(cfg.AllowedIPs) > 0 {
			allowed = ipAllowed(c.IP(), cfg.AllowedIPs)
		}
		if allowed && cfg.Auth != nil {
			allowed = cfg.Auth(c)
		}
		if cfg.Audit != nil {
			cfg.Audit(c, c.Method()+" "+c.Path(), allowed)
		}
		if !allowed {
			return c.Status(fh.StatusUnauthorized).JSON(fh.Map{"error": "admin_unauthorized"})
		}
		return c.Next()
	}
	p := trim(cfg.Prefix)
	app.Get(p+"/runtime", mw, func(c fh.Ctx) error { return c.JSON(app.RuntimeInfo()) })
	app.Get(p+"/routes", mw, func(c fh.Ctx) error { return c.JSON(app.Routes()) })
	app.Get(p+"/queue/stats", mw, func(c fh.Ctx) error {
		q := app.Queue()
		if q == nil {
			return c.Status(fh.StatusNotFound).JSON(fh.Map{"error": "queue_disabled"})
		}
		st, err := q.Stats()
		if err != nil {
			return err
		}
		return c.JSON(st)
	})
	app.Get(p+"/queue/jobs", mw, func(c fh.Ctx) error {
		q := app.Queue()
		if q == nil {
			return c.Status(fh.StatusNotFound).JSON(fh.Map{"error": "queue_disabled"})
		}
		limit, _ := strconv.Atoi(c.Query("limit"))
		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
		defer cancel()
		jobs, err := q.ListJobs(ctx, c.Query("state"), limit)
		if err != nil {
			return c.Status(fh.StatusNotImplemented).JSON(fh.Map{"error": err.Error()})
		}
		return c.JSON(fh.Map{"jobs": jobs, "count": len(jobs)})
	})
	app.Post(p+"/queue/:id/retry", mw, func(c fh.Ctx) error {
		q := app.Queue()
		if q == nil {
			return c.Status(fh.StatusNotFound).JSON(fh.Map{"error": "queue_disabled"})
		}
		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
		defer cancel()
		if err := q.RetryFailed(ctx, c.Param("id")); err != nil {
			return c.Status(fh.StatusBadRequest).JSON(fh.Map{"error": err.Error()})
		}
		return c.Status(fh.StatusAccepted).JSON(fh.Map{"status": "requeued", "id": c.Param("id")})
	})
	app.Post(p+"/queue/:id/discard", mw, func(c fh.Ctx) error {
		q := app.Queue()
		if q == nil {
			return c.Status(fh.StatusNotFound).JSON(fh.Map{"error": "queue_disabled"})
		}
		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
		defer cancel()
		if err := q.DiscardFailed(ctx, c.Param("id")); err != nil {
			return c.Status(fh.StatusBadRequest).JSON(fh.Map{"error": err.Error()})
		}
		return c.Status(fh.StatusAccepted).JSON(fh.Map{"status": "discarded", "id": c.Param("id")})
	})
	app.Post(p+"/queue/purge", mw, func(c fh.Ctx) error {
		q := app.Queue()
		if q == nil {
			return c.Status(fh.StatusNotFound).JSON(fh.Map{"error": "queue_disabled"})
		}
		limit, _ := strconv.Atoi(c.Query("limit"))
		state := c.Query("state")
		before := time.Now().UTC()
		if raw := c.Query("before"); raw != "" {
			parsed, err := time.Parse(time.RFC3339, raw)
			if err != nil {
				return c.Status(fh.StatusBadRequest).JSON(fh.Map{"error": "invalid before timestamp; use RFC3339"})
			}
			before = parsed
		}
		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
		defer cancel()
		n, err := q.PurgeJobs(ctx, state, before, limit)
		if err != nil {
			return c.Status(fh.StatusBadRequest).JSON(fh.Map{"error": err.Error()})
		}
		return c.Status(fh.StatusAccepted).JSON(fh.Map{"status": "purged", "count": n, "state": state})
	})
	app.Post(p+"/drain", mw, func(c fh.Ctx) error {
		return c.Status(fh.StatusAccepted).JSON(fh.Map{"status": "drain_requires_shutdown_signal", "draining": app.IsDraining()})
	})
	return app
}
func StaticToken(header, token string) AuthFunc {
	return func(c fh.Ctx) bool { return header != "" && token != "" && fh.ConstantTimeEqual(c.Get(header), token) }
}
func trim(s string) string {
	for len(s) > 1 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
func ipAllowed(raw string, ranges []string) bool {
	ip := net.ParseIP(strings.TrimSpace(raw))
	if ip == nil {
		return false
	}
	for _, r := range ranges {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		if strings.Contains(r, "/") {
			_, n, err := net.ParseCIDR(r)
			if err == nil && n.Contains(ip) {
				return true
			}
			continue
		}
		if want := net.ParseIP(r); want != nil && want.Equal(ip) {
			return true
		}
	}
	return false
}
