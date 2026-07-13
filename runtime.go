package fh

import (
	"context"
	"runtime"
	"time"
)

type RuntimeInfo struct {
	Time       time.Time           `json:"time"`
	GoVersion  string              `json:"go_version"`
	Goroutines int                 `json:"goroutines"`
	Draining   bool                `json:"draining"`
	Routes     int                 `json:"routes"`
	Queue      QueueStats          `json:"queue,omitempty"`
	Config     SafeConfig          `json:"config"`
	Health     []HealthCheckResult `json:"health,omitempty"`
}

func (a *App) RuntimeInfo() RuntimeInfo {
	var q QueueStats
	if a != nil && a.reliability != nil && a.reliability.queue != nil {
		q, _ = a.reliability.queue.Stats()
	}
	_, checks := a.HealthStatus(context.Background())
	return RuntimeInfo{Time: time.Now().UTC(), GoVersion: runtime.Version(), Goroutines: runtime.NumGoroutine(), Draining: a != nil && a.IsDraining(), Routes: len(a.Routes()), Queue: q, Config: a.SafeConfig(), Health: checks}
}

func (a *App) IsDraining() bool { return a != nil && a.draining.Load() }

// EnableHealth mounts liveness/readiness routes. /ready surfaces raw
// health-check error strings (which can include DSNs or internal
// hostnames), so in deployments reachable from outside a trusted network,
// pass an auth middleware.
func (a *App) EnableHealth(prefix string, middleware ...HandlerFunc) *App {
	if prefix == "" {
		prefix = "/_fh"
	}
	prefix = trimRightSlash(prefix)
	a.Get(prefix+"/health", withHandlers(middleware, func(c Ctx) error { return c.JSON(Map{"status": "ok", "time": time.Now().UTC()}) })...)
	a.Get(prefix+"/live", withHandlers(middleware, func(c Ctx) error { return c.JSON(Map{"status": "alive"}) })...)
	a.Get(prefix+"/ready", withHandlers(middleware, func(c Ctx) error {
		ready, checks := a.HealthStatus(c.Context())
		if !ready {
			status := "unready"
			if a.IsDraining() {
				status = "draining"
			}
			return c.Status(StatusServiceUnavailable).JSON(Map{"status": status, "checks": checks})
		}
		return c.JSON(Map{"status": "ready", "checks": checks})
	})...)
	return a
}

// EnableRuntime mounts runtime/route-table/queue-stats introspection routes.
// /routes exposes the full route table annotated with which routes require
// auth — a reconnaissance map for an attacker — so pass an auth middleware
// in any deployment reachable from outside a trusted network.
func (a *App) EnableRuntime(prefix string, middleware ...HandlerFunc) *App {
	if prefix == "" {
		prefix = "/_fh"
	}
	prefix = trimRightSlash(prefix)
	a.Get(prefix+"/runtime", withHandlers(middleware, func(c Ctx) error { return c.JSON(a.RuntimeInfo()) })...)
	a.EnableRouteList(prefix+"/routes", middleware...)
	if a.reliability != nil && a.reliability.queue != nil {
		a.Get(prefix+"/queue/stats", withHandlers(middleware, func(c Ctx) error {
			st, err := a.reliability.queue.Stats()
			if err != nil {
				return err
			}
			return c.JSON(st)
		})...)
	}
	return a
}

func trimRightSlash(s string) string {
	for len(s) > 1 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
