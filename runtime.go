package fh

import (
	"context"
	"runtime"
	"time"
)

type RuntimeInfo struct {
	Time                  time.Time           `json:"time"`
	GoVersion             string              `json:"go_version"`
	Goroutines            int                 `json:"goroutines"`
	Draining              bool                `json:"draining"`
	Routes                int                 `json:"routes"`
	Queue                 QueueStats          `json:"queue,omitempty"`
	Config                SafeConfig          `json:"config"`
	Health                []HealthCheckResult `json:"health,omitempty"`
	Kernel                KernelRuntimeInfo   `json:"kernel"`
	BufferedRequestBytes  int64               `json:"buffered_request_bytes"`
	BufferedResponseBytes int64               `json:"buffered_response_bytes"`
	BodyBudgetRejections  uint64              `json:"body_budget_rejections"`
}

func (a *App) RuntimeInfo() RuntimeInfo {
	var q QueueStats
	if a != nil && a.reliability != nil && a.reliability.queue != nil {
		var err error
		q, err = a.reliability.queue.Stats()
		if err != nil {
			a.Logger().Warn("fh: failed to collect queue stats", "error", err)
		}
	}
	_, checks := a.HealthStatus(context.Background())
	return RuntimeInfo{Time: time.Now().UTC(), GoVersion: runtime.Version(), Goroutines: runtime.NumGoroutine(), Draining: a != nil && a.IsDraining(), Routes: len(a.Routes()), Queue: q, Config: a.SafeConfig(), Health: checks, Kernel: a.KernelRuntimeInfo(), BufferedRequestBytes: a.bufferedRequestBytes.Load(), BufferedResponseBytes: a.bufferedResponseBytes.Load(), BodyBudgetRejections: a.bodyBudgetRejections.Load()}
}

func (a *App) IsDraining() bool { return a != nil && a.draining.Load() }

// EnableHealth mounts liveness/readiness routes. The built-in readiness
// response reports aggregate status only; use HealthStatus directly for
// detailed diagnostics and protect any endpoint that exposes those details.
func (a *App) EnableHealth(prefix string, middleware ...HandlerFunc) *App {
	if prefix == "" {
		prefix = "/_fh"
	}
	prefix = trimRightSlash(prefix)
	a.Get(prefix+"/health", withHandlers(middleware, func(c Ctx) error { return c.JSON(Map{"status": "ok", "time": time.Now().UTC()}) })...)
	a.Get(prefix+"/live", withHandlers(middleware, func(c Ctx) error { return c.JSON(Map{"status": "alive"}) })...)
	a.Get(prefix+"/ready", withHandlers(middleware, func(c Ctx) error {
		if a.IsDraining() {
			return c.Status(StatusServiceUnavailable).JSON(Map{"status": "draining"})
		}
		ready, _ := a.HealthStatus(c.Context())
		if !ready {
			return c.Status(StatusServiceUnavailable).JSON(Map{"status": "unready"})
		}
		return c.JSON(Map{"status": "ready"})
	})...)
	return a
}

// EnableRuntime mounts runtime/route-table/queue-stats introspection routes.
// /runtime and /routes expose operational details, including health-check
// diagnostics and the route table, so pass an auth middleware in deployments
// reachable from outside a trusted network.
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
