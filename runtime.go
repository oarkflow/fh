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

func (a *App) EnableHealth(prefix string) *App {
	if prefix == "" {
		prefix = "/_fh"
	}
	prefix = trimRightSlash(prefix)
	a.Get(prefix+"/health", func(c Ctx) error { return c.JSON(Map{"status": "ok", "time": time.Now().UTC()}) })
	a.Get(prefix+"/live", func(c Ctx) error { return c.JSON(Map{"status": "alive"}) })
	a.Get(prefix+"/ready", func(c Ctx) error {
		ready, checks := a.HealthStatus(c.Context())
		if !ready {
			status := "unready"
			if a.IsDraining() {
				status = "draining"
			}
			return c.Status(StatusServiceUnavailable).JSON(Map{"status": status, "checks": checks})
		}
		return c.JSON(Map{"status": "ready", "checks": checks})
	})
	return a
}

func (a *App) EnableRuntime(prefix string) *App {
	if prefix == "" {
		prefix = "/_fh"
	}
	prefix = trimRightSlash(prefix)
	a.Get(prefix+"/runtime", func(c Ctx) error { return c.JSON(a.RuntimeInfo()) })
	a.EnableRouteList(prefix + "/routes")
	if a.reliability != nil && a.reliability.queue != nil {
		a.Get(prefix+"/queue/stats", func(c Ctx) error {
			st, err := a.reliability.queue.Stats()
			if err != nil {
				return err
			}
			return c.JSON(st)
		})
	}
	return a
}

func trimRightSlash(s string) string {
	for len(s) > 1 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
