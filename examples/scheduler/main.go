package main

import (
	"fmt"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/scheduler"
)

func main() {
	app := fh.New()

	sched := scheduler.New(scheduler.Config{
		MaxConcurrent: 500,
		QueueTimeout:  3 * time.Second,
		QueueSize:     2000,
		DefaultPriority: scheduler.PriorityNormal,
		PerPriority: map[scheduler.Priority]int{
			scheduler.PriorityCritical: 50,
			scheduler.PriorityHigh:     100,
			scheduler.PriorityNormal:   200,
			scheduler.PriorityLow:      50,
			scheduler.PriorityLowest:   20,
		},
		PriorityFunc: func(c fh.Ctx) scheduler.Priority {
			// Admin endpoints are critical
			if len(c.Path()) > 2 && c.Path()[:4] == "/admin" {
				return scheduler.PriorityCritical
			}
			// API requests are high priority
			if c.Get("X-Priority") == "high" {
				return scheduler.PriorityHigh
			}
			// Background jobs are low priority
			if c.Get("X-Priority") == "low" {
				return scheduler.PriorityLow
			}
			return scheduler.PriorityNormal
		},
		OnShed: func(c fh.Ctx) error {
			c.Set("Retry-After", "5")
			return c.Status(fh.StatusServiceUnavailable).JSON(fh.Map{
				"error": "overloaded",
				"msg":   "server busy, try again later",
			})
		},
	})

	// All routes go through the scheduler
	app.Use(sched.Handler())

	app.Get("/api/data", func(c fh.Ctx) error {
		time.Sleep(50 * time.Millisecond)
		return c.JSON(fh.Map{"data": "result"})
	})

	app.Get("/admin/health", func(c fh.Ctx) error {
		stats := sched.Stats()
		return c.JSON(fh.Map{
			"status":       "ok",
			"in_flight":    stats.TotalInFlight,
			"by_priority":  stats.ByPriority,
			"rejected":     stats.Rejected,
		})
	})

	app.Get("/background/job", func(c fh.Ctx) error {
		time.Sleep(200 * time.Millisecond)
		return c.JSON(fh.Map{"status": "completed"})
	})

	fmt.Println("Priority scheduler example on :3000")
	fmt.Println("  GET /api/data          - normal priority")
	fmt.Println("  GET /admin/health      - critical priority")
	fmt.Println("  GET /background/job    - low priority")
	fmt.Println("  Set X-Priority: high   - elevates to high")
	app.Listen(":3000")
}
