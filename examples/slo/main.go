package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/oarkflow/fh"
)

func main() {
	app := fh.New()

	// Create SLO tracker with alert callbacks
	tracker := fh.NewSLOTracker(fh.SLOTrackerConfig{
		CheckInterval:     5 * time.Second,
		AlertThreshold:    2.0,
		MaxLatencySamples: 10000,
		OnAlert: func(route string, state fh.SLOSnapshot) {
			fmt.Printf("ALERT: SLO violation on %s (burn_rate=%.2f, budget_remaining=%.2f%%)\n",
				route, state.BurnRate, state.ErrorBudgetRemaining*100)
		},
		OnRecovery: func(route string, state fh.SLOSnapshot) {
			fmt.Printf("RECOVERED: SLO back to normal on %s\n", route)
		},
	})
	defer tracker.Stop()

	// Register SLOs for routes — static, dynamic, wildcard, and regex patterns
	tracker.Register("/api/users", fh.SLO{
		Availability:   0.999, // 99.9% availability
		P99Latency:     200 * time.Millisecond,
		P95Latency:     100 * time.Millisecond,
		BurnRateWindow: 5 * time.Minute,
	})

	tracker.Register("/api/orders/:id", fh.SLO{
		Availability:   0.9999, // 99.99% availability
		P99Latency:     500 * time.Millisecond,
		BurnRateWindow: 5 * time.Minute,
	})

	tracker.Register("/files/*", fh.SLO{
		Availability:   0.99, // 99% availability
		P99Latency:     time.Second,
		BurnRateWindow: 10 * time.Minute,
	})

	tracker.Register(`^/api/v[0-9]+/reports$`, fh.SLO{
		Availability:   0.995, // 99.5% availability
		P99Latency:     800 * time.Millisecond,
		BurnRateWindow: 5 * time.Minute,
	})

	// Single middleware matches every request against all registered patterns
	app.Use(tracker.Handler())

	app.Get("/api/users", func(c fh.Ctx) error {
		// Simulate variable latency
		time.Sleep(time.Duration(rand.Intn(150)) * time.Millisecond)
		return c.JSON(fh.Map{"users": []string{"alice", "bob"}})
	})

	app.Get("/api/orders/:id", func(c fh.Ctx) error {
		time.Sleep(time.Duration(rand.Intn(300)) * time.Millisecond)
		return c.JSON(fh.Map{"order": c.Param("id")})
	})

	app.Get("/files/*", func(c fh.Ctx) error {
		return c.JSON(fh.Map{"file": c.Path()})
	})

	app.Get("/api/v1/reports", func(c fh.Ctx) error {
		time.Sleep(time.Duration(rand.Intn(400)) * time.Millisecond)
		return c.JSON(fh.Map{"reports": []string{"daily", "weekly"}})
	})

	// SLO dashboard endpoint
	app.Get("/admin/slo", func(c fh.Ctx) error {
		return c.JSON(tracker.Snapshot())
	})

	fmt.Println("SLO tracking example on :3000")
	fmt.Println("  GET /api/users        - 99.9% avail, 200ms P99 (static)")
	fmt.Println("  GET /api/orders/:id   - 99.99% avail, 500ms P99 (dynamic)")
	fmt.Println("  GET /files/*          - 99% avail, 1s P99 (wildcard)")
	fmt.Println("  GET /api/v1/reports   - 99.5% avail, 800ms P99 (regex)")
	fmt.Println("  GET /admin/slo        - SLO dashboard")
	app.Listen(":3000")
}
