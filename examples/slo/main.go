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

	// Register SLOs for routes
	tracker.Register("/api/users", fh.SLO{
		Availability: 0.999,   // 99.9% availability
		P99Latency:   200 * time.Millisecond,
		P95Latency:   100 * time.Millisecond,
		BurnRateWindow: 5 * time.Minute,
	})

	tracker.Register("/api/orders", fh.SLO{
		Availability: 0.9999,  // 99.99% availability
		P99Latency:   500 * time.Millisecond,
		BurnRateWindow: 5 * time.Minute,
	})

	// Apply SLO middleware
	app.Use(fh.SLOMiddleware(tracker, "/api/users"))
	app.Use(fh.SLOMiddleware(tracker, "/api/orders"))

	app.Get("/api/users", func(c fh.Ctx) error {
		// Simulate variable latency
		time.Sleep(time.Duration(rand.Intn(150)) * time.Millisecond)
		return c.JSON(fh.Map{"users": []string{"alice", "bob"}})
	})

	app.Get("/api/orders", func(c fh.Ctx) error {
		time.Sleep(time.Duration(rand.Intn(300)) * time.Millisecond)
		return c.JSON(fh.Map{"orders": []string{"order-1", "order-2"}})
	})

	// SLO dashboard endpoint
	app.Get("/admin/slo", func(c fh.Ctx) error {
		snapshot := tracker.Snapshot()
		return c.JSON(snapshot)
	})

	fmt.Println("SLO tracking example on :3000")
	fmt.Println("  GET /api/users   - 99.9% avail, 200ms P99")
	fmt.Println("  GET /api/orders  - 99.99% avail, 500ms P99")
	fmt.Println("  GET /admin/slo   - SLO dashboard")
	app.Listen(":3000")
}
