package main

import (
	"fmt"
	"time"

	"github.com/oarkflow/fh"
)

func main() {
	app := fh.New()

	// Attach a budget to every request with a 2-second deadline
	app.Use(fh.BudgetMiddleware(fh.BudgetConfig{
		Deadline:         2 * time.Second,
		MaxCPUTime:       1 * time.Second,
		MaxQueueTime:     500 * time.Millisecond,
		MaxBodyBytes:     1 << 20, // 1MB
		MaxResponseBytes: 5 << 20, // 5MB
		MaxMemoryBytes:   64 << 20, // 64MB
		MaxUpstreamCalls: 5,
		MaxRetries:       3,
		MaxLogBytes:      1 << 20, // 1MB
	}))

	app.Get("/order/:id", func(c fh.Ctx) error {
		budget := fh.BudgetFromContext(c.Context())
		if budget == nil {
			return c.SendString("no budget")
		}

		// Carve a sub-budget for database call (700ms, 30% memory)
		dbBudget := budget.Child(700*time.Millisecond, fh.BudgetSplit{
			MemoryFraction:   0.3,
			UpstreamFraction: 0.5,
			RetryFraction:    1.0,
			LogFraction:      1.0,
		})

		// Check if we have time for the DB call
		if dbBudget.Remaining() < 100*time.Millisecond {
			return c.Status(fh.StatusServiceUnavailable).JSON(fh.Map{
				"error":     "deadline_exceeded",
				"remaining": dbBudget.Remaining().String(),
			})
		}

		// Check memory budget before large allocation
		if !dbBudget.CheckMemory(10 << 20) {
			return c.Status(fh.StatusInsufficientStorage).JSON(fh.Map{
				"error": "memory_budget_exceeded",
			})
		}

		// Simulate DB call
		time.Sleep(200 * time.Millisecond)

		return c.JSON(fh.Map{
			"order_id":         c.Param("id"),
			"budget_remaining": budget.Remaining().String(),
			"db_budget":        dbBudget.Remaining().String(),
		})
	})

	app.Post("/checkout", func(c fh.Ctx) error {
		budget := fh.BudgetFromContext(c.Context())

		// Check body size against budget
		if !budget.CheckBodySize(int64(len(c.Body()))) {
			return c.Status(fh.StatusPayloadTooLarge).JSON(fh.Map{
				"error": "body_too_large",
			})
		}

		// Simulate checkout processing
		time.Sleep(100 * time.Millisecond)

		return c.JSON(fh.Map{"status": "success"})
	})

	fmt.Println("Request budget example on :3000")
	fmt.Println("  GET /order/123   - hierarchical sub-budgets")
	fmt.Println("  POST /checkout   - body size budget check")
	app.Listen(":3000")
}
