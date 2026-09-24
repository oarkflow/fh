package main

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/fh/ref/eventbus"
)

// ReadModel represents the UI-optimized projection of our data (CQRS)
type ReadModel struct {
	UserBalances map[string]float64
}

func main() {
	fmt.Println("=== EVENT SOURCING & CQRS DEMO ===")
	
	bus := eventbus.NewBus()
	ctx := context.Background()

	// The Read Model Database (optimized for fast UI queries)
	readModel := &ReadModel{
		UserBalances: make(map[string]float64),
	}

	// 1. Subscribe a Projector to update the Read Model asynchronously
	bus.Subscribe("BalanceDeposited", func(ctx context.Context, e eventbus.Event) error {
		userID := e.Aggregate
		amount := e.Payload["amount"].(float64)
		
		time.Sleep(50 * time.Millisecond) // Simulate DB write
		readModel.UserBalances[userID] += amount
		fmt.Printf("[PROJECTOR] Read Model Updated: User %s balance is now $%.2f\n", userID, readModel.UserBalances[userID])
		return nil
	})

	bus.Subscribe("BalanceWithdrawn", func(ctx context.Context, e eventbus.Event) error {
		userID := e.Aggregate
		amount := e.Payload["amount"].(float64)
		
		time.Sleep(50 * time.Millisecond) // Simulate DB write
		readModel.UserBalances[userID] -= amount
		fmt.Printf("[PROJECTOR] Read Model Updated: User %s balance is now $%.2f\n", userID, readModel.UserBalances[userID])
		return nil
	})

	// 2. The Business Command (The "Write" Path)
	// Instead of updating a row directly, we generate Immutable Events.
	fmt.Println("\nExecuting Business Commands...")
	events := []eventbus.Event{
		{
			ID:        "evt-1",
			Aggregate: "user-99",
			Type:      "BalanceDeposited",
			Payload:   map[string]any{"amount": 100.00},
			Timestamp: time.Now(),
		},
		{
			ID:        "evt-2",
			Aggregate: "user-99",
			Type:      "BalanceWithdrawn",
			Payload:   map[string]any{"amount": 35.50},
			Timestamp: time.Now(),
		},
	}

	// Publish the events to the event store/bus
	bus.Publish(ctx, events...)

	// Wait for async projectors to finish
	time.Sleep(200 * time.Millisecond)

	// 3. UI Query (The "Read" Path)
	fmt.Println("\n=== UI QUERY ===")
	fmt.Printf("User 99 Current Balance (from Read Model): $%.2f\n", readModel.UserBalances["user-99"])
}
