package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/oarkflow/fh/ref/saga"
)

func main() {
	fmt.Println("=== DISTRIBUTED SAGA TRANSACTION DEMO ===")
	ctx := context.Background()
	orchestrator := saga.NewOrchestrator()

	// 1. First Step: Deduct Inventory
	orchestrator.AddStep(
		"Deduct_Inventory",
		func(ctx context.Context) error {
			time.Sleep(100 * time.Millisecond) // Simulate DB call
			// Logic: UPDATE inventory SET count = count - 1
			return nil 
		},
		func(ctx context.Context) error {
			time.Sleep(50 * time.Millisecond) // Simulate DB rollback
			// Rollback Logic: UPDATE inventory SET count = count + 1
			return nil
		},
	)

	// 2. Second Step: Charge Credit Card (External API)
	orchestrator.AddStep(
		"Charge_Credit_Card",
		func(ctx context.Context) error {
			time.Sleep(200 * time.Millisecond) // Simulate Stripe API call
			// Logic: Stripe charge successful
			return nil 
		},
		func(ctx context.Context) error {
			time.Sleep(150 * time.Millisecond) // Simulate Stripe API refund
			// Rollback Logic: Stripe refund
			return nil
		},
	)

	// 3. Third Step: Provision Cloud Server (Fails!)
	orchestrator.AddStep(
		"Provision_Cloud_Server",
		func(ctx context.Context) error {
			time.Sleep(300 * time.Millisecond) // Simulate AWS API call
			
			// DISASTER STRIKES! AWS is down.
			// This will trigger the Saga to automatically rollback Step 2 and Step 1.
			return errors.New("AWS API timeout (504 Gateway Time-out)")
		},
		func(ctx context.Context) error {
			// This won't run because the action failed.
			return nil
		},
	)

	// Execute the Saga Workflow
	fmt.Println("Starting Order Checkout Workflow...")
	err := orchestrator.Execute(ctx)
	
	fmt.Println("\n=== FINAL RESULT ===")
	if err != nil {
		fmt.Printf("Workflow Failed Gracefully: %v\n", err)
		fmt.Println("Status: Eventual Consistency Maintained (No orphaned charges or missing inventory).")
	} else {
		fmt.Println("Workflow Succeeded.")
	}
}
