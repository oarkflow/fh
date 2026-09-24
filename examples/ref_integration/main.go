package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/ref"
	"github.com/oarkflow/ref/capability"
	"github.com/oarkflow/ref/effect"
	"github.com/oarkflow/ref/eventbus"
	"github.com/oarkflow/ref/execution"
	"github.com/oarkflow/ref/fact"
	"github.com/oarkflow/ref/graph"
	"github.com/oarkflow/ref/intent"
	"github.com/oarkflow/ref/invocation"
	"github.com/oarkflow/ref/saga"
)

// =====================================================================
// 1. ADAPTER: fh <-> ref (Zero Dependency Decoupling)
// =====================================================================

type refAdapter struct {
	engine *ref.Engine
}

func (a *refAdapter) Capabilities() any { return a.engine.Capabilities() }
func (a *refAdapter) Intents() any      { return a.engine.Intents() }
func (a *refAdapter) RegisterDefinition(def any) error {
	return a.engine.RegisterDefinition(def.(*intent.Definition))
}
func (a *refAdapter) Compile() error { return a.engine.Compile() }
func (a *refAdapter) Dispatch(ctx context.Context, inv any) (any, error) {
	return a.engine.Dispatch(ctx, inv.(*invocation.Invocation))
}
func (a *refAdapter) DispatchPreview(ctx context.Context, inv any) (any, error) {
	return a.engine.DispatchPreview(ctx, inv.(*invocation.Invocation))
}
func (a *refAdapter) Plan(name string) (any, bool) {
	return a.engine.Plan(intent.Name(name))
}

// =====================================================================
// 2. DOMAIN FACTS (Strictly Typed Data)
// =====================================================================

type CheckoutRequest struct {
	UserID string `json:"user_id"`
	ItemID string `json:"item_id"`
	Qty    int    `json:"qty"`
}

type OrderResult struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

var (
	KeyCheckoutReq = fact.NewKey[CheckoutRequest]("checkout.request")
	KeyInventory   = fact.NewKey[bool]("inventory.available")
)

// =====================================================================
// 3. ENGINE SETUP (CQRS, Sagas, Capabilities, Intents, Budgets)
// =====================================================================

func buildEngine() *ref.Engine {
	fmt.Println("=> Initializing REF Engine Features...")

	// Feature A: CQRS / Event Sourcing (Tier-0 Event Bus)
	bus := eventbus.NewBus()
	
	bus.Subscribe("OrderPlacedEvent", func(ctx context.Context, e eventbus.Event) error {
		req := e.Payload["req"].(CheckoutRequest)
		fmt.Printf("[CQRS/Async] Projected Order to Read-Model for User %s\n", req.UserID)
		return nil
	})

	bus.Subscribe("OrderFailedEvent", func(ctx context.Context, e eventbus.Event) error {
		fmt.Printf("[CQRS/Async] Received Order Failure Event: %v\n", e.Payload["req"])
		return nil
	})

	// Initialize the engine
	engine := ref.NewEngine()

	// Feature B: Capabilities & Execution Graph
	// This simulates a database lookup that is safely isolated.
	_ = engine.Capabilities().Register(capability.Registration{
		Name:        "CheckInventory",
		Kind:        graph.ReadNode,
		Speculation: graph.PreAuthSafe, // Allowed to run concurrently with Identity
		Requires:    []fact.AnyKey{KeyCheckoutReq.Any()},
		Provides:    []fact.AnyKey{KeyInventory.Any()},
		Run: func(nc *execution.NodeContext) error {
			slot, _ := nc.SlotOf(KeyCheckoutReq.DefinitionID())
			req, _ := fact.Get[CheckoutRequest](nc.Facts(), slot)

			fmt.Printf("[Capability] Checking inventory for Item %s (Qty: %d)\n", req.ItemID, req.Qty)
			// Business Logic: Only 5 in stock
			available := req.Qty <= 5

			outSlot, _ := nc.SlotOf(KeyInventory.DefinitionID())
			fact.Put(nc.Facts(), outSlot, available)
			return nil
		},
	})

	// Feature C: Intent Definition (The Business Workflow)
	checkoutIntent := intent.Definition{
		Name: "ProcessCheckout",
		Spec: intent.Spec{
			Requires: []fact.AnyKey{KeyCheckoutReq.Any(), KeyInventory.Any()},
			// Feature D: Execution Budgets / Loadshedding
			Timeout:       2 * time.Second, // Max time before aborting
			MaxMemory:     1024 * 1024 * 5, // 5MB limit
			MaxDBQueries:  10,              // Guardrails
		},
		InputKey: KeyCheckoutReq.Any(),
		
		// Decode input manually (since we aren't using the built-in JSON HTTP decoder)
		DecodeNode: func(nc *execution.NodeContext) error {
			var req CheckoutRequest
			if err := json.Unmarshal(nc.Invocation().Input.RawBytes(), &req); err != nil {
				return err
			}
			slot, _ := nc.SlotOf(KeyCheckoutReq.DefinitionID())
			fact.Put(nc.Facts(), slot, req)
			return nil
		},

		// The Core Business Logic
		Run: func(nc *execution.NodeContext) (any, effect.EffectPlan, intent.OutcomeMeta, error) {
			s1, _ := nc.SlotOf(KeyCheckoutReq.DefinitionID())
			req, _ := fact.Get[CheckoutRequest](nc.Facts(), s1)

			s2, _ := nc.SlotOf(KeyInventory.DefinitionID())
			available, _ := fact.Get[bool](nc.Facts(), s2)

			if !available {
				// Simulate notifying CQRS system of failure
				bus.Publish(context.Background(), eventbus.Event{
					Type: "OrderFailedEvent",
					Payload: map[string]any{"req": req},
				})
				return nil, effect.EffectPlan{}, intent.OutcomeMeta{}, fmt.Errorf("insufficient inventory for item %s", req.ItemID)
			}

			// Feature E: SAGA - Distributed Transaction & Compensation
			s := saga.NewOrchestrator()

			// Step 1: Charge Payment
			s.AddStep("ChargePayment", func(ctx context.Context) error {
				fmt.Printf("[Saga - Payment] Charging card for User %s...\n", req.UserID)
				if req.UserID == "poor-user" {
					return fmt.Errorf("card declined: insufficient funds") // TRIGGERS SAGA ROLLBACK
				}
				return nil
			}, func(ctx context.Context) error {
				fmt.Printf("[Saga - Rollback] Refunding payment for User %s...\n", req.UserID)
				return nil
			})

			// Step 2: Reserve Shipping Label
			s.AddStep("ReserveShipping", func(ctx context.Context) error {
				fmt.Printf("[Saga - Shipping] Reserving tracking number for Item %s...\n", req.ItemID)
				return nil
			}, func(ctx context.Context) error {
				fmt.Printf("[Saga - Rollback] Cancelling tracking number for Item %s...\n", req.ItemID)
				return nil
			})

			// Execute the Saga Orchestrator
			if err := s.Execute(context.Background()); err != nil {
				bus.Publish(context.Background(), eventbus.Event{
					Type: "OrderFailedEvent",
					Payload: map[string]any{"req": req},
				})
				return nil, effect.EffectPlan{}, intent.OutcomeMeta{}, fmt.Errorf("checkout saga aborted: %w", err)
			}

			// Broadcast Success Event to CQRS Bus
			bus.Publish(context.Background(), eventbus.Event{
				Type: "OrderPlacedEvent",
				Payload: map[string]any{"req": req},
			})

			// Return strongly typed Output
			res := OrderResult{
				OrderID: fmt.Sprintf("ORD-%d", time.Now().UnixNano()),
				Status:  "CONFIRMED",
			}
			return res, effect.EffectPlan{}, intent.OutcomeMeta{}, nil
		},
	}

	_ = engine.RegisterDefinition(&checkoutIntent)

	// Compile to Immutable Generation (Tier-0 Lock-Free Data structure)
	fmt.Println("=> Compiling REF Graph...")
	if err := engine.Compile(); err != nil {
		panic(err)
	}
	return engine
}

// =====================================================================
// 4. HTTP SERVER (fh Framework wrapping REF)
// =====================================================================

func main() {
	// Initialize Engine
	engine := buildEngine()

	// Initialize Web Framework
	app := fh.New()

	// Inject decoupled interface adapter
	app.SetREF(&refAdapter{engine: engine})

	// Setup POST endpoint
	app.Post("/api/checkout", func(c fh.Ctx) error {
		refEngine := c.App().REF()

		// Read raw request body
		body := c.BodyRaw()
		
		// Create the invocation dynamically
		inv := &invocation.Invocation{
			ID:     invocation.ID(fmt.Sprintf("inv-%d", time.Now().UnixNano())),
			Intent: "ProcessCheckout",
			Input:  invocation.NewInputDirect(body, "application/json"),
		}

		// Dispatch workflow to REF Engine
		resAny, err := refEngine.Dispatch(c.Context(), inv)
		if err != nil {
			return c.Status(http.StatusUnprocessableEntity).JSON(map[string]string{
				"error": err.Error(),
			})
		}

		// Cast the agnostic result to our strong struct and return JSON
		res := resAny.(*ref.DispatchResult)
		return c.Status(http.StatusOK).JSON(res.Value)
	})

	go func() {
		fmt.Println("\n=== Comprehensive REF E-Commerce Example ===")
		fmt.Println("Server running on :8080")
		fmt.Println("\nTry these commands:")
		fmt.Println("  [1. SUCCESS] curl -X POST -H \"Content-Type: application/json\" -d '{\"user_id\":\"tony\",\"item_id\":\"book\",\"qty\":1}' http://localhost:8080/api/checkout")
		fmt.Println("  [2. INVENTORY ERROR] curl -X POST -H \"Content-Type: application/json\" -d '{\"user_id\":\"tony\",\"item_id\":\"book\",\"qty\":99}' http://localhost:8080/api/checkout")
		fmt.Println("  [3. SAGA ROLLBACK] curl -X POST -H \"Content-Type: application/json\" -d '{\"user_id\":\"poor-user\",\"item_id\":\"book\",\"qty\":1}' http://localhost:8080/api/checkout")
		
		if err := app.Listen(":8080"); err != nil {
			panic(err)
		}
	}()

	// Wait for interrupt
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	fmt.Println("\nShutting down...")
}
