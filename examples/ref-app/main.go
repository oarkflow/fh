package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/ref"
	"github.com/oarkflow/fh/ref/debug"
	refCli "github.com/oarkflow/fh/ref/transport/cli"
	refHttp "github.com/oarkflow/fh/ref/transport/http"
)

func buildEngine(app *fh.App) *ref.Engine {
	// Enable REF on the fh application
	engine := app.EnableREF()

	// 1. Register Capabilities (Producers & Policies)
	if err := ref.RegisterCapability(engine, NewAppAuthCapability()); err != nil {
		log.Fatalf("failed to register auth capability: %v", err)
	}
	if err := ref.RegisterCapability(engine, NewAppTenantCapability()); err != nil {
		log.Fatalf("failed to register tenant capability: %v", err)
	}
	if err := ref.RegisterCapability(engine, NewOrderPolicyCapability()); err != nil {
		log.Fatalf("failed to register policy capability: %v", err)
	}

	// 2. Register strongly-typed Intent Contracts
	if err := ref.Register(engine, CreateOrderIntent{}); err != nil {
		log.Fatalf("failed to register order.create intent: %v", err)
	}

	// 3. Compile the execution graph into immutable runtime plans
	if err := engine.Compile(); err != nil {
		log.Fatalf("failed to compile engine: %v", err)
	}

	return engine
}

func main() {
	app := fh.NewFast()
	engine := buildEngine(app)

	// Check if invoked in CLI demonstration mode
	if len(os.Args) > 1 && os.Args[1] == "cli" {
		runCLIDemo(engine)
		return
	}

	// Mount HTTP endpoint for the intent
	// The HTTP adapter transparently decodes input, dispatches through REF, and projects outcomes
	app.Post("/orders", refHttp.Adapter(engine, "order.create"))

	// Mount Debug Introspection endpoints
	app.Get("/ref/inspect/:intent", func(c fh.Ctx) error {
		itName := c.Params("intent")
		plan, ok := engine.Plan(ref.Intent[any, any](nil).Name())
		if p, found := engine.Plan(CreateOrderIntent{}.Name()); found && itName == "order.create" {
			plan = p
			ok = true
		}
		if !ok {
			return c.Status(404).SendString("intent plan not found")
		}
		summary := debug.InspectPlan(plan)
		return c.JSON(summary)
	})

	app.Get("/ref/diagram", func(c fh.Ctx) error {
		plan, ok := engine.Plan(CreateOrderIntent{}.Name())
		if !ok {
			return c.Status(404).SendString("intent plan not found")
		}
		c.Set("Content-Type", "text/plain; charset=utf-8")
		return c.SendString(debug.ToMermaid(plan))
	})

	fmt.Println("=================================================================")
	fmt.Println(" Runtime Execution Fabric (REF) - Example Server")
	fmt.Println("=================================================================")
	fmt.Println(" Endpoints:")
	fmt.Println("   POST /orders                -> Place order (Intent: order.create)")
	fmt.Println("   GET  /ref/inspect/order.create -> Execution Graph Plan Summary")
	fmt.Println("   GET  /ref/diagram           -> Mermaid DAG Diagram")
	fmt.Println("=================================================================")
	fmt.Println(" Test with curl:")
	fmt.Println(`   curl -X POST http://localhost:8088/orders \`)
	fmt.Println(`     -H "Content-Type: application/json" \`)
	fmt.Println(`     -H "Authorization: Bearer my-token" \`)
	fmt.Println(`     -H "X-Tenant-ID: acme-corp" \`)
	fmt.Println(`     -d '{"sku":"WIDGET-100","quantity":2,"price":49.99}'`)
	fmt.Println("=================================================================")

	if err := app.Listen(":8088"); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func runCLIDemo(engine *ref.Engine) {
	fmt.Println("Running REF CLI Dispatch Demonstration...")
	cliCmd := refCli.Command(engine, "order.create")

	payload := []byte(`{"sku":"LAPTOP-X","quantity":1,"price":1299.00}`)
	out, err := cliCmd(context.Background(), nil, payload)
	if err != nil {
		fmt.Printf("CLI Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("CLI Response: %s\n", string(out))
}
