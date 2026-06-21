package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/oarkflow/fh"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] != "serve" {
		runCLI()
		return
	}
	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, chainStore, broker, cleanup, err := openRuntimeStorage()
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup()
	engine := NewEngine(store, chainStore, broker)
	registerExampleHandlers(engine)
	engine.OnFinal(func(t *Task) {
		log.Printf("workflow final task=%s workflow=%s status=%s", t.ID, t.WorkflowID, t.Status)
	})
	cfg, err := LoadBCL("bcl")
	if err != nil {
		log.Fatal(err)
	}
	if err := engine.LoadConfig(cfg); err != nil {
		log.Fatal(err)
	}
	if err := engine.Start(root); err != nil {
		log.Fatal(err)
	}
	dynamic, err := NewHTTPApp(engine, cfg)
	if err != nil {
		log.Fatal(err)
	}

	app := fh.New()
	registerOperations(app, engine, cfg)
	if err := dynamic.Register(app); err != nil {
		log.Fatal(err)
	}

	addr := cfg.Server.Address
	if addr == "" {
		addr = ":8080"
	}
	log.Println("listening on", addr)
	log.Println(`dynamic route: curl -s -X POST localhost:8080/api/email/send -H 'Content-Type: application/json' -H 'X-API-Key: dev-secret' -d '{"to":"a@b.com","subject":"Hi","body":"Hello"}' | jq`)
	log.Fatal(app.Listen(addr))
}

func registerOperations(app *fh.App, engine *Engine, cfg *Config) {
	app.Get("/openapi.json", opsOpenAPI(cfg))
	app.Get("/metrics", opsMetrics(engine))
	app.Get("/ops/metadata", opsGuard(opsMetadata(engine, cfg)))
	app.Get("/ops/validate", opsGuard(opsValidate(cfg, engine)))
	app.Get("/ops/metrics", opsGuard(opsMetrics(engine)))
	app.Get("/ops/outbox", opsGuard(opsOutbox(engine)))
	app.Get("/ops/leases", opsGuard(opsLeases(engine)))
	app.Get("/ops/workflows", opsGuard(opsWorkflows(engine)))
	app.Get("/ops/workflows/:id", opsGuard(opsWorkflow(engine)))
	app.Get("/ops/workflows/:id/metadata", opsGuard(opsWorkflow(engine)))
	app.Get("/ops/workflows/:id/versions", opsGuard(opsWorkflowVersions(engine)))
	app.Get("/ops/workflows/:id/graph.svg", opsGuard(opsWorkflowGraph(engine)))
	app.Get("/ops/tasks", opsGuard(listTasks(engine)))
	app.Get("/ops/tasks/:id", opsGuard(taskGet(engine)))
	app.All("/ops/tasks/:id/:op", opsGuard(taskOps(engine)))
	app.Get("/ops/dlq", opsGuard(listDLQ(engine)))
	app.Get("/ops/dlq/:id", opsGuard(dlqGet(engine)))
	app.Post("/ops/dlq/:id/discard", opsGuard(dlqDiscard(engine)))
	app.Post("/ops/dlq/:id/replay", opsGuard(dlqReplay(engine)))
	app.Get("/ops/chains", opsGuard(listChains(engine)))
	app.Get("/ops/chains/:id", opsGuard(chainGet(engine)))
	app.Post("/workflow/:workflow", legacyWorkflowHandler(engine, false))
	app.Post("/workflow/:workflow/async", legacyWorkflowHandler(engine, true))
	app.Post("/node/:workflow/:node", legacyNodeHandler(engine))
}

func listTasks(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error { return writeJSON(c, fh.StatusOK, engine.Store().List()) }
}
func listDLQ(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error { return writeJSON(c, fh.StatusOK, engine.Store().ListDLQ()) }
}
func listChains(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error { return writeJSON(c, fh.StatusOK, engine.ChainStore().List()) }
}

func chainGet(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		run, err := engine.ChainStore().Get(c.Param("id"))
		if err != nil {
			return writeJSON(c, fh.StatusNotFound, map[string]any{"error": err.Error()})
		}
		return writeJSON(c, fh.StatusOK, run)
	}
}

func taskGet(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		t, err := engine.Store().Get(c.Param("id"))
		if err != nil {
			return writeJSON(c, fh.StatusNotFound, map[string]any{"error": err.Error()})
		}
		return writeJSON(c, fh.StatusOK, t)
	}
}

func taskOps(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		id := c.Param("id")
		switch c.Param("op") {
		case "graph.svg":
			svg, err := engine.TaskSVG(id)
			if err != nil {
				return writeJSON(c, fh.StatusNotFound, map[string]any{"error": err.Error()})
			}
			c.Type("image/svg+xml")
			return c.SendString(svg)
		case "audit":
			t, err := engine.Store().Get(id)
			if err != nil {
				return writeJSON(c, fh.StatusNotFound, map[string]any{"error": err.Error()})
			}
			return writeJSON(c, fh.StatusOK, t.Audit)
		case "pause":
			t, err := engine.PauseTask(id)
			if err != nil {
				return writeJSON(c, fh.StatusBadRequest, map[string]any{"error": err.Error()})
			}
			return writeJSON(c, fh.StatusOK, t)
		case "cancel":
			t, err := engine.CancelTask(id)
			if err != nil {
				return writeJSON(c, fh.StatusBadRequest, map[string]any{"error": err.Error()})
			}
			return writeJSON(c, fh.StatusOK, t)
		case "resume":
			var input any
			_ = json.Unmarshal(c.Body(), &input)
			t, err := engine.ResumeTask(c.Context(), id, input)
			if err != nil {
				return writeJSON(c, fh.StatusBadRequest, map[string]any{"error": err.Error()})
			}
			return writeJSON(c, fh.StatusOK, t)
		case "continue":
			var body struct {
				Strategy ErrorStrategy `json:"strategy"`
				Result   any           `json:"result"`
			}
			_ = json.Unmarshal(c.Body(), &body)
			t, err := engine.ContinueTask(c.Context(), id, body.Strategy, body.Result)
			if err != nil {
				return writeJSON(c, fh.StatusBadRequest, map[string]any{"error": err.Error()})
			}
			return writeJSON(c, fh.StatusOK, t)
		case "restart":
			t, err := engine.RestartTask(c.Context(), id)
			if err != nil {
				return writeJSON(c, fh.StatusBadRequest, map[string]any{"error": err.Error()})
			}
			return writeJSON(c, fh.StatusOK, t)
		default:
			return writeJSON(c, fh.StatusNotFound, map[string]any{"error": "unknown operation"})
		}
	}
}

func legacyWorkflowHandler(engine *Engine, async bool) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		input, err := readJSONBody(c)
		if err != nil {
			return writeJSON(c, fh.StatusBadRequest, map[string]any{"error": err.Error()})
		}
		if async {
			task, err := engine.RunAsync(c.Context(), c.Param("workflow"), input)
			if err != nil {
				return writeJSON(c, fh.StatusInternalServerError, map[string]any{"error": err.Error()})
			}
			return writeJSON(c, fh.StatusAccepted, task)
		}
		task, err := engine.RunSync(c.Context(), c.Param("workflow"), input)
		if err != nil {
			return writeJSON(c, fh.StatusInternalServerError, taskOrError(task, err))
		}
		return writeJSON(c, fh.StatusOK, task)
	}
}

func legacyNodeHandler(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		input, err := readJSONBody(c)
		if err != nil {
			return writeJSON(c, fh.StatusBadRequest, map[string]any{"error": err.Error()})
		}
		state, result, err := engine.RunStandaloneNode(c.Context(), c.Param("workflow"), c.Param("node"), input)
		if err != nil {
			return writeJSON(c, fh.StatusInternalServerError, map[string]any{"state": state, "error": err.Error()})
		}
		return writeJSON(c, fh.StatusOK, map[string]any{"state": state, "result": result})
	}
}

func runCLI() {
	cmd := os.Args[1]
	bclDir := "bcl"
	if len(os.Args) > 2 {
		bclDir = os.Args[2]
	}
	cfg, err := LoadBCL(bclDir)
	if err != nil {
		log.Fatal(err)
	}
	engine := NewEngine(NewMemoryTaskStore(), NewMemoryChainStore(), NewMemoryBroker())
	registerExampleHandlers(engine)
	if err := engine.LoadConfig(cfg); err != nil {
		log.Fatal(err)
	}
	switch cmd {
	case "validate":
		if err := ValidateConfig(cfg, engine); err != nil {
			log.Fatal(err)
		}
		fmt.Println("BCL validation OK")
	case "metadata":
		b, _ := json.MarshalIndent(map[string]any{"workflows": engine.ListWorkflowMetadata(), "routes": FlattenRoutes(cfg), "schemas": engine.Schemas()}, "", "  ")
		fmt.Println(string(b))
	case "openapi":
		b, _ := json.MarshalIndent(GenerateOpenAPI(cfg), "", "  ")
		fmt.Println(string(b))
	case "graph":
		if len(os.Args) < 4 {
			log.Fatal("usage: dagflow graph <bcl_dir> <workflow_id>")
		}
		svg, err := engine.WorkflowSVG(os.Args[3], true)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(svg)
	default:
		log.Fatalf("unknown command %s; use serve|validate|metadata|openapi|graph", cmd)
	}
}
