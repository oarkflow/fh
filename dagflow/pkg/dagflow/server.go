package dagflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/oarkflow/fh"
)

type RegisterFunc func(*Engine)

type ServerOptions struct {
	BCLPath  string
	Address  string
	Register RegisterFunc
	OnFinal  FinalCallback
}

func DefaultBCLPath() string { return "app/bcl" }

func RunServer(ctx context.Context, opt ServerOptions) error {
	bclPath := opt.BCLPath
	if bclPath == "" {
		bclPath = DefaultBCLPath()
	}
	store, chainStore, broker, cleanup, err := OpenRuntimeStorage()
	if err != nil {
		return err
	}
	defer cleanup()
	engine := NewEngine(store, chainStore, broker)
	if opt.Register != nil {
		opt.Register(engine)
	}
	if opt.OnFinal != nil {
		engine.OnFinal(opt.OnFinal)
	}
	engine.OnFinal(func(t *Task) {
		log.Printf("workflow final task=%s workflow=%s status=%s", t.ID, t.WorkflowID, t.Status)
	})
	cfg, err := LoadBCL(bclPath)
	if err != nil {
		return err
	}
	if err := engine.LoadConfig(cfg); err != nil {
		return err
	}
	if err := ValidateConfig(cfg, engine); err != nil {
		return err
	}
	if err := engine.Start(ctx); err != nil {
		return err
	}
	dynamic, err := NewHTTPApp(engine, cfg)
	if err != nil {
		return err
	}
	app := fh.New()
	RegisterOperations(app, engine, cfg, bclPath)
	if err := dynamic.Register(app); err != nil {
		return err
	}
	addr := opt.Address
	if addr == "" {
		addr = cfg.Server.Address
	}
	if addr == "" {
		addr = ":8080"
	}
	log.Println("listening on", addr)
	return app.Listen(addr)
}

func RunCLI(args []string, bclDefault string, register RegisterFunc) error {
	if len(args) == 0 {
		return fmt.Errorf("missing command")
	}
	cmd := args[0]
	bclDir := bclDefault
	if bclDir == "" {
		bclDir = DefaultBCLPath()
	}
	if len(args) > 1 {
		bclDir = args[1]
	}
	cfg, err := LoadBCL(bclDir)
	if err != nil {
		return err
	}
	engine := NewEngine(NewMemoryTaskStore(), NewMemoryChainStore(), NewMemoryBroker())
	if register != nil {
		register(engine)
	}
	if err := engine.LoadConfig(cfg); err != nil {
		return err
	}
	switch cmd {
	case "validate":
		if err := ValidateConfig(cfg, engine); err != nil {
			return err
		}
		fmt.Println("BCL validation OK")
	case "metadata":
		b, _ := json.MarshalIndent(map[string]any{"workflows": engine.ListWorkflowMetadata(), "routes": FlattenRoutes(cfg), "schemas": engine.Schemas(), "handlers": engine.HandlerNames(), "scripts": engine.ScriptNames()}, "", "  ")
		fmt.Println(string(b))
	case "openapi":
		b, _ := json.MarshalIndent(GenerateOpenAPI(cfg), "", "  ")
		fmt.Println(string(b))
	case "graph":
		if len(args) < 3 {
			return fmt.Errorf("usage: dagflow graph <bcl_dir> <workflow_id>")
		}
		svg, err := engine.WorkflowSVG(args[2], true)
		if err != nil {
			return err
		}
		fmt.Println(svg)
	default:
		return fmt.Errorf("unknown command %s; use serve|validate|metadata|openapi|graph", cmd)
	}
	return nil
}

func RegisterOperations(app *fh.App, engine *Engine, cfg *Config, bclRoot ...string) {
	app.Get("/openapi.json", opsOpenAPI(cfg))
	app.Get("/metrics", opsMetrics(engine))
	app.Get("/ops/metadata", opsGuard(opsMetadata(engine, cfg)))
	root := DefaultBCLPath()
	if len(bclRoot) > 0 && bclRoot[0] != "" {
		root = bclRoot[0]
	}
	RegisterBCLAdmin(app, engine, cfg, root)
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
	return func(c *fh.Ctx) error {
		tasks := engine.Store().List()
		out := make([]TaskActivitySummary, 0, len(tasks))
		for _, t := range tasks {
			out = append(out, taskActivitySummary(t))
		}
		return writeJSON(c, fh.StatusOK, out)
	}
}
func listDLQ(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error { return writeJSON(c, fh.StatusOK, engine.Store().ListDLQ()) }
}
func listChains(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		runs := engine.ChainStore().List()
		out := make([]PublicChainState, 0, len(runs))
		for _, r := range runs {
			out = append(out, publicChainState(r))
		}
		return writeJSON(c, fh.StatusOK, out)
	}
}

func chainGet(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		run, err := engine.ChainStore().Get(c.Param("id"))
		if err != nil {
			return writeJSON(c, fh.StatusNotFound, map[string]any{"error": err.Error()})
		}
		return writeJSON(c, fh.StatusOK, publicChainState(run))
	}
}

func taskGet(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		t, err := engine.Store().Get(c.Param("id"))
		if err != nil {
			return writeJSON(c, fh.StatusNotFound, map[string]any{"error": err.Error()})
		}
		return writeJSON(c, fh.StatusOK, publicTaskState(t))
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
		case "activities", "audit":
			t, err := engine.Store().Get(id)
			if err != nil {
				return writeJSON(c, fh.StatusNotFound, map[string]any{"error": err.Error()})
			}
			return writeJSON(c, fh.StatusOK, t.Audit)
		case "summary":
			t, err := engine.Store().Get(id)
			if err != nil {
				return writeJSON(c, fh.StatusNotFound, map[string]any{"error": err.Error()})
			}
			return writeJSON(c, fh.StatusOK, taskActivitySummary(t))
		case "debug":
			t, err := engine.Store().Get(id)
			if err != nil {
				return writeJSON(c, fh.StatusNotFound, map[string]any{"error": err.Error()})
			}
			return writeJSON(c, fh.StatusOK, t)
		case "pause":
			t, err := engine.PauseTask(id)
			if err != nil {
				return writeJSON(c, fh.StatusBadRequest, map[string]any{"error": err.Error()})
			}
			return writeJSON(c, fh.StatusOK, publicTaskState(t))
		case "cancel":
			t, err := engine.CancelTask(id)
			if err != nil {
				return writeJSON(c, fh.StatusBadRequest, map[string]any{"error": err.Error()})
			}
			return writeJSON(c, fh.StatusOK, publicTaskState(t))
		case "resume":
			var input any
			_ = json.Unmarshal(c.Body(), &input)
			t, err := engine.ResumeTask(c.Context(), id, input)
			if err != nil {
				return writeJSON(c, fh.StatusBadRequest, map[string]any{"error": err.Error()})
			}
			return writeJSON(c, fh.StatusOK, publicTaskState(t))
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
			return writeJSON(c, fh.StatusOK, publicTaskState(t))
		case "restart":
			t, err := engine.RestartTask(c.Context(), id)
			if err != nil {
				return writeJSON(c, fh.StatusBadRequest, map[string]any{"error": err.Error()})
			}
			return writeJSON(c, fh.StatusOK, publicTaskState(t))
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
			return writeJSON(c, fh.StatusAccepted, publicTaskReceipt(task))
		}
		task, err := engine.RunSync(c.Context(), c.Param("workflow"), input)
		if err != nil {
			return writeJSON(c, fh.StatusInternalServerError, taskOrError(task, err))
		}
		return writeJSON(c, fh.StatusOK, publicTaskResult(task))
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
			return writeJSON(c, fh.StatusInternalServerError, map[string]any{"error": err.Error(), "node": state.NodeID, "status": state.Status})
		}
		return writeJSON(c, fh.StatusOK, publicResult(result))
	}
}

// Environment helpers are intentionally in the core package because they are
// generic runtime bootstrapping primitives. Apps can bypass this and construct
// Engine with their own Storage/Broker directly.
func MustServe(ctx context.Context, opt ServerOptions) {
	if err := RunServer(ctx, opt); err != nil {
		log.Fatal(err)
	}
}

func ExitOnCLIError(err error) {
	if err != nil {
		log.Fatal(err)
	}
	_ = os.Stdout
}
