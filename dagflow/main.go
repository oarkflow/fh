package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
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
	app, err := NewHTTPApp(engine, cfg)
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ops/metadata", opsGuard(opsMetadata(engine, cfg)))
	mux.HandleFunc("/ops/validate", opsGuard(opsValidate(cfg, engine)))
	mux.HandleFunc("/openapi.json", opsOpenAPI(cfg))
	mux.HandleFunc("/metrics", opsMetrics(engine))
	mux.HandleFunc("/ops/metrics", opsGuard(opsMetrics(engine)))
	mux.HandleFunc("/ops/outbox", opsGuard(opsOutbox(engine)))
	mux.HandleFunc("/ops/leases", opsGuard(opsLeases(engine)))
	mux.HandleFunc("/ops/workflows", opsGuard(opsWorkflows(engine)))
	mux.HandleFunc("/ops/workflows/", opsGuard(opsWorkflow(engine)))
	mux.HandleFunc("/ops/tasks", opsGuard(listTasks(engine)))
	mux.HandleFunc("/ops/dlq", opsGuard(listDLQ(engine)))
	mux.HandleFunc("/ops/dlq/", opsGuard(dlqOps(engine)))
	mux.HandleFunc("/ops/tasks/", opsGuard(taskOps(engine)))
	mux.HandleFunc("/ops/chains", opsGuard(listChains(engine)))
	mux.HandleFunc("/ops/chains/", opsGuard(chainGet(engine)))
	mux.HandleFunc("/workflow/", legacyWorkflowHandler(engine))
	mux.HandleFunc("/node/", legacyNodeHandler(engine))
	mux.Handle("/", app)

	addr := cfg.Server.Address
	if addr == "" {
		addr = ":8080"
	}
	log.Println("listening on", addr)
	log.Println(`dynamic route: curl -s -X POST localhost:8080/api/email/send -H 'Content-Type: application/json' -H 'X-API-Key: dev-secret' -d '{"to":"a@b.com","subject":"Hi","body":"Hello"}' | jq`)
	log.Println(`pause route:   curl -s -X POST localhost:8080/api/email/approval -H 'Content-Type: application/json' -H 'X-API-Key: dev-secret' -d '{"to":"a@b.com","subject":"Need approval","body":"Hello"}' | jq`)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func listTasks(engine *Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, engine.Store().List()) }
}
func listDLQ(engine *Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, engine.Store().ListDLQ()) }
}
func listChains(engine *Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, engine.ChainStore().List()) }
}
func chainGet(engine *Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/ops/chains/")
		run, err := engine.ChainStore().Get(id)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, run)
	}
}

func taskOps(engine *Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/ops/tasks/"), "/")
		parts := strings.Split(path, "/")
		if len(parts) == 0 || parts[0] == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "task id required"})
			return
		}
		id := parts[0]
		if len(parts) == 1 {
			t, err := engine.Store().Get(id)
			if err != nil {
				writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, t)
			return
		}
		op := parts[1]
		switch op {
		case "graph.svg":
			svg, err := engine.TaskSVG(id)
			if err != nil {
				writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
				return
			}
			w.Header().Set("Content-Type", "image/svg+xml")
			_, _ = w.Write([]byte(svg))
		case "audit":
			t, err := engine.Store().Get(id)
			if err != nil {
				writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, t.Audit)
		case "pause":
			t, err := engine.PauseTask(id)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, t)
		case "cancel":
			t, err := engine.CancelTask(id)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, t)
		case "resume":
			var input any
			_ = json.NewDecoder(r.Body).Decode(&input)
			t, err := engine.ResumeTask(r.Context(), id, input)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, t)
		case "continue":
			var body struct {
				Strategy ErrorStrategy `json:"strategy"`
				Result   any           `json:"result"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			t, err := engine.ContinueTask(r.Context(), id, body.Strategy, body.Result)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, t)
		case "restart":
			t, err := engine.RestartTask(r.Context(), id)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, t)
		default:
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "unknown operation"})
		}
	}
}

func workflowGraph(engine *Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/ops/workflows/"), "/")
		parts := strings.Split(path, "/")
		if len(parts) < 2 || parts[1] != "graph.svg" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "use /ops/workflows/{id}/graph.svg"})
			return
		}
		nested := r.URL.Query().Get("nested") == "true"
		svg, err := engine.WorkflowSVG(parts[0], nested)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "image/svg+xml")
		_, _ = w.Write([]byte(svg))
	}
}

func legacyWorkflowHandler(engine *Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/workflow/")
		parts := strings.Split(strings.Trim(path, "/"), "/")
		workflowID := parts[0]
		input, err := readJSONBody(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		if len(parts) > 1 && parts[1] == "async" {
			task, err := engine.RunAsync(r.Context(), workflowID, input)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusAccepted, task)
			return
		}
		task, err := engine.RunSync(r.Context(), workflowID, input)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, taskOrError(task, err))
			return
		}
		writeJSON(w, http.StatusOK, task)
	}
}

func legacyNodeHandler(engine *Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
			return
		}
		parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/node/"), "/"), "/")
		if len(parts) != 2 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "use /node/{workflow_id}/{node_id}"})
			return
		}
		input, err := readJSONBody(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		state, result, err := engine.RunStandaloneNode(r.Context(), parts[0], parts[1], input)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"state": state, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"state": state, "result": result})
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
	store := NewMemoryTaskStore()
	engine := NewEngine(store, NewMemoryChainStore(), NewMemoryBroker())
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
