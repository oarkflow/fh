// Command ref-app is a complete application built directly on the Runtime
// Execution Fabric, with real infrastructure behind every capability.
//
// It is the code-first counterpart to examples/ref-platform, which builds the same
// class of application from a BCL document. Here everything is Go: capabilities are
// functions, intents are types, and the wiring below is explicit. What both share is
// REF itself — facts resolved by readiness, deny-dominant policy, and an effect
// barrier that no business logic can write past.
//
// Nothing in this example is simulated. Orders are rows in PostgreSQL, stock is a
// checked constraint, the notification outbox is a table written inside the order's
// own transaction, the rate limiter is a real shared counter, tokens are real HMAC
// signatures over real bcrypt-verified logins, and metrics are real Prometheus
// output. The only stand-in is the default notifier, which prints rather than
// sending — and it says so in /healthz and at startup.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/ref"
)

func main() {
	var (
		cliIntent = flag.String("intent", "", "dispatch one intent from the command line instead of serving")
		cliToken  = flag.String("token", "", "bearer token for -intent")
		cliInput  = flag.String("input", "{}", "JSON input for -intent")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := LoadConfig(os.LookupEnv)
	if err != nil {
		// Configuration problems are reported all at once and in full, because the
		// person reading this is trying to start the process, not debug it.
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	deps, err := OpenDeps(ctx, cfg, log)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer func() { _ = deps.Close() }()

	app := fh.NewFast()
	engine, err := BuildEngineWithApp(app, deps)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if *cliIntent != "" {
		// The same engine, the same policy, the same effects — reached from a
		// terminal instead of a socket.
		out, err := DispatchCLI(ctx, engine, deps, *cliIntent, *cliToken, []byte(*cliInput))
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
		return
	}

	MountRoutes(app, engine, deps)
	MountQueue(deps, engine)

	// Recover before serving. A process that died between committing a transaction
	// and scheduling its delivery left notifications in the outbox; they are found
	// and re-scheduled here, before the first new request adds to them.
	recovered, err := deps.Effects.Recover(ctx)
	if err != nil {
		log.Warn("effect recovery failed", slog.String("error", err.Error()))
	} else if len(recovered) > 0 {
		log.Info("rescheduled deliveries left by a previous run", slog.Int("transactions", len(recovered)))
	}

	if err := deps.Queue.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "ref-app: start queue: %v\n", err)
		os.Exit(1)
	}
	go deps.Outbox.Run(ctx)

	log.Info("ref-app listening",
		slog.String("addr", cfg.Addr),
		slog.String("environment", cfg.Environment),
		slog.String("notifier", deps.Notifier.Describe()),
		slog.Int("intents", len(engine.Intents().All())))
	printBanner(engine, cfg)

	// Shut down in the order that loses least: stop accepting requests, then stop
	// the background loops (the deferred deps.Close), so an in-flight effect
	// transaction gets the chance to commit rather than being rolled back under a
	// request that would otherwise have succeeded.
	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		if err := app.ShutdownWithTimeout(15 * time.Second); err != nil {
			log.Warn("shutdown", slog.String("error", err.Error()))
		}
	}()

	if err := app.Listen(cfg.Addr); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintf(os.Stderr, "ref-app: %v\n", err)
		os.Exit(1)
	}
}

// printBanner lists what is actually mounted, generated from the engine rather than
// written out by hand, so it cannot drift from what the process will serve.
func printBanner(engine *ref.Engine, cfg Config) {
	fmt.Println("------------------------------------------------------------------")
	fmt.Println(" ref-app — a complete application on the Runtime Execution Fabric")
	fmt.Println("------------------------------------------------------------------")
	fmt.Println(" HTTP")
	for _, line := range []string{
		"POST   /auth/register          create an account, receive a token",
		"POST   /auth/login             exchange credentials for a token",
		"GET    /catalog                tenant catalogue (cached, anonymous)",
		"POST   /orders                 place an order",
		"GET    /orders                 list orders the policy allows",
		"GET    /orders/:id             read one order",
		"POST   /orders/:id/cancel      cancel and restock",
		"GET    /healthz                database, outbox and observation health",
		"GET    /metrics                Prometheus exposition",
		"GET    /ref/intents            what this engine can do",
		"GET    /ref/inspect/:intent    the compiled plan",
		"GET    /ref/diagram/:intent    the plan as a Mermaid diagram",
	} {
		fmt.Println("   " + line)
	}
	fmt.Println(" Intents compiled:")
	for name, def := range engine.Intents().All() {
		fmt.Printf("   %-16s %s\n", name, def.Spec.Description)
	}
	fmt.Println(" Example:")
	fmt.Printf("   curl -X POST http://localhost%s/auth/register \\\n", cfg.Addr)
	fmt.Println(`     -H 'Content-Type: application/json' -H 'X-Tenant-ID: acme' \`)
	fmt.Println(`     -d '{"email":"ada@example.com","name":"Ada","password":"correct horse battery"}'`)
	fmt.Println("------------------------------------------------------------------")
}

// encodeJSON is used by the CLI path and the tests to print a value the same way the
// HTTP layer would.
func encodeJSON(value any) string {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	return string(encoded)
}
