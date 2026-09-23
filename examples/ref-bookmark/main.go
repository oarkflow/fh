// Command bookmarks serves the bookmark-manager application declared in app.bcl.
//
// There is deliberately no business logic here. Everything this program does is
// load a BCL document, mount the routes it declares and serve them: the schema,
// the identity model, the authorization rules, the caching, the circuit breaker,
// the durable health-check process with its saga rollback all live in app.bcl.
//
// What the host process still owns, and always will, is:
//   - Driver imports (the blank pgx import below makes "pgx" resolvable)
//   - Graceful shutdown (SIGTERM drains in-flight requests and releases leases)
//   - SPI adapters (a Redis cache or Kafka queue would be ten lines here)
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/ref/platform"
)

func main() {
	config := flag.String("config", "examples/ref-bookmark/app.bcl", "path to the application document")
	addr := flag.String("addr", ":8090", "address to listen on")
	flag.Parse()

	// Compile-time validation: a missing secret, an unreachable database, a
	// misspelled action or an intent that references a nonexistent capability
	// is a startup failure — not a 500 on the first request.
	opts := platform.DefaultLoadOptions()
	app, err := platform.LoadFile(context.Background(), *config, opts)
	if err != nil {
		log.Fatalf("bookmarks: compile: %v", err)
	}
	defer app.Close()

	server := fh.NewFast()
	if err := app.Mount(server); err != nil {
		log.Fatalf("bookmarks: mount: %v", err)
	}

	// Log every route the document declared so the operator can verify the
	// mount without reading the BCL file.
	for _, route := range app.Document.Routes {
		log.Printf("bookmarks: %-6s %s -> %s", route.Method, route.Path, route.Intent)
	}

	// Shutdown order: stop accepting connections first, then drain the queue
	// consumers and release every process lease this replica holds.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		log.Print("bookmarks: shutting down")
		if err := server.ShutdownWithTimeout(10 * time.Second); err != nil {
			log.Printf("bookmarks: shutdown: %v", err)
		}
	}()

	log.Printf("bookmarks: listening on %s", *addr)
	if err := server.Listen(*addr); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("bookmarks: %v", err)
	}
}
