// Command orders serves the order-fulfilment application declared in app.bcl.
//
// There is deliberately no business logic here. Everything this program does is
// load a BCL document, mount the routes it declares and serve them: the schema,
// the identity model, the authorization rules, the caching, the durable
// fulfilment process with its human approval gate and its saga rollback all live
// in app.bcl. That is the claim the example exists to demonstrate, so any
// temptation to "just add a handler" here should go into the document instead.
//
// What the host process still owns, and always will, is the driver imports (the
// blank pgx import below is what makes driver "pgx" resolvable) and any adapter
// registered through ref/platform/spi — a Redis cache or a Kafka queue would be
// ten lines here, not a change to the platform.
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
	config := flag.String("config", "examples/ref-platform/app.bcl", "path to the application document")
	addr := flag.String("addr", ":8089", "address to listen on")
	replica := flag.String("replica", os.Getenv("REPLICA_ID"), "identifies this process in process leases")
	flag.Parse()

	// The document is compiled before anything listens. A missing secret, an
	// unreachable database, a misspelled action or an unreachable process step is
	// a startup failure here rather than a 500 on the first request that needed it.
	opts := platform.DefaultLoadOptions()
	opts.ReplicaID = *replica
	app, err := platform.LoadFile(context.Background(), *config, opts)
	if err != nil {
		log.Fatalf("orders: %v", err)
	}
	defer app.Close()

	server := fh.NewFast()
	if err := app.Mount(server); err != nil {
		log.Fatalf("orders: mount: %v", err)
	}

	for _, route := range app.Document.Routes {
		log.Printf("orders: %-6s %s -> %s", route.Method, route.Path, route.Intent)
	}

	// Shutting down in this order matters: stop accepting requests first, then
	// close the platform, which drains the queue consumers, releases every process
	// lease this replica holds and closes the pools. A lease left behind would
	// stall a run until it expired.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		log.Print("orders: shutting down")
		if err := server.ShutdownWithTimeout(10 * time.Second); err != nil {
			log.Printf("orders: shutdown: %v", err)
		}
	}()

	log.Printf("orders: listening on %s (replica %s)", *addr, *replica)
	if err := server.Listen(*addr); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("orders: %v", err)
	}
}
