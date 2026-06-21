package dagflow

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/oarkflow/fh"
)

func OpenRuntimeStorage() (TaskStore, ChainStore, Broker, func(), error) {
	backend := strings.ToLower(os.Getenv("DAGFLOW_STORE"))
	switch backend {
	case "postgres", "postgresql", "pg":
		dsn := os.Getenv("DAGFLOW_POSTGRES_DSN")
		pg, err := NewPostgresStorage(dsn)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		log.Println("storage=postgres durable queue=postgres")
		return pg, pg.ChainStore(), NewPostgresBroker(pg, os.Getenv("DAGFLOW_WORKER_ID")), func() { _ = pg.Close() }, nil
	case "file", "", "dev":
		path := os.Getenv("DAGFLOW_FILE_STORE")
		if path == "" {
			path = "data/dagflow.json"
		}
		fs, err := NewFileStore(path)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		log.Println("storage=file queue=memory; set DAGFLOW_STORE=postgres for production")
		return fs, fs.ChainStore(), NewMemoryBroker(), func() {}, nil
	default:
		return nil, nil, nil, nil, fmt.Errorf("unsupported DAGFLOW_STORE %q", backend)
	}
}

func opsGuard(next fh.HandlerFunc) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		token := os.Getenv("DAGFLOW_ADMIN_TOKEN")
		if token == "" && os.Getenv("DAGFLOW_ENV") == "production" {
			return writeJSON(c, fh.StatusServiceUnavailable, map[string]any{"error": "DAGFLOW_ADMIN_TOKEN is required in production"})
		}
		if token != "" {
			got := c.Get("X-Admin-Token")
			if got == "" && strings.HasPrefix(c.Get("Authorization"), "Bearer ") {
				got = strings.TrimPrefix(c.Get("Authorization"), "Bearer ")
			}
			if got != token {
				return writeJSON(c, fh.StatusForbidden, map[string]any{"error": "admin token required"})
			}
		}
		return next(c)
	}
}
