package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
)

func openRuntimeStorage() (TaskStore, ChainStore, Broker, func(), error) {
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

func opsGuard(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := os.Getenv("DAGFLOW_ADMIN_TOKEN")
		if token == "" && os.Getenv("DAGFLOW_ENV") == "production" {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "DAGFLOW_ADMIN_TOKEN is required in production"})
			return
		}
		if token != "" {
			got := r.Header.Get("X-Admin-Token")
			if got == "" && strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
				got = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			}
			if got != token {
				writeJSON(w, http.StatusForbidden, map[string]any{"error": "admin token required"})
				return
			}
		}
		next(w, r)
	}
}
