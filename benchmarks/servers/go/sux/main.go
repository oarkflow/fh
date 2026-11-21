package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"code.icod.de/dalu/sux"
	"github.com/oarkflow/fh/benchmarks/servers/go/benchstats"
)

type user struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type messageResponse struct {
	Message string `json:"message"`
}

type queryResponse struct {
	Query string `json:"query"`
}

var users = benchmarkUsers()

func benchmarkUsers() []user {
	result := make([]user, 100)
	for i := range result {
		result[i] = user{ID: i + 1, Name: "User " + strconv.Itoa(i+1)}
	}
	return result
}

func main() {
	addr := os.Getenv("FH_BENCH_ADDR")
	if addr == "" {
		addr = "127.0.0.1:3005"
	}

	router := sux.New()
	router.GET("/plaintext", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "Hello, World!")
	})
	router.GET("/json", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, messageResponse{Message: "Hello, World!"})
	})
	router.GET("/users/:id", func(w http.ResponseWriter, r *http.Request) {
		params := sux.ParamsFromContext(r)
		writeJSON(w, user{Name: "User " + params.Get("id")})
	})
	router.GET("/search", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, queryResponse{Query: r.URL.Query().Get("q")})
	})
	router.POST("/echo", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 4<<20)
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	})
	router.GET("/users", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, users) })
	router.GET("/__bench/stats", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, benchstats.Read()) })

	// Match the other servers' no-Date response behavior. net/http adds Date
	// unless the handler explicitly supplies an empty value.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header()["Date"] = nil
		router.ServeHTTP(w, r)
	})
	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	log.Fatal(server.ListenAndServe())
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		return
	}
}
