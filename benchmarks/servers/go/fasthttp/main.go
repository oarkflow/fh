package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"

	"github.com/oarkflow/fh/benchmarks/servers/go/benchstats"
	"github.com/valyala/fasthttp"
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

func writeJSON(ctx *fasthttp.RequestCtx, value any) {
	ctx.SetContentType("application/json")
	body, err := json.Marshal(value)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}
	ctx.SetBody(body)
}

func requestHandler(ctx *fasthttp.RequestCtx) {
	path := ctx.Path()
	switch {
	case string(path) == "/plaintext" && ctx.IsGet():
		ctx.SetBodyString("Hello, World!")
	case string(path) == "/json" && ctx.IsGet():
		writeJSON(ctx, messageResponse{Message: "Hello, World!"})
	case len(path) > len("/users/") && string(path[:len("/users/")]) == "/users/" && ctx.IsGet():
		writeJSON(ctx, user{Name: "User " + string(path[len("/users/"):])})
	case string(path) == "/search" && ctx.IsGet():
		writeJSON(ctx, queryResponse{Query: string(ctx.QueryArgs().Peek("q"))})
	case string(path) == "/echo" && ctx.IsPost():
		ctx.SetContentType("application/json")
		ctx.SetBody(ctx.PostBody())
	case string(path) == "/users" && ctx.IsGet():
		writeJSON(ctx, users)
	case string(path) == "/__bench/stats" && ctx.IsGet():
		writeJSON(ctx, benchstats.Read())
	default:
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		ctx.SetBodyString("Not Found")
	}
}

func main() {
	addr := os.Getenv("FH_BENCH_ADDR")
	if addr == "" {
		addr = "127.0.0.1:3004"
	}
	server := &fasthttp.Server{
		Handler:               requestHandler,
		ReadBufferSize:        16 << 10,
		MaxRequestBodySize:    4 << 20,
		NoDefaultDate:         true,
		NoDefaultServerHeader: true,
	}
	log.Fatal(server.ListenAndServe(addr))
}
