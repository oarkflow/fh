package main

import (
	"encoding/json"
	"log"
	"strconv"

	"github.com/valyala/fasthttp"
)

type user struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
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
		writeJSON(ctx, map[string]string{"message": "Hello, World!"})
	case len(path) > len("/users/") && string(path[:len("/users/")]) == "/users/" && ctx.IsGet():
		writeJSON(ctx, user{Name: "User " + string(path[len("/users/"):])})
	case string(path) == "/search" && ctx.IsGet():
		writeJSON(ctx, map[string]string{"query": string(ctx.QueryArgs().Peek("q"))})
	case string(path) == "/echo" && ctx.IsPost():
		var value map[string]any
		if err := json.Unmarshal(ctx.PostBody(), &value); err != nil {
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			return
		}
		writeJSON(ctx, value)
	case string(path) == "/users" && ctx.IsGet():
		writeJSON(ctx, users)
	default:
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		ctx.SetBodyString("Not Found")
	}
}

func main() {
	server := &fasthttp.Server{
		Handler:               requestHandler,
		ReadBufferSize:        16 << 10,
		MaxRequestBodySize:    4 << 20,
		NoDefaultDate:         true,
		NoDefaultServerHeader: true,
	}
	log.Fatal(server.ListenAndServe("127.0.0.1:3004"))
}
