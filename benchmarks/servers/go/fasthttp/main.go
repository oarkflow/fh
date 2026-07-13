package main

import (
	"encoding/json"
	"log"
	"strconv"

	"github.com/valyala/fasthttp"
)

type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

var users []User

func init() {
	users = make([]User, 100)
	for i := 0; i < 100; i++ {
		users[i] = User{ID: i + 1, Name: "User " + strconv.Itoa(i+1)}
	}
}

func requestHandler(ctx *fasthttp.RequestCtx) {
	path := string(ctx.Path())
	method := string(ctx.Method())

	switch {
	case method == "GET" && string(path) == "/plaintext":
		ctx.SetBodyString("Hello, World!")

	case method == "GET" && string(path) == "/json":
		ctx.SetContentType("application/json")
		body, _ := json.Marshal(map[string]string{"message": "Hello, World!"})
		ctx.SetBody(body)

	case method == "GET" && len(path) > 7 && path[:7] == "/users/":
		id := path[7:]
		ctx.SetContentType("application/json")
		body, _ := json.Marshal(User{Name: "User " + id})
		ctx.SetBody(body)

	case method == "GET" && string(path) == "/search":
		q := string(ctx.QueryArgs().Peek("q"))
		ctx.SetContentType("application/json")
		body, _ := json.Marshal(map[string]string{"query": q})
		ctx.SetBody(body)

	case method == "POST" && string(path) == "/echo":
		ctx.SetContentType("application/json")
		var value map[string]any
		if err := json.Unmarshal(ctx.PostBody(), &value); err != nil {
			ctx.SetStatusCode(400)
			return
		}
		body, _ := json.Marshal(value)
		ctx.SetBody(body)

	case method == "GET" && string(path) == "/users":
		ctx.SetContentType("application/json")
		body, _ := json.Marshal(users)
		ctx.SetBody(body)

	case method == "GET" && path == "/methods/get",
		method == "HEAD" && path == "/methods/head",
		method == "POST" && path == "/methods/post",
		method == "PUT" && path == "/methods/put",
		method == "PATCH" && path == "/methods/patch",
		method == "DELETE" && path == "/methods/delete",
		method == "OPTIONS" && path == "/methods/options",
		method == "CONNECT" && path == "/methods/connect",
		method == "TRACE" && path == "/methods/trace",
		method == "QUERY" && path == "/methods/query":
		ctx.SetBodyString("OK")

	default:
		ctx.SetStatusCode(404)
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
	log.Fatal(server.ListenAndServe(":3004"))
}
