package main

import (
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
		ctx.SetBodyString(`{"message":"Hello, World!"}`)

	case method == "GET" && len(path) > 7 && path[:7] == "/users/":
		id := path[7:]
		ctx.SetContentType("application/json")
		ctx.SetBodyString(`{"id":0,"name":"User ` + id + `"}`)

	case method == "GET" && string(path) == "/search":
		q := string(ctx.QueryArgs().Peek("q"))
		ctx.SetContentType("application/json")
		ctx.SetBodyString(`{"query":"` + q + `"}`)

	case method == "POST" && string(path) == "/echo":
		ctx.SetContentType("application/json")
		ctx.SetBody(ctx.PostBody())

	case method == "GET" && string(path) == "/users":
		ctx.SetContentType("application/json")
		usersJson := `[`
		for i, u := range users {
			if i > 0 {
				usersJson += `,`
			}
			usersJson += `{"id":` + strconv.Itoa(u.ID) + `,"name":"` + u.Name + `"}`
		}
		usersJson += `]`
		ctx.SetBodyString(usersJson)

	default:
		ctx.SetStatusCode(404)
		ctx.SetBodyString("Not Found")
	}
}

func main() {
	log.Fatal(fasthttp.ListenAndServe(":3004", requestHandler))
}
