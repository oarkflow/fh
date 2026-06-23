package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
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

func jsonResponse(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func handler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		switch {
		case r.URL.Path == "/plaintext":
			w.Write([]byte("Hello, World!"))

		case r.URL.Path == "/json":
			jsonResponse(w, map[string]string{"message": "Hello, World!"})

		case strings.HasPrefix(r.URL.Path, "/users/"):
			id := strings.TrimPrefix(r.URL.Path, "/users/")
			if id == "" {
				jsonResponse(w, users)
				return
			}
			jsonResponse(w, User{Name: "User " + id})

		case r.URL.Path == "/search":
			q := r.URL.Query().Get("q")
			jsonResponse(w, map[string]string{"query": q})

		default:
			w.WriteHeader(404)
			w.Write([]byte("Not Found"))
		}

	case "POST":
		if r.URL.Path == "/echo" {
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			jsonResponse(w, body)
		} else {
			w.WriteHeader(404)
			w.Write([]byte("Not Found"))
		}

	default:
		w.WriteHeader(405)
	}
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", handler)
	log.Fatal(http.ListenAndServe(":3005", mux))
}
