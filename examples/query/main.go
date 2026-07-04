package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/fh"
)

// --- Basic handler example (app.Query) ---

type SearchQuery struct {
	Query string `json:"query"`
	Page  int    `json:"page"`
}

type SearchResult struct {
	Title string `json:"title"`
	Score float64 `json:"score"`
}

var fakeIndex = []SearchResult{
	{"fasthttp routing", 0.95},
	{"fasthttp middleware", 0.88},
	{"fasthttp performance", 0.82},
}

func searchHandler(c fh.Ctx) error {
	var q SearchQuery
	if err := c.BodyParser(&q); err != nil {
		return c.Status(fh.StatusBadRequest).SendString("invalid body: " + err.Error())
	}
	if q.Query == "" {
		return c.Status(fh.StatusBadRequest).SendString("query is required")
	}
	if q.Page < 1 {
		q.Page = 1
	}
	start := (q.Page - 1) * 10
	if start > len(fakeIndex) {
		start = len(fakeIndex)
	}
	end := start + 10
	if end > len(fakeIndex) {
		end = len(fakeIndex)
	}
	return c.JSON(fh.Map{
		"results": fakeIndex[start:end],
		"total":   len(fakeIndex),
		"page":    q.Page,
	})
}

// --- Typed handler example (app.QueryTyped) ---

type SearchRequest struct {
	Query string `json:"query" validate:"required"`
	Page  int    `json:"page"`
}

type SearchResponse struct {
	Results []SearchResult `json:"results"`
	Total   int            `json:"total"`
	Page    int            `json:"page"`
}

func main() {
	app := fh.New()

	// Basic QUERY route — same convenience as Get/Post
	app.Query("/search", searchHandler)

	// Typed QUERY route — automatic JSON parse, validate, and response
	app.QueryTyped("/typed/search", func(c fh.Ctx, req SearchRequest) (SearchResponse, error) {
		if req.Page < 1 {
			req.Page = 1
		}
		start := (req.Page - 1) * 10
		if start > len(fakeIndex) {
			start = len(fakeIndex)
		}
		end := start + 10
		if end > len(fakeIndex) {
			end = len(fakeIndex)
		}
		return SearchResponse{
			Results: fakeIndex[start:end],
			Total:   len(fakeIndex),
			Page:    req.Page,
		}, nil
	})

	app.Get("/demo", func(c fh.Ctx) error {
		return c.SendString(`<h1>QUERY method demo</h1>
<p>Send a QUERY request with JSON body to:</p>
<ul>
  <li><code>QUERY /search</code> — basic handler</li>
  <li><code>QUERY /typed/search</code> — typed handler</li>
</ul>
<p>Example:</p>
<pre>
curl -X QUERY http://localhost:8082/search \
  -H "Content-Type: application/json" \
  -d '{"query":"fasthttp","page":1}'
</pre>`)
	})

	fmt.Println("Server starting on :8082")
	log.Fatal(app.Listen(":8082"))
}
