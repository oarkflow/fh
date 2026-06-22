package main

import (
	"fmt"
	"log"
	"net/url"

	"github.com/oarkflow/fh"
)

/*
This example demonstrates all built-in codecs with BodyParser and QueryParser,
covering every supported target type: map, struct, custom binder, and raw any.

Endpoints:

  POST /json-map         JSON body → map[string]any
  POST /json-struct      JSON body → struct with json tags
  POST /xml-struct       XML  body → struct with xml tags
  POST /form-map         form body → map[string]any
  POST /form-struct      form body → struct with form tags
  POST /form-values      form body → url.Values
  POST /form-form        form body → fasthttp.Form (typed)
  POST /form-binder      form body → custom FormBinder
  POST /multipart        multipart body → map[string]any
  POST /ndjson           NDJSON body → []map[string]any
  POST /csv              CSV body → [][]string
  POST /text             text/plain body → *string, *[]byte, *any
  POST /binary           octet-stream body → *[]byte
  POST /custom           custom codec → map[string]any
  GET  /query-map        QueryParser → map[string]any
  GET  /query-struct     QueryParser → struct with form tags
*/

// ── Structs ───────────────────────────────────────────────────────────────────

type Profile struct {
	Name  string `json:"name"  form:"name"`
	Email string `json:"email" form:"email"`
	Age   int    `json:"age"   form:"age"`
}

type Address struct {
	City  string `form:"city"`
	State string `form:"state"`
}

type Person struct {
	Name    string  `form:"name"`
	Address Address `form:"address"`
}

// ── Custom FormBinder ─────────────────────────────────────────────────────────

type MyForm struct {
	Title  string
	Count  int
	Active bool
}

func (m *MyForm) BindForm(f fh.Form) error {
	m.Title = f.First("title")
	m.Count, _ = f.Int("count")
	m.Active, _ = f.Bool("active")
	return nil
}

// ── Custom codec (application/x-myapp) ────────────────────────────────────────

type myCodec struct{}

func (myCodec) ContentType() string { return "application/x-myapp" }
func (myCodec) Unmarshal(data []byte, v any) error {
	// Simple key=value format: name=John&age=30
	switch dst := v.(type) {
	case *map[string]any:
		m := make(map[string]any)
		for i := 0; i < len(data); {
			end := i
			for end < len(data) && data[end] != '&' {
				end++
			}
			pair := data[i:end]
			if eq := indexByte(pair, '='); eq >= 0 {
				key := string(pair[:eq])
				val := string(pair[eq+1:])
				m[key] = val
			}
			i = end + 1
		}
		*dst = m
		return nil
	}
	return fmt.Errorf("myapp: unsupported type %T", v)
}

func (myCodec) Marshal(v any) ([]byte, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("myapp: unsupported marshal type %T", v)
	}
	var buf []byte
	for k, val := range m {
		if len(buf) > 0 {
			buf = append(buf, '&')
		}
		buf = append(buf, k...)
		buf = append(buf, '=')
		buf = append(buf, fmt.Sprint(val)...)
	}
	return buf, nil
}

func indexByte(b []byte, c byte) int {
	for i, v := range b {
		if v == c {
			return i
		}
	}
	return -1
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	app := fh.New()

	// Register custom codec.
	fh.RegisterCodec(myCodec{})

	// ── JSON ────────────────────────────────────────────────────────────────

	app.Post("/json-map", func(c *fh.Ctx) error {
		var data map[string]any
		if err := c.BodyParser(&data); err != nil {
			return c.Status(400).SendString("BodyParser error: " + err.Error())
		}
		return c.JSON(data)
	})

	app.Post("/json-struct", func(c *fh.Ctx) error {
		var p Profile
		if err := c.BodyParser(&p); err != nil {
			return c.Status(400).SendString("BodyParser error: " + err.Error())
		}
		return c.JSON(p)
	})

	// ── XML ─────────────────────────────────────────────────────────────────

	app.Post("/xml-struct", func(c *fh.Ctx) error {
		var p Profile
		if err := c.BodyParser(&p); err != nil {
			return c.Status(400).SendString("BodyParser error: " + err.Error())
		}
		return c.JSON(p)
	})

	// ── Form (application/x-www-form-urlencoded) ────────────────────────────

	app.Post("/form-map", func(c *fh.Ctx) error {
		var data map[string]any
		if err := c.BodyParser(&data); err != nil {
			return c.Status(400).SendString("BodyParser error: " + err.Error())
		}
		return c.JSON(data)
	})

	app.Post("/form-struct", func(c *fh.Ctx) error {
		var p Profile
		if err := c.BodyParser(&p); err != nil {
			return c.Status(400).SendString("BodyParser error: " + err.Error())
		}
		return c.JSON(p)
	})

	app.Post("/form-nested", func(c *fh.Ctx) error {
		var p Person
		if err := c.BodyParser(&p); err != nil {
			return c.Status(400).SendString("BodyParser error: " + err.Error())
		}
		return c.JSON(p)
	})

	app.Post("/form-values", func(c *fh.Ctx) error {
		v := make(url.Values)
		if err := c.BodyParser(&v); err != nil {
			return c.Status(400).SendString("BodyParser error: " + err.Error())
		}
		return c.JSON(map[string][]string(v))
	})

	app.Post("/form-form", func(c *fh.Ctx) error {
		var f fh.Form
		if err := c.BodyParser(&f); err != nil {
			return c.Status(400).SendString("BodyParser error: " + err.Error())
		}
		return c.SendString(f.First("message"))
	})

	app.Post("/form-binder", func(c *fh.Ctx) error {
		var m MyForm
		if err := c.BodyParser(&m); err != nil {
			return c.Status(400).SendString("BodyParser error: " + err.Error())
		}
		return c.JSON(map[string]any{
			"title":  m.Title,
			"count":  m.Count,
			"active": m.Active,
		})
	})

	// ── Multipart ───────────────────────────────────────────────────────────

	app.Post("/multipart", func(c *fh.Ctx) error {
		var data map[string]any
		if err := c.BodyParser(&data); err != nil {
			return c.Status(400).SendString("BodyParser error: " + err.Error())
		}
		return c.JSON(data)
	})

	// ── NDJSON ──────────────────────────────────────────────────────────────

	app.Post("/ndjson", func(c *fh.Ctx) error {
		var data []map[string]any
		if err := c.BodyParser(&data); err != nil {
			return c.Status(400).SendString("BodyParser error: " + err.Error())
		}
		return c.JSON(data)
	})

	// ── CSV ─────────────────────────────────────────────────────────────────

	app.Post("/csv", func(c *fh.Ctx) error {
		var data [][]string
		if err := c.BodyParser(&data); err != nil {
			return c.Status(400).SendString("BodyParser error: " + err.Error())
		}
		return c.JSON(data)
	})

	// ── Text ────────────────────────────────────────────────────────────────

	app.Post("/text-string", func(c *fh.Ctx) error {
		var s string
		if err := c.BodyParser(&s); err != nil {
			return c.Status(400).SendString("BodyParser error: " + err.Error())
		}
		return c.SendString("got: " + s)
	})

	app.Post("/text-bytes", func(c *fh.Ctx) error {
		var b []byte
		if err := c.BodyParser(&b); err != nil {
			return c.Status(400).SendString("BodyParser error: " + err.Error())
		}
		return c.SendString("got " + fmt.Sprint(len(b)) + " bytes")
	})

	app.Post("/text-any", func(c *fh.Ctx) error {
		var v any
		if err := c.BodyParser(&v); err != nil {
			return c.Status(400).SendString("BodyParser error: " + err.Error())
		}
		return c.SendString("got: " + v.(string))
	})

	// ── Binary ──────────────────────────────────────────────────────────────

	app.Post("/binary", func(c *fh.Ctx) error {
		var b []byte
		if err := c.BodyParser(&b); err != nil {
			return c.Status(400).SendString("BodyParser error: " + err.Error())
		}
		return c.SendString("got " + fmt.Sprint(len(b)) + " bytes")
	})

	// ── Custom codec ────────────────────────────────────────────────────────

	app.Post("/custom", func(c *fh.Ctx) error {
		var data map[string]any
		if err := c.BodyParser(&data); err != nil {
			return c.Status(400).SendString("BodyParser error: " + err.Error())
		}
		return c.JSON(data)
	})

	// ── QueryParser ─────────────────────────────────────────────────────────

	app.Get("/query-map", func(c *fh.Ctx) error {
		var q map[string]any
		if err := c.QueryParser(&q); err != nil {
			return c.Status(400).SendString("QueryParser error: " + err.Error())
		}
		return c.JSON(q)
	})

	app.Get("/query-struct", func(c *fh.Ctx) error {
		var p Profile
		if err := c.QueryParser(&p); err != nil {
			return c.Status(400).SendString("QueryParser error: " + err.Error())
		}
		return c.JSON(p)
	})

	// ── Start ────────────────────────────────────────────────────────────────

	fmt.Println("Codec example listening on :3000")
	fmt.Println()
	fmt.Println("Try these requests:")
	fmt.Println()
	fmt.Println("  # JSON body → map")
	fmt.Println(`  curl -X POST -d '{"name":"Alice","email":"a@b.com","age":30}' \`)
	fmt.Println(`    -H "Content-Type: application/json" http://localhost:3000/json-map`)
	fmt.Println()
	fmt.Println("  # JSON body → struct with json tags")
	fmt.Println(`  curl -X POST -d '{"name":"Bob","email":"b@c.com","age":25}' \`)
	fmt.Println(`    -H "Content-Type: application/json" http://localhost:3000/json-struct`)
	fmt.Println()
	fmt.Println("  # XML body → struct")
	fmt.Println(`  curl -X POST -d '<root><name>John</name><email>j@d.com</email><age>35</age></root>' \`)
	fmt.Println(`    -H "Content-Type: application/xml" http://localhost:3000/xml-struct`)
	fmt.Println()
	fmt.Println("  # Form body → map")
	fmt.Println(`  curl -X POST -d 'name=Carol&email=c@e.com&age=28' \`)
	fmt.Println(`    -H "Content-Type: application/x-www-form-urlencoded" \`)
	fmt.Println(`    http://localhost:3000/form-map`)
	fmt.Println()
	fmt.Println("  # Form body → struct (form tags)")
	fmt.Println(`  curl -X POST -d 'name=Dave&email=d@f.com&age=22' \`)
	fmt.Println(`    -H "Content-Type: application/x-www-form-urlencoded" \`)
	fmt.Println(`    http://localhost:3000/form-struct`)
	fmt.Println()
	fmt.Println("  # Form body → nested struct (bracket notation)")
	fmt.Println(`  curl -X POST -d 'name=Eve&address[city]=NYC&address[state]=NY' \`)
	fmt.Println(`    -H "Content-Type: application/x-www-form-urlencoded" \`)
	fmt.Println(`    http://localhost:3000/form-nested`)
	fmt.Println()
	fmt.Println("  # Form body → url.Values")
	fmt.Println(`  curl -X POST -d 'key1=val1&key2=val2' \`)
	fmt.Println(`    -H "Content-Type: application/x-www-form-urlencoded" \`)
	fmt.Println(`    http://localhost:3000/form-values`)
	fmt.Println()
	fmt.Println("  # Form body → custom FormBinder")
	fmt.Println(`  curl -X POST -d 'title=Hello&count=3&active=true' \`)
	fmt.Println(`    -H "Content-Type: application/x-www-form-urlencoded" \`)
	fmt.Println(`    http://localhost:3000/form-binder`)
	fmt.Println()
	fmt.Println("  # Multipart form → map")
	fmt.Println(`  curl -X POST -F 'name=Frank' -F 'age=40' \`)
	fmt.Println(`    http://localhost:3000/multipart`)
	fmt.Println()
	fmt.Println("  # NDJSON → []map[string]any")
	fmt.Println(`  printf '{"a":1}\n{"b":2}\n' | curl -X POST --data-binary @- \`)
	fmt.Println(`    -H "Content-Type: application/x-ndjson" \`)
	fmt.Println(`    http://localhost:3000/ndjson`)
	fmt.Println()
	fmt.Println("  # CSV → [][]string")
	fmt.Println(`  printf 'name,age\nGrace,45\n' | curl -X POST --data-binary @- \`)
	fmt.Println(`    -H "Content-Type: text/csv" http://localhost:3000/csv`)
	fmt.Println()
	fmt.Println("  # Text/plain → *string")
	fmt.Println(`  curl -X POST -d 'hello world' \`)
	fmt.Println(`    -H "Content-Type: text/plain" http://localhost:3000/text-string`)
	fmt.Println()
	fmt.Println("  # Octet-stream → *[]byte")
	fmt.Println(`  curl -X POST --data-binary @/bin/ls \`)
	fmt.Println(`    -H "Content-Type: application/octet-stream" \`)
	fmt.Println(`    http://localhost:3000/binary`)
	fmt.Println()
	fmt.Println("  # Custom codec (application/x-myapp)")
	fmt.Println(`  curl -X POST -d 'name=Hank&role=admin' \`)
	fmt.Println(`    -H "Content-Type: application/x-myapp" http://localhost:3000/custom`)
	fmt.Println()
	fmt.Println("  # QueryParser → map")
	fmt.Println(`  curl 'http://localhost:3000/query-map?name=Iris&age=32'`)
	fmt.Println()
	fmt.Println("  # QueryParser → struct (form tags)")
	fmt.Println(`  curl 'http://localhost:3000/query-struct?name=Jack&email=j@k.com&age=27'`)

	log.Fatal(app.Listen(":3000"))
}
