package main

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	fh "github.com/orgware/fasthttp"
	"github.com/orgware/fasthttp/middleware"
	"github.com/orgware/fasthttp/template"
)

//go:embed public
var publicFiles embed.FS

var startTime = time.Now()

func main() {
	// ──────────────────────────────────────────────────────────────────────
	// 1. APP CONFIG + CREATION
	// ──────────────────────────────────────────────────────────────────────
	splEngine := template.NewSPL("examples/views")
	splEngine.Config(template.SPLConfig{
		Directory: "examples/views",
		SSR:       true,
		Globals:   map[string]any{"siteName": "SPL Fasthttp Demo"},
	})

	app := fh.New(fh.Config{
		ReadTimeout:         10 * time.Second,
		WriteTimeout:        10 * time.Second,
		IdleTimeout:         60 * time.Second,
		MaxConnections:      1000,
		ReadBufferSize:      8192,
		MaxRequestBodySize:  4 * 1024 * 1024,
		DisableKeepAlive:    false,
		TemplateEngine:      splEngine,
	})

	// ──────────────────────────────────────────────────────────────────────
	// 2. LIFECYCLE HOOKS
	// ──────────────────────────────────────────────────────────────────────
	app.OnListen(func() error {
		log.Println("[OnListen] server started")
		return nil
	})
	app.OnShutdown(func() error {
		log.Println("[OnShutdown] server shutting down")
		return nil
	})
	app.OnConnect(func(conn net.Conn) {
		log.Printf("[OnConnect] %s", conn.RemoteAddr())
	})
	app.OnClose(func(conn net.Conn) {
		log.Printf("[OnClose] %s", conn.RemoteAddr())
	})
	app.OnError(func(err error) {
		log.Printf("[OnError] %v", err)
	})

	// ──────────────────────────────────────────────────────────────────────
	// 3. GLOBAL MIDDLEWARE
	// ──────────────────────────────────────────────────────────────────────
	app.Use(middleware.Logger(middleware.LoggerConfig{
		Format: "[${ip}] ${method} ${path} → ${status} (${latency})\n",
	}))
	app.Use(middleware.SecurityHeaders())
	app.Use(middleware.RequestID())
	app.Use(middleware.Recover())
	app.Use(middleware.CORS(middleware.CORSConfig{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "PATCH"},
		AllowHeaders:     []string{"Content-Type", "Authorization"},
		AllowCredentials: true,
		MaxAge:           86400,
	}))
	app.Use(middleware.RateLimiter(middleware.RateLimiterConfig{
		Max:    100,
		Window: time.Minute,
	}))
	app.Use(middleware.Compress())

	// Per-request start time via Locals
	app.Use(func(ctx *fh.Ctx) error {
		ctx.Locals("start", time.Now())
		return ctx.Next()
	})

	// ──────────────────────────────────────────────────────────────────────
	// 4. ALL HTTP METHODS
	// ──────────────────────────────────────────────────────────────────────

	type Country struct {
		Code   string
		Name   string
		Region string
	}
	type Role struct {
		Value       string
		Label       string
		Permissions []string
	}
	type Priority string
	type FormConfig struct {
		MaxBioLength int
		MinAge       int
		MaxAge       int
		AllowSignup  bool
	}

	app.Get("/", func(ctx *fh.Ctx) error {
		return ctx.Render("index", map[string]any{
			"title": "SPL Template Engine &mdash; Fasthttp Demo",
			"countries": []Country{
				{Code: "us", Name: "United States", Region: "Americas"},
				{Code: "uk", Name: "United Kingdom", Region: "Europe"},
				{Code: "ca", Name: "Canada", Region: "Americas"},
				{Code: "au", Name: "Australia", Region: "Oceania"},
				{Code: "de", Name: "Germany", Region: "Europe"},
				{Code: "jp", Name: "Japan", Region: "Asia"},
				{Code: "in", Name: "India", Region: "Asia"},
			},
			"roles": []Role{
				{Value: "developer", Label: "Developer", Permissions: []string{"read", "write", "deploy"}},
				{Value: "designer", Label: "Designer", Permissions: []string{"read", "write"}},
				{Value: "manager", Label: "Project Manager", Permissions: []string{"read", "write", "admin"}},
				{Value: "devops", Label: "DevOps Engineer", Permissions: []string{"read", "write", "deploy", "admin"}},
			},
			"priorities": []Priority{"low", "medium", "high", "critical"},
			"config": FormConfig{
				MaxBioLength: 280,
				MinAge:       0,
				MaxAge:       150,
				AllowSignup:  true,
			},
			"regionColors": map[string]string{
				"Americas": "#3b82f6",
				"Europe":   "#22c55e",
				"Asia":     "#f59e0b",
				"Oceania":  "#a855f7",
			},
		})
	})

	app.Get("/get", func(ctx *fh.Ctx) error {
		return ctx.SendString("GET response")
	})
	app.Post("/post", func(ctx *fh.Ctx) error {
		return ctx.SendString("POST response")
	})
	app.Put("/put", func(ctx *fh.Ctx) error {
		return ctx.SendString("PUT response")
	})
	app.Delete("/delete", func(ctx *fh.Ctx) error {
		return ctx.SendStatus(204)
	})
	app.Patch("/patch", func(ctx *fh.Ctx) error {
		return ctx.SendString("PATCH response")
	})
	app.Head("/head", func(ctx *fh.Ctx) error {
		return ctx.SendString("")
	})
	app.Options("/options", func(ctx *fh.Ctx) error {
		ctx.Set("Allow", "GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS")
		return ctx.SendStatus(204)
	})
	app.Connect("/connect", func(ctx *fh.Ctx) error {
		return ctx.SendString("CONNECT response")
	})
	app.Trace("/trace", func(ctx *fh.Ctx) error {
		return ctx.Type("message/http").SendString("TRACE response")
	})
	app.All("/any-method", func(ctx *fh.Ctx) error {
		return ctx.JSON(map[string]string{"method": ctx.Method()})
	})

	// ──────────────────────────────────────────────────────────────────────
	// 5. ROUTE PARAMS + QUERY
	// ──────────────────────────────────────────────────────────────────────
	app.Get("/users/:id", func(ctx *fh.Ctx) error {
		return ctx.SendString("user: " + ctx.Param("id"))
	})
	app.Get("/orgs/:org/repos/:repo/commits/:sha", func(ctx *fh.Ctx) error {
		return ctx.JSON(map[string]string{
			"org":  ctx.Param("org"),
			"repo": ctx.Param("repo"),
			"sha":  ctx.Param("sha"),
		})
	})
	app.Get("/static/spl-runtime.min.js", func(ctx *fh.Ctx) error {
		ctx.Set("Content-Type", "application/javascript")
		ctx.Set("Cache-Control", "public, max-age=31536000, immutable")
		return ctx.SendString(splEngine.RuntimeJS())
	})
	subFS, _ := fs.Sub(publicFiles, "public")
	app.StaticFS("/static", subFS, fh.StaticConfig{
		Compress:   true,
		MaxAge:     3600,
		StripSlash: true,
	})
	app.StaticFS("/embed", subFS, fh.StaticConfig{
		Compress:   true,
		StripSlash: true,
	})
	app.Get("/search", func(ctx *fh.Ctx) error {
		q := ctx.Query("q")
		page := ctx.Query("page")
		if page == "" {
			page = "1"
		}
		return ctx.JSON(map[string]any{"query": q, "page": page})
	})

	// ──────────────────────────────────────────────────────────────────────
	// 6. GROUP + NESTED GROUP
	// ──────────────────────────────────────────────────────────────────────
	v1 := app.Group("/api/v1")
	v1.Get("/ping", func(ctx *fh.Ctx) error {
		return ctx.SendString("pong")
	})

	users := v1.Group("/users")
	users.Get("", func(ctx *fh.Ctx) error {
		return ctx.SendString("list users")
	})
	users.Get("/:id", func(ctx *fh.Ctx) error {
		return ctx.SendString("get user " + ctx.Param("id"))
	})

	admin := v1.Group("/admin",
		middleware.BasicAuth("admin", "secret"),
		middleware.IPWhitelist("127.0.0.1", "::1"),
		middleware.Timeout(5*time.Second),
	)
	admin.Get("/stats", func(ctx *fh.Ctx) error {
		return ctx.JSON(map[string]string{"status": "admin only"})
	})

	// ──────────────────────────────────────────────────────────────────────
	// 7. REQUEST DATA
	// ──────────────────────────────────────────────────────────────────────
	type payload struct {
		Name  string `json:"name"`
		Email string `json:"email"`
	}

	app.Post("/body", func(ctx *fh.Ctx) error {
		var p payload
		if err := ctx.BodyParser(&p); err != nil {
			return ctx.Status(400).SendString("bad json: " + err.Error())
		}
		return ctx.JSON(p)
	})
	app.Post("/raw-body", func(ctx *fh.Ctx) error {
		return ctx.SendBytes(ctx.Body())
	})
	app.Get("/req-headers", func(ctx *fh.Ctx) error {
		return ctx.JSON(map[string]string{
			"user-agent":    ctx.Get("User-Agent"),
			"authorization": ctx.Get("Authorization"),
			"ip":            ctx.IP(),
		})
	})
	app.Get("/method-path", func(ctx *fh.Ctx) error {
		return ctx.JSON(map[string]string{
			"method": ctx.Method(),
			"path":   ctx.Path(),
		})
	})
	app.Post("/trailer", func(ctx *fh.Ctx) error {
		return ctx.SendString("trailer: " + ctx.Trailer("X-Checksum"))
	})

	// ──────────────────────────────────────────────────────────────────────
	// 8. CONTEXT
	// ──────────────────────────────────────────────────────────────────────
	app.Get("/context-deadline", func(ctx *fh.Ctx) error {
		deadline, ok := ctx.Context().Deadline()
		if !ok {
			return ctx.SendString("no deadline")
		}
		return ctx.SendString("deadline: " + deadline.Format(time.RFC3339Nano))
	})
	app.Get("/set-context", func(ctx *fh.Ctx) error {
		ctx_timeout, cancel := context.WithTimeout(ctx.Context(), 100*time.Millisecond)
		defer cancel()
		ctx.SetContext(ctx_timeout)
		return ctx.SendString("context with 100ms timeout")
	})
	app.Get("/locals", func(ctx *fh.Ctx) error {
		start := ctx.Locals("start").(time.Time)
		return ctx.SendString("latency so far: " + time.Since(start).String())
	})

	// ──────────────────────────────────────────────────────────────────────
	// 9. RESPONSE BUILDERS
	// ──────────────────────────────────────────────────────────────────────
	app.Get("/json", func(ctx *fh.Ctx) error {
		return ctx.JSON(map[string]any{"message": "hello", "n": 42})
	})
	app.Get("/status-code", func(ctx *fh.Ctx) error {
		return ctx.Status(201).SendString("Created")
	})
	app.Get("/send-bytes", func(ctx *fh.Ctx) error {
		return ctx.Send([]byte("raw bytes"))
	})
	app.Get("/html", func(ctx *fh.Ctx) error {
		html := `<h1>Hello fasthttp</h1><p>HTML response with <code>Type()</code> and <code>SendString()</code>.</p>`
		return ctx.Type("text/html; charset=utf-8").SendString(html)
	})
	app.Get("/type", func(ctx *fh.Ctx) error {
		return ctx.Type("text/csv").SendString("a,b,c\n1,2,3")
	})
	app.Get("/redirect", func(ctx *fh.Ctx) error {
		return ctx.Redirect("/get", 302)
	})
	app.Get("/status-code-get", func(ctx *fh.Ctx) error {
		return ctx.Status(400).SendString("bad request")
	})
	app.Post("/status-code-check", func(ctx *fh.Ctx) error {
		ctx.SendString("ok")
		return nil
	})
	app.Get("/check-status", func(ctx *fh.Ctx) error {
		return ctx.JSON(map[string]int{
			"last_status": ctx.StatusCode(),
		})
	})

	// ──────────────────────────────────────────────────────────────────────
	// 10. RESPONSE HEADERS
	// ──────────────────────────────────────────────────────────────────────
	app.Get("/set-header", func(ctx *fh.Ctx) error {
		ctx.Set("X-Custom", "value1")
		ctx.Append("X-Custom", "value2")
		ctx.Append("X-Custom", "value3")
		return ctx.SendString("check X-Custom header (should be: value1, value2, value3)")
	})

	// ──────────────────────────────────────────────────────────────────────
	// 11. RESPONDED CHECK + TRANSFORM BODY
	// ──────────────────────────────────────────────────────────────────────
	app.Get("/responded", func(ctx *fh.Ctx) error {
		if ctx.Responded() {
			return nil
		}
		return ctx.SendString("first response")
	})
	app.Get("/transform", func(ctx *fh.Ctx) error {
		ctx.TransformBody(func(body []byte) ([]byte, error) {
			return []byte(strings.ToUpper(string(body))), nil
		})
		return ctx.SendString("hello")
	})

	// ──────────────────────────────────────────────────────────────────────
	// 12. STREAMING
	// ──────────────────────────────────────────────────────────────────────
	app.Get("/stream", func(ctx *fh.Ctx) error {
		return ctx.Stream(func(w *fh.StreamWriter) error {
			for i := 0; i < 5; i++ {
				fmt.Fprintf(w, "line %d\n", i+1)
			}
			return nil
		})
	})
	app.Get("/stream-reader", func(ctx *fh.Ctx) error {
		return ctx.SendStream(strings.NewReader("streamed content"))
	})

	// ──────────────────────────────────────────────────────────────────────
	// 13. HIJACK + UPGRADE
	// ──────────────────────────────────────────────────────────────────────
	app.Get("/hijack", func(ctx *fh.Ctx) error {
		return ctx.Hijack(func(conn net.Conn) error {
			defer conn.Close()
			conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nhijacked!\n"))
			return nil
		})
	})
	app.Get("/upgrade", func(ctx *fh.Ctx) error {
		return ctx.Upgrade("echo-protocol", func(conn net.Conn) error {
			defer conn.Close()
			buf := make([]byte, 256)
			for {
				n, err := conn.Read(buf)
				if err != nil {
					return err
				}
				conn.Write([]byte("echo: "))
				conn.Write(buf[:n])
			}
		})
	})

	// ──────────────────────────────────────────────────────────────────────
	// 14. WEBSOCKET
	// ──────────────────────────────────────────────────────────────────────
	app.Get("/ws", fh.WebSocket(func(ws *fh.WSConn) error {
		for {
			op, msg, err := ws.ReadMessage()
			if err != nil {
				return err
			}
			switch op {
			case fh.WSText:
				ws.WriteMessage(fh.WSText, []byte("echo: "+string(msg)))
			case fh.WSClose:
				return nil
			case fh.WSPing:
				ws.WriteMessage(fh.WSPong, nil)
			}
		}
	}))

	// ──────────────────────────────────────────────────────────────────────
	// 15. UPTIME
	// ──────────────────────────────────────────────────────────────────────
	app.Get("/uptime", func(ctx *fh.Ctx) error {
		return ctx.JSON(map[string]string{
			"uptime":     time.Since(startTime).String(),
			"start_time": startTime.Format(time.RFC3339),
		})
	})

	// ──────────────────────────────────────────────────────────────────────
	// 16. SPL TEMPLATE API ENDPOINTS
	// ──────────────────────────────────────────────────────────────────────
	app.Post("/api/submit", func(ctx *fh.Ctx) error {
		var payload map[string]any
		if err := ctx.BodyParser(&payload); err != nil {
			return ctx.Status(400).JSON(map[string]any{"error": "Invalid JSON", "success": false})
		}
		return ctx.JSON(map[string]any{
			"success":   true,
			"message":   "Form submitted successfully!",
			"id":        fmt.Sprintf("SUB-%d", 1000+len(payload)),
			"timestamp": time.Now().Format(time.RFC3339),
		})
	})

	var (
		quoteMu  sync.Mutex
		quoteIdx int
	)
	app.Get("/api/quote", func(ctx *fh.Ctx) error {
		quotes := []map[string]string{
			{"text": "The only way to do great work is to love what you do.", "author": "Steve Jobs"},
			{"text": "Code is like humor. When you have to explain it, it's bad.", "author": "Cory House"},
			{"text": "First, solve the problem. Then, write the code.", "author": "John Johnson"},
			{"text": "Simplicity is the soul of efficiency.", "author": "Austin Freeman"},
			{"text": "Make it work, make it right, make it fast.", "author": "Kent Beck"},
		}
		quoteMu.Lock()
		idx := quoteIdx % len(quotes)
		quoteIdx++
		quoteMu.Unlock()
		return ctx.JSON(quotes[idx])
	})

	var (
		todoMu     sync.Mutex
		todos      []map[string]any
		todoNextID int
	)
	app.Get("/api/todos", func(ctx *fh.Ctx) error {
		todoMu.Lock()
		list := todos
		todoMu.Unlock()
		if list == nil {
			list = []map[string]any{}
		}
		return ctx.JSON(list)
	})
	app.Post("/api/todos", func(ctx *fh.Ctx) error {
		var form map[string]any
		if err := ctx.BodyParser(&form); err != nil {
			return ctx.Status(400).JSON(map[string]any{"error": "Invalid JSON"})
		}
		todoMu.Lock()
		todoNextID++
		todos = append(todos, map[string]any{
			"id":       todoNextID,
			"title":    form["title"],
			"priority": form["priority"],
			"notes":    form["notes"],
		})
		list := make([]map[string]any, len(todos))
		copy(list, todos)
		todoMu.Unlock()
		return ctx.Status(201).JSON(list)
	})

	// ──────────────────────────────────────────────────────────────────────
	// 17. GRACEFUL SHUTDOWN + LISTEN
	// ──────────────────────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("shutting down...")
		if err := app.ShutdownWithTimeout(10 * time.Second); err != nil {
			log.Printf("shutdown error: %v", err)
		}
	}()

	addr := ":8081"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	log.Printf("fasthttp listening on %s", addr)
	if err := app.Listen(addr); err != nil {
		log.Fatal(err)
	}
}
