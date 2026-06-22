package main

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/basicauth"
	"github.com/oarkflow/fh/mw/compress"
	"github.com/oarkflow/fh/mw/cors"
	"github.com/oarkflow/fh/mw/ipwhitelist"
	"github.com/oarkflow/fh/mw/logger"
	"github.com/oarkflow/fh/mw/ratelimiter"
	"github.com/oarkflow/fh/mw/recover"
	"github.com/oarkflow/fh/mw/requestid"
	"github.com/oarkflow/fh/mw/security"
	"github.com/oarkflow/fh/mw/session"
	"github.com/oarkflow/fh/mw/timeout"
	"github.com/oarkflow/fh/pkg/websocket"
	"github.com/oarkflow/template"
)

//go:embed public
var publicFiles embed.FS

var startTime = time.Now()

func main() {
	// ──────────────────────────────────────────────────────────────────────
	// 1. APP CONFIG + CREATION
	// ──────────────────────────────────────────────────────────────────────
	splEngine := template.NewSPL("views")
	splEngine.Config(template.SPLConfig{
		Directory:  "views",
		SSR:        true,
		SecureMode: true,
		Globals:    map[string]any{"siteName": "SPL Fasthttp Demo"},
	})
	splEngine.HydrationRuntimeURL("/static/spl-runtime.min.js?v=" + splEngine.RuntimeVersion())
	splEngine.HydrationAssets("/static/hydration")

	app := fh.New(fh.Config{
		ReadTimeout:        10 * time.Second,
		WriteTimeout:       10 * time.Second,
		IdleTimeout:        60 * time.Second,
		MaxConnections:     1000,
		ReadBufferSize:     8192,
		MaxRequestBodySize: 4 * 1024 * 1024,
		DisableKeepAlive:   false,
		TemplateEngine:     splEngine,
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
	app.Use(logger.New(logger.Config{
		Format: "[${ip}] ${method} ${path} → ${status} (${latency})\n",
	}))
	app.Use(security.New(security.Config{
		ContentSecurityPolicy: "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'self'; frame-ancestors 'none'",
		FrameDeny:             true,
		ContentTypeNosniff:    true,
		XSSProtection:         "0",
		ReferrerPolicy:        "no-referrer",
		PermissionsPolicy:     "geolocation=(), microphone=(), camera=()",
	}))
	app.Use(requestid.New())
	app.Use(recover.New())
	app.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "PATCH"},
		AllowHeaders:     []string{"Content-Type", "Authorization"},
		AllowCredentials: true,
		MaxAge:           86400,
	}))
	app.Use(ratelimiter.New(ratelimiter.Config{
		Max:    100,
		Window: time.Minute,
	}))
	app.Use(compress.New())

	// ──────────────────────────────────────────────────────────────────────
	// 3.5 SESSION
	// ──────────────────────────────────────────────────────────────────────
	sessionStore := session.NewMemoryStore(5 * time.Minute)
	sessionOptions := []session.SessionOption{
		session.SessionCookieName("demo_sid"),
		session.SessionMaxAge(24 * time.Hour),
		session.SessionHTTPOnly(true),
		session.SessionSecure(false), // localhost HTTP only; omit this option behind TLS
		session.SessionPath("/"),
	}
	if secret := os.Getenv("SESSION_SECRET"); len(secret) >= 32 {
		sessionOptions = append(sessionOptions, session.SessionSecret([]byte(secret)))
	} else {
		log.Println("WARNING: SESSION_SECRET is unset; using an ephemeral development key")
	}
	sessionManager := session.NewSessionManager(sessionStore, sessionOptions...)
	app.Use(session.New(sessionManager))

	// Auth middleware: redirects to /login when session has no "user" key.
	auth := func(ctx *fh.Ctx) error {
		s := session.Get(ctx)
		if s.Get("user") == nil {
			return ctx.Redirect("/login", 302)
		}
		return ctx.Next()
	}

	app.Get("/login", func(ctx *fh.Ctx) error {
		s := session.Get(ctx)
		if s.Get("user") != nil {
			return ctx.Redirect("/dashboard", 302)
		}
		return ctx.Render("login", map[string]any{"title": "Sign In", "siteName": "fasthttp", "error": ""})
	})
	app.Post("/api/login", func(ctx *fh.Ctx) error {
		body := string(ctx.Body())
		username := parseFormValue(body, "username")
		password := parseFormValue(body, "password")
		if username != "admin" || password != "password" {
			return ctx.Render("login", map[string]any{"title": "Sign In", "siteName": "fasthttp", "error": "Invalid credentials"})
		}
		s := session.Get(ctx)
		s.Set("user", username)
		s.Set("role", "admin")
		s.Set("loginTime", time.Now().Format(time.RFC3339))
		sessionManager.Regenerate(ctx, s)
		return ctx.Redirect("/dashboard", 302)
	})
	app.Get("/dashboard", auth, func(ctx *fh.Ctx) error {
		s := session.Get(ctx)
		return ctx.Render("dashboard", map[string]any{
			"title":     "Dashboard",
			"siteName":  "fasthttp",
			"user":      s.Get("user"),
			"role":      s.Get("role"),
			"loginTime": s.Get("loginTime"),
			"sessionID": s.ID[:16] + "...",
		})
	})
	app.Post("/api/logout", func(ctx *fh.Ctx) error {
		s := session.Get(ctx)
		sessionManager.Destroy(ctx, s)
		return ctx.Redirect("/login", 302)
	})

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
	app.Get("/static/hydration/:asset", func(ctx *fh.Ctx) error {
		asset, ok := splEngine.HydrationAsset(ctx.Param("asset"))
		if !ok {
			return ctx.SendStatus(404)
		}
		ctx.Set("Content-Type", "application/javascript; charset=utf-8")
		ctx.Set("Cache-Control", "public, max-age=31536000, immutable")
		return ctx.SendString(asset)
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
		basicauth.New("admin", "secret"),
		ipwhitelist.New("127.0.0.1", "::1"),
		timeout.New(5*time.Second),
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
	app.Post("/body-form", func(ctx *fh.Ctx) error {
		var data map[string]any
		if err := ctx.BodyParser(&data); err != nil {
			return ctx.Status(400).SendString("bad form: " + err.Error())
		}
		return ctx.JSON(data)
	})
	app.Get("/query-parse", func(ctx *fh.Ctx) error {
		var q map[string]any
		if err := ctx.QueryParser(&q); err != nil {
			return ctx.Status(400).SendString("query error: " + err.Error())
		}
		return ctx.JSON(q)
	})
	// ──────────────────────────────────────────────────────────────────────
	// 7b. CUSTOM CODEC EXAMPLE
	// ──────────────────────────────────────────────────────────────────────
	// 7b. CUSTOM CODEC EXAMPLE
	// ──────────────────────────────────────────────────────────────────────
	// RegisterCodec is called at init time (simulated here).
	// In a real app, register custom codecs in an init() function or
	// at the start of main().
	fh.RegisterCodec(customCodec{})
	app.Post("/custom-codec", func(ctx *fh.Ctx) error {
		var s string
		if err := ctx.BodyParser(&s); err != nil {
			return ctx.Status(400).SendString("bad custom: " + err.Error())
		}
		return ctx.SendString("custom: " + s)
	})

	// ──────────────────────────────────────────────────────────────────────
	// 7c. TRAILERS (request + response)
	// ──────────────────────────────────────────────────────────────────────
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

	// Trailers with streaming — compute a SHA-256 checksum over the body
	// and send it as a trailer after the final chunk. The Trailer header is
	// announced automatically from SetTrailer calls.
	app.Get("/stream-trailer", func(ctx *fh.Ctx) error {
		hash := sha256.New()
		return ctx.Stream(func(w *fh.StreamWriter) error {
			for i := range 3 {
				chunk := fmt.Appendf(nil, "chunk-%d payload\n", i+1)
				hash.Write(chunk)
				if _, err := w.Write(chunk); err != nil {
					return err
				}
			}
			ctx.SetTrailer("X-Content-SHA256", hex.EncodeToString(hash.Sum(nil)))
			return nil
		})
	})

	// Non-streaming trailers — set before SendString.
	app.Get("/trailer-nonstream", func(ctx *fh.Ctx) error {
		ctx.SetTrailer("X-Checksum", "sha256=abc123def456")
		ctx.SetTrailer("X-Processing-Time", "42ms")
		return ctx.SendString("trailers set on non-streaming response")
	})

	// ──────────────────────────────────────────────────────────────────────
	// 13. HIJACK + UPGRADE
	// ──────────────────────────────────────────────────────────────────────
	app.Get("/hijack", func(ctx *fh.Ctx) error {
		return ctx.Hijack(func(conn *fh.ResponseConn) error {
			defer conn.Close()
			conn.SetHeader(fh.HeaderContentTypeBytes, fh.MimeTextPlainBytes)
			conn.WriteHeader(fh.StatusOK)
			conn.Write([]byte("hijacked!\n"))
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

	wsManager := websocket.NewManager()

	wsConfig := websocket.Config{
		MaxMessageSize:       1 << 20,
		MaxFrameSize:         64 << 10,
		MaxFragments:         32,
		ReadTimeout:          90 * time.Second,
		WriteTimeout:         10 * time.Second,
		PingInterval:         30 * time.Second,
		PongTimeout:          75 * time.Second,
		MaxMessagesPerSecond: 128,
		AllowedOrigins:      []string{"https://yourdomain.com", "http://localhost:3000"},
		Subprotocols:        []string{"json", "chat.v1"},
		EnableHeartbeat:     true,
		Manager:             wsManager,
	}

	// ──────────────────────────────────────────────────────────────────────
	// 14. WEBSOCKET
	// ──────────────────────────────────────────────────────────────────────
	app.Get("/ws", websocket.NewWithConfig(wsConfig, func(ws *websocket.Conn) error {
		for {
			op, msg, err := ws.ReadMessage()
			if err != nil {
				return err
			}
			switch op {
			case websocket.Text:
				ws.WriteMessage(websocket.Text, []byte("echo: "+string(msg)))
			case websocket.Binary:
			if err := ws.WriteMessage(websocket.Binary, msg); err != nil {
				return err
			}
			case websocket.Close:
				return nil
			case websocket.Ping:
				ws.WriteMessage(websocket.Pong, nil)
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

// customCodec demonstrates how to register a custom body codec.
type customCodec struct{}

func (customCodec) ContentType() string { return "application/vnd.myapp+custom" }

func (customCodec) Unmarshal(data []byte, v any) error {
	if len(data) == 0 {
		return nil
	}
	switch dst := v.(type) {
	case *string:
		*dst = "custom(" + string(data) + ")"
		return nil
	case *any:
		*dst = map[string]string{"custom": string(data)}
		return nil
	}
	return fmt.Errorf("custom: unsupported target type %T", v)
}

func parseFormValue(body, key string) string {
	prefix := key + "="
	for _, part := range strings.Split(body, "&") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, prefix) {
			val := part[len(prefix):]
			if decoded, err := url.QueryUnescape(val); err == nil {
				return decoded
			}
			return val
		}
	}
	return ""
}
