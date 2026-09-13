# Request & Response

## Request

### Body Parsing

```go
// Raw body
bytes := c.Body()        // returns buffer slice (invalid after handler returns)
bytes := c.BodyCopy()    // safe copy
bytes := c.BodyRaw()     // raw bytes before any processing

// Incremental upload consumption (enable with fh.WithStreamRequestBody(true))
err := c.StreamBody(func(r io.Reader) error {
    _, err := io.Copy(destination, r)
    return err
})

// Auto-detect and decode based on Content-Type
var user User
c.BodyParser(&user)

// Defensive codec limits are configured process-wide during startup.
fh.SetCodecOptions(fh.CodecOptions{MaxFormPairs: 5000})
```

### Route Parameters

```go
c.Params("id")            // route param
c.Params("name", "guest") // with default

// Access the raw param map
c.AllParams()             // map[string]string
```

### Query Parameters

```go
c.Query("page")           // single value
c.Query("sort", "asc")    // with default

// Decode query string to struct
var filter Filter
c.QueryParser(&filter)
```

### Headers

```go
c.Get("Content-Type")           // single request header
c.GetReqHeaders()               // all request headers
c.Hostname()                    // host without port
c.IP()                          // remote IP
c.Protocol()                    // http or https
c.Secure()                      // true if TLS
c.ConnectProtocol()             // extended CONNECT protocol, if present
```

### Cookies

```go
cookie := c.GetCookie("session") // get cookie value
c.SetCookie(&fh.Cookie{Name: "theme", Value: "dark", Path: "/"})
c.DelCookie("session")
```

### Multipart / File Upload

```go
form, err := c.MultipartForm()  // parse multipart form
file, err := c.FormFile("avatar") // get uploaded file header
err := c.SaveFile(file, "/uploads/avatar.jpg") // save to disk
```

### Request Metadata

```go
c.Method()                      // HTTP method
c.Path()                        // request path
c.OriginalURL()                 // original URL with query string
c.Method() == fh.MethodGET      // method check
```

### Locals (Request-Scoped Storage)

```go
c.Locals("key", value)          // set
val := c.Locals("key")          // get (returns any)
```

### Context

```go
ctx := c.Context()              // get Go context
c.SetContext(ctx)               // set Go context (for cancellation/deadline)
```

### Trailers (Chunked Transfer)

```go
trailer := c.Trailer("ETag")    // get trailer value
```

### Flash Messages (Requires Session Middleware)

```go
c.Flash("message", "Saved!")    // set flash
msg := c.Flash("message")       // get and delete flash
```

### Reliability

```go
c.ServerOutbox()                // reliability outbox
c.ServerInbox()                 // reliability inbox
```

---

## Response

### Response Writers

```go
// Text
c.SendString("hello")           // text/plain
c.SendBytes([]byte{1,2,3})      // binary
c.Send(data)                    // alias for SendBytes

// Structured
c.JSON(data)                    // application/json
c.XML(data)                     // application/xml
c.HTML("<h1>Title</h1>")       // text/html

// Files
c.SendFile("doc.pdf")           // file download
c.SendStream(reader)            // stream from io.Reader

// Status
c.SendStatus(204)               // status only, no body
c.SendStatus(201)               // with generated status text

// Redirect
c.Redirect("/login")            // default 302
c.Redirect("/login", 301)       // permanent redirect
c.RedirectTo("user.profile", map[string]string{"id": "42"})
c.RedirectBack("/", 302)        // redirect to referrer or fallback

// Templates
c.Render("index", data)         // render template
c.Render("index", data, "main") // render with layout

// Error Details (RFC 9457)
c.Problem(fh.Problem{
    Type:   "https://example.com/errors/validation",
    Title:  "Validation Error",
    Detail: "Invalid email format",
    Status: 422,
    Extensions: map[string]any{
        "field": "email",
    },
})

// Streaming
c.Stream(func(w *fh.StreamWriter) error {
    for i := 0; i < 10; i++ {
        if _, err := fmt.Fprintf(w, "chunk %d\n", i); err != nil {
            return err
        }
        time.Sleep(100 * time.Millisecond)
    }
    return nil
})

// Server-Sent Events
c.SSE(func(events *fh.SSE) error {
    for i := 0; i < 5; i++ {
        if err := events.WriteEvent(fh.SSEEvent{Event: "update", Data: "hello"}); err != nil {
            return err
        }
        time.Sleep(1 * time.Second)
    }
    return nil
})

// Protocol Upgrade
c.Hijack(func(conn *fh.ResponseConn) error {
    // raw connection access
    return nil
})
c.Upgrade("example-protocol", func(conn net.Conn) error {
    // upgraded HTTP/1 connection or HTTP/2 extended-CONNECT stream
    return nil
})
```

### Response Headers

```go
c.Set("X-Custom", "value")      // set response header
for key, value := range headers { c.Set(key, value) }
c.Type("json")                  // set Content-Type shortcut
c.Append("Vary", "Accept")      // append to header
```

### Response Status

```go
c.Status(201)                   // set status code (chainable)
```

### Cookies

```go
c.SetCookie(&fh.Cookie{
    Name:     "session",
    Value:    "abc123",
    HTTPOnly: true,
    Secure:   true,
    MaxAge:   3600,
    Path:     "/",
    SameSite: fh.SameSiteLax,
})
```

### Hooks

```go
c.OnBeforeResponse(func(c fh.Ctx) error {
    // called just before response is written
    return nil
})
```
