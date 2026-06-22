package fh_test

import (
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/compress"
)

func TestStaticFileServing(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("hello world"), 0644)
	os.MkdirAll(filepath.Join(dir, "sub"), 0755)
	os.WriteFile(filepath.Join(dir, "sub", "nested.txt"), []byte("nested"), 0644)

	app := fh.New()
	app.Static("/static", dir)

	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/static/hello.txt", "", nil)
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}
	if body != "hello world" {
		t.Fatalf("expected 'hello world', got %q", body)
	}
}

func TestStaticSubpath(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "sub"), 0755)
	os.WriteFile(filepath.Join(dir, "sub", "nested.txt"), []byte("nested"), 0644)

	app := fh.New()
	app.Static("/static", dir)

	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/static/sub/nested.txt", "", nil)
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}
	if body != "nested" {
		t.Fatalf("expected 'nested', got %q", body)
	}
}

func TestStaticNotFound(t *testing.T) {
	dir := t.TempDir()
	app := fh.New()
	app.Static("/static", dir)
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/static/missing.txt", "", nil)
	if code != 404 {
		t.Fatalf("expected 404, got %d", code)
	}
	if body == "" {
		t.Fatal("expected non-empty body for 404")
	}
}

func TestStaticIndexFile(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "index.html"), []byte("<h1>index</h1>"), 0644)

	app := fh.New()
	app.Static("/static", dir)
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/static/", "", nil)
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}
	if body != "<h1>index</h1>" {
		t.Fatalf("expected '<h1>index</h1>', got %q", body)
	}
}

func TestStaticRedirectToTrailingSlash(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "mydir"), 0755)
	os.WriteFile(filepath.Join(dir, "mydir", "index.html"), []byte("dir index"), 0644)

	app := fh.New()
	app.Static("/static", dir)
	addr := testServer(t, app)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.Write([]byte("GET /static/mydir HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
	conn.(*net.TCPConn).CloseWrite()
	resp, _ := io.ReadAll(conn)

	if !strings.Contains(string(resp), "301") {
		t.Fatalf("expected 301 redirect, got: %s", resp)
	}
	if !strings.Contains(string(resp), "Location: /static/mydir/") {
		t.Fatalf("expected Location: /static/mydir/, got: %s", resp)
	}
}

func TestStaticETag(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "file.txt"), []byte("content"), 0644)

	app := fh.New()
	app.Static("/static", dir)
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/static/file.txt", "", nil)
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}
	if !strings.Contains(body, "content") {
		t.Fatalf("expected body to contain 'content', got %q", body)
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	req := "GET /static/file.txt HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
	conn.Write([]byte(req))
	conn.(*net.TCPConn).CloseWrite()
	resp, _ := io.ReadAll(conn)

	if !strings.Contains(string(resp), "ETag") {
		t.Fatalf("expected ETag header in response, got: %s", resp)
	}
}

func TestStaticETagMatch(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "file.txt"), []byte("content"), 0644)

	app := fh.New()
	app.Static("/static", dir)
	addr := testServer(t, app)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	req := "GET /static/file.txt HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
	conn.Write([]byte(req))
	conn.(*net.TCPConn).CloseWrite()
	resp, _ := io.ReadAll(conn)
	conn.Close()

	etag := extractHeader(string(resp), "ETag")
	if etag == "" {
		t.Fatal("expected ETag header")
	}

	code, body := doRequest(t, addr, "GET", "/static/file.txt", "", map[string]string{
		"If-None-Match": etag,
	})
	if code != 304 {
		t.Fatalf("expected 304 for matching ETag, got %d", code)
	}
	if body != "" {
		t.Fatalf("expected empty body for 304, got %q", body)
	}
}

func TestStaticIfModifiedSince(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "file.txt"), []byte("content"), 0644)

	app := fh.New()
	app.Static("/static", dir)
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/static/file.txt", "", map[string]string{
		"If-Modified-Since": "Fri, 01 Jan 2099 00:00:00 GMT",
	})
	if code != 304 {
		t.Fatalf("expected 304 for If-Modified-Since in future, got %d", code)
	}
	if body != "" {
		t.Fatalf("expected empty body for 304, got %q", body)
	}
}

func TestStaticCacheControl(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "file.txt"), []byte("content"), 0644)

	app := fh.New()
	app.Static("/static", dir, fh.StaticConfig{
		MaxAge: 3600,
	})
	addr := testServer(t, app)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	req := "GET /static/file.txt HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
	conn.Write([]byte(req))
	conn.(*net.TCPConn).CloseWrite()
	resp, _ := io.ReadAll(conn)

	if !strings.Contains(string(resp), "Cache-Control: public, max-age=3600") {
		t.Fatalf("expected Cache-Control header, got: %s", resp)
	}
}

func TestStaticLastModified(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "file.txt"), []byte("content"), 0644)

	app := fh.New()
	app.Static("/static", dir)
	addr := testServer(t, app)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	req := "GET /static/file.txt HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
	conn.Write([]byte(req))
	conn.(*net.TCPConn).CloseWrite()
	resp, _ := io.ReadAll(conn)

	if !strings.Contains(string(resp), "Last-Modified") {
		t.Fatalf("expected Last-Modified header, got: %s", resp)
	}
}

func TestStaticCompression(t *testing.T) {
	dir := t.TempDir()
	content := strings.Repeat("hello world ", 100)
	os.WriteFile(filepath.Join(dir, "file.txt"), []byte(content), 0644)

	app := fh.New()
	app.Static("/static", dir, fh.StaticConfig{
		Compress: true,
	})
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/static/file.txt", "", map[string]string{
		"Accept-Encoding": "gzip",
	})
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}

	if code == 200 {
		if body == content {
			t.Fatal("expected compressed body, got uncompressed")
		}
	}
}

func TestStaticContentType(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "style.css"), []byte("body { color: red; }"), 0644)
	os.WriteFile(filepath.Join(dir, "script.js"), []byte("console.log('hi');"), 0644)
	os.WriteFile(filepath.Join(dir, "data.json"), []byte(`{"key": "value"}`), 0644)

	app := fh.New()
	app.Static("/static", dir)
	addr := testServer(t, app)

	tests := []struct {
		path        string
		contentType string
	}{
		{"/static/style.css", "text/css"},
		{"/static/script.js", "text/javascript"},
		{"/static/data.json", "application/json"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			req := "GET " + tt.path + " HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
			conn.Write([]byte(req))
			conn.(*net.TCPConn).CloseWrite()
			resp, _ := io.ReadAll(conn)
			conn.Close()

			if !strings.Contains(string(resp), tt.contentType) {
				t.Fatalf("expected Content-Type %q in response, got: %s", tt.contentType, resp)
			}
		})
	}
}

func TestStaticGroup(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("group static"), 0644)

	app := fh.New()
	v1 := app.Group("/v1")
	v1.Static("/static", dir)
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/v1/static/hello.txt", "", nil)
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}
	if body != "group static" {
		t.Fatalf("expected 'group static', got %q", body)
	}
}

func TestStaticFS(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "embed.txt"), []byte("embedded"), 0644)
	filesys := os.DirFS(dir)

	app := fh.New()
	app.StaticFS("/embed", filesys)
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/embed/embed.txt", "", nil)
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}
	if body != "embedded" {
		t.Fatalf("expected 'embedded', got %q", body)
	}
}

func TestStaticGroupFS(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "data.txt"), []byte("group embed"), 0644)
	filesys := os.DirFS(dir)

	app := fh.New()
	api := app.Group("/api")
	api.StaticFS("/files", filesys)
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/api/files/data.txt", "", nil)
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}
	if body != "group embed" {
		t.Fatalf("expected 'group embed', got %q", body)
	}
}

func TestStaticBrowse(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a.txt"), []byte("file a"), 0644)
	os.MkdirAll(filepath.Join(dir, "subdir"), 0755)
	os.WriteFile(filepath.Join(dir, "subdir", "b.txt"), []byte("file b"), 0644)

	app := fh.New()
	app.Static("/static", dir, fh.StaticConfig{
		Browse: true,
	})
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/static/", "", nil)
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}
	if !strings.Contains(body, "a.txt") {
		t.Fatalf("expected directory listing to contain 'a.txt', got: %s", body)
	}
	if !strings.Contains(body, "subdir/") {
		t.Fatalf("expected directory listing to contain 'subdir/', got: %s", body)
	}
}

func TestStaticBrowseForbidden(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "subdir"), 0755)

	app := fh.New()
	app.Static("/static", dir) // Browse defaults to false
	addr := testServer(t, app)

	code, _ := doRequest(t, addr, "GET", "/static/subdir/", "", nil)
	if code != 403 {
		t.Fatalf("expected 403, got %d", code)
	}
}

func TestStaticCustomIndex(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "default.htm"), []byte("custom index"), 0644)

	app := fh.New()
	app.Static("/static", dir, fh.StaticConfig{
		Index: "default.htm",
	})
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/static/", "", nil)
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}
	if body != "custom index" {
		t.Fatalf("expected 'custom index', got %q", body)
	}
}

func TestStaticRootPrefix(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "index.html"), []byte("root index"), 0644)
	os.WriteFile(filepath.Join(dir, "app.js"), []byte("root js"), 0644)

	app := fh.New()
	app.Static("/", dir)
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/", "", nil)
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}
	if body != "root index" {
		t.Fatalf("expected 'root index', got %q", body)
	}

	code, body = doRequest(t, addr, "GET", "/app.js", "", nil)
	if code != 200 {
		t.Fatalf("expected 200 for /app.js, got %d", code)
	}
	if body != "root js" {
		t.Fatalf("expected 'root js', got %q", body)
	}
}

func TestStaticHeadRequest(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "file.txt"), []byte("content"), 0644)

	app := fh.New()
	app.Static("/static", dir)
	addr := testServer(t, app)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	req := "HEAD /static/file.txt HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
	conn.Write([]byte(req))
	conn.(*net.TCPConn).CloseWrite()
	resp, _ := io.ReadAll(conn)

	if !strings.Contains(string(resp), "200") {
		t.Fatalf("expected 200 for HEAD, got: %s", resp)
	}
}

type noopFS struct{}

func (n noopFS) Open(name string) (fs.File, error) {
	return nil, fs.ErrNotExist
}

func TestStaticFSNotExist(t *testing.T) {
	app := fh.New()
	app.StaticFS("/noop", noopFS{})
	addr := testServer(t, app)

	code, _ := doRequest(t, addr, "GET", "/noop/anything", "", nil)
	if code != 404 {
		t.Fatalf("expected 404, got %d", code)
	}
}

// ── Helpers ────────────────────────────────────────────────────────────

func extractHeader(resp, name string) string {
	for _, line := range strings.Split(resp, "\r\n") {
		if strings.HasPrefix(line, name+": ") {
			return strings.TrimPrefix(line, name+": ")
		}
	}
	return ""
}

func TestStaticForbidsDirectoryTraversal(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "safe.txt"), []byte("safe"), 0644)

	app := fh.New()
	app.Static("/static", dir)
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/static/../../../etc/passwd", "", nil)
	if code != 404 {
		t.Fatalf("expected 404 for directory traversal, got %d (body: %q)", code, body)
	}
}

func TestStaticInlineCompressionWithMiddleware(t *testing.T) {
	dir := t.TempDir()
	content := strings.Repeat("x", 1000)
	os.WriteFile(filepath.Join(dir, "data.txt"), []byte(content), 0644)

	app := fh.New()
	app.Use(compress.New())
	app.Static("/static", dir, fh.StaticConfig{
		Compress: true,
	})
	addr := testServer(t, app)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.Write([]byte("GET /static/data.txt HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: gzip\r\nConnection: close\r\n\r\n"))
	conn.(*net.TCPConn).CloseWrite()
	resp, _ := io.ReadAll(conn)
	respStr := string(resp)

	if !strings.Contains(respStr, "200") {
		t.Fatalf("expected 200, got: %s", respStr)
	}
	if strings.Contains(respStr, "Content-Encoding: gzip") {
		t.Log("compression applied (by middleware or inline)")
	}
}

func TestStaticStripSlash(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(dir+"/index.html", []byte("<h1>index</h1>"), 0644)
	os.MkdirAll(dir+"/sub", 0755)
	os.WriteFile(dir+"/sub/file.txt", []byte("nested"), 0644)

	app := fh.New()
	app.Static("/static", dir, fh.StaticConfig{
		Browse:     true,
		StripSlash: true,
	})
	addr := testServer(t, app)

	// Both with and without trailing slash should serve the same content
	for _, path := range []string{"/static", "/static/"} {
		code, body := doRequest(t, addr, "GET", path, "", nil)
		if code != 200 {
			t.Fatalf("GET %s: expected 200, got %d (body: %q)", path, code, body)
		}
		if !strings.Contains(body, "<h1>index</h1>") {
			t.Fatalf("GET %s: expected index content, got %q", path, body)
		}
	}

	// Sub-path with trailing slash should also work
	code, body := doRequest(t, addr, "GET", "/static/sub/", "", nil)
	if code != 200 {
		t.Fatalf("GET /static/sub/: expected 200, got %d (body: %q)", code, body)
	}
	if !strings.Contains(body, "file.txt") {
		t.Fatalf("GET /static/sub/: expected directory listing, got %q", body)
	}

	// No redirect should happen
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.Write([]byte("GET /static HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
	conn.(*net.TCPConn).CloseWrite()
	resp, _ := io.ReadAll(conn)
	if strings.Contains(string(resp), "301") {
		t.Fatal("expected no redirect with StripSlash")
	}
}

func TestStaticStripSlashFS(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(dir+"/index.html", []byte("<h1>index</h1>"), 0644)

	app := fh.New()
	app.StaticFS("/embed", os.DirFS(dir), fh.StaticConfig{
		Browse:     true,
		StripSlash: true,
	})
	addr := testServer(t, app)

	for _, path := range []string{"/embed", "/embed/"} {
		code, body := doRequest(t, addr, "GET", path, "", nil)
		if code != 200 {
			t.Fatalf("GET %s: expected 200, got %d (body: %q)", path, code, body)
		}
		if !strings.Contains(body, "<h1>index</h1>") {
			t.Fatalf("GET %s: expected index content, got %q", path, body)
		}
	}
}

func TestStaticGroupStripSlash(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(dir+"/index.html", []byte("<h1>index</h1>"), 0644)

	app := fh.New()
	g := app.Group("/v1")
	g.Static("/static", dir, fh.StaticConfig{
		Browse:     true,
		StripSlash: true,
	})
	addr := testServer(t, app)

	for _, path := range []string{"/v1/static", "/v1/static/"} {
		code, body := doRequest(t, addr, "GET", path, "", nil)
		if code != 200 {
			t.Fatalf("GET %s: expected 200, got %d (body: %q)", path, code, body)
		}
		if !strings.Contains(body, "<h1>index</h1>") {
			t.Fatalf("GET %s: expected index content, got %q", path, body)
		}
	}
}
