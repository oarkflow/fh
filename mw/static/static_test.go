package static

import (
	"fmt"
	"io"
	"mime"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

func testServer(t *testing.T, app *fh.App) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { app.Shutdown() })
	go app.Serve(ln)
	time.Sleep(10 * time.Millisecond)
	return ln.Addr().String()
}

// rawResponse is a minimally parsed HTTP/1.1 response: status code, headers
// (first value per name), and body.
type rawResponse struct {
	status  int
	headers map[string]string
	body    string
}

func rawGet(t *testing.T, addr, target string, extraHeaders ...string) rawResponse {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n", target)
	for _, h := range extraHeaders {
		req += h + "\r\n"
	}
	req += "\r\n"

	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	return parseResponse(t, raw)
}

func parseResponse(t *testing.T, raw []byte) rawResponse {
	t.Helper()
	s := string(raw)
	headEnd := strings.Index(s, "\r\n\r\n")
	if headEnd < 0 {
		t.Fatalf("malformed response (no header terminator): %q", s)
	}
	head := s[:headEnd]
	body := s[headEnd+4:]
	lines := strings.Split(head, "\r\n")
	var proto, statusStr string
	fmt.Sscan(lines[0], &proto, &statusStr)
	var code int
	fmt.Sscan(statusStr, &code)

	headers := map[string]string{}
	for _, line := range lines[1:] {
		idx := strings.Index(line, ":")
		if idx < 0 {
			continue
		}
		k := strings.TrimSpace(line[:idx])
		v := strings.TrimSpace(line[idx+1:])
		headers[strings.ToLower(k)] = v
	}
	return rawResponse{status: code, headers: headers, body: body}
}

func mustWriteFile(t *testing.T, p string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// ── Path traversal / root confinement ───────────────────────────────────────

// TestServesFileWithinRoot is the baseline sanity check that ordinary
// requests are served from the configured root.
func TestServesFileWithinRoot(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "ok.txt"), []byte("ok-content"))

	app := fh.New()
	app.Get("/assets/*", New(root))
	addr := testServer(t, app)

	resp := rawGet(t, addr, "/assets/ok.txt")
	if resp.status != fh.StatusOK {
		t.Fatalf("expected 200, got %d", resp.status)
	}
	if resp.body != "ok-content" {
		t.Fatalf("expected body %q, got %q", "ok-content", resp.body)
	}
}

// TestPathTraversalNeverEscapesRoot is the core security regression guard:
// every classic directory-escape technique (plain "..", encoded "..",
// doubled slashes, deeply nested "..") must be rejected rather than leaking
// a file that lives outside the configured root. A regression here (e.g. a
// change to cleanRootName that stops anchoring the path at "/") would let
// an attacker read arbitrary files on the host.
func TestPathTraversalNeverEscapesRoot(t *testing.T) {
	root := t.TempDir()
	pub := filepath.Join(root, "public")
	mustWriteFile(t, filepath.Join(pub, "ok.txt"), []byte("ok-content"))

	outside := t.TempDir()
	mustWriteFile(t, filepath.Join(outside, "secret.txt"), []byte("SECRET-OUTSIDE-ROOT"))

	app := fh.New()
	app.Get("/assets/*", New(pub))
	addr := testServer(t, app)

	targets := []string{
		"/assets/../../../../../../../../etc/passwd",
		"/assets/..%2f..%2f..%2f..%2fetc%2fpasswd",
		"/assets/%2e%2e/%2e%2e/etc/passwd",
		"/assets/....//....//....//etc/passwd",
		"/assets/..\\..\\etc\\passwd",
	}
	for _, target := range targets {
		resp := rawGet(t, addr, target)
		if strings.Contains(resp.body, "SECRET") || strings.Contains(resp.body, "root:") {
			t.Fatalf("PATH TRAVERSAL: %s leaked content outside root: status=%d body=%q", target, resp.status, resp.body)
		}
		if resp.status == fh.StatusOK {
			t.Fatalf("PATH TRAVERSAL: %s unexpectedly returned 200 (body=%q)", target, resp.body)
		}
	}

	// A traversal sequence that lexically resolves to a real file outside the
	// root's parent must also fail: point directly at the sibling secret
	// using the exact number of ".." segments needed to reach it.
	rel, err := filepath.Rel(pub, filepath.Join(outside, "secret.txt"))
	if err != nil {
		t.Fatal(err)
	}
	target := "/assets/" + filepath.ToSlash(rel)
	resp := rawGet(t, addr, target)
	if strings.Contains(resp.body, "SECRET-OUTSIDE-ROOT") {
		t.Fatalf("PATH TRAVERSAL: exact relative escape %q leaked the secret file (status=%d)", target, resp.status)
	}

	// The legitimate file must still be reachable — traversal defenses must
	// not be so aggressive that they break normal requests.
	if ok := rawGet(t, addr, "/assets/ok.txt"); ok.status != fh.StatusOK || ok.body != "ok-content" {
		t.Fatalf("expected legitimate file to still be served, got status=%d body=%q", ok.status, ok.body)
	}
}

// TestSymlinkEscapeNeverLeaksOutsideRoot guards against a symlink placed
// inside the served root (whether by an attacker with partial write access,
// or accidentally) that points outside the root. Neither a symlinked file
// nor a symlinked directory must allow reading content that lives outside
// the configured root.
func TestSymlinkEscapeNeverLeaksOutsideRoot(t *testing.T) {
	root := t.TempDir()
	pub := filepath.Join(root, "public")
	if err := os.MkdirAll(pub, 0o755); err != nil {
		t.Fatal(err)
	}

	outside := t.TempDir()
	secretFile := filepath.Join(outside, "secret.txt")
	mustWriteFile(t, secretFile, []byte("SECRET-DATA"))

	if err := os.Symlink(secretFile, filepath.Join(pub, "escape.txt")); err != nil {
		t.Skipf("symlink not supported on this platform: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(pub, "escapedir")); err != nil {
		t.Fatalf("symlink dir: %v", err)
	}

	app := fh.New()
	app.Get("/pub/*", New(pub))
	addr := testServer(t, app)

	for _, target := range []string{"/pub/escape.txt", "/pub/escapedir/secret.txt"} {
		resp := rawGet(t, addr, target)
		if strings.Contains(resp.body, "SECRET-DATA") {
			t.Fatalf("SYMLINK ESCAPE: %s leaked outside-root content (status=%d, body=%q)", target, resp.status, resp.body)
		}
	}
}

// TestDoubleDotSegmentInsideValidPathIsRejected verifies a request that
// mixes a legitimate leading segment with an embedded ".." component (as
// opposed to a pure ".." prefix) is still confined.
func TestDoubleDotSegmentInsideValidPathIsRejected(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "ok.txt"), []byte("ok-content"))
	outside := t.TempDir()
	mustWriteFile(t, filepath.Join(outside, "secret.txt"), []byte("SECRET"))

	app := fh.New()
	app.Get("/assets/*", New(root))
	addr := testServer(t, app)

	rel, err := filepath.Rel(root, filepath.Join(outside, "secret.txt"))
	if err != nil {
		t.Fatal(err)
	}
	target := "/assets/subdir/" + filepath.ToSlash(rel)
	resp := rawGet(t, addr, target)
	if strings.Contains(resp.body, "SECRET") {
		t.Fatalf("PATH TRAVERSAL via embedded '..': %s leaked secret (status=%d)", target, resp.status)
	}
}

// ── Directory / index / SPA fallback behavior ───────────────────────────────

func TestDirectoryWithIndexServesIndexHTML(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "index.html"), []byte("<h1>home</h1>"))

	app := fh.New()
	app.Get("/*", New(root))
	addr := testServer(t, app)

	resp := rawGet(t, addr, "/")
	if resp.status != fh.StatusOK || resp.body != "<h1>home</h1>" {
		t.Fatalf("expected index.html to be served, got status=%d body=%q", resp.status, resp.body)
	}
}

// TestDirectoryWithoutIndexAndBrowseDisabledIsForbidden ensures the default
// (Browse:false) is fail-closed: a directory with no index file must not be
// listable or otherwise disclose its contents.
func TestDirectoryWithoutIndexAndBrowseDisabledIsForbidden(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "dir", "secret.txt"), []byte("secret"))

	app := fh.New()
	app.Get("/assets/*", New(root))
	addr := testServer(t, app)

	resp := rawGet(t, addr, "/assets/dir")
	if resp.status != fh.StatusForbidden {
		t.Fatalf("expected 403 for unlisted directory, got %d (body=%q)", resp.status, resp.body)
	}
}

func TestBrowseListsDirectoryAndEscapesFilenames(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "normal.txt"), []byte("x"))
	mustWriteFile(t, filepath.Join(root, ".hidden"), []byte("x"))
	// A filename containing HTML metacharacters must be escaped in the
	// generated listing, not injected raw (stored XSS via filename).
	xssName := `"><img src=x onerror=alert(1)>.txt`
	mustWriteFile(t, filepath.Join(root, xssName), []byte("x"))

	app := fh.New()
	app.Get("/assets/*", New(root, Config{Root: root, Prefix: "/assets", Browse: true}))
	addr := testServer(t, app)

	resp := rawGet(t, addr, "/assets/")
	if resp.status != fh.StatusOK {
		t.Fatalf("expected 200 for directory listing, got %d", resp.status)
	}
	if strings.Contains(resp.body, ".hidden") {
		t.Fatalf("hidden (dotfile) entry must not appear in directory listing: %q", resp.body)
	}
	if strings.Contains(resp.body, "<img src=x onerror=alert(1)>") {
		t.Fatalf("STORED XSS: unescaped filename injected into directory listing HTML: %q", resp.body)
	}
	if !strings.Contains(resp.body, "normal.txt") {
		t.Fatalf("expected normal.txt to be listed: %q", resp.body)
	}
}

func TestSPAFallbackServesFallbackFileForUnknownPaths(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "index.html"), []byte("<app-shell>"))

	app := fh.New()
	app.Get("/*", New(root, Config{Root: root, SPAFallback: "index.html"}))
	addr := testServer(t, app)

	resp := rawGet(t, addr, "/some/client/route")
	if resp.status != fh.StatusOK || resp.body != "<app-shell>" {
		t.Fatalf("expected SPA fallback to serve index.html, got status=%d body=%q", resp.status, resp.body)
	}
}

func TestNoSPAFallbackReturnsNotFoundForUnknownPaths(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "index.html"), []byte("<app-shell>"))

	app := fh.New()
	app.Get("/*", New(root))
	addr := testServer(t, app)

	resp := rawGet(t, addr, "/some/client/route")
	if resp.status != fh.StatusNotFound {
		t.Fatalf("expected 404 without SPAFallback configured, got %d", resp.status)
	}
}

// ── Headers: content-type, cache-control, ETag, Content-Disposition ────────

func TestContentTypeMatchesExtension(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "app.js"), []byte("console.log(1)"))

	app := fh.New()
	app.Get("/assets/*", New(root))
	addr := testServer(t, app)

	resp := rawGet(t, addr, "/assets/app.js")
	want := mime.TypeByExtension(".js")
	if want != "" && resp.headers["content-type"] != want {
		t.Fatalf("expected Content-Type %q, got %q", want, resp.headers["content-type"])
	}
}

func TestCacheControlPrecedence(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "a.txt"), []byte("a"))

	t.Run("ExplicitCacheControlWins", func(t *testing.T) {
		app := fh.New()
		app.Get("/assets/*", New(root, Config{Root: root, CacheControl: "no-store", Immutable: true, MaxAge: time.Hour}))
		addr := testServer(t, app)
		resp := rawGet(t, addr, "/assets/a.txt")
		if resp.headers["cache-control"] != "no-store" {
			t.Fatalf("expected explicit CacheControl to take precedence, got %q", resp.headers["cache-control"])
		}
	})

	t.Run("ImmutableSetsLongLivedCache", func(t *testing.T) {
		app := fh.New()
		app.Get("/assets/*", New(root, Config{Root: root, Immutable: true}))
		addr := testServer(t, app)
		resp := rawGet(t, addr, "/assets/a.txt")
		if !strings.Contains(resp.headers["cache-control"], "immutable") {
			t.Fatalf("expected immutable Cache-Control, got %q", resp.headers["cache-control"])
		}
	})

	t.Run("MaxAgeSetsPublicMaxAge", func(t *testing.T) {
		app := fh.New()
		app.Get("/assets/*", New(root, Config{Root: root, MaxAge: 60 * time.Second}))
		addr := testServer(t, app)
		resp := rawGet(t, addr, "/assets/a.txt")
		if resp.headers["cache-control"] != "public, max-age=60" {
			t.Fatalf("expected max-age=60, got %q", resp.headers["cache-control"])
		}
	})

	t.Run("NoCacheOptionsSetNoHeader", func(t *testing.T) {
		app := fh.New()
		app.Get("/assets/*", New(root))
		addr := testServer(t, app)
		resp := rawGet(t, addr, "/assets/a.txt")
		if _, ok := resp.headers["cache-control"]; ok {
			t.Fatalf("expected no Cache-Control header by default, got %q", resp.headers["cache-control"])
		}
	})
}

func TestETagConditionalRequestReturnsNotModified(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "a.txt"), []byte("a-content"))

	app := fh.New()
	app.Get("/assets/*", New(root, Config{Root: root, ETag: true}))
	addr := testServer(t, app)

	first := rawGet(t, addr, "/assets/a.txt")
	etag := first.headers["etag"]
	if etag == "" {
		t.Fatal("expected ETag header on first response")
	}

	second := rawGet(t, addr, "/assets/a.txt", "If-None-Match: "+etag)
	if second.status != fh.StatusNotModified {
		t.Fatalf("expected 304 for matching If-None-Match, got %d", second.status)
	}

	third := rawGet(t, addr, "/assets/a.txt", `If-None-Match: "stale-etag"`)
	if third.status != fh.StatusOK {
		t.Fatalf("expected 200 for non-matching If-None-Match, got %d", third.status)
	}
}

func TestLastModifiedHeaderIsSet(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "a.txt"), []byte("a"))

	app := fh.New()
	app.Get("/assets/*", New(root, Config{Root: root, LastModified: true}))
	addr := testServer(t, app)

	resp := rawGet(t, addr, "/assets/a.txt")
	if resp.headers["last-modified"] == "" {
		t.Fatal("expected Last-Modified header to be set")
	}
}

// TestDownloadContentDispositionSanitizesFilename guards against response
// header injection through a crafted filename: characters that could break
// out of the quoted filename value (quotes, CR, LF, backslash) must be
// neutralized.
func TestDownloadContentDispositionSanitizesFilename(t *testing.T) {
	root := t.TempDir()
	// Filesystems reject literal CR/LF/backslash in filenames on most OSes,
	// but a quote is a legal filename character we can actually create.
	dangerous := `report".txt`
	mustWriteFile(t, filepath.Join(root, dangerous), []byte("data"))

	app := fh.New()
	app.Get("/assets/*", New(root, Config{Root: root, Download: true}))
	addr := testServer(t, app)

	resp := rawGet(t, addr, "/assets/"+dangerous)
	cd := resp.headers["content-disposition"]
	if !strings.HasPrefix(cd, "attachment;") {
		t.Fatalf("expected attachment disposition, got %q", cd)
	}
	if strings.Contains(cd, `".txt"`) || strings.Count(cd, `"`) != 2 {
		t.Fatalf("Content-Disposition filename was not sanitized, header injection risk: %q", cd)
	}
}

func TestSanitizeFilenameNeutralizesInjectionCharacters(t *testing.T) {
	got := sanitizeFilename("evil\"\r\n\\/name.txt")
	if strings.ContainsAny(got, "\"\r\n\\/") {
		t.Fatalf("sanitizeFilename left dangerous characters in place: %q", got)
	}
}

// ── Malformed / edge-case input ─────────────────────────────────────────────

func TestMissingFileReturnsNotFound(t *testing.T) {
	root := t.TempDir()
	app := fh.New()
	app.Get("/assets/*", New(root))
	addr := testServer(t, app)

	resp := rawGet(t, addr, "/assets/does-not-exist.txt")
	if resp.status != fh.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.status)
	}
}

func TestEmptyRootRequestServesIndexOrDirectory(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "index.html"), []byte("home"))
	app := fh.New()
	app.Use(New(root, Config{Root: root, Prefix: "/"}))
	addr := testServer(t, app)

	resp := rawGet(t, addr, "/")
	if resp.status != fh.StatusOK || resp.body != "home" {
		t.Fatalf("expected mounted-via-Use root request to serve index.html, got status=%d body=%q", resp.status, resp.body)
	}
}

func TestPanicsOnInvalidRoot(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected New to panic when the root does not exist")
		}
	}()
	New(filepath.Join(t.TempDir(), "does", "not", "exist"))
}

// ── Concurrency ──────────────────────────────────────────────────────────────

// TestConcurrentRequestsAreRaceFree exercises the handler from many
// goroutines at once (run with -race) to catch any shared mutable state
// introduced by a future change; static.New's handler is expected to be
// pure/read-only after setup.
func TestConcurrentRequestsAreRaceFree(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "a.txt"), []byte("a-content"))
	mustWriteFile(t, filepath.Join(root, "b.txt"), []byte("b-content"))

	app := fh.New()
	app.Get("/assets/*", New(root, Config{Root: root, ETag: true, LastModified: true, MaxAge: time.Minute}))
	addr := testServer(t, app)

	var wg sync.WaitGroup
	errs := make(chan string, 64)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := "a.txt"
			want := "a-content"
			if i%2 == 0 {
				name = "b.txt"
				want = "b-content"
			}
			resp := rawGet(t, addr, "/assets/"+name)
			if resp.status != fh.StatusOK || resp.body != want {
				errs <- fmt.Sprintf("iteration %d: status=%d body=%q", i, resp.status, resp.body)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}

// ── cleanRootName unit tests ─────────────────────────────────────────────────

func TestCleanRootNameNeutralizesTraversal(t *testing.T) {
	cases := map[string]string{
		"":                              ".",
		"a.txt":                         "a.txt",
		"../../etc/passwd":              "etc/passwd",
		"/../../etc/passwd":             "etc/passwd",
		"dir/../../etc/passwd":          "etc/passwd",
		"./a/./b":                       "a/b",
		"a/../../../../b":               "b",
		strings.Repeat("../", 50) + "x": "x",
	}
	for in, want := range cases {
		if got := cleanRootName(in); got != want {
			t.Errorf("cleanRootName(%q) = %q, want %q", in, got, want)
		}
		if strings.Contains(cleanRootName(in), "..") {
			t.Errorf("cleanRootName(%q) = %q still contains '..'", in, cleanRootName(in))
		}
	}
}

func TestCleanRootNameResultNeverEscapesForIndexJoin(t *testing.T) {
	// path.Join(name, "index.html") must stay confined too.
	name := cleanRootName("../../../secret")
	joined := path.Join(name, "index.html")
	if strings.HasPrefix(joined, "..") || strings.Contains(joined, "../") {
		t.Fatalf("joined index path escapes root: %q", joined)
	}
}
