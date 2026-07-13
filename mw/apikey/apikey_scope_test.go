package apikey

import (
	"fmt"
	"io"
	"net"
	"strings"
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

func doRequest(t *testing.T, addr, key string) int {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	req := fmt.Sprintf("GET /x HTTP/1.1\r\nHost: localhost\r\nX-API-Key: %s\r\n\r\n", key)
	conn.Write([]byte(req))
	conn.(*net.TCPConn).CloseWrite()
	resp, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	parts := strings.SplitN(string(resp), "\r\n", 2)
	var proto, status string
	fmt.Sscan(parts[0], &proto, &status)
	var code int
	fmt.Sscan(status, &code)
	return code
}

func newApp(cfg Config) *fh.App {
	app := fh.New()
	app.Use(New(cfg))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })
	return app
}

// TestStaticKeyCannotBypassRequiredScopes proves a key accepted via the
// static Keys allowlist (which carries no scope metadata) is rejected when
// the app requires a scope, instead of silently bypassing the requirement
// as it did before scope enforcement was centralized.
func TestStaticKeyCannotBypassRequiredScopes(t *testing.T) {
	app := newApp(Config{Keys: []string{"legacy-ops-key"}, RequiredScopes: []string{"admin"}})
	addr := testServer(t, app)
	if code := doRequest(t, addr, "legacy-ops-key"); code == 200 {
		t.Fatalf("expected static key with no scope metadata to be rejected when RequiredScopes is set, got %d", code)
	}
}

// TestStaticKeyWorksWithoutRequiredScopes proves the fix doesn't break the
// common case where no scopes are required at all.
func TestStaticKeyWorksWithoutRequiredScopes(t *testing.T) {
	app := newApp(Config{Keys: []string{"legacy-ops-key"}})
	addr := testServer(t, app)
	if code := doRequest(t, addr, "legacy-ops-key"); code != 200 {
		t.Fatalf("expected 200 when no scopes are required, got %d", code)
	}
}

// TestStoreKeyWithSufficientScopeStillWorks proves the Store path (which
// does carry real scope metadata) still succeeds when it actually satisfies
// RequiredScopes, i.e. the centralized check didn't regress the correct
// case.
func TestStoreKeyWithSufficientScopeStillWorks(t *testing.T) {
	key, hash, err := Generate("fh_test", 16)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := SplitKey(key)
	store := NewMemoryStore(KeyRecord{ID: id, Hash: hash, Scopes: []string{"admin", "read"}})
	app := newApp(Config{Store: store, RequiredScopes: []string{"admin"}})
	addr := testServer(t, app)
	if code := doRequest(t, addr, key); code != 200 {
		t.Fatalf("expected 200 for a store-backed key with sufficient scope, got %d", code)
	}
}

// TestStoreKeyWithInsufficientScopeStillRejected proves the Store path
// still rejects a key lacking the required scope (pre-existing behavior,
// unaffected by centralizing the check).
func TestStoreKeyWithInsufficientScopeStillRejected(t *testing.T) {
	key, hash, err := Generate("fh_test", 16)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := SplitKey(key)
	store := NewMemoryStore(KeyRecord{ID: id, Hash: hash, Scopes: []string{"read"}})
	app := newApp(Config{Store: store, RequiredScopes: []string{"admin"}})
	addr := testServer(t, app)
	if code := doRequest(t, addr, key); code == 200 {
		t.Fatalf("expected key lacking required scope to be rejected, got %d", code)
	}
}
