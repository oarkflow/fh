package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOpenBrowserCommand(t *testing.T) {
	cases := map[string]string{"darwin": "open", "windows": "rundll32", "linux": "xdg-open", "freebsd": "xdg-open"}
	for goos, wantName := range cases {
		name, args := openBrowserCommand(goos, "http://127.0.0.1:8080")
		if name != wantName {
			t.Fatalf("%s: got command %q, want %q", goos, name, wantName)
		}
		if len(args) == 0 || args[len(args)-1] != "http://127.0.0.1:8080" {
			t.Fatalf("%s: args %v do not end with the URL", goos, args)
		}
	}
}

func TestReadDevCredentials(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".env"), []byte("# comment\nAPP_LOGIN_USER=alice\nAPP_LOGIN_PASSWORD='s3cret'\nOTHER=1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	username, password := readDevCredentials(root)
	if username != "alice" || password != "s3cret" {
		t.Fatalf("got %q/%q, want alice/s3cret", username, password)
	}
}

func TestReadDevCredentialsFallsBackWithoutEnv(t *testing.T) {
	username, password := readDevCredentials(t.TempDir())
	if username != "admin" || password != "change-me-in-development" {
		t.Fatalf("got %q/%q, want the documented development defaults", username, password)
	}
}

func TestFreeLoopbackAddr(t *testing.T) {
	addr, err := freeLoopbackAddr()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix([]byte(addr), []byte("127.0.0.1:")) {
		t.Fatalf("got %q, want a 127.0.0.1 address", addr)
	}
}

// fakeApp is a minimal stand-in for the generated server, replicating just
// enough of its HTTP contract for runChecklist to exercise: the SPL page,
// static/WASM assets, the login/session/secure-bootstrap/logout lifecycle,
// and the 426 an unauthenticated/plain request to /api/* gets back.
func fakeApp(t *testing.T) *httptest.Server {
	t.Helper()
	const sessionCookie = "fh_session"
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<!doctype html><html><head><link rel="stylesheet" href="/assets/app.css"></head><body><div id="app"></div><script src="/assets/app.js" type="module"></script></body></html>`))
	})
	asset := func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("content")) }
	for _, path := range []string{"/assets/app.js", "/assets/app.css", "/wasm/securefetch.wasm", "/wasm/wasm_exec.js", "/wasm/index.js", "/wasm/secure-fetch.js"} {
		mux.HandleFunc(path, asset)
	}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	authenticated := func(r *http.Request) bool {
		cookie, err := r.Cookie(sessionCookie)
		return err == nil && cookie.Value == "valid"
	}
	mux.HandleFunc("/auth/session", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"authenticated": authenticated(r)})
	})
	mux.HandleFunc("/auth/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Origin") == "" {
			http.Error(w, `{"detail":"origin required"}`, http.StatusForbidden)
			return
		}
		http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "valid", Path: "/"})
		json.NewEncoder(w).Encode(map[string]any{"authenticated": true})
	})
	mux.HandleFunc("/auth/logout", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/", MaxAge: -1})
		json.NewEncoder(w).Encode(map[string]any{})
	})
	mux.HandleFunc("/secure-config.json", func(w http.ResponseWriter, r *http.Request) {
		if !authenticated(r) {
			http.Error(w, `{"detail":"login required"}`, http.StatusUnauthorized)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"wasmURL": "/wasm/securefetch.wasm", "registrationToken": "grant-token", "pinnedServerKey": "pinned-key",
		})
	})
	mux.HandleFunc("/api/me", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"code":"FH_SECURE_REQUIRED"}`, http.StatusUpgradeRequired)
	})
	return httptest.NewServer(mux)
}

func TestRunChecklistAgainstFakeApp(t *testing.T) {
	server := fakeApp(t)
	defer server.Close()

	var out bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runChecklist(ctx, server.URL, "admin", "change-me-in-development", &out); err != nil {
		t.Fatalf("runChecklist failed: %v\noutput:\n%s", err, out.String())
	}
}

func TestRunChecklistFailsClosedWithoutSecureEnforcement(t *testing.T) {
	server := fakeApp(t)
	defer server.Close()
	// A generated app that forgot to enforce the secure transport would
	// answer /api/me directly instead of 426 - the checklist must catch
	// that instead of passing silently.
	mux := http.NewServeMux()
	// Wrap: reuse fakeApp's handlers by proxying isn't trivial with net/http,
	// so build a second server whose only difference is /api/me returning 200.
	_ = mux
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/me" {
			w.Write([]byte(`{"userID":"admin"}`))
			return
		}
		server.Config.Handler.ServeHTTP(w, r)
	}))
	defer broken.Close()

	var out bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runChecklist(ctx, broken.URL, "admin", "change-me-in-development", &out); err == nil {
		t.Fatal("expected the checklist to fail when /api/me does not require the secure transport")
	}
}
