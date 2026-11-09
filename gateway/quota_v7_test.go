package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh/mira"
)

// This test models a slow or unreachable upstream. The gateway must reserve
// capacity before doing a network handshake, not after a MIRA session is live.
func TestConnectReservationPreventsHandshakeStorm(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var count atomic.Int32
	gw, err := New(Config{
		Authenticate: func(*http.Request) (string, error) { return "alice", nil },
		DialBackend: func(ctx context.Context, _ string) (*mira.Session, error) {
			if count.Add(1) == 1 {
				close(entered)
			}
			select {
			case <-release:
				return nil, errors.New("upstream offline")
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
		AllowPlaintextLoopback:  true,
		MaxSessions:             1,
		MaxSessionsPerPrincipal: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer gw.Close()
	h := gw.Handler()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := httptest.NewRequest(http.MethodPost, "http://localhost/v1/connect", strings.NewReader("{}"))
		r.RemoteAddr = "127.0.0.1:12111"
		h.ServeHTTP(httptest.NewRecorder(), r)
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("first dial never started")
	}
	for i := 0; i < 30; i++ {
		r := httptest.NewRequest(http.MethodPost, "http://localhost/v1/connect", strings.NewReader("{}"))
		r.RemoteAddr = "127.0.0.1:12111"
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("request %d got %d", i, w.Code)
		}
	}
	if count.Load() != 1 {
		t.Fatalf("started %d upstream handshakes despite a one-slot quota", count.Load())
	}
	close(release)
	wg.Wait()
	gw.mu.Lock()
	pending := gw.pending
	gw.mu.Unlock()
	if pending != 0 {
		t.Fatalf("leaked %d pending slots", pending)
	}
}
