package session

import (
	"testing"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

func newTestSession() *Session {
	store := kv.NewMemoryStore(kv.WithShardCount(1), kv.WithMaxEntries(100))
	mgr := NewSessionManager(store, SessionSecrets([]byte("test-secret-must-be-32-bytes-ok!!")))
	return mgr.NewSession()
}

func TestFlashSetAndGet(t *testing.T) {
	s := newTestSession()
	s.Set("user", "alice")

	// Set a flash value
	s.Flash("success", "Item created")

	// Get it back
	v := s.Flash("success")
	if v != "Item created" {
		t.Fatalf("expected 'Item created', got %v", v)
	}

	// Second read should return nil (consumed)
	v = s.Flash("success")
	if v != nil {
		t.Fatalf("expected nil on second read, got %v", v)
	}
}

func TestFlashMultipleKeys(t *testing.T) {
	s := newTestSession()

	s.Flash("success", "ok")
	s.Flash("error", "fail")
	s.Flash("info", "note")

	if s.Flash("success") != "ok" {
		t.Fatal("success flash missing")
	}
	if s.Flash("error") != "fail" {
		t.Fatal("error flash missing")
	}
	if s.Flash("info") != "note" {
		t.Fatal("info flash missing")
	}

	// All consumed
	if s.Flash("success") != nil {
		t.Fatal("success flash not consumed")
	}
}

func TestFlashOverwrite(t *testing.T) {
	s := newTestSession()

	s.Flash("msg", "first")
	s.Flash("msg", "second")

	v := s.Flash("msg")
	if v != "second" {
		t.Fatalf("expected 'second', got %v", v)
	}
}

func TestFlashAll(t *testing.T) {
	s := newTestSession()

	s.Flash("success", "ok")
	s.Flash("error", "fail")

	all := s.FlashAll()
	if len(all) != 2 {
		t.Fatalf("expected 2 flash entries, got %d", len(all))
	}
	if all["success"] != "ok" {
		t.Fatalf("expected success=ok, got %v", all["success"])
	}
	if all["error"] != "fail" {
		t.Fatalf("expected error=fail, got %v", all["error"])
	}

	// Second call should return nil (consumed)
	all = s.FlashAll()
	if all != nil {
		t.Fatalf("expected nil on second FlashAll, got %v", all)
	}
}

func TestFlashAllEmpty(t *testing.T) {
	s := newTestSession()

	all := s.FlashAll()
	if all != nil {
		t.Fatalf("expected nil, got %v", all)
	}
}

func TestFlashAllAtomicity(t *testing.T) {
	s := newTestSession()

	s.Flash("a", 1)
	s.Flash("b", 2)

	// FlashAll consumes everything
	all := s.FlashAll()
	if len(all) != 2 {
		t.Fatalf("expected 2, got %d", len(all))
	}

	// Individual reads should also return nil
	if s.Flash("a") != nil {
		t.Fatal("flash 'a' not consumed by FlashAll")
	}
	if s.Flash("b") != nil {
		t.Fatal("flash 'b' not consumed by FlashAll")
	}
}

func TestFlashAllSnapshotIsolation(t *testing.T) {
	s := newTestSession()
	s.Flash("key", "value")

	all := s.FlashAll()
	// Modifying the returned map should not affect the session
	all["key"] = "modified"
	all["new"] = "added"

	// Session should have no flash data left
	if s.Flash("key") != nil {
		t.Fatal("flash data leaked")
	}
}

func TestFlashGetWithoutSet(t *testing.T) {
	s := newTestSession()

	v := s.Flash("nonexistent")
	if v != nil {
		t.Fatalf("expected nil, got %v", v)
	}
}

func TestFlashZeroValue(t *testing.T) {
	s := newTestSession()

	s.Flash("count", 0)
	s.Flash("name", "")
	s.Flash("ok", false)

	if s.Flash("count") != 0 {
		t.Fatal("zero int not stored")
	}
	if s.Flash("name") != "" {
		t.Fatal("empty string not stored")
	}
	if s.Flash("ok") != false {
		t.Fatal("false not stored")
	}
}

func TestFlashViaCtx(t *testing.T) {
	app := fh.New()
	store := kv.NewMemoryStore(kv.WithShardCount(1), kv.WithMaxEntries(100))
	mgr := NewSessionManager(store, SessionSecrets([]byte("test-secret-must-be-32-bytes-ok!!")))

	app.Use(New(mgr))

	app.Post("/set", func(c fh.Ctx) error {
		c.Flash("success", "Item saved")
		c.Flash("count", 42)
		return c.SendStatus(200)
	})

	app.Get("/get", func(c fh.Ctx) error {
		v := c.Flash("success")
		if v != "Item saved" {
			return c.SendStatus(500)
		}
		// Second read should be nil
		if c.Flash("success") != nil {
			return c.SendStatus(501)
		}
		return c.SendStatus(200)
	})

	app.Get("/all", func(c fh.Ctx) error {
		all := c.FlashAll()
		if all == nil {
			return c.SendStatus(204)
		}
		return c.JSON(all)
	})
}

func TestFlashRedirectWithFlash(t *testing.T) {
	app := fh.New()
	store := kv.NewMemoryStore(kv.WithShardCount(1), kv.WithMaxEntries(100))
	mgr := NewSessionManager(store, SessionSecrets([]byte("test-secret-must-be-32-bytes-ok!!")))

	app.Use(New(mgr))

	app.Post("/action", func(c fh.Ctx) error {
		return c.RedirectWithFlash("/result", 302, map[string]any{
			"success": "Done!",
			"type":    "info",
		})
	})

	app.Get("/result", func(c fh.Ctx) error {
		msg := c.Flash("success")
		typ := c.Flash("type")
		if msg != "Done!" || typ != "info" {
			return c.SendStatus(500)
		}
		// Verify consumed
		if c.Flash("success") != nil {
			return c.SendStatus(501)
		}
		return c.SendStatus(200)
	})
}

func TestFlashCleanupOnSessionClear(t *testing.T) {
	s := newTestSession()
	s.Flash("key", "value")
	s.Clear()

	all := s.FlashAll()
	if all != nil {
		t.Fatalf("expected nil after clear, got %v", all)
	}
}

func TestFlashConcurrentAccess(t *testing.T) {
	s := newTestSession()
	done := make(chan bool, 20)

	for i := 0; i < 10; i++ {
		go func(n int) {
			s.Flash("key", n)
			done <- true
		}(i)
	}
	for i := 0; i < 10; i++ {
		go func() {
			s.Flash("key")
			done <- true
		}()
	}

	for i := 0; i < 20; i++ {
		<-done
	}
	// Should not panic
}
