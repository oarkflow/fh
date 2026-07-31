package ipthrottle

import (
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

func startTestApp(t *testing.T, middleware fh.HandlerFunc) string {
	t.Helper()
	app := fh.New(fh.WithStartupBanner(fh.StartupBannerConfig{Disabled: true}))
	app.Use(middleware)
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Serve(ln) }()
	t.Cleanup(func() { _ = app.ShutdownWithTimeout(time.Second) })
	return "http://" + ln.Addr().String()
}

func TestLimitersArePerInstanceAndReturn429(t *testing.T) {
	first := startTestApp(t, New(Config{MaxPerIP: 1, Window: time.Minute}))
	resp, err := http.Get(first)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	resp, err = http.Get(first)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second status = %d, want 429", resp.StatusCode)
	}

	second := startTestApp(t, New(Config{MaxPerIP: 1, Window: time.Minute}))
	resp, err = http.Get(second)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("independent limiter inherited state: status=%d", resp.StatusCode)
	}
}

func TestIPCardinalityLimitFailsClosed(t *testing.T) {
	addr := startTestApp(t, New(Config{
		MaxPerIP: 10,
		MaxIPs:   1,
		Window:   time.Minute,
		KeyFunc:  func(c fh.Ctx) string { return c.Get("X-Test-IP") },
	}))
	for i, ip := range []string{"192.0.2.1", "192.0.2.2"} {
		req, _ := http.NewRequest(http.MethodGet, addr, nil)
		req.Header.Set("X-Test-IP", ip)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		want := http.StatusOK
		if i == 1 {
			want = http.StatusTooManyRequests
		}
		if resp.StatusCode != want {
			t.Fatalf("request %d status = %d, want %d", i, resp.StatusCode, want)
		}
	}
}

// TestIncrementOverFileStore proves Increment works identically on top of a
// kv.FileStore as it does on top of a kv.MemoryStore, since both are plain
// kv.Store implementations passed to the same function.
func TestIncrementUnderLimitAllowedFileStore(t *testing.T) {
	store, err := kv.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("kv.NewFileStore() error = %v", err)
	}
	defer store.Close()

	now := time.Now()
	for i := 1; i <= 3; i++ {
		count, err := Increment(store, "1.2.3.4", time.Minute, 0, now)
		if err != nil {
			t.Fatalf("Increment() error = %v", err)
		}
		if count != i {
			t.Fatalf("count = %d, want %d", count, i)
		}
	}
}

func TestIncrementOverLimitRejectedFileStore(t *testing.T) {
	store, err := kv.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("kv.NewFileStore() error = %v", err)
	}
	defer store.Close()

	now := time.Now()
	// MaxPerIP enforcement lives in the middleware, not Increment, but the
	// MaxIPs cardinality cap does live in Increment: prove it fails closed
	// exactly like it does over a kv.MemoryStore.
	if _, err := Increment(store, "1.2.3.4", time.Minute, 1, now); err != nil {
		t.Fatalf("first Increment() error = %v", err)
	}
	_, err = Increment(store, "5.6.7.8", time.Minute, 1, now)
	if !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("second key: err = %v, want ErrCapacityExceeded", err)
	}
}

func TestIncrementWindowResetAfterExpiryFileStore(t *testing.T) {
	store, err := kv.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("kv.NewFileStore() error = %v", err)
	}
	defer store.Close()

	now := time.Now()
	count, err := Increment(store, "1.2.3.4", time.Minute, 0, now)
	if err != nil {
		t.Fatalf("Increment() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}

	later := now.Add(2 * time.Minute)
	count, err = Increment(store, "1.2.3.4", time.Minute, 0, later)
	if err != nil {
		t.Fatalf("Increment() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("count after window reset = %d, want 1", count)
	}
}

// TestIncrementMatchesAcrossStoreKinds runs the same scenario against both a
// kv.MemoryStore and a kv.FileStore and asserts identical outcomes.
func TestIncrementMatchesAcrossStoreKinds(t *testing.T) {
	mem := kv.NewMemoryStore()
	file, err := kv.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("kv.NewFileStore() error = %v", err)
	}
	defer file.Close()

	now := time.Now()
	for i := 0; i < 5; i++ {
		memCount, memErr := Increment(mem, "k", time.Minute, 0, now)
		fileCount, fileErr := Increment(file, "k", time.Minute, 0, now)
		if memErr != nil || fileErr != nil {
			t.Fatalf("unexpected error: mem=%v file=%v", memErr, fileErr)
		}
		if memCount != fileCount {
			t.Fatalf("iteration %d: mem count=%d file count=%d", i, memCount, fileCount)
		}
	}
}
