package ipthrottle

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/oarkflow/fh"
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
