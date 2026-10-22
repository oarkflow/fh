package fh

import (
	"encoding/json"
	"testing"
)

type jsonHotpathUser struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func benchmarkJSONHotpath(b *testing.B, encode func(*DefaultCtx) error) {
	b.Helper()
	conn := &benchConn{buf: make([]byte, 4096)}
	app := NewFast(WithDisableHTTP2(true), WithDisablePanicRecovery(true), WithSendDateHeader(false), WithStartupBannerDisabled(true))
	ctx := acquireCtx(conn, app)
	defer releaseCtx(ctx)
	ctx.Header.KeepAlive = true
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.reset()
		ctx.Header.KeepAlive = true
		conn.Reset()
		if err := encode(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONHotpathMap(b *testing.B) {
	benchmarkJSONHotpath(b, func(c *DefaultCtx) error {
		return c.JSON(Map{"message": "Hello, World!"})
	})
}

func BenchmarkJSONHotpathStruct(b *testing.B) {
	benchmarkJSONHotpath(b, func(c *DefaultCtx) error {
		return c.JSON(jsonHotpathUser{Name: "User 42"})
	})
}

func BenchmarkJSONHotpathString(b *testing.B) {
	benchmarkJSONHotpath(b, func(c *DefaultCtx) error {
		return c.JSONString(`{"message":"Hello, World!"}`)
	})
}

func BenchmarkJSONHotpathStdlibMap(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(Map{"message": "Hello, World!"})
		if err != nil {
			b.Fatal(err)
		}
	}
}
