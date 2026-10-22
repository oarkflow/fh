package fh

import (
	"net"
	"strings"
	"testing"
)

type countingWriteConn struct {
	net.Conn
	writes int
}

func (c *countingWriteConn) Write(p []byte) (int, error) {
	c.writes++
	return c.Conn.Write(p)
}

func TestSmallJSONUsesContiguousWriteWithLazyBuffer(t *testing.T) {
	cases := []struct {
		name  string
		body  string
		value any
	}{
		{"Map", `{"message":"Hello, World!"}`, Map{"message": "Hello, World!"}},
		{"map[string]string", `{"message":"Hello, World!"}`, map[string]string{"message": "Hello, World!"}},
		{"struct", `{"id":42,"name":"User 42"}`, jsonHotpathUser{ID: 42, Name: "User 42"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app := NewFast(WithDisableHTTP2(true), WithSendDateHeader(false))
			output := &benchConn{buf: make([]byte, 256)}
			conn := &countingWriteConn{Conn: output}
			ctx := acquireHTTP1Ctx(conn, app, &connState{})
			defer releaseCtx(ctx)
			ctx.Header.KeepAlive = true
			if err := ctx.JSON(tc.value); err != nil {
				t.Fatal(err)
			}
			if got := string(output.buf[:output.pos]); !strings.Contains(got, tc.body) {
				t.Fatalf("JSON response = %q", got)
			}
			if conn.writes != 1 {
				t.Fatalf("small JSON response used %d writes", conn.writes)
			}
		})
	}
}
