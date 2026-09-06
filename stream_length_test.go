package fh_test

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/oarkflow/fh"
)

func TestSendStreamLengthBoundsResponse(t *testing.T) {
	tests := []struct {
		name string
		body string
		size int64
		want string
		err  bool
	}{
		{name: "short source", body: "hello", size: 8, want: "hello", err: true},
		{name: "long source", body: "hello", size: 3, want: "hel"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app := fh.New()
			app.Get("/", func(c fh.Ctx) error {
				return c.SendStreamLength(strings.NewReader(tc.body), tc.size)
			})
			resp, err := app.Test(httptest.NewRequest("GET", "/", nil))
			if err != nil {
				if !tc.err {
					t.Fatal(err)
				}
				return
			}
			if tc.err {
				t.Fatal("expected a truncated response error")
			}
			defer resp.Body.Close()
			got, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Fatalf("body = %q, want %q", got, tc.want)
			}
		})
	}
}

type noReadReader struct{}

func (noReadReader) Read([]byte) (int, error) {
	panic("stream source was read for a bodyless response")
}

func TestSendStreamLengthSkipsBodylessResponses(t *testing.T) {
	app := fh.New()
	app.Get("/", func(c fh.Ctx) error {
		return c.SendStreamLength(noReadReader{}, 1234)
	})
	resp, err := app.Test(httptest.NewRequest("HEAD", "/", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Length"); got != "1234" {
		t.Fatalf("Content-Length = %q, want 1234", got)
	}
}
