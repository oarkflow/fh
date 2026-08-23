package fh

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type sampleXML struct {
	XMLName xml.Name `xml:"user"`
	ID      int      `xml:"id"`
	Name    string   `xml:"name"`
}

func testRequest(t *testing.T, app *App, req *http.Request) *http.Response {
	t.Helper()
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test failed: %v", err)
	}
	return resp
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	return string(b)
}

func TestModernCtxMethods(t *testing.T) {
	t.Run("Vary", func(t *testing.T) {
		app := New()
		app.Get("/test", func(c Ctx) error {
			c.Vary("Accept-Encoding", "User-Agent")
			c.Vary("Accept-Encoding") // duplicate should be ignored
			return c.SendString("ok")
		})
		req := httptest.NewRequest("GET", "/test", nil)
		resp := testRequest(t, app, req)
		if v := resp.Header.Get("Vary"); v != "Accept-Encoding, User-Agent" {
			t.Errorf("expected Vary 'Accept-Encoding, User-Agent', got %q", v)
		}
	})

	t.Run("IsXHR", func(t *testing.T) {
		app := New()
		app.Get("/test", func(c Ctx) error {
			if c.IsXHR() {
				return c.SendString("xhr")
			}
			return c.SendString("standard")
		})
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("X-Requested-With", "XMLHttpRequest")
		resp := testRequest(t, app, req)
		if body := readBody(t, resp); body != "xhr" {
			t.Errorf("expected xhr, got %q", body)
		}
	})

	t.Run("ProtocolAndBaseURL", func(t *testing.T) {
		app := New()
		app.Get("/test", func(c Ctx) error {
			return c.SendString(c.Protocol() + "|" + c.BaseURL())
		})
		req := httptest.NewRequest("GET", "http://api.example.com/test", nil)
		req.Host = "api.example.com"
		resp := testRequest(t, app, req)
		if body := readBody(t, resp); body != "http|http://api.example.com" {
			t.Errorf("expected http|http://api.example.com, got %q", body)
		}
	})

	t.Run("Subdomains", func(t *testing.T) {
		app := New()
		app.Get("/test", func(c Ctx) error {
			subs := c.Subdomains()
			res := ""
			for i, s := range subs {
				if i > 0 {
					res += ","
				}
				res += s
			}
			return c.SendString(res)
		})
		req := httptest.NewRequest("GET", "http://admin.v2.example.com/test", nil)
		req.Host = "admin.v2.example.com"
		resp := testRequest(t, app, req)
		if body := readBody(t, resp); body != "admin,v2" {
			t.Errorf("expected admin,v2, got %q", body)
		}
	})

	t.Run("Accepts", func(t *testing.T) {
		app := New()
		app.Get("/test", func(c Ctx) error {
			best := c.Accepts("application/json", "text/html", "application/xml")
			return c.SendString(best)
		})
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Accept", "text/html;q=0.9, application/json;q=0.8")
		resp := testRequest(t, app, req)
		if body := readBody(t, resp); body != "text/html" {
			t.Errorf("expected text/html, got %q", body)
		}
	})

	t.Run("AcceptsEncodings", func(t *testing.T) {
		app := New()
		app.Get("/test", func(c Ctx) error {
			best := c.AcceptsEncodings("br", "gzip", "deflate")
			return c.SendString(best)
		})
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Accept-Encoding", "gzip;q=0.5, deflate;q=0.8")
		resp := testRequest(t, app, req)
		if body := readBody(t, resp); body != "deflate" {
			t.Errorf("expected deflate, got %q", body)
		}
	})

	t.Run("XML", func(t *testing.T) {
		app := New()
		app.Get("/test", func(c Ctx) error {
			return c.XML(sampleXML{ID: 123, Name: "Alice"})
		})
		req := httptest.NewRequest("GET", "/test", nil)
		resp := testRequest(t, app, req)
		if ct := resp.Header.Get("Content-Type"); ct != MIMEApplicationXMLCharsetUTF8 {
			t.Errorf("expected xml content-type, got %q", ct)
		}
		if body := readBody(t, resp); body != "<user><id>123</id><name>Alice</name></user>" {
			t.Errorf("unexpected xml body: %q", body)
		}
	})

	t.Run("Format", func(t *testing.T) {
		app := New()
		app.Get("/test", func(c Ctx) error {
			return c.Format(map[string]HandlerFunc{
				"application/json": func(c Ctx) error {
					return c.JSON(Map{"format": "json"})
				},
				"text/html": func(c Ctx) error {
					return c.HTML("<h1>html</h1>")
				},
			})
		})
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Accept", "application/json")
		resp := testRequest(t, app, req)
		if body := readBody(t, resp); body != `{"format":"json"}` {
			t.Errorf("expected format json, got %q", body)
		}
	})

	t.Run("Range", func(t *testing.T) {
		app := New()
		app.Get("/test", func(c Ctx) error {
			ranges, err := c.Range(1000)
			if err != nil {
				return err
			}
			return c.JSON(ranges)
		})
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Range", "bytes=0-499, 500-999")
		resp := testRequest(t, app, req)
		if body := readBody(t, resp); body != `[{"Start":0,"End":499},{"Start":500,"End":999}]` {
			t.Errorf("expected range segments, got %q", body)
		}
	})

	t.Run("QueryMultiple", func(t *testing.T) {
		app := New()
		app.Get("/test", func(c Ctx) error {
			tags := c.QueryMultiple("tag")
			return c.JSON(tags)
		})
		req := httptest.NewRequest("GET", "/test?tag=go&tag=web&tag=http", nil)
		resp := testRequest(t, app, req)
		if body := readBody(t, resp); body != `["go","web","http"]` {
			t.Errorf("expected multiple tags, got %q", body)
		}
	})

	t.Run("IPs", func(t *testing.T) {
		app := New()
		app.Get("/test", func(c Ctx) error {
			return c.JSON(c.IPs())
		})
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("X-Forwarded-For", "203.0.113.195, 70.41.3.18, 150.172.238.178")
		resp := testRequest(t, app, req)
		if body := readBody(t, resp); body != `["203.0.113.195","70.41.3.18","150.172.238.178"]` {
			t.Errorf("expected IPs slice, got %q", body)
		}
	})

	t.Run("SearchPurgeReport", func(t *testing.T) {
		for _, m := range []struct {
			method string
			path   string
			expect string
		}{
			{"SEARCH", "/search", "search-ok"},
			{"PURGE", "/purge", "purge-ok"},
			{"REPORT", "/report", "report-ok"},
			{"SEARCH", "/v1/search", "grp-search-ok"},
			{"PURGE", "/v1/purge", "grp-purge-ok"},
			{"REPORT", "/v1/report", "grp-report-ok"},
		} {
			app := New()
			app.Search("/search", func(c Ctx) error { return c.SendString("search-ok") })
			app.Purge("/purge", func(c Ctx) error { return c.SendString("purge-ok") })
			app.Report("/report", func(c Ctx) error { return c.SendString("report-ok") })

			grp := app.Group("/v1")
			grp.Search("/search", func(c Ctx) error { return c.SendString("grp-search-ok") })
			grp.Purge("/purge", func(c Ctx) error { return c.SendString("grp-purge-ok") })
			grp.Report("/report", func(c Ctx) error { return c.SendString("grp-report-ok") })

			req := httptest.NewRequest(m.method, m.path, nil)
			resp := testRequest(t, app, req)
			if body := readBody(t, resp); body != m.expect {
				t.Errorf("expected %q for %s %s, got %q", m.expect, m.method, m.path, body)
			}
		}
	})

	t.Run("ClearSiteData", func(t *testing.T) {
		app := New()
		app.Get("/logout", func(c Ctx) error {
			c.ClearSiteData("cache", "cookies", "storage")
			return c.SendString("logged out")
		})
		req := httptest.NewRequest("GET", "/logout", nil)
		resp := testRequest(t, app, req)
		if v := resp.Header.Get("Clear-Site-Data"); v != `"cache", "cookies", "storage"` {
			t.Errorf("expected Clear-Site-Data '\"cache\", \"cookies\", \"storage\"', got %q", v)
		}
	})

	t.Run("ClientHints", func(t *testing.T) {
		app := New()
		app.Get("/hints", func(c Ctx) error {
			c.AcceptCH("Sec-CH-UA-Model", "Width")
			c.CriticalCH("Sec-CH-UA-Platform-Version")
			return c.SendString("hints set")
		})
		req := httptest.NewRequest("GET", "/hints", nil)
		resp := testRequest(t, app, req)
		if v := resp.Header.Get("Critical-CH"); v != "Sec-CH-UA-Platform-Version" {
			t.Errorf("expected Critical-CH, got %q", v)
		}
		if v := resp.Header.Get("Accept-CH"); !strings.Contains(v, "Sec-CH-UA-Model") || !strings.Contains(v, "Sec-CH-UA-Platform-Version") {
			t.Errorf("expected Accept-CH to contain hints, got %q", v)
		}
	})

	t.Run("StaleWhileRevalidate", func(t *testing.T) {
		app := New()
		app.Get("/cached", func(c Ctx) error {
			c.Set("Cache-Control", "max-age=600")
			c.StaleWhileRevalidate(30 * time.Second)
			return c.SendString("cached")
		})
		req := httptest.NewRequest("GET", "/cached", nil)
		resp := testRequest(t, app, req)
		if v := resp.Header.Get("Cache-Control"); !strings.Contains(v, "stale-while-revalidate=30") {
			t.Errorf("expected Cache-Control to include stale-while-revalidate=30, got %q", v)
		}
	})
}
