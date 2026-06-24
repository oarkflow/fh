package fh

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"testing"
)

func TestDecodeFormSimple(t *testing.T) {
	var v map[string]any
	if err := DecodeForm([]byte("name=John&age=30"), &v); err != nil {
		t.Fatal(err)
	}
	if v["name"] != "John" {
		t.Fatalf("expected John, got %v", v["name"])
	}
	if v["age"] != "30" {
		t.Fatalf("expected 30, got %v", v["age"])
	}
}

func TestDecodeFormNested(t *testing.T) {
	var v map[string]any
	if err := DecodeForm([]byte("user[name]=John&user[age]=30"), &v); err != nil {
		t.Fatal(err)
	}
	user, ok := v["user"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested map, got %T", v["user"])
	}
	if user["name"] != "John" {
		t.Fatalf("expected John, got %v", user["name"])
	}
	if user["age"] != "30" {
		t.Fatalf("expected 30, got %v", user["age"])
	}
}

func TestDecodeFormArray(t *testing.T) {
	var v map[string]any
	if err := DecodeForm([]byte("items[]=a&items[]=b&items[]=c"), &v); err != nil {
		t.Fatal(err)
	}
	items, ok := v["items"].([]any)
	if !ok {
		t.Fatalf("expected array, got %T", v["items"])
	}
	if len(items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(items))
	}
	if items[0] != "a" || items[1] != "b" || items[2] != "c" {
		t.Fatalf("unexpected items: %v", items)
	}
}

func TestDecodeFormRepeatedKey(t *testing.T) {
	var v map[string]any
	if err := DecodeForm([]byte("tag=go&tag=http"), &v); err != nil {
		t.Fatal(err)
	}
	tags, ok := v["tag"].([]string)
	if !ok {
		t.Fatalf("expected []string, got %T", v["tag"])
	}
	if len(tags) != 2 || tags[0] != "go" || tags[1] != "http" {
		t.Fatalf("unexpected tags: %v", tags)
	}
}

func TestDecodeFormURLEncoding(t *testing.T) {
	var v map[string]any
	if err := DecodeForm([]byte("key=hello+world&path=%2Ftest"), &v); err != nil {
		t.Fatal(err)
	}
	if v["key"] != "hello world" {
		t.Fatalf("expected 'hello world', got %q", v["key"])
	}
	if v["path"] != "/test" {
		t.Fatalf("expected '/test', got %q", v["path"])
	}
}

func TestDecodeFormEmpty(t *testing.T) {
	v := make(map[string]any)
	if err := DecodeForm([]byte(""), &v); err != nil {
		t.Fatal(err)
	}
	if len(v) != 0 {
		t.Fatalf("expected empty map, got %v", v)
	}
}

func TestFormCodecContentType(t *testing.T) {
	var fc formCodec
	if fc.ContentType() != "application/x-www-form-urlencoded" {
		t.Fatalf("unexpected content type: %s", fc.ContentType())
	}
}

func TestJsonCodec(t *testing.T) {
	var jc jsonCodec
	var v map[string]any
	if err := jc.Unmarshal([]byte(`{"name":"John","age":30}`), &v); err != nil {
		t.Fatal(err)
	}
	if v["name"] != "John" {
		t.Fatalf("expected John, got %v", v["name"])
	}
	switch age := v["age"].(type) {
	case float64:
		if age != 30 {
			t.Fatalf("expected 30, got %v", age)
		}
	case json.Number:
		n, _ := age.Int64()
		if n != 30 {
			t.Fatalf("expected 30, got %v", n)
		}
	default:
		t.Fatalf("unexpected type for age: %T(%v)", v["age"], v["age"])
	}
}

func TestMatchCodec(t *testing.T) {
	tests := []struct {
		ct   string
		want string
	}{
		{"application/json", "application/json"},
		{"application/json; charset=utf-8", "application/json"},
		{"application/xml", "application/xml"},
		{"application/x-www-form-urlencoded", "application/x-www-form-urlencoded"},
		{"multipart/form-data; boundary=abc", "multipart/form-data"},
		{"text/plain", "text/plain"},
		{"text/plain; charset=utf-8", "text/plain"},
		{"", ""},
		{"application/octet-stream", "application/octet-stream"},
	}
	for _, tt := range tests {
		c := matchCodec(tt.ct)
		if tt.want == "" {
			if c != nil {
				t.Errorf("matchCodec(%q) = %v, want nil", tt.ct, c)
			}
			continue
		}
		if c == nil {
			t.Errorf("matchCodec(%q) = nil, want %s", tt.ct, tt.want)
			continue
		}
		if c.ContentType() != tt.want {
			t.Errorf("matchCodec(%q).ContentType() = %s, want %s", tt.ct, c.ContentType(), tt.want)
		}
	}
}

func TestDecodeFormAnyTarget(t *testing.T) {
	var v any
	if err := DecodeForm([]byte("key=val"), &v); err != nil {
		t.Fatal(err)
	}
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", v)
	}
	if m["key"] != "val" {
		t.Fatalf("expected val, got %v", m["key"])
	}
}

func TestXMLCodec(t *testing.T) {
	var xc xmlCodec
	type Person struct {
		Name string `xml:"name"`
		Age  int    `xml:"age"`
	}
	var p Person
	if err := xc.Unmarshal([]byte(`<Person><name>John</name><age>30</age></Person>`), &p); err != nil {
		t.Fatal(err)
	}
	if p.Name != "John" {
		t.Fatalf("expected John, got %v", p.Name)
	}
	if p.Age != 30 {
		t.Fatalf("expected 30, got %v", p.Age)
	}
	// xml.Unmarshal does not support map[string]any directly.
}

func TestXMLCodecEmpty(t *testing.T) {
	var xc xmlCodec
	var p struct{ Name string }
	if err := xc.Unmarshal(nil, &p); err != nil {
		t.Fatal(err)
	}
}

func TestXMLCodecInvalid(t *testing.T) {
	var xc xmlCodec
	var p struct{ Name string }
	err := xc.Unmarshal([]byte(`<broken>`), &p)
	if err == nil {
		t.Fatal("expected error for malformed XML")
	}
}

func TestMultipartCodecDirectError(t *testing.T) {
	var mc multipartCodec
	var v map[string]any
	err := mc.Unmarshal([]byte("content"), &v)
	if err == nil {
		t.Fatal("expected error for direct multipart unmarshal without boundary")
	}
}

func TestMultipartCodecWithContentType(t *testing.T) {
	var mc multipartCodec
	var v map[string]any
	body := "--BOUNDARY\r\nContent-Disposition: form-data; name=\"field1\"\r\n\r\nvalue1\r\n--BOUNDARY\r\nContent-Disposition: form-data; name=\"field2\"\r\n\r\nvalue2\r\n--BOUNDARY--\r\n"
	ct := "multipart/form-data; boundary=BOUNDARY"
	if err := mc.UnmarshalWithContentType([]byte(body), ct, &v); err != nil {
		t.Fatal(err)
	}
	if v["field1"] != "value1" {
		t.Fatalf("expected value1, got %v", v["field1"])
	}
	if v["field2"] != "value2" {
		t.Fatalf("expected value2, got %v", v["field2"])
	}
}

func TestMultipartCodecMissingBoundary(t *testing.T) {
	var mc multipartCodec
	var v map[string]any
	err := mc.UnmarshalWithContentType([]byte("content"), "multipart/form-data", &v)
	if err == nil {
		t.Fatal("expected error for missing boundary")
	}
}

func TestMultipartCodecWithAny(t *testing.T) {
	var mc multipartCodec
	var v any
	body := "--B\r\nContent-Disposition: form-data; name=\"x\"\r\n\r\n42\r\n--B--\r\n"
	ct := "multipart/form-data; boundary=B"
	if err := mc.UnmarshalWithContentType([]byte(body), ct, &v); err != nil {
		t.Fatal(err)
	}
	mf, ok := v.(*MultipartForm)
	if !ok {
		t.Fatalf("expected *MultipartForm, got %T", v)
	}
	if mf.First("x") != "42" {
		t.Fatalf("expected 42, got %v", mf.First("x"))
	}
}

func TestBodyParserMultipart(t *testing.T) {
	srv := New()
	srv.Post("/upload", func(c Ctx) error {
		var data map[string]any
		if err := c.BodyParser(&data); err != nil {
			return c.Status(400).SendString(err.Error())
		}
		return c.JSON(data)
	})
	client := runPipeApp(t, srv)
	body := "--x\r\nContent-Disposition: form-data; name=\"title\"\r\n\r\nHello\r\n--x\r\nContent-Disposition: form-data; name=\"count\"\r\n\r\n3\r\n--x--\r\n"
	req := "POST /upload HTTP/1.1\r\nHost: local\r\nContent-Type: multipart/form-data; boundary=x\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\nConnection: close\r\n\r\n" + body
	go func() { _, _ = io.WriteString(client, req) }()
	resp, err := io.ReadAll(client)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(resp, []byte("Hello")) || !bytes.Contains(resp, []byte("3")) {
		t.Fatalf("unexpected response: %q", resp)
	}
}

func TestBodyParserXML(t *testing.T) {
	srv := New()
	srv.Post("/xml", func(c Ctx) error {
		var data struct {
			Name string `xml:"name"`
			Age  int    `xml:"age"`
		}
		if err := c.BodyParser(&data); err != nil {
			return c.Status(400).SendString(err.Error())
		}
		return c.JSON(map[string]any{"name": data.Name, "age": data.Age})
	})
	client := runPipeApp(t, srv)
	body := `<root><name>John</name><age>30</age></root>`
	req := "POST /xml HTTP/1.1\r\nHost: local\r\nContent-Type: application/xml\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\nConnection: close\r\n\r\n" + body
	go func() { _, _ = io.WriteString(client, req) }()
	resp, err := io.ReadAll(client)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(resp, []byte("John")) || !bytes.Contains(resp, []byte("30")) {
		t.Fatalf("unexpected response: %q", resp)
	}
}

func TestTextCodecString(t *testing.T) {
	var tc textCodec
	var s string
	if err := tc.Unmarshal([]byte("hello"), &s); err != nil {
		t.Fatal(err)
	}
	if s != "hello" {
		t.Fatalf("expected hello, got %q", s)
	}
}

func TestTextCodecBytes(t *testing.T) {
	var tc textCodec
	var b []byte
	if err := tc.Unmarshal([]byte("data"), &b); err != nil {
		t.Fatal(err)
	}
	if string(b) != "data" {
		t.Fatalf("expected data, got %q", b)
	}
}

func TestTextCodecAny(t *testing.T) {
	var tc textCodec
	var v any
	if err := tc.Unmarshal([]byte("anything"), &v); err != nil {
		t.Fatal(err)
	}
	s, ok := v.(string)
	if !ok {
		t.Fatalf("expected string, got %T", v)
	}
	if s != "anything" {
		t.Fatalf("expected anything, got %q", s)
	}
}

func TestTextCodecUnsupported(t *testing.T) {
	var tc textCodec
	var n int
	err := tc.Unmarshal([]byte("42"), &n)
	if err == nil {
		t.Fatal("expected error for unsupported type")
	}
}

func TestDecodeFormRepeatedKeyToArray(t *testing.T) {
	var v map[string]any
	if err := DecodeForm([]byte("id=1&id=2&id=3"), &v); err != nil {
		t.Fatal(err)
	}
	ids, ok := v["id"].([]string)
	if !ok {
		t.Fatalf("expected []string, got %T", v["id"])
	}
	if len(ids) != 3 || ids[0] != "1" || ids[1] != "2" || ids[2] != "3" {
		t.Fatalf("unexpected ids: %v", ids)
	}
}

func TestDecodeFormAutoArray(t *testing.T) {
	var v map[string]any
	if err := DecodeForm([]byte("items[0]=a&items[1]=b&items[2]=c"), &v); err != nil {
		t.Fatal(err)
	}
	items, ok := v["items"].([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", v["items"])
	}
	if len(items) != 3 || items[0] != "a" || items[1] != "b" || items[2] != "c" {
		t.Fatalf("unexpected items: %v", items)
	}
}

// ── Struct tag based decoding ────────────────────────────────────────────

type structDecodeTarget struct {
	Name  string `form:"name"`
	Email string `form:"email"`
	Age   int    `form:"age"`
}

func TestDecodeFormToStruct(t *testing.T) {
	var s structDecodeTarget
	if err := DecodeForm([]byte("name=John&email=john@test.com&age=30"), &s); err != nil {
		t.Fatal(err)
	}
	if s.Name != "John" {
		t.Fatalf("expected John, got %q", s.Name)
	}
	if s.Email != "john@test.com" {
		t.Fatalf("expected john@test.com, got %q", s.Email)
	}
	if s.Age != 30 {
		t.Fatalf("expected 30, got %d", s.Age)
	}
}

func TestDecodeFormToStructDefaultName(t *testing.T) {
	type target struct {
		FirstName string `form:"first_name"`
		LastName  string `form:"last_name"`
	}
	var s target
	if err := DecodeForm([]byte("first_name=John&last_name=Doe"), &s); err != nil {
		t.Fatal(err)
	}
	if s.FirstName != "John" {
		t.Fatalf("expected John, got %q", s.FirstName)
	}
	if s.LastName != "Doe" {
		t.Fatalf("expected Doe, got %q", s.LastName)
	}
}

func TestDecodeFormToStructBool(t *testing.T) {
	type target struct {
		Active bool `form:"active"`
	}
	var s target
	if err := DecodeForm([]byte("active=true"), &s); err != nil {
		t.Fatal(err)
	}
	if !s.Active {
		t.Fatal("expected true")
	}
}

func TestDecodeFormToStructFloat(t *testing.T) {
	type target struct {
		Score float64 `form:"score"`
	}
	var s target
	if err := DecodeForm([]byte("score=3.14"), &s); err != nil {
		t.Fatal(err)
	}
	if s.Score != 3.14 {
		t.Fatalf("expected 3.14, got %f", s.Score)
	}
}

func TestDecodeFormToStructSlice(t *testing.T) {
	type target struct {
		Tags []string `form:"tags"`
	}
	var s target
	if err := DecodeForm([]byte("tags=go&tags=http&tags=fast"), &s); err != nil {
		t.Fatal(err)
	}
	if len(s.Tags) != 3 || s.Tags[0] != "go" || s.Tags[1] != "http" || s.Tags[2] != "fast" {
		t.Fatalf("unexpected tags: %v", s.Tags)
	}
}

func TestDecodeFormToStructPtr(t *testing.T) {
	type target struct {
		Name *string `form:"name"`
	}
	var s target
	if err := DecodeForm([]byte("name=John"), &s); err != nil {
		t.Fatal(err)
	}
	if s.Name == nil {
		t.Fatal("expected non-nil pointer")
	}
	if *s.Name != "John" {
		t.Fatalf("expected John, got %q", *s.Name)
	}
}

func TestDecodeFormToStructNested(t *testing.T) {
	type Address struct {
		City  string `form:"city"`
		State string `form:"state"`
	}
	type target struct {
		Name    string  `form:"name"`
		Address Address `form:"address"`
	}
	var s target
	if err := DecodeForm([]byte("name=John&address[city]=NYC&address[state]=NY"), &s); err != nil {
		t.Fatal(err)
	}
	if s.Name != "John" {
		t.Fatalf("expected John, got %q", s.Name)
	}
	if s.Address.City != "NYC" {
		t.Fatalf("expected NYC, got %q", s.Address.City)
	}
	if s.Address.State != "NY" {
		t.Fatalf("expected NY, got %q", s.Address.State)
	}
}

func TestDecodeFormToStructNotPointer(t *testing.T) {
	var s struct{ Name string }
	err := DecodeForm([]byte("name=John"), s)
	if err == nil {
		t.Fatal("expected error for non-pointer")
	}
}

func TestBodyParserToStruct(t *testing.T) {
	srv := New()
	srv.Post("/submit", func(c Ctx) error {
		var s structDecodeTarget
		if err := c.BodyParser(&s); err != nil {
			return c.Status(400).SendString("error: " + err.Error())
		}
		return c.JSON(s)
	})
	client := runPipeApp(t, srv)
	body := "name=Alice&email=alice@test.com&age=25"
	req := "POST /submit HTTP/1.1\r\nHost: local\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\nConnection: close\r\n\r\n" + body
	go func() { _, _ = io.WriteString(client, req) }()
	resp, err := io.ReadAll(client)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(resp, []byte("Alice")) || !bytes.Contains(resp, []byte("alice@test.com")) || !bytes.Contains(resp, []byte("25")) {
		t.Fatalf("unexpected response: %q", resp)
	}
}

func TestQueryParserToStruct(t *testing.T) {
	srv := New()
	srv.Get("/search", func(c Ctx) error {
		var s structDecodeTarget
		if err := c.QueryParser(&s); err != nil {
			return c.Status(400).SendString("error: " + err.Error())
		}
		return c.JSON(s)
	})
	client := runPipeApp(t, srv)
	req := "GET /search?name=Bob&email=bob@test.com&age=35 HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
	go func() { _, _ = io.WriteString(client, req) }()
	resp, err := io.ReadAll(client)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(resp, []byte("Bob")) || !bytes.Contains(resp, []byte("bob@test.com")) || !bytes.Contains(resp, []byte("35")) {
		t.Fatalf("unexpected response: %q", resp)
	}
}
