package fh

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

type testPayload struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

type customEngine struct{}

func (customEngine) Marshal(v any) ([]byte, error) { return []byte(`{"engine":"custom"}`), nil }
func (customEngine) Unmarshal(data []byte, v any) error {
	p := v.(*testPayload)
	p.Name = "custom"
	p.Age = 99
	return nil
}
func (customEngine) NewEncoder(w io.Writer) JSONEncoder { return customEncoder{w: w} }
func (customEngine) NewDecoder(r io.Reader) JSONDecoder { return customDecoder{} }
func (customEngine) Valid(data []byte) bool             { return len(bytes.TrimSpace(data)) > 0 }

type customEncoder struct{ w io.Writer }

func (e customEncoder) Encode(v any) error {
	_, err := e.w.Write([]byte(`{"encoded":true}` + "\n"))
	return err
}

type customDecoder struct{}

func (customDecoder) Decode(v any) error {
	p := v.(*testPayload)
	p.Name = "stream"
	p.Age = 7
	return nil
}

type appPayload struct{ V string }

func (a appPayload) AppendJSON(dst []byte) ([]byte, error) {
	return append(dst, `{"v":"`+a.V+`"}`...), nil
}

type unmarshalPayload struct{ Seen bool }

func (u *unmarshalPayload) UnmarshalJSON(b []byte) error {
	u.Seen = strings.Contains(string(b), "seen")
	return nil
}

func TestJSONEngineCanBeReplaced(t *testing.T) {
	defer ResetJSONEngine()
	MustSetJSONEngine(customEngine{})

	out, err := EncodeBody("application/json", testPayload{Name: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != `{"engine":"custom"}` {
		t.Fatalf("unexpected output: %s", out)
	}

	var p testPayload
	if err := DecodeBody([]byte(`{"name":"ignored"}`), "application/json", &p); err != nil {
		t.Fatal(err)
	}
	if p.Name != "custom" || p.Age != 99 {
		t.Fatalf("unexpected decoded payload: %#v", p)
	}
}

func TestJSONEngineStructuredSuffix(t *testing.T) {
	defer ResetJSONEngine()
	MustSetJSONEngine(customEngine{})
	var p testPayload
	if err := DecodeBody([]byte(`{"x":1}`), "application/vnd.api+json; charset=utf-8", &p); err != nil {
		t.Fatal(err)
	}
	if p.Name != "custom" {
		t.Fatalf("suffix did not use json engine: %#v", p)
	}
}

func TestJSONAppenderAndUnmarshalerFastPaths(t *testing.T) {
	out, err := EncodeBody("application/json", appPayload{V: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != `{"v":"ok"}` {
		t.Fatalf("unexpected appender output: %s", out)
	}

	var u unmarshalPayload
	if err := DecodeBody([]byte(`{"seen":true}`), "application/json", &u); err != nil {
		t.Fatal(err)
	}
	if !u.Seen {
		t.Fatalf("UnmarshalJSON fast path was not used")
	}
}

func TestJSONEncoderDecoderFactories(t *testing.T) {
	defer ResetJSONEngine()
	MustSetJSONEngine(customEngine{})
	var buf bytes.Buffer
	if err := NewJSONEncoder(&buf).Encode(testPayload{}); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != `{"encoded":true}`+"\n" {
		t.Fatalf("bad encoded output: %q", got)
	}
	var p testPayload
	if err := NewJSONDecoder(strings.NewReader(`{}`)).Decode(&p); err != nil {
		t.Fatal(err)
	}
	if p.Name != "stream" || p.Age != 7 {
		t.Fatalf("bad stream decode: %#v", p)
	}
}
