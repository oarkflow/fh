package validate

import (
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

func testServer(t *testing.T, app *fh.App) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { app.Shutdown() })
	go app.Serve(ln)
	time.Sleep(10 * time.Millisecond)
	return ln.Addr().String()
}

func doRequest(t *testing.T, addr, method, path, body string, headers map[string]string) (statusCode int, respBody string) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	req := fmt.Sprintf("%s %s HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n", method, path)
	req += "Content-Type: application/json\r\n"
	for k, v := range headers {
		req += k + ": " + v + "\r\n"
	}
	if body != "" {
		req += fmt.Sprintf("Content-Length: %d\r\n", len(body))
	}
	req += "\r\n" + body

	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	resp, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	parts := strings.SplitN(string(resp), "\r\n", 2)
	var proto, status string
	fmt.Sscan(parts[0], &proto, &status)
	fmt.Sscan(status, &statusCode)
	idx := strings.Index(string(resp), "\r\n\r\n")
	if idx >= 0 {
		respBody = string(resp)[idx+4:]
	}
	return
}

type createUserRequest struct {
	Name  string `json:"name" validate:"required,min=2"`
	Email string `json:"email" validate:"required,email"`
}

// TestBodyRejectsMissingRequiredField proves invalid input is actually
// rejected (422), not silently allowed through to the handler.
func TestBodyRejectsMissingRequiredField(t *testing.T) {
	called := false
	app := fh.New()
	app.Post("/users", Body(func() any { return &createUserRequest{} }), func(c fh.Ctx) error {
		called = true
		return c.SendStatus(fh.StatusCreated)
	})
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "POST", "/users", `{"name":"","email":"a@b.com"}`, nil)
	if status != fh.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for a missing required field, got %d (body=%q)", status, body)
	}
	if called {
		t.Fatal("handler must not run when validation fails")
	}
	if !strings.Contains(body, "validation_failed") || !strings.Contains(body, "name") {
		t.Fatalf("expected validation error body to mention the failing field, got %q", body)
	}
}

// TestBodyRejectsInvalidEmail proves a semantically-invalid (but
// syntactically well-formed) field value is rejected too, not just empty
// ones.
func TestBodyRejectsInvalidEmail(t *testing.T) {
	app := fh.New()
	app.Post("/users", Body(func() any { return &createUserRequest{} }), func(c fh.Ctx) error {
		return c.SendStatus(fh.StatusCreated)
	})
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "POST", "/users", `{"name":"Alice","email":"not-an-email"}`, nil)
	if status != fh.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for an invalid email, got %d (body=%q)", status, body)
	}
}

// TestBodyAcceptsValidInput is the positive-path sanity check: a fully
// valid payload must reach the handler and get its normal response.
func TestBodyAcceptsValidInput(t *testing.T) {
	app := fh.New()
	app.Post("/users", Body(func() any { return &createUserRequest{} }), func(c fh.Ctx) error {
		return c.SendStatus(fh.StatusCreated)
	})
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "POST", "/users", `{"name":"Alice","email":"alice@example.com"}`, nil)
	if status != fh.StatusCreated {
		t.Fatalf("expected 201 for valid input, got %d", status)
	}
}

// TestBodyMalformedJSONReturnsGenericBadRequestWithoutLeakingInternals
// checks two invariants at once: malformed JSON is rejected (400, not
// silently defaulting/passing through), and the error message returned to
// the client is the generic "Invalid request body" string rather than the
// raw underlying JSON-decode error (which could leak internal details such
// as struct field names/types, byte offsets, or parser internals).
func TestBodyMalformedJSONReturnsGenericBadRequestWithoutLeakingInternals(t *testing.T) {
	called := false
	app := fh.New()
	app.Post("/users", Body(func() any { return &createUserRequest{} }), func(c fh.Ctx) error {
		called = true
		return c.SendStatus(fh.StatusCreated)
	})
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "POST", "/users", `{"name": invalid-json-here`, nil)
	if status != fh.StatusBadRequest {
		t.Fatalf("expected 400 for malformed JSON body, got %d (body=%q)", status, body)
	}
	if called {
		t.Fatal("handler must not run for a malformed body")
	}
	if !strings.Contains(body, "Invalid request body") {
		t.Fatalf("expected the generic 'Invalid request body' message, got %q", body)
	}
	// The generic message must not have been replaced by (or concatenated
	// with) the underlying parser error, which would leak internals.
	for _, leaky := range []string{"invalid character", "unexpected end of JSON", "createUserRequest", "json:"} {
		if strings.Contains(body, leaky) {
			t.Fatalf("error message leaked internal parser/type details (%q) in body: %q", leaky, body)
		}
	}
}

// TestBodyEmptyBodySkipsParsingButStillValidates proves an empty body does
// not bypass validation silently — BodyParser is skipped for an empty body
// (nothing to decode), but the zero-valued struct must still fail its
// "required" rules rather than being treated as implicitly valid.
func TestBodyEmptyBodySkipsParsingButStillValidates(t *testing.T) {
	app := fh.New()
	app.Post("/users", Body(func() any { return &createUserRequest{} }), func(c fh.Ctx) error {
		return c.SendStatus(fh.StatusCreated)
	})
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "POST", "/users", "", nil)
	if status != fh.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for an empty body (zero-valued struct fails required fields), got %d (body=%q)", status, body)
	}
}

// TestBodyNilFactorySkipsValidationEntirely documents the intentional
// escape hatch: a factory returning nil bypasses parsing/validation and
// goes straight to the handler.
func TestBodyNilFactorySkipsValidationEntirely(t *testing.T) {
	called := false
	app := fh.New()
	app.Post("/users", Body(func() any { return nil }), func(c fh.Ctx) error {
		called = true
		return c.SendStatus(fh.StatusOK)
	})
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "POST", "/users", `{invalid-json`, nil)
	if status != fh.StatusOK || !called {
		t.Fatalf("expected a nil factory to bypass validation and reach the handler, got status=%d called=%v", status, called)
	}
}

// TestSkipConfigBypassesValidation verifies the Skip predicate genuinely
// disables validation for matching requests, including ones that would
// otherwise fail.
func TestSkipConfigBypassesValidation(t *testing.T) {
	app := fh.New()
	app.Post("/users", Body(func() any { return &createUserRequest{} }, Config{
		Skip: func(c fh.Ctx) bool { return c.Get("X-Skip-Validation") == "true" },
	}), func(c fh.Ctx) error {
		return c.SendStatus(fh.StatusCreated)
	})
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "POST", "/users", `{"name":"","email":"bad"}`, map[string]string{"X-Skip-Validation": "true"})
	if status != fh.StatusCreated {
		t.Fatalf("expected Skip to bypass validation and admit invalid input, got %d", status)
	}

	status2, _ := doRequest(t, addr, "POST", "/users", `{"name":"","email":"bad"}`, nil)
	if status2 != fh.StatusUnprocessableEntity {
		t.Fatalf("expected validation to still run when Skip's condition is false, got %d", status2)
	}
}

// TestCustomOnErrorReplacesDefaultResponse verifies the configurable error
// responder is actually invoked with the ValidationError, replacing the
// default 422 body.
func TestCustomOnErrorReplacesDefaultResponse(t *testing.T) {
	app := fh.New()
	app.Post("/users", Body(func() any { return &createUserRequest{} }, Config{
		OnError: func(c fh.Ctx, ve *fh.ValidationError) error {
			return c.Status(fh.StatusBadRequest).SendString("custom-error:" + ve.Fields[0].Field)
		},
	}), func(c fh.Ctx) error {
		return c.SendStatus(fh.StatusCreated)
	})
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "POST", "/users", `{"name":"","email":"a@b.com"}`, nil)
	if status != fh.StatusBadRequest {
		t.Fatalf("expected custom OnError status 400, got %d", status)
	}
	if !strings.HasPrefix(body, "custom-error:") {
		t.Fatalf("expected custom OnError body, got %q", body)
	}
}

type customValidatorRequest struct {
	Name     string `json:"name" validate:"required"`
	Password string `json:"password"`
	Confirm  string `json:"confirm"`
}

func (r *customValidatorRequest) Validate() error {
	if r.Password != r.Confirm {
		return fmt.Errorf("password and confirm must match")
	}
	return nil
}

// TestStructTagValidationRunsBeforeCustomValidator verifies the documented
// precedence: struct-tag validation (fh.ValidateStruct) is checked first
// and short-circuits before the type's own Validator.Validate() runs, so a
// request failing both never surfaces the (differently-shaped, 400) custom
// validator error in place of the (422) field error.
func TestStructTagValidationRunsBeforeCustomValidator(t *testing.T) {
	app := fh.New()
	app.Post("/register", Body(func() any { return &customValidatorRequest{} }), func(c fh.Ctx) error {
		return c.SendStatus(fh.StatusCreated)
	})
	addr := testServer(t, app)

	// Missing required "name" AND mismatched password/confirm: struct-tag
	// validation must win and report the 422 field error, not the
	// Validator's 400 error.
	status, body := doRequest(t, addr, "POST", "/register", `{"name":"","password":"a","confirm":"b"}`, nil)
	if status != fh.StatusUnprocessableEntity {
		t.Fatalf("expected the struct-tag validation error (422) to take precedence, got %d (body=%q)", status, body)
	}
}

// TestCustomValidatorRunsWhenStructTagsPass verifies the custom Validator
// is still consulted (and can still reject) once struct-tag validation has
// passed.
func TestCustomValidatorRunsWhenStructTagsPass(t *testing.T) {
	called := false
	app := fh.New()
	app.Post("/register", Body(func() any { return &customValidatorRequest{} }), func(c fh.Ctx) error {
		called = true
		return c.SendStatus(fh.StatusCreated)
	})
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "POST", "/register", `{"name":"Alice","password":"a","confirm":"b"}`, nil)
	if status != fh.StatusBadRequest {
		t.Fatalf("expected the custom Validator's error (400) once struct tags pass, got %d (body=%q)", status, body)
	}
	if called {
		t.Fatal("handler must not run when the custom Validator rejects the request")
	}
	if !strings.Contains(body, "password and confirm must match") {
		t.Fatalf("expected the custom validator's message in the response, got %q", body)
	}

	status2, _ := doRequest(t, addr, "POST", "/register", `{"name":"Alice","password":"a","confirm":"a"}`, nil)
	if status2 != fh.StatusCreated {
		t.Fatalf("expected success once both struct tags and the custom validator pass, got %d", status2)
	}
}

type searchQuery struct {
	Q    string `query:"q" validate:"required,min=1"`
	Page int    `query:"page" validate:"min=1"`
}

// TestQueryRejectsMissingRequiredParam proves query-parameter validation
// actually rejects missing/invalid input rather than treating it as
// optional by default.
func TestQueryRejectsMissingRequiredParam(t *testing.T) {
	called := false
	app := fh.New()
	app.Get("/search", Query(&searchQuery{}), func(c fh.Ctx) error {
		called = true
		return c.SendStatus(fh.StatusOK)
	})
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "GET", "/search", "", nil)
	if status != fh.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for a missing required query param, got %d (body=%q)", status, body)
	}
	if called {
		t.Fatal("handler must not run when query validation fails")
	}
}

// TestQueryAcceptsValidParams is the positive-path sanity check for the
// Query middleware.
func TestQueryAcceptsValidParams(t *testing.T) {
	app := fh.New()
	app.Get("/search", Query(&searchQuery{}), func(c fh.Ctx) error {
		return c.SendStatus(fh.StatusOK)
	})
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "GET", "/search?q=hello&page=1", "", nil)
	if status != fh.StatusOK {
		t.Fatalf("expected 200 for valid query params, got %d", status)
	}
}

type authHeaders struct {
	Authorization string `header:"Authorization" validate:"required"`
}

// TestHeadersRejectsMissingRequiredHeader proves header validation
// actually enforces "required", the most common auth-adjacent use case.
func TestHeadersRejectsMissingRequiredHeader(t *testing.T) {
	called := false
	app := fh.New()
	app.Get("/data", Headers(&authHeaders{}), func(c fh.Ctx) error {
		called = true
		return c.SendStatus(fh.StatusOK)
	})
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "GET", "/data", "", nil)
	if status != fh.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for a missing required header, got %d", status)
	}
	if called {
		t.Fatal("handler must not run when header validation fails")
	}
}

func TestHeadersAcceptsPresentRequiredHeader(t *testing.T) {
	app := fh.New()
	app.Get("/data", Headers(&authHeaders{}), func(c fh.Ctx) error {
		return c.SendStatus(fh.StatusOK)
	})
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "GET", "/data", "", map[string]string{"Authorization": "Bearer x"})
	if status != fh.StatusOK {
		t.Fatalf("expected 200 when the required header is present, got %d", status)
	}
}

// TestValidationErrorResponseDoesNotLeakRawFieldMessageBeyondIntendedText
// checks the standard error envelope only exposes the structured field
// list (code/message pairs derived from the validation rules), not, e.g.,
// a Go error string or struct dump.
func TestValidationErrorResponseShapeIsStructuredNotRaw(t *testing.T) {
	app := fh.New()
	app.Post("/users", Body(func() any { return &createUserRequest{} }), func(c fh.Ctx) error {
		return c.SendStatus(fh.StatusCreated)
	})
	addr := testServer(t, app)

	_, body := doRequest(t, addr, "POST", "/users", `{"name":"","email":""}`, nil)
	for _, want := range []string{`"error":"validation_failed"`, `"errors":`, `"field":"name"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected structured validation response to contain %q, got %q", want, body)
		}
	}
	// Must not contain Go-internal representations.
	for _, leaky := range []string{"reflect.Value", "*fh.ValidationError", "0x"} {
		if strings.Contains(body, leaky) {
			t.Fatalf("validation error response leaked internal representation %q: %q", leaky, body)
		}
	}
}
