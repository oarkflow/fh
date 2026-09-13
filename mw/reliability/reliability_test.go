package reliability

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
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

func doRequest(t *testing.T, addr, method, path, contentType, body string) (statusCode int, respBody string) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	req := fmt.Sprintf("%s %s HTTP/1.1\r\nHost: localhost\r\n", method, path)
	if contentType != "" {
		req += "Content-Type: " + contentType + "\r\n"
	}
	if body != "" {
		req += fmt.Sprintf("Content-Length: %d\r\n", len(body))
	}
	req += "Connection: close\r\n\r\n" + body

	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	resp, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	raw := string(resp)
	head, rest, found := strings.Cut(raw, "\r\n\r\n")
	if !found {
		head = raw
	} else {
		respBody = rest
	}
	lines := strings.Split(head, "\r\n")
	if len(lines) > 0 {
		var proto, status string
		fmt.Sscan(lines[0], &proto, &status)
		fmt.Sscan(status, &statusCode)
	}
	return
}

// TestNewWithDisabledPolicyIsPassthrough proves ReliabilityPolicy{Enabled:
// false} runs the downstream handler normally without engaging any
// reliability plumbing (no journal/idempotency requirements are imposed).
func TestNewWithDisabledPolicyIsPassthrough(t *testing.T) {
	var calls atomic.Int32
	app := fh.New()
	app.Use(New(fh.ReliabilityPolicy{Enabled: false}))
	app.Get("/", func(c fh.Ctx) error {
		calls.Add(1)
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "GET", "/", "", "")
	if status != fh.StatusOK || body != "ok" {
		t.Fatalf("status=%d body=%q, want 200/\"ok\"", status, body)
	}
	if calls.Load() != 1 {
		t.Fatalf("handler called %d times, want 1", calls.Load())
	}
}

// TestNewWithEnabledPolicyButNoReliabilityRuntimeFailsOpen proves that
// enabling a policy on an app that never configured a Reliability runtime
// (fh.New() with no Reliability config) still runs the handler: ApplyPolicy
// must fail open (via its nil-receiver check) rather than blocking every
// request because the subsystem was never wired up.
func TestNewWithEnabledPolicyButNoReliabilityRuntimeFailsOpen(t *testing.T) {
	var calls atomic.Int32
	app := fh.New()
	app.Use(New(fh.ReliabilityPolicy{Enabled: true}))
	app.Get("/", func(c fh.Ctx) error {
		calls.Add(1)
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "GET", "/", "", "")
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if calls.Load() != 1 {
		t.Fatalf("handler called %d times, want 1", calls.Load())
	}
}

type echoReq struct {
	Name string `json:"name"`
}
type echoRes struct {
	Greeting string `json:"greeting"`
}

// TestEndpointSuccessReturnsHandlerJSON proves the happy path: the request
// body is parsed into Req, Handle is invoked with it, and its Res is
// marshaled back as the JSON response.
func TestEndpointSuccessReturnsHandlerJSON(t *testing.T) {
	app := fh.New()
	app.Post("/", Endpoint(EndpointOptions[echoReq, echoRes]{
		Handle: func(ctx context.Context, c fh.Ctx, req echoReq) (echoRes, error) {
			return echoRes{Greeting: "hello " + req.Name}, nil
		},
	}))
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "POST", "/", "application/json", `{"name":"world"}`)
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200, body=%q", status, body)
	}
	if !strings.Contains(body, `"hello world"`) {
		t.Fatalf("body = %q, want it to contain the handler's greeting", body)
	}
}

// TestEndpointValidateErrorPreventsHandleFromRunning proves a Validate
// failure short-circuits before Handle is ever invoked.
func TestEndpointValidateErrorPreventsHandleFromRunning(t *testing.T) {
	var handleCalls atomic.Int32
	app := fh.New()
	app.Post("/", Endpoint(EndpointOptions[echoReq, echoRes]{
		Validate: func(c fh.Ctx, req *echoReq) error {
			if req.Name == "" {
				return fh.BadRequest("name is required")
			}
			return nil
		},
		Handle: func(ctx context.Context, c fh.Ctx, req echoReq) (echoRes, error) {
			handleCalls.Add(1)
			return echoRes{}, nil
		},
	}))
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "POST", "/", "application/json", `{}`)
	if status != fh.StatusBadRequest {
		t.Fatalf("status = %d, want 400", status)
	}
	if handleCalls.Load() != 0 {
		t.Fatalf("Handle ran %d times despite a Validate failure", handleCalls.Load())
	}
}

// TestEndpointHandleErrorPropagates proves an error returned by Handle
// becomes the response's error (non-2xx), rather than being swallowed into
// a 200.
func TestEndpointHandleErrorPropagates(t *testing.T) {
	app := fh.New()
	app.Post("/", Endpoint(EndpointOptions[echoReq, echoRes]{
		Handle: func(ctx context.Context, c fh.Ctx, req echoReq) (echoRes, error) {
			return echoRes{}, fh.Conflict("already exists")
		},
	}))
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "POST", "/", "application/json", `{"name":"x"}`)
	if status != fh.StatusConflict {
		t.Fatalf("status = %d, want 409", status)
	}
}

// TestEndpointMalformedJSONBodyDoesNotPanicAndReturnsError proves malformed
// input to BodyParser is handled gracefully: no panic, and the request
// fails rather than reaching Handle with a zero-valued/garbage Req.
func TestEndpointMalformedJSONBodyDoesNotPanicAndReturnsError(t *testing.T) {
	var handleCalls atomic.Int32
	app := fh.New()
	app.Post("/", Endpoint(EndpointOptions[echoReq, echoRes]{
		Handle: func(ctx context.Context, c fh.Ctx, req echoReq) (echoRes, error) {
			handleCalls.Add(1)
			return echoRes{}, nil
		},
	}))
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "POST", "/", "application/json", `{not valid json`)
	if status < 400 {
		t.Fatalf("status = %d, want a 4xx/5xx error status for malformed JSON", status)
	}
	if handleCalls.Load() != 0 {
		t.Fatalf("Handle ran %d times despite malformed input", handleCalls.Load())
	}
}

// TestEndpointNilHandleReturnsZeroValueJSON proves that omitting Handle
// (e.g. a not-yet-implemented endpoint stub) returns the zero value of Res
// as JSON rather than panicking on a nil function call.
func TestEndpointNilHandleReturnsZeroValueJSON(t *testing.T) {
	app := fh.New()
	app.Post("/", Endpoint(EndpointOptions[echoReq, echoRes]{}))
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "POST", "/", "application/json", `{"name":"x"}`)
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if !strings.Contains(body, `"greeting":""`) {
		t.Fatalf("body = %q, want the zero-valued Res as JSON", body)
	}
}

// TestEndpointAsyncWithoutQueueConfiguredReturnsError proves the documented
// failure mode: Async: true against an app with no durable queue configured
// returns an explicit error instead of silently dropping the job or
// panicking on a nil Queue.
func TestEndpointAsyncWithoutQueueConfiguredReturnsError(t *testing.T) {
	var handleCalls atomic.Int32
	app := fh.New()
	app.Post("/", Endpoint(EndpointOptions[echoReq, echoRes]{
		Async:     true,
		QueueType: "test.job",
		Handle: func(ctx context.Context, c fh.Ctx, req echoReq) (echoRes, error) {
			handleCalls.Add(1)
			return echoRes{}, nil
		},
	}))
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "POST", "/", "application/json", `{"name":"x"}`)
	if status < 400 {
		t.Fatalf("status = %d, want an error status when Async is requested with no queue configured", status)
	}
	if handleCalls.Load() != 0 {
		t.Fatalf("Handle ran %d times; the async path must not fall through to the sync Handle", handleCalls.Load())
	}
}

// TestEndpointValidateReceivesParsedRequestBody proves Validate is called
// with the actually-parsed request, not a zero value, so field-level
// validation logic operates on real data.
func TestEndpointValidateReceivesParsedRequestBody(t *testing.T) {
	var seenName string
	app := fh.New()
	app.Post("/", Endpoint(EndpointOptions[echoReq, echoRes]{
		Validate: func(c fh.Ctx, req *echoReq) error {
			seenName = req.Name
			return nil
		},
		Handle: func(ctx context.Context, c fh.Ctx, req echoReq) (echoRes, error) {
			return echoRes{Greeting: req.Name}, nil
		},
	}))
	addr := testServer(t, app)

	doRequest(t, addr, "POST", "/", "application/json", `{"name":"alice"}`)
	if seenName != "alice" {
		t.Fatalf("Validate saw name = %q, want %q", seenName, "alice")
	}
}

// TestConcurrentEndpointRequestsDoNotCrossContaminate hammers a single
// Endpoint-backed route with distinct payloads concurrently and checks each
// response reflects its own request's data, guarding against any shared
// mutable state (e.g. a reused Req value, or a data race on EndpointOptions)
// smuggling one request's data into another's response.
func TestConcurrentEndpointRequestsDoNotCrossContaminate(t *testing.T) {
	app := fh.New()
	app.Post("/", Endpoint(EndpointOptions[echoReq, echoRes]{
		Handle: func(ctx context.Context, c fh.Ctx, req echoReq) (echoRes, error) {
			return echoRes{Greeting: "hello " + req.Name}, nil
		},
	}))
	addr := testServer(t, app)

	const n = 40
	var wg sync.WaitGroup
	wg.Add(n)
	errs := make(chan string, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("user-%d", i)
			status, body := doRequest(t, addr, "POST", "/", "application/json", fmt.Sprintf(`{"name":%q}`, name))
			if status != fh.StatusOK {
				errs <- fmt.Sprintf("request %d: status = %d, want 200", i, status)
				return
			}
			want := fmt.Sprintf(`"hello %s"`, name)
			if !strings.Contains(body, want) {
				errs <- fmt.Sprintf("request %d: body = %q, want it to contain %q", i, body, want)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}
